//! The `RunScheduled` arm: the scheduler's claimed-job execution.

use crate::committer::*;

/// Executes one claimed scheduled job through the normal write path and
/// finalizes its row. Best-effort finalize: the txn has already committed +
/// fanned out by the time we touch the row again, so a finalize failure is
/// logged, not propagated. `at-least-once` recovery (the scheduler's
/// `reset_running` on startup) handles the rare crash window between commit
/// and finalize.
pub(in crate::committer) async fn handle_scheduled(
    ctx: &CommitterCtx,
    id: String,
    kind: String,
    txn: Transaction,
    cron: Option<String>,
    every_ms: Option<i64>,
) -> Result<(), RtDbError> {
    let schema = match ctx.schemas.get(&ctx.pool, &ctx.db).await {
        Ok(schema) => schema,
        Err(err) => {
            let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, "schema load failed").await;
            return Err(err);
        }
    };
    // ENH-011 / ARC-004: enforce per-db storage cap (best-effort stale-read,
    // kept current by the background warmer) before the scheduled write. A
    // scheduled job has no interactive principal; on rejection, mirror the
    // execute_txn-failure path below — record the quota metric, mark the job
    // row errored (so it surfaces in the scheduler admin UI), and return
    // `Ok(())` (the scheduler records the error rather than propagating —
    // fire-and-forget, no caller to surface it to). Uniform — no admin bypass.
    let storage_cap = ctx.hot.load().max_storage_bytes_per_db;
    if storage_cap > 0
        && let Err(e) = ctx.quotas.enforce(&ctx.pool, &ctx.db, storage_cap).await
    {
        ctx.metrics
            .record_quota_rejection(&ctx.db, crate::metrics::QuotaKind::Storage);
        let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, &e.message).await;
        return Ok(());
    }
    match execute_txn(&ctx.pool, &ctx.db, &schema, &txn, &PrincipalCtx::bypass()).await {
        Ok(outcome) => {
            // Four-tap publication (fan_out → op-feed → audit → webhook → quota-
            // refresh). Scheduled jobs carry no interactive principal
            // (`owner = None`); `source = "scheduled"` distinguishes from
            // mutate/ttl/migrate in delivered payloads.
            publish_taps(
                ctx,
                &schema,
                &outcome.write_set,
                None,
                "scheduled",
                true,
                true,
            )
            .await;
            let finalize = match kind.as_str() {
                "oneshot" => scheduler::finalize_one_shot_done(&ctx.pool, &ctx.db, &id).await,
                "cron" => match cron.as_deref() {
                    Some(expr) => match scheduler::next_fire(expr, now_ms()) {
                        Ok(next) => {
                            scheduler::finalize_recurring_next(&ctx.pool, &ctx.db, &id, next).await
                        }
                        Err(err) => {
                            scheduler::mark_error(&ctx.pool, &ctx.db, &id, &err.message).await
                        }
                    },
                    None => {
                        scheduler::mark_error(&ctx.pool, &ctx.db, &id, "cron job missing expr")
                            .await
                    }
                },
                // Interval re-arms from each actual fire time (cron parity:
                // windows missed during the fire's latency are skipped, not
                // backfilled).
                "interval" => match every_ms {
                    Some(ms) => {
                        scheduler::finalize_recurring_next(&ctx.pool, &ctx.db, &id, now_ms() + ms)
                            .await
                    }
                    None => {
                        scheduler::mark_error(
                            &ctx.pool,
                            &ctx.db,
                            &id,
                            "interval job missing everyMs",
                        )
                        .await
                    }
                },
                other => {
                    scheduler::mark_error(&ctx.pool, &ctx.db, &id, &format!("unknown kind {other}"))
                        .await
                }
            };
            if let Err(err) = finalize {
                tracing::error!(db = %ctx.db, %id, error = %err, "scheduled job finalize failed");
            }
        }
        Err(err) => {
            // Execution failed (precondition/step error). No retry (see spec):
            // one-shot records the error and stops; cron logs and reschedules.
            let msg = err.message.clone();
            match kind.as_str() {
                "cron" => match cron.as_deref() {
                    Some(expr) => match scheduler::next_fire(expr, now_ms()) {
                        Ok(next) => {
                            let _ = scheduler::reschedule_recurring_error(
                                &ctx.pool, &ctx.db, &id, next, &msg,
                            )
                            .await;
                        }
                        Err(_) => {
                            let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, &msg).await;
                        }
                    },
                    None => {
                        let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, &msg).await;
                    }
                },
                "interval" => match every_ms {
                    Some(ms) => {
                        let _ = scheduler::reschedule_recurring_error(
                            &ctx.pool,
                            &ctx.db,
                            &id,
                            now_ms() + ms,
                            &msg,
                        )
                        .await;
                    }
                    None => {
                        let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, &msg).await;
                    }
                },
                _ => {
                    let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, &msg).await;
                }
            }
        }
    }
    Ok(())
}
