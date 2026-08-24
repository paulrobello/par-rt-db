//! The `RunWorkflowAdvance` arm: one step of a durable workflow.

use crate::committer::*;

/// Advance a claimed workflow run (FM-29). Executes the current step's txn
/// as the system (bypass) principal — same fire path as `handle_scheduled` —
/// publishes through the tap sites with `source = "workflow"`, and loops
/// while the next gate is already due. awaitSignal steps take a side-table
/// branch instead — park / consume-signal / timeout — writing no documents
/// and firing no taps. Claim discipline: the row stays
/// `running` for the whole loop (the scheduler only claims `pending` rows),
/// so a no-sleep chain completes in one turn, bounded by the spec's step
/// count (≤ `workflows::MAX_WORKFLOW_STEPS` at submit). At-least-once per
/// step: a crash after `execute_txn` commits but before the bookkeeping
/// write re-executes that step on resume (`workflows::reset_running`).
pub(in crate::committer) async fn handle_workflow_advance(
    ctx: &CommitterCtx,
    mut row: crate::workflows::WorkflowRow,
) -> Result<(), RtDbError> {
    let schema = match ctx.schemas.get(&ctx.pool, &ctx.db).await {
        Ok(schema) => schema,
        Err(err) => {
            let outcome = failed_outcome(&row, "schema load failed");
            // mark_failed logs internally on its own write failure; this
            // discard is intentional — `err` (the schema-load failure) is
            // already returned below and is the one that matters to the caller.
            let _ = crate::workflows::mark_failed(
                &ctx.pool,
                &ctx.db,
                &row.id,
                &outcome,
                "schema load failed",
            )
            .await;
            return Err(err);
        }
    };
    // ENH-011/ARC-004 storage cap — checked once at entry, like the other
    // arms. Unlike `handle_scheduled`'s terminal `mark_error`, a quota
    // rejection counts as a retryable step failure: raising the cap
    // mid-retry lets the run recover.
    let storage_cap = ctx.hot.load().max_storage_bytes_per_db;
    let mut quota_err: Option<RtDbError> = None;
    if storage_cap > 0
        && let Err(e) = ctx.quotas.enforce(&ctx.pool, &ctx.db, storage_cap).await
    {
        quota_err = Some(e);
    }
    loop {
        // Cancel/terminal check at each step boundary (spec §Semantics): a
        // row cancelled or deleted out from under a running advance stops it.
        match crate::workflows::status_of(&ctx.pool, &ctx.db, &row.id).await {
            Ok(Some(crate::protocol::WorkflowStatus::Running)) => {}
            Ok(Some(_)) | Ok(None) => return Ok(()),
            Err(err) => return Err(err),
        }
        let Some(step) = row.spec.steps.get(row.current_step as usize) else {
            // Defensive: `current_step` past the last index means a corrupt
            // row (submit-time validation and the state machine keep it in
            // range). Mark failed rather than panic the committer task.
            let outcome = failed_outcome(&row, "step index out of range");
            let msg = "workflow current_step out of range";
            crate::workflows::mark_failed(&ctx.pool, &ctx.db, &row.id, &outcome, msg).await?;
            return Ok(());
        };
        let retry = step.retry.unwrap_or_default();
        if let Some(sig) = &step.await_signal {
            // awaitSignal: side-table only — no document writes, no taps
            // (spec §Server design). The wake discriminator is the claimed
            // row itself: `signal_payload` set = delivered, else
            // `waited_since` NULL = first arrival, set = gate expired.
            let now = now_ms();
            if let Some(payload) = row.signal_payload.take() {
                ctx.metrics
                    .record_workflow_step(crate::metrics::WorkflowStepOutcome::Success);
                let finished = row.current_step as usize + 1 >= row.spec.steps.len();
                let record = crate::protocol::StepOutcome {
                    step_index: row.current_step,
                    status: crate::protocol::OutcomeStatus::Success,
                    attempts: row.attempts + 1,
                    at: now,
                    error: None,
                    signal: Some(payload),
                };
                if finished {
                    crate::workflows::finalize_success(&ctx.pool, &ctx.db, &row.id, &record)
                        .await?;
                    return Ok(());
                }
                crate::workflows::record_signal_success(
                    &ctx.pool,
                    &ctx.db,
                    &row.id,
                    row.current_step + 1,
                    &record,
                )
                .await?;
                row.current_step += 1;
                row.attempts = 0;
                row.waited_since = None;
                row.wait_name = None;
                // Same next-gate logic as the txn path:
                let next = &row.spec.steps[row.current_step as usize];
                // Clamp before the u64→i64 cast: a serde-accepted u64 above
                // i64::MAX would wrap negative ⇒ an instantly-due gate.
                let sleep_ms = next.sleep_before_ms.unwrap_or(0).min(i64::MAX as u64) as i64;
                let gate = now.saturating_add(sleep_ms);
                if gate > now_ms() {
                    crate::workflows::set_pending(&ctx.pool, &ctx.db, &row.id, gate).await?;
                    return Ok(());
                }
                continue;
            }
            // Timeout gate, clamped like the sleep gate (u64→i64 wrap
            // hazard); an omitted timeoutMs is never due — cancel is the
            // only escape.
            let timeout_gate = match sig.timeout_ms {
                Some(ms) => now.saturating_add(ms.min(i64::MAX as u64) as i64),
                None => i64::MAX,
            };
            if row.waited_since.is_none() {
                // First arrival (or crash-recovered boundary): park.
                crate::workflows::park_waiting(
                    &ctx.pool,
                    &ctx.db,
                    &row.id,
                    row.attempts,
                    &sig.name,
                    timeout_gate,
                )
                .await?;
                return Ok(());
            }
            // The row parked and its gate expired: a timed-out attempt. A
            // retry waits the FULL timeoutMs again — never backoff.
            row.attempts += 1;
            if row.attempts < retry.max_attempts {
                crate::workflows::park_waiting(
                    &ctx.pool,
                    &ctx.db,
                    &row.id,
                    row.attempts,
                    &sig.name,
                    timeout_gate,
                )
                .await?;
                ctx.metrics
                    .record_workflow_step(crate::metrics::WorkflowStepOutcome::Retry);
                return Ok(());
            }
            let error = format!("awaitSignal '{}' timed out", sig.name);
            let record = crate::protocol::StepOutcome {
                step_index: row.current_step,
                status: crate::protocol::OutcomeStatus::Failed,
                attempts: row.attempts,
                at: now,
                error: Some(error.clone()),
                signal: None,
            };
            crate::workflows::mark_failed(&ctx.pool, &ctx.db, &row.id, &record, &error).await?;
            ctx.metrics
                .record_workflow_step(crate::metrics::WorkflowStepOutcome::Fail);
            return Ok(());
        }
        // Every step carries exactly one of txn/awaitSignal (submit-time
        // `validate_spec`); the branch above handled the latter. A txn-less
        // step here is a corrupt row — mark failed rather than panic the
        // committer task, mirroring the out-of-range guard above.
        let txn = match step.txn.as_ref() {
            Some(txn) => txn,
            None => {
                let outcome = failed_outcome(&row, "step has neither txn nor awaitSignal");
                crate::workflows::mark_failed(
                    &ctx.pool,
                    &ctx.db,
                    &row.id,
                    &outcome,
                    "workflow step missing txn",
                )
                .await?;
                return Ok(());
            }
        };
        let exec = match quota_err.take() {
            Some(e) => Err(e),
            None => execute_txn(&ctx.pool, &ctx.db, &schema, txn, &PrincipalCtx::bypass()).await,
        };
        match exec {
            Ok(outcome) => {
                // Four-tap publication (fan_out → op-feed → audit → webhook →
                // quota-refresh). Workflow steps fire as the system principal
                // (`owner = None`); `source = "workflow"` distinguishes them
                // from scheduled/ttl/migrate in delivered payloads.
                publish_taps(
                    ctx,
                    &schema,
                    &outcome.write_set,
                    None,
                    "workflow",
                    true,
                    true,
                )
                .await;
                ctx.metrics
                    .record_workflow_step(crate::metrics::WorkflowStepOutcome::Success);
                let now = now_ms();
                let finished = row.current_step as usize + 1 >= row.spec.steps.len();
                let record = crate::protocol::StepOutcome {
                    step_index: row.current_step,
                    status: crate::protocol::OutcomeStatus::Success,
                    attempts: row.attempts + 1,
                    at: now,
                    error: None,
                    signal: None,
                };
                if finished {
                    crate::workflows::finalize_success(&ctx.pool, &ctx.db, &row.id, &record)
                        .await?;
                    return Ok(());
                }
                // Write the boundary while staying `running` (the scheduler
                // only claims `pending` rows), then compute the next gate:
                // due now → keep looping; future → release to `pending`.
                crate::workflows::record_step_success(
                    &ctx.pool,
                    &ctx.db,
                    &row.id,
                    row.current_step + 1,
                    &record,
                )
                .await?;
                row.current_step += 1;
                row.attempts = 0;
                let next = &row.spec.steps[row.current_step as usize];
                // Clamp before the u64→i64 cast: a serde-accepted u64 above
                // i64::MAX would wrap negative ⇒ an instantly-due gate.
                let sleep_ms = next.sleep_before_ms.unwrap_or(0).min(i64::MAX as u64) as i64;
                let gate = now.saturating_add(sleep_ms);
                if gate > now_ms() {
                    crate::workflows::set_pending(&ctx.pool, &ctx.db, &row.id, gate).await?;
                    return Ok(());
                }
            }
            Err(err) => {
                let now = now_ms();
                row.attempts += 1;
                if row.attempts < retry.max_attempts {
                    // Clamp before the u64→i64 cast (same wrap hazard as the
                    // sleep gate above).
                    let backoff = crate::workflows::backoff_ms(&retry, row.attempts)
                        .min(i64::MAX as u64) as i64;
                    crate::workflows::schedule_retry(
                        &ctx.pool,
                        &ctx.db,
                        &row.id,
                        row.attempts,
                        now.saturating_add(backoff),
                    )
                    .await?;
                    ctx.metrics
                        .record_workflow_step(crate::metrics::WorkflowStepOutcome::Retry);
                    return Ok(());
                }
                let record = crate::protocol::StepOutcome {
                    step_index: row.current_step,
                    status: crate::protocol::OutcomeStatus::Failed,
                    attempts: row.attempts,
                    at: now,
                    error: Some(err.message.clone()),
                    signal: None,
                };
                crate::workflows::mark_failed(&ctx.pool, &ctx.db, &row.id, &record, &err.message)
                    .await?;
                ctx.metrics
                    .record_workflow_step(crate::metrics::WorkflowStepOutcome::Fail);
                return Ok(());
            }
        }
    }
}

/// Terminal record for an advance that failed before any step could run
/// (schema load) or on a corrupt row (step index out of range).
pub(in crate::committer) fn failed_outcome(
    row: &crate::workflows::WorkflowRow,
    error: &str,
) -> crate::protocol::StepOutcome {
    crate::protocol::StepOutcome {
        step_index: row.current_step,
        status: crate::protocol::OutcomeStatus::Failed,
        attempts: row.attempts.max(1),
        at: now_ms(),
        error: Some(error.to_string()),
        signal: None,
    }
}
