//! The `RunReaper` arm: TTL expiry sweeps, which are durable writes.

use crate::committer::*;
use std::collections::HashSet;

/// Runs one TTL reaper sweep. For each table with `ttl`, batch-deletes expired
/// rows and publishes through the four tap sites with `source = "ttl"`. TTL
/// deletes are system-initiated (`owner = None`), bypassing per-row auth like
/// scheduled jobs. Fire-and-forget — errors are logged, not surfaced; a failed
/// delete retries on the next sweep. Each table's delete is an independent
/// statement so one table's failure does not abort the others.
///
/// Single-writer invariant: this runs inside the committer task's serialized
/// turn. It issues the DELETE directly (not via `execute_txn`) because TTL
/// expiry is not a client mutation — there is no idempotency key, no owner
/// pre-check, and no per-step result to return. A delete captures no
/// `doc_values`, so `fan_out` table-level re-runs (sound over-approximation).
pub(in crate::committer) async fn handle_reaper(
    ctx: &CommitterCtx,
    admission_epoch: i64,
) -> Result<(), RtDbError> {
    let (quiesced_at_admission, admission_generation) =
        crate::db::quiesce_admission(&ctx.read_pool, &ctx.db).await?;
    if quiesced_at_admission {
        return Ok(());
    }
    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let now = now_ms();
    let mut write_set = WriteSet::default();
    for (table_name, table_def) in &schema.tables {
        let Some(ttl) = &table_def.ttl else {
            continue;
        };
        let pg_schema_name = crate::ddl::pg_schema(&ctx.db);
        let table_ident = crate::ddl::pg_table(table_name);
        let col = crate::ddl::pg_col(&ttl.field);
        // FM-33: when some table declares an `onDelete` field referencing this
        // one, a bulk DELETE would strand (cascade/setNull) or ignore
        // (restrict) the children. Select the expired batch, then per-row
        // cascade with `force_hard = true` — TTL expiry is a real delete even
        // on a softDelete table; the reaper is the collector of last resort.
        if crate::txn::has_on_delete_children(&schema, table_name) {
            let ids: Vec<(String,)> = match sqlx::query_as(sqlx::AssertSqlSafe(format!(
                "SELECT id FROM \"{pg_schema_name}\".\"{table_ident}\" \
                 WHERE \"{col}\" IS NOT NULL AND \"{col}\" < $1 \
                 ORDER BY \"{col}\" LIMIT $2"
            )))
            .bind(now)
            .bind(ctx.ttl_batch)
            .fetch_all(&ctx.pool)
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    if matches!(
                        crate::db::database_exists(&ctx.read_pool, &ctx.db).await,
                        Ok(false)
                    ) {
                        return Ok(());
                    }
                    tracing::warn!(
                        db = %ctx.db, table = %table_name, error = %e,
                        "ttl reaper select failed"
                    );
                    continue;
                }
            };
            if ids.is_empty() {
                continue;
            }
            let Ok(mut conn) = ctx.pool.acquire().await else {
                continue;
            };
            // `visited` is shared across the sweep so a row already cascaded
            // by an earlier expired row's cascade is skipped, not an error;
            // the budget is fresh per initiating row (`MAX_CASCADE_ROWS` is
            // per initiating delete).
            let mut visited: HashSet<(String, String)> = HashSet::new();
            let mut reclaimed: u64 = 0;
            for (id,) in ids {
                crate::db::ensure_admission_epoch(&mut conn, &ctx.db, admission_epoch).await?;
                let mut cascade_rows = 0usize;
                if let Err(e) = crate::txn::delete_row_cascade(
                    &mut conn,
                    &pg_schema_name,
                    &schema,
                    table_name,
                    &id,
                    &mut write_set,
                    &mut visited,
                    &mut cascade_rows,
                    true,
                )
                .await
                {
                    if matches!(
                        crate::db::database_exists(&ctx.read_pool, &ctx.db).await,
                        Ok(false)
                    ) {
                        return Ok(());
                    }
                    // Per-row statements autocommit, so cascade work before
                    // the failure is durable and stays in `write_set` — it
                    // publishes below. The failed row remains expired and
                    // retries on the next sweep (at-least-once).
                    tracing::warn!(
                        db = %ctx.db, table = %table_name, doc_id = %id, error = %e,
                        "ttl reaper cascade failed"
                    );
                } else {
                    reclaimed += 1;
                }
            }
            if reclaimed > 0 {
                // Per-pass observability (card 01a192a72c): one line per
                // reclaiming table pass, next to the `record_ttl_expired`
                // metric the same ops add to.
                tracing::info!(
                    db = %ctx.db, table = %table_name, reclaimed,
                    "ttl reaper pass reclaimed rows"
                );
            }
            continue;
        }
        // Change-feed contract: the batch delete and its change-log rows
        // commit together — a committed expiry is observable, a rolled-back
        // one leaves nothing behind.
        let Ok(mut tx) = ctx.pool.begin().await else {
            continue;
        };
        if sqlx::query("SELECT pg_advisory_xact_lock($1)")
            .bind(crate::db::quiesce_key(&ctx.db))
            .execute(&mut *tx)
            .await
            .is_err()
        {
            continue;
        }
        if crate::db::ensure_quiesce_generation(&mut tx, &ctx.db, admission_generation)
            .await
            .is_err()
        {
            let _ = tx.rollback().await;
            return Ok(());
        }
        if crate::db::ensure_admission_epoch(&mut tx, &ctx.db, admission_epoch)
            .await
            .is_err()
        {
            let _ = tx.rollback().await;
            return Ok(());
        }
        let rows: Vec<(String,)> = match sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "DELETE FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE id IN (
                 SELECT id FROM \"{pg_schema_name}\".\"{table_ident}\"
                 WHERE \"{col}\" IS NOT NULL AND \"{col}\" < $1
                 ORDER BY \"{col}\" LIMIT $2
             ) RETURNING id"
        )))
        .bind(now)
        .bind(ctx.ttl_batch)
        .fetch_all(&mut *tx)
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                // A dropped db removes the schema mid-sweep; treat as a no-op
                // exit like the scheduler/cleanup tasks do. The dropped tx
                // rolls the whole batch back atomically.
                if matches!(
                    crate::db::database_exists(&ctx.read_pool, &ctx.db).await,
                    Ok(false)
                ) {
                    return Ok(());
                }
                tracing::warn!(db = %ctx.db, table = %table_name, error = %e,
                    "ttl reaper delete failed"
                );
                continue;
            }
        };
        if rows.is_empty() {
            continue;
        }
        let mut batch_ws = WriteSet::default();
        for (id,) in &rows {
            batch_ws.touch(table_name, id, OpKind::Delete);
        }
        if let Err(e) = crate::change_log::append(&mut tx, &pg_schema_name, &batch_ws).await {
            tracing::warn!(db = %ctx.db, table = %table_name, error = %e,
                "ttl reaper change-log append failed; retrying next sweep"
            );
            continue;
        }
        if let Err(e) = tx.commit().await {
            tracing::warn!(db = %ctx.db, table = %table_name, error = %e,
                "ttl reaper batch commit failed; retrying next sweep"
            );
            continue;
        }
        // Per-pass observability (card 01a192a72c): one line per reclaiming
        // table pass, next to the `record_ttl_expired` metric.
        tracing::info!(
            db = %ctx.db, table = %table_name, reclaimed = rows.len() as u64,
            "ttl reaper pass reclaimed rows"
        );
        write_set.tables.extend(batch_ws.tables);
        write_set.docs.extend(batch_ws.docs);
        write_set.ops.append(&mut batch_ws.ops);
    }
    if write_set.ops.is_empty() {
        return Ok(());
    }
    // Four-tap publication (fan_out → op-feed → audit → webhook). No quota
    // refresh — the reaper only frees storage. `owner = None`, `source = "ttl"`
    // (system-initiated expiry, no interactive principal). On the cascade path
    // the ops include the children (hard-deleted or setNull-patched), matching
    // the op-feed's per-durable-write contract.
    publish_taps(ctx, &schema, &write_set, None, "ttl", true, false).await;
    for _ in 0..write_set.ops.len() {
        ctx.metrics.record_ttl_expired();
    }
    Ok(())
}
