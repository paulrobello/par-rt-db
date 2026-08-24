//! The `RunReaper` arm: TTL expiry sweeps, which are durable writes.

use crate::committer::*;

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
pub(in crate::committer) async fn handle_reaper(ctx: &CommitterCtx) -> Result<(), RtDbError> {
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
            let ids: Vec<(String,)> = match sqlx::query_as(&format!(
                "SELECT id FROM \"{pg_schema_name}\".\"{table_ident}\" \
                 WHERE \"{col}\" IS NOT NULL AND \"{col}\" < $1 \
                 ORDER BY \"{col}\" LIMIT $2"
            ))
            .bind(now)
            .bind(ctx.ttl_batch)
            .fetch_all(&ctx.pool)
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    if matches!(
                        crate::db::database_exists(&ctx.pool, &ctx.db).await,
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
            for (id,) in ids {
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
                        crate::db::database_exists(&ctx.pool, &ctx.db).await,
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
                }
            }
            continue;
        }
        let rows: Vec<(String,)> = match sqlx::query_as(&format!(
            "DELETE FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE id IN (
                 SELECT id FROM \"{pg_schema_name}\".\"{table_ident}\"
                 WHERE \"{col}\" IS NOT NULL AND \"{col}\" < $1
                 ORDER BY \"{col}\" LIMIT $2
             ) RETURNING id"
        ))
        .bind(now)
        .bind(ctx.ttl_batch)
        .fetch_all(&ctx.pool)
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                // A dropped db removes the schema mid-sweep; treat as a no-op
                // exit like the scheduler/cleanup tasks do.
                if matches!(
                    crate::db::database_exists(&ctx.pool, &ctx.db).await,
                    Ok(false)
                ) {
                    return Ok(());
                }
                tracing::warn!(
                    db = %ctx.db, table = %table_name, error = %e,
                    "ttl reaper delete failed"
                );
                continue;
            }
        };
        if rows.is_empty() {
            continue;
        }
        for (id,) in rows {
            write_set.touch(table_name, &id, OpKind::Delete);
        }
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
