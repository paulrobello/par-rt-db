//! The `RunPurge` arm: admin-initiated reclamation of insertOnly rows, which
//! are durable writes. Companion to the TTL reaper: the reaper collects rows
//! past their `ttl` field on its own cadence; the purge removes rows a
//! named predicate matches, early, when a client bug floods a table.

use crate::committer::*;
use std::collections::HashSet;

/// The reply payload of a `RunPurge` arm. `deleted` counts initiating rows
/// (cascade children are not counted); `truncated` mirrors the by-query
/// steps — true when more rows still match the predicate and the caller
/// should re-run.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PurgeOutcome {
    pub deleted: u64,
    pub truncated: bool,
}

/// Runs one admin purge: batch-deletes the rows `filter` selects on one
/// insertOnly table and publishes through the four tap sites with
/// `source = "purge"`. System-initiated like the TTL reaper (`owner = None`,
/// bypassing per-row auth) — the admin gate on the HTTP route is the
/// authorization. The table gate is re-checked here so a future caller
/// cannot skip the HTTP handler's check.
///
/// Single-writer invariant: this runs inside the committer task's serialized
/// turn. Like the reaper it issues the DELETEs directly (not via
/// `execute_txn`) — there is no idempotency key and no per-step result; a
/// delete captures no `doc_values`, so `fan_out` table-level re-runs (sound
/// over-approximation). Each row deletes through `delete_row_cascade` with
/// `force_hard = true` so softDelete rows leave storage and `onDelete`
/// children are handled — the purge is a reclamation tool, not a soft delete.
pub(in crate::committer) async fn handle_purge(
    ctx: &CommitterCtx,
    table_name: &str,
    filter: Option<&crate::query::FilterExpr>,
    limit: u32,
    admission_epoch: i64,
) -> Result<PurgeOutcome, RtDbError> {
    let (quiesced_at_admission, _admission_generation) =
        crate::db::quiesce_admission(&ctx.read_pool, &ctx.db).await?;
    if quiesced_at_admission {
        return Err(RtDbError::read_only());
    }
    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let table_def = schema
        .tables
        .get(table_name)
        .ok_or_else(|| RtDbError::not_found(format!("table '{table_name}' not found")))?;
    if !table_def.insert_only {
        return Err(RtDbError::bad_request(format!(
            "purge applies only to insertOnly tables ('{table_name}' is not insertOnly)"
        )));
    }
    let pg_schema_name = crate::ddl::pg_schema(&ctx.db);
    let table_ident = crate::ddl::pg_table(table_name);
    let (where_sql, binds, limit_ph) = crate::query::compile_scan_where(
        table_def,
        &crate::auth::PrincipalCtx::bypass(),
        None,
        filter,
    )?;
    let base = format!("SELECT \"id\" FROM \"{pg_schema_name}\".\"{table_ident}\"");
    let sql = if where_sql.is_empty() {
        format!("{base} ORDER BY \"created_at\", \"id\" LIMIT ${limit_ph}")
    } else {
        format!("{base} WHERE {where_sql} ORDER BY \"created_at\", \"id\" LIMIT ${limit_ph}")
    };
    let mut query = sqlx::query_as::<_, (String,)>(sqlx::AssertSqlSafe(sql));
    for bind in binds {
        query = match bind {
            crate::dsl::EqBind::Text(v) => query.bind(v),
            crate::dsl::EqBind::Num(v) => query.bind(v),
            crate::dsl::EqBind::Bool(v) => query.bind(v),
            crate::dsl::EqBind::I64(v) => query.bind(v),
        };
    }
    // +1 so `truncated` is knowable without a second scan.
    query = query.bind(i64::from(limit) + 1);
    let rows = query.fetch_all(&ctx.pool).await?;
    let truncated = rows.len() as u32 > limit;
    let take = std::cmp::min(rows.len(), limit as usize);
    let mut write_set = WriteSet::default();
    let mut deleted: u64 = 0;
    if take > 0 {
        let Ok(mut conn) = ctx.pool.acquire().await else {
            return Err(RtDbError::internal("purge could not acquire a connection"));
        };
        // `visited` is shared across the batch so a row already cascaded by
        // an earlier row's delete is skipped, not an error; the budget is
        // fresh per initiating row (`MAX_CASCADE_ROWS` is per initiating
        // delete), matching the reaper's cascade path.
        let mut visited: HashSet<(String, String)> = HashSet::new();
        let mut failure: Option<RtDbError> = None;
        for (id,) in &rows[..take] {
            if let Err(e) =
                crate::db::ensure_admission_epoch(&mut conn, &ctx.db, admission_epoch).await
            {
                failure = Some(e);
                break;
            }
            let mut cascade_rows = 0usize;
            if let Err(e) = crate::txn::delete_row_cascade(
                &mut conn,
                &pg_schema_name,
                &schema,
                table_name,
                id,
                &mut write_set,
                &mut visited,
                &mut cascade_rows,
                true,
            )
            .await
            {
                // Per-row statements autocommit, so the prefix before the
                // failure is durable and stays in `write_set` — it publishes
                // below. The failed row still matches the predicate and a
                // re-run picks it up (at-least-once). Stop here: the error
                // surfaces to the operator after the taps.
                tracing::warn!(
                    db = %ctx.db, table = %table_name, doc_id = %id, error = %e,
                    "admin purge row delete failed"
                );
                failure = Some(e);
                break;
            }
            deleted += 1;
        }
        if let Some(e) = failure {
            if !write_set.ops.is_empty() {
                publish_taps(ctx, &schema, &write_set, None, "purge", true, false).await;
            }
            return Err(e);
        }
    }
    if write_set.ops.is_empty() {
        return Ok(PurgeOutcome {
            deleted: 0,
            truncated,
        });
    }
    // Four-tap publication (fan_out → op-feed → audit → webhook). No quota
    // refresh — the purge only frees storage. `owner = None`, `source =
    // "purge"` (admin-initiated reclamation through the admin gate).
    publish_taps(ctx, &schema, &write_set, None, "purge", true, false).await;
    tracing::info!(
        db = %ctx.db,
        table = %table_name,
        deleted,
        truncated,
        "admin purge committed"
    );
    Ok(PurgeOutcome { deleted, truncated })
}
