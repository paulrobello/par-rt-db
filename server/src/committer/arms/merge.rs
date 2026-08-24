//! The `RunMergeUsers` arm: re-homing an anonymous user's rows.

use crate::committer::*;

/// FM-27 committer arm: per table, select candidate rows whose
/// principal-bearing fields reference `anon_id`, rewrite the docs in Rust,
/// and apply per-row updates via `txn::apply_update` (recomputing indexed
/// columns + bumping version). A unique-index collision on one row (surfaced
/// by the sqlx→RtDbError mapping as `ErrorCode::Conflict`) skips that row
/// into `conflicts` and continues. Publishes through `publish_taps` with
/// `source = "merge"`, `owner = None` (system-initiated) so subscriptions,
/// op-feed, audit, and webhooks all fire.
///
/// Single-writer invariant: document writes happen only here, inside the
/// serialized committer turn. Like `handle_reaper`, statements issue directly
/// on the pool with NO explicit transaction, so a per-row 23505 aborts only
/// that row; unlike it, each successful row is captured on the `WriteSet`
/// with before/after values so `fan_out`'s window checks see the doc crossing
/// an eq boundary (a re-stamp is exactly that).
///
/// Abort semantics: because the per-row statements autocommit, rows restamped
/// before a mid-merge failure (any non-conflict error) are already durable.
/// The abort path therefore does NOT return early — it breaks out of the
/// loops, publishes `publish_taps` + the metric for everything that committed,
/// and only then returns the recorded error. Returning without publishing
/// would leave live subscriptions stale (no `fan_out` ran at all, so the
/// verify-skip safety net cannot help) and silently skip the op-feed/audit/
/// webhook taps — violating the "every durable write publishes here" contract.
pub(in crate::committer) async fn handle_merge_users(
    ctx: &CommitterCtx,
    anon_id: &str,
    real_id: &str,
) -> Result<crate::merge::MergeDbResult, RtDbError> {
    use crate::merge::{
        FieldKind, MergeConflict, MergeDbResult, principal_bearing_fields, rewrite_doc,
    };

    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let pg_schema_name = crate::ddl::pg_schema(&ctx.db);
    let mut result = MergeDbResult::default();
    let mut write_set = WriteSet::default();
    let mut restamped = 0usize;
    let mut abort: Option<RtDbError> = None;

    for (table_name, table_def) in &schema.tables {
        let fields = principal_bearing_fields(table_def);
        if fields.is_empty() {
            continue;
        }
        let indexed = crate::ddl::indexed_fields(table_def);
        let table_ident = crate::ddl::pg_table(table_name);

        // One predicate per principal-bearing field, OR-joined; each binds the
        // anon uid once. Scalar fields use their typed f_ column when indexed,
        // else the jsonb doc path; arrays use jsonb containment.
        let mut predicates: Vec<String> = Vec::new();
        let mut binds = 0usize;
        for pf in &fields {
            binds += 1;
            let ph = format!("${binds}");
            predicates.push(match pf.kind {
                FieldKind::Scalar if indexed.contains(&pf.field) => {
                    format!("\"{}\" = {ph}", crate::ddl::pg_col(&pf.field))
                }
                FieldKind::Scalar => {
                    format!("\"doc\"->'{}' = to_jsonb({ph}::text)", pf.field)
                }
                FieldKind::Array => {
                    format!("\"doc\"->'{}' @> to_jsonb({ph}::text)", pf.field)
                }
            });
        }
        let sql = format!(
            "SELECT \"id\", \"doc\", \"created_at\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE {}",
            predicates.join(" OR ")
        );
        let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64)>(&sql);
        for _ in 0..binds {
            query = query.bind(anon_id);
        }
        let rows = match query.fetch_all(&ctx.pool).await {
            Ok(rows) => rows,
            Err(err) => {
                // Dropped-db guard, mirroring handle_reaper's tolerance: a db
                // removed mid-merge loses its schema — return what restamped.
                if matches!(database_exists(&ctx.pool, &ctx.db).await, Ok(false)) {
                    break;
                }
                // Db alive but the scan failed: abort rather than skip — a skipped
                // table's docs would be permanently stranded after the orchestrator's
                // guarded anon delete (the anon id would no longer exist).
                tracing::error!(
                    db = %ctx.db, table = %table_name, error = %err,
                    "merge: table scan failed; aborting so earlier restamps still publish"
                );
                abort = Some(err.into());
                break;
            }
        };

        let mut conn = match ctx.pool.acquire().await {
            Ok(conn) => conn,
            Err(err) => {
                // Same abort contract as a row error: earlier tables' rows are
                // already committed, so break to the publish-then-return-Err path
                // below instead of early-returning past publish_taps.
                abort = Some(err.into());
                break;
            }
        };
        let mut table_count = 0usize;
        for (id, doc_value, created_at) in rows {
            let serde_json::Value::Object(mut doc) = doc_value else {
                continue;
            };
            // Snapshot the pre-rewrite body first: `fan_out`'s window checks
            // need the before-state (anon uid) to see the doc LEAVING an eq
            // window — a before==after capture would let a skip fire.
            let pre_doc = doc.clone();
            if !rewrite_doc(&mut doc, &fields, anon_id, real_id) {
                continue;
            }
            // This path bypasses the `do_*` write functions, so computed
            // stamping runs here too — a computed expr over a principal-
            // bearing field must see the rewritten uid, never the pre-merge
            // one. A stamp failure follows the same abort contract as a
            // failing apply_update below: rows already committed still
            // publish.
            let doc = match crate::txn::stamp_computed(table_def, doc, now_ms()) {
                Ok(doc) => doc,
                Err(err) => {
                    abort = Some(err);
                    break;
                }
            };
            match crate::txn::apply_update(
                &mut conn,
                &pg_schema_name,
                table_def,
                table_name,
                &id,
                &doc,
            )
            .await
            {
                Ok(()) => {
                    write_set.touch(table_name, &id, OpKind::Patch);
                    write_set.capture_doc(
                        table_name,
                        &id,
                        Some(Some(&pre_doc)),
                        Some(Some(&doc)),
                        Some(created_at),
                    );
                    table_count += 1;
                }
                Err(err) if err.code == crate::error::ErrorCode::Conflict => {
                    // 23505: the restamped row would collide with a row the
                    // real user already owns. Skip, report, keep going.
                    tracing::warn!(
                        db = %ctx.db, table = %table_name, id = %id,
                        "merge: unique conflict, row keeps anon owner"
                    );
                    result.conflicts.push(MergeConflict {
                        table: table_name.clone(),
                        id,
                    });
                }
                Err(err) => {
                    // Non-conflict failure: stop restamping, but the rows that
                    // already committed must still publish below — see the
                    // abort-semantics note on this fn. Breaks the ROW loop
                    // only, so this table's bookkeeping below still runs.
                    abort = Some(err);
                    break;
                }
            }
        }
        if table_count > 0 {
            result.tables.insert(table_name.clone(), table_count);
            restamped += table_count;
        }
        // After the aborted table's bookkeeping, stop walking further tables.
        if abort.is_some() {
            break;
        }
    }

    if !write_set.ops.is_empty() {
        publish_taps(ctx, &schema, &write_set, None, "merge", true, false).await;
    }
    for _ in 0..restamped {
        ctx.metrics.record_merge_doc();
    }
    if let Some(err) = abort {
        tracing::error!(
            db = %ctx.db, error = %err,
            "merge: aborted mid-way; taps were still published for the rows that committed"
        );
        return Err(err);
    }
    Ok(result)
}
