//! The `RunBulkDeleteStart`/`RunBulkDeleteChunk` arms: the background bulk
//! delete (ARC-005). The start arm is the read-only id freeze; each chunk arm
//! deletes exactly the ids it carries, skipping ids already gone (idempotent),
//! and publishes through the four tap sites with `source = "bulk"`. Not
//! forwardable (Stage 4c): a shadow replica's submit falls through to the
//! local CONFLICT backstop — the honest "reach the owning replica" answer,
//! same shape as the fire-and-forget arms.

use crate::committer::*;
use std::collections::HashSet;

/// Reply payload of `RunBulkDeleteStart`: the frozen matching ids (order:
/// `created_at, id` — stable across chunks) and the over-cap flag the HTTP
/// handler maps to BadRequest. Public because integration tests exercise the
/// over-cap branch by submitting the arm with a test-only tiny cap.
#[derive(Debug)]
pub struct BulkStartResult {
    pub ids: Vec<String>,
    pub matched_over_cap: bool,
}

/// Runs the read-only id freeze inside the committer's serialized turn,
/// mirroring the purge arm's guarded-transaction preamble (quiesce advisory
/// lock, admission epoch, transactional re-read of the freeze flags) and its
/// statement timeout. The scan compiles the filter with the SUBMITTING
/// caller's per-row auth context — the same set a `DeleteByQuery` step would
/// select at this moment, so a caller cannot delete rows it cannot read.
/// `cap + 1` is selected so `matched_over_cap` is knowable without a second
/// scan; the rolled-back read-only turn writes nothing.
pub(in crate::committer) async fn handle_bulk_delete_start(
    ctx: &CommitterCtx,
    table_name: &str,
    filter: Option<&crate::query::FilterExpr>,
    principal_ctx: &crate::auth::PrincipalCtx,
    id_cap: usize,
    admission_epoch: i64,
) -> Result<BulkStartResult, RtDbError> {
    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let table_def = schema
        .tables
        .get(table_name)
        .ok_or_else(|| RtDbError::not_found(format!("table '{table_name}' not found")))?;
    // Admission capture mirrors the purge/reaper arms: quiesce state read
    // off-turn, the generation re-verified transactionally below.
    let (quiesced_at_admission, admission_generation) =
        crate::db::quiesce_admission(&ctx.read_pool, &ctx.db).await?;
    if quiesced_at_admission {
        return Err(RtDbError::read_only());
    }

    let pg_schema_name = crate::ddl::pg_schema(&ctx.db);
    let table_ident = crate::ddl::pg_table(table_name);
    let (where_sql, binds, _limit_ph) = crate::query::compile_scan_where(
        table_def,
        principal_ctx,
        principal_ctx.user_id.as_deref(),
        filter,
    )?;
    let base = format!("FROM \"{pg_schema_name}\".\"{table_ident}\"");
    let where_clause = if where_sql.is_empty() {
        String::new()
    } else {
        format!(" WHERE {where_sql}")
    };
    let ids_sql = format!(
        "SELECT \"id\" {base}{where_clause} \
         ORDER BY \"created_at\", \"id\" LIMIT ${}",
        binds.len() + 1
    );

    let mut tx = ctx.pool.begin().await?;
    // Quiesce fence + epoch + statement timeout + freeze re-read — the purge
    // arm's guard stack on the same open transaction.
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(crate::db::quiesce_key(&ctx.db))
        .execute(&mut *tx)
        .await?;
    crate::db::ensure_admission_epoch(&mut tx, &ctx.db, admission_epoch).await?;
    let row: (String, bool, bool, i64) = sqlx::query_as(sqlx::AssertSqlSafe(
        "SELECT set_config('statement_timeout', $1, true), \
         COALESCE((SELECT read_only FROM rtdb_auth.databases WHERE name = $2), FALSE), \
         COALESCE((SELECT quiesced FROM rtdb_auth.databases WHERE name = $2), FALSE), \
         COALESCE((SELECT quiesce_generation FROM rtdb_auth.databases WHERE name = $2), 0)",
    ))
    .bind(crate::txn::STATEMENT_TIMEOUT_MS.to_string())
    .bind(&ctx.db)
    .fetch_one(&mut *tx)
    .await?;
    if row.1 || row.2 || row.3 != admission_generation {
        tx.rollback().await?;
        return Err(RtDbError::read_only());
    }

    let mut ids_query = sqlx::query_as::<_, (String,)>(sqlx::AssertSqlSafe(ids_sql));
    for bind in &binds {
        ids_query = match bind {
            crate::dsl::EqBind::Text(v) => ids_query.bind(v.clone()),
            crate::dsl::EqBind::Num(v) => ids_query.bind(*v),
            crate::dsl::EqBind::Bool(v) => ids_query.bind(*v),
            crate::dsl::EqBind::I64(v) => ids_query.bind(*v),
        };
    }
    ids_query = ids_query.bind(i64::try_from(id_cap).unwrap_or(i64::MAX).saturating_add(1));
    let rows: Vec<(String,)> = ids_query.fetch_all(&mut *tx).await?;
    tx.rollback().await?;
    let over_cap = rows.len() > id_cap;
    Ok(BulkStartResult {
        ids: rows.into_iter().map(|(id,)| id).collect(),
        matched_over_cap: over_cap,
    })
}

/// Runs one chunk of a bulk delete. Deletes EXACTLY the ids it carries
/// (selected in the freeze turn — no re-scan), skipping ids already gone
/// (hard-deleted, or soft-deleted on a softDelete table) so a re-submitted op
/// is idempotent and `deleted` counts only rows that actually left. Same
/// guarded preamble as the start arm. Each initiating row deletes through
/// `delete_row_cascade` with `force_hard = false` — the FM-33 soft-delete-aware
/// path, identical to the `DeleteByQuery` step's. The cascade budget is shared
/// across the chunk exactly as the `DeleteByQuery` step shares one across the
/// step: over-budget conflicts and the chunk turn fails atomically (the op's
/// remaining ids stay; a re-submit resumes over the survivors).
pub(in crate::committer) async fn handle_bulk_delete_chunk(
    ctx: &CommitterCtx,
    table_name: &str,
    ids: &[String],
    owner: Option<&str>,
    admission_epoch: i64,
) -> Result<u64, RtDbError> {
    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let table_def = schema
        .tables
        .get(table_name)
        .ok_or_else(|| RtDbError::not_found(format!("table '{table_name}' not found")))?;
    let (quiesced_at_admission, admission_generation) =
        crate::db::quiesce_admission(&ctx.read_pool, &ctx.db).await?;
    if quiesced_at_admission {
        return Err(RtDbError::read_only());
    }
    let pg_schema_name = crate::ddl::pg_schema(&ctx.db);
    let table_ident = crate::ddl::pg_table(table_name);

    let mut tx = ctx.pool.begin().await?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(crate::db::quiesce_key(&ctx.db))
        .execute(&mut *tx)
        .await?;
    crate::db::ensure_admission_epoch(&mut tx, &ctx.db, admission_epoch).await?;
    let row: (String, bool, bool, i64) = sqlx::query_as(sqlx::AssertSqlSafe(
        "SELECT set_config('statement_timeout', $1, true), \
         COALESCE((SELECT read_only FROM rtdb_auth.databases WHERE name = $2), FALSE), \
         COALESCE((SELECT quiesced FROM rtdb_auth.databases WHERE name = $2), FALSE), \
         COALESCE((SELECT quiesce_generation FROM rtdb_auth.databases WHERE name = $2), 0)",
    ))
    .bind(crate::txn::STATEMENT_TIMEOUT_MS.to_string())
    .bind(&ctx.db)
    .fetch_one(&mut *tx)
    .await?;
    if row.1 || row.2 || row.3 != admission_generation {
        tx.rollback().await?;
        return Err(RtDbError::read_only());
    }

    // Select which of the chunk's ids are still LIVE (present, and not
    // soft-deleted on a softDelete table). Gone ids are skipped, not errors:
    // the delete is idempotent and `deleted` stays honest.
    let live_sql = if table_def.soft_delete {
        format!(
            "SELECT \"id\" FROM \"{pg_schema_name}\".\"{table_ident}\" \
             WHERE \"id\" = ANY($1) AND \"deleted_at\" IS NULL"
        )
    } else {
        format!(
            "SELECT \"id\" FROM \"{pg_schema_name}\".\"{table_ident}\" \
             WHERE \"id\" = ANY($1)"
        )
    };
    let live: Vec<(String,)> = sqlx::query_as(sqlx::AssertSqlSafe(live_sql))
        .bind(ids)
        .fetch_all(&mut *tx)
        .await?;

    let mut write_set = WriteSet::default();
    let mut visited: HashSet<(String, String)> = HashSet::new();
    let mut cascade_rows = 0usize;
    let mut deleted: u64 = 0;
    for (id,) in &live {
        let mut cctx = crate::txn::CascadeCtx {
            conn: &mut tx,
            pg_schema_name: &pg_schema_name,
            schema: &schema,
            write_set: &mut write_set,
            visited: &mut visited,
            force_hard: false,
        };
        crate::txn::delete_row_cascade(&mut cctx, table_name, id, &mut cascade_rows).await?;
        deleted += 1;
    }

    if deleted > 0 {
        // Durable change feed commits with the deletes (outbox discipline),
        // then the four-tap publication post-commit (fan_out -> op-feed ->
        // audit -> webhook), source = "bulk", owner = the submitting
        // principal's user id when it had one (machine tokens write with
        // owner = None, matching a machine-initiated mutate).
        crate::change_log::append(&mut tx, &pg_schema_name, &write_set).await?;
        tx.commit().await?;
        publish_taps(ctx, &schema, &write_set, owner, "bulk", true, false).await;
    } else {
        tx.commit().await?;
    }
    Ok(deleted)
}
