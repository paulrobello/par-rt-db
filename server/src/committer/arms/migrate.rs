//! The `RunMigrate` arm: schema-migration execution.

use crate::committer::*;

/// Applies a declarative migration through the committer, mirroring
/// `handle_mutate`'s post-commit tap-site block so the same four downstream
/// surfaces (subscription fan-out, op-feed, audit, webhook) observe the writes.
///
/// Single-writer invariant: this runs in the committer task's turn and opens its
/// own `pool.begin()` inside that task (the only writer). It never calls
/// `execute_txn`. The pre-migration schema is read from `meta` (NOT the cache)
/// so `plan_migration` operates on authoritative state; `apply_migration` re-reads
/// `meta` inside the tx for its DDL decisions. The derived schema is validated
/// before any DML because directive targets (rename `to`, changeType `to`,
/// evalExpr `set`) are new user input interpolated into SQL.
///
/// `dry_run` runs the DDL+DML to collect the preview but rolls the tx back and
/// publishes through no tap site. On commit, the derived schema is persisted to
/// `meta` (same shape as `ddl::push_schema`'s tail), the cache is refreshed, and
/// the four taps fire with `owner = None` and `source = "migrate"` (no
/// interactive principal, like `handle_scheduled`).
pub(in crate::committer) async fn handle_migrate(
    ctx: &CommitterCtx,
    request: crate::migrate::MigrateRequest,
) -> Result<crate::migrate::MigrateResult, RtDbError> {
    let schema = crate::db::load_schema(&ctx.pool, &ctx.db)
        .await?
        .ok_or_else(|| RtDbError::not_found("database has no schema"))?;
    let derived = crate::migrate::plan_migration(&schema, &request.directives)?;
    // The directive targets (rename `to`, changeType `to`, evalExpr `set`) are
    // new user input that ends up interpolated into SQL; validating the derived
    // schema catches invalid identifiers/types before any DML runs.
    // `plan_migration` folds structurally but does not call `validate`.
    derived.validate()?;
    derived
        .check_table_quota(ctx.hot.load().max_tables_per_db)
        .inspect_err(|_e| {
            ctx.metrics
                .record_quota_rejection(&ctx.db, crate::metrics::QuotaKind::Tables);
        })?;
    // ENH-011 / ARC-004: enforce per-db storage cap (best-effort stale-read,
    // kept current by the background warmer) before the migration writes.
    // Uniform — no admin bypass (migrate is admin-only, but a cap applies the
    // same as any other growing write). `enforce(cap=0)` is a no-op (unset cap).
    let storage_cap = ctx.hot.load().max_storage_bytes_per_db;
    if storage_cap > 0 {
        ctx.quotas
            .enforce(&ctx.pool, &ctx.db, storage_cap)
            .await
            .inspect_err(|_e| {
                ctx.metrics
                    .record_quota_rejection(&ctx.db, crate::metrics::QuotaKind::Storage);
            })?;
    }

    let mut tx = ctx.pool.begin().await?;
    let fx = crate::migrate::apply_migration(
        &mut tx,
        &ctx.db,
        &request.directives,
        &derived,
        request.dry_run,
    )
    .await?;

    if request.dry_run {
        // Preview only: the DDL+DML ran inside the tx to produce `fx.reports`,
        // but nothing is committed and no tap site fires.
        tx.rollback().await?;
        return Ok(crate::migrate::MigrateResult {
            applied: false,
            schema: derived,
            directives: fx.reports,
        });
    }

    // Persist the derived schema (single jsonb blob in "{db_<db>}".meta — same
    // shape as `ddl::push_schema`'s tail upsert). The committer is the only
    // writer for this db, so the read-modify-write under the committer turn is
    // safe. `pg_schema(db)` is already validated/lowercased by `db::create`.
    let schema_json = serde_json::to_value(&derived)
        .map_err(|e| RtDbError::internal(format!("failed to serialize schema: {e}")))?;
    let schema_name = crate::ddl::pg_schema(&ctx.db);
    sqlx::query(&format!(
        "INSERT INTO \"{schema_name}\".meta (key, value) VALUES ('schema', $1) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value"
    ))
    .bind(schema_json)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    ctx.schemas.put(&ctx.db, derived.clone()).await;

    // Schema history capture — best-effort, like the audit/webhook taps below.
    // `derived` is the post-migration schema; principal is None (migrate carries
    // no interactive principal — matches the audit `owner = None` for migrate).
    if let Err(err) =
        crate::schema_history::capture(&ctx.pool, &ctx.db, "migrate", None, &derived).await
    {
        tracing::warn!(db = %ctx.db, error = %err, "schema history capture failed");
    }

    // Four-tap publication (fan_out → op-feed → audit → webhook → quota-refresh)
    // — same contract as `handle_mutate`. The hand-built `WriteSet` carries the
    // touched tables (the subscription re-run gate) and the per-doc ops;
    // `docs`/`doc_values` empty ⇒ table-level re-run, the safe over-approximation
    // for a migration (some ops may touch docs whose ids weren't recorded at
    // the fine-grained (table, id) level — re-running is always sound, never
    // under-approximates). `owner = None`, `source = "migrate"`.
    let write_set = WriteSet {
        tables: fx.touched,
        ops: fx.ops.clone(),
        ..Default::default()
    };
    publish_taps(ctx, &derived, &write_set, None, "migrate", true, true).await;

    Ok(crate::migrate::MigrateResult {
        applied: true,
        schema: derived,
        directives: fx.reports,
    })
}
