//! The schema arms: `RunPushSchema` and `RunRestoreSchema`.

use crate::committer::*;
use crate::schema::SchemaDefExt;

/// Restores the database's schema shape to a captured `schema_history`
/// snapshot, mirroring `handle_migrate`'s structure (load current → begin tx →
/// DDL → meta upsert → commit → cache refresh → capture → fan-out).
///
/// Single-writer invariant: like `handle_migrate`, this opens its own
/// `pool.begin()` inside the committer task's serialized turn (the only
/// writer) and never calls `execute_txn`. The destructive reconcile
/// (`ddl::reconcile_schema_destructive`) drops tables/columns/indexes present
/// in the live shape but absent from the target snapshot, then
/// `apply_schema_additive` creates the inverse — all in the one tx.
///
/// Two `schema_history` captures bracket the apply: the OUTGOING (current)
/// schema first, so the restore is itself a versioned, undoable operation; the
/// INCOMING (target) schema after, so "latest history row == live schema"
/// stays invariant. Both best-effort (warn, never propagate — the schema change
/// already committed by then), matching the audit/webhook tap discipline.
/// Restore does NOT write `audit_log`/`webhook` rows — it is DDL, not DocOps;
/// `schema_history` is its trail.
///
/// Subscription re-evaluation: the reconcile returns the touched table set,
/// which feeds `fan_out` as a `WriteSet` (table-level re-run, the safe
/// over-approximation — no per-doc `doc_values` are captured for a shape
/// change, mirroring `handle_migrate`).
/// Schema push inside the serialized committer turn. The backfill UPDATEs a
/// push can run (ttl `defaultDurationMs`, computed entries) are document
/// writes, so they belong here rather than on the HTTP task; afterwards the
/// backfill-affected tables' subscriptions re-run table-level (the
/// restore/migrate over-approximation — no per-doc `doc_values`, no DocOps,
/// so `publish_taps` runs with `docop_taps=false`: push backfills are
/// invisible to the op feed/audit/webhooks by the same discipline restore
/// has).
pub(in crate::committer) async fn handle_push_schema(
    ctx: &CommitterCtx,
    schema: crate::schema::SchemaDef,
) -> Result<crate::schema::SchemaDef, RtDbError> {
    let (applied, backfilled) = crate::ddl::push_schema(&ctx.pool, &ctx.db, schema).await?;
    ctx.schemas.put(&ctx.db, applied.clone()).await;
    if let Err(err) =
        crate::schema_history::capture(&ctx.pool, &ctx.db, "push", None, &applied).await
    {
        tracing::warn!(db = %ctx.db, error = %err, "schema history capture failed");
    }
    let write_set = WriteSet {
        tables: backfilled,
        ..Default::default()
    };
    publish_taps(ctx, &applied, &write_set, None, "push", false, false).await;
    Ok(applied)
}

pub(in crate::committer) async fn handle_restore_schema(
    ctx: &CommitterCtx,
    target_version: i64,
) -> Result<i64, RtDbError> {
    let current = crate::db::load_schema(&ctx.pool, &ctx.db)
        .await?
        .ok_or_else(|| RtDbError::not_found("database has no schema"))?;
    let entry = crate::schema_history::get(&ctx.pool, &ctx.db, target_version)
        .await?
        .ok_or_else(|| RtDbError::not_found("schema version not found"))?;
    let target: crate::schema::SchemaDef = serde_json::from_value(entry.schema).map_err(|e| {
        tracing::error!(db = %ctx.db, error = %e, "failed to decode schema snapshot");
        RtDbError::internal("failed to decode schema snapshot")
    })?;
    target.validate()?;

    // Safety net: capture the outgoing schema first so the restore is undoable.
    if let Err(err) =
        crate::schema_history::capture(&ctx.pool, &ctx.db, "restore", None, &current).await
    {
        tracing::warn!(db = %ctx.db, error = %err, "schema history capture (outgoing) failed");
    }

    let mut tx = ctx.pool.begin().await?;
    let touched =
        crate::ddl::reconcile_schema_destructive(&mut tx, &ctx.db, &current, &target).await?;
    // Persist the target blob (same shape as push/migrate tails).
    let schema_json = serde_json::to_value(&target).map_err(|e| {
        tracing::error!(db = %ctx.db, error = %e, "failed to serialize schema");
        RtDbError::internal("failed to serialize schema")
    })?;
    let schema_name = crate::ddl::pg_schema(&ctx.db);
    sqlx::query(&format!(
        "INSERT INTO \"{schema_name}\".meta (key, value) VALUES ('schema', $1) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value"
    ))
    .bind(schema_json)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    ctx.schemas.put(&ctx.db, target.clone()).await;

    // Capture the incoming (target) state so the latest history row == live schema.
    if let Err(err) =
        crate::schema_history::capture(&ctx.pool, &ctx.db, "restore", None, &target).await
    {
        tracing::warn!(db = %ctx.db, error = %err, "schema history capture (incoming) failed");
    }

    // Re-evaluate subscriptions: dropped tables/columns invalidate their subs.
    // Table-level re-run (no per-doc `doc_values`) — the safe over-approximation
    // for a shape change, same as `handle_migrate`. Routed through `publish_taps`
    // with `docop_taps=false`: restore is pure DDL, no DocOps are produced, so
    // the op-feed/audit/webhook taps are skipped — but the exception is now
    // visible at the call site rather than hidden by a direct `fan_out` call,
    // and a future change to the tap sequence stays consistent across handlers.
    let write_set = WriteSet {
        tables: touched.into_iter().collect(),
        ..Default::default()
    };
    publish_taps(ctx, &target, &write_set, None, "restore", false, false).await;

    Ok(target_version)
}
