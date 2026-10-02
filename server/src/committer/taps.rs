//! The four-tap publication every durable write passes through.
//!
//! **This is the load-bearing "every durable write publishes here" contract**
//! referenced from `CLAUDE.md`. It lives in its own module so the visibility
//! rule is structural: `publish_taps` is `pub(super)`, so only the committer
//! module and its arms can call it, and a new durable-write sink cannot
//! quietly re-derive the sequence somewhere else.

use super::*;

/// ENH-047: which arms write the audit/webhook taps WHERE.
///
/// **Transactional (in `execute_txn_with_side`'s transaction, atomic with the
/// documents):** `handle_mutate` ("mutate"), `handle_scheduled`
/// ("scheduled"), `handle_workflow_advance` ("workflow").
///
/// **Best-effort, post-commit** (still via `publish_taps`' pool-form calls,
/// a failure warned and never propagated): `handle_reaper` ("ttl"),
/// `handle_migrate` ("migrate"), `handle_merge_users` ("merge"). These arms
/// own their own transactions (reaper batch tx, merge per-row tx, migrate tx)
/// but their per-row/batch loops make a failure inside the tx ambiguous at
/// arm level (a reaper batch fails its whole batch; a merge row conflict
/// skips just that row), so they keep the simpler post-commit discipline.
///
/// `handle_push_schema` ("push") and `handle_restore_schema` ("restore") emit
/// no DocOps (`docop_taps = false`) and never touch audit/webhooks either way.
///
/// The op-feed `publish`, subscription `fan_out`, and the cross-replica
/// NOTIFYs always stay post-commit: they are in-memory and must only observe
/// committed writes.
///
/// ---
///
/// Four-tap publication of a durable write: subscription `fan_out` → op-feed
/// `publish` → audit-log `write_audit_rows` → webhook `enqueue_for_ops`, with
/// an optional fire-and-forget storage-cache refresh at the end.
///
/// **This is the load-bearing "every durable write publishes here" contract**
/// referenced from `CLAUDE.md`. Folding the four `handle_*` arms' shared tail
/// into one helper converts a silent omission into a single call-site decision:
/// a new durable-write sink calls `publish_taps` instead of re-deriving the
/// four-tap sequence, and a non-DocOp sink (e.g. `handle_restore_schema`) can
/// opt out of the op-feed/audit/webhook taps without leaving a "missing tap"
/// gap at the call site.
///
/// Parameters:
/// - `schema`: post-write schema the subscription re-runs read against.
/// - `write_set`: the durable write's touched tables + per-doc ops.
/// - `owner`: interactive principal's user id, or `None` for system-initiated
///   writes (scheduled jobs, TTL reaper, schema migrations).
/// - `source`: short tag embedded in audit rows, webhook payloads, and op-feed
///   attribution — `"mutate"` / `"scheduled"` / `"ttl"` / `"migrate"` /
///   `"merge"`.
/// - `docop_taps`: when `false`, only `fan_out` runs. Used by paths that are
///   DDL, not DocOps (e.g. `handle_restore_schema`) so the exception is
///   visible at the call site rather than reading as a missed tap.
/// - `refresh_quota_cache`: when `true`, fire-and-forget a storage-cache
///   refresh after the taps (growing writes — mutate/scheduled/migrate). The
///   reaper (`false`) only frees storage; restore (`false`) changes no bytes.
///
/// The audit and webhook taps are best-effort: a logging/enqueue failure is
/// warned and never propagated. The write has already committed and fanned
/// out by the time these run, so they cannot be allowed to fail the mutation.
pub(in crate::committer) async fn publish_taps(
    ctx: &CommitterCtx,
    schema: &crate::schema::SchemaDef,
    write_set: &WriteSet,
    owner: Option<&str>,
    source: &'static str,
    docop_taps: bool,
    refresh_quota_cache: bool,
) {
    publish_taps_skip_side_writes(
        ctx,
        schema,
        write_set,
        owner,
        source,
        docop_taps,
        refresh_quota_cache,
        // None of these arms write the audit/webhook taps in-transaction.
        crate::txn::TxnSideWrites {
            source: "",
            audit: false,
            webhooks: false,
        },
    )
    .await;
}

/// [`publish_taps`] with an ENH-047 override: when `side.audit`/`side.webhooks`
/// are set, the audit and webhook INSERTs already ran on the write's open
/// transaction inside `execute_txn_with_side`, so the post-commit tap skips
/// them. Everything else (fan-out, NOTIFYs, op-feed) runs as before.
#[allow(clippy::too_many_arguments)] // same shape as `publish_taps` + the ENH-047 override
pub(in crate::committer) async fn publish_taps_skip_side_writes(
    ctx: &CommitterCtx,
    schema: &crate::schema::SchemaDef,
    write_set: &WriteSet,
    owner: Option<&str>,
    source: &'static str,
    docop_taps: bool,
    refresh_quota_cache: bool,
    side: crate::txn::TxnSideWrites,
) {
    ctx.subs
        .fan_out(&ctx.read_pool, &ctx.db, schema, write_set)
        .await;
    // ARC-001: cross-replica subscription invalidation. The local `fan_out`
    // above only reaches subscribers connected to THIS replica; peers holding
    // subscriptions over the same database need the write set too, or their
    // clients stay stale until their own replica happens to write. Published
    // once per commit (not per op) and before the `docop_taps` early return —
    // a DDL-only write (`handle_restore_schema`) invalidates subscriptions on
    // every replica exactly as it does locally.
    if ctx.multi_instance {
        crate::notify::publish_write_set(&ctx.read_pool, &ctx.instance_id, &ctx.db, write_set)
            .await;
    }
    if !docop_taps {
        return;
    }
    // Op-feed completeness: every durable document write publishes here.
    ctx.op_feed.publish(&ctx.db, owner, &write_set.ops).await;
    // ENH-022 Stage 2 / ARC-006: cross-instance op-feed fan-out. When
    // `multi_instance` is on, emit batched `pg_notify`s (chunked under
    // `notify::OP_NOTIFY_CHUNK_LIMIT`) so peer replicas sharing this Postgres
    // inject the events into their own rings. Spawned off the committer turn —
    // mirroring the quota-cache-refresh spawn below — so a 1000-row
    // deleteByQuery or TTL sweep never holds the serialized turn on a string of
    // `pg_notify` round trips. Best-effort, like the audit/webhook taps below:
    // a `pg_notify` failure logs and never fails the committed write. NOT a
    // second writer: the write already committed inside this serialized turn;
    // NOTIFY only notifies.
    if ctx.multi_instance {
        let pool = ctx.read_pool.clone();
        let instance_id = ctx.instance_id.clone();
        let db = ctx.db.clone();
        let owner = owner.map(|s| s.to_string());
        let ops = write_set.ops.clone();
        tokio::spawn(async move {
            crate::notify::publish_ops(&pool, &instance_id, &db, owner.as_deref(), source, &ops)
                .await;
        });
    }
    // Durable audit tap (the persistent counterpart to the op-feed above).
    // Skipped when the audit rows were already written in-transaction
    // (ENH-047).
    if ctx.audit_log_enabled
        && !side.audit
        && let Err(err) =
            crate::audit::write_audit_rows(&ctx.read_pool, &ctx.db, owner, source, &write_set.ops)
                .await
    {
        tracing::warn!(db = %ctx.db, source, error = %err, "audit log write failed");
    }
    // Webhook enqueue tap — mirrors the audit tap above. Skipped when the
    // delivery rows were already enqueued in-transaction (ENH-047).
    if ctx.webhooks_enabled
        && !side.webhooks
        && let Err(err) =
            crate::webhook::enqueue_for_ops(&ctx.read_pool, &ctx.db, owner, source, &write_set.ops)
                .await
    {
        tracing::warn!(db = %ctx.db, source, error = %err, "webhook enqueue failed");
    }
    if refresh_quota_cache && ctx.hot.load().max_storage_bytes_per_db != 0 {
        // ARC-103: gate the per-write cache refresh on a configured cap, mirroring
        // `run_quota_warmer`'s tick gate above exactly. On a default instance
        // (cap = 0) `enforce` returns Ok(0) immediately and the spawned catalog
        // aggregate would populate a cache nothing reads — a per-mutation
        // `pg_total_relation_size` scan competing with the serialized committer
        // for the same pool. With a cap configured the warmer (ARC-004) keeps the
        // reading bounded-stale; this spawn tightens it right after a growing
        // write so a subsequent enforce sees the fresh size. Divergent gates are
        // how this drifted in the first place — keep them identical.
        let quotas = ctx.quotas.clone();
        let pool = ctx.read_pool.clone();
        let db = ctx.db.clone();
        tokio::spawn(async move {
            if let Err(e) = quotas.refresh(&pool, &db).await {
                tracing::warn!(db = %db, error = %e, "quota cache refresh failed");
            }
        });
    }
}
