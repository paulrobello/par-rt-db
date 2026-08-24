//! The four-tap publication every durable write passes through.
//!
//! **This is the load-bearing "every durable write publishes here" contract**
//! referenced from `CLAUDE.md`. It lives in its own module so the visibility
//! rule is structural: `publish_taps` is `pub(super)`, so only the committer
//! module and its arms can call it, and a new durable-write sink cannot
//! quietly re-derive the sequence somewhere else.

use super::*;

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
    ctx.subs
        .fan_out(&ctx.pool, &ctx.db, schema, write_set)
        .await;
    // ARC-001: cross-replica subscription invalidation. The local `fan_out`
    // above only reaches subscribers connected to THIS replica; peers holding
    // subscriptions over the same database need the write set too, or their
    // clients stay stale until their own replica happens to write. Published
    // once per commit (not per op) and before the `docop_taps` early return —
    // a DDL-only write (`handle_restore_schema`) invalidates subscriptions on
    // every replica exactly as it does locally.
    if ctx.multi_instance {
        crate::notify::publish_write_set(&ctx.pool, &ctx.instance_id, &ctx.db, write_set).await;
    }
    if !docop_taps {
        return;
    }
    // Op-feed completeness: every durable document write publishes here.
    ctx.op_feed.publish(&ctx.db, owner, &write_set.ops).await;
    // ENH-022 Stage 2: cross-instance op-feed fan-out. When `multi_instance` is
    // on, emit one `pg_notify` per DocOp so peer replicas sharing this Postgres
    // inject the event into their own rings. Best-effort, like the audit/webhook
    // taps below — a `pg_notify` failure logs and never fails the committed
    // write. NOT a second writer: the write already committed inside this
    // serialized turn; NOTIFY only notifies.
    if ctx.multi_instance {
        crate::notify::publish_ops(
            &ctx.pool,
            &ctx.instance_id,
            &ctx.db,
            owner,
            source,
            &write_set.ops,
        )
        .await;
    }
    // Durable audit tap (the persistent counterpart to the op-feed above).
    if ctx.audit_log_enabled
        && let Err(err) =
            crate::audit::write_audit_rows(&ctx.pool, &ctx.db, owner, source, &write_set.ops).await
    {
        tracing::warn!(db = %ctx.db, source, error = %err, "audit log write failed");
    }
    // Webhook enqueue tap — mirrors the audit tap above.
    if ctx.webhooks_enabled
        && let Err(err) =
            crate::webhook::enqueue_for_ops(&ctx.pool, &ctx.db, owner, source, &write_set.ops).await
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
        let pool = ctx.pool.clone();
        let db = ctx.db.clone();
        tokio::spawn(async move {
            let _ = quotas.refresh(&pool, &db).await;
        });
    }
}
