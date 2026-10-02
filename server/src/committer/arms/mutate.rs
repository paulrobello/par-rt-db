//! The `Mutate` arm: the client-facing transaction path.

use crate::committer::*;

pub(in crate::committer) async fn handle_mutate(
    ctx: &CommitterCtx,
    idempotency_key: Option<String>,
    txn: Transaction,
    principal_ctx: PrincipalCtx,
) -> Result<TxnOutcome, RtDbError> {
    // The op-feed / audit / webhook tap sites carry the caller's uid as the
    // write's `principal` — same value the pre-Task-5 `owner` carried.
    let owner = principal_ctx.user_id.as_deref();
    // An empty string is not a meaningful key (it would be one shared dedup
    // slot for the whole db) — treat it the same as no key at all.
    let idempotency_key = idempotency_key.filter(|key| !key.is_empty());

    if let Some(key) = &idempotency_key
        && let Some(results) = mutation_log::check(&ctx.pool, &ctx.db, key).await?
    {
        return Ok(TxnOutcome {
            results,
            write_set: WriteSet::default(),
        });
    }

    // Per-database read-only freeze: after the idempotency replay (a retry of
    // an already-committed write returns its cached result, not an error) and
    // before any work. One indexed lookup per write; principal-agnostic — the
    // freeze covers machine tokens, OAuth users, admin direct mutate, and the
    // forwarded-owner path alike, on every transport. System arms (scheduled
    // fires, workflow advances, TTL reaping) and admin schema ops bypass this
    // gate by construction: they never enter the Mutate arm.
    if crate::db::is_read_only(&ctx.pool, &ctx.db).await? {
        return Err(RtDbError::read_only());
    }

    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    // ENH-011 / ARC-004: enforce per-db storage cap before the first write.
    // Uniform — no admin bypass — `enforce(cap=0)` is a no-op, so an unset cap
    // is the fast path. `enforce` is a cheap stale-read on the hot path (no
    // `pg_total_relation_size` scan in the serialized turn); a per-db background
    // warmer (`run_quota_warmer`) plus this path's post-commit refresh keep the
    // reading current, and the only inline measure is a one-time cold start.
    let storage_cap = ctx.hot.load().max_storage_bytes_per_db;
    if storage_cap > 0
        && let Err(e) = ctx.quotas.enforce(&ctx.pool, &ctx.db, storage_cap).await
    {
        ctx.metrics
            .record_quota_rejection(&ctx.db, crate::metrics::QuotaKind::Storage);
        return Err(e);
    }
    // ARC-003: the dedup row is written inside execute_txn's transaction
    // (atomic with the write it guards), so this arm no longer fills the cache
    // post-commit. The TTL is read live from hot config so a
    // `PATCH /admin/config` to `idempotencyTtlMs` takes effect on the next
    // mutate, no restart.
    let idem = idempotency_key
        .as_deref()
        .map(|key| (key, ctx.hot.load().idempotency_ttl_ms));
    let outcome = match execute_txn(&ctx.pool, &ctx.db, &schema, &txn, &principal_ctx, idem).await {
        Ok(outcome) => outcome,
        Err(err) if crate::txn::is_idempotency_replay(&err) => {
            // A previous execution already committed under this key (its
            // dedup row raced ours inside the serialized turn). Replay the
            // stored result with an empty write set — no second fan-out.
            let key = idempotency_key.as_deref().unwrap_or_default();
            let results = mutation_log::check(&ctx.pool, &ctx.db, key)
                .await?
                .ok_or_else(|| {
                    tracing::error!(
                        db = %ctx.db,
                        key,
                        "idempotency replay flagged but no cached result found"
                    );
                    RtDbError::internal("failed to read cached mutation result")
                })?;
            return Ok(TxnOutcome {
                results,
                write_set: WriteSet::default(),
            });
        }
        Err(err) => return Err(err),
    };
    // Four-tap publication (fan_out → op-feed → audit → webhook → quota-refresh).
    // `owner = principal_ctx.user_id` carries the interactive uid into the
    // op-feed/audit/webhook payloads; `source = "mutate"` distinguishes the
    // interactive tap from scheduled/ttl/migrate.
    publish_taps(
        ctx,
        &schema,
        &outcome.write_set,
        owner,
        "mutate",
        true,
        true,
    )
    .await;

    Ok(outcome)
}
