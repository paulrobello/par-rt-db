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

    // Per-database read-only freeze: after the idempotency replay (a retry of
    // an already-committed write returns its cached result, not an error) and
    // before any work. Principal-agnostic — the freeze covers machine tokens,
    // OAuth users, admin direct mutate, and the forwarded-owner path alike, on
    // every transport. System arms (scheduled fires, workflow advances, TTL
    // reaping) and admin schema ops bypass this gate by construction: they
    // never enter the Mutate arm.
    //
    // ENH-048: the freeze flag and the idempotency lookup are no longer
    // separate pre-transaction round trips — `execute_txn_with_side`'s
    // combined preamble statement (below) reads both inside the write
    // transaction, in the documented order (replay wins over the freeze).
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
    // mutate, no restart. ENH-048: the key also drives the in-transaction
    // preamble (cache lookup + freeze read on the first statement).
    let idem = idempotency_key
        .as_deref()
        .map(|key| (key, ctx.hot.load().idempotency_ttl_ms));
    let preamble = crate::txn::MutatePreamble {
        idem_key: idempotency_key.as_deref(),
        check_freeze: true,
    };
    // ENH-047: the audit and webhook side-writes run INSIDE execute_txn's
    // transaction (the outbox guarantee), so this arm's tap skips them.
    let side = crate::txn::TxnSideWrites {
        source: "mutate",
        audit: ctx.audit_log_enabled,
        webhooks: ctx.webhooks_enabled,
    };
    let outcome = match crate::txn::execute_txn_with_side(
        &ctx.pool,
        &ctx.db,
        &schema,
        &txn,
        &principal_ctx,
        idem,
        side,
        preamble,
    )
    .await
    {
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
    // Four-tap publication (fan_out → op-feed → [audit+webhook already done
    // in-transaction] → quota-refresh).
    // `owner = principal_ctx.user_id` carries the interactive uid into the
    // op-feed payload; `source = "mutate"` distinguishes the interactive tap
    // from scheduled/ttl/migrate. Audit and webhook were written on the open
    // transaction inside `execute_txn_with_side` (ENH-047), so the post-commit
    // tap runs only the in-memory/NOTIFY work.
    publish_taps_skip_side_writes(
        ctx,
        &schema,
        &outcome.write_set,
        owner,
        "mutate",
        true,
        true,
        side,
    )
    .await;

    Ok(outcome)
}
