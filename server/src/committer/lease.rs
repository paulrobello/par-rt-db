//! Per-database write-ownership lease (ENH-022 Stage 4): the advisory-lock
//! key, the lease acquisition, and the CONFLICT reply a SHADOW committer gives
//! a write it must not execute.

use super::*;

/// Stable advisory-lock key for `db`'s ownership lease (ENH-022 Stage 4,
/// option A1 of docs/superpowers/specs/2026-08-22-multi-instance-stage4-design.md):
/// the first 8 bytes of the db name's SHA-256, read as an i64.
/// The fixed substring every ownership-conflict CONFLICT carries. ARC-009:
/// producer (`acquire_ownership_lease`, `reply_ownership_conflict`) and
/// consumer (`forward::is_shadow_ownership_conflict`) both interpolate this
/// constant, so the wording can no longer drift — previously `forward.rs`
/// matched a hard-coded copy and a message edit here silently broke the
/// takeover-race detection.
pub(crate) const SHADOW_CONFLICT_MARKER: &str = "single-writer lease";

pub(in crate::committer) fn db_ownership_key(db: &str) -> i64 {
    let hex = crate::db::sha256_hex(db);
    u64::from_str_radix(&hex[..16], 16).unwrap_or(0) as i64
}

/// ENH-022 Stage 4: acquire `db`'s single-writer ownership lease — a dedicated
/// one-connection pool whose backend takes `pg_try_advisory_lock(key(db))`.
/// The caller runs the db's committer ON this pool, so the lease and every
/// write share one backend: no other replica can acquire mid-flight, and an
/// owner's death (kill -9, container stop) drops the backend's session and
/// releases the lock — the next replica's acquire is the failover. Unbounded
/// idle/lifetime keep the locked connection alive for as long as the pool
/// lives; eviction/drop-db drops it, closing the session and releasing the
/// lease. Returns `CONFLICT` when another replica holds the lease.
///
/// ARC-001: a Postgres session advisory lock does not survive a backend
/// replacement. If the lease backend dies (idle restart, OOM, operator
/// `pg_terminate_backend`) and the pool reopens a connection, the dead backend
/// released the lock the moment it died — another replica may already own it —
/// and the NEW backend holds nothing, so this replica would write unlocked.
/// Fencing is therefore applied at TWO points, both cheap:
/// 1. Right after the pool acquire inside this function (fresh acquisition):
///    `pg_try_advisory_lock` is taken explicitly; `false` => another replica
///    owns the lease => `CONFLICT`.
/// 2. [`verify_lease`] at the start of every write turn: confirms the lease
///    backend STILL holds the lock. A lost lock (backend replaced, operator
///    `pg_advisory_unlock_all`) flips the `lease_lost` flag and demotes the
///    committer to a shadow instead of writing unlocked.
///
/// The pool uses `min_connections(0)`: no background maintenance task. A
/// maintenance task would race to re-take the advisory lock on a fresh backend
/// after the lease backend's death — silently "re-owning" the lease outside
/// the committer's control — and its in-flight connect() retry loop would
/// starve the pool's single slot. With min=0 the lease connection is opened
/// only by the committer's own acquire, always fenced.
///
/// Returns the pool plus the `lease_lost` flag (also stored on the channel
/// entry and `CommitterCtx`).
pub(in crate::committer) async fn acquire_ownership_lease(
    pool: &PgPool,
    db: &str,
) -> Result<(PgPool, Arc<AtomicBool>), RtDbError> {
    let key = db_ownership_key(db);
    let lease_lost = Arc::new(AtomicBool::new(false));
    let lease = sqlx::pool::PoolOptions::<sqlx::Postgres>::new()
        .max_connections(1)
        .min_connections(0)
        .idle_timeout(None)
        .max_lifetime(None)
        // ARC-001: fail fast when the lease backend is unreachable. Without
        // this, `acquire` blocks for sqlx's default 30s on every write turn
        // of a demoted/dead-lease owner, starving the committer turn and any
        // takeover attempt. A starved lease connection errors immediately
        // and the turn fails closed instead of hanging.
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect_lazy_with((*pool.connect_options()).clone());
    let mut conn = match lease.acquire().await {
        Ok(conn) => conn,
        Err(err) => {
            // ARC-001: a failed acquire here means the advisory lock is held
            // by SOMEONE (another replica). Probe pg_locks to decide: if the
            // lock is taken, the correct answer to the caller is CONFLICT
            // (the lease is contested), not an internal error — the
            // shadow/takeover machinery relies on CONFLICT to retry or
            // demote.
            let held: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype = 'advisory' \
                 AND granted AND ((classid::bigint << 32) | objid::bigint) = $1)",
            )
            .bind(key)
            .fetch_one(pool)
            .await
            .unwrap_or(true);
            tracing::error!(
                db,
                error = %err,
                lease_held_elsewhere = held,
                "ownership lease connection failed"
            );
            if held {
                return Err(RtDbError::new(
                    crate::error::ErrorCode::Conflict,
                    format!(
                        "database '{db}' is owned by another instance ({SHADOW_CONFLICT_MARKER}); \
                         writes must reach the owning replica until it releases"
                    ),
                ));
            }
            return Err(RtDbError::internal(
                "failed to establish ownership lease connection",
            ));
        }
    };
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(key)
        .fetch_one(&mut *conn)
        .await
        .map_err(|err| {
            tracing::error!(db, error = %err, "ownership lease acquire failed");
            RtDbError::internal("failed to acquire ownership lease")
        })?;
    if !acquired {
        // The connection returns to the pool and is closed with it on drop —
        // a failed acquire leaves nothing held.
        return Err(RtDbError::new(
            crate::error::ErrorCode::Conflict,
            format!(
                "database '{db}' is owned by another instance ({SHADOW_CONFLICT_MARKER}); \
                 writes must reach the owning replica until it releases"
            ),
        ));
    }
    Ok((lease, lease_lost))
}

/// ARC-001 belt-and-braces check, run on the lease connection at the start of
/// each write turn (multi-instance owners only): confirm this backend still
/// holds the session advisory lock. Covers a backend that is alive but lost
/// its lock outside the reconnect path (e.g. `pg_advisory_unlock_all` by an
/// operator). `pg_locks` splits a bigint key into `classid` (high 32 bits) and
/// `objid` (low 32); reproduce `db_ownership_key`'s value to compare. One
/// round trip per write turn; treat `false` exactly like `lease_lost`.
pub(in crate::committer) async fn verify_lease(conn: &mut sqlx::PgConnection, db: &str) -> bool {
    let key = db_ownership_key(db);
    let held: bool = sqlx::query_scalar(
        "SELECT EXISTS(
            SELECT 1 FROM pg_locks
            WHERE locktype = 'advisory'
              AND pid = pg_backend_pid()
              AND granted
              AND ((classid::bigint << 32) | objid::bigint) = $1
        )",
    )
    .bind(key)
    .fetch_one(conn)
    .await
    .unwrap_or_else(|err| {
        tracing::error!(db, error = %err, "lease verification query failed");
        false
    });
    held
}

/// Replies CONFLICT to a write arm that reached a SHADOW (non-owner)
/// committer (ENH-022 Stage 4). Fire-and-forget arms have no reply — the
/// shadow runs no scheduler/reaper pollers, so those only arrive from a
/// submit racing the takeover; log loudly instead.
pub(in crate::committer) async fn reply_ownership_conflict(
    ctx: &CommitterCtx,
    req: CommitterRequest,
) {
    let err = RtDbError::new(
        crate::error::ErrorCode::Conflict,
        format!(
            "database '{}' is owned by another instance ({SHADOW_CONFLICT_MARKER}); \
             writes must reach the owning replica until it releases",
            ctx.db
        ),
    );
    match req {
        CommitterRequest::Mutate { reply, .. } => {
            let _ = reply.send(Err(err));
        }
        CommitterRequest::RunMigrate { reply, .. } => {
            let _ = reply.send(Err(err));
        }
        CommitterRequest::RunPushSchema { reply, .. } => {
            let _ = reply.send(Err(err));
        }
        CommitterRequest::RunMergeUsers { reply, .. } => {
            let _ = reply.send(Err(err));
        }
        CommitterRequest::RunPurge { reply, .. } => {
            let _ = reply.send(Err(err));
        }
        CommitterRequest::RunBulkDeleteStart { reply, .. } => {
            let _ = reply.send(Err(err));
        }
        CommitterRequest::RunBulkDeleteChunk { reply, .. } => {
            let _ = reply.send(Err(err));
        }
        CommitterRequest::RunRestoreSchema { reply, .. } => {
            let _ = reply.send(Err(err));
        }
        CommitterRequest::Quiesce { reply } => {
            let _ = reply.send(Err(err));
        }
        CommitterRequest::RunScheduled { .. }
        | CommitterRequest::RunReaper { .. }
        | CommitterRequest::RunWorkflowAdvance { .. } => {
            tracing::warn!(
                db = %ctx.db,
                "ownership conflict on a fire-and-forget write arm"
            );
        }
        CommitterRequest::Subscribe { .. } | CommitterRequest::Shutdown => {}
    }
}

/// True for every request whose handling writes documents — the arms a SHADOW
/// (non-owner) committer must reject, and the submits that attempt the
/// ownership upgrade in `submit`.
pub(in crate::committer) fn request_needs_write(req: &CommitterRequest) -> bool {
    matches!(
        req,
        CommitterRequest::Mutate { .. }
            | CommitterRequest::RunScheduled { .. }
            | CommitterRequest::RunMigrate { .. }
            | CommitterRequest::RunReaper { .. }
            | CommitterRequest::RunPurge { .. }
            | CommitterRequest::RunBulkDeleteStart { .. }
            | CommitterRequest::RunBulkDeleteChunk { .. }
            | CommitterRequest::RunWorkflowAdvance { .. }
            | CommitterRequest::RunMergeUsers { .. }
            | CommitterRequest::RunPushSchema { .. }
            | CommitterRequest::RunRestoreSchema { .. }
    )
}

/// What [`verify_write_lease_and_demote`] decided about the current turn.
pub(in crate::committer) enum LeaseVerification {
    /// The lease holds (or the turn needs no lease) — `req` comes back for
    /// normal dispatch by `run_committer`.
    Held(Box<CommitterRequest>),
    /// The lease is lost: this request and everything queued behind it were
    /// answered CONFLICT and the committer task must shut itself down so the
    /// supervisor clears the entry and the next submit re-acquires (or
    /// demotes to a shadow).
    Lost,
}

/// ARC-001 belt-and-braces per-turn lease verification for a multi-instance
/// OWNER before any write turn, extracted verbatim from `run_committer`'s
/// dispatch loop. The `after_connect` hook catches a backend REPLACEMENT;
/// this covers the remaining gap — a still-alive backend whose lock was taken
/// away (e.g. `pg_advisory_unlock_all`) — and first-turn detection when the
/// flag was set by a concurrent arm. Fail closed: a verification error counts
/// as lost. On a lost lease the write replies CONFLICT (shadow behavior),
/// everything already queued behind it is drained and answered CONFLICT too,
/// and [`LeaseVerification::Lost`] tells the caller to shut the task down so
/// the supervisor clears the entry; the next submit respawns via
/// `channel_for`, which re-acquires the lease (or becomes a shadow).
/// `Committers::submit` routing is untouched.
pub(in crate::committer) async fn verify_write_lease_and_demote(
    ctx: &CommitterCtx,
    req: CommitterRequest,
    rx: &mut mpsc::Receiver<CommitterRequest>,
) -> LeaseVerification {
    // Only a multi-instance owner holding the lease needs the check; every
    // other combination (single-instance, shadow, read arm) passes straight
    // through.
    let needs_lease_check = ctx.multi_instance && ctx.owns_writes && request_needs_write(&req);
    if !needs_lease_check {
        return LeaseVerification::Held(Box::new(req));
    }
    let lost_by_flag = ctx
        .lease_lost
        .as_ref()
        .is_some_and(|lost| lost.load(std::sync::atomic::Ordering::SeqCst));
    let lease_ok = if lost_by_flag {
        false
    } else {
        match ctx.pool.acquire().await {
            Ok(mut conn) => verify_lease(&mut conn, &ctx.db).await,
            Err(err) => {
                tracing::error!(
                    db = %ctx.db,
                    error = %err,
                    "ownership lease connection lost before write"
                );
                false
            }
        }
    };
    if lease_ok {
        return LeaseVerification::Held(Box::new(req));
    }
    // ENH-049: this is ARC-001's demotion point — the held lease is gone, so
    // count it and log with the replica identity.
    ctx.metrics.record_lease_lost();
    tracing::warn!(
        db = %ctx.db,
        instance_id = %ctx.instance_id,
        "ownership lease lost mid-life; demoting committer to shadow"
    );
    if let Some(lost) = &ctx.lease_lost {
        lost.store(true, std::sync::atomic::Ordering::SeqCst);
    }
    reply_ownership_conflict(ctx, req).await;
    // ARC-001: drain anything already queued behind this request and answer
    // CONFLICT, otherwise those oneshot reply channels would be dropped
    // silently when this task exits (surfacing as a generic "task dropped the
    // reply" error instead of the honest lease-conflict retry signal).
    while let Ok(queued) = rx.try_recv() {
        reply_ownership_conflict(ctx, queued).await;
    }
    // Self-shutdown: the supervisor clears the channel entry on task exit, so
    // the next submit re-enters `channel_for` and either re-acquires the
    // lease or becomes a shadow.
    LeaseVerification::Lost
}

#[cfg(test)]
mod tests {
    use super::db_ownership_key;

    #[test]
    fn quiesce_and_ownership_keys_use_distinct_namespaces() {
        let db = "tadvisory_namespace_probe";
        assert_ne!(db_ownership_key(db), crate::db::quiesce_key(db));
    }
}
