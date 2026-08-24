//! Per-database write-ownership lease (ENH-022 Stage 4): the advisory-lock
//! key, the lease acquisition, and the CONFLICT reply a SHADOW committer gives
//! a write it must not execute.

use super::*;

/// Stable advisory-lock key for `db`'s ownership lease (ENH-022 Stage 4,
/// option A1 of docs/superpowers/specs/2026-08-22-multi-instance-stage4-design.md):
/// the first 8 bytes of the db name's SHA-256, read as an i64.
pub(in crate::committer) fn db_ownership_key(db: &str) -> i64 {
    let hex = crate::db::sha256_hex(db);
    u64::from_str_radix(&hex[..16], 16).unwrap_or(0) as i64
}

/// ENH-022 Stage 4: acquire `db`'s single-writer ownership lease — a dedicated
/// one-connection pool whose backend takes `pg_try_advisory_lock(key(db))`.
/// The caller runs the db's committer and its pollers ON this pool, so the
/// lease and every write share one backend: no other replica can acquire
/// mid-flight (split-brain is impossible by construction, not by fencing),
/// and an owner's death (kill -9, container stop) drops the backend's session
/// and releases the lock — the next replica's acquire is the failover.
/// `min_connections(1)` + unbounded idle/lifetime keep the locked connection
/// alive for as long as the pool (its clones in the committer/poller tasks)
/// lives; eviction/drop-db drops them, closing the session and releasing the
/// lease. Returns `CONFLICT` when another replica holds the lease.
pub(in crate::committer) async fn acquire_ownership_lease(
    pool: &PgPool,
    db: &str,
) -> Result<PgPool, RtDbError> {
    let lease = sqlx::pool::PoolOptions::<sqlx::Postgres>::new()
        .max_connections(1)
        .min_connections(1)
        .idle_timeout(None)
        .max_lifetime(None)
        .connect_lazy_with((*pool.connect_options()).clone());
    let key = db_ownership_key(db);
    let mut conn = lease.acquire().await.map_err(|err| {
        tracing::error!(db, error = %err, "ownership lease connection failed");
        RtDbError::internal("failed to establish ownership lease connection")
    })?;
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
                "database '{db}' is owned by another instance (single-writer lease); \
                 writes must reach the owning replica until it releases"
            ),
        ));
    }
    Ok(lease)
}

/// True for every request whose handling writes documents — the arms a SHADOW
/// (non-owner) committer must reject, and the submits that attempt the
/// ownership upgrade in `submit`.
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
            "database '{}' is owned by another instance (single-writer lease); \
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
        CommitterRequest::RunRestoreSchema { reply, .. } => {
            let _ = reply.send(Err(err));
        }
        CommitterRequest::RunScheduled { .. }
        | CommitterRequest::RunReaper
        | CommitterRequest::RunWorkflowAdvance { .. } => {
            tracing::warn!(
                db = %ctx.db,
                "ownership conflict on a fire-and-forget write arm"
            );
        }
        CommitterRequest::Subscribe { .. } | CommitterRequest::Shutdown => {}
    }
}

pub(in crate::committer) fn request_needs_write(req: &CommitterRequest) -> bool {
    matches!(
        req,
        CommitterRequest::Mutate { .. }
            | CommitterRequest::RunScheduled { .. }
            | CommitterRequest::RunMigrate { .. }
            | CommitterRequest::RunReaper
            | CommitterRequest::RunWorkflowAdvance { .. }
            | CommitterRequest::RunMergeUsers { .. }
            | CommitterRequest::RunPushSchema { .. }
            | CommitterRequest::RunRestoreSchema { .. }
    )
}
