//! Schema-DDL admission gate: bounds how many schema-DDL passes (push, migrate,
//! restore) may hold pool connections at once, and serializes them against
//! `pg_dump` backups.
//!
//! 2026-11-05 incident: tenant schema pushes re-issuing `ADD COLUMN IF NOT
//! EXISTS` queued ~15s each (one 125s) behind a 03:00Z physical backup's
//! ACCESS SHARE locks. Every queued ALTER held a sqlx pool connection, the
//! shared query pool exhausted (2,153 acquire timeouts), and ordinary API
//! queries 500'd fleet-wide for ~9 minutes while Postgres itself stayed
//! healthy. Three bounds close that failure mode:
//!
//! 1. **Concurrency cap** — at most [`MAX_CONCURRENT_SCHEMA_DDL`] DDL passes
//!    execute at once server-wide, so lock waits can hold only that many pool
//!    connections no matter how many databases are mid-migration. Late
//!    pushers wait at most [`DDL_PERMIT_WAIT`] for a slot and then get a
//!    retryable 409 instead of piling on.
//! 2. **Backup gate** — a backup (cron or manual) sets `backup_in_flight`
//!    BEFORE draining the permits, so new DDL defers with a clear retryable
//!    409 while in-flight DDL finishes first. Either direction of the
//!    collision is closed: DDL never queues behind a running dump's table
//!    locks, and the dump never starts on top of a half-applied migration.
//!    A no-op schema push (tables map already matches — see
//!    `ddl::push_schema_gated`) skips this gate entirely, so unchanged
//!    re-pushes keep succeeding during a backup.
//! 3. **`lock_timeout`** — gated DDL transactions pin
//!    [`DDL_LOCK_TIMEOUT`] via `SET LOCAL`, so even the passes that do run
//!    cannot hold a permit + connection indefinitely behind a stuck lock.
//!
//! The gate is per-process (`AppState::runtime`). Under `RTDB_MULTI_INSTANCE`
//! only the lease owner executes DDL, so the owning replica's gate is the one
//! that counts; a backup enabled on a different replica than the owner is not
//! serialized by this gate (documented in `deploy/README.md`) — the concurrency
//! cap still bounds the blast radius there.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::error::RtDbError;

/// Server-wide cap on concurrently-executing schema-DDL passes. 2 keeps a
/// multi-database deploy moving (two passes in flight) while guaranteeing that
/// lock waits can pin at most 2 of the pool's connections during a collision.
pub const MAX_CONCURRENT_SCHEMA_DDL: usize = 2;

/// How long a schema change waits for a DDL slot before failing with a
/// retryable 409. Generous against a slow-but-legit pass; short enough that a
/// stuck pair surfaces an error rather than queueing committer turns forever.
const DDL_PERMIT_WAIT: Duration = Duration::from_secs(30);

/// How long a starting backup waits for in-flight DDL passes to drain before
/// proceeding anyway. If DDL is still stuck after this, skipping the backup is
/// worse than running it: the concurrency cap (not the gate) is what bounds
/// pool damage, so the dump proceeds with an error log.
const BACKUP_DRAIN_WAIT: Duration = Duration::from_secs(15 * 60);

/// `lock_timeout` pinned inside gated DDL transactions (`SET LOCAL`, so it is
/// transaction-scoped and pool-safe). Bounds how long one pass may wait on a
/// table lock — the incident's 125s ALTER wait — after which the client gets
/// an error and retries instead of the server holding the connection.
pub const DDL_LOCK_TIMEOUT: Duration = Duration::from_secs(30);

/// `SET LOCAL lock_timeout` statement for a gated DDL transaction. The value
/// is the compile-time constant above — never client input. The explicit `s`
/// suffix matters: a bare number is milliseconds in Postgres time GUCs.
pub fn set_local_lock_timeout_sql() -> String {
    format!("SET LOCAL lock_timeout = '{}s'", DDL_LOCK_TIMEOUT.as_secs())
}

pub struct SchemaGate {
    permits: Arc<Semaphore>,
    backup_in_flight: AtomicBool,
}

impl Default for SchemaGate {
    fn default() -> Self {
        Self::new()
    }
}

impl SchemaGate {
    pub fn new() -> Self {
        Self {
            permits: Arc::new(Semaphore::new(MAX_CONCURRENT_SCHEMA_DDL)),
            backup_in_flight: AtomicBool::new(false),
        }
    }

    /// Schema-DDL passes currently holding a slot (0..=MAX). Test and
    /// observability seam.
    pub fn ddl_in_flight(&self) -> usize {
        MAX_CONCURRENT_SCHEMA_DDL.saturating_sub(self.permits.available_permits())
    }

    /// Whether a backup currently holds the gate (new DDL defers).
    pub fn backup_active(&self) -> bool {
        self.backup_in_flight.load(Ordering::Acquire)
    }

    /// Admits one schema-DDL pass. Fails fast with a retryable 409 while a
    /// backup holds the gate; otherwise waits up to [`DDL_PERMIT_WAIT`] for a
    /// slot. The flag check before the acquire is race-safe: a backup that
    /// sets the flag after this check is still draining permits, so this
    /// pass's acquire blocks until the backup completes — the pass runs after
    /// the dump, never during it.
    pub async fn begin_ddl(&self) -> Result<tokio::sync::SemaphorePermit<'_>, RtDbError> {
        if self.backup_in_flight.load(Ordering::Acquire) {
            tracing::warn!(
                "schema change deferred: a backup is in flight; retry after it completes"
            );
            return Err(RtDbError::conflict(
                "schema change deferred: a backup is in flight; retry after it completes",
            ));
        }
        let permit = tokio::time::timeout(DDL_PERMIT_WAIT, self.permits.acquire())
            .await
            .map_err(|_| {
                tracing::warn!(
                    in_flight = self.ddl_in_flight(),
                    "schema change admitted too slow: DDL slots busy; retry"
                );
                RtDbError::conflict(
                    "schema operations are busy (concurrent schema changes in flight); retry",
                )
            })?
            .map_err(|_| RtDbError::internal("schema DDL semaphore closed"))?;
        Ok(permit)
    }

    /// Marks the backup in flight, then drains every DDL slot so the dump does
    /// not start on top of a half-applied migration. New DDL defers (409) from
    /// the flag onward; the returned hold keeps the slots until dropped. If
    /// draining exceeds [`BACKUP_DRAIN_WAIT`] the backup proceeds anyway —
    /// see the constant's doc.
    pub async fn begin_backup(self: &Arc<Self>) -> BackupHold {
        self.backup_in_flight.store(true, Ordering::Release);
        tracing::info!("backup window open: schema DDL deferred until it completes");
        let drained = tokio::time::timeout(
            BACKUP_DRAIN_WAIT,
            Arc::clone(&self.permits).acquire_many_owned(MAX_CONCURRENT_SCHEMA_DDL as u32),
        )
        .await;
        match drained {
            Ok(Ok(permits)) => BackupHold {
                gate: Arc::clone(self),
                _permits: Some(permits),
            },
            // The semaphore is never closed; map defensively anyway.
            Ok(Err(_)) => BackupHold {
                gate: Arc::clone(self),
                _permits: None,
            },
            Err(_) => {
                tracing::error!(
                    "backup drain timed out after {}s with DDL still in flight; \
                     proceeding — DDL concurrency stays capped, so pool damage is bounded",
                    BACKUP_DRAIN_WAIT.as_secs()
                );
                BackupHold {
                    gate: Arc::clone(self),
                    _permits: None,
                }
            }
        }
    }
}

/// RAII hold on the backup gate: clearing the flag and releasing the drained
/// DDL slots on drop, so a panic in the backup path cannot wedge DDL off.
/// The flag clears before the permits release: once DDL is admitted again it
/// may proceed even if the dump's tail is still finishing — harmless next to
/// the alternative (spurious 409s for a backup that has stopped reading).
pub struct BackupHold {
    gate: Arc<SchemaGate>,
    _permits: Option<OwnedSemaphorePermit>,
}

impl Drop for BackupHold {
    fn drop(&mut self) {
        self.gate.backup_in_flight.store(false, Ordering::Release);
        tracing::info!("backup window closed: schema DDL re-admitted");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn begin_ddl_admits_up_to_cap_then_waits() {
        let gate = SchemaGate::new();
        let p1 = gate.begin_ddl().await.expect("first permit");
        let p2 = gate.begin_ddl().await.expect("second permit");
        assert_eq!(gate.ddl_in_flight(), MAX_CONCURRENT_SCHEMA_DDL);
        // Third admission does not resolve while both permits are held.
        let mut third = std::pin::pin!(gate.begin_ddl());
        for _ in 0..20 {
            assert!(
                tokio::time::timeout(Duration::from_millis(5), third.as_mut())
                    .await
                    .is_err()
            );
        }
        drop(p2);
        let p3 = third
            .as_mut()
            .await
            .expect("third admitted after a release");
        assert_eq!(gate.ddl_in_flight(), MAX_CONCURRENT_SCHEMA_DDL);
        drop(p3);
        drop(p1);
        assert_eq!(gate.ddl_in_flight(), 0);
    }

    #[tokio::test]
    async fn backup_defers_new_ddl_and_drains_in_flight() {
        let gate = Arc::new(SchemaGate::new());
        let permit = gate.begin_ddl().await.expect("in-flight DDL permit");

        let gate_arc = Arc::clone(&gate);
        let mut hold = std::pin::pin!(gate_arc.begin_backup());
        // First poll sets the flag (new DDL defers from here on) and starts
        // draining; the hold cannot finish while the in-flight permit lives.
        for _ in 0..20 {
            assert!(
                tokio::time::timeout(Duration::from_millis(5), hold.as_mut())
                    .await
                    .is_err(),
                "backup hold must not finish while DDL is in flight"
            );
        }
        assert!(gate.backup_active());
        let err = gate.begin_ddl().await.expect_err("deferred during backup");
        assert!(err.to_string().contains("backup is in flight"));
        drop(permit);
        let hold = hold.await;
        assert_eq!(gate.ddl_in_flight(), MAX_CONCURRENT_SCHEMA_DDL); // backup holds all slots
        drop(hold);
        assert!(!gate.backup_active());
        let _re_admitted = gate.begin_ddl().await.expect("re-admitted after backup");
    }
}
