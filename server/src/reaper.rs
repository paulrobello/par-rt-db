//! Per-database document TTL reaper. A periodic timer (mirroring
//! `mutation_log::run_cleanup`) that enqueues a fire-and-forget `RunReaper` on
//! the committer channel; `committer::handle_reaper` performs the batch delete
//! inside the serialized committer turn so the four tap sites fire. See
//! `docs/superpowers/specs/2026-08-01-document-ttl-design.md`.

use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::mpsc::Sender;

use crate::committer::CommitterRequest;
use crate::db::database_exists;

/// The per-db TTL reaper loop. Every `sweep_interval`, enqueues one
/// `RunReaper` on the committer channel. Exits when the committer channel
/// closes (its task died) or when the database is dropped (the
/// `database_exists` check mirrors `scheduler::run_scheduler` and
/// `mutation_log::run_cleanup`, so `delete-db` retires this task cleanly).
pub async fn run_reaper(
    pool: PgPool,
    db: String,
    committer_tx: Sender<CommitterRequest>,
    sweep_interval: Duration,
) {
    let mut tick = tokio::time::interval(sweep_interval);
    tick.tick().await; // skip the immediate first tick
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if matches!(database_exists(&pool, &db).await, Ok(false)) {
                    tracing::info!(db = %db, "ttl reaper: database removed, exiting");
                    return;
                }
                if committer_tx.send(CommitterRequest::RunReaper).await.is_err() {
                    tracing::debug!(db = %db, "ttl reaper: committer channel closed, exiting");
                    return;
                }
            }
            _ = committer_tx.closed() => {
                tracing::debug!(db = %db, "ttl reaper: committer channel closed, exiting");
                return;
            }
        }
    }
}
