//! Per-database document TTL reaper. A periodic timer (mirroring
//! `mutation_log::run_cleanup`) that enqueues a fire-and-forget `RunReaper` on
//! the committer channel; `committer::handle_reaper` performs the batch delete
//! inside the serialized committer turn so the four tap sites fire. See
//! `docs/superpowers/specs/2026-08-01-document-ttl-design.md`.

use std::time::Duration;

use sqlx::PgPool;
use tokio::sync::mpsc::Sender;

use crate::committer::CommitterRequest;
use crate::db::{SchemaCache, database_exists};

/// The per-db TTL reaper loop. Every `sweep_interval`, enqueues one
/// `RunReaper` on the committer channel — but only when at least one table in
/// the db's schema declares a `ttl` (ARC-102: a db with no TTL tables would
/// otherwise wake the committer every sweep just for `handle_reaper` to loop
/// over zero matching tables and return). Exits when the committer channel
/// closes (its task died) or when the database is dropped (the
/// `database_exists` check mirrors `scheduler::run_scheduler` and
/// `mutation_log::run_cleanup`, so `delete-db` retires this task cleanly).
pub async fn run_reaper(
    pool: PgPool,
    db: String,
    committer_tx: Sender<CommitterRequest>,
    sweep_interval: Duration,
    schemas: SchemaCache,
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
                // ARC-102: skip the committer turn entirely when no table
                // declares a ttl. `handle_reaper` would read the same schema
                // and loop past every table (all `ttl = None`) doing zero
                // DELETEs — this gate avoids the channel send + serialized
                // turn. The schema cache read is a cheap in-memory RwLock; on
                // a miss it loads from Postgres, but the result is cached for
                // `handle_migrate`'s subsequent `put`.
                if !has_ttl_tables(&schemas, &pool, &db).await {
                    continue;
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

/// Returns true when at least one table in `db`'s schema declares a `ttl`.
/// A schema-load failure (no schema pushed yet, or transient DB error) returns
/// false — `handle_reaper` would hit the same failure and do nothing, so the
/// gate is sound: skipping a useless committer turn is equivalent.
async fn has_ttl_tables(schemas: &SchemaCache, pool: &PgPool, db: &str) -> bool {
    let schema = match schemas.get(pool, db).await {
        Ok(s) => s,
        Err(_) => return false,
    };
    schema.tables.values().any(|t| t.ttl.is_some())
}
