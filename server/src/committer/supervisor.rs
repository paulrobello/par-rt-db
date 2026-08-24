//! Background supervision of the per-db committer fleet: the idle-reclamation
//! sweep that retires committers for databases nobody is using, and the
//! per-db storage-quota cache warmer.

use super::*;

/// One idle-reclamation sweep pass (ARC-102 step 4), factored free so both the
/// [`Committers::reclaim_idle_once`] test seam and the spawned sweep loop share
/// one implementation. Returns the number of dbs retired this pass.
///
/// Soundness: a db meeting all three gates (stale activity, no live
/// subscriptions, no pending scheduled jobs) has no outstanding client demand
/// and no due background work, so retiring its tasks loses nothing — the next
/// request respawns them via `channel_for`. The eviction re-checks
/// `last_activity` under the lock so a request that arrived between snapshot and
/// evict refreshes the clock and survives. The `Shutdown` is enqueued (not
/// prepended), so it runs behind any in-flight queued work and the single-writer
/// invariant holds. `scheduler::next_due` gates pending cron/one-shot jobs so a
/// db with future due work is never reclaimed.
pub(in crate::committer) async fn reclaim_idle_pass(
    pool: &PgPool,
    subs: &Arc<SubscriptionManager>,
    channels: &Arc<Mutex<HashMap<String, ChannelEntry>>>,
    threshold: std::time::Duration,
) -> usize {
    if threshold.is_zero() {
        return 0;
    }
    let now = std::time::Instant::now();
    // Snapshot stale candidates under the lock; release before the async per-db
    // checks so a DB query (next_due) never blocks submits.
    let candidates: Vec<String> = {
        let guard = channels.lock().await;
        guard
            .iter()
            .filter(|(_, e)| {
                now.checked_duration_since(e.last_activity)
                    .is_some_and(|age| age >= threshold)
            })
            .map(|(db, _)| db.clone())
            .collect()
    };
    let mut reclaimed = 0;
    for db in candidates {
        // Cheap in-memory check first: a db with live subscriptions is not idle
        // regardless of write activity — the audit's load-bearing guard.
        if subs.count_for_db(&db).await > 0 {
            continue;
        }
        // A pending (future-due or past-due-unclaimed) scheduled job means the
        // db has due background work — reclaiming it would stall the cron /
        // one-shot until the next client request. An error here (transient DB
        // failure or a concurrently-dropped schema) is treated as "not idle this
        // pass" — safe, never over-eager.
        match crate::scheduler::next_due(pool, &db).await {
            Ok(None) => {}
            _ => continue,
        }
        // FM-29: same for pending workflow runs — reclaiming a db mid-run
        // would stall the chain until the next client request respawns the
        // tasks. Any pending row (future gate or past-due-unclaimed) counts.
        match crate::workflows::next_due(pool, &db).await {
            Ok(None) => {}
            _ => continue,
        }
        // Mark the entry draining + enqueue Shutdown, but do NOT remove it.
        // `channel_for` must not hand out a dying sender (the task is exiting),
        // so a request arriving during the drain waits for the supervisor to
        // clear the entry on the task's exit before spawning a fresh task — that
        // ordering is what keeps the single-writer invariant intact. The
        // supervisor removes the entry; reclaim never does (ARC-102 step 4).
        let retire = {
            let mut guard = channels.lock().await;
            match guard.get_mut(&db) {
                Some(entry)
                    if !entry.draining
                        && now
                            .checked_duration_since(entry.last_activity)
                            .is_some_and(|age| age >= threshold) =>
                {
                    entry.draining = true;
                    Some(entry.sender.clone())
                }
                _ => None,
            }
        };
        if let Some(sender) = retire {
            let _ = sender.try_send(CommitterRequest::Shutdown);
            tracing::info!(db = %db, "committer: idle database reclaimed (ARC-102)");
            reclaimed += 1;
        }
    }
    reclaimed
}

/// Per-db storage-quota cache warmer (ARC-004). Periodically re-measures the
/// db's on-disk size off the committer critical path so `quota::UsageCache::
/// enforce` is a cheap stale-read on the hot path instead of a
/// `pg_total_relation_size` scan that stalls the serialized write turn.
///
/// Mirrors the reaper/scheduler/cleanup lifecycle: self-terminates when the
/// committer channel closes (its task died) or the database is dropped (the
/// `database_exists` check). The first tick fires immediately to seed the
/// cache, then once per `warm_interval`. Each tick reads the live hot config
/// and skips the measure when no storage cap is configured
/// (`max_storage_bytes_per_db == 0`, the default) — so quota-less dbs pay zero
/// background scan cost, and a runtime cap enable via `PATCH /admin/config`
/// takes effect on the next tick. Writes nothing to document tables; it only
/// measures + updates the in-memory cache (a pure reader), so the single-writer
/// invariant is intact.
pub(in crate::committer) async fn run_quota_warmer(
    pool: PgPool,
    db: String,
    quotas: Arc<crate::quota::UsageCache>,
    hot: Arc<ArcSwap<HotConfig>>,
    warm_interval: std::time::Duration,
    committer_tx: mpsc::Sender<CommitterRequest>,
) {
    let mut tick = tokio::time::interval(warm_interval);
    loop {
        tokio::select! {
            _ = committer_tx.closed() => {
                tracing::debug!(db = %db, "quota warmer: committer channel closed, exiting");
                return;
            }
            _ = tick.tick() => {
                // No cap configured (default) → nothing to warm; keep looping
                // so a runtime cap enable is picked up on the next tick.
                if hot.load().max_storage_bytes_per_db == 0 {
                    continue;
                }
                if matches!(database_exists(&pool, &db).await, Ok(false)) {
                    tracing::info!(db = %db, "quota warmer: database removed, exiting");
                    return;
                }
                if let Err(e) = quotas.refresh(&pool, &db).await {
                    tracing::warn!(db = %db, error = %e, "quota warmer: storage refresh failed");
                }
            }
        }
    }
}
