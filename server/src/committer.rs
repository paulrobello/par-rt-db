//! Committer — the correctness core. Each database owns one committer task that
//! serializes ALL writes, then — before dequeuing the next message — re-runs
//! affected subscriptions, diffs against the last pushed value, and pushes only
//! on change. This serialization is load-bearing: `execute_txn`/`execute_query`
//! run READ COMMITTED with no row locking, so every durable write must pass
//! through here. Handles four request arms — `RunMutate`, `RunScheduled`,
//! `RunMigrate`, `RunReaper` — plus `RunRestoreSchema`, `RunMergeUsers`, and
//! `RunWorkflowAdvance`, and publishes each at the four tap sites (subscription
//! fan-out, op-feed, audit log, webhooks). Never add a second writer.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use arc_swap::ArcSwap;
use sqlx::PgPool;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Mutex, mpsc, oneshot};
use tracing::Instrument;

use crate::auth::PrincipalCtx;
use crate::config::HotConfig;
use crate::db::{SchemaCache, database_exists, now_ms};
use crate::error::RtDbError;
use crate::metrics::Metrics;
use crate::mutation_log;
use crate::protocol::ServerMessage;
use crate::query::{Query, canonical, execute_query};
use crate::scheduler;
use crate::subs::{ConnId, SubscriptionManager};
use crate::txn::{OpKind, Transaction, TxnOutcome, WriteSet, execute_txn};

/// Bound on each per-db committer task's inbox.
const CHANNEL_BUFFER: usize = 64;

pub enum CommitterRequest {
    Mutate {
        idempotency_key: Option<String>,
        txn: Transaction,
        principal_ctx: PrincipalCtx,
        /// Monotonic instant the request was enqueued (ENH-018). Subtracted
        /// from `Instant::now()` at dequeue to produce `queue_wait_ms` on the
        /// `committer.mutate` span — the gap between enqueue and execution is
        /// invisible today and is the single most likely source of surprising
        /// latency under load (the ARC-102 idle-poller class of problem).
        enqueued_at: std::time::Instant,
        reply: oneshot::Sender<Result<TxnOutcome, RtDbError>>,
    },
    Subscribe {
        conn: ConnId,
        query_id: String,
        query: Box<Query>,
        tx: UnboundedSender<ServerMessage>,
        principal_ctx: PrincipalCtx,
        reply: oneshot::Sender<Result<(), RtDbError>>,
    },
    /// A scheduled/cron job whose `due_at` arrived. Fire-and-forget: the
    /// scheduler does not wait for a reply (it would only stall the timer).
    /// The committer executes the txn through the normal `execute_txn` path
    /// and finalizes the row.
    RunScheduled {
        id: String,
        kind: String,
        txn: Box<Transaction>,
        cron: Option<String>,
    },
    /// Apply a declarative schema migration on this database. Serialized through
    /// the per-db committer like `Mutate`, so the migration's DDL+DML and the
    /// four downstream taps (fan-out / op-feed / audit / webhook) all observe
    /// the same single-writer ordering a mutate does. `reply` carries the
    /// `MigrateResult` (post-migration schema + per-directive reports).
    RunMigrate {
        request: crate::migrate::MigrateRequest,
        reply: oneshot::Sender<Result<crate::migrate::MigrateResult, RtDbError>>,
    },
    /// A TTL reaper sweep is due. Fire-and-forget like `RunScheduled`: the
    /// reaper task does not wait for a reply. The committer runs the batch
    /// delete inside its serialized turn and publishes through the four tap
    /// sites with `source = "ttl"`.
    RunReaper,
    /// A claimed workflow run is due for advancement (FM-29). Fire-and-forget
    /// like `RunReaper`: the scheduler claims the row; the committer executes
    /// the current step's txn and writes the bookkeeping inside its serialized
    /// turn (single-writer invariant — the scheduler never executes a step).
    RunWorkflowAdvance {
        row: Box<crate::workflows::WorkflowRow>,
    },
    /// FM-27: re-stamp every principal-bearing field referencing `anon_id` to
    /// `real_id` across this db's tables, inside the serialized committer turn.
    /// `reply` carries the per-table restamp counts and skipped conflicts.
    RunMergeUsers {
        anon_id: String,
        real_id: String,
        reply: oneshot::Sender<Result<crate::merge::MergeDbResult, RtDbError>>,
    },
    /// Restore the database's schema shape to a captured `schema_history`
    /// snapshot. Serialized through the committer like `RunMigrate`: the
    /// destructive DDL reconcile runs inside the serialized turn, the outgoing
    /// schema is captured first (so the restore is itself undoable), and the
    /// incoming schema is captured after. `reply` carries the restored version.
    RunRestoreSchema {
        target_version: i64,
        reply: oneshot::Sender<Result<i64, RtDbError>>,
    },
    /// Best-effort immediate retirement of all five per-db tasks. Enqueued by
    /// `drop_db` behind any in-flight work so the committer exits once the
    /// queue drains (it would otherwise block on `recv()` until all four
    /// poller tasks independently detect db-deletion on their own poll cadence
    /// — up to 60s). On receipt `run_committer` breaks its loop, drops `rx`,
    /// and every poller's `committer_tx.closed()` resolves immediately. Does
    /// not carry a reply: the cascade is fire-and-forget and the task exits
    /// are not joined (ARC-125).
    Shutdown,
}

/// Owns one serialized committer task per database. Every mutation and every
/// subscribe for a given database is funneled through that single task, which
/// processes messages strictly one at a time — see `run_committer` for the
/// ordering guarantee this provides.
///
/// This is the property `execute_txn` (see `txn.rs`) depends on: it runs
/// under READ COMMITTED with no row locking; correctness depends on all
/// writes for a database being serialized through the per-db committer.
/// Never call `execute_txn` from a non-committer production path.
pub struct Committers {
    pool: PgPool,
    subs: Arc<SubscriptionManager>,
    schemas: SchemaCache,
    op_feed: Arc<crate::op_feed::OpFeed>,
    hot: Arc<ArcSwap<HotConfig>>,
    audit_log_enabled: bool,
    webhooks_enabled: bool,
    ttl_sweep_interval: std::time::Duration,
    ttl_batch: i64,
    metrics: Arc<Metrics>,
    quotas: Arc<crate::quota::UsageCache>,
    quota_cache_ttl_secs: u64,
    /// 0 = idle reclamation off (default, preserves today's behavior — a db's
    /// tasks live for the process once spawned). When non-zero, a background
    /// sweep retires per-db committers whose last client activity is older than
    /// this and which hold no live subscriptions and no pending scheduled jobs
    /// (ARC-102 step 4). See `reclaim_idle_once` / `spawn_idle_reclaimer`.
    idle_threshold: std::time::Duration,
    /// This process's replica id, used to tag cross-instance NOTIFY payloads so
    /// a receiving instance can skip its own notifications (ENH-022 Stage 2).
    /// Only meaningful when `multi_instance` is true.
    instance_id: String,
    /// When true, `publish_taps` also emits one `pg_notify` per DocOp so peer
    /// replicas sharing this Postgres see the write in their op-feed rings
    /// (ENH-022 Stage 2). Default false — a single-instance deploy never calls
    /// `pg_notify`, so the feature is zero-cost when off.
    multi_instance: bool,
    channels: Arc<Mutex<HashMap<String, ChannelEntry>>>,
}

/// One entry in the per-db channel map: the committer's sender plus the last
/// time a client request touched this database (ARC-102 step 4). `last_activity`
/// is refreshed on every `channel_for` hit, so it reflects client demand — a db
/// whose only activity is its pollers ticking (and finding nothing to do) reads
/// as idle and is reclaimable. Monotonic `Instant`, not wall-clock, so the idle
/// comparison is immune to clock jumps.
struct ChannelEntry {
    sender: mpsc::Sender<CommitterRequest>,
    last_activity: std::time::Instant,
    /// Set by the idle-reclaim sweep when it has enqueued `Shutdown` for this
    /// db's committer. A `channel_for` caller that finds a draining entry must
    /// NOT use its sender (the task is exiting) — it waits for the supervisor
    /// to clear the entry on the task's exit, then spawns a fresh task. This is
    /// what keeps the single-writer invariant intact across idle reclamation:
    /// the new committer can start only after the old one is dead (ARC-102
    /// step 4). `drop_db` does not need this — it deletes the db, so a
    /// concurrent `channel_for` misses `database_exists` and never respawns.
    draining: bool,
}

impl Committers {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        pool: PgPool,
        subs: Arc<SubscriptionManager>,
        schemas: SchemaCache,
        op_feed: Arc<crate::op_feed::OpFeed>,
        hot: Arc<ArcSwap<HotConfig>>,
        audit_log_enabled: bool,
        webhooks_enabled: bool,
        ttl_sweep_interval_secs: u64,
        ttl_batch: i64,
        metrics: Arc<Metrics>,
        quotas: Arc<crate::quota::UsageCache>,
        quota_cache_ttl_secs: u64,
        idle_reclaim_secs: u64,
        instance_id: String,
        multi_instance: bool,
    ) -> Self {
        Self {
            pool,
            subs,
            schemas,
            op_feed,
            hot,
            audit_log_enabled,
            webhooks_enabled,
            ttl_sweep_interval: std::time::Duration::from_secs(ttl_sweep_interval_secs),
            ttl_batch,
            metrics,
            quotas,
            quota_cache_ttl_secs,
            idle_threshold: std::time::Duration::from_secs(idle_reclaim_secs),
            instance_id,
            multi_instance,
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Removes `db`'s committer channel from the map so future mutate/subscribe
    /// requests 404 (the next `submit` would fail `database_exists` first
    /// anyway after `drop_database`), then enqueues a best-effort `Shutdown`
    /// so the five per-db tasks retire immediately rather than each waiting up
    /// to its own poll cadence (ARC-125, recurring ARC-012). Used by
    /// `delete-db`.
    ///
    /// The five per-db tasks spawned by `channel_for` are: the committer
    /// itself, the scheduler, the mutation-log cleanup task, the TTL reaper,
    /// and the storage-quota warmer (the reaper and warmer were added after
    /// the original three; the prior comment named only three). Each poller
    /// task also holds its own clone of the channel sender, so removing the
    /// map entry alone does not close the channel. The enqueued `Shutdown`
    /// makes `run_committer` break its loop and drop `rx`; that drop makes
    /// every poller's `committer_tx.closed()` selector resolve immediately, so
    /// all five tasks exit near-instantly instead of each independently
    /// detecting db-deletion on its own poll cadence (the scheduler polls
    /// every 2s, the reaper/warmer/cleanup every 60s).
    ///
    /// `try_send` is used so `drop_db` never blocks on a full channel buffer;
    /// on failure the db-deletion self-termination each poller already has
    /// still retires the tasks (just slower). Residual: the five task exits
    /// are not awaited (no `JoinHandle` registry), so a `drop_db` caller sees
    /// the channel evicted immediately but the task teardown completes
    /// asynchronously. The single-writer invariant is preserved throughout —
    /// `Shutdown` runs inside the committer's serialized turn behind any
    /// queued work, and no new requests can reach the retiring tasks once the
    /// map entry is gone.
    pub async fn drop_db(&self, db: &str) {
        let sender = {
            let mut guard = self.channels.lock().await;
            guard.remove(db)
        };
        if let Some(entry) = sender {
            let _ = entry.sender.try_send(CommitterRequest::Shutdown);
        }
    }

    /// Bundles this `Committers`'s shared state with a per-task `db` into a
    /// `CommitterCtx` ready to hand to `run_committer` by value. Constructs
    /// the ctx once at spawn time in `channel_for` so `run_committer` accepts
    /// a single `ctx` argument instead of re-receiving 12 individual params
    /// (ARC-002). All fields are `Clone` (`Arc`/`PgPool`/`bool`/`i64`/`u64`),
    /// so this is cheap reference-bumps + primitive copies.
    fn make_ctx(&self, db: String) -> CommitterCtx {
        CommitterCtx {
            pool: self.pool.clone(),
            db,
            subs: self.subs.clone(),
            schemas: self.schemas.clone(),
            op_feed: self.op_feed.clone(),
            hot: self.hot.clone(),
            audit_log_enabled: self.audit_log_enabled,
            webhooks_enabled: self.webhooks_enabled,
            ttl_batch: self.ttl_batch,
            metrics: self.metrics.clone(),
            quotas: self.quotas.clone(),
            instance_id: self.instance_id.clone(),
            multi_instance: self.multi_instance,
        }
    }

    /// Submits a request to `db`'s committer task, lazily spawning it on
    /// first use. Errors `NotFound` if `db` isn't a registered database. If
    /// the send fails because the committer task is gone (e.g. it panicked),
    /// evicts `db`'s stale sender from `channels` before returning the
    /// error, so the next request respawns a fresh task instead of every
    /// future request to `db` failing forever. The eviction only removes the
    /// entry if it still holds the same sender that just failed (`same_channel`)
    /// — otherwise a concurrent caller already respawned under this db key and
    /// evicting would drop the live replacement instead of the dead one.
    pub async fn submit(&self, db: &str, req: CommitterRequest) -> Result<(), RtDbError> {
        let sender = self.channel_for(db).await?;
        if sender.send(req).await.is_err() {
            let mut guard = self.channels.lock().await;
            if guard
                .get(db)
                .is_some_and(|current| current.sender.same_channel(&sender))
            {
                guard.remove(db);
            }
            return Err(RtDbError::internal("committer task is no longer running"));
        }
        Ok(())
    }

    /// Returns `db`'s committer sender, lazily spawning the task on first use.
    /// No `.await` occurs while `channels` is locked: the cache-hit fast path
    /// checks and releases the lock immediately; on a miss, the lock is dropped
    /// before the `database_exists` query, then re-acquired to insert
    /// (double-checking in case another task won the race and already spawned
    /// one).
    ///
    /// ARC-102 step 4: an entry flagged `draining` (the idle-reclaim sweep has
    /// enqueued `Shutdown`) is NEVER handed out — its task is exiting, so a
    /// request queued on its sender would either race the shutdown or hang. A
    /// caller that finds a draining entry waits for the supervisor to clear it
    /// on the task's exit, then loops to pick up a concurrent respawn or spawn
    /// one itself. This bounds the loop so a stuck drain surfaces as an error
    /// instead of a hang, and guarantees the new committer starts only after the
    /// old one is dead — preserving the single-writer invariant.
    async fn channel_for(&self, db: &str) -> Result<mpsc::Sender<CommitterRequest>, RtDbError> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            // Fast path: a live (non-draining) entry. Refresh its idle clock
            // (ARC-102 step 4) and return. A draining entry falls through to
            // the wait; an absent entry falls through to the spawn.
            let mut draining = false;
            {
                let mut guard = self.channels.lock().await;
                if let Some(entry) = guard.get_mut(db) {
                    if entry.draining {
                        draining = true;
                    } else {
                        entry.last_activity = std::time::Instant::now();
                        return Ok(entry.sender.clone());
                    }
                }
            }
            if draining {
                if std::time::Instant::now() >= deadline {
                    return Err(RtDbError::internal(
                        "committer for database is draining and did not exit in time",
                    ));
                }
                // The supervisor removes the draining entry once the task exits;
                // retry to either observe its removal (→ spawn) or pick up a
                // concurrent respawn.
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                continue;
            }

            // Miss: confirm the db exists, then spawn under the lock with a
            // double-check (another caller may have spawned while we queried).
            if !database_exists(&self.pool, db).await? {
                return Err(RtDbError::not_found(format!("database '{db}' not found")));
            }
            let mut guard = self.channels.lock().await;
            if let Some(entry) = guard.get_mut(db) {
                if entry.draining {
                    // Someone marked it draining while we checked existence.
                    drop(guard);
                    continue;
                }
                entry.last_activity = std::time::Instant::now();
                return Ok(entry.sender.clone());
            }

            let (tx, rx) = mpsc::channel(CHANNEL_BUFFER);
            let committer_handle = tokio::spawn(run_committer(self.make_ctx(db.to_string()), rx));
            // SEC-127: supervisor — evicts the cached Sender when the committer
            // task exits (panic or normal completion), so a dead entry is cleared
            // immediately rather than lingering until `submit`'s send-failure path
            // catches up on the next request. The `same_channel` guard avoids
            // clobbering a concurrent respawn under the same db key. For a
            // draining entry this removal is also the signal a waiting
            // `channel_for` caller loops on (ARC-102 step 4).
            {
                let db_owned = db.to_string();
                let channels = Arc::clone(&self.channels);
                let supervised = tx.clone();
                tokio::spawn(async move {
                    let _ = committer_handle.await;
                    let mut guard = channels.lock().await;
                    if guard
                        .get(&db_owned)
                        .is_some_and(|current| current.sender.same_channel(&supervised))
                    {
                        guard.remove(&db_owned);
                    }
                });
            }
            tokio::spawn(scheduler::run_scheduler(
                self.pool.clone(),
                db.to_string(),
                tx.clone(),
            ));
            // Per-db dedup-row expiry sweep (ARC-007): owns `mutation_log`'s
            // periodic DELETE so `mutation_log::check` is a pure SELECT on the
            // hot path. Exits when the committer channel closes (same lifecycle
            // signal the scheduler task uses).
            tokio::spawn(mutation_log::run_cleanup(
                self.pool.clone(),
                db.to_string(),
                tx.clone(),
            ));
            // Per-db TTL reaper: enqueues a fire-and-forget `RunReaper` every
            // `ttl_sweep_interval`; the committer's `handle_reaper` performs the
            // batch delete inside its serialized turn. Same lifecycle as the
            // scheduler/cleanup tasks (exits on channel close or db removal).
            tokio::spawn(crate::reaper::run_reaper(
                self.pool.clone(),
                db.to_string(),
                tx.clone(),
                self.ttl_sweep_interval,
                self.schemas.clone(),
            ));
            // Per-db storage-quota cache warmer (ARC-004): periodically re-measures
            // the db's on-disk size off the committer turn so `enforce` is a cheap
            // stale-read. Same lifecycle as the reaper/cleanup tasks (exits on
            // channel close or db removal); a no-op tick when no storage cap is set.
            tokio::spawn(run_quota_warmer(
                self.pool.clone(),
                db.to_string(),
                self.quotas.clone(),
                self.hot.clone(),
                std::time::Duration::from_secs(self.quota_cache_ttl_secs),
                tx.clone(),
            ));
            guard.insert(
                db.to_string(),
                ChannelEntry {
                    sender: tx.clone(),
                    last_activity: std::time::Instant::now(),
                    draining: false,
                },
            );
            return Ok(tx);
        }
    }

    /// Spawns `db`'s per-db tasks (committer + scheduler + reaper + warmers)
    /// if not already running, without submitting anything. Workflow start
    /// surfaces call this before `workflows::insert`: steps fire from the
    /// per-db scheduler, which only exists once the tasks spawn — a run
    /// started on a cold db (no Mutate/Subscribe since creation) would
    /// otherwise sit `pending` until unrelated data-plane traffic spawns them.
    pub(crate) async fn ensure_spawned(&self, db: &str) -> Result<(), RtDbError> {
        self.channel_for(db).await.map(|_| ())
    }

    /// Executes `txn` on `db` and waits for the fan-out-then-reply cycle to
    /// complete. `idempotency_key`, when present, is the caller-opted-in
    /// dedup key: a repeat call with the same `db` + key replays the first
    /// call's cached results instead of re-executing. This is distinct from
    /// any transport-level reply-correlation id (e.g. WS's `mutId`, which is
    /// always sent and never reaches this layer) — dedup only ever applies
    /// when a caller explicitly supplies this key.
    pub async fn mutate(
        &self,
        db: &str,
        idempotency_key: Option<String>,
        txn: Transaction,
        principal_ctx: PrincipalCtx,
    ) -> Result<TxnOutcome, RtDbError> {
        let (reply, reply_rx) = oneshot::channel();
        // ENH-018: stamp the enqueue instant so the committer's dequeue can
        // derive `queue_wait_ms` on the `committer.mutate` span.
        let enqueued_at = std::time::Instant::now();
        self.submit(
            db,
            CommitterRequest::Mutate {
                idempotency_key,
                txn,
                principal_ctx,
                enqueued_at,
                reply,
            },
        )
        .await?;
        reply_rx
            .await
            .map_err(|_| RtDbError::internal("committer task dropped the reply"))?
    }

    /// Applies `request`'s directives on `db` and waits for the commit-then-
    /// fan-out cycle to complete. Like `mutate`, this funnels through the per-db
    /// committer so the migration is serialized with concurrent writes, runs the
    /// four downstream taps (subscription fan-out, op-feed, audit, webhook) on
    /// the durable result, and — when `request.dry_run` is false — persists the
    /// derived schema. `dry_run` rolls the migration tx back and publishes
    /// nothing. See `handle_migrate` for the load-bearing tap-site contract.
    pub async fn migrate(
        &self,
        db: &str,
        request: crate::migrate::MigrateRequest,
    ) -> Result<crate::migrate::MigrateResult, RtDbError> {
        let (reply, reply_rx) = oneshot::channel();
        self.submit(db, CommitterRequest::RunMigrate { request, reply })
            .await?;
        reply_rx
            .await
            .map_err(|_| RtDbError::internal("committer task dropped the reply"))?
    }

    /// Restores `db`'s schema shape to the captured `schema_history` snapshot
    /// at `target_version` and waits for the commit-then-fan-out cycle to
    /// complete. Funneled through the per-db committer like `migrate` so the
    /// destructive reconcile (drop tables/columns/indexes in the live shape but
    /// not the target, then additive-create the inverse) runs inside the only
    /// writer's serialized turn. The outgoing (current) schema is captured to
    /// history first, so a restore is itself undoable; the incoming (target)
    /// schema is captured after, so the latest history row always equals the
    /// live schema. Returns the restored version. See `handle_restore_schema`.
    pub async fn restore_schema(&self, db: &str, target_version: i64) -> Result<i64, RtDbError> {
        let (reply, reply_rx) = oneshot::channel();
        self.submit(
            db,
            CommitterRequest::RunRestoreSchema {
                target_version,
                reply,
            },
        )
        .await?;
        reply_rx
            .await
            .map_err(|_| RtDbError::internal("committer task dropped the reply"))?
    }

    /// Runs the FM-27 anon→real merge for one database inside its serialized
    /// committer turn. Document rewrites happen there (single-writer
    /// invariant); storage/session/user-row steps live in `merge::merge_users`
    /// (later tasks). Returns per-table restamp counts and skipped conflicts.
    pub async fn merge_users(
        &self,
        db: &str,
        anon_id: &str,
        real_id: &str,
    ) -> Result<crate::merge::MergeDbResult, RtDbError> {
        let (reply, reply_rx) = oneshot::channel();
        self.submit(
            db,
            CommitterRequest::RunMergeUsers {
                anon_id: anon_id.to_string(),
                real_id: real_id.to_string(),
                reply,
            },
        )
        .await?;
        reply_rx
            .await
            .map_err(|_| RtDbError::internal("committer task dropped the reply"))?
    }

    /// Runs `query` on `db`, sends the initial result on `tx`, and registers
    /// the subscription for future push-on-change updates. `principal_ctx` is
    /// the subscriber's per-row auth identity (captured on the `SubEntry` and
    /// applied to every re-run in `fan_out`); `user_id = None` = bypass.
    pub async fn subscribe(
        &self,
        db: &str,
        conn: ConnId,
        query_id: String,
        query: Query,
        tx: UnboundedSender<ServerMessage>,
        principal_ctx: PrincipalCtx,
    ) -> Result<(), RtDbError> {
        let (reply, reply_rx) = oneshot::channel();
        self.submit(
            db,
            CommitterRequest::Subscribe {
                conn,
                query_id,
                query: Box::new(query),
                tx,
                principal_ctx,
                reply,
            },
        )
        .await?;
        reply_rx
            .await
            .map_err(|_| RtDbError::internal("committer task dropped the reply"))?
    }

    /// Returns true when `db`'s committer task is currently spawned (it has a
    /// live channel entry). A test/observability seam for idle reclamation
    /// (ARC-102 step 4): production paths use `submit`/`channel_for`, which
    /// respawn on demand, so a `false` here is not itself an error.
    pub async fn is_spawned(&self, db: &str) -> bool {
        self.channels.lock().await.contains_key(db)
    }

    /// One idle-reclamation sweep pass (ARC-102 step 4). A no-op when
    /// `idle_threshold` is zero. Delegates to [`reclaim_idle_pass`]; tests call
    /// this directly for determinism instead of waiting on the sweep loop's
    /// cadence.
    pub async fn reclaim_idle_once(&self) -> usize {
        reclaim_idle_pass(&self.pool, &self.subs, &self.channels, self.idle_threshold).await
    }

    /// Spawns the long-lived idle-reclamation sweep for the whole server (one
    /// task, not per-db). A no-op when `idle_threshold` is zero (the default),
    /// so an instance that does not opt in pays zero background cost. The sweep
    /// cadence is `min(idle_threshold, 60s)` so a db is retired within roughly
    /// one sweep of going idle without spamming the lock on huge thresholds.
    /// Called once from `AppState::new` after the `Committers` is constructed.
    pub fn spawn_idle_reclaimer(&self) {
        if self.idle_threshold.is_zero() {
            return;
        }
        let channels = Arc::clone(&self.channels);
        let subs = Arc::clone(&self.subs);
        let pool = self.pool.clone();
        let threshold = self.idle_threshold;
        let sweep_interval = threshold.min(std::time::Duration::from_secs(60));
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(sweep_interval);
            // Skip the immediate first tick — a db just spawned is fresh.
            tick.tick().await;
            loop {
                tick.tick().await;
                reclaim_idle_pass(&pool, &subs, &channels, threshold).await;
            }
        });
    }
}

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
async fn reclaim_idle_pass(
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

/// Shared, unchanging context for one per-db committer task: the pool, the
/// database name, the schema/subscription state, and the live hot config
/// (read for the dedup TTL at `mutation_log::store` time), bundled to keep
/// the per-request handlers' argument lists small.
struct CommitterCtx {
    pool: PgPool,
    db: String,
    subs: Arc<SubscriptionManager>,
    schemas: SchemaCache,
    op_feed: Arc<crate::op_feed::OpFeed>,
    hot: Arc<ArcSwap<HotConfig>>,
    audit_log_enabled: bool,
    webhooks_enabled: bool,
    ttl_batch: i64,
    metrics: Arc<Metrics>,
    /// Per-db storage-usage cache (ENH-011). Stale-read by `enforce` at the three
    /// committer arms (`handle_mutate`/`handle_scheduled`/`handle_migrate`) so the
    /// serialized turn never blocks on `pg_total_relation_size` (ARC-004); kept
    /// current by a per-db background warmer (`run_quota_warmer`) + the post-commit
    /// refresh. The upload route reads it via `current_usage` (TTL-bounded).
    quotas: Arc<crate::quota::UsageCache>,
    /// This process's replica id (ENH-022 Stage 2). Tagged onto every NOTIFY
    /// payload so a receiving instance can skip its own notifications. Only read
    /// when `multi_instance` is true.
    instance_id: String,
    /// When true, `publish_taps` also emits one `pg_notify` per DocOp (ENH-022
    /// Stage 2). False on a single-instance deploy — the publish tap is
    /// zero-cost when off.
    multi_instance: bool,
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
async fn run_quota_warmer(
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

/// The per-db committer task loop: processes exactly one `CommitterRequest`
/// to completion before pulling the next. This is the correctness property
/// the whole system relies on:
/// - `Mutate`: the transaction commits, then every affected subscription is
///   re-run and (if changed) pushed, and only then does the caller's `reply`
///   resolve — fan-out is always complete before the next request is taken
///   off the channel.
/// - `Subscribe`: the query runs, its initial result is sent on `tx`, and
///   only then is the subscription registered — both steps happen before any
///   other message (in particular, a concurrent `Mutate`) can be processed,
///   so no update between execute and register can be missed.
///
/// Takes `ctx` by value (ARC-002): the ctx is constructed once in
/// `channel_for` via `Committers::make_ctx`, eliminating the prior 12-arg
/// signature and the second copy of the field list that used to live here.
async fn run_committer(ctx: CommitterCtx, mut rx: mpsc::Receiver<CommitterRequest>) {
    if let Err(err) = mutation_log::ensure_table(&ctx.pool, &ctx.db).await {
        tracing::error!(db = %ctx.db, error = %err, "failed to ensure mutations dedup table");
    }
    if let Err(err) = crate::storage::ensure_table(&ctx.pool, &ctx.db).await {
        tracing::error!(db = %ctx.db, error = %err, "committer: storage::ensure_table failed");
    }
    while let Some(req) = rx.recv().await {
        // ENH-018: the `db` field on every span is bounded (one per database
        // name, not per document), so it is safe to put on a span attribute —
        // unlike doc ids or content, which would blow up cardinality.
        match req {
            CommitterRequest::Mutate {
                idempotency_key,
                txn,
                principal_ctx,
                enqueued_at,
                reply,
            } => {
                // ENH-018: `queue_wait_ms` is the delta between enqueue and
                // dequeue — the gap the committer's serialized queue can
                // introduce, and the single most useful per-request latency
                // signal (the ARC-102 idle-poller class of problem). The span
                // wraps the whole mutate via `.instrument` so the child
                // `txn.execute` and `subs.fan_out` spans nest under it.
                let queue_wait_ms = enqueued_at.elapsed().as_millis() as u64;
                let span = tracing::info_span!(
                    "committer.mutate",
                    db = %ctx.db,
                    queue_wait_ms,
                    steps = txn.steps.len(),
                );
                let outcome = handle_mutate(&ctx, idempotency_key, txn, principal_ctx)
                    .instrument(span)
                    .await;
                let _ = reply.send(outcome);
            }
            CommitterRequest::Subscribe {
                conn,
                query_id,
                query,
                tx,
                principal_ctx,
                reply,
            } => {
                let span = tracing::info_span!("committer.subscribe", db = %ctx.db);
                let result = handle_subscribe(&ctx, conn, query_id, *query, tx, principal_ctx)
                    .instrument(span)
                    .await;
                let _ = reply.send(result);
            }
            CommitterRequest::RunScheduled {
                id,
                kind,
                txn,
                cron,
            } => {
                let span = tracing::info_span!(
                    "committer.scheduled",
                    db = %ctx.db,
                    kind,
                    id,
                );
                let outcome = handle_scheduled(&ctx, id, kind, *txn, cron)
                    .instrument(span)
                    .await;
                if let Err(err) = outcome {
                    tracing::error!(db = %ctx.db, error = %err, "scheduled job handling failed");
                }
            }
            CommitterRequest::RunMigrate { request, reply } => {
                let directives = request.directives.len();
                let dry_run = request.dry_run;
                let span = tracing::info_span!(
                    "committer.migrate",
                    db = %ctx.db,
                    directives,
                    dry_run,
                );
                let result = handle_migrate(&ctx, request).instrument(span).await;
                let _ = reply.send(result);
            }
            CommitterRequest::RunReaper => {
                let span = tracing::info_span!("committer.reaper", db = %ctx.db);
                let outcome = handle_reaper(&ctx).instrument(span).await;
                if let Err(err) = outcome {
                    tracing::error!(db = %ctx.db, error = %err, "ttl reaper handling failed");
                }
            }
            CommitterRequest::RunWorkflowAdvance { row } => {
                let id = row.id.clone();
                let span = tracing::info_span!("committer.workflow", db = %ctx.db, id);
                let outcome = handle_workflow_advance(&ctx, *row).instrument(span).await;
                if let Err(err) = outcome {
                    tracing::error!(db = %ctx.db, error = %err, "workflow advance failed");
                }
            }
            CommitterRequest::RunMergeUsers {
                anon_id,
                real_id,
                reply,
            } => {
                let span = tracing::info_span!("committer.merge_users", db = %ctx.db);
                let outcome = handle_merge_users(&ctx, &anon_id, &real_id)
                    .instrument(span)
                    .await;
                if let Err(err) = &outcome {
                    tracing::error!(db = %ctx.db, error = %err, "merge users handling failed");
                }
                let _ = reply.send(outcome);
            }
            CommitterRequest::RunRestoreSchema {
                target_version,
                reply,
            } => {
                let span = tracing::info_span!(
                    "committer.restore_schema",
                    db = %ctx.db,
                    target_version,
                );
                let outcome = handle_restore_schema(&ctx, target_version)
                    .instrument(span)
                    .await;
                let _ = reply.send(outcome);
            }
            CommitterRequest::Shutdown => {
                // ARC-125: best-effort retirement requested by `drop_db`.
                // Breaking here drops `rx`, which makes every poller task's
                // `committer_tx.closed()` selector resolve immediately. Any
                // requests queued ahead of this `Shutdown` were already
                // processed in their serialized order, so the single-writer
                // invariant is intact.
                tracing::info!(db = %ctx.db, "committer: shutdown requested, exiting");
                break;
            }
        }
    }
}

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
async fn publish_taps(
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

async fn handle_mutate(
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
    let outcome = execute_txn(&ctx.pool, &ctx.db, &schema, &txn, &principal_ctx).await?;
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

    if let Some(key) = &idempotency_key {
        // The dedup TTL is read live from hot config so a `PATCH /admin/config`
        // to `idempotencyTtlMs` takes effect on the next mutate, no restart.
        // The mutation already committed and fanned out by this point — a
        // caching failure here must never turn a successful write into a
        // client-visible error. Best-effort: log and move on. (A retry with
        // this key will simply re-execute, same as if it had never cached.)
        let ttl_ms = ctx.hot.load().idempotency_ttl_ms;
        if let Err(err) =
            mutation_log::store(&ctx.pool, &ctx.db, key, &outcome.results, ttl_ms).await
        {
            tracing::error!(
                db = %ctx.db,
                error = %err,
                "failed to cache mutation result for idempotency key; a retry with this key will re-execute"
            );
        }
    }

    Ok(outcome)
}

/// Executes one claimed scheduled job through the normal write path and
/// finalizes its row. Best-effort finalize: the txn has already committed +
/// fanned out by the time we touch the row again, so a finalize failure is
/// logged, not propagated. `at-least-once` recovery (the scheduler's
/// `reset_running` on startup) handles the rare crash window between commit
/// and finalize.
async fn handle_scheduled(
    ctx: &CommitterCtx,
    id: String,
    kind: String,
    txn: Transaction,
    cron: Option<String>,
) -> Result<(), RtDbError> {
    let schema = match ctx.schemas.get(&ctx.pool, &ctx.db).await {
        Ok(schema) => schema,
        Err(err) => {
            let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, "schema load failed").await;
            return Err(err);
        }
    };
    // ENH-011 / ARC-004: enforce per-db storage cap (best-effort stale-read,
    // kept current by the background warmer) before the scheduled write. A
    // scheduled job has no interactive principal; on rejection, mirror the
    // execute_txn-failure path below — record the quota metric, mark the job
    // row errored (so it surfaces in the scheduler admin UI), and return
    // `Ok(())` (the scheduler records the error rather than propagating —
    // fire-and-forget, no caller to surface it to). Uniform — no admin bypass.
    let storage_cap = ctx.hot.load().max_storage_bytes_per_db;
    if storage_cap > 0
        && let Err(e) = ctx.quotas.enforce(&ctx.pool, &ctx.db, storage_cap).await
    {
        ctx.metrics
            .record_quota_rejection(&ctx.db, crate::metrics::QuotaKind::Storage);
        let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, &e.message).await;
        return Ok(());
    }
    match execute_txn(&ctx.pool, &ctx.db, &schema, &txn, &PrincipalCtx::bypass()).await {
        Ok(outcome) => {
            // Four-tap publication (fan_out → op-feed → audit → webhook → quota-
            // refresh). Scheduled jobs carry no interactive principal
            // (`owner = None`); `source = "scheduled"` distinguishes from
            // mutate/ttl/migrate in delivered payloads.
            publish_taps(
                ctx,
                &schema,
                &outcome.write_set,
                None,
                "scheduled",
                true,
                true,
            )
            .await;
            let finalize = match kind.as_str() {
                "oneshot" => scheduler::finalize_one_shot_done(&ctx.pool, &ctx.db, &id).await,
                "cron" => match cron.as_deref() {
                    Some(expr) => match scheduler::next_fire(expr, now_ms()) {
                        Ok(next) => {
                            scheduler::finalize_cron_next(&ctx.pool, &ctx.db, &id, next).await
                        }
                        Err(err) => {
                            scheduler::mark_error(&ctx.pool, &ctx.db, &id, &err.message).await
                        }
                    },
                    None => {
                        scheduler::mark_error(&ctx.pool, &ctx.db, &id, "cron job missing expr")
                            .await
                    }
                },
                other => {
                    scheduler::mark_error(&ctx.pool, &ctx.db, &id, &format!("unknown kind {other}"))
                        .await
                }
            };
            if let Err(err) = finalize {
                tracing::error!(db = %ctx.db, %id, error = %err, "scheduled job finalize failed");
            }
        }
        Err(err) => {
            // Execution failed (precondition/step error). No retry (see spec):
            // one-shot records the error and stops; cron logs and reschedules.
            let msg = err.message.clone();
            match kind.as_str() {
                "cron" => match cron.as_deref() {
                    Some(expr) => match scheduler::next_fire(expr, now_ms()) {
                        Ok(next) => {
                            let _ = scheduler::reschedule_cron_error(
                                &ctx.pool, &ctx.db, &id, next, &msg,
                            )
                            .await;
                        }
                        Err(_) => {
                            let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, &msg).await;
                        }
                    },
                    None => {
                        let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, &msg).await;
                    }
                },
                _ => {
                    let _ = scheduler::mark_error(&ctx.pool, &ctx.db, &id, &msg).await;
                }
            }
        }
    }
    Ok(())
}

/// Advance a claimed workflow run (FM-29). Executes the current step's txn
/// as the system (bypass) principal — same fire path as `handle_scheduled` —
/// publishes through the tap sites with `source = "workflow"`, and loops
/// while the next gate is already due. Claim discipline: the row stays
/// `running` for the whole loop (the scheduler only claims `pending` rows),
/// so a no-sleep chain completes in one turn, bounded by the spec's step
/// count (≤ `workflows::MAX_WORKFLOW_STEPS` at submit). At-least-once per
/// step: a crash after `execute_txn` commits but before the bookkeeping
/// write re-executes that step on resume (`workflows::reset_running`).
async fn handle_workflow_advance(
    ctx: &CommitterCtx,
    mut row: crate::workflows::WorkflowRow,
) -> Result<(), RtDbError> {
    let schema = match ctx.schemas.get(&ctx.pool, &ctx.db).await {
        Ok(schema) => schema,
        Err(err) => {
            let outcome = failed_outcome(&row, "schema load failed");
            let _ = crate::workflows::mark_failed(
                &ctx.pool,
                &ctx.db,
                &row.id,
                &outcome,
                "schema load failed",
            )
            .await;
            return Err(err);
        }
    };
    // ENH-011/ARC-004 storage cap — checked once at entry, like the other
    // arms. Unlike `handle_scheduled`'s terminal `mark_error`, a quota
    // rejection counts as a retryable step failure: raising the cap
    // mid-retry lets the run recover.
    let storage_cap = ctx.hot.load().max_storage_bytes_per_db;
    let mut quota_err: Option<RtDbError> = None;
    if storage_cap > 0
        && let Err(e) = ctx.quotas.enforce(&ctx.pool, &ctx.db, storage_cap).await
    {
        quota_err = Some(e);
    }
    loop {
        // Cancel/terminal check at each step boundary (spec §Semantics): a
        // row cancelled or deleted out from under a running advance stops it.
        match crate::workflows::status_of(&ctx.pool, &ctx.db, &row.id).await {
            Ok(Some(crate::protocol::WorkflowStatus::Running)) => {}
            Ok(Some(_)) | Ok(None) => return Ok(()),
            Err(err) => return Err(err),
        }
        let Some(step) = row.spec.steps.get(row.current_step as usize) else {
            // Defensive: `current_step` past the last index means a corrupt
            // row (submit-time validation and the state machine keep it in
            // range). Mark failed rather than panic the committer task.
            let outcome = failed_outcome(&row, "step index out of range");
            let msg = "workflow current_step out of range";
            crate::workflows::mark_failed(&ctx.pool, &ctx.db, &row.id, &outcome, msg).await?;
            return Ok(());
        };
        let retry = step.retry.unwrap_or_default();
        let exec = match quota_err.take() {
            Some(e) => Err(e),
            None => {
                execute_txn(
                    &ctx.pool,
                    &ctx.db,
                    &schema,
                    &step.txn,
                    &PrincipalCtx::bypass(),
                )
                .await
            }
        };
        match exec {
            Ok(outcome) => {
                // Four-tap publication (fan_out → op-feed → audit → webhook →
                // quota-refresh). Workflow steps fire as the system principal
                // (`owner = None`); `source = "workflow"` distinguishes them
                // from scheduled/ttl/migrate in delivered payloads.
                publish_taps(
                    ctx,
                    &schema,
                    &outcome.write_set,
                    None,
                    "workflow",
                    true,
                    true,
                )
                .await;
                ctx.metrics
                    .record_workflow_step(crate::metrics::WorkflowStepOutcome::Success);
                let now = now_ms();
                let finished = row.current_step as usize + 1 >= row.spec.steps.len();
                let record = crate::protocol::StepOutcome {
                    step_index: row.current_step,
                    status: crate::protocol::OutcomeStatus::Success,
                    attempts: row.attempts + 1,
                    at: now,
                    error: None,
                };
                if finished {
                    crate::workflows::finalize_success(&ctx.pool, &ctx.db, &row.id, &record)
                        .await?;
                    return Ok(());
                }
                // Write the boundary while staying `running` (the scheduler
                // only claims `pending` rows), then compute the next gate:
                // due now → keep looping; future → release to `pending`.
                crate::workflows::record_step_success(
                    &ctx.pool,
                    &ctx.db,
                    &row.id,
                    row.current_step + 1,
                    &record,
                )
                .await?;
                row.current_step += 1;
                row.attempts = 0;
                let next = &row.spec.steps[row.current_step as usize];
                // Clamp before the u64→i64 cast: a serde-accepted u64 above
                // i64::MAX would wrap negative ⇒ an instantly-due gate.
                let sleep_ms = next.sleep_before_ms.unwrap_or(0).min(i64::MAX as u64) as i64;
                let gate = now.saturating_add(sleep_ms);
                if gate > now_ms() {
                    crate::workflows::set_pending(&ctx.pool, &ctx.db, &row.id, gate).await?;
                    return Ok(());
                }
            }
            Err(err) => {
                let now = now_ms();
                row.attempts += 1;
                if row.attempts < retry.max_attempts {
                    // Clamp before the u64→i64 cast (same wrap hazard as the
                    // sleep gate above).
                    let backoff = crate::workflows::backoff_ms(&retry, row.attempts)
                        .min(i64::MAX as u64) as i64;
                    crate::workflows::schedule_retry(
                        &ctx.pool,
                        &ctx.db,
                        &row.id,
                        row.attempts,
                        now.saturating_add(backoff),
                    )
                    .await?;
                    ctx.metrics
                        .record_workflow_step(crate::metrics::WorkflowStepOutcome::Retry);
                    return Ok(());
                }
                let record = crate::protocol::StepOutcome {
                    step_index: row.current_step,
                    status: crate::protocol::OutcomeStatus::Failed,
                    attempts: row.attempts,
                    at: now,
                    error: Some(err.message.clone()),
                };
                crate::workflows::mark_failed(&ctx.pool, &ctx.db, &row.id, &record, &err.message)
                    .await?;
                ctx.metrics
                    .record_workflow_step(crate::metrics::WorkflowStepOutcome::Fail);
                return Ok(());
            }
        }
    }
}

/// Terminal record for an advance that failed before any step could run
/// (schema load) or on a corrupt row (step index out of range).
fn failed_outcome(
    row: &crate::workflows::WorkflowRow,
    error: &str,
) -> crate::protocol::StepOutcome {
    crate::protocol::StepOutcome {
        step_index: row.current_step,
        status: crate::protocol::OutcomeStatus::Failed,
        attempts: row.attempts.max(1),
        at: now_ms(),
        error: Some(error.to_string()),
    }
}

/// Runs one TTL reaper sweep. For each table with `ttl`, batch-deletes expired
/// rows and publishes through the four tap sites with `source = "ttl"`. TTL
/// deletes are system-initiated (`owner = None`), bypassing per-row auth like
/// scheduled jobs. Fire-and-forget — errors are logged, not surfaced; a failed
/// delete retries on the next sweep. Each table's delete is an independent
/// statement so one table's failure does not abort the others.
///
/// Single-writer invariant: this runs inside the committer task's serialized
/// turn. It issues the DELETE directly (not via `execute_txn`) because TTL
/// expiry is not a client mutation — there is no idempotency key, no owner
/// pre-check, and no per-step result to return. A delete captures no
/// `doc_values`, so `fan_out` table-level re-runs (sound over-approximation).
async fn handle_reaper(ctx: &CommitterCtx) -> Result<(), RtDbError> {
    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let now = now_ms();
    let mut write_set = WriteSet::default();
    for (table_name, table_def) in &schema.tables {
        let Some(ttl) = &table_def.ttl else {
            continue;
        };
        let pg_schema_name = crate::ddl::pg_schema(&ctx.db);
        let table_ident = crate::ddl::pg_table(table_name);
        let col = crate::ddl::pg_col(&ttl.field);
        // FM-33: when some table declares an `onDelete` field referencing this
        // one, a bulk DELETE would strand (cascade/setNull) or ignore
        // (restrict) the children. Select the expired batch, then per-row
        // cascade with `force_hard = true` — TTL expiry is a real delete even
        // on a softDelete table; the reaper is the collector of last resort.
        if crate::txn::has_on_delete_children(&schema, table_name) {
            let ids: Vec<(String,)> = match sqlx::query_as(&format!(
                "SELECT id FROM \"{pg_schema_name}\".\"{table_ident}\" \
                 WHERE \"{col}\" IS NOT NULL AND \"{col}\" < $1 \
                 ORDER BY \"{col}\" LIMIT $2"
            ))
            .bind(now)
            .bind(ctx.ttl_batch)
            .fetch_all(&ctx.pool)
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    if matches!(
                        crate::db::database_exists(&ctx.pool, &ctx.db).await,
                        Ok(false)
                    ) {
                        return Ok(());
                    }
                    tracing::warn!(
                        db = %ctx.db, table = %table_name, error = %e,
                        "ttl reaper select failed"
                    );
                    continue;
                }
            };
            if ids.is_empty() {
                continue;
            }
            let Ok(mut conn) = ctx.pool.acquire().await else {
                continue;
            };
            // `visited` is shared across the sweep so a row already cascaded
            // by an earlier expired row's cascade is skipped, not an error;
            // the budget is fresh per initiating row (`MAX_CASCADE_ROWS` is
            // per initiating delete).
            let mut visited: HashSet<(String, String)> = HashSet::new();
            for (id,) in ids {
                let mut cascade_rows = 0usize;
                if let Err(e) = crate::txn::delete_row_cascade(
                    &mut conn,
                    &pg_schema_name,
                    &schema,
                    table_name,
                    &id,
                    &mut write_set,
                    &mut visited,
                    &mut cascade_rows,
                    true,
                )
                .await
                {
                    if matches!(
                        crate::db::database_exists(&ctx.pool, &ctx.db).await,
                        Ok(false)
                    ) {
                        return Ok(());
                    }
                    // Per-row statements autocommit, so cascade work before
                    // the failure is durable and stays in `write_set` — it
                    // publishes below. The failed row remains expired and
                    // retries on the next sweep (at-least-once).
                    tracing::warn!(
                        db = %ctx.db, table = %table_name, doc_id = %id, error = %e,
                        "ttl reaper cascade failed"
                    );
                }
            }
            continue;
        }
        let rows: Vec<(String,)> = match sqlx::query_as(&format!(
            "DELETE FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE id IN (
                 SELECT id FROM \"{pg_schema_name}\".\"{table_ident}\"
                 WHERE \"{col}\" IS NOT NULL AND \"{col}\" < $1
                 ORDER BY \"{col}\" LIMIT $2
             ) RETURNING id"
        ))
        .bind(now)
        .bind(ctx.ttl_batch)
        .fetch_all(&ctx.pool)
        .await
        {
            Ok(rows) => rows,
            Err(e) => {
                // A dropped db removes the schema mid-sweep; treat as a no-op
                // exit like the scheduler/cleanup tasks do.
                if matches!(
                    crate::db::database_exists(&ctx.pool, &ctx.db).await,
                    Ok(false)
                ) {
                    return Ok(());
                }
                tracing::warn!(
                    db = %ctx.db, table = %table_name, error = %e,
                    "ttl reaper delete failed"
                );
                continue;
            }
        };
        if rows.is_empty() {
            continue;
        }
        for (id,) in rows {
            write_set.touch(table_name, &id, OpKind::Delete);
        }
    }
    if write_set.ops.is_empty() {
        return Ok(());
    }
    // Four-tap publication (fan_out → op-feed → audit → webhook). No quota
    // refresh — the reaper only frees storage. `owner = None`, `source = "ttl"`
    // (system-initiated expiry, no interactive principal). On the cascade path
    // the ops include the children (hard-deleted or setNull-patched), matching
    // the op-feed's per-durable-write contract.
    publish_taps(ctx, &schema, &write_set, None, "ttl", true, false).await;
    for _ in 0..write_set.ops.len() {
        ctx.metrics.record_ttl_expired();
    }
    Ok(())
}

/// FM-27 committer arm: per table, select candidate rows whose
/// principal-bearing fields reference `anon_id`, rewrite the docs in Rust,
/// and apply per-row updates via `txn::apply_update` (recomputing indexed
/// columns + bumping version). A unique-index collision on one row (surfaced
/// by the sqlx→RtDbError mapping as `ErrorCode::Conflict`) skips that row
/// into `conflicts` and continues. Publishes through `publish_taps` with
/// `source = "merge"`, `owner = None` (system-initiated) so subscriptions,
/// op-feed, audit, and webhooks all fire.
///
/// Single-writer invariant: document writes happen only here, inside the
/// serialized committer turn. Like `handle_reaper`, statements issue directly
/// on the pool with NO explicit transaction, so a per-row 23505 aborts only
/// that row; unlike it, each successful row is captured on the `WriteSet`
/// with before/after values so `fan_out`'s window checks see the doc crossing
/// an eq boundary (a re-stamp is exactly that).
///
/// Abort semantics: because the per-row statements autocommit, rows restamped
/// before a mid-merge failure (any non-conflict error) are already durable.
/// The abort path therefore does NOT return early — it breaks out of the
/// loops, publishes `publish_taps` + the metric for everything that committed,
/// and only then returns the recorded error. Returning without publishing
/// would leave live subscriptions stale (no `fan_out` ran at all, so the
/// verify-skip safety net cannot help) and silently skip the op-feed/audit/
/// webhook taps — violating the "every durable write publishes here" contract.
async fn handle_merge_users(
    ctx: &CommitterCtx,
    anon_id: &str,
    real_id: &str,
) -> Result<crate::merge::MergeDbResult, RtDbError> {
    use crate::merge::{
        FieldKind, MergeConflict, MergeDbResult, principal_bearing_fields, rewrite_doc,
    };

    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let pg_schema_name = crate::ddl::pg_schema(&ctx.db);
    let mut result = MergeDbResult::default();
    let mut write_set = WriteSet::default();
    let mut restamped = 0usize;
    let mut abort: Option<RtDbError> = None;

    for (table_name, table_def) in &schema.tables {
        let fields = principal_bearing_fields(table_def);
        if fields.is_empty() {
            continue;
        }
        let indexed = crate::ddl::indexed_fields(table_def);
        let table_ident = crate::ddl::pg_table(table_name);

        // One predicate per principal-bearing field, OR-joined; each binds the
        // anon uid once. Scalar fields use their typed f_ column when indexed,
        // else the jsonb doc path; arrays use jsonb containment.
        let mut predicates: Vec<String> = Vec::new();
        let mut binds = 0usize;
        for pf in &fields {
            binds += 1;
            let ph = format!("${binds}");
            predicates.push(match pf.kind {
                FieldKind::Scalar if indexed.contains(&pf.field) => {
                    format!("\"{}\" = {ph}", crate::ddl::pg_col(&pf.field))
                }
                FieldKind::Scalar => {
                    format!("\"doc\"->'{}' = to_jsonb({ph}::text)", pf.field)
                }
                FieldKind::Array => {
                    format!("\"doc\"->'{}' @> to_jsonb({ph}::text)", pf.field)
                }
            });
        }
        let sql = format!(
            "SELECT \"id\", \"doc\", \"created_at\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE {}",
            predicates.join(" OR ")
        );
        let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64)>(&sql);
        for _ in 0..binds {
            query = query.bind(anon_id);
        }
        let rows = match query.fetch_all(&ctx.pool).await {
            Ok(rows) => rows,
            Err(err) => {
                // Dropped-db guard, mirroring handle_reaper's tolerance: a db
                // removed mid-merge loses its schema — return what restamped.
                if matches!(database_exists(&ctx.pool, &ctx.db).await, Ok(false)) {
                    break;
                }
                // Db alive but the scan failed: abort rather than skip — a skipped
                // table's docs would be permanently stranded after the orchestrator's
                // guarded anon delete (the anon id would no longer exist).
                tracing::error!(
                    db = %ctx.db, table = %table_name, error = %err,
                    "merge: table scan failed; aborting so earlier restamps still publish"
                );
                abort = Some(err.into());
                break;
            }
        };

        let mut conn = match ctx.pool.acquire().await {
            Ok(conn) => conn,
            Err(err) => {
                // Same abort contract as a row error: earlier tables' rows are
                // already committed, so break to the publish-then-return-Err path
                // below instead of early-returning past publish_taps.
                abort = Some(err.into());
                break;
            }
        };
        let mut table_count = 0usize;
        for (id, doc_value, created_at) in rows {
            let serde_json::Value::Object(mut doc) = doc_value else {
                continue;
            };
            // Snapshot the pre-rewrite body first: `fan_out`'s window checks
            // need the before-state (anon uid) to see the doc LEAVING an eq
            // window — a before==after capture would let a skip fire.
            let pre_doc = doc.clone();
            if !rewrite_doc(&mut doc, &fields, anon_id, real_id) {
                continue;
            }
            match crate::txn::apply_update(
                &mut conn,
                &pg_schema_name,
                table_def,
                table_name,
                &id,
                &doc,
            )
            .await
            {
                Ok(()) => {
                    write_set.touch(table_name, &id, OpKind::Patch);
                    write_set.capture_doc(
                        table_name,
                        &id,
                        Some(Some(&pre_doc)),
                        Some(Some(&doc)),
                        Some(created_at),
                    );
                    table_count += 1;
                }
                Err(err) if err.code == crate::error::ErrorCode::Conflict => {
                    // 23505: the restamped row would collide with a row the
                    // real user already owns. Skip, report, keep going.
                    tracing::warn!(
                        db = %ctx.db, table = %table_name, id = %id,
                        "merge: unique conflict, row keeps anon owner"
                    );
                    result.conflicts.push(MergeConflict {
                        table: table_name.clone(),
                        id,
                    });
                }
                Err(err) => {
                    // Non-conflict failure: stop restamping, but the rows that
                    // already committed must still publish below — see the
                    // abort-semantics note on this fn. Breaks the ROW loop
                    // only, so this table's bookkeeping below still runs.
                    abort = Some(err);
                    break;
                }
            }
        }
        if table_count > 0 {
            result.tables.insert(table_name.clone(), table_count);
            restamped += table_count;
        }
        // After the aborted table's bookkeeping, stop walking further tables.
        if abort.is_some() {
            break;
        }
    }

    if !write_set.ops.is_empty() {
        publish_taps(ctx, &schema, &write_set, None, "merge", true, false).await;
    }
    for _ in 0..restamped {
        ctx.metrics.record_merge_doc();
    }
    if let Some(err) = abort {
        tracing::error!(
            db = %ctx.db, error = %err,
            "merge: aborted mid-way; taps were still published for the rows that committed"
        );
        return Err(err);
    }
    Ok(result)
}

/// Applies a declarative migration through the committer, mirroring
/// `handle_mutate`'s post-commit tap-site block so the same four downstream
/// surfaces (subscription fan-out, op-feed, audit, webhook) observe the writes.
///
/// Single-writer invariant: this runs in the committer task's turn and opens its
/// own `pool.begin()` inside that task (the only writer). It never calls
/// `execute_txn`. The pre-migration schema is read from `meta` (NOT the cache)
/// so `plan_migration` operates on authoritative state; `apply_migration` re-reads
/// `meta` inside the tx for its DDL decisions. The derived schema is validated
/// before any DML because directive targets (rename `to`, changeType `to`,
/// evalExpr `set`) are new user input interpolated into SQL.
///
/// `dry_run` runs the DDL+DML to collect the preview but rolls the tx back and
/// publishes through no tap site. On commit, the derived schema is persisted to
/// `meta` (same shape as `ddl::push_schema`'s tail), the cache is refreshed, and
/// the four taps fire with `owner = None` and `source = "migrate"` (no
/// interactive principal, like `handle_scheduled`).
async fn handle_migrate(
    ctx: &CommitterCtx,
    request: crate::migrate::MigrateRequest,
) -> Result<crate::migrate::MigrateResult, RtDbError> {
    let schema = crate::db::load_schema(&ctx.pool, &ctx.db)
        .await?
        .ok_or_else(|| RtDbError::not_found("database has no schema"))?;
    let derived = crate::migrate::plan_migration(&schema, &request.directives)?;
    // The directive targets (rename `to`, changeType `to`, evalExpr `set`) are
    // new user input that ends up interpolated into SQL; validating the derived
    // schema catches invalid identifiers/types before any DML runs.
    // `plan_migration` folds structurally but does not call `validate`.
    derived.validate()?;
    derived
        .check_table_quota(ctx.hot.load().max_tables_per_db)
        .inspect_err(|_e| {
            ctx.metrics
                .record_quota_rejection(&ctx.db, crate::metrics::QuotaKind::Tables);
        })?;
    // ENH-011 / ARC-004: enforce per-db storage cap (best-effort stale-read,
    // kept current by the background warmer) before the migration writes.
    // Uniform — no admin bypass (migrate is admin-only, but a cap applies the
    // same as any other growing write). `enforce(cap=0)` is a no-op (unset cap).
    let storage_cap = ctx.hot.load().max_storage_bytes_per_db;
    if storage_cap > 0 {
        ctx.quotas
            .enforce(&ctx.pool, &ctx.db, storage_cap)
            .await
            .inspect_err(|_e| {
                ctx.metrics
                    .record_quota_rejection(&ctx.db, crate::metrics::QuotaKind::Storage);
            })?;
    }

    let mut tx = ctx.pool.begin().await?;
    let fx = crate::migrate::apply_migration(
        &mut tx,
        &ctx.db,
        &request.directives,
        &derived,
        request.dry_run,
    )
    .await?;

    if request.dry_run {
        // Preview only: the DDL+DML ran inside the tx to produce `fx.reports`,
        // but nothing is committed and no tap site fires.
        tx.rollback().await?;
        return Ok(crate::migrate::MigrateResult {
            applied: false,
            schema: derived,
            directives: fx.reports,
        });
    }

    // Persist the derived schema (single jsonb blob in "{db_<db>}".meta — same
    // shape as `ddl::push_schema`'s tail upsert). The committer is the only
    // writer for this db, so the read-modify-write under the committer turn is
    // safe. `pg_schema(db)` is already validated/lowercased by `db::create`.
    let schema_json = serde_json::to_value(&derived)
        .map_err(|e| RtDbError::internal(format!("failed to serialize schema: {e}")))?;
    let schema_name = crate::ddl::pg_schema(&ctx.db);
    sqlx::query(&format!(
        "INSERT INTO \"{schema_name}\".meta (key, value) VALUES ('schema', $1) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value"
    ))
    .bind(schema_json)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    ctx.schemas.put(&ctx.db, derived.clone()).await;

    // Schema history capture — best-effort, like the audit/webhook taps below.
    // `derived` is the post-migration schema; principal is None (migrate carries
    // no interactive principal — matches the audit `owner = None` for migrate).
    if let Err(err) =
        crate::schema_history::capture(&ctx.pool, &ctx.db, "migrate", None, &derived).await
    {
        tracing::warn!(db = %ctx.db, error = %err, "schema history capture failed");
    }

    // Four-tap publication (fan_out → op-feed → audit → webhook → quota-refresh)
    // — same contract as `handle_mutate`. The hand-built `WriteSet` carries the
    // touched tables (the subscription re-run gate) and the per-doc ops;
    // `docs`/`doc_values` empty ⇒ table-level re-run, the safe over-approximation
    // for a migration (some ops may touch docs whose ids weren't recorded at
    // the fine-grained (table, id) level — re-running is always sound, never
    // under-approximates). `owner = None`, `source = "migrate"`.
    let write_set = WriteSet {
        tables: fx.touched,
        ops: fx.ops.clone(),
        ..Default::default()
    };
    publish_taps(ctx, &derived, &write_set, None, "migrate", true, true).await;

    Ok(crate::migrate::MigrateResult {
        applied: true,
        schema: derived,
        directives: fx.reports,
    })
}

/// Restores the database's schema shape to a captured `schema_history`
/// snapshot, mirroring `handle_migrate`'s structure (load current → begin tx →
/// DDL → meta upsert → commit → cache refresh → capture → fan-out).
///
/// Single-writer invariant: like `handle_migrate`, this opens its own
/// `pool.begin()` inside the committer task's serialized turn (the only
/// writer) and never calls `execute_txn`. The destructive reconcile
/// (`ddl::reconcile_schema_destructive`) drops tables/columns/indexes present
/// in the live shape but absent from the target snapshot, then
/// `apply_schema_additive` creates the inverse — all in the one tx.
///
/// Two `schema_history` captures bracket the apply: the OUTGOING (current)
/// schema first, so the restore is itself a versioned, undoable operation; the
/// INCOMING (target) schema after, so "latest history row == live schema"
/// stays invariant. Both best-effort (warn, never propagate — the schema change
/// already committed by then), matching the audit/webhook tap discipline.
/// Restore does NOT write `audit_log`/`webhook` rows — it is DDL, not DocOps;
/// `schema_history` is its trail.
///
/// Subscription re-evaluation: the reconcile returns the touched table set,
/// which feeds `fan_out` as a `WriteSet` (table-level re-run, the safe
/// over-approximation — no per-doc `doc_values` are captured for a shape
/// change, mirroring `handle_migrate`).
async fn handle_restore_schema(ctx: &CommitterCtx, target_version: i64) -> Result<i64, RtDbError> {
    let current = crate::db::load_schema(&ctx.pool, &ctx.db)
        .await?
        .ok_or_else(|| RtDbError::not_found("database has no schema"))?;
    let entry = crate::schema_history::get(&ctx.pool, &ctx.db, target_version)
        .await?
        .ok_or_else(|| RtDbError::not_found("schema version not found"))?;
    let target: crate::schema::SchemaDef = serde_json::from_value(entry.schema).map_err(|e| {
        tracing::error!(db = %ctx.db, error = %e, "failed to decode schema snapshot");
        RtDbError::internal("failed to decode schema snapshot")
    })?;
    target.validate()?;

    // Safety net: capture the outgoing schema first so the restore is undoable.
    if let Err(err) =
        crate::schema_history::capture(&ctx.pool, &ctx.db, "restore", None, &current).await
    {
        tracing::warn!(db = %ctx.db, error = %err, "schema history capture (outgoing) failed");
    }

    let mut tx = ctx.pool.begin().await?;
    let touched =
        crate::ddl::reconcile_schema_destructive(&mut tx, &ctx.db, &current, &target).await?;
    // Persist the target blob (same shape as push/migrate tails).
    let schema_json = serde_json::to_value(&target).map_err(|e| {
        tracing::error!(db = %ctx.db, error = %e, "failed to serialize schema");
        RtDbError::internal("failed to serialize schema")
    })?;
    let schema_name = crate::ddl::pg_schema(&ctx.db);
    sqlx::query(&format!(
        "INSERT INTO \"{schema_name}\".meta (key, value) VALUES ('schema', $1) \
         ON CONFLICT (key) DO UPDATE SET value = excluded.value"
    ))
    .bind(schema_json)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    ctx.schemas.put(&ctx.db, target.clone()).await;

    // Capture the incoming (target) state so the latest history row == live schema.
    if let Err(err) =
        crate::schema_history::capture(&ctx.pool, &ctx.db, "restore", None, &target).await
    {
        tracing::warn!(db = %ctx.db, error = %err, "schema history capture (incoming) failed");
    }

    // Re-evaluate subscriptions: dropped tables/columns invalidate their subs.
    // Table-level re-run (no per-doc `doc_values`) — the safe over-approximation
    // for a shape change, same as `handle_migrate`. Routed through `publish_taps`
    // with `docop_taps=false`: restore is pure DDL, no DocOps are produced, so
    // the op-feed/audit/webhook taps are skipped — but the exception is now
    // visible at the call site rather than hidden by a direct `fan_out` call,
    // and a future change to the tap sequence stays consistent across handlers.
    let write_set = WriteSet {
        tables: touched.into_iter().collect(),
        ..Default::default()
    };
    publish_taps(ctx, &target, &write_set, None, "restore", false, false).await;

    Ok(target_version)
}

async fn handle_subscribe(
    ctx: &CommitterCtx,
    conn: ConnId,
    query_id: String,
    query: Query,
    tx: UnboundedSender<ServerMessage>,
    principal_ctx: PrincipalCtx,
) -> Result<(), RtDbError> {
    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let result = execute_query(&ctx.pool, &ctx.db, &schema, &query, &principal_ctx, false).await?;
    let last = canonical(&result);
    // Mirror `subs::fan_out`: a serialization failure is logged and surfaced
    // as an internal error so the subscriber sees an explicit error rather
    // than a silently-pushed `{"result": null}` (QA-004). In practice
    // `QueryResult` has only serializable leaves, so this never fires today —
    // but the failure shape is no longer silent.
    let value = serde_json::to_value(&result).map_err(|err| {
        tracing::error!(error = %err, db = %ctx.db, "failed to serialize initial query result");
        RtDbError::internal("failed to serialize initial query result")
    })?;

    if tx
        .send(ServerMessage::QueryUpdate {
            query_id: query_id.clone(),
            result: value,
        })
        .is_err()
    {
        tracing::debug!(
            db = %ctx.db,
            query_id,
            "subscribe: connection already gone, not registering"
        );
        return Ok(());
    }

    // Resolve the table def so `register` can derive a fine-grained ReadSet —
    // `Indexed` (count / collect / unique on an eq-prefix window) or `Ordered`
    // (take / first / paginate, whose top-N boundary is seeded from `result`).
    // `execute_query` above already resolved the same table successfully, so
    // this lookup won't miss in practice; propagating its error (rather than
    // falling back to `Table`) matches today's behavior — a subscription whose
    // table has vanished between execute and register is already a transient
    // error path.
    let table_def = schema.table(&query.table)?;

    // ENH-011: enforce the per-db concurrent-subscription cap (RTDB_MAX_SUBS_PER_DB,
    // hot-reloadable). Uniform — no admin bypass — because `PrincipalCtx` cannot
    // distinguish an admin from a machine token at the committer (both arrive as
    // `PrincipalCtx::bypass()`, `user_id == None`); the db-level gate has already
    // authorized the connection. Runs before registration so a rejected subscribe
    // never enters the shard. `count_for_db` is approximate (a concurrent
    // unsubscribe can drop the count), which is acceptable — the cap is a guard
    // rail, not an exact budget, and a near-concurrent subscribe still lands within
    // `cap + (concurrent subscribers)` of the limit.
    let sub_cap = ctx.hot.load().max_subs_per_db;
    if sub_cap > 0 {
        let n = ctx.subs.count_for_db(&ctx.db).await;
        if n >= sub_cap {
            ctx.metrics
                .record_quota_rejection(&ctx.db, crate::metrics::QuotaKind::Subs);
            return Err(RtDbError::quota_exceeded(format!(
                "db '{}' has {} active subscription(s), limit is {sub_cap}",
                ctx.db, n
            )));
        }
    }

    ctx.subs
        .register(
            &ctx.db,
            conn,
            query_id,
            query,
            tx,
            last,
            principal_ctx,
            table_def,
            &result,
        )
        .await?;
    Ok(())
}
