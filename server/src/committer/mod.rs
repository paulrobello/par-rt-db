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
use tokio::sync::{Mutex, Notify, mpsc, oneshot};
use tracing::Instrument;

use crate::auth::PrincipalCtx;
use crate::config::{Config, HotConfig};
use crate::db::{SchemaCache, database_exists, now_ms};
use crate::error::RtDbError;
use crate::metrics::Metrics;
use crate::mutation_log;
use crate::protocol::ServerMessage;
use crate::query::{Query, diff_canonical, execute_query};
use crate::scheduler;
use crate::subs::{ConnId, SubscriptionManager};
use crate::txn::{OpKind, Transaction, TxnOutcome, WriteSet, execute_txn};

pub(in crate::committer) mod arms;
mod forwarding;
mod lease;
mod supervisor;
mod taps;

use arms::merge::handle_merge_users;
use arms::migrate::handle_migrate;
use arms::mutate::handle_mutate;
use arms::reaper::handle_reaper;
use arms::scheduled::handle_scheduled;
use arms::schema::{handle_push_schema, handle_restore_schema};
use arms::subscribe::handle_subscribe;
use arms::workflow::handle_workflow_advance;
use forwarding::{
    decode_or_internal, fail_forwarded_reply, forward_write_of, mint_forward_idempotency_key,
};
use lease::{acquire_ownership_lease, reply_ownership_conflict, request_needs_write};
use supervisor::{reclaim_idle_pass, run_quota_warmer};
use taps::publish_taps;

/// Bound on each per-db committer task's inbox.
const CHANNEL_BUFFER: usize = 64;

/// Bound on how long `channel_for` waits for a draining entry's committer
/// task to exit (ARC-015) before giving up and surfacing an error, rather
/// than hanging forever on a stuck drain.
const CHANNEL_DRAIN_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);

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
        every_ms: Option<i64>,
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
    /// Apply a schema push on this database. Serialized through the per-db
    /// committer like `RunMigrate` — a push's backfill UPDATEs (ttl
    /// `defaultDurationMs`, computed entries) are document writes, so they
    /// belong inside the single-writer turn; running them on the HTTP task
    /// raced concurrent committer txns and left subscriptions serving stale
    /// values until the table's next write. After the apply, the
    /// backfill-affected tables' subscriptions re-run (table-level, no
    /// per-doc captures). `reply` carries the applied schema.
    RunPushSchema {
        schema: crate::schema::SchemaDef,
        reply: oneshot::Sender<Result<crate::schema::SchemaDef, RtDbError>>,
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
///
/// `Clone` (ENH-022 Stage 4c): every field is a `PgPool`, `Arc`, scalar, or
/// the shared `channels` map, so a clone is reference bumps — the forward
/// listener task holds a clone to execute forwarded writes as the owner.
#[derive(Clone)]
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
    /// Stage 4c origin-side forwarding handle. `Some` exactly when
    /// `multi_instance` is true (built in `AppState::new`, shared with the
    /// forward listener). A write submitted against a SHADOW committer is
    /// broadcast through it to the lease owner; a failed/timeout forward
    /// falls back to the local takeover attempt.
    forwarder: Option<Arc<crate::forward::Forwarder>>,
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
    /// Notified (ARC-015) when the entry's committer task has exited and the
    /// supervisor closure has removed it from `channels`. A `channel_for`
    /// caller waiting on a draining entry awaits this instead of sleep-polling
    /// the map every 2ms. See `channel_for`'s drain-wait for the
    /// missed-wakeup-safe registration order.
    drained: Arc<Notify>,
    /// ENH-022 Stage 4: `Some` on the OWNING replica — the one-connection
    /// lease pool whose backend holds the db's advisory lock; the committer
    /// and its pollers run on it (writes and lease share one backend, so no
    /// other replica can ever be mid-write). `None` = a SHADOW committer on a
    /// non-owner: read arms (Subscribe's initial run) work, every write arm
    /// rejects with CONFLICT, and a write submit attempts the ownership
    /// upgrade first (`submit`'s needs-write path) — which is the failover
    /// path when the owner dies and its backend releases the lock.
    lease: Option<PgPool>,
}

/// Config scalars the committer needs (ARC-204): `Committers::new` takes
/// these as one named struct instead of a positional list where same-typed
/// neighbors (`u64` secs next to `u64` secs) could transpose silently. The
/// six non-config handles (pool, subscription manager, schema cache,
/// op-feed, hot config, metrics) stay separate parameters — they are
/// dependencies, not config.
pub struct CommitterConfig {
    pub audit_log_enabled: bool,
    pub webhooks_enabled: bool,
    pub ttl_sweep_interval_secs: u64,
    pub ttl_batch: i64,
    /// Per-db storage-usage cache, shared with `AppState`'s storage paths.
    /// A runtime handle rather than boot config, but carried here — paired
    /// with `quota_cache_ttl_secs`, which configures its refresh — so
    /// `Committers::new` stays under clippy's too-many-arguments threshold.
    pub quotas: Arc<crate::quota::UsageCache>,
    pub quota_cache_ttl_secs: u64,
    /// 0 = idle reclamation off; see `Committer::idle_threshold`.
    pub idle_reclaim_secs: u64,
    /// This process's replica id. Resolved once at boot in `AppState::new`
    /// (explicit `RTDB_INSTANCE_ID` or generated) and shared with presence,
    /// the NOTIFY listeners, and `AppState.instance_id` — passed in rather
    /// than re-derived here so every consumer tags payloads with the same id.
    pub instance_id: String,
    pub multi_instance: bool,
    /// Stage 4c forwarding handle, `Some` exactly when `multi_instance`.
    /// Built in `AppState::new` and shared with the forward listener task
    /// (`forward::run_forward_listener`), which resolves its replies.
    pub forwarder: Option<Arc<crate::forward::Forwarder>>,
}

impl CommitterConfig {
    /// Derive the committer's config slice from the boot `Config` in one
    /// place. `quotas`, `instance_id`, and `forwarder` are the boot-built
    /// handles/ids shared with `AppState` (see the field docs).
    pub fn from_config(
        config: &Config,
        quotas: Arc<crate::quota::UsageCache>,
        instance_id: String,
        forwarder: Option<Arc<crate::forward::Forwarder>>,
    ) -> Self {
        Self {
            audit_log_enabled: config.audit_log_enabled,
            webhooks_enabled: config.webhooks_enabled,
            ttl_sweep_interval_secs: config.ttl_sweep_interval_secs,
            ttl_batch: config.ttl_batch,
            quotas,
            quota_cache_ttl_secs: config.quota_cache_ttl_secs,
            idle_reclaim_secs: config.db_idle_reclaim_secs,
            instance_id,
            multi_instance: config.multi_instance.enabled,
            forwarder,
        }
    }
}

impl Committers {
    pub fn new(
        pool: PgPool,
        subs: Arc<SubscriptionManager>,
        schemas: SchemaCache,
        op_feed: Arc<crate::op_feed::OpFeed>,
        hot: Arc<ArcSwap<HotConfig>>,
        metrics: Arc<Metrics>,
        cfg: CommitterConfig,
    ) -> Self {
        Self {
            pool,
            subs,
            schemas,
            op_feed,
            hot,
            metrics,
            quotas: cfg.quotas,
            audit_log_enabled: cfg.audit_log_enabled,
            webhooks_enabled: cfg.webhooks_enabled,
            ttl_sweep_interval: std::time::Duration::from_secs(cfg.ttl_sweep_interval_secs),
            ttl_batch: cfg.ttl_batch,
            quota_cache_ttl_secs: cfg.quota_cache_ttl_secs,
            idle_threshold: std::time::Duration::from_secs(cfg.idle_reclaim_secs),
            instance_id: cfg.instance_id,
            multi_instance: cfg.multi_instance,
            forwarder: cfg.forwarder,
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
    fn make_ctx(&self, db: String, pool: PgPool, owns_writes: bool) -> CommitterCtx {
        CommitterCtx {
            pool,
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
            owns_writes,
        }
    }

    /// Submits a request to `db`'s committer task, lazily spawning it on
    /// first use. Errors `NotFound` if `db` isn't a registered database.
    ///
    /// ENH-022 Stage 4c: under multi-instance, a WRITE arm that lands on a
    /// SHADOW (non-owner) committer is forwarded to the lease owner over
    /// NOTIFY (`forward_or_takeover`) instead of replying CONFLICT; only the
    /// owner executes, and its outcome travels back to this replica's caller.
    /// Read arms and single-instance submits are unchanged.
    pub async fn submit(&self, db: &str, req: CommitterRequest) -> Result<(), RtDbError> {
        if self.multi_instance && request_needs_write(&req) {
            let (sender, is_shadow) = self.channel_for(db, false).await?;
            if is_shadow {
                return self.forward_or_takeover(db, req).await;
            }
            return self.send_and_evict(db, sender, req).await;
        }
        let (sender, _) = self.channel_for(db, false).await?;
        self.send_and_evict(db, sender, req).await
    }

    /// Owner-side submit used by the forward listener (`forward.rs`): the
    /// same send-and-evict path as `submit`, but it NEVER forwards — this
    /// replica has already been verified to hold `db`'s lease, so
    /// re-broadcasting would loop the request around the fleet.
    pub(crate) async fn submit_owned(
        &self,
        db: &str,
        req: CommitterRequest,
    ) -> Result<(), RtDbError> {
        let (sender, _) = self.channel_for(db, false).await?;
        self.send_and_evict(db, sender, req).await
    }

    /// Send into `db`'s committer channel, evicting the stale entry if the
    /// task is gone (e.g. it panicked) so the next request respawns a fresh
    /// task. The eviction only removes the entry if it still holds the same
    /// sender that just failed (`same_channel`) — otherwise a concurrent
    /// caller already respawned under this db key and evicting would drop the
    /// live replacement instead of the dead one.
    async fn send_and_evict(
        &self,
        db: &str,
        sender: mpsc::Sender<CommitterRequest>,
        req: CommitterRequest,
    ) -> Result<(), RtDbError> {
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

    /// True when THIS replica holds `db`'s ownership lease (a live, non-
    /// draining entry with `lease: Some`). The forward listener uses this to
    /// decide whether a broadcast write belongs here — every replica receives
    /// the NOTIFY, and only the owner executes + replies.
    pub(crate) async fn is_owner(&self, db: &str) -> bool {
        let guard = self.channels.lock().await;
        guard
            .get(db)
            .is_some_and(|entry| !entry.draining && entry.lease.is_some())
    }

    /// Stage 4c: forward a write that landed on this SHADOW (non-owner)
    /// committer to the lease owner, and on a forward that produced no owner
    /// reply (owner dead, or notify failed) attempt the lease takeover —
    /// `channel_for(db, true)` retires the shadow and respawns with an
    /// acquire attempt; if another replica still owns, the respawn is a
    /// shadow whose write arm replies CONFLICT (the `run_committer`
    /// backstop), which is the honest answer: retry reaches the new owner.
    async fn forward_or_takeover(
        &self,
        db: &str,
        mut req: CommitterRequest,
    ) -> Result<(), RtDbError> {
        let Some(forwarder) = self.forwarder.clone() else {
            // multi_instance without a forwarder is a wiring bug (AppState
            // always builds one); Stage 4a behavior is the safe fallback.
            return self.takeover_submit(db, req).await;
        };
        // ARC-003: mint an idempotency key for an unkeyed forwarded mutate.
        // The forward path has a documented timeout ambiguity — a reply racing
        // the timeout is dropped, and the origin then takes over and RESUBMITS
        // the same write, which without a key executes it a second time. The
        // minted key is stamped on both the request and the forwarded payload,
        // so the owner logs it in the shared `mutations` dedup table and the
        // takeover's resubmission comes back as a replay of the first outcome
        // instead of a duplicate write. Only `Mutate` needs this: migrate,
        // push, merge, and restore are idempotent by construction. The key is
        // internal — it never appears in the client's response.
        mint_forward_idempotency_key(&mut req);
        let Some(write) = forward_write_of(&req) else {
            // Fire-and-forget write arm reaching a shadow — only possible
            // when a poller raced the takeover. Submit locally; the shadow's
            // CONFLICT backstop (`run_committer`) answers it.
            return self.submit_owned(db, req).await;
        };
        match forwarder.forward(db, write).await {
            Ok(Ok(value)) => {
                self.complete_forwarded_reply(db, req, value).await;
                Ok(())
            }
            Ok(Err(err)) => {
                fail_forwarded_reply(req, err);
                Ok(())
            }
            Err(fail) => {
                tracing::info!(
                    db = %db,
                    reason = %match &fail {
                        crate::forward::ForwardFail::Notify(_) => "notify",
                        crate::forward::ForwardFail::Timeout => "timeout",
                    },
                    "forward: no owner replied; attempting lease takeover"
                );
                self.takeover_submit(db, req).await
            }
        }
    }

    /// The takeover half of the Stage 4c failover path: retire any shadow and
    /// respawn with a lease attempt (`channel_for(db, true)`), then submit the
    /// original request. Owner acquired → the write executes locally; lease
    /// still held elsewhere → the respawned shadow replies CONFLICT.
    async fn takeover_submit(&self, db: &str, req: CommitterRequest) -> Result<(), RtDbError> {
        let (sender, _) = self.channel_for(db, true).await?;
        self.send_and_evict(db, sender, req).await
    }

    /// Deliver the owner's serialized outcome into the original request's
    /// reply channel, decoding it back into the arm's concrete type. For a
    /// Mutate, also re-run this replica's local subscriptions against the
    /// owner's `WriteSet` — the owner fanned out to ITS subscribers inside
    /// its committer turn; this pass extends the same invalidation to the
    /// subscribers connected to THIS replica (reads fan out safely under
    /// READ COMMITTED). `doc_values` did not travel (it is `#[serde(skip)]`),
    /// so Indexed/Ordered subscriptions degrade to their conservative
    /// "unrankable ⇒ re-run" fallback — never a missed push.
    async fn complete_forwarded_reply(
        &self,
        db: &str,
        req: CommitterRequest,
        value: serde_json::Value,
    ) {
        match req {
            CommitterRequest::Mutate { reply, .. } => {
                match serde_json::from_value::<TxnOutcome>(value.clone()) {
                    Ok(outcome) => {
                        if outcome.write_set != WriteSet::default() {
                            match self.schemas.get(&self.pool, db).await {
                                Ok(schema) => {
                                    self.subs
                                        .fan_out(&self.pool, db, &schema, &outcome.write_set)
                                        .await;
                                }
                                Err(err) => tracing::warn!(
                                    db = %db,
                                    error = %err,
                                    "forwarded mutate fan-out skipped: schema fetch failed"
                                ),
                            }
                        }
                        let _ = reply.send(Ok(outcome));
                    }
                    Err(err) => {
                        let _ = reply.send(Err(RtDbError::internal(format!(
                            "forwarded mutate reply failed to decode: {err}; payload: {value}"
                        ))));
                    }
                }
            }
            CommitterRequest::RunMigrate { reply, .. } => {
                decode_or_internal::<crate::migrate::MigrateResult>(value, reply)
            }
            CommitterRequest::RunPushSchema { reply, .. } => {
                decode_or_internal::<crate::schema::SchemaDef>(value, reply)
            }
            CommitterRequest::RunMergeUsers { reply, .. } => {
                decode_or_internal::<crate::merge::MergeDbResult>(value, reply)
            }
            CommitterRequest::RunRestoreSchema { reply, .. } => {
                decode_or_internal::<i64>(value, reply)
            }
            // `forward_write_of` returned Some only for the five arms above;
            // anything else never reaches this method. Drop it with an
            // internal error — never resubmit: the owner already executed.
            other => {
                tracing::error!("complete_forwarded_reply: unmatched request arm (unreachable)");
                fail_forwarded_reply(
                    other,
                    RtDbError::internal("forwarded reply had no matching arm"),
                );
            }
        }
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
    /// `upgrade` requests the ownership upgrade (ENH-022 Stage 4): if the
    /// cached entry is a SHADOW (non-owner), retire it and loop; the spawn
    /// path then takes the lease (or loses the race to a new owner, yielding
    /// a fresh shadow whose write arm replies CONFLICT). Stage 4c moved the
    /// every-write upgrade onto the forward-timeout path — `submit` forwards
    /// a shadow's write to the owner and calls this ONLY when no owner
    /// answered — so ownership now follows demand solely as the failover
    /// path, which is what keeps a write from ping-ponging the lease between
    /// replicas under a load balancer.
    ///
    /// Returns the sender plus whether it belongs to a SHADOW (`false` =
    /// this replica owns `db`'s lease, or the server is single-instance).
    async fn channel_for(
        &self,
        db: &str,
        upgrade: bool,
    ) -> Result<(mpsc::Sender<CommitterRequest>, bool), RtDbError> {
        let deadline = std::time::Instant::now() + CHANNEL_DRAIN_DEADLINE;
        loop {
            // Fast path: a live (non-draining) entry. Refresh its idle clock
            // (ARC-102 step 4) and return. A draining entry falls through to
            // the wait; an absent entry falls through to the spawn.
            let mut guard = self.channels.lock().await;
            if let Some(entry) = guard.get_mut(db) {
                if entry.draining {
                    // ARC-015: register interest in this entry's
                    // drain-completion `Notify` — via `notified()` +
                    // `enable()` — while STILL holding the lock, then await
                    // it (bounded by `deadline`) after releasing. The
                    // supervisor closure that removes a draining entry and
                    // calls `notify_waiters()` (see `channel_for`'s spawn
                    // site below) also needs this same lock, so whichever
                    // side acquires it second is guaranteed to observe the
                    // other's effect: `enable()` can never miss a concurrent
                    // `notify_waiters()`. `enable()` (not a bare `.await` on
                    // `notified()`) is required here — a `Notified` future
                    // only registers as a waiter once polled/enabled, so
                    // constructing it is not by itself race-safe. This
                    // replaces the old 2ms sleep-poll without reintroducing
                    // its race.
                    let notify = entry.drained.clone();
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    drop(guard);
                    if std::time::Instant::now() >= deadline {
                        return Err(RtDbError::internal(
                            "committer for database is draining and did not exit in time",
                        ));
                    }
                    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                    let _ = tokio::time::timeout(remaining, notified).await;
                    continue;
                } else if upgrade && self.multi_instance && entry.lease.is_none() {
                    // ENH-022 Stage 4/4c upgrade (takeover) path: retire
                    // the shadow (its exit clears the entry) and loop —
                    // the spawn path then takes the ownership lease,
                    // which is the failover path when the previous owner
                    // has died or never answered a forward.
                    entry.draining = true;
                    let sender = entry.sender.clone();
                    let notify = entry.drained.clone();
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    drop(guard);
                    let _ = sender.send(CommitterRequest::Shutdown).await;
                    if std::time::Instant::now() >= deadline {
                        return Err(RtDbError::internal(
                            "committer for database is draining and did not exit in time",
                        ));
                    }
                    let remaining = deadline.saturating_duration_since(std::time::Instant::now());
                    let _ = tokio::time::timeout(remaining, notified).await;
                    continue;
                } else {
                    entry.last_activity = std::time::Instant::now();
                    return Ok((entry.sender.clone(), entry.lease.is_none()));
                }
            }
            drop(guard);

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
                return Ok((entry.sender.clone(), entry.lease.is_none()));
            }

            // ENH-022 Stage 4: in multi-instance mode, first writer to a db
            // takes the ownership lease (advisory lock on a dedicated
            // one-connection pool) and runs the committer + pollers ON it —
            // writes and lease share one backend, so no replica can be
            // mid-write while another owns. A replica that LOSES the acquire
            // gets a SHADOW committer instead: read arms work, write arms
            // reject CONFLICT, and a write submit attempts the upgrade (the
            // failover path once the owner dies).
            let (ctx_pool, poller_pool, lease, owns_writes) = if self.multi_instance {
                match acquire_ownership_lease(&self.pool, db).await {
                    Ok(lease_pool) => (
                        lease_pool.clone(),
                        lease_pool.clone(),
                        Some(lease_pool),
                        true,
                    ),
                    Err(err) if err.code == crate::error::ErrorCode::Conflict => {
                        (self.pool.clone(), self.pool.clone(), None, false)
                    }
                    Err(err) => return Err(err),
                }
            } else {
                (self.pool.clone(), self.pool.clone(), None, true)
            };
            let (tx, rx) = mpsc::channel(CHANNEL_BUFFER);
            let committer_handle = tokio::spawn(run_committer(
                self.make_ctx(db.to_string(), ctx_pool, owns_writes),
                rx,
            ));
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
                        && let Some(entry) = guard.remove(&db_owned)
                    {
                        // ARC-015: wake any `channel_for` caller parked on this
                        // entry's drain-wait. Done under the same lock as the
                        // removal so it races correctly against a waiter's
                        // registration (see `channel_for`'s drain-wait comment).
                        entry.drained.notify_waiters();
                    }
                });
            }
            if owns_writes {
                tokio::spawn(scheduler::run_scheduler(
                    poller_pool.clone(),
                    db.to_string(),
                    tx.clone(),
                ));
            }
            // Per-db dedup-row expiry sweep (ARC-007): owns `mutation_log`'s
            // periodic DELETE so `mutation_log::check` is a pure SELECT on the
            // hot path. Exits when the committer channel closes (same lifecycle
            // signal the scheduler task uses).
            if owns_writes {
                tokio::spawn(mutation_log::run_cleanup(
                    poller_pool.clone(),
                    db.to_string(),
                    tx.clone(),
                ));
            }
            // Per-db TTL reaper: enqueues a fire-and-forget `RunReaper` every
            // `ttl_sweep_interval`; the committer's `handle_reaper` performs the
            // batch delete inside its serialized turn. Same lifecycle as the
            // scheduler/cleanup tasks (exits on channel close or db removal).
            if owns_writes {
                tokio::spawn(crate::reaper::run_reaper(
                    poller_pool.clone(),
                    db.to_string(),
                    tx.clone(),
                    self.ttl_sweep_interval,
                    self.schemas.clone(),
                ));
            }
            // Per-db storage-quota cache warmer (ARC-004): periodically re-measures
            // the db's on-disk size off the committer turn so `enforce` is a cheap
            // stale-read. Same lifecycle as the reaper/cleanup tasks (exits on
            // channel close or db removal); a no-op tick when no storage cap is set.
            if owns_writes {
                tokio::spawn(run_quota_warmer(
                    poller_pool.clone(),
                    db.to_string(),
                    self.quotas.clone(),
                    self.hot.clone(),
                    std::time::Duration::from_secs(self.quota_cache_ttl_secs),
                    tx.clone(),
                ));
            }
            let is_shadow = lease.is_none();
            guard.insert(
                db.to_string(),
                ChannelEntry {
                    sender: tx.clone(),
                    last_activity: std::time::Instant::now(),
                    draining: false,
                    drained: Arc::new(Notify::new()),
                    lease,
                },
            );
            return Ok((tx, is_shadow));
        }
    }

    /// Spawns `db`'s per-db tasks (committer + scheduler + reaper + warmers)
    /// if not already running, without submitting anything. Workflow start
    /// surfaces call this before `workflows::insert`: steps fire from the
    /// per-db scheduler, which only exists once the tasks spawn — a run
    /// started on a cold db (no Mutate/Subscribe since creation) would
    /// otherwise sit `pending` until unrelated data-plane traffic spawns them.
    pub(crate) async fn ensure_spawned(&self, db: &str) -> Result<(), RtDbError> {
        self.channel_for(db, false).await.map(|_| ())
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

    /// Applies a schema push on `db` and waits for the commit-then-fan-out
    /// cycle to complete. Funneled through the per-db committer like `mutate`
    /// so the push's backfill UPDATEs are serialized with concurrent writes
    /// (single-writer invariant) and the backfill-affected tables'
    /// subscriptions re-run on the durable result. Returns the applied
    /// schema. See `handle_push_schema`.
    pub async fn push_schema(
        &self,
        db: &str,
        schema: crate::schema::SchemaDef,
    ) -> Result<crate::schema::SchemaDef, RtDbError> {
        let (reply, reply_rx) = oneshot::channel();
        self.submit(db, CommitterRequest::RunPushSchema { schema, reply })
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
    ///
    /// ARC-009: returns the sweep's `JoinHandle` (`None` when the sweep is
    /// disabled) so `AppState::new` can register it with `BackgroundTasks` and
    /// have it stop on shutdown. Previously this spawned and dropped the
    /// handle, leaving one untracked loop alive past graceful shutdown.
    #[must_use]
    pub fn spawn_idle_reclaimer(&self) -> Option<tokio::task::JoinHandle<()>> {
        if self.idle_threshold.is_zero() {
            return None;
        }
        let channels = Arc::clone(&self.channels);
        let subs = Arc::clone(&self.subs);
        let pool = self.pool.clone();
        let threshold = self.idle_threshold;
        let sweep_interval = threshold.min(std::time::Duration::from_secs(60));
        Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(sweep_interval);
            // Skip the immediate first tick — a db just spawned is fresh.
            tick.tick().await;
            loop {
                tick.tick().await;
                reclaim_idle_pass(&pool, &subs, &channels, threshold).await;
            }
        }))
    }
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
    /// ENH-022 Stage 4: false on a SHADOW committer (a non-owner replica's
    /// read-only stand-in). Every write arm rejects with CONFLICT; the
    /// ownership upgrade (the failover path) happens in `submit` before a
    /// write ever reaches a shadow.
    pub owns_writes: bool,
    /// When true, `publish_taps` also emits one `pg_notify` per DocOp (ENH-022
    /// Stage 2). False on a single-instance deploy — the publish tap is
    /// zero-cost when off.
    multi_instance: bool,
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
        // ENH-022 Stage 4: a SHADOW committer (non-owner replica) serves read
        // arms only — a write arm reaching here lost the ownership-upgrade
        // race in `submit` (another replica holds the lease), so it replies
        // CONFLICT instead of writing.
        if !ctx.owns_writes && request_needs_write(&req) {
            reply_ownership_conflict(&ctx, req).await;
            continue;
        }
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
                every_ms,
            } => {
                let span = tracing::info_span!(
                    "committer.scheduled",
                    db = %ctx.db,
                    kind,
                    id,
                );
                let outcome = handle_scheduled(&ctx, id, kind, *txn, cron, every_ms)
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
            CommitterRequest::RunPushSchema { schema, reply } => {
                let span = tracing::info_span!("committer.push_schema", db = %ctx.db);
                let result = handle_push_schema(&ctx, schema).instrument(span).await;
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
