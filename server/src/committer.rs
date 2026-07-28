use std::collections::HashMap;
use std::sync::Arc;

use arc_swap::ArcSwap;
use sqlx::PgPool;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::config::HotConfig;
use crate::db::{SchemaCache, database_exists, now_ms};
use crate::error::RtDbError;
use crate::mutation_log;
use crate::protocol::ServerMessage;
use crate::query::{Query, canonical, execute_query};
use crate::scheduler;
use crate::subs::{ConnId, SubscriptionManager};
use crate::txn::{Transaction, TxnOutcome, WriteSet, execute_txn};

/// Bound on each per-db committer task's inbox.
const CHANNEL_BUFFER: usize = 64;

pub enum CommitterRequest {
    Mutate {
        idempotency_key: Option<String>,
        txn: Transaction,
        owner: Option<String>,
        reply: oneshot::Sender<Result<TxnOutcome, RtDbError>>,
    },
    Subscribe {
        conn: ConnId,
        query_id: String,
        query: Box<Query>,
        tx: UnboundedSender<ServerMessage>,
        owner: Option<String>,
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
    channels: Mutex<HashMap<String, mpsc::Sender<CommitterRequest>>>,
}

impl Committers {
    pub fn new(
        pool: PgPool,
        subs: Arc<SubscriptionManager>,
        schemas: SchemaCache,
        op_feed: Arc<crate::op_feed::OpFeed>,
        hot: Arc<ArcSwap<HotConfig>>,
        audit_log_enabled: bool,
        webhooks_enabled: bool,
    ) -> Self {
        Self {
            pool,
            subs,
            schemas,
            op_feed,
            hot,
            audit_log_enabled,
            webhooks_enabled,
            channels: Mutex::new(HashMap::new()),
        }
    }

    /// Removes `db`'s committer channel from the map so future mutate/subscribe
    /// requests 404 (the next `submit` would fail `database_exists` first
    /// anyway after `drop_database`). Used by `delete-db`.
    ///
    /// Does NOT cleanly stop the per-db committer, scheduler, or
    /// mutation-log cleanup tasks: those tasks each hold their own clone of
    /// the channel sender, so removing this map entry does not close the
    /// channel. After `DROP SCHEMA CASCADE` removes `scheduled_txns` and
    /// `mutations`, those tasks' next polls log best-effort errors and
    /// continue until the process restarts. This preserves the single-writer
    /// invariant — the channel itself stays single-consumer, and no new
    /// requests can reach the orphan tasks — at the cost of short-lived
    /// orphan tasks. A clean shutdown would require either a
    /// `CommitterRequest::Shutdown` variant with an ACK round-trip or a
    /// `JoinHandle` registry; both are invasive for a rare admin op, so the
    /// map eviction is the documented minimum.
    pub async fn drop_db(&self, db: &str) {
        let mut guard = self.channels.lock().await;
        guard.remove(db);
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
                .is_some_and(|current| current.same_channel(&sender))
            {
                guard.remove(db);
            }
            return Err(RtDbError::internal("committer task is no longer running"));
        }
        Ok(())
    }

    /// Returns `db`'s committer sender, lazily spawning the task on first
    /// use. No `.await` occurs while `channels` is locked: the cache-hit
    /// fast path checks and releases the lock immediately; on a miss, the
    /// lock is dropped before the `database_exists` query, then re-acquired
    /// to insert (double-checking in case another task won the race and
    /// already spawned one).
    async fn channel_for(&self, db: &str) -> Result<mpsc::Sender<CommitterRequest>, RtDbError> {
        {
            let guard = self.channels.lock().await;
            if let Some(sender) = guard.get(db) {
                return Ok(sender.clone());
            }
        }

        if !database_exists(&self.pool, db).await? {
            return Err(RtDbError::not_found(format!("database '{db}' not found")));
        }

        let mut guard = self.channels.lock().await;
        if let Some(sender) = guard.get(db) {
            return Ok(sender.clone());
        }

        let (tx, rx) = mpsc::channel(CHANNEL_BUFFER);
        tokio::spawn(run_committer(
            self.pool.clone(),
            db.to_string(),
            self.subs.clone(),
            self.schemas.clone(),
            self.op_feed.clone(),
            self.hot.clone(),
            self.audit_log_enabled,
            self.webhooks_enabled,
            rx,
        ));
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
        guard.insert(db.to_string(), tx.clone());
        Ok(tx)
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
        owner: Option<String>,
    ) -> Result<TxnOutcome, RtDbError> {
        let (reply, reply_rx) = oneshot::channel();
        self.submit(
            db,
            CommitterRequest::Mutate {
                idempotency_key,
                txn,
                owner,
                reply,
            },
        )
        .await?;
        reply_rx
            .await
            .map_err(|_| RtDbError::internal("committer task dropped the reply"))?
    }

    /// Runs `query` on `db`, sends the initial result on `tx`, and registers
    /// the subscription for future push-on-change updates. `owner` is the
    /// subscriber's per-row auth identity (captured on the `SubEntry` and
    /// applied to every re-run in `fan_out`); `None` = bypass.
    pub async fn subscribe(
        &self,
        db: &str,
        conn: ConnId,
        query_id: String,
        query: Query,
        tx: UnboundedSender<ServerMessage>,
        owner: Option<String>,
    ) -> Result<(), RtDbError> {
        let (reply, reply_rx) = oneshot::channel();
        self.submit(
            db,
            CommitterRequest::Subscribe {
                conn,
                query_id,
                query: Box::new(query),
                tx,
                owner,
                reply,
            },
        )
        .await?;
        reply_rx
            .await
            .map_err(|_| RtDbError::internal("committer task dropped the reply"))?
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
#[allow(clippy::too_many_arguments)]
async fn run_committer(
    pool: PgPool,
    db: String,
    subs: Arc<SubscriptionManager>,
    schemas: SchemaCache,
    op_feed: Arc<crate::op_feed::OpFeed>,
    hot: Arc<ArcSwap<HotConfig>>,
    audit_log_enabled: bool,
    webhooks_enabled: bool,
    mut rx: mpsc::Receiver<CommitterRequest>,
) {
    if let Err(err) = mutation_log::ensure_table(&pool, &db).await {
        tracing::error!(db = %db, error = %err, "failed to ensure mutations dedup table");
    }
    if let Err(err) = crate::storage::ensure_table(&pool, &db).await {
        tracing::error!(db = %db, error = %err, "committer: storage::ensure_table failed");
    }
    let ctx = CommitterCtx {
        pool,
        db,
        subs,
        schemas,
        op_feed,
        hot,
        audit_log_enabled,
        webhooks_enabled,
    };
    while let Some(req) = rx.recv().await {
        match req {
            CommitterRequest::Mutate {
                idempotency_key,
                txn,
                owner,
                reply,
            } => {
                let outcome = handle_mutate(&ctx, idempotency_key, txn, owner).await;
                let _ = reply.send(outcome);
            }
            CommitterRequest::Subscribe {
                conn,
                query_id,
                query,
                tx,
                owner,
                reply,
            } => {
                let result = handle_subscribe(&ctx, conn, query_id, *query, tx, owner).await;
                let _ = reply.send(result);
            }
            CommitterRequest::RunScheduled {
                id,
                kind,
                txn,
                cron,
            } => {
                if let Err(err) = handle_scheduled(&ctx, id, kind, *txn, cron).await {
                    tracing::error!(db = %ctx.db, error = %err, "scheduled job handling failed");
                }
            }
        }
    }
}

async fn handle_mutate(
    ctx: &CommitterCtx,
    idempotency_key: Option<String>,
    txn: Transaction,
    owner: Option<String>,
) -> Result<TxnOutcome, RtDbError> {
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
    let outcome = execute_txn(&ctx.pool, &ctx.db, &schema, &txn, owner.as_deref()).await?;
    ctx.subs
        .fan_out(&ctx.pool, &ctx.db, &schema, &outcome.write_set)
        .await;
    // Op-feed completeness: every durable document write must publish here, in handle_scheduled, or a future third site.
    ctx.op_feed
        .publish(&ctx.db, owner.as_deref(), &outcome.write_set.ops)
        .await;
    // Durable audit tap (the persistent counterpart to the op-feed above).
    // Best-effort: a logging failure is warned, never surfaced to the client —
    // the mutation has already committed and fanned out by this point.
    if ctx.audit_log_enabled
        && let Err(err) = crate::audit::write_audit_rows(
            &ctx.pool,
            &ctx.db,
            owner.as_deref(),
            "mutate",
            &outcome.write_set.ops,
        )
        .await
    {
        tracing::warn!(db = %ctx.db, error = %err, "audit log write failed");
    }
    // Webhook enqueue tap — mirrors the audit tap above: best-effort, warned on
    // failure, never surfaces to the client. The mutation has already committed
    // and fanned out by this point. `source = "mutate"` distinguishes the
    // interactive tap from the scheduled one below in delivered payloads.
    if ctx.webhooks_enabled
        && let Err(err) = crate::webhook::enqueue_for_ops(
            &ctx.pool,
            &ctx.db,
            owner.as_deref(),
            "mutate",
            &outcome.write_set.ops,
        )
        .await
    {
        tracing::warn!(db = %ctx.db, error = %err, "webhook enqueue failed");
    }

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
    match execute_txn(&ctx.pool, &ctx.db, &schema, &txn, None).await {
        Ok(outcome) => {
            ctx.subs
                .fan_out(&ctx.pool, &ctx.db, &schema, &outcome.write_set)
                .await;
            // Op-feed completeness: every durable document write must publish here, in handle_mutate, or a future third site.
            ctx.op_feed
                .publish(&ctx.db, None, &outcome.write_set.ops)
                .await;
            // Durable audit tap — scheduled jobs carry no interactive principal
            // (the op-feed publish above also passes `None`), so `principal`
            // is NULL and `source = "scheduled"`.
            if ctx.audit_log_enabled
                && let Err(err) = crate::audit::write_audit_rows(
                    &ctx.pool,
                    &ctx.db,
                    None,
                    "scheduled",
                    &outcome.write_set.ops,
                )
                .await
            {
                tracing::warn!(db = %ctx.db, error = %err, "audit log write failed");
            }
            // Webhook enqueue tap — scheduled jobs carry no interactive
            // principal (the audit tap above also passes `None`), so the
            // payload's `owner` is null and `source = "scheduled"`.
            if ctx.webhooks_enabled
                && let Err(err) = crate::webhook::enqueue_for_ops(
                    &ctx.pool,
                    &ctx.db,
                    None,
                    "scheduled",
                    &outcome.write_set.ops,
                )
                .await
            {
                tracing::warn!(db = %ctx.db, error = %err, "webhook enqueue failed");
            }
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

async fn handle_subscribe(
    ctx: &CommitterCtx,
    conn: ConnId,
    query_id: String,
    query: Query,
    tx: UnboundedSender<ServerMessage>,
    owner: Option<String>,
) -> Result<(), RtDbError> {
    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let result = execute_query(&ctx.pool, &ctx.db, &schema, &query, owner.as_deref()).await?;
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

    ctx.subs
        .register(&ctx.db, conn, query_id, query, tx, last, owner)
        .await;
    Ok(())
}
