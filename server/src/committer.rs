use std::collections::HashMap;
use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Mutex, mpsc, oneshot};

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
    channels: Mutex<HashMap<String, mpsc::Sender<CommitterRequest>>>,
}

impl Committers {
    pub fn new(
        pool: PgPool,
        subs: Arc<SubscriptionManager>,
        schemas: SchemaCache,
        op_feed: Arc<crate::op_feed::OpFeed>,
    ) -> Self {
        Self {
            pool,
            subs,
            schemas,
            op_feed,
            channels: Mutex::new(HashMap::new()),
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
            rx,
        ));
        tokio::spawn(scheduler::run_scheduler(
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
/// database name, and the schema/subscription state, bundled to keep the
/// per-request handlers' argument lists small.
struct CommitterCtx {
    pool: PgPool,
    db: String,
    subs: Arc<SubscriptionManager>,
    schemas: SchemaCache,
    // Consumed in Task 3 (`ctx.op_feed.publish(...)`); remove the allow when that lands.
    #[allow(dead_code)]
    op_feed: Arc<crate::op_feed::OpFeed>,
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
async fn run_committer(
    pool: PgPool,
    db: String,
    subs: Arc<SubscriptionManager>,
    schemas: SchemaCache,
    op_feed: Arc<crate::op_feed::OpFeed>,
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

    if let Some(key) = &idempotency_key {
        // The mutation already committed and fanned out by this point — a
        // caching failure here must never turn a successful write into a
        // client-visible error. Best-effort: log and move on. (A retry with
        // this key will simply re-execute, same as if it had never cached.)
        if let Err(err) = mutation_log::store(
            &ctx.pool,
            &ctx.db,
            key,
            &outcome.results,
            mutation_log::DEDUP_TTL_MS,
        )
        .await
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
    let value = serde_json::to_value(&result).unwrap_or_else(|err| {
        tracing::error!(error = %err, "failed to serialize initial query result");
        serde_json::Value::Null
    });

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
