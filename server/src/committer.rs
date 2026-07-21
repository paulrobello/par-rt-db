use std::collections::HashMap;
use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::db::{SchemaCache, database_exists};
use crate::error::RtDbError;
use crate::protocol::ServerMessage;
use crate::query::{Query, canonical, execute_query};
use crate::subs::{ConnId, SubscriptionManager};
use crate::txn::{Transaction, TxnOutcome, execute_txn};

/// Bound on each per-db committer task's inbox.
const CHANNEL_BUFFER: usize = 64;

pub enum CommitterRequest {
    Mutate {
        txn: Transaction,
        reply: oneshot::Sender<Result<TxnOutcome, RtDbError>>,
    },
    Subscribe {
        conn: ConnId,
        query_id: String,
        query: Query,
        tx: UnboundedSender<ServerMessage>,
        reply: oneshot::Sender<Result<(), RtDbError>>,
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
    channels: Mutex<HashMap<String, mpsc::Sender<CommitterRequest>>>,
}

impl Committers {
    pub fn new(pool: PgPool, subs: Arc<SubscriptionManager>, schemas: SchemaCache) -> Self {
        Self {
            pool,
            subs,
            schemas,
            channels: Mutex::new(HashMap::new()),
        }
    }

    /// Submits a request to `db`'s committer task, lazily spawning it on
    /// first use. Errors `NotFound` if `db` isn't a registered database. If
    /// the send fails because the committer task is gone (e.g. it panicked),
    /// evicts `db`'s stale sender from `channels` before returning the
    /// error, so the next request respawns a fresh task instead of every
    /// future request to `db` failing forever.
    pub async fn submit(&self, db: &str, req: CommitterRequest) -> Result<(), RtDbError> {
        let sender = self.channel_for(db).await?;
        if sender.send(req).await.is_err() {
            self.channels.lock().await.remove(db);
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
            rx,
        ));
        guard.insert(db.to_string(), tx.clone());
        Ok(tx)
    }

    /// Executes `txn` on `db` and waits for the fan-out-then-reply cycle to
    /// complete.
    pub async fn mutate(&self, db: &str, txn: Transaction) -> Result<TxnOutcome, RtDbError> {
        let (reply, reply_rx) = oneshot::channel();
        self.submit(db, CommitterRequest::Mutate { txn, reply })
            .await?;
        reply_rx
            .await
            .map_err(|_| RtDbError::internal("committer task dropped the reply"))?
    }

    /// Runs `query` on `db`, sends the initial result on `tx`, and registers
    /// the subscription for future push-on-change updates.
    pub async fn subscribe(
        &self,
        db: &str,
        conn: ConnId,
        query_id: String,
        query: Query,
        tx: UnboundedSender<ServerMessage>,
    ) -> Result<(), RtDbError> {
        let (reply, reply_rx) = oneshot::channel();
        self.submit(
            db,
            CommitterRequest::Subscribe {
                conn,
                query_id,
                query,
                tx,
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
    mut rx: mpsc::Receiver<CommitterRequest>,
) {
    let ctx = CommitterCtx {
        pool,
        db,
        subs,
        schemas,
    };
    while let Some(req) = rx.recv().await {
        match req {
            CommitterRequest::Mutate { txn, reply } => {
                let outcome = handle_mutate(&ctx, txn).await;
                let _ = reply.send(outcome);
            }
            CommitterRequest::Subscribe {
                conn,
                query_id,
                query,
                tx,
                reply,
            } => {
                let result = handle_subscribe(&ctx, conn, query_id, query, tx).await;
                let _ = reply.send(result);
            }
        }
    }
}

async fn handle_mutate(ctx: &CommitterCtx, txn: Transaction) -> Result<TxnOutcome, RtDbError> {
    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let outcome = execute_txn(&ctx.pool, &ctx.db, &schema, &txn).await?;
    ctx.subs
        .fan_out(&ctx.pool, &ctx.db, &schema, &outcome.write_set)
        .await;
    Ok(outcome)
}

async fn handle_subscribe(
    ctx: &CommitterCtx,
    conn: ConnId,
    query_id: String,
    query: Query,
    tx: UnboundedSender<ServerMessage>,
) -> Result<(), RtDbError> {
    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let result = execute_query(&ctx.pool, &ctx.db, &schema, &query).await?;
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
        .register(&ctx.db, conn, query_id, query, tx, last)
        .await;
    Ok(())
}
