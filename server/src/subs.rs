use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use sqlx::PgPool;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

use crate::protocol::ServerMessage;
use crate::query::{Query, canonical, execute_query};
use crate::schema::SchemaDef;
use crate::txn::WriteSet;

pub type ConnId = u64;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// Allocates a fresh, process-unique connection id.
pub fn next_conn_id() -> ConnId {
    NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed)
}

struct SubEntry {
    query: Query,
    tx: UnboundedSender<ServerMessage>,
    last: String,
}

/// One database's subscriptions, keyed by `(connection, queryId)`.
type DbSubs = HashMap<(ConnId, String), SubEntry>;

/// Registered live-query subscriptions, keyed by database then by
/// `(connection, queryId)`. `register` and `fan_out` are called only from the
/// per-db committer task (see `committer.rs`), which serializes them against
/// every mutation; `remove`/`remove_conn` may be called from anywhere (e.g.
/// connection teardown).
pub struct SubscriptionManager {
    subs: Mutex<HashMap<String, DbSubs>>,
}

impl SubscriptionManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            subs: Mutex::new(HashMap::new()),
        })
    }

    pub async fn remove(&self, db: &str, conn: ConnId, query_id: &str) {
        let mut guard = self.subs.lock().await;
        if let Some(db_subs) = guard.get_mut(db) {
            db_subs.remove(&(conn, query_id.to_string()));
            if db_subs.is_empty() {
                guard.remove(db);
            }
        }
    }

    pub async fn remove_conn(&self, db: &str, conn: ConnId) {
        let mut guard = self.subs.lock().await;
        if let Some(db_subs) = guard.get_mut(db) {
            db_subs.retain(|(c, _), _| *c != conn);
            if db_subs.is_empty() {
                guard.remove(db);
            }
        }
    }

    /// Registers a subscription that has already sent its initial
    /// `QueryUpdate` with `last` as the canonical form of that initial result.
    /// Called only by the committer task, immediately after the initial send,
    /// so no fan-out between execute and register can be missed.
    pub(crate) async fn register(
        &self,
        db: &str,
        conn: ConnId,
        query_id: String,
        query: Query,
        tx: UnboundedSender<ServerMessage>,
        last: String,
    ) {
        let mut guard = self.subs.lock().await;
        guard
            .entry(db.to_string())
            .or_default()
            .insert((conn, query_id), SubEntry { query, tx, last });
    }

    /// Re-runs every subscription on `db` whose query table is in
    /// `write_set`, pushing a `QueryUpdate` only when the canonical result
    /// changed. A subscriber whose re-run errors (e.g. an evolved schema) is
    /// logged and skipped, never fails the caller. Send errors (receiver
    /// dropped) are ignored; connection teardown is expected to call
    /// `remove_conn` separately.
    pub(crate) async fn fan_out(
        &self,
        pool: &PgPool,
        db: &str,
        schema: &SchemaDef,
        write_set: &WriteSet,
    ) {
        let mut guard = self.subs.lock().await;
        let Some(db_subs) = guard.get_mut(db) else {
            return;
        };

        for ((_, query_id), entry) in db_subs.iter_mut() {
            if !write_set.tables.contains(&entry.query.table) {
                continue;
            }

            let result = match execute_query(pool, db, schema, &entry.query).await {
                Ok(result) => result,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        db,
                        query_id,
                        "subscription re-run failed; skipping"
                    );
                    continue;
                }
            };

            let canon = canonical(&result);
            if canon == entry.last {
                continue;
            }

            let value = match serde_json::to_value(&result) {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        db,
                        query_id,
                        "failed to serialize query result; skipping"
                    );
                    continue;
                }
            };

            entry.last = canon;
            let _ = entry.tx.send(ServerMessage::QueryUpdate {
                query_id: query_id.clone(),
                result: value,
            });
        }
    }
}
