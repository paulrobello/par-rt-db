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

/// What a subscription's result depends on, used to skip needless re-runs.
/// Derived once from the (immutable) `Query` at registration.
#[derive(Debug, Clone)]
enum ReadSet {
    /// A `get(id)` point read: the result is exactly this one document, so a
    /// write to any other document cannot change it.
    Point { id: String },
    /// Every other shape (take / collect / count / paginate / unique / first /
    /// search / vector): another document can enter the result, so re-run on
    /// any write to the table (today's behavior).
    Table,
}

impl ReadSet {
    fn from_query(query: &Query) -> Self {
        match &query.get {
            Some(id) => ReadSet::Point { id: id.clone() },
            None => ReadSet::Table,
        }
    }
}

struct SubEntry {
    query: Query,
    tx: UnboundedSender<ServerMessage>,
    last: String,
    read_set: ReadSet,
    /// The subscriber's per-row auth identity, captured at subscribe time.
    /// `None` = bypass (machine tokens / scheduled jobs); `Some(user_id)` =
    /// re-run this subscription's query filtered to that user's rows.
    owner: Option<String>,
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

    /// Total active subscriptions across all databases (a dashboard gauge).
    pub async fn count(&self) -> usize {
        let guard = self.subs.lock().await;
        guard.values().map(|db_subs| db_subs.len()).sum()
    }

    /// Registers a subscription that has already sent its initial
    /// `QueryUpdate` with `last` as the canonical form of that initial result.
    /// Called only by the committer task, immediately after the initial send,
    /// so no fan-out between execute and register can be missed. `owner` is
    /// the subscriber's per-row auth identity (see `SubEntry::owner`).
    // Each arg is independently required by the committer's register path;
    // bundling them into a context struct would add indirection without
    // reducing coupling (same call as `ws::handle_text_frame`).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn register(
        &self,
        db: &str,
        conn: ConnId,
        query_id: String,
        query: Query,
        tx: UnboundedSender<ServerMessage>,
        last: String,
        owner: Option<String>,
    ) {
        let read_set = ReadSet::from_query(&query);
        let mut guard = self.subs.lock().await;
        guard.entry(db.to_string()).or_default().insert(
            (conn, query_id),
            SubEntry {
                query,
                tx,
                last,
                read_set,
                owner,
            },
        );
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

            // A `get(id)` point read depends only on its one document, so a
            // write that didn't touch it cannot change the result — skip the
            // re-run. Every other shape stays table-level (re-runs below).
            if let ReadSet::Point { id } = &entry.read_set
                && !write_set
                    .docs
                    .contains(&(entry.query.table.clone(), id.clone()))
            {
                continue;
            }

            let result =
                match execute_query(pool, db, schema, &entry.query, entry.owner.as_deref()).await {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn q(value: serde_json::Value) -> Query {
        serde_json::from_value(value).expect("parse query")
    }

    #[test]
    fn get_query_is_a_point_read() {
        let query = q(serde_json::json!({ "table": "t", "get": "abc" }));
        assert!(matches!(
            ReadSet::from_query(&query),
            ReadSet::Point { id } if id == "abc"
        ));
    }

    #[test]
    fn non_get_queries_are_table_level() {
        let cases = [
            serde_json::json!({ "table": "t" }),            // collect
            serde_json::json!({ "table": "t", "take": 5 }), // take
            serde_json::json!({ "table": "t", "index": "by_x", "eq": ["v"] }), // eq
            serde_json::json!({ "table": "t", "index": "by_x", "eq": ["v"], "unique": true }), // unique
            serde_json::json!({ "table": "t", "first": true }), // first
            serde_json::json!({ "table": "t", "count": true }), // count
        ];
        for case in cases {
            let query = q(case);
            assert!(
                matches!(ReadSet::from_query(&query), ReadSet::Table),
                "non-get query must be Table-level"
            );
        }
    }
}
