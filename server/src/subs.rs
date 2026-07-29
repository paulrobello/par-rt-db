use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use sqlx::PgPool;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

use crate::protocol::ServerMessage;
use crate::query::{Query, canonical, execute_query};
use crate::schema::{FieldType, SchemaDef, TableDef};
use crate::txn::{DocValues, EqBind, WriteSet, eq_bind_for, eq_binds};

pub type ConnId = u64;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// Allocates a fresh, process-unique connection id.
pub fn next_conn_id() -> ConnId {
    NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed)
}

/// What a subscription's result depends on, used to skip needless re-runs.
/// Derived once from the (immutable) `Query` + the table def at registration.
#[derive(Debug, Clone)]
enum ReadSet {
    /// A `get(id)` point read: the result is exactly this one document, so a
    /// write to any other document cannot change it.
    Point { id: String },
    /// A `count`, `collect`, or `unique` query filtered on a btree index's
    /// eq-prefix (and an optional range bound on the next index field). A write
    /// to a document provably outside the window cannot change the result, so
    /// `fan_out` can skip the re-run. See `IndexedRead` for the soundness model.
    Indexed(IndexedRead),
    /// Every other shape (take / first / paginate / distinct / aggregate /
    /// search / vector / hybrid): another document can enter or leave the
    /// ordered/truncated/value-sensitive window even when its eq-prefix is
    /// unchanged, so re-run on any write to the table (today's behavior).
    Table,
}

/// The eq-prefix (+ optional range bound) window of an `Indexed`
/// subscription. `fan_out` re-runs only when a written document may have
/// crossed this window's boundary.
///
/// `eq` carries the index field name, its declared `FieldType` (so a written
/// doc's field value is typed identically to the DB's stored column), and the
/// typed bind the query pins it to. `range` is an optional inequality bound
/// (`gt`/`gte`/`lt`/`lte`) on the index field immediately after the eq-prefix
/// (`index.fields[eq.len()]`); present only when the eq-prefix is a strict
/// prefix of the index (a full-arity eq has no remaining field to range on).
///
/// `content_bearing` is true for `collect` and `unique` (return doc bodies — a
/// member's content change matters) and false for `count` (pure membership).
#[derive(Debug, Clone)]
struct IndexedRead {
    eq: Vec<(String, FieldType, EqBind)>,
    range: Option<RangeBound>,
    content_bearing: bool,
}

#[derive(Debug, Clone)]
struct RangeBound {
    field: String,
    field_type: FieldType,
    /// (value, inclusive): a `gte` bound is `(v, true)`, a `gt` bound is `(v, false)`.
    lower: Option<(EqBind, bool)>,
    /// (value, inclusive): a `lte` bound is `(v, true)`, an `lt` bound is `(v, false)`.
    upper: Option<(EqBind, bool)>,
}

impl IndexedRead {
    /// Whether `doc` falls inside this subscription's eq-prefix + range window.
    /// Pure and total: never panics. Returns `false` (outside) on a typing
    /// failure, missing field, or comparison doubt.
    ///
    /// NOTE: `false` here means fan_out will SKIP this doc (`indexed_affects`
    /// returns false), so a wrong `false` is UNDER-approximation — a missed
    /// push. The comparison path therefore requires every `EqBind` variant to
    /// have a real `cmp_binds` arm (see that function's doc); the eq-prefix
    /// path's typing fallbacks are themselves under-approximate and only safe
    /// insofar as they fire on truly untellable cases (e.g. a doc whose field
    /// has the wrong JSON type — a schema violation that should already have
    /// been rejected at write time).
    fn in_window(&self, doc: &serde_json::Map<String, serde_json::Value>) -> bool {
        // Eq prefix: AND of equalities. One miss ⇒ outside.
        for (field, ty, want) in &self.eq {
            let doc_val = doc.get(field).unwrap_or(&serde_json::Value::Null);
            let Ok(have) = eq_bind_for(ty, doc_val) else {
                return false;
            };
            if &have != want {
                return false;
            }
        }
        // Optional range bound on the field after the eq-prefix.
        if let Some(r) = &self.range {
            let doc_val = doc.get(&r.field).unwrap_or(&serde_json::Value::Null);
            let Ok(have) = eq_bind_for(&r.field_type, doc_val) else {
                return false;
            };
            if let Some((bound, inclusive)) = &r.lower
                && !satisfies_lower(&have, bound, *inclusive)
            {
                return false;
            }
            if let Some((bound, inclusive)) = &r.upper
                && !satisfies_upper(&have, bound, *inclusive)
            {
                return false;
            }
        }
        true
    }
}

/// Three-way comparison of two typed binds.
///
/// **Every `EqBind` variant MUST have an arm here.** Returns `None` only on a
/// variant mismatch (e.g. `Text` vs `Num`), which shouldn't happen in practice
/// — both binds derive from the same `FieldType` — but is defensive against a
/// bug. `Num` uses `partial_cmp` so a (theoretically impossible from JSON) NaN
/// is treated as unorderable rather than panicking.
///
/// The `_ => None` arm is NOT a soundness backstop. `None` propagates through
/// `satisfies_lower`/`satisfies_upper` as `false`, which makes `in_window`
/// return `false`, which makes `indexed_affects` return `false` for created/
/// updated docs, which makes `fan_out` SKIP the re-run (`if !affects { continue; }`).
/// That is UNDER-approximation — a missed push — and violates the committer's
/// load-bearing "never under-approximate" invariant. So adding a new `EqBind`
/// variant without a corresponding arm here is a correctness bug, not a
/// conservative fallback.
fn cmp_binds(a: &EqBind, b: &EqBind) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (EqBind::Text(x), EqBind::Text(y)) => Some(x.cmp(y)),
        (EqBind::Num(x), EqBind::Num(y)) => x.partial_cmp(y),
        (EqBind::Bool(x), EqBind::Bool(y)) => Some(x.cmp(y)),
        (EqBind::I64(x), EqBind::I64(y)) => Some(x.cmp(y)),
        _ => None,
    }
}

/// Whether `have >= bound` (inclusive) or `have > bound` (exclusive), per
/// `inclusive`. Comparison doubt ⇒ `false` (judged outside the bound — every `EqBind` variant must be orderable in `cmp_binds`, else a missed arm would under-approximate here).
fn satisfies_lower(have: &EqBind, bound: &EqBind, inclusive: bool) -> bool {
    match cmp_binds(have, bound) {
        Some(std::cmp::Ordering::Greater) => true,
        Some(std::cmp::Ordering::Equal) => inclusive,
        Some(std::cmp::Ordering::Less) | None => false,
    }
}

/// Whether `have <= bound` (inclusive) or `have < bound` (exclusive), per
/// `inclusive`. Comparison doubt ⇒ `false` (judged outside the bound — every `EqBind` variant must be orderable in `cmp_binds`, else a missed arm would under-approximate here).
fn satisfies_upper(have: &EqBind, bound: &EqBind, inclusive: bool) -> bool {
    match cmp_binds(have, bound) {
        Some(std::cmp::Ordering::Less) => true,
        Some(std::cmp::Ordering::Equal) => inclusive,
        Some(std::cmp::Ordering::Greater) | None => false,
    }
}

/// Whether a written doc's before/after affects an `Indexed` subscription, per
/// the spec's per-doc decision:
/// - deleted (after None) ⇒ `true` (values gone, must re-run).
/// - created (before None, after Some) ⇒ `in_window(after)` (entered iff now in).
/// - updated (both Some) ⇒ `content_bearing ? (in_window(before) || in_window(after))`
///   `(collect/unique — a member's body change matters)` else
///   `(in_window(before) != in_window(after))` `(count — only membership flips matter)`.
///
/// Owner filtering and `filter` are intentionally NOT consulted here: they can
/// only narrow the real result, so ignoring them over-approximates (re-runs a
/// matching-but-filtered-out or not-visible doc), never under-approximates.
fn indexed_affects(indexed: &IndexedRead, values: &DocValues) -> bool {
    match (&values.before, &values.after) {
        // after None ⇒ deleted this txn. Always re-run.
        (_, None) => true,
        // Created this txn: entered iff the new state is in window.
        (None, Some(after)) => indexed.in_window(after),
        // Updated: both states known.
        (Some(before), Some(after)) => {
            if indexed.content_bearing {
                // collect / unique: a body change to a current/past member matters.
                indexed.in_window(before) || indexed.in_window(after)
            } else {
                // count: only a membership flip (in↔out) matters.
                indexed.in_window(before) != indexed.in_window(after)
            }
        }
    }
}

impl ReadSet {
    fn from_query(query: &Query, table_def: &TableDef) -> Self {
        if let Some(id) = &query.get {
            return ReadSet::Point { id: id.clone() };
        }
        match Self::try_indexed(query, table_def) {
            Some(indexed) => ReadSet::Indexed(indexed),
            None => ReadSet::Table,
        }
    }

    /// Derive an `Indexed` window from the query + table def. Returns `None`
    /// (⇒ fall back to `Table`, today's coarse behavior) when ANY of:
    /// - the terminal is not one of `count` / `collect` (no terminal) / `unique`;
    /// - a truncating/value-sensitive/ranking terminal is set
    ///   (`take`/`first`/`paginate`/`distinct`/`aggregate`/`search`/`vector`/`hybrid`);
    /// - no `index` is declared, or it has no eq bind AND no range bound
    ///   (the window would be the whole table → no skip benefit);
    /// - the index or any eq/range value fails to type (defensive: any doubt
    ///   ⇒ `Table`, which can only over-approximate).
    ///
    /// `from_query` never panics.
    fn try_indexed(query: &Query, table_def: &TableDef) -> Option<IndexedRead> {
        // "No terminal" (collect) = none of the terminals below is set. `take`
        // is checked here too: a `take`-less collect is the eligible shape.
        let is_collect = !query.unique
            && !query.count
            && !query.first
            && !query.distinct
            && query.aggregate.is_none()
            && query.paginate.is_none()
            && query.search.is_none()
            && query.vector_search.is_none()
            && query.hybrid_search.is_none()
            && query.take.is_none();
        let eligible_terminal = query.count || query.unique || is_collect;
        if !eligible_terminal {
            return None;
        }

        // Need an index with at least one eq bind OR a range bound — otherwise
        // the window is the whole table and there is no skip to be had.
        let index_name = query.index.as_ref()?;
        let index_def = table_def.index(index_name).ok()?;
        let has_range =
            query.gt.is_some() || query.gte.is_some() || query.lt.is_some() || query.lte.is_some();
        if query.eq.is_empty() && !has_range {
            return None;
        }

        // Type the eq-prefix via the shared typer (`eq_binds`); a type mismatch
        // (e.g. a string value against a Number field) ⇒ fall back to `Table`.
        let binds = eq_binds(table_def, index_def, &query.eq).ok()?;
        let eq_len = binds.len();

        // Zip the typed binds with their (field name, FieldType) from the table.
        let mut eq: Vec<(String, FieldType, EqBind)> = Vec::with_capacity(eq_len);
        for (field_name, bind) in index_def.fields[..eq_len].iter().zip(binds) {
            let field_type = table_def.fields.get(field_name)?.clone();
            eq.push((field_name.clone(), field_type, bind));
        }

        // Optional range bound on `index.fields[eq_len]` — present only when
        // the eq-prefix is a strict prefix of the index. A full-arity eq has no
        // remaining field to range on, which is fine (`range = None`).
        let range = if eq_len < index_def.fields.len() {
            let range_field = &index_def.fields[eq_len];
            let field_type = table_def.fields.get(range_field)?.clone();
            let lower = query
                .gte
                .as_ref()
                .and_then(|v| eq_bind_for(&field_type, v).ok())
                .map(|b| (b, true))
                .or_else(|| {
                    query
                        .gt
                        .as_ref()
                        .and_then(|v| eq_bind_for(&field_type, v).ok())
                        .map(|b| (b, false))
                });
            let upper = query
                .lte
                .as_ref()
                .and_then(|v| eq_bind_for(&field_type, v).ok())
                .map(|b| (b, true))
                .or_else(|| {
                    query
                        .lt
                        .as_ref()
                        .and_then(|v| eq_bind_for(&field_type, v).ok())
                        .map(|b| (b, false))
                });
            (lower.is_some() || upper.is_some()).then_some(RangeBound {
                field: range_field.clone(),
                field_type,
                lower,
                upper,
            })
        } else {
            None
        };

        Some(IndexedRead {
            eq,
            range,
            // collect / unique return doc bodies (a member's content change
            // matters); count returns only a cardinality.
            content_bearing: query.unique || is_collect,
        })
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

/// Registered live-query subscriptions, sharded per database. Each database's
/// subscriptions live behind its own `Arc<Mutex<DbSubs>>`, so a per-db
/// committer's `fan_out` (a Postgres re-run per affected subscription) holds
/// ONLY its own shard lock — it does not collide with other databases' writes,
/// subscribes, or teardowns. The outer `Mutex<HashMap<..>>` is acquired only
/// long enough to look up or insert the target shard's `Arc` (an in-memory
/// op with no I/O), then dropped before any per-shard lock is taken.
///
/// `register` and `fan_out` are called only from the per-db committer task
/// (see `committer.rs`), which serializes them against every mutation;
/// `remove`/`remove_conn` may be called from anywhere (e.g. connection
/// teardown).
///
/// Lock discipline: the outer lock is NEVER held while waiting on an inner
/// shard lock — every path drops outer before taking inner, so there is no
/// lock-ordering cycle. Empty shards are RETAINED (lazy) rather than evicted
/// under a second lock acquire: evicting a now-empty shard would race a
/// concurrent `register` that already cloned this same Arc (before the
/// eviction) and would orphan its subscription — `fan_out` would no longer
/// find the shard. Shard count is bounded by the number of databases (each
/// persistent), and an empty shard is a single empty HashMap, so retaining
/// them costs nothing measurable.
pub struct SubscriptionManager {
    subs: Mutex<HashMap<String, Arc<Mutex<DbSubs>>>>,
}

impl SubscriptionManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            subs: Mutex::new(HashMap::new()),
        })
    }

    /// Get-or-insert the shard for `db` (used by `register`). The outer lock
    /// is held only across the entry API; the returned `Arc` clones out.
    async fn shard_insert(&self, db: &str) -> Arc<Mutex<DbSubs>> {
        let mut guard = self.subs.lock().await;
        guard
            .entry(db.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(HashMap::new())))
            .clone()
    }

    /// Clone the shard `Arc` for `db` if it exists (used by `remove`,
    /// `remove_conn`, `fan_out`). The outer lock is held only across the get.
    async fn shard_get(&self, db: &str) -> Option<Arc<Mutex<DbSubs>>> {
        let guard = self.subs.lock().await;
        guard.get(db).cloned()
    }

    pub async fn remove(&self, db: &str, conn: ConnId, query_id: &str) {
        let Some(shard) = self.shard_get(db).await else {
            return;
        };
        let mut db_subs = shard.lock().await;
        db_subs.remove(&(conn, query_id.to_string()));
        // Empty shard is retained (lazy) — see the struct doc. Evicting here
        // would orphan a concurrently-registered subscription.
    }

    pub async fn remove_conn(&self, db: &str, conn: ConnId) {
        let Some(shard) = self.shard_get(db).await else {
            return;
        };
        let mut db_subs = shard.lock().await;
        db_subs.retain(|(c, _), _| *c != conn);
        // Empty shard is retained (lazy) — see the struct doc.
    }

    /// Drops every subscription for `db` and removes its shard, used by
    /// `delete-db` to evict a deleted database's live-query state. Live
    /// `/sync` connections to `db` will see errors on their next op (the
    /// schema is gone), which is acceptable for a deleted database. Unlike
    /// `remove`/`remove_conn`, this drops the shard Arc entirely — fine here
    /// because no future `register` will target a deleted db (the next
    /// `submit` for it 404s at `database_exists` before reaching the committer).
    pub async fn drop_db(&self, db: &str) {
        let mut guard = self.subs.lock().await;
        guard.remove(db);
    }

    /// Total active subscriptions across all databases (a dashboard gauge).
    /// Approximate by design — each shard is locked individually after the
    /// outer map is released, so a subscribe/unsubscribe racing this call can
    /// shift the count by one. That is acceptable for a metrics gauge and
    /// avoids holding the outer lock while waiting on every shard's inner
    /// lock (which would re-introduce the global serialization ARC-001
    /// removed).
    pub async fn count(&self) -> usize {
        let shards: Vec<Arc<Mutex<DbSubs>>> = {
            let guard = self.subs.lock().await;
            guard.values().cloned().collect()
        };
        let mut total = 0;
        for shard in shards {
            total += shard.lock().await.len();
        }
        total
    }

    /// Registers a subscription that has already sent its initial
    /// `QueryUpdate` with `last` as the canonical form of that initial result.
    /// Called only by the committer task, immediately after the initial send,
    /// so no fan-out between execute and register can be missed. `owner` is
    /// the subscriber's per-row auth identity (see `SubEntry::owner`).
    /// `table_def` resolves the query's index/field types so an `Indexed`
    /// ReadSet can be derived for fine-grained invalidation (v2); a `get` query
    /// ignores it. The def is borrowed only for derivation — the stored
    /// `ReadSet` owns its binds, so the subscription survives a later schema
    /// evolution (an `Indexed` referencing a since-changed field biases to
    /// re-run via `in_window`'s "any doubt ⇒ outside" rule).
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
        table_def: &TableDef,
    ) {
        let read_set = ReadSet::from_query(&query, table_def);
        let shard = self.shard_insert(db).await;
        let mut db_subs = shard.lock().await;
        db_subs.insert(
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
        // Clone the shard Arc out under the outer lock, then drop the outer
        // guard before the per-subscription re-runs. This is the heart of
        // ARC-001: the re-runs (each a Postgres round-trip) hold only this
        // db's shard lock, never the global map lock, so a slow `fan_out` on
        // db A cannot stall writes/subscribes/teardowns on db B.
        let Some(shard) = self.shard_get(db).await else {
            return;
        };
        let mut db_subs = shard.lock().await;

        for ((_, query_id), entry) in db_subs.iter_mut() {
            if !write_set.tables.contains(&entry.query.table) {
                continue;
            }

            // A `get(id)` point read depends only on its one document, so a
            // write that didn't touch it cannot change the result — skip the
            // re-run.
            if let ReadSet::Point { id } = &entry.read_set
                && !write_set
                    .docs
                    .contains(&(entry.query.table.clone(), id.clone()))
            {
                continue;
            }

            // An `Indexed` (count / collect / unique on an eq-prefix window)
            // subscription needs to re-run only when a written document on its
            // table CROSSED the window boundary (per `indexed_affects`). If
            // every written doc is provably irrelevant, skip the re-run. This
            // is the v2 win: writes to documents outside the window no longer
            // trigger a needless Postgres round-trip + canonical diff. Sound
            // only because every `EqBind` variant has a real `cmp_binds` arm —
            // `in_window` returns false on comparison doubt, which would
            // UNDER-approximate (skip a real push), so the typing layer must
            // never return `false` on a value that's actually inside the window.
            if let ReadSet::Indexed(indexed) = &entry.read_set {
                let table = &entry.query.table;
                let mut affects = false;
                for (doc_table, doc_id) in &write_set.docs {
                    if doc_table != table {
                        continue;
                    }
                    match write_set
                        .doc_values
                        .get(&(doc_table.clone(), doc_id.clone()))
                    {
                        // A written `(table,id)` missing from `doc_values`
                        // (shouldn't happen — every `touch` site also captures)
                        // ⇒ treat as affecting so we never miss a push.
                        None => {
                            affects = true;
                            break;
                        }
                        Some(values) => {
                            if indexed_affects(indexed, values) {
                                affects = true;
                                break;
                            }
                        }
                    }
                }
                if !affects {
                    continue;
                }
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
    use crate::schema::{IndexDef, VectorIndexSpec};
    use std::collections::BTreeMap;

    fn q(value: serde_json::Value) -> Query {
        serde_json::from_value(value).expect("parse query")
    }

    /// A table with string/number/boolean indexable fields and a couple of
    /// multi-field indexes — enough to exercise eq-prefix + range derivation
    /// and `in_window` typing across all three indexable scalar types.
    fn test_table_def() -> TableDef {
        TableDef {
            fields: BTreeMap::from([
                ("status".to_string(), FieldType::String),
                ("order".to_string(), FieldType::Number),
                ("flag".to_string(), FieldType::Boolean),
                ("label".to_string(), FieldType::String),
            ]),
            indexes: vec![
                IndexDef {
                    name: "by_status".to_string(),
                    fields: vec!["status".to_string()],
                    search: false,
                    vector: None,
                },
                IndexDef {
                    name: "by_status_order".to_string(),
                    fields: vec!["status".to_string(), "order".to_string()],
                    search: false,
                    vector: None,
                },
                IndexDef {
                    name: "by_flag".to_string(),
                    fields: vec!["flag".to_string()],
                    search: false,
                    vector: None,
                },
            ],
            owner_field: None,
            collaborators_field: None,
        }
    }

    // Suppress the unused warning for VectorIndexSpec: it's part of IndexDef's
    // construction shape (search/vector defaults) but the test table is btree-only.
    #[allow(dead_code)]
    fn _unused_vector_spec() -> VectorIndexSpec {
        VectorIndexSpec {
            dimensions: 1,
            filter_fields: vec![],
        }
    }

    #[test]
    fn get_query_is_a_point_read() {
        let query = q(serde_json::json!({ "table": "t", "get": "abc" }));
        assert!(matches!(
            ReadSet::from_query(&query, &test_table_def()),
            ReadSet::Point { id } if id == "abc"
        ));
    }

    #[test]
    fn non_indexed_queries_are_table_level() {
        let td = test_table_def();
        // Truncating / value-sensitive / ranking terminals stay Table even with
        // an index + eq — their result depends on more than window membership.
        let cases = [
            serde_json::json!({ "table": "t", "take": 5 }), // take
            serde_json::json!({ "table": "t", "index": "by_status", "eq": ["x"], "take": 5 }),
            serde_json::json!({ "table": "t", "index": "by_status", "eq": ["x"], "first": true }),
            serde_json::json!({ "table": "t", "index": "by_status", "eq": ["x"], "paginate": { "numItems": 10 } }),
            serde_json::json!({ "table": "t", "index": "by_status", "eq": ["x"], "distinct": true }),
            serde_json::json!({ "table": "t", "index": "by_status_order", "eq": ["x"], "aggregate": { "op": "sum" } }),
            // No index + no eq ⇒ window is the whole table ⇒ no skip benefit.
            serde_json::json!({ "table": "t" }),
            serde_json::json!({ "table": "t", "count": true }),
            // Index but no eq and no range ⇒ whole-table window again.
            serde_json::json!({ "table": "t", "index": "by_status", "count": true }),
        ];
        for case in cases {
            let query = q(case);
            assert!(
                matches!(ReadSet::from_query(&query, &td), ReadSet::Table),
                "expected Table-level for {:?}",
                query
            );
        }
    }

    #[test]
    fn count_collect_unique_on_eq_prefix_derive_indexed() {
        let td = test_table_def();
        // count → Indexed, content_bearing=false
        let query = q(
            serde_json::json!({ "table": "t", "index": "by_status", "eq": ["backlog"], "count": true }),
        );
        match ReadSet::from_query(&query, &td) {
            ReadSet::Indexed(idx) => {
                assert_eq!(idx.eq.len(), 1);
                assert_eq!(idx.eq[0].0, "status");
                assert!(matches!(idx.eq[0].1, FieldType::String));
                assert!(matches!(idx.eq[0].2, EqBind::Text(ref s) if s == "backlog"));
                assert!(!idx.content_bearing);
                assert!(idx.range.is_none());
            }
            other => panic!("count+eq should be Indexed, got {other:?}"),
        }

        // collect (no terminal) → Indexed, content_bearing=true
        let query = q(serde_json::json!({ "table": "t", "index": "by_status", "eq": ["backlog"] }));
        match ReadSet::from_query(&query, &td) {
            ReadSet::Indexed(idx) => assert!(idx.content_bearing),
            other => panic!("collect+eq should be Indexed, got {other:?}"),
        }

        // unique → Indexed, content_bearing=true
        let query = q(
            serde_json::json!({ "table": "t", "index": "by_status", "eq": ["backlog"], "unique": true }),
        );
        match ReadSet::from_query(&query, &td) {
            ReadSet::Indexed(idx) => assert!(idx.content_bearing),
            other => panic!("unique+eq should be Indexed, got {other:?}"),
        }
    }

    #[test]
    fn range_bound_is_captured_when_present() {
        let td = test_table_def();
        // collect with eq=[status] + gte on order (the field after the eq prefix).
        let query = q(
            serde_json::json!({ "table": "t", "index": "by_status_order", "eq": ["backlog"], "gte": 10 }),
        );
        match ReadSet::from_query(&query, &td) {
            ReadSet::Indexed(idx) => {
                assert_eq!(idx.eq.len(), 1);
                let r = idx.range.expect("range bound present");
                assert_eq!(r.field, "order");
                assert!(matches!(r.field_type, FieldType::Number));
                // gte ⇒ lower inclusive
                let (lower, inc) = r.lower.expect("lower bound");
                assert!(matches!(lower, EqBind::Num(n) if n == 10.0));
                assert!(inc);
                assert!(r.upper.is_none());
            }
            other => panic!("collect+eq+range should be Indexed, got {other:?}"),
        }

        // lt ⇒ upper exclusive
        let query = q(
            serde_json::json!({ "table": "t", "index": "by_status_order", "eq": ["backlog"], "lt": 100 }),
        );
        match ReadSet::from_query(&query, &td) {
            ReadSet::Indexed(idx) => {
                let r = idx.range.expect("range");
                let (upper, inc) = r.upper.expect("upper");
                assert!(matches!(upper, EqBind::Num(n) if n == 100.0));
                assert!(!inc);
                assert!(r.lower.is_none());
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn range_only_no_eq_derives_indexed() {
        let td = test_table_def();
        // eq=[] + a range bound on index.fields[0]. Eligible per the spec
        // ("eq non-empty OR a range bound present").
        let query = q(serde_json::json!({ "table": "t", "index": "by_status_order", "gte": "m" }));
        match ReadSet::from_query(&query, &td) {
            ReadSet::Indexed(idx) => {
                assert!(idx.eq.is_empty());
                let r = idx.range.expect("range");
                assert_eq!(r.field, "status");
            }
            other => panic!("range-only should be Indexed, got {other:?}"),
        }
    }

    #[test]
    fn wrong_typed_eq_value_falls_back_to_table() {
        let td = test_table_def();
        // eq value "not-a-number" against a Number field — typing fails ⇒ Table.
        let query = q(
            serde_json::json!({ "table": "t", "index": "by_status_order", "eq": ["backlog", "oops"] }),
        );
        assert!(matches!(ReadSet::from_query(&query, &td), ReadSet::Table));
    }

    // ---- in_window unit cases (spec test #8: null/wrong-typed/missing ⇒ false) ----

    fn indexed_status_eq(status: &str, content_bearing: bool) -> IndexedRead {
        IndexedRead {
            eq: vec![(
                "status".to_string(),
                FieldType::String,
                EqBind::Text(status.to_string()),
            )],
            range: None,
            content_bearing,
        }
    }

    #[test]
    fn in_window_matching_eq_returns_true() {
        let idx = indexed_status_eq("backlog", true);
        let doc = serde_json::json!({ "status": "backlog" })
            .as_object()
            .unwrap()
            .clone();
        assert!(idx.in_window(&doc));
    }

    #[test]
    fn in_window_non_matching_eq_returns_false() {
        let idx = indexed_status_eq("backlog", true);
        let doc = serde_json::json!({ "status": "done" })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.in_window(&doc));
    }

    #[test]
    fn in_window_missing_field_returns_false() {
        let idx = indexed_status_eq("backlog", true);
        let doc = serde_json::json!({ "other": "x" })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.in_window(&doc));
    }

    #[test]
    fn in_window_null_field_returns_false() {
        let idx = indexed_status_eq("backlog", true);
        let doc = serde_json::json!({ "status": null })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.in_window(&doc));
    }

    #[test]
    fn in_window_wrong_typed_field_returns_false() {
        let idx = indexed_status_eq("backlog", true);
        // status declared String; doc carries a number ⇒ typing fails ⇒ outside.
        let doc = serde_json::json!({ "status": 5 })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.in_window(&doc));
    }

    #[test]
    fn in_window_range_satisfied_and_not() {
        let idx = IndexedRead {
            eq: vec![(
                "status".to_string(),
                FieldType::String,
                EqBind::Text("backlog".to_string()),
            )],
            range: Some(RangeBound {
                field: "order".to_string(),
                field_type: FieldType::Number,
                lower: Some((EqBind::Num(10.0), true)), // gte 10
                upper: Some((EqBind::Num(20.0), false)), // lt 20
            }),
            content_bearing: true,
        };
        // In range.
        let doc = serde_json::json!({ "status": "backlog", "order": 15 })
            .as_object()
            .unwrap()
            .clone();
        assert!(idx.in_window(&doc));
        // Below lower bound.
        let doc = serde_json::json!({ "status": "backlog", "order": 5 })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.in_window(&doc));
        // At lower bound (inclusive) — in.
        let doc = serde_json::json!({ "status": "backlog", "order": 10 })
            .as_object()
            .unwrap()
            .clone();
        assert!(idx.in_window(&doc));
        // At upper bound (exclusive) — out.
        let doc = serde_json::json!({ "status": "backlog", "order": 20 })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.in_window(&doc));
        // Above upper bound.
        let doc = serde_json::json!({ "status": "backlog", "order": 25 })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.in_window(&doc));
    }

    #[test]
    fn in_window_range_null_order_field_returns_false() {
        let idx = IndexedRead {
            eq: vec![(
                "status".to_string(),
                FieldType::String,
                EqBind::Text("backlog".to_string()),
            )],
            range: Some(RangeBound {
                field: "order".to_string(),
                field_type: FieldType::Number,
                lower: Some((EqBind::Num(10.0), true)),
                upper: None,
            }),
            content_bearing: true,
        };
        // order absent ⇒ doc.get returns Null ⇒ typing fails ⇒ outside.
        let doc = serde_json::json!({ "status": "backlog" })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.in_window(&doc));
    }

    #[test]
    fn in_window_boolean_eq_matches() {
        let idx = IndexedRead {
            eq: vec![("flag".to_string(), FieldType::Boolean, EqBind::Bool(true))],
            range: None,
            content_bearing: false,
        };
        let doc = serde_json::json!({ "flag": true })
            .as_object()
            .unwrap()
            .clone();
        assert!(idx.in_window(&doc));
        let doc = serde_json::json!({ "flag": false })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.in_window(&doc));
    }

    // ---- indexed_affects per-doc decision (spec's fan_out table) ----

    fn doc_with_status(status: &str) -> serde_json::Map<String, serde_json::Value> {
        serde_json::json!({ "status": status })
            .as_object()
            .unwrap()
            .clone()
    }

    #[test]
    fn affects_deleted_is_always_true() {
        let idx = indexed_status_eq("backlog", false);
        let values = DocValues {
            before: Some(doc_with_status("backlog")),
            after: None,
        };
        assert!(indexed_affects(&idx, &values));
        // Even a delete of a doc that was never in window still re-runs
        // (values gone ⇒ conservative re-run).
        let values = DocValues {
            before: Some(doc_with_status("done")),
            after: None,
        };
        assert!(indexed_affects(&idx, &values));
    }

    #[test]
    fn affects_created_in_window_true_outside_false() {
        let idx = indexed_status_eq("backlog", false);
        let in_win = DocValues {
            before: None,
            after: Some(doc_with_status("backlog")),
        };
        assert!(indexed_affects(&idx, &in_win));
        let out_win = DocValues {
            before: None,
            after: Some(doc_with_status("done")),
        };
        assert!(!indexed_affects(&idx, &out_win));
    }

    #[test]
    fn affects_count_only_membership_flip() {
        let idx = indexed_status_eq("backlog", false); // count
        // stayed outside — no affect
        let stayed_out = DocValues {
            before: Some(doc_with_status("done")),
            after: Some(doc_with_status("done")),
        };
        assert!(!indexed_affects(&idx, &stayed_out));
        // body-only change to a member (eq unchanged) — count unaffected
        let member_body_change = DocValues {
            before: Some(doc_with_status("backlog")),
            after: Some(doc_with_status("backlog")),
        };
        assert!(!indexed_affects(&idx, &member_body_change));
        // entered (done → backlog) — count increased
        let entered = DocValues {
            before: Some(doc_with_status("done")),
            after: Some(doc_with_status("backlog")),
        };
        assert!(indexed_affects(&idx, &entered));
        // left (backlog → done) — count decreased (regression guard for `before`)
        let left = DocValues {
            before: Some(doc_with_status("backlog")),
            after: Some(doc_with_status("done")),
        };
        assert!(indexed_affects(&idx, &left));
    }

    #[test]
    fn affects_collect_is_content_bearing() {
        let idx = indexed_status_eq("backlog", true); // collect
        // body-only change to a member — collect re-runs (content matters)
        let member_body_change = DocValues {
            before: Some(doc_with_status("backlog")),
            after: Some(doc_with_status("backlog")),
        };
        assert!(indexed_affects(&idx, &member_body_change));
        // stayed outside — no affect
        let stayed_out = DocValues {
            before: Some(doc_with_status("done")),
            after: Some(doc_with_status("done")),
        };
        assert!(!indexed_affects(&idx, &stayed_out));
    }
}
