//! Client-side optimistic-update projection. Pure: given a query, its last
//! authoritative result, and a transaction, produce the projected result to
//! overlay immediately (before the server round-trip), or `Skip` when the
//! effect is ambiguous. Ports `ts-client/src/optimistic.ts` (conservative:
//! only unambiguous cases overlay).
//!
//! The reactive `RtDbClient` caches each subscription's last result and holds
//! neither a schema nor a table store, so an overlay can only be computed from
//! the documents already in that cached result. This module mirrors the
//! server/in-memory DSL semantics for the cases where the effect on the result
//! set is unambiguous from those documents alone, and declines to guess
//! everywhere else: a wrong overlay is worse than a brief wait for the
//! authoritative `queryUpdate`.
//!
//! `now: i64` (epoch-millis) is taken as a parameter so this function is pure
//! and clock-free.
//!
//! Canonical no-op detection: `serde_json` is depended on with its default
//! features only (no `preserve_order`), so its `Map` is `BTreeMap`-backed and
//! `Value::eq` is canonical (key order is already sorted). If `preserve_order`
//! is ever enabled, replace the direct equality in `finalize`/`project_get`
//! with a key-sorting canonicalizer.
use std::sync::atomic::{AtomicU64, Ordering};

use serde_json::{Map, Value};

use crate::mutation::{Step, Transaction};
use crate::query::Query;

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A clearly-branded temporary id for an optimistically-inserted doc (replaced
/// on reconcile with the server-assigned id).
fn synthetic_id() -> String {
    format!("__optimistic__{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

/// Outcome of projecting a transaction onto a subscription's last result.
#[derive(Debug)]
pub enum OptimisticProjection {
    /// Do not overlay (no-op or ambiguous effect).
    Skip,
    /// Overlay this value immediately.
    Overlaid(Value),
}

/// Project `txn` onto `last` (the cached result for `query`).
///
/// Routes to one of three shapes — unfiltered array, filtered array
/// (delete-only), or `get(id)` point-read — or returns `Skip` when the query
/// shape or step kind makes the effect ambiguous.
pub fn project_optimistic_update(
    query: &Query,
    last: &Value,
    txn: &Transaction,
    now: i64,
) -> OptimisticProjection {
    if query.get.is_some() {
        return project_get(query, last, txn);
    }
    if !is_array_query(query) {
        return OptimisticProjection::Skip;
    }
    if has_filter(query) {
        project_filtered_array(query, last, txn)
    } else {
        project_unfiltered_array(query, last, txn, now)
    }
}

/// `get`-point-read, `unique`/`first`/`count`/`distinct`/`paginate`, and the
/// `search`/`vector_search` terminals are non-array shapes (or rank-based)
/// whose result we cannot project from cached documents alone. A `filter`
/// predicate is NOT excluded here: a filtered collect is still an array read,
/// just one whose membership we cannot evaluate (handled by `has_filter`).
fn is_array_query(q: &Query) -> bool {
    q.get.is_none()
        && !q.unique
        && !q.first
        && !q.count
        && !q.distinct
        && q.paginate.is_none()
        && q.search.is_none()
        && q.vector_search.is_none()
}

/// A query whose result membership depends on a predicate we cannot evaluate
/// without the schema (index/eq/range or a db-side `filter`). Only deletes of
/// already-cached docs are unambiguous under such a filter.
fn has_filter(q: &Query) -> bool {
    q.index.is_some()
        || !q.eq.is_empty()
        || q.gt.is_some()
        || q.gte.is_some()
        || q.lt.is_some()
        || q.lte.is_some()
        || q.filter.is_some()
}

/// Unfiltered full-table read (`collect`/`take` with no index/eq/range/filter):
/// every doc is present, so insert/patch/replace/delete on a known id are all
/// unambiguous.
fn project_unfiltered_array(
    query: &Query,
    last: &Value,
    txn: &Transaction,
    now: i64,
) -> OptimisticProjection {
    let Some(mut working) = last.as_array().cloned() else {
        return OptimisticProjection::Skip;
    };
    for step in &txn.steps {
        if step.table() != Some(query.table.as_str()) {
            continue;
        }
        match step {
            Step::Insert { doc, .. } => {
                // A full-table window already at its `take` limit would evict an
                // unknown doc — we can't pick the right window, so decline.
                if let Some(take) = query.take
                    && (working.len() as u32) >= take
                {
                    return OptimisticProjection::Skip;
                }
                let mut d = doc.clone();
                d.insert("_id".into(), Value::String(synthetic_id()));
                d.insert("_creationTime".into(), Value::Number(now.into()));
                d.insert("_version".into(), Value::Number(1.into()));
                working.push(Value::Object(d));
            }
            Step::Patch { id, fields, .. } => merge_by_id(&mut working, id, fields),
            Step::Replace { id, doc, .. } => replace_by_id(&mut working, id, doc),
            Step::Delete { id, .. } => remove_by_id(&mut working, id),
            // FM-33: the restored doc's body is not in the cached result
            // (soft-deleted rows are invisible to reads), so there is nothing
            // unambiguous to overlay — the authoritative update delivers the
            // restored row. Same fallthrough as the ts-client's switch.
            Step::Undelete { .. } => {}
            Step::Upsert { .. } => return OptimisticProjection::Skip,
            Step::ExpectVersion { .. } | Step::ExpectAbsent { .. } => {}
            // By-query steps match an unbounded set of rows by a filter this
            // projection can't evaluate (no table store, no schema) — the effect
            // on the cached result is membership-ambiguous, so decline.
            Step::PatchByQuery { .. } | Step::DeleteByQuery { .. } => {
                return OptimisticProjection::Skip;
            }
            // Schedule/CancelSchedule and workflow steps act on future
            // execution, not this result — nothing to project.
            Step::Schedule { .. }
            | Step::CancelSchedule { .. }
            | Step::StartWorkflow { .. }
            | Step::CancelWorkflow { .. } => {}
        }
    }
    finalize(Value::Array(working), last)
}

/// Filtered read (index/eq/range or `filter` predicate): only a delete of a
/// doc already known to be in the result is unambiguous — adding or changing a
/// doc may move it in or out of the filter.
fn project_filtered_array(query: &Query, last: &Value, txn: &Transaction) -> OptimisticProjection {
    let Some(mut working) = last.as_array().cloned() else {
        return OptimisticProjection::Skip;
    };
    for step in &txn.steps {
        if step.table() != Some(query.table.as_str()) {
            continue;
        }
        match step {
            Step::Delete { id, .. } => remove_by_id(&mut working, id),
            // FM-33: undelete restores a doc whose body is not in this cached
            // result — nothing unambiguous to overlay (ts-client fallthrough).
            Step::Undelete { .. } => {}
            // insert/patch/replace/upsert are membership-ambiguous under a filter.
            Step::Insert { .. }
            | Step::Patch { .. }
            | Step::Replace { .. }
            | Step::Upsert { .. } => return OptimisticProjection::Skip,
            Step::ExpectVersion { .. } | Step::ExpectAbsent { .. } => {}
            // By-query steps match rows by a filter we can't evaluate here —
            // the effect on the filtered result is ambiguous, so decline.
            Step::PatchByQuery { .. } | Step::DeleteByQuery { .. } => {
                return OptimisticProjection::Skip;
            }
            // Schedule/CancelSchedule and workflow steps act on future
            // execution, not this result — nothing to project.
            Step::Schedule { .. }
            | Step::CancelSchedule { .. }
            | Step::StartWorkflow { .. }
            | Step::CancelWorkflow { .. } => {}
        }
    }
    finalize(Value::Array(working), last)
}

/// Point read by id: the result is exactly that id's doc (or null), so
/// patch/replace/delete of the same id are unambiguous; a freshly inserted id
/// can never match a pre-existing `get(target)`.
fn project_get(query: &Query, last: &Value, txn: &Transaction) -> OptimisticProjection {
    let target = query.get.as_deref().unwrap_or("");
    let mut working = last.clone();
    for step in &txn.steps {
        if step.table() != Some(query.table.as_str()) {
            continue;
        }
        match step {
            Step::Delete { id, .. } if id == target => working = Value::Null,
            Step::Patch { id, fields, .. } if id == target => {
                if let Some(mut d) = working.as_object().cloned() {
                    for (k, v) in fields {
                        d.insert(k.clone(), v.clone());
                    }
                    working = Value::Object(d);
                }
            }
            Step::Replace { id, doc, .. } if id == target => {
                if let Some(old) = last.as_object() {
                    let mut d = doc.clone();
                    if let Some(oid) = old.get("_id") {
                        d.insert("_id".into(), oid.clone());
                    }
                    if let Some(ct) = old.get("_creationTime") {
                        d.insert("_creationTime".into(), ct.clone());
                    }
                    d.remove("_version");
                    working = Value::Object(d);
                }
            }
            Step::Upsert { .. } => return OptimisticProjection::Skip,
            // A by-query step may patch/delete the target row, but the filter is
            // unevaluable here — decline rather than guess.
            Step::PatchByQuery { .. } | Step::DeleteByQuery { .. } => {
                return OptimisticProjection::Skip;
            }
            // Insert (fresh id never matches a pre-existing get target),
            // ExpectVersion/ExpectAbsent (preconditions, no data effect),
            // Undelete (FM-33: the restored body is not the cached value, and
            // restoring a live target is a no-op), and non-target
            // Patch/Replace/Delete: nothing to do here.
            _ => {}
        }
    }
    finalize(working, last)
}

/// `Step` is now a `par_rt_db_core::mutation` type (ARC-004 follow-up) — the
/// orphan rule forbids `impl Step { .. }` here, so this crate-private helper
/// moves to an extension trait. NOT the same helper as the server's
/// `dsl::StepTableExt` (see its doc comment): this one masks `ExpectAbsent`
/// too, which is correct for optimistic projection but wrong for the
/// server's table-scope authorization gate.
trait OptimisticStepTable {
    fn table(&self) -> Option<&str>;
}

impl OptimisticStepTable for Step {
    /// The table this step targets. Every variant except `ExpectAbsent` and the
    /// schedule/workflow steps carries one; `ExpectAbsent` is a precondition
    /// with no data effect, so its table is masked here (returning `None` makes
    /// the per-step table guard skip it, which is harmless since the variant is
    /// a no-op in every projection). Schedule/CancelSchedule and the workflow
    /// steps act on future execution, not the queried table.
    fn table(&self) -> Option<&str> {
        match self {
            Step::Insert { table, .. }
            | Step::Patch { table, .. }
            | Step::Replace { table, .. }
            | Step::Delete { table, .. }
            | Step::Undelete { table, .. }
            | Step::ExpectVersion { table, .. }
            | Step::Upsert { table, .. }
            | Step::PatchByQuery { table, .. }
            | Step::DeleteByQuery { table, .. } => Some(table.as_str()),
            Step::ExpectAbsent { .. }
            | Step::Schedule { .. }
            | Step::CancelSchedule { .. }
            | Step::StartWorkflow { .. }
            | Step::CancelWorkflow { .. } => None,
        }
    }
}

fn finalize(next: Value, last: &Value) -> OptimisticProjection {
    if next == *last {
        OptimisticProjection::Skip
    } else {
        OptimisticProjection::Overlaid(next)
    }
}

fn merge_by_id(working: &mut [Value], id: &str, fields: &Map<String, Value>) {
    for v in working.iter_mut() {
        if v.get("_id").and_then(Value::as_str) == Some(id)
            && let Some(d) = v.as_object_mut()
        {
            for (k, val) in fields {
                d.insert(k.clone(), val.clone());
            }
        }
    }
}

fn replace_by_id(working: &mut [Value], id: &str, doc: &Map<String, Value>) {
    for v in working.iter_mut() {
        if v.get("_id").and_then(Value::as_str) == Some(id) {
            let mut d = doc.clone();
            if let Some(old_id) = v.get("_id").cloned() {
                d.insert("_id".into(), old_id);
            }
            if let Some(ct) = v.get("_creationTime").cloned() {
                d.insert("_creationTime".into(), ct);
            }
            d.remove("_version");
            *v = Value::Object(d);
        }
    }
}

fn remove_by_id(working: &mut Vec<Value>, id: &str) {
    working.retain(|v| v.get("_id").and_then(Value::as_str) != Some(id));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::Mutation;
    use crate::query::TableQuery;
    use crate::wire::FilterExpr;
    use serde_json::json;

    fn collect_query() -> Query {
        TableQuery::new("items").collect()
    }

    #[test]
    fn insert_overlays_on_unfiltered_collect() {
        let q = collect_query();
        let last = json!([{ "_id": "a", "_creationTime": 1, "_version": 1, "title": "x" }]);
        let txn = Mutation::new()
            .insert("items", json!({ "title": "y" }))
            .build();
        match project_optimistic_update(&q, &last, &txn, 99) {
            OptimisticProjection::Overlaid(v) => {
                let arr = v.as_array().expect("overlaid is array");
                assert_eq!(arr.len(), 2);
                assert!(
                    arr[1]["_id"]
                        .as_str()
                        .expect("synthetic _id is string")
                        .starts_with("__optimistic__")
                );
                assert_eq!(arr[1]["_creationTime"], 99);
                assert_eq!(arr[1]["_version"], 1);
                assert_eq!(arr[1]["title"], "y");
            }
            OptimisticProjection::Skip => panic!("expected overlay"),
        }
    }

    #[test]
    fn patch_overlays_by_id() {
        let q = collect_query();
        let last = json!([{ "_id": "a", "_creationTime": 1, "_version": 1, "n": 1 }]);
        let txn = Mutation::new()
            .patch("items", "a", json!({ "n": 2 }))
            .build();
        match project_optimistic_update(&q, &last, &txn, 99) {
            OptimisticProjection::Overlaid(v) => assert_eq!(v[0]["n"], 2),
            OptimisticProjection::Skip => panic!("expected overlay"),
        }
    }

    #[test]
    fn delete_overlays_by_id() {
        let q = collect_query();
        let last = json!([
            { "_id": "a", "_creationTime": 1, "_version": 1 },
            { "_id": "b", "_creationTime": 2, "_version": 1 }
        ]);
        let txn = Mutation::new().delete("items", "a").build();
        match project_optimistic_update(&q, &last, &txn, 99) {
            OptimisticProjection::Overlaid(v) => {
                assert_eq!(v.as_array().expect("overlaid is array").len(), 1);
            }
            OptimisticProjection::Skip => panic!("expected overlay"),
        }
    }

    #[test]
    fn noop_patch_returns_skip() {
        let q = collect_query();
        let last = json!([{ "_id": "a", "_creationTime": 1, "_version": 1, "n": 1 }]);
        // patch to the same value → canonical-equal → Skip
        let txn = Mutation::new()
            .patch("items", "a", json!({ "n": 1 }))
            .build();
        assert!(matches!(
            project_optimistic_update(&q, &last, &txn, 99),
            OptimisticProjection::Skip
        ));
    }

    #[test]
    fn insert_skips_when_take_window_full() {
        let q = TableQuery::new("items").take(1);
        let last = json!([{ "_id": "a", "_creationTime": 1, "_version": 1 }]);
        let txn = Mutation::new()
            .insert("items", json!({ "title": "y" }))
            .build();
        assert!(matches!(
            project_optimistic_update(&q, &last, &txn, 99),
            OptimisticProjection::Skip
        ));
    }

    #[test]
    fn filtered_array_delete_only() {
        // index/eq filtered array: only delete projects
        let q = TableQuery::new("items")
            .with_index("by_status", &[json!("active")])
            .collect();
        let last = json!([{ "_id": "a", "_creationTime": 1, "_version": 1 }]);
        let del = Mutation::new().delete("items", "a").build();
        assert!(matches!(
            project_optimistic_update(&q, &last, &del, 99),
            OptimisticProjection::Overlaid(_)
        ));
        let ins = Mutation::new()
            .insert("items", json!({ "title": "y" }))
            .build();
        assert!(matches!(
            project_optimistic_update(&q, &last, &ins, 99),
            OptimisticProjection::Skip
        ));
    }

    #[test]
    fn filter_predicate_treated_as_filtered_array() {
        // Gap-fix: a collect with a `filter` predicate routes to delete-only
        // projection, not unfiltered-array. Delete overlays; insert skips.
        let q = TableQuery::new("items")
            .filter(FilterExpr::Eq {
                field: "status".into(),
                value: json!("done"),
            })
            .collect();
        let last = json!([
            { "_id": "a", "_creationTime": 1, "_version": 1 },
            { "_id": "b", "_creationTime": 2, "_version": 1 }
        ]);
        let del = Mutation::new().delete("items", "a").build();
        match project_optimistic_update(&q, &last, &del, 99) {
            OptimisticProjection::Overlaid(v) => {
                assert_eq!(v.as_array().expect("overlaid is array").len(), 1);
            }
            OptimisticProjection::Skip => panic!("delete under filter should overlay"),
        }
        let ins = Mutation::new()
            .insert("items", json!({ "title": "y" }))
            .build();
        assert!(matches!(
            project_optimistic_update(&q, &last, &ins, 99),
            OptimisticProjection::Skip
        ));
    }

    #[test]
    fn get_point_read_patch() {
        let q = TableQuery::get("items", "a");
        let last = json!({ "_id": "a", "_creationTime": 1, "_version": 1, "n": 1 });
        let txn = Mutation::new()
            .patch("items", "a", json!({ "n": 2 }))
            .build();
        match project_optimistic_update(&q, &last, &txn, 99) {
            OptimisticProjection::Overlaid(v) => {
                assert_eq!(v["n"], 2);
                assert_eq!(v["_id"], "a");
                assert_eq!(v["_creationTime"], 1);
            }
            OptimisticProjection::Skip => panic!("expected overlay"),
        }
    }

    #[test]
    fn always_skip_terminals() {
        // unique, first, count, paginate, search, vector_search all → Skip
        // regardless of txn.
        let last = json!([{ "_id": "a", "_creationTime": 1, "_version": 1 }]);
        let txn = Mutation::new()
            .insert("items", json!({ "title": "y" }))
            .patch("items", "a", json!({ "n": 2 }))
            .delete("items", "a")
            .build();
        let terminals: Vec<Query> = vec![
            TableQuery::new("items")
                .with_index("by_status", &[json!("active")])
                .unique(),
            TableQuery::new("items").first(),
            TableQuery::new("items").count(),
            TableQuery::new("items").paginate(None, 10),
            TableQuery::new("items")
                .search("search_idx", "query", ())
                .take(5),
            TableQuery::new("items")
                .vector_search("vec_idx", vec![1.0, 0.0], 5, ())
                .take(5),
        ];
        for q in terminals {
            assert!(
                matches!(
                    project_optimistic_update(&q, &last, &txn, 99),
                    OptimisticProjection::Skip
                ),
                "terminal query should skip: {:?}",
                q
            );
        }
    }

    #[test]
    fn synthetic_id_format() {
        // Two inserts in two calls produce __optimistic__N with incrementing N.
        let q = collect_query();
        let last = json!([]);
        let txn1 = Mutation::new()
            .insert("items", json!({ "title": "a" }))
            .build();
        let txn2 = Mutation::new()
            .insert("items", json!({ "title": "b" }))
            .build();
        let id1 = match project_optimistic_update(&q, &last, &txn1, 1) {
            OptimisticProjection::Overlaid(v) => v[0]["_id"]
                .as_str()
                .expect("synthetic _id is string")
                .to_owned(),
            OptimisticProjection::Skip => panic!("expected overlay"),
        };
        let id2 = match project_optimistic_update(&q, &last, &txn2, 2) {
            OptimisticProjection::Overlaid(v) => v[0]["_id"]
                .as_str()
                .expect("synthetic _id is string")
                .to_owned(),
            OptimisticProjection::Skip => panic!("expected overlay"),
        };
        assert!(id1.starts_with("__optimistic__"));
        assert!(id2.starts_with("__optimistic__"));
        assert_ne!(id1, id2, "synthetic ids must be unique");
    }
}
