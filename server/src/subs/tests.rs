use super::liveness::HostLive;
use super::read_set::*;
use super::*;
use crate::schema::{FieldType, IndexDef};
use crate::txn::{DocValues, EqBind};
use std::collections::BTreeMap;

#[tokio::test]
async fn revoked_host_subscriptions_are_removed_but_transient_errors_retained() {
    let manager = SubscriptionManager::new();
    let schema = q(serde_json::json!({"table":"agents"}));
    let principal = crate::auth::Principal::Machine {
        db: "test".into(),
        token_id: "revoked".into(),
        read_only: false,
        tables: None,
        host_capability: Some(crate::auth::host_policy::HostCapability::ParMeshV1 {
            host: "testhost".into(),
        }),
    };
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let entry = || SubEntry {
        query: schema.clone(),
        tx: tx.clone(),
        last: "[]".into(),
        read_set: ReadSet::Table,
        principal_ctx: principal.row_ctx(),
        live_host_principal: Some(principal.clone()),
    };
    let mut entries = HashMap::from([((1, "a".into()), entry()), ((1, "b".into()), entry())]);
    let mut checks = 0;
    let states = host_liveness(&entries, "test", |_| {
        checks += 1;
        std::future::ready(Err(RtDbError::internal("temporary")))
    })
    .await;
    assert_eq!(checks, 1);
    apply_host_liveness(&mut entries, &states);
    assert_eq!(
        entries.len(),
        2,
        "transient failure must retain subscriptions"
    );
    assert!(entries.values().all(|entry| !host_may_push(entry, &states)));
    let states = host_liveness(&entries, "test", |_| {
        std::future::ready(Err(RtDbError::unauthorized("revoked")))
    })
    .await;
    apply_host_liveness(&mut entries, &states);
    assert!(entries.is_empty());
    drop(manager);
}

fn q(value: serde_json::Value) -> Query {
    serde_json::from_value(value).expect("parse query")
}

// Session liveness unit tests (review of d710c6d): Dead removes the
// subscription, a transient lookup failure retains it but suppresses this
// pass's pushes (fail-closed, mirroring host tokens), and N subscriptions
// on one session cost one liveness check.
#[tokio::test]
async fn revoked_session_subs_removed_transient_retained_but_suppressed() {
    let manager = SubscriptionManager::new();
    let schema = q(serde_json::json!({"table":"agents"}));
    let hash = "deadbeef".to_string();
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let entry = || SubEntry {
        query: schema.clone(),
        tx: tx.clone(),
        last: "[]".into(),
        read_set: ReadSet::Table,
        principal_ctx: PrincipalCtx {
            session_hash: Some(hash.clone()),
            ..Default::default()
        },
        live_host_principal: None,
    };
    let mut entries = HashMap::from([((1, "a".into()), entry()), ((1, "b".into()), entry())]);

    // Transient: both retained (one shared check), but no push allowed.
    let mut checks = 0;
    let states = session_liveness(&entries, |_| {
        checks += 1;
        std::future::ready(Err(RtDbError::internal("temporary")))
    })
    .await;
    assert_eq!(checks, 1, "unique-hash dedupe: one check for two subs");
    apply_session_liveness(&mut entries, &states);
    assert_eq!(entries.len(), 2, "transient failure must retain subs");
    assert!(entries.values().all(|e| !session_may_push(e, &states)));

    // Dead: both removed.
    let states = session_liveness(&entries, |_| {
        std::future::ready(Err(RtDbError::unauthorized("revoked")))
    })
    .await;
    apply_session_liveness(&mut entries, &states);
    assert!(entries.is_empty());
    drop(manager);
}

// A session-hash-less subscription (bypass/machine) is unaffected by the
// session gate: it pushes without any liveness state.
#[test]
fn session_may_push_bypass_unaffected() {
    let manager = SubscriptionManager::new();
    let schema = q(serde_json::json!({"table":"agents"}));
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let entry = SubEntry {
        query: schema,
        tx,
        last: "[]".into(),
        read_set: ReadSet::Table,
        principal_ctx: PrincipalCtx::bypass(),
        live_host_principal: None,
    };
    let states: HashMap<String, HostLive> = HashMap::new();
    assert!(session_may_push(&entry, &states));
    drop(manager);
}

/// A table with string/number/boolean indexable fields and a couple of
/// multi-field indexes — enough to exercise eq-prefix + range derivation
/// and `in_window` typing across all three indexable scalar types.
fn test_table_def() -> TableDef {
    TableDef {
        constraints: std::collections::BTreeMap::new(),
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
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
                unique: false,
                r#where: None,
                language: None,
                trgm: false,
            },
            IndexDef {
                name: "by_status_order".to_string(),
                fields: vec!["status".to_string(), "order".to_string()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
                trgm: false,
            },
            IndexDef {
                name: "by_flag".to_string(),
                fields: vec!["flag".to_string()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
                trgm: false,
            },
        ],
        owner_field: None,
        collaborators_field: None,
        ttl: None,
        updated_at_field: None,
        auto_increment_field: None,
        authorize: None,
        soft_delete: false,

        ..Default::default()
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
fn value_sensitive_queries_are_table_level() {
    let td = test_table_def();
    // Value-sensitive / ranking terminals stay Table even with an index +
    // eq: their result depends on the VALUES of the matching set, which
    // neither window membership nor a sort boundary can bound.
    // (`aggregate` is NOT in this list — see
    // `aggregate_on_eq_prefix_derives_indexed` below: an aggregate's
    // window is the same eq/range WHERE clause `count`/`collect` already
    // window-bound.)
    let cases = [
        serde_json::json!({ "table": "t", "index": "by_status", "eq": ["x"], "distinct": true }),
        // Untruncated collect / count with no eq and no range ⇒ the window
        // is the whole table and there is no boundary ⇒ no skip benefit.
        serde_json::json!({ "table": "t" }),
        serde_json::json!({ "table": "t", "count": true }),
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
fn aggregate_on_eq_prefix_derives_indexed() {
    let td = test_table_def();
    // Ungrouped scalar sum over the eq-prefix window.
    let query = q(
        serde_json::json!({ "table": "t", "index": "by_status_order", "eq": ["backlog"], "aggregate": { "op": "sum" } }),
    );
    match ReadSet::from_query(&query, &td) {
        ReadSet::Indexed(idx) => {
            assert_eq!(idx.window.eq.len(), 1);
            assert_eq!(idx.window.eq[0].0, "status");
            // A member's field-VALUE change (not just membership) can
            // move a sum, so aggregate is content-bearing like collect.
            assert!(idx.content_bearing);
        }
        other => panic!("aggregate+eq should be Indexed, got {other:?}"),
    }

    // Grouped (legacy groupBy: true) still windows on the same eq prefix
    // — grouping only buckets the matching set, it doesn't widen it.
    let query = q(
        serde_json::json!({ "table": "t", "index": "by_status", "eq": ["backlog"], "aggregate": { "op": "count", "groupBy": true } }),
    );
    assert!(matches!(
        ReadSet::from_query(&query, &td),
        ReadSet::Indexed(_)
    ));

    // No eq and no range ⇒ whole-table window ⇒ no skip benefit ⇒ Table.
    let query = q(
        serde_json::json!({ "table": "t", "index": "by_status_order", "aggregate": { "op": "count" } }),
    );
    assert!(matches!(ReadSet::from_query(&query, &td), ReadSet::Table));

    // No index at all ⇒ Table (same as count/collect without an index).
    let query = q(serde_json::json!({ "table": "t", "aggregate": { "op": "count" } }));
    assert!(matches!(ReadSet::from_query(&query, &td), ReadSet::Table));
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
            assert_eq!(idx.window.eq.len(), 1);
            assert_eq!(idx.window.eq[0].0, "status");
            assert!(matches!(idx.window.eq[0].1, FieldType::String));
            assert!(matches!(idx.window.eq[0].2, EqBind::Text(ref s) if s == "backlog"));
            assert!(!idx.content_bearing);
            assert!(idx.window.range.is_none());
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
            assert_eq!(idx.window.eq.len(), 1);
            let r = idx.window.range.expect("range bound present");
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
            let r = idx.window.range.expect("range");
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
            assert!(idx.window.eq.is_empty());
            let r = idx.window.range.expect("range");
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

// ---- Window::contains unit cases (spec test #8: null/wrong-typed/missing ⇒ false) ----

/// A window pinning `status` to one value, with no range bound.
fn status_window(status: &str) -> Window {
    Window {
        eq: vec![(
            "status".to_string(),
            FieldType::String,
            EqBind::Text(status.to_string()),
        )],
        range: None,
    }
}

fn indexed_status_eq(status: &str, content_bearing: bool) -> IndexedRead {
    IndexedRead {
        window: status_window(status),
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
    assert!(idx.window.contains(&doc));
}

#[test]
fn in_window_non_matching_eq_returns_false() {
    let idx = indexed_status_eq("backlog", true);
    let doc = serde_json::json!({ "status": "done" })
        .as_object()
        .unwrap()
        .clone();
    assert!(!idx.window.contains(&doc));
}

#[test]
fn in_window_missing_field_returns_false() {
    let idx = indexed_status_eq("backlog", true);
    let doc = serde_json::json!({ "other": "x" })
        .as_object()
        .unwrap()
        .clone();
    assert!(!idx.window.contains(&doc));
}

#[test]
fn in_window_null_field_returns_false() {
    let idx = indexed_status_eq("backlog", true);
    let doc = serde_json::json!({ "status": null })
        .as_object()
        .unwrap()
        .clone();
    assert!(!idx.window.contains(&doc));
}

#[test]
fn in_window_wrong_typed_field_returns_false() {
    let idx = indexed_status_eq("backlog", true);
    // status declared String; doc carries a number ⇒ typing fails ⇒ outside.
    let doc = serde_json::json!({ "status": 5 })
        .as_object()
        .unwrap()
        .clone();
    assert!(!idx.window.contains(&doc));
}

#[test]
fn in_window_range_satisfied_and_not() {
    let idx = Window {
        eq: vec![(
            "status".to_string(),
            FieldType::String,
            EqBind::Text("backlog".to_string()),
        )],
        range: Some(RangeBound {
            field: "order".to_string(),
            field_type: FieldType::Number,
            lower: Some((EqBind::Num(10.0), true)),  // gte 10
            upper: Some((EqBind::Num(20.0), false)), // lt 20
        }),
    };
    // In range.
    let doc = serde_json::json!({ "status": "backlog", "order": 15 })
        .as_object()
        .unwrap()
        .clone();
    assert!(idx.contains(&doc));
    // Below lower bound.
    let doc = serde_json::json!({ "status": "backlog", "order": 5 })
        .as_object()
        .unwrap()
        .clone();
    assert!(!idx.contains(&doc));
    // At lower bound (inclusive) — in.
    let doc = serde_json::json!({ "status": "backlog", "order": 10 })
        .as_object()
        .unwrap()
        .clone();
    assert!(idx.contains(&doc));
    // At upper bound (exclusive) — out.
    let doc = serde_json::json!({ "status": "backlog", "order": 20 })
        .as_object()
        .unwrap()
        .clone();
    assert!(!idx.contains(&doc));
    // Above upper bound.
    let doc = serde_json::json!({ "status": "backlog", "order": 25 })
        .as_object()
        .unwrap()
        .clone();
    assert!(!idx.contains(&doc));
}

#[test]
fn in_window_range_null_order_field_returns_false() {
    let idx = Window {
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
    };
    // order absent ⇒ doc.get returns Null ⇒ typing fails ⇒ outside.
    let doc = serde_json::json!({ "status": "backlog" })
        .as_object()
        .unwrap()
        .clone();
    assert!(!idx.contains(&doc));
}

#[test]
fn in_window_boolean_eq_matches() {
    let idx = Window {
        eq: vec![("flag".to_string(), FieldType::Boolean, EqBind::Bool(true))],
        range: None,
    };
    let doc = serde_json::json!({ "flag": true })
        .as_object()
        .unwrap()
        .clone();
    assert!(idx.contains(&doc));
    let doc = serde_json::json!({ "flag": false })
        .as_object()
        .unwrap()
        .clone();
    assert!(!idx.contains(&doc));
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
        created_at: None,
    };
    assert!(indexed_affects(&idx, &values));
    // Even a delete of a doc that was never in window still re-runs
    // (values gone ⇒ conservative re-run).
    let values = DocValues {
        before: Some(doc_with_status("done")),
        after: None,
        created_at: None,
    };
    assert!(indexed_affects(&idx, &values));
}

#[test]
fn affects_created_in_window_true_outside_false() {
    let idx = indexed_status_eq("backlog", false);
    let in_win = DocValues {
        before: None,
        after: Some(doc_with_status("backlog")),
        created_at: None,
    };
    assert!(indexed_affects(&idx, &in_win));
    let out_win = DocValues {
        before: None,
        after: Some(doc_with_status("done")),
        created_at: None,
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
        created_at: None,
    };
    assert!(!indexed_affects(&idx, &stayed_out));
    // body-only change to a member (eq unchanged) — count unaffected
    let member_body_change = DocValues {
        before: Some(doc_with_status("backlog")),
        after: Some(doc_with_status("backlog")),
        created_at: None,
    };
    assert!(!indexed_affects(&idx, &member_body_change));
    // entered (done → backlog) — count increased
    let entered = DocValues {
        before: Some(doc_with_status("done")),
        after: Some(doc_with_status("backlog")),
        created_at: None,
    };
    assert!(indexed_affects(&idx, &entered));
    // left (backlog → done) — count decreased (regression guard for `before`)
    let left = DocValues {
        before: Some(doc_with_status("backlog")),
        after: Some(doc_with_status("done")),
        created_at: None,
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
        created_at: None,
    };
    assert!(indexed_affects(&idx, &member_body_change));
    // stayed outside — no affect
    let stayed_out = DocValues {
        before: Some(doc_with_status("done")),
        after: Some(doc_with_status("done")),
        created_at: None,
    };
    assert!(!indexed_affects(&idx, &stayed_out));
}

// =================================================================
// v3: ordered top-N boundary tracking (take / first / paginate)
// =================================================================

// ---- derivation ----

#[test]
fn take_first_paginate_derive_ordered() {
    let td = test_table_def();

    // take + index with a strict-prefix eq ⇒ the remaining index field is
    // the leading sort column.
    let query = q(
        serde_json::json!({ "table": "t", "index": "by_status_order", "eq": ["backlog"], "take": 5 }),
    );
    match ReadSet::from_query(&query, &td) {
        ReadSet::Ordered(o) => {
            assert!(matches!(o.terminal, OrderedTerminal::Take(5)));
            assert_eq!(o.window.eq.len(), 1);
            assert_eq!(o.sort_fields.len(), 1);
            assert_eq!(o.sort_fields[0].0, "order");
            assert!(!o.desc);
            assert!(o.boundary.is_none(), "unbounded until register seeds it");
        }
        other => panic!("take+eq should be Ordered, got {other:?}"),
    }

    // first ⇒ Ordered with a First terminal.
    let query =
        q(serde_json::json!({ "table": "t", "index": "by_status", "eq": ["x"], "first": true }));
    assert!(matches!(
        ReadSet::from_query(&query, &td),
        ReadSet::Ordered(o) if matches!(o.terminal, OrderedTerminal::First)
    ));

    // paginate ⇒ Ordered with a Paginate terminal.
    let query = q(
        serde_json::json!({ "table": "t", "index": "by_status", "eq": ["x"], "paginate": { "numItems": 10 } }),
    );
    assert!(matches!(
        ReadSet::from_query(&query, &td),
        ReadSet::Ordered(o) if matches!(o.terminal, OrderedTerminal::Paginate)
    ));

    // desc order is carried.
    let query = q(serde_json::json!({ "table": "t", "take": 3, "order": "desc" }));
    assert!(matches!(
        ReadSet::from_query(&query, &td),
        ReadSet::Ordered(o) if o.desc
    ));
}

/// ENH-030: `paginate` now legally composes with the ranked terminals, so
/// the ranking guard in `try_ordered` is load-bearing rather than
/// defensive. A paginated ranked query is ordered by relevance/distance/
/// fusion score, which the index-tail boundary does not model, so it must
/// degrade to `Table` (always re-run). Skipping such a re-run against a
/// boundary that does not describe the query would UNDER-approximate and
/// silently drop pushes.
#[test]
fn paginated_ranked_terminals_fall_back_to_table() {
    let td = test_table_def();
    // search + paginate ⇒ Table.
    let query = q(serde_json::json!({
        "table": "t",
        "search": {"index": "search_body", "query": "x"},
        "paginate": {"numItems": 10}
    }));
    assert!(matches!(ReadSet::from_query(&query, &td), ReadSet::Table));
    // vectorSearch + paginate ⇒ Table.
    let query = q(serde_json::json!({
        "table": "t",
        "vectorSearch": {"index": "by_embedding", "vector": [0.0, 0.0, 0.0], "limit": 5},
        "paginate": {"numItems": 10}
    }));
    assert!(matches!(ReadSet::from_query(&query, &td), ReadSet::Table));
    // hybridSearch + paginate ⇒ Table.
    let query = q(serde_json::json!({
        "table": "t",
        "hybridSearch": {"query": "x", "vector": [0.0, 0.0, 0.0], "limit": 5},
        "paginate": {"numItems": 10}
    }));
    assert!(matches!(ReadSet::from_query(&query, &td), ReadSet::Table));
}

#[test]
fn bare_take_without_index_is_ordered_on_created_at() {
    // No index at all: the window matches everything and `created_at` is
    // the only sort component — still bounded by the top-N boundary.
    let query = q(serde_json::json!({ "table": "t", "take": 5 }));
    match ReadSet::from_query(&query, &test_table_def()) {
        ReadSet::Ordered(o) => {
            assert!(o.window.eq.is_empty());
            assert!(o.window.range.is_none());
            assert!(o.sort_fields.is_empty());
        }
        other => panic!("bare take should be Ordered, got {other:?}"),
    }
}

#[test]
fn ordered_falls_back_to_table_on_unresolvable_index() {
    let td = test_table_def();
    // Unknown index ⇒ no window can be derived ⇒ Table (over-approximate).
    let query = q(serde_json::json!({ "table": "t", "index": "nope", "take": 5 }));
    assert!(matches!(ReadSet::from_query(&query, &td), ReadSet::Table));
    // Wrongly-typed eq value ⇒ Table for the ordered path too.
    let query = q(
        serde_json::json!({ "table": "t", "index": "by_status_order", "eq": ["backlog", "oops"], "take": 5 }),
    );
    assert!(matches!(ReadSet::from_query(&query, &td), ReadSet::Table));
}

// ---- fixtures ----

/// A `take(n)` read over `status = "backlog"` sorted by `order`.
fn ordered_take(n: u32, desc: bool, boundary: Option<SortKey>) -> OrderedRead {
    OrderedRead {
        window: status_window("backlog"),
        sort_fields: vec![("order".to_string(), FieldType::Number)],
        desc,
        terminal: OrderedTerminal::Take(n),
        boundary,
    }
}

fn key(order: f64, created_at: i64) -> SortKey {
    SortKey {
        fields: vec![EqBind::Num(order)],
        created_at,
    }
}

fn doc_at(status: &str, order: f64) -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({ "status": status, "order": order })
        .as_object()
        .expect("object")
        .clone()
}

/// A written doc that existed before the txn and still does (an update).
fn updated(
    before: serde_json::Map<String, serde_json::Value>,
    after: serde_json::Map<String, serde_json::Value>,
    created_at: i64,
) -> DocValues {
    DocValues {
        before: Some(before),
        after: Some(after),
        created_at: Some(created_at),
    }
}

fn created(after: serde_json::Map<String, serde_json::Value>, created_at: i64) -> DocValues {
    DocValues {
        before: None,
        after: Some(after),
        created_at: Some(created_at),
    }
}

// ---- compare_keys / ranks_at_or_before ----

#[test]
fn compare_keys_orders_by_field_then_created_at() {
    use std::cmp::Ordering;
    assert_eq!(
        compare_keys(&key(1.0, 100), &key(2.0, 100)),
        Some(Ordering::Less)
    );
    assert_eq!(
        compare_keys(&key(2.0, 100), &key(1.0, 999)),
        Some(Ordering::Greater),
        "the leading sort field outranks created_at"
    );
    assert_eq!(
        compare_keys(&key(1.0, 100), &key(1.0, 200)),
        Some(Ordering::Less),
        "created_at breaks a tie on the sort field"
    );
}

#[test]
fn compare_keys_reports_doubt_rather_than_guessing() {
    // Exact tie on every carried component: the DB would break it on `id`
    // under its own collation, which this deliberately does not model.
    assert_eq!(compare_keys(&key(1.0, 100), &key(1.0, 100)), None);
    // Arity mismatch (a schema evolved under a live subscription).
    let short = SortKey {
        fields: vec![],
        created_at: 100,
    };
    assert_eq!(compare_keys(&key(1.0, 100), &short), None);
    // Variant mismatch: no `cmp_binds` arm.
    let texty = SortKey {
        fields: vec![EqBind::Text("x".into())],
        created_at: 100,
    };
    assert_eq!(compare_keys(&key(1.0, 100), &texty), None);
}

#[test]
fn ranks_at_or_before_follows_the_sort_direction() {
    let boundary = key(10.0, 500);
    // Ascending: smaller keys come first, so they are at-or-before.
    assert_eq!(
        ranks_at_or_before(&key(5.0, 500), &boundary, false),
        Some(true)
    );
    assert_eq!(
        ranks_at_or_before(&key(20.0, 500), &boundary, false),
        Some(false)
    );
    // Descending: larger keys come first.
    assert_eq!(
        ranks_at_or_before(&key(20.0, 500), &boundary, true),
        Some(true)
    );
    assert_eq!(
        ranks_at_or_before(&key(5.0, 500), &boundary, true),
        Some(false)
    );
    // Doubt propagates.
    assert_eq!(ranks_at_or_before(&boundary, &boundary, false), None);
}

// ---- boundary extraction ----

fn result_doc(order: f64, created_at: i64) -> serde_json::Value {
    serde_json::json!({
        "status": "backlog",
        "order": order,
        "_id": "abc",
        "_creationTime": created_at,
        "_version": 1
    })
}

#[test]
fn boundary_set_only_when_the_result_was_full() {
    let ordered = ordered_take(2, false, None);
    // Exactly N docs ⇒ bounded by the last one.
    let full = QueryResult::Docs(vec![result_doc(1.0, 100), result_doc(2.0, 200)]);
    assert_eq!(
        ordered.boundary_from_result(&full),
        Some(key(2.0, 200)),
        "a full take is bounded by its Nth doc"
    );
    // Fewer than N ⇒ unbounded: anything matching can still enter.
    let partial = QueryResult::Docs(vec![result_doc(1.0, 100)]);
    assert_eq!(ordered.boundary_from_result(&partial), None);
    assert_eq!(
        ordered.boundary_from_result(&QueryResult::Docs(vec![])),
        None
    );
}

#[test]
fn boundary_for_first_and_paginate_terminals() {
    let first = OrderedRead {
        terminal: OrderedTerminal::First,
        ..ordered_take(1, false, None)
    };
    assert_eq!(
        first.boundary_from_result(&QueryResult::Doc(Some(result_doc(3.0, 300)))),
        Some(key(3.0, 300))
    );
    assert_eq!(first.boundary_from_result(&QueryResult::Doc(None)), None);

    let paginate = OrderedRead {
        terminal: OrderedTerminal::Paginate,
        ..ordered_take(1, false, None)
    };
    // A next cursor means a further doc exists ⇒ the page's last doc bounds it.
    let has_next = QueryResult::Paginated(crate::query::PaginatedResult {
        docs: vec![result_doc(1.0, 100), result_doc(2.0, 200)],
        next_cursor: Some("cursor".to_string()),
    });
    assert_eq!(
        paginate.boundary_from_result(&has_next),
        Some(key(2.0, 200))
    );
    // No next cursor: an insert beyond the last row would flip hasNext on
    // and mint a cursor, so the page must NOT be treated as bounded.
    let last_page = QueryResult::Paginated(crate::query::PaginatedResult {
        docs: vec![result_doc(1.0, 100), result_doc(2.0, 200)],
        next_cursor: None,
    });
    assert_eq!(paginate.boundary_from_result(&last_page), None);
}

#[test]
fn boundary_unset_when_a_result_doc_cannot_be_ranked() {
    let ordered = ordered_take(1, false, None);
    // Missing `_creationTime` / missing sort field ⇒ no boundary (the
    // over-approximating answer), never a panic.
    let no_time = QueryResult::Docs(vec![serde_json::json!({ "status": "backlog", "order": 1 })]);
    assert_eq!(ordered.boundary_from_result(&no_time), None);
    let no_sort_field = QueryResult::Docs(vec![serde_json::json!({
        "status": "backlog", "_id": "a", "_creationTime": 1, "_version": 1
    })]);
    assert_eq!(ordered.boundary_from_result(&no_sort_field), None);
}

// ---- ordered_affects: the skip decision ----

#[test]
fn ordered_unbounded_degenerates_to_window_membership() {
    let ordered = ordered_take(5, false, None);
    // In-window insert affects; out-of-window insert does not.
    assert!(ordered_affects(
        &ordered,
        &created(doc_at("backlog", 999.0), 999)
    ));
    assert!(!ordered_affects(
        &ordered,
        &created(doc_at("done", 1.0), 100)
    ));
}

#[test]
fn ordered_skips_writes_beyond_the_boundary() {
    // Top-2 of `order` ascending; boundary = (order 20, created 200).
    let ordered = ordered_take(2, false, Some(key(20.0, 200)));

    // A doc ranking after the boundary cannot be in the top 2, in any of
    // its states ⇒ skip.
    assert!(!ordered_affects(
        &ordered,
        &created(doc_at("backlog", 50.0), 500)
    ));
    assert!(!ordered_affects(
        &ordered,
        &updated(doc_at("backlog", 50.0), doc_at("backlog", 60.0), 500)
    ));
    // Out of the window entirely ⇒ skip regardless of rank.
    assert!(!ordered_affects(
        &ordered,
        &updated(doc_at("done", 1.0), doc_at("done", 2.0), 100)
    ));
}

#[test]
fn ordered_reruns_for_writes_at_or_before_the_boundary() {
    let ordered = ordered_take(2, false, Some(key(20.0, 200)));

    // Inserted ahead of the boundary ⇒ enters the top 2.
    assert!(ordered_affects(
        &ordered,
        &created(doc_at("backlog", 5.0), 50)
    ));
    // A member's body/rank change ⇒ the result carries doc bodies.
    assert!(ordered_affects(
        &ordered,
        &updated(doc_at("backlog", 10.0), doc_at("backlog", 12.0), 100)
    ));
    // Moving OUT of the window from inside the top 2 ⇒ the before-state
    // was a member (regression guard for dropping `before`).
    assert!(ordered_affects(
        &ordered,
        &updated(doc_at("backlog", 10.0), doc_at("done", 10.0), 100)
    ));
    // Moving INTO the window from beyond the boundary, landing ahead of it.
    assert!(ordered_affects(
        &ordered,
        &updated(doc_at("done", 5.0), doc_at("backlog", 5.0), 50)
    ));
    // Exactly at the boundary ⇒ a tie compare_keys reports as doubt ⇒ re-run.
    assert!(ordered_affects(
        &ordered,
        &updated(doc_at("backlog", 20.0), doc_at("backlog", 20.0), 200)
    ));
}

#[test]
fn ordered_desc_inverts_the_boundary_test() {
    // Newest-first feed: boundary = (order 20, created 200).
    let ordered = ordered_take(2, true, Some(key(20.0, 200)));
    // Larger sorts first ⇒ a larger key is INSIDE the window.
    assert!(ordered_affects(
        &ordered,
        &created(doc_at("backlog", 50.0), 500)
    ));
    // Smaller sorts later ⇒ beyond the boundary ⇒ skip.
    assert!(!ordered_affects(
        &ordered,
        &created(doc_at("backlog", 5.0), 50)
    ));
}

// ---- skip-verification sampler ----

#[test]
fn sampler_is_off_by_default_and_touches_no_state() {
    let mgr = SubscriptionManager::new();
    for _ in 0..10 {
        assert!(!mgr.sample_skip_verification());
    }
    // Disabled means the counter is never even incremented, so a busy
    // instance pays nothing for the knob being present.
    assert_eq!(mgr.skip_seq.load(Ordering::Relaxed), 0);
}

#[test]
fn sampler_every_1_verifies_every_skip() {
    let mgr = SubscriptionManager::with_instrumentation(None, 1);
    for _ in 0..5 {
        assert!(mgr.sample_skip_verification());
    }
}

#[test]
fn sampler_every_n_verifies_one_in_n_deterministically() {
    let mgr = SubscriptionManager::with_instrumentation(None, 3);
    // Deterministic 1-in-3 starting with the first skip, so a test can pin
    // the rate and assert exact counts.
    let picks: Vec<bool> = (0..7).map(|_| mgr.sample_skip_verification()).collect();
    assert_eq!(
        picks,
        vec![true, false, false, true, false, false, true],
        "expected every 3rd skip to be verified"
    );
}

#[test]
fn ordered_reruns_when_a_doc_cannot_be_ranked() {
    let ordered = ordered_take(2, false, Some(key(20.0, 200)));

    // Delete: no captured values at all ⇒ always re-run.
    let deleted = DocValues {
        before: None,
        after: None,
        created_at: None,
    };
    assert!(ordered_affects(&ordered, &deleted));

    // In-window but `created_at` was not captured ⇒ unrankable ⇒ re-run.
    let no_created_at = DocValues {
        before: None,
        after: Some(doc_at("backlog", 50.0)),
        created_at: None,
    };
    assert!(ordered_affects(&ordered, &no_created_at));

    // In-window but the sort field is missing / null / wrongly typed ⇒
    // Postgres would order the SQL NULL by its own rule ⇒ re-run.
    for bad in [
        serde_json::json!({ "status": "backlog" }),
        serde_json::json!({ "status": "backlog", "order": null }),
        serde_json::json!({ "status": "backlog", "order": "not-a-number" }),
    ] {
        let values = created(bad.as_object().expect("object").clone(), 500);
        assert!(
            ordered_affects(&ordered, &values),
            "unrankable doc must re-run: {bad}"
        );
    }
}
