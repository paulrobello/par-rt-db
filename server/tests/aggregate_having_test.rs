//! Aggregate `having` (post-group filter) and the grouped-result `truncated`
//! marker, against the real Postgres-backed query path.

use std::collections::BTreeMap;

use crate::common::{fresh_db, kanban_schema_json, test_state};
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::error::ErrorCode;
use rtdb_server::query::{
    AggregateOp, AggregateSpec, GroupBy, HavingCmp, Query, QueryResult, execute_query,
};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, execute_txn};
use serde_json::{Value, json};
use sqlx::PgPool;

fn schema() -> SchemaDef {
    serde_json::from_value(kanban_schema_json()).expect("parse kanban schema")
}

fn obj(value: Value) -> serde_json::Map<String, Value> {
    value.as_object().expect("json object").clone()
}

async fn txn(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    steps: Vec<Step>,
) -> anyhow::Result<Value> {
    let out = execute_txn(
        pool,
        db,
        schema,
        &Transaction { steps },
        &PrincipalCtx::bypass(),
        None,
    )
    .await?;
    Ok(Value::Array(out.results))
}

async fn project(pool: &PgPool, db: &str, schema: &SchemaDef) -> anyhow::Result<String> {
    let r = txn(
        pool,
        db,
        schema,
        vec![Step::Insert {
            table: "projects".into(),
            doc: obj(json!({
                "name": "P", "description": null, "status": "active", "tags": [], "updatedAt": 1.0
            })),
        }],
    )
    .await?;
    Ok(r[0]["id"].as_str().expect("id").to_string())
}

fn items(project_id: &str, rows: &[(&str, f64)]) -> Step {
    Step::InsertMany {
        table: "workItems".into(),
        docs: rows
            .iter()
            .map(|(status, order)| {
                obj(json!({
                    "projectId": project_id, "title": "t", "status": status,
                    "order": order, "completedAt": null
                }))
            })
            .collect(),
    }
}

fn having(pairs: &[(&str, HavingCmp)]) -> Option<BTreeMap<String, HavingCmp>> {
    Some(
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect(),
    )
}

fn gte(v: f64) -> HavingCmp {
    HavingCmp {
        gte: Some(v),
        ..Default::default()
    }
}

fn query(index: &str, project_id: &str, eq_status: bool, spec: AggregateSpec) -> Query {
    let mut eq = vec![json!(project_id)];
    if eq_status {
        eq.push(json!("backlog"));
    }
    Query {
        table: "workItems".into(),
        index: Some(index.into()),
        eq,
        aggregate: Some(spec),
        ..Default::default()
    }
}

async fn run(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    q: &Query,
) -> Result<QueryResult, rtdb_server::error::RtDbError> {
    execute_query(pool, db, schema, q, &PrincipalCtx::bypass(), false).await
}

fn wire(result: &QueryResult) -> Value {
    serde_json::to_value(result).expect("serialize result")
}

async fn seeded_db(
    state: &std::sync::Arc<rtdb_server::AppState>,
    rows: &[(&str, f64)],
) -> anyhow::Result<(crate::common::TestDb, SchemaDef, String)> {
    let db = fresh_db(state).await;
    let schema = schema();
    let pid = project(&state.pool, &db, &schema).await?;
    txn(&state.pool, &db, &schema, vec![items(&pid, rows)]).await?;
    Ok((db, schema, pid))
}

/// backlog orders 1,1,3 / in_progress 2 / done 4:
/// counts 3/1/1, sums 5/2/4; composite (status,order) counts: (backlog,1)=2, rest 1.
const ROWS: [(&str, f64); 5] = [
    ("backlog", 1.0),
    ("backlog", 1.0),
    ("backlog", 3.0),
    ("in_progress", 2.0),
    ("done", 4.0),
];

fn keys_of(v: &Value) -> Vec<Value> {
    v.as_array()
        .expect("array")
        .iter()
        .map(|r| r.get("key").or_else(|| r.get("keys")).expect("key").clone())
        .collect()
}

// Single-op legacy grouped form: the having name is the lowercase op name.
#[tokio::test]
async fn having_single_op_drops_failing_groups() -> anyhow::Result<()> {
    let state = test_state().await;
    let (db, schema, pid) = seeded_db(&state, &ROWS).await?;
    let spec = AggregateSpec {
        op: Some(AggregateOp::Count),
        aggregates: None,
        group_by: GroupBy::Bool(true),
        having: having(&[("count", gte(2.0))]),
    };
    let r = run(
        &state.pool,
        &db,
        &schema,
        &query("by_project_status_order", &pid, false, spec),
    )
    .await?;
    assert_eq!(wire(&r), json!([{"key": "backlog", "value": 3}]));
    Ok(())
}

// Multi-op alias form, with two bounds on one alias and a second alias ANDed.
#[tokio::test]
async fn having_multi_op_alias_filters_on_alias() -> anyhow::Result<()> {
    let state = test_state().await;
    let (db, schema, pid) = seeded_db(&state, &ROWS).await?;
    let aggregates = BTreeMap::from([
        ("n".to_string(), AggregateOp::Count),
        ("total".to_string(), AggregateOp::Sum),
    ]);
    let mk = |h| AggregateSpec {
        op: None,
        aggregates: Some(aggregates.clone()),
        group_by: GroupBy::Bool(true),
        having: h,
    };
    let gt3 = HavingCmp {
        gt: Some(3.0),
        ..Default::default()
    };
    let r = run(
        &state.pool,
        &db,
        &schema,
        &query(
            "by_project_status_order",
            &pid,
            false,
            mk(having(&[("total", gt3.clone())])),
        ),
    )
    .await?;
    // sums: backlog 5, done 4, in_progress 2 -> in_progress is absent.
    assert_eq!(
        keys_of(&wire(&r)),
        vec![json!(["backlog"]), json!(["done"])]
    );

    let lt5 = HavingCmp {
        lt: Some(5.0),
        ..Default::default()
    };
    let r = run(
        &state.pool,
        &db,
        &schema,
        &query(
            "by_project_status_order",
            &pid,
            false,
            mk(having(&[("total", gt3), ("n", lt5)])),
        ),
    )
    .await?;
    // total > 3 AND n < 5: both still pass; add n >= 2 to drop done.
    assert_eq!(keys_of(&wire(&r)).len(), 2);
    let r = run(
        &state.pool,
        &db,
        &schema,
        &query(
            "by_project_status_order",
            &pid,
            false,
            mk(having(&[("n", gte(2.0))])),
        ),
    )
    .await?;
    assert_eq!(keys_of(&wire(&r)), vec![json!(["backlog"])]);
    Ok(())
}

// Composite groupBy form.
#[tokio::test]
async fn having_composite_group_by() -> anyhow::Result<()> {
    let state = test_state().await;
    let (db, schema, pid) = seeded_db(&state, &ROWS).await?;
    let spec = AggregateSpec {
        op: None,
        aggregates: Some(BTreeMap::from([("n".to_string(), AggregateOp::Count)])),
        group_by: GroupBy::Fields(vec!["status".into(), "order".into()]),
        having: having(&[("n", gte(2.0))]),
    };
    let r = run(
        &state.pool,
        &db,
        &schema,
        &query("by_project_status_order", &pid, false, spec),
    )
    .await?;
    assert_eq!(
        wire(&r),
        json!([{"keys": ["backlog", 1], "values": {"n": 2}}])
    );
    Ok(())
}

#[tokio::test]
async fn having_rejects_bad_specs() -> anyhow::Result<()> {
    let state = test_state().await;
    let (db, schema, pid) = seeded_db(&state, &ROWS).await?;
    let base = |group_by, h| AggregateSpec {
        op: Some(AggregateOp::Count),
        aggregates: None,
        group_by,
        having: h,
    };
    let cases = [
        // having without groupBy
        base(GroupBy::Bool(false), having(&[("count", gte(1.0))])),
        // unknown aggregate name
        base(GroupBy::Bool(true), having(&[("nope", gte(1.0))])),
        // no bound
        base(
            GroupBy::Bool(true),
            having(&[("count", HavingCmp::default())]),
        ),
        // empty having
        base(GroupBy::Bool(true), Some(BTreeMap::new())),
    ];
    for spec in cases {
        let err = run(
            &state.pool,
            &db,
            &schema,
            &query("by_project_status_order", &pid, false, spec),
        )
        .await
        .expect_err("bad having must be rejected");
        assert_eq!(err.code, ErrorCode::BadRequest);
    }
    Ok(())
}

// Cap + marker: 4097 distinct `order` groups overflow MAX_TAKE (4096). Without
// having the result is the `{groups, truncated: true}` shape; with a having that
// leaves one group the cap never triggers (cap applies AFTER having) and the
// result is the bare array with no marker. A small result also omits it.
#[tokio::test]
async fn grouped_aggregate_truncated_marker_and_cap_after_having() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let schema = schema();
    let pid = project(&state.pool, &db, &schema).await?;
    // 4097 distinct orders (0..=4096), order 0 inserted twice -> 4097 groups.
    let mut all: Vec<(&str, f64)> = (0..=4096).map(|o| ("backlog", f64::from(o))).collect();
    all.push(("backlog", 0.0));
    for chunk in all.chunks(1000) {
        txn(&state.pool, &db, &schema, vec![items(&pid, chunk)]).await?;
    }
    let spec = |h| AggregateSpec {
        op: Some(AggregateOp::Count),
        aggregates: None,
        group_by: GroupBy::Bool(true),
        having: h,
    };
    let q = |h| query("by_project_and_order", &pid, false, spec(h));

    let capped = wire(&run(&state.pool, &db, &schema, &q(None)).await?);
    assert_eq!(capped["truncated"], json!(true));
    let groups = capped["groups"].as_array().expect("groups array");
    assert_eq!(groups.len(), 4096);
    assert_eq!(groups[0], json!({"key": 0, "value": 2}));

    let filtered = wire(
        &run(
            &state.pool,
            &db,
            &schema,
            &q(having(&[("count", gte(2.0))])),
        )
        .await?,
    );
    assert_eq!(filtered, json!([{"key": 0, "value": 2}]));

    // Multi-op grouped analogue carries the marker too.
    let multi = AggregateSpec {
        op: None,
        aggregates: Some(BTreeMap::from([("n".to_string(), AggregateOp::Count)])),
        group_by: GroupBy::Bool(true),
        having: None,
    };
    let m = wire(
        &run(
            &state.pool,
            &db,
            &schema,
            &query("by_project_and_order", &pid, false, multi),
        )
        .await?,
    );
    assert_eq!(m["truncated"], json!(true));
    assert_eq!(m["groups"].as_array().expect("groups").len(), 4096);
    Ok(())
}

// Under the cap the marker is omitted and the result is the legacy bare array.
#[tokio::test]
async fn grouped_aggregate_under_cap_omits_marker() -> anyhow::Result<()> {
    let state = test_state().await;
    let (db, schema, pid) = seeded_db(&state, &ROWS).await?;
    let spec = AggregateSpec {
        op: Some(AggregateOp::Count),
        aggregates: None,
        group_by: GroupBy::Bool(true),
        having: None,
    };
    let r = run(
        &state.pool,
        &db,
        &schema,
        &query("by_project_status_order", &pid, false, spec),
    )
    .await?;
    assert!(wire(&r).is_array());
    Ok(())
}
