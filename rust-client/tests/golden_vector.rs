//! QA-001: Golden-vector parity test (rust-client view).
//!
//! Loads `wire-corpus/golden-vector.json` (repo root — the single source of
//! truth) and runs each query case through the rust-client in-memory engine,
//! comparing canonicalized projected results. The same fixture is consumed by
//! the ts-client, python-client, and server (against Postgres) tests; a
//! divergence in any one implementation surfaces there.
//!
//! The fixture encodes the dataset, the per-case wire-shape `Query`, and the
//! expected canonical result. System fields (`_id`, `_creationTime`, `_owner`,
//! `_updatedAt`) are projected out before comparison so the client's
//! id-minting order doesn't cause spurious divergence — the audit point is to
//! catch **sort-comparator / boundary / terminal-cascade** divergence, not
//! id-minting drift.

use par_rt_db_client::in_memory::InMemoryRtDbClientOptions;
use par_rt_db_client::schema::{FieldType, Schema, Table};
use par_rt_db_client::{InMemoryRtDbClient, Mutation, Query, StepResult};
use serde_json::{Value, json};
use std::sync::atomic::{AtomicI64, Ordering};

#[derive(serde::Deserialize)]
struct Fixture {
    #[serde(rename = "schema_table")]
    schema_table: String,
    #[serde(rename = "schema_fields")]
    schema_fields: Value,
    #[serde(rename = "schema_indexes")]
    schema_indexes: Vec<IndexSpec>,
    seed: Vec<Value>,
    cases: Vec<Case>,
}

#[derive(serde::Deserialize)]
struct IndexSpec {
    name: String,
    fields: Vec<String>,
}

#[derive(serde::Deserialize)]
struct Case {
    id: String,
    query: Value,
    #[serde(default)]
    expected: Option<Value>,
    #[serde(default, rename = "expected_scalar")]
    expected_scalar: Option<i64>,
    #[serde(default, rename = "expected_unordered")]
    expected_unordered: bool,
    #[serde(default, rename = "expected_has_next_cursor")]
    expected_has_next_cursor: bool,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("../../wire-corpus/golden-vector.json"))
        .expect("parse golden-vector.json")
}

fn build_schema(fx: &Fixture) -> Schema {
    // Translate the fixture's `schema_fields` (a JSON object of name→type-string)
    // into the rust-client FieldType. Only the types the fixture uses are
    // implemented; new types must be added here.
    let mut table = Table::new();
    for (name, ty) in fx.schema_fields.as_object().expect("schema_fields object") {
        let ft = match ty.as_str().expect("field type string") {
            "string" => FieldType::String,
            "number" => FieldType::Number,
            "optional(string)" => FieldType::optional(FieldType::String),
            other => panic!("fixture field type not implemented: {other}"),
        };
        table = table.field(name, ft);
    }
    for ix in &fx.schema_indexes {
        let fields: Vec<&str> = ix.fields.iter().map(String::as_str).collect();
        table = table.index(&ix.name, &fields);
    }
    Schema::builder().table(&fx.schema_table, table).build()
}

fn seed_client(fx: &Fixture) -> InMemoryRtDbClient {
    // Deterministic clock + constant 0.0 RNG so each insert mints a distinct
    // _id (mirrors the ts/python harnesses).
    static COUNTER: AtomicI64 = AtomicI64::new(1_700_000_000_000);
    let now = || COUNTER.fetch_add(1, Ordering::SeqCst);
    let mut c =
        InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default().now(now).random(|| 0.0));
    c.push_schema(&build_schema(fx)).expect("push_schema");
    // Mutate is async; use tokio runtime to drive it. We can't mark this fn
    // async because it's called from a sync #[test] setup, so block_on.
    let rt = tokio::runtime::Runtime::new().expect("tokio runtime");
    rt.block_on(async {
        for doc in &fx.seed {
            let txn = Mutation::new()
                .insert(&fx.schema_table, doc.clone())
                .build();
            let results = c.mutate(&txn, None).await.expect("insert ok");
            assert!(
                matches!(&results[0], StepResult::Insert { .. }),
                "expected Insert, got {:?}",
                results[0]
            );
        }
    });
    c
}

/// Project a doc to (name, status, order) — drops system fields so id-minting
/// order differences don't cause spurious divergence.
fn project(doc: &Value) -> Value {
    json!({
        "name": doc.get("name").cloned().unwrap_or(Value::Null),
        "status": doc.get("status").cloned().unwrap_or(Value::Null),
        "order": doc.get("order").cloned().unwrap_or(Value::Null),
    })
}

fn project_list(docs: &[Value]) -> Vec<Value> {
    docs.iter().map(project).collect()
}

fn run_case(c: &InMemoryRtDbClient, case: &Case) -> Value {
    let q: Query = serde_json::from_value(case.query.clone()).expect("parse Query from fixture");
    c.run_query(&q).expect("run_query")
}

#[test]
fn golden_vector_parity() {
    let fx = load_fixture();
    let c = seed_client(&fx);

    for case in &fx.cases {
        let result = run_case(&c, case);

        if let Some(scalar) = case.expected_scalar {
            assert_eq!(
                result,
                json!(scalar),
                "{}: expected count {}",
                case.id,
                scalar
            );
            continue;
        }

        if case.expected_unordered {
            let mut got: Vec<Value> = result
                .as_array()
                .unwrap_or_else(|| panic!("{}: expected array", case.id))
                .iter()
                .map(project)
                .collect();
            got.sort_by(|a, b| {
                a["name"]
                    .as_str()
                    .unwrap_or("")
                    .cmp(b["name"].as_str().unwrap_or(""))
            });
            let mut want: Vec<Value> = case
                .expected
                .as_ref()
                .unwrap_or_else(|| panic!("{}: expected_unordered requires expected", case.id))
                .as_array()
                .unwrap_or_else(|| panic!("{}: expected_unordered requires array", case.id))
                .to_vec();
            want.sort_by(|a, b| {
                a["name"]
                    .as_str()
                    .unwrap_or("")
                    .cmp(b["name"].as_str().unwrap_or(""))
            });
            assert_eq!(got, want, "{}: unordered mismatch", case.id);
            continue;
        }

        if case.expected_has_next_cursor {
            let page = result
                .as_object()
                .unwrap_or_else(|| panic!("{}: expected PaginatedResult object", case.id));
            let docs = page
                .get("docs")
                .and_then(Value::as_array)
                .unwrap_or_else(|| panic!("{}: PaginatedResult.docs array", case.id));
            let got = project_list(docs);
            let want = case
                .expected
                .as_ref()
                .and_then(Value::as_array)
                .unwrap_or_else(|| panic!("{}: paginate requires array expected", case.id));
            assert_eq!(got, want.to_vec(), "{}: page mismatch", case.id);
            assert!(
                page.get("nextCursor")
                    .map(|v| !v.is_null())
                    .unwrap_or(false),
                "{}: expected nextCursor present",
                case.id
            );
            continue;
        }

        if let Some(expected) = &case.expected {
            if expected.is_array() {
                let got = project_list(
                    result
                        .as_array()
                        .unwrap_or_else(|| panic!("{}: expected array", case.id)),
                );
                assert_eq!(
                    got,
                    expected.as_array().unwrap().to_vec(),
                    "{}: ordered mismatch",
                    case.id
                );
                continue;
            }
            // single-doc terminal (get / first / unique)
            let got = project(&result);
            assert_eq!(got, *expected, "{}: single-doc mismatch", case.id);
            continue;
        }
        panic!("{}: case has no expected shape", case.id);
    }
}
