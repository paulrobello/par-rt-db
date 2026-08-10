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
use par_rt_db_client::schema::{DistanceMetric, FieldType, Schema, Table};
use par_rt_db_client::{InMemoryRtDbClient, Mutation, Query, StepResult};
use serde::Deserialize as _;
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
    #[serde(default)]
    search: bool,
    #[serde(default)]
    vector: Option<VectorIndexSpecFixture>,
}

#[derive(serde::Deserialize)]
struct VectorIndexSpecFixture {
    dimensions: u32,
}

#[derive(serde::Deserialize)]
struct Case {
    id: String,
    query: Value,
    #[serde(default)]
    expected: Option<Value>,
    #[serde(default, rename = "expected_scalar")]
    expected_scalar: Option<i64>,
    /// Scalar `aggregate` result; a present JSON `null` (empty-set aggregate) is
    /// distinct from an absent field via the custom deserializer below.
    #[serde(
        default,
        rename = "expected_value",
        deserialize_with = "deserialize_present_value"
    )]
    expected_value: Option<Value>,
    #[serde(default, rename = "expected_groups")]
    expected_groups: Option<Vec<Value>>,
    #[serde(default, rename = "expected_distinct")]
    expected_distinct: Option<Vec<Value>>,
    #[serde(default, rename = "expected_unordered")]
    expected_unordered: bool,
    #[serde(default, rename = "expected_has_next_cursor")]
    expected_has_next_cursor: bool,
}

/// Deserialize a `Value` verbatim when present (including JSON `null`, which
/// serde's `Option<Value>` would otherwise collapse to `None`).
fn deserialize_present_value<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Some(Value::deserialize(deserializer)?))
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("../../wire-corpus/golden-vector.json"))
        .expect("parse golden-vector.json")
}

fn field_type_from_shorthand(shorthand: &str) -> FieldType {
    match shorthand {
        "string" => FieldType::String,
        "number" => FieldType::Number,
        "optional(string)" => FieldType::optional(FieldType::String),
        "array(string)" => FieldType::array(FieldType::String),
        other => {
            if let Some(rest) = other.strip_prefix("vector(") {
                let dims = rest
                    .trim_end_matches(')')
                    .parse::<u32>()
                    .unwrap_or_else(|_| panic!("vector(N): bad dimensions in {other}"));
                return FieldType::vector(dims);
            }
            panic!("fixture field type not implemented: {other}");
        }
    }
}

fn build_schema(fx: &Fixture) -> Schema {
    // Translate the fixture's `schema_fields` (a JSON object of name→type-string)
    // into the rust-client FieldType. Only the types the fixture uses are
    // implemented; new types must be added here.
    let mut table = Table::new();
    for (name, ty) in fx.schema_fields.as_object().expect("schema_fields object") {
        table = table.field(
            name,
            field_type_from_shorthand(ty.as_str().expect("field type string")),
        );
    }
    for ix in &fx.schema_indexes {
        let fields: Vec<&str> = ix.fields.iter().map(String::as_str).collect();
        if ix.search {
            table = table.search_index(&ix.name, &fields, None);
        } else if let Some(vec) = &ix.vector {
            // A vector index names a single Vector field at fields[0].
            let field = ix.fields.first().expect("vector index names a field");
            table =
                table.vector_index(&ix.name, field, vec.dimensions, &[], DistanceMetric::Cosine);
        } else {
            table = table.index(&ix.name, &fields);
        }
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

/// Compare two JSON values with numeric tolerance (so `6` == `6.0` across the
/// SQL-numeric server result and the f64 client aggregate result).
fn json_eq_numeric(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(xf), Some(yf)) => (xf - yf).abs() < f64::EPSILON || xf == yf,
            _ => x == y,
        },
        (Value::Null, Value::Null) => true,
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| json_eq_numeric(a, b))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).is_some_and(|yv| json_eq_numeric(v, yv)))
        }
        _ => a == b,
    }
}

fn assert_json_eq_numeric(got: &Value, want: &Value, msg: &str) {
    if !json_eq_numeric(got, want) {
        panic!("{msg}: got {got}, want {want}");
    }
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

        if let Some(want) = &case.expected_value {
            // aggregate scalar: a bare number, or null for an empty match set.
            assert_json_eq_numeric(
                &result,
                want,
                &format!("{}: aggregate scalar mismatch", case.id),
            );
            continue;
        }

        if let Some(want_groups) = &case.expected_groups {
            let got = result
                .as_array()
                .unwrap_or_else(|| panic!("{}: aggregate groupBy must return array", case.id));
            assert_eq!(
                got.len(),
                want_groups.len(),
                "{}: group count mismatch",
                case.id
            );
            for (i, (g, w)) in got.iter().zip(want_groups).enumerate() {
                let wk = w
                    .get("key")
                    .unwrap_or_else(|| panic!("{}: group {i} missing key", case.id));
                let wv = w
                    .get("value")
                    .unwrap_or_else(|| panic!("{}: group {i} missing value", case.id));
                let gk = g
                    .get("key")
                    .unwrap_or_else(|| panic!("{}: group {i} missing key", case.id));
                let gv = g
                    .get("value")
                    .unwrap_or_else(|| panic!("{}: group {i} missing value", case.id));
                assert_eq!(gk, wk, "{}: group {i} key mismatch", case.id);
                assert_json_eq_numeric(gv, wv, &format!("{}: group {i} value mismatch", case.id));
            }
            continue;
        }

        if let Some(want) = &case.expected_distinct {
            let got = result
                .as_array()
                .unwrap_or_else(|| panic!("{}: distinct must return array", case.id));
            assert_eq!(
                got.len(),
                want.len(),
                "{}: distinct count mismatch",
                case.id
            );
            for (i, (g, w)) in got.iter().zip(want).enumerate() {
                assert_json_eq_numeric(g, w, &format!("{}: distinct[{i}] mismatch", case.id));
            }
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
