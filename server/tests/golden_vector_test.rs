//! QA-001: Golden-vector parity test (server view — Postgres-backed).
//!
//! Loads `wire-corpus/golden-vector.json` (repo root — the single source of
//! truth) and runs each query case through the server's `execute_query`
//! against a real Postgres database (via `fresh_db` + `execute_txn` seeding),
//! comparing canonicalized projected results. The same fixture is consumed by
//! the ts-client, rust-client, and python-client in-memory engines; a
//! divergence between any client and the server (the source of truth) surfaces
//! here just as it does in the client parity tests.
//!
//! System fields (`_id`, `_creationTime`, `_owner`, `_updatedAt`) are projected
//! out before comparison so id-minting order differences don't cause spurious
//! divergence — the audit point is to catch **sort-comparator / boundary /
//! terminal-cascade** divergence, not id-minting drift.

mod common;

use common::{TestDb, test_state, wrap_test_db};
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db;
use rtdb_server::ddl;
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, execute_txn};
use serde::Deserialize as _;
use serde_json::{Value, json};

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
    /// Scalar `aggregate` result (sum/avg/min/max/count over the whole match
    /// set). A bare JSON number, or `null` for an empty match set. Compared
    /// numerically so SQL `numeric` (e.g. `6`) and client `f64` (e.g. `6.0`)
    /// agree. Deserialized with a custom fn so a present `null` (empty-set
    /// aggregate) is distinct from an absent field (no aggregate case).
    #[serde(
        default,
        rename = "expected_value",
        deserialize_with = "deserialize_present_value"
    )]
    expected_value: Option<Value>,
    /// Grouped `aggregate` result: list of `{key, value}` sorted by key.
    /// Values compared numerically; keys compared exactly.
    #[serde(default, rename = "expected_groups")]
    expected_groups: Option<Vec<Value>>,
    /// `distinct` result: list of distinct index-field values, ascending.
    /// Numbers compared numerically; other values compared exactly.
    #[serde(default, rename = "expected_distinct")]
    expected_distinct: Option<Vec<Value>>,
    #[serde(default, rename = "expected_unordered")]
    expected_unordered: bool,
    #[serde(default, rename = "expected_has_next_cursor")]
    expected_has_next_cursor: bool,
}

fn load_fixture() -> Fixture {
    serde_json::from_str(include_str!("../../wire-corpus/golden-vector.json"))
        .expect("parse golden-vector.json")
}

/// Deserialize a `Value` that, when present on the wire, is captured verbatim
/// — including JSON `null` (which serde's default `Option<Value>` would
/// collapse to `None`, hiding the empty-set-aggregate case where `null` is the
/// expected result). Absent fields stay `None` via `#[serde(default)]`.
fn deserialize_present_value<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Some(Value::deserialize(deserializer)?))
}

/// Translate the fixture's shorthand schema (e.g. `"name": "string"`) into the
/// server's verbose wire shape (`"name": {"type":"string"}`). The fixture is
/// shared across 4 polyglot tests; each test's `build_schema` is responsible
/// for translating to its native form. The server's wire form is the verbose
/// one because that's what `push_schema` receives over HTTP and what
/// `serde_json::from_value` parses here.
fn verbose_field_type(shorthand: &str) -> Value {
    match shorthand {
        "string" => json!({"type": "string"}),
        "number" => json!({"type": "number"}),
        "optional(string)" => json!({"type": "optional", "inner": {"type": "string"}}),
        "array(string)" => json!({"type": "array", "element": {"type": "string"}}),
        other => {
            if let Some(rest) = other.strip_prefix("vector(") {
                let dims = rest
                    .trim_end_matches(')')
                    .parse::<u32>()
                    .unwrap_or_else(|_| panic!("vector(N): bad dimensions in {other}"));
                return json!({"type": "vector", "dimensions": dims});
            }
            panic!("fixture field type not implemented: {other}")
        }
    }
}

fn build_schema(fx: &Fixture) -> SchemaDef {
    let mut fields = serde_json::Map::new();
    for (name, ty) in fx.schema_fields.as_object().expect("schema_fields object") {
        fields.insert(
            name.clone(),
            verbose_field_type(ty.as_str().expect("type string")),
        );
    }
    let indexes: Vec<Value> = fx
        .schema_indexes
        .iter()
        .map(|ix| {
            let mut v = json!({"name": ix.name, "fields": ix.fields});
            if ix.search {
                v["search"] = json!(true);
            }
            if let Some(vec) = &ix.vector {
                v["vector"] = json!({"dimensions": vec.dimensions});
            }
            v
        })
        .collect();
    let schema_json = json!({
        "tables": {
            &fx.schema_table: {
                "fields": fields,
                "indexes": indexes,
            }
        }
    });
    serde_json::from_value(schema_json).expect("schema parses")
}

/// Create a bare test database (no pre-seeded kanban fixture — `fresh_db`
/// pushes that fixture which would conflict with our items-only schema) and
/// push the fixture's schema + seed docs into it.
///
/// Returns the `TestDb` RAII guard alongside the name+schema so the caller can
/// hold the guard for the test's lifetime — `TestDb::drop` queues async DB
/// cleanup, and if the guard dies inside this function the cleanup worker can
/// race the test (case 1 reads succeed while case 2 hits "relation does not
/// exist" because the schema was already dropped).
async fn seed_db(fx: &Fixture) -> anyhow::Result<(String, SchemaDef, TestDb)> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&pool, &name)
        .await
        .map_err(|e| anyhow::anyhow!("create_database: {e}"))?;
    // RAII wrap so the test DB is scheduled for cleanup even if the test
    // panics. The guard is returned to the caller so its Drop fires only when
    // the test itself ends, not when this function returns.
    let test_db = wrap_test_db(name.clone());
    let schema = build_schema(fx);
    ddl::push_schema(&pool, &name, schema.clone())
        .await
        .map_err(|e| anyhow::anyhow!("push_schema: {e:?}"))?;
    for (i, doc) in fx.seed.iter().enumerate() {
        let txn = Transaction {
            steps: vec![Step::Insert {
                table: fx.schema_table.clone(),
                doc: doc.as_object().expect("seed doc object").clone(),
            }],
        };
        execute_txn(&pool, &name, &schema, &txn, &PrincipalCtx::bypass())
            .await
            .map_err(|e| anyhow::anyhow!("seed insert #{i}: {e:?}"))?;
    }
    Ok((name, schema, test_db))
}

/// Project a doc down to (name, status, order) — drops system fields so id
/// minting doesn't enter the comparison.
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

/// Compare two JSON values, treating two numbers as equal when their f64
/// representations match (so SQL `numeric` `6` and client `f64` `6.0` agree on
/// aggregate scalars and group values where the int/float representation
/// diverges across engines).
fn json_eq_numeric(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            let xi = x.as_f64();
            let yi = y.as_f64();
            match (xi, yi) {
                (Some(xf), Some(yf)) => (xf - yf).abs() < f64::EPSILON || xf == yf,
                _ => x == y,
            }
        }
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

/// Assert two values equal under numeric-tolerant comparison, with a message.
fn assert_json_eq_numeric(got: &Value, want: &Value, msg: &str) {
    if !json_eq_numeric(got, want) {
        panic!("{msg}: got {got}, want {want}");
    }
}

#[tokio::test]
async fn golden_vector_parity() -> anyhow::Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter("rtdb_server=debug")
        .try_init();
    let fx = load_fixture();
    let state = test_state().await;
    let pool = state.pool.clone();
    let (db, schema, _test_db) = seed_db(&fx).await?;
    eprintln!("seeded db={db}");

    for case in &fx.cases {
        let q: Query = serde_json::from_value(case.query.clone()).expect("parse Query");
        let result = execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass())
            .await
            .map_err(|e| anyhow::anyhow!("case {}: {e:?}", case.id))?;

        if let Some(scalar) = case.expected_scalar {
            match result {
                QueryResult::Count(n) => {
                    assert_eq!(n, scalar, "{}: expected count {}", case.id, scalar)
                }
                other => panic!("{}: expected Count, got {:?}", case.id, other),
            }
            continue;
        }

        if let Some(want) = &case.expected_value {
            let got = match result {
                QueryResult::Aggregate(v) => v,
                other => panic!("{}: expected Aggregate, got {:?}", case.id, other),
            };
            assert_json_eq_numeric(
                &got,
                want,
                &format!("{}: aggregate scalar mismatch", case.id),
            );
            continue;
        }

        if let Some(want_groups) = &case.expected_groups {
            let groups = match result {
                QueryResult::AggregateGroups(g) => g,
                other => panic!("{}: expected AggregateGroups, got {:?}", case.id, other),
            };
            let got: Vec<Value> = groups
                .iter()
                .map(|g| json!({"key": g.key, "value": g.value}))
                .collect();
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
                assert_eq!(
                    g.get("key").unwrap(),
                    wk,
                    "{}: group {i} key mismatch",
                    case.id
                );
                assert_json_eq_numeric(
                    g.get("value").unwrap(),
                    wv,
                    &format!("{}: group {i} value mismatch", case.id),
                );
            }
            continue;
        }

        if let Some(want) = &case.expected_distinct {
            let got = match result {
                QueryResult::Distinct(v) => v,
                other => panic!("{}: expected Distinct, got {:?}", case.id, other),
            };
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
            let got = match result {
                QueryResult::Docs(docs) => project_list(&docs),
                other => panic!("{}: expected Docs, got {:?}", case.id, other),
            };
            let mut got_sorted = got.clone();
            got_sorted.sort_by_key(|v| v["name"].as_str().unwrap_or("").to_string());
            let mut want = case
                .expected
                .as_ref()
                .and_then(Value::as_array)
                .unwrap_or_else(|| panic!("{}: expected_unordered requires array", case.id))
                .clone();
            want.sort_by_key(|v| v["name"].as_str().unwrap_or("").to_string());
            assert_eq!(got_sorted, want, "{}: unordered mismatch", case.id);
            continue;
        }

        if case.expected_has_next_cursor {
            let page = match result {
                QueryResult::Paginated(p) => p,
                other => panic!("{}: expected Paginated, got {:?}", case.id, other),
            };
            let got = project_list(&page.docs);
            let want = case
                .expected
                .as_ref()
                .and_then(Value::as_array)
                .unwrap_or_else(|| panic!("{}: paginate requires array expected", case.id));
            assert_eq!(got, want.to_vec(), "{}: page mismatch", case.id);
            assert!(
                page.next_cursor.is_some(),
                "{}: expected nextCursor",
                case.id
            );
            continue;
        }

        if let Some(expected) = &case.expected {
            if expected.is_array() {
                let got = match result {
                    QueryResult::Docs(docs) => project_list(&docs),
                    other => panic!("{}: expected Docs, got {:?}", case.id, other),
                };
                assert_eq!(
                    got,
                    expected.as_array().unwrap().to_vec(),
                    "{}: ordered mismatch",
                    case.id
                );
                continue;
            }
            // single-doc terminal (get / first / unique)
            let got = match result {
                QueryResult::Doc(Some(v)) => project(&v),
                QueryResult::Doc(None) => Value::Null,
                other => panic!("{}: expected Doc, got {:?}", case.id, other),
            };
            assert_eq!(got, *expected, "{}: single-doc mismatch", case.id);
            continue;
        }
        panic!("{}: case has no expected shape", case.id);
    }

    Ok(())
}
