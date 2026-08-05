mod common;

use common::test_state;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::ddl::push_schema;
use rtdb_server::error::ErrorCode;
use rtdb_server::query::{Query, QueryResult, VectorSearchQuery, execute_query};
use rtdb_server::schema::{
    DistanceMetric, FieldType, IndexDef, SchemaDef, TableDef, VectorIndexSpec,
};
use rtdb_server::txn::{Step, Transaction, execute_txn};
use sqlx::Row;
use std::collections::BTreeMap;

fn vector_schema(dim: u32, with_filter: bool) -> SchemaDef {
    vector_schema_with_metric(dim, with_filter, DistanceMetric::Cosine)
}

fn vector_schema_with_metric(dim: u32, with_filter: bool, metric: DistanceMetric) -> SchemaDef {
    let mut fields = BTreeMap::new();
    fields.insert(
        "embedding".to_string(),
        FieldType::Vector { dimensions: dim },
    );
    if with_filter {
        fields.insert("userId".to_string(), FieldType::String);
    }
    let indexes = vec![IndexDef {
        name: "by_embedding".to_string(),
        fields: vec!["embedding".to_string()],
        search: false,
        vector: Some(VectorIndexSpec {
            dimensions: dim,
            filter_fields: if with_filter {
                vec!["userId".to_string()]
            } else {
                vec![]
            },
            metric,
        }),
        unique: false,
        r#where: None,
        language: None,
    }];
    let mut tables = BTreeMap::new();
    tables.insert(
        "docs".to_string(),
        TableDef {
            fields,
            indexes,
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            authorize: None,
        },
    );
    SchemaDef { tables }
}

#[tokio::test]
async fn pgvector_extension_available_after_db_create() {
    let state = test_state().await;
    // fresh_db creates a database, which now runs CREATE EXTENSION vector.
    let db_name = common::fresh_db(&state).await;

    let row = sqlx::query("SELECT extversion FROM pg_extension WHERE extname = 'vector'")
        .fetch_one(&state.pool)
        .await
        .expect("vector extension row");
    let version: String = row.get("extversion");
    assert!(!version.is_empty(), "vector extension installed: {version}");

    // And the cosine-distance operator resolves (proves the extension is usable).
    let dist: f64 = sqlx::query_scalar("SELECT '[1,0,0]'::vector <=> '[0,1,0]'::vector")
        .fetch_one(&state.pool)
        .await
        .expect("cosine distance");
    assert!(
        (dist - 1.0).abs() < 1e-6,
        "orthogonal vectors have cosine distance 1, got {dist}"
    );

    let _ = db_name; // created; isolation by unique name
}

#[tokio::test]
async fn push_schema_creates_vector_column_and_hnsw_index() {
    let state = test_state().await;
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &db)
        .await
        .expect("create database");
    let db = common::wrap_test_db(db);
    push_schema(&state.pool, &db, vector_schema(3, true))
        .await
        .expect("push vector schema");

    // The vector column exists with type vector(3).
    let col: (String,) = sqlx::query_as(
        "SELECT format_type(a.atttypid, a.atttypmod) \
         FROM pg_attribute a JOIN pg_class c ON a.attrelid = c.oid \
         JOIN pg_namespace n ON c.relnamespace = n.oid \
         WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = 'v_by_embedding'",
    )
    .bind(format!("db_{db}"))
    .bind("t_docs")
    .fetch_one(&state.pool)
    .await
    .expect("vector column row");
    assert_eq!(col.0, "vector(3)");

    // An HNSW index exists on it — assert both existence and access method.
    let idx: (String,) = sqlx::query_as(
        "SELECT indexdef FROM pg_indexes \
         WHERE schemaname = $1 AND tablename = $2 AND indexname = 'i_docs_by_embedding'",
    )
    .bind(format!("db_{db}"))
    .bind("t_docs")
    .fetch_one(&state.pool)
    .await
    .expect("index row");
    assert!(
        idx.0.contains("USING hnsw"),
        "vector index should be HNSW, got indexdef: {}",
        idx.0
    );

    // A filterField (userId) gets a typed f_ column; the vector field does NOT.
    let filter_col: (i64,) = sqlx::query_as(
        "SELECT count(*) FROM pg_attribute a \
         JOIN pg_class c ON a.attrelid = c.oid \
         JOIN pg_namespace n ON c.relnamespace = n.oid \
         WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = 'f_userid'",
    )
    .bind(format!("db_{db}"))
    .bind("t_docs")
    .fetch_one(&state.pool)
    .await
    .expect("filter column row");
    assert_eq!(
        filter_col.0, 1,
        "vector filterField should get an f_ column"
    );

    let vec_field_col: (i64,) = sqlx::query_as(
        "SELECT count(*) FROM pg_attribute a \
         JOIN pg_class c ON a.attrelid = c.oid \
         JOIN pg_namespace n ON c.relnamespace = n.oid \
         WHERE n.nspname = $1 AND c.relname = $2 AND a.attname = 'f_embedding'",
    )
    .bind(format!("db_{db}"))
    .bind("t_docs")
    .fetch_one(&state.pool)
    .await
    .expect("vector field column row");
    assert_eq!(
        vec_field_col.0, 0,
        "the vector field must NOT get an f_ column (owned by v_)"
    );
}

#[tokio::test]
async fn changing_vector_dims_is_rejected() {
    let state = test_state().await;
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &db)
        .await
        .expect("create database");
    let db = common::wrap_test_db(db);
    push_schema(&state.pool, &db, vector_schema(3, false))
        .await
        .expect("push initial vector schema");
    let err = push_schema(&state.pool, &db, vector_schema(4, false))
        .await
        .expect_err("changing dimensions must be rejected");
    assert_eq!(
        err.code,
        ErrorCode::BadRequest,
        "dimension change should be a BadRequest: {:?}",
        err
    );
    // Changing dimensions changes both the `embedding` field type
    // (`Vector{dimensions:N}`) and the index spec. The field-type guard in
    // `detect_destructive_changes` fires first, so the message names the field
    // — either way it's a clean BadRequest naming the offending change, not a 500.
    assert!(
        err.message.contains("changed type of field")
            || err.message.contains("changed vector spec"),
        "message names the dimension change: {}",
        err.message
    );
}

fn vec_doc(emb: Vec<f64>) -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({ "embedding": emb })
        .as_object()
        .expect("vec_doc object")
        .clone()
}

async fn vec_db(state: &std::sync::Arc<rtdb_server::AppState>) -> common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    push_schema(&state.pool, &name, vector_schema(3, false))
        .await
        .expect("push vector schema");
    common::wrap_test_db(name)
}

/// Like `vec_db` but with a caller-chosen metric and dimension (ENH-007). The
/// metric-ranking tests need 2-D vectors to construct a case whose ordering
/// differs between cosine and L2/inner-product.
async fn vec_db_with_metric(
    state: &std::sync::Arc<rtdb_server::AppState>,
    dim: u32,
    metric: DistanceMetric,
) -> common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create database");
    push_schema(
        &state.pool,
        &name,
        vector_schema_with_metric(dim, false, metric),
    )
    .await
    .expect("push vector schema");
    common::wrap_test_db(name)
}

/// Task 5: an insert must maintain the `v_<index>` column — writing the doc's
/// vector field into `v_by_embedding` cast as `vector`. The bound value is the
/// JSON array's text form (`[1.0,2.0,3.0]`), which pgvector accepts and
/// normalizes on output to `[1,2,3]` (integer-valued floats render without `.0`).
#[tokio::test]
async fn insert_maintains_vector_column() {
    let state = test_state().await;
    let db = vec_db(&state).await;
    let schema = vector_schema(3, false);
    execute_txn(
        &state.pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "docs".into(),
                doc: vec_doc(vec![1.0, 2.0, 3.0]),
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("insert vector doc");
    let row: (Option<String>,) = sqlx::query_as(&format!(
        "SELECT \"v_by_embedding\"::text FROM \"db_{db}\".\"t_docs\""
    ))
    .fetch_one(&state.pool)
    .await
    .expect("read vector column");
    assert_eq!(row.0.as_deref(), Some("[1,2,3]"));
}

/// Task 5 follow-up: patching a Vector field must also update the `v_<index>`
/// column. If `apply_update`'s vector handling regressed, inserts would keep
/// working (covered above) while embedding patches silently dropped — so this
/// pins the patch path to the same `v_by_embedding` invariant.
#[tokio::test]
async fn patch_maintains_vector_column() {
    let state = test_state().await;
    let db = vec_db(&state).await;
    let schema = vector_schema(3, false);

    let insert_outcome = execute_txn(
        &state.pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "docs".into(),
                doc: vec_doc(vec![1.0, 2.0, 3.0]),
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("insert vector doc");
    let id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("inserted id")
        .to_string();

    let mut fields = serde_json::Map::new();
    fields.insert("embedding".to_string(), serde_json::json!([4.0, 5.0, 6.0]));

    execute_txn(
        &state.pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "docs".into(),
                id,
                fields,
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("patch vector embedding");

    let row: (Option<String>,) = sqlx::query_as(&format!(
        "SELECT \"v_by_embedding\"::text FROM \"db_{db}\".\"t_docs\""
    ))
    .fetch_one(&state.pool)
    .await
    .expect("read vector column after patch");
    assert_eq!(row.0.as_deref(), Some("[4,5,6]"));
}

/// Task 6: vectorSearch ranks rows by cosine distance (`<=>`) ascending and
/// honors `limit`. With query `[1,0,0]` and rows `[1,0,0]`/`[0,1,0]`/`[0.9,0.4,0]`,
/// the closest two are `[1,0,0]` (distance 0) and `[0.9,0.4,0]` (~0.21); the
/// omitted row is `[0,1,0]` (distance 1) — confirming ranking, not insertion.
#[tokio::test]
async fn vector_search_ranks_by_cosine_and_applies_limit() {
    let state = test_state().await;
    let db = vec_db(&state).await;
    let schema = vector_schema(3, false);
    for emb in [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.9, 0.4, 0.0]] {
        execute_txn(
            &state.pool,
            &db,
            &schema,
            &Transaction {
                steps: vec![Step::Insert {
                    table: "docs".into(),
                    doc: vec_doc(emb.to_vec()),
                }],
            },
            &PrincipalCtx::bypass(),
        )
        .await
        .expect("insert vector doc");
    }
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "vectorSearch": {"index": "by_embedding", "vector": [1.0, 0.0, 0.0], "limit": 2}
    }))
    .expect("parse vectorSearch query");
    let res = execute_query(&state.pool, &db, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect("execute vectorSearch");
    let docs = match res {
        QueryResult::Docs(d) => d,
        _ => panic!("expected Docs"),
    };
    assert_eq!(docs.len(), 2, "limit honored");
    assert_eq!(
        docs[0]["embedding"],
        serde_json::json!([1.0, 0.0, 0.0]),
        "identical vector ranks first"
    );
    assert_eq!(
        docs[1]["embedding"],
        serde_json::json!([0.9, 0.4, 0.0]),
        "second-closest ranks next; the farthest ([0,1,0]) is omitted"
    );
}

/// `vectorSearch` rejects a query vector whose length differs from the index's
/// declared dimensions. Otherwise pgvector would 500 at query time.
#[tokio::test]
async fn vector_search_rejects_length_mismatch() {
    let state = test_state().await;
    let db = vec_db(&state).await;
    let schema = vector_schema(3, false);
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "vectorSearch": {"index": "by_embedding", "vector": [1.0, 0.0], "limit": 1}
    }))
    .expect("parse vectorSearch query");
    let err = execute_query(&state.pool, &db, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect_err("length mismatch should be rejected");
    assert!(
        err.message.contains("dimensions"),
        "message names dimensions: {}",
        err.message
    );
}

/// A non-finite query vector (NaN / Infinity) must surface as a clean
/// `BadRequest`, never a 500. serde_json can't carry NaN, so the query is
/// constructed in Rust — the only path that puts a non-finite `f32` into
/// `vs.vector`. The check runs before the vector is bound, so pgvector never
/// sees the value.
#[tokio::test]
async fn vector_search_rejects_non_finite_query_vector() {
    let state = test_state().await;
    let db = vec_db(&state).await;
    let schema = vector_schema(3, false);
    for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
        let q = Query {
            table: "docs".to_string(),
            vector_search: Some(VectorSearchQuery {
                index: "by_embedding".to_string(),
                vector: vec![1.0, bad, 0.0],
                limit: 1,
                filter: BTreeMap::new(),
            }),
            ..empty_query()
        };
        let err = execute_query(&state.pool, &db, &schema, &q, &PrincipalCtx::bypass())
            .await
            .expect_err("non-finite query vector should be rejected");
        assert_eq!(
            err.code,
            ErrorCode::BadRequest,
            "non-finite vector should be BadRequest, got {:?}: {}",
            err.code,
            err.message
        );
        assert!(
            err.message.contains("finite"),
            "message calls out finiteness: {}",
            err.message
        );
    }
}

/// Minimal `Query` with only `table` set; used as a base for Rust-constructed
/// vector queries so the test only spells out the vector terminal.
fn empty_query() -> Query {
    Query {
        table: String::new(),
        get: None,
        index: None,
        eq: Vec::new(),
        gt: None,
        gte: None,
        lt: None,
        lte: None,
        order: None,
        take: None,
        unique: false,
        first: false,
        count: false,
        distinct: false,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        aggregate: None,
    }
}

/// Naming an unknown or non-vector index is a clear BadRequest, never a 500.
#[tokio::test]
async fn vector_search_rejects_unknown_index() {
    let state = test_state().await;
    let db = vec_db(&state).await;
    let schema = vector_schema(3, false);
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "vectorSearch": {"index": "nope", "vector": [1.0, 0.0, 0.0], "limit": 1}
    }))
    .expect("parse vectorSearch query");
    let err = execute_query(&state.pool, &db, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect_err("unknown index should be rejected");
    assert!(
        err.message.contains("vector index 'nope' not found"),
        "message names the missing index: {}",
        err.message
    );
}

/// `limit` outside `1..=256` is a BadRequest — pin the constant so a future
/// bump is a conscious decision, not silent drift.
#[tokio::test]
async fn vector_search_rejects_out_of_range_limit() {
    let state = test_state().await;
    let db = vec_db(&state).await;
    let schema = vector_schema(3, false);
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "vectorSearch": {"index": "by_embedding", "vector": [1.0, 0.0, 0.0], "limit": 0}
    }))
    .expect("parse vectorSearch query");
    let err = execute_query(&state.pool, &db, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect_err("limit < 1 should be rejected");
    assert!(
        err.message.contains("vectorSearch limit"),
        "message names the limit cap: {}",
        err.message
    );
    let q_hi = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "vectorSearch": {"index": "by_embedding", "vector": [1.0, 0.0, 0.0], "limit": 257}
    }))
    .expect("parse vectorSearch query");
    let err = execute_query(&state.pool, &db, &schema, &q_hi, &PrincipalCtx::bypass())
        .await
        .expect_err("limit > 256 should be rejected");
    assert!(
        err.message.contains("vectorSearch limit"),
        "message names the limit cap: {}",
        err.message
    );
}

/// `filter` restricts to rows matching the eq-map over the index's declared
/// filterFields. Two identical embeddings under different `userId`s, filtered
/// to one, must return exactly that one.
#[tokio::test]
async fn vector_search_applies_eq_filter() {
    let state = test_state().await;
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &db)
        .await
        .expect("create database");
    let db = common::wrap_test_db(db);
    push_schema(&state.pool, &db, vector_schema(3, true))
        .await
        .expect("push vector schema with filterField");
    let schema = vector_schema(3, true);
    for (user_id, emb) in [("a", [1.0, 0.0, 0.0]), ("b", [1.0, 0.0, 0.0])] {
        let mut doc = vec_doc(emb.to_vec());
        doc.insert("userId".to_string(), serde_json::json!(user_id));
        execute_txn(
            &state.pool,
            &db,
            &schema,
            &Transaction {
                steps: vec![Step::Insert {
                    table: "docs".into(),
                    doc,
                }],
            },
            &PrincipalCtx::bypass(),
        )
        .await
        .expect("insert vector doc with userId");
    }
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "vectorSearch": {
            "index": "by_embedding",
            "vector": [1.0, 0.0, 0.0],
            "limit": 10,
            "filter": {"userId": "a"}
        }
    }))
    .expect("parse vectorSearch query");
    let res = execute_query(&state.pool, &db, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect("execute vectorSearch with filter");
    let docs = match res {
        QueryResult::Docs(d) => d,
        _ => panic!("expected Docs"),
    };
    assert_eq!(docs.len(), 1, "filter restricts to userId=a");
    assert_eq!(docs[0]["userId"], "a");
}

/// `vectorSearch` cannot be combined with any other terminal — the `take`
/// combination is the subtle one (vectorSearch carries its own limit), so pin
/// it explicitly.
#[tokio::test]
async fn vector_search_rejects_combination_with_take() {
    let state = test_state().await;
    let db = vec_db(&state).await;
    let schema = vector_schema(3, false);
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "vectorSearch": {"index": "by_embedding", "vector": [1.0, 0.0, 0.0], "limit": 1},
        "take": 5
    }))
    .expect("parse vectorSearch query");
    let err = execute_query(&state.pool, &db, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect_err("vectorSearch + take should be rejected");
    assert!(
        err.message.contains("cannot be combined"),
        "message calls out the combination: {}",
        err.message
    );
}

/// Wire shape: `vectorSearch` (camelCase), `vector` as JSON array, optional
/// `filter` map. Round-trips through serde and the snake_case form never
/// appears on the wire.
#[test]
fn vector_search_wire_round_trips() {
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "vectorSearch": {"index": "by_embedding", "vector": [0.1, 0.2], "limit": 5, "filter": {"userId": "u1"}}
    }))
    .expect("parse vectorSearch query");
    let back = serde_json::to_value(&q).expect("serialize query");
    assert_eq!(back["vectorSearch"]["index"], "by_embedding");
    assert_eq!(back["vectorSearch"]["limit"], 5);
    assert_eq!(back["vectorSearch"]["filter"]["userId"], "u1");
    // camelCase on the wire; the snake_case Rust field never appears.
    assert!(back.get("vector_search").is_none());
    // Empty filter is skipped on serialize (default + skip_serializing_if).
    let q_no_filter = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "vectorSearch": {"index": "by_embedding", "vector": [0.1, 0.2], "limit": 5}
    }))
    .expect("parse vectorSearch query without filter");
    let back_nf = serde_json::to_value(&q_no_filter).expect("serialize query");
    assert!(
        back_nf["vectorSearch"].get("filter").is_none(),
        "empty filter should be omitted on the wire"
    );
}

// ===================== hybridSearch (RRF fusion) =====================

/// A schema with BOTH a full-text search index (`search_body` over title+body)
/// and a vector index (`by_embedding` over the 3-dim embedding field). Required
/// for the `hybridSearch` terminal — it fuses ts_rank and cosine distance.
fn hybrid_schema_json() -> serde_json::Value {
    serde_json::json!({"tables":{"docs":{
        "fields":{
            "title":{"type":"string"},
            "body":{"type":"string"},
            "embedding":{"type":"vector","dimensions":3}
        },
        "indexes":[
            {"name":"search_body","fields":["title","body"],"search":true},
            {"name":"by_embedding","fields":["embedding"],"vector":{"dimensions":3}}
        ]
    }}})
}

fn hybrid_schema() -> SchemaDef {
    serde_json::from_value(hybrid_schema_json()).expect("parse hybrid schema")
}

async fn hybrid_db(state: &std::sync::Arc<rtdb_server::AppState>) -> (common::TestDb, SchemaDef) {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create db");
    let schema = hybrid_schema();
    push_schema(&state.pool, &name, schema.clone())
        .await
        .expect("push hybrid schema");
    (common::wrap_test_db(name), schema)
}

fn hybrid_doc(
    title: &str,
    body: &str,
    emb: Vec<f64>,
) -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({ "title": title, "body": body, "embedding": emb })
        .as_object()
        .expect("hybrid doc object")
        .clone()
}

/// Seed three docs: one that matches both the text query AND is vector-near
/// (`[1,0,0]`-ish), one that matches only the text query, and one that is only
/// vector-near (no text match). Under RRF the doc matching BOTH axes must rank
/// above the two single-axis matches.
#[tokio::test]
async fn hybrid_search_fuses_text_and_vector_via_rrf() {
    let state = test_state().await;
    let (db, schema) = hybrid_db(&state).await;
    let pool = &state.pool;
    // Matches text "database" and is vector-close to [1,0,0].
    execute_txn(
        pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "docs".into(),
                doc: hybrid_doc("database intro", "database database", vec![1.0, 0.0, 0.0]),
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("insert both");
    // Matches text only (vector-far from [1,0,0]).
    execute_txn(
        pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "docs".into(),
                doc: hybrid_doc("database notes", "database", vec![0.0, 0.0, 0.0]),
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("insert text-only");
    // Vector-near only (no text match for "database").
    execute_txn(
        pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "docs".into(),
                doc: hybrid_doc("cooking", "recipes", vec![0.9, 0.4, 0.0]),
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("insert vector-only");

    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "hybridSearch": {
            "query": "database",
            "vector": [1.0, 0.0, 0.0],
            "limit": 3
        }
    }))
    .expect("parse hybridSearch query");
    let res = execute_query(pool, &db, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect("execute hybridSearch");
    let docs = match res {
        QueryResult::Docs(d) => d,
        other => panic!("expected Docs, got {other:?}"),
    };
    assert_eq!(docs.len(), 3, "all three candidates surface (union)");
    // The doc matching BOTH text and vector ranks first under RRF.
    assert_eq!(
        docs[0]["title"].as_str(),
        Some("database intro"),
        "both-axis match ranks first: {:?}",
        docs.iter().map(|d| d["title"].as_str()).collect::<Vec<_>>()
    );
}

/// A named search index is honored; an unknown name surfaces as BadRequest.
#[tokio::test]
async fn hybrid_search_auto_selects_and_names_indexes() {
    let state = test_state().await;
    let (db, schema) = hybrid_db(&state).await;
    let pool = &state.pool;
    execute_txn(
        pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "docs".into(),
                doc: hybrid_doc("database", "body", vec![1.0, 0.0, 0.0]),
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("insert");
    // Auto-select both indexes (no names).
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "hybridSearch": {"query": "database", "vector": [1.0, 0.0, 0.0], "limit": 5}
    }))
    .expect("parse auto hybridSearch");
    let res = execute_query(pool, &db, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect("auto-select hybridSearch");
    assert!(matches!(res, QueryResult::Docs(_)));
    // Explicit names resolve the same indexes.
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "hybridSearch": {
            "query": "database",
            "vector": [1.0, 0.0, 0.0],
            "limit": 5,
            "searchIndex": "search_body",
            "vectorIndex": "by_embedding"
        }
    }))
    .expect("parse named hybridSearch");
    let res = execute_query(pool, &db, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect("named hybridSearch");
    assert!(matches!(res, QueryResult::Docs(_)));

    // Unknown search index → BadRequest.
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "hybridSearch": {
            "query": "database", "vector": [1.0, 0.0, 0.0], "limit": 5,
            "searchIndex": "nope"
        }
    }))
    .expect("parse bad-search-index hybridSearch");
    let err = execute_query(pool, &db, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect_err("unknown search index rejected");
    assert_eq!(err.code, ErrorCode::BadRequest);
}

/// A table with only a vector index (no search index) → BadRequest.
#[tokio::test]
async fn hybrid_search_requires_a_search_index() {
    let state = test_state().await;
    let db = vec_db(&state).await; // vector-only schema
    let schema = vector_schema(3, false);
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "hybridSearch": {"query": "database", "vector": [1.0, 0.0, 0.0], "limit": 5}
    }))
    .expect("parse hybridSearch");
    let err = execute_query(&state.pool, &db, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect_err("missing search index rejected");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("search index"),
        "message names the missing search index: {}",
        err.message
    );
}

/// A table with only a search index (no vector index) → BadRequest.
#[tokio::test]
async fn hybrid_search_requires_a_vector_index() {
    let state = test_state().await;
    // Reuse the search_test schema shape (btree + search index, no vector).
    let schema: SchemaDef = serde_json::from_value(serde_json::json!({"tables":{"notes":{
        "fields":{"title":{"type":"string"},"body":{"type":"string"}},
        "indexes":[
            {"name":"by_title","fields":["title"]},
            {"name":"search_body","fields":["title","body"],"search":true}
        ]
    }}}))
    .expect("parse search-only schema");
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create db");
    let name = common::wrap_test_db(name);
    push_schema(&state.pool, &name, schema.clone())
        .await
        .expect("push schema");
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "notes",
        "hybridSearch": {"query": "database", "vector": [1.0, 0.0, 0.0], "limit": 5}
    }))
    .expect("parse hybridSearch");
    let err = execute_query(&state.pool, &name, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect_err("missing vector index rejected");
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("vector index"),
        "message names the missing vector index: {}",
        err.message
    );
}

/// `hybridSearch` + `search` → BadRequest (hybrid IS the combination).
#[tokio::test]
async fn hybrid_search_is_mutually_exclusive_with_search() {
    let state = test_state().await;
    let (db, schema) = hybrid_db(&state).await;
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "hybridSearch": {"query": "database", "vector": [1.0, 0.0, 0.0], "limit": 5},
        "search": {"index": "search_body", "query": "database"}
    }))
    .expect("parse hybrid+search query");
    let err = execute_query(&state.pool, &db, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect_err("hybrid+search rejected");
    assert_eq!(err.code, ErrorCode::BadRequest);
}

/// `hybridSearch` rejects a query vector whose length differs from the index
/// dimensions (mirrors `vectorSearch` length-mismatch guard).
#[tokio::test]
async fn hybrid_search_rejects_length_mismatch() {
    let state = test_state().await;
    let (db, schema) = hybrid_db(&state).await;
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "hybridSearch": {"query": "database", "vector": [1.0, 0.0], "limit": 5}
    }))
    .expect("parse mismatched hybridSearch");
    let err = execute_query(&state.pool, &db, &schema, &q, &PrincipalCtx::bypass())
        .await
        .expect_err("length mismatch rejected");
    assert_eq!(err.code, ErrorCode::BadRequest);
}

/// `hybridSearch` parses camelCase on the wire and round-trips; snake_case
/// Rust field never appears and optional fields are omitted when absent.
#[test]
fn hybrid_search_wire_round_trips() {
    let q = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "hybridSearch": {"query": "hello", "vector": [0.1, 0.2, 0.3], "limit": 10}
    }))
    .expect("parse hybridSearch query");
    let back = serde_json::to_value(&q).expect("serialize query");
    assert_eq!(back["hybridSearch"]["query"], "hello");
    assert_eq!(back["hybridSearch"]["limit"], 10);
    // Optional fields (searchIndex/vectorIndex/k) omitted when absent.
    assert!(
        back["hybridSearch"].get("searchIndex").is_none(),
        "searchIndex omitted when absent"
    );
    assert!(
        back["hybridSearch"].get("vectorIndex").is_none(),
        "vectorIndex omitted when absent"
    );
    assert!(
        back["hybridSearch"].get("k").is_none(),
        "k omitted when absent (defaults to 60 server-side)"
    );
    // snake_case Rust field never appears on the wire.
    assert!(back.get("hybrid_search").is_none());
    // An explicit k/searchIndex/vectorIndex round-trips.
    let q_full = serde_json::from_value::<Query>(serde_json::json!({
        "table": "docs",
        "hybridSearch": {
            "query": "hello", "vector": [0.1, 0.2, 0.3], "limit": 10,
            "searchIndex": "search_body", "vectorIndex": "by_embedding", "k": 42
        }
    }))
    .expect("parse full hybridSearch");
    let back_full = serde_json::to_value(&q_full).expect("serialize");
    assert_eq!(back_full["hybridSearch"]["k"], 42);
    assert_eq!(back_full["hybridSearch"]["searchIndex"], "search_body");
    assert_eq!(back_full["hybridSearch"]["vectorIndex"], "by_embedding");
}

/// ENH-007: pushing a vector schema compiles the HNSW index with the declared
/// metric's opclass — `vector_cosine_ops` / `vector_l2_ops` / `vector_ip_ops`.
#[tokio::test]
async fn push_schema_compiles_declared_metric_opclass() {
    let state = test_state().await;
    for (metric, opclass) in [
        (DistanceMetric::Cosine, "vector_cosine_ops"),
        (DistanceMetric::L2, "vector_l2_ops"),
        (DistanceMetric::Ip, "vector_ip_ops"),
    ] {
        let raw = format!("t{}", uuid::Uuid::now_v7().simple());
        rtdb_server::db::create_database(&state.pool, &raw)
            .await
            .expect("create database");
        let db = common::wrap_test_db(raw);
        push_schema(
            &state.pool,
            &db,
            vector_schema_with_metric(3, false, metric),
        )
        .await
        .expect("push vector schema");
        let idx: (String,) = sqlx::query_as(
            "SELECT indexdef FROM pg_indexes \
             WHERE schemaname = $1 AND tablename = $2 AND indexname = 'i_docs_by_embedding'",
        )
        .bind(format!("db_{db}"))
        .bind("t_docs")
        .fetch_one(&state.pool)
        .await
        .expect("index row");
        assert!(
            idx.0.contains(opclass),
            "metric {:?} should compile opclass {opclass}, got: {}",
            metric,
            idx.0
        );
    }
}

/// ENH-007: vectorSearch ranks by the index's declared distance metric, not
/// always cosine. With query `[1,0]` and rows `[1,1]` + `[3,0]`, the metrics
/// disagree on the nearest neighbor: cosine picks `[3,0]` (same direction,
/// distance 0); L2 picks `[1,1]` (Euclidean 1 < 2); inner product picks `[3,0]`
/// (largest dot product). Each metric's index must surface its matching row.
#[tokio::test]
async fn vector_search_honors_declared_metric() {
    let state = test_state().await;
    for (metric, nearest) in [
        (DistanceMetric::Cosine, vec![3.0, 0.0]),
        (DistanceMetric::L2, vec![1.0, 1.0]),
        (DistanceMetric::Ip, vec![3.0, 0.0]),
    ] {
        let db = vec_db_with_metric(&state, 2, metric).await;
        let schema = vector_schema_with_metric(2, false, metric);
        for emb in [[1.0, 1.0], [3.0, 0.0]] {
            execute_txn(
                &state.pool,
                &db,
                &schema,
                &Transaction {
                    steps: vec![Step::Insert {
                        table: "docs".into(),
                        doc: vec_doc(emb.to_vec()),
                    }],
                },
                &PrincipalCtx::bypass(),
            )
            .await
            .expect("insert vector doc");
        }
        let q = serde_json::from_value::<Query>(serde_json::json!({
            "table": "docs",
            "vectorSearch": {"index": "by_embedding", "vector": [1.0, 0.0], "limit": 1}
        }))
        .expect("parse vectorSearch query");
        let res = execute_query(&state.pool, &db, &schema, &q, &PrincipalCtx::bypass())
            .await
            .expect("execute vectorSearch");
        let docs = match res {
            QueryResult::Docs(d) => d,
            _ => panic!("expected Docs"),
        };
        assert_eq!(docs.len(), 1, "limit honored for {:?}", metric);
        assert_eq!(
            docs[0]["embedding"],
            serde_json::json!(nearest),
            "metric {:?} should rank {nearest:?} first",
            metric
        );
    }
}
