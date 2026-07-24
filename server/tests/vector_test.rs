mod common;

use common::test_state;
use rtdb_server::ddl::push_schema;
use rtdb_server::schema::{FieldType, IndexDef, SchemaDef, TableDef, VectorIndexSpec};
use sqlx::Row;
use std::collections::BTreeMap;

fn vector_schema(dim: u32, with_filter: bool) -> SchemaDef {
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
        }),
    }];
    let mut tables = BTreeMap::new();
    tables.insert("docs".to_string(), TableDef { fields, indexes });
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

    // An HNSW index exists on it.
    let idx: (i64,) = sqlx::query_as(
        "SELECT count(*) FROM pg_indexes \
         WHERE schemaname = $1 AND tablename = $2 AND indexname = 'i_docs_by_embedding'",
    )
    .bind(format!("db_{db}"))
    .bind("t_docs")
    .fetch_one(&state.pool)
    .await
    .expect("index row");
    assert_eq!(idx.0, 1);

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
    push_schema(&state.pool, &db, vector_schema(3, false))
        .await
        .expect("push initial vector schema");
    let err = push_schema(&state.pool, &db, vector_schema(4, false)).await;
    assert!(err.is_err(), "changing dimensions must be rejected");
}
