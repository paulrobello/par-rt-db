mod common;

use common::test_state;
use rtdb_server::ddl::push_schema;
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::schema::{FieldType, IndexDef, SchemaDef, TableDef};
use rtdb_server::txn::{Step, Transaction, execute_txn};
use sqlx::PgPool;
use std::collections::BTreeMap;

/// A schema with an owner-gated `notes` table (`ownerField: "userId"`) and a
/// plain `open` table (no owner_field). Mirrors the field-by-field `Query`
/// construction style of `query_test.rs` (`Query` does not impl `Default`).
fn owner_schema() -> SchemaDef {
    let mut notes_fields = BTreeMap::new();
    notes_fields.insert("title".to_string(), FieldType::String);
    notes_fields.insert("userId".to_string(), FieldType::String);
    let notes_indexes = vec![IndexDef {
        name: "by_user".into(),
        fields: vec!["userId".into()],
        search: false,
        vector: None,
    }];
    let mut tables = BTreeMap::new();
    tables.insert(
        "notes".to_string(),
        TableDef {
            fields: notes_fields,
            indexes: notes_indexes,
            owner_field: Some("userId".into()),
        },
    );
    let mut open_fields = BTreeMap::new();
    open_fields.insert("name".to_string(), FieldType::String);
    tables.insert(
        "open".to_string(),
        TableDef {
            fields: open_fields,
            indexes: vec![],
            owner_field: None,
        },
    );
    SchemaDef { tables }
}

/// All fields are `pub`, so build a `User` principal inline; `expires_at`
/// is `i64::MAX` so the session never expires (we don't call `authorize` on
/// the direct-executor path, but keep it honest anyway).
#[allow(dead_code)]
fn user(id: &str) -> rtdb_server::auth::Principal {
    rtdb_server::auth::Principal::User {
        user_id: id.into(),
        email: format!("{id}@x"),
        name: None,
        expires_at: i64::MAX,
        github_id: None,
        github_login: None,
    }
}

/// Creates a fresh uniquely-named database and pushes the owner schema.
async fn setup() -> (sqlx::PgPool, String, SchemaDef) {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &db).await.unwrap();
    let schema = owner_schema();
    push_schema(&pool, &db, owner_schema()).await.unwrap();
    (pool, db, schema)
}

/// Inserts a `notes` row owned by `uid` and returns the new doc id.
async fn seed_note(pool: &PgPool, db: &str, schema: &SchemaDef, title: &str, uid: &str) -> String {
    let mut doc = serde_json::Map::new();
    doc.insert("title".into(), title.into());
    doc.insert("userId".into(), uid.into());
    let outcome = execute_txn(
        pool,
        db,
        schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "notes".into(),
                doc,
            }],
        },
    )
    .await
    .expect("seed insert");
    outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string()
}

/// Collects the `title` of each doc in a `QueryResult::Docs`.
fn titles(res: QueryResult) -> Vec<String> {
    match res {
        QueryResult::Docs(docs) => docs
            .into_iter()
            .map(|d| d["title"].as_str().expect("title").to_string())
            .collect(),
        other => panic!("expected Docs, got {other:?}"),
    }
}

/// Builds a `notes` query with every field spelled out (no `..Default::default()`
/// because `Query` does not derive `Default`). Overridable by the caller.
fn notes_query() -> Query {
    Query {
        table: "notes".to_string(),
        get: None,
        index: None,
        eq: vec![],
        gt: None,
        gte: None,
        lt: None,
        lte: None,
        order: None,
        take: None,
        unique: false,
        first: false,
        count: false,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
    }
}

// (1) An authenticated user sees only their own rows on an owner-gated table.
#[tokio::test]
async fn user_reads_only_own_rows_on_owner_table() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    seed_note(&pool, &db, &schema, "a1", "alice").await;
    seed_note(&pool, &db, &schema, "a2", "alice").await;
    seed_note(&pool, &db, &schema, "b1", "bob").await;

    let mut q = notes_query();
    q.take = Some(100);
    let res = execute_query(&pool, &db, &schema, &q, Some("alice")).await?;

    let mut got = titles(res);
    got.sort();
    assert_eq!(got, vec!["a1".to_string(), "a2".to_string()]);
    Ok(())
}

// (2) A bypass caller (machine token / scheduled job — `owner = None`) sees
// every row regardless of owner_field, preserving today's behavior.
#[tokio::test]
async fn bypass_owner_reads_all_rows() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    seed_note(&pool, &db, &schema, "a1", "alice").await;
    seed_note(&pool, &db, &schema, "a2", "alice").await;
    seed_note(&pool, &db, &schema, "b1", "bob").await;

    let mut q = notes_query();
    q.take = Some(100);
    let res = execute_query(&pool, &db, &schema, &q, None).await?;

    let mut got = titles(res);
    got.sort();
    assert_eq!(
        got,
        vec!["a1".to_string(), "a2".to_string(), "b1".to_string()]
    );
    Ok(())
}

// (3) A `get(id)` point read of an unowned doc reads as absent (`Doc(None)`),
// while the owner reads `Doc(Some)` — silent filter, Convex-like.
#[tokio::test]
async fn get_point_read_filters_unowned() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    let id = seed_note(&pool, &db, &schema, "alice's note", "alice").await;

    let mut q = notes_query();
    q.get = Some(id.clone());

    let bob_res = execute_query(&pool, &db, &schema, &q, Some("bob")).await?;
    match bob_res {
        QueryResult::Doc(None) => {}
        other => panic!("bob get(alice's doc): expected Doc(None), got {other:?}"),
    }

    let alice_res = execute_query(&pool, &db, &schema, &q, Some("alice")).await?;
    match alice_res {
        QueryResult::Doc(Some(v)) => assert_eq!(v["title"], "alice's note"),
        other => panic!("alice get(own doc): expected Doc(Some), got {other:?}"),
    }
    Ok(())
}

// (4) A table without an owner_field is unaffected by the owner argument:
// `Some("alice")` still returns every `open` row.
#[tokio::test]
async fn non_owner_table_is_unaffected_by_owner() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;

    // seed two open rows
    for name in ["one", "two"] {
        let mut doc = serde_json::Map::new();
        doc.insert("name".into(), name.into());
        execute_txn(
            &pool,
            &db,
            &schema,
            &Transaction {
                steps: vec![Step::Insert {
                    table: "open".into(),
                    doc,
                }],
            },
        )
        .await?;
    }

    let q = Query {
        table: "open".to_string(),
        get: None,
        index: None,
        eq: vec![],
        gt: None,
        gte: None,
        lt: None,
        lte: None,
        order: None,
        take: Some(100),
        unique: false,
        first: false,
        count: false,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
    };
    let res = execute_query(&pool, &db, &schema, &q, Some("alice")).await?;
    let mut got: Vec<String> = match res {
        QueryResult::Docs(docs) => docs
            .into_iter()
            .map(|d| d["name"].as_str().expect("name").to_string())
            .collect(),
        other => panic!("expected Docs, got {other:?}"),
    };
    got.sort();
    assert_eq!(got, vec!["one".to_string(), "two".to_string()]);
    Ok(())
}
