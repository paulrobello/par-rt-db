mod common;

use common::test_state;
use rtdb_server::ddl::push_schema;
use rtdb_server::error::ErrorCode;
use rtdb_server::protocol::ServerMessage;
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::schema::{FieldType, IndexDef, SchemaDef, TableDef, VectorIndexSpec};
use rtdb_server::subs::next_conn_id;
use rtdb_server::txn::{Step, Transaction, execute_txn};
use sqlx::PgPool;
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::time::timeout;

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
        None,
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

/// Fetches a single `notes` doc by id with a bypass caller (sees every row),
/// decoupling write-enforcement tests from the read-path filtering tests.
async fn fetch_doc(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    id: &str,
) -> Option<serde_json::Value> {
    let mut q = notes_query();
    q.get = Some(id.to_string());
    match execute_query(pool, db, schema, &q, None)
        .await
        .expect("fetch_doc query")
    {
        QueryResult::Doc(v) => v,
        other => panic!("expected Doc, got {other:?}"),
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
            None,
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

/// Drains the initial `QueryUpdate` that `subscribe` always sends, asserting
/// it is a `Docs` result (the empty-table initial state in this test).
async fn drain_initial(rx: &mut UnboundedReceiver<ServerMessage>) {
    let msg = timeout(Duration::from_secs(2), rx.recv())
        .await
        .expect("initial QueryUpdate should arrive")
        .expect("channel not closed");
    assert!(
        matches!(msg, ServerMessage::QueryUpdate { .. }),
        "expected initial QueryUpdate, got {msg:?}"
    );
}

// (5) A write by user B does not push B's rows to A's subscription, while B's
// own subscription does receive the new row. Drives the public `Committers`
// API — the same surface ws.rs uses — with mpsc receivers we own. Guards the
// receive calls with `timeout` so the test can never hang on a missing push.
#[tokio::test]
async fn fan_out_does_not_push_cross_user_rows() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &db).await?;
    push_schema(&state.pool, &db, owner_schema()).await?;

    // Alice and Bob each open a `notes` subscription carrying their own owner.
    // `next_conn_id` mints a process-unique ConnId the same way ws.rs does.
    let (alice_tx, mut alice_rx) = mpsc::unbounded_channel::<ServerMessage>();
    let (bob_tx, mut bob_rx) = mpsc::unbounded_channel::<ServerMessage>();
    let q = notes_query();
    state
        .committers
        .subscribe(
            &db,
            next_conn_id(),
            "alice-q".into(),
            q.clone(),
            alice_tx,
            Some("alice".into()),
        )
        .await?;
    state
        .committers
        .subscribe(
            &db,
            next_conn_id(),
            "bob-q".into(),
            q.clone(),
            bob_tx,
            Some("bob".into()),
        )
        .await?;

    // Each subscribe pushes one initial QueryUpdate (empty table here).
    drain_initial(&mut alice_rx).await;
    drain_initial(&mut bob_rx).await;

    // Bob inserts a note owned by bob (userId set directly in the doc; Task 5
    // adds owner stamping at write time, which is not this task's concern).
    let mut doc = serde_json::Map::new();
    doc.insert("title".into(), "bob's note".into());
    doc.insert("userId".into(), "bob".into());
    state
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Insert {
                    table: "notes".into(),
                    doc,
                }],
            },
            None,
        )
        .await?;

    // mutate returned ⇒ fan_out has completed (committer serializes mutate's
    // full write + fan-out before replying). Alice's owner-filtered re-run
    // excluded bob's row ⇒ canonical unchanged ⇒ no push. Bob's re-run
    // included his new row ⇒ exactly one push.
    let alice_extra = timeout(Duration::from_millis(150), alice_rx.recv()).await;
    assert!(
        alice_extra.is_err(),
        "alice must not receive bob's row, but got: {:?}",
        alice_extra.unwrap()
    );

    let bob_msg = timeout(Duration::from_secs(2), bob_rx.recv())
        .await
        .expect("bob should receive his new row")
        .expect("bob channel closed");
    let bob_titles: Vec<String> = match bob_msg {
        // `QueryResult::Docs` is `#[serde(untagged)]` and serializes to a bare
        // JSON array of docs — parse it as such (QueryResult is serialize-only).
        ServerMessage::QueryUpdate { result, .. } => {
            let docs: Vec<serde_json::Value> =
                serde_json::from_value(result).expect("bob push deserializes as Docs array");
            docs.into_iter()
                .map(|d| d["title"].as_str().expect("title").to_string())
                .collect()
        }
        other => panic!("bob expected QueryUpdate, got {other:?}"),
    };
    assert_eq!(bob_titles, vec!["bob's note".to_string()]);
    Ok(())
}

// (6) `search` on an owner-gated table must not surface cross-user rows: each
// user sees only their own matching docs, while a bypass caller sees all. This
// closes the gap left by Task 2's filter-injection (search rejects `q.filter`,
// so the predicate must be added directly to its SQL).
#[tokio::test]
async fn search_filters_to_own_rows() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &db).await?;
    let mut notes_fields = BTreeMap::new();
    notes_fields.insert("title".to_string(), FieldType::String);
    notes_fields.insert("body".to_string(), FieldType::String);
    notes_fields.insert("userId".to_string(), FieldType::String);
    let notes_indexes = vec![IndexDef {
        name: "search_content".into(),
        fields: vec!["title".into(), "body".into()],
        search: true,
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
    let schema = SchemaDef { tables };
    push_schema(&pool, &db, schema.clone()).await?;

    // Both users seed a doc whose title/body match the search term "database".
    for (uid, title) in [("alice", "alice database"), ("bob", "bob database")] {
        let mut doc = serde_json::Map::new();
        doc.insert("title".into(), title.into());
        doc.insert("body".into(), "database".into());
        doc.insert("userId".into(), uid.into());
        execute_txn(
            &pool,
            &db,
            &schema,
            &Transaction {
                steps: vec![Step::Insert {
                    table: "notes".into(),
                    doc,
                }],
            },
            None,
        )
        .await?;
    }

    let q = Query {
        table: "notes".to_string(),
        search: Some(rtdb_server::query::SearchQuery {
            index: "search_content".into(),
            query: "database".into(),
        }),
        ..notes_query()
    };

    let alice_titles = titles(execute_query(&pool, &db, &schema, &q, Some("alice")).await?);
    assert_eq!(alice_titles, vec!["alice database".to_string()]);

    let bob_titles = titles(execute_query(&pool, &db, &schema, &q, Some("bob")).await?);
    assert_eq!(bob_titles, vec!["bob database".to_string()]);

    let mut bypass = titles(execute_query(&pool, &db, &schema, &q, None).await?);
    bypass.sort();
    assert_eq!(
        bypass,
        vec!["alice database".to_string(), "bob database".to_string()]
    );
    Ok(())
}

// (7) Same guarantee for `vectorSearch`: it also rejects `q.filter`, so the
// owner predicate is threaded into its WHERE. Identical embeddings under two
// users — each user's search returns only their own row, bypass returns both.
#[tokio::test]
async fn vector_search_filters_to_own_rows() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &db).await?;
    let mut docs_fields = BTreeMap::new();
    docs_fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 3 });
    docs_fields.insert("userId".to_string(), FieldType::String);
    let docs_indexes = vec![IndexDef {
        name: "by_embedding".into(),
        fields: vec!["embedding".into()],
        search: false,
        vector: Some(VectorIndexSpec {
            dimensions: 3,
            filter_fields: vec![],
        }),
    }];
    let mut tables = BTreeMap::new();
    tables.insert(
        "docs".to_string(),
        TableDef {
            fields: docs_fields,
            indexes: docs_indexes,
            owner_field: Some("userId".into()),
        },
    );
    let schema = SchemaDef { tables };
    push_schema(&pool, &db, schema.clone()).await?;

    // Identical embedding under each user; owner_field is the only distinguisher.
    for uid in ["alice", "bob"] {
        let mut doc = serde_json::Map::new();
        doc.insert("embedding".into(), serde_json::json!([1.0, 0.0, 0.0]));
        doc.insert("userId".into(), uid.into());
        execute_txn(
            &pool,
            &db,
            &schema,
            &Transaction {
                steps: vec![Step::Insert {
                    table: "docs".into(),
                    doc,
                }],
            },
            None,
        )
        .await?;
    }

    let q = Query {
        table: "docs".to_string(),
        vector_search: Some(rtdb_server::query::VectorSearchQuery {
            index: "by_embedding".into(),
            vector: vec![1.0, 0.0, 0.0],
            limit: 10,
            filter: BTreeMap::new(),
        }),
        ..notes_query()
    };

    let owners = |res: QueryResult| -> Vec<String> {
        match res {
            QueryResult::Docs(docs) => docs
                .into_iter()
                .map(|d| d["userId"].as_str().expect("userId").to_string())
                .collect(),
            other => panic!("expected Docs, got {other:?}"),
        }
    };

    assert_eq!(
        owners(execute_query(&pool, &db, &schema, &q.clone(), Some("alice")).await?),
        vec!["alice".to_string()]
    );
    assert_eq!(
        owners(execute_query(&pool, &db, &schema, &q.clone(), Some("bob")).await?),
        vec!["bob".to_string()]
    );
    let mut bypass = owners(execute_query(&pool, &db, &schema, &q, None).await?);
    bypass.sort();
    assert_eq!(bypass, vec!["alice".to_string(), "bob".to_string()]);
    Ok(())
}

// (8) On insert into an owner-gated table, the server stamps the caller's
// identity into `userId` even when the client omitted it — the stored doc is
// owned by the caller, not by "nobody".
#[tokio::test]
async fn insert_auto_stamps_owner() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    let mut doc = serde_json::Map::new();
    doc.insert("title".into(), "untitled".into());
    // userId deliberately omitted — server must stamp it.
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "notes".into(),
                doc,
            }],
        },
        Some("alice"),
    )
    .await
    .expect("insert should succeed");
    let id = outcome.results[0]["id"].as_str().expect("id").to_string();

    let doc = fetch_doc(&pool, &db, &schema, &id)
        .await
        .expect("doc present");
    assert_eq!(doc["userId"].as_str(), Some("alice"));
    assert_eq!(doc["title"].as_str(), Some("untitled"));
    Ok(())
}

// (9) A user cannot create a row owned by someone else: even with `userId`
// explicitly set to "bob" in the insert doc, the server overwrites it with
// the caller's identity ("alice"). The stamp is unforgeable.
#[tokio::test]
async fn insert_cannot_forge_another_users_owner() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    let mut doc = serde_json::Map::new();
    doc.insert("title".into(), "forgery".into());
    doc.insert("userId".into(), "bob".into()); // attempted forgery
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "notes".into(),
                doc,
            }],
        },
        Some("alice"),
    )
    .await
    .expect("insert should succeed");
    let id = outcome.results[0]["id"].as_str().expect("id").to_string();

    let doc = fetch_doc(&pool, &db, &schema, &id)
        .await
        .expect("doc present");
    assert_eq!(
        doc["userId"].as_str(),
        Some("alice"),
        "server must overwrite the client-supplied owner"
    );
    Ok(())
}

// (10) Patching a doc you don't own is Forbidden, AND the whole transaction
// rolls back — including a preceding step that would have succeeded on its
// own (here: alice inserting her own note, which the server stamps). This is
// the security guarantee: the ownership check runs inside the sqlx txn, so a
// `Forbidden` from any step returns via `?` before `tx.commit()` and reverts
// every prior effect. No partial write, no TOCTOU window.
#[tokio::test]
async fn patch_on_unowned_doc_is_forbidden_and_atomic() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    let bob_id = seed_note(&pool, &db, &schema, "bob's note", "bob").await;

    // Step 1 would succeed on its own (alice inserts her own note; stamped).
    // Step 2 is a Forbidden patch of bob's note. The combined txn must fail
    // AND roll back step 1.
    let mut alice_insert_doc = serde_json::Map::new();
    alice_insert_doc.insert("title".into(), "alice temp".into());
    let mut patch_fields = serde_json::Map::new();
    patch_fields.insert("title".into(), "hacked".into());
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![
                Step::Insert {
                    table: "notes".into(),
                    doc: alice_insert_doc,
                },
                Step::Patch {
                    table: "notes".into(),
                    id: bob_id.clone(),
                    fields: patch_fields,
                },
            ],
        },
        Some("alice"),
    )
    .await
    .expect_err("patch on unowned doc must fail");

    assert_eq!(
        err.code,
        ErrorCode::Forbidden,
        "expected FORBIDDEN, got {:?}: {}",
        err.code,
        err.message
    );

    // Atomicity: bob's note is unchanged...
    let bob_doc = fetch_doc(&pool, &db, &schema, &bob_id)
        .await
        .expect("bob present");
    assert_eq!(bob_doc["title"].as_str(), Some("bob's note"));

    // ...and alice's preceding insert was rolled back (no "alice temp" row).
    let mut q = notes_query();
    q.take = Some(100);
    let all = titles(execute_query(&pool, &db, &schema, &q, None).await?);
    assert!(
        !all.iter().any(|t| t == "alice temp"),
        "preceding insert must be rolled back, got {all:?}"
    );
    assert_eq!(all, vec!["bob's note".to_string()]);
    Ok(())
}

// (11) Delete and Replace on a doc you don't own are Forbidden, and the
// target survives untouched.
#[tokio::test]
async fn delete_and_replace_on_unowned_doc_are_forbidden() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    let bob_id = seed_note(&pool, &db, &schema, "bob's note", "bob").await;

    // Delete by alice -> Forbidden.
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Delete {
                table: "notes".into(),
                id: bob_id.clone(),
            }],
        },
        Some("alice"),
    )
    .await
    .expect_err("delete on unowned doc must fail");
    assert_eq!(err.code, ErrorCode::Forbidden);

    // Bob's note survived the delete attempt.
    let bob_doc = fetch_doc(&pool, &db, &schema, &bob_id)
        .await
        .expect("bob present after delete attempt");
    assert_eq!(bob_doc["title"].as_str(), Some("bob's note"));

    // Replace by alice -> Forbidden.
    let mut replace_doc = serde_json::Map::new();
    replace_doc.insert("title".into(), "hacked".into());
    replace_doc.insert("userId".into(), "alice".into());
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Replace {
                table: "notes".into(),
                id: bob_id.clone(),
                doc: replace_doc,
            }],
        },
        Some("alice"),
    )
    .await
    .expect_err("replace on unowned doc must fail");
    assert_eq!(err.code, ErrorCode::Forbidden);

    // Bob's note survived the replace attempt too.
    let bob_doc = fetch_doc(&pool, &db, &schema, &bob_id)
        .await
        .expect("bob present after replace attempt");
    assert_eq!(bob_doc["title"].as_str(), Some("bob's note"));
    assert_eq!(bob_doc["userId"].as_str(), Some("bob"));
    Ok(())
}

// (12) Upsert exercises both write-enforcement branches: the insert branch
// (no match) stamps the caller as owner, and the update branch (match on an
// existing doc) requires the caller to already own that doc.
#[tokio::test]
async fn upsert_insert_branch_stamps_and_update_branch_checks_owner() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;

    // (a) No match -> insert branch stamps alice (overwriting the forged
    // userId="bob" in the insert doc).
    let mut insert_doc = serde_json::Map::new();
    insert_doc.insert("title".into(), "alice's upsert".into());
    insert_doc.insert("userId".into(), "bob".into()); // attempted forgery
    let empty_patch = serde_json::Map::new();
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Upsert {
                table: "notes".into(),
                index: "by_user".into(),
                eq: vec!["alice".into()],
                insert: insert_doc,
                patch: empty_patch,
            }],
        },
        Some("alice"),
    )
    .await
    .expect("upsert insert should succeed");
    assert_eq!(outcome.results[0]["inserted"].as_bool(), Some(true));
    let alice_id = outcome.results[0]["id"].as_str().expect("id").to_string();
    let doc = fetch_doc(&pool, &db, &schema, &alice_id)
        .await
        .expect("alice doc present");
    assert_eq!(
        doc["userId"].as_str(),
        Some("alice"),
        "insert branch must stamp caller"
    );

    // (b) Match on bob's doc by alice -> update branch -> Forbidden.
    let bob_id = seed_note(&pool, &db, &schema, "bob's note", "bob").await;
    let mut patch_fields = serde_json::Map::new();
    patch_fields.insert("title".into(), "hacked".into());
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Upsert {
                table: "notes".into(),
                index: "by_user".into(),
                eq: vec!["bob".into()],
                insert: serde_json::Map::new(),
                patch: patch_fields,
            }],
        },
        Some("alice"),
    )
    .await
    .expect_err("upsert update on unowned doc must fail");
    assert_eq!(err.code, ErrorCode::Forbidden);

    // Bob's note is unchanged.
    let bob_doc = fetch_doc(&pool, &db, &schema, &bob_id)
        .await
        .expect("bob present");
    assert_eq!(bob_doc["title"].as_str(), Some("bob's note"));
    Ok(())
}

// (13) A bypass caller (owner=None — machine token / scheduled job) is exempt
// from ownership enforcement: it can patch a doc it doesn't "own" because it
// owns everything. Preserves today's machine-token full-access behavior.
#[tokio::test]
async fn machine_bypass_ignores_ownership() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    let bob_id = seed_note(&pool, &db, &schema, "bob's note", "bob").await;

    let mut patch_fields = serde_json::Map::new();
    patch_fields.insert("title".into(), "machine-changed".into());
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "notes".into(),
                id: bob_id.clone(),
                fields: patch_fields,
            }],
        },
        None, // bypass — machine token / scheduled job
    )
    .await
    .expect("bypass patch should succeed");

    let doc = fetch_doc(&pool, &db, &schema, &bob_id)
        .await
        .expect("doc present");
    assert_eq!(doc["title"].as_str(), Some("machine-changed"));
    assert_eq!(
        doc["userId"].as_str(),
        Some("bob"),
        "bypass must not restamp"
    );
    Ok(())
}
