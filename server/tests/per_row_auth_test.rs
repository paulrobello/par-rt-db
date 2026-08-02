mod common;

use common::{admin_post, mint_user_session, spawn_app, test_state};
use futures_util::{SinkExt, StreamExt};
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::ddl::push_schema;
use rtdb_server::error::ErrorCode;
use rtdb_server::protocol::ServerMessage;
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::schema::{FieldType, IndexDef, SchemaDef, TableDef, VectorIndexSpec};
use rtdb_server::subs::next_conn_id;
use rtdb_server::txn::{Step, Transaction, execute_txn};
use serde_json::json;
use sqlx::PgPool;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

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
        unique: false,
        r#where: None,
    }];
    let mut tables = BTreeMap::new();
    tables.insert(
        "notes".to_string(),
        TableDef {
            fields: notes_fields,
            indexes: notes_indexes,
            owner_field: Some("userId".into()),
            collaborators_field: None,
            ttl: None,
            authorize: None,
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
            collaborators_field: None,
            ttl: None,
            authorize: None,
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
        &PrincipalCtx::bypass(),
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
    match execute_query(pool, db, schema, &q, &PrincipalCtx::bypass())
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
        distinct: false,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        aggregate: None,
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
    let res = execute_query(
        &pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await?;

    let mut got = titles(res);
    got.sort();
    assert_eq!(got, vec!["a1".to_string(), "a2".to_string()]);
    Ok(())
}

// (1a) `owner_filter`'s `And`-composition branch: a query that supplies BOTH a
// client `filter` AND is owner-gated must AND-compose the two predicates. This
// is the one path that threads client-filter binds, then the owner bind, then
// LIMIT — a bind-offset regression would error or surface the wrong rows. To
// make the owner predicate load-bearing under the client filter, bob is seeded
// with a row whose title ALSO equals "a-keep": dropping the owner filter would
// then return bob's row too, failing the assert. "a-drop" is alice's own but
// fails the client filter, proving the client predicate is also applied.
#[tokio::test]
async fn owner_filter_composes_with_client_filter() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    seed_note(&pool, &db, &schema, "a-keep", "alice").await;
    seed_note(&pool, &db, &schema, "a-drop", "alice").await;
    seed_note(&pool, &db, &schema, "a-keep", "bob").await;

    let mut q = notes_query();
    q.take = Some(100);
    q.filter = Some(rtdb_server::query::FilterExpr::Eq {
        field: "title".into(),
        value: serde_json::json!("a-keep"),
    });
    let res = execute_query(
        &pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await?;

    let mut got = titles(res);
    got.sort();
    assert_eq!(got, vec!["a-keep".to_string()]);
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
    let res = execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass()).await?;

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

    let bob_res = execute_query(
        &pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx {
            user_id: Some("bob".to_string()),
            email: None,
        },
    )
    .await?;
    match bob_res {
        QueryResult::Doc(None) => {}
        other => panic!("bob get(alice's doc): expected Doc(None), got {other:?}"),
    }

    let alice_res = execute_query(
        &pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await?;
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
            &PrincipalCtx::bypass(),
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
        distinct: false,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        aggregate: None,
    };
    let res = execute_query(
        &pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await?;
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
        .realtime
        .committers
        .subscribe(
            &db,
            next_conn_id(),
            "alice-q".into(),
            q.clone(),
            alice_tx,
            PrincipalCtx {
                user_id: Some("alice".to_string()),
                email: None,
            },
        )
        .await?;
    state
        .realtime
        .committers
        .subscribe(
            &db,
            next_conn_id(),
            "bob-q".into(),
            q.clone(),
            bob_tx,
            PrincipalCtx {
                user_id: Some("bob".to_string()),
                email: None,
            },
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
        .realtime
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
            PrincipalCtx::bypass(),
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
        unique: false,
        r#where: None,
    }];
    let mut tables = BTreeMap::new();
    tables.insert(
        "notes".to_string(),
        TableDef {
            fields: notes_fields,
            indexes: notes_indexes,
            owner_field: Some("userId".into()),
            collaborators_field: None,
            ttl: None,
            authorize: None,
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
            &PrincipalCtx::bypass(),
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

    let alice_titles = titles(
        execute_query(
            &pool,
            &db,
            &schema,
            &q,
            &PrincipalCtx {
                user_id: Some("alice".to_string()),
                email: None,
            },
        )
        .await?,
    );
    assert_eq!(alice_titles, vec!["alice database".to_string()]);

    let bob_titles = titles(
        execute_query(
            &pool,
            &db,
            &schema,
            &q,
            &PrincipalCtx {
                user_id: Some("bob".to_string()),
                email: None,
            },
        )
        .await?,
    );
    assert_eq!(bob_titles, vec!["bob database".to_string()]);

    let mut bypass = titles(execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass()).await?);
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
        unique: false,
        r#where: None,
    }];
    let mut tables = BTreeMap::new();
    tables.insert(
        "docs".to_string(),
        TableDef {
            fields: docs_fields,
            indexes: docs_indexes,
            owner_field: Some("userId".into()),
            collaborators_field: None,
            ttl: None,
            authorize: None,
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
            &PrincipalCtx::bypass(),
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
        owners(
            execute_query(
                &pool,
                &db,
                &schema,
                &q.clone(),
                &PrincipalCtx {
                    user_id: Some("alice".to_string()),
                    email: None
                }
            )
            .await?
        ),
        vec!["alice".to_string()]
    );
    assert_eq!(
        owners(
            execute_query(
                &pool,
                &db,
                &schema,
                &q.clone(),
                &PrincipalCtx {
                    user_id: Some("bob".to_string()),
                    email: None
                }
            )
            .await?
        ),
        vec!["bob".to_string()]
    );
    let mut bypass = owners(execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass()).await?);
    bypass.sort();
    assert_eq!(bypass, vec!["alice".to_string(), "bob".to_string()]);
    Ok(())
}

// (7a) `vectorSearch` with a non-empty `filterField` eq-map AND owner-gated:
// the vector path's bind numbering is filter eq-binds ($1..$k), then owner id
// ($k+1), then qvec, then LIMIT — a non-empty filter combined with owner
// enforcement exercises the full bind-offset accumulation. Uses a `category`
// filterField distinct from the `userId` owner_field so the two predicates are
// independent: alice owns a category="y" doc the filter excludes, and bob owns
// a category="x" doc the owner filter excludes — only alice's category="x" doc
// survives both. Identical embeddings so ranking is not a confounder.
#[tokio::test]
async fn vector_search_composes_filter_fields_with_owner() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &db).await?;
    let mut docs_fields = BTreeMap::new();
    docs_fields.insert("embedding".to_string(), FieldType::Vector { dimensions: 3 });
    docs_fields.insert("userId".to_string(), FieldType::String);
    docs_fields.insert("category".to_string(), FieldType::String);
    let docs_indexes = vec![IndexDef {
        name: "by_embedding".into(),
        fields: vec!["embedding".into()],
        search: false,
        vector: Some(VectorIndexSpec {
            dimensions: 3,
            filter_fields: vec!["category".into()],
        }),
        unique: false,
        r#where: None,
    }];
    let mut tables = BTreeMap::new();
    tables.insert(
        "docs".to_string(),
        TableDef {
            fields: docs_fields,
            indexes: docs_indexes,
            owner_field: Some("userId".into()),
            collaborators_field: None,
            ttl: None,
            authorize: None,
        },
    );
    let schema = SchemaDef { tables };
    push_schema(&pool, &db, schema.clone()).await?;

    // Identical embedding under each (user, category) combo; owner_field and
    // category are the only distinguishers.
    for (uid, cat) in [("alice", "x"), ("alice", "y"), ("bob", "x")] {
        let mut doc = serde_json::Map::new();
        doc.insert("embedding".into(), serde_json::json!([1.0, 0.0, 0.0]));
        doc.insert("userId".into(), uid.into());
        doc.insert("category".into(), cat.into());
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
            &PrincipalCtx::bypass(),
        )
        .await?;
    }

    let mut filter = BTreeMap::new();
    filter.insert("category".into(), serde_json::json!("x"));
    let q = Query {
        table: "docs".to_string(),
        vector_search: Some(rtdb_server::query::VectorSearchQuery {
            index: "by_embedding".into(),
            vector: vec![1.0, 0.0, 0.0],
            limit: 10,
            filter,
        }),
        ..notes_query()
    };

    let res = execute_query(
        &pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await?;
    let docs = match res {
        QueryResult::Docs(d) => d,
        other => panic!("expected Docs, got {other:?}"),
    };
    assert_eq!(docs.len(), 1, "owner + filterField compose to one row");
    assert_eq!(docs[0]["userId"].as_str(), Some("alice"));
    assert_eq!(docs[0]["category"].as_str(), Some("x"));
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
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
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
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
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
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
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
    let all = titles(execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass()).await?);
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
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
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
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
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
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
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
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
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
        &PrincipalCtx::bypass(), // bypass — machine token / scheduled job
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

// (14) Post-review fix: a `User` caller cannot transfer ownership of a doc they
// own by Patching `userId` to another user. The Patch arm re-stamps the field
// map with the caller's identity before writing, exactly as Insert does.
#[tokio::test]
async fn patch_cannot_transfer_ownership() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    let alice_id = seed_note(&pool, &db, &schema, "alice's note", "alice").await;

    // Alice tries to give her note to bob via a Patch carrying userId="bob".
    let mut patch_fields = serde_json::Map::new();
    patch_fields.insert("title".into(), "alice's note".into());
    patch_fields.insert("userId".into(), "bob".into()); // attempted transfer
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "notes".into(),
                id: alice_id.clone(),
                fields: patch_fields,
            }],
        },
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await
    .expect("patch on owned doc should succeed");

    // The stored doc is still owned by alice — the field was re-stamped.
    let doc = fetch_doc(&pool, &db, &schema, &alice_id)
        .await
        .expect("doc present");
    assert_eq!(
        doc["userId"].as_str(),
        Some("alice"),
        "Patch must not let a user transfer ownership"
    );

    // And a query as bob does NOT return alice's doc.
    let q = notes_query();
    let bob_titles = titles(
        execute_query(
            &pool,
            &db,
            &schema,
            &q,
            &PrincipalCtx {
                user_id: Some("bob".to_string()),
                email: None,
            },
        )
        .await?,
    );
    assert!(
        !bob_titles.iter().any(|t| t == "alice's note"),
        "bob must not see alice's note, got {bob_titles:?}"
    );
    Ok(())
}

// (15) Same guarantee for Replace: alice cannot replace her doc with one
// carrying userId="bob"; the server re-stamps the incoming doc with alice.
#[tokio::test]
async fn replace_cannot_transfer_ownership() -> anyhow::Result<()> {
    let (pool, db, schema) = setup().await;
    let alice_id = seed_note(&pool, &db, &schema, "alice's note", "alice").await;

    let mut replace_doc = serde_json::Map::new();
    replace_doc.insert("title".into(), "replaced".into());
    replace_doc.insert("userId".into(), "bob".into()); // attempted transfer
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Replace {
                table: "notes".into(),
                id: alice_id.clone(),
                doc: replace_doc,
            }],
        },
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await
    .expect("replace on owned doc should succeed");

    let doc = fetch_doc(&pool, &db, &schema, &alice_id)
        .await
        .expect("doc present");
    assert_eq!(
        doc["userId"].as_str(),
        Some("alice"),
        "Replace must not let a user transfer ownership"
    );
    assert_eq!(doc["title"].as_str(), Some("replaced"));
    Ok(())
}

// (16) Strongest end-to-end check: bob is subscribed with `owner: Some("bob")`,
// alice inserts+stamps her note, then patches its `userId` to "bob". Without
// the post-review re-stamp, bob's feed would receive alice's doc (the patch
// moved the doc into his owner bucket). With the fix, the patch's owner-change
// is stamped back to alice and bob's feed stays empty. Mirrors the public
// `Committers` API pattern from `fan_out_does_not_push_cross_user_rows`.
#[tokio::test]
async fn patch_owner_change_does_not_inject_into_other_users_feed() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &db).await?;
    let schema = owner_schema();
    push_schema(&pool, &db, schema.clone()).await?;

    // Bob subscribes on `notes` carrying his owner identity.
    let (bob_tx, mut bob_rx) = mpsc::unbounded_channel::<ServerMessage>();
    state
        .realtime
        .committers
        .subscribe(
            &db,
            next_conn_id(),
            "bob-q".into(),
            notes_query(),
            bob_tx,
            PrincipalCtx {
                user_id: Some("bob".to_string()),
                email: None,
            },
        )
        .await?;
    drain_initial(&mut bob_rx).await;

    // Alice inserts her own note (server stamps userId=alice).
    let mut insert_doc = serde_json::Map::new();
    insert_doc.insert("title".into(), "alice's note".into());
    insert_doc.insert("userId".into(), "alice".into());
    let outcome = state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Insert {
                    table: "notes".into(),
                    doc: insert_doc,
                }],
            },
            PrincipalCtx {
                user_id: Some("alice".to_string()),
                email: None,
            },
        )
        .await?;
    let alice_id = outcome.results[0]["id"]
        .as_str()
        .expect("alice insert id")
        .to_string();

    // Bob should not have received alice's insert (cross-user).
    let bob_extra = timeout(Duration::from_millis(150), bob_rx.recv()).await;
    assert!(
        bob_extra.is_err(),
        "bob must not receive alice's insert, but got: {:?}",
        bob_extra.unwrap()
    );

    // Alice now patches her doc trying to set userId="bob" — the re-stamp
    // forces it back to "alice", so the canonical owner-filtered result for
    // bob is unchanged and the committer must NOT push.
    let mut patch_fields = serde_json::Map::new();
    patch_fields.insert("userId".into(), "bob".into()); // attempted injection
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Patch {
                    table: "notes".into(),
                    id: alice_id.clone(),
                    fields: patch_fields,
                }],
            },
            PrincipalCtx {
                user_id: Some("alice".to_string()),
                email: None,
            },
        )
        .await?;

    // The patch was applied (title unchanged) but the doc is STILL alice's.
    let stored = fetch_doc(&pool, &db, &schema, &alice_id)
        .await
        .expect("doc present");
    assert_eq!(
        stored["userId"].as_str(),
        Some("alice"),
        "patch owner-change must be re-stamped back to caller"
    );

    // And bob's feed stays empty: no QueryUpdate for the patch.
    let bob_inject = timeout(Duration::from_millis(250), bob_rx.recv()).await;
    assert!(
        bob_inject.is_err(),
        "bob must not receive alice's patch owner-change, but got: {:?}",
        bob_inject.unwrap()
    );
    Ok(())
}

// ===========================================================================
// End-to-end over the real wire (HTTP + WebSocket) — Task 7 capstone.
//
// These exercise the full property through `spawn_app`: two real OAuth-style
// `Principal::User` sessions minted via `common::mint_user_session`, each
// resolving to its own `user_id`, with allowlisting, schema push, and reads /
// writes all going over HTTP / WS exactly as a client SDK would.
// ===========================================================================

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn ws_connect(addr: SocketAddr) -> WsStream {
    let (ws, _) = connect_async(format!("ws://{addr}/sync"))
        .await
        .expect("connect websocket");
    ws
}

async fn ws_send_json(ws: &mut WsStream, msg: serde_json::Value) {
    ws.send(WsMessage::Text(msg.to_string().into()))
        .await
        .expect("send frame");
}

async fn ws_recv_json(ws: &mut WsStream) -> serde_json::Value {
    match ws.next().await.expect("stream ended").expect("frame ok") {
        WsMessage::Text(text) => serde_json::from_str(&text).expect("parse json"),
        other => panic!("expected text frame, got {other:?}"),
    }
}

async fn ws_auth(ws: &mut WsStream, token: &str, db: &str) -> serde_json::Value {
    ws_send_json(ws, json!({"type": "auth", "token": token, "db": db})).await;
    ws_recv_json(ws).await
}

/// Sends a bearer-token request body to an `/api/*` route.
async fn api_post(
    addr: SocketAddr,
    path: &str,
    token: &str,
    body: serde_json::Value,
) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("http://{addr}{path}"))
        .header("Authorization", format!("Bearer {token}"))
        .json(&body)
        .send()
        .await
        .expect("send api request")
}

/// Sorts the `title` field out of a `/api/query` list result (`QueryResult::Docs`
/// serializes to a bare JSON array of docs).
fn titles_from_list(result: &serde_json::Value) -> Vec<String> {
    let mut titles: Vec<String> = result
        .as_array()
        .expect("result is docs array")
        .iter()
        .map(|d| d["title"].as_str().expect("title").to_string())
        .collect();
    titles.sort();
    titles
}

/// Shared setup for the wire tests: spawns the app, creates a fresh db, pushes
/// the owner-gated `notes` schema via the admin route, mints two real user
/// sessions (alice + bob), and allowlists both emails. Returns `(addr, db_name,
/// alice_token, bob_token)`. Schema seeding is left to the caller.
///
/// `user_id`s are derived from `db_name` (not the bare strings "alice"/"bob")
/// because `rtdb_auth.users.id` is a shared PRIMARY KEY and the three wire
/// tests run concurrently — `alice_uid(db_name)` is the value that must also
/// be written into a seeded doc's `userId` field so the owner filter matches.
fn alice_uid(db_name: &str) -> String {
    format!("alice-{db_name}")
}
fn bob_uid(db_name: &str) -> String {
    format!("bob-{db_name}")
}

async fn wire_setup_two_users() -> (SocketAddr, String, String, String) {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db_name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &db_name)
        .await
        .expect("create db");

    let push = admin_post(
        addr,
        "/admin/push-schema",
        json!({"db": db_name, "schema": serde_json::to_value(owner_schema()).expect("serialize schema")}),
    )
    .await;
    assert_eq!(push.status(), reqwest::StatusCode::OK, "push-schema failed");

    let alice_email = format!("alice-{db_name}@example.com");
    let bob_email = format!("bob-{db_name}@example.com");
    let alice_token = mint_user_session(&state.pool, &alice_uid(&db_name), &alice_email).await;
    let bob_token = mint_user_session(&state.pool, &bob_uid(&db_name), &bob_email).await;
    for email in [&alice_email, &bob_email] {
        let r = admin_post(
            addr,
            "/admin/allowlist",
            json!({"db": db_name, "action": "add", "email": email}),
        )
        .await;
        assert_eq!(r.status(), reqwest::StatusCode::OK, "allowlist add failed");
    }

    (addr, db_name, alice_token, bob_token)
}

// (E1) Over the wire: an authenticated user's `POST /api/query` on an
// owner-gated table returns only their own rows. Two real `Principal::User`
// sessions carry the identity the read path filters on. Seeded via the bypass
// (machine-token) caller so the writes are not themselves gated by this test.
#[tokio::test]
async fn http_query_filters_by_owner_over_the_wire() -> anyhow::Result<()> {
    let state = test_state().await;
    let (addr, db_name, alice_token, bob_token) = wire_setup_two_users().await;

    let schema = owner_schema();
    seed_note(
        &state.pool,
        &db_name,
        &schema,
        "alice's note",
        &alice_uid(&db_name),
    )
    .await;
    seed_note(
        &state.pool,
        &db_name,
        &schema,
        "bob's note",
        &bob_uid(&db_name),
    )
    .await;

    // Alice sees only her row.
    let resp = api_post(
        addr,
        "/api/query",
        &alice_token,
        json!({"db": db_name, "query": {"table": "notes"}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(
        titles_from_list(&body["result"]),
        vec!["alice's note".to_string()]
    );

    // Bob sees only his row.
    let resp = api_post(
        addr,
        "/api/query",
        &bob_token,
        json!({"db": db_name, "query": {"table": "notes"}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(
        titles_from_list(&body["result"]),
        vec!["bob's note".to_string()]
    );
    Ok(())
}

// (E2) Over the wire: `POST /api/mutate` PATCHing a doc the caller doesn't own
// returns 403 FORBIDDEN — the write enforcement tested in (10) now reachable
// through the HTTP transport with a real `Principal::User` session.
#[tokio::test]
async fn http_mutate_forbidden_on_unowned_doc() -> anyhow::Result<()> {
    let state = test_state().await;
    let (addr, db_name, alice_token, _) = wire_setup_two_users().await;

    let schema = owner_schema();
    let bob_id = seed_note(
        &state.pool,
        &db_name,
        &schema,
        "bob's note",
        &bob_uid(&db_name),
    )
    .await;

    let resp = api_post(
        addr,
        "/api/mutate",
        &alice_token,
        json!({"db": db_name, "txn": {"steps": [
            {"op": "patch", "table": "notes", "id": bob_id, "fields": {"title": "hacked"}}
        ]}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], json!("FORBIDDEN"));
    Ok(())
}

// (E3) CAPSTONE — over the reactive WebSocket wire: alice and bob each open a
// WS, authenticate with their own session token, and subscribe to `notes`.
// When bob inserts his own note over his WS, his feed receives it and alice's
// does NOT. This proves owner-gating composes end-to-end: WS auth → principal
// → owner injection on subscribe + mutate → committer fan-out's owner-filtered
// re-run. `timeout` guards every receive so a missing push can't hang the test.
#[tokio::test]
async fn ws_subscription_no_cross_user_push() -> anyhow::Result<()> {
    let (addr, db_name, alice_token, bob_token) = wire_setup_two_users().await;

    // Both open a WS and authenticate with their own session token.
    let mut alice_ws = ws_connect(addr).await;
    let a_auth = ws_auth(&mut alice_ws, &alice_token, &db_name).await;
    assert_eq!(a_auth["type"], json!("authOk"));
    assert_eq!(a_auth["user"]["kind"], json!("user"));

    let mut bob_ws = ws_connect(addr).await;
    let b_auth = ws_auth(&mut bob_ws, &bob_token, &db_name).await;
    assert_eq!(b_auth["type"], json!("authOk"));
    assert_eq!(b_auth["user"]["kind"], json!("user"));

    // Both subscribe to notes (empty table → one initial QueryUpdate each).
    ws_send_json(
        &mut alice_ws,
        json!({"type": "subscribe", "queryId": "alice-q", "query": {"table": "notes"}}),
    )
    .await;
    ws_send_json(
        &mut bob_ws,
        json!({"type": "subscribe", "queryId": "bob-q", "query": {"table": "notes"}}),
    )
    .await;
    for ws in [&mut alice_ws, &mut bob_ws] {
        let init = timeout(Duration::from_secs(2), ws_recv_json(ws))
            .await
            .expect("initial QueryUpdate should arrive");
        assert_eq!(init["type"], json!("queryUpdate"));
    }

    // Bob inserts a note over his WS. `userId` is deliberately omitted — the
    // server stamps it from bob's principal on the mutate path.
    ws_send_json(
        &mut bob_ws,
        json!({"type": "mutate", "mutId": "m1", "txn": {"steps": [
            {"op": "insert", "table": "notes", "doc": {"title": "bob's note"}}
        ]}}),
    )
    .await;

    // Bob must receive his note's QueryUpdate (mutateOk may interleave before
    // it, so loop until we see it).
    let mut saw_bob_note = false;
    for _ in 0..4 {
        let msg = timeout(Duration::from_secs(2), ws_recv_json(&mut bob_ws))
            .await
            .expect("bob should receive mutateOk/queryUpdate within 2s");
        if msg["type"] == "queryUpdate" {
            assert_eq!(msg["queryId"], json!("bob-q"));
            assert_eq!(
                titles_from_list(&msg["result"]),
                vec!["bob's note".to_string()]
            );
            saw_bob_note = true;
            break;
        }
    }
    assert!(saw_bob_note, "bob never received his note's QueryUpdate");

    // Alice must NOT receive bob's note: the owner-filtered re-run left her
    // canonical result unchanged, so no push is emitted. A timeout here is
    // success; a message is the failure (and is printed for diagnosis).
    let alice_extra = timeout(Duration::from_millis(250), ws_recv_json(&mut alice_ws)).await;
    assert!(
        alice_extra.is_err(),
        "alice must not receive bob's note, but got: {:?}",
        alice_extra.unwrap()
    );
    Ok(())
}

// ===========================================================================
// Collaborators (ownerField + collaboratorsField) — additive multi-user share.
//
// A table may declare both `ownerField: "userId"` and `collaboratorsField:
// "collaborators"` (an array-of-strings field). A user may read/write a row if
// they are the owner OR appear in the collaborators array. Machine tokens and
// admin bypass as before. A table WITHOUT `collaboratorsField` behaves
// exactly as in the owner-only tests above (the existing cases all pass
// unchanged).
// ===========================================================================

/// Schema with a `notes` table declaring BOTH ownerField and collaboratorsField
/// (an `Array<String>` of additional user ids admitted on the row).
fn collab_schema() -> SchemaDef {
    let mut notes_fields = BTreeMap::new();
    notes_fields.insert("title".to_string(), FieldType::String);
    notes_fields.insert("userId".to_string(), FieldType::String);
    notes_fields.insert(
        "collaborators".to_string(),
        FieldType::Optional {
            inner: Box::new(FieldType::Array {
                element: Box::new(FieldType::String),
            }),
        },
    );
    let notes_indexes = vec![IndexDef {
        name: "by_user".into(),
        fields: vec!["userId".into()],
        search: false,
        vector: None,
        unique: false,
        r#where: None,
    }];
    let mut tables = BTreeMap::new();
    tables.insert(
        "notes".to_string(),
        TableDef {
            fields: notes_fields,
            indexes: notes_indexes,
            owner_field: Some("userId".into()),
            collaborators_field: Some("collaborators".into()),
            ttl: None,
            authorize: None,
        },
    );
    SchemaDef { tables }
}

/// Creates a fresh uniquely-named database and pushes the collab schema.
async fn setup_collab() -> (sqlx::PgPool, String, SchemaDef) {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &db).await.unwrap();
    let schema = collab_schema();
    push_schema(&pool, &db, collab_schema()).await.unwrap();
    (pool, db, schema)
}

/// Inserts a `notes` row owned by `uid` with the given collaborators list.
async fn seed_collab_note(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    title: &str,
    uid: &str,
    collaborators: &[&str],
) -> String {
    let mut doc = serde_json::Map::new();
    doc.insert("title".into(), title.into());
    doc.insert("userId".into(), uid.into());
    doc.insert(
        "collaborators".into(),
        serde_json::Value::Array(
            collaborators
                .iter()
                .map(|c| serde_json::Value::String((*c).to_string()))
                .collect(),
        ),
    );
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
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("seed insert");
    outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string()
}

// (C1) Owner-only read filtering still applies on a collaborators table: alice
// owns the row and bob is a collaborator — both may see it. carol (neither) is
// filtered out (silent, Convex-like).
#[tokio::test]
async fn collab_owner_and_collaborator_both_read_shared_row() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_collab().await;
    seed_collab_note(&pool, &db, &schema, "shared", "alice", &["bob"]).await;

    let mut q = notes_query();
    q.take = Some(100);

    // Alice (owner) sees it.
    let res = execute_query(
        &pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await?;
    assert_eq!(titles(res), vec!["shared".to_string()]);

    // Bob (collaborator) sees it.
    let res = execute_query(
        &pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx {
            user_id: Some("bob".to_string()),
            email: None,
        },
    )
    .await?;
    assert_eq!(titles(res), vec!["shared".to_string()]);

    // Carol (neither) does not.
    let res = execute_query(
        &pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx {
            user_id: Some("carol".to_string()),
            email: None,
        },
    )
    .await?;
    assert!(titles(res).is_empty());

    // Bypass caller sees everything (machine-token / admin path unchanged).
    let res = execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass()).await?;
    assert_eq!(titles(res), vec!["shared".to_string()]);
    Ok(())
}

// (C2) A row with no collaborators list (missing/empty array) degrades to
// owner-only — the OR predicate short-circuits to "owner matches". This
// preserves the byte-identical-to-today semantics when no collaborators are
// named, even on a table that DECLARES collaboratorsField.
#[tokio::test]
async fn collab_missing_array_degrades_to_owner_only() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_collab().await;
    // Seed a row with NO collaborators array (just title + userId).
    let mut doc = serde_json::Map::new();
    doc.insert("title".into(), "alice-only".into());
    doc.insert("userId".into(), "alice".into());
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
        &PrincipalCtx::bypass(),
    )
    .await?;

    let mut q = notes_query();
    q.take = Some(100);

    // Alice sees it; bob (would-be collaborator) does not — no array to be in.
    assert_eq!(
        titles(
            execute_query(
                &pool,
                &db,
                &schema,
                &q,
                &PrincipalCtx {
                    user_id: Some("alice".to_string()),
                    email: None
                }
            )
            .await?
        ),
        vec!["alice-only".to_string()]
    );
    assert!(
        titles(
            execute_query(
                &pool,
                &db,
                &schema,
                &q,
                &PrincipalCtx {
                    user_id: Some("bob".to_string()),
                    email: None
                }
            )
            .await?
        )
        .is_empty()
    );
    Ok(())
}

// (C3) Point-read (get) honors the same OR semantics: alice (owner) and bob
// (collaborator) each get Doc(Some); carol (neither) gets Doc(None). Silent
// filter, Convex-like.
#[tokio::test]
async fn collab_point_read_filters_non_collaborator() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_collab().await;
    let id = seed_collab_note(&pool, &db, &schema, "shared", "alice", &["bob"]).await;

    let mut q = notes_query();
    q.get = Some(id.clone());

    for uid in ["alice", "bob"] {
        match execute_query(
            &pool,
            &db,
            &schema,
            &q,
            &PrincipalCtx {
                user_id: Some(uid.to_string()),
                email: None,
            },
        )
        .await?
        {
            QueryResult::Doc(Some(v)) => assert_eq!(v["title"], "shared"),
            other => panic!("{uid} get(shared): expected Doc(Some), got {other:?}"),
        }
    }
    match execute_query(
        &pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx {
            user_id: Some("carol".to_string()),
            email: None,
        },
    )
    .await?
    {
        QueryResult::Doc(None) => {}
        other => panic!("carol get(shared): expected Doc(None), got {other:?}"),
    }
    Ok(())
}

// (C4) Write enforcement: a collaborator (bob) may patch/delete a row he is
// listed in; a non-collaborator (carol) gets Forbidden. The ownership pre-check
// runs inside the sqlx txn so a Forbidden from any step rolls back the rest.
#[tokio::test]
async fn collab_collaborator_can_write_non_collaborator_forbidden() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_collab().await;
    let id = seed_collab_note(&pool, &db, &schema, "shared", "alice", &["bob"]).await;

    // Carol (neither) patch -> Forbidden.
    let mut patch_fields = serde_json::Map::new();
    patch_fields.insert("title".into(), "carol-hacked".into());
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "notes".into(),
                id: id.clone(),
                fields: patch_fields,
            }],
        },
        &PrincipalCtx {
            user_id: Some("carol".to_string()),
            email: None,
        },
    )
    .await
    .expect_err("non-collaborator patch must fail");
    assert_eq!(err.code, ErrorCode::Forbidden);

    // Bob (collaborator) patch -> Ok. The patch's stamp_owner re-stamps userId
    // to "bob" on a patch (the field map carries userId for owner_field), so
    // the stored doc remains owner-gated by the original author (alice) — only
    // the title changes.
    let mut patch_fields = serde_json::Map::new();
    patch_fields.insert("title".into(), "bob-patched".into());
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "notes".into(),
                id: id.clone(),
                fields: patch_fields,
            }],
        },
        &PrincipalCtx {
            user_id: Some("bob".to_string()),
            email: None,
        },
    )
    .await
    .expect("collaborator patch should succeed");

    let stored = fetch_doc(&pool, &db, &schema, &id)
        .await
        .expect("doc present");
    assert_eq!(stored["title"].as_str(), Some("bob-patched"));
    // The patch arm re-stamps owner to the caller (bob); this matches the
    // existing per-row-auth guarantee that a user cannot transfer ownership.
    assert_eq!(stored["userId"].as_str(), Some("bob"));

    // Carol delete -> Forbidden.
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Delete {
                table: "notes".into(),
                id: id.clone(),
            }],
        },
        &PrincipalCtx {
            user_id: Some("carol".to_string()),
            email: None,
        },
    )
    .await
    .expect_err("non-collaborator delete must fail");
    assert_eq!(err.code, ErrorCode::Forbidden);

    // Bob (still a collaborator on the now-bob-owned doc) delete -> Ok.
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Delete {
                table: "notes".into(),
                id: id.clone(),
            }],
        },
        &PrincipalCtx {
            user_id: Some("bob".to_string()),
            email: None,
        },
    )
    .await
    .expect("collaborator delete should succeed");
    assert!(fetch_doc(&pool, &db, &schema, &id).await.is_none());
    Ok(())
}

// (C5) Upsert update branch honors collaborator pre-check: bob (collaborator)
// may upsert-update alice's doc; carol (neither) is Forbidden.
#[tokio::test]
async fn collab_upsert_update_branch_checks_collaborator() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_collab().await;
    let _id = seed_collab_note(&pool, &db, &schema, "shared", "alice", &["bob"]).await;

    let mut patch = serde_json::Map::new();
    patch.insert("title".into(), "bob-upserted".into());

    // Carol (neither) -> upsert-update is Forbidden.
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Upsert {
                table: "notes".into(),
                index: "by_user".into(),
                eq: vec!["alice".into()],
                insert: serde_json::Map::new(),
                patch: patch.clone(),
            }],
        },
        &PrincipalCtx {
            user_id: Some("carol".to_string()),
            email: None,
        },
    )
    .await
    .expect_err("non-collaborator upsert-update must fail");
    assert_eq!(err.code, ErrorCode::Forbidden);

    // Bob (collaborator) -> upsert-update Ok.
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Upsert {
                table: "notes".into(),
                index: "by_user".into(),
                eq: vec!["alice".into()],
                insert: serde_json::Map::new(),
                patch,
            }],
        },
        &PrincipalCtx {
            user_id: Some("bob".to_string()),
            email: None,
        },
    )
    .await
    .expect("collaborator upsert-update should succeed");
    assert_eq!(outcome.results[0]["inserted"].as_bool(), Some(false));
    Ok(())
}

// (C6) Subscriptions fan out to BOTH owner AND collaborator when a row is
// shared, but NOT to an unrelated user. Drives the public Committers API; a
// missing push can never hang the test (every receive is timeout-guarded).
#[tokio::test]
async fn collab_fan_out_reaches_owner_and_collaborator() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &db).await?;
    let schema = collab_schema();
    push_schema(&pool, &db, schema.clone()).await?;

    // Alice (the future owner), bob (the future collaborator), and carol
    // (neither) each subscribe with their own owner identity.
    let (alice_tx, mut alice_rx) = mpsc::unbounded_channel::<ServerMessage>();
    let (bob_tx, mut bob_rx) = mpsc::unbounded_channel::<ServerMessage>();
    let (carol_tx, mut carol_rx) = mpsc::unbounded_channel::<ServerMessage>();
    let q = notes_query();
    state
        .realtime
        .committers
        .subscribe(
            &db,
            next_conn_id(),
            "alice-q".into(),
            q.clone(),
            alice_tx,
            PrincipalCtx {
                user_id: Some("alice".to_string()),
                email: None,
            },
        )
        .await?;
    state
        .realtime
        .committers
        .subscribe(
            &db,
            next_conn_id(),
            "bob-q".into(),
            q.clone(),
            bob_tx,
            PrincipalCtx {
                user_id: Some("bob".to_string()),
                email: None,
            },
        )
        .await?;
    state
        .realtime
        .committers
        .subscribe(
            &db,
            next_conn_id(),
            "carol-q".into(),
            q,
            carol_tx,
            PrincipalCtx {
                user_id: Some("carol".to_string()),
                email: None,
            },
        )
        .await?;

    // Drain the three initial QueryUpdate messages (empty table).
    drain_initial(&mut alice_rx).await;
    drain_initial(&mut bob_rx).await;
    drain_initial(&mut carol_rx).await;

    // Insert via the committer (not execute_txn directly) so fan_out fires:
    // alice owns it, bob is listed as a collaborator.
    let mut shared = serde_json::Map::new();
    shared.insert("title".into(), serde_json::Value::String("shared".into()));
    shared.insert("userId".into(), serde_json::Value::String("alice".into()));
    shared.insert(
        "collaborators".into(),
        serde_json::Value::Array(vec![serde_json::Value::String("bob".into())]),
    );
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            Transaction {
                steps: vec![Step::Insert {
                    table: "notes".into(),
                    doc: shared,
                }],
            },
            PrincipalCtx::bypass(),
        )
        .await?;

    // Alice and bob each receive exactly one QueryUpdate carrying the shared
    // row; carol receives nothing (her owner/collab-filtered re-run matched
    // nothing, so no push).
    let alice_msg = timeout(Duration::from_secs(2), alice_rx.recv())
        .await
        .expect("alice (owner) should receive the shared row")
        .expect("alice channel closed");
    let bob_msg = timeout(Duration::from_secs(2), bob_rx.recv())
        .await
        .expect("bob (collaborator) should receive the shared row")
        .expect("bob channel closed");
    for (who, msg) in [("alice", alice_msg), ("bob", bob_msg)] {
        let titles: Vec<String> = match msg {
            ServerMessage::QueryUpdate { result, .. } => {
                let docs: Vec<serde_json::Value> =
                    serde_json::from_value(result).expect("push deserializes as Docs array");
                docs.into_iter()
                    .map(|d| d["title"].as_str().expect("title").to_string())
                    .collect()
            }
            other => panic!("{who} expected QueryUpdate, got {other:?}"),
        };
        assert_eq!(titles, vec!["shared".to_string()], "{who} push");
    }

    let carol_extra = timeout(Duration::from_millis(200), carol_rx.recv()).await;
    assert!(
        carol_extra.is_err(),
        "carol (neither owner nor collaborator) must not receive the shared row, but got: {:?}",
        carol_extra.unwrap()
    );
    Ok(())
}

// (C7) `owner_filter`'s And-composition with a client `filter` still composes
// on a collaborators table: alice's title="x" row is shared with bob; a query
// for title="x" as bob returns it (the OR predicate is appended next to the
// client filter), and a query for title="zz" returns nothing for either.
#[tokio::test]
async fn collab_or_predicate_composes_with_client_filter() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_collab().await;
    seed_collab_note(&pool, &db, &schema, "x", "alice", &["bob"]).await;
    seed_collab_note(&pool, &db, &schema, "y", "alice", &["bob"]).await;

    let mut q = notes_query();
    q.take = Some(100);
    q.filter = Some(rtdb_server::query::FilterExpr::Eq {
        field: "title".into(),
        value: serde_json::json!("x"),
    });

    // Bob (collaborator): the OR predicate admits the row, AND the client
    // filter narrows to title="x" → exactly one match.
    let mut got = titles(
        execute_query(
            &pool,
            &db,
            &schema,
            &q,
            &PrincipalCtx {
                user_id: Some("bob".to_string()),
                email: None,
            },
        )
        .await?,
    );
    got.sort();
    assert_eq!(got, vec!["x".to_string()]);

    // Same query for title="zz" → no rows for bob.
    let mut q2 = notes_query();
    q2.take = Some(100);
    q2.filter = Some(rtdb_server::query::FilterExpr::Eq {
        field: "title".into(),
        value: serde_json::json!("zz"),
    });
    let got = titles(
        execute_query(
            &pool,
            &db,
            &schema,
            &q2,
            &PrincipalCtx {
                user_id: Some("bob".to_string()),
                email: None,
            },
        )
        .await?,
    );
    assert!(got.is_empty());
    Ok(())
}

// ============================================================================
// Task 6: `authorize` predicate enforcement on the read scan path.
// ============================================================================

/// Schema with a `posts` table declaring `authorize: Or[Eq{owner,$user},
/// Eq{visibility,"public"}]` — a row is visible to a user when they own it OR
/// it is public. No `ownerField`/`collaboratorsField`, so owner/collab
/// enforcement is inert; the `authorize` predicate is the sole gate.
fn authorize_schema() -> SchemaDef {
    use rtdb_server::query::FilterExpr;
    let mut posts_fields = BTreeMap::new();
    posts_fields.insert("body".to_string(), FieldType::String);
    posts_fields.insert("owner".to_string(), FieldType::String);
    posts_fields.insert("visibility".to_string(), FieldType::String);
    // `by_owner` lets the upsert-update test (T8-4) target a matched doc via the
    // declared index; additive — the T6/T7 read tests do not reference indexes.
    let posts_indexes = vec![IndexDef {
        name: "by_owner".into(),
        fields: vec!["owner".into()],
        search: false,
        vector: None,
        unique: false,
        r#where: None,
    }];
    let authorize = Some(FilterExpr::Or {
        exprs: vec![
            FilterExpr::Eq {
                field: "owner".into(),
                value: json!({"$user": true}),
            },
            FilterExpr::Eq {
                field: "visibility".into(),
                value: json!("public"),
            },
        ],
    });
    let mut tables = BTreeMap::new();
    tables.insert(
        "posts".to_string(),
        TableDef {
            fields: posts_fields,
            indexes: posts_indexes,
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            authorize,
        },
    );
    SchemaDef { tables }
}

/// Creates a fresh uniquely-named database and pushes the authorize schema.
async fn setup_authorize() -> (sqlx::PgPool, String, SchemaDef) {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &db).await.unwrap();
    let schema = authorize_schema();
    push_schema(&pool, &db, schema.clone()).await.unwrap();
    (pool, db, schema)
}

/// Inserts a `posts` row with the given owner/visibility; returns its doc id.
async fn seed_post(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    body: &str,
    owner: &str,
    visibility: &str,
) -> String {
    let mut doc = serde_json::Map::new();
    doc.insert("body".into(), body.into());
    doc.insert("owner".into(), owner.into());
    doc.insert("visibility".into(), visibility.into());
    let outcome = execute_txn(
        pool,
        db,
        schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "posts".into(),
                doc,
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("seed post insert");
    outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string()
}

/// A `posts` query with every field spelled out (mirrors `notes_query`).
fn posts_query() -> Query {
    Query {
        table: "posts".to_string(),
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
        distinct: false,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        aggregate: None,
    }
}

/// Collects the `body` of each doc in a `QueryResult::Docs`.
fn bodies(res: QueryResult) -> Vec<String> {
    match res {
        QueryResult::Docs(docs) => docs
            .into_iter()
            .map(|d| d["body"].as_str().expect("body").to_string())
            .collect(),
        other => panic!("expected Docs, got {other:?}"),
    }
}

// (T6-1) A user querying an `authorize`-gated table sees their own rows (any
// visibility) PLUS every public row (regardless of owner), and never sees
// another user's private rows. Seed the four ownership/visibility combinations
// and assert user A sees exactly {A-private, A-public, B-public}.
#[tokio::test]
async fn authorize_user_sees_own_and_public_not_others_private() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_authorize().await;
    seed_post(&pool, &db, &schema, "a-priv", "alice", "private").await;
    seed_post(&pool, &db, &schema, "a-pub", "alice", "public").await;
    seed_post(&pool, &db, &schema, "b-priv", "bob", "private").await;
    seed_post(&pool, &db, &schema, "b-pub", "bob", "public").await;

    let mut q = posts_query();
    q.take = Some(100);
    let res = execute_query(
        &pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await?;

    let mut got = bodies(res);
    got.sort();
    assert_eq!(
        got,
        vec![
            "a-priv".to_string(),
            "a-pub".to_string(),
            "b-pub".to_string()
        ]
    );
    Ok(())
}

// (T6-2) Bypass callers (machine tokens, admin, scheduled jobs) are NOT subject
// to `authorize` — they see every row. This is the security-default bypass.
#[tokio::test]
async fn authorize_bypass_sees_all_rows() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_authorize().await;
    seed_post(&pool, &db, &schema, "a-priv", "alice", "private").await;
    seed_post(&pool, &db, &schema, "a-pub", "alice", "public").await;
    seed_post(&pool, &db, &schema, "b-priv", "bob", "private").await;
    seed_post(&pool, &db, &schema, "b-pub", "bob", "public").await;

    let mut q = posts_query();
    q.take = Some(100);
    let res = execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass()).await?;

    let mut got = bodies(res);
    got.sort();
    assert_eq!(
        got,
        vec![
            "a-priv".to_string(),
            "a-pub".to_string(),
            "b-priv".to_string(),
            "b-pub".to_string(),
        ]
    );
    Ok(())
}

// (T6-3) `count` terminal also honors `authorize` — user A counts 3 (own two
// plus the one public), not 4.
#[tokio::test]
async fn authorize_count_filters_user() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_authorize().await;
    seed_post(&pool, &db, &schema, "a-priv", "alice", "private").await;
    seed_post(&pool, &db, &schema, "a-pub", "alice", "public").await;
    seed_post(&pool, &db, &schema, "b-priv", "bob", "private").await;
    seed_post(&pool, &db, &schema, "b-pub", "bob", "public").await;

    let mut q = posts_query();
    q.count = true;
    let res = execute_query(
        &pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await?;
    match res {
        QueryResult::Count(n) => assert_eq!(n, 3),
        other => panic!("expected Count, got {other:?}"),
    }

    // Bypass count sees all 4.
    let res = execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass()).await?;
    match res {
        QueryResult::Count(n) => assert_eq!(n, 4),
        other => panic!("expected Count, got {other:?}"),
    }
    Ok(())
}

// (T6-4) Principal markers (`{"$user":true}` / `{"$email":true}`) are valid
// only in a server-declared `authorize`; a client-supplied `.filter()` carrying
// one is rejected at the query boundary with `BadRequest`.
#[tokio::test]
async fn client_filter_rejects_principal_marker() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_authorize().await;
    seed_post(&pool, &db, &schema, "a-pub", "alice", "public").await;

    let mut q = posts_query();
    q.take = Some(100);
    q.filter = Some(rtdb_server::query::FilterExpr::Eq {
        field: "owner".into(),
        value: json!({"$user": true}),
    });
    let err = execute_query(
        &pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await
    .expect_err("client filter with $user marker must be rejected");
    assert_eq!(
        err.code,
        rtdb_server::error::ErrorCode::BadRequest,
        "expected BadRequest, got {err:?}"
    );
    // Explicit marker-rejection message (not the incidental "value must be a
    // string/number/boolean" the jsonb path would emit).
    assert!(
        err.message.contains("principal markers"),
        "expected marker-specific message, got: {}",
        err.message
    );
    Ok(())
}

// ============================================================================
// Task 7: `authorize` predicate enforcement on the point-read (`get`) path.
// ----------------------------------------------------------------------------

// (T7-1) User A `get(B-private-id)` → `null` (silent, Convex-style); user A
// `get(A-private-id)` → the doc; machine/bypass `get(B-private-id)` → the doc.
// Seeds the four ownership/visibility combinations and exercises each principal
// against a private row's id (the case that distinguishes authorize-on-get from
// the prior behavior, where point-read only consulted ownerField/collaborators
// — both absent here, so A could previously fetch B's private doc by id).
#[tokio::test]
async fn authorize_get_filters_unauthorized_user() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_authorize().await;
    let a_priv_id = seed_post(&pool, &db, &schema, "a-priv", "alice", "private").await;
    let _a_pub_id = seed_post(&pool, &db, &schema, "a-pub", "alice", "public").await;
    let b_priv_id = seed_post(&pool, &db, &schema, "b-priv", "bob", "private").await;
    let _b_pub_id = seed_post(&pool, &db, &schema, "b-pub", "bob", "public").await;

    // User A reads B's private doc by id → filtered away (silent Doc(None)).
    let mut q = posts_query();
    q.get = Some(b_priv_id.clone());
    let res = execute_query(
        &pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await?;
    match res {
        QueryResult::Doc(None) => {}
        other => panic!("user A get(B-private) should be silent None, got {other:?}"),
    }

    // User A reads their own private doc by id → returned.
    let mut q = posts_query();
    q.get = Some(a_priv_id.clone());
    let res = execute_query(
        &pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await?;
    match res {
        QueryResult::Doc(Some(d)) => assert_eq!(d["body"].as_str(), Some("a-priv")),
        other => panic!("user A get(A-private) should return the doc, got {other:?}"),
    }

    // Bypass caller reads B's private doc by id → returned (authorize is
    // user-only; machine/admin/scheduled paths enforce nothing here).
    let mut q = posts_query();
    q.get = Some(b_priv_id.clone());
    let res = execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass()).await?;
    match res {
        QueryResult::Doc(Some(d)) => assert_eq!(d["body"].as_str(), Some("b-priv")),
        other => panic!("bypass get(B-private) should return the doc, got {other:?}"),
    }

    Ok(())
}

// (T7-2) A user may read another user's PUBLIC doc by id (the `Or` branch of
// the authorize predicate). Confirms the predicate is evaluated, not blanket
// owner-only filtering — public rows are shared with everyone.
#[tokio::test]
async fn authorize_get_allows_other_users_public() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_authorize().await;
    let b_pub_id = seed_post(&pool, &db, &schema, "b-pub", "bob", "public").await;

    let mut q = posts_query();
    q.get = Some(b_pub_id);
    let res = execute_query(
        &pool,
        &db,
        &schema,
        &q,
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await?;
    match res {
        QueryResult::Doc(Some(d)) => assert_eq!(d["body"].as_str(), Some("b-pub")),
        other => panic!("user A get(B-public) should return the doc, got {other:?}"),
    }
    Ok(())
}

// ============================================================================
// Task 8: `authorize` predicate enforcement on the write pre-check
// (patch/replace/delete/upsert-update → Forbidden).
// ----------------------------------------------------------------------------
// `check_owner`/`check_owner_doc` now also evaluate `table.authorize` against
// the fetched doc when the caller is a user. The `posts` schema declares
// `authorize: Or[Eq{owner,$user}, Eq{visibility,"public"}]` and NO ownerField,
// so the owner/collab check is inert here — this isolates authorize-on-writes
// from the pre-existing ownerField enforcement (covered by tests 10–13).

/// Fetches a single `posts` doc by id with a bypass caller (so the read-path
/// authorize filter doesn't hide it), mirroring `fetch_doc` for `notes`.
async fn fetch_post(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    id: &str,
) -> Option<serde_json::Value> {
    let mut q = posts_query();
    q.get = Some(id.to_string());
    match execute_query(pool, db, schema, &q, &PrincipalCtx::bypass()).await {
        Ok(QueryResult::Doc(d)) => d,
        other => panic!("expected Doc, got {other:?}"),
    }
}

// (T8-1) User A `patch(B-private)` → Forbidden AND the txn aborts atomically:
// a preceding insert by A is rolled back, and B-private is left unchanged.
// The predicate makes B-private invisible to A (A neither owns it nor is it
// public), so the write pre-check rejects before `do_patch` runs.
#[tokio::test]
async fn authorize_patch_on_unauthorized_doc_is_forbidden_and_atomic() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_authorize().await;
    let b_priv_id = seed_post(&pool, &db, &schema, "b-priv", "bob", "private").await;

    // Step 1 would succeed on its own (A inserts own private post). Step 2 is a
    // Forbidden patch of B's private post. The combined txn must fail AND roll
    // back step 1.
    let mut a_insert = serde_json::Map::new();
    a_insert.insert("body".into(), "a-temp".into());
    a_insert.insert("owner".into(), "alice".into());
    a_insert.insert("visibility".into(), "private".into());
    let mut patch_fields = serde_json::Map::new();
    patch_fields.insert("body".into(), "hacked".into());
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![
                Step::Insert {
                    table: "posts".into(),
                    doc: a_insert,
                },
                Step::Patch {
                    table: "posts".into(),
                    id: b_priv_id.clone(),
                    fields: patch_fields,
                },
            ],
        },
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await
    .expect_err("patch on unauthorized doc must fail");
    assert_eq!(
        err.code,
        ErrorCode::Forbidden,
        "expected FORBIDDEN, got {:?}: {}",
        err.code,
        err.message
    );

    // Atomicity: B-private is unchanged...
    let b_doc = fetch_post(&pool, &db, &schema, &b_priv_id)
        .await
        .expect("b-private present");
    assert_eq!(b_doc["body"].as_str(), Some("b-priv"));

    // ...and A's preceding insert was rolled back (no "a-temp" row).
    let mut q = posts_query();
    q.take = Some(100);
    let all = bodies(execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass()).await?);
    assert!(
        !all.iter().any(|b| b == "a-temp"),
        "preceding insert must be rolled back, got {all:?}"
    );
    assert_eq!(all, vec!["b-priv".to_string()]);
    Ok(())
}

// (T8-2) User A `patch(own private)` → ok (owns it). User A `delete(B-public)`
// → ok (public is authorized under the `Or` branch — anyone may mutate a
// public row). Confirms the predicate is evaluated, not blanket owner-only.
#[tokio::test]
async fn authorize_patch_own_and_delete_other_public_succeed() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_authorize().await;
    let a_priv_id = seed_post(&pool, &db, &schema, "a-priv", "alice", "private").await;
    let b_pub_id = seed_post(&pool, &db, &schema, "b-pub", "bob", "public").await;

    // A patches own private doc -> ok.
    let mut patch_fields = serde_json::Map::new();
    patch_fields.insert("body".into(), "a-edited".into());
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "posts".into(),
                id: a_priv_id.clone(),
                fields: patch_fields,
            }],
        },
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await
    .expect("patch own doc should succeed");
    let a_doc = fetch_post(&pool, &db, &schema, &a_priv_id)
        .await
        .expect("a-priv present");
    assert_eq!(a_doc["body"].as_str(), Some("a-edited"));

    // A deletes B's public doc -> ok (public branch of authorize predicate).
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Delete {
                table: "posts".into(),
                id: b_pub_id.clone(),
            }],
        },
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await
    .expect("delete other user's public doc should succeed");
    assert!(
        fetch_post(&pool, &db, &schema, &b_pub_id).await.is_none(),
        "b-public should be deleted"
    );
    Ok(())
}

// (T8-3) Replace and Delete on a doc the user is NOT authorized for (B-private)
// are both Forbidden, and the target survives untouched. Mirrors the ownerField
// test (test 11) for the authorize path.
#[tokio::test]
async fn authorize_replace_and_delete_on_unauthorized_doc_are_forbidden() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_authorize().await;
    let b_priv_id = seed_post(&pool, &db, &schema, "b-priv", "bob", "private").await;

    // Delete by alice on B-private -> Forbidden.
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Delete {
                table: "posts".into(),
                id: b_priv_id.clone(),
            }],
        },
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await
    .expect_err("delete on unauthorized doc must fail");
    assert_eq!(err.code, ErrorCode::Forbidden);

    // B-private survived the delete attempt.
    let b_doc = fetch_post(&pool, &db, &schema, &b_priv_id)
        .await
        .expect("b-private present after delete attempt");
    assert_eq!(b_doc["body"].as_str(), Some("b-priv"));

    // Replace by alice on B-private -> Forbidden.
    let mut replace_doc = serde_json::Map::new();
    replace_doc.insert("body".into(), "hacked".into());
    replace_doc.insert("owner".into(), "alice".into());
    replace_doc.insert("visibility".into(), "private".into());
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Replace {
                table: "posts".into(),
                id: b_priv_id.clone(),
                doc: replace_doc,
            }],
        },
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await
    .expect_err("replace on unauthorized doc must fail");
    assert_eq!(err.code, ErrorCode::Forbidden);

    // B-private survived the replace attempt too.
    let b_doc = fetch_post(&pool, &db, &schema, &b_priv_id)
        .await
        .expect("b-private present after replace attempt");
    assert_eq!(b_doc["body"].as_str(), Some("b-priv"));
    assert_eq!(b_doc["owner"].as_str(), Some("bob"));
    Ok(())
}

// (T8-4) Upsert-update branch: when the matched doc is one the caller is not
// authorized for (B-private), the update is Forbidden — `check_owner_doc`
// evaluates the predicate on the in-hand doc. The insert branch (no match) is
// unaffected (insert enforcement is T9).
#[tokio::test]
async fn authorize_upsert_update_on_unauthorized_doc_is_forbidden() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_authorize().await;
    let b_priv_id = seed_post(&pool, &db, &schema, "b-priv", "bob", "private").await;

    // Match B-private via the `by_owner` index (eq=["bob"]). The update branch
    // hits check_owner_doc, which must reject alice (B-private is not hers and
    // not public).
    let mut insert_doc = serde_json::Map::new();
    insert_doc.insert("body".into(), "ignored".into());
    insert_doc.insert("owner".into(), "bob".into());
    insert_doc.insert("visibility".into(), "private".into());
    let mut patch_fields = serde_json::Map::new();
    patch_fields.insert("body".into(), "hacked".into());
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Upsert {
                table: "posts".into(),
                index: "by_owner".into(),
                eq: vec!["bob".into()],
                insert: insert_doc,
                patch: patch_fields,
            }],
        },
        &PrincipalCtx {
            user_id: Some("alice".to_string()),
            email: None,
        },
    )
    .await
    .expect_err("upsert update on unauthorized doc must fail");
    assert_eq!(err.code, ErrorCode::Forbidden);

    // B-private survived untouched.
    let b_doc = fetch_post(&pool, &db, &schema, &b_priv_id)
        .await
        .expect("b-private present");
    assert_eq!(b_doc["body"].as_str(), Some("b-priv"));
    Ok(())
}

// (T8-5) Bypass callers (machine tokens, admin, scheduled jobs) are NOT subject
// to `authorize` on the write path either — they can patch/delete/replace any
// row. Preserves machine-token full-access behavior on authorize-only tables.
#[tokio::test]
async fn authorize_bypass_write_ignores_predicate() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_authorize().await;
    let b_priv_id = seed_post(&pool, &db, &schema, "b-priv", "bob", "private").await;

    // Bypass patches B-private -> ok.
    let mut patch_fields = serde_json::Map::new();
    patch_fields.insert("body".into(), "machine-changed".into());
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "posts".into(),
                id: b_priv_id.clone(),
                fields: patch_fields,
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("bypass patch should succeed");

    let doc = fetch_post(&pool, &db, &schema, &b_priv_id)
        .await
        .expect("doc present");
    assert_eq!(doc["body"].as_str(), Some("machine-changed"));
    assert_eq!(doc["owner"].as_str(), Some("bob"));
    Ok(())
}

// ===========================================================================
// Task 9 — insert auto-stamp + verify against `authorize`
//
// `authorize` inserts are stamped for every `Eq { field, $user }` leaf
// reachable through `And`/`Or` (the caller's user_id is forced, overwriting any
// client value — unforgeable, like `stamp_owner`), then verified: a predicate
// with no stampable leaf (e.g. `Eq{visibility,"public"}` or
// `Contains{editors,$user}`) stamps nothing and the inserted doc must satisfy
// it from client values alone, else `Forbidden`. These four tests pin the four
// cases of the brief.
// ===========================================================================

/// Schema for the T9 insert-stamp tests: four tables, one per case.
/// - `owned` — `Eq{owner,$user}` (stampable, single leaf).
/// - `or_posts` — `Or[Eq{owner,$user}, Eq{visibility,"public"}]` (stampable via the Or arm).
/// - `public_only` — `Eq{visibility,"public"}` (no `$user` leaf, not stampable).
/// - `edited` — `Contains{editors,$user}` (array-only, not stampable).
fn insert_stamp_schema() -> SchemaDef {
    use rtdb_server::query::FilterExpr;
    let mk_table = |fields: Vec<(String, FieldType)>, authorize: Option<FilterExpr>| -> TableDef {
        let fields: BTreeMap<String, FieldType> = fields.into_iter().collect();
        TableDef {
            fields,
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            authorize,
        }
    };
    let mut tables = BTreeMap::new();
    // (a) Eq{owner,$user} — `by_owner` declared so the Upsert-insert test
    // (T9-upsert-insert) can target a (non-)match via `eq_lookup`; the other
    // owned-table tests do not reference it.
    let mut owned = mk_table(
        vec![
            ("body".into(), FieldType::String),
            ("owner".into(), FieldType::String),
        ],
        Some(FilterExpr::Eq {
            field: "owner".into(),
            value: json!({"$user": true}),
        }),
    );
    owned.indexes.push(IndexDef {
        name: "by_owner".into(),
        fields: vec!["owner".into()],
        search: false,
        vector: None,
        unique: false,
        r#where: None,
    });
    tables.insert("owned".to_string(), owned);
    // (b) Or[Eq{owner,$user}, Eq{visibility,"public"}]
    tables.insert(
        "or_posts".to_string(),
        mk_table(
            vec![
                ("body".into(), FieldType::String),
                ("owner".into(), FieldType::String),
                ("visibility".into(), FieldType::String),
            ],
            Some(FilterExpr::Or {
                exprs: vec![
                    FilterExpr::Eq {
                        field: "owner".into(),
                        value: json!({"$user": true}),
                    },
                    FilterExpr::Eq {
                        field: "visibility".into(),
                        value: json!("public"),
                    },
                ],
            }),
        ),
    );
    // (c) Eq{visibility,"public"} — no $user leaf, not stampable
    tables.insert(
        "public_only".to_string(),
        mk_table(
            vec![
                ("body".into(), FieldType::String),
                ("visibility".into(), FieldType::String),
            ],
            Some(FilterExpr::Eq {
                field: "visibility".into(),
                value: json!("public"),
            }),
        ),
    );
    // (d) Contains{editors,$user} — array-only, not stampable
    tables.insert(
        "edited".to_string(),
        mk_table(
            vec![
                ("body".into(), FieldType::String),
                (
                    "editors".into(),
                    FieldType::Array {
                        element: Box::new(FieldType::String),
                    },
                ),
            ],
            Some(FilterExpr::Contains {
                field: "editors".into(),
                value: json!({"$user": true}),
            }),
        ),
    );
    SchemaDef { tables }
}

/// Creates a fresh uniquely-named database and pushes the insert-stamp schema.
async fn setup_insert_stamp() -> (sqlx::PgPool, String, SchemaDef) {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &db).await.unwrap();
    let schema = insert_stamp_schema();
    push_schema(&pool, &db, schema.clone()).await.unwrap();
    (pool, db, schema)
}

/// Fetches a single doc by id with a bypass caller (sees every row), on an
/// arbitrary table. Decouples the post-insert persisted-state assertion from
/// the read-path filtering.
async fn fetch_doc_bypass(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    table: &str,
    id: &str,
) -> Option<serde_json::Value> {
    let q = Query {
        table: table.to_string(),
        get: Some(id.to_string()),
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
        distinct: false,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        aggregate: None,
    };
    match execute_query(pool, db, schema, &q, &PrincipalCtx::bypass()).await {
        Ok(QueryResult::Doc(d)) => d,
        other => panic!("expected Doc, got {other:?}"),
    }
}

/// The caller's principal for these tests (a fixed user).
fn alice_ctx() -> PrincipalCtx {
    PrincipalCtx {
        user_id: Some("alice".to_string()),
        email: None,
    }
}

// (T9-a) `authorize: Eq{owner,$user}` — client sends `owner="someoneElse"`; the
// server stamps `owner=caller` over it (unforgeable), the predicate now passes,
// and the insert succeeds. Asserting the persisted owner equals the caller
// proves the stamp actually ran; a missing stamp would either reject the insert
// (predicate fails on `owner="someoneElse"`) or, worse, persist the lie.
#[tokio::test]
async fn authorize_insert_stamps_eq_user_leaf_to_caller() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_insert_stamp().await;

    let mut doc = serde_json::Map::new();
    doc.insert("body".into(), "x".into());
    doc.insert("owner".into(), "someoneElse".into());
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "owned".into(),
                doc,
            }],
        },
        &alice_ctx(),
    )
    .await
    .expect("insert should succeed: owner is stamped to caller");
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    let persisted = fetch_doc_bypass(&pool, &db, &schema, "owned", &id)
        .await
        .expect("row present");
    assert_eq!(persisted["owner"].as_str(), Some("alice"));
    assert_eq!(persisted["body"].as_str(), Some("x"));
    Ok(())
}

// (T9-b) `Or[owner==$user, visibility=="public"]` — the client sends a doc that
// fails BOTH arms pre-stamp (`owner="bob"`, `visibility="private"`); the server
// stamps `owner=caller`, so the first arm passes and the insert always
// succeeds. This is the common "public OR owned" rule.
#[tokio::test]
async fn authorize_insert_or_with_user_leaf_always_succeeds() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_insert_stamp().await;

    let mut doc = serde_json::Map::new();
    doc.insert("body".into(), "x".into());
    doc.insert("owner".into(), "bob".into());
    doc.insert("visibility".into(), "private".into());
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "or_posts".into(),
                doc,
            }],
        },
        &alice_ctx(),
    )
    .await
    .expect("insert should succeed: owner arm is stamped to caller");
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    let persisted = fetch_doc_bypass(&pool, &db, &schema, "or_posts", &id)
        .await
        .expect("row present");
    assert_eq!(persisted["owner"].as_str(), Some("alice"));
    assert_eq!(persisted["visibility"].as_str(), Some("private"));
    Ok(())
}

// (T9-c) `authorize: Eq{visibility,"public"}` — no `$user` leaf, so the server
// stamps nothing. The client sends `visibility="private"`, the predicate fails
// on the post-stamp (unchanged) doc, and the insert is `Forbidden`. Proves a
// non-$user predicate is enforceable on inserts without any stamping escape.
#[tokio::test]
async fn authorize_insert_without_user_leaf_forbidden_when_predicate_fails() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_insert_stamp().await;

    let mut doc = serde_json::Map::new();
    doc.insert("body".into(), "x".into());
    doc.insert("visibility".into(), "private".into());
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "public_only".into(),
                doc,
            }],
        },
        &alice_ctx(),
    )
    .await
    .expect_err("insert must fail: visibility=private fails the predicate");
    assert_eq!(err.code, ErrorCode::Forbidden);

    // Nothing was persisted (Forbidden rolled back the txn). A bypass count
    // sees every row, so 0 confirms the insert never landed.
    let q = Query {
        table: "public_only".to_string(),
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
        count: true,
        distinct: false,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        aggregate: None,
    };
    let res = execute_query(&pool, &db, &schema, &q, &PrincipalCtx::bypass()).await?;
    match res {
        QueryResult::Count(n) => assert_eq!(n, 0, "no row should be persisted"),
        other => panic!("expected Count, got {other:?}"),
    }
    Ok(())
}

// (T9-d) `authorize: Contains{editors,$user}` — array-only, not stampable (no
// `Eq{field,$user}` leaf). The client omits itself from the array; the
// predicate fails on the post-stamp (unchanged) doc, and the insert is
// `Forbidden`. Proves an array-membership predicate cannot be bypassed by an
// insert that forgets to include the caller.
#[tokio::test]
async fn authorize_insert_with_array_only_predicate_forbidden_when_self_omitted()
-> anyhow::Result<()> {
    let (pool, db, schema) = setup_insert_stamp().await;

    let mut doc = serde_json::Map::new();
    doc.insert("body".into(), "x".into());
    doc.insert("editors".into(), json!(["bob"]));
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "edited".into(),
                doc,
            }],
        },
        &alice_ctx(),
    )
    .await
    .expect_err("insert must fail: alice is not in editors");
    assert_eq!(err.code, ErrorCode::Forbidden);
    Ok(())
}

// (T9-d-positive) The same `Contains{editors,$user}` predicate ALLOWS an insert
// when the client includes itself in the array — proves the predicate is
// satisfiable from client values alone (case (d)'s "not stampable" does not
// mean "always rejected"). Belt-and-suspenders for the negative case above.
#[tokio::test]
async fn authorize_insert_with_array_only_predicate_succeeds_when_self_included()
-> anyhow::Result<()> {
    let (pool, db, schema) = setup_insert_stamp().await;

    let mut doc = serde_json::Map::new();
    doc.insert("body".into(), "x".into());
    doc.insert("editors".into(), json!(["alice", "bob"]));
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "edited".into(),
                doc,
            }],
        },
        &alice_ctx(),
    )
    .await
    .expect("insert should succeed: alice is in editors");
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    let persisted = fetch_doc_bypass(&pool, &db, &schema, "edited", &id)
        .await
        .expect("row present");
    assert_eq!(persisted["body"].as_str(), Some("x"));
    Ok(())
}

// (T9-bypass) Bypass callers (machine tokens, admin, scheduled jobs) are NOT
// subject to insert stamp/verify — they can insert any doc on an
// `authorize`-gated table, even one that would fail the predicate for a user.
// Preserves machine-token full-access behavior on the insert path (mirrors the
// T8-5 bypass guarantee for patch/replace/delete).
#[tokio::test]
async fn authorize_insert_bypass_skips_stamp_and_verify() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_insert_stamp().await;

    // Bypass inserts a `public_only` row with visibility="private" — a user
    // caller would be Forbidden; bypass must succeed.
    let mut doc = serde_json::Map::new();
    doc.insert("body".into(), "machine".into());
    doc.insert("visibility".into(), "private".into());
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "public_only".into(),
                doc,
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("bypass insert should skip authorize verify");
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    let persisted = fetch_doc_bypass(&pool, &db, &schema, "public_only", &id)
        .await
        .expect("row present");
    assert_eq!(persisted["visibility"].as_str(), Some("private"));
    Ok(())
}

// (T9-upsert-insert) The Upsert insert branch (no match) goes through the same
// stamp+verify as Insert. Asserts the second call site (txn.rs ~:1154) is wired:
// client `owner="someoneElse"` → stamped to caller → predicate passes → upsert
// reports `inserted: true` with the persisted owner equal to the caller.
#[tokio::test]
async fn authorize_upsert_insert_stamps_eq_user_leaf_to_caller() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_insert_stamp().await;

    let mut insert = serde_json::Map::new();
    insert.insert("body".into(), "x".into());
    insert.insert("owner".into(), "someoneElse".into());
    let mut patch = serde_json::Map::new();
    patch.insert("body".into(), "ignored".into());
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Upsert {
                table: "owned".into(),
                index: "by_owner".into(),
                eq: vec!["alice".into()],
                insert,
                patch,
            }],
        },
        &alice_ctx(),
    )
    .await
    .expect("upsert-insert should succeed: owner is stamped to caller");
    assert_eq!(outcome.results[0]["inserted"].as_bool(), Some(true));
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    let persisted = fetch_doc_bypass(&pool, &db, &schema, "owned", &id)
        .await
        .expect("row present");
    assert_eq!(persisted["owner"].as_str(), Some("alice"));
    Ok(())
}

// ===========================================================================
// Task 8.5 — patch/replace/upsert-update re-stamp + verify against `authorize`
//
// Task 9 stamped+verified only the Insert and Upsert-INSERT paths. A review
// found that `stamp_owner` ALSO runs on Patch/Replace/Upsert-update (making
// `ownerField` immutable-by-patch), but `stamp_authorize` did not — so on an
// `authorize` table a user could `patch` an `Eq{owner,$user}` field to ANOTHER
// user's id, injecting a doc into that user's read view. The spec's "authorize
// subsumes ownerField" claim failed on patch. These tests pin the closed gap:
// re-stamp every `$user` leaf on each write path (like `stamp_owner`), then
// post-write-verify the resulting doc (catches no-`$user`-arm predicates the
// re-stamp alone cannot satisfy).
// ===========================================================================

/// Builds a `get(id)` query on `table` (avoids retyping the 20-field `Query`
/// literal when checking another user's read view).
fn get_query(table: &str, id: &str) -> Query {
    Query {
        table: table.to_string(),
        get: Some(id.to_string()),
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
        distinct: false,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        aggregate: None,
    }
}

// (T8.5-a) Injection closed: `or_posts` = `Or[Eq{owner,$user}, Eq{visibility,"public"}]`.
// Alice creates a private doc (owner stamped alice by the insert path), then
// `patch`es it setting `owner="bob"`. The patch is applied but `owner` is
// RE-STAMPED to alice, so the persisted owner is alice (NOT bob) and bob does
// not gain a read view of the doc. Before the fix owner became bob and bob
// could read alice's private doc — the patch-injection gap.
#[tokio::test]
async fn authorize_patch_re_stamps_eq_user_leaf_closing_injection() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_insert_stamp().await;

    // Alice creates a private doc (the insert path stamps owner=alice).
    let mut insert = serde_json::Map::new();
    insert.insert("body".into(), "a-secret".into());
    insert.insert("owner".into(), "alice".into());
    insert.insert("visibility".into(), "private".into());
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "or_posts".into(),
                doc: insert,
            }],
        },
        &alice_ctx(),
    )
    .await
    .expect("insert ok");
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    // Alice attempts to inject the doc into bob's read view by patching owner=bob.
    let mut patch = serde_json::Map::new();
    patch.insert("owner".into(), "bob".into());
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "or_posts".into(),
                id: id.clone(),
                fields: patch,
            }],
        },
        &alice_ctx(),
    )
    .await
    .expect("patch ok (owner re-stamped to alice)");

    // Persisted owner == alice (NOT bob): the re-stamp overwrote the client value.
    let persisted = fetch_doc_bypass(&pool, &db, &schema, "or_posts", &id)
        .await
        .expect("row present");
    assert_eq!(
        persisted["owner"].as_str(),
        Some("alice"),
        "patch must re-stamp owner to the caller, not bob"
    );

    // Bob does NOT gain visibility: get(id) as bob is filtered to Doc(None).
    let res = execute_query(
        &pool,
        &db,
        &schema,
        &get_query("or_posts", &id),
        &PrincipalCtx {
            user_id: Some("bob".to_string()),
            email: None,
        },
    )
    .await?;
    match res {
        QueryResult::Doc(None) => {}
        other => panic!("bob must not see alice's private doc, got {other:?}"),
    }
    Ok(())
}

// (T8.5-b) Replace re-stamp: Alice `replace`s the doc with `owner="bob"` — the
// persisted owner is alice (re-stamped). Parity with the patch path and with
// `stamp_owner` on Replace.
#[tokio::test]
async fn authorize_replace_re_stamps_eq_user_leaf() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_insert_stamp().await;

    let mut insert = serde_json::Map::new();
    insert.insert("body".into(), "a-original".into());
    insert.insert("owner".into(), "alice".into());
    insert.insert("visibility".into(), "private".into());
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "or_posts".into(),
                doc: insert,
            }],
        },
        &alice_ctx(),
    )
    .await
    .expect("insert ok");
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    // Replace attempts to set owner=bob; the re-stamp overwrites it to alice.
    let mut replace = serde_json::Map::new();
    replace.insert("body".into(), "a-replaced".into());
    replace.insert("owner".into(), "bob".into());
    replace.insert("visibility".into(), "private".into());
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Replace {
                table: "or_posts".into(),
                id: id.clone(),
                doc: replace,
            }],
        },
        &alice_ctx(),
    )
    .await
    .expect("replace ok (owner re-stamped to alice)");

    let persisted = fetch_doc_bypass(&pool, &db, &schema, "or_posts", &id)
        .await
        .expect("row present");
    assert_eq!(
        persisted["owner"].as_str(),
        Some("alice"),
        "replace must re-stamp owner to the caller"
    );
    assert_eq!(persisted["body"].as_str(), Some("a-replaced"));
    Ok(())
}

// (T8.5-c) No-`$user`-arm verify: `public_only` = `Eq{visibility,"public"}` (no
// stampable leaf). Alice creates a public doc, then `patch`es visibility=private.
// The re-stamp is a no-op (no $user leaf), and the post-write verify fails on
// the resulting doc (visibility=private fails the predicate) → Forbidden, and
// the txn rolls back atomically (the doc stays public). Guards predicates the
// re-stamp alone cannot satisfy.
#[tokio::test]
async fn authorize_patch_no_user_leaf_forbidden_when_predicate_fails() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_insert_stamp().await;

    let mut insert = serde_json::Map::new();
    insert.insert("body".into(), "x".into());
    insert.insert("visibility".into(), "public".into());
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "public_only".into(),
                doc: insert,
            }],
        },
        &alice_ctx(),
    )
    .await
    .expect("insert ok (visibility=public satisfies the predicate)");
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    // Patch visibility=private → the merged doc fails Eq{visibility,"public"}.
    let mut patch = serde_json::Map::new();
    patch.insert("visibility".into(), "private".into());
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "public_only".into(),
                id: id.clone(),
                fields: patch,
            }],
        },
        &alice_ctx(),
    )
    .await
    .expect_err("patch must fail: visibility=private fails the predicate");
    assert_eq!(err.code, ErrorCode::Forbidden);

    // Atomic: the doc survived untouched (still public).
    let persisted = fetch_doc_bypass(&pool, &db, &schema, "public_only", &id)
        .await
        .expect("row present");
    assert_eq!(persisted["visibility"].as_str(), Some("public"));
    Ok(())
}

// (T8.5-d) Bypass unchanged: a bypass caller (machine/admin/scheduled) patching
// owner is NOT re-stamped nor verified — it can write any value. Confirms the
// stamp/verify is user-only (ctx.user_id None ⇒ no-op), preserving machine-token
// full access on the patch path.
#[tokio::test]
async fn authorize_bypass_patch_skips_stamp_and_verify() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_insert_stamp().await;

    let mut insert = serde_json::Map::new();
    insert.insert("body".into(), "x".into());
    insert.insert("owner".into(), "alice".into());
    insert.insert("visibility".into(), "private".into());
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "or_posts".into(),
                doc: insert,
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("bypass insert ok");
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    // Bypass patches owner=bob — no re-stamp, persisted owner is exactly as sent.
    let mut patch = serde_json::Map::new();
    patch.insert("owner".into(), "bob".into());
    execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "or_posts".into(),
                id: id.clone(),
                fields: patch,
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("bypass patch ok");

    let persisted = fetch_doc_bypass(&pool, &db, &schema, "or_posts", &id)
        .await
        .expect("row present");
    assert_eq!(
        persisted["owner"].as_str(),
        Some("bob"),
        "bypass must not re-stamp owner"
    );
    Ok(())
}

// (T8.5-e) Upsert-update re-stamp: the third write path. `owned` =
// `Eq{owner,$user}` with the `by_owner` index. Alice inserts (owner=alice), then
// an upsert matches her doc (eq=["alice"]) whose update patch sets owner=bob.
// The patch is re-stamped (owner=alice), so the persisted owner stays alice.
// Before the fix the upsert-update branch re-stamped nothing and owner became bob.
#[tokio::test]
async fn authorize_upsert_update_re_stamps_eq_user_leaf() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_insert_stamp().await;

    let mut insert = serde_json::Map::new();
    insert.insert("body".into(), "x".into());
    insert.insert("owner".into(), "alice".into());
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "owned".into(),
                doc: insert,
            }],
        },
        &alice_ctx(),
    )
    .await
    .expect("insert ok");
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string();

    // Upsert matches alice's doc via by_owner; the update patch sets owner=bob.
    let mut insert_doc = serde_json::Map::new();
    insert_doc.insert("body".into(), "ignored".into());
    insert_doc.insert("owner".into(), "alice".into());
    let mut patch = serde_json::Map::new();
    patch.insert("owner".into(), "bob".into());
    let outcome = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Upsert {
                table: "owned".into(),
                index: "by_owner".into(),
                eq: vec!["alice".into()],
                insert: insert_doc,
                patch,
            }],
        },
        &alice_ctx(),
    )
    .await
    .expect("upsert-update ok (owner re-stamped to alice)");
    assert_eq!(outcome.results[0]["inserted"].as_bool(), Some(false));

    let persisted = fetch_doc_bypass(&pool, &db, &schema, "owned", &id)
        .await
        .expect("row present");
    assert_eq!(
        persisted["owner"].as_str(),
        Some("alice"),
        "upsert-update must re-stamp owner to the caller"
    );
    Ok(())
}

// ============================================================================
// Composition: a table declaring BOTH `ownerField` and `authorize`.
// ----------------------------------------------------------------------------
// The two gates AND — a row must pass the owner/collaborator gate AND the
// `authorize` predicate. `check_owner` runs both in sequence (txn.rs), the scan
// path ANDs both fragments into the WHERE clause (query.rs:
// `where_conditions.join(" AND ")`), and the point-read re-checks both. Every
// other test declares at most one gate; this pins the conjunction on a single
// table so a future change that ORs them (or silently drops one gate) fails
// loudly. The predicate is a pure `Eq{visibility,"public"}` with no `$user`
// marker, so the two gates are independent — passing one cannot implicitly
// satisfy the other.

/// `docs` table declaring BOTH `ownerField: "owner"` and
/// `authorize: Eq{visibility,"public"}`: a row is visible to a user only when
/// they own it AND it is public.
fn composed_schema() -> SchemaDef {
    use rtdb_server::query::FilterExpr;
    let mut fields = BTreeMap::new();
    fields.insert("body".to_string(), FieldType::String);
    fields.insert("owner".to_string(), FieldType::String);
    fields.insert("visibility".to_string(), FieldType::String);
    let mut tables = BTreeMap::new();
    tables.insert(
        "docs".to_string(),
        TableDef {
            fields,
            indexes: vec![],
            owner_field: Some("owner".into()),
            collaborators_field: None,
            ttl: None,
            authorize: Some(FilterExpr::Eq {
                field: "visibility".into(),
                value: json!("public"),
            }),
        },
    );
    SchemaDef { tables }
}

async fn setup_composed() -> (sqlx::PgPool, String, SchemaDef) {
    let state = test_state().await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &db).await.unwrap();
    let schema = composed_schema();
    push_schema(&pool, &db, schema.clone()).await.unwrap();
    (pool, db, schema)
}

/// Seeds a `docs` row (bypass caller) with explicit owner/visibility; returns id.
async fn seed_doc(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    body: &str,
    owner: &str,
    visibility: &str,
) -> String {
    let mut doc = serde_json::Map::new();
    doc.insert("body".into(), body.into());
    doc.insert("owner".into(), owner.into());
    doc.insert("visibility".into(), visibility.into());
    let outcome = execute_txn(
        pool,
        db,
        schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "docs".into(),
                doc,
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("seed doc insert");
    outcome.results[0]["id"]
        .as_str()
        .expect("id string")
        .to_string()
}

fn docs_query() -> Query {
    Query {
        table: "docs".to_string(),
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
        distinct: false,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
        aggregate: None,
    }
}

// A row passing only ONE gate is denied on scan, point-read, and patch. Seeds
// the four owner×visibility combinations; alice owns the "alice" rows yet sees
// only the single row she owns AND that is public.
#[tokio::test]
async fn owner_and_authorize_both_gates_must_pass() -> anyhow::Result<()> {
    let (pool, db, schema) = setup_composed().await;
    // both pass        owner pass / auth fail   auth pass / owner fail   both fail
    let both = seed_doc(&pool, &db, &schema, "alice-pub", "alice", "public").await;
    let owner_only = seed_doc(&pool, &db, &schema, "alice-priv", "alice", "private").await;
    let auth_only = seed_doc(&pool, &db, &schema, "bob-pub", "bob", "public").await;
    let _neither = seed_doc(&pool, &db, &schema, "bob-priv", "bob", "private").await;

    let alice = PrincipalCtx {
        user_id: Some("alice".to_string()),
        email: None,
    };

    // (1) Scan: alice sees only the row she owns AND that is public.
    let mut q = docs_query();
    q.take = Some(100);
    let mut got = bodies(execute_query(&pool, &db, &schema, &q, &alice).await?);
    got.sort();
    assert_eq!(got, vec!["alice-pub".to_string()]);

    // (2) Point-read: owning a row is not enough when `authorize` denies it.
    let mut q = docs_query();
    q.get = Some(owner_only.clone());
    match execute_query(&pool, &db, &schema, &q, &alice).await? {
        QueryResult::Doc(None) => {}
        other => panic!("get(owner-pass/auth-fail) must be None, got {other:?}"),
    }
    // (3) Point-read: a public row is not enough when the caller doesn't own it.
    let mut q = docs_query();
    q.get = Some(auth_only.clone());
    match execute_query(&pool, &db, &schema, &q, &alice).await? {
        QueryResult::Doc(None) => {}
        other => panic!("get(auth-pass/owner-fail) must be None, got {other:?}"),
    }
    // (4) Point-read: both gates pass → returned.
    let mut q = docs_query();
    q.get = Some(both.clone());
    match execute_query(&pool, &db, &schema, &q, &alice).await? {
        QueryResult::Doc(Some(d)) => assert_eq!(d["body"].as_str(), Some("alice-pub")),
        other => panic!("get(both-pass) must return the doc, got {other:?}"),
    }

    // (5) Patch: owning a row is not enough when `authorize` denies it.
    let mut patch = serde_json::Map::new();
    patch.insert("body".into(), "hacked".into());
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "docs".into(),
                id: owner_only.clone(),
                fields: patch,
            }],
        },
        &alice,
    )
    .await
    .expect_err("patch on owner-pass/auth-fail must be forbidden");
    assert_eq!(err.code, ErrorCode::Forbidden);

    // (6) Patch: a public row is not enough when the caller doesn't own it.
    let mut patch = serde_json::Map::new();
    patch.insert("body".into(), "hacked".into());
    let err = execute_txn(
        &pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Patch {
                table: "docs".into(),
                id: auth_only.clone(),
                fields: patch,
            }],
        },
        &alice,
    )
    .await
    .expect_err("patch on auth-pass/owner-fail must be forbidden");
    assert_eq!(err.code, ErrorCode::Forbidden);

    Ok(())
}
