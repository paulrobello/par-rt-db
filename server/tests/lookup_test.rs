//! Integration tests for server-side lookup expansion (the read-DSL id join).
//!
//! Covers the card's matrix: single-`Id` and `Array<Id>` lookups, dangling
//! ids, unauthorized joined rows (`ownerField` + `authorize`), cap truncation
//! and the `cap > 1000` rejection, unknown lookup table `NOT_FOUND`, non-id
//! field `BAD_REQUEST`, `as` collisions `BAD_REQUEST`, >4 lookups, projection
//! naming an alias, composition with `paginate` + `search`, and the
//! subscription dispatch union (a lookup-table write re-runs the subscription
//! and the canonical diff pushes only on visible change).

use rtdb_server::auth::PrincipalCtx;
use rtdb_server::ddl;
use rtdb_server::error::ErrorCode;
use rtdb_server::protocol::ServerMessage;
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::schema::SchemaDef;
use rtdb_server::subs::next_conn_id;
use rtdb_server::txn::{Step, Transaction, execute_txn};
use serde_json::{Value, json};
use tokio::sync::mpsc::UnboundedReceiver;

use crate::common::{test_state, wrap_test_db};

/// posts(id-reference table) + users + tags. posts.authorId is an
/// `Optional<Id users>`; users.tagIds is an `Array<Id tags>`. users carries
/// `ownerField: ownerId` and an `authorize` predicate (`owner == $user`) so
/// both per-row predicates can deny a joined row.
fn lookup_schema() -> SchemaDef {
    let tables: Value = json!({
        "tables": {
        "users": {
            "fields": {
                "name": {"type": "string"},
                "ownerId": {"type": "string"},
                "tagIds": {"type": "array", "element": {"type": "id", "table": "tags"}}
            },
            "indexes": [{"name": "by_name", "fields": ["name"]}],
            "ownerField": "ownerId",
            "authorize": {"op": "eq", "field": "ownerId", "value": {"$user": true}}
        },
        "tags": {"fields": {"label": {"type": "string"}}},
        "posts": {
            "fields": {
                "title": {"type": "string"},
                "authorId": {"type": "optional", "inner": {"type": "id", "table": "users"}}
            },
            "indexes": [{"name": "by_title", "fields": ["title"], "search": true}]
        }
        }
    });
    serde_json::from_value(tables).expect("parse lookup schema")
}

async fn fresh_db(state: &std::sync::Arc<rtdb_server::AppState>) -> crate::common::TestDb {
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &name)
        .await
        .expect("create db");
    ddl::push_schema(&state.pool, &name, lookup_schema())
        .await
        .expect("push schema");
    wrap_test_db(name)
}

async fn insert(
    state: &std::sync::Arc<rtdb_server::AppState>,
    db: &str,
    table: &str,
    doc: Value,
) -> String {
    let mut doc_obj = doc.as_object().cloned().expect("object doc");
    // Array fields are required (non-Optional), so default the users table's
    // tagIds to empty when the caller doesn't care about them.
    if table == "users" && !doc_obj.contains_key("tagIds") {
        doc_obj.insert("tagIds".into(), Value::Array(Vec::new()));
    }
    let txn = Transaction {
        steps: vec![Step::Insert {
            table: table.to_string(),
            doc: doc_obj,
        }],
    };
    let outcome = execute_txn(
        &state.pool,
        db,
        &lookup_schema(),
        &txn,
        &PrincipalCtx::bypass(),
        None,
    )
    .await
    .expect("insert");
    outcome.results[0]["id"].as_str().expect("id").to_string()
}

fn collect_posts_lookup() -> Query {
    Query {
        table: "posts".into(),
        lookups: vec![par_rt_db_core::query::LookupSpec {
            field: "authorId".into(),
            table: "users".into(),
            r#as: "author".into(),
            cap: None,
        }],
        ..Default::default()
    }
}

async fn run_query(
    state: &std::sync::Arc<rtdb_server::AppState>,
    db: &str,
    q: &Query,
    ctx: &PrincipalCtx,
) -> Result<QueryResult, rtdb_server::error::RtDbError> {
    execute_query(&state.pool, db, &lookup_schema(), q, ctx, false).await
}

fn err_code(r: Result<QueryResult, rtdb_server::error::RtDbError>) -> ErrorCode {
    r.expect_err("expected error").code
}

// =====================================================================
// Happy paths
// =====================================================================

#[tokio::test]
async fn single_id_lookup_joins_author() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let ann = insert(
        &state,
        &db.0,
        "users",
        json!({"name": "Ann", "ownerId": "u1"}),
    )
    .await;
    insert(
        &state,
        &db.0,
        "posts",
        json!({"title": "p1", "authorId": ann}),
    )
    .await;
    insert(&state, &db.0, "posts", json!({"title": "p2"})).await;

    let r = run_query(
        &state,
        &db.0,
        &collect_posts_lookup(),
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("query");
    let QueryResult::Docs(docs) = r else {
        panic!("expected Docs");
    };
    assert_eq!(docs.len(), 2);
    for doc in &docs {
        let author = &doc["author"];
        if doc["title"] == "p1" {
            assert_eq!(author["name"], "Ann");
            // Child system fields ride the joined doc (normalized format).
            assert!(author["_id"].is_string());
            assert!(author["_creationTime"].is_number());
            assert!(author["_version"].is_number());
        } else {
            assert!(author.is_null(), "absent authorId reads as null");
        }
    }
}

#[tokio::test]
async fn array_id_lookup_preserves_order_and_caps() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let mut tag_ids = Vec::new();
    for label in ["t1", "t2", "t3"] {
        tag_ids.push(insert(&state, &db.0, "tags", json!({"label": label})).await);
    }
    insert(
        &state,
        &db.0,
        "users",
        json!({"name": "U", "ownerId": "u1", "tagIds": tag_ids}),
    )
    .await;

    let q = Query {
        table: "users".into(),
        lookups: vec![par_rt_db_core::query::LookupSpec {
            field: "tagIds".into(),
            table: "tags".into(),
            r#as: "tags".into(),
            cap: Some(2),
        }],
        ..Default::default()
    };
    let r = run_query(&state, &db.0, &q, &PrincipalCtx::bypass())
        .await
        .expect("query");
    let QueryResult::Docs(docs) = r else {
        panic!("expected Docs")
    };
    assert_eq!(docs.len(), 1);
    let tags = docs[0]["tags"].as_array().expect("tags array");
    // Declared id-array order, truncated to the first `cap` entries.
    assert_eq!(tags.len(), 2);
    assert_eq!(tags[0]["label"], "t1");
    assert_eq!(tags[1]["label"], "t2");
}

#[tokio::test]
async fn dangling_ids_omit_not_error() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    // Well-formed 32-hex ids that reference no row.
    insert(
        &state,
        &db.0,
        "posts",
        json!({"title": "p1", "authorId": "e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0"}),
    )
    .await;
    insert(
        &state,
        &db.0,
        "users",
        json!({"name": "U", "ownerId": "u1", "tagIds": ["d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1"]}),
    )
    .await;

    let r = run_query(
        &state,
        &db.0,
        &collect_posts_lookup(),
        &PrincipalCtx::bypass(),
    )
    .await
    .expect("query");
    let QueryResult::Docs(docs) = r else {
        panic!("expected Docs")
    };
    assert!(
        docs[0]["author"].is_null(),
        "dangling single id reads as null"
    );

    let q = Query {
        table: "users".into(),
        lookups: vec![par_rt_db_core::query::LookupSpec {
            field: "tagIds".into(),
            table: "tags".into(),
            r#as: "tags".into(),
            cap: None,
        }],
        ..Default::default()
    };
    let r = run_query(&state, &db.0, &q, &PrincipalCtx::bypass())
        .await
        .expect("query");
    let QueryResult::Docs(docs) = r else {
        panic!("expected Docs")
    };
    assert!(
        docs[0]["tags"].as_array().expect("tags").is_empty(),
        "dangling array ids are dropped"
    );
}

// =====================================================================
// Authorization on joined rows (D1 — omit, don't error)
// =====================================================================

#[tokio::test]
async fn unauthorized_joined_row_omitted() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    // users carries ownerField=ownerId AND authorize(ownerId == $user); a
    // joined row failing both predicates for caller "u2" reads as absent.
    let ann = insert(
        &state,
        &db.0,
        "users",
        json!({"name": "Ann", "ownerId": "u1"}),
    )
    .await;
    let mine = insert(
        &state,
        &db.0,
        "users",
        json!({"name": "Mine", "ownerId": "u2"}),
    )
    .await;
    insert(
        &state,
        &db.0,
        "posts",
        json!({"title": "p1", "authorId": ann}),
    )
    .await;
    insert(
        &state,
        &db.0,
        "posts",
        json!({"title": "p2", "authorId": mine}),
    )
    .await;

    let ctx = PrincipalCtx {
        user_id: Some("u2".into()),
        ..Default::default()
    };
    let r = run_query(&state, &db.0, &collect_posts_lookup(), &ctx)
        .await
        .expect("query");
    let QueryResult::Docs(docs) = r else {
        panic!("expected Docs")
    };
    assert_eq!(docs.len(), 2);
    for doc in &docs {
        if doc["title"] == "p1" {
            assert!(doc["author"].is_null(), "row owned by u1 must be omitted");
        } else {
            assert_eq!(doc["author"]["name"], "Mine", "caller's own row joins");
        }
    }
}

// =====================================================================
// Compile-time validation and error mapping
// =====================================================================

#[tokio::test]
async fn unknown_lookup_table_is_not_found() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let q = Query {
        table: "posts".into(),
        lookups: vec![par_rt_db_core::query::LookupSpec {
            field: "authorId".into(),
            table: "ghost".into(),
            r#as: "author".into(),
            cap: None,
        }],
        ..Default::default()
    };
    assert_eq!(
        err_code(run_query(&state, &db.0, &q, &PrincipalCtx::bypass()).await),
        ErrorCode::NotFound
    );
}

#[tokio::test]
async fn field_not_id_is_bad_request() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let q = Query {
        table: "posts".into(),
        lookups: vec![par_rt_db_core::query::LookupSpec {
            field: "title".into(),
            table: "users".into(),
            r#as: "author".into(),
            cap: None,
        }],
        ..Default::default()
    };
    assert_eq!(
        err_code(run_query(&state, &db.0, &q, &PrincipalCtx::bypass()).await),
        ErrorCode::BadRequest
    );
}

#[tokio::test]
async fn declared_table_mismatch_is_bad_request() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let q = Query {
        table: "posts".into(),
        lookups: vec![par_rt_db_core::query::LookupSpec {
            field: "authorId".into(),
            table: "tags".into(),
            r#as: "author".into(),
            cap: None,
        }],
        ..Default::default()
    };
    assert_eq!(
        err_code(run_query(&state, &db.0, &q, &PrincipalCtx::bypass()).await),
        ErrorCode::BadRequest
    );
}

#[tokio::test]
async fn cap_over_1000_is_bad_request() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let q = Query {
        table: "users".into(),
        lookups: vec![par_rt_db_core::query::LookupSpec {
            field: "tagIds".into(),
            table: "tags".into(),
            r#as: "tags".into(),
            cap: Some(1001),
        }],
        ..Default::default()
    };
    assert_eq!(
        err_code(run_query(&state, &db.0, &q, &PrincipalCtx::bypass()).await),
        ErrorCode::BadRequest
    );
}

#[tokio::test]
async fn alias_collisions_are_bad_request() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    // Collides with a declared field.
    let q = Query {
        table: "posts".into(),
        lookups: vec![par_rt_db_core::query::LookupSpec {
            field: "authorId".into(),
            table: "users".into(),
            r#as: "title".into(),
            cap: None,
        }],
        ..Default::default()
    };
    assert_eq!(
        err_code(run_query(&state, &db.0, &q, &PrincipalCtx::bypass()).await),
        ErrorCode::BadRequest
    );
    // `_`-prefixed alias (system-field namespace).
    let q = Query {
        table: "posts".into(),
        lookups: vec![par_rt_db_core::query::LookupSpec {
            field: "authorId".into(),
            table: "users".into(),
            r#as: "_id".into(),
            cap: None,
        }],
        ..Default::default()
    };
    assert_eq!(
        err_code(run_query(&state, &db.0, &q, &PrincipalCtx::bypass()).await),
        ErrorCode::BadRequest
    );
    // Duplicate alias across two lookups.
    let q = Query {
        table: "posts".into(),
        lookups: vec![
            par_rt_db_core::query::LookupSpec {
                field: "authorId".into(),
                table: "users".into(),
                r#as: "author".into(),
                cap: None,
            },
            par_rt_db_core::query::LookupSpec {
                field: "authorId".into(),
                table: "users".into(),
                r#as: "author".into(),
                cap: None,
            },
        ],
        ..Default::default()
    };
    assert_eq!(
        err_code(run_query(&state, &db.0, &q, &PrincipalCtx::bypass()).await),
        ErrorCode::BadRequest
    );
}

#[tokio::test]
async fn more_than_four_lookups_is_bad_request() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let lk = || par_rt_db_core::query::LookupSpec {
        field: "authorId".into(),
        table: "users".into(),
        r#as: "author".into(),
        cap: None,
    };
    let q = Query {
        table: "posts".into(),
        lookups: vec![lk(), lk(), lk(), lk(), lk()],
        ..Default::default()
    };
    assert_eq!(
        err_code(run_query(&state, &db.0, &q, &PrincipalCtx::bypass()).await),
        ErrorCode::BadRequest
    );
}

#[tokio::test]
async fn lookups_with_docless_terminals_are_bad_request() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let mut base = collect_posts_lookup();
    base.count = true;
    assert_eq!(
        err_code(run_query(&state, &db.0, &base, &PrincipalCtx::bypass()).await),
        ErrorCode::BadRequest
    );
    let mut base = collect_posts_lookup();
    base.distinct = true;
    assert_eq!(
        err_code(run_query(&state, &db.0, &base, &PrincipalCtx::bypass()).await),
        ErrorCode::BadRequest
    );
    let mut base = collect_posts_lookup();
    base.aggregate = Some(rtdb_server::query::AggregateSpec {
        op: Some(rtdb_server::query::AggregateOp::Count),
        aggregates: None,
        group_by: rtdb_server::query::GroupBy::Bool(false),
    });
    assert_eq!(
        err_code(run_query(&state, &db.0, &base, &PrincipalCtx::bypass()).await),
        ErrorCode::BadRequest
    );
}

// =====================================================================
// Projection + terminal composition
// =====================================================================

#[tokio::test]
async fn projection_may_name_the_alias_and_omitting_drops_it() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let ann = insert(
        &state,
        &db.0,
        "users",
        json!({"name": "Ann", "ownerId": "u1"}),
    )
    .await;
    insert(
        &state,
        &db.0,
        "posts",
        json!({"title": "p1", "authorId": ann}),
    )
    .await;

    // Alias kept; unlisted parent field (title) dropped.
    let q = Query {
        table: "posts".into(),
        lookups: collect_posts_lookup().lookups,
        fields: Some(vec!["author".into()]),
        ..Default::default()
    };
    let r = run_query(&state, &db.0, &q, &PrincipalCtx::bypass())
        .await
        .expect("query");
    let QueryResult::Docs(docs) = r else {
        panic!("expected Docs")
    };
    assert_eq!(docs[0]["author"]["name"], "Ann");
    assert!(docs[0].get("title").is_none());

    // Omitting the alias from fields drops the joined docs.
    let q = Query {
        table: "posts".into(),
        lookups: collect_posts_lookup().lookups,
        fields: Some(vec!["title".into()]),
        ..Default::default()
    };
    let r = run_query(&state, &db.0, &q, &PrincipalCtx::bypass())
        .await
        .expect("query");
    let QueryResult::Docs(docs) = r else {
        panic!("expected Docs")
    };
    assert_eq!(docs[0]["title"], "p1");
    assert!(docs[0].get("author").is_none());
}

#[tokio::test]
async fn composes_with_paginate_and_search() {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let ann = insert(
        &state,
        &db.0,
        "users",
        json!({"name": "Ann", "ownerId": "u1"}),
    )
    .await;
    for title in ["alpha", "bravo", "charlie"] {
        insert(
            &state,
            &db.0,
            "posts",
            json!({"title": title, "authorId": ann}),
        )
        .await;
    }

    // paginate: expansion applies to the returned page's docs only; cursor
    // semantics are unchanged.
    let q = Query {
        table: "posts".into(),
        lookups: collect_posts_lookup().lookups,
        paginate: Some(rtdb_server::query::Paginate {
            cursor: None,
            num_items: 2,
        }),
        ..Default::default()
    };
    let r = run_query(&state, &db.0, &q, &PrincipalCtx::bypass())
        .await
        .expect("query");
    let QueryResult::Paginated(page) = r else {
        panic!("expected Paginated")
    };
    assert_eq!(page.docs.len(), 2);
    assert!(page.docs.iter().all(|d| d["author"]["name"] == "Ann"));
    assert!(page.next_cursor.is_some());

    // search + lookups: ranked hits expand too (cursor derivation never reads
    // lookup fields, so paging stays sound). `by_title` is a search index in
    // the shared schema.
    let q: Query = serde_json::from_value(json!({
        "table": "posts",
        "search": {"index": "by_title", "query": "alpha"},
        "lookups": [{"field": "authorId", "table": "users", "as": "author"}]
    }))
    .expect("parse search query");
    let r = run_query(&state, &db.0, &q, &PrincipalCtx::bypass())
        .await
        .expect("search query");
    let QueryResult::Docs(docs) = r else {
        panic!("expected Docs")
    };
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["author"]["name"], "Ann");
}

// =====================================================================
// Subscription dispatch: lookup-table writes reach the re-run pass and the
// canonical diff pushes only on visible change
// =====================================================================

fn drain_initial(rx: &mut UnboundedReceiver<ServerMessage>) {
    match rx.try_recv() {
        Ok(ServerMessage::QueryUpdate { .. }) => {}
        other => panic!("expected initial QueryUpdate, got {other:?}"),
    }
}

async fn expect_update(rx: &mut UnboundedReceiver<ServerMessage>, label: &str) {
    loop {
        match tokio::time::timeout(std::time::Duration::from_secs(5), rx.recv()).await {
            Ok(Some(ServerMessage::QueryUpdate { .. })) => return,
            Ok(Some(_)) => continue,
            other => panic!("{label}: expected QueryUpdate, got {other:?}"),
        }
    }
}

fn expect_no_update(rx: &mut UnboundedReceiver<ServerMessage>, label: &str) {
    // `mutate().await` returns after fan_out completes, so nothing may follow.
    if let Ok(msg) = rx.try_recv() {
        panic!("{label}: expected no push, got {msg:?}");
    }
}

/// A projected lookup subscription pushes on a child write that changes a
/// projected joined field, and stays silent on a child write that changes
/// nothing the projection exposes (`_version` is stripped from the diff).
#[tokio::test]
async fn lookup_subscription_pushes_on_visible_child_change_only() {
    use rtdb_server::query as q;

    let state = test_state().await;
    let db = fresh_db(&state).await;
    let ann = insert(
        &state,
        &db.0,
        "users",
        json!({"name": "Ann", "ownerId": "u1"}),
    )
    .await;
    insert(
        &state,
        &db.0,
        "posts",
        json!({"title": "p1", "authorId": ann}),
    )
    .await;

    let sub_query: q::Query = serde_json::from_value(json!({
        "table": "posts",
        "lookups": [{"field": "authorId", "table": "users", "as": "author"}],
        "fields": ["title", "author"]
    }))
    .expect("parse sub query");

    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    state
        .realtime
        .committers
        .subscribe(
            &db.0,
            next_conn_id(),
            "lookup-sub".to_string(),
            sub_query,
            tx,
            PrincipalCtx::bypass(),
        )
        .await
        .expect("subscribe");
    drain_initial(&mut rx);

    // A child-table (lookup table) write MUST reach the re-run pass: the
    // dispatch gate unions the lookup tables into the write-set check.
    state
        .realtime
        .committers
        .mutate(
            &db.0,
            None,
            Transaction {
                steps: vec![Step::Patch {
                    table: "users".into(),
                    id: ann.clone(),
                    fields: [("name".to_string(), Value::String("Ann2".into()))]
                        .into_iter()
                        .collect(),
                }],
            },
            PrincipalCtx::bypass(),
        )
        .await
        .expect("patch child");
    expect_update(&mut rx, "visible joined-field change must push").await;

    // A second child write changing only `_version` (nothing projected) is
    // re-run but diffs equal — no push. Bump version via a patch writing the
    // same name back.
    state
        .realtime
        .committers
        .mutate(
            &db.0,
            None,
            Transaction {
                steps: vec![Step::Patch {
                    table: "users".into(),
                    id: ann.clone(),
                    fields: [("name".to_string(), Value::String("Ann2".into()))]
                        .into_iter()
                        .collect(),
                }],
            },
            PrincipalCtx::bypass(),
        )
        .await
        .expect("idempotent patch child");
    expect_no_update(&mut rx, "invisible child write must not push");

    // And a parent-table write still flows through the ordinary path.
    state
        .realtime
        .committers
        .mutate(
            &db.0,
            None,
            Transaction {
                steps: vec![Step::Insert {
                    table: "posts".into(),
                    doc: json!({"title": "p2", "authorId": ann})
                        .as_object()
                        .cloned()
                        .expect("object"),
                }],
            },
            PrincipalCtx::bypass(),
        )
        .await
        .expect("parent insert");
    expect_update(&mut rx, "parent insert changes the result set").await;
}
