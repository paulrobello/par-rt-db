//! Durable change feed (`GET /api/db/{db}/changes`) — integration tests.
//!
//! Covers the write-path stamping contract (kinds, post-images, net-per-id
//! rows, rollback atomicity, contiguity) at the `execute_txn` engine level,
//! and the HTTP surface (machine-token gate, allowlist filtering, pagination
//! `nextSeq` rules, expired-cursor typing, read-path table seeding, snapshot
//! import stamping). See the change-feed design spec for the contract.

use crate::common::{
    admin_post, fresh_db, kanban_schema_json, mint_user_session, spawn_app, test_state,
    wrap_test_db,
};
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::change_log;
use rtdb_server::db;
use rtdb_server::error::ErrorCode;
use rtdb_server::schema::SchemaDef;
use rtdb_server::snapshot;
use rtdb_server::txn::{Step, Transaction, execute_txn};

fn kanban_schema() -> SchemaDef {
    serde_json::from_value(kanban_schema_json()).expect("parse kanban schema")
}

fn doc(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    value.as_object().expect("json object").clone()
}

fn project_doc(name: &str) -> serde_json::Map<String, serde_json::Value> {
    doc(serde_json::json!({
        "name": name,
        "description": null,
        "status": "active",
        "tags": ["a"],
        "updatedAt": 1.0
    }))
}

async fn insert_project(pool: &sqlx::PgPool, db: &str, name: &str) -> anyhow::Result<String> {
    let outcome = execute_txn(
        pool,
        db,
        &kanban_schema(),
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: project_doc(name),
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await?;
    let id = outcome.results[0]["id"]
        .as_str()
        .expect("minted id")
        .to_string();
    Ok(id)
}

// --- engine-level stamping ---

#[tokio::test]
async fn stamping_records_kind_and_post_image_per_write() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let pool = state.pool.clone();

    let inserted = insert_project(&pool, &db.0, "alpha").await?;
    let id2 = insert_project(&pool, &db.0, "beta").await?;

    // A delete lands as kind=delete with no post-image.
    execute_txn(
        &pool,
        &db.0,
        &kanban_schema(),
        &Transaction {
            steps: vec![Step::Delete {
                table: "projects".to_string(),
                id: id2.clone(),
            }],
        },
        &PrincipalCtx::bypass(),
    )
    .await?;

    let head = change_log::head(&pool, &db.0).await?;
    assert_eq!(head.seq, 3, "three writes, three net rows");
    assert!(!head.log_id.is_empty(), "log_id minted at seed time");

    let page = change_log::read_page(&pool, &db.0, 0, None, 100).await?;
    let kinds: Vec<&str> = page.ops.iter().map(|op| op.kind.as_str()).collect();
    assert_eq!(kinds, ["insert", "insert", "delete"]);
    assert_eq!(page.ops[0].doc_id, inserted);
    let body = page.ops[0].doc.as_ref().expect("insert carries post-image");
    assert_eq!(body["name"], serde_json::json!("alpha"));
    assert!(page.ops[2].doc.is_none(), "delete rows carry no post-image");
    assert_eq!(page.ops[2].doc_id, id2);
    // Seqs are contiguous and ascending (transactional counter, no gaps).
    assert_eq!(
        page.ops.iter().map(|op| op.seq).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    Ok(())
}

#[tokio::test]
async fn rolled_back_txn_leaves_no_rows_and_no_gap() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let pool = state.pool.clone();
    insert_project(&pool, &db.0, "alpha").await?;

    // A valid insert followed by a bad step (unknown table) aborts the txn.
    let err = execute_txn(
        &pool,
        &db.0,
        &kanban_schema(),
        &Transaction {
            steps: vec![
                Step::Insert {
                    table: "projects".to_string(),
                    doc: project_doc("ghost"),
                },
                Step::Delete {
                    table: "no_such_table".to_string(),
                    id: "x".to_string(),
                },
            ],
        },
        &PrincipalCtx::bypass(),
    )
    .await;
    assert!(err.is_err(), "unknown-table step fails the txn");

    let head = change_log::head(&pool, &db.0).await?;
    assert_eq!(head.seq, 1, "rollback advances nothing");
    let page = change_log::read_page(&pool, &db.0, 0, None, 100).await?;
    assert_eq!(page.ops.len(), 1, "only the committed write is in the log");
    assert_eq!(
        page.ops[0].seq, 1,
        "next committed write keeps the seq contiguous"
    );
    Ok(())
}

#[tokio::test]
async fn net_row_for_insert_then_delete_in_one_txn() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let pool = state.pool.clone();
    let id = insert_project(&pool, &db.0, "ephemeral").await?;
    let before = change_log::head(&pool, &db.0).await?.seq;

    // Re-insert the same id (upsert hits it) then delete it, in ONE txn: the
    // touches collapse to one net row — kind delete, doc NULL.
    execute_txn(
        &pool,
        &db.0,
        &kanban_schema(),
        &Transaction {
            steps: vec![
                Step::Upsert {
                    table: "projects".to_string(),
                    index: "by_name".to_string(),
                    eq: vec![serde_json::json!("ephemeral")],
                    insert: project_doc("ephemeral"),
                    patch: doc(serde_json::json!({"updatedAt": 3.0})),
                },
                Step::Delete {
                    table: "projects".to_string(),
                    id: id.clone(),
                },
            ],
        },
        &PrincipalCtx::bypass(),
    )
    .await?;

    let page = change_log::read_page(&pool, &db.0, before, None, 100).await?;
    assert_eq!(
        page.ops.len(),
        1,
        "two touches of one id collapse to one net row"
    );
    assert_eq!(page.ops[0].kind, "delete");
    assert_eq!(page.ops[0].doc_id, id);
    assert!(page.ops[0].doc.is_none());
    Ok(())
}

// --- HTTP surface ---

async fn mint_machine_token(
    addr: std::net::SocketAddr,
    db_name: &str,
    tables: Option<Vec<String>>,
) -> String {
    let resp = admin_post(
        addr,
        "/admin/mint-token",
        serde_json::json!({ "db": db_name, "name": "change-feed-test", "tables": tables }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "mint ok");
    let body: serde_json::Value = resp.json().await.expect("mint json");
    body["token"].as_str().expect("token").to_string()
}

async fn get_changes(
    addr: std::net::SocketAddr,
    db_name: &str,
    token: &str,
    query: &str,
) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("http://{addr}/api/db/{db_name}/changes{query}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .expect("send changes request")
}

#[tokio::test]
async fn http_route_serves_pages_and_nextseq_rules() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let _guard = crate::common::background_guard(&state);
    let token = mint_machine_token(addr, &db.0, None).await;

    // Empty log: since=0 is a valid cursor; head=0; logId present.
    let resp = get_changes(addr, &db.0, &token, "").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["ops"], serde_json::json!([]));
    assert_eq!(body["nextSeq"], serde_json::json!(0));
    assert_eq!(body["head"], serde_json::json!(0));
    let log_id = body["logId"].as_str().expect("logId").to_string();
    assert!(!log_id.is_empty());

    // Three writes, then a full page (limit 2) carries last-returned nextSeq.
    for name in ["a", "b", "c"] {
        insert_project(&state.pool, &db.0, name).await?;
    }
    let resp = get_changes(addr, &db.0, &token, "?limit=2").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["ops"].as_array().expect("ops").len(), 2);
    assert_eq!(body["nextSeq"], serde_json::json!(2), "full page: last seq");
    assert_eq!(body["head"], serde_json::json!(3));

    // Resume from the full page's cursor: the short tail page jumps to head.
    let resp = get_changes(addr, &db.0, &token, "?since=2").await;
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["ops"].as_array().expect("ops").len(), 1);
    assert_eq!(body["nextSeq"], serde_json::json!(3), "short page: head");
    assert_eq!(body["logId"], serde_json::json!(log_id));

    // Re-reading the same cursor returns the same page (idempotent).
    let again = get_changes(addr, &db.0, &token, "?since=2").await;
    let again: serde_json::Value = again.json().await?;
    assert_eq!(again, body);
    Ok(())
}

#[tokio::test]
async fn filtered_short_page_jumps_nextseq_to_head() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let _guard = crate::common::background_guard(&state);
    // Scoped to a table nothing writes: every page is empty but must still
    // advance the cursor (the livelock the full-page rule exists for).
    let scoped = mint_machine_token(addr, &db.0, Some(vec!["tasks".to_string()])).await;

    insert_project(&state.pool, &db.0, "p1").await?;

    let resp = get_changes(addr, &db.0, &scoped, "").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["ops"], serde_json::json!([]));
    assert_eq!(
        body["nextSeq"],
        serde_json::json!(1),
        "short filtered page: head"
    );
    Ok(())
}

#[tokio::test]
async fn user_principal_is_forbidden_and_cross_db_token_rejected() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let _guard = crate::common::background_guard(&state);

    let session = mint_user_session(&state.pool, "user-feed", "feed@example.com").await;
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/api/db/{}/changes", db.0))
        .header("Authorization", format!("Bearer {session}"))
        .send()
        .await
        .expect("send");
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    let body: serde_json::Value = resp.json().await.expect("envelope");
    assert_eq!(body["code"], serde_json::json!("FORBIDDEN"));

    // A token minted for a foreign db is rejected by the per-db authorize
    // gate (Forbidden — the db exists, the token just isn't scoped for it).
    let other = fresh_db(&state).await;
    let foreign = mint_machine_token(addr, &other.0, None).await;
    let resp = get_changes(addr, &db.0, &foreign, "").await;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    Ok(())
}

#[tokio::test]
async fn expired_cursor_is_typed_never_silent() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let _guard = crate::common::background_guard(&state);
    let token = mint_machine_token(addr, &db.0, None).await;

    for name in ["a", "b", "c"] {
        insert_project(&state.pool, &db.0, name).await?;
    }

    // Ahead-of-log cursor: typed CURSOR_EXPIRED (410).
    let resp = get_changes(addr, &db.0, &token, "?since=999").await;
    assert_eq!(resp.status(), reqwest::StatusCode::GONE);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], serde_json::json!("CURSOR_EXPIRED"));

    // Below-retention cursor: trim down to the newest 1 row, then a since=0
    // cursor predates the window (oldest retained = 3 > 0 + 1).
    change_log::trim_expired(&state.pool, &db.0, 1).await?;
    let resp = get_changes(addr, &db.0, &token, "?since=0").await;
    assert_eq!(resp.status(), reqwest::StatusCode::GONE);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], serde_json::json!("CURSOR_EXPIRED"));

    // The boundary is exact: since = oldest-1 is still servable.
    let resp = get_changes(addr, &db.0, &token, "?since=2").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["ops"].as_array().expect("ops").len(), 1);
    Ok(())
}

#[tokio::test]
async fn read_seeds_missing_tables_on_old_dbs() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let addr = spawn_app(state.clone()).await;
    let _guard = crate::common::background_guard(&state);
    let token = mint_machine_token(addr, &db.0, None).await;

    // Simulate a pre-change-feed db: drop the tables the read path needs.
    let schema = rtdb_server::ddl::pg_schema(&db.0);
    for table in ["changes", "change_head"] {
        sqlx::query(&format!("DROP TABLE \"{schema}\".\"{table}\""))
            .execute(&state.pool)
            .await?;
    }
    let resp = get_changes(addr, &db.0, &token, "").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "read re-seeds");
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["head"], serde_json::json!(0));
    assert!(!body["logId"].as_str().expect("logId").is_empty());
    Ok(())
}

#[tokio::test]
async fn snapshot_import_stamps_the_feed() -> anyhow::Result<()> {
    let state = test_state().await;
    let source = fresh_db(&state).await;
    let schema = kanban_schema();
    insert_project(&state.pool, &source.0, "imported").await?;
    insert_project(&state.pool, &source.0, "imported2").await?;

    let jsonl = snapshot::export_database(&state.pool, &source.0, &schema).await?;

    // Import into a LIVE second database (fresh, but with its own head row).
    let target = wrap_test_db(format!("t{}", uuid::Uuid::now_v7().simple()));
    db::create_database(&state.pool, &target.0).await?;
    let before = change_log::head(&state.pool, &target.0).await?;
    assert_eq!(before.seq, 0);

    snapshot::import_database(&state.pool, &target.0, &jsonl).await?;

    let head = change_log::head(&state.pool, &target.0).await?;
    assert_eq!(head.seq, 2, "both restored rows are observable");
    let page = change_log::read_page(&state.pool, &target.0, 0, None, 100).await?;
    assert_eq!(page.ops.len(), 2);
    assert!(page.ops.iter().all(|op| op.kind == "insert"));
    let body = page.ops[0].doc.as_ref().expect("post-image");
    assert_eq!(body["name"], serde_json::json!("imported"));
    Ok(())
}

#[tokio::test]
async fn patch_clamps_retention_and_restores_default_on_zero() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let _guard = crate::common::background_guard(&state);

    let patch = |addr: std::net::SocketAddr, body: serde_json::Value| {
        reqwest::Client::new()
            .patch(format!("http://{addr}/admin/config"))
            .header("Authorization", "Bearer test-admin-key")
            .json(&body)
            .send()
    };
    // 5 clamps up to MIN_MAX_ROWS; 0 restores the default.
    let resp = patch(addr, serde_json::json!({"changeLogMaxRows": 5})).await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(
        body["hot"]["changeLogMaxRows"],
        serde_json::json!(rtdb_server::change_log::MIN_MAX_ROWS)
    );
    let resp = patch(addr, serde_json::json!({"changeLogMaxRows": 0})).await?;
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(
        body["hot"]["changeLogMaxRows"],
        serde_json::json!(rtdb_server::change_log::DEFAULT_MAX_ROWS)
    );
    Ok(())
}

#[test]
fn cursor_expired_wire_shape() {
    // The exhaustive corpus regeneration test (`error_codes_match_wire_corpus`)
    // pins the committed JSON against the enum; this pins the code's own wire
    // name and HTTP status, which the feed route emits.
    let err = rtdb_server::error::RtDbError::new(ErrorCode::CursorExpired, "gone");
    let json = serde_json::to_value(&err).unwrap();
    assert_eq!(json["code"], serde_json::json!("CURSOR_EXPIRED"));
    assert_eq!(err.status(), reqwest::StatusCode::GONE);
}
