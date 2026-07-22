mod common;

use common::{
    admin_get, admin_post, admin_post_raw, fresh_db, kanban_schema_json, spawn_app, test_state,
};
use rtdb_server::db;
use rtdb_server::txn::{Step, Transaction, execute_txn};

fn fresh_name() -> String {
    format!("t{}", uuid::Uuid::now_v7().simple())
}

// (a) create-db + push-schema via HTTP succeed; columns and index show up as expected.
#[tokio::test]
async fn create_db_and_push_schema_via_http_creates_expected_columns_and_index()
-> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let name = fresh_name();

    let resp = admin_post(addr, "/admin/create-db", serde_json::json!({"name": name})).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = admin_post(
        addr,
        "/admin/push-schema",
        serde_json::json!({"db": name, "schema": kanban_schema_json()}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let pg_schema = format!("db_{name}");
    let columns: Vec<(String, String)> = sqlx::query_as(
        "SELECT column_name::text, is_nullable::text FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = 't_workitems' \
         AND column_name IN ('f_projectid', 'f_status') \
         ORDER BY column_name",
    )
    .bind(pg_schema.as_str())
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        columns,
        vec![
            ("f_projectid".to_string(), "NO".to_string()),
            ("f_status".to_string(), "NO".to_string()),
        ]
    );

    let index_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_indexes WHERE schemaname = $1 AND indexname = $2",
    )
    .bind(pg_schema.as_str())
    .bind("i_workitems_by_project_and_status")
    .fetch_one(&pool)
    .await?;
    assert_eq!(index_count, 1);

    Ok(())
}

// (b) re-pushing the identical schema succeeds (idempotent no-op).
#[tokio::test]
async fn repushing_identical_schema_is_idempotent() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let resp = admin_post(
        addr,
        "/admin/push-schema",
        serde_json::json!({"db": name, "schema": kanban_schema_json()}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    Ok(())
}

// (c) additive push (new optional indexed field on projects + index) adds a nullable column.
#[tokio::test]
async fn additive_push_adds_nullable_column_for_new_indexed_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let mut schema = kanban_schema_json();
    schema["tables"]["projects"]["fields"]["archived"] =
        serde_json::json!({"type": "optional", "inner": {"type": "boolean"}});
    schema["tables"]["projects"]["indexes"]
        .as_array_mut()
        .expect("indexes array")
        .push(serde_json::json!({"name": "by_archived", "fields": ["archived"]}));

    let resp = admin_post(
        addr,
        "/admin/push-schema",
        serde_json::json!({"db": name, "schema": schema}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let pg_schema = format!("db_{name}");
    let nullable: String = sqlx::query_scalar(
        "SELECT is_nullable::text FROM information_schema.columns \
         WHERE table_schema = $1 AND table_name = 't_projects' AND column_name = 'f_archived'",
    )
    .bind(pg_schema.as_str())
    .fetch_one(&pool)
    .await?;
    assert_eq!(nullable, "YES");

    Ok(())
}

// (d) destructive push (drop workItems.title) -> 400 BAD_REQUEST naming workItems.title.
#[tokio::test]
async fn destructive_push_removing_a_field_is_rejected() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let mut schema = kanban_schema_json();
    schema["tables"]["workItems"]["fields"]
        .as_object_mut()
        .expect("fields object")
        .remove("title");

    let resp = admin_post(
        addr,
        "/admin/push-schema",
        serde_json::json!({"db": name, "schema": schema}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "BAD_REQUEST");
    assert!(
        body["message"]
            .as_str()
            .expect("message string")
            .contains("workItems.title")
    );

    Ok(())
}

// (d2) B1: push-schema against a database that was never created -> 404 NOT_FOUND
// (not a raw 500 from the missing schema's `meta` table).
#[tokio::test]
async fn push_schema_against_unknown_database_is_not_found() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;
    let name = fresh_name();

    let resp = admin_post(
        addr,
        "/admin/push-schema",
        serde_json::json!({"db": name, "schema": kanban_schema_json()}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "NOT_FOUND");

    Ok(())
}

// (e) wrong admin key -> 401 UNAUTHORIZED.
#[tokio::test]
async fn wrong_admin_key_is_unauthorized() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/admin/create-db"))
        .header("Authorization", "Bearer wrong-key")
        .json(&serde_json::json!({"name": fresh_name()}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "UNAUTHORIZED");

    Ok(())
}

// (f) bad db name -> 400.
#[tokio::test]
async fn bad_database_name_is_rejected() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;

    let resp = admin_post(
        addr,
        "/admin/create-db",
        serde_json::json!({"name": "Bad-Name"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "BAD_REQUEST");

    Ok(())
}

// (g) GET /admin/dbs lists the created db.
#[tokio::test]
async fn list_dbs_includes_created_database() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/admin/dbs"))
        .header("Authorization", "Bearer test-admin-key")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let body: serde_json::Value = resp.json().await?;
    let databases = body["databases"].as_array().expect("databases array");
    assert!(
        databases
            .iter()
            .any(|value| value.as_str() == Some(name.as_str()))
    );

    Ok(())
}

// (h) export then import into a fresh database round-trips docs, indexes, and schema.
#[tokio::test]
async fn export_then_import_round_trips_docs_indexes_and_schema() -> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let source_db = fresh_db(&state).await;

    let schema = state.schemas.get(&pool, &source_db).await?;
    let insert_outcome = execute_txn(
        &pool,
        &source_db,
        &schema,
        &Transaction {
            steps: vec![
                Step::Insert {
                    table: "projects".to_string(),
                    doc: serde_json::json!({
                        "name": "Roadmap",
                        "status": "active",
                        "tags": ["q3"],
                        "updatedAt": 1
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                },
                Step::Insert {
                    table: "projects".to_string(),
                    doc: serde_json::json!({
                        "name": "Archive",
                        "status": "archived",
                        "tags": [],
                        "updatedAt": 2
                    })
                    .as_object()
                    .unwrap()
                    .clone(),
                },
            ],
        },
    )
    .await?;
    let project_id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("project id")
        .to_string();

    execute_txn(
        &pool,
        &source_db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "workItems".to_string(),
                doc: serde_json::json!({
                    "projectId": project_id,
                    "title": "Ship it",
                    "status": "in_progress",
                    "order": 1
                })
                .as_object()
                .unwrap()
                .clone(),
            }],
        },
    )
    .await?;

    let export_resp = admin_get(addr, &format!("/admin/export-db?db={source_db}")).await;
    assert_eq!(export_resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        export_resp
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok()),
        Some("application/x-ndjson")
    );
    let jsonl = export_resp.text().await?;
    let lines: Vec<&str> = jsonl.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 4); // 1 schema line + 2 projects + 1 workItem

    let target_db = fresh_name();
    let resp = admin_post(
        addr,
        "/admin/create-db",
        serde_json::json!({"name": target_db}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let import_resp = admin_post_raw(
        addr,
        &format!("/admin/import-db?db={target_db}"),
        jsonl.clone(),
    )
    .await;
    assert_eq!(import_resp.status(), reqwest::StatusCode::OK);
    let import_body: serde_json::Value = import_resp.json().await?;
    assert_eq!(import_body["ok"], true);

    let source_schema = db::load_schema(&pool, &source_db)
        .await?
        .expect("source schema");
    let target_schema = db::load_schema(&pool, &target_db)
        .await?
        .expect("target schema");
    assert_eq!(source_schema, target_schema);

    let source_projects: Vec<(String, serde_json::Value, i64, i64)> = sqlx::query_as(&format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"db_{source_db}\".\"t_projects\" ORDER BY \"id\""
    ))
    .fetch_all(&pool)
    .await?;
    let target_projects: Vec<(String, serde_json::Value, i64, i64)> = sqlx::query_as(&format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"db_{target_db}\".\"t_projects\" ORDER BY \"id\""
    ))
    .fetch_all(&pool)
    .await?;
    assert_eq!(source_projects, target_projects);

    let source_items: Vec<(String, serde_json::Value, i64, i64)> = sqlx::query_as(&format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"db_{source_db}\".\"t_workitems\" ORDER BY \"id\""
    ))
    .fetch_all(&pool)
    .await?;
    let target_items: Vec<(String, serde_json::Value, i64, i64)> = sqlx::query_as(&format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"db_{target_db}\".\"t_workitems\" ORDER BY \"id\""
    ))
    .fetch_all(&pool)
    .await?;
    assert_eq!(source_items, target_items);

    let index_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_indexes WHERE schemaname = $1 AND indexname = $2",
    )
    .bind(format!("db_{target_db}"))
    .bind("i_workitems_by_project_and_status")
    .fetch_one(&pool)
    .await?;
    assert_eq!(index_count, 1);

    Ok(())
}

// (i) export of an empty database (schema pushed, no docs) yields just the schema line.
#[tokio::test]
async fn export_of_empty_database_yields_only_schema_line() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let resp = admin_get(addr, &format!("/admin/export-db?db={name}")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let jsonl = resp.text().await?;
    let lines: Vec<&str> = jsonl.lines().filter(|l| !l.trim().is_empty()).collect();
    assert_eq!(lines.len(), 1);

    let line: serde_json::Value = serde_json::from_str(lines[0])?;
    assert_eq!(line["kind"], "schema");
    assert_eq!(line["schema"], kanban_schema_json());

    Ok(())
}

// (j) wrong admin key on export-db -> 401 UNAUTHORIZED.
#[tokio::test]
async fn export_db_wrong_admin_key_is_unauthorized() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/admin/export-db?db={name}"))
        .header("Authorization", "Bearer wrong-key")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "UNAUTHORIZED");

    Ok(())
}

// (k) wrong admin key on import-db -> 401 UNAUTHORIZED.
#[tokio::test]
async fn import_db_wrong_admin_key_is_unauthorized() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/admin/import-db?db={}", fresh_name()))
        .header("Authorization", "Bearer wrong-key")
        .body("{}")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "UNAUTHORIZED");

    Ok(())
}

// (l) export-db against an unknown database -> 404 NOT_FOUND.
#[tokio::test]
async fn export_db_of_unknown_database_is_not_found() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;

    let resp = admin_get(addr, &format!("/admin/export-db?db={}", fresh_name())).await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "NOT_FOUND");

    Ok(())
}

// (m) import-db into an unknown database -> 404 NOT_FOUND.
#[tokio::test]
async fn import_db_into_unknown_database_is_not_found() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;

    let jsonl = format!(
        "{}\n",
        serde_json::json!({"kind": "schema", "schema": kanban_schema_json()})
    );
    let resp = admin_post_raw(
        addr,
        &format!("/admin/import-db?db={}", fresh_name()),
        jsonl,
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "NOT_FOUND");

    Ok(())
}

// (n) B3: when import-db's doc-replay phase fails (id collision) after its
// internal push_schema already committed the new schema to Postgres, the
// stale pre-import schema cache entry must be invalidated rather than left
// serving the old schema forever.
#[tokio::test]
async fn import_db_doc_replay_failure_after_schema_commit_refreshes_schema_cache()
-> anyhow::Result<()> {
    let state = test_state().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let target_db = fresh_db(&state).await;

    // Warm the schema cache with the pre-import schema, as a running server would
    // have it cached from ordinary traffic against `target_db`.
    let old_schema = state.schemas.get(&pool, &target_db).await?;
    assert!(
        !old_schema.tables["projects"]
            .fields
            .contains_key("priority")
    );

    // Seed one document so its id can collide with an imported doc line, forcing
    // the doc-replay phase to fail after `import_database`'s internal
    // `push_schema` call has already committed.
    let insert_outcome = execute_txn(
        &pool,
        &target_db,
        &old_schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "projects".to_string(),
                doc: serde_json::json!({
                    "name": "Existing",
                    "status": "active",
                    "tags": [],
                    "updatedAt": 1
                })
                .as_object()
                .unwrap()
                .clone(),
            }],
        },
    )
    .await?;
    let existing_id = insert_outcome.results[0]["id"]
        .as_str()
        .expect("project id")
        .to_string();

    let mut new_schema = kanban_schema_json();
    new_schema["tables"]["projects"]["fields"]["priority"] =
        serde_json::json!({"type": "optional", "inner": {"type": "number"}});

    let jsonl = format!(
        "{}\n{}\n",
        serde_json::json!({"kind": "schema", "schema": new_schema}),
        serde_json::json!({
            "kind": "doc",
            "table": "projects",
            "id": existing_id,
            "doc": {"name": "Colliding", "status": "active", "tags": [], "updatedAt": 2},
            "createdAt": 2,
            "version": 1
        })
    );

    let import_resp =
        admin_post_raw(addr, &format!("/admin/import-db?db={target_db}"), jsonl).await;
    assert_eq!(
        import_resp.status(),
        reqwest::StatusCode::INTERNAL_SERVER_ERROR
    );

    // A stale cache entry would still report the pre-import schema here; the
    // fix invalidates it on the failed import so this reload reflects what the
    // committed `push_schema` actually wrote to Postgres.
    let refreshed_schema = state.schemas.get(&pool, &target_db).await?;
    assert!(
        refreshed_schema.tables["projects"]
            .fields
            .contains_key("priority")
    );

    Ok(())
}
