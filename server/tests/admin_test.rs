mod common;

use common::{admin_post, fresh_db, kanban_schema_json, spawn_app, test_state};

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
