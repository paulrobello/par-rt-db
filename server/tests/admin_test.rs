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

// delete-db happy path: confirm==name removes the schema, the registry row, the
// per-db allowlist row, and CASCADE-drops every document table `push_schema`
// created. After it, GET /admin/dbs no longer lists the db and the next
// create-db for the same name succeeds (registry row is gone). Minted tokens
// for the db are also purged.
#[tokio::test]
async fn delete_db_drops_schema_and_registry_row_and_cascades_document_tables() -> anyhow::Result<()>
{
    let state = test_state().await;
    let pool = state.pool.clone();
    let addr = spawn_app(state.clone()).await;
    let name = fresh_name();

    // Create + push a schema (so document tables exist) + mint a token + allow
    // a user, to prove CASCADE/DELETE reaches every per-db surface.
    admin_post(addr, "/admin/create-db", serde_json::json!({"name": name})).await;
    admin_post(
        addr,
        "/admin/push-schema",
        serde_json::json!({"db": name, "schema": kanban_schema_json()}),
    )
    .await;
    let minted: serde_json::Value = admin_post(
        addr,
        "/admin/mint-token",
        serde_json::json!({"db": name, "name": "ci"}),
    )
    .await
    .json()
    .await?;
    let token_id = minted["tokenId"].as_str().unwrap().to_string();
    admin_post(
        addr,
        "/admin/allowlist",
        serde_json::json!({"db": name, "action": "add", "email": "a@b.com"}),
    )
    .await;

    let resp = admin_post(
        addr,
        "/admin/delete-db",
        serde_json::json!({"name": name, "confirm": name}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body, serde_json::json!({"ok": true}));

    // Schema gone (CASCADE removed meta, mutations, scheduled_txns, storage, and
    // the document tables push_schema created).
    let pg_schema = format!("db_{name}");
    let schema_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.schemata WHERE schema_name = $1",
    )
    .bind(&pg_schema)
    .fetch_one(&pool)
    .await?;
    assert_eq!(schema_count, 0, "schema {pg_schema} should be gone");

    // Registry row gone.
    let (in_registry,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM rtdb_auth.databases WHERE name = $1")
            .bind(&name)
            .fetch_one(&pool)
            .await?;
    assert_eq!(in_registry, 0, "registry row should be gone");

    // Token row for this db purged.
    let (tokens_for_db,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM rtdb_auth.machine_tokens WHERE db_name = $1")
            .bind(&name)
            .fetch_one(&pool)
            .await?;
    assert_eq!(tokens_for_db, 0, "tokens for db should be purged");
    let (token_id_row,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM rtdb_auth.machine_tokens WHERE id = $1")
            .bind(&token_id)
            .fetch_one(&pool)
            .await?;
    assert_eq!(token_id_row, 0, "minted token row should be purged");

    // Allowlist row for this db purged.
    let (allowlist_for_db,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM rtdb_auth.allowlist WHERE db_name = $1")
            .bind(&name)
            .fetch_one(&pool)
            .await?;
    assert_eq!(
        allowlist_for_db, 0,
        "allowlist rows for db should be purged"
    );

    // GET /admin/dbs no longer lists it.
    let resp = admin_get(addr, "/admin/dbs").await;
    let body: serde_json::Value = resp.json().await?;
    let listed = body["databases"]
        .as_array()
        .expect("databases array")
        .iter()
        .any(|d| d.as_str() == Some(&name));
    assert!(
        !listed,
        "deleted db should not appear in /admin/dbs: {body}"
    );

    // The same name is reclaimable — registry row is gone, so create succeeds.
    let resp = admin_post(addr, "/admin/create-db", serde_json::json!({"name": name})).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    // Clean up the reclaimed db to keep the suite's Postgres tidy.
    admin_post(
        addr,
        "/admin/delete-db",
        serde_json::json!({"name": name, "confirm": name}),
    )
    .await;
    Ok(())
}

// delete-db requires confirm == name exactly; a mismatch yields 400 and changes
// nothing.
#[tokio::test]
async fn delete_db_rejects_mismatched_confirm_with_400_and_changes_nothing() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_name();
    admin_post(addr, "/admin/create-db", serde_json::json!({"name": name})).await;

    let resp = admin_post(
        addr,
        "/admin/delete-db",
        serde_json::json!({"name": name, "confirm": "not-the-name"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // The db still exists.
    let resp = admin_get(addr, "/admin/dbs").await;
    let body: serde_json::Value = resp.json().await?;
    let listed = body["databases"]
        .as_array()
        .expect("databases array")
        .iter()
        .any(|d| d.as_str() == Some(&name));
    assert!(listed, "db should still be listed after a rejected delete");

    // Clean up.
    admin_post(
        addr,
        "/admin/delete-db",
        serde_json::json!({"name": name, "confirm": name}),
    )
    .await;
    Ok(())
}

// delete-db on a name that was never registered returns 404, not 500.
#[tokio::test]
async fn delete_db_on_unknown_name_returns_404() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_name();

    let resp = admin_post(
        addr,
        "/admin/delete-db",
        serde_json::json!({"name": name, "confirm": name}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    Ok(())
}

// delete-db is idempotent in the partial-deletion sense: after a successful
// delete, a second delete 404s (it is no longer in the registry) rather than
// 500ing. `DROP SCHEMA IF EXISTS` keeps the durable step safe even if the
// schema was already gone.
#[tokio::test]
async fn delete_db_double_delete_returns_404_not_500() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_name();
    admin_post(addr, "/admin/create-db", serde_json::json!({"name": name})).await;

    let resp = admin_post(
        addr,
        "/admin/delete-db",
        serde_json::json!({"name": name, "confirm": name}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = admin_post(
        addr,
        "/admin/delete-db",
        serde_json::json!({"name": name, "confirm": name}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
    Ok(())
}

// delete-db is admin-gated: a missing bearer yields 401, not a deletion.
#[tokio::test]
async fn delete_db_requires_admin() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_name();
    admin_post(addr, "/admin/create-db", serde_json::json!({"name": name})).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/admin/delete-db"))
        .json(&serde_json::json!({"name": name, "confirm": name}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // The db still exists.
    let resp = admin_get(addr, "/admin/dbs").await;
    let body: serde_json::Value = resp.json().await?;
    let listed = body["databases"]
        .as_array()
        .expect("databases array")
        .iter()
        .any(|d| d.as_str() == Some(&name));
    assert!(
        listed,
        "db should still be listed after an unauthenticated delete"
    );

    // Clean up.
    admin_post(
        addr,
        "/admin/delete-db",
        serde_json::json!({"name": name, "confirm": name}),
    )
    .await;
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
        None,
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
        None,
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
        None,
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

// Admin schedule endpoints (GET /admin/db/{db}/schedules, POST .../schedules,
// .../schedules/{id}/{cancel,pause,resume}) are thin admin-gated wrappers over
// the same scheduler accessors as the per-db /api/schedule* routes. End-to-end:
// create a one-shot (far-future afterMs so the scheduler, not spawned for this
// db during the test, can't drain it), list it, pause → paused, resume →
// pending, cancel → list empty. The scheduler task is not started for this db
// (no mutate/subscribe touches it), so the row sits untouched between calls.
#[tokio::test]
async fn admin_schedule_endpoints_create_pause_resume_cancel_roundtrip() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    // Create: admin POST /admin/db/{db}/schedules.
    let resp = admin_post(
        addr,
        &format!("/admin/db/{name}/schedules"),
        serde_json::json!({
            "when": {"type": "afterMs", "ms": 3_600_000},
            "txn": {"steps": []},
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let id = body["id"].as_str().expect("schedule id").to_string();

    // List: admin GET /admin/db/{db}/schedules shows the pending one-shot.
    let resp = admin_get(addr, &format!("/admin/db/{name}/schedules")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let listed = body["schedules"].as_array().expect("schedules array");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0]["id"], serde_json::json!(id));
    assert_eq!(listed[0]["kind"], serde_json::json!("oneshot"));
    assert_eq!(listed[0]["status"], serde_json::json!("pending"));

    // Pause → status paused.
    let resp = admin_post(
        addr,
        &format!("/admin/db/{name}/schedules/{id}/pause"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.json::<serde_json::Value>().await?["ok"],
        serde_json::json!(true)
    );
    let body: serde_json::Value = admin_get(addr, &format!("/admin/db/{name}/schedules"))
        .await
        .json()
        .await?;
    assert_eq!(body["schedules"][0]["status"], serde_json::json!("paused"));

    // Resume → status pending again.
    let resp = admin_post(
        addr,
        &format!("/admin/db/{name}/schedules/{id}/resume"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.json::<serde_json::Value>().await?["ok"],
        serde_json::json!(true)
    );
    let body: serde_json::Value = admin_get(addr, &format!("/admin/db/{name}/schedules"))
        .await
        .json()
        .await?;
    assert_eq!(body["schedules"][0]["status"], serde_json::json!("pending"));

    // Cancel → row deleted → list empty.
    let resp = admin_post(
        addr,
        &format!("/admin/db/{name}/schedules/{id}/cancel"),
        serde_json::json!({}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        resp.json::<serde_json::Value>().await?["ok"],
        serde_json::json!(true)
    );
    let body: serde_json::Value = admin_get(addr, &format!("/admin/db/{name}/schedules"))
        .await
        .json()
        .await?;
    assert!(
        body["schedules"]
            .as_array()
            .expect("schedules array")
            .is_empty()
    );

    Ok(())
}

// Admin schedule endpoints require the admin key — a non-admin bearer (here a
// machine token for the db) is rejected. The token is valid for ordinary
// per-db /api/schedule routes but must NOT gate onto /admin/*.
#[tokio::test]
async fn admin_schedule_endpoints_reject_non_admin_bearer() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    // A wrong admin bearer on the list route is 401.
    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/admin/db/{name}/schedules"))
        .header("Authorization", "Bearer not-the-admin-key")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    Ok(())
}

// Admin schedule routes 404 on an unknown database (mirrors /admin/db/{db}/query).
#[tokio::test]
async fn admin_schedule_list_unknown_db_is_404() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let bogus = fresh_name();

    let resp = admin_get(addr, &format!("/admin/db/{bogus}/schedules")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    Ok(())
}
