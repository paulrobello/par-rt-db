mod common;

use std::collections::BTreeMap;
use std::sync::Arc;

use common::{spawn_app, test_config, test_hot, wrap_test_db};
use rtdb_server::AppState;
use rtdb_server::auth::{Principal, PrincipalCtx, authorize, resolve_bearer};
use rtdb_server::ddl::push_schema;
use rtdb_server::query::{Query, QueryResult, execute_query};
use rtdb_server::schema::{FieldType, IndexDef, SchemaDef, TableDef};
use rtdb_server::txn::{Step, Transaction, execute_txn};
use serde_json::json;

/// A schema with one owner-gated `notes` table (`ownerField: "userId"`) so the
/// anonymous user's per-row ownership is exercisable end-to-end.
fn owner_schema() -> SchemaDef {
    let mut fields = BTreeMap::new();
    fields.insert("title".to_string(), FieldType::String);
    fields.insert("userId".to_string(), FieldType::String);
    let mut tables = BTreeMap::new();
    tables.insert(
        "notes".to_string(),
        TableDef {
            fields,
            indexes: vec![IndexDef {
                name: "by_user".into(),
                fields: vec!["userId".into()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
            }],
            owner_field: Some("userId".into()),
            collaborators_field: None,
            ttl: None,
            authorize: None,
        },
    );
    SchemaDef { tables }
}

/// AppState with `auth_anonymous_enabled = true`. Mirrors `test_state`'s
/// pool/bootstrap boilerplate (the helper itself hard-codes the default config).
async fn anon_enabled_state() -> Arc<AppState> {
    let mut config = test_config();
    config.auth_anonymous_enabled = true;
    let pool = sqlx::PgPool::connect(&config.database_url)
        .await
        .expect("connect to test postgres");
    rtdb_server::db::bootstrap(&pool)
        .await
        .expect("bootstrap rtdb_auth");
    AppState::new(pool, config, test_hot())
}

/// A `notes` collect query (no index/eq — returns every doc visible to `ctx`,
/// which the owner gate narrows to the caller's own rows).
fn collect_notes() -> Query {
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
        aggregate: None,
        paginate: None,
        filter: None,
        search: None,
        vector_search: None,
        hybrid_search: None,
    }
}

/// Mints an anonymous session via the live HTTP endpoint and returns the token.
async fn mint_anon(addr: std::net::SocketAddr) -> String {
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/auth/anonymous"))
        .send()
        .await
        .expect("POST /auth/anonymous");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("json body");
    body["token"].as_str().expect("token string").to_string()
}

// (anon-a) The endpoint mints a session token + HttpOnly cookie, and the token
// resolves to an anonymous user principal that authorizes for any database
// (no allowlist entry) — criterion 1.
#[tokio::test]
async fn anonymous_mints_session_and_authorizes() -> anyhow::Result<()> {
    let state = anon_enabled_state().await;
    let addr = spawn_app(state.clone()).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/auth/anonymous"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    // The session cookie is set for the browser-dashboard path.
    let set_cookie = resp
        .headers()
        .get("set-cookie")
        .expect("Set-Cookie present")
        .to_str()?;
    assert!(
        set_cookie.contains("rtdb_session="),
        "session cookie: {set_cookie}"
    );
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["user"]["kind"], "user");
    let token = body["token"].as_str().expect("token").to_string();

    // The token resolves to an anonymous user (no email, flagged anonymous).
    let principal = resolve_bearer(&state.pool, &token).await?;
    let anon_uid = match &principal {
        Principal::User {
            anonymous,
            user_id,
            email,
            ..
        } => {
            assert!(*anonymous, "anonymous flag set");
            assert!(email.is_none(), "anonymous user has no email");
            user_id.clone()
        }
        other => panic!("expected User principal, got {other:?}"),
    };
    assert!(!anon_uid.is_empty());

    // Authorizes for an arbitrary database with no allowlist entry (boot gate).
    // (The anon branch returns Ok before any db lookup, so the db need not exist.)
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    authorize(&state.pool, &principal, &db).await?;
    Ok(())
}

// (anon-b) Disabled by default: the endpoint returns a clear FORBIDDEN error
// when RTDB_AUTH_ANONYMOUS_ENABLED is off — criterion 2.
#[tokio::test]
async fn anonymous_disabled_returns_forbidden() -> anyhow::Result<()> {
    let state = common::test_state().await; // auth_anonymous_enabled defaults false
    let addr = spawn_app(state.clone()).await;

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/auth/anonymous"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "FORBIDDEN");
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|m| m.contains("disabled")),
        "message should say disabled: {body}"
    );
    Ok(())
}

// (anon-c) An anonymous user owns its own inserted docs: an insert with no
// `userId` is server-stamped with the anon uid, the anon reads it back, and a
// different principal cannot see it — criterion 3 (per-row owner write).
#[tokio::test]
async fn anonymous_owns_its_inserted_docs() -> anyhow::Result<()> {
    let state = anon_enabled_state().await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_anon(addr).await;
    let anon = resolve_bearer(&state.pool, &token).await?;
    let anon_uid = match &anon {
        Principal::User { user_id, .. } => user_id.clone(),
        _ => panic!("expected User"),
    };
    let ctx = anon.row_ctx();

    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &db).await?;
    let db = wrap_test_db(db);
    let schema = owner_schema();
    push_schema(&state.pool, &db, schema.clone()).await?;

    // Insert a note with NO userId — the server stamps the anon uid (ownerField).
    let outcome = execute_txn(
        &state.pool,
        &db,
        &schema,
        &Transaction {
            steps: vec![Step::Insert {
                table: "notes".to_string(),
                doc: json!({ "title": "my draft" })
                    .as_object()
                    .expect("object")
                    .clone(),
            }],
        },
        &ctx,
    )
    .await?;
    let id = outcome.results[0]["id"].as_str().expect("id").to_string();
    assert_eq!(outcome.write_set.docs.len(), 1);

    // The anon reads its own row back (1 visible doc).
    match execute_query(&state.pool, &db, &schema, &collect_notes(), &ctx).await? {
        QueryResult::Docs(docs) => {
            assert_eq!(docs.len(), 1, "anon sees its own note");
            assert_eq!(docs[0]["_id"].as_str(), Some(id.as_str()));
            assert_eq!(docs[0]["userId"].as_str(), Some(anon_uid.as_str()));
        }
        other => panic!("expected Docs, got {other:?}"),
    }

    // A different principal sees zero rows (the anon's note is not theirs).
    let other_ctx = PrincipalCtx {
        user_id: Some("someone-else".to_string()),
        email: None,
        tables: None,
    };
    match execute_query(&state.pool, &db, &schema, &collect_notes(), &other_ctx).await? {
        QueryResult::Docs(docs) => assert_eq!(docs.len(), 0, "other user sees nothing"),
        other => panic!("expected Docs, got {other:?}"),
    }
    Ok(())
}
