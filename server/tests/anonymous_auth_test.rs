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
            defaults: std::collections::BTreeMap::new(),
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
            updated_at_field: None,
            authorize: None,
            soft_delete: false,
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
// resolves to an anonymous user principal. SEC-103: an anonymous principal is
// authorized ONLY for a database that has opted in via
// `rtdb_auth.databases.anonymous_enabled`; a db that exists but has NOT opted
// in is rejected (Forbidden). The master kill switch
// (`RTDB_AUTH_ANONYMOUS_ENABLED`) is checked at mint — criterion 1.
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

    // SEC-103: create db A and opt it in, then authorize succeeds.
    let db_a = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &db_a).await?;
    opt_in_anonymous(&state.pool, &db_a).await;
    authorize(&state.pool, &principal, &db_a).await?;

    // SEC-103: create db B but do NOT opt it in → authorize rejects (Forbidden).
    let db_b = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &db_b).await?;
    let err = authorize(&state.pool, &principal, &db_b)
        .await
        .expect_err("anon not authorized for non-opted-in db B");
    assert_eq!(err.code, rtdb_server::error::ErrorCode::Forbidden);
    Ok(())
}

/// SEC-103: opts a database in to anonymous principal access by setting the
/// `rtdb_auth.databases.anonymous_enabled` column directly (the test analogue
/// of `PATCH /admin/db/{db}/anonymous-access`).
async fn opt_in_anonymous(pool: &sqlx::PgPool, db: &str) {
    sqlx::query("UPDATE rtdb_auth.databases SET anonymous_enabled = TRUE WHERE name = $1")
        .bind(db)
        .execute(pool)
        .await
        .expect("opt in anonymous");
}

// (anon-sec103) The per-database gate is the core SEC-103 fix: an anonymous
// principal authorized for db A is rejected for db B. This closes the "enabling
// anon for one guest app makes EVERY database reachable" hole. The master kill
// is on (anon was minted); db A opted in; db B did not.
#[tokio::test]
async fn sec103_anon_authorized_for_a_rejected_for_b() -> anyhow::Result<()> {
    let state = anon_enabled_state().await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_anon(addr).await;
    let principal = resolve_bearer(&state.pool, &token).await?;

    let db_a = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &db_a).await?;
    opt_in_anonymous(&state.pool, &db_a).await;

    let db_b = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &db_b).await?;

    // Authorized for A (opted in).
    authorize(&state.pool, &principal, &db_a).await?;
    // Rejected for B (not opted in) — the SEC-103 property.
    let err = authorize(&state.pool, &principal, &db_b)
        .await
        .expect_err("anon must be rejected for non-opted-in db B");
    assert_eq!(err.code, rtdb_server::error::ErrorCode::Forbidden);
    // And the message names the per-db gate so an operator can diagnose it.
    assert!(
        err.message
            .contains("anonymous access is not enabled for this database"),
        "expected per-db anon message, got: {}",
        err.message
    );
    Ok(())
}

// (anon-sec103-ttl) Anonymous sessions use the short independent TTL
// (`anonymous_session_ttl_days`, default 1) rather than the standard 30-day
// session TTL, so the ephemeral rows don't persist for a month. Verified by
// reading back the session row's `expires_at`.
#[tokio::test]
async fn sec103_anon_session_uses_short_ttl() -> anyhow::Result<()> {
    let state = anon_enabled_state().await;
    let addr = spawn_app(state.clone()).await;
    let token = mint_anon(addr).await;

    // The minted session row's expires_at should be ~1 day out (not ~30).
    // `anonymous_session_ttl_days` defaults to 1 in test_config.
    let hash = rtdb_server::db::sha256_hex(&token);
    let (expires_at_ms,): (i64,) =
        sqlx::query_as("SELECT expires_at FROM rtdb_auth.sessions WHERE token_hash = $1")
            .bind(&hash)
            .fetch_one(&state.pool)
            .await?;
    let now = rtdb_server::db::now_ms();
    let delta_days = (expires_at_ms - now) as f64 / 86_400_000.0;
    // ~1 day, well short of the 30-day standard TTL. Allow a generous window
    // for test timing; the point is it's NOT 30 days.
    assert!(
        (0.9..=1.1).contains(&delta_days),
        "anon session TTL should be ~1 day, got {delta_days:.2} days"
    );
    Ok(())
}

// (anon-sec103-ratelimit) POST /auth/anonymous is per-IP rate-limited when
// `anonymous_rate_limit_per_ip_rpm > 0`: after the limit is exhausted within
// one minute, further calls return 429 RATE_LIMITED. Closes the "unbounded
// anon mint" flood vector.
#[tokio::test]
async fn sec103_anon_mint_is_ip_rate_limited() -> anyhow::Result<()> {
    // Build a state with the per-IP anon rate limit turned on (2 RPM for a
    // fast test). The default test_config sets it to 0 (off); override here.
    let mut config = test_config();
    config.auth_anonymous_enabled = true;
    config.anonymous_rate_limit_per_ip_rpm = 2;
    let pool = sqlx::PgPool::connect(&config.database_url)
        .await
        .expect("connect to test postgres");
    rtdb_server::db::bootstrap(&pool).await.expect("bootstrap");
    let state = AppState::new(pool, config, test_hot());
    let addr = spawn_app(state.clone()).await;

    let client = reqwest::Client::new();
    let url = format!("http://{addr}/auth/anonymous");
    // First 2 calls within the same minute are allowed.
    for i in 0..2 {
        let resp = client.post(&url).send().await?;
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::OK,
            "call {} should be allowed",
            i + 1
        );
    }
    // Third call in the same minute is denied with 429 RATE_LIMITED.
    let resp = client.post(&url).send().await?;
    assert_eq!(resp.status(), reqwest::StatusCode::TOO_MANY_REQUESTS);
    // Retry-After header is present (the limiter surfaces a positive hint).
    assert!(
        resp.headers().get("retry-after").is_some(),
        "Retry-After header should be present on a 429"
    );
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "RATE_LIMITED");
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
    match execute_query(&state.pool, &db, &schema, &collect_notes(), &ctx, false).await? {
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
    match execute_query(
        &state.pool,
        &db,
        &schema,
        &collect_notes(),
        &other_ctx,
        false,
    )
    .await?
    {
        QueryResult::Docs(docs) => assert_eq!(docs.len(), 0, "other user sees nothing"),
        other => panic!("expected Docs, got {other:?}"),
    }
    Ok(())
}
