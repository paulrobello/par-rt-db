mod common;
use common::{admin_delete, admin_get, mint_user_session, spawn_app, test_state};
use rtdb_server::auth::session::{
    create_admin_session, delete_session_by_hash, delete_sessions_for_user, list_sessions,
};
use rtdb_server::db::sha256_hex;
use serde_json::Value;

#[tokio::test]
async fn list_and_delete_sessions_works() -> anyhow::Result<()> {
    let state = test_state().await;
    let suffix = uuid::Uuid::now_v7().simple();
    let id_a = format!("u-list-a-{suffix}");
    let email_a = format!("a-{suffix}@example.com");
    let id_b = format!("u-list-b-{suffix}");
    let email_b = format!("b-{suffix}@example.com");

    mint_user_session(&state.pool, &id_a, &email_a).await;
    // second session, same user — inserted a tick later so created_at can differ
    mint_user_session(&state.pool, &id_a, &email_a).await;
    mint_user_session(&state.pool, &id_b, &email_b).await;

    // filter to user A — exactly the two we just seeded
    let for_a = list_sessions(&state.pool, Some(&id_a), 1000).await?;
    assert_eq!(for_a.len(), 2, "two sessions for user A");
    assert!(for_a.iter().all(|s| s.user_id == id_a));
    assert_eq!(for_a[0].email.as_deref(), Some(email_a.as_str()));

    // filter by email also works
    let by_email = list_sessions(&state.pool, Some(&email_b), 1000).await?;
    assert_eq!(by_email.len(), 1);

    // newest-first ordering
    assert!(for_a[0].created_at >= for_a[1].created_at);

    // revoke one of A's sessions by hash (fetched from the list, not recomputed)
    let real_hash = for_a[0].token_hash.clone();
    let n = delete_session_by_hash(&state.pool, &real_hash).await?;
    assert_eq!(n, 1);
    assert_eq!(
        list_sessions(&state.pool, Some(&id_a), 1000).await?.len(),
        1
    );

    // revoke all remaining for A
    let n = delete_sessions_for_user(&state.pool, &id_a).await?;
    assert_eq!(n, 1);
    assert_eq!(
        list_sessions(&state.pool, Some(&id_a), 1000).await?.len(),
        0
    );

    // idempotent: deleting a gone hash is 0, not an error
    assert_eq!(delete_session_by_hash(&state.pool, &real_hash).await?, 0);
    Ok(())
}

#[tokio::test]
async fn admin_can_list_revoke_one_and_revoke_all_sessions() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let suffix = uuid::Uuid::now_v7().simple();
    let id = format!("u-http-{suffix}");
    let email = format!("http-{suffix}@example.com");
    mint_user_session(&state.pool, &id, &email).await;
    mint_user_session(&state.pool, &id, &email).await;

    // GET list (server-wide, filtered by user)
    let resp = admin_get(addr, &format!("/admin/sessions?user={id}")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    let sessions = body["sessions"].as_array().expect("sessions array");
    assert_eq!(sessions.len(), 2);
    assert!(sessions[0]["tokenHash"].is_string());

    // DELETE one by hash
    let hash = sessions[0]["tokenHash"].as_str().unwrap();
    let resp = admin_delete(addr, &format!("/admin/sessions/{hash}")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(list_sessions(&state.pool, Some(&id), 1000).await?.len(), 1);

    // DELETE all for user (bare path + ?user=)
    let resp = admin_delete(addr, &format!("/admin/sessions?user={id}")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    assert_eq!(body["revoked"], 1);
    assert_eq!(list_sessions(&state.pool, Some(&id), 1000).await?.len(), 0);
    Ok(())
}

#[tokio::test]
async fn sessions_endpoints_require_admin() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    // no admin bearer
    let resp = reqwest::get(format!("http://{addr}/admin/sessions")).await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    let resp = reqwest::Client::new()
        .delete(format!("http://{addr}/admin/sessions?user=anyone"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn revoke_all_without_user_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let resp = admin_delete(addr, "/admin/sessions").await; // no ?user=
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    // both scopes at once is equally refused
    let resp = admin_delete(addr, "/admin/sessions?user=abc&expired=true").await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    Ok(())
}

#[tokio::test]
async fn admin_can_remove_all_expired_sessions() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let suffix = uuid::Uuid::now_v7().simple();
    let id = format!("u-exp-{suffix}");
    let email = format!("exp-{suffix}@example.com");

    // Two live user sessions; one to be expired in place.
    let tok_live = mint_user_session(&state.pool, &id, &email).await;
    let tok_dead = mint_user_session(&state.pool, &id, &email).await;
    let dead_hash = sha256_hex(&tok_dead);
    sqlx::query("UPDATE rtdb_auth.sessions SET expires_at = 1 WHERE token_hash = $1")
        .bind(&dead_hash)
        .execute(&state.pool)
        .await?;

    // An expired admin-key login row too — the list unions both tables, so
    // the cleanup must sweep both.
    let admin_tok = create_admin_session(&state.pool, 30).await?;
    sqlx::query("UPDATE rtdb_auth.admin_sessions SET expires_at = 1 WHERE token_hash = $1")
        .bind(sha256_hex(&admin_tok))
        .execute(&state.pool)
        .await?;

    let resp = admin_delete(addr, "/admin/sessions?expired=true").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    assert_eq!(body["ok"], serde_json::json!(true));
    // The sweep is instance-wide and tests share one Postgres, so other
    // binaries' expired rows may ride along — assert the floor, then the
    // per-row outcomes below (which are isolated to this test's ids).
    assert!(
        body["revoked"].as_u64().unwrap_or(0) >= 2,
        "at least this test's expired user row + expired admin row"
    );

    // The live session survives; the expired ones are gone.
    let remaining = list_sessions(&state.pool, Some(&id), 1000).await?;
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].token_hash, sha256_hex(&tok_live));
    let admin_row: Option<(String,)> =
        sqlx::query_as("SELECT token_hash FROM rtdb_auth.admin_sessions WHERE token_hash = $1")
            .bind(sha256_hex(&admin_tok))
            .fetch_optional(&state.pool)
            .await?;
    assert!(admin_row.is_none(), "expired admin-key row was swept");

    // Repeat sweep still succeeds (idempotent — count not asserted: another
    // test binary may expire rows into the shared DB at any moment).
    let resp = admin_delete(addr, "/admin/sessions?expired=true").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    Ok(())
}
