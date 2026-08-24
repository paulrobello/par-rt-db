//! Hot-config live reload (`PATCH /admin/config`) — server integration tests.
//!
//! `admin_test.rs` covers `GET /admin/config` (redaction + the SEC-108 gate);
//! until now no integration test PATCHed the endpoint. These close that gap:
//! multi-field happy path round-tripped through GET, quota-cap patching, the
//! live-reload proof (a patched `maxTablesPerDb` is consumed by the push-schema
//! enforcement path, not just echoed back), the 400 validation surface, and the
//! admin gate.
//!
//! Each test spawns its own app: hot config lives per-`AppState` behind an
//! `ArcSwap`, so a PATCH in one test must not leak into another's. The PATCH
//! also persists the merged row to `rtdb_config`, but no test AppState ever
//! loads it (`load_hot` runs only in `main.rs`), so the shared single-row table
//! cannot carry state between tests.

use std::net::SocketAddr;

use crate::common::{admin_get, admin_post, spawn_app, test_state, wrap_test_db};
use rtdb_server::db;

/// A PATCH helper, mirroring `crate::common::admin_post` (POST-only): bearer-authed
/// with the test admin key `crate::common::test_config` seeds.
async fn admin_patch(addr: SocketAddr, path: &str, body: serde_json::Value) -> reqwest::Response {
    reqwest::Client::new()
        .patch(format!("http://{addr}{path}"))
        .header("Authorization", "Bearer test-admin-key")
        .json(&body)
        .send()
        .await
        .expect("send admin request")
}

/// GETs `/admin/config` and returns its `hot` object (the camelCase wire form
/// of `HotConfig` — hot values are shown in full, unlike the masked boot
/// fields).
async fn hot_via_get(addr: SocketAddr) -> serde_json::Value {
    let resp = admin_get(addr, "/admin/config").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("config json");
    assert!(
        body["hot"].is_object(),
        "config response carries a hot object: {body}"
    );
    body["hot"].clone()
}

/// Copied inline from `server/tests/quota_test.rs:28` because it is not in
/// `common`. Table count is the only knob the cap check reads, so bare
/// single-field tables suffice.
fn schema_with_n_tables(n: usize) -> serde_json::Value {
    let mut tables = serde_json::Map::new();
    for i in 0..n {
        tables.insert(
            format!("t{i}"),
            serde_json::json!({ "fields": { "name": { "type": "string" } } }),
        );
    }
    serde_json::json!({ "tables": tables })
}

// A multi-field PATCH applies every provided field (each fully replaces the
// prior value), and both the PATCH response and a follow-up GET report them in
// `hot`. Origin entries are trimmed and empties dropped by the handler before
// validation, so the round-tripped list is the cleaned one.
#[tokio::test]
async fn patch_applies_multiple_fields_and_get_round_trips() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;

    let resp = admin_patch(
        addr,
        "/admin/config",
        serde_json::json!({
            "allowedOrigins": ["https://one.example", "  https://two.example:8443  ", ""],
            "sessionTtlDays": 7,
            "idempotencyTtlMs": 60_000,
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    // The PATCH response is the same redacted config GET returns.
    let patched: serde_json::Value = resp.json().await?;
    assert_eq!(patched["hot"]["sessionTtlDays"], serde_json::json!(7));

    let hot: serde_json::Value = hot_via_get(addr).await;
    assert_eq!(
        hot["allowedOrigins"],
        serde_json::json!(["https://one.example", "https://two.example:8443"])
    );
    assert_eq!(hot["sessionTtlDays"], serde_json::json!(7));
    assert_eq!(hot["idempotencyTtlMs"], serde_json::json!(60_000));
    Ok(())
}

// Quota caps (ENH-011) are patchable hot fields like the original four: a low
// non-zero `maxTablesPerDb` round-trips through the redacted config (0 would
// mean unlimited, the seed default, so it proves nothing).
#[tokio::test]
async fn patch_max_tables_per_db_round_trips() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;

    let resp = admin_patch(
        addr,
        "/admin/config",
        serde_json::json!({"maxTablesPerDb": 2}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let hot: serde_json::Value = hot_via_get(addr).await;
    assert_eq!(hot["maxTablesPerDb"], serde_json::json!(2));
    Ok(())
}

// THE live-reload proof: PATCH swaps the `ArcSwap<HotConfig>` that the
// enforcement path re-loads per request — the admin push-schema handler reads
// `state.runtime.hot.load().max_tables_per_db` before every push
// (`admin/dbs.rs`), and the cap rejects a schema whose table count exceeds it
// (`SchemaDef::check_table_quota`). With the cap PATCHed to 2 a 3-table push
// fails 507 QUOTA_EXCEEDED; after PATCHing the cap to 5 the SAME push succeeds.
// That round trip is only possible if the swap is actually consumed by
// enforcement, not just echoed back by GET.
#[tokio::test]
async fn patched_table_cap_is_enforced_live_and_raising_it_unblocks() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    // Created directly (not via `fresh_db`, which pushes the 2-table kanban
    // fixture), so wrap for RAII cleanup — mirrors `quota_test.rs`.
    let db = wrap_test_db(format!("t{}", uuid::Uuid::now_v7().simple()));
    db::create_database(&state.pool, db.as_str()).await?;

    let resp = admin_patch(
        addr,
        "/admin/config",
        serde_json::json!({"maxTablesPerDb": 2}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // 2 tables: at cap -> ok.
    let r = admin_post(
        addr,
        "/admin/push-schema",
        serde_json::json!({ "db": db.as_str(), "schema": schema_with_n_tables(2) }),
    )
    .await;
    assert_eq!(r.status(), reqwest::StatusCode::OK);

    // 3 tables: over cap -> 507 INSUFFICIENT_STORAGE (QUOTA_EXCEEDED).
    let r = admin_post(
        addr,
        "/admin/push-schema",
        serde_json::json!({ "db": db.as_str(), "schema": schema_with_n_tables(3) }),
    )
    .await;
    assert_eq!(r.status(), reqwest::StatusCode::INSUFFICIENT_STORAGE);
    let body: serde_json::Value = r.json().await?;
    assert_eq!(body["code"], "QUOTA_EXCEEDED");

    // Raise the cap live; the SAME over-cap push now succeeds.
    let resp = admin_patch(
        addr,
        "/admin/config",
        serde_json::json!({"maxTablesPerDb": 5}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let r = admin_post(
        addr,
        "/admin/push-schema",
        serde_json::json!({ "db": db.as_str(), "schema": schema_with_n_tables(3) }),
    )
    .await;
    assert_eq!(r.status(), reqwest::StatusCode::OK);
    Ok(())
}

// Every invalid PATCH is 400 BAD_REQUEST and leaves the hot config untouched:
// validation runs on the merged copy and the ArcSwap is only stored after the
// row persists, so a rejected PATCH cannot partially apply the other fields it
// carried either. `maxFileSize` over the ceiling uses the literal value of
// `HARD_MAX_FILE_SIZE` in `src/config.rs` (2 GiB, `2 * 1024 * 1024 * 1024`) —
// the constant is `pub(crate)`, invisible to an integration test. An origin
// without a scheme (`not-an-origin`) fails `HotConfig::origins_valid`, which
// accepts only `http(s)://host[:port]`. An unknown field is a
// `deny_unknown_fields` deserialization failure, mapped to 400 by the `ApiJson`
// extractor.
#[tokio::test]
async fn invalid_patches_are_400_and_change_nothing() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;

    let hot_before: serde_json::Value = hot_via_get(addr).await;

    let cases: Vec<serde_json::Value> = vec![
        serde_json::json!({"sessionTtlDays": 0}),
        serde_json::json!({"maxFileSize": 0}),
        serde_json::json!({"maxFileSize": 2 * 1024 * 1024 * 1024u64 + 1}),
        serde_json::json!({"allowedOrigins": ["https://ok.example", "not-an-origin"]}),
        serde_json::json!({"bogusField": 1}),
    ];
    for body in &cases {
        let resp = admin_patch(addr, "/admin/config", body.clone()).await;
        assert_eq!(
            resp.status(),
            reqwest::StatusCode::BAD_REQUEST,
            "case must be rejected: {body}"
        );
        let error: serde_json::Value = resp.json().await?;
        assert_eq!(
            error["code"], "BAD_REQUEST",
            "case must carry the BAD_REQUEST envelope: {body}"
        );
    }

    let hot_after: serde_json::Value = hot_via_get(addr).await;
    assert_eq!(
        hot_after, hot_before,
        "rejected PATCHes must not change any hot value"
    );
    Ok(())
}

// PATCH /admin/config is gated by the same path-based admin middleware as every
// /admin route (SEC-108): a missing bearer and a wrong bearer are both 401
// before the handler runs.
#[tokio::test]
async fn patch_config_requires_admin() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state).await;

    // Missing bearer -> 401.
    let resp = reqwest::Client::new()
        .patch(format!("http://{addr}/admin/config"))
        .json(&serde_json::json!({"sessionTtlDays": 7}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);

    // Wrong bearer -> 401 + the RtDbError envelope.
    let resp = reqwest::Client::new()
        .patch(format!("http://{addr}/admin/config"))
        .header("Authorization", "Bearer wrong-key")
        .json(&serde_json::json!({"sessionTtlDays": 7}))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(body["code"], "UNAUTHORIZED");
    Ok(())
}
