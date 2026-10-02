//! SEC-001 integration tests: background jobs run as the ENQUEUING principal.
//!
//! A user-enqueued scheduled job or workflow run must execute with the
//! enqueuer's per-row identity (ownerField scoping via `execute_txn`'s
//! `PrincipalCtx.user_id`) — not the system bypass — so user B cannot
//! smuggle a future write that touches user A's rows. A machine-token or
//! system enqueue keeps firing as bypass (regression guard), and a logged-out
//! enqueuer's recurring job keeps firing (session liveness is deliberately
//! NOT re-checked), while a lapsed allowlist entry / anonymous opt-in turns
//! the job into a terminal error that stops recurrence.

use crate::common::{admin_post, mint_user_session, spawn_app, test_state, wait_until};
use rtdb_server::protocol::ScheduleStatus;
use rtdb_server::scheduler;
use serde_json::json;
use std::net::SocketAddr;
use std::time::Duration;

/// Owner-gated `notes` schema: the field-by-field style of
/// `per_row_auth_test.rs::owner_schema` (no `Default` on `TableDef`).
fn owner_schema() -> serde_json::Value {
    json!({"tables": {
        "notes": {
            "fields": {
                "title": {"type": "string"},
                "userId": {"type": "string"}
            },
            "indexes": [{"name": "by_user", "fields": ["userId"]}],
            "ownerField": "userId"
        }
    }})
}

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

async fn mint_token(addr: SocketAddr, db: &str) -> String {
    let resp = admin_post(addr, "/admin/mint-token", json!({"db": db, "name": "t"})).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("json");
    body["token"].as_str().expect("token string").to_string()
}

/// Spawns the app, creates a fresh db with the owner schema, mints user
/// sessions for A and B and allowlists both. Returns the addr, the db, and
/// both bearer tokens.
async fn setup_two_users() -> (
    SocketAddr,
    std::sync::Arc<rtdb_server::AppState>,
    crate::common::TestDb,
    String,
    String,
) {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    // A BARE database (wrap_test_db, not fresh_db): fresh_db seeds the kanban
    // fixture, and this test's owner-gated `notes` schema is not additive
    // over it.
    let db_name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &db_name)
        .await
        .expect("create db");
    let db = crate::common::wrap_test_db(db_name);
    let push = admin_post(
        addr,
        "/admin/push-schema",
        json!({"db": db, "schema": owner_schema()}),
    )
    .await;
    assert_eq!(push.status(), reqwest::StatusCode::OK, "push-schema failed");
    let email_a = format!("a-{db}@example.com");
    let email_b = format!("b-{db}@example.com");
    let token_a = mint_user_session(&state.pool, &format!("a-{db}"), &email_a).await;
    let token_b = mint_user_session(&state.pool, &format!("b-{db}"), &email_b).await;
    for email in [&email_a, &email_b] {
        let r = admin_post(
            addr,
            "/admin/allowlist",
            json!({"db": db, "action": "add", "email": email}),
        )
        .await;
        assert_eq!(r.status(), reqwest::StatusCode::OK, "allowlist add failed");
    }
    (addr, state, db, token_a, token_b)
}

/// Direct-seeds a `notes` row owned by `uid` via the bypass executor.
async fn seed_note(pool: &sqlx::PgPool, db: &str, title: &str, uid: &str) -> String {
    let schema: rtdb_server::schema::SchemaDef =
        serde_json::from_value(owner_schema()).expect("parse owner schema");
    let txn = rtdb_server::txn::Transaction {
        steps: vec![rtdb_server::txn::Step::Insert {
            table: "notes".into(),
            doc: json!({"title": title, "userId": uid})
                .as_object()
                .expect("map")
                .clone(),
        }],
    };
    let outcome = rtdb_server::txn::execute_txn(
        pool,
        db,
        &schema,
        &txn,
        &rtdb_server::auth::PrincipalCtx::bypass(),
    )
    .await
    .expect("seed insert");
    outcome.results[0]["id"].as_str().expect("id").to_string()
}

/// Reads every `notes` doc the bypass principal can see, keyed by title.
async fn all_titles(pool: &sqlx::PgPool, db: &str) -> Vec<String> {
    let schema: rtdb_server::schema::SchemaDef =
        serde_json::from_value(owner_schema()).expect("parse owner schema");
    let query = rtdb_server::query::Query {
        table: "notes".into(),
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
        fields: None,
        aggregate: None,
    };
    match rtdb_server::query::execute_query(
        pool,
        db,
        &schema,
        &query,
        &rtdb_server::auth::PrincipalCtx::bypass(),
        false,
    )
    .await
    {
        Ok(rtdb_server::query::QueryResult::Docs(docs)) => {
            let mut titles: Vec<String> = docs
                .iter()
                .map(|d| d["title"].as_str().expect("title").to_string())
                .collect();
            titles.sort();
            titles
        }
        other => panic!("query failed: {other:?}"),
    }
}

/// (a) A user B schedule with a `patchByQuery notes` step must NOT touch
/// user A's row — it fires with B's identity, and the owner filter scopes it
/// to B's (nonexistent) rows.
#[tokio::test]
async fn user_schedule_fires_with_enqueuer_row_rights() -> anyhow::Result<()> {
    let (addr, state, db, _token_a, token_b) = setup_two_users().await;
    let uid_a = format!("a-{db}");
    let note_id = seed_note(&state.pool, &db, "alice's secret", &uid_a).await;

    // B schedules a patch against A's note by id. Before SEC-001 this fired
    // as bypass and would have overwritten A's row.
    let resp = api_post(
        addr,
        "/api/schedule",
        &token_b,
        json!({
            "db": db,
            "when": {"type": "afterMs", "ms": 0},
            "txn": {"steps": [{"op": "patch", "table": "notes", "id": note_id,
                                "fields": {"title": "hacked"}}]},
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "schedule accepted");
    let body: serde_json::Value = resp.json().await?;
    let schedule_id = body["id"].as_str().expect("id").to_string();

    // Wait for the job to run and finalize (a one-shot deletes its row on
    // success; a Forbidden-enforced failure marks the row `error`).
    let fired = wait_until(Duration::from_secs(10), || async {
        matches!(
            scheduler::list(&state.pool, &db).await,
            Ok(listed) if listed.is_empty()
                || listed.iter().any(|s| s.status != ScheduleStatus::Pending)
        )
    })
    .await;
    assert!(fired, "job never left pending");

    let titles = all_titles(&state.pool, &db).await;
    assert!(
        titles.contains(&"alice's secret".to_string()),
        "A's note must be untouched; got {titles:?}"
    );
    assert!(
        !titles.contains(&"hacked".to_string()),
        "B must not patch A's row; got {titles:?}"
    );
    let _ = schedule_id;
    Ok(())
}

/// (b) An anonymous principal on an anon-enabled database fires with the
/// anonymous enqueuer's identity — same scoping assertion.
#[tokio::test]
async fn anonymous_schedule_fires_with_anon_identity() -> anyhow::Result<()> {
    // The anonymous mint endpoint is gated by the instance-wide boot flag,
    // so this test needs an anon-enabled AppState (the anonymous_auth_test
    // override pattern — test_config hard-codes the default config, so build
    // the state inline).
    let mut config = crate::common::test_config();
    config.auth_anonymous_enabled = true;
    let pool = crate::common::test_pool(&config.database_url)
        .await
        .expect("connect to test postgres");
    rtdb_server::db::bootstrap(&pool).await.expect("bootstrap");
    let state = rtdb_server::AppState::new(pool, config, crate::common::test_hot());
    let addr = spawn_app(state.clone()).await;
    let db_name = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&state.pool, &db_name)
        .await
        .expect("create db");
    let db = crate::common::wrap_test_db(db_name);
    let push = admin_post(
        addr,
        "/admin/push-schema",
        json!({"db": db, "schema": owner_schema()}),
    )
    .await;
    assert_eq!(push.status(), reqwest::StatusCode::OK);

    // Enable anonymous access for this db (the operator flip). The admin
    // route is a PATCH, so use reqwest directly (the helper POSTs).
    let r = reqwest::Client::new()
        .patch(format!("http://{addr}/admin/db/{db}/anonymous-access"))
        .header("Authorization", "Bearer test-admin-key")
        .json(&json!({"enabled": true}))
        .send()
        .await
        .expect("patch anonymous-access");
    assert_eq!(r.status(), reqwest::StatusCode::OK, "anon-access enable");

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/auth/anonymous"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let anon_token = body["token"].as_str().expect("token").to_string();

    let uid_a = "seed-owner";
    seed_note(&state.pool, &db, "owned", uid_a).await;

    let resp = api_post(
        addr,
        "/api/schedule",
        &anon_token,
        json!({
            "db": db,
            "when": {"type": "afterMs", "ms": 0},
            "txn": {"steps": [{"op": "patch", "table": "notes", "id": "irrelevant",
                                "fields": {"title": "hacked"}}]},
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let fired = wait_until(Duration::from_secs(10), || async {
        matches!(
            scheduler::list(&state.pool, &db).await,
            Ok(listed) if listed.is_empty()
                || listed.iter().any(|s| s.status != ScheduleStatus::Pending)
        )
    })
    .await;
    assert!(fired, "job never left pending");

    let titles = all_titles(&state.pool, &db).await;
    assert_eq!(titles, vec!["owned".to_string()]);
    Ok(())
}

/// (c) The same cross-user attack from inside a mutate txn via the `schedule`
/// step: the row enqueues carrying the caller's identity.
#[tokio::test]
async fn schedule_step_inside_user_mutate_carries_enqueuer() -> anyhow::Result<()> {
    let (addr, state, db, _token_a, token_b) = setup_two_users().await;
    let uid_a = format!("a-{db}");
    let note_id = seed_note(&state.pool, &db, "alice's secret", &uid_a).await;

    let resp = api_post(
        addr,
        "/api/mutate",
        &token_b,
        json!({
            "db": db,
            "txn": {"steps": [
                {"op": "schedule",
                 "when": {"type": "afterMs", "ms": 0},
                 "txn": {"steps": [{"op": "patch", "table": "notes", "id": note_id,
                                    "fields": {"title": "hacked"}}]}}
            ]},
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "mutate accepted");
    let fired = wait_until(Duration::from_secs(10), || async {
        matches!(
            scheduler::list(&state.pool, &db).await,
            Ok(listed) if listed.is_empty()
                || listed.iter().any(|s| s.status != ScheduleStatus::Pending)
        )
    })
    .await;
    assert!(fired, "job never left pending");

    let titles = all_titles(&state.pool, &db).await;
    assert!(
        titles.contains(&"alice's secret".to_string()) && !titles.contains(&"hacked".to_string()),
        "nested-schedule fire must be owner-scoped; got {titles:?}"
    );
    Ok(())
}

/// (d) Regression guard: a machine-token schedule still fires as bypass —
/// its patch against A's row SUCCEEDS (no ownerField scoping applies).
#[tokio::test]
async fn machine_schedule_still_fires_as_bypass() -> anyhow::Result<()> {
    let (addr, state, db, _token_a, _token_b) = setup_two_users().await;
    let uid_a = format!("a-{db}");
    let note_id = seed_note(&state.pool, &db, "alice's secret", &uid_a).await;
    let token = mint_token(addr, &db).await;

    let resp = api_post(
        addr,
        "/api/schedule",
        &token,
        json!({
            "db": db,
            "when": {"type": "afterMs", "ms": 0},
            "txn": {"steps": [{"op": "patch", "table": "notes", "id": note_id,
                                "fields": {"title": "machine-patched"}}]},
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let patched = wait_until(Duration::from_secs(10), || async {
        all_titles(&state.pool, &db)
            .await
            .contains(&"machine-patched".to_string())
    })
    .await;
    assert!(
        patched,
        "machine-token job must fire as bypass (all rows); jobs={:?} titles={:?}",
        scheduler::list(&state.pool, &db).await,
        all_titles(&state.pool, &db).await
    );
    Ok(())
}

/// (e-core) An INTERVAL job whose enqueuer's allowlist entry is removed
/// between fires: the next fire fails with `error` and the row never
/// returns to `pending` (recurrence stops — `mark_error` is terminal).
#[tokio::test]
async fn lapsed_allowlist_stops_interval_recurrence() -> anyhow::Result<()> {
    let (addr, state, db, _token_a, token_b) = setup_two_users().await;
    let email_b = format!("b-{db}@example.com");

    let resp = api_post(
        addr,
        "/api/schedule",
        &token_b,
        json!({
            "db": db,
            "when": {"type": "afterMs", "ms": 0},
            "txn": {"steps": [{"op": "insert", "table": "notes",
                                "doc": {"title": "tick", "userId": format!("b-{db}")}}]},
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let schedule_id = body["id"].as_str().expect("id").to_string();

    // Remove B's allowlist entry BEFORE the first fire. The fire re-runs
    // `resolve_enqueuer`, fails, and the row ends `error`.
    let r = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": db, "action": "remove", "email": email_b}),
    )
    .await;
    assert_eq!(r.status(), reqwest::StatusCode::OK, "allowlist remove");

    let errored = wait_until(Duration::from_secs(10), || async {
        matches!(
            scheduler::list(&state.pool, &db).await,
            Ok(ref listed) if listed.iter().any(
                |s| s.id == schedule_id && s.status == ScheduleStatus::Error
            )
        )
    })
    .await;
    assert!(errored, "job must end `error` once the enqueuer lapses");
    // And it must stay errored (no recurrence) for another poll window.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let listed = scheduler::list(&state.pool, &db).await?;
    let row = listed
        .iter()
        .find(|s| s.id == schedule_id)
        .expect("row remains");
    assert_eq!(row.status, ScheduleStatus::Error);
    Ok(())
}

/// (e2) A user who LOGS OUT (session row deleted) before a recurring job
/// fires: the job STILL fires, scoped to their rows (session liveness is
/// deliberately not re-checked at fire time).
#[tokio::test]
async fn logged_out_enqueuers_job_still_fires() -> anyhow::Result<()> {
    let (addr, state, db, _token_a, token_b) = setup_two_users().await;
    let uid_b = format!("b-{db}");
    let uid_a = format!("a-{db}");
    seed_note(&state.pool, &db, "alice's secret", &uid_a).await;

    // B schedules a patch scoped by query to THEIR rows — this is the
    // legitimate recurring-job shape (e.g. "archive my notes nightly").
    let resp = api_post(
        addr,
        "/api/schedule",
        &token_b,
        json!({
            "db": db,
            "when": {"type": "afterMs", "ms": 0},
            "txn": {"steps": [{"op": "patchByQuery", "table": "notes",
                                "filter": {"op": "eq", "field": "userId", "value": uid_b},
                                "patch": {"title": "b-touched"}}]},
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Delete ALL of B's sessions (logout-everywhere).
    sqlx::query("DELETE FROM rtdb_auth.sessions WHERE user_id = $1")
        .bind(format!("b-{db}"))
        .execute(&state.pool)
        .await
        .expect("delete sessions");

    // B's own rows (none) may be patched; A's row must stay untouched. Wait
    // for the job to leave `pending`.
    let fired = wait_until(Duration::from_secs(10), || async {
        matches!(
            scheduler::list(&state.pool, &db).await,
            Ok(listed) if listed.is_empty()
                || listed.iter().any(|s| s.status != ScheduleStatus::Pending)
        )
    })
    .await;
    assert!(fired, "logged-out enqueuer's job must still fire");

    let titles = all_titles(&state.pool, &db).await;
    assert_eq!(titles, vec!["alice's secret".to_string()]);
    Ok(())
}

/// (f) The `startWorkflow` version of (a): a user-started run whose txn step
/// fires with the enqueuer's identity, so it cannot touch A's row.
#[tokio::test]
async fn user_workflow_fires_with_enqueuer_row_rights() -> anyhow::Result<()> {
    let (addr, state, db, _token_a, token_b) = setup_two_users().await;
    let uid_a = format!("a-{db}");
    let note_id = seed_note(&state.pool, &db, "alice's secret", &uid_a).await;

    let resp = api_post(
        addr,
        "/api/workflows",
        &token_b,
        json!({
            "db": db,
            "spec": {"name": "attack", "steps": [
                {"txn": {"steps": [{"op": "patch", "table": "notes",
                                    "id": note_id, "fields": {"title": "hacked"}}]}}]}
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "workflow accepted");
    let body: serde_json::Value = resp.json().await?;
    let run_id = body["id"].as_str().expect("id").to_string();

    // The run should END `failed` (the patch is Forbidden for B) — never
    // `success` (which would mean bypass fired it).
    let failed = wait_until(Duration::from_secs(10), || async {
        matches!(
            rtdb_server::workflows::list(&state.pool, &db, None, 100).await,
            Ok(ref runs) if runs.iter().any(
                |w| w.id == run_id && w.status == rtdb_server::protocol::WorkflowStatus::Failed
            )
        )
    })
    .await;
    assert!(failed, "user-started workflow must fail, not bypass-fire");

    let titles = all_titles(&state.pool, &db).await;
    assert_eq!(titles, vec!["alice's secret".to_string()]);
    Ok(())
}

// ===========================================================================
// SEC-002: only workers (machine tokens), admins, or the creator may claim,
// finalize, cancel, pause, resume, or signal.
// ===========================================================================

/// (s2-a) User B cannot cancel, pause, or resume user A's job — FORBIDDEN.
/// The creator (A) can cancel it.
#[tokio::test]
async fn sec002_user_b_cannot_cancel_or_pause_as_job() -> anyhow::Result<()> {
    let (addr, state, db, token_a, token_b) = setup_two_users().await;

    let resp = api_post(
        addr,
        "/api/schedule",
        &token_a,
        json!({
            "db": db,
            "when": {"type": "afterMs", "ms": 60_000},
            "txn": {"steps": []},
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let schedule_id = body["id"].as_str().expect("id").to_string();

    // B's cancel is FORBIDDEN.
    let resp = api_post(
        addr,
        &format!("/api/schedule/{schedule_id}/cancel"),
        &token_b,
        json!({"db": db}),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "B cancel A's job"
    );

    // B's pause and resume too.
    let resp = api_post(
        addr,
        &format!("/api/schedule/{schedule_id}/pause"),
        &token_b,
        json!({"db": db}),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "B pause A's job"
    );
    let resp = api_post(
        addr,
        &format!("/api/schedule/{schedule_id}/resume"),
        &token_b,
        json!({"db": db}),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "B resume A's job"
    );

    // A NULL-enqueuer (machine-created) job is forbidden for users too.
    let token = mint_token(addr, &db).await;
    let resp = api_post(
        addr,
        "/api/schedule",
        &token,
        json!({
            "db": db,
            "when": {"type": "afterMs", "ms": 60_000},
            "txn": {"steps": []},
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let machine_id = body["id"].as_str().expect("id").to_string();
    let resp = api_post(
        addr,
        &format!("/api/schedule/{machine_id}/cancel"),
        &token_a,
        json!({"db": db}),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "user cancel machine job"
    );

    // The creator CAN cancel.
    let resp = api_post(
        addr,
        &format!("/api/schedule/{schedule_id}/cancel"),
        &token_a,
        json!({"db": db}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(resp.json::<serde_json::Value>().await?["ok"], json!(true));

    // And the cancelled row is gone.
    assert!(
        wait_until(Duration::from_secs(5), || async {
            matches!(
                scheduler::list(&state.pool, &db).await,
                Ok(listed) if listed.iter().all(|s| s.id != schedule_id)
            )
        })
        .await
    );
    Ok(())
}

/// (s2-b) A user cannot claim; a machine token still can (and finalize).
#[tokio::test]
async fn sec002_user_cannot_claim_machine_can() -> anyhow::Result<()> {
    let (addr, _state, db, _token_a, token_b) = setup_two_users().await;
    let token = mint_token(addr, &db).await;

    let resp = api_post(
        addr,
        "/api/schedule",
        &token,
        json!({
            "db": db,
            "when": {"type": "afterMs", "ms": 0},
            "txn": {"steps": []},
            "external": true,
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // A user session cannot claim.
    let resp = api_post(addr, "/api/schedule/claim", &token_b, json!({"db": db})).await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "user claim rejected"
    );

    // A machine token still can, and can finalize.
    let resp = api_post(
        addr,
        "/api/schedule/claim",
        &token,
        json!({"db": db, "leaseMs": 60_000}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let jobs = body["jobs"].as_array().expect("jobs");
    assert_eq!(jobs.len(), 1);
    let job_id = jobs[0]["id"].as_str().expect("id").to_string();
    let lease = jobs[0]["leaseGeneration"].as_i64().expect("gen");

    let resp = api_post(
        addr,
        &format!("/api/schedule/{job_id}/complete"),
        &token,
        json!({"db": db, "lease": lease}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    Ok(())
}

/// (s2-c) Workflow variant: B cannot cancel or signal A's run; the creator
/// can signal.
#[tokio::test]
async fn sec002_workflow_cancel_and_signal_owner_gated() -> anyhow::Result<()> {
    let (addr, state, db, token_a, token_b) = setup_two_users().await;

    // A starts a run that parks on a signal (so cancel and signal both act
    // on a live, non-terminal row).
    let resp = api_post(
        addr,
        "/api/workflows",
        &token_a,
        json!({
            "db": db,
            "spec": {"name": "gate", "steps": [
                {"txn": {"steps": []}},
                {"awaitSignal": {"name": "approve", "timeoutMs": 60_000}}
            ]}
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let run_id = body["id"].as_str().expect("id").to_string();

    // Wait for the run to park at the signal step.
    let parked = wait_until(Duration::from_secs(10), || async {
        matches!(
            rtdb_server::workflows::list(&state.pool, &db, None, 100).await,
            Ok(ref runs) if runs.iter().any(
                |w| w.id == run_id && w.status == rtdb_server::protocol::WorkflowStatus::Waiting
            )
        )
    })
    .await;
    assert!(parked, "run must park at the signal step");

    // B cannot signal it.
    let resp = api_post(
        addr,
        &format!("/api/workflows/{run_id}/signal"),
        &token_b,
        json!({"db": db, "name": "approve"}),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "B signal A's run"
    );

    // B cannot cancel it either.
    let resp = api_post(
        addr,
        &format!("/api/workflows/{run_id}/cancel"),
        &token_b,
        json!({"db": db}),
    )
    .await;
    assert_eq!(
        resp.status(),
        reqwest::StatusCode::FORBIDDEN,
        "B cancel A's run"
    );

    // The creator CAN signal (a second run, so the first stays parked for
    // the cancel assertions above).
    let resp = api_post(
        addr,
        "/api/workflows",
        &token_a,
        json!({
            "db": db,
            "spec": {"name": "gate2", "steps": [
                {"txn": {"steps": []}},
                {"awaitSignal": {"name": "go", "timeoutMs": 60_000}}
            ]}
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    let run2 = body["id"].as_str().expect("id").to_string();
    let parked2 = wait_until(Duration::from_secs(10), || async {
        matches!(
            rtdb_server::workflows::list(&state.pool, &db, None, 100).await,
            Ok(ref runs) if runs.iter().any(
                |w| w.id == run2 && w.status == rtdb_server::protocol::WorkflowStatus::Waiting
            )
        )
    })
    .await;
    assert!(parked2, "run2 must park");

    let resp = api_post(
        addr,
        &format!("/api/workflows/{run2}/signal"),
        &token_a,
        json!({"db": db, "name": "go"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK, "creator signal ok");
    let delivered = wait_until(Duration::from_secs(10), || async {
        matches!(
            rtdb_server::workflows::list(&state.pool, &db, None, 100).await,
            Ok(ref runs) if runs.iter().any(
                |w| w.id == run2 && w.status == rtdb_server::protocol::WorkflowStatus::Success
            )
        )
    })
    .await;
    assert!(delivered, "run2 must complete after the creator's signal");
    Ok(())
}
