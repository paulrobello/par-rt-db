//! ENH-011 per-db resource quotas — server enforcement integration tests.
//!
//! Task 4: `maxTablesPerDb` at the schema-acceptance paths (admin push-schema
//! HTTP handler and committer migrate handler). The check lives in those
//! handlers — NOT in `ddl::push_schema` — so the test exercises the real HTTP
//! handler via `spawn_app` + `admin_post` rather than calling `ddl::push_schema`
//! directly (which would also bypass the handler the check lives in, and would
//! additionally break the test harness's `fresh_db` that pushes the kanban
//! fixture regardless of any cap a test has set).
//!
//! The test db is created directly via `db::create_database` (not `fresh_db`),
//! so it is wrapped in a `TestDb` via `wrap_test_db` to schedule RAII cleanup
//! — otherwise the dev DB would accumulate `db_t<uuid>` schemas.

use std::sync::Arc;

use crate::common::{admin_post, fresh_db, spawn_app, test_hot, test_state, wrap_test_db};
use axum::http::StatusCode;
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::config::HotConfig;
use rtdb_server::db;
use rtdb_server::query::Query;
use rtdb_server::subs::next_conn_id;
use rtdb_server::txn::{Step, Transaction};

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

#[tokio::test]
async fn push_schema_over_table_cap_is_rejected() -> anyhow::Result<()> {
    let state = crate::common::test_state().await;
    state.runtime.hot.store(Arc::new(HotConfig {
        max_tables_per_db: 3,
        ..crate::common::test_hot()
    }));
    let addr = spawn_app(state.clone()).await;
    // Created directly (not via `fresh_db`), so wrap for RAII cleanup — same
    // `ensure_cleanup_worker` pattern `fresh_db` uses, so the dev DB doesn't
    // accumulate `db_t<uuid>` schemas from this test.
    let db = wrap_test_db(format!("t{}", uuid::Uuid::now_v7().simple()));
    db::create_database(&state.pool, db.as_str()).await?;

    // 3 tables: at cap -> ok
    let r = admin_post(
        addr,
        "/admin/push-schema",
        serde_json::json!({ "db": db.as_str(), "schema": schema_with_n_tables(3) }),
    )
    .await;
    assert_eq!(r.status(), StatusCode::OK);

    // 4 tables: over cap -> 507 INSUFFICIENT_STORAGE (QUOTA_EXCEEDED)
    let r = admin_post(
        addr,
        "/admin/push-schema",
        serde_json::json!({ "db": db.as_str(), "schema": schema_with_n_tables(4) }),
    )
    .await;
    assert_eq!(r.status(), StatusCode::INSUFFICIENT_STORAGE);
    Ok(())
}

// Task 5: `maxSubsPerDb` enforcement at `committer::handle_subscribe`.
// `collect_work_items` is the only helper this test needs; copied inline from
// `server/tests/subs_test.rs:52` because the Query/Insert helpers are NOT in
// `common`. `fresh_db` pushes the kanban fixture that defines `workItems`.
fn collect_work_items() -> Query {
    serde_json::from_value(serde_json::json!({ "table": "workItems" })).expect("parse query")
}

#[tokio::test]
async fn subscribe_over_cap_is_rejected() -> anyhow::Result<()> {
    let state = test_state().await;
    state.runtime.hot.store(Arc::new(HotConfig {
        max_subs_per_db: 2,
        ..test_hot()
    }));
    let db = fresh_db(&state).await; // pushes the kanban fixture (workItems table)

    for qid in ["q1", "q2"] {
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        state
            .realtime
            .committers
            .subscribe(
                &db,
                next_conn_id(),
                qid.to_string(),
                collect_work_items(),
                tx,
                PrincipalCtx::bypass(),
            )
            .await?;
    }
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
    let err = state
        .realtime
        .committers
        .subscribe(
            &db,
            next_conn_id(),
            "q3".to_string(),
            collect_work_items(),
            tx,
            PrincipalCtx::bypass(),
        )
        .await
        .expect_err("third subscribe over cap must be rejected");
    assert_eq!(err.code, rtdb_server::error::ErrorCode::QuotaExceeded);
    Ok(())
}

// Task 6: `maxStorageBytesPerDb` enforcement at the committer arms. The inline
// `insert_work_item`/`work_item_doc` helpers are copied from
// `server/tests/subs_test.rs:12` because they are not in `common`. `fresh_db`
// pushes the kanban fixture that defines `workItems`.
fn work_item_doc(status: &str, order: f64) -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({
        "projectId": "0".repeat(32),
        "title": "item",
        "status": status,
        "order": order,
        "completedAt": null
    })
    .as_object()
    .expect("json object")
    .clone()
}

fn insert_work_item(status: &str, order: f64) -> Transaction {
    Transaction {
        steps: vec![Step::Insert {
            table: "workItems".to_string(),
            doc: work_item_doc(status, order),
        }],
    }
}

#[tokio::test]
async fn mutation_cache_growth_does_not_consume_storage_quota() -> anyhow::Result<()> {
    let state = crate::common::test_state().await;
    let db = crate::common::fresh_db(&state).await;
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 1.0),
            PrincipalCtx::bypass(),
        )
        .await?;
    let before = rtdb_server::quota::UsageCache::measure(&state.pool, &db).await?;
    let cap = before + 65536;
    state.runtime.hot.store(Arc::new(HotConfig {
        max_storage_bytes_per_db: cap,
        ..crate::common::test_hot()
    }));
    // Large, low-compressibility cached results exceed the available headroom.
    for i in 0..128 {
        let payload = (0..32)
            .map(|_| uuid::Uuid::new_v4().to_string())
            .collect::<String>();
        rtdb_server::mutation_log::store(
            &state.pool,
            &db,
            &format!("cache-{i}"),
            &[serde_json::json!(payload)],
            300000,
        )
        .await?;
    }
    let after = rtdb_server::quota::UsageCache::measure(&state.pool, &db).await?;
    let cache_bytes: i64 = sqlx::query_scalar("SELECT pg_total_relation_size($1::regclass)")
        .bind(format!("\"db_{db}\".mutation_cache_v2"))
        .fetch_one(&state.pool)
        .await?;
    assert!(
        cache_bytes as u64 > cap - before,
        "cache must exceed quota headroom"
    );
    // Other background bookkeeping can allocate a page during the fill.
    assert!(
        after < cap,
        "replay cache must not consume quota headroom: {before} -> {after}, cap {cap}"
    );
    state.limits.quotas.evict(&db);
    state
        .realtime
        .committers
        .mutate(
            &db,
            Some("legacy-write-after-cache".into()),
            insert_work_item("backlog", 2.0),
            PrincipalCtx::bypass(),
        )
        .await?;
    Ok(())
}

#[tokio::test]
async fn mutate_over_storage_cap_is_rejected() -> anyhow::Result<()> {
    let state = crate::common::test_state().await;
    let db = crate::common::fresh_db(&state).await;

    // seed a doc so the table has non-zero on-disk size (deterministic)
    state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 1.0),
            PrincipalCtx::bypass(),
        )
        .await?;

    // now a 1-byte cap: the seeded table's footprint already exceeds it
    state.runtime.hot.store(Arc::new(HotConfig {
        max_storage_bytes_per_db: 1,
        ..crate::common::test_hot()
    }));

    let err = state
        .realtime
        .committers
        .mutate(
            &db,
            None,
            insert_work_item("backlog", 2.0),
            PrincipalCtx::bypass(),
        )
        .await
        .expect_err("write over the 1-byte storage cap must be rejected");
    assert_eq!(err.code, rtdb_server::error::ErrorCode::QuotaExceeded);
    Ok(())
}

// Task 7: `maxStorageBytesPerDb` enforcement at the HTTP upload route. The
// `mint_token` helper and the upload request are copied inline from
// `server/tests/storage_test.rs:134`/`:148` because the upload helpers are NOT
// in `common`. `fresh_db` brings up a bare db; the storage side table is
// ensured lazily inside `upload_handler`.
async fn mint_token(addr: std::net::SocketAddr, db: &str) -> String {
    let resp = crate::common::admin_post(
        addr,
        "/admin/mint-token",
        serde_json::json!({ "db": db, "name": "test-token" }),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    resp.json::<serde_json::Value>().await.expect("json")["token"]
        .as_str()
        .expect("token")
        .to_string()
}

#[tokio::test]
async fn upload_over_storage_cap_is_rejected() -> anyhow::Result<()> {
    let state = crate::common::test_state().await;
    let db = crate::common::fresh_db(&state).await;
    let addr = crate::common::spawn_app(state.clone()).await;
    let token = mint_token(addr, &db).await;

    // 1-byte cap: any upload (5 bytes) exceeds it (0 used + 5 > 1)
    state.runtime.hot.store(Arc::new(HotConfig {
        max_storage_bytes_per_db: 1,
        ..crate::common::test_hot()
    }));

    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/api/storage/{db}"))
        .bearer_auth(token)
        .header("content-type", "application/octet-stream")
        .body(b"hello".to_vec())
        .send()
        .await?;
    assert_eq!(resp.status(), StatusCode::INSUFFICIENT_STORAGE);
    Ok(())
}

// the-cube b8d4af55: enforcement charged engine bookkeeping (the change log's
// `changes`/`change_head` plus `workflows`) while /admin/dbs/{db}/stats
// reported doc-table bytes only — a change-log-heavy db hit HTTP 507 at 2 GiB
// while the dashboard advertised ~1 GiB of headroom. Both surfaces must use
// one basis: doc tables + blob tables, engine bookkeeping excluded (the change
// log is bounded by its own row-count retention, so the byte quota excluding
// it does not unbound disk). The test pins the exact set difference: the
// pre-fix basis minus the new basis is exactly changes + change_head +
// workflows.
#[tokio::test]
async fn quota_basis_excludes_change_log_and_matches_admin_stats() -> anyhow::Result<()> {
    let state = crate::common::test_state().await;
    let db = fresh_db(&state).await;

    // Grow the change log: every commit appends full document copies to
    // `changes`, so padded titles put a measurable retained history in it.
    for i in 0..40 {
        let doc = serde_json::json!({
            "projectId": "0".repeat(32),
            "title": format!("item-{i}{}", "x".repeat(4096)),
            "status": "backlog",
            "order": i as f64,
            "completedAt": null
        });
        state
            .realtime
            .committers
            .mutate(
                &db,
                None,
                Transaction {
                    steps: vec![Step::Insert {
                        table: "workItems".to_string(),
                        doc: doc.as_object().expect("json object").clone(),
                    }],
                },
                PrincipalCtx::bypass(),
            )
            .await?;
    }

    // Ordinary-relation sums via `pg_total_relation_size`, schema `$1`-bound.
    // The dynamic fragments are audited constants (table-name lists); only the
    // schema name is runtime data and it is `$1`-bound.
    let sum_sql = |extra: &str| {
        format!(
            "SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0)::bigint \
                 FROM pg_class c \
                 JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1 AND c.relkind = 'r' {extra}"
        )
    };
    let schema = format!("db_{db}");
    let sql = sum_sql(
        "AND c.relname NOT IN ('meta','mutations','mutation_cache_v2','scheduled_txns','schema_history')",
    );
    let old_basis: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
        .bind(&schema)
        .fetch_one(&state.pool)
        .await?;
    let sql = sum_sql("AND c.relname IN ('changes','change_head','workflows')");
    let log_tables: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
        .bind(&schema)
        .fetch_one(&state.pool)
        .await?;

    let basis = rtdb_server::quota::UsageCache::measure(&state.pool, &db).await?;
    assert!(
        old_basis > i64::try_from(basis)?,
        "change log must have charged the old basis: {old_basis} vs {basis}"
    );
    assert_eq!(
        i64::try_from(basis)? + log_tables,
        old_basis,
        "excluded set must be exactly changes + change_head + workflows"
    );

    // Admin stats must report the same basis enforcement acts on.
    let addr = crate::common::spawn_app(state.clone()).await;
    let resp = crate::common::admin_get(addr, &format!("/admin/dbs/{db}/stats")).await;
    assert_eq!(resp.status(), StatusCode::OK);
    let body: serde_json::Value = resp.json().await?;
    assert_eq!(
        body["storageUsedBytes"].as_u64(),
        Some(basis),
        "admin stats must match the enforcement basis"
    );
    Ok(())
}
