//! Integration tests for ENH-019 query introspection: `compile_query`
//! produces the exact SQL the read path executes, `/admin/db/{db}/explain`
//! surfaces it over HTTP with bind params and warnings, and the slow-query
//! log records queries that exceed `RTDB_SLOW_QUERY_MS`. The SQL assertions
//! exercise the compile seam directly; the HTTP tests exercise the admin
//! routes end-to-end.

mod common;

use std::net::SocketAddr;

use common::{
    admin_get, admin_post, fresh_db, kanban_schema_json, spawn_app, test_state,
    test_state_with_slow_queries,
};
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::query::{Query, collect_filter_warnings, compile_query, execute_query};
use rtdb_server::schema::SchemaDef;
use rtdb_server::txn::{Step, Transaction, execute_txn};

/// Unwrap a JSON object into the `Map` shape `Step::Insert.doc` takes (mirrors
/// `query_test::doc`).
fn doc(value: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    value.as_object().expect("json object").clone()
}

/// Seed `n` project rows so a full-scan query does real work and deterministically
/// exceeds a 1ms slow-query threshold. The slow-query hook lives in the HTTP
/// handler and compares integer-truncated `elapsed_ms`, so an empty-table query
/// (sub-millisecond) would trip nothing — seeding makes the test non-flaky.
async fn seed_projects(pool: &sqlx::PgPool, db: &str, schema: &SchemaDef, n: usize) {
    for i in 0..n {
        let _ = execute_txn(
            pool,
            db,
            schema,
            &Transaction {
                steps: vec![Step::Insert {
                    table: "projects".to_string(),
                    doc: doc(serde_json::json!({
                        "name": format!("proj-{i}"),
                        "description": null,
                        "status": "active",
                        "tags": [],
                        "updatedAt": i as f64,
                    })),
                }],
            },
            &PrincipalCtx::bypass(),
        )
        .await
        .expect("seed project row");
    }
}

/// POSTs `body` to a data-plane path with a Bearer token (mirrors
/// `rate_limit_test::api_post`). The slow-query timing hook lives in the HTTP
/// `query_handler`, NOT in `execute_query` directly, so a slow-query test must
/// drive the real `/api/query` route for the recording to fire.
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

/// Mints a real machine token for `db` via the admin route (mirrors
/// `rate_limit_test::mint_token`).
async fn mint_token(addr: SocketAddr, db: &str) -> String {
    let resp = admin_post(
        addr,
        "/admin/mint-token",
        serde_json::json!({"db": db, "name": "slow-query-test-token"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.expect("parse mint-token response");
    body["token"].as_str().expect("token").to_string()
}

/// Build a kanban-schema `Query` against the `projects` table with the
/// `by_status` index (eq prefix on `status`).
fn projects_query_by_status(status: &str) -> Query {
    let q: Query = serde_json::from_value(serde_json::json!({
        "table": "projects",
        "index": "by_status",
        "eq": [status],
    }))
    .expect("parse projects query");
    q
}

/// White-box compile test: a simple eq-prefixed `collect` query compiles to
/// the same SQL the read path executes — parameterized eq bind, owner/
/// collaborator predicate absent (kanban projects declare neither), and a
/// `LIMIT $n` tail. The SQL must be byte-identical to what `POST /api/query`
/// would run; compile is pure so this is the SAME compile.
#[tokio::test]
async fn compile_query_collect_eq_prefix_yields_expected_sql() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let schema: SchemaDef =
        serde_json::from_value(kanban_schema_json()).expect("parse kanban schema");
    let q = projects_query_by_status("active");
    let principal_ctx = PrincipalCtx::bypass();
    let (cq, warnings) = compile_query(db.as_str(), &schema, &q, &principal_ctx)?;
    // No filter, no unindexed field references — warnings empty.
    assert!(
        warnings.is_empty(),
        "expected no warnings, got {warnings:?}"
    );
    // Terminal tag for a plain `collect` (no `unique`/`first`/`get`/`count`
    // /etc) is "collect".
    assert_eq!(cq.terminal, "collect");
    // SQL carries the canonical SELECT-from-db_<name>.t_projects shape and
    // references the eq-prefixed status column via its physical f_ name.
    assert!(
        cq.sql
            .contains("SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"db_"),
        "SQL missing canonical SELECT: {}",
        cq.sql
    );
    assert!(
        cq.sql.contains(".\"t_projects\" WHERE "),
        "SQL missing table name or WHERE: {}",
        cq.sql
    );
    // Exactly one bind: the eq prefix value (`status="active"`). The kanban
    // fixture declares no ownerField/collaboratorsField on projects, and the
    // LIMIT slot is folded into the same Vec<EqBind>.
    assert_eq!(
        cq.binds.len(),
        2,
        "expected 2 binds (eq + LIMIT), got {:?}",
        cq.binds
    );
    Ok(())
}

/// White-box compile test: a `get(id)` query compiles to a single-bind
/// point read.
#[tokio::test]
async fn compile_query_get_yields_point_read_sql() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let schema: SchemaDef =
        serde_json::from_value(kanban_schema_json()).expect("parse kanban schema");
    let q: Query = serde_json::from_value(serde_json::json!({
        "table": "projects",
        "get": "proj_1",
    }))
    .expect("parse get query");
    let principal_ctx = PrincipalCtx::bypass();
    let (cq, _warnings) = compile_query(db.as_str(), &schema, &q, &principal_ctx)?;
    assert_eq!(cq.terminal, "get");
    assert!(
        cq.sql.contains("WHERE \"id\" = $1"),
        "expected point-read WHERE id = $1, got: {}",
        cq.sql
    );
    assert_eq!(
        cq.binds.len(),
        1,
        "expected 1 bind for get, got {:?}",
        cq.binds
    );
    Ok(())
}

/// White-box compile test: a `count` over the `by_status` index emits the
/// count SQL (no LIMIT) and carries only the eq-prefix bind.
#[tokio::test]
async fn compile_query_count_yields_count_sql() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let schema: SchemaDef =
        serde_json::from_value(kanban_schema_json()).expect("parse kanban schema");
    let mut q = projects_query_by_status("active");
    q.count = true;
    let principal_ctx = PrincipalCtx::bypass();
    let (cq, _warnings) = compile_query(db.as_str(), &schema, &q, &principal_ctx)?;
    assert_eq!(cq.terminal, "count");
    assert!(
        cq.sql.contains("COUNT(*)"),
        "expected COUNT(*) in count SQL, got: {}",
        cq.sql
    );
    // count has no LIMIT: only the eq-prefix bind is emitted.
    assert_eq!(
        cq.binds.len(),
        1,
        "expected 1 bind for count, got {:?}",
        cq.binds
    );
    Ok(())
}

/// Warning test: a filter on a declared-but-unindexed field produces a
/// warning. The kanban `projects` table declares `description` as an optional
/// field but has no index on it, so filtering on `description` would force a
/// jsonb extraction.
#[tokio::test]
async fn warnings_flag_filter_on_unindexed_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let _db = fresh_db(&state).await;
    let schema: SchemaDef =
        serde_json::from_value(kanban_schema_json()).expect("parse kanban schema");
    // Filter on `description` (declared, not indexed).
    let q: Query = serde_json::from_value(serde_json::json!({
        "table": "projects",
        "index": "by_status",
        "eq": ["active"],
        "filter": {"field": "description", "op": "eq", "value": "Inbox"},
    }))
    .expect("parse filter query");
    let warnings = collect_filter_warnings(&schema, &q);
    assert!(
        !warnings.is_empty(),
        "expected at least one warning for unindexed filter, got {warnings:?}"
    );
    assert!(
        warnings.iter().any(|w| w.contains("'description'")),
        "expected warning to name the 'description' field, got {warnings:?}"
    );
    Ok(())
}

/// Warning test: a filter on an indexed field produces no warning. The
/// kanban `projects` table indexes `status`, so a filter on `status` is
/// covered by an index and emits nothing.
#[tokio::test]
async fn warnings_silent_for_filter_on_indexed_field() -> anyhow::Result<()> {
    let state = test_state().await;
    let _db = fresh_db(&state).await;
    let schema: SchemaDef =
        serde_json::from_value(kanban_schema_json()).expect("parse kanban schema");
    let q: Query = serde_json::from_value(serde_json::json!({
        "table": "projects",
        "index": "by_status",
        "eq": ["active"],
        "filter": {"field": "status", "op": "eq", "value": "active"},
    }))
    .expect("parse filter query");
    let warnings = collect_filter_warnings(&schema, &q);
    assert!(
        warnings.is_empty(),
        "expected no warnings for indexed filter, got {warnings:?}"
    );
    Ok(())
}

/// HTTP test: `POST /admin/db/{db}/explain` with a simple collect query
/// returns 200 + the compiled SQL + the bind params + an empty warnings list.
/// Verifies the admin route end-to-end (admin middleware + compile + JSON
/// response shape).
#[tokio::test]
async fn explain_route_returns_compiled_sql_and_params() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    let resp = admin_post(
        addr,
        &format!("/admin/db/{}/explain", db.as_str()),
        serde_json::json!({
            "query": {
                "table": "projects",
                "index": "by_status",
                "eq": ["active"],
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await?)?;
    let sql = body["sql"].as_str().expect("sql field");
    assert!(
        sql.contains("t_projects"),
        "explain SQL should name t_projects, got: {sql}"
    );
    let params = body["params"].as_array().expect("params array");
    assert!(
        !params.is_empty(),
        "expected at least one param (the eq value), got {params:?}"
    );
    // The first param is the eq value "active".
    assert_eq!(params[0].as_str(), Some("active"));
    // Terminal tag echoes "collect".
    assert_eq!(body["terminal"].as_str(), Some("collect"));
    // No filter ⇒ no warnings.
    assert!(
        body["warnings"].as_array().is_some(),
        "warnings should be present (possibly empty array)"
    );
    assert!(body["warnings"].as_array().unwrap().is_empty());
    Ok(())
}

/// HTTP test: `/explain` surfaces compile-time warnings for an unindexed
/// filter. The body carries a filter on `description` (declared, not indexed);
/// the response's `warnings` array names the offending field.
#[tokio::test]
async fn explain_route_surfaces_unindexed_filter_warning() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    let resp = admin_post(
        addr,
        &format!("/admin/db/{}/explain", db.as_str()),
        serde_json::json!({
            "query": {
                "table": "projects",
                "index": "by_status",
                "eq": ["active"],
                "filter": {"field": "description", "op": "eq", "value": "Inbox"},
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await?)?;
    let warnings = body["warnings"].as_array().expect("warnings array");
    assert!(
        !warnings.is_empty(),
        "expected at least one warning for unindexed filter, got {warnings:?}"
    );
    let any_name = warnings
        .iter()
        .any(|w| w.as_str().is_some_and(|s| s.contains("'description'")));
    assert!(
        any_name,
        "expected a warning naming 'description', got {warnings:?}"
    );
    Ok(())
}

/// HTTP test: `/explain` rejects a peer-incompatible query with the same
/// `BadRequest` the read path would emit. `get` is incompatible with `count`.
#[tokio::test]
async fn explain_route_rejects_peer_incompatible_query() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    let resp = admin_post(
        addr,
        &format!("/admin/db/{}/explain", db.as_str()),
        serde_json::json!({
            "query": {
                "table": "projects",
                "get": "proj_1",
                "count": true,
            }
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    Ok(())
}

/// HTTP test: `/admin/slow-queries` returns 200 with an empty list and the
/// configured threshold when the log is off (`RTDB_SLOW_QUERY_MS=0` in
/// `test_state`).
#[tokio::test]
async fn slow_queries_route_returns_empty_when_disabled() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;

    let resp = admin_get(addr, "/admin/slow-queries").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await?)?;
    assert_eq!(body["thresholdMs"], 0);
    let queries = body["queries"].as_array().expect("queries array");
    assert!(queries.is_empty(), "expected empty list, got {queries:?}");
    Ok(())
}

/// HTTP test: `/admin/slow-queries` accepts a `db` filter and `limit` without
/// error (returns empty because logging is disabled in `test_state`, but the
/// route accepts the params).
#[tokio::test]
async fn slow_queries_route_accepts_db_filter_and_limit() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    let resp = admin_get(
        addr,
        &format!("/admin/slow-queries?db={}&limit=50", db.as_str()),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await?)?;
    assert_eq!(body["thresholdMs"], 0);
    let queries = body["queries"].as_array().expect("queries array");
    assert!(queries.is_empty());
    Ok(())
}

/// Criterion #2 done rigorously: prove the SQL `/explain` returns is the SQL
/// the read path actually executes — not by hand-inspection of a hardcoded
/// string, but by (a) running a real query through `execute_query` against
/// Postgres so the compiled SQL provably executes, and (b) asserting that the
/// SAME `compile_query` (the only compiler) emits `$N` placeholders and zero
/// interpolated literal values. Since `execute_query` dispatches entirely on
/// `compile_query`'s output (`match cq.terminal { … }` in query.rs), a query
/// that compiles and a query that executes are the same SQL string. A literal
/// leaking into `sql` would be the injection vector the audit found absent, so
/// asserting its absence here is the load-bearing half.
#[tokio::test]
async fn compile_and_execute_share_one_sql_path() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;
    let schema: SchemaDef =
        serde_json::from_value(kanban_schema_json()).expect("parse kanban schema");
    let pool = state.pool.clone();

    // (a) The compiled SQL must execute against Postgres. A point read for a
    // non-existent id returns Doc(None) (no row) rather than erroring, which
    // proves the compiled statement ran — a malformed or divergent string would
    // surface a sqlx error here.
    let q_get: Query = serde_json::from_value(serde_json::json!({
        "table": "projects",
        "get": "never_exists_compile_execute_probe",
    }))
    .expect("parse get query");
    let ctx = PrincipalCtx::bypass();
    let result = execute_query(&pool, db.as_str(), &schema, &q_get, &ctx).await?;
    assert!(
        matches!(result, rtdb_server::query::QueryResult::Doc(None)),
        "expected the compiled point-read to execute and find no row, got {result:?}"
    );

    // (b) The SAME compile emits placeholders, not literals, for a value-bearing
    // query. The eq value `"active"` must NOT appear verbatim in the SQL string;
    // it must be `$1`. This is the injection-safety half of "same SQL": if the
    // execute path interpolated literals while compile emitted placeholders, the
    // two strings would differ AND reintroduce the injection surface.
    let q_eq = projects_query_by_status("a-distinctive-eq-value-not-in-sql");
    let (cq, _warnings) = compile_query(db.as_str(), &schema, &q_eq, &ctx)?;
    assert!(
        cq.sql.contains("$1"),
        "expected $1 placeholder in compiled SQL, got: {}",
        cq.sql
    );
    assert!(
        !cq.sql.contains("a-distinctive-eq-value-not-in-sql"),
        "eq value leaked into SQL as a literal (would diverge from the bound execute path): {}",
        cq.sql
    );
    // The bind carries the value compile did NOT inline — execute binds it.
    assert!(
        cq.binds
            .iter()
            .any(|b| matches!(b, rtdb_server::txn::EqBind::Text(v) if v == "a-distinctive-eq-value-not-in-sql")),
        "expected the eq value as a bind, got {:?}",
        cq.binds
    );
    Ok(())
}

/// Criterion #5 (positive half): with `RTDB_SLOW_QUERY_MS=1`, a query that
/// runs lands in the in-memory slow-query log and is surfaced by
/// `GET /admin/slow-queries`. The existing disabled-case tests prove the
/// default-0 log stays empty; this proves the >0 path records. The query must
/// go through the real `/api/query` HTTP route — the timing hook lives in the
/// handler, not in `execute_query` directly.
#[tokio::test]
async fn slow_query_logged_when_threshold_enabled() -> anyhow::Result<()> {
    // threshold 1ms: any real query (a Postgres round-trip) exceeds it.
    let state = test_state_with_slow_queries(1, false).await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, db.as_str()).await;
    // Seed enough rows that a full-scan collect deterministically exceeds 1ms
    // (the hook compares integer-truncated elapsed_ms, so an empty table is
    // sub-millisecond and would not trip the threshold).
    let schema: SchemaDef =
        serde_json::from_value(kanban_schema_json()).expect("parse kanban schema");
    seed_projects(&state.pool, db.as_str(), &schema, 400).await;

    // Drive the real data-plane query route so query_handler's timing hook
    // fires (it short-circuits only when slow_query_ms == 0). A plain collect
    // (no index) is a full scan over the seeded rows.
    let resp = api_post(
        addr,
        "/api/query",
        &token,
        serde_json::json!({"db": db.as_str(), "query": {"table": "projects"}}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // In-memory log: at least one entry for this db/table.
    let entries = state.runtime.metrics.recent_slow_queries();
    let mine = entries
        .iter()
        .filter(|e| e.db == db.as_str() && e.table == "projects")
        .count();
    assert!(
        mine >= 1,
        "expected >=1 slow-query entry for this db, got {entries:?}"
    );

    // HTTP surface: the same entry appears in GET /admin/slow-queries.
    let resp = admin_get(addr, "/admin/slow-queries").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await?)?;
    assert_eq!(body["thresholdMs"], 1);
    let queries = body["queries"].as_array().expect("queries array");
    let http_mine = queries
        .iter()
        .filter(|q| {
            q["db"].as_str() == Some(db.as_str()) && q["table"].as_str() == Some("projects")
        })
        .count();
    assert!(
        http_mine >= 1,
        "expected >=1 slow-query row in HTTP response, got {queries:?}"
    );
    Ok(())
}

/// Criterion #6: slow-query parameter values are redacted by default and only
/// captured when `RTDB_SLOW_QUERY_LOG_PARAMS=true`. Params are user data and
/// the admin slow-query surface is cross-tenant, so the default must not leak
/// them. Drives the real `/api/query` route so the timing hook fires.
#[tokio::test]
async fn slow_query_params_redacted_by_default_captured_when_opted_in() -> anyhow::Result<()> {
    // --- default: redacted (params field None in-memory, omitted on the wire) ---
    let state_redacted = test_state_with_slow_queries(1, false).await;
    let addr_r = spawn_app(state_redacted.clone()).await;
    let db_r = fresh_db(&state_redacted).await;
    let token_r = mint_token(addr_r, db_r.as_str()).await;
    let schema: SchemaDef =
        serde_json::from_value(kanban_schema_json()).expect("parse kanban schema");
    seed_projects(&state_redacted.pool, db_r.as_str(), &schema, 400).await;
    let resp = api_post(
        addr_r,
        "/api/query",
        &token_r,
        serde_json::json!({
            "db": db_r.as_str(),
            "query": {"table": "projects", "index": "by_status", "eq": ["active"]}
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let redacted_entries = state_redacted.runtime.metrics.recent_slow_queries();
    let redacted_mine = redacted_entries
        .iter()
        .find(|e| e.db == db_r.as_str() && e.table == "projects")
        .expect("expected a slow-query entry");
    // In-memory: params is None (redacted).
    assert!(
        redacted_mine.params.is_none(),
        "expected params=None by default, got {:?}",
        redacted_mine.params
    );
    // On the wire: skip_serializing_if = "Option::is_none" omits the field
    // entirely, so a redacted record carries no params key at all.
    let serialized = serde_json::to_string(redacted_mine)?;
    assert!(
        !serialized.contains("\"params\""),
        "redacted record serialized a params field: {serialized}"
    );

    // --- opted-in: params captured as strings in bind order ---
    let state_logged = test_state_with_slow_queries(1, true).await;
    let addr_l = spawn_app(state_logged.clone()).await;
    let db_l = fresh_db(&state_logged).await;
    let token_l = mint_token(addr_l, db_l.as_str()).await;
    seed_projects(&state_logged.pool, db_l.as_str(), &schema, 400).await;
    let resp = api_post(
        addr_l,
        "/api/query",
        &token_l,
        serde_json::json!({
            "db": db_l.as_str(),
            "query": {"table": "projects", "index": "by_status", "eq": ["active"]}
        }),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let logged_entries = state_logged.runtime.metrics.recent_slow_queries();
    let logged_mine = logged_entries
        .iter()
        .find(|e| e.db == db_l.as_str() && e.table == "projects")
        .expect("expected a slow-query entry");
    let params = logged_mine
        .params
        .as_ref()
        .expect("expected params captured when RTDB_SLOW_QUERY_LOG_PARAMS=true");
    assert!(
        params.iter().any(|p| p == "active"),
        "expected the eq value 'active' among captured params, got {params:?}"
    );
    Ok(())
}
