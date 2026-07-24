# Realtime Dashboard — Phase 5: Admin document access (data-browser read/write) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let an authenticated dashboard admin read and write documents across every database — over HTTP (`POST /admin/db/{db}/query|mutate`, `owner=None`) and over the live WebSocket (`/sync`, where an admin bypasses the per-db `authorize` check and subscribes/mutates with `owner=None`) — with a server-enforced affected-docs cap so a dashboard mutation can never mass-edit.

**Architecture:** The admin is a new *caller* of the existing core, not a second writer. HTTP admin writes route through the existing `committers.mutate(..., owner=None)` (single writer, fan-out, op-feed tap, idempotency all fire unchanged). The `/sync` bypass is confined to `ws.rs`: compute `auth::is_admin` once at the handshake (the bearer is fixed for the connection), thread an `admin: bool` into the frame loop, and in the `Subscribe`/`Mutate` arms skip `authorize` and use `owner=None` when `admin`. The cap is a **step-count pre-check** (admin-only): every DSL step touches at most one document, so `txn.steps.len()` is an upper bound on affected docs — rejecting over-cap *before* the committer guarantees an over-cap mutation never becomes durable, with no change to `execute_txn` (which has 64 test callers).

**Tech Stack:** Rust (axum 0.8, tokio, sqlx 0.8), Postgres 17, tokio-tungstenite (WS tests).

## Global Constraints

From the spec (`docs/superpowers/specs/2026-07-24-realtime-dashboard-design.md` §2 "/sync", §5 "Data-browser write path") and repo invariants:

- **Admin = a real caller of the existing core.** Never call `execute_txn`/`execute_query` outside the committer for writes; admin writes go through `state.committers.mutate(db, key, txn, owner=None)`. No second writer, no new commit site.
- **`owner=None` for admin.** Admin reads (`execute_query(..., None)`) and writes (`mutate(..., None)`) bypass per-row `ownerField` filtering — an admin sees/touches every row. Non-admin principals keep `owner_of(principal)` exactly as today.
- **The `/sync` bypass is confined to `ws.rs`.** The security core `auth::authorize` is **not modified**. The bypass is: `is_admin(principal)` at the handshake → skip the handshake `authorize`; thread `admin: bool` → in `Subscribe`/`Mutate`, skip the per-op `authorize` and use `owner=None`. Machine principals are never admin (`is_admin` returns `false`), so machine-token `/sync` connections are unchanged. The schedule family (`Schedule`/`Cancel`/…) keeps its `authorize` call — only `Subscribe` and `Mutate` get the bypass (spec §2).
- **Cap is admin-only and a pre-check.** `RTDB_MAX_AFFECTED_DOCS` (env, default **100**), a boot `Config` field. Enforced as `if txn.steps.len() > cap { BadRequest }` **before** the mutation reaches the committer — on the HTTP admin path (`admin_mutate`) and the `/sync` admin `Mutate` arm. Normal app mutations (`/api/mutate`, non-admin `/sync`) are **uncapped** (zero impact on existing apps). The spec explicitly permits pre-check over post-check-rollback ("Exact pre-check vs. post-check-and-rollback mechanism is a plan detail; the invariant is: an over-cap mutation never becomes durable."); step-count is a sound upper bound because each step touches ≤1 doc (`Insert`/`Patch`/`Replace`/`Delete`/`Upsert` touch 1; `ExpectVersion`/`ExpectAbsent` touch 0).
- **`auth::authorize` and `auth::owner_of` signatures are unchanged.** Do not edit `auth/mod.rs` for this phase.
- Wire casing: admin request bodies are camelCase where the existing types are (`idempotencyKey`). `db` comes from the path (`/admin/db/{db}/...`), not the body.
- **Errors:** `RtDbError` envelope; unknown db → `NotFound` (`database_exists` check, matching `get_schema`); over-cap → `BadRequest`. No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`. Every value `$n`-bound.
- `make checkall` is the gate (the dev Postgres must be up; if a container already holds 55434, run the server steps directly — see `dev-workflow-conventions`). Tests share one Postgres and isolate by uniquely-named databases; never drop a db/schema you didn't create.

---

## File Structure

- **Modify `server/src/config.rs`** — add `max_affected_docs: usize` (+ `RTDB_MAX_AFFECTED_DOCS` env parse, default 100) to `Config`.
- **Modify `server/src/admin.rs`** — add `admin_query` / `admin_mutate` handlers + request/response types + `/admin/db/{db}/{query,mutate}` routes.
- **Modify `server/src/ws.rs`** — `authenticate` returns `is_admin`; thread `admin: bool` through `handle_socket` → `handle_text_frame`; `Subscribe`/`Mutate` arms apply the bypass + `owner=None`; admin `Mutate` enforces the cap.
- **Modify the 4 `Config {}` test builders** (`tests/common/mod.rs`, `tests/healthz_test.rs`, `auth/google.rs`, `auth/github.rs`) — add `max_affected_docs: 100`.
- **Modify `server/tests/dashboard_test.rs`** — Phase 5 HTTP tests (admin query owner-bypass, admin mutate, cap).
- **Modify `server/tests/ws_test.rs`** (or a new section in `dashboard_test.rs`) — `/sync` admin-bypass tests.

---

### Task 1: Admin HTTP query/mutate + `RTDB_MAX_AFFECTED_DOCS` cap

**Files:** `server/src/config.rs`, `server/src/admin.rs`, the 4 `Config {}` test builders, `server/tests/dashboard_test.rs`.

**Interfaces:**
- Produces: `Config.max_affected_docs: usize`; `POST /admin/db/{db}/query` → `admin_query`; `POST /admin/db/{db}/mutate` → `admin_mutate`.
- Consumes: `require_admin`, `db::database_exists`, `state.schemas.get`, `query::execute_query(..., owner=None)`, `state.committers.mutate(db, key, txn, None)`, `state.metrics.record_{query,mutation}`.

- [ ] **Step 1: Add `max_affected_docs` to `Config`**

In `server/src/config.rs`, add the field to the `Config` struct (after `google_client_secret`) and parse it in `from_env`:

```rust
    pub max_affected_docs: usize,     // RTDB_MAX_AFFECTED_DOCS, default 100 (admin data-browser guardrail)
```

```rust
        let max_affected_docs = match std::env::var("RTDB_MAX_AFFECTED_DOCS") {
            Ok(v) => v.parse::<usize>().unwrap_or(100),
            Err(_) => 100,
        };
```

and add `max_affected_docs,` to the `Ok(Self { … })` literal.

- [ ] **Step 2: Add the field to the 4 test `Config {}` builders**

Add `max_affected_docs: 100,` to the `Config { … }` literal in: `tests/common/mod.rs` (`test_config`), `tests/healthz_test.rs` (`test_config`), `auth/google.rs` (`from_config_returns_none_without_credentials`), `auth/github.rs` (same). (`main.rs` calls `Config::from_env`, no literal to update.)

- [ ] **Step 3: Write the failing tests**

Append to `server/tests/dashboard_test.rs`. These use the existing `common::test_state` / `spawn_app` / `mint_user_session` helpers and the admin key `"test-admin-key"`. Define a small `ownerField` schema inline (mirror `per_row_auth_test.rs`'s owner-field schema shape — a table with a string field plus `ownerField` naming it).

```rust
fn owner_field_schema() -> serde_json::Value {
    serde_json::json!({"tables":{"notes":{"fields":{
        "body":{"type":"string"},
        "owner":{"type":"string"}
    },"indexes":[{"name":"by_owner","fields":["owner"]}],"ownerField":"owner"}}})
}

// Admin document read bypasses per-row ownerField (owner=None sees every row).
#[tokio::test]
async fn admin_query_sees_all_rows_regardless_of_owner() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let pool = state.pool.clone();

    // Fresh db with an ownerField table.
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &db).await?;
    let schema: rtdb_server::schema::SchemaDef = serde_json::from_value(owner_field_schema())?;
    rtdb_server::ddl::push_schema(&pool, &db, schema).await?;

    // Two users; each inserts one note as themselves (owner stamped server-side).
    let a_tok = common::mint_user_session(&pool, "u-admin-q-a", "a@example.com").await;
    let b_tok = common::mint_user_session(&pool, "u-admin-q-b", "b@example.com").await;
    let insert = |tok: String, body: &str| {
        let db = db.clone();
        async move {
            reqwest::Client::new()
                .post(format!("http://127.0.0.1:0/api/mutate")) // placeholder; real host below
                ;
            tok
        }
    };
    let _ = (insert,); // (see note below — use the real `addr` + admin_post-style helpers)
    let _ = (a_tok, b_tok);
    // Insert one note as user A and one as user B via POST /api/mutate with each
    // session bearer, then:
    //   - admin POST /admin/db/{db}/query {query:{table:"notes"}} -> 2 rows (owner=None).
    //   - user A POST /api/query {db, query:{table:"notes"}} -> 1 row (only A's).
    // Assert admin sees both; user A sees only their own. (Use common::admin_post
    // for the admin path; raw reqwest with the session bearer for the user paths,
    // mirroring oauth_test's bearer_get.)
    Ok(())
}
```

> **Implementer note:** the sketch above is intentionally not runnable as-is — write the real insert/query requests using `reqwest::Client::new().post(format!("http://{addr}/api/mutate"))` with `.bearer_auth(tok)` for user writes, `common::admin_post`-style raw reqwest to `http://{addr}/admin/db/{db}/query` for the admin read. The assertions are the load-bearing part: **admin query returns 2 rows; user-A query returns 1**. Follow the exact request shapes in `http_api.rs` (`/api/mutate` body `{db, txn:{steps:[{insert:{table,doc}}]}}`; `/api/query` body `{db, query:{table:"notes"}}`; admin query body `{query:{table:"notes"}}`, db in path).

```rust
// Admin document write goes through the committer with owner=None; an over-cap
// mutation is rejected before it becomes durable.
#[tokio::test]
async fn admin_mutate_writes_and_cap_rejects() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let pool = state.pool.clone();
    let db = format!("t{}", uuid::Uuid::now_v7().simple());
    rtdb_server::db::create_database(&pool, &db).await?;
    let schema: rtdb_server::schema::SchemaDef =
        serde_json::from_value(common::kanban_schema_json())?;
    rtdb_server::ddl::push_schema(&pool, &db, schema).await?;
    let bearer = "Bearer test-admin-key";

    // A single-step insert succeeds and is durable.
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/admin/db/{db}/mutate"))
        .header("Authorization", bearer)
        .json(&serde_json::json!({"txn":{"steps":[{"insert":{"table":"projects","doc":{"name":"p","status":"active","order":1}}}]}}))
        .send().await?;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Over-cap: 101 insert steps > default cap 100 -> 400, and none durable.
    let steps: Vec<serde_json::Value> = (0..101)
        .map(|i| serde_json::json!({"insert":{"table":"projects","doc":{"name":format!("p{i}"),"status":"active","order":i}}}))
        .collect();
    let resp = reqwest::Client::new()
        .post(format!("http://{addr}/admin/db/{db}/mutate"))
        .header("Authorization", bearer)
        .json(&serde_json::json!({"txn":{"steps":steps}}))
        .send().await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);

    // Confirm the over-cap batch wrote nothing: query still has exactly 1 project.
    let q: serde_json::Value = reqwest::Client::new()
        .post(format!("http://{addr}/admin/db/{db}/query"))
        .header("Authorization", bearer)
        .json(&serde_json::json!({"query":{"table":"projects"}}))
        .send().await?.json().await?;
    assert_eq!(q["result"]["values"].as_array().unwrap().len(), 1);
    Ok(())
}
```

> Confirm the `QueryResult` JSON shape (`values` array) by reading `query.rs`'s `QueryResult` Serialize — match the field name exactly in the assertion. The kanban `projects` table requires `name/status/order` (see `common::kanban_schema_json`), so the insert docs above satisfy its schema.

- [ ] **Step 4: Run the tests to verify they fail**

`cd server && cargo test --test dashboard_test admin_query_sees_all_rows admin_mutate_writes_and_cap_rejects` → FAIL (no `/admin/db/{db}/*` routes yet).

- [ ] **Step 5: Add `admin_query` and `admin_mutate` to `admin.rs`**

Add imports (alongside the existing `use crate::{AppState, auth, db, ddl, snapshot};`):

```rust
use crate::query::{Query, QueryResult, execute_query};
use crate::txn::Transaction;
```

Add the handlers (place near `get_schema`/`db_stats`):

```rust
#[derive(Deserialize)]
struct AdminQueryRequest {
    query: Query,
}

#[derive(Serialize)]
struct AdminQueryResponse {
    result: QueryResult,
}

/// `POST /admin/db/{db}/query` — admin document read. `owner=None` bypasses
/// per-row `ownerField`, so an admin sees every row in every table.
async fn admin_query(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<AdminQueryRequest>,
) -> Result<Json<AdminQueryResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let schema = state.schemas.get(&state.pool, &db).await?;
    let result = execute_query(&state.pool, &db, &schema, &body.query, None).await?;
    state.metrics.record_query();
    Ok(Json(AdminQueryResponse { result }))
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AdminMutateRequest {
    txn: Transaction,
    #[serde(default)]
    idempotency_key: Option<String>,
}

#[derive(Serialize)]
struct AdminMutateResponse {
    results: Vec<serde_json::Value>,
}

/// `POST /admin/db/{db}/mutate` — admin document write through the existing
/// committer (`owner=None`). The step-count cap is the server-side guardrail:
/// each step touches at most one doc, so rejecting over-cap here guarantees an
/// over-cap mutation never reaches the committer (never becomes durable).
async fn admin_mutate(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(db): Path<String>,
    ApiJson(body): ApiJson<AdminMutateRequest>,
) -> Result<Json<AdminMutateResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    if !db::database_exists(&state.pool, &db).await? {
        return Err(RtDbError::not_found("unknown database"));
    }
    let cap = state.config.max_affected_docs;
    if body.txn.steps.len() > cap {
        return Err(RtDbError::bad_request(format!(
            "mutation has {} step(s), exceeding the limit of {cap}",
            body.txn.steps.len()
        )));
    }
    let outcome = state
        .committers
        .mutate(&db, body.idempotency_key, body.txn, None)
        .await?;
    state.metrics.record_mutation();
    Ok(Json(AdminMutateResponse {
        results: outcome.results,
    }))
}
```

- [ ] **Step 6: Register the routes**

In `admin_routes()`, add:

```rust
        .route("/admin/db/{db}/query", post(admin_query))
        .route("/admin/db/{db}/mutate", post(admin_mutate))
```

- [ ] **Step 7: Run the tests + gate**

`cd server && cargo test --test dashboard_test admin_query_sees_all_rows admin_mutate_writes_and_cap_rejects` → PASS. Then `cargo fmt --all -- --check && cargo clippy --all-targets -- -D warnings && cargo test` → all green.

- [ ] **Step 8: Commit**

```bash
git add -A && git commit -m "feat(server): admin document read/write + RTDB_MAX_AFFECTED_DOCS cap (#18 phase 5)"
```

---

### Task 2: `/sync` admin bypass + cap

**Files:** `server/src/ws.rs`, `server/tests/ws_test.rs` (or `dashboard_test.rs`).

**Interfaces:**
- Consumes: `auth::is_admin` (computed once at handshake), `auth::authorize`, `auth::owner_of`, `state.config.max_affected_docs`.
- Produces: `authenticate` returns `(Principal, String, bool)` (the bool is `is_admin`); `handle_text_frame` gains `admin: bool`.

- [ ] **Step 1: Write the failing test (admin connects to a non-allowlisted db)**

In `server/tests/ws_test.rs` (mirror the existing WS connection helpers — `tokio_tungstenite` connect, send `Auth`, expect `AuthOk`/`AuthErr`). Use `common::test_state` + `spawn_app`, `common::mint_user_session` for an admin OAuth session, and add that user to `rtdb_auth.admins`:

```text
// admin_ws_bypasses_authorize_for_any_db:
//   - spawn app; mint an OAuth session for user U; INSERT U into rtdb_auth.admins.
//   - open /sync to a db U is NOT allowlisted for, Auth{token=U_session, db}.
//     expect AuthOk (admin bypass).   <- the assertion
//   - a second user V (NOT an admin, not allowlisted) -> AuthErr / close.   <- negative
```

Also: **admin sees all rows on an ownerField table** over `/sync` — subscribe as admin to a table with rows owned by another user, expect the initial `QueryUpdate` to contain all rows; subscribe as the owning user, expect only their row.

> Follow the exact WS handshake sequence used by the existing `ws_test.rs` tests (the `Auth` first frame, `AuthOk` reply, then `Subscribe`). Read `ws_test.rs`'s helper that opens a connection and performs auth; reuse it.

- [ ] **Step 2: Run to verify it fails** — admin connection to a non-allowlisted db is currently rejected (`authorize` fails → `AuthErr`).

- [ ] **Step 3: Compute `is_admin` at the handshake and skip `authorize` for admins**

In `server/src/ws.rs`, change `authenticate`'s return type to `Option<(Principal, String, bool)>` and its body: after `resolve_bearer` succeeds, compute `let admin = auth::is_admin(&state.pool, &principal).await;` and only run `authorize` when `!admin`:

```rust
    let principal = match resolve_bearer(&state.pool, &token).await {
        Ok(principal) => principal,
        Err(err) => { fail_and_close(socket, err).await; return None; }
    };
    // Admin OAuth sessions are authorized for every database (dashboard live
    // tables). `is_admin` is computed once here — the bearer is fixed for the
    // connection — and threaded to every Subscribe/Mutate so each op re-applies
    // the bypass without another lookup. Machine principals are never admin.
    let admin = auth::is_admin(&state.pool, &principal).await;
    if !admin {
        if let Err(err) = authorize(&state.pool, &principal, &db).await {
            fail_and_close(socket, err).await;
            return None;
        }
    }
    // ...existing AuthOk send...
    Some((principal, db, admin))
```

Update `handle_socket`: `let Some((principal, db, admin)) = authenticate(&mut socket, &state).await else { return };` and pass `admin` as a new arg to every `handle_text_frame(...)` call.

- [ ] **Step 4: Thread `admin` through `handle_text_frame` and apply the bypass**

Add `admin: bool` to `handle_text_frame`'s signature (and its doc-comment param count note). Replace the `Subscribe` and `Mutate` arms:

```rust
        ClientMessage::Subscribe { query_id, query } => {
            let authed = if admin {
                Ok(())
            } else {
                authorize(&state.pool, principal, db).await
            };
            match authed {
                Ok(()) => {
                    let owner = if admin {
                        None
                    } else {
                        owner_of(principal).map(|s| s.to_string())
                    };
                    if let Err(error) = state
                        .committers
                        .subscribe(db, conn_id, query_id.clone(), *query, out_tx.clone(), owner)
                        .await
                    {
                        let _ = out_tx.send(ServerMessage::SubscribeErr { query_id, error });
                    } else {
                        state.metrics.record_query();
                    }
                }
                Err(error) => {
                    let _ = out_tx.send(ServerMessage::SubscribeErr { query_id, error });
                }
            }
            false
        }
```

```rust
        ClientMessage::Mutate { mut_id, idempotency_key, txn } => {
            let authed = if admin {
                Ok(())
            } else {
                authorize(&state.pool, principal, db).await
            };
            match authed {
                Ok(()) => {
                    let cap = state.config.max_affected_docs;
                    if admin && txn.steps.len() > cap {
                        let _ = out_tx.send(ServerMessage::MutateErr {
                            mut_id,
                            error: RtDbError::bad_request(format!(
                                "mutation has {} step(s), exceeding the limit of {cap}",
                                txn.steps.len()
                            )),
                        });
                        return false;
                    }
                    let owner = if admin {
                        None
                    } else {
                        owner_of(principal).map(|s| s.to_string())
                    };
                    match state.committers.mutate(db, idempotency_key, txn, owner).await {
                        Ok(outcome) => {
                            state.metrics.record_mutation();
                            let _ = out_tx.send(ServerMessage::MutateOk {
                                mut_id,
                                results: outcome.results,
                            });
                        }
                        Err(error) => {
                            let _ = out_tx.send(ServerMessage::MutateErr { mut_id, error });
                        }
                    }
                }
                Err(error) => {
                    let _ = out_tx.send(ServerMessage::MutateErr { mut_id, error });
                }
            }
            false
        }
```

Leave the `Unsubscribe`, `Ping`, and schedule arms unchanged (they don't take `owner`, and the schedule family keeps `authorize` per spec §2).

- [ ] **Step 5: Run the tests + gate**

`cd server && cargo test --test ws_test admin_ws` (and the owner-bypass subscribe test) → PASS. Then fmt + clippy + full `cargo test` → all green.

- [ ] **Step 6: Commit**

```bash
git add -A && git commit -m "feat(server): /sync admin bypass (is_admin → skip authorize + owner=None) + cap (#18 phase 5)"
```

---

## Self-Review (controller, before final review)

- **Spec coverage:** §2 `/sync` admin bypass → Task 2 (is_admin at handshake + Subscribe/Mutate arms, schedule family unchanged). §5 admin read/write → Task 1 (`/admin/db/{db}/query|mutate`, `owner=None`). §5 cap → Tasks 1+2 (step-count pre-check, admin-only, default 100). ✅
- **Invariants:** No second writer (admin writes via `committers.mutate`); `auth::authorize`/`owner_of` unmodified; non-admin paths byte-identical in behavior; over-cap never durable (pre-check before committer). ✅
- **Cap soundness:** `affected_docs ≤ steps.len()` (each step ≤1 doc), so `steps.len() ≤ cap ⟹ affected ≤ cap` — an over-cap mutation is rejected before execution. Documented as the spec-permitted pre-check choice. ✅
- **No test churn in the core:** `execute_txn` signature unchanged (64 test callers safe); `Committers::mutate` signature unchanged (35 callers safe). ✅
