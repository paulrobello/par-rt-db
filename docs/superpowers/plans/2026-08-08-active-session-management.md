# Active-Session Management Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give operators an admin HTTP + dashboard surface to list and revoke interactive sessions, and make a revoked session take effect on the next WS op over an already-open connection.

**Architecture:** Three additions on the existing auth substrate. (1) Carry the session's `token_hash` on `Principal::User` and add a per-op `SELECT EXISTS` liveness check that runs for every interactive principal — admin or not — so a deleted session row is denied on its next op (mirrors the machine-token live-revoke pattern). (2) A new `admin/sessions.rs` submodule with `GET /admin/sessions`, `DELETE /admin/sessions/{token_hash}`, `DELETE /admin/sessions?user=` — all `require_admin`, backed by new `auth/session.rs` list/delete functions. (3) Mirror the surface in ts-client (`listSessions`/`revokeSession`/`revokeUserSessions`) and a dashboard `SessionsPage`.

**Tech Stack:** Rust (axum/sqlx/tokio), TypeScript (ts-client + Vite/React dashboard), Vitest, reqwest + tokio-tungstenite for integration tests.

## Global Constraints

- `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests) is the gate; must be green before the card closes. Integration tests need `make dev-db-up` (dev Postgres on `127.0.0.1:55434`).
- Server is the source of truth for the wire contract; DTO casing is camelCase on the wire (`#[serde(rename_all = "camelCase")]`).
- No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings.
- `Principal` is internal (`Debug + Clone`, never serialized) — adding a field does not touch the wire.
- SQL: bind every value via `$n`; `token_hash`/`user_id` are values, not identifiers.
- Errors: `RtDbError::unauthorized` → wire code `UNAUTHORIZED`; `bad_request` → `BAD_REQUEST`. Unknown `token_hash` on delete is idempotent (not an error).
- Spec: `docs/superpowers/specs/2026-08-08-active-session-management-design.md`. Read it first.
- Commit after every task (trunk-based repo; commit on the worktree branch).

---

### Task 1: Live session revocation — principal field + per-op check (non-admin path)

This is the correctness core for acceptance criterion #2. Adding a field to `Principal::User` breaks every construction site; that ripple is part of this task.

**Files:**
- Modify: `server/src/auth/mod.rs` (enum field, accessor, `session_still_valid`, `authorize` User arm)
- Modify: `server/src/auth/session.rs` (`resolve_session` sets the hash)
- Modify: every other `Principal::User { … }` construction site (set `session_hash: None`): `server/src/auth/provider.rs`, `server/src/auth/mod.rs` (test literals), `server/tests/common/mod.rs`, `server/tests/anonymous_auth_test.rs`, `server/tests/http_api_test.rs`, `server/tests/per_row_auth_test.rs` — 17 sites total across these files
- Test: `server/tests/ws_test.rs` (add one integration test)

**Interfaces:**
- Produces: `Principal::session_hash(&self) -> Option<&str>`; `pub async fn session_still_valid(pool: &PgPool, principal: &Principal) -> Result<(), RtDbError>` (in `auth/mod.rs`); `Principal::User` gains `session_hash: Option<String>`.

- [ ] **Step 1: Add the field + accessor + helper (failing compile is the test for the wiring)**

In `server/src/auth/mod.rs`, add the field to the `User` variant (after `github_login`):

```rust
        /// sha256 digest of the session token == `rtdb_auth.sessions.token_hash`
        /// PK. `None` for principals not built from a session row (test fixtures,
        /// the OAuth-callback principal that is never the connection principal).
        /// Set by `session::resolve_session`; the per-op `session_still_valid`
        /// check reads it to deny a revoked session on its next op.
        session_hash: Option<String>,
```

Add an accessor on `impl Principal` (next to `is_read_only`):

```rust
    /// The `rtdb_auth.sessions.token_hash` backing this principal, if any.
    /// `None` for `Machine` and for `User` principals not built from a session
    /// row. Used by the per-op live-revocation check (`session_still_valid`).
    pub fn session_hash(&self) -> Option<&str> {
        match self {
            Principal::User { session_hash, .. } => session_hash.as_deref(),
            Principal::Machine { .. } => None,
        }
    }
```

Add the liveness helper (after `authorize`):

```rust
/// Live check that the session backing `principal` still exists (has not been
/// revoked via the admin surface). Mirrors the machine-token per-op re-check:
/// a session deleted mid-connection must be denied on its very next op over an
/// already-open `/sync`. `Ok(())` for principals with no session hash. Errors
/// `Unauthorized` ("session revoked") when the row is gone. Expiry is handled
/// separately by the cached `expires_at` comparison in `authorize`; this check
/// is purely for revocation (row deletion).
pub async fn session_still_valid(
    pool: &PgPool,
    principal: &Principal,
) -> Result<(), RtDbError> {
    let Some(hash) = principal.session_hash() else {
        return Ok(());
    };
    let (live,): (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM rtdb_auth.sessions WHERE token_hash = $1)",
    )
    .bind(hash)
    .fetch_one(pool)
    .await?;
    if live {
        Ok(())
    } else {
        Err(RtDbError::unauthorized("session revoked"))
    }
}
```

- [ ] **Step 2: Fold the check into the `authorize` User arm**

In `authorize` (`auth/mod.rs`), at the top of the `Principal::User { .. }` arm (before the `expires_at` check so a revoked session is rejected even if its cached expiry is still future):

```rust
        Principal::User {
            expires_at, ..
        } => {
            // Live revocation: a session deleted via the admin surface must be
            // denied on its next op. (Non-admin path; admins bypass `authorize`
            // per-op and are covered by the explicit check in the WS arms.)
            session_still_valid(pool, principal).await?;
            if *expires_at < now_ms() {
                return Err(RtDbError::unauthorized("session expired"));
            }
            // …existing anonymous + allowlist logic unchanged…
```

Note the match binding must drop `session_hash` from the moved set or keep `..`; use `..` (the arm only reads `expires_at`, `email`, `anonymous`). Keep the existing explicit bindings the code already has — only insert the `session_still_valid` call.

- [ ] **Step 3: Populate the hash in `resolve_session`**

In `server/src/auth/session.rs` `resolve_session`, the `hash` local already holds `sha256_hex(token)`. Set it on the returned principal:

```rust
    Ok(Some(Principal::User {
        user_id,
        email,
        name: None,
        expires_at,
        anonymous,
        github_id,
        github_login: github_id.is_some().then_some(login),
        session_hash: Some(hash),
    }))
```

- [ ] **Step 4: Fix the other 16 construction sites**

`grep -rn "Principal::User {" server/src server/tests`. At every site other than `resolve_session`, add `session_hash: None,` to the struct literal. Files: `auth/provider.rs` (1), `auth/mod.rs` test module (3), `tests/common/mod.rs`, `tests/anonymous_auth_test.rs`, `tests/http_api_test.rs`, `tests/per_row_auth_test.rs`. Do NOT set `Some` anywhere except `resolve_session` — those principals are not the connection-auth principal.

- [ ] **Step 5: Write the failing integration test (criterion #2, non-admin path)**

Append to `server/tests/ws_test.rs` (reuses that file's `ws_connect`/`send_json`/`recv_json`/`auth`/`insert_work_item_txn` helpers and the already-imported `mint_user_session`, `admin_post`):

```rust
// Live session revocation: a session deleted mid-connection is rejected on the
// NEXT mutate over the SAME open socket (UNAUTHORIZED, not a close), and the
// connection stays usable (a following ping still pongs). Revoke is done by the
// same row DELETE the admin endpoint performs — proving the per-op check.
#[tokio::test]
async fn revoked_session_is_rejected_on_next_ws_op_over_open_connection() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    let token = mint_user_session(&state.pool, "u_rev", "urev@example.com").await;
    let add = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": db.as_str(), "action": "add", "email": "urev@example.com"}),
    )
    .await;
    assert_eq!(add.status(), reqwest::StatusCode::OK);

    let mut ws = ws_connect(addr).await;
    let auth_msg = auth(&mut ws, &token, db.as_str()).await;
    assert_eq!(auth_msg["type"], json!("authOk"));

    // mutate succeeds while the session is live
    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m1", "txn": insert_work_item_txn()}),
    )
    .await;
    let mut saw_ok = false;
    for _ in 0..2 {
        let m = recv_json(&mut ws).await;
        if m["type"] == json!("mutateOk") {
            assert_eq!(m["mutId"], json!("m1"));
            saw_ok = true;
        }
    }
    assert!(saw_ok, "expected mutateOk before revocation");

    // revoke the session directly (exactly what DELETE /admin/sessions/{hash} does)
    let hash = rtdb_server::db::sha256_hex(&token);
    sqlx::query("DELETE FROM rtdb_auth.sessions WHERE token_hash = $1")
        .bind(&hash)
        .execute(&state.pool)
        .await?;

    // the NEXT mutate on the SAME open connection is now rejected
    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m2", "txn": insert_work_item_txn()}),
    )
    .await;
    let err_msg = recv_json(&mut ws).await;
    assert_eq!(err_msg["type"], json!("mutateErr"));
    assert_eq!(err_msg["mutId"], json!("m2"));
    assert_eq!(err_msg["error"]["code"], json!("UNAUTHORIZED"));

    // connection stays open (revocation errors the op, does not close)
    send_json(&mut ws, json!({"type": "ping"})).await;
    assert_eq!(recv_json(&mut ws).await["type"], json!("pong"));

    Ok(())
}
```

- [ ] **Step 6: Run the test and the auth/ws suites**

Run: `cd server && cargo test --test ws_test revoked_session_is_rejected_on_next_ws_op_over_open_connection` (dev Postgres must be up: `make dev-db-up`).
Expected: PASS. Also run `cargo test --test ws_test --test oauth_test --test auth_test --test per_row_auth_test --test anonymous_auth_test --test http_api_test` to confirm the construction-site fixes didn't break existing tests.
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add server/src/auth/mod.rs server/src/auth/session.rs server/src/auth/provider.rs \
        server/tests/common/mod.rs server/tests/ws_test.rs \
        server/tests/anonymous_auth_test.rs server/tests/http_api_test.rs server/tests/per_row_auth_test.rs
git commit -m "feat(auth): live per-op session revocation on open connections"
```

---

### Task 2: Live revocation for admin sessions (WS admin-bypass arms)

Admin principals skip `authorize` per-op (`ws.rs` Subscribe/Mutate arms: `if admin { Ok(()) } else { authorize(..) }`), so the Task-1 check inside `authorize` never runs for them. Add `session_still_valid` to the two admin branches so an admin whose session is revoked is also kicked on the next op.

**Files:**
- Modify: `server/src/ws.rs` (Subscribe arm ~line 356, Mutate arm ~line 427)
- Test: `server/tests/ws_test.rs` (add one integration test)

**Interfaces:**
- Consumes: `auth::session_still_valid` (Task 1).
- Produces: admin-session live revocation parity (no new public API).

- [ ] **Step 1: Add the check to both admin branches**

In `server/src/ws.rs`, the Subscribe arm currently:

```rust
            let admin = is_admin(&state.pool, principal).await;
            let authed = if admin {
                Ok(())
            } else {
                authorize(&state.pool, principal, db).await
            };
```

Change the `if admin` branch to also enforce session liveness (admins still must hold a live session — revocation applies to every interactive principal):

```rust
            let admin = is_admin(&state.pool, principal).await;
            let authed = if admin {
                session_still_valid(&state.pool, principal).await
            } else {
                authorize(&state.pool, principal, db).await
            };
```

Make the identical change in the Mutate arm (~line 427). Add `session_still_valid` to the existing `use crate::auth::{…}` import at the top of `ws.rs`.

- [ ] **Step 2: Write the failing test (admin session, revoke, next op errors)**

Append to `server/tests/ws_test.rs`:

```rust
// An admin OAuth session bypasses per-db `authorize`, so the session-liveness
// check must run on the admin branch too: revoking an admin's session rejects
// the next mutate over the SAME open connection.
#[tokio::test]
async fn revoked_admin_session_is_rejected_on_next_ws_op() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;

    let token = mint_user_session(&state.pool, "u_admin", "admin@example.com").await;
    // make the user a dashboard admin (server-wide)
    sqlx::query("INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, NULL, $2)")
        .bind("admin@example.com")
        .bind(rtdb_server::db::now_ms())
        .execute(&state.pool)
        .await?;

    let mut ws = ws_connect(addr).await;
    let auth_msg = auth(&mut ws, &token, db.as_str()).await;
    assert_eq!(auth_msg["type"], json!("authOk"));

    // admin mutate succeeds (bypasses authorize) while the session is live
    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m1", "txn": insert_work_item_txn()}),
    )
    .await;
    let mut saw_ok = false;
    for _ in 0..2 {
        let m = recv_json(&mut ws).await;
        if m["type"] == json!("mutateOk") {
            saw_ok = true;
        }
    }
    assert!(saw_ok, "expected mutateOk before revocation");

    let hash = rtdb_server::db::sha256_hex(&token);
    sqlx::query("DELETE FROM rtdb_auth.sessions WHERE token_hash = $1")
        .bind(&hash)
        .execute(&state.pool)
        .await?;

    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m2", "txn": insert_work_item_txn()}),
    )
    .await;
    let err_msg = recv_json(&mut ws).await;
    assert_eq!(err_msg["type"], json!("mutateErr"));
    assert_eq!(err_msg["error"]["code"], json!("UNAUTHORIZED"));
    Ok(())
}
```

- [ ] **Step 3: Run the test**

Run: `cd server && cargo test --test ws_test revoked_admin_session_is_rejected_on_next_ws_op`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add server/src/ws.rs server/tests/ws_test.rs
git commit -m "feat(auth): enforce session revocation on admin WS ops"
```

---

### Task 3: `auth/session.rs` list + delete functions

The DB-layer functions the admin handlers will call. Pure data access; unit-testable.

**Files:**
- Modify: `server/src/auth/session.rs` (add three fns)
- Test: `server/src/auth/session.rs` `#[cfg(test)]` module is absent today — add the coverage as an integration test instead: `server/tests/sessions_test.rs` (new file, created in Task 4 alongside the HTTP tests). For this task, add a focused integration test here.

**Interfaces:**
- Produces:
  - `pub async fn list_sessions(pool, user_filter: Option<&str>, limit: i64) -> Result<Vec<SessionInfo>, RtDbError>`
  - `pub async fn delete_session_by_hash(pool, token_hash: &str) -> Result<u64, RtDbError>`
  - `pub async fn delete_sessions_for_user(pool, user_id: &str) -> Result<u64, RtDbError>`
  - `pub struct SessionInfo { token_hash, user_id, email, login, anonymous, created_at, expires_at }` (all `pub`, `#[derive(Serialize)]` with `#[serde(rename_all = "camelCase")]`)

- [ ] **Step 1: Add the `SessionInfo` struct + three functions**

In `server/src/auth/session.rs`:

```rust
use serde::Serialize;

/// One row of the admin sessions list. `token_hash` is a non-reversible sha256
/// digest (the plaintext token is never stored), so it is safe to surface to an
/// authenticated admin and lets the UI target a specific row.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    pub token_hash: String,
    pub user_id: String,
    pub email: Option<String>,
    /// Display hint: GitHub handle when the user has a `github_id`, else the
    /// stored display name (same convention as `resolve_session`).
    pub login: Option<String>,
    pub anonymous: bool,
    pub created_at: i64,
    pub expires_at: i64,
}

/// Lists sessions newest-first. When `user_filter` is `Some`, matches rows whose
/// `user_id` OR `users.email` equals it (an operator may paste either). `limit`
/// is clamped to `[1, 1000]` by the caller.
pub async fn list_sessions(
    pool: &PgPool,
    user_filter: Option<&str>,
    limit: i64,
) -> Result<Vec<SessionInfo>, RtDbError> {
    // (token_hash, user_id, email, login, anonymous, created_at, expires_at)
    type Row = (String, String, Option<String>, Option<String>, bool, i64, i64);
    let rows: Vec<Row> = if let Some(u) = user_filter {
        sqlx::query_as(
            "SELECT s.token_hash, s.user_id, u.email, u.login, u.anonymous, \
                    s.created_at, s.expires_at \
             FROM rtdb_auth.sessions s JOIN rtdb_auth.users u ON u.id = s.user_id \
             WHERE s.user_id = $1 OR u.email = $1 \
             ORDER BY s.created_at DESC LIMIT $2",
        )
        .bind(u)
        .bind(limit)
        .fetch_all(pool)
        .await?
    } else {
        sqlx::query_as(
            "SELECT s.token_hash, s.user_id, u.email, u.login, u.anonymous, \
                    s.created_at, s.expires_at \
             FROM rtdb_auth.sessions s JOIN rtdb_auth.users u ON u.id = s.user_id \
             ORDER BY s.created_at DESC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(pool)
        .await?
    };
    Ok(rows
        .into_iter()
        .map(|(token_hash, user_id, email, login, anonymous, created_at, expires_at)| {
            SessionInfo {
                token_hash,
                user_id,
                email,
                login,
                anonymous,
                created_at,
                expires_at,
            }
        })
        .collect())
}

/// Deletes one session by its token_hash (the admin revoke-one path). Idempotent:
/// returns 0 if the row is already gone — never an error.
pub async fn delete_session_by_hash(pool: &PgPool, token_hash: &str) -> Result<u64, RtDbError> {
    let result = sqlx::query("DELETE FROM rtdb_auth.sessions WHERE token_hash = $1")
        .bind(token_hash)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}

/// Deletes every session for `user_id` (the admin revoke-all path). Idempotent.
pub async fn delete_sessions_for_user(pool: &PgPool, user_id: &str) -> Result<u64, RtDbError> {
    let result = sqlx::query("DELETE FROM rtdb_auth.sessions WHERE user_id = $1")
        .bind(user_id)
        .execute(pool)
        .await?;
    Ok(result.rows_affected())
}
```

- [ ] **Step 2: Write the failing integration test**

Create `server/tests/sessions_test.rs`:

```rust
mod common;
use common::{mint_user_session, test_state};
use rtdb_server::auth::session::{
    delete_session_by_hash, delete_sessions_for_user, list_sessions,
};
use rtdb_server::db::sha256_hex;

#[tokio::test]
async fn list_and_delete_sessions_works() -> anyhow::Result<()> {
    let state = test_state().await;
    mint_user_session(&state.pool, "u_list_a", "a@example.com").await;
    mint_user_session(&state.pool, "u_list_a", "a@example.com").await; // second session, same user
    mint_user_session(&state.pool, "u_list_b", "b@example.com").await;

    // list all (≥3 seeded here; other tests may add more) — filter to user A
    let for_a = list_sessions(&state.pool, Some("u_list_a"), 1000).await?;
    assert_eq!(for_a.len(), 2, "two sessions for user A");
    assert!(for_a.iter().all(|s| s.user_id == "u_list_a"));
    assert_eq!(for_a[0].email.as_deref(), Some("a@example.com"));

    // filter by email also works
    let by_email = list_sessions(&state.pool, Some("b@example.com"), 1000).await?;
    assert_eq!(by_email.len(), 1);

    // newest-first ordering
    assert!(for_a[0].created_at >= for_a[1].created_at);

    // revoke one of A's sessions by hash
    let hash = sha256_hex("unused"); // placeholder; fetch a real hash from the list
    let real_hash = for_a[0].token_hash.clone();
    let _ = hash; // (suppress unused — real test uses real_hash)
    let n = delete_session_by_hash(&state.pool, &real_hash).await?;
    assert_eq!(n, 1);
    assert_eq!(list_sessions(&state.pool, Some("u_list_a"), 1000).await?.len(), 1);

    // revoke all remaining for A
    let n = delete_sessions_for_user(&state.pool, "u_list_a").await?;
    assert_eq!(n, 1);
    assert_eq!(list_sessions(&state.pool, Some("u_list_a"), 1000).await?.len(), 0);

    // idempotent: deleting a gone hash is 0, not an error
    assert_eq!(delete_session_by_hash(&state.pool, &real_hash).await?, 0);
    Ok(())
}
```

(Remove the `hash`/`_ = hash` placeholder lines before committing — keep only `real_hash`. They are here only to show the hash comes from the list, not by recomputing it.)

- [ ] **Step 3: Run the test**

Run: `cd server && cargo test --test sessions_test list_and_delete_sessions_works`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add server/src/auth/session.rs server/tests/sessions_test.rs
git commit -m "feat(auth): session list/delete-by-hash/delete-for-user helpers"
```

---

### Task 4: Admin HTTP surface (`admin/sessions.rs` + routes)

Acceptance criterion #1: admin can list + revoke-one + revoke-all over HTTP, and unauthenticated is rejected.

**Files:**
- Create: `server/src/admin/sessions.rs`
- Modify: `server/src/admin/mod.rs` (`mod sessions;`, `use sessions::*;`, register 3 routes)
- Test: `server/tests/sessions_test.rs` (add HTTP tests)

**Interfaces:**
- Consumes: `auth::session::{list_sessions, delete_session_by_hash, delete_sessions_for_user, SessionInfo}` (Task 3); `super::{require_admin, OkResponse}`.
- Produces: `GET /admin/sessions`, `DELETE /admin/sessions/{token_hash}`, `DELETE /admin/sessions?user=`.

- [ ] **Step 1: Write the failing HTTP tests**

Append to `server/tests/sessions_test.rs`:

```rust
use common::{admin_delete, admin_get, spawn_app};
use rtdb_server::auth::session::list_sessions;
use serde_json::Value;

#[tokio::test]
async fn admin_can_list_revoke_one_and_revoke_all_sessions() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    mint_user_session(&state.pool, "u_http", "http@example.com").await;
    mint_user_session(&state.pool, "u_http", "http@example.com").await;

    // GET list (server-wide, filtered by user)
    let resp = admin_get(addr, "/admin/sessions?user=u_http").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    let sessions = body["sessions"].as_array().expect("sessions array");
    assert_eq!(sessions.len(), 2);
    assert!(sessions[0]["tokenHash"].is_string());

    // DELETE one by hash
    let hash = sessions[0]["tokenHash"].as_str().unwrap();
    let resp = admin_delete(addr, &format!("/admin/sessions/{hash}")).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(list_sessions(&state.pool, Some("u_http"), 1000).await?.len(), 1);

    // DELETE all for user (bare path + ?user=)
    let resp = admin_delete(addr, "/admin/sessions?user=u_http").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: Value = resp.json().await?;
    assert_eq!(body["revoked"], 1);
    assert_eq!(list_sessions(&state.pool, Some("u_http"), 1000).await?.len(), 0);
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
        .send().await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    Ok(())
}

#[tokio::test]
async fn revoke_all_without_user_is_bad_request() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let resp = admin_delete(addr, "/admin/sessions").await; // no ?user=
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    Ok(())
}
```

- [ ] **Step 2: Create `server/src/admin/sessions.rs`**

```rust
//! Admin interactive-session routes: list, revoke one, revoke all for a user.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, Query as QueryParams, State};
use axum::http::HeaderMap;
use serde::{Deserialize, Serialize};

use crate::auth::session::{self, SessionInfo};
use crate::error::RtDbError;

use super::{OkResponse, require_admin};

const DEFAULT_LIMIT: i64 = 200;
const MAX_LIMIT: i64 = 1000;

#[derive(Deserialize)]
pub(super) struct SessionsParams {
    /// Optional: match `user_id` OR `email`. Omitted ⇒ all sessions (server-wide).
    #[serde(default)]
    user: Option<String>,
    /// Optional; clamped to [1, 1000], default 200.
    #[serde(default)]
    limit: Option<i64>,
}

#[derive(Serialize)]
pub(super) struct SessionsResponse {
    sessions: Vec<SessionInfo>,
}

pub(super) async fn list_sessions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<SessionsParams>,
) -> Result<Json<SessionsResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT);
    let sessions = session::list_sessions(&state.pool, params.user.as_deref(), limit).await?;
    Ok(Json(SessionsResponse { sessions }))
}

/// Revoke a single session by its `token_hash` (path param).
pub(super) async fn revoke_session_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(token_hash): Path<String>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    session::delete_session_by_hash(&state.pool, &token_hash).await?;
    Ok(Json(OkResponse { ok: true }))
}

#[derive(Deserialize)]
pub(super) struct RevokeUserParams {
    user: String,
}

#[derive(Serialize)]
pub(super) struct RevokeUserResponse {
    ok: bool,
    revoked: u64,
}

/// Revoke every session for a user. Requires `?user=` — a bare DELETE is a 400
/// (refuse to revoke every session instance-wide from one unscoped call).
pub(super) async fn revoke_user_sessions_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<RevokeUserParams>,
) -> Result<Json<RevokeUserResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let revoked = session::delete_sessions_for_user(&state.pool, &params.user).await?;
    Ok(Json(RevokeUserResponse { ok: true, revoked }))
}
```

- [ ] **Step 3: Register the module + routes in `admin/mod.rs`**

Add to the module declarations (alphabetical, after `mod schedules;`):

```rust
mod sessions;
```

Add to the `use …::*;` block:

```rust
use sessions::*;
```

In `admin_routes()`, add (near the `/admin/tokens` route):

```rust
        .route(
            "/admin/sessions",
            get(list_sessions_handler).delete(revoke_user_sessions_handler),
        )
        .route("/admin/sessions/{token_hash}", delete(revoke_session_handler))
```

- [ ] **Step 4: Run the tests**

Run: `cd server && cargo test --test sessions_test`
Expected: all PASS (the three HTTP tests + the Task-3 helper test).

- [ ] **Step 5: Commit**

```bash
git add server/src/admin/sessions.rs server/src/admin/mod.rs server/tests/sessions_test.rs
git commit -m "feat(admin): GET/DELETE /admin/sessions list + revoke surface"
```

---

### Task 5: ts-client mirror (`listSessions` / `revokeSession` / `revokeUserSessions`)

**Files:**
- Modify: `ts-client/src/admin.ts` (DTO + 3 methods)
- Modify: `ts-client/src/in_memory.ts` (parallel `InMemoryAdminClient` stubs)
- Modify: `ts-client/src/index.ts` (re-export `SessionInfo`)
- Test: `ts-client/tests/admin.test.ts`

**Interfaces:**
- Consumes: the HTTP endpoints from Task 4.
- Produces: `SessionInfo`, `RtDbAdminClient.listSessions`, `.revokeSession`, `.revokeUserSessions`.

- [ ] **Step 1: Write the failing test**

In `ts-client/tests/admin.test.ts`, mirror the existing `listTokens`/`revokeToken` test pattern (find one with `mock-fetch` / the in-memory client and copy its shape). Add:

```ts
import { describe, it, expect } from "vitest";
// use the existing InMemoryAdminClient + RtDbAdminClient wiring the file already imports

describe("RtDbAdminClient sessions", () => {
  it("lists and revokes sessions", async () => {
    const client = /* the same InMemoryAdminClient/fixture the file's other tests use */;
    const sessions = await client.listSessions({ user: "u1" });
    expect(Array.isArray(sessions)).toBe(true);
    await client.revokeSession("0".repeat(64));
    const res = await client.revokeUserSessions("u1");
    expect(res.ok).toBe(true);
    expect(typeof res.revoked).toBe("number");
  });
});
```

(Match the exact fixture/constructor the file already uses for its token tests — read the file first and reuse that setup rather than inventing a new one.)

- [ ] **Step 2: Add the DTO + methods to `ts-client/src/admin.ts`**

Add the DTO next to `TokenInfo` (~line 38):

```ts
export interface SessionInfo {
  tokenHash: string;
  userId: string;
  email: string | null;
  login: string | null;
  anonymous: boolean;
  createdAt: number;
  expiresAt: number;
}
```

Add the methods to `RtDbAdminClient` (mirror `listTokens` ~line 461 and `revokeToken` ~line 369):

```ts
  async listSessions(filter?: { user?: string; limit?: number }): Promise<SessionInfo[]> {
    const qs = new URLSearchParams();
    if (filter?.user) qs.set("user", filter.user);
    if (filter?.limit !== undefined) qs.set("limit", String(filter.limit));
    const suffix = qs.toString() ? `?${qs}` : "";
    const body = await this.request("GET", `/admin/sessions${suffix}`);
    return (body as { sessions: SessionInfo[] }).sessions;
  }

  async revokeSession(tokenHash: string): Promise<void> {
    await this.request("DELETE", `/admin/sessions/${encodeURIComponent(tokenHash)}`);
  }

  async revokeUserSessions(userId: string): Promise<{ ok: boolean; revoked: number }> {
    return this.request("DELETE", `/admin/sessions?user=${encodeURIComponent(userId)}`);
  }
```

Confirm `request` supports `"DELETE"` with no body (read its signature; if it requires a payload arg, pass `undefined`). The existing `deleteWebhook` (~line 393) is the DELETE precedent — match how it calls `request`.

- [ ] **Step 3: Mirror stubs in `InMemoryAdminClient` (`ts-client/src/in_memory.ts`)**

Add the same three methods returning sensible in-memory values (e.g. `listSessions` returns `[]` or a stored array; `revokeSession`/`revokeUserSessions` return `{ ok: true, revoked: 0 }`). Match the pattern the file's `listTokens`/`revokeToken` stubs use.

- [ ] **Step 4: Re-export the type from `ts-client/src/index.ts`**

Add `SessionInfo` to the `export type { … } from "./admin.js";` block (~line 98).

- [ ] **Step 5: Build + run the test**

Run: `cd ts-client && bun run build && bunx vitest run tests/admin.test.ts`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add ts-client/src/admin.ts ts-client/src/in_memory.ts ts-client/src/index.ts ts-client/tests/admin.test.ts
git commit -m "feat(ts-client): listSessions / revokeSession / revokeUserSessions admin API"
```

---

### Task 6: Dashboard Sessions page

**Files:**
- Modify: `dashboard/src/lib/types.ts` (add `SessionRow`)
- Modify: `dashboard/src/lib/admin.tsx` (add 3 methods to `AdminClient`)
- Create: `dashboard/src/pages/SessionsPage.tsx`
- Create: `dashboard/src/pages/SessionsPage.module.css`
- Modify: `dashboard/src/App.tsx` (route)
- Modify: `dashboard/src/shell/AppShell.tsx` (nav entry)

**Interfaces:**
- Consumes: `AdminClient` (`useAdmin()`), the ts-client types.
- Produces: the `/sessions` dashboard page.

- [ ] **Step 1: Add the type + client methods**

In `dashboard/src/lib/types.ts` next to `TokenRow` (~line 151):

```ts
export interface SessionRow {
  tokenHash: string;
  userId: string;
  email: string | null;
  login: string | null;
  anonymous: boolean;
  createdAt: number;
  expiresAt: number;
}
```

In `dashboard/src/lib/admin.tsx` `AdminClient`, mirror `listTokens` (~line 209) and `revokeToken` (~line 226). Add:

```ts
  listSessions(filter?: { user?: string; limit?: number }) {
    const qs = new URLSearchParams();
    if (filter?.user) qs.set("user", filter.user);
    if (filter?.limit !== undefined) qs.set("limit", String(filter.limit));
    const suffix = qs.toString() ? `?${qs}` : "";
    return this.req<{ sessions: SessionRow[] }>(`/admin/sessions${suffix}`);
  }
  revokeSession(tokenHash: string): Promise<{ ok: boolean }> {
    return this.req("/admin/sessions/" + encodeURIComponent(tokenHash), { method: "DELETE" });
  }
  revokeUserSessions(userId: string): Promise<{ ok: boolean; revoked: number }> {
    return this.req(`/admin/sessions?user=${encodeURIComponent(userId)}`, { method: "DELETE" });
  }
```

(Confirm `req` accepts a `method: "DELETE"` option by reading its signature — `deleteWebhook` is the precedent. Import `SessionRow` from `./types` alongside the existing `TokenRow` import.)

- [ ] **Step 2: Create `dashboard/src/pages/SessionsPage.tsx`**

Clone the structure of `dashboard/src/pages/TokensPage.tsx`: keep `useAdmin()`, the `useState` for rows/loading/error/`pendingId`/`confirmingRevoke`, the `refresh` callback, the table render, and the inline two-step confirm block. Drop the db-selector and the mint form (sessions are server-wide); add an optional user-filter input that feeds `refresh`. Columns: user (`login` ?? `email` ?? `userId`), email, anonymous badge, created, expires, and a revoke button per row. Example skeleton (fill in the JSX by following `TokensPage.tsx` line-for-line):

```tsx
import { useCallback, useEffect, useState } from "react";
import { useAdmin } from "../lib/admin";
import type { SessionRow } from "../lib/types";
import { Button, Placard, Spinner } from "../components/ui";
import styles from "./SessionsPage.module.css";

export default function SessionsPage() {
  const { client } = useAdmin();
  const [userFilter, setUserFilter] = useState("");
  const [sessions, setSessions] = useState<SessionRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [listError, setListError] = useState<string | null>(null);
  const [pendingHash, setPendingHash] = useState<string | null>(null);
  const [confirmingRevoke, setConfirmingRevoke] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  const refresh = useCallback(async () => {
    setLoading(true); setListError(null);
    try {
      const { sessions } = await client.listSessions(
        userFilter.trim() ? { user: userFilter.trim() } : undefined,
      );
      setSessions(sessions);
    } catch (e) {
      setListError(e instanceof Error ? e.message : String(e));
    } finally { setLoading(false); }
  }, [client, userFilter]);

  useEffect(() => { refresh(); /* eslint-disable-next-line react-hooks/exhaustive-deps */ }, []);

  async function revoke(row: SessionRow) {
    setPendingHash(row.tokenHash); setActionError(null);
    try {
      await client.revokeSession(row.tokenHash);
      setConfirmingRevoke(null);
      await refresh();
    } catch (e) {
      setActionError(e instanceof Error ? e.message : String(e));
    } finally { setPendingHash(null); }
  }

  // Render: a filter input + "Refresh" button, then a <table> of sessions.
  // Each row's action cell mirrors TokensPage's confirm pattern:
  //   confirmingRevoke === row.tokenHash ? (Confirm | No) : Revoke button.
  // See TokensPage.tsx lines 181-267 for the exact pattern to copy.
  return ( /* … table + filter, following TokensPage … */ );
}
```

Create `dashboard/src/pages/SessionsPage.module.css` by copying `TokensPage.module.css` and trimming the mint-form rules (keep the table/row/action styles).

- [ ] **Step 3: Register the route + nav**

In `dashboard/src/App.tsx`, import the page (next to the `TokensPage` import, ~line 22) and add the route inside the `AppShell` group (next to the `tokens` route, ~line 60):

```tsx
import SessionsPage from "./pages/SessionsPage";
…
<Route path="sessions" element={<SessionsPage />} />
```

In `dashboard/src/shell/AppShell.tsx`, add a nav entry to the `NAV` array (~line 16, after the Tokens entry):

```tsx
{ to: "/sessions", label: "Sessions" },
```

- [ ] **Step 4: Typecheck + build**

Run: `cd dashboard && bun run build` (the dashboard resolves `@par-rt-db/client` from `ts-client/dist`, so ensure `make ts-client-build` ran first).
Expected: build succeeds, no type errors.

- [ ] **Step 5: Commit**

```bash
git add dashboard/src/lib/types.ts dashboard/src/lib/admin.tsx \
        dashboard/src/pages/SessionsPage.tsx dashboard/src/pages/SessionsPage.module.css \
        dashboard/src/App.tsx dashboard/src/shell/AppShell.tsx
git commit -m "feat(dashboard): Sessions page (list + revoke with confirm guard)"
```

---

### Task 7: Verify, document, and close the card

**Files:**
- Modify: `FEATURE_MATRIX.md` (flip the active-session-management row, note client-mirror status)
- Modify: `CLAUDE.md` only if the Auth invariant wording needs the new tap-site/revoke note (it likely does not — session revoke is not a document write; check §"Op-feed tap" — no change needed)
- Kanban: move the card to done after criteria pass

- [ ] **Step 1: Run the full gate**

Run from repo root: `make checkall` (dev Postgres must be up).
Expected: green (fmt-check + clippy `-D warnings` across all six packages + typecheck + the full test suite including the new `sessions_test` and `ws_test` cases). Fix anything that fails — do not declare done on a red gate.

- [ ] **Step 2: Verify the dashboard page (criterion #3) with agentchrome**

`make ts-client-build`, then start the dashboard dev server (`cd dashboard && bun run dev`, Vite on :8310 proxying :8300) and the server (`cd server && cargo run` with `RTDB_DATABASE_URL`/`RTDB_PORT`/`RTDB_ADMIN_KEY` per the run-server-locally memory). Log in as admin, navigate to the Sessions page, confirm the list renders and the two-step revoke confirm works (per the dashboard-agentchrome-testing memory: navigate in-SPA via `pushState`+`popstate` to avoid the full-reload login drop; use `/auth/me` to restore the session). Take a screenshot for the record. Shut down with `agentchrome connect --disconnect`.

- [ ] **Step 3: Update FEATURE_MATRIX.md**

Find the row for session management / active sessions (or add one near the auth rows). Flip it to ✅ and note "server + ts-client + dashboard" mirror status. If `CLAUDE.md`'s Auth section mentions session revocation, confirm it is still accurate (the new live-revoke behavior is a strengthening, not a contradiction — no edit required unless a sentence now reads false).

- [ ] **Step 4: Check each acceptance criterion and close the card**

Verify one at a time and report per criterion:
1. Admin can list per user + revoke one + revoke-all over HTTP — ✅ (Task 4 tests).
2. Revoked session rejected on the next WS op over an open connection — ✅ (Task 1 + Task 2 tests).
3. `make checkall` green + dashboard Sessions page lists/revokes with confirm guard — ✅ (Steps 1–2).

Only if all three pass: `kanban item done --id 019fe1b86ff87153ba7074c4760cf508`. Commit doc updates:

```bash
git add FEATURE_MATRIX.md
git commit -m "docs: active-session management shipped (FEATURE_MATRIX + criteria)"
```

Then push the branch (the merge/push step is the integration checkpoint — confirm before pushing per the push-after-merge memory).
