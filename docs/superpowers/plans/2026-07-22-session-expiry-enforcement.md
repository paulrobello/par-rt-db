# Live Session-Expiry Enforcement on Open WS Connections — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reject Subscribe/Mutate operations on an open WebSocket connection with `UNAUTHORIZED` once the connection's GitHub OAuth session has expired, without force-closing the connection — closing the documented gap in FEATURE_MATRIX.md rank 8.

**Architecture:** `authorize()` in `server/src/auth/mod.rs` already re-runs on every WS Subscribe and Mutate (`server/src/ws.rs:292` and `:313`), catching machine-token revocation and allowlist removal live — but its `Principal::User` arm never checks session expiry. The fix: add an `expires_at: i64` field to `Principal::User` (populated once at session resolution in `server/src/auth/session.rs`, where `expires_at` is already fetched and in scope) and check it at the top of `authorize()`'s user arm, before the allowlist query. A session's `expires_at` is immutable once minted (no refresh/extend logic exists anywhere — verified via `grep -rn "expires_at" server/src`), so this cached comparison against `now_ms()` is exactly as live as a fresh DB query would be, at zero extra DB round-trips per operation. `server/src/ws.rs` needs **no changes**: on any `authorize()` error it already sends `SubscribeErr`/`MutateErr` carrying the `RtDbError` envelope and keeps the connection open (`handle_text_frame`, `server/src/ws.rs:258-336` — the `Err` arms return `false`, keeping the select loop running). This is a security-relevant auth change: keep it minimal and surgical.

**Tech Stack:** Rust (axum, tokio, sqlx/Postgres), integration tests via `cargo test` against loopback Postgres (`make dev-db-up`, `127.0.0.1:55434`), wiremock for GitHub OAuth mocks, tokio-tungstenite for WS test clients.

## Global Constraints

- Execution happens in an isolated worktree created by the orchestrating session via `superpowers:using-git-worktrees` before Task 1 — no worktree task in this plan.
- Model routing for execution: implementer subagents use `model: sonnet`; reviewer subagents (spec-compliance + code-quality) use `model: fable` (matches `.superpowers/sdd/progress.md` convention).
- Do NOT push. Do NOT touch the kanban board. (User's standing instructions for this work.)
- Server-only change. No client/TS changes: `Principal` is never serialized to the wire, so there is no protocol change.
- Scope guard — do NOT touch: machine-token handling, allowlist/revocation logic, session *deletion*/logout handling, `server/src/ws.rs`, `server/src/auth/session.rs`'s existing expired-row early-return/lazy-delete branch, or `CLAUDE.md` (its Auth-section "known deferred gap" sentence becomes stale, but updating it is explicitly out of scope; the orchestrator flags it as a follow-up).
- Documentation changes are scoped to exactly two FEATURE_MATRIX.md rows (section 1 line 40, section 2 line 60).
- No `unwrap()`/`expect()` outside `#[cfg(test)]`; zero clippy warnings under `-D warnings`; every error is the `RtDbError` envelope.
- `make dev-db-up` (repo root) must be running before any test run. `make checkall` (fmt-check + clippy + typecheck + tests, both packages) is the gate — it must be fully green before each commit.
- HARD REQUIREMENT: the core auth commit message must clearly flag this as a security-relevant auth change for review. Git history has no prior security-prefix precedent (verified: `git log --oneline | grep -i secur` is empty), so use a `[security]` marker suffix on the conventional message as specified in Task 1 Step 9 — do not silently drop it.

---

### Task 1: Session-expiry check in `authorize()` (core auth change + direct unit-level test)

**Files:**
- Modify: `server/src/auth/mod.rs` (import at line 7, `Principal` enum lines 14-24, `authorize` doc comment + `Principal::User` arm lines 49-100, unit test literal at lines 134-145)
- Modify: `server/src/auth/session.rs:64-68` (`Principal::User` construction in `resolve_session`)
- Test: `server/tests/http_api_test.rs` (imports at lines 5-9; existing test `authorize_user_branch_matches_allowlist_case_insensitively` at lines 293-328; new test appended directly after it)

**Interfaces:**
- Consumes: existing `pub fn now_ms() -> i64` (`server/src/db.rs:213`, milliseconds since epoch); existing `RtDbError::unauthorized(msg)` constructor (`server/src/error.rs`, wire code `UNAUTHORIZED`); existing `authorize(pool: &PgPool, principal: &Principal, db: &str) -> Result<(), RtDbError>`.
- Produces: `Principal::User` gains a required field `expires_at: i64` (epoch ms; every construction site must supply it — use `i64::MAX` in tests that are not about expiry). `authorize()`'s signature is unchanged; new behavior: when `*expires_at < now_ms()`, it returns `Err(RtDbError::unauthorized("session expired"))` *before* the allowlist query. Task 2's WS test and Task 3's matrix text rely on exactly this mechanism and message.

- [ ] **Step 1: Write the failing test in `server/tests/http_api_test.rs`**

Add the `db` import. The current import block at lines 5-8 is:

```rust
use common::{admin_post, fresh_db, spawn_app, test_state};
use rtdb_server::auth::{Principal, authorize};
use rtdb_server::error::ErrorCode;
use rtdb_server::protocol::ServerMessage;
```

Insert `use rtdb_server::db;` between the `auth` and `error` lines (alphabetical, matching existing order):

```rust
use rtdb_server::auth::{Principal, authorize};
use rtdb_server::db;
use rtdb_server::error::ErrorCode;
```

Then append this test immediately after the closing `}` of `authorize_user_branch_matches_allowlist_case_insensitively` (line 328). The email is *allowlisted* on purpose: expiry must be the only possible reason for rejection, so the assertion cannot pass for the wrong reason (a non-allowlisted email would fail with `FORBIDDEN` regardless of expiry):

```rust
// (j2) authorize's User branch rejects a session whose expiry has passed,
// even for an allowlisted email — the email is added to the allowlist first
// so expiry is the only possible reason for rejection.
#[tokio::test]
async fn authorize_user_branch_rejects_expired_session() -> anyhow::Result<()> {
    let state = test_state().await;
    let addr = spawn_app(state.clone()).await;
    let name = fresh_db(&state).await;

    let resp = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": name, "action": "add", "email": "user@example.com"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let expired = Principal::User {
        user_id: "u1".to_string(),
        email: "user@example.com".to_string(),
        name: None,
        expires_at: db::now_ms() - 1,
    };
    let err = authorize(&state.pool, &expired, &name)
        .await
        .expect_err("expired session should be unauthorized even when allowlisted");
    assert_eq!(err.code, ErrorCode::Unauthorized);

    Ok(())
}
```

- [ ] **Step 2: Run the test to verify it fails for the right reason**

Run (from repo root): `cd server && cargo test --test http_api_test authorize_user_branch_rejects_expired_session`

Expected: **compile error** — `error[E0559]: variant `Principal::User` has no field named `expires_at``. That is the correct red for this change: the field does not exist yet. (No DB needed for this step since it fails at compile time.)

- [ ] **Step 3: Add the `expires_at` field and the expiry check in `server/src/auth/mod.rs`**

Change the import at line 7 from `use crate::db::sha256_hex;` to:

```rust
use crate::db::{now_ms, sha256_hex};
```

Change the `Principal::User` variant (lines 14-24) to add the field after `name`:

```rust
#[derive(Debug, Clone)]
pub enum Principal {
    Machine {
        db: String,
        token_id: String,
    },
    User {
        user_id: String,
        email: String,
        name: Option<String>,
        expires_at: i64,
    },
}
```

Replace `authorize`'s doc comment (the `///` block directly above `pub async fn authorize` at line 58) with this — it adds one precise sentence about the expiry check in the existing doc style:

```rust
/// Authorization for a database: a machine token must match `db` exactly and
/// still be un-revoked — checked live against `rtdb_auth.machine_tokens` on
/// every call, so a token revoked mid-session is denied on its very next
/// operation rather than only at the next fresh connection; a user must hold
/// an unexpired session and be present in `rtdb_auth.allowlist` for `db`.
/// Session expiry is checked against `expires_at`, captured once at session
/// resolution — a session's expiry is immutable once minted, so this cached
/// comparison is exactly as live as a fresh DB query, without costing one
/// per operation. Allowlist emails are stored lowercase (see
/// `admin::allowlist_write`), so the principal's email is lowercased here
/// before the lookup — the sole choke point for case-insensitive comparison.
```

Replace the `Principal::User` arm inside `authorize` (currently `Principal::User { email, .. } => { ... }`, lines 79-99) with:

```rust
        Principal::User {
            email, expires_at, ..
        } => {
            if *expires_at < now_ms() {
                return Err(RtDbError::unauthorized("session expired"));
            }

            let row: Option<(String,)> = sqlx::query_as(
                "SELECT email FROM rtdb_auth.allowlist WHERE db_name = $1 AND email = $2",
            )
            .bind(db)
            .bind(email.to_lowercase())
            .fetch_optional(pool)
            .await?;

            if row.is_some() {
                Ok(())
            } else {
                Err(RtDbError::forbidden(
                    "user is not allowlisted for this database",
                ))
            }
        }
```

The allowlist query and `Machine` arm are byte-for-byte unchanged. Do not touch `resolve_bearer` or `authed_user` logic.

- [ ] **Step 4: Populate the field in `server/src/auth/session.rs`**

In `resolve_session`, `expires_at` is already an `i64` local in scope (destructured from the query row at line 52; the `if expires_at < now_ms()` early-return branch above it stays untouched). Change only the final construction (lines 64-68) to:

```rust
    Ok(Some(Principal::User {
        user_id,
        email,
        name: None,
        expires_at,
    }))
```

- [ ] **Step 5: Fix the now-non-compiling `Principal::User` literal in `server/src/auth/mod.rs`'s unit test**

In `authed_user_for_user_carries_email_and_name` (line 134), the literal gains the required field. `i64::MAX` reads unambiguously as "not expired" and avoids calling `now_ms()` in a test that is not about expiry:

```rust
    #[test]
    fn authed_user_for_user_carries_email_and_name() {
        let principal = Principal::User {
            user_id: "u".to_string(),
            email: "a@b.com".to_string(),
            name: Some("Alice".to_string()),
            expires_at: i64::MAX,
        };
        let user = authed_user(&principal);
        assert_eq!(user.kind, "user");
        assert_eq!(user.email, Some("a@b.com".to_string()));
        assert_eq!(user.name, Some("Alice".to_string()));
    }
```

(`authed_user_for_machine_has_no_email_or_name` uses the `Machine` variant and needs no change.)

- [ ] **Step 6: Fix the two now-non-compiling literals in the existing `http_api_test.rs` test**

In `authorize_user_branch_matches_allowlist_case_insensitively` (lines 293-328), add `expires_at: i64::MAX,` to both literals:

```rust
    let allowed = Principal::User {
        user_id: "u1".to_string(),
        email: "User@Example.com".to_string(),
        name: None,
        expires_at: i64::MAX,
    };
```

```rust
    let not_allowed = Principal::User {
        user_id: "u2".to_string(),
        email: "someone-else@example.com".to_string(),
        name: None,
        expires_at: i64::MAX,
    };
```

Everything else in that test is unchanged.

- [ ] **Step 7: Run the targeted tests to verify green**

Run (repo root): `make dev-db-up`
Expected: the loopback Postgres container is up (idempotent if already running).

Run: `cd server && cargo test --test http_api_test authorize_user_branch`
Expected: `test authorize_user_branch_matches_allowlist_case_insensitively ... ok`, `test authorize_user_branch_rejects_expired_session ... ok`, `test result: ok. 2 passed`.

Run: `cd server && cargo test --lib auth::`
Expected: both `authed_user_*` unit tests pass, `test result: ok. 2 passed`.

- [ ] **Step 8: Run the full gate**

Run (repo root): `make fmt` then `make checkall`
Expected: fmt-check, clippy (`-D warnings`), typecheck, and the full server + client test suites all pass. Fix anything red before committing (do not commit on red).

- [ ] **Step 9: Commit (security-flagged — hard requirement)**

```bash
git add server/src/auth/mod.rs server/src/auth/session.rs server/tests/http_api_test.rs
git commit -m "fix(auth): enforce session expiry on open WS connections [security]

Principal::User now carries the session's immutable expires_at, and
authorize() rejects expired sessions with UNAUTHORIZED before the
allowlist query — on every Subscribe/Mutate over an already-open
connection, closing the FEATURE_MATRIX rank-8 gap. Security-relevant
auth change; review the authorize() path."
```

---

### Task 2: WS integration test — mid-connection expiry denies ops, connection stays usable

**Files:**
- Test: `server/tests/oauth_test.rs` (new test inserted immediately after `allowlist_removal_mid_session_fails_mutate_without_closing_connection`, i.e. after the closing `}` at line 348, before the `// (d)` test)

**Interfaces:**
- Consumes: Task 1's behavior — `authorize()` returns `RtDbError::unauthorized("session expired")` (wire code `"UNAUTHORIZED"`) when the principal's `expires_at` has passed; `server/src/ws.rs`'s existing `Err` handling, which sends `subscribeErr`/`mutateErr` and keeps the connection open. Existing test helpers at `server/tests/oauth_test.rs:1-204`, reused as-is with **no changes and no new helpers**: `oauth_state`, `mount_github_mocks`, `verified_primary_email`, `login_flow`, `ws_connect`, `ws_auth`, `ws_send_json`, `ws_recv_json`, `insert_work_item_txn`, plus `admin_post`/`fresh_db` from `common` and `db::now_ms`/`db::sha256_hex` via the file's existing `use rtdb_server::{AppState, db};` import. Task 1 required no structural change to any shared helper, so none is needed here — do not refactor working test helpers.
- Produces: test name `session_expiry_mid_connection_denies_operations_but_keeps_connection_usable`, cited by Task 3's FEATURE_MATRIX row text.

- [ ] **Step 1: Write the integration test**

Insert after line 348 (blank line between tests, matching the file's style). It mirrors the sibling revocation test's structure exactly, swapping "allowlist removal" for "session expiry via direct UPDATE". The `for _ in 0..2` loop around the first mutate is required because the insert also triggers a `queryUpdate` for `q1`, and its ordering relative to `mutateOk` is not guaranteed:

```rust
// (c3) rank 8: live session-expiry enforcement on every WS op. Subscribe and
// mutate succeed while the session is valid; after the session's expires_at
// passes (simulated by backdating the row — expires_at is immutable once
// minted, so this is equivalent to real time passing), the next subscribe
// AND the next mutate on the SAME open connection get subscribeErr/mutateErr
// UNAUTHORIZED (not a close) and the connection stays usable (a following
// ping still pongs), so a client can retry with a fresh token.
#[tokio::test]
async fn session_expiry_mid_connection_denies_operations_but_keeps_connection_usable()
-> anyhow::Result<()> {
    let mock = MockServer::start().await;
    mount_github_mocks(&mock, verified_primary_email("user@example.com")).await;
    let (state, addr) = oauth_state(&mock).await;
    let db_name = fresh_db(&state).await;
    let token = login_flow(addr, "http://localhost:5173").await;

    let add_resp = admin_post(
        addr,
        "/admin/allowlist",
        json!({"db": db_name, "action": "add", "email": "user@example.com"}),
    )
    .await;
    assert_eq!(add_resp.status(), reqwest::StatusCode::OK);

    let mut ws = ws_connect(addr).await;
    let auth_msg = ws_auth(&mut ws, &token, &db_name).await;
    assert_eq!(auth_msg["type"], json!("authOk"));

    // While the session is valid: subscribe and mutate both succeed.
    ws_send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q1", "query": {"table": "workItems"}}),
    )
    .await;
    let sub_msg = ws_recv_json(&mut ws).await;
    assert_eq!(sub_msg["type"], json!("queryUpdate"));

    ws_send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m1", "txn": insert_work_item_txn()}),
    )
    .await;
    let mut saw_mutate_ok = false;
    for _ in 0..2 {
        let msg = ws_recv_json(&mut ws).await;
        if msg["type"] == json!("mutateOk") {
            assert_eq!(msg["mutId"], json!("m1"));
            saw_mutate_ok = true;
        }
    }
    assert!(saw_mutate_ok, "expected mutateOk while session is valid");

    // The session expires while the connection stays open.
    sqlx::query("UPDATE rtdb_auth.sessions SET expires_at = $1 WHERE token_hash = $2")
        .bind(db::now_ms() - 1_000)
        .bind(db::sha256_hex(&token))
        .execute(&state.pool)
        .await?;

    // The next subscribe on the same open connection is rejected.
    ws_send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "q2", "query": {"table": "workItems"}}),
    )
    .await;
    let sub_err = ws_recv_json(&mut ws).await;
    assert_eq!(sub_err["type"], json!("subscribeErr"));
    assert_eq!(sub_err["queryId"], json!("q2"));
    assert_eq!(sub_err["error"]["code"], json!("UNAUTHORIZED"));

    // And so is the next mutate.
    ws_send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "m2", "txn": insert_work_item_txn()}),
    )
    .await;
    let mut_err = ws_recv_json(&mut ws).await;
    assert_eq!(mut_err["type"], json!("mutateErr"));
    assert_eq!(mut_err["mutId"], json!("m2"));
    assert_eq!(mut_err["error"]["code"], json!("UNAUTHORIZED"));

    // Connection stays open (not closed by the expiry failure): a subsequent
    // ping still round-trips.
    ws_send_json(&mut ws, json!({"type": "ping"})).await;
    let pong = ws_recv_json(&mut ws).await;
    assert_eq!(pong["type"], json!("pong"));

    Ok(())
}
```

(Do not add a duplicate of the existing `expired_session_returns_unauthorized` test at line 417 — that covers the different, already-handled expired-*before*-connect case via `/auth/me`.)

- [ ] **Step 2: Run the new test — expect PASS, and understand why there is no red step**

Run (repo root): `make dev-db-up`, then: `cd server && cargo test --test oauth_test session_expiry_mid_connection`

Expected: `test session_expiry_mid_connection_denies_operations_but_keeps_connection_usable ... ok`, `test result: ok. 1 passed`.

Honest TDD note: this test covers behavior Task 1 already implemented, so no natural failing state exists and none should be fabricated. Coverage is proven by the discriminating assertions after the UPDATE: before Task 1's change, those exact frames produced `queryUpdate` and `mutateOk` (the known gap), so the `subscribeErr`/`mutateErr` `UNAUTHORIZED` assertions can only pass with the new `authorize()` branch in place. If this test FAILS, stop and debug Task 1's change — do not weaken assertions to make it pass.

- [ ] **Step 3: Run the full existing oauth suite to confirm no regressions**

Run: `cd server && cargo test --test oauth_test`
Expected: all tests pass (previously-existing tests plus the new one), `test result: ok.` with 0 failed.

- [ ] **Step 4: Run the full gate**

Run (repo root): `make fmt` then `make checkall`
Expected: fully green. Fix anything red before committing.

- [ ] **Step 5: Commit**

```bash
git add server/tests/oauth_test.rs
git commit -m "test(server): cover mid-connection session expiry over an open WS [security]

Proves a session expiring after connect is rejected UNAUTHORIZED on the
next subscribe AND mutate, while the connection itself stays usable for
retry — matching the existing revocation behavior."
```

---

### Task 3: FEATURE_MATRIX.md updates (both rows) + final gate

**Files:**
- Modify: `FEATURE_MATRIX.md:40` (section 1, "Live permission revocation" row)
- Modify: `FEATURE_MATRIX.md:60` (section 2, gap-matrix rank 8 row)

**Interfaces:**
- Consumes: Task 1's mechanism (`expires_at` on `Principal::User`, checked in `authorize` before the allowlist query, immutable once minted so no extra round-trip) and the test names from Task 1 (`authorize_user_branch_rejects_expired_session` in `http_api_test.rs`) and Task 2 (`session_expiry_mid_connection_denies_operations_but_keeps_connection_usable` in `oauth_test.rs`).
- Produces: nothing downstream — final task.

- [ ] **Step 1: Update section 1, line 40**

Current line 40:

```
| Live permission revocation | ✅ | 🟡 | `authorize` re-runs on every Subscribe/Mutate; session *expiry* mid-connection is a known gap (row 8 below) |
```

Replace with (parity reached; "known gap" framing dropped, cross-reference kept):

```
| Live permission revocation | ✅ | ✅ | `authorize` re-runs on every Subscribe/Mutate; machine-token revocation, allowlist removal, and session expiry are all checked live per op (row 8 below) |
```

- [ ] **Step 2: Update section 2, line 60 (rank 8)**

Current line 60:

```
| 8 | 2 | **Live session-expiry enforcement** on open WS | ✅ | 🟡 | Med | S | Already on the kanban backlog. `authorize` re-runs per op but doesn't check session expiry; add the expiry check to that path. |
```

Replace with (matching the "Implemented — mechanism + test files" style of rows 1, 2, 3, 6, 7):

```
| 8 | 2 | **Live session-expiry enforcement** on open WS | ✅ | ✅ | Med | S | Implemented — `Principal::User` carries the session's `expires_at` (captured once at session resolution in `session.rs`), and `authorize` checks it before the allowlist query on every Subscribe/Mutate, rejecting with `UNAUTHORIZED` while leaving the connection open for retry with a fresh token; no extra DB round-trip because a session's expiry is immutable once minted. Integration coverage in `oauth_test.rs` (mid-connection expiry over an open WS denies subscribe and mutate but keeps the connection usable) and `http_api_test.rs` (direct `authorize` rejection of an expired, allowlisted principal). |
```

Change nothing else in FEATURE_MATRIX.md, and do not edit `CLAUDE.md` (out of scope — see Global Constraints).

- [ ] **Step 3: Run the full gate one final time**

Run (repo root): `make checkall`
Expected: fully green (fmt-check, clippy `-D warnings`, typecheck, all server + client tests). This is the user's explicit acceptance gate — fix anything red before committing.

- [ ] **Step 4: Commit**

```bash
git add FEATURE_MATRIX.md
git commit -m "docs: mark live session-expiry enforcement FEATURE_MATRIX rank 8 implemented"
```
