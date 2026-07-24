# Realtime Dashboard — Phase 1: Auth Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let the dashboard authenticate as a real human (GitHub/Google OAuth session on a server-wide admin allowlist) in addition to the raw admin key, so the admin key never has to live in browser JS.

**Architecture:** Add an `rtdb_auth.admins` table (email + optional GitHub id), a startup seed from `RTDB_ADMIN_EMAILS`, an `auth::is_admin` membership check, and an `AdminPrincipal` returned by a rewritten async `require_admin` that accepts either the admin key (constant-time) or an allowlisted OAuth session. Add admins CRUD endpoints. The existing nine admin handlers switch to the new gate; their behavior is unchanged for the admin-key path.

**Tech Stack:** Rust, axum, sqlx, Postgres 17, tokio, `subtle` (constant-time compare).

## Global Constraints

(copy verbatim from `CLAUDE.md`; apply to every task)

- Double-quote every SQL identifier; bind every value via `$n`; never interpolate an unvalidated value. Use `fetch_optional` for any lookup that can legitimately miss.
- Every failure is an `RtDbError` `{code, message}`; client-facing 500s carry a **generic** message — never stringify a sqlx/serde error into the body.
- No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings under `-D warnings`.
- `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests) is the definition of done; `make dev-db-up` is **required before any test run** (integration tests hit a real Postgres at `127.0.0.1:55434`).
- Tests share one Postgres and isolate via uniquely-named databases; never drop a database or schema you didn't create. Bootstrap DDL is additive (`CREATE TABLE IF NOT EXISTS`) and already serialized by `pg_advisory_xact_lock` in `db::bootstrap`.
- Match the existing wire-casing exactly where present (e.g. `tokenId`); new mixed-case request fields use `#[serde(rename = "githubId")]`.

**Test-file convention for this plan:** `server/tests/dashboard_test.rs` accumulates tests across all phases. It has exactly one top-level line — `mod common;` — and **no `use` block**. Every reference is fully qualified (`common::test_state()`, `rtdb_server::auth::seed_admin_emails`, `rtdb_server::db::now_ms`, `std::sync::Arc<rtdb_server::AppState>`, etc.). This keeps the file compiling at every task boundary as later tasks append functions. Local test helpers (`user_session`, `resolve`, `bearer_get`, `admin_delete`) are defined as plain `fn`s where first used and reused by later tasks.

## File Structure

- `server/src/db.rs` — add the `rtdb_auth.admins` table to `bootstrap_ddl` (additive).
- `server/src/auth/mod.rs` — add `is_admin(pool, &Principal) -> bool` and `seed_admin_emails(pool, &[String])`.
- `server/src/admin.rs` — add `AdminPrincipal` enum; rewrite `require_admin` to async, returning `AdminPrincipal`; update all nine handler call sites; add `list_admins` / `add_admin` / `remove_admin` handlers + route.
- `server/src/main.rs` — parse `RTDB_ADMIN_EMAILS` and call `auth::seed_admin_emails` after `db::bootstrap`.
- `server/tests/dashboard_test.rs` — **create**; Phase 1 auth integration tests (accumulates dashboard tests across later phases).

---

## Task 1: `rtdb_auth.admins` table + `seed_admin_emails` + startup wiring

**Files:**
- Modify: `server/src/db.rs` (inside `bootstrap_ddl`, after the `machine_tokens` `CREATE TABLE` block).
- Modify: `server/src/auth/mod.rs` (new `seed_admin_emails`).
- Modify: `server/src/main.rs` (call the seed after bootstrap).
- Test: `server/tests/dashboard_test.rs` (create with `mod common;` + this test).

**Interfaces:**
- Produces: `pub async fn seed_admin_emails(pool: &PgPool, emails: &[String]) -> Result<(), RtDbError>` in `auth`; the `rtdb_auth.admins(email text PK, github_id bigint, added_at bigint)` table available to every later task.

- [ ] **Step 1: Write the failing test**

Create `server/tests/dashboard_test.rs` with exactly this content:

```rust
mod common;

// Seeding lowercases emails, is idempotent, and stores them with a NULL github_id.
#[tokio::test]
async fn seed_admin_emails_lowercases_and_is_idempotent() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let pool = state.pool.clone();

    rtdb_server::auth::seed_admin_emails(
        &pool,
        &["Foo@Bar.com".to_string(), "  ".to_string(), "a@b.com".to_string()],
    )
    .await?;
    // Re-seed the same address: ON CONFLICT DO NOTHING keeps it a single row.
    rtdb_server::auth::seed_admin_emails(&pool, &["foo@bar.com".to_string()]).await?;

    let rows: Vec<(String, Option<i64>)> = sqlx::query_as(
        "SELECT email, github_id FROM rtdb_auth.admins ORDER BY email",
    )
    .fetch_all(&pool)
    .await?;
    assert_eq!(
        rows,
        vec![
            ("a@b.com".to_string(), None),
            ("foo@bar.com".to_string(), None),
        ]
    );
    Ok(())
}
```

- [ ] **Step 2: Run test to verify it fails**

```
cd server && cargo test --test dashboard_test seed_admin_emails_lowercases_and_is_idempotent
```
Expected: **compile error** — `seed_admin_emails` is not exported (function does not exist yet).

- [ ] **Step 3: Add the table**

In `server/src/db.rs`, inside `bootstrap_ddl`, immediately after the `machine_tokens` `CREATE TABLE IF NOT EXISTS ... .execute(&mut *conn).await?;` block (the one ending just before `CREATE SCHEMA IF NOT EXISTS rtdb`), insert:

```rust
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS rtdb_auth.admins (
            email text PRIMARY KEY,
            github_id bigint,
            added_at bigint NOT NULL
        )",
    )
    .execute(&mut *conn)
    .await?;
```

- [ ] **Step 4: Add `seed_admin_emails`**

In `server/src/auth/mod.rs`, after the `owner_of` function (before `authed_user`), add:

```rust
/// Idempotently seeds `RTDB_ADMIN_EMAILS` into `rtdb_auth.admins` at startup
/// (see `main.rs`). Emails are lowercased and trimmed; blanks are skipped.
/// Seeded rows carry a NULL `github_id` and are matched by email at login.
pub async fn seed_admin_emails(pool: &PgPool, emails: &[String]) -> Result<(), RtDbError> {
    let now = now_ms();
    for raw in emails {
        let email = raw.trim().to_lowercase();
        if email.is_empty() {
            continue;
        }
        sqlx::query(
            "INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, NULL, $2) \
             ON CONFLICT (email) DO NOTHING",
        )
        .bind(&email)
        .bind(now)
        .execute(pool)
        .await?;
    }
    Ok(())
}
```

- [ ] **Step 5: Run test to verify it passes**

```
cd server && cargo test --test dashboard_test seed_admin_emails_lowercases_and_is_idempotent
```
Expected: PASS.

- [ ] **Step 6: Wire startup seeding**

In `server/src/main.rs`, change the `use` line to include `auth`:

```rust
use rtdb_server::{AppState, auth, build_router, config::Config, db};
```

Immediately after the `db::bootstrap(&pool).await ...` block (after its closing `});`, before `let port = config.port;`), insert:

```rust
    let admin_emails: Vec<String> = match std::env::var("RTDB_ADMIN_EMAILS") {
        Ok(v) if !v.is_empty() => v
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        _ => Vec::new(),
    };
    auth::seed_admin_emails(&pool, &admin_emails).await.unwrap_or_else(|err| {
        tracing::warn!(error = %err, "failed to seed admin emails");
    });
```

- [ ] **Step 7: Run the full gate**

```
cd server && cargo fmt && make checkall
```
Expected: PASS (fmt-check + clippy + typecheck + tests). `make dev-db-up` must be running first.

- [ ] **Step 8: Commit**

```bash
git add server/src/db.rs server/src/auth/mod.rs server/src/main.rs server/tests/dashboard_test.rs
git commit -m "feat(server): add rtdb_auth.admins table + RTDB_ADMIN_EMAILS seed (#18)"
```

---

## Task 2: `AdminPrincipal` + `is_admin` + async `require_admin` (update all handlers)

**Files:**
- Modify: `server/src/auth/mod.rs` (new `is_admin`).
- Modify: `server/src/admin.rs` (new `AdminPrincipal`; rewrite `require_admin`; update nine call sites).
- Test: `server/tests/dashboard_test.rs` (append helpers + the `is_admin` test).

**Interfaces:**
- Consumes: `rtdb_auth.admins` (Task 1), `auth::resolve_bearer`, `auth::Principal`.
- Produces: `pub async fn is_admin(pool: &PgPool, principal: &Principal) -> bool` (in `auth`, reused by the dashboard WS bypass in Phase 5); `AdminPrincipal` and `pub(crate) async fn require_admin(&AppState, &HeaderMap) -> Result<AdminPrincipal, RtDbError>` (in `admin`, reused by every later admin route).

- [ ] **Step 1: Write the failing tests**

Append to `server/tests/dashboard_test.rs` (no new `use` lines — fully qualified):

```rust

// Create an OAuth user + session and return the plaintext session bearer.
async fn user_session(
    state: &std::sync::Arc<rtdb_server::AppState>,
    email: &str,
    github_id: Option<i64>,
) -> String {
    let pool = &state.pool;
    let user_id = format!("u{}", uuid::Uuid::now_v7().simple());
    sqlx::query(
        "INSERT INTO rtdb_auth.users (id, github_id, login, email, created_at) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(&user_id)
    .bind(github_id)
    .bind(email)
    .bind(email)
    .bind(rtdb_server::db::now_ms())
    .execute(pool)
    .await
    .unwrap();
    rtdb_server::auth::session::create_session(pool, &user_id, 30)
        .await
        .unwrap()
}

async fn resolve_principal(
    state: &std::sync::Arc<rtdb_server::AppState>,
    token: &str,
) -> rtdb_server::auth::Principal {
    rtdb_server::auth::resolve_bearer(&state.pool, token)
        .await
        .unwrap()
}

// is_admin is false with no admins; true when the user's email is allowlisted;
// true when the user's github_id is allowlisted (independently of email).
#[tokio::test]
async fn is_admin_matches_email_or_github_id() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let pool = state.pool.clone();

    let email_tok = user_session(&state, "owner@example.com", None).await;
    let gh_tok = user_session(&state, "ghonly@example.com", Some(7)).await;
    let stranger_tok = user_session(&state, "stranger@example.com", None).await;

    // No admins yet → nobody is admin.
    assert!(!rtdb_server::auth::is_admin(&pool, &resolve_principal(&state, &email_tok).await).await);

    // Add owner by email.
    sqlx::query("INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, NULL, $2)")
        .bind("owner@example.com")
        .bind(rtdb_server::db::now_ms())
        .execute(&pool)
        .await?;
    assert!(rtdb_server::auth::is_admin(&pool, &resolve_principal(&state, &email_tok).await).await);

    // gh user is not yet admin (no email match, github 7 not listed).
    assert!(!rtdb_server::auth::is_admin(&pool, &resolve_principal(&state, &gh_tok).await).await);

    // Add an admin row keyed on github_id 7 under an unrelated email → gh user matches.
    sqlx::query("INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, $2, $3)")
        .bind("someone-else@example.com")
        .bind(7_i64)
        .bind(rtdb_server::db::now_ms())
        .execute(&pool)
        .await?;
    assert!(rtdb_server::auth::is_admin(&pool, &resolve_principal(&state, &gh_tok).await).await);

    // A user matched by neither is not admin.
    assert!(!rtdb_server::auth::is_admin(&pool, &resolve_principal(&state, &stranger_tok).await).await);
    Ok(())
}
```

- [ ] **Step 2: Run tests to verify they fail**

```
cd server && cargo test --test dashboard_test is_admin_matches_email_or_github_id
```
Expected: **compile error** — `rtdb_server::auth::is_admin` does not exist.

- [ ] **Step 3: Add `is_admin`**

In `server/src/auth/mod.rs`, immediately after `seed_admin_emails` (added in Task 1), add:

```rust
/// Whether `principal` is a server-wide dashboard admin — present in
/// `rtdb_auth.admins` by email (lowercased) or GitHub id. Machine principals are
/// never admin. Used by the admin gate (`admin::require_admin`) and the dashboard
/// WS bypass (Phase 5). Returns `false` on DB error rather than propagating, so a
/// transient failure degrades to "not admin" (deny) without an error envelope.
pub async fn is_admin(pool: &PgPool, principal: &Principal) -> bool {
    let Principal::User { email, github_id, .. } = principal else {
        return false;
    };
    let email = email.to_lowercase();
    let Ok(exists) = sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM rtdb_auth.admins WHERE email = $1 OR github_id = $2)",
    )
    .bind(&email)
    .bind(*github_id)
    .fetch_one(pool)
    .await
    else {
        return false;
    };
    exists
}
```

- [ ] **Step 4: Run the is_admin test to verify it passes**

```
cd server && cargo test --test dashboard_test is_admin_matches_email_or_github_id
```
Expected: PASS.

- [ ] **Step 5: Add `AdminPrincipal` and rewrite `require_admin`**

In `server/src/admin.rs`, replace the existing `require_admin` function (the `fn require_admin(headers: &HeaderMap, expected: &str) -> Result<(), RtDbError> { ... }` block near the top) with:

```rust
/// Who an admin request was made as: the raw admin key (CLI/automation) or an
/// OAuth user on the server-wide admin allowlist (browser dashboard).
pub(crate) enum AdminPrincipal {
    Key,
    User(auth::Principal),
}

fn bearer_value(headers: &HeaderMap) -> Result<&str, RtDbError> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| RtDbError::unauthorized("missing admin bearer token"))
}

/// Admin gate. Tries the raw admin key first (constant-time compare), then a
/// resolved session/machine principal — admitting only OAuth users present in
/// `rtdb_auth.admins`. Machine tokens and non-allowlisted/expired users are
/// rejected. The admin-key path returns before any DB lookup, so machine/CLI
/// admin calls stay cheap; the session path costs one `resolve_bearer` + one
/// `is_admin` query per request (acceptable for low-frequency dashboard traffic).
pub(crate) async fn require_admin(
    state: &AppState,
    headers: &HeaderMap,
) -> Result<AdminPrincipal, RtDbError> {
    let provided = bearer_value(headers)?;
    if bool::from(provided.as_bytes().ct_eq(state.config.admin_key.as_bytes())) {
        return Ok(AdminPrincipal::Key);
    }
    let principal = match auth::resolve_bearer(&state.pool, provided).await {
        Ok(principal) => principal,
        Err(_) => return Err(RtDbError::unauthorized("invalid admin credential")),
    };
    if auth::is_admin(&state.pool, &principal).await {
        Ok(AdminPrincipal::User(principal))
    } else {
        Err(RtDbError::forbidden("not a dashboard admin"))
    }
}
```

`use subtle::ConstantTimeEq;` is already imported at the top of `admin.rs` (the `ct_eq` call relies on it). `AppState`, `auth`, and `RtDbError` are already imported there.

- [ ] **Step 6: Update all nine handler call sites**

Every existing admin handler currently calls `require_admin(&headers, &state.config.admin_key)?;`. Replace each occurrence with `require_admin(&state, &headers).await?;`. The nine handlers (all in `server/src/admin.rs`):

1. `create_db`
2. `push_schema`
3. `list_dbs`
4. `mint_token`
5. `revoke_token`
6. `allowlist_write`
7. `allowlist_list`
8. `export_db`
9. `import_db`

Before (identical at every site):

```rust
    require_admin(&headers, &state.config.admin_key)?;
```

After:

```rust
    require_admin(&state, &headers).await?;
```

- [ ] **Step 7: Add a test that the admin-key path still authorizes**

Append to `server/tests/dashboard_test.rs`:

```rust

// The admin-key path still authorizes after the require_admin rewrite.
#[tokio::test]
async fn admin_key_path_still_authorizes() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state).await;

    // /admin/admins is added in Task 3; use an existing route to prove the key path.
    let resp = common::admin_get(addr, "/admin/dbs").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    Ok(())
}
```

- [ ] **Step 8: Run the full gate**

```
cd server && cargo fmt && make checkall
```
Expected: PASS. All pre-existing admin tests must still pass — the admin-key behavior is unchanged.

- [ ] **Step 9: Commit**

```bash
git add server/src/auth/mod.rs server/src/admin.rs server/tests/dashboard_test.rs
git commit -m "feat(server): AdminPrincipal + is_admin + session admin gate (#18)"
```

---

## Task 3: Admins CRUD endpoints

**Files:**
- Modify: `server/src/admin.rs` (three handlers + route).
- Test: `server/tests/dashboard_test.rs` (append).

**Interfaces:**
- Consumes: `require_admin` (Task 2), `crate::db::now_ms`.
- Produces: `GET /admin/admins`, `POST /admin/admins`, `DELETE /admin/admins`.

- [ ] **Step 1: Write the failing tests**

Append to `server/tests/dashboard_test.rs`:

```rust

// DELETE with an admin-key bearer + JSON body.
async fn admin_delete(
    addr: std::net::SocketAddr,
    path: &str,
    body: serde_json::Value,
) -> reqwest::Response {
    reqwest::Client::new()
        .delete(format!("http://{addr}{path}"))
        .header("Authorization", "Bearer test-admin-key")
        .json(&body)
        .send()
        .await
        .unwrap()
}

#[tokio::test]
async fn admins_crud_round_trip() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state).await;

    // Add by email.
    let resp = common::admin_post(
        addr,
        "/admin/admins",
        serde_json::json!({"email": "Crew@Example.com"}),
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Listed, lowercased. (Other parallel tests may add rows; assert membership.)
    let body: serde_json::Value = common::admin_get(addr, "/admin/admins").await.json().await?;
    let emails: Vec<String> = body["admins"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|m| m["email"].as_str().map(String::from))
        .collect();
    assert!(emails.iter().any(|e| e == "crew@example.com"));

    // Remove.
    let resp =
        admin_delete(addr, "/admin/admins", serde_json::json!({"email": "crew@example.com"})).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let body: serde_json::Value = common::admin_get(addr, "/admin/admins").await.json().await?;
    let emails: Vec<String> = body["admins"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|m| m["email"].as_str().map(String::from))
        .collect();
    assert!(!emails.iter().any(|e| e == "crew@example.com"));
    Ok(())
}

// Adding an admin with a blank email is a 400, not a silent no-op.
#[tokio::test]
async fn add_admin_requires_email() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state).await;
    let resp = common::admin_post(addr, "/admin/admins", serde_json::json!({"email": "  "})).await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    Ok(())
}
```

- [ ] **Step 2: Run tests to verify they fail**

```
cd server && cargo test --test dashboard_test admins_crud_round_trip
```
Expected: FAIL — `GET /admin/admins` returns 404 (route not registered).

- [ ] **Step 3: Add the handlers**

In `server/src/admin.rs`, immediately before `pub fn admin_routes()`, add:

```rust
#[derive(Serialize)]
struct AdminMember {
    email: String,
    #[serde(skip_serializing_if = "Option::is_none", rename = "githubId")]
    github_id: Option<i64>,
}

#[derive(Serialize)]
struct AdminsResponse {
    admins: Vec<AdminMember>,
}

async fn list_admins(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<AdminsResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let rows: Vec<(String, Option<i64>)> =
        sqlx::query_as("SELECT email, github_id FROM rtdb_auth.admins ORDER BY email")
            .fetch_all(&state.pool)
            .await?;
    Ok(Json(AdminsResponse {
        admins: rows
            .into_iter()
            .map(|(email, github_id)| AdminMember { email, github_id })
            .collect(),
    }))
}

#[derive(Deserialize)]
struct AddAdminRequest {
    email: String,
    #[serde(rename = "githubId")]
    github_id: Option<i64>,
}

async fn add_admin(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<AddAdminRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    let email = body.email.trim().to_lowercase();
    if email.is_empty() {
        return Err(RtDbError::bad_request("email is required"));
    }
    // ON CONFLICT merge: keep any existing github_id if the new one is absent.
    sqlx::query(
        "INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, $2, $3) \
         ON CONFLICT (email) DO UPDATE SET \
            github_id = COALESCE(EXCLUDED.github_id, rtdb_auth.admins.github_id)",
    )
    .bind(&email)
    .bind(body.github_id)
    .bind(crate::db::now_ms())
    .execute(&state.pool)
    .await?;
    Ok(Json(OkResponse { ok: true }))
}

#[derive(Deserialize)]
struct RemoveAdminRequest {
    email: String,
}

async fn remove_admin(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<RemoveAdminRequest>,
) -> Result<Json<OkResponse>, RtDbError> {
    require_admin(&state, &headers).await?;
    sqlx::query("DELETE FROM rtdb_auth.admins WHERE email = $1")
        .bind(body.email.trim().to_lowercase())
        .execute(&state.pool)
        .await?;
    Ok(Json(OkResponse { ok: true }))
}
```

`Serialize`/`Deserialize`/`Json`/`Arc`/`HeaderMap`/`ApiJson`/`OkResponse`/`require_admin` are all already in scope at the top of `admin.rs`. `crate::db::now_ms` is fully qualified, so no new import is needed.

- [ ] **Step 4: Register the route**

In `server/src/admin.rs`, inside `admin_routes()`, add this line (e.g. immediately after the `/admin/allowlist` route):

```rust
        .route(
            "/admin/admins",
            get(list_admins).post(add_admin).delete(remove_admin),
        )
```

- [ ] **Step 5: Run tests to verify they pass**

```
cd server && cargo test --test dashboard_test
```
Expected: PASS (all dashboard tests).

- [ ] **Step 6: Run the full gate**

```
cd server && cargo fmt && make checkall
```
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add server/src/admin.rs server/tests/dashboard_test.rs
git commit -m "feat(server): admin allowlist CRUD endpoints GET/POST/DELETE /admin/admins (#18)"
```

---

## Task 4: Session-path end-to-end tests

**Files:**
- Test: `server/tests/dashboard_test.rs` (append).

**Interfaces:**
- Consumes: `require_admin` + `/admin/admins` (Tasks 2–3), `user_session` (Task 2).

- [ ] **Step 1: Write the tests**

Append to `server/tests/dashboard_test.rs` (`user_session` is already defined in Task 2):

```rust

// GET with an arbitrary bearer (for the session-path tests below).
async fn bearer_get(addr: std::net::SocketAddr, path: &str, token: &str) -> reqwest::Response {
    reqwest::Client::new()
        .get(format!("http://{addr}{path}"))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap()
}

// An allowlisted OAuth session is admitted to an admin route.
#[tokio::test]
async fn oauth_admin_session_is_admitted() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state.clone()).await;
    let pool = state.pool.clone();

    let token = user_session(&state, "dash@example.com", None).await;
    sqlx::query("INSERT INTO rtdb_auth.admins (email, github_id, added_at) VALUES ($1, NULL, $2)")
        .bind("dash@example.com")
        .bind(rtdb_server::db::now_ms())
        .execute(&pool)
        .await?;

    let resp = bearer_get(addr, "/admin/admins", &token).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    Ok(())
}

// A valid OAuth session NOT on the admin allowlist is rejected (403).
#[tokio::test]
async fn non_admin_session_is_forbidden() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state).await;

    let token = user_session(&state, "nobody@example.com", None).await;
    let resp = bearer_get(addr, "/admin/admins", &token).await;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    Ok(())
}

// A missing bearer is rejected (401).
#[tokio::test]
async fn missing_bearer_is_unauthorized() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state).await;

    let resp = reqwest::Client::new()
        .get(format!("http://{addr}/admin/admins"))
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::UNAUTHORIZED);
    Ok(())
}

// A machine token is never an admin, even if it reaches an admin route.
#[tokio::test]
async fn machine_token_is_not_admin() -> anyhow::Result<()> {
    let state = common::test_state().await;
    let addr = common::spawn_app(state).await;

    // Create a db + mint a machine token through the admin API.
    let name = format!("t{}", uuid::Uuid::now_v7().simple());
    common::admin_post(addr, "/admin/create-db", serde_json::json!({"name": name})).await;
    let resp: serde_json::Value = common::admin_post(
        addr,
        "/admin/mint-token",
        serde_json::json!({"db": name, "name": "tok"}),
    )
    .await
    .json()
    .await?;
    let token = resp["token"].as_str().unwrap().to_string();

    let resp = bearer_get(addr, "/admin/admins", &token).await;
    assert_eq!(resp.status(), reqwest::StatusCode::FORBIDDEN);
    Ok(())
}
```

- [ ] **Step 2: Run tests to verify they pass**

```
cd server && cargo test --test dashboard_test
```
Expected: PASS. These exercise already-implemented behavior from Tasks 1–3; they are the end-to-end confirmation of the auth model.

- [ ] **Step 3: Run the full gate**

```
cd server && cargo fmt && make checkall
```
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add server/tests/dashboard_test.rs
git commit -m "test(server): dashboard admin auth — session admitted, non-admin/machine rejected (#18)"
```

---

## Phase 1 Done — Definition of Done

- `rtdb_auth.admins` exists and is seeded from `RTDB_ADMIN_EMAILS` on startup.
- `require_admin` accepts the admin key **or** an OAuth session on the admin allowlist; machine tokens and non-allowlisted users are rejected (401 missing / 403 forbidden).
- `GET/POST/DELETE /admin/admins` manage the allowlist.
- `make checkall` is green; all pre-existing admin tests still pass.
- FEATURE_MATRIX #18 is not yet flipped (it flips when the dashboard is usable end-to-end, after later phases) — the "Admin control plane" row may note the new admin-session auth path.

## Next phases (separate plans, written when reached)

Phase 2 metadata read-back · Phase 3 metrics + op feed · Phase 4 config + dynamic CORS · Phase 5 admin document access · Phase 6 static hosting. Then the `/impeccable`-driven frontend.
