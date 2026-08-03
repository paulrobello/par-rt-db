# Login-CSRF Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the accepted login-CSRF residual risk by binding the OAuth `state` token to the initiating browser via a double-submit nonce cookie, verified at the callback, default-on with a break-glass kill-switch.

**Architecture:** `GET /auth/{provider}/begin` sets a short-lived `SameSite=None;HttpOnly` cookie whose value is the `state_token`; `GET /auth/{provider}/callback` rejects (400) any callback whose cookie does not constant-time-equal the `state` query param. A boot `Config` flag `oauth_login_csrf` (env `RTDB_OAUTH_LOGIN_CSRF`, default `true`) is the break-glass switch. The CORS layer gains `allow_credentials(true)` so cross-origin SDK consumers can store the cookie. The ts-client's `begin` fetch gains `credentials: "include"`. No wire-protocol change.

**Tech Stack:** Rust (axum/tower-http 0.6, sqlx, `subtle::ConstantTimeEq` already a dep), TypeScript (ts-client), bash env-drift check.

## Global Constraints

- **Security change — manual review gate.** This is an auth change. Ship default-on with the `RTDB_OAUTH_LOGIN_CSRF` break-glass env. Every existing setting must keep working; `RTDB_OAUTH_LOGIN_CSRF=false` must restore today's exact behavior. The final commit must be **flagged for Paul's manual review before push/deploy** — do not push or deploy without confirmation (repo trunk-based habit: commit on `main` is fine; push/deploy is gated).
- **Preserve OAuth invariants:** HttpOnly cookie delivery (SEC-001/002), single-use TTL-bounded `state`, origin allowlist, `noopener,noreferrer` popup (SEC-012), state-token-keyed cross-origin poll — all unchanged.
- **Cookie attributes (load-bearing, exact):** name `rtdb-oauth-csrf`; value = the `state_token`; `HttpOnly`; `SameSite=None`; `Secure` only when `request_is_secure` (X-Forwarded-Proto=https); `Path=/`; `Max-Age=600`.
- **No `unwrap`/`expect` outside `#[cfg(test)]`.** Zero clippy warnings under `-D warnings`. Validate every cookie value against injection chars (fail closed), mirroring `set_session_cookie`.
- **Gate:** `make checkall` (fmt-check + clippy -D warnings + typecheck + tests, incl. `env-drift-check`) must pass before the work is done. Dev Postgres on `127.0.0.1:55434` is required (`make dev-db-up`).
- **Doc-sync:** when code lands, update `.env.example`, `docker-compose.yml`, `CLAUDE.md`, `FEATURE_MATRIX.md`, `SPEC_STATUS.md`. The one-way `env-drift-check` fails if a var documented in `.env.example` is not forwarded in `docker-compose.yml`.

---

## File Structure

- `server/src/config.rs` — add `oauth_login_csrf: bool` field + default-true env parse.
- `server/src/auth/cookie.rs` — add `OAUTH_CSRF_COOKIE` const + read/set/clear helpers (mirror the existing session-cookie helpers).
- `server/src/auth/provider.rs` — set the cookie at `provider_begin`, verify it at `provider_callback` (constant-time), clear it on the callback success path.
- `server/src/lib.rs` — `.allow_credentials(true)` on the CORS layer.
- `server/tests/common/mod.rs` + `server/tests/healthz_test.rs` — add the new `Config` field to the two literal `test_config()` constructors.
- `server/tests/oauth_test.rs` — enable cookie-store on the test client (so existing begin→callback flows replay the new cookie), add `oauth_state_with_csrf`, add 3 CSRF tests.
- `ts-client/src/react.tsx` — `credentials: "include"` on the `begin` fetch.
- `.env.example`, `docker-compose.yml`, `CLAUDE.md`, `FEATURE_MATRIX.md`, `docs/superpowers/SPEC_STATUS.md` — doc/config sync.

---

## Task 1: Add `oauth_login_csrf` boot Config flag (default true)

**Files:**
- Modify: `server/src/config.rs:54-68` (struct field), `server/src/config.rs:166-176` (env parse), `server/src/config.rs:230-262` (`Ok(Self { … })`)
- Modify: `server/tests/common/mod.rs:9` (`test_config()`), `server/tests/healthz_test.rs:4` (`test_config()`)

**Interfaces:**
- Produces: `pub oauth_login_csrf: bool` on `Config`, read in Task 3 as `state.config.oauth_login_csrf`. Default `true`; only `"false" | "0" | "no"` (case-insensitive) disables it.

- [ ] **Step 1: Add the struct field.** In `server/src/config.rs`, immediately after the `audit_log_enabled` field (around line 55), add:

```rust
    // Login-CSRF defense: bind the OAuth `state` to the initiating browser via a
    // double-submit nonce cookie set at /begin and verified at /callback. On by
    // default — only "false"/"0"/"no" (case-insensitive) disables it (break-glass,
    // restores pre-hardening behavior). RTDB_OAUTH_LOGIN_CSRF.
    pub oauth_login_csrf: bool,
```

- [ ] **Step 2: Add the env parse.** In `server/src/config.rs` `from_env`, immediately after the `audit_log_enabled` parse (around line 170), add — note this is **default-true**, the inverse of the audit/webhook parses:

```rust
        // Login-CSRF: default ON (security). Only an explicit falsy spelling
        // disables it (break-glass). Anything else, including unset, stays on.
        let oauth_login_csrf = match std::env::var("RTDB_OAUTH_LOGIN_CSRF") {
            Ok(v) => !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"),
            Err(_) => true,
        };
```

- [ ] **Step 3: Add the field to the `Ok(Self { … })` return.** In `server/src/config.rs` around line 243 (after `audit_log_enabled,` in the struct literal), add:

```rust
            oauth_login_csrf,
```

- [ ] **Step 4: Update `test_config()` in `server/tests/common/mod.rs`.** In the `Config { … }` literal, after the `audit_log_enabled: false,` line, add:

```rust
        oauth_login_csrf: true,
```

- [ ] **Step 5: Update `test_config()` in `server/tests/healthz_test.rs`.** Same edit — after its `audit_log_enabled` line (or wherever audit/webhook sit; if absent, place it next to the other bool fields), add `oauth_login_csrf: true,`. If unsure of exact neighbors, run Step 6 and the compiler names the missing field.

- [ ] **Step 6: Verify it compiles.**

Run: `cd server && cargo build --tests 2>&1 | tail -20`
Expected: BUILD SUCCEEDED. If a `Config { … }` literal elsewhere is missing the field, the compiler names the file — add `oauth_login_csrf: true,` there too. (Sanity sweep: `grep -rn "Config {" server/` to confirm no other literal constructors exist beyond the two test_config sites + from_env.)

- [ ] **Step 7: Commit.**

```bash
git add server/src/config.rs server/tests/common/mod.rs server/tests/healthz_test.rs
git commit -m "feat(auth): RTDB_OAUTH_LOGIN_CSRF boot flag (default-on, for login-CSRF hardening)"
```

---

## Task 2: CSRF nonce cookie helpers in `cookie.rs` (TDD)

**Files:**
- Modify: `server/src/auth/cookie.rs` (add const + 3 helpers + unit tests)

**Interfaces:**
- Produces (all `pub(crate)`, mirror the session-cookie helpers):
  - `const OAUTH_CSRF_COOKIE: &str = "rtdb-oauth-csrf";`
  - `fn oauth_csrf_cookie(headers: &HeaderMap) -> Option<&str>` — reads the cookie.
  - `fn set_oauth_csrf_cookie(value: &str, secure: bool) -> Result<HeaderValue, RtDbError>` — builds the `Set-Cookie` (`SameSite=None; HttpOnly; Path=/; Max-Age=600` + `; Secure` when `secure`), injection-validated.
  - `fn clear_oauth_csrf_cookie() -> HeaderValue` — `Max-Age=0` expiry.

- [ ] **Step 1: Write the failing unit tests.** Append to the `#[cfg(test)] mod tests` block in `server/src/auth/cookie.rs`:

```rust
    #[test]
    fn oauth_csrf_cookie_reads_among_pairs() {
        let mut h = HeaderMap::new();
        h.insert(
            "cookie",
            "rtdb_session=abc; rtdb-oauth-csrf=xyz-state; lang=en".parse().unwrap(),
        );
        assert_eq!(oauth_csrf_cookie(&h), Some("xyz-state"));
    }

    #[test]
    fn oauth_csrf_cookie_missing_is_none() {
        let mut h = HeaderMap::new();
        h.insert("cookie", "rtdb_session=abc".parse().unwrap());
        assert_eq!(oauth_csrf_cookie(&h), None);
    }

    #[test]
    fn set_oauth_csrf_cookie_includes_attributes() {
        let plain = set_oauth_csrf_cookie("deadbeef", false)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(plain.contains("rtdb-oauth-csrf=deadbeef"));
        assert!(plain.contains("HttpOnly"));
        assert!(plain.contains("SameSite=None"));
        assert!(plain.contains("Max-Age=600"));
        assert!(!plain.contains("Secure"));

        let secure = set_oauth_csrf_cookie("deadbeef", true)
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert!(secure.contains("Secure"));
    }

    #[test]
    fn set_oauth_csrf_cookie_rejects_injection_chars() {
        assert!(set_oauth_csrf_cookie("a;b", true).is_err());
        assert!(set_oauth_csrf_cookie("a,b", true).is_err());
        assert!(set_oauth_csrf_cookie("a b", true).is_err());
        assert!(set_oauth_csrf_cookie("", true).is_err());
        assert!(set_oauth_csrf_cookie("deadbeef-0123", true).is_ok());
    }
```

- [ ] **Step 2: Run tests to verify they fail.**

Run: `cd server && cargo test --lib cookie::tests 2>&1 | tail -15`
Expected: COMPILE ERROR — `oauth_csrf_cookie` / `set_oauth_csrf_cookie` not defined.

- [ ] **Step 3: Implement the helpers.** In `server/src/auth/cookie.rs`, after the `clear_session_cookie` fn (before `request_is_secure`), add:

```rust
/// Cookie name carrying the login-CSRF double-submit nonce. Its value is the
/// OAuth `state` token minted at `/begin`; `/callback` requires it to match.
pub(crate) const OAUTH_CSRF_COOKIE: &str = "rtdb-oauth-csrf";

/// `Max-Age` (seconds) for the CSRF nonce — matches `STATE_TTL_MS` (10 min).
const CSRF_MAX_AGE_SECS: u64 = 600;

/// Reads the `rtdb-oauth-csrf` cookie value from the `Cookie:` header, if present.
pub(crate) fn oauth_csrf_cookie(headers: &HeaderMap) -> Option<&str> {
    let raw = headers.get(axum::http::header::COOKIE)?.to_str().ok()?;
    raw.split(';').find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        (name.trim() == OAUTH_CSRF_COOKIE).then_some(value.trim())
    })
}

/// Builds the `Set-Cookie` for the CSRF nonce. `SameSite=None` is required: the
/// provider → callback redirect is a top-level cross-site navigation, so `Lax`
/// would not attach the cookie and the defense would never fire. `Secure` mirrors
/// the session cookie (omitted for local http dev). Same injection-char guard as
/// `set_session_cookie` (fails closed).
pub(crate) fn set_oauth_csrf_cookie(value: &str, secure: bool) -> Result<HeaderValue, RtDbError> {
    if value.is_empty()
        || value
            .bytes()
            .any(|b| matches!(b, b';' | b',' | b' ' | b'\t' | b'\r' | b'\n') || b < 0x20)
    {
        return Err(RtDbError::internal(
            "oauth csrf cookie value contains illegal characters",
        ));
    }
    let mut s = format!(
        "{OAUTH_CSRF_COOKIE}={value}; HttpOnly; SameSite=None; Path=/; Max-Age={CSRF_MAX_AGE_SECS}"
    );
    if secure {
        s.push_str("; Secure");
    }
    HeaderValue::from_str(&s).map_err(|_| RtDbError::internal("invalid oauth csrf cookie value"))
}

/// Builds the `Set-Cookie` that deletes the CSRF nonce (single-use hygiene on a
/// successful callback).
pub(crate) fn clear_oauth_csrf_cookie() -> HeaderValue {
    HeaderValue::from_str(&format!(
        "{OAUTH_CSRF_COOKIE}=; HttpOnly; SameSite=None; Path=/; Max-Age=0; Expires=Thu, 01 Jan 1970 00:00:00 GMT"
    ))
    .expect("static clear-cookie template is a valid header value")
}
```

- [ ] **Step 4: Run tests to verify they pass.**

Run: `cd server && cargo test --lib cookie::tests 2>&1 | tail -15`
Expected: 4 passed (plus the pre-existing cookie tests).

- [ ] **Step 5: Commit.**

```bash
git add server/src/auth/cookie.rs
git commit -m "feat(auth): rtdb-oauth-csrf double-submit cookie helpers"
```

---

## Task 3: Wire the cookie into `begin` / `callback` + OAuth tests

**Files:**
- Modify: `server/src/auth/provider.rs:1-22` (import), `:189-229` (`provider_begin`), `:243-273` (`provider_callback`), `:280-297` (`callback_close_response`)
- Modify: `server/tests/oauth_test.rs:110-115` (`no_redirect_client`), `:26-35` (`oauth_state`), append 3 new tests

**Interfaces:**
- Consumes: `crate::auth::cookie::{oauth_csrf_cookie, set_oauth_csrf_cookie, clear_oauth_csrf_cookie, request_is_secure}` (Task 2), `state.config.oauth_login_csrf` (Task 1).
- Produces: the live CSRF gate — `begin` sets the cookie, `callback` enforces it.

- [ ] **Step 1: Add the `subtle` import.** At the top of `server/src/auth/provider.rs`, add to the existing `use` group:

```rust
use subtle::ConstantTimeEq;
```

- [ ] **Step 2: Set the cookie at `provider_begin`.** Change the signature to take `headers: HeaderMap` and append the `Set-Cookie`. Replace the `provider_begin` signature + tail:

Old (signature):
```rust
async fn provider_begin<P: OAuthProvider>(
    State(state): State<Arc<AppState>>,
    QueryParams(params): QueryParams<BeginParams>,
) -> Response {
```
New:
```rust
async fn provider_begin<P: OAuthProvider>(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    QueryParams(params): QueryParams<BeginParams>,
) -> Response {
```

Old (tail, the response return):
```rust
    let authorize_url = provider.authorize_url(&redirect_uri, &state_token);
    Json(BeginResponse {
        authorize_url,
        state: state_token,
    })
    .into_response()
}
```
New:
```rust
    let authorize_url = provider.authorize_url(&redirect_uri, &state_token);
    let mut response = Json(BeginResponse {
        authorize_url,
        state: state_token.clone(),
    })
    .into_response();
    // Login-CSRF: bind this `state` to the initiating browser so a callback the
    // attacker induced the victim to load (the attacker's own exchange) carries no
    // matching cookie and is rejected at `provider_callback`. SameSite=None so it
    // survives the provider → callback cross-site redirect.
    let secure = crate::auth::cookie::request_is_secure(&headers);
    if let Ok(csrf) = crate::auth::cookie::set_oauth_csrf_cookie(&state_token, secure) {
        response.headers_mut().append(SET_COOKIE, csrf);
    }
    response
}
```

- [ ] **Step 3: Verify the cookie at `provider_callback` (before `claim_pending`).** In `server/src/auth/provider.rs`, insert this block as the **first** statements of `provider_callback`'s body (before `if !claim_pending(…)`):

```rust
    // Login-CSRF gate: the browser that hits the callback must be the one that hit
    // begin (it carries the matching nonce cookie). Checked before the entry is
    // claimed, so a rejected callback leaves the state Pending and a legit retry
    // still works. Constant-time compare for hygiene.
    if state.config.oauth_login_csrf {
        let ok = crate::auth::cookie::oauth_csrf_cookie(&headers).is_some_and(|c| {
            bool::from(c.as_bytes().ct_eq(params.state.as_bytes()))
        });
        if !ok {
            return RtDbError::bad_request("login CSRF check failed").into_response();
        }
    }
```

- [ ] **Step 4: Clear the cookie on callback success.** In `callback_close_response` (`server/src/auth/provider.rs:290-296`), change `insert` → `append` and add the clear. Old:

```rust
    match crate::auth::cookie::set_session_cookie(token, secure) {
        Ok(cookie) => {
            response.headers_mut().insert(SET_COOKIE, cookie);
            response
        }
        Err(err) => err.into_response(),
    }
```
New:

```rust
    match crate::auth::cookie::set_session_cookie(token, secure) {
        Ok(cookie) => {
            response.headers_mut().append(SET_COOKIE, cookie);
            response
                .headers_mut()
                .append(SET_COOKIE, crate::auth::cookie::clear_oauth_csrf_cookie());
            response
        }
        Err(err) => err.into_response(),
    }
```

- [ ] **Step 5: Enable cookie-store on the OAuth test client.** In `server/tests/oauth_test.rs` `no_redirect_client` (line 110), add `.cookie_store(true)` so the begin→callback→poll flows replay the new nonce cookie (a cross-origin-equivalent client would otherwise drop it):

```rust
fn no_redirect_client() -> reqwest::Client {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .cookie_store(true)
        .build()
        .expect("build client")
}
```

- [ ] **Step 6: Refactor `oauth_state` to take a CSRF flag.** In `server/tests/oauth_test.rs`, rename the body of `oauth_state` (line 26) into a parameterized helper and delegate. Replace the existing `oauth_state` fn with:

```rust
/// Like `oauth_state`, but lets a test disable the login-CSRF check (kill-switch).
async fn oauth_state_with_csrf(mock: &MockServer, csrf: bool) -> (Arc<AppState>, SocketAddr) {
    let mut cfg = test_config();
    cfg.github_base_url = mock.uri();
    cfg.github_api_url = mock.uri();
    cfg.github_client_id = Some("test-client".into());
    cfg.github_client_secret = Some("test-secret".into());
    cfg.oauth_login_csrf = csrf;

    let pool = sqlx::PgPool::connect(&cfg.database_url)
        .await
        .expect("connect to test postgres");
    db::bootstrap(&pool).await.expect("bootstrap rtdb_auth");

    let state = AppState::new(pool, cfg, common::test_hot());
    let addr = spawn_app(state.clone()).await;
    (state, addr)
}

/// Builds an `AppState` whose GitHub URLs point at `mock`, with OAuth
/// credentials configured, and spawns it. Bootstrap runs on a fresh
/// connection to the shared test Postgres, matching `test_state()`'s setup.
async fn oauth_state(mock: &MockServer) -> (Arc<AppState>, SocketAddr) {
    oauth_state_with_csrf(mock, true).await
}
```

- [ ] **Step 7: Add the three CSRF tests.** Append to `server/tests/oauth_test.rs`:

```rust
/// Login-CSRF: a callback from a different browser (no matching nonce cookie)
/// is rejected 400, even with a valid begun state — the attacker-induced-callback
/// scenario. The state entry stays Pending (not claimed).
#[tokio::test]
async fn login_csrf_rejected_without_cookie() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    mount_github_mocks(
        &mock,
        json!([{"email": "csrf-none@example.com", "verified": true, "primary": true}]),
    )
    .await;
    let (_state, addr) = oauth_state(&mock).await;

    // begin on a cookie-storing client (it stores the nonce)…
    let begin_client = no_redirect_client();
    let state_token = begin_login(&begin_client, addr, "http://localhost:5173").await;
    // …but the callback comes from a different, cookie-less client.
    let callback_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let resp = callback(&callback_client, addr, &state_token).await;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    Ok(())
}

/// Login-CSRF: a callback whose cookie value does not match `state` is rejected.
#[tokio::test]
async fn login_csrf_rejected_with_wrong_cookie() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    mount_github_mocks(
        &mock,
        json!([{"email": "csrf-wrong@example.com", "verified": true, "primary": true}]),
    )
    .await;
    let (_state, addr) = oauth_state(&mock).await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?; // no cookie store → no auto-replay
    let state_token = begin_login(&client, addr, "http://localhost:5173").await;
    let resp = client
        .get(format!("http://{addr}/auth/callback?code=abc&state={state_token}"))
        .header("cookie", "rtdb-oauth-csrf=not-the-real-state")
        .send()
        .await?;
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
    Ok(())
}

/// Kill-switch: with RTDB_OAUTH_LOGIN_CSRF=false, a cookie-less callback succeeds
/// (today's pre-hardening behavior).
#[tokio::test]
async fn login_csrf_kill_switch_off_allows_cookieless_callback() -> anyhow::Result<()> {
    let mock = MockServer::start().await;
    // distinct github_id/login so it never collides with other parallel tests.
    mount_github_user_mocks(
        &mock,
        4242,
        "csrfkill",
        json!([{"email": "csrf-kill@example.com", "verified": true, "primary": true}]),
    )
    .await;
    let (_state, addr) = oauth_state_with_csrf(&mock, false).await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?; // no cookie store → no nonce sent
    let state_token = begin_login(&client, addr, "http://localhost:5173").await;
    let resp = callback(&client, addr, &state_token).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    Ok(())
}
```

- [ ] **Step 8: Run the OAuth suite.**

Run: `cd server && make dev-db-up && cargo test --test oauth_test 2>&1 | tail -30`
Expected: all pass — existing `login_flow`/callback tests pass (the cookie-storing client replays the nonce), plus the 3 new CSRF tests. If `login_flow` callers fail with 400, confirm `no_redirect_client` got `.cookie_store(true)` (Step 5).

- [ ] **Step 9: Commit.**

```bash
git add server/src/auth/provider.rs server/tests/oauth_test.rs
git commit -m "feat(auth): enforce login-CSRF double-submit cookie at OAuth callback"
```

---

## Task 4: Allow credentialed CORS (cross-origin cookie storage)

**Files:**
- Modify: `server/src/lib.rs:139-159` (`cors_layer`)

**Interfaces:**
- Produces: `Access-Control-Allow-Credentials: true` on allowed-origin responses, so a cross-origin SDK consumer's credentialed `/begin` fetch can store the nonce cookie. Safe because the origin check is already an exact-match predicate that reflects the specific request origin (never `*`).

- [ ] **Step 1: Add `allow_credentials(true)`.** In `server/src/lib.rs` `cors_layer`, append to the `CorsLayer` builder chain (after `.allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE])`):

```rust
        .allow_credentials(true)
```

tower-http 0.6 accepts a `bool` here (`bool: Into<AllowCredentials>`). If clippy/build rejects it, use `.allow_credentials(tower_http::cors::AllowCredentials::yes())` instead — same effect.

- [ ] **Step 2: Verify build + lint.**

Run: `cd server && cargo build 2>&1 | tail -5 && cargo clippy -- -D warnings 2>&1 | tail -10`
Expected: clean. (Combining credentials with a wildcard origin would be a CORS-spec violation, but this layer reflects the specific origin via predicate, so it is compliant.)

- [ ] **Step 3: Commit.**

```bash
git add server/src/lib.rs
git commit -m "feat(cors): allow_credentials so cross-origin OAuth /begin can store the CSRF cookie"
```

---

## Task 5: ts-client sends `credentials: "include"` on `/begin`

**Files:**
- Modify: `ts-client/src/react.tsx:241-243` (the `signInWithOAuth` begin fetch)

**Interfaces:**
- Produces: a cross-origin SDK consumer's `begin` fetch is credentialed, so the browser stores the `rtdb-oauth-csrf` cookie set by the server. The same-origin dashboard needs no change (its default `credentials: "same-origin"` already stores it) and is intentionally left untouched (surgical).

- [ ] **Step 1: Add `credentials: "include"`.** In `ts-client/src/react.tsx` `signInWithOAuth`:

Old:
```typescript
    const beginResp = await fetch(
      `${api}/auth/${provider}/begin?origin=${encodeURIComponent(spaOrigin)}`,
    );
```
New:
```typescript
    const beginResp = await fetch(
      `${api}/auth/${provider}/begin?origin=${encodeURIComponent(spaOrigin)}`,
      { credentials: "include" },
    );
```

- [ ] **Step 2: Build + test the ts-client.**

Run: `cd ts-client && bun run build && bunx vitest run 2>&1 | tail -20`
Expected: build succeeds; existing tests pass. (If a react.tsx unit test asserts the begin fetch shape, update its mock to expect `credentials: "include"`.)

- [ ] **Step 3: Commit.**

```bash
git add ts-client/src/react.tsx
git commit -m "feat(client): send credentials:'include' on OAuth /begin (login-CSRF rollout)"
```

---

## Task 6: Docs + config-drift + SPEC_STATUS sync; flag for manual review

**Files:**
- Modify: `.env.example`, `docker-compose.yml`, `CLAUDE.md`, `FEATURE_MATRIX.md`, `docs/superpowers/SPEC_STATUS.md`

- [ ] **Step 1: Document + forward the env var.** In `.env.example`, after the `RTDB_SESSION_TTL_DAYS=30` line (near line 51, in the auth section), add:

```dotenv
# Login-CSRF defense: bind OAuth state to the initiating browser via a
# double-submit nonce cookie (set at /begin, verified at /callback). On by
# default. Set to false/0/no only as break-glass (restores pre-hardening
# behavior). Cross-origin SDK consumers must send credentials:'include' on /begin.
RTDB_OAUTH_LOGIN_CSRF=true
```

In `docker-compose.yml`, in the server service's `environment:` block (after `RTDB_SESSION_TTL_DAYS`), add:

```yaml
      RTDB_OAUTH_LOGIN_CSRF: ${RTDB_OAUTH_LOGIN_CSRF:-true}
```

- [ ] **Step 2: Update CLAUDE.md Auth section.** Find the sentence about the OAuth flow in the Auth paragraph and replace the "accepted residual risk (login CSRF)" framing. Change the parenthetical that says login CSRF is an accepted residual risk to note the defense, e.g.:

> Login-CSRF is defended by a double-submit nonce cookie (`rtdb-oauth-csrf`, value = the `state` token; `SameSite=None;HttpOnly`, 10-min) set at `/begin` and constant-time-verified at `/callback` (disable via `RTDB_OAUTH_LOGIN_CSRF=false`); the CORS layer sets `Access-Control-Allow-Credentials` so cross-origin SDK consumers can store it, and the ts-client sends `credentials:"include"` on `/begin`.

Add `RTDB_OAUTH_LOGIN_CSRF` to the boot-config list alongside the other security gates if one exists. Stay surgical — edit only the auth-login-CSRF framing, not surrounding text.

- [ ] **Step 3: Update FEATURE_MATRIX.md Auth row.** In the Auth row's notes, append a sentence noting login-CSRF is now defended by a double-submit state cookie (kill-switch `RTDB_OAUTH_LOGIN_CSRF`).

- [ ] **Step 4: Update SPEC_STATUS.md — add this spec AND fix the two stale rows.** In `docs/superpowers/SPEC_STATUS.md`:
  - Add a row: `` `2026-08-02-login-csrf-hardening-design.md` | Implemented | 2026-08-02 | Auth section | — ``
  - Fix the `2026-07-24-per-row-authorization-design.md` row: its Status is `Implemented (v1: owner-field match)` and Follow-on says "v3 … not yet specced" — update to reflect that **v3 (the `authorize` general declarative predicate DSL) shipped** (commits 5fe075e / e7fc6b9 / 85141c2, 2026-08-02). Set Status to `Implemented (v1–v3)` and clear/replace the "not yet specced" follow-on note.
  - Fix the `2026-07-25-python-client-design.md` row: its Status says "reactive WS client pending" — the Python reactive `ws` surface ships (`pip install par-rt-db[ws]`), so update Status to `Implemented (core DSL + sync HTTP/admin/storage + reactive WS)`.
  - Also update the "Per-row auth / fine-grained invalidation" note bullet if it still claims v3 is unspecced.

- [ ] **Step 5: Run the env-drift check + the full gate.**

Run: `cd /Users/probello/Repos/par-rt-db && make dev-db-up && make checkall 2>&1 | tail -40`
Expected: PASS — env-drift-check reports `RTDB_OAUTH_LOGIN_CSRF` forwarded; fmt-check, clippy -D warnings, typecheck, and all tests (incl. the new oauth_test CSRF cases) pass.

- [ ] **Step 6: Commit.**

```bash
git add .env.example docker-compose.yml CLAUDE.md FEATURE_MATRIX.md docs/superpowers/SPEC_STATUS.md
git commit -m "docs(auth): login-CSRF hardening shipped + spec-status sync (model-C predicate, python WS)"
```

- [ ] **Step 7: Flag for manual review — DO NOT push or deploy.** This is an auth security change. Report to Paul: the commit range, the default-on posture, the break-glass env, the cross-origin consumer impact (projects.pardev.net + hackzors — board items already filed), and **wait for explicit confirmation before `git push` / `make deploy`.**

---

## Self-Review

**Spec coverage:** Spec §3.1 cookie attributes → Task 2 (helpers encode them exactly). §3.2 server changes → Tasks 1–4. §3.3 data flow → Task 3 (begin set / callback verify+clear). §3.4 client mirroring → Task 5 (ts-client required; rust/python confirmed to have no `/begin` call; dashboard same-origin, no change — surgical deviation from the spec's "for uniformity", justified). §4 error handling → Task 3 Step 3 (400 before claim, kill-switch path) + Task 3 kill-switch test. §5 testing → Task 2 unit tests + Task 3 three CSRF tests. §6 docs/config-drift → Task 6. §7 manual-review flag → Task 6 Step 7. All spec sections covered.

**Placeholder scan:** No TBD/TODO. Every code step contains verbatim code. The two `mount_github_*` helpers and `begin_login`/`callback` referenced in Task 3 Step 7 exist verbatim in `oauth_test.rs` (confirmed: `mount_github_mocks` line 41, `mount_github_user_mocks` line 48, `begin_login` line 117, `callback` line 146).

**Type consistency:** `oauth_login_csrf: bool` (Task 1) read as `state.config.oauth_login_csrf` (Task 3). Helper names match across Task 2 (defined) and Task 3 (used): `oauth_csrf_cookie`, `set_oauth_csrf_cookie`, `clear_oauth_csrf_cookie`, `request_is_secure`. `oauth_state_with_csrf` defined and used in Task 3. `OAUTH_CSRF_COOKIE` const used inside the helpers only. Cookie name `rtdb-oauth-csrf` consistent across cookie.rs, the wrong-cookie test, and CLAUDE.md note.

**Execution note:** Tasks 1→5 are sequential (Task 3 depends on 1+2; Task 6 is last and runs the full gate). Each task commits atomically and compiles green at its boundary. The whole change ships behind a default-on flag with a verified kill-switch test.
