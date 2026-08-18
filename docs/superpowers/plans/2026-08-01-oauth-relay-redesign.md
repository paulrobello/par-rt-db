# OAuth Popup Relay Redesign (SEC-012) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `window.opener` postMessage OAuth relay with state-token polling so the popup can open with `noopener,noreferrer` (tabnabbing hardening), working for both same-origin (dashboard) and cross-origin (SDK) consumers.

**Architecture:** `GET /auth/{provider}/begin` mints a state token + returns the provider authorize URL; the parent opens that URL with `noopener` and polls `GET /auth/state?state=`; the callback marks the entry `Completed`/`Failed` and closes the popup. No `window.opener`, no postMessage, no `origin` interpolation.

**Tech Stack:** Rust (axum/sqlx, server), TypeScript (ts-client `react.tsx`, dashboard `session.tsx`), bun/vitest (ts-client tests).

## Global Constraints

- Wire casing is load-bearing — match `server/src/auth/provider.rs` exactly.
- The single-use, TTL-bounded (10 min, `STATE_TTL_MS`) OAuth state invariant is preserved: a callback may complete an entry once; a replay rejects; the poll retrieves once.
- Origin allowlist still enforced at `begin` (the only place `origin` enters), against the live `state.runtime.hot.load().allowed_origins`.
- HttpOnly session cookie (`SameSite=Lax`) delivery is unchanged; the callback still sets it.
- `complete_login` is async/slow — never hold the `oauth_states` `Mutex` across it.
- No `unwrap()`/`expect()` outside `#[cfg(test)]`; zero clippy warnings under `-D warnings`.
- `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests) is the gate; `make dev-db-up` is required for any server test run.
- Clients mirror the server: server change (Task 1) lands first; ts-client (Task 2) and dashboard (Task 3) follow it.

---

## File Structure

- **Modify:** `server/src/auth/provider.rs` — state model, `begin`/`callback`/`state` handlers, routes; remove `provider_start`/`consume_state`/`escape_js_string`/`callback_html_response`/`redirect_found`/`StartParams` + the `callback_tests` module.
- **Modify:** `server/tests/oauth_test.rs` — `begin_login` helper, `login_flow`, renamed/added tests.
- **Modify:** `ts-client/src/react.tsx` — rewrite `signInWithOAuth`; `signInWithGitHub`/`signInWithGoogle` signatures unchanged.
- **Modify (test):** `ts-client/tests/` — add/cover the rewritten `signInWithOAuth` (fetch-mock).
- **Modify:** `dashboard/src/lib/session.tsx` — rewrite `signInWithOAuth` (begin + noopener + poll + `/auth/me`).
- **Modify:** `FEATURE_MATRIX.md`, `CLAUDE.md` — docs.

---

## Task 1: Server — state-token relay (begin / callback / state)

**Files:**
- Modify: `server/src/auth/provider.rs`
- Modify: `server/tests/oauth_test.rs`

**Interfaces:**
- Consumes: `OAuthProvider` trait (`from_config`, `callback_path`, `authorize_url`, `complete_login`), `crate::auth::{resolve_bearer, authed_user, Principal}`, `crate::auth::cookie::{set_session_cookie, request_is_secure}`, `db::{now_ms, random_token}`, `AppState.auth.oauth_states: Mutex<HashMap<String, OAuthStateEntry>>`.
- Produces: `GET /auth/{provider}/begin` → `{authorize_url, state}`; `GET /auth/{provider}/callback` (reworked); `GET /auth/state` → `{status: pending|complete|expired|error}`.

- [ ] **Step 1: Write the failing server tests first** (update `server/tests/oauth_test.rs`)

  Replace the `start_login` helper with `begin_login` (the begin endpoint returns JSON, not a 302):

  ```rust
  async fn begin_login(client: &reqwest::Client, addr: SocketAddr, origin: &str) -> String {
      let resp = client
          .get(format!("http://{addr}/auth/github/begin?origin={origin}"))
          .send()
          .await
          .expect("send github begin");
      assert_eq!(resp.status(), reqwest::StatusCode::OK);
      let body: serde_json::Value = resp.json().await.expect("begin json");
      body["state"].as_str().expect("state field in begin response").to_string()
  }
  ```

  Rework `login_flow` to drive begin → callback → poll, and assert the callback body carries no token (it is cookie-only) and the poll returns `complete`:

  ```rust
  async fn login_flow(addr: SocketAddr, origin: &str) -> String {
      let client = no_redirect_client();
      let state_token = begin_login(&client, addr, origin).await;
      let resp = callback(&client, addr, &state_token).await;
      assert_eq!(resp.status(), reqwest::StatusCode::OK);
      assert!(
          resp.headers().get(reqwest::header::CONTENT_TYPE)
              .expect("content-type").to_str().expect("utf8")
              .starts_with("text/html")
      );
      assert_eq!(
          resp.headers().get("content-security-policy")
              .expect("csp").to_str().expect("utf8"),
          "default-src 'none'; script-src 'unsafe-inline'"
      );
      let set_cookie = resp.headers().get(reqwest::header::SET_COOKIE)
          .expect("set-cookie").to_str().expect("utf8");
      let token = extract_token_from_cookie(set_cookie);
      let body = resp.text().await.expect("read callback body");
      assert!(!body.contains(&token), "token leaked into callback HTML");

      // The poll returns the token (one-shot) and the entry is then gone.
      let poll = client
          .get(format!("http://{addr}/auth/state?state={state_token}"))
          .send().await.expect("poll");
      let pv: serde_json::Value = poll.json().await.expect("poll json");
      assert_eq!(pv["status"], "complete");
      assert_eq!(pv["token"].as_str(), Some(token.as_str()));
      assert!(pv["user"]["email"].as_str().is_some());
      token
  }
  ```

  Add a `state_poll_returns_pending_before_callback` test (begin, then poll before callback → `pending`), and a `state_poll_returns_error_after_failed_login` test (use a code the mock rejects, e.g. `code=bad`, so `complete_login` errors → the poll returns `error`). The callback helper can take a `code` parameter:

  ```rust
  async fn callback_with(client: &reqwest::Client, addr: SocketAddr, state: &str, code: &str) -> reqwest::Response {
      client.get(format!("http://{addr}/auth/callback?code={code}&state={state}"))
          .send().await.expect("send callback")
  }
  ```

  Rename `github_start_with_disallowed_origin_returns_forbidden` → `github_begin_with_disallowed_origin_returns_forbidden` and point it at `/auth/github/begin`. Keep `replayed_state_returns_bad_request` but drive it via `begin_login` + two `callback`s (first OK, second 400). Update `full_oauth_flow_returns_html_with_session_token` to the new shape (close HTML + cookie + poll), or fold its assertions into `login_flow` and remove the duplicate.

- [ ] **Step 2: Run the tests to verify they fail**

  Run: `cd server && cargo test --test oauth_test`
  Expected: FAIL (compile errors — `start_login` gone, new helpers reference unbuilt routes/handlers).

- [ ] **Step 3: Implement the state model + helpers in `provider.rs`**

  Replace the `OAuthStateEntry` struct and `consume_state` fn with:

  ```rust
  /// Outcome of a pending OAuth login, driven by the callback. `Pending` →
  /// `Claiming` (first callback wins) → `Completed` | `Failed`.
  pub enum LoginOutcome {
      Pending,
      Claiming,
      Completed(String),
      Failed,
  }

  pub struct OAuthStateEntry {
      pub expires_at: i64,
      pub outcome: LoginOutcome,
  }
  ```

  Add the three helpers (all lock `state.auth.oauth_states`, none await under the lock except `poll_login`'s post-lock resolve):

  ```rust
  /// `Pending → Claiming` for the first caller; `false` for a replay or an
  /// already-terminal/expired entry. This is the single-use claim that makes a
  /// replayed callback reject.
  async fn claim_pending(state: &Arc<AppState>, state_token: &str) -> bool {
      let mut states = state.auth.oauth_states.lock().await;
      let now = now_ms();
      match states.get_mut(state_token) {
          Some(entry) if entry.expires_at > now && matches!(entry.outcome, LoginOutcome::Pending) => {
              entry.outcome = LoginOutcome::Claiming;
              true
          }
          _ => false,
      }
  }

  /// Sets the terminal outcome after `complete_login` (`Claiming → Completed | Failed`).
  async fn set_outcome(state: &Arc<AppState>, state_token: &str, outcome: LoginOutcome) {
      let mut states = state.auth.oauth_states.lock().await;
      if let Some(entry) = states.get_mut(state_token) {
          entry.outcome = outcome;
      }
  }

  enum PollResult {
      Pending,
      Complete { token: String, user: AuthedUser },
      Failed,
      Expired,
  }

  /// One-shot retrieval for the polling endpoint. Removes the entry on a
  /// terminal outcome; leaves it in place while pending.
  async fn poll_login(state: &Arc<AppState>, state_token: &str) -> PollResult {
      let taken: Option<Result<String, ()>> = {
          let mut states = state.auth.oauth_states.lock().await;
          let now = now_ms();
          states.retain(|_, e| e.expires_at > now);
          match states.remove(state_token) {
              None => None,
              Some(entry) => match entry.outcome {
                  LoginOutcome::Pending | LoginOutcome::Claiming => {
                      states.insert(state_token.to_string(), entry); // not ready — put back
                      return PollResult::Pending;
                  }
                  LoginOutcome::Completed(t) => Some(Ok(t)),
                  LoginOutcome::Failed => Some(Err(())),
              },
          }
      };
      match taken {
          None => PollResult::Expired,
          Some(Err(())) => PollResult::Failed,
          Some(Ok(token)) => match resolve_bearer(&state.pool, &token).await {
              Ok(principal @ Principal::User { .. }) => PollResult::Complete {
                  token,
                  user: authed_user(&principal),
              },
              _ => PollResult::Expired, // token did not resolve — treat as gone
          },
      }
  }
  ```

- [ ] **Step 4: Implement `provider_begin` and the `BeginResponse`**

  Add (replacing `provider_start` + `StartParams`):

  ```rust
  #[derive(Deserialize)]
  struct BeginParams {
      origin: String,
  }

  #[derive(serde::Serialize)]
  struct BeginResponse {
      authorize_url: String,
      state: String,
  }

  /// `GET /auth/{provider}/begin?origin=<parent origin>`: validates the origin
  /// against the live allowlist, mints a single-use state token, and returns the
  /// provider authorize URL + the state. The parent opens the authorize URL in a
  /// `noopener` popup and polls `/auth/state`.
  async fn provider_begin<P: OAuthProvider>(
      State(state): State<Arc<AppState>>,
      QueryParams(params): QueryParams<BeginParams>,
  ) -> Response {
      let Some(provider) = P::from_config(&state.config) else {
          return unconfigured_response(P::name());
      };
      if !state
          .runtime
          .hot
          .load()
          .allowed_origins
          .iter()
          .any(|allowed| allowed == &params.origin)
      {
          return RtDbError::forbidden("origin not allowed").into_response();
      }
      let state_token = random_token();
      let now = now_ms();
      {
          let mut states = state.auth.oauth_states.lock().await;
          states.retain(|_, entry| entry.expires_at > now);
          states.insert(
              state_token.clone(),
              OAuthStateEntry {
                  expires_at: now + STATE_TTL_MS,
                  outcome: LoginOutcome::Pending,
              },
          );
      }
      let redirect_uri = format!("{}{}", state.config.public_url, provider.callback_path());
      let authorize_url = provider.authorize_url(&redirect_uri, &state_token);
      Json(BeginResponse {
          authorize_url,
          state: state_token,
      })
      .into_response()
  }
  ```

- [ ] **Step 5: Rework `provider_callback` + the close-HTML response**

  Replace `callback_html_response` with a no-interpolation close response, and rework the handler to claim → complete → set terminal outcome:

  ```rust
  async fn provider_callback<P: OAuthProvider>(
      State(state): State<Arc<AppState>>,
      headers: HeaderMap,
      QueryParams(params): QueryParams<CallbackParams>,
  ) -> Response {
      if !claim_pending(&state, &params.state).await {
          return RtDbError::bad_request("invalid or expired state").into_response();
      }
      let Some(provider) = P::from_config(&state.config) else {
          set_outcome(&state, &params.state, LoginOutcome::Failed).await;
          return unconfigured_response(P::name());
      };
      let secure = crate::auth::cookie::request_is_secure(&headers);
      match provider.complete_login(&state, &params.code).await {
          Ok(token) => {
              set_outcome(&state, &params.state, LoginOutcome::Completed(token.clone())).await;
              callback_close_response(&token, secure)
          }
          Err(err) => {
              set_outcome(&state, &params.state, LoginOutcome::Failed).await;
              err.into_response()
          }
      }
  }

  /// The popup-closing HTML the callback returns on success. Nothing is
  /// interpolated (no `origin`, no token) — the token rides the HttpOnly
  /// Set-Cookie, so there is no self-XSS surface and the parent learns of
  /// completion by polling `/auth/state`, not via `window.opener`.
  fn callback_close_response(token: &str, secure: bool) -> Response {
      let html = "<!doctype html><html><head><meta charset=\"utf-8\"><title>Signed in</title>\
                  </head><body><script>window.close();</script>\
                  <p style=\"font-family:sans-serif\">Sign-in complete. You may close this window.</p>\
                  </body></html>";
      let mut response = Html(html).into_response();
      response.headers_mut().insert(
          HeaderName::from_static("content-security-policy"),
          HeaderValue::from_static("default-src 'none'; script-src 'unsafe-inline'"),
      );
      match crate::auth::cookie::set_session_cookie(token, secure) {
          Ok(cookie) => {
              response.headers_mut().insert(SET_COOKIE, cookie);
              response
          }
          Err(err) => err.into_response(),
      }
  }
  ```

- [ ] **Step 6: Implement `auth_state`**

  ```rust
  #[derive(Deserialize)]
  struct StateQuery {
      state: String,
  }

  #[derive(serde::Serialize)]
  #[serde(tag = "status", rename_all = "lowercase")]
  enum StateResponse {
      Pending,
      Complete { token: String, user: AuthedUser },
      Expired,
      Error,
  }

  /// `GET /auth/state?state=`: provider-agnostic polling endpoint. The `state`
  /// token (minted at `/auth/{provider}/begin`) is the capability — no cookie
  /// required, so this works cross-origin (the SDK on a different origin) where
  /// the `SameSite=Lax` session cookie would not be sent.
  async fn auth_state(
      State(state): State<Arc<AppState>>,
      QueryParams(params): QueryParams<StateQuery>,
  ) -> Response {
      match poll_login(&state, &params.state).await {
          PollResult::Pending => Json(StateResponse::Pending).into_response(),
          PollResult::Complete { token, user } => {
              Json(StateResponse::Complete { token, user }).into_response()
          }
          PollResult::Failed => Json(StateResponse::Error).into_response(),
          PollResult::Expired => Json(StateResponse::Expired).into_response(),
      }
  }
  ```

- [ ] **Step 7: Update `auth_routes` and remove the dead old code**

  Replace `auth_routes()` body's start routes with begin routes + add `/auth/state`:

  ```rust
  pub fn auth_routes() -> Router<Arc<AppState>> {
      Router::new()
          .route("/auth/github/begin", get(provider_begin::<GithubProvider>))
          .route("/auth/callback", get(provider_callback::<GithubProvider>))
          .route("/auth/google/begin", get(provider_begin::<GoogleProvider>))
          .route("/auth/google/callback", get(provider_callback::<GoogleProvider>))
          .route("/auth/gitlab/begin", get(provider_begin::<GitlabProvider>))
          .route("/auth/gitlab/callback", get(provider_callback::<GitlabProvider>))
          .route("/auth/state", get(auth_state))
          .route("/auth/logout", post(logout))
          .route("/auth/me", get(me))
          .route("/auth/validate", get(validate))
  }
  ```

  Remove the now-dead items: `provider_start`, `StartParams`, `consume_state`, `redirect_found` (grep first to confirm it was only used by `provider_start`), `callback_html_response`, `escape_js_string`, and the entire `#[cfg(test)] mod callback_tests`. If removing any leaves an unused import (`Body`, `LOCATION`), remove those imports too. Grep to confirm no remaining callers: `rg -n "provider_start|consume_state|redirect_found|callback_html_response|escape_js_string|StartParams" server/src`.

- [ ] **Step 8: Run the tests to verify they pass**

  Run: `cd server && cargo test --test oauth_test`
  Expected: PASS (all oauth tests green, including pending/error/replay/begin-forbidden).

- [ ] **Step 9: Run the full server gate**

  Run: `cd server && cargo fmt && cargo clippy --all-targets -- -D warnings && cargo test`
  Expected: fmt clean, clippy clean, all tests pass.

- [ ] **Step 10: Commit**

  ```bash
  git add server/src/auth/provider.rs server/tests/oauth_test.rs
  git commit -m "feat(server): state-token OAuth relay (SEC-012)

  Replace the window.opener postMessage relay with begin/noopener-popup/
  poll: GET /auth/{provider}/begin mints state + returns authorizeUrl;
  GET /auth/state returns pending|complete|expired|error. The callback
  marks the entry Completed/Failed and closes the popup (no origin
  interpolation — retires the SEC-005 self-XSS surface). Works cross-origin
  (SameSite=Lax cookie would not be sent) since state is the capability."
  ```

---

## Task 2: ts-client — `signInWithOAuth` (token mode)

**Files:**
- Modify: `ts-client/src/react.tsx`
- Modify/Create: `ts-client/tests/` (a test file covering the rewrite; find the existing auth test and extend it, or add `ts-client/tests/oauth-relay.test.ts`)

**Interfaces:**
- Consumes: `GET /auth/{provider}/begin` → `{authorizeUrl, state}`; `GET /auth/state?state=` → `{status, token?, user?}` (Task 1).
- Produces: `signInWithOAuth(baseUrl, provider): Promise<string>` (the session token); `signInWithGitHub`/`signInWithGoogle` signatures unchanged.

- [ ] **Step 1: Write the failing test** (fetch-mock the begin → pending → complete sequence)

  In the chosen test file, mock global `fetch` to return: begin → `{authorizeUrl, state:"s1"}`; first `/auth/state` → `{status:"pending"}`; second → `{status:"complete", token:"tok", user:{...}}`; mock `window.open` to a no-op stub. Assert `signInWithOAuth(base, "github")` resolves to `"tok"`. Add a case where a poll returns `{status:"expired"}` → rejects. (Use vitest fake timers or a small poll interval override to keep the test fast — if the poll interval is a module constant, export it for tests or inject it.)

- [ ] **Step 2: Run the test to verify it fails**

  Run: `cd ts-client && bunx vitest run tests/<file>.test.ts`
  Expected: FAIL (old impl waits for `data.token` via postMessage; the mock never triggers it).

- [ ] **Step 3: Rewrite `signInWithOAuth`**

  Replace the body of `signInWithOAuth` in `ts-client/src/react.tsx`:

  ```ts
  const OAUTH_POLL_INTERVAL_MS = 800;
  const OAUTH_POLL_TIMEOUT_MS = 180_000;

  /** One poll of `/auth/state`; resolves with the token on `complete`. */
  async function pollOAuthState(apiBase: string, state: string): Promise<
    | { done: true; token: string }
    | { done: false }
  > {
    const resp = await fetch(`${apiBase}/auth/state?state=${encodeURIComponent(state)}`);
    if (!resp.ok) return { done: false };
    const data = (await resp.json()) as { status?: string; token?: string };
    if (data.status === "complete" && typeof data.token === "string") {
      return { done: true, token: data.token };
    }
    if (data.status === "expired" || data.status === "error") {
      throw new Error(data.status === "expired" ? "sign-in expired" : "sign-in failed");
    }
    return { done: false };
  }

  const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

  /**
   * Begins a provider OAuth flow, opens the authorize URL in a `noopener`
   * popup (SEC-012 tabnabbing hardening), and polls `/auth/state` until the
   * session token is ready. `noopener` means `window.open` returns null, so the
   * old postMessage/closed-poll relay is replaced by this poll.
   */
  function signInWithOAuth(baseUrl: string, provider: "github" | "google"): Promise<string> {
    const api = baseUrl.replace(/\/+$/, "");
    const spaOrigin = window.location.origin;
    return (async () => {
      const beginResp = await fetch(
        `${api}/auth/${provider}/begin?origin=${encodeURIComponent(spaOrigin)}`,
      );
      if (!beginResp.ok) {
        throw new Error(`could not start sign-in (${beginResp.status})`);
      }
      const began = (await beginResp.json()) as { authorizeUrl: string; state: string };
      window.open(began.authorizeUrl, "rtdb-auth", "noopener,noreferrer,width=600,height=700");
      const deadline = Date.now() + OAUTH_POLL_TIMEOUT_MS;
      while (Date.now() < deadline) {
        try {
          const r = await pollOAuthState(api, began.state);
          if (r.done) return r.token;
        } catch (err) {
          throw err;
        }
        await sleep(OAUTH_POLL_INTERVAL_MS);
      }
      throw new Error("sign-in timed out");
    })();
  }
  ```

  Leave `signInWithGitHub`/`signInWithGoogle` unchanged (they delegate and return the token). Leave `useRtDbAuth.signIn` unchanged — it already branches on `cookieMode` and does `client.setToken(token)` (token mode) or `client.setToken(null)` (cookie mode).

- [ ] **Step 4: Run the test to verify it passes**

  Run: `cd ts-client && bunx vitest run tests/<file>.test.ts`
  Expected: PASS.

- [ ] **Step 5: Build + lint + typecheck ts-client**

  Run: `cd ts-client && bun run build && bunx vitest run && bunx biome check src`
  Expected: build + tests + lint clean.

- [ ] **Step 6: Commit**

  ```bash
  git add ts-client/src/react.tsx ts-client/tests/
  git commit -m "feat(ts-client): signInWithOAuth via begin+poll, noopener popup (SEC-012)

  Open the authorize URL with noopener,noreferrer and poll /auth/state for
  the token instead of awaiting a window.opener postMessage (which noopener
  severs and which never carried the token post-SEC-001)."
  ```

---

## Task 3: dashboard — `signInWithOAuth` (cookie mode, same-origin)

**Files:**
- Modify: `dashboard/src/lib/session.tsx`

**Interfaces:**
- Consumes: `GET /auth/{provider}/begin` → `{authorizeUrl, state}`; `GET /auth/state?state=` → `{status}`; `GET /auth/me` (cookie-authenticated, same-origin).

- [ ] **Step 1: Rewrite `signInWithOAuth` in `dashboard/src/lib/session.tsx`**

  Replace the existing `signInWithOAuth` (the postMessage + closed-poll version) with a begin + noopener + poll + `/auth/me` version:

  ```ts
  function signInWithOAuth(provider: "github" | "google" | "gitlab"): Promise<void> {
    const origin = window.location.origin;
    const beginUrl = `${origin}/auth/${provider}/begin?origin=${encodeURIComponent(origin)}`;
    const poll = (state: string, deadline: number, resolve: () => void, reject: (e: Error) => void) => {
      if (Date.now() > deadline) {
        reject(new Error("sign-in timed out"));
        return;
      }
      fetch(`${origin}/auth/state?state=${encodeURIComponent(state)}`)
        .then((r) => (r.ok ? r.json() : null))
        .then((data: { status?: string } | null) => {
          if (data?.status === "complete") {
            // The HttpOnly cookie was set by the callback; load the user.
            fetch("/auth/me")
              .then((r) => (r.ok ? r.json() : Promise.reject(new Error("could not load session"))))
              .then((u: AuthedUser) => {
                setError(null);
                setUser(u);
                setMethod("oauth");
                resolve();
              })
              .catch(reject);
          } else if (data?.status === "expired" || data?.status === "error") {
            reject(new Error(`sign-in ${data.status}`));
          } else {
            setTimeout(() => poll(state, deadline, resolve, reject), 800);
          }
        })
        .catch(() => setTimeout(() => poll(state, deadline, resolve, reject), 800));
    };
    return new Promise<void>((resolve, reject) => {
      fetch(beginUrl)
        .then((r) => (r.ok ? r.json() : Promise.reject(new Error("could not start sign-in"))))
        .then((b: { authorizeUrl: string; state: string }) => {
          // noopener: window.open returns null — no blocked-popup detect, no
          // closed-poll; the polling timeout covers blocked/closed/abandoned.
          window.open(b.authorizeUrl, "rtdb-oauth", "noopener,noreferrer,popup,width=560,height=720");
          poll(b.state, Date.now() + 180_000, resolve, reject);
        })
        .catch(reject);
    });
  }
  ```

- [ ] **Step 2: Typecheck the dashboard (against a freshly built ts-client)**

  Run: `cd ~/Repos/par-rt-db && make ts-client-build && cd dashboard && bunx tsc --noEmit`
  Expected: typecheck clean.

- [ ] **Step 3: Commit**

  ```bash
  git add dashboard/src/lib/session.tsx
  git commit -m "feat(dashboard): signInWithOAuth via begin+poll, noopener popup (SEC-012)

  Same-origin cookie-mode mirror of the ts-client change: begin, open the
  authorize URL with noopener, poll /auth/state, then load /auth/me on
  complete. Drops the window.opener postMessage wait."
  ```

---

## Task 4: Docs — FEATURE_MATRIX + CLAUDE.md

**Files:**
- Modify: `FEATURE_MATRIX.md`
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update `FEATURE_MATRIX.md`** — flip SEC-012 to resolved. Add/adjust a note under the auth/security section: "OAuth popup opens with `noopener,noreferrer` (SEC-012 tabnabbing hardening); completion is relayed via `GET /auth/state` polling keyed on the single-use state token (works cross-origin; the `SameSite=Lax` session cookie is not sent cross-site). The callback HTML no longer interpolates the parent origin (SEC-005 surface retired)."

- [ ] **Step 2: Update `CLAUDE.md`** — in the Auth section, replace the postMessage-relay description with: the OAuth flow is `GET /auth/{provider}/begin` (mints state, returns `authorizeUrl`) → parent opens `authorizeUrl` with `noopener,noreferrer` → `GET /auth/{provider}/callback` sets the cookie + closes the popup → parent polls `GET /auth/state?state=` (`pending|complete|expired|error`). Note the route list change (`/auth/{provider}` 302 start removed; `/auth/{provider}/begin` + `/auth/state` added).

- [ ] **Step 3: Commit**

  ```bash
  git add FEATURE_MATRIX.md CLAUDE.md
  git commit -m "docs: SEC-012 OAuth relay redesign (begin + /auth/state poll)"
  ```

---

## Final verification

- [ ] Run the whole gate: `cd ~/Repos/par-rt-db && make dev-db-up && make checkall`
  Expected: fmt-check + clippy `-D warnings` + typecheck (server, ts-client, rust-client, dashboard, python-client) + all tests green. (If `oauth_test` intermittently `PoolTimedOut`s under full-suite load, retry — known transient contention, not a regression.)
- [ ] After the gate is green and the branch is reviewed (SDD final review), merge to `main` (squash) and push, then `make deploy`.
- [ ] **After deploy**, sync the projects vendored client: `cd ~/Repos/projects && make sync-client`, commit, and close the projects SEC-012 card (`019fbe75ff1073d3a4ee12c6e5cd38bd`).
