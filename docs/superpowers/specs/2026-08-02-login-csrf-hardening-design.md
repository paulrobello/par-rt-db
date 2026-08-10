# Login-CSRF Hardening — Design

**Status:** Implemented (2026-08-10).
**Date:** 2026-08-02.
**Repo:** `~/Repos/par-rt-db`.
**Supersedes:** the "Accepted residual risk (login CSRF)" paragraph in
`2026-07-21-par-rt-db-design.md` §Auth and the CSRF posture carried forward by
`2026-08-01-oauth-relay-redesign.md`.
**Feature matrix:** Auth section (per-row / OAuth login row).

## 1. Problem

The OAuth `state` token is bound to the initiating **origin** (validated against
the live `allowed_origins` allowlist at `GET /auth/{provider}/begin`) but **not**
to the initiating **browser** — there is no PKCE and no state cookie. The accepted
residual risk is a **login-CSRF / account-confusion** attack:

1. The attacker starts their own OAuth flow on the par-rt-db instance; the server
   mints `state_A` and stores a pending `OAuthStateEntry`.
2. The attacker completes the provider authorize step in their own browser,
   obtaining a valid `code` bound to `state_A`, but does **not** finish the
   callback themselves.
3. The attacker induces a **victim's** browser to load
   `/auth/{provider}/callback?code=<attacker's code>&state=state_A` at an
   allowed origin.
4. The server completes the exchange, mints a session for the **attacker's**
   identity, and sets the session cookie / hands the token to whoever polls
   `/auth/state?state=state_A`. If the victim's SPA is the poller, the victim is
   now logged in **as the attacker**.

This does **not** compromise the victim's own account (no secret is leaked to the
attacker), but it is a planted-session / account-confusion vulnerability. The
MVP spec accepted it for a personal, allowlisted deployment; this spec closes it.

## 2. Why a state cookie (and not the alternatives)

**Double-submit nonce cookie — chosen.** Bind `state` to a browser cookie the
attacker cannot forge for the victim. At `begin`, set a short-lived cookie whose
value is the `state_token`; at `callback`, require the cookie to match the `state`
query param. An attacker can know `state_A` but cannot write a cookie into the
victim's `rtdb.pardev.net` cookie jar (only an `rtdb.pardev.net` response can, and
the attacker does not control that origin), so the victim's callback carries no
matching cookie and is rejected.

**PKCE — rejected for this threat.** PKCE binds the authorization code to a
server-held `code_verifier`, defending **code interception**. It does **not**
defend login-CSRF: in the attack above the attacker is using their *own*
code/verifier pair, which the server happily completes. PKCE remains worth adding
*separately* (code interception is a real but lower-priority threat given
server-side HTTPS exchange) — filed as a follow-up, out of scope here.

**Referer / tightening the origin check at callback — rejected.** The callback is
a top-level cross-site redirect; it carries no origin evidence a server can trust
(`Referer` is unreliable and often absent). The origin allowlist already runs at
`begin`; a browser-bound cookie is the sound binding at the callback, which is the
single point where a session is minted.

## 3. Design

### 3.1 The CSRF cookie

A dedicated, throwaway nonce cookie, distinct from the session cookie
(`rtdb_session`). It is **not** the session credential and carries no capability
beyond proving the browser that hits `callback` is the one that hit `begin`.

| Attribute | Value | Reason |
|---|---|---|
| Name | `rtdb-oauth-csrf` | `__Host-` prefix is unavailable (it requires `SameSite=Strict`; we need `None` for cross-origin consumers). |
| Value | the `state_token` | Textbook double-submit; `state_token` is already unguessable + single-use. `HttpOnly` so JS cannot read it; no conflict with `/auth/state` polling, which carries `state` as a URL param the SPA already holds from the `begin` JSON. |
| `HttpOnly` | yes | JS can never read the nonce. |
| `SameSite` | `None` | The provider → callback redirect is a top-level **cross-site** navigation; `Lax` would not attach the cookie to it, and the cookie would never reach the callback. `None` is required for the defense to function at all. |
| `Secure` | when `request_is_secure` | Mirrors the session cookie: omitted for local http dev so the cookie is still accepted, present behind the Cloudflare tunnel (`X-Forwarded-Proto: https`). |
| `Path` | `/` | Must reach `/auth/{provider}/callback`. |
| `Max-Age` | `600` (10 min) | Matches `STATE_TTL_MS`; the cookie lives exactly as long as the pending state entry. |

Exposure note: `SameSite=None` broadens which requests attach this cookie, but the
cookie is a single-use, 10-minute, HttpOnly nonce with no bearer value. This is
deliberately narrower than the `2026-08-01` relay redesign's reason for keeping
the **session** cookie `Lax` — that decision (don't make the *session* cookie
`SameSite=None`) is unchanged.

### 3.2 Server changes

**`server/src/auth/cookie.rs`** — three helpers mirroring the existing
session-cookie shape:

- `pub(crate) const OAUTH_CSRF_COOKIE: &str = "rtdb-oauth-csrf";`
- `oauth_csrf_cookie(headers: &HeaderMap) -> Option<&str>` — same `Cookie:`-header
  parser as `session_cookie`.
- `set_oauth_csrf_cookie(value: &str, secure: bool) -> Result<HeaderValue, RtDbError>`
  — `{name}={value}; HttpOnly; SameSite=None; Path=/; Max-Age=600` plus `; Secure`
  when `secure`. Same injection-char validation as `set_session_cookie` (rejects
  `; ,` whitespace/control/empty — fails closed).
- `clear_oauth_csrf_cookie() -> HeaderValue` — `Max-Age=0; Expires=...1970...` for
  the success callback (single-use hygiene).

**`server/src/auth/provider.rs`**

- `provider_begin` gains a `headers: HeaderMap` extractor. After the `state_token`
  is minted and the entry stored, it computes `secure = request_is_secure(&headers)`
  and inserts `SET_COOKIE: set_oauth_csrf_cookie(&state_token, secure)` into the
  `BeginResponse` JSON response. (Inserting a `Set-Cookie` header on a `Json`
  response: build the `Response`, then `headers_mut().insert(SET_COOKIE, …)`.)
- `provider_callback` (already takes `headers: HeaderMap`): **before** `claim_pending`,
  when enforcement is on, verify the cookie:
  ```text
  if oauth_login_csrf_enabled {
      match oauth_csrf_cookie(&headers) {
          Some(c) if constant_time_eq(c.as_bytes(), params.state.as_bytes()) => {}
          _ => return 400 BAD_REQUEST "login CSRF check failed",
      }
  }
  ```
  The check runs **before** the entry is claimed, so a rejected callback leaves the
  state `Pending` and a legitimate retry (from a real `begin`) still works. Use a
  constant-time comparison for hygiene even though the state is not a long-lived
  secret.
- `callback_close_response`: on the success path, emit **both**
  `set_session_cookie(token, secure)` and `clear_oauth_csrf_cookie()`. Two
  `Set-Cookie` headers on one response requires `headers_mut().append(SET_COOKIE, …)`
  (`insert` would replace); the impl uses `append` for the second.

**`server/src/config.rs` + `server/src/lib.rs`** — new boot `Config` field:

- `oauth_login_csrf: bool`, env `RTDB_OAUTH_LOGIN_CSRF`, **default `true`**.
  This is the break-glass kill-switch. Read from `state.config` in
  `provider_callback` (boot config — changing it is a restart, matching how the
  other boot security gates like `RTDB_AUDIT_LOG_ENABLED` behave). When `false`,
  the cookie is still **set** at `begin` (so flipping back to `true` needs no
  client change) but **not verified** at `callback` — today's exact behavior.

**`server/src/lib.rs` `cors_layer`** — add `.allow_credentials(true)`. This is
required for a **cross-origin** consumer (e.g. `projects.pardev.net`) to have the
CSRF cookie *stored* by its credentialed `/begin` fetch. It is safe because the
origin check is already an exact-match `AllowOrigin::predicate` that echoes the
specific request origin (never the `*` wildcard), which is the CORS-spec
requirement for credentialed responses. The same-origin dashboard needs no
credentials change.

### 3.3 Data flow

1. SPA → `GET /auth/{provider}/begin?origin=` (cross-origin fetch uses
   `credentials: 'include'`). Server: allowlist-check origin → mint `state` →
   store entry → **`Set-Cookie: rtdb-oauth-csrf=<state>`** → return
   `{ authorizeUrl, state }`.
2. SPA opens `authorizeUrl` in a `noopener` popup; user consents at the provider.
3. Provider 302 → `GET /auth/{provider}/callback?code=&state=`. This is a
   top-level **cross-site** navigation, so the browser attaches the
   `SameSite=None` cookie. Server: **verify cookie == state** (else 400) →
   `claim_pending` → exchange → mint session → `Set-Cookie: rtdb_session` →
   **clear `rtdb-oauth-csrf`** → return popup-closing HTML.
4. Parent polls `GET /auth/state?state=` (unchanged). The token is delivered via
   the cookie (same-origin dashboard) and via the poll JSON (cross-origin token
   mode), exactly as today.

The `/auth/state` poll and the session cookie's `SameSite=Lax` posture are
untouched. The CSRF gate is entirely at the callback — the single point where a
session is minted.

### 3.4 Client mirroring

The wire protocol is unchanged. The only client-side change is the `/begin` fetch
must be made with **`credentials: 'include'`** so the cookie is stored for
cross-origin consumers (and is a harmless no-op for the same-origin dashboard,
which already runs credentialed). To be applied during implementation:

- `ts-client` — the auth `begin` helper; verify and add `credentials: 'include'`.
- `dashboard` — set `credentials: 'include'` on the `begin` call for uniformity
  (same-origin already works).
- `rust-client` / `python-client` — confirm whether their auth surfaces call
  `begin` at all. The CLI/admin-key path does not use the browser OAuth popup, so
  they may need no change; mirror only where a `begin` call exists.

No client wire-type or DSL change is required.

## 4. Error handling & failure modes

- **Missing/mismatched cookie at callback** → `400 BAD_REQUEST`, generic message
  "login CSRF check failed" (no secret leakage). Entry is not claimed; a legit
  retry from a real `begin` still succeeds.
- **Kill-switch off** (`RTDB_OAUTH_LOGIN_CSRF=false`) → cookie still set but not
  verified → today's exact behavior. Pure break-glass; flipping back to `true`
  needs no client change because the cookie is always set.
- **Cookie blocked** (a consumer that did not send `credentials: 'include'`, or a
  rare browser config) → login fails **loudly** at the callback (400) rather than
  silently. The dashboard and updated official clients will not hit this.
- **Cookie value injection** → rejected by `set_oauth_csrf_cookie`'s char
  validation (fails closed), mirroring `set_session_cookie`.

## 5. Testing

Server (`server/tests/`):

- `login_csrf_rejected_without_cookie` — `callback` with no `rtdb-oauth-csrf`
  cookie → `400`.
- `login_csrf_rejected_with_wrong_cookie` — `callback` with a cookie whose value
  differs from `state` → `400`.
- `login_csrf_kill_switch_disables_check` — same two cases with
  `RTDB_OAUTH_LOGIN_CSRF=false` → both succeed (today's behavior).
- `login_csrf_happy_path` — `begin` sets the cookie; `callback` with a matching
  cookie → session minted and `rtdb-oauth-csrf` cleared.
- `begin_sets_csrf_cookie_attributes` — assert the `Set-Cookie` carries
  `SameSite=None`, `HttpOnly`, `Max-Age=600`, and `Secure` only when
  `X-Forwarded-Proto: https`.

Unit (`server/src/auth/cookie.rs` `#[cfg(test)]`), mirroring existing cookie
tests:

- `set_oauth_csrf_cookie_includes_attributes` — `SameSite=None`, `HttpOnly`,
  optional `Secure`.
- `set_oauth_csrf_cookie_rejects_injection_chars` — `; ,` space/control/empty
  rejected.
- `oauth_csrf_cookie_reads_among_pairs` / `missing` / `no_prefix-match`.

## 6. Docs & config-drift

- `.env.example` **and** `docker-compose.yml` environment block: add
  `RTDB_OAUTH_LOGIN_CSRF=true` (the repo's env-drift-check requires both, or
  `make checkall` fails).
- `CLAUDE.md` Auth section: replace the "Accepted residual risk (login CSRF)"
  framing with "defended by a double-submit nonce cookie; disable via
  `RTDB_OAUTH_LOGIN_CSRF`"; note the new `allow_credentials` CORS posture.
- `FEATURE_MATRIX.md` Auth row: note the hardening.
- `docs/superpowers/SPEC_STATUS.md`: add a row for this spec (Implemented once
  shipped), and — separately — fix the two stale rows noted in this session
  (per-row auth v3 `authorize` predicate is shipped; the Python reactive WS
  client is shipped).

## 7. Security review flag

This is an auth security change. Per repo policy it ships **default-on** with an
explicit break-glass env (`RTDB_OAUTH_LOGIN_CSRF`), preserves all existing
configuration (every current setting continues to work; flipping the switch
restores prior behavior exactly), and must be **flagged for manual review at the
code-commit step** — not committed silently inside a larger change. The existing
OAuth invariants it must not regress: HttpOnly cookie delivery (SEC-001/002),
single-use TTL-bounded `state`, origin allowlist, `noopener,noreferrer` popup
(SEC-012), and the state-token-keyed cross-origin poll.

## 8. Out of scope

- **PKCE** (code-interception defense) — filed as a separate follow-up.
- Native **Sign-in-with-Apple** (separate JWT client-secret provider).
- The `SameSite` posture of the **session** cookie (stays `Lax` by design).
