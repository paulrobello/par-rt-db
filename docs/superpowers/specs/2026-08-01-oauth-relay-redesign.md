# OAuth Popup Relay Redesign (SEC-012) — Design

**Date:** 2026-08-01
**Status:** Implemented (2026-08-10)
**Kanban:** `par-rt-db` — "[SEC-012] OAuth popup noopener breaks the postMessage relay" (`019fc00ba23d7ff29a922ce34196adf2`)
**Severity:** Low (tabnabbing hardening for a trusted same-operator popup)

## Motivation

`signInWithOAuth` opens the OAuth popup **without** `noopener` so the server
callback page can relay completion back to the opener via
`window.opener.postMessage({type:"rtdb-auth"}, origin)`. `noopener` is the
standard mitigation for **reverse tabnabbing** (a malicious opened page
reaching back into `window.opener` to navigate or spoof the parent). Adding it
is the SEC-012 fix — but it breaks the relay in both directions:

- In the popup, `window.opener` is `null`, so the callback page cannot post a
  completion message to its opener.
- In the parent, `window.open(..., "noopener")` returns `null`, so the parent
  has no handle to poll `popup.closed` or to message the popup.

So the current relay cannot be patched with a one-line `noopener` addition; the
completion signal needs a channel that does not rely on either window handle.

A second, pre-existing defect surfaces in the same area: since SEC-001 phase 2
the callback carries the session token in the HttpOnly `Set-Cookie` only and the
`postMessage` payload is `{type:"rtdb-auth"}` with **no** `token` field — but
`ts-client/src/react.tsx::signInWithOAuth` still awaits `data.token` (a string).
That token-mode path therefore never resolves; it hangs until the popup's
closed-poll rejects. This redesign fixes both.

## Goals

- Add `noopener,noreferrer` to every OAuth popup open (tabnabbing hardening).
- Replace the `window.opener`/handle relay with a channel that works without
  either window handle, for **both** the same-origin dashboard (cookie mode) and
  cross-origin SDK consumers (token mode, e.g. projects.example.com).
- Preserve every existing invariant: HttpOnly cookie delivery (SEC-001/SEC-002),
  single-use TTL-bounded OAuth state, origin allowlist, CSRF defense, and the
  provider-pluggable `OAuthProvider` trait shape.

## Non-goals

- Changing session-cookie `SameSite`. It stays `Lax` (this is exactly why
  cookie-keyed polling is rejected — see Approach).
- Adding new OAuth providers (separate backlog item).
- A server-side opaque "device code" distinct from the OAuth `state` token; the
  state token already is the capability (single-use, TTL-bounded, unguessable).

## Why not cookie-keyed polling (the original note)

The kanban analysis proposed "the parent polls a cookie-authenticated
`GET /auth/state`." The session cookie is `SameSite=Lax` (`auth/cookie.rs`), so
the browser attaches it **only** to same-site requests. The dashboard is
same-origin with the server, so that works there — but a cross-origin consumer
(`projects.example.com` polling `rtdb.example.com`) would never send the cookie,
and the poll could never see the session. Making it work cross-origin would
require `SameSite=None; Secure`, broadening cookie exposure for every route. The
state-token-keyed design below works for both without touching cookie scope.

## Approach — state-token polling (device-code style)

The OAuth `state` token already binds the login start to its callback and is
held by the parent (it mints/knows it at the begin step). It becomes the polling
capability. The popup needs no `window.opener` at all.

### Flow

1. **`GET /auth/{provider}/begin?origin=<parent origin>`** (new; replaces the
   302 `provider_start`). Validates `origin` against the live
   `hot.allowed_origins` (else 403), mints a `state_token`, stores an entry, and
   returns JSON `{ authorizeUrl, state }`. The `authorizeUrl` is the provider's
   authorize URL with `redirect_uri` and `state` already embedded.
2. Parent opens the popup **directly to `authorizeUrl`** with
   `noopener,noreferrer`. The popup navigates to the provider; no server page is
   loaded in the popup first (one redirect fewer than today).
3. Provider redirects to **`GET /auth/{provider}/callback?code=&state=`**
   (unchanged URL). The handler looks the entry up by `state` (it does **not**
   remove it yet); if the entry is already `completed` → 400 (single-use
   callback); otherwise marks it `completed`, runs `complete_login`, stores the
   resulting session token on the entry, sets the HttpOnly session cookie, and
   responds with minimal HTML that just closes the popup — no `postMessage`, no
   `origin` interpolation.
4. Parent **polls `GET /auth/state?state=<state_token>`** (the capability is the
   query param, consistent with the existing state-in-URL pattern):
   - entry absent / expired → `{ "status": "expired" }` (parent stops, rejects);
   - `Pending` / `Claiming` → `{ "status": "pending" }`;
   - `Completed` → `{ "status": "complete", "token": "<session>", "user": {...} }`,
     entry removed (one-shot retrieval);
   - `Failed` → `{ "status": "error" }`, entry removed (parent rejects promptly
     instead of hanging until timeout).

   Poll cadence ~800 ms; the parent gives up after ~180 s (well under the state
   TTL of 10 min) and rejects "sign-in timed out". A `Failed` outcome comes from
   a `complete_login` error in the callback (e.g. the IdP rejected the code).

### Forced tradeoff: lost blocked-popup detection

With `noopener`, `window.open` returns `null` on success *and* on block, so the
parent can neither detect a blocked popup nor poll `popup.closed`. The single
polling timeout now covers blocked / closed / abandoned uniformly. This is
inherent to `noopener` and acceptable for a low-severity hardening of an
operator/trusted popup. There is no message listener and no closed-poll in the
new client.

## Server changes (`server/src/auth/provider.rs`)

### `OAuthStateEntry`

```rust
pub struct OAuthStateEntry {
    pub expires_at: i64,           // STATE_TTL_MS (10 min) from begin
    pub outcome: LoginOutcome,     // Pending until the callback terminal-transitions it
}
// `origin` is validated against the allowlist at `begin` and then discarded —
// nothing reads it after that (the callback no longer interpolates it), so it
// is not stored (no dead field).

pub enum LoginOutcome {
    Pending,
    Completed(String), // session token — the poll retrieves + removes the entry
    Failed,            // complete_login errored — the poll surfaces this, not pending
}
```

`consume_state` (destructive remove) is replaced by two non-destructive
operations, both under the existing `oauth_states` `Mutex`:

- `claim_pending(state) -> bool` (used by the callback to win the entry
  atomically: returns `true` the first time, `false` for a replay or an already
  terminal entry). Implemented as: lock, `get_mut`, if `outcome == Pending` →
  leave it Pending and return `true`; else return `false`. (The entry stays
  `Pending` during the exchange; the callback sets the terminal outcome after.)
  `complete_login` then runs **outside** the lock (it is slow / networked); a
  second callback while the first is mid-exchange calls `claim_pending`, sees
  `Pending`, and... 

  Correction — that race would let two callbacks both exchange. So `claim_pending`
  instead flips `Pending → Claiming` (an in-flight marker) on the first call and
  returns `true`; a second sees non-`Pending` and returns `false`. Then the first
  callback sets `Claiming → Completed(token)` or `Claiming → Failed` after
  `complete_login`.

  Final shape: `LoginOutcome = Pending | Claiming | Completed(String) | Failed`.
  `claim_pending` is the only `Pending → Claiming` transition (first-wins);
  `complete_login`'s result drives `Claiming → Completed | Failed`.
- After `complete_login`: lock, `get_mut`, set the terminal outcome (`Completed`
  on Ok, `Failed` on Err). A failure must terminal-transition so the poll does
  not hang on `Pending` (the correctness gap this avoids).

`poll(state) -> PollResult` serves the poll: lock, `get_mut` (or `remove` for the
terminal cases):
- entry absent / `expires_at` passed → `Expired`;
- `Pending` or `Claiming` → `Pending` (entry left in place);
- `Completed(token)` → remove, resolve user via the same
  `resolve_bearer`/`authed_user` path `/auth/me` uses, return
  `Complete(token, user)`;
- `Failed` → remove, return `Failed`.

### Handlers

- **`provider_begin<P>`** (`GET /auth/{provider}/begin?origin=`): validates
  origin, mints `state_token`, inserts the entry, returns
  `Json(BeginResponse { authorize_url, state })`. `authorize_url` is built with
  `provider.authorize_url(&redirect_uri, &state_token)`.
- **`provider_callback<P>`** (`GET /auth/{provider}/callback`): the existing
  route, reworked — `claim_pending(state)` (else 400 "invalid or expired state";
  this is also the replay rejection), then `complete_login`; on `Ok` set
  `Completed(token)` + Set-Cookie + `callback_close_html()`; on `Err` set
  `Failed` and return the error response (no `window.close()` — the popup shows
  the error). The callback no longer reads or interpolates `origin`.
- **`auth_state`** (`GET /auth/state?state=`): provider-agnostic; returns
  `pending` / `complete` (with token + user) / `expired` / `error`.
- **Removed:** `provider_start` (the 302) and its `StartParams`. The popup no
  longer loads a server page first; the parent opens `authorizeUrl` directly.

### Callback HTML

```html
<!doctype html><html><head><meta charset="utf-8">
<title>Signed in</title></head><body>
<script>window.close();</script>
<p style="font-family:sans-serif">Sign-in complete. You may close this window.</p>
</body></html>
```

With the `default-src 'none'; script-src 'unsafe-inline'` CSP retained. Because
nothing is interpolated, the SEC-005 self-XSS escape surface (origin
interpolation into a JS string literal) is **retired**: `escape_js_string` and
its `callback_tests` module are removed (they tested behavior that no longer
exists). This is the only orphan created by the change and is cleaned up per the
surgical-changes rule.

### Routes

`auth_routes()` gains `/auth/{provider}/begin` (per provider: github, google,
gitlab) and `/auth/state`; the per-provider `provider_start` route is removed.
`/auth/{provider}/callback`, `/auth/logout`, `/auth/me`, `/auth/validate` are
unchanged. CORS already covers `/auth/*` globally (the `CorsLayer` predicate on
`allowed_origins`), so cross-origin `begin`/`state` GETs work for an allowed
parent with no credentials needed (the state token is the capability).

## Client changes

### `ts-client/src/react.tsx` (token mode; cross-origin consumers)

`signInWithOAuth(baseUrl, provider)` is rewritten to:

1. `fetch(${baseUrl}/auth/${provider}/begin?origin=${encodeURIComponent(spaOrigin)})`
   → `{ authorizeUrl, state }`.
2. `window.open(authorizeUrl, "rtdb-auth", "noopener,noreferrer,width=600,height=700")`
   (return value ignored — always `null` under `noopener`).
3. Poll `${baseUrl}/auth/state?state=${state}` every 800 ms: on `complete` →
   resolve `{ token, user }`; on `expired` → reject; on network error → keep
   polling; after 180 s → reject "timed out".

The `useRtDbAuth.signIn` wrapper branches on `client.cookieMode` exactly as
today: cookie mode calls `client.setToken(null)` (the HttpOnly cookie
authenticates); token mode persists + `setToken(token)`. The broken `data.token`
wait is gone. `signInWithGitHub` / `signInWithGoogle` keep their signatures
(returning `Promise<string>` — the token; in cookie mode a caller that ignores
it is fine).

### `dashboard/src/lib/session.tsx` (cookie mode; same-origin)

`signInWithOAuth` adopts the same begin + `noopener` + poll shape, then loads
the user from the poll's `complete` response (or, equivalently, `/auth/me` with
the now-set cookie). It drops the `message` listener and closed-poll. The popup
open gains `noopener,noreferrer`.

### rust-client / python-client

Untouched. They have no browser OAuth popup (the `/auth/` references there are
`/auth/validate`, the backend player-token validation route).

## Security analysis

- **State token as capability.** `/auth/state` returns the session token to
  whoever presents the matching `state`. `state` is a 128-bit server-minted
  random (`random_token`), unguessable, single-retrieval, TTL-bounded (10 min).
  It is already transmitted in URLs (the callback redirect, the provider
  authorize URL) today, so query-param polling adds no new exposure surface; it
  is over HTTPS; and one-shot retrieval bounds any log-leak window.
- **Origin allowlist.** Still enforced at `begin` (the only place `origin`
  enters), exactly as `provider_start` does today. The callback and the poll
  never trust an `origin` parameter.
- **Single-use callback.** `mark_started` makes the first callback win and any
  replay reject, preserving today's replay protection. The provider `code` is
  single-use at the IdP regardless.
- **CSRF.** Unchanged: the state token remains the double-submit-style bind
  between begin and callback.
- **SEC-005 (callback self-XSS).** Retired — no interpolation in the callback
  HTML.
- **SEC-001/SEC-002 (HttpOnly cookie).** Unchanged; the callback still sets it,
  the dashboard still relies on it, and token mode never reads script-readable
  storage it did not already read.

## Testing

**Server (`server/tests/oauth_test.rs`, real or mocked IdP as today):**

- Update `start_login` → `begin_login`: GET `/auth/github/begin?origin=` returns
  200 JSON `{authorizeUrl, state}`; extract `state` from the body.
- Update `login_flow`: begin → callback (assert 200, `text/html`, the CSP, the
  Set-Cookie carries the token, the body does **not** contain the token) → poll
  `/auth/state?state=` until `complete` and assert the token/user come back and
  the entry is gone on the next poll.
- `replayed_state_returns_bad_request`: a second callback with the same `state`
  returns 400 (the first marked it completed); the poll still retrieves the
  token once.
- `github_begin_with_disallowed_origin_returns_forbidden` (renamed from
  `..._start_...`): a disallowed origin at `begin` returns 403.
- `/auth/state` before the callback completes returns `pending`; for an
  unknown/expired state returns `expired`.
- A `complete_login` failure (e.g. mocked IdP rejects the code) terminal-transitions
  the entry to `Failed`; the poll returns `error` and removes the entry (the
  parent rejects promptly, does not hang to timeout).
- Unit tests in `provider.rs` for the new entry transitions (`claim_pending`
  first-wins, `Pending→Claiming→Completed/Failed`, poll one-shot) replace the
  removed `callback_tests`.

**ts-client (`ts-client/tests/`):** unit-test the rewritten `signInWithOAuth`
against a fetch-mock: `begin` → poll sequence → resolves with the token on
`complete`, rejects on `expired`, rejects on timeout. (A live-server variant can
stay `#[ignore]`-style if one exists.)

**dashboard:** the change is small and covered by the server contract; no new
dashboard test scaffold is required, but the begin/poll helpers mirror the
ts-client ones.

## Docs to update

- `FEATURE_MATRIX.md` — flip SEC-012 to resolved (note: popup `noopener` +
  state-token polling; callback self-XSS surface retired).
- `CLAUDE.md` — update the Auth-section note: the OAuth relay is now
  begin → noopener popup → `/auth/state` poll (no `window.opener` postMessage);
  `/auth/{provider}/begin` + `/auth/state` join the route list; callback HTML no
  longer interpolates origin.
- Client READMEs that document `signInWithOAuth` (ts-client), if any.
- This spec.

## Kanban / cross-repo

Mark par-rt-db SEC-012 `done` only after `make checkall` passes and the server +
ts-client + dashboard + docs are updated. Then, **after deploy**, rebuild the
projects vendored client (`make sync-client` in `~/Repos/projects`) to close the
projects SEC-012 card (`019fbe75ff1073d3a4ee12c6e5cd38bd`), which is blocked on
this.
