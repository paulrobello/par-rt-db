# Active-Session Management — Design

**Date:** 2026-08-08
**Status:** Implemented (2026-08-10)
**Card:** kanban `par-rt-db` — *Active-session management (list + revoke from console)*
Supersedes the card's endpoint sketch where noted (§5 deviations).

## 1. Problem

`rtdb_auth.sessions` already stores every interactive session (`token_hash`, `user_id`,
`expires_at`, `created_at`) and `authorize` resolves/checks a session on use — but there is
**no operator surface** to see active sessions or force one (or a whole user's sessions) to
end. Machine tokens can be revoked from the console; interactive OAuth/anonymous sessions
cannot. This is an ops + security gap vs. Convex's dashboard.

A second, hidden gap surfaced during grounding: **session revocation is not currently live.**
The WS handler resolves a `Principal` once at handshake (`ws.rs` `authenticate`) and reuses it
for the connection's lifetime. On each op it re-runs `authorize`, but the `User` arm checks a
*cached* `expires_at` plus the allowlist — it never re-queries `rtdb_auth.sessions` (unlike
machine tokens, which run a live `SELECT EXISTS` per op, `auth/mod.rs:134`). Admin principals
bypass `authorize` entirely per op (`ws.rs` Subscribe/Mutate arms). So deleting a session row
has **no effect on an already-open connection** until it reconnects.

This spec closes both gaps: an admin list/revoke surface **and** the per-op live check that
makes a revoke actually take effect on open connections.

## 2. Goals / non-goals

**Goals**
- Admin can list active sessions (server-wide, newest-first) and revoke one or all of a user's
  sessions over HTTP.
- A revoked session is rejected on the next WS op over an already-open connection (live
  revocation), proven by an integration test.
- Dashboard Sessions page lists + revokes with a confirm guard; ts-client admin gains the
  matching methods.

**Non-goals**
- No anon→real session merge, no session metadata (IP/UA), no per-session scopes.
- No new WS frames or protocol fields — revocation rides the existing per-op auth gate.
- No change to client behavior; the SDK only gains admin helpers.

## 3. Live session revocation (correctness core)

This is the load-bearing change; without it the admin surface would be cosmetic.

### 3.1 Carry the session key on the principal

Add a field to `Principal::User` (`auth/mod.rs`):

```rust
Principal::User {
    user_id: String,
    email: Option<String>,
    name: Option<String>,
    expires_at: i64,
    anonymous: bool,
    github_id: Option<i64>,
    github_login: Option<String>,
    session_hash: Option<String>, // NEW: sha256 digest == rtdb_auth.sessions.token_hash PK
    // …
}
```

`session::resolve_session` already computes `sha256_hex(token)`; it sets `session_hash = Some(hash)`.
`Principal` is `Debug + Clone` and **not** `Serialize` — it never crosses the wire, and
`authed_user()` does not read the field — so the ripple is contained to `resolve_session` and the
server-side `Principal::User` test literals. `session_hash` is `Option` only so test/fixture
principals can omit it with `None`; every real resolved `User` carries `Some`.

### 3.2 Per-op liveness check

Add a helper in `auth/mod.rs`:

```rust
/// Live check that the session backing `principal` still exists (has not been
/// revoked). Mirrors the machine-token per-op re-check: a session deleted via
/// the admin surface must be denied on its very next op over an already-open
/// `/sync`. `Ok(())` for principals not backed by a session row.
pub async fn session_still_valid(pool: &PgPool, principal: &Principal) -> Result<(), RtDbError> {
    let Some(hash) = principal.session_hash() else { return Ok(()) };
    let (live,): (bool,) = sqlx::query_as(
        "SELECT EXISTS(SELECT 1 FROM rtdb_auth.sessions WHERE token_hash = $1)",
    )
    .bind(hash).fetch_one(pool).await?;
    if live { Ok(()) } else { Err(RtDbError::unauthorized("session revoked")) }
}
```

`session_hash()` is a small accessor on `Principal` returning `Option<&str>` (`None` for
`Machine`, `Some`/`None` for `User`).

**Where it runs** — every interactive op, admin or not:
- Fold the check into the `User` arm of `authorize` (covers the non-admin Subscribe/Mutate and
  the entire schedule family + presence-join, which all route through `authorize`).
- The Subscribe and Mutate arms **also** short-circuit `authorize` for admins (`if admin { Ok(()) }`).
  Add `session_still_valid(&state.pool, principal).await?` into those two admin branches so an
  admin whose session is revoked is also kicked on the next op.

Net cost: one PK `EXISTS` per interactive op. This is already the established cost profile —
machine tokens do the same (`auth/mod.rs:134`), and non-admin `User` ops already pay an allowlist
`EXISTS` per op. Expiry continues to be gated by the cached, immutable `expires_at` comparison
already in `authorize`; the new check is purely for revocation (row deletion) and is orthogonal
to expiry.

### 3.3 Effect

`DELETE FROM rtdb_auth.sessions WHERE token_hash = $1` (the admin revoke) → the next
Subscribe/Mutate/Schedule on that connection: `session_still_valid` returns `Unauthorized` →
`SubscribeErr`/`MutateErr`/`ScheduleErr`, **connection stays open** (matches the existing
mid-session-revocation behavior for tokens/allowlist). Anonymous sessions (which also have a
row) are revocable the same way.

## 4. Admin HTTP surface

New submodule `server/src/admin/sessions.rs`, mirrored from `admin/tokens.rs` (`mod sessions;
use sessions::*;` in `admin/mod.rs`; routes registered in `admin_routes()`). Every handler starts
with `require_admin(&state, &headers).await?`.

### 4.1 Underlying functions in `auth/session.rs`

- `list_sessions(pool, user_filter: Option<&str>, limit: i64) -> Vec<SessionRow>`
  ```sql
  SELECT s.token_hash, s.user_id, s.created_at, s.expires_at,
         u.email, u.login, u.anonymous
  FROM rtdb_auth.sessions s
  JOIN rtdb_auth.users u ON u.id = s.user_id
  [WHERE s.user_id = $1 OR u.email = $1]   -- only when a filter is given
  ORDER BY s.created_at DESC
  LIMIT $n
  ```
  The `user` query param matches either `user_id` or `email` (OR), so an operator can paste
  either. `login` is returned as a display hint (GitHub handle when `github_id` is set, else the
  stored display name — same convention as `resolve_session`).
- `delete_session_by_hash(pool, &hash) -> u64` — `DELETE … WHERE token_hash = $1` (returns row
  count; idempotent — 0 is not an error).
- `delete_sessions_for_user(pool, &user_id) -> u64` — `DELETE … WHERE user_id = $1`.

### 4.2 Routes

| Method + path | Handler | Body / query | Returns |
|---|---|---|---|
| `GET /admin/sessions` | `list_sessions_handler` | `?user=&limit=` (both optional; `limit` default 200, capped 1000) | `{ sessions: SessionRow[] }` |
| `DELETE /admin/sessions/{token_hash}` | `revoke_session_handler` | path param | `{ ok: true }` |
| `DELETE /admin/sessions` | `revoke_user_sessions_handler` | `?user={userId}` (required) | `{ ok: true, revoked: N }` |

`token_hash` is URL-safe (64 hex chars). Revoking one vs. all is distinguished by the path having
the `{token_hash}` segment (one) vs. the bare path with `?user=` (all-for-user). A bare
`DELETE /admin/sessions` with no `user` is `400 BAD_REQUEST` (refuse to revoke *all* sessions
instance-wide from a single unscoped call).

### 4.3 DTO

```rust
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionRow {
    token_hash: String,
    user_id: String,
    email: Option<String>,
    anonymous: bool,
    created_at: i64,
    expires_at: i64,
}
```

`token_hash` is a non-reversible sha256 digest (the plaintext token is never stored), so surfacing
it to an authenticated admin is safe and lets the UI target a specific row without a redundant
surrogate `id` column.

## 5. Deviations from the card's literal text

1. **Drop the `?db=` filter.** `rtdb_auth.sessions` has no `db` column — an OAuth/anonymous login
   is server-wide (it grants across every DB the user is allowlisted on). Listing is therefore
   server-wide, filtered by user/email, shaped like `listAdmins()` rather than per-DB `listTokens()`.
2. **Key by `token_hash`.** The card implied a session id; the natural key is the existing PK
   (`token_hash`), avoiding a new column. Revoke-all is keyed by `user_id`.
3. **`DELETE /admin/sessions/{token_hash}` is a path-param DELETE** (not a POST body), consistent
   with the storage `DELETE /admin/db/{db}/storage/{id}` precedent; revoke-all uses the bare path
   with `?user=`.

## 6. Client surfaces

### 6.1 ts-client (`ts-client/src/admin.ts`)

Add `SessionInfo` DTO (next to `TokenInfo`) and three methods mirroring `listTokens`/`revokeToken`:

```ts
interface SessionInfo { tokenHash: string; userId: string; email: string | null;
                        anonymous: boolean; createdAt: number; expiresAt: number; }
listSessions(filter?: { user?: string; limit?: number }): Promise<SessionInfo[]>
revokeSession(tokenHash: string): Promise<void>
revokeUserSessions(userId: string): Promise<{ ok: boolean; revoked: number }>
```

Re-export `SessionInfo` from `index.ts` (`export type { … } from "./admin.js"`). Mirror stubs in
`InMemoryAdminClient` (`in_memory.ts`) and a test in `tests/admin.test.ts`.

### 6.2 Dashboard

- `SessionRow` type in `dashboard/src/lib/types.ts` (next to `TokenRow`).
- `AdminClient.listSessions` / `revokeSession` / `revokeUserSessions` in `dashboard/src/lib/admin.tsx`
  (mirror `listTokens` / `revokeToken`; auth rides the HttpOnly `rtdb_session` cookie — no change).
- New `dashboard/src/pages/SessionsPage.tsx`, cloned from `TokensPage.tsx`: drop the mint form and
  the db-selector (sessions are server-wide); add an optional user-filter input; keep the table +
  the inline two-step confirm (`confirmingRevoke` state — the established pattern; there is no
  shared modal component). Co-locate `SessionsPage.module.css`.
- Route `<Route path="sessions" element={<SessionsPage />} />` in `App.tsx` and a
  `{ to: "/sessions", label: "Sessions" }` nav entry in `shell/AppShell.tsx` (after Tokens).

## 7. Tests (map to acceptance criteria)

1. **AC #1 — list + revoke over HTTP, admin-gated.** Integration test (`server/tests`): seed two
   users with multiple sessions; assert `GET /admin/sessions` lists newest-first and honors `?user=`;
   `DELETE /admin/sessions/{hash}` removes one; `DELETE /admin/sessions?user=` removes all for that
   user; a request without admin creds is `401`. Add `auth::session` unit coverage for the three
   new functions.
2. **AC #2 — live revocation on an open connection.** The key test: open `/sync` as a session
   user, assert a `Mutate`/`Subscribe` succeeds; revoke that session via admin over the **same**
   test fixture; assert the **next** op on the **same** open socket returns an error
   (`Unauthorized`), connection still open.
3. **AC #3 — gate + dashboard.** `make checkall` green; dashboard `SessionsPage` renders the list
   and the confirm guard (component test or agentchrome verification).

## 8. Invariants preserved

- **Single-writer untouched.** Session revoke is a direct `DELETE` on `rtdb_auth.sessions` — it
  touches no document table, no subscription, no committer. (It is not a document write, so the
  op-feed/audit/webhook tap-site contract does not apply.)
- **SQL safety.** `token_hash`/`user_id` are bound via `$n`; no identifier interpolation.
- **Errors.** Failures use the `RtDbError` envelope; unknown `token_hash` is idempotent (not an
  error); unscoped revoke-all is `400`.
- **Auth.** Every admin route goes through `require_admin`; the live check re-uses the existing
  per-op gate (no new WS machinery, no new frames).
- **Clients mirror the core.** Server DTO/behavior changes propagate to ts-client + dashboard.
