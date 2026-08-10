# par-rt-db Client-Completeness Sweep — Design Spec

**Date:** 2026-07-26
**Status:** Implemented (2026-08-10) — the four clients (ts, rust, python, dashboard) are at feature parity; the per-item scope decisions below landed via the follow-on plans
via the writing-plans skill, one short plan per item.
**Repo:** `~/Repos/par-rt-db`
**End goal:** close every open client-parity gap surfaced by the python-client spec's "Related"
audit list (`docs/superpowers/specs/2026-07-25-python-client-design.md:358`), verified against current
code, so `FEATURE_MATRIX.md`'s client-parity rows are complete across all four clients.

## Purpose

A focused sweep that closes the scattered parity gaps the python-client spec catalogued as a "separate
workstream": ts-admin missing endpoints, ts Google OAuth + `/auth/me`, the ts in-memory `filter` bug,
rust optimistic updates (#12), and the rust in-memory harness (#19). Each item is **reference-backed**
— a server route, a rust-client method, or a ts-client module already exists as the implementation to
mirror — so this is port/parity work, not novel design. Server/protocol changes are out of scope; the
clients speak the surface as it exists today.

Two corrections to the python spec's 1-day-old framing, confirmed by reading current code:

1. **The admin-endpoint lag is cross-client, not ts-only.** rust-client's admin module
   (`rust-client/src/http.rs:499-675`, `#[cfg(feature = "admin")]`) ships the *same* 10-method subset as
   ts-client (`ts-client/src/admin.ts`) — both lag the server's 20 routes (`server/src/admin.rs`,
   registered in `admin_routes()` at `:907-935`). Both need the missing methods.
2. **The in-memory `filter` behavior is the only silent bug.** `ts-client/src/in_memory.ts:930-964`
   never evaluates `q.filter` and returns unfiltered results. `search`/`vectorSearch` are honestly
   stubbed — they return `[]` with an explanatory comment (`in_memory.ts:858-860`, `:885-887`), so they
   are unimplemented, not buggy.

## Verified gap inventory

Evidence gathered 2026-07-26 against current `main`. Sizes: S ≤1 day, M 1–3 days, M–L ~3–5 days.

| Item | Gap | Client(s) | Size | Reference |
|---|---|---|---|---|
| **B** | Google OAuth + `/auth/me` | ts | S | `signInWithGitHub` (`react.tsx:170-207`); rust `auth_me` (`http.rs:427-434`) |
| **C** | In-memory `filter` silently ignored | ts | M | server predicate semantics (`server/src/query.rs`) |
| **A-ts** | Admin endpoint parity (browser) | ts | M | server `admin.rs` (20 routes) |
| **A-rust** | Admin endpoint parity (machine) | rust | M | server `admin.rs` (machine-relevant subset) |
| **D** | Optimistic updates (#12) | rust | M | `ts-client/src/optimistic.ts` (229 lines) + `client.ts` hooks |
| **E** | In-memory test harness (#19) | rust | M–L | `ts-client/src/in_memory.ts` (1,249 lines) |

## Decisions (settled during brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| Sweep scope | All 5 gaps | User chose; closes every open client-parity row in `FEATURE_MATRIX.md`. |
| Sequencing | ts-wave (B → C → A-ts) then rust-wave (D → A-rust → E) | Client-grouped so no two waves edit the same package concurrently; C before E is the one hard dependency (E ports the fixed `filter`). |
| Plan granularity | One short plan per item | User approved; items are independent and reference-backed, so each gets its own spec→plan→implement cycle. |
| Admin surface split | **ts**: machine + browser (`login`/`logout`); **rust**: machine only | rust is a server-side machine client — cookie-session admin login and popup OAuth do not apply. Excludes `login`/`logout` and any OAuth helper from rust. |
| C scope | `filter` evaluator only | `search`/`vector` are honest `[]` stubs, not bugs; widening to in-memory ranking is scope creep that belongs elsewhere. |
| E search/vector | `filter` correct; `search`/`vector` honest `[]` stubs | Naive in-memory text/vector ranking is low test-harness value and would inflate the already-largest item. Matches current ts-harness behavior. |
| Typing (rust) | Reuse existing byte-identical DSL/wire types (`mutation.rs`, `query.rs`, `schema.rs`, `cursor.rs`, `wire.rs`) | The optimistic projection and in-memory executor consume types that already exist; no new wire surface. |

## Per-item design

### B — Google OAuth + `/auth/me` (ts, S)

- Add `signInWithGoogle` to `ts-client/src/react.tsx`, a near-clone of `signInWithGitHub`
  (`react.tsx:170-207`): identical popup + `rtdb-auth` `postMessage` handshake, only the URL path changes
  to `${baseUrl}/auth/google`. Server already mounts `GoogleProvider` at `/auth/google` +
  `/auth/google/callback` (`server/src/auth/provider.rs:429-433`, impl in `server/src/auth/google.rs`).
- Grow `useRtDbAuth().signIn` (`react.tsx:131-137`, currently hardcodes GitHub) to take
  `provider?: "github" | "google"` defaulting to `"github"`.
- Add `authMe()` to `ts-client/src/http.ts`, parallel to the existing `validateSessionToken`
  (`http.ts:94-97`): `GET /auth/me` (server route `provider.rs:435`, handler `me` at `:378`) with the
  client's own bearer, returning `AuthedUser`. Keep `validateSessionToken` (`GET /auth/validate`,
  arbitrary token argument) — distinct server semantics: `/auth/me` is session-only and uses the client
  bearer; `/auth/validate` accepts any token passed in.

### C — In-memory `filter` fix (ts, M)

- Add a recursive `FilterExpr` evaluator to `ts-client/src/in_memory.ts`: leaves
  `eq/neq/gt/gte/lt/lte/in` (`{field, value}` / `in` `{field, values[]}`) plus `and/or` combinators
  (`{exprs[]}`). Type-coercion semantics match `server/src/query.rs` (the executor the real server uses).
- Wire it into the row-filter loop at `in_memory.ts:930-964` so a non-matching doc `continue`s, the same
  way the existing index-eq-prefix (`:932-944`) and range-bound (`:945-962`) checks do.
- **Out of scope for C:** `search`/`vectorSearch` stay as their honest `[]` stubs.

### A-ts — ts admin endpoints (ts, M)

Add the missing methods to `ts-client/src/admin.ts`, each a one-liner over the existing
`RtDbAdminClient.request` helper (constructor `admin.ts:18`), mirroring `server/src/admin.rs` handlers:

- **Machine + browser:** `adminsList/add/remove` (`GET/POST/DELETE /admin/admins`, `admin.rs:386/403/432`),
  `getSchema` (`GET /admin/dbs/{db}/schema`, `:445`), `dbStats` (`GET /admin/dbs/{db}/stats`, `:596`),
  `adminQuery` (`POST /admin/db/{db}/query`, `:470`, owner bypass), `adminMutate` (`POST /admin/db/{db}/mutate`,
  `:504`), `listTokens` (`GET /admin/tokens`, `:551`), `metrics` (`GET /admin/metrics`, `:640`),
  `getConfig` (`GET /admin/config`, `:695`), `patchConfig` (`PATCH /admin/config`, `:715`),
  `opsRecent` (`GET /admin/ops/recent`, `:779`).
- **Browser-only:** `login` (`POST /admin/login`, `:111`, sets the cookie session), `logout`
  (`POST /admin/logout`, `:133`, clears the cookie).
- `stream()` — the `/admin/stream` WS (`admin.rs:815`), authing via the `Sec-WebSocket-Protocol:
  rtdb-admin.<token>` subprotocol (`admin.rs:820-842`) since browsers cannot set the `Authorization`
  header on a WS handshake. Mirrors the dashboard's existing connection path.

### D — Optimistic updates (rust, M)

- New `rust-client/src/optimistic.rs` — a near-mechanical port of `ts-client/src/optimistic.ts`. Same
  conservative projection model (correctness over coverage — only unambiguous cases overlay):
  - Unfiltered array (`collect`/`take`): insert/patch/replace/delete on a known id.
  - Filtered array (index/eq/range): delete-only of a doc already in the result (membership can't be
    evaluated for insert/patch/replace/upsert → SKIP).
  - `get(id)` point read: patch/replace/delete of that same id.
  - `upsert`/`first`/`unique`/`count`/`paginate`: always SKIP.
  - Inserted docs get a synthetic `__optimistic__<n>` id reconciled away on the server reply.
- Wire into `rust-client/src/ws.rs`:
  - Extend `SubState` (`ws.rs:117-124`) with `server_last: Option<Value>` + `optimistic: bool`.
  - Add `Config { optimistic_updates: bool, .. }` (`ws.rs:92`), default `false`. Off ⇒ current behavior
    bit-for-bit.
  - Add a reverse index on `ClientInner` (`ws.rs:238-254`): `mutId → set<queryId>`.
  - Hook apply (on mutate-send, around `deliver_mutate` at `ws.rs:968`), reconcile (on
    `queryUpdate`/`mutateOk` in `apply_server_message` at `ws.rs:1048`), and rollback (on `mutateErr`,
    the `reject_inflight`/`reject_all` paths at `ws.rs:1141-1163`, and teardown).
  - **Risk:** the rust client funnels every write through one driver task via `Cmd::Mutate` (`ws.rs:204`),
    so the overlay must be applied to matching subs' `watch` channels after the send is queued but before
    the server reply — more delicate than ts's flat event handlers. This is the bulk of the work.
- Public API mirrors ts: `Config.optimistic_updates` opt-in. Reference tests: port `ts-client/tests/optimistic.test.ts`.

### A-rust — rust admin endpoints (rust, M)

Add the **machine-relevant** admin methods only, to the `#[cfg(feature = "admin")]` block in
`rust-client/src/http.rs:499-675`, mirroring the server handlers: `admins_add`/`admins_remove`/`admins_list`, `get_schema`,
`db_stats`, `admin_query`, `admin_mutate`, `list_tokens`, `metrics`, `get_config`, `patch_config`,
`ops_recent`, and `stream` (WS). For `stream()`, rust authenticates via the `Authorization: Bearer
<admin_key>` header on the WS handshake — the server's `/admin/stream` upgrade gate accepts the header
(the CLI/automation path, `admin.rs:815`) **or** the `Sec-WebSocket-Protocol: rtdb-admin.<token>`
subprotocol; rust takes the header path since, unlike browsers, tokio-tungstenite can set it, and rust is
a CLI/automation client. (ts's `stream()` in A-ts uses the subprotocol because browsers cannot set the
header — same server gate, different transport constraint.)

**Deliberately excluded from rust:** `login`/`logout` (cookie-session) and any popup OAuth — rust is a
server-side machine client; those do not apply.

### E — In-memory test harness (rust, M–L)

- New `rust-client/src/in_memory.rs` — a port of `ts-client/src/in_memory.ts` (1,249 lines): `push_schema`,
  typed `query::<T: DeserializeOwned>()` (mirroring `http.rs`'s typed query), `mutate` (mutId idempotency
  cache + snapshot/restore atomic rollback on step failure), reactive `subscribe` returning the same
  `tokio::sync::watch::Receiver<Snapshot>` the WS client uses (`ws.rs:265-279`) — no socket, no driver
  task; the harness writes `Snapshot` updates directly to each sub's `watch::Sender` on notify — `tick()`
  (timer-less scheduler advance), and schedule/storage stubs.
- High reuse of existing byte-identical types: `mutation.rs:11-49` (`Step`, 7 variants), `query.rs:23-71`
  (`Query` incl. `filter`/`search`/`vector_search`), `schema.rs`, `cursor.rs`, `wire.rs` (`FilterExpr`,
  `SearchQuery`, `VectorSearchQuery`).
- **`filter` implemented correctly** (inherits C's fix pattern — predicate evaluator over
  `serde_json::Value`). **`search`/`vector` honest `[]` stubs** (current ts-harness behavior).
- System fields (`_id`/`_creationTime`/`_version`) stored separately and merged at read time, mirroring
  the server's `doc` jsonb + system columns (`in_memory.ts:1012` `mergeDoc`).
- Reference tests: port `ts-client/tests/in_memory.test.ts` coverage (schema push, insert+read, upsert by
  index, query by index, transactions, subscribe, keyset pagination, schedules), plus filter-evaluator
  coverage.

## Cross-cutting

- **Order:** B → C → A-ts (ts wave) → D → A-rust → E (rust wave). C before E is the one hard dependency.
- **Docs:** after each wave, update `FEATURE_MATRIX.md` — the "Mirrored across" client columns and rows
  #12 / #19 status — and the relevant client READMEs. A stale doc that contradicts the code is a bug.
- **Verification:** `make checkall` is the gate (fmt-check + clippy `-D warnings` + typecheck + tests).
  Each item adds unit tests: filter evaluator (C, E), optimistic projection (D), in-memory semantics
  (E); the admin/OAuth surfaces (A-ts, A-rust, B) add shape + wire-parity coverage against the server
  routes. Live-server integration tests are opt-in and unchanged.
- **Wire contract:** no protocol changes. ts `protocol.ts`, rust `wire.rs`, server `protocol.rs`, python
  `wire.py` stay byte-identical; this sweep only adds client-side surface that calls existing routes.

## Out of scope

- Any server/protocol change — the clients speak the surface as it exists today.
- python-client runtime (HTTP/WS/admin/in-memory) — a separate workstream with its own spec (the
  approved `2026-07-25-python-client-design.md`).
- In-memory `search`/`vector` ranking (naive text/cosine) in either harness — honest `[]` stubs ship
  instead; full in-memory ranking is deferred.
- A rust derive-macro typing layer — unchanged from the rust-client spec's deferral.
- Browser OAuth helpers in rust — rust is a machine client.

## Success criteria

1. `make checkall` green across all five packages after each item.
2. ts-client: Google sign-in works (`signInWithGoogle` + provider-aware `signIn`); `authMe()` round-trips;
   the in-memory harness returns `filter`-correct results; the admin client covers all 20 server routes
   (machine + browser).
3. rust-client: optimistic updates ship behind `Config.optimistic_updates` (off = bit-for-bit current
   behavior; projection tests ported from ts); the admin client covers the machine-relevant routes;
   `InMemoryRtDbClient` ports the ts harness with correct `filter` and reactive `subscribe`.
4. `FEATURE_MATRIX.md` rows #12 and #19 flip to ✅ rust; the "Mirrored across" columns show ✅ts ✅rust
   ✅python for every affected surface.

## Phasing → per-item plans

The writing-plans skill emits one short plan per item, executed in order:

1. **B** — Google OAuth + `/auth/me` (ts).
2. **C** — in-memory `filter` evaluator (ts).
3. **A-ts** — ts admin endpoints (machine + browser).
4. **D** — rust optimistic updates.
5. **A-rust** — rust admin endpoints (machine).
6. **E** — rust in-memory harness (ports C's filter fix).

Each plan: implement → unit tests → `make checkall` → atomic commit → `FEATURE_MATRIX.md`/README update.

## Related

- python-client design (audit source): `docs/superpowers/specs/2026-07-25-python-client-design.md`.
- rust-client design (template): `docs/superpowers/specs/2026-07-22-rust-client-design.md`.
- ts-client: `ts-client/src/`. rust-client: `rust-client/src/`. Server admin: `server/src/admin.rs`.
