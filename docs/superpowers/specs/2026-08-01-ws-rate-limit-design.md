# WebSocket Message-Level Rate Limiting — Design

**Date:** 2026-08-01
**Status:** Implemented (2026-08-10)
**Kanban:** `par-rt-db` — "WebSocket message-level rate limiting" (`019fbe20719c7ef3a0ffb4de002202b8`)
**Severity:** Low (fair-share / resource-protection hardening; no auth or correctness surface)

## Motivation

HTTP requests are rate-limited per machine-token and per-database
(`RTDB_RATE_LIMIT_PER_TOKEN_RPM` / `RTDB_RATE_LIMIT_PER_DB_RPM`, an in-memory
fixed-window `RateLimiter` on `AppState`, returning 429 `RATE_LIMITED` +
`Retry-After`, checked after `authorize`). A single noisy app on a multi-db
instance can otherwise starve the others by saturating the committer and
connection pool.

The WebSocket handler is **not** covered by that limiter today. It has only a
per-connection tumbling 10s / 200-message cap (`ws.rs`'s local `RateLimiter`
struct) that counts *every* frame type and **closes the socket** on exceed. That
cap protects against one connection flooding frames, but it does not give
fair-share across identities or databases: a client that opens many connections,
or drives heavy `Mutate`/`Subscribe` work over a few, is not throttled per
identity the way an equivalent HTTP client is. `CLAUDE.md` calls WS-message-level
limiting a "future enhancement"; this ships it.

The ship also closes a pre-existing client-mirror gap: the server's
`RATE_LIMITED` code and `retryAfter` field (added for the HTTP limiter) were
never fully mirrored into the three clients. Over HTTP a 429 response can be read
opportunistically, but over WS the error arrives as a **parsed message** whose
inner `error` envelope must deserialize — so the gap becomes a correctness
defect, not a nicety.

## Goals

- Extend the **existing** per-token/per-db fixed-window limiter to inbound WS
  `Mutate` and `Subscribe` frames, so one identity's WS work is bounded the same
  way its HTTP work is.
- Reject a rate-limited WS op with a typed `RATE_LIMITED` error carrying a
  `retryAfter` hint, over the existing per-op error channels (`MutateErr` /
  `SubscribeErr`), **without closing the connection**.
- Complete the `RATE_LIMITED` + `retryAfter` mirror in all three clients (TS,
  Rust, Python), satisfying the "clients mirror the core" invariant.

## Non-goals

- New configuration. WS frames reuse the existing `rate_limit_per_token_rpm` /
  `rate_limit_per_db_rpm` ceilings (one identity = one budget across all
  transports).
- Limiting message types other than `Mutate` and `Subscribe`. `Unsubscribe`,
  `Ping`, and the schedule family stay covered only by the per-connection cap.
- An auto-retry-on-`RATE_LIMITED` helper in any client. Retry-on-rate-limit is a
  backoff-policy decision; exposing `retryAfter` is the mirror. Clients keep
  `retryOnPrecondition` / `retry_on_precondition` as their only auto-retry.
- Changing the per-connection cap's behavior. It stays a coarse flood valve that
  closes the socket; its misleading `bad_request("rate limit exceeded")` close
  message is noted as a possible follow-up, not changed here.
- WS-message-level limiting for unauthenticated/`Auth` frames. Auth happens once
  at the handshake; rate limiting applies to post-auth ops.

## Approach

Reuse the shared limiter, not a parallel one. The WS handler already holds the
resolved `principal` and authorized `db` for the whole connection, so each
post-auth `Mutate`/`Subscribe` can run the identical per-token→per-db check the
HTTP path runs, against the same `AppState` limiter and the same RPM ceilings.
An identity doing both HTTP and WS draws from one pool — the natural meaning of
"per token" / "per db".

Two limiters coexist after this change, by design:

| Limiter | Scope | Granularity | On exceed |
|---|---|---|---|
| per-connection cap (`ws.rs`, unchanged) | one socket | any frame type, 200/10s | **close** the socket |
| per-token/per-db (shared, new for WS) | identity / database | `Mutate`+`Subscribe`, RPM | typed error frame, **connection open** |

The first is a blunt DoS valve; the second is fair-share with a retry hint.

## Detailed design

### Server — `rate_limit.rs`

Extract the per-token-then-per-db check sequence (currently inline in
`check_http_rate_limits`) into a shared helper:

```rust
/// Runs the per-token then per-db fixed-window checks in order, returning the
/// first denial (with its retry-after hint) or `Allowed`. The per-token check
/// applies only to `Principal::Machine` (OAuth sessions have no machine-token
/// identity and skip straight to per-db) — matching `check_http_rate_limits`
/// exactly. Same order and same increment semantics as the HTTP path: a request
/// that passes the token check but denies on db still consumes token budget
/// (preserving today's behavior).
pub async fn evaluate(state: &AppState, principal: &Principal, db: &str) -> RateDecision
```

`check_http_rate_limits` becomes a thin wrapper:

```rust
match evaluate(state, principal, db).await {
    RateDecision::Denied { retry_after_secs } => Err(RtDbError::rate_limited(retry_after_secs)),
    RateDecision::Allowed => Ok(()),
}
```

HTTP behavior is byte-identical (same order, same increments, same error) and
stays guarded by the existing `rate_limit.rs` tests. The module doc is updated:
drop the "HTTP-only for v1 … message-level WS limiting is a documented
follow-up" note — it has shipped.

`RateKey` / `RateDecision` / `RateLimiter::check` are unchanged.

### Server — `ws.rs`

1. **Rename** the local `struct RateLimiter` (the per-connection 10s/200-msg cap)
   to `ConnRateLimiter`, and its `hit` call site with it. Pure local rename;
   behavior unchanged. This disambiguates now that `ws.rs` also calls into the
   shared `rate_limit` module.

2. In the **`Subscribe`** arm, on the `authorize`/`is_admin` `Ok(())` path,
   before `committers.subscribe(...)`:

   ```rust
   if let RateDecision::Denied { retry_after_secs } =
       crate::rate_limit::evaluate(state, principal, db).await
   {
       let _ = out_tx.send(ServerMessage::SubscribeErr {
           query_id,
           error: RtDbError::rate_limited(retry_after_secs),
       });
       return false; // connection stays open
   }
   ```

3. In the **`Mutate`** arm, on the `Ok(())` path, before the
   `max_affected_docs` cap check and `committers.mutate(...)`: the same check,
   sending `MutateErr { mut_id, error: RtDbError::rate_limited(...) }` and
   `return false`.

Placement is **after** `authorize` (an unauthorized op consumes no budget —
parity with HTTP) and before the expensive committer work. Admin OAuth sessions
carry no machine-token identity, so the per-token branch is skipped for them and
only the per-db ceiling applies — exactly as in HTTP; no special-casing. The
per-connection `ConnRateLimiter.hit()` still runs first on every frame and still
closes on >200/10s; the new check is strictly additive on the two committer-
touching arms.

### Clients — complete the `RATE_LIMITED` + `retryAfter` mirror

**TS** (`ts-client/src/errors.ts`):
- Add `"RATE_LIMITED"` to the `RtDbErrorCode` union **and** to the `CODES` set.
  Today `RtDbErrorEnvelope.isEnvelope` returns `false` for a `RATE_LIMITED`
  payload because the code is absent from `CODES` — this is the bug.
- Add `retryAfter?: number` to `RtDbErrorEnvelope`; add `readonly retryAfter?:
  number` to the `RtDbError` class, parsed in `fromEnvelope`.

**Rust** (`rust-client/src/error.rs`) — the mandatory correctness fix:
- Add the `RateLimited` variant to `ErrorCode` (serialized `"RATE_LIMITED"` via
  the existing `#[serde(rename_all = "SCREAMING_SNAKE_CASE")]`).
- Add `#[serde(default, skip_serializing_if = "Option::is_none", rename =
  "retryAfter")] pub retry_after: Option<u32>` to both `ErrorEnvelope` and
  `RtDbError`; thread it through `from_envelope`; add
  `RtDbError::rate_limited(retry_after_secs: u32)`.
- Extend the `error_code_round_trips_all_variants` test to include `RateLimited`.
  Without this variant, a WS `MutateErr`/`SubscribeErr` whose `error.code` is
  `"RATE_LIMITED"` fails to deserialize in the rust client.

**Python** (`python-client/src/par_rt_db/errors.py`):
- `RATE_LIMITED` (→ 429) already exists. Add `retry_after: int | None` to
  `RtDbError`, parsed from `envelope.get("retryAfter")` in `from_envelope`, and
  expose it as an attribute.

All three keep the wire shape byte-identical to the server's
`{code, message, retryAfter?}` envelope (existing tests that assert non-rate
errors omit `retryAfter` still pass — the field stays `skip_serializing_if =
None`).

### Testing

**Server** (`server/tests/`, a WS integration test):
- Configure a small per-token RPM (or per-db), open one `/sync` connection with a
  machine token, and send N+1 `Mutate` (and separately `Subscribe`) frames.
  Assert the (N+1)th yields `MutateErr` / `SubscribeErr` with
  `code: "RATE_LIMITED"` and a positive `retryAfter`, and that the connection is
  **not** closed (a frame sent after the limit still gets a response — e.g. a
  `Ping`→`Pong`, or a valid op once a new minute window opens).
- Assert an authorize-failed op returns the auth error, not a rate error
  (rate check is gated on the `Ok(())` path).
- Existing per-connection-cap and HTTP rate-limit tests remain unchanged and
  guard the unchanged behavior.

**Clients**: each adds a parse / round-trip test for a `RATE_LIMITED` envelope
carrying `retryAfter` (TS `isEnvelope`+`fromEnvelope`, Rust `ErrorCode`
round-trip + `retry_after`, Python `from_envelope` → `.retry_after`).

### Docs

- `CLAUDE.md` (Two-transports section): replace "HTTP-only v1 — WS-message-level
  limiting is a future enhancement" with the shipped behavior (shared limiter on
  `Mutate`/`Subscribe`, typed error frame, connection stays open; per-connection
  cap unchanged).
- `FEATURE_MATRIX.md`: update the rate-limiting row if present.
- Move the kanban item to `done` once `make checkall` passes.

## Verification

`make checkall` (fmt-check + clippy `-D warnings` + typecheck + full test suite;
`make dev-db-up` is required — integration tests hit a real Postgres). The
dashboard builds against `ts-client/dist`, so `make ts-client-build` must precede
the dashboard typecheck on a fresh checkout.

## Open questions

None. Budget model (shared with HTTP), op scope (`Mutate`+`Subscribe`), and
client-mirror scope (all three clients) were confirmed during design review.
