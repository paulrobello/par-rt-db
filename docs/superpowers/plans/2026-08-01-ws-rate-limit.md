# WebSocket Message-Level Rate Limiting Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend the existing per-token/per-db fixed-window rate limiter to inbound WS `Mutate`/`Subscribe` frames (typed `RATE_LIMITED` error, connection stays open), and complete the `RATE_LIMITED`+`retryAfter` mirror across all three clients.

**Architecture:** Extract the per-token→per-db check in `rate_limit.rs` into a shared `evaluate()` helper (HTTP becomes a thin wrapper — byte-identical). `ws.rs` calls `evaluate()` in the `Mutate`/`Subscribe` arms after `authorize`, sending `MutateErr`/`SubscribeErr` with `RtDbError::rate_limited(retry_after)` on denial (connection stays open). The three clients add the `RATE_LIMITED` code + `retryAfter` field they drifted on during the HTTP rate-limit ship. No new config — reuses `rate_limit_per_token_rpm` / `rate_limit_per_db_rpm`.

**Tech Stack:** Rust (axum/tokio server, `cargo`), TypeScript (`ts-client`, bun/vitest), Rust (`rust-client`, cargo), Python (`python-client`, uv/pytest).

## Global Constraints

- **Wire envelope is byte-identical across four implementations** (`server/src/error.rs`, `ts-client/src/errors.ts`, `rust-client/src/error.rs`, `python-client/src/par_rt_db/errors.py`): `{code, message}` with optional `retryAfter` (camelCase), omitted when absent (`skip_serializing_if`/optional). Error codes are `SCREAMING_SNAKE_CASE`.
- **`RATE_LIMITED` must serialize as `"RATE_LIMITED"`** in every client (Rust via `#[serde(rename_all = "SCREAMING_SNAKE_CASE")]`; TS/Python as literal strings).
- **Single committer / shared-resource invariants are untouched.** The rate check runs in the WS handler task (not the committer); it only calls the existing in-memory limiter and sends a frame.
- **HTTP rate-limit behavior must stay byte-identical** — same check order (per-token then per-db), same increment semantics (a request that passes token but denies on db still consumes token budget). The existing `server/tests/rate_limit_test.rs` guards this.
- **The per-connection WS cap (`ConnRateLimiter`, 200 msgs/10s, closes socket) stays unchanged** — it is a coarse flood valve, complementary to the new per-identity limit.
- **Verification gate:** `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests). Server tests need `make dev-db-up` (dev Postgres on `127.0.0.1:55434`). On a fresh checkout run `make ts-client-build` before the dashboard typecheck.
- **No `unwrap()`/`expect()` outside `#[cfg(test)]`. Zero clippy warnings.**
- Branch: `ws-rate-limit` (in-place; already created). Commit after each task.

---

## File Structure

**Server (`server/src/`):**
- `rate_limit.rs` — extract shared `evaluate()`; thin `check_http_rate_limits` wrapper; update module doc.
- `ws.rs` — rename local `RateLimiter`→`ConnRateLimiter`; call `evaluate()` in `Mutate`+`Subscribe` arms.

**Server tests (`server/tests/`):**
- `ws_test.rs` — two new integration tests (mutate + subscribe rate-limited, connection stays open).

**Clients:**
- `ts-client/src/errors.ts` (+ `tests/errors.test.ts`) — `"RATE_LIMITED"` in union + `CODES`; `retryAfter` on envelope + `RtDbError`.
- `rust-client/src/error.rs` — `RateLimited` variant; `retry_after` field; `rate_limited()` ctor; tests.
- `python-client/src/par_rt_db/errors.py` (+ `tests/test_errors.py`) — parse `retryAfter` in `from_envelope`; `.retry_after` attr.

**Docs:**
- `CLAUDE.md` — Two-transports rate-limit sentence → shipped behavior.
- `FEATURE_MATRIX.md` — rate-limit row if present.

---

### Task 1: Extract shared `evaluate()` helper in `rate_limit.rs`

Pure refactor — no behavior change. The existing `server/tests/rate_limit_test.rs` (4 HTTP tests) is the safety net. This task unlocks Task 2.

**Files:**
- Modify: `server/src/rate_limit.rs:102-129` (`check_http_rate_limits`)

**Interfaces:**
- Produces: `pub async fn evaluate(state: &AppState, principal: &Principal, db: &str) -> RateDecision` — returns `Denied { retry_after_secs }` (first denial, per-token then per-db) or `Allowed`. Token check applies only to `Principal::Machine`. Consumed by Task 2.

- [ ] **Step 1: Add the `evaluate` helper above `check_http_rate_limits`**

Insert this function in `server/src/rate_limit.rs`, immediately before `check_http_rate_limits`:

```rust
/// Runs the per-token then per-db fixed-window checks in order, returning the
/// first denial (with its `retry_after_secs` hint) or `Allowed`. The per-token
/// check applies only to `Principal::Machine` (OAuth sessions have no
/// machine-token identity and skip straight to per-db). Shared by the HTTP
/// gate (`check_http_rate_limits`) and the WS `Mutate`/`Subscribe` arms.
pub async fn evaluate(state: &AppState, principal: &Principal, db: &str) -> RateDecision {
    let token_limit = state.config.rate_limit_per_token_rpm;
    if token_limit > 0
        && let Principal::Machine { token_id, .. } = principal
        && let RateDecision::Denied { retry_after_secs } = state
            .rate_limiter
            .check(RateKey::Token(token_id.clone()), token_limit)
            .await
    {
        return RateDecision::Denied { retry_after_secs };
    }

    let db_limit = state.config.rate_limit_per_db_rpm;
    if db_limit > 0
        && let RateDecision::Denied { retry_after_secs } = state
            .rate_limiter
            .check(RateKey::Db(db.to_string()), db_limit)
            .await
    {
        return RateDecision::Denied { retry_after_secs };
    }

    RateDecision::Allowed
}
```

- [ ] **Step 2: Rewrite `check_http_rate_limits` as a thin wrapper over `evaluate`**

Replace the entire body of `check_http_rate_limits` (the two `if token_limit` / `if db_limit` blocks returning `Err`) with:

```rust
pub async fn check_http_rate_limits(
    state: &AppState,
    principal: &Principal,
    db: &str,
) -> Result<(), RtDbError> {
    match evaluate(state, principal, db).await {
        RateDecision::Denied { retry_after_secs } => {
            Err(RtDbError::rate_limited(retry_after_secs))
        }
        RateDecision::Allowed => Ok(()),
    }
}
```

Keep its existing doc comment. The `use crate::error::RtDbError;` import at the top of the file is already present and still needed.

- [ ] **Step 3: Run the HTTP rate-limit suite + server gate**

```bash
make dev-db-up
cd server && cargo test --test rate_limit_test && cargo fmt --check && cargo clippy --all-targets -- -D warnings
```
Expected: all 4 `rate_limit_test` tests PASS (byte-identical HTTP behavior preserved), fmt clean, clippy clean.

- [ ] **Step 4: Commit**

```bash
git add server/src/rate_limit.rs
git commit -m "refactor(rate_limit): extract shared evaluate() helper (no behavior change)"
```

---

### Task 2: WS `Mutate`/`Subscribe` rate limiting + `ConnRateLimiter` rename

The feature task. Test-first: the new WS tests fail before the wiring exists.

**Files:**
- Modify: `server/src/ws.rs:82-105` (rename local limiter), `ws.rs:347-396` (Subscribe arm), `ws.rs:402-456` (Mutate arm), `ws.rs:6` (import)
- Test: `server/tests/ws_test.rs` (append two tests + edit import)

**Interfaces:**
- Consumes: `crate::rate_limit::evaluate` (Task 1) and `RateDecision`.
- Produces: WS `Mutate`/`Subscribe` now emit `MutateErr`/`SubscribeErr` with `code: "RATE_LIMITED"` + `retryAfter` when over budget; connection stays open.

- [ ] **Step 1: Write the failing mutate rate-limit test**

In `server/tests/ws_test.rs`, add `test_state_with_rate_limits` to the existing common import (line 6):

```rust
use common::{admin_post, fresh_db, mint_user_session, spawn_app, test_state, test_state_with_rate_limits};
```

Append this test at the end of the file:

```rust
// Per-token/per-db rate limiting (shared with HTTP via rate_limit::evaluate):
// with per_token_rpm = 3, the first 3 mutates succeed and the 4th in the same
// minute returns a mutateErr RATE_LIMITED with a positive retryAfter — and the
// connection stays open (a subsequent ping still pongs). Mirrors the HTTP
// assertions in rate_limit_test over the WS transport.
#[tokio::test]
async fn ws_mutate_rate_limited_keeps_connection_open() -> anyhow::Result<()> {
    let state = test_state_with_rate_limits(3, 0).await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &token, &db).await;

    let txn = insert_work_item_txn();
    for i in 1..=3 {
        send_json(
            &mut ws,
            json!({"type": "mutate", "mutId": format!("ok-{i}"), "txn": txn.clone()}),
        )
        .await;
        let msg = recv_json(&mut ws).await;
        assert_eq!(
            msg["type"], json!("mutateOk"),
            "mutate {i} under the per-token limit should succeed: {msg}"
        );
    }

    // 4th in the same minute → mutateErr RATE_LIMITED with a retryAfter hint.
    send_json(
        &mut ws,
        json!({"type": "mutate", "mutId": "limited", "txn": txn}),
    )
    .await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("mutateErr"));
    assert_eq!(msg["mutId"], json!("limited"));
    assert_eq!(msg["error"]["code"], json!("RATE_LIMITED"));
    let retry_after = msg["error"]["retryAfter"].as_u64().expect("retryAfter present");
    assert!(
        (1..=60).contains(&retry_after),
        "retryAfter within one fixed-window minute: got {retry_after}"
    );

    // Connection stays open: a ping on the same socket still pongs.
    send_json(&mut ws, json!({"type": "ping"})).await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("pong"));
    Ok(())
}

// Same gate on Subscribe: 3 subscribes return their initial queryUpdate, the
// 4th returns subscribeErr RATE_LIMITED, and the connection stays open.
#[tokio::test]
async fn ws_subscribe_rate_limited_keeps_connection_open() -> anyhow::Result<()> {
    let state = test_state_with_rate_limits(3, 0).await;
    let addr = spawn_app(state.clone()).await;
    let db = fresh_db(&state).await;
    let token = mint_token(addr, &db).await;

    let mut ws = ws_connect(addr).await;
    auth(&mut ws, &token, &db).await;

    for i in 1..=3 {
        send_json(
            &mut ws,
            json!({"type": "subscribe", "queryId": format!("q{i}"), "query": {"table": "workItems"}}),
        )
        .await;
        let msg = recv_json(&mut ws).await;
        assert_eq!(
            msg["type"], json!("queryUpdate"),
            "subscribe {i} should return its initial queryUpdate: {msg}"
        );
    }

    send_json(
        &mut ws,
        json!({"type": "subscribe", "queryId": "qlim", "query": {"table": "workItems"}}),
    )
    .await;
    let msg = recv_json(&mut ws).await;
    assert_eq!(msg["type"], json!("subscribeErr"));
    assert_eq!(msg["queryId"], json!("qlim"));
    assert_eq!(msg["error"]["code"], json!("RATE_LIMITED"));
    assert!(msg["error"]["retryAfter"].as_u64().is_some(), "retryAfter present");

    send_json(&mut ws, json!({"type": "ping"})).await;
    assert_eq!(recv_json(&mut ws).await["type"], json!("pong"));
    Ok(())
}
```

- [ ] **Step 2: Run the new tests to verify they fail**

```bash
cd server && cargo test --test ws_test ws_mutate_rate_limited ws_subscribe_rate_limited
```
Expected: FAIL — the 4th mutate/subscribe currently succeeds (no per-op limit), so the `mutateErr`/`subscribeErr` RATE_LIMITED assertion fails.

- [ ] **Step 3: Rename the local per-connection limiter to `ConnRateLimiter`**

In `server/src/ws.rs`, rename the local struct + impl + construction site so it no longer collides with the shared `rate_limit::RateLimiter`:

- `struct RateLimiter {` → `struct ConnRateLimiter {` (around line 82)
- `impl RateLimiter for` → `impl ConnRateLimiter {` (around line 87)
- In `handle_socket`: `let mut rate_limiter = RateLimiter::new();` → `let mut rate_limiter = ConnRateLimiter::new();` (around line 125)

The `rate_limiter.hit()` call site (around line 329) and the `&mut rate_limiter` param name are unchanged — only the type name changes.

- [ ] **Step 4: Add the rate-limit import to `ws.rs`**

Add to the import block near the top of `server/src/ws.rs` (after the `use crate::error::{ErrorCode, RtDbError};` line):

```rust
use crate::rate_limit::{RateDecision, evaluate};
```

- [ ] **Step 5: Wire `evaluate` into the `Subscribe` arm**

In `handle_text_frame`'s `ClientMessage::Subscribe { query_id, query }` arm, inside the `Ok(())` branch of `match authed` (the branch that currently begins `let owner = if admin {`), insert this rate check as the **first** statements in that block, before `let owner = ...`:

```rust
                    if let RateDecision::Denied { retry_after_secs } =
                        evaluate(state, principal, db).await
                    {
                        let _ = out_tx.send(ServerMessage::SubscribeErr {
                            query_id,
                            error: RtDbError::rate_limited(retry_after_secs),
                        });
                        return false;
                    }
```

`query_id` is still owned at this point (first used later via `query_id.clone()` in `committers.subscribe`), so moving it into `SubscribeErr` and returning is correct. The connection stays open (`return false`, not a close).

- [ ] **Step 6: Wire `evaluate` into the `Mutate` arm**

In the `ClientMessage::Mutate { mut_id, idempotency_key, txn }` arm, inside the `Ok(())` branch of `match authed`, insert this as the **first** statements in that block, before the `max_affected_docs` cap check (`let cap = state.config.max_affected_docs;`):

```rust
                    if let RateDecision::Denied { retry_after_secs } =
                        evaluate(state, principal, db).await
                    {
                        let _ = out_tx.send(ServerMessage::MutateErr {
                            mut_id,
                            error: RtDbError::rate_limited(retry_after_secs),
                        });
                        return false;
                    }
```

`mut_id` is still owned here (first used later in the cap-check `MutateErr` or the success `MutateOk`).

- [ ] **Step 7: Run the new tests to verify they pass**

```bash
cd server && cargo test --test ws_test ws_mutate_rate_limited ws_subscribe_rate_limited
```
Expected: both PASS.

- [ ] **Step 8: Run the full WS + rate-limit suites + server gate**

```bash
cd server && cargo test --test ws_test --test rate_limit_test && cargo fmt --check && cargo clippy --all-targets -- -D warnings
```
Expected: all WS tests (including the pre-existing `rate_limit_exceeded_closes_connection` per-connection-cap test) and all HTTP rate-limit tests PASS; fmt + clippy clean.

- [ ] **Step 9: Commit**

```bash
git add server/src/ws.rs server/tests/ws_test.rs
git commit -m "feat(ws): per-token/per-db rate limiting on Mutate/Subscribe (typed error, conn stays open)"
```

---

### Task 3: TS client — `RATE_LIMITED` code + `retryAfter`

Test-first. Today `RtDbError.isEnvelope` rejects `"RATE_LIMITED"` (absent from `CODES`) and the envelope has no `retryAfter`.

**Files:**
- Modify: `ts-client/src/errors.ts`
- Test: `ts-client/tests/errors.test.ts`

**Interfaces:**
- Produces: `"RATE_LIMITED"` in `RtDbErrorCode` + `CODES`; `retryAfter?: number` on `RtDbErrorEnvelope` and `RtDbError`.

- [ ] **Step 1: Write the failing tests**

Append to `ts-client/tests/errors.test.ts` (inside the existing `describe("RtDbError", ...)` block):

```ts
  it("recognizes a RATE_LIMITED envelope with retryAfter", () => {
    const raw: unknown = {
      code: "RATE_LIMITED",
      message: "rate limit exceeded",
      retryAfter: 42,
    };
    expect(RtDbError.isEnvelope(raw)).toBe(true);
    const e = RtDbError.fromEnvelope(
      raw as { code: "RATE_LIMITED"; message: string; retryAfter: number },
    );
    expect(e.code).toBe("RATE_LIMITED");
    expect(e.retryAfter).toBe(42);
  });

  it("retryAfter is optional on the envelope", () => {
    const raw: unknown = { code: "NOT_FOUND", message: "x" };
    const e = RtDbError.fromEnvelope(raw as { code: "NOT_FOUND"; message: string });
    expect(e.retryAfter).toBeUndefined();
  });
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd ts-client && bunx vitest run tests/errors.test.ts
```
Expected: FAIL — `isEnvelope` returns `false` for `RATE_LIMITED` (not in `CODES`), and `retryAfter` does not exist on `RtDbError`.

- [ ] **Step 3: Add `RATE_LIMITED` to the code union and `CODES` set**

In `ts-client/src/errors.ts`, add `"RATE_LIMITED"` to the `RtDbErrorCode` union (after `"INTERNAL"`) and to the `CODES` set:

```ts
export type RtDbErrorCode =
  | "UNAUTHORIZED"
  | "FORBIDDEN"
  | "NOT_FOUND"
  | "SCHEMA_VIOLATION"
  | "PRECONDITION_FAILED"
  | "CONFLICT"
  | "BAD_REQUEST"
  | "INTERNAL"
  | "RATE_LIMITED";

const CODES: ReadonlySet<string> = new Set<RtDbErrorCode>([
  "UNAUTHORIZED",
  "FORBIDDEN",
  "NOT_FOUND",
  "SCHEMA_VIOLATION",
  "PRECONDITION_FAILED",
  "CONFLICT",
  "BAD_REQUEST",
  "INTERNAL",
  "RATE_LIMITED",
]);
```

- [ ] **Step 4: Add `retryAfter` to the envelope and `RtDbError`**

In `ts-client/src/errors.ts`, update the interface and class:

```ts
export interface RtDbErrorEnvelope {
  code: RtDbErrorCode;
  message: string;
  retryAfter?: number;
}

/** The single error type surfaced by every client transport. */
export class RtDbError extends Error {
  readonly code: RtDbErrorCode;
  readonly retryAfter?: number;

  constructor(code: RtDbErrorCode, message: string, retryAfter?: number) {
    super(message);
    this.name = "RtDbError";
    this.code = code;
    this.retryAfter = retryAfter;
  }

  static isEnvelope(value: unknown): value is RtDbErrorEnvelope {
    return (
      typeof value === "object" &&
      value !== null &&
      "code" in value &&
      "message" in value &&
      typeof (value as { message: unknown }).message === "string" &&
      typeof (value as { code: unknown }).code === "string" &&
      CODES.has((value as { code: string }).code)
    );
  }

  static fromEnvelope(envelope: RtDbErrorEnvelope): RtDbError {
    return new RtDbError(envelope.code, envelope.message, envelope.retryAfter);
  }
}
```

The 3rd constructor arg is optional, so existing 2-arg call sites are unchanged.

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cd ts-client && bunx vitest run tests/errors.test.ts
```
Expected: PASS (all, including the pre-existing three).

- [ ] **Step 6: Format + typecheck + commit**

```bash
cd ts-client && bunx biome format --write src/errors.ts tests/errors.test.ts && bunx tsc --noEmit
git add ts-client/src/errors.ts ts-client/tests/errors.test.ts
git commit -m "feat(ts-client): add RATE_LIMITED code + retryAfter to error envelope"
```

---

### Task 4: Rust client — `RateLimited` variant + `retry_after`

The mandatory correctness fix: without the `RateLimited` variant, a WS `MutateErr`/`SubscribeErr` whose `error.code` is `"RATE_LIMITED"` fails to deserialize. Test-first.

**Files:**
- Modify: `rust-client/src/error.rs`
- Test: inline `#[cfg(test)] mod tests` in the same file.

**Interfaces:**
- Produces: `ErrorCode::RateLimited`; `retry_after: Option<u32>` on `ErrorEnvelope` and `RtDbError` (serde `rename = "retryAfter"`, `skip_serializing_if = "Option::is_none"`); `RtDbError::rate_limited(retry_after_secs: u32)`.

- [ ] **Step 1: Write the failing tests**

In `rust-client/src/error.rs`, inside `mod tests`, add `RateLimited` to the `error_code_round_trips_all_variants` array (after `ErrorCode::Conflict`):

```rust
    #[test]
    fn error_code_round_trips_all_variants() {
        let all = [
            ErrorCode::Unauthorized,
            ErrorCode::Forbidden,
            ErrorCode::NotFound,
            ErrorCode::SchemaViolation,
            ErrorCode::PreconditionFailed,
            ErrorCode::BadRequest,
            ErrorCode::Internal,
            ErrorCode::Conflict,
            ErrorCode::RateLimited,
        ];
        for c in all {
            let v = serde_json::to_value(c).unwrap();
            let back: ErrorCode = serde_json::from_value(v).unwrap();
            assert_eq!(c, back);
        }
    }
```

And append these two tests:

```rust
    #[test]
    fn rate_limited_round_trips_with_retry_after() {
        let err = RtDbError::rate_limited(42);
        let v = serde_json::to_value(&err).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"code":"RATE_LIMITED","message":"rate limit exceeded","retryAfter":42})
        );
        let back: RtDbError = serde_json::from_value(v).unwrap();
        assert_eq!(back.code, ErrorCode::RateLimited);
        assert_eq!(back.retry_after, Some(42));
    }

    #[test]
    fn non_rate_limited_error_omits_retry_after() {
        // Wire shape stays {code, message} for every non-rate error — the field
        // is skip-serialized when None, guarding a wire-shape regression.
        let v = serde_json::to_value(&RtDbError::new(ErrorCode::BadRequest, "x")).unwrap();
        assert_eq!(v, serde_json::json!({"code":"BAD_REQUEST","message":"x"}));
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd rust-client && cargo test --lib error::tests
```
Expected: FAIL — `ErrorCode::RateLimited` and `RtDbError::rate_limited` do not exist (compile error), and `RtDbError`/`ErrorEnvelope` have no `retry_after` field.

- [ ] **Step 3: Add the `RateLimited` variant**

In `rust-client/src/error.rs`, add the variant to the `ErrorCode` enum (after `Conflict`):

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    Unauthorized,
    Forbidden,
    NotFound,
    SchemaViolation,
    PreconditionFailed,
    BadRequest,
    Internal,
    /// Unique-index violation (mirrors server `error::ErrorCode::Conflict`,
    /// HTTP 409). Serialized as `"CONFLICT"` by the container `rename_all`.
    Conflict,
    /// Mirrors server `error::ErrorCode::RateLimited` (HTTP 429). Serialized
    /// `"RATE_LIMITED"`; the carrying envelope includes `retryAfter` when set.
    RateLimited,
}
```

- [ ] **Step 4: Add the `retry_after` field + `rate_limited` constructor**

Update `ErrorEnvelope`, `RtDbError`, and the impl block:

```rust
/// Raw `{code, message, retryAfter?}` as it appears on the wire (HTTP body /
/// WS error frame). `retry_after` is present only on `RATE_LIMITED`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub code: ErrorCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "retryAfter")]
    pub retry_after: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct RtDbError {
    pub code: ErrorCode,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "retryAfter")]
    pub retry_after: Option<u32>,
}

impl RtDbError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retry_after: None,
        }
    }

    pub fn from_envelope(env: ErrorEnvelope) -> Self {
        Self {
            code: env.code,
            message: env.message,
            retry_after: env.retry_after,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }

    /// Rate-limit denial mirroring the server's `RtDbError::rate_limited`
    /// (`code: RATE_LIMITED`, `retryAfter: retry_after_secs`).
    pub fn rate_limited(retry_after_secs: u32) -> Self {
        Self {
            code: ErrorCode::RateLimited,
            message: "rate limit exceeded".to_string(),
            retry_after: Some(retry_after_secs),
        }
    }
}
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
cd rust-client && cargo test --lib error::tests
```
Expected: PASS.

- [ ] **Step 6: Run the rust-client gate + commit**

```bash
cd rust-client && cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
git add rust-client/src/error.rs
git commit -m "fix(rust-client): add RateLimited variant + retry_after (WS frame deserialization)"
```

---

### Task 5: Python client — parse `retryAfter` in `from_envelope`

`RATE_LIMITED` (→429) already exists; only `retryAfter` parsing is missing. Test-first.

**Files:**
- Modify: `python-client/src/par_rt_db/errors.py`
- Test: `python-client/tests/test_errors.py`

**Interfaces:**
- Produces: `RtDbError.retry_after: int | None`, parsed from `envelope["retryAfter"]` in `from_envelope`.

- [ ] **Step 1: Write the failing tests**

Append to `python-client/tests/test_errors.py`:

```python
def test_rate_limited_envelope_carries_retry_after():
    err = RtDbError.from_envelope(
        {"code": "RATE_LIMITED", "message": "rate limit exceeded", "retryAfter": 42}
    )
    assert err.code is ErrorCode.RATE_LIMITED
    assert err.status_code == 429
    assert err.retry_after == 42


def test_envelope_without_retry_after_leaves_it_none():
    err = RtDbError.from_envelope({"code": "NOT_FOUND", "message": "x"})
    assert err.retry_after is None
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
cd python-client && uv run pytest -q tests/test_errors.py::test_rate_limited_envelope_carries_retry_after tests/test_errors.py::test_envelope_without_retry_after_leaves_it_none
```
Expected: FAIL — `RtDbError` has no `retry_after` attribute (`AttributeError`).

- [ ] **Step 3: Add `retry_after` to `RtDbError` and parse it**

In `python-client/src/par_rt_db/errors.py`, update the `RtDbError` class:

```python
class RtDbError(Exception):
    """The single client error type. Mirrors the server's ``{code, message,
    retryAfter?}`` envelope."""

    code: ErrorCode
    message: str
    retry_after: int | None

    def __init__(
        self,
        code: ErrorCode | str,
        message: str,
        retry_after: int | None = None,
    ) -> None:
        self.code = code if isinstance(code, ErrorCode) else ErrorCode(code)
        self.message = message
        self.retry_after = retry_after
        super().__init__(f"{self.code.value}: {message}")

    @property
    def status_code(self) -> int:
        """HTTP status this code maps to."""
        return _STATUS[self.code]

    @classmethod
    def from_envelope(cls, envelope: dict[str, Any]) -> RtDbError:
        """Build from a parsed ``{code, message, retryAfter?}`` body."""
        try:
            code = ErrorCode(envelope.get("code", "INTERNAL"))
        except ValueError:
            code = ErrorCode.INTERNAL
        raw_retry = envelope.get("retryAfter")
        retry_after = raw_retry if isinstance(raw_retry, int) else None
        return cls(code, str(envelope.get("message", "")), retry_after)
```

`retry_after` defaults to `None`, so existing 2-arg call sites (`RtDbError(ErrorCode.PRECONDITION_FAILED, "version mismatch")`) are unchanged. `from_http` delegates to `from_envelope` and inherits the parsing.

- [ ] **Step 4: Run the tests to verify they pass**

```bash
cd python-client && uv run pytest -q tests/test_errors.py
```
Expected: PASS (all, including pre-existing).

- [ ] **Step 5: Run the python-client gate + commit**

```bash
cd python-client && uv run ruff format src/par_rt_db/errors.py tests/test_errors.py && uv run ruff check && uv run pyright && uv run pytest -q
git add python-client/src/par_rt_db/errors.py python-client/tests/test_errors.py
git commit -m "feat(python-client): parse retryAfter in RtDbError.from_envelope"
```

---

### Task 6: Docs + module-doc + kanban reconciliation

**Files:**
- Modify: `server/src/rate_limit.rs` (module doc), `CLAUDE.md`, `FEATURE_MATRIX.md`

- [ ] **Step 1: Update the `rate_limit.rs` module doc**

In `server/src/rate_limit.rs`, replace the "HTTP-only for v1" paragraph (lines 8-9):

```rust
//! HTTP-only for v1: the WebSocket handler keeps its existing per-connection
//! frame cap (`ws.rs`); message-level WS limiting is a documented follow-up.
```

with:

```rust
//! Shared by HTTP (`check_http_rate_limits`) and the reactive WS handler: the
//! `Mutate` and `Subscribe` arms call `evaluate` after re-authorizing and, on a
//! denial, reply with a typed `RATE_LIMITED` error (`MutateErr`/`SubscribeErr`)
//! carrying `retryAfter` — the connection stays open. The WS handler's separate
//! per-connection frame cap (`ws.rs::ConnRateLimiter`, 200 msgs/10s) is a coarse
//! flood valve that closes the socket and is independent of this limiter.
```

- [ ] **Step 2: Update `CLAUDE.md` (Two-transports section)**

Find the sentence in `CLAUDE.md` describing HTTP rate limiting that ends with "...WS-message-level limiting is a future enhancement)." Update the trailing clause so it reflects shipped behavior. Replace:

> HTTP-only v1 — the existing per-connection WS frame cap stays, WS-message-level limiting is a future enhancement

with:

> The same limiter also covers inbound WS `Mutate`/`Subscribe` frames (after the per-op `authorize` re-run): a denial replies with a `RATE_LIMITED` `MutateErr`/`SubscribeErr` carrying `retryAfter` and the connection stays open. The per-connection WS frame cap (200 msgs/10s, closes the socket) is a separate coarse flood valve.

(Match the exact wording/voice of the surrounding paragraph; locate the sentence with `grep -n "WS-message-level limiting is a future enhancement" CLAUDE.md`.)

- [ ] **Step 3: Update `FEATURE_MATRIX.md` if it has a rate-limiting row**

```bash
grep -n "rate.limit\|Rate.limit\|RATE_LIMIT" FEATURE_MATRIX.md
```
If a row exists and still implies WS is excluded, update it to note WS `Mutate`/`Subscribe` coverage and that all three clients mirror `RATE_LIMITED`+`retryAfter`. If no row exists, skip.

- [ ] **Step 4: Run the full gate**

```bash
make dev-db-up
make checkall
```
Expected: PASS end to end (fmt-check + clippy `-D warnings` + typecheck across server/ts-client/rust-client/dashboard/python-client + full test suite). If the dashboard typecheck fails on a fresh checkout, run `make ts-client-build` first.

- [ ] **Step 5: Commit docs**

```bash
git add server/src/rate_limit.rs CLAUDE.md FEATURE_MATRIX.md
git commit -m "docs: WS message-level rate limiting shipped (shared limiter, Mutate/Subscribe)"
```

- [ ] **Step 6: Reconcile the kanban item**

```bash
kanban item done --id 019fbe20719c7ef3a0ffb4de002202b8
```
(The kanban item was moved to `in_progress` at execution kickoff. `done` only after `make checkall` passes — the gate, not "code written".)

---

## Self-Review (completed during planning)

**Spec coverage:** every spec section maps to a task — shared `evaluate` (Task 1), WS wiring + rename (Task 2), TS mirror (Task 3), Rust mirror incl. mandatory deserialization fix (Task 4), Python mirror (Task 5), docs + module doc + kanban (Task 6). No spec requirement is untasked.

**Placeholder scan:** all code blocks contain real, runnable code matching the verified current source; no TBD/TODO/"add error handling"/"similar to Task N".

**Type/signature consistency:** `evaluate(state, principal, db) -> RateDecision` is defined in Task 1 and consumed identically in Task 2; `RateDecision::Denied { retry_after_secs }` matches `rate_limit.rs`; `RtDbError::rate_limited(retry_after_secs)` matches `error.rs`; `ConnRateLimiter` rename is consistent across struct/impl/construction; `retryAfter` (wire) / `retry_after` (Rust/Python) / `retryAfter` (TS) naming is consistent within each language; test helper names (`test_state_with_rate_limits`, `ws_connect`, `recv_json`, `mint_token`, `insert_work_item_txn`) match the verified `ws_test.rs`/`common/mod.rs`.
