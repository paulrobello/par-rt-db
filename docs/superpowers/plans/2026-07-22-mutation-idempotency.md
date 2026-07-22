# Safe mutation retry via idempotency keys — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a caller retry a mutation safely by reusing its `mutId`, without ever double-applying it, by adding a server-side per-database dedup table keyed on the existing `mutId`.

**Architecture:** Each database's Postgres schema gains a `mutations(mut_id PRIMARY KEY, result jsonb, expires_at bigint)` table, a sibling of the existing `meta` table. The single per-db committer task checks this table before executing a mutation carrying a `mut_id` and stores the result after — safe because the committer already serializes every mutation for a db, so two retries of the same `mut_id` can never race. Both the WS and HTTP one-shot transports plumb an optional `mut_id` through to the committer; the client SDKs let a caller supply their own stable id for retries, but never auto-retry on their own.

**Tech Stack:** Rust (axum/tokio/sqlx, Postgres 17) server; TypeScript client SDK (vitest).

## Global Constraints

- Dedup TTL is a fixed constant: 5 minutes (`300_000` ms). Not configurable in this pass.
- The WS wire vocabulary does not change — `mutId` is already required on `ClientMessage::Mutate`.
- The new HTTP `mutId` field must be additive: `#[serde(default)]`, optional, non-breaking for existing callers.
- No automatic client-side retry is introduced anywhere. Retry stays something the caller does explicitly by calling `mutate()` again with the same id. `client.ts`'s existing reject-on-close behavior (`handleClose`/`rejectPendingMutates`) does not change.
- Final verification gate: `make checkall` from the repo root (fmt-check + clippy `-D warnings` + typecheck + tests, both `server/` and `client/`) must be fully green.
- `make dev-db-up` must be running before any server test (`RTDB_TEST_DATABASE_URL` defaults to `postgres://rtdb:rtdb@127.0.0.1:55434/rtdb`).

---

### Task 1: Server dedup storage layer

**Files:**
- Create: `server/src/mutation_log.rs`
- Modify: `server/src/lib.rs:9` (register the new module)
- Modify: `server/src/db.rs:137-141` (`create_database` gains the `mutations` table)
- Create: `server/tests/mutation_dedup_test.rs`

**Interfaces:**
- Produces: `pub const mutation_log::DEDUP_TTL_MS: i64`; `pub async fn mutation_log::ensure_table(pool: &PgPool, db: &str) -> Result<(), RtDbError>`; `pub async fn mutation_log::check(pool: &PgPool, db: &str, mut_id: &str) -> Result<Option<Vec<serde_json::Value>>, RtDbError>`; `pub async fn mutation_log::store(pool: &PgPool, db: &str, mut_id: &str, results: &[serde_json::Value], ttl_ms: i64) -> Result<(), RtDbError>`. Task 2 wires these into `committer.rs`.

- [ ] **Step 1: Write `server/src/mutation_log.rs`**

```rust
use serde_json::Value;
use sqlx::PgPool;

use crate::db::now_ms;
use crate::ddl::pg_schema;
use crate::error::RtDbError;

/// How long a cached mutation result stays valid for dedup.
pub const DEDUP_TTL_MS: i64 = 5 * 60 * 1000;

/// Ensures the per-db mutation dedup table exists. Idempotent — safe to call
/// once per committer task startup, covering databases created before this
/// table existed (new databases get it from `db::create_database` instead).
pub async fn ensure_table(pool: &PgPool, db: &str) -> Result<(), RtDbError> {
    let schema = pg_schema(db);
    sqlx::query(&format!(
        "CREATE TABLE IF NOT EXISTS \"{schema}\".mutations (
            mut_id text PRIMARY KEY,
            result jsonb NOT NULL,
            expires_at bigint NOT NULL
        )"
    ))
    .execute(pool)
    .await?;
    Ok(())
}

/// Deletes `db`'s expired dedup entries, then looks up `mut_id`. `Some` means
/// this exact mutation already ran and its results should be replayed as-is,
/// with no re-execution and no fan-out. `None` means it's safe to execute.
pub async fn check(pool: &PgPool, db: &str, mut_id: &str) -> Result<Option<Vec<Value>>, RtDbError> {
    let schema = pg_schema(db);
    let now = now_ms();

    sqlx::query(&format!(
        "DELETE FROM \"{schema}\".mutations WHERE expires_at < $1"
    ))
    .bind(now)
    .execute(pool)
    .await?;

    let row: Option<(Value,)> = sqlx::query_as(&format!(
        "SELECT result FROM \"{schema}\".mutations WHERE mut_id = $1"
    ))
    .bind(mut_id)
    .fetch_optional(pool)
    .await?;

    match row {
        Some((value,)) => {
            let results: Vec<Value> = serde_json::from_value(value).map_err(|err| {
                tracing::error!(error = %err, db, mut_id, "failed to deserialize cached mutation result");
                RtDbError::internal("failed to read cached mutation result")
            })?;
            Ok(Some(results))
        }
        None => Ok(None),
    }
}

/// Caches `results` under `mut_id` for `ttl_ms`. Uses `ON CONFLICT DO NOTHING`
/// as a safety net only — the per-db committer already serializes every
/// mutation for `db`, so two concurrent stores of the same `mut_id` cannot
/// actually happen.
pub async fn store(
    pool: &PgPool,
    db: &str,
    mut_id: &str,
    results: &[Value],
    ttl_ms: i64,
) -> Result<(), RtDbError> {
    let schema = pg_schema(db);
    let expires_at = now_ms() + ttl_ms;
    let value = serde_json::to_value(results).map_err(|err| {
        tracing::error!(error = %err, db, mut_id, "failed to serialize mutation result for caching");
        RtDbError::internal("failed to cache mutation result")
    })?;

    sqlx::query(&format!(
        "INSERT INTO \"{schema}\".mutations (mut_id, result, expires_at) VALUES ($1, $2, $3)
         ON CONFLICT (mut_id) DO NOTHING"
    ))
    .bind(mut_id)
    .bind(value)
    .bind(expires_at)
    .execute(pool)
    .await?;
    Ok(())
}
```

- [ ] **Step 2: Register the module in `server/src/lib.rs`**

In `server/src/lib.rs`, insert a new line between `pub mod http_api;` and `pub mod protocol;` (alphabetical order, matching the existing list):

```rust
pub mod http_api;
pub mod mutation_log;
pub mod protocol;
```

- [ ] **Step 3: Add the `mutations` table to `create_database` in `server/src/db.rs`**

In `server/src/db.rs`, immediately after the existing `meta` table creation (the `sqlx::query(&format!("CREATE TABLE \"{schema_name}\".meta ..."))` block, currently lines 137-141), add:

```rust
    sqlx::query(&format!(
        "CREATE TABLE \"{schema_name}\".mutations (
            mut_id text PRIMARY KEY,
            result jsonb NOT NULL,
            expires_at bigint NOT NULL
        )"
    ))
    .execute(&mut *tx)
    .await?;
```

This runs inside the same transaction as `meta`'s creation, so both tables exist atomically with the schema itself.

- [ ] **Step 4: Write `server/tests/mutation_dedup_test.rs`**

```rust
mod common;

use common::{fresh_db, test_state};
use rtdb_server::mutation_log;

#[tokio::test]
async fn check_returns_none_when_absent() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let result = mutation_log::check(&state.pool, &db, "mut-1").await?;
    assert!(result.is_none());

    Ok(())
}

#[tokio::test]
async fn store_then_check_returns_cached_results() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let results = vec![serde_json::json!({"id": "abc123"})];
    mutation_log::store(&state.pool, &db, "mut-2", &results, mutation_log::DEDUP_TTL_MS).await?;

    let cached = mutation_log::check(&state.pool, &db, "mut-2").await?;
    assert_eq!(cached, Some(results));

    Ok(())
}

#[tokio::test]
async fn expired_entry_returns_none() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let results = vec![serde_json::json!({"id": "xyz789"})];
    mutation_log::store(&state.pool, &db, "mut-3", &results, 1).await?;
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    let cached = mutation_log::check(&state.pool, &db, "mut-3").await?;
    assert!(cached.is_none());

    Ok(())
}
```

- [ ] **Step 5: Run the new tests**

Run: `cd server && RTDB_TEST_DATABASE_URL="postgres://rtdb:rtdb@127.0.0.1:55434/rtdb" cargo test --test mutation_dedup_test`
Expected: PASS (3 passed). Requires `make dev-db-up` already running (check `docker ps` for a healthy `*-postgres-1` container first — reuse it rather than starting a second one; see repo convention on dev-db port conflicts across worktrees).

- [ ] **Step 6: Run the full server gate**

Run: `cd server && cargo fmt --check && cargo clippy --all-targets -- -D warnings`
Expected: no diffs, no warnings.

- [ ] **Step 7: Commit**

```bash
git add server/src/mutation_log.rs server/src/lib.rs server/src/db.rs server/tests/mutation_dedup_test.rs
git commit -m "feat(server): add per-db mutation dedup storage layer"
```

---

### Task 2: Wire `mut_id` through the committer, WS, and HTTP

**Files:**
- Modify: `server/src/committer.rs` (imports, `CommitterRequest::Mutate`, `Committers::mutate`, `run_committer`, `handle_mutate`)
- Modify: `server/src/ws.rs:312-330` (`ClientMessage::Mutate` arm)
- Modify: `server/src/http_api.rs:69-93` (`MutateRequest`, `mutate_handler`)
- Modify: `server/tests/subs_test.rs:157` (existing call site)
- Modify: `server/tests/mutation_dedup_test.rs` (append end-to-end tests)

**Interfaces:**
- Consumes: `mutation_log::{DEDUP_TTL_MS, ensure_table, check, store}` from Task 1.
- Produces: `Committers::mutate(&self, db: &str, mut_id: Option<String>, txn: Transaction) -> Result<TxnOutcome, RtDbError>` — the new signature every caller (WS, HTTP, tests) must use from here on.

- [ ] **Step 1: Update `server/src/committer.rs` imports**

At the top of `server/src/committer.rs`, add `std::collections::BTreeSet` and the new module:

```rust
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use sqlx::PgPool;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::{Mutex, mpsc, oneshot};

use crate::db::{SchemaCache, database_exists};
use crate::error::RtDbError;
use crate::mutation_log;
use crate::protocol::ServerMessage;
use crate::query::{Query, canonical, execute_query};
use crate::subs::{ConnId, SubscriptionManager};
use crate::txn::{Transaction, TxnOutcome, execute_txn};
```

- [ ] **Step 2: Add `mut_id` to `CommitterRequest::Mutate`**

```rust
pub enum CommitterRequest {
    Mutate {
        mut_id: Option<String>,
        txn: Transaction,
        reply: oneshot::Sender<Result<TxnOutcome, RtDbError>>,
    },
    Subscribe {
        conn: ConnId,
        query_id: String,
        query: Box<Query>,
        tx: UnboundedSender<ServerMessage>,
        reply: oneshot::Sender<Result<(), RtDbError>>,
    },
}
```

- [ ] **Step 3: Update `Committers::mutate`'s signature**

```rust
    /// Executes `txn` on `db` and waits for the fan-out-then-reply cycle to
    /// complete. `mut_id`, when present, is the idempotency key: a repeat
    /// call with the same `db` + `mut_id` replays the first call's cached
    /// results instead of re-executing.
    pub async fn mutate(
        &self,
        db: &str,
        mut_id: Option<String>,
        txn: Transaction,
    ) -> Result<TxnOutcome, RtDbError> {
        let (reply, reply_rx) = oneshot::channel();
        self.submit(db, CommitterRequest::Mutate { mut_id, txn, reply })
            .await?;
        reply_rx
            .await
            .map_err(|_| RtDbError::internal("committer task dropped the reply"))?
    }
```

- [ ] **Step 4: Ensure the dedup table exists at committer startup, and dispatch `mut_id` in `run_committer`**

```rust
async fn run_committer(
    pool: PgPool,
    db: String,
    subs: Arc<SubscriptionManager>,
    schemas: SchemaCache,
    mut rx: mpsc::Receiver<CommitterRequest>,
) {
    if let Err(err) = mutation_log::ensure_table(&pool, &db).await {
        tracing::error!(db = %db, error = %err, "failed to ensure mutations dedup table");
    }
    let ctx = CommitterCtx {
        pool,
        db,
        subs,
        schemas,
    };
    while let Some(req) = rx.recv().await {
        match req {
            CommitterRequest::Mutate { mut_id, txn, reply } => {
                let outcome = handle_mutate(&ctx, mut_id, txn).await;
                let _ = reply.send(outcome);
            }
            CommitterRequest::Subscribe {
                conn,
                query_id,
                query,
                tx,
                reply,
            } => {
                let result = handle_subscribe(&ctx, conn, query_id, *query, tx).await;
                let _ = reply.send(result);
            }
        }
    }
}
```

- [ ] **Step 5: Check/store dedup in `handle_mutate`**

```rust
async fn handle_mutate(
    ctx: &CommitterCtx,
    mut_id: Option<String>,
    txn: Transaction,
) -> Result<TxnOutcome, RtDbError> {
    if let Some(id) = &mut_id {
        if let Some(results) = mutation_log::check(&ctx.pool, &ctx.db, id).await? {
            return Ok(TxnOutcome {
                results,
                write_set: BTreeSet::new(),
            });
        }
    }

    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let outcome = execute_txn(&ctx.pool, &ctx.db, &schema, &txn).await?;
    ctx.subs
        .fan_out(&ctx.pool, &ctx.db, &schema, &outcome.write_set)
        .await;

    if let Some(id) = &mut_id {
        mutation_log::store(&ctx.pool, &ctx.db, id, &outcome.results, mutation_log::DEDUP_TTL_MS)
            .await?;
    }

    Ok(outcome)
}
```

- [ ] **Step 6: Update the WS `Mutate` arm in `server/src/ws.rs`**

Change line 314 only (the `mut_id` binding at line 312 and its later uses at 317/322/326 stay exactly as they are — `mut_id` is still needed there to tag the reply):

```rust
        ClientMessage::Mutate { mut_id, txn } => {
            match authorize(&state.pool, principal, db).await {
                Ok(()) => match state.committers.mutate(db, Some(mut_id.clone()), txn).await {
                    Ok(outcome) => {
                        let _ = out_tx.send(ServerMessage::MutateOk {
                            mut_id,
                            results: outcome.results,
                        });
                    }
                    Err(error) => {
                        let _ = out_tx.send(ServerMessage::MutateErr { mut_id, error });
                    }
                },
                Err(error) => {
                    let _ = out_tx.send(ServerMessage::MutateErr { mut_id, error });
                }
            }
            false
        }
```

- [ ] **Step 7: Add the optional `mut_id` field to HTTP's `MutateRequest` in `server/src/http_api.rs`**

```rust
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MutateRequest {
    db: String,
    txn: Transaction,
    #[serde(default)]
    mut_id: Option<String>,
}
```

And update `mutate_handler`:

```rust
async fn mutate_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    ApiJson(body): ApiJson<MutateRequest>,
) -> Result<Json<MutateResponse>, RtDbError> {
    let token = bearer_token(&headers)?;
    let principal = resolve_bearer(&state.pool, token).await?;
    authorize(&state.pool, &principal, &body.db).await?;

    let outcome = state.committers.mutate(&body.db, body.mut_id, body.txn).await?;
    Ok(Json(MutateResponse {
        results: outcome.results,
    }))
}
```

- [ ] **Step 8: Update the existing call site in `server/tests/subs_test.rs`**

Change line 157 from `state.committers.mutate(&db, insert_project()).await?;` to:

```rust
    state.committers.mutate(&db, None, insert_project()).await?;
```

- [ ] **Step 9: Append end-to-end dedup tests to `server/tests/mutation_dedup_test.rs`**

Add these imports and helper alongside the existing ones at the top of the file:

```rust
use rtdb_server::txn::{Step, Transaction};

fn valid_project_doc() -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({
        "name": "Alpha",
        "description": null,
        "status": "active",
        "tags": ["a", "b"],
        "updatedAt": 1.0
    })
    .as_object()
    .expect("json object")
    .clone()
}
```

Then append these three tests:

```rust
#[tokio::test]
async fn same_mut_id_dedups_and_replays_cached_result() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let txn = Transaction {
        steps: vec![Step::Insert {
            table: "projects".to_string(),
            doc: valid_project_doc(),
        }],
    };

    let first = state
        .committers
        .mutate(&db, Some("retry-key-1".to_string()), txn.clone())
        .await?;
    let second = state
        .committers
        .mutate(&db, Some("retry-key-1".to_string()), txn.clone())
        .await?;

    assert_eq!(first.results, second.results);

    let pg_schema = format!("db_{db}");
    let count: (i64,) =
        sqlx::query_as(&format!("SELECT COUNT(*) FROM \"{pg_schema}\".\"t_projects\""))
            .fetch_one(&state.pool)
            .await?;
    assert_eq!(count.0, 1);

    Ok(())
}

#[tokio::test]
async fn no_mut_id_does_not_dedup() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let txn = Transaction {
        steps: vec![Step::Insert {
            table: "projects".to_string(),
            doc: valid_project_doc(),
        }],
    };

    state.committers.mutate(&db, None, txn.clone()).await?;
    state.committers.mutate(&db, None, txn.clone()).await?;

    let pg_schema = format!("db_{db}");
    let count: (i64,) =
        sqlx::query_as(&format!("SELECT COUNT(*) FROM \"{pg_schema}\".\"t_projects\""))
            .fetch_one(&state.pool)
            .await?;
    assert_eq!(count.0, 2);

    Ok(())
}

#[tokio::test]
async fn expired_mut_id_re_executes() -> anyhow::Result<()> {
    let state = test_state().await;
    let db = fresh_db(&state).await;

    let txn = Transaction {
        steps: vec![Step::Insert {
            table: "projects".to_string(),
            doc: valid_project_doc(),
        }],
    };

    mutation_log::store(&state.pool, &db, "retry-key-2", &[], 0).await?;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;

    state
        .committers
        .mutate(&db, Some("retry-key-2".to_string()), txn.clone())
        .await?;

    let pg_schema = format!("db_{db}");
    let count: (i64,) =
        sqlx::query_as(&format!("SELECT COUNT(*) FROM \"{pg_schema}\".\"t_projects\""))
            .fetch_one(&state.pool)
            .await?;
    assert_eq!(count.0, 1);

    Ok(())
}
```

- [ ] **Step 10: Run the full test file plus the callers that changed**

Run: `cd server && RTDB_TEST_DATABASE_URL="postgres://rtdb:rtdb@127.0.0.1:55434/rtdb" cargo test --test mutation_dedup_test --test subs_test --test ws_test --test http_api_test`
Expected: PASS, all tests green.

- [ ] **Step 11: Run the full server gate**

Run: `cd server && cargo fmt --check && cargo clippy --all-targets -- -D warnings`
Expected: no diffs, no warnings.

- [ ] **Step 12: Commit**

```bash
git add server/src/committer.rs server/src/ws.rs server/src/http_api.rs server/tests/subs_test.rs server/tests/mutation_dedup_test.rs
git commit -m "feat(server): dedup mutations by mutId across WS and HTTP"
```

---

### Task 3: Client SDK — opt-in `mutId` for safe manual retry

**Files:**
- Modify: `client/src/client.ts:212-227` (`mutate()`)
- Modify: `client/src/http.ts:31-34` (`mutate()`)
- Modify: `client/tests/client.test.ts` (append one test)
- Modify: `client/tests/http.test.ts` (append two tests)

**Interfaces:**
- Consumes: nothing new from server tasks (client changes are independent of server internals; they only add an optional field to existing wire messages already accepted server-side after Task 2).
- Produces: `RtDbClient.mutate(txn: TransactionJson, opts?: { mutId?: string }): Promise<unknown[]>`; `RtDbHttpClient.mutate(txn: TransactionJson, opts?: { mutId?: string }): Promise<unknown[]>`.

- [ ] **Step 1: Add the optional `mutId` parameter to `RtDbClient.mutate()` in `client/src/client.ts`**

Replace the current `mutate` method (lines 212-227):

```ts
  mutate(txn: TransactionJson, opts?: { mutId?: string }): Promise<unknown[]> {
    const mutId = opts?.mutId ?? `mut-${++this.counter}`;
    return new Promise<unknown[]>((resolve, reject) => {
      if (this.stopped) {
        reject(new RtDbError("INTERNAL", "client is closed"));
        return;
      }
      const entry: QueuedMutate = { mutId, txn, resolve, reject };
      if (this.authState === "authenticated" && this.socket) {
        this.dispatchMutate(entry);
      } else {
        // Never sent yet: flush once on the next authOk. Not a retry.
        this.unsentMutates.push(entry);
      }
    });
  }
```

- [ ] **Step 2: Add the optional `opts` parameter to `RtDbHttpClient.mutate()` in `client/src/http.ts`**

Replace the current `mutate` method (lines 31-34):

```ts
  async mutate(txn: TransactionJson, opts?: { mutId?: string }): Promise<unknown[]> {
    const body = await this.post("/api/mutate", { db: this.db, txn, mutId: opts?.mutId });
    return (body as { results: unknown[] }).results;
  }
```

`JSON.stringify` drops an `undefined`-valued property, so when `opts` is omitted the request body has no `mutId` key at all — identical to today's wire shape.

- [ ] **Step 3: Append a test to `client/tests/client.test.ts`**

Add this `it()` inside the existing `describe("RtDbClient", ...)` block, after the `"resolves mutate on mutateOk and rejects on mutateErr"` test:

```ts
  it("uses an explicit opts.mutId as the wire mutId instead of an internal counter id", () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    client.mutate(
      { steps: [{ op: "insert", table: "items", doc: {} }] },
      { mutId: "caller-key-1" },
    );
    const frame = frames(sockets[0]).find((m) => m.type === "mutate") as unknown as {
      mutId: string;
    };
    expect(frame.mutId).toBe("caller-key-1");
  });
```

- [ ] **Step 4: Append two tests to `client/tests/http.test.ts`**

Add these inside the existing `describe("RtDbHttpClient", ...)` block, after the `"posts a mutation and returns the results array"` test:

```ts
  it("forwards opts.mutId in the request body when provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ results: ["new-id"] }));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    await client.mutate(mutation().insert("items", { title: "x" }).build(), {
      mutId: "caller-key-1",
    });

    const [, init] = fetchMock.mock.calls[0];
    expect(JSON.parse(init.body).mutId).toBe("caller-key-1");
  });

  it("omits mutId from the request body when not provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ results: ["new-id"] }));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    await client.mutate(mutation().insert("items", { title: "x" }).build());

    const [, init] = fetchMock.mock.calls[0];
    expect(JSON.parse(init.body)).not.toHaveProperty("mutId");
  });
```

- [ ] **Step 5: Run the client test suite**

Run: `cd client && bun run test`
Expected: PASS, all tests green (including the 3 new ones).

- [ ] **Step 6: Run the client typecheck**

Run: `cd client && bun run typecheck`
Expected: no errors.

- [ ] **Step 7: Commit**

```bash
git add client/src/client.ts client/src/http.ts client/tests/client.test.ts client/tests/http.test.ts
git commit -m "feat(client): let callers supply an explicit mutId for safe manual retry"
```

---

### Task 4: Docs — FEATURE_MATRIX and implementation plan

**Files:**
- Modify: `FEATURE_MATRIX.md` (row 4's `par-rt-db` column and "Implementation sketch" cell; section 5 "Recommended order")
- Modify: `docs/superpowers/plans/2026-07-22-mutation-idempotency.md` (this file — already created; commit it)

**Interfaces:** none — documentation only.

- [ ] **Step 1: Update row 4 in `FEATURE_MATRIX.md`**

Find the row (currently):

```
| 4 | 2 | **Safe mutation retry** (idempotency keys) | ✅ auto-retry, exactly-once | ❌ at-most-once by design | High | M | Server-side dedup table keyed by `mutId` (result cached, TTL'd); client can then retry on reconnect without double-apply. Biggest reliability gap for flaky networks; today only the explicit `PRECONDITION_FAILED` helper retries. |
```

Replace it with (`❌` becomes `✅`, and the sketch cell describes what shipped, matching the style of rows 1/6/7/13):

```
| 4 | 2 | **Safe mutation retry** (idempotency keys) | ✅ auto-retry, exactly-once | ✅ | High | M | Implemented — a per-db `mutations(mut_id, result, expires_at)` table (sibling of the existing `meta` table), checked and stored inside the single per-db committer task so a retry with the same `mutId` replays the cached result instead of re-executing (`mutation_log.rs`, `committer.rs`). TTL is a fixed 5 minutes. Deliberately opt-in, not automatic: `client.ts`'s reject-on-close behavior is unchanged, and callers retry by supplying the same `mutId` again — `RtDbClient.mutate(txn, {mutId})` and `RtDbHttpClient.mutate(txn, {mutId})`. Mirrored end-to-end: HTTP's `MutateRequest` gains an additive optional `mutId` field, with integration coverage in `mutation_dedup_test.rs` and passthrough coverage in `client.test.ts`/`http.test.ts`. |
```

- [ ] **Step 2: Update section 5 ("Recommended order") in `FEATURE_MATRIX.md`**

Find the sentence:

```
The next highest-value batch is
**#4 (safe retry)** and **#5 (pagination)**, the two remaining medium builds with the
highest app-facing payoff; #5's prerequisite (row 1's range support, for keyset pagination)
is now in place.
```

Replace with:

```
**#4 (safe retry)** is done as of 2026-07-22 — see its row above for what shipped. The next
highest-value build is **#5 (pagination)**, the remaining medium build with the highest
app-facing payoff; its prerequisite (row 1's range support, for keyset pagination) is
already in place.
```

- [ ] **Step 3: Commit the FEATURE_MATRIX update**

```bash
git add FEATURE_MATRIX.md
git commit -m "docs(feature-matrix): mark rank-4 safe mutation retry implemented"
```

- [ ] **Step 4: Commit the implementation plan doc**

```bash
git add docs/superpowers/plans/2026-07-22-mutation-idempotency.md
git commit -m "docs(plans): add implementation plan for mutation idempotency"
```

- [ ] **Step 5: Run the full project gate**

Run: `make checkall` from the repo root (or, if another worktree already owns the fixed dev-db port, the per-package equivalent: `cd server && cargo fmt --check && cargo clippy --all-targets -- -D warnings && RTDB_TEST_DATABASE_URL="postgres://rtdb:rtdb@127.0.0.1:55434/rtdb" cargo test`, then `cd client && bun run fmt-check && bun run lint && bun run typecheck && bun run test`).
Expected: fully green — this is the final gate before the kanban item is marked done.
