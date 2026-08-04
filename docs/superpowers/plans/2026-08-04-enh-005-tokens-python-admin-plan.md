# ENH-005 Scoped & Time-Limited Tokens + Python Admin Bootstrap — Implementation Plan (Unit 1)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give machine tokens an optional expiry, a read-only capability, and a table allowlist — enforced live at the auth and executor boundaries — and bootstrap the Python admin client with the new token surface.

**Architecture:** Add three columns to `rtdb_auth.machine_tokens`. Resolve the full token row in `resolve_bearer` and thread `read_only` + `tables` onto `Principal::Machine`; keep expiry + revocation as live checks in `authorize`. Enforce read-only at the write gates (WS mutate/schedule, HTTP mutate, storage) and table-scoping at the executor boundary (query/txn/subs) alongside the existing per-row-auth checks. Mirror the new `MintTokenRequest`/`TokenRow` fields byte-identically across ts-client, rust-client, the new python admin module, and the dashboard. Build a dashboard Tokens page.

**Tech Stack:** Rust (axum/sqlx/tokio), TypeScript (bun/vite React dashboard), Python (httpx, pyright), Postgres 17.

## Global Constraints

- **Wire contract is byte-identical** across `server/src/auth`+`admin.rs`, `ts-client/src/admin.ts`, `rust-client/src/wire.rs`, `dashboard/src/lib/types.ts`, and the new `python-client/src/par_rt_db/admin.py`. Casing is camelCase on the wire (`expiresAt`, `readOnly`, `tokenId`, `createdAt`); Rust structs use `#[serde(rename_all = "camelCase")]` or per-field `rename`. New fields are **optional** on the wire so old clients keep working.
- **New DB columns are additive with safe defaults**: `expires_at BIGINT NULL`, `read_only BOOLEAN NOT NULL DEFAULT false`, `tables TEXT[] NULL`. Existing full-access tokens (NULL/empty new fields) must behave exactly as before.
- **Single-writer invariant untouched**: no new code path calls `execute_txn` outside the committer. New checks are read-only gates that run before/within existing paths.
- **SQL safety**: double-quote identifiers, bind every value via `$n`, never interpolate. Use `fetch_optional` for lookups that can miss.
- **Errors**: every new failure is an `RtDbError { code, message }`. Read-only/table-scope denial → `Forbidden`/403; expired → `Unauthorized`/401. No `unwrap`/`expect` outside `#[cfg(test)]`. Zero clippy warnings (`-D warnings`).
- **Gate**: `make checkall` (fmt-check + clippy + typecheck + tests) must pass before the ENH-005 checkbox flips. Integration tests need `make dev-db-up` (dev Postgres on `127.0.0.1:55434`).
- **Machine tokens bypass per-row rules (no user identity) but are bound by their own capabilities.** Scheduled jobs (system-initiated, `owner=None`, no token) are unaffected by token capabilities. Admin principals bypass everything.

**Branch:** `enh-005-tokens` (in-place feature branch off `main`, per par-rt-db's trunk-based practice). Orchestrator creates it before Task 1.

---

## File Structure

**Server (Rust) — contract source of truth:**
- `server/src/db.rs` (modify ~`:99-110`) — extend the `machine_tokens` ensure with `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for the three new columns.
- `server/src/auth/tokens.rs` (modify `:9-31`) — `mint_token` gains `expires_at: Option<i64>`, `read_only: bool`, `tables: Option<&[String]>`.
- `server/src/auth/mod.rs` (modify `:18-119`) — extend `Principal::Machine` with `read_only: bool`, `tables: Option<Vec<String>>`; add `Principal::is_read_only()`; extend `resolve_bearer` to fetch the full row; extend `authorize` Machine arm with expiry; add `authorize_table()`.
- `server/src/ws.rs` (modify mutate/schedule arms ~`:358,430,494,552,585`) — read-only gate.
- `server/src/http_api.rs` (modify mutate + storage handlers) — read-only gate.
- `server/src/query.rs`, `server/src/txn.rs`, `server/src/subs.rs` — `authorize_table` gate at the executor boundary.
- `server/src/admin.rs` (modify `:231-264, 952-981`) — extend `MintTokenRequest`, `TokenRow`, `mint_token`/`list_tokens` handlers.
- `server/tests/auth_test.rs` (or the existing token test binary) — new integration tests.

**ts-client:** `ts-client/src/admin.ts` (`:36-41,200`), `ts-client/tests/admin.test.ts`.

**rust-client:** `rust-client/src/wire.rs` (`:473,543`), `rust-client/tests/`.

**python-client (NEW admin surface):** `python-client/src/par_rt_db/admin.py` (sync `RtDbAdminClient` + async `AsyncRtDbAdminClient`), `python-client/src/par_rt_db/__init__.py` (export), `python-client/tests/test_admin.py`.

**dashboard:** `dashboard/src/lib/types.ts` (`:98`), `dashboard/src/lib/admin.tsx` (add `mintToken`/`revokeToken`), `dashboard/src/pages/TokensPage.tsx` (+ `.module.css`), `dashboard/src/App.tsx` (`:42-58`), `dashboard/src/shell/AppShell.tsx` (`:9-19`).

**Docs:** `ENHANCEMENTS.md` (flip ENH-005), `FEATURE_MATRIX.md` (token row).

---

## Task 1: Server — token table migration + `mint_token` signature

**Files:**
- Modify: `server/src/db.rs` (the `ensure` for `rtdb_auth.machine_tokens`, ~`:99-110`)
- Modify: `server/src/auth/tokens.rs:9-31`
- Test: `server/tests/auth_test.rs` (the existing token binary; add a test)

**Interfaces:**
- Produces: `mint_token(pool, db, name, expires_at: Option<i64>, read_only: bool, tables: Option<&[String]>) -> Result<(String, String), RtDbError>` (returns `(id, plaintext)` unchanged). Columns `expires_at`, `read_only`, `tables` now exist on the table.

- [ ] **Step 1: Write the failing test**

In `server/tests/auth_test.rs`, add (using the existing `TestDb`/`TestCtx` harness already used by token tests in that binary):

```rust
#[tokio::test]
async fn mint_token_records_capabilities() {
    let ctx = TestCtx::setup().await;
    let (id, _plaintext) =
        auth::tokens::mint_token(&ctx.pool, "dbx", "scraper", Some(1_700_000_000_000), true, Some(&["users".into()]))
            .await
            .unwrap();
    let row: (Option<i64>, bool, Option<Vec<String>>) = sqlx::query_as(
        "SELECT expires_at, read_only, tables FROM rtdb_auth.machine_tokens WHERE id = $1",
    )
    .bind(&id)
    .fetch_one(&ctx.pool)
    .await
    .unwrap();
    assert_eq!(row.0, Some(1_700_000_000_000));
    assert!(row.1);
    assert_eq!(row.2, Some(vec!["users".to_string()]));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test auth_test mint_token_records_capabilities`
Expected: FAIL — column `expires_at` does not exist (and signature mismatch).

- [ ] **Step 3: Add the columns**

In `server/src/db.rs`, after the `CREATE TABLE IF NOT EXISTS rtdb_auth.machine_tokens (...)` statement, add idempotent alters (run in the same ensure function, after the create):

```rust
sqlx::query(
    "ALTER TABLE rtdb_auth.machine_tokens ADD COLUMN IF NOT EXISTS expires_at BIGINT NULL",
).execute(pool).await?;
sqlx::query(
    "ALTER TABLE rtdb_auth.machine_tokens ADD COLUMN IF NOT EXISTS read_only BOOLEAN NOT NULL DEFAULT false",
).execute(pool).await?;
sqlx::query(
    "ALTER TABLE rtdb_auth.machine_tokens ADD COLUMN IF NOT EXISTS tables TEXT[] NULL",
).execute(pool).await?;
```

- [ ] **Step 4: Extend `mint_token`**

In `server/src/auth/tokens.rs`, change the signature and INSERT:

```rust
pub async fn mint_token(
    pool: &PgPool,
    db: &str,
    name: &str,
    expires_at: Option<i64>,
    read_only: bool,
    tables: Option<&[String]>,
) -> Result<(String, String), RtDbError> {
    let id = new_id();
    let token = random_token();
    let hash = sha256_hex(&token);
    sqlx::query(
        "INSERT INTO rtdb_auth.machine_tokens \
         (id, db_name, name, token_hash, revoked, created_at, expires_at, read_only, tables) \
         VALUES ($1, $2, $3, $4, false, $5, $6, $7, $8)",
    )
    .bind(&id)
    .bind(db)
    .bind(name)
    .bind(&hash)
    .bind(now_ms())
    .bind(expires_at)
    .bind(read_only)
    .bind(tables)
    .execute(pool)
    .await?;
    Ok((id, token))
}
```

Update every existing caller of `mint_token` (the admin handler at `admin.rs:244` and any test) to pass the new args — pass `None, false, None` for the old full-access behavior. The compiler will flag each call site.

- [ ] **Step 5: Run test to verify it passes**

Run: `cargo test --test auth_test mint_token_records_capabilities`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add server/src/db.rs server/src/auth/tokens.rs server/tests/auth_test.rs server/src/admin.rs
git commit -m "feat(server): token table gains expires_at/read_only/tables + mint_token args (ENH-005)"
```

---

## Task 2: Server — thread capabilities onto `Principal` + live expiry

**Files:**
- Modify: `server/src/auth/mod.rs:18-119`
- Test: `server/tests/auth_test.rs`

**Interfaces:**
- Produces: `Principal::Machine { db: String, token_id: String, read_only: bool, tables: Option<Vec<String>> }`; `Principal::is_read_only(&self) -> bool`; `authorize` rejects expired tokens live.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn resolve_bearer_threads_capabilities_and_expiry() {
    let ctx = TestCtx::setup().await;
    let (_id, plaintext) =
        auth::tokens::mint_token(&ctx.pool, "dbx", "ro", None, true, Some(&["users".into()])).await.unwrap();

    let p = auth::resolve_bearer(&ctx.pool, &plaintext).await.unwrap();
    match p {
        auth::Principal::Machine { read_only, tables, .. } => {
            assert!(read_only);
            assert_eq!(tables, Some(vec!["users".to_string()]));
        }
        _ => panic!("expected Machine"),
    }

    // Expired token is rejected on authorize.
    let (_eid, ept) =
        auth::tokens::mint_token(&ctx.pool, "dbx", "exp", Some(1), false, None).await.unwrap(); // expires_at in the past
    let ep = auth::resolve_bearer(&ctx.pool, &ept).await.unwrap();
    let err = auth::authorize(&ctx.pool, &ep, "dbx").await.unwrap_err();
    assert!(matches!(err.code(), crate::error::ErrorCode::Unauthorized));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --test auth_test resolve_bearer_threads_capabilities_and_expiry`
Expected: FAIL — `Principal::Machine` has no `read_only`/`tables` fields.

- [ ] **Step 3: Extend `Principal` and resolution**

In `server/src/auth/mod.rs`:

```rust
pub enum Principal {
    Machine {
        db: String,
        token_id: String,
        read_only: bool,
        tables: Option<Vec<String>>,
    },
    User { /* unchanged */ },
}

impl Principal {
    pub fn is_read_only(&self) -> bool {
        matches!(self, Principal::Machine { read_only: true, .. })
    }
}
```

Extend the machine lookup in `resolve_bearer` to fetch the full row:

```rust
let machine: Option<(String, String, bool, Option<Vec<String>>, Option<i64>)> = sqlx::query_as(
    "SELECT id, db_name, read_only, tables, expires_at \
     FROM rtdb_auth.machine_tokens WHERE token_hash = $1 AND NOT revoked",
)
.bind(&hash)
.fetch_optional(pool)
.await?;

if let Some((token_id, db, read_only, tables, _expires_at)) = machine {
    return Ok(Principal::Machine { db, token_id, read_only, tables });
}
```

(Expiry is not stored on `Principal`; it stays a live check in `authorize`.)

Extend the `authorize` Machine arm's EXISTS predicate to include expiry:

```rust
let (live,): (bool,) = sqlx::query_as(
    "SELECT EXISTS(SELECT 1 FROM rtdb_auth.machine_tokens \
     WHERE id = $1 AND NOT revoked AND (expires_at IS NULL OR expires_at > $2))",
)
.bind(token_id)
.bind(now_ms())
.fetch_one(pool)
.await?;
```

Fix every other `Principal::Machine { .. }` match arm the compiler flags (add the two new fields or use `..`). `is_read_only()` is the helper for the read-only check.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test --test auth_test resolve_bearer_threads_capabilities_and_expiry`
Expected: PASS. Then `cargo build -p par-rt-db` to confirm all match arms compile.

- [ ] **Step 5: Commit**

```bash
git add server/src/auth/mod.rs server/tests/auth_test.rs
git commit -m "feat(server): thread read_only/tables onto Principal, live token expiry (ENH-005)"
```

---

## Task 3: Server — enforce read-only at the write gates

**Files:**
- Modify: `server/src/ws.rs` (Mutate + Schedule per-op arms)
- Modify: `server/src/http_api.rs` (mutate + storage upload/delete handlers)
- Test: `server/tests/auth_test.rs` (or the http/ws integration test binary)

**Interfaces:**
- Consumes: `Principal::is_read_only()` from Task 2.
- Produces: a read-only machine token is rejected with `Forbidden` on any write path (WS Mutate/Schedule, HTTP mutate, storage upload/delete); reads are unaffected.

- [ ] **Step 1: Write the failing test**

Use the existing live-server HTTP integration harness (`http_integration.rs` style) or the WS test binary. Test against HTTP mutate for determinism:

```rust
#[tokio::test]
async fn read_only_token_cannot_mutate_but_can_query() {
    let ctx = TestCtx::setup_with_db("dbx").await;
    let (_id, ro) = ctx.mint("dbx", "ro", None, true, None).await; // helper minting a token
    let client = TestClient::new(&ctx, &ro);

    // Mutate is forbidden.
    let resp = client.mutate("dbx", r#"[{"insert":{"table":"t","id":"a","doc":{}}}]"#).await;
    assert_eq!(resp.status(), 403);

    // Query is allowed (no row, but must not be 403).
    let resp = client.query("dbx", r#"{"table":"t","take":10}"#).await;
    assert_ne!(resp.status(), 403);
}
```

(Adapt helper names to the existing test binaries; the assertion is the contract.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test read_only_token_cannot_mutate_but_can_query`
Expected: FAIL — mutate returns 200 (no read-only enforcement yet).

- [ ] **Step 3: Add the read-only gate**

At each write entrypoint, after the existing `authorize(...)` call, add a read-only check on the principal. For HTTP, in the mutate handler and storage upload/delete handlers:

```rust
authorize(&state.pool, &principal, &db).await?;
if principal.is_read_only() {
    return Err(RtDbError::forbidden("read-only token cannot mutate"));
}
```

For WS, in the `Mutate` and `Schedule` per-op arms (where `authorize` is re-run), add the same check before enqueueing — return a `MutateErr`/`ScheduleErr` with code `Forbidden`.

(Storage upload/delete are writes; gate them too. Query/subscribe are reads; do not gate.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test read_only_token_cannot_mutate_but_can_query`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/src/ws.rs server/src/http_api.rs server/tests/
git commit -m "feat(server): enforce read-only tokens at write gates (ENH-005)"
```

---

## Task 4: Server — enforce table allowlist at the executor boundary

**Files:**
- Modify: `server/src/auth/mod.rs` (add `authorize_table`)
- Modify: `server/src/query.rs` (read path), `server/src/txn.rs` (per-step write path), `server/src/subs.rs` (register)
- Test: `server/tests/auth_test.rs`

**Interfaces:**
- Produces: `pub fn authorize_table(principal: &Principal, table: &str) -> Result<(), RtDbError>` — `Ok(())` for `User`/admin/`None` tables; `Forbidden` only when `Machine` with a non-empty `tables` allowlist not containing `table`.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn table_scoped_token_restricted() {
    let ctx = TestCtx::setup_with_db("dbx").await;
    let (_id, tok) = ctx.mint("dbx", "scoped", None, false, Some(&["allowed".into()])).await;
    let client = TestClient::new(&ctx, &tok);

    // Mutate on an allowed table succeeds (push schema first).
    ctx.push_schema("dbx", r#"[{"name":"allowed","fields":[]}]"#).await;
    ctx.push_schema("dbx", r#"[{"name":"forbidden","fields":[]}]"#).await;
    assert_eq!(client.mutate_table("dbx","allowed").await.status(), 200);
    assert_eq!(client.mutate_table("dbx","forbidden").await.status(), 403);

    // Query on a forbidden table is also rejected.
    assert_eq!(client.query_table("dbx","forbidden").await.status(), 403);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test table_scoped_token_restricted`
Expected: FAIL — both tables allowed (no enforcement).

- [ ] **Step 3: Add `authorize_table` and wire it**

In `server/src/auth/mod.rs`:

```rust
/// Table allowlist gate for machine tokens. `User`/admin/`None`-tables bypass.
pub fn authorize_table(principal: &Principal, table: &str) -> Result<(), RtDbError> {
    if let Principal::Machine { tables: Some(list), .. } = principal {
        if !list.is_empty() && !list.iter().any(|t| t == table) {
            return Err(RtDbError::forbidden("token is not scoped for this table"));
        }
    }
    Ok(())
}
```

Call it at the executor boundary:
- **Reads** (`query.rs`): where the query's table is known and the principal is available — call `authorize_table(&principal, &query.table)?` before executing. (Admin/`owner=None` paths skip it; machine tokens carry the principal.)
- **Writes** (`txn.rs`): for each step, `authorize_table(&principal, &step.table)?` inside the serialized txn, before applying. Machine principal is available on the mutate request.
- **Subscriptions** (`subs.rs` register): `authorize_table(&principal, &query.table)?` at registration so a scoped token cannot subscribe to a forbidden table.

The principal is already threaded into these paths for per-row auth (`PrincipalCtx`); reuse that plumbing. If the principal is `None` (admin/scheduled), skip.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test table_scoped_token_restricted`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add server/src/auth/mod.rs server/src/query.rs server/src/txn.rs server/src/subs.rs server/tests/
git commit -m "feat(server): enforce token table allowlist at executor boundary (ENH-005)"
```

---

## Task 5: Server — admin endpoints expose the new token shapes

**Files:**
- Modify: `server/src/admin.rs:231-264` (`MintTokenRequest`, `MintTokenResponse`), `:952-981` (`TokenRow`, `list_tokens`)
- Test: `server/tests/admin_test.rs` (or the admin test binary)

**Interfaces:**
- Produces: `POST /admin/mint-token` accepts optional `expiresAt`, `readOnly`, `tables`; `GET /admin/tokens?db=` returns rows including `expiresAt`, `readOnly`, `tables`. Wire field names camelCase.

- [ ] **Step 1: Write the failing test**

```rust
#[tokio::test]
async fn mint_and_list_token_with_capabilities() {
    let ctx = AdminTestCtx::setup().await;
    let body = ctx.post("/admin/mint-token", serde_json::json!({
        "db": "dbx", "name": "scraper", "readOnly": true, "tables": ["users"], "expiresAt": 1_700_000_000_000
    })).await;
    let token_id = body["tokenId"].as_str().unwrap().to_string();

    let listed = ctx.get("/admin/tokens?db=dbx").await;
    let row = listed["tokens"].as_array().unwrap().iter().find(|r| r["id"] == token_id).unwrap();
    assert_eq!(row["readOnly"], true);
    assert_eq!(row["tables"], serde_json::json!(["users"]));
    assert_eq!(row["expiresAt"], 1_700_000_000_000);
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test mint_and_list_token_with_capabilities`
Expected: FAIL — fields not serialized.

- [ ] **Step 3: Extend the structs and handlers**

```rust
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MintTokenRequest {
    db: String,
    name: String,
    #[serde(default)]
    expires_at: Option<i64>,
    #[serde(default)]
    read_only: bool,
    #[serde(default)]
    tables: Option<Vec<String>>,
}
```

In `mint_token` handler, pass them through to `tokens::mint_token(&pool, &req.db, &req.name, req.expires_at, req.read_only, req.tables.as_deref())`.

Extend `TokenRow` (`#[serde(rename_all="camelCase")]`) with `expires_at: Option<i64>`, `read_only: bool`, `tables: Option<Vec<String>>`, and extend the `list_tokens` SQL to `SELECT id, name, created_at, revoked, expires_at, read_only, tables FROM rtdb_auth.machine_tokens WHERE db_name = $1 ORDER BY created_at`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test mint_and_list_token_with_capabilities`
Expected: PASS.

- [ ] **Step 5: Run the server gate so far**

Run: `cd server && cargo test && cargo clippy -- -D warnings && cargo fmt --check`
Expected: all green (server-only subset; full `make checkall` after client mirrors).

- [ ] **Step 6: Commit**

```bash
git add server/src/admin.rs server/tests/
git commit -m "feat(server): admin mint/list tokens expose expiresAt/readOnly/tables (ENH-005)"
```

---

## Task 6: ts-client mirror

**Files:**
- Modify: `ts-client/src/admin.ts:36-41` (`TokenInfo`), `:200` (`mintToken`)
- Test: `ts-client/tests/admin.test.ts`

**Interfaces:**
- Consumes: server wire from Task 5.
- Produces: `mintToken(db, name, opts?: { expiresAt?, readOnly?, tables? })`; `TokenInfo` gains `expiresAt?: number; readOnly: boolean; tables?: string[]`.

- [ ] **Step 1: Write the failing test**

```ts
test("mintToken sends capabilities and listTokens returns them", async () => {
  const { tokenId } = await admin.mintToken("dbx", "scraper", { readOnly: true, tables: ["users"], expiresAt: 1700000000000 });
  const rows = await admin.listTokens("dbx");
  const row = rows.find((r) => r.id === tokenId)!;
  expect(row.readOnly).toBe(true);
  expect(row.tables).toEqual(["users"]);
  expect(row.expiresAt).toBe(1700000000000);
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd ts-client && bunx vitest run tests/admin.test.ts`
Expected: FAIL — `mintToken` takes no opts; `readOnly` undefined.

- [ ] **Step 3: Extend the types and method**

```ts
export interface TokenInfo {
  id: string; name: string; createdAt: number; revoked: boolean;
  expiresAt?: number; readOnly: boolean; tables?: string[];
}
export interface MintTokenOptions { expiresAt?: number; readOnly?: boolean; tables?: string[] }

async mintToken(db: string, name: string, opts: MintTokenOptions = {}): Promise<{ tokenId: string; token: string }> {
  return this.req("/admin/mint-token", { method: "POST", body: JSON.stringify({ db, name, ...opts }) });
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd ts-client && bunx vitest run tests/admin.test.ts`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add ts-client/src/admin.ts ts-client/tests/admin.test.ts
git commit -m "feat(ts-client): mirror token capabilities (ENH-005)"
```

---

## Task 7: rust-client mirror

**Files:**
- Modify: `rust-client/src/wire.rs:473` (`MintTokenRequest`), `:543` (`TokenInfo`)
- Modify: the admin method in `rust-client/src/admin.rs` (or wherever `mint_token`/`list_tokens` client methods live)
- Test: `rust-client/tests/`

**Interfaces:**
- Produces: `MintTokenRequest` gains optional `expires_at`/`read_only`/`tables` (serde camelCase); `TokenInfo` gains the same. Wire-identical to the server.

- [ ] **Step 1: Write the failing test**

In the rust-client test binary, assert that `mint_token` with `read_only`/`tables` round-trips and `list_tokens` returns them (against the server contract; use a typed fixture if no live server).

- [ ] **Step 2: Run test to verify it fails**

Run: `cd rust-client && cargo test mint_token`
Expected: FAIL — fields absent.

- [ ] **Step 3: Extend the structs**

```rust
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct MintTokenRequest<'a> {
    db: &'a str,
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    expires_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")] read_only: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")] tables: Option<&'a [String]>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenInfo {
    pub id: String, pub name: String, pub created_at: i64, pub revoked: bool,
    #[serde(default)] pub expires_at: Option<i64>,
    #[serde(default)] pub read_only: bool,
    #[serde(default)] pub tables: Option<Vec<String>>,
}
```

Update the `mint_token` client method to accept the new optional args and build the request.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd rust-client && cargo test mint_token`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add rust-client/src/
git commit -m "feat(rust-client): mirror token capabilities (ENH-005)"
```

---

## Task 8: Python admin client bootstrap (token surface)

**Files:**
- Create: `python-client/src/par_rt_db/admin.py`
- Modify: `python-client/src/par_rt_db/__init__.py` (export `RtDbAdminClient`, `AsyncRtDbAdminClient`)
- Modify: `python-client/pyproject.toml` (ensure admin methods ride the existing `[http]`/`[aio]` extras — httpx already the dep; no new extra needed)
- Test: `python-client/tests/test_admin.py`

**Interfaces:**
- Produces: `RtDbAdminClient` (sync, httpx) and `AsyncRtDbAdminClient` (async, httpx.AsyncClient) with a `req()` helper, bearer-authed, JSON-error aware. Token methods: `mint_token(db, name, *, expires_at=None, read_only=False, tables=None) -> MintedToken`, `revoke_token(token_id) -> None`, `list_tokens(db) -> list[TokenInfo]`. This task ships **only the token methods + class skeleton**; later units and the parity sweep add the rest.

- [ ] **Step 1: Write the failing test**

Mirror the existing python-client test style (see `python-client/tests/`; use the in-memory harness or a live server if one is running). Test the dataclass shapes and the request body:

```python
def test_mint_and_list_token_with_capabilities(admin_client):
    minted = admin_client.mint_token("dbx", "scraper", read_only=True, tables=["users"], expires_at=1700000000000)
    assert minted.token_id
    rows = admin_client.list_tokens("dbx")
    row = next(r for r in rows if r.id == minted.token_id)
    assert row.read_only is True
    assert row.tables == ["users"]
    assert row.expires_at == 1700000000000
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd python-client && uv run pytest -q tests/test_admin.py`
Expected: FAIL — module `par_rt_db.admin` does not exist.

- [ ] **Step 3: Create the admin module**

Create `python-client/src/par_rt_db/admin.py` with two classes mirroring the existing sync/async client split in the package (read `http.py`/the async client for the base URL + auth + `req` pattern, and follow it). Skeleton:

```python
from dataclasses import dataclass
from typing import Optional

@dataclass
class MintedToken:
    token_id: str
    token: str

@dataclass
class TokenInfo:
    id: str
    name: str
    created_at: int
    revoked: bool
    expires_at: Optional[int] = None
    read_only: bool = False
    tables: Optional[list[str]] = None

class RtDbAdminClient:
    def __init__(self, base_url: str, admin_key: str): ...
    def mint_token(self, db, name, *, expires_at=None, read_only=False, tables=None) -> MintedToken: ...
    def revoke_token(self, token_id) -> None: ...
    def list_tokens(self, db) -> list[TokenInfo]: ...

class AsyncRtDbAdminClient:
    def __init__(self, base_url: str, admin_key: str): ...
    async def mint_token(self, db, name, *, expires_at=None, read_only=False, tables=None) -> MintedToken: ...
    async def revoke_token(self, token_id) -> None: ...
    async def list_tokens(self, db) -> list[TokenInfo]: ...
```

Implement `req()` to POST/GET with bearer admin key, raise on the `{code,message}` error envelope (mirror the existing client's error handling), and serialize the camelCase body (`expiresAt`, `readOnly`, `tables`).

- [ ] **Step 4: Run test to verify it passes**

Run: `cd python-client && uv run pytest -q tests/test_admin.py`
Expected: PASS.

- [ ] **Step 5: Typecheck and lint**

Run: `cd python-client && uv run pyright && uv run ruff check .`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add python-client/src/par_rt_db/admin.py python-client/src/par_rt_db/__init__.py python-client/tests/test_admin.py
git commit -m "feat(python-client): bootstrap admin client with token surface (ENH-005)"
```

---

## Task 9: Dashboard Tokens page

**Files:**
- Modify: `dashboard/src/lib/types.ts:98` (`TokenRow`), `dashboard/src/lib/admin.tsx` (add `mintToken`/`revokeToken`)
- Create: `dashboard/src/pages/TokensPage.tsx`, `dashboard/src/pages/TokensPage.module.css`
- Modify: `dashboard/src/App.tsx:42-58` (route), `dashboard/src/shell/AppShell.tsx:9-19` (nav)
- Test: `dashboard` typecheck (the dashboard has no unit-test runner by default; gate on `tsc` + a Playwright/vitest smoke if one exists — otherwise a typecheck + manual route check)

**Interfaces:**
- Consumes: ts-client `TokenInfo` shape (Task 6).
- Produces: a Tokens page reachable from the nav, with a mint form (name + expiry + read-only toggle + tables multi-select) and a list with revoke.

- [ ] **Step 1: Extend types and client**

In `dashboard/src/lib/types.ts`, extend `TokenRow` with `expiresAt?: number; readOnly: boolean; tables?: string[]`. In `dashboard/src/lib/admin.tsx`, add `mintToken(db, name, opts)` and `revokeToken(tokenId)` mirroring `ts-client` (POST/DELETE with the session bearer).

- [ ] **Step 2: Build the page**

Create `TokensPage.tsx` following the `StoragePage`/`ScheduledJobsPage` per-db pattern: `useAdmin()` client, db selector, `load()` calling `listTokens(db)`, a mint form, a table with revoked/expiry/readOnly/tables badges and a revoke action. Co-located `.module.css`. Register the route in `App.tsx` and add a nav entry in `AppShell.tsx`.

- [ ] **Step 3: Typecheck and build**

Run: `make ts-client-build && cd dashboard && bunx tsc --noEmit && bun run build`
Expected: clean (ts-client dist must be built first — the dashboard resolves `@par-rt-db/client` from `ts-client/dist`).

- [ ] **Step 4: Commit**

```bash
git add dashboard/src/ ts-client/dist  # only if dist is committed; it is gitignored — do not add it
git commit -m "feat(dashboard): Tokens page with scoped/expiring token mint (ENH-005)"
```

(dist is gitignored — only stage `dashboard/src/`.)

---

## Task 10: Docs + full gate + close ENH-005

**Files:**
- Modify: `ENHANCEMENTS.md` (flip ENH-005 `[ ]`→`[x]`), `FEATURE_MATRIX.md` (token row note)

- [ ] **Step 1: Run the full gate**

Run: `make dev-db-up && make checkall`
Expected: fmt-check + clippy `-D warnings` + typecheck + tests all green across server, ts-client, rust-client, dashboard, python-client.

- [ ] **Step 2: Update docs**

Flip the ENH-005 checkbox in `ENHANCEMENTS.md`. Update the token row in `FEATURE_MATRIX.md` if it references token capabilities. Add a one-line note to the dashboard README if it lists pages.

- [ ] **Step 3: Commit and report**

```bash
git add ENHANCEMENTS.md FEATURE_MATRIX.md dashboard/README.md
git commit -m "docs: ENH-005 complete — scoped/time-limited tokens + python admin bootstrap"
```

Report: server gate green, all four clients mirror the new token shape, dashboard Tokens page ships, ENH-005 checkbox flipped. Note that Units 2–5 (webhooks, audit, subscriptions, python parity sweep) get their own plans next.

---

## Self-Review (completed)

- **Spec coverage:** §2.1 table → Task 1; §2.2 capability threading → Task 2; §2.3 read-only enforcement → Task 3, table-scoping → Task 4; §2.4 wire shapes → Tasks 5–8; §2.5 dashboard → Task 9; §6 python admin bootstrap (token slice) → Task 8. Verification §8 → Task 10. All §2 requirements covered.
- **Placeholder scan:** No TBD/TODO. Test code is concrete; helper names (`TestCtx`, `AdminTestCtx`, `TestClient`) are flagged "adapt to existing binaries" where the exact harness name must be confirmed against the test file — the implementer reads the binary first.
- **Type consistency:** `Principal::Machine { db, token_id, read_only, tables }` used identically in Tasks 2–4. `mint_token` signature consistent across Tasks 1, 5, 6, 7, 8. Wire field names camelCase everywhere (`expiresAt`, `readOnly`, `tables`).
- **Scope:** Single unit (ENH-005 + python admin token slice). Units 2–5 are separate plans.

## Out of scope for this plan (deferred to later units)

- Python admin methods beyond tokens (webhooks/audit/subscriptions/legacy) → Units 2–5 + parity sweep.
- Per-subscription (not per-db) counters, webhook secrets → spec §9.
