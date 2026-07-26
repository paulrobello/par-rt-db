# Client Sweep — Item A-ts: ts-client Admin Endpoint Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring `ts-client/src/admin.ts` from 10 admin methods up to parity with the server's HTTP admin surface by adding the 14 missing endpoints (auth/session, admin allowlist, schema/stats read-back, owner-bypass docs, tokens, metrics, hot config, op feed).

**Architecture:** Each new method is a thin wrapper over the existing private `request(method, path, payload)` helper (`admin.ts:105`), reusing the `Authorization: Bearer <adminKey>` header and the envelope-aware error path — exactly the pattern the existing 10 methods use. Two enablers: (1) widen `request`'s `method` union to include `"PATCH" | "DELETE"`; (2) add small response-type interfaces for the admin-specific shapes. No wire/protocol changes — the methods call routes the server already mounts and unwrap the server's existing response wrappers (`{admins}`, `{tokens}`, `{result}`, `{ops}`, etc.).

**Scope note:** the `/admin/stream` WebSocket is **deferred** to a follow-up — it's structurally distinct (WS layer + subprotocol auth + frame parsing), and the dashboard already consumes the live op-feed. This plan covers the 14 HTTP routes only.

**Tech Stack:** TypeScript, Vitest, biome. ts-client is a bun workspace.

## Global Constraints

- **No wire/protocol changes** — `protocol.ts` is untouched except reusing already-exported types (`QueryJson`, `TransactionJson`, `QueryResultJson`, `SchemaJson`). Define new admin-specific response interfaces in `admin.ts`.
- **Match server wire casing exactly** (from `server/src/admin.rs` serde structs): `adminKey` (login body), `idempotencyKey` (mutate body), `allowedOrigins`/`sessionTtlDays`/`maxFileSize` (config patch, `deny_unknown_fields`), and the rename-only fields `githubId`, `createdAt`, `rowCount`, `sizeBytes`, `totalSizeBytes`, `docId`, `queriesTotal`/`mutationsTotal`/etc. Bare structs use literal names (`ok`, `result`, `results`, `db`, `table`, `n`).
- Every body-bearing request sends `content-type: application/json` (the `request` helper already does).
- Auth: `Authorization: Bearer ${this.adminKey}` on every call (the helper already sets it). `login`/`logout` are the only routes without a `require_admin` gate server-side, but sending the header is harmless.
- ESM `.js` import specifiers; biome formatting.
- Tests follow `ts-client/tests/admin.test.ts`: inject a `vi.fn().mockResolvedValue(jsonResponse(...))` fetch via the constructor's `fetch` option; assert exact URL, `Authorization: Bearer k` header, parsed JSON body, and unwrapped return value. Run `bunx biome check` on touched files before commit.
- Verification: each task runs `cd ts-client && bunx vitest run tests/admin.test.ts`; the final task runs the ts-client gate. Full cross-package `make checkall` runs at branch finish.
- Commits: one atomic commit per task, conventional style.

## Reference: server response wrappers + casing (the contract)

| Method | Route | Request body / params | Response (server shape) | ts return |
|---|---|---|---|---|
| `login(adminKey)` | POST `/admin/login` | `{adminKey}` | 204 (no body) | `void` |
| `logout()` | POST `/admin/logout` | — | 204 (no body) | `void` |
| `adminsList()` | GET `/admin/admins` | — | `{admins: [{email, githubId?}]}` | `AdminMember[]` |
| `addAdmin(email, githubId?)` | POST `/admin/admins` | `{email, githubId?}` | `{ok}` | `void` |
| `removeAdmin(email)` | DELETE `/admin/admins` | `{email}` | `{ok}` | `void` |
| `getSchema(db)` | GET `/admin/dbs/{db}/schema` | path `db` | `SchemaJson` (bare `{tables}`) | `SchemaJson` |
| `dbStats(db)` | GET `/admin/dbs/{db}/stats` | path `db` | `{tables: [{name, rowCount, sizeBytes}], totalSizeBytes}` | `DbStats` |
| `listTokens(db)` | GET `/admin/tokens?db=` | query `db` | `{tokens: [{id, name, createdAt, revoked}]}` | `TokenInfo[]` |
| `metrics()` | GET `/admin/metrics` | — | `MetricsSnapshot` (bare) | `MetricsSnapshot` |
| `getConfig()` | GET `/admin/config` | — | `ConfigResponse` (bare) | `ConfigResponse` |
| `patchConfig(patch)` | PATCH `/admin/config` | `{allowedOrigins?, sessionTtlDays?, maxFileSize?}` | `ConfigResponse` | `ConfigResponse` |
| `opsRecent(opts?)` | GET `/admin/ops/recent?db=&table=&n=` | optional query `db`/`table`/`n` | `{ops: OpEvent[]}` | `OpEvent[]` |
| `adminQuery(db, query)` | POST `/admin/db/{db}/query` | `{query}` | `{result}` | `QueryResultJson` |
| `adminMutate(db, txn, opts?)` | POST `/admin/db/{db}/mutate` | `{txn, idempotencyKey?}` | `{results}` | `unknown[]` |

(`/admin/db/{db}/...` uses singular `db`; `/admin/dbs/{db}/...` uses plural `dbs`. Encode path params with `encodeURIComponent`.)

---

## File Structure

- `ts-client/src/admin.ts` — widen `request` method union; add the 14 methods + the response-type interfaces (`AdminMember`, `TableStat`, `DbStats`, `TokenInfo`, `MetricsSnapshot`, `HotConfig`, `ConfigResponse`, `HotConfigPatch`, `OpEvent`, `OpEventKind`).
- `ts-client/tests/admin.test.ts` — happy-path + unwrap + error-envelope tests for each new method, mirroring the existing pattern.
- `FEATURE_MATRIX.md` — note ts-client admin parity (the "Admin control plane" row's ts-client coverage).
- `ts-client/README.md` — only if it enumerates admin methods.

---

## Task 1: Widen `request` + auth/session + admin allowlist

**Files:**
- Modify: `ts-client/src/admin.ts` (`request` signature `:105`; add 5 methods + `AdminMember` interface)
- Test: `ts-client/tests/admin.test.ts`

**Interfaces:**
- Consumes: the existing `request` helper, `this.url`, `this.adminKey`.
- Produces: `login`, `logout`, `adminsList`, `addAdmin`, `removeAdmin`; `request` now accepts `"PATCH" | "DELETE"`.

- [ ] **Step 1: Write the failing tests**

Add to `ts-client/tests/admin.test.ts` (inside a new `describe("RtDbAdminClient — new endpoints", …)` block, reusing the file's existing `jsonResponse` helper and `new RtDbAdminClient({url, adminKey, fetch})` pattern):

```ts
describe("RtDbAdminClient — new endpoints", () => {
  it("login POSTs {adminKey} to /admin/login and resolves void on 204", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.login("secret")).resolves.toBeUndefined();
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/login");
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body)).toEqual({ adminKey: "secret" });
  });

  it("logout POSTs /admin/logout with no body and resolves void on 204", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.logout()).resolves.toBeUndefined();
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/logout");
    expect(init.method).toBe("POST");
  });

  it("adminsList GETs /admin/admins and unwraps {admins}", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({ admins: [{ email: "a@x.com", githubId: 1 }, { email: "b@x.com" }] }),
    );
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    const rows = await admin.adminsList();
    expect(rows).toEqual([{ email: "a@x.com", githubId: 1 }, { email: "b@x.com" }]);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/admins");
  });

  it("addAdmin POSTs {email, githubId?} to /admin/admins", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await admin.addAdmin("A@X.com", 7);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/admins");
    expect(JSON.parse(init.body)).toEqual({ email: "A@X.com", githubId: 7 });
  });

  it("removeAdmin DELETEs {email} to /admin/admins", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await admin.removeAdmin("a@x.com");
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/admins");
    expect(init.method).toBe("DELETE");
    expect(JSON.parse(init.body)).toEqual({ email: "a@x.com" });
  });

  it("surfaces a 401 error envelope from login as RtDbError", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({ code: "UNAUTHORIZED", message: "bad admin key" }, 401),
    );
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.login("wrong")).rejects.toMatchObject({
      name: "RtDbError",
      code: "UNAUTHORIZED",
      message: "bad admin key",
    });
  });
});
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd ts-client && bunx vitest run tests/admin.test.ts -t "new endpoints"`
Expected: FAIL — `admin.login` etc. are not methods.

- [ ] **Step 3: Widen `request` and add the 5 methods + `AdminMember`**

In `ts-client/src/admin.ts`:

(a) Widen the `request` method parameter type (`admin.ts:105`) from `"GET" | "POST"` to `"GET" | "POST" | "PATCH" | "DELETE"`.

(b) Add the interface near the other top-level types:

```ts
export interface AdminMember {
  email: string;
  githubId?: number;
}
```

(c) Add the methods inside `class RtDbAdminClient` (mirror the existing one-liners):

```ts
  /** Cookie-session login (POST /admin/login). Sets the server's HttpOnly `rtdb_session`
   *  cookie on 204. A browser auto-attaches the cookie thereafter; a Node caller must wire
   *  its own cookie jar onto the injected `fetch` to reuse the session. */
  async login(adminKey: string): Promise<void> {
    await this.request("POST", "/admin/login", { adminKey });
  }

  /** Clear the admin session cookie (POST /admin/logout, always 204). */
  async logout(): Promise<void> {
    await this.request("POST", "/admin/logout");
  }

  /** List server-wide dashboard admin emails (GET /admin/admins). */
  async adminsList(): Promise<AdminMember[]> {
    const body = await this.request("GET", "/admin/admins");
    return (body as { admins: AdminMember[] }).admins;
  }

  /** Add (or upsert) a dashboard admin (POST /admin/admins). */
  async addAdmin(email: string, githubId?: number): Promise<void> {
    await this.request("POST", "/admin/admins", githubId === undefined ? { email } : { email, githubId });
  }

  /** Remove a dashboard admin (DELETE /admin/admins, body-on-DELETE). */
  async removeAdmin(email: string): Promise<void> {
    await this.request("DELETE", "/admin/admins", { email });
  }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cd ts-client && bunx vitest run tests/admin.test.ts`
Expected: PASS — the 6 new cases + all pre-existing admin tests pass. Also run `bunx tsc --noEmit` (clean).

- [ ] **Step 5: Format + commit**

```
cd ts-client && bunx biome check --write src/admin.ts tests/admin.test.ts
git add ts-client/src/admin.ts ts-client/tests/admin.test.ts
git commit -m "feat(ts-client): admin auth/session + admin-allowlist endpoints"
```

---

## Task 2: Schema/stats read-back + tokens + metrics + hot config + op feed

**Files:**
- Modify: `ts-client/src/admin.ts` (add 7 methods + response interfaces)
- Test: `ts-client/tests/admin.test.ts`

**Interfaces:**
- Consumes: `request` (now PATCH-capable from Task 1), `SchemaJson` from `./protocol.js`.
- Produces: `getSchema`, `dbStats`, `listTokens`, `metrics`, `getConfig`, `patchConfig`, `opsRecent`; interfaces `TableStat`, `DbStats`, `TokenInfo`, `MetricsSnapshot`, `HotConfig`, `ConfigResponse`, `HotConfigPatch`, `OpEvent`, `OpEventKind`.

- [ ] **Step 1: Write the failing tests** (add to the `describe("RtDbAdminClient — new endpoints", …)` block):

```ts
  it("getSchema GETs /admin/dbs/{db}/schema and returns the bare SchemaJson", async () => {
    const schema = { tables: { items: { fields: { title: { type: "string" } } } } };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(schema));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.getSchema("kanban")).resolves.toEqual(schema);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/dbs/kanban/schema");
  });

  it("dbStats GETs /admin/dbs/{db}/stats and unwraps the stats object", async () => {
    const stats = { tables: [{ name: "items", rowCount: 3, sizeBytes: 4096 }], totalSizeBytes: 4096 };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(stats));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.dbStats("kanban")).resolves.toEqual(stats);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/dbs/kanban/stats");
  });

  it("listTokens GETs /admin/tokens?db= and unwraps {tokens}", async () => {
    const tokens = [{ id: "t1", name: "ci", createdAt: 1, revoked: false }];
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ tokens }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.listTokens("kanban")).resolves.toEqual(tokens);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/tokens?db=kanban");
  });

  it("metrics GETs /admin/metrics and returns the bare snapshot", async () => {
    const snap = { queriesTotal: 5, mutationsTotal: 2, uploadsTotal: 0, wsConnections: 1,
                   activeSubscriptions: 1, poolSize: 4, poolIdle: 3, uptimeSeconds: 99 };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(snap));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.metrics()).resolves.toEqual(snap);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/metrics");
  });

  it("getConfig GETs /admin/config and returns the redacted response", async () => {
    const cfg = { port: 8300, publicUrl: "http://h", githubBaseUrl: "", githubApiUrl: "",
                  databaseUrlConfigured: true, adminKeyConfigured: true, githubConfigured: false,
                  googleConfigured: false, hot: { allowedOrigins: [], sessionTtlDays: 30, maxFileSize: 5242880 },
                  version: "0.1.0", gitCommit: "abc", admins: [] };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(cfg));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.getConfig()).resolves.toEqual(cfg);
  });

  it("patchConfig PATCHes /admin/config and returns the new ConfigResponse", async () => {
    const cfg = { port: 8300, publicUrl: "http://h", githubBaseUrl: "", githubApiUrl: "",
                  databaseUrlConfigured: true, adminKeyConfigured: true, githubConfigured: false,
                  googleConfigured: false, hot: { allowedOrigins: ["https://app.x"], sessionTtlDays: 30, maxFileSize: 5242880 },
                  version: "0.1.0", gitCommit: "abc", admins: [] };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(cfg));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.patchConfig({ allowedOrigins: ["https://app.x"] })).resolves.toEqual(cfg);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/config");
    expect(init.method).toBe("PATCH");
    expect(JSON.parse(init.body)).toEqual({ allowedOrigins: ["https://app.x"] });
  });

  it("opsRecent GETs /admin/ops/recent with optional db/table/n and unwraps {ops}", async () => {
    const ops = [{ db: "kanban", table: "items", docId: "x", kind: "insert", ts: 1 }];
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ops }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.opsRecent({ db: "kanban", n: 50 })).resolves.toEqual(ops);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/ops/recent?db=kanban&n=50");
  });

  it("patchConfig surfaces a 400 validation envelope as RtDbError", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({ code: "BAD_REQUEST", message: "sessionTtlDays must be >= 1" }, 400),
    );
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.patchConfig({ sessionTtlDays: 0 })).rejects.toMatchObject({
      code: "BAD_REQUEST", message: "sessionTtlDays must be >= 1",
    });
  });
```

- [ ] **Step 2: Run to verify RED** — `cd ts-client && bunx vitest run tests/admin.test.ts -t "new endpoints"` (the new cases fail; pre-existing pass).

- [ ] **Step 3: Add the interfaces + 7 methods** in `ts-client/src/admin.ts`:

```ts
export interface TableStat { name: string; rowCount: number; sizeBytes: number; }
export interface DbStats { tables: TableStat[]; totalSizeBytes: number; }
export interface TokenInfo { id: string; name: string; createdAt: number; revoked: boolean; }
export interface MetricsSnapshot {
  queriesTotal: number; mutationsTotal: number; uploadsTotal: number; wsConnections: number;
  activeSubscriptions: number; poolSize: number; poolIdle: number; uptimeSeconds: number;
}
export interface HotConfig { allowedOrigins: string[]; sessionTtlDays: number; maxFileSize: number; }
export interface ConfigResponse {
  port: number; publicUrl: string; githubBaseUrl: string; githubApiUrl: string;
  databaseUrlConfigured: boolean; adminKeyConfigured: boolean; githubConfigured: boolean;
  googleConfigured: boolean; hot: HotConfig; version: string; gitCommit: string; admins: AdminMember[];
}
export interface HotConfigPatch {
  allowedOrigins?: string[]; sessionTtlDays?: number; maxFileSize?: number;
}
export type OpEventKind = "insert" | "patch" | "replace" | "delete" | "upsert";
export interface OpEvent {
  db: string; table: string; docId: string; kind: OpEventKind; ts: number; owner?: string | null;
}
```

```ts
  /** Read a database's pushed schema (GET /admin/dbs/{db}/schema). */
  async getSchema(db: string): Promise<SchemaJson> {
    return (await this.request("GET", `/admin/dbs/${encodeURIComponent(db)}/schema`)) as SchemaJson;
  }

  /** Per-table row counts + storage sizes (GET /admin/dbs/{db}/stats). */
  async dbStats(db: string): Promise<DbStats> {
    return (await this.request("GET", `/admin/dbs/${encodeURIComponent(db)}/stats`)) as DbStats;
  }

  /** List tokens for a database, no secrets (GET /admin/tokens?db=). */
  async listTokens(db: string): Promise<TokenInfo[]> {
    const body = await this.request("GET", `/admin/tokens?db=${encodeURIComponent(db)}`);
    return (body as { tokens: TokenInfo[] }).tokens;
  }

  /** Server metrics snapshot (GET /admin/metrics). */
  async metrics(): Promise<MetricsSnapshot> {
    return (await this.request("GET", "/admin/metrics")) as MetricsSnapshot;
  }

  /** Redacted server config (GET /admin/config). Secrets surface as configured-bools, not values. */
  async getConfig(): Promise<ConfigResponse> {
    return (await this.request("GET", "/admin/config")) as ConfigResponse;
  }

  /** Patch hot-reloadable config (PATCH /admin/config). Each present field fully replaces the
   *  prior value; the server validates (sessionTtlDays>=1, maxFileSize within bounds, origin shape). */
  async patchConfig(patch: HotConfigPatch): Promise<ConfigResponse> {
    return (await this.request("PATCH", "/admin/config", patch)) as ConfigResponse;
  }

  /** Recent op-feed events, newest-first (GET /admin/ops/recent). All filter opts optional. */
  async opsRecent(opts?: { db?: string; table?: string; n?: number }): Promise<OpEvent[]> {
    const params = new URLSearchParams();
    if (opts?.db) params.set("db", opts.db);
    if (opts?.table) params.set("table", opts.table);
    if (opts?.n !== undefined) params.set("n", String(opts.n));
    const qs = params.toString();
    const body = await this.request("GET", `/admin/ops/recent${qs ? `?${qs}` : ""}`);
    return (body as { ops: OpEvent[] }).ops;
  }
```

(Add `SchemaJson` to the `./protocol.js` named import in `admin.ts` if not already imported.)

- [ ] **Step 4: Run GREEN** — `cd ts-client && bunx vitest run tests/admin.test.ts` (all pass) + `bunx tsc --noEmit`.

- [ ] **Step 5: Format + commit**
```
cd ts-client && bunx biome check --write src/admin.ts tests/admin.test.ts
git add ts-client/src/admin.ts ts-client/tests/admin.test.ts
git commit -m "feat(ts-client): admin schema/stats/tokens/metrics/config/ops endpoints"
```

---

## Task 3: Owner-bypass document query/mutate

**Files:**
- Modify: `ts-client/src/admin.ts` (add `adminQuery`, `adminMutate`)
- Test: `ts-client/tests/admin.test.ts`

**Interfaces:**
- Consumes: `request`; `QueryJson`, `TransactionJson`, `QueryResultJson` from `./protocol.js`; `RtQuery` from `./query.js` (the existing `RtDbHttpClient.query` reads `query.json` — accept `RtQuery<R>` or `QueryJson`; mirror whichever `RtDbHttpClient` uses, confirmed in `ts-client/src/http.ts:43`).

- [ ] **Step 1: Write the failing tests**:

```ts
  it("adminQuery POSTs {query} to /admin/db/{db}/query and unwraps {result}", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ result: [{ _id: "a" }] }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    const q = { json: { table: "items" } };
    await expect(admin.adminQuery("kanban", q)).resolves.toEqual([{ _id: "a" }]);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/db/kanban/query");
    expect(JSON.parse(init.body)).toEqual({ query: { table: "items" } });
  });

  it("adminMutate POSTs {txn, idempotencyKey?} to /admin/db/{db}/mutate and unwraps {results}", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ results: ["new-id"] }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    const txn = { steps: [{ op: "insert", table: "items", doc: { title: "x" } }] };
    await expect(admin.adminMutate("kanban", txn, { idempotencyKey: "k1" })).resolves.toEqual(["new-id"]);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/db/kanban/mutate");
    expect(JSON.parse(init.body)).toEqual({ txn, idempotencyKey: "k1" });
  });

  it("adminMutate omits idempotencyKey when not provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ results: [] }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    const txn = { steps: [] };
    await admin.adminMutate("kanban", txn);
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({ txn });
  });
```

- [ ] **Step 2: RED** — `cd ts-client && bunx vitest run tests/admin.test.ts -t "adminQuery|adminMutate"` (fail).

- [ ] **Step 3: Add the methods** (mirror the parameter shape `RtDbHttpClient.query`/`mutate` use — confirm against `http.ts`; `query` takes an `RtQuery<R>` whose `.json` is the `QueryJson`, `mutate` takes a `TransactionJson`):

```ts
  /** Owner-bypass document read (POST /admin/db/{db}/query). Admin sees every row regardless
   *  of per-row ownerField. Body and result shapes match /api/query. */
  async adminQuery<R>(db: string, query: RtQuery<R>): Promise<R> {
    const body = await this.request("POST", `/admin/db/${encodeURIComponent(db)}/query`, {
      query: query.json,
    });
    return (body as { result: R }).result;
  }

  /** Owner-bypass document write (POST /admin/db/{db}/mutate). Body shapes match /api/mutate;
   *  `idempotencyKey` is the opt-in safe-retry key. Capped server-side by RTDB_MAX_AFFECTED_DOCS. */
  async adminMutate(
    db: string,
    txn: TransactionJson,
    opts?: { idempotencyKey?: string },
  ): Promise<unknown[]> {
    const body = await this.request("POST", `/admin/db/${encodeURIComponent(db)}/mutate`, {
      txn,
      idempotencyKey: opts?.idempotencyKey,
    });
    return (body as { results: unknown[] }).results;
  }
```

(Add `RtQuery` to the `./query.js` named import in `admin.ts` if not already imported. Confirm the exact `RtQuery`/`TransactionJson` import paths against the existing `http.ts` imports — match them.)

- [ ] **Step 4: GREEN** — `cd ts-client && bunx vitest run tests/admin.test.ts` + `bunx tsc --noEmit`.

- [ ] **Step 5: Format + commit**
```
cd ts-client && bunx biome check --write src/admin.ts tests/admin.test.ts
git add ts-client/src/admin.ts ts-client/tests/admin.test.ts
git commit -m "feat(ts-client): admin owner-bypass query/mutate endpoints"
```

---

## Task 4: Docs + ts-client gate

**Files:**
- Modify: `FEATURE_MATRIX.md` (admin control-plane row), `ts-client/README.md` (only if it enumerates admin methods)

- [ ] **Step 1 — FEATURE_MATRIX**: In the "Admin control plane" row (§1) or the relevant client-parity note, update the ts-client admin coverage to reflect it now covers the full server admin HTTP surface (was 10 methods → now 24). Note that `/admin/stream` (WS) remains the one deferred admin route.

- [ ] **Step 2 — README**: Only if `ts-client/README.md` enumerates admin methods, add the new ones; otherwise no change.

- [ ] **Step 3 — ts-client gate**:
```
cd ts-client
bunx vitest run          # full suite — all green
bunx tsc --noEmit        # exit 0
bunx biome check src/admin.ts tests/admin.test.ts
```

- [ ] **Step 4 — Commit**
```
git add FEATURE_MATRIX.md ts-client/README.md   # drop README if unchanged
git commit -m "docs(ts-client): admin endpoint parity (24 HTTP routes; /admin/stream deferred)"
```

---

## Self-Review (completed during authoring)

- **Spec coverage:** A-ts = the 14 HTTP admin methods. Task 1 = auth/session + allowlist (5) + `request` widening; Task 2 = read-back/tokens/metrics/config/ops (7); Task 3 = owner-bypass query/mutate (2); Task 4 = docs/gate. ✅
- **Scope:** `/admin/stream` (WS) explicitly deferred — flagged to the user; structurally distinct, dashboard covers the use case. ✅
- **Placeholders:** every method body + every test is real code. The two "confirm against existing imports/shape" points (`RtQuery`/`TransactionJson` import paths in Task 3; `SchemaJson` import in Task 2) are verify-points for the implementer, flagged not placeholdered. ✅
- **Type consistency:** `request` widened once in Task 1, consumed by PATCH (Task 2) and DELETE (Task 1). Response interfaces match the server casing table exactly (camelCase + rename-only fields). ✅
- **Wire casing:** verified against `admin.rs` serde structs (adminKey, idempotencyKey, allowedOrigins/sessionTtlDays/maxFileSize, githubId/createdAt/rowCount/sizeBytes/totalSizeBytes/docId, MetricsSnapshot fields). ✅
