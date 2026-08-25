import { afterEach, describe, expect, it, vi } from "vitest";
import type {
  AuditEntry,
  ExplainResult,
  HotConfig,
  HotConfigPatch,
  MergeReport,
  SessionInfo,
  SlowQueriesResponse,
  SlowQueryEntry,
  SubscriptionInfo,
  SubscriptionsResponse,
  Webhook,
  WebhookDelivery,
} from "../src/admin.js";
import { RtDbAdminClient } from "../src/admin.js";
import type { WebSocketLike } from "../src/client.js";
import { RtDbError } from "../src/errors.js";
import { Migration } from "../src/migration.js";
import {
  PROTOCOL_VERSION,
  type SchemaHistoryEntry,
  type SchemaHistoryEntrySummary,
  type TransactionJson,
  type WorkflowSpec,
} from "../src/protocol.js";
import { defineSchema, defineTable, t } from "../src/schema.js";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

describe("RtDbAdminClient", () => {
  it("pushes a schema by serializing the SchemaDefinition to JSON", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    const schema = defineSchema({ notes: defineTable({ body: t.string() }) });

    await admin.pushSchema("kanban", schema);

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/push-schema");
    expect(init.headers.Authorization).toBe("Bearer k");
    expect(JSON.parse(init.body)).toEqual({
      db: "kanban",
      schema: { tables: { notes: { fields: { body: { type: "string" } } } } },
    });
  });

  it("mints a token and returns id + token", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ tokenId: "id1", token: "secret" }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });

    await expect(admin.mintToken("kanban", "cli")).resolves.toEqual({
      tokenId: "id1",
      token: "secret",
    });
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/mint-token");
    // Backward compat: omitting opts POSTs exactly {db, name} — no undefined keys leak.
    expect(JSON.parse(init.body)).toEqual({ db: "kanban", name: "cli" });
  });

  it("mintToken spreads capability opts into the body and omits absent fields", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ tokenId: "id2", token: "s2" }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });

    await admin.mintToken("dbx", "scraper", {
      readOnly: true,
      tables: ["users"],
      expiresAt: 1700000000000,
    });
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/mint-token");
    expect(init.method).toBe("POST");
    // Only the provided fields are sent — server defaults apply to the rest.
    expect(JSON.parse(init.body)).toEqual({
      db: "dbx",
      name: "scraper",
      readOnly: true,
      tables: ["users"],
      expiresAt: 1700000000000,
    });
  });

  it("lists databases", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ databases: ["kanban", "demo"] }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });

    await expect(admin.listDbs()).resolves.toEqual(["kanban", "demo"]);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/dbs");
  });

  it("lists an allowlist via query string", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ emails: ["a@b.com"] }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });

    await expect(admin.allowlistList("kanban")).resolves.toEqual(["a@b.com"]);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/allowlist?db=kanban");
  });

  it("exports a database as JSONL text", async () => {
    const jsonl = '{"kind":"schema","schema":{"tables":{}}}\n';
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        new Response(jsonl, { status: 200, headers: { "content-type": "application/x-ndjson" } }),
      );
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });

    await expect(admin.exportDb("kanban")).resolves.toBe(jsonl);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/export-db?db=kanban");
    expect(init.method).toBe("GET");
    expect(init.headers.Authorization).toBe("Bearer k");
  });

  it("imports a JSONL snapshot into a database", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    const jsonl = '{"kind":"schema","schema":{"tables":{}}}\n';

    await admin.importDb("kanban", jsonl);

    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/import-db?db=kanban");
    expect(init.method).toBe("POST");
    expect(init.headers.Authorization).toBe("Bearer k");
    expect(init.headers["content-type"]).toBe("application/x-ndjson");
    expect(init.body).toBe(jsonl);
  });

  it("throws RtDbError when exportDb receives an error envelope", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ code: "NOT_FOUND", message: "unknown database" }, 404));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });

    await expect(admin.exportDb("missing")).rejects.toThrow("unknown database");
  });
});

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

  it("throws RtDbError INTERNAL when a non-204 2xx body is not valid JSON", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response("not-json", { status: 200 }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });

    await expect(admin.listBackups()).rejects.toBeInstanceOf(RtDbError);
    await expect(admin.listBackups()).rejects.toMatchObject({
      name: "RtDbError",
      code: "INTERNAL",
      message: "admin request to /admin/backups returned 2xx with no JSON object body",
    });
  });

  it("adminsList GETs /admin/admins and unwraps {admins}", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
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
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ code: "UNAUTHORIZED", message: "bad admin key" }, 401));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.login("wrong")).rejects.toMatchObject({
      name: "RtDbError",
      code: "UNAUTHORIZED",
      message: "bad admin key",
    });
  });

  it("getSchema GETs /admin/dbs/{db}/schema and returns the bare SchemaJson", async () => {
    const schema = { tables: { items: { fields: { title: { type: "string" } } } } };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(schema));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.getSchema("kanban")).resolves.toEqual(schema);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/dbs/kanban/schema");
  });

  it("dbStats GETs /admin/dbs/{db}/stats and unwraps the stats object", async () => {
    const stats = {
      tables: [{ name: "items", rowCount: 3, sizeBytes: 4096 }],
      totalSizeBytes: 4096,
      tablesQuota: 10,
      tablesUsed: 1,
      storageQuotaBytes: 1048576,
      storageUsedBytes: 4096,
      subsQuota: 50,
      subsUsed: 3,
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(stats));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.dbStats("kanban")).resolves.toEqual(stats);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/dbs/kanban/stats");
  });

  it("listTokens GETs /admin/tokens?db= and unwraps {tokens} with capability fields", async () => {
    const tokens = [
      // Full-access token: expiresAt null, readOnly false, tables null.
      {
        id: "t1",
        name: "ci",
        createdAt: 1,
        revoked: false,
        expiresAt: null,
        readOnly: false,
        tables: null,
      },
      // Restricted token: carries every capability field populated.
      {
        id: "t2",
        name: "scraper",
        createdAt: 2,
        revoked: false,
        expiresAt: 1700000000000,
        readOnly: true,
        tables: ["users"],
      },
    ];
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ tokens }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.listTokens("kanban")).resolves.toEqual(tokens);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/tokens?db=kanban");
  });

  it("metrics GETs /admin/metrics and returns the bare snapshot", async () => {
    const snap = {
      queriesTotal: 5,
      mutationsTotal: 2,
      uploadsTotal: 0,
      wsConnections: 1,
      activeSubscriptions: 1,
      poolSize: 4,
      poolIdle: 3,
      uptimeSeconds: 99,
      queryLatency: { p50: 110, p95: 220, p99: 330 },
      mutateLatency: { p50: 440, p95: 550, p99: 660 },
      subscribeLatency: { p50: 770, p95: 880, p99: 990 },
      subsRerunsTotal: 12,
      subsSkipsPointTotal: 3,
      subsSkipsIndexedTotal: 4,
      subsSkipsOrderedTotal: 5,
      subsSkipVerificationsTotal: 0,
      subsMissedPushesTotal: 0,
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(snap));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.metrics()).resolves.toEqual(snap);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/metrics");
  });

  it("getConfig GETs /admin/config and returns the redacted response", async () => {
    const cfg = {
      port: 8300,
      publicUrl: "http://h",
      githubBaseUrl: "",
      githubApiUrl: "",
      databaseUrlConfigured: true,
      adminKeyConfigured: true,
      githubConfigured: false,
      googleConfigured: false,
      gitlabConfigured: false,
      oidcConfigured: false,
      hot: {
        allowedOrigins: [],
        sessionTtlDays: 30,
        maxFileSize: 5242880,
        idempotencyTtlMs: 300000,
      },
      version: "0.1.0",
      gitCommit: "abc",
      admins: [],
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(cfg));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.getConfig()).resolves.toEqual(cfg);
  });

  it("patchConfig PATCHes /admin/config and returns the new ConfigResponse", async () => {
    const cfg = {
      port: 8300,
      publicUrl: "http://h",
      githubBaseUrl: "",
      githubApiUrl: "",
      databaseUrlConfigured: true,
      adminKeyConfigured: true,
      githubConfigured: false,
      googleConfigured: false,
      gitlabConfigured: false,
      oidcConfigured: false,
      hot: {
        allowedOrigins: ["https://app.x"],
        sessionTtlDays: 30,
        maxFileSize: 5242880,
        idempotencyTtlMs: 300000,
      },
      version: "0.1.0",
      gitCommit: "abc",
      admins: [],
    };
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
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse({ code: "BAD_REQUEST", message: "sessionTtlDays must be >= 1" }, 400),
      );
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.patchConfig({ sessionTtlDays: 0 })).rejects.toMatchObject({
      code: "BAD_REQUEST",
      message: "sessionTtlDays must be >= 1",
    });
  });

  it("adminQuery POSTs {query} to /admin/db/{db}/query and unwraps {result}", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ result: [{ _id: "a" }] }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    const q = { json: { table: "items" } };
    await expect(admin.adminQuery("kanban", q)).resolves.toEqual([{ _id: "a" }]);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/db/kanban/query");
    expect(JSON.parse(init.body)).toEqual({ query: { table: "items" } });
  });

  it("adminQuery includeDeleted:true adds the internal param; omitted keeps the body lean", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ result: [{ _id: "a", deleted_at: 1 }] }))
      .mockResolvedValueOnce(jsonResponse({ result: [{ _id: "b" }] }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    const q = { json: { table: "items" } };
    await expect(admin.adminQuery("kanban", q, { includeDeleted: true })).resolves.toEqual([
      { _id: "a", deleted_at: 1 },
    ]);
    await expect(admin.adminQuery("kanban", q)).resolves.toEqual([{ _id: "b" }]);
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({
      query: { table: "items" },
      includeDeleted: true,
    });
    // Default call: the param is absent entirely (wire shape unchanged).
    expect(JSON.parse(fetchMock.mock.calls[1][1].body)).toEqual({ query: { table: "items" } });
  });

  it("adminMutate POSTs {txn, idempotencyKey?} to /admin/db/{db}/mutate and unwraps {results}", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ results: ["new-id"] }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    const txn: TransactionJson = {
      steps: [{ op: "insert", table: "items", doc: { title: "x" } }],
    };
    await expect(admin.adminMutate("kanban", txn, { idempotencyKey: "k1" })).resolves.toEqual([
      "new-id",
    ]);
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

  it("deleteDb POSTs {name, confirm} to /admin/delete-db", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await admin.deleteDb("kanban", "kanban");
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/delete-db");
    expect(init.method).toBe("POST");
    expect(init.headers.Authorization).toBe("Bearer k");
    expect(JSON.parse(init.body)).toEqual({ name: "kanban", confirm: "kanban" });
  });

  it("deleteDb surfaces a 400 confirmation-mismatch envelope as RtDbError", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse(
          { code: "BAD_REQUEST", message: "confirmation does not match database name" },
          400,
        ),
      );
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.deleteDb("kanban", "wrong")).rejects.toMatchObject({
      name: "RtDbError",
      code: "BAD_REQUEST",
      message: "confirmation does not match database name",
    });
  });

  it("deleteDb surfaces a 404 unknown-database envelope as RtDbError", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ code: "NOT_FOUND", message: "database not found" }, 404));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.deleteDb("missing", "missing")).rejects.toMatchObject({
      name: "RtDbError",
      code: "NOT_FOUND",
      message: "database not found",
    });
  });

  it("migrate POSTs {directives, dryRun} to /admin/db/{db}/migrate and decodes MigrateResultJson", async () => {
    const result = {
      applied: true,
      schema: { tables: {} },
      directives: [{ op: "renameField", affectedRows: 2 }],
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(result));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    const req = new Migration().renameField("users", "name", "fullName").dryRun().build();
    await expect(admin.migrate("kanban", req)).resolves.toEqual(result);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/db/kanban/migrate");
    expect(init.method).toBe("POST");
    expect(init.headers.Authorization).toBe("Bearer k");
    expect(JSON.parse(init.body)).toEqual({
      directives: [{ op: "renameField", table: "users", from: "name", to: "fullName" }],
      dryRun: true,
    });
  });

  it("migrate surfaces a 400 validation envelope as RtDbError", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse(
          { code: "BAD_REQUEST", message: "renamed field 'users.nope' does not exist" },
          400,
        ),
      );
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(
      admin.migrate("kanban", new Migration().renameField("users", "nope", "x").build()),
    ).rejects.toMatchObject({
      name: "RtDbError",
      code: "BAD_REQUEST",
      message: "renamed field 'users.nope' does not exist",
    });
  });

  it("getSchemaHistory GETs /admin/db/{db}/schema/history and unwraps {entries}", async () => {
    // Two fixtures: a push by an interactive principal, and a system-initiated
    // restore where `principal` comes back as JSON null.
    const entries: SchemaHistoryEntrySummary[] = [
      { version: 3, capturedAt: 1_700_000_000_003, source: "restore", principal: null },
      { version: 2, capturedAt: 1_700_000_000_002, source: "migrate", principal: "u@x.com" },
      { version: 1, capturedAt: 1_700_000_000_001, source: "push", principal: "u@x.com" },
    ];
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ entries }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.getSchemaHistory("kanban")).resolves.toEqual(entries);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/db/kanban/schema/history");
    expect(init.method).toBe("GET");
    expect(init.headers.Authorization).toBe("Bearer k");
  });

  it("getSchemaHistory forwards limit/offset as a query string and omits it when absent", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ entries: [] }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await admin.getSchemaHistory("kanban", { limit: 10, offset: 20 });
    expect(fetchMock.mock.calls[0][0]).toBe(
      "http://h:8300/admin/db/kanban/schema/history?limit=10&offset=20",
    );
  });

  it("getSchemaVersion GETs /admin/db/{db}/schema/history/{version} and returns the entry with schema", async () => {
    const entry: SchemaHistoryEntry = {
      version: 2,
      capturedAt: 1_700_000_000_002,
      source: "migrate",
      principal: "u@x.com",
      schema: { tables: { items: { fields: { title: { type: "string" } } } } },
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(entry));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.getSchemaVersion("kanban", 2)).resolves.toEqual(entry);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/db/kanban/schema/history/2");
    expect(init.method).toBe("GET");
    expect(init.headers.Authorization).toBe("Bearer k");
  });

  it("restoreSchema POSTs {version, confirm} to /admin/db/{db}/schema/restore and unwraps {ok, restoredTo}", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true, restoredTo: 2 }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.restoreSchema("kanban", 2, "kanban")).resolves.toEqual({
      ok: true,
      restoredTo: 2,
    });
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/db/kanban/schema/restore");
    expect(init.method).toBe("POST");
    expect(init.headers.Authorization).toBe("Bearer k");
    expect(JSON.parse(init.body)).toEqual({ version: 2, confirm: "kanban" });
  });

  it("restoreSchema surfaces a 400 confirmation-mismatch envelope as RtDbError", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse({ code: "BAD_REQUEST", message: "confirm must equal the database name" }, 400),
      );
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.restoreSchema("kanban", 2, "wrong")).rejects.toMatchObject({
      name: "RtDbError",
      code: "BAD_REQUEST",
      message: "confirm must equal the database name",
    });
  });

  it("restoreSchema surfaces a 404 unknown-snapshot envelope as RtDbError", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse({ code: "NOT_FOUND", message: "schema snapshot not found" }, 404),
      );
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.restoreSchema("kanban", 99, "kanban")).rejects.toMatchObject({
      name: "RtDbError",
      code: "NOT_FOUND",
      message: "schema snapshot not found",
    });
  });

  describe("streamAdmin", () => {
    /** Minimal `WebSocketLike` stand-in: records the URL/protocols it was opened with
     *  and lets a test push frames or simulate a server-side close. */
    class FakeSocket {
      onopen: (() => void) | null = null;
      onmessage: ((ev: { data: unknown }) => void) | null = null;
      onclose: ((ev: { code: number; reason: string }) => void) | null = null;
      onerror: (() => void) | null = null;
      closed = false;
      emit(data: unknown) {
        this.onmessage?.({ data: typeof data === "string" ? data : JSON.stringify(data) });
      }
      serverClose(code = 1006, reason = "") {
        this.closed = true;
        this.onclose?.({ code, reason });
      }
      send(): void {}
      close(): void {
        this.closed = true;
      }
    }

    const gauges = {
      queriesTotal: 5,
      mutationsTotal: 3,
      uploadsTotal: 0,
      wsConnections: 1,
      activeSubscriptions: 2,
      poolSize: 10,
      poolIdle: 8,
      uptimeSeconds: 99,
      queryLatency: { p50: 1, p95: 2, p99: 3 },
      mutateLatency: { p50: 1, p95: 2, p99: 3 },
      subscribeLatency: { p50: 1, p95: 2, p99: 3 },
      subsRerunsTotal: 0,
      subsSkipsPointTotal: 0,
      subsSkipsIndexedTotal: 0,
      subsSkipsOrderedTotal: 0,
      subsSkipVerificationsTotal: 0,
      subsMissedPushesTotal: 0,
    };

    it("opens a ws:// URL and carries the admin key in the rtdb-admin subprotocol", async () => {
      let openedUrl = "";
      let protocols: string | string[] | undefined;
      const socket = new FakeSocket();
      const admin = new RtDbAdminClient({
        url: "http://h:8300",
        adminKey: "secret",
        webSocketFactory: (url, protos) => {
          openedUrl = url;
          protocols = protos;
          return socket;
        },
      });

      const iter = admin.streamAdmin({ db: "kanban", table: "notes" });
      const pending = iter.next();
      socket.emit({
        kind: "op",
        event: { db: "kanban", table: "notes", docId: "d1", kind: "insert", ts: 7 },
      });
      const { value } = await pending;
      await iter.return(undefined);

      expect(openedUrl).toBe("ws://h:8300/admin/stream?db=kanban&table=notes");
      expect(protocols).toBe("rtdb-admin.secret");
      expect(value).toEqual({
        kind: "op",
        event: { db: "kanban", table: "notes", docId: "d1", kind: "insert", ts: 7 },
      });
    });

    it("yields op and gauges frames in arrival order", async () => {
      const socket = new FakeSocket();
      const admin = new RtDbAdminClient({
        url: "https://h:8300",
        adminKey: "k",
        webSocketFactory: () => socket,
      });

      const iter = admin.streamAdmin();
      const a = iter.next();
      socket.emit({ kind: "op", event: { db: "d", table: "t", docId: "1", kind: "patch", ts: 1 } });
      const first = await a;
      const b = iter.next();
      socket.emit({ kind: "gauges", gauges });
      const second = await b;
      await iter.return(undefined);

      expect(first.value).toEqual({
        kind: "op",
        event: { db: "d", table: "t", docId: "1", kind: "patch", ts: 1 },
      });
      expect(second.value).toEqual({ kind: "gauges", gauges });
    });

    it("ends the iterator when the server closes the socket", async () => {
      const socket = new FakeSocket();
      const admin = new RtDbAdminClient({
        url: "http://h:8300",
        adminKey: "k",
        webSocketFactory: () => socket,
      });

      const iter = admin.streamAdmin();
      const pending = iter.next();
      socket.serverClose(1006, "");
      await expect(pending).resolves.toEqual({ done: true, value: undefined });
      expect(socket.closed).toBe(true);
    });

    it("ignores malformed frames and closes the socket on AbortSignal", async () => {
      const socket = new FakeSocket();
      const ctrl = new AbortController();
      const admin = new RtDbAdminClient({
        url: "http://h:8300",
        adminKey: "k",
        webSocketFactory: () => socket,
      });

      const collected: unknown[] = [];
      const consume = (async () => {
        for await (const frame of admin.streamAdmin({ signal: ctrl.signal })) {
          collected.push(frame);
        }
      })();

      socket.emit("not-json"); // malformed — ignored
      socket.emit({
        kind: "op",
        event: { db: "d", table: "t", docId: "9", kind: "delete", ts: 2 },
      });
      // let the microtask queue drain so the frame is consumed
      await Promise.resolve();
      await Promise.resolve();
      ctrl.abort();
      await consume;

      expect(collected).toHaveLength(1);
      expect(socket.closed).toBe(true);
    });
  });
});

describe("RtDbAdminClient backups", () => {
  it("backupNow POSTs {} to /admin/backup and resolves void on 202", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 202 }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.backupNow()).resolves.toBeUndefined();
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/backup");
    expect(init.method).toBe("POST");
    expect(init.headers.Authorization).toBe("Bearer k");
    expect(JSON.parse(init.body)).toEqual({});
  });

  it("listBackups GETs /admin/backups and returns {running, backups}", async () => {
    const payload = {
      running: false,
      backups: [{ name: "rtdb-20260728T143045Z.dump", sizeBytes: 1024, createdMs: 1234567890 }],
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(payload));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.listBackups()).resolves.toEqual(payload);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/backups");
  });

  it("downloadBackup GETs /admin/backups/{name} and returns the raw binary Response", async () => {
    const blob = new Response(new Uint8Array([1, 2, 3]), {
      status: 200,
      headers: { "content-type": "application/octet-stream" },
    });
    const fetchMock = vi.fn().mockResolvedValue(blob);
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    const res = await admin.downloadBackup("rtdb-20260728T143045Z.dump");
    expect(res).toBe(blob);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/backups/rtdb-20260728T143045Z.dump");
    expect(init.method).toBe("GET");
    expect(init.headers.Authorization).toBe("Bearer k");
  });

  it("downloadBackup throws RtDbError on a non-OK status", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ code: "NOT_FOUND", message: "backup not found" }, 404));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.downloadBackup("missing.dump")).rejects.toMatchObject({
      name: "RtDbError",
      code: "NOT_FOUND",
      message: "backup not found",
    });
  });

  it("deleteBackup DELETEs /admin/backups/{name} and resolves void on 204", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.deleteBackup("rtdb-20260728T143045Z.dump")).resolves.toBeUndefined();
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/backups/rtdb-20260728T143045Z.dump");
    expect(init.method).toBe("DELETE");
    expect(init.headers.Authorization).toBe("Bearer k");
  });

  it("restoreBackup POSTs {name, confirm === name} to /admin/restore and unwraps the result", async () => {
    const result = { target: "rtdb_restored_20260804", instructions: "review then promote" };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(result));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.restoreBackup("rtdb-20260728T143045Z.dump")).resolves.toEqual(result);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/restore");
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body)).toEqual({
      name: "rtdb-20260728T143045Z.dump",
      confirm: "rtdb-20260728T143045Z.dump",
    });
  });

  it("restoreBackup surfaces a 400 confirmation-mismatch envelope as RtDbError", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse({ code: "BAD_REQUEST", message: "confirm must match backup name" }, 400),
      );
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.restoreBackup("x")).rejects.toMatchObject({
      name: "RtDbError",
      code: "BAD_REQUEST",
      message: "confirm must match backup name",
    });
  });
});

describe("RtDbAdminClient webhooks", () => {
  it("listWebhooks GETs /admin/db/{db}/webhooks and unwraps {webhooks}", async () => {
    // Two fixture rows: one all-tables (table:null), one scoped to a table.
    const webhooks: Webhook[] = [
      {
        id: 1,
        db: "kanban",
        table: null,
        url: "https://a.example/hook",
        events: ["*"],
        createdAt: 1_700_000_000_000,
        enabled: true,
        secret: null,
      },
      {
        id: 2,
        db: "kanban",
        table: "items",
        url: "https://b.example/hook",
        events: ["insert", "patch"],
        createdAt: 1_700_000_000_001,
        enabled: false,
        secret: "deadbeef".repeat(8),
      },
    ];
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ webhooks }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.listWebhooks("kanban")).resolves.toEqual(webhooks);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/db/kanban/webhooks");
    expect(init.method).toBe("GET");
    expect(init.headers.Authorization).toBe("Bearer k");
  });

  it("createWebhook POSTs only provided keys and unwraps {id}", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ id: 7 }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(
      admin.createWebhook("kanban", {
        url: "https://x.example/hook",
        table: "items",
        events: ["insert"],
        enabled: false,
      }),
    ).resolves.toEqual({ id: 7 });
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/db/kanban/webhooks");
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body)).toEqual({
      url: "https://x.example/hook",
      table: "items",
      events: ["insert"],
      enabled: false,
    });
  });

  it("createWebhook sends {table:null} as JSON null when called with table:null", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ id: 8 }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await admin.createWebhook("kanban", { url: "https://x.example/hook", table: null });
    const body = JSON.parse(fetchMock.mock.calls[0][1].body);
    expect(body).toEqual({ url: "https://x.example/hook", table: null });
  });

  it("createWebhook omits absent option keys (no undefined leaks)", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ id: 9 }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await admin.createWebhook("kanban", { url: "https://x.example/hook" });
    const body = JSON.parse(fetchMock.mock.calls[0][1].body);
    // Only `url` was provided; server defaults apply to the rest.
    expect(body).toEqual({ url: "https://x.example/hook" });
    expect("table" in body).toBe(false);
    expect("events" in body).toBe(false);
    expect("enabled" in body).toBe(false);
  });

  it("editWebhook PUTs to /admin/db/{db}/webhooks/{id} and returns the updated Webhook", async () => {
    const updated: Webhook = {
      id: 3,
      db: "kanban",
      table: "items",
      url: "https://new.example/hook",
      events: ["insert"],
      createdAt: 1_700_000_000_000,
      enabled: false,
      secret: null,
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(updated));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(
      admin.editWebhook("kanban", 3, {
        url: "https://new.example/hook",
        events: ["insert"],
        enabled: false,
      }),
    ).resolves.toEqual(updated);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/db/kanban/webhooks/3");
    expect(init.method).toBe("PUT");
    expect(JSON.parse(init.body)).toEqual({
      url: "https://new.example/hook",
      events: ["insert"],
      enabled: false,
    });
  });

  it("editWebhook sends table:null as JSON null (clear to all-tables)", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        id: 3,
        db: "kanban",
        table: null,
        url: "u",
        events: ["*"],
        createdAt: 1,
        enabled: true,
      }),
    );
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await admin.editWebhook("kanban", 3, { table: null });
    const body = JSON.parse(fetchMock.mock.calls[0][1].body);
    // Key present with JSON null — server reads Some(None) and clears the table filter.
    expect(body).toEqual({ table: null });
    expect("table" in body).toBe(true);
  });

  it("editWebhook omits table entirely when the caller did not provide it (unchanged)", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        id: 3,
        db: "kanban",
        table: "items",
        url: "u",
        events: ["*"],
        createdAt: 1,
        enabled: true,
      }),
    );
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await admin.editWebhook("kanban", 3, { enabled: false });
    const body = JSON.parse(fetchMock.mock.calls[0][1].body);
    // No `table` key on the wire — server leaves the existing filter alone.
    expect(body).toEqual({ enabled: false });
    expect("table" in body).toBe(false);
  });

  it("deleteWebhook DELETEs /admin/db/{db}/webhooks/{id} and resolves void on {ok:true}", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.deleteWebhook("kanban", 5)).resolves.toBeUndefined();
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/db/kanban/webhooks/5");
    expect(init.method).toBe("DELETE");
    expect(init.headers.Authorization).toBe("Bearer k");
  });

  it("listDeliveries GETs the deliveries path with status/limit/offset query and unwraps {deliveries}", async () => {
    const deliveries: WebhookDelivery[] = [
      {
        id: 11,
        attempts: 0,
        status: "pending",
        nextAttempt: 1_700_000_000_000,
        lastError: null,
        payload: { db: "kanban", table: "items", docId: "x", kind: "insert", ts: 1 },
      },
      {
        id: 12,
        attempts: 2,
        status: "retrying",
        nextAttempt: 1_700_000_000_004,
        lastError: "HTTP 503",
        payload: { db: "kanban", table: "items", docId: "y", kind: "patch", ts: 2 },
      },
    ];
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ deliveries }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(
      admin.listDeliveries("kanban", 3, { status: "retrying", limit: 50, offset: 100 }),
    ).resolves.toEqual(deliveries);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe(
      "http://h:8300/admin/db/kanban/webhooks/3/deliveries?status=retrying&limit=50&offset=100",
    );
    expect(init.method).toBe("GET");
    expect(init.headers.Authorization).toBe("Bearer k");
  });

  it("listDeliveries omits the query string when no opts are provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ deliveries: [] }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.listDeliveries("kanban", 3)).resolves.toEqual([]);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/db/kanban/webhooks/3/deliveries");
  });
});

describe("RtDbAdminClient audit", () => {
  it("getAudit GETs /admin/audit with provided filters and unwraps {entries}", async () => {
    // Two fixture rows: one labeled with op/principal, one system-initiated
    // (TTL reaper) where op/principal come back as JSON null.
    const entries: AuditEntry[] = [
      {
        id: 1,
        tsMs: 1_700_000_000_000,
        db: "kanban",
        table: "items",
        op: "insert",
        docId: "a",
        principal: "user@example.com",
        source: "mutate",
      },
      {
        id: 2,
        tsMs: 1_700_000_000_001,
        db: "kanban",
        table: "items",
        op: null,
        docId: "b",
        principal: null,
        source: "ttl",
      },
    ];
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ entries }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(
      admin.getAudit({ db: "kanban", table: "items", op: "insert", limit: 50, offset: 100 }),
    ).resolves.toEqual(entries);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe(
      "http://h:8300/admin/audit?db=kanban&table=items&op=insert&limit=50&offset=100",
    );
    expect(init.method).toBe("GET");
    expect(init.headers.Authorization).toBe("Bearer k");
  });

  it("getAudit omits the query string when no opts are provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ entries: [] }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.getAudit()).resolves.toEqual([]);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/audit");
  });

  it("getAudit omits undefined filters (no param leaks) and URL-encodes values", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ entries: [] }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    // Only principal/source provided; db/table/op/limit/offset absent.
    await admin.getAudit({ principal: "user@example.com", source: "ttl" });
    expect(fetchMock.mock.calls[0][0]).toBe(
      "http://h:8300/admin/audit?principal=user%40example.com&source=ttl",
    );
  });
});

describe("RtDbAdminClient subscriptions", () => {
  it("listSubscriptions GETs /admin/subscriptions with optional db and unwraps the body", async () => {
    // One interactive-principal sub and one system sub (machine token) where
    // principal comes back as JSON null.
    const subscriptions: SubscriptionInfo[] = [
      {
        db: "kanban",
        table: "items",
        terminal: "collect",
        readSetClass: "indexed",
        principal: { userId: "u1", email: "user@example.com" },
      },
      {
        db: "kanban",
        table: "items",
        terminal: "get",
        readSetClass: "point",
        principal: null,
      },
    ];
    const payload: SubscriptionsResponse = {
      subscriptions,
      subsRerunsTotal: 12,
      subsSkipsPointTotal: 3,
      subsSkipsIndexedTotal: 4,
      subsSkipsOrderedTotal: 5,
      subsMissedPushesTotal: 0,
      perDb: [
        {
          db: "kanban",
          reruns: 12,
          skipsPoint: 3,
          skipsIndexed: 4,
          skipsOrdered: 5,
          missed: 0,
          skips: 12,
          rerunRatio: 0.5,
        },
      ],
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(payload));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.listSubscriptions({ db: "kanban" })).resolves.toEqual(payload);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/subscriptions?db=kanban");
    expect(init.method).toBe("GET");
    expect(init.headers.Authorization).toBe("Bearer k");
  });

  it("listSubscriptions omits the query string when no opts are provided", async () => {
    const payload: SubscriptionsResponse = {
      subscriptions: [],
      subsRerunsTotal: 0,
      subsSkipsPointTotal: 0,
      subsSkipsIndexedTotal: 0,
      subsSkipsOrderedTotal: 0,
      subsMissedPushesTotal: 0,
      perDb: [],
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(payload));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.listSubscriptions()).resolves.toEqual(payload);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/subscriptions");
  });
});

describe("RtDbAdminClient sessions", () => {
  it("listSessions GETs /admin/sessions with user+limit and unwraps {sessions}", async () => {
    // Two fixture rows: one interactive (email/login populated), one anonymous
    // where email/login come back as JSON null.
    const sessions: SessionInfo[] = [
      {
        tokenHash: "a".repeat(64),
        userId: "u1",
        email: "user@example.com",
        login: "user",
        anonymous: false,
        createdAt: 1_700_000_000_000,
        expiresAt: 1_700_000_000_000 + 86_400_000,
      },
      {
        tokenHash: "b".repeat(64),
        userId: "u2",
        email: null,
        login: null,
        anonymous: true,
        createdAt: 1_700_000_000_001,
        expiresAt: 1_700_000_000_001 + 3_600_000,
      },
    ];
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ sessions }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.listSessions({ user: "u1", limit: 50 })).resolves.toEqual(sessions);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/sessions?user=u1&limit=50");
    expect(init.method).toBe("GET");
    expect(init.headers.Authorization).toBe("Bearer k");
  });

  it("listSessions omits the query string when no filter is provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ sessions: [] }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.listSessions()).resolves.toEqual([]);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/sessions");
  });

  it("revokeSession DELETEs /admin/sessions/{tokenHash} and resolves void on {ok:true}", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.revokeSession("0".repeat(64))).resolves.toBeUndefined();
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe(`http://h:8300/admin/sessions/${"0".repeat(64)}`);
    expect(init.method).toBe("DELETE");
    expect(init.headers.Authorization).toBe("Bearer k");
  });

  it("revokeUserSessions DELETEs /admin/sessions?user= and returns {ok, revoked}", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true, revoked: 3 }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.revokeUserSessions("u1")).resolves.toEqual({ ok: true, revoked: 3 });
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/sessions?user=u1");
    expect(init.method).toBe("DELETE");
    expect(init.headers.Authorization).toBe("Bearer k");
  });

  it("revokeExpiredSessions DELETEs /admin/sessions?expired=true and returns {ok, revoked}", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true, revoked: 7 }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.revokeExpiredSessions()).resolves.toEqual({ ok: true, revoked: 7 });
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/sessions?expired=true");
    expect(init.method).toBe("DELETE");
    expect(init.headers.Authorization).toBe("Bearer k");
  });
});

describe("RtDbAdminClient anonymous access (SEC-103)", () => {
  it("getAnonymousAccess GETs /admin/db/{db}/anonymous-access and unwraps {enabled}", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ enabled: true }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.getAnonymousAccess("kanban")).resolves.toEqual({ enabled: true });
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/db/kanban/anonymous-access");
    expect(init.method).toBe("GET");
    expect(init.headers.Authorization).toBe("Bearer k");
  });

  it("setAnonymousAccess PATCHes {enabled} and resolves void on {ok:true}", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ ok: true }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.setAnonymousAccess("kanban", false)).resolves.toBeUndefined();
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/db/kanban/anonymous-access");
    expect(init.method).toBe("PATCH");
    expect(JSON.parse(init.body)).toEqual({ enabled: false });
  });
});

describe("RtDbAdminClient mergeUsers", () => {
  it("mergeUsers POSTs {anonUserId, realUserId, confirm: realUserId} and returns the MergeReport", async () => {
    const report: MergeReport = {
      dbs: {
        kanban: {
          tables: { tasks: 3, notes: 1 },
          conflicts: [{ table: "tasks", id: "t7" }],
        },
      },
      storageRepointed: 2,
      sessionsRepointed: 1,
      anonDeleted: true,
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(report));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.mergeUsers("anon1", "real1")).resolves.toEqual(report);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/merge-users");
    expect(init.method).toBe("POST");
    expect(init.headers.Authorization).toBe("Bearer k");
    expect(JSON.parse(init.body)).toEqual({
      anonUserId: "anon1",
      realUserId: "real1",
      confirm: "real1",
    });
  });

  it("mergeUsers surfaces a 404 missing-anon-row envelope as RtDbError", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse(
          { code: "NOT_FOUND", message: "anonymous user not found; nothing to merge" },
          404,
        ),
      );
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.mergeUsers("missing", "real1")).rejects.toMatchObject({
      name: "RtDbError",
      code: "NOT_FOUND",
      message: "anonymous user not found; nothing to merge",
    });
  });
});

describe("HotConfig quota fields (ENH-011)", () => {
  it("HotConfig carries maxTablesPerDb / maxStorageBytesPerDb / maxSubsPerDb", () => {
    const hot: HotConfig = {
      allowedOrigins: [],
      sessionTtlDays: 30,
      maxFileSize: 5_242_880,
      idempotencyTtlMs: 300_000,
      maxTablesPerDb: 25,
      maxStorageBytesPerDb: 104_857_600,
      maxSubsPerDb: 500,
    };
    expect(hot.maxTablesPerDb).toBe(25);
    expect(hot.maxStorageBytesPerDb).toBe(104_857_600);
    expect(hot.maxSubsPerDb).toBe(500);
  });

  it("HotConfigPatch makes the quota fields optional (omittable)", () => {
    const patchOnlyOrigins: HotConfigPatch = { allowedOrigins: ["https://app.x"] };
    const patchOneQuota: HotConfigPatch = { maxSubsPerDb: 50 };
    expect(patchOnlyOrigins.maxTablesPerDb).toBeUndefined();
    expect((patchOneQuota as HotConfigPatch).maxSubsPerDb).toBe(50);
  });

  it("QUOTA_EXCEEDED is a recognized error code on the wire", () => {
    // The validator the client actually uses is RtDbError.isEnvelope, which
    // checks the CODES set. QUOTA_EXCEEDED must be a known code so an envelope
    // carrying it rebuilds as an RtDbError instead of falling back to INTERNAL.
    const raw: unknown = { code: "QUOTA_EXCEEDED", message: "too many tables" };
    expect(RtDbError.isEnvelope(raw)).toBe(true);
    const e = RtDbError.fromEnvelope(raw as { code: "QUOTA_EXCEEDED"; message: string });
    expect(e.code).toBe("QUOTA_EXCEEDED");
    expect(e.message).toBe("too many tables");
  });

  it("RtDbError carries the HTTP status threaded through fromEnvelope/constructor", () => {
    const raw: unknown = { code: "FORBIDDEN", message: "no" };
    expect(RtDbError.fromEnvelope(raw as { code: "FORBIDDEN"; message: string }, 403).status).toBe(
      403,
    );
    // Omitted status is undefined — backward compatible.
    expect(
      RtDbError.fromEnvelope(raw as { code: "FORBIDDEN"; message: string }).status,
    ).toBeUndefined();
    expect(new RtDbError("INTERNAL", "x", undefined, 500).status).toBe(500);
  });
});

describe("RtDbAdminClient cookie mode (ARC-106)", () => {
  it("omits adminKey: sends credentials:include, no Authorization, echoes X-Rtdb-Csrf", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ databases: [] }));
    // Plant a readable CSRF cookie the way /admin/login does.
    const old = document.cookie;
    // biome-ignore lint/suspicious/noDocumentCookie: jsdom has no Cookie Store API; document.cookie is the only way to plant a readable cookie
    document.cookie = "rtdb-admin-csrf=nonce123;path=/";
    try {
      const admin = new RtDbAdminClient({ url: "http://h:8300", fetch: fetchMock });
      await admin.listDbs();
      const [, init] = fetchMock.mock.calls[0];
      expect(init.credentials).toBe("include");
      expect(init.headers.Authorization).toBeUndefined();
      expect(init.headers["X-Rtdb-Csrf"]).toBe("nonce123");
      // ARC-013: protocol header rides in cookie mode too.
      expect(init.headers["X-Rtdb-Protocol"]).toBe(String(PROTOCOL_VERSION));
    } finally {
      // best-effort cleanup; jsdom retains other cookies too
      // biome-ignore lint/suspicious/noDocumentCookie: jsdom has no Cookie Store API
      document.cookie = "rtdb-admin-csrf=;max-age=0;path=/";
      // biome-ignore lint/suspicious/noDocumentCookie: jsdom has no Cookie Store API
      if (old) document.cookie = old;
    }
  });

  it("bearer mode (adminKey set) is unchanged: no credentials:include, Authorization present", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ databases: [] }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await admin.listDbs();
    const [, init] = fetchMock.mock.calls[0];
    expect(init.credentials).toBeUndefined();
    expect(init.headers.Authorization).toBe("Bearer k");
    // ARC-013: protocol header rides on every admin call, bearer mode included.
    expect(init.headers["X-Rtdb-Protocol"]).toBe(String(PROTOCOL_VERSION));
  });

  it("cookie-mode errors carry the HTTP status on the thrown RtDbError", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ code: "FORBIDDEN", message: "no" }, 403));
    const admin = new RtDbAdminClient({ url: "http://h:8300", fetch: fetchMock });
    const e = (await admin.getConfig().catch((e: unknown) => e)) as RtDbError;
    expect(e).toBeInstanceOf(RtDbError);
    expect(e.status).toBe(403);
  });

  it("cookie-mode streamAdmin omits the rtdb-admin subprotocol", async () => {
    // The factory inspects the protocols arg; in cookie mode it should be
    // omitted (the browser attaches the session cookie to the upgrade).
    let captured: string | string[] | undefined = "sentinel";
    const admin = new RtDbAdminClient({
      url: "http://h:8300",
      fetch: vi.fn(),
      webSocketFactory: (_url, protocols) => {
        captured = protocols;
        const sock: { close(): void; onclose: (() => void) | null } = {
          close() {},
          onclose: null,
        };
        // Fire onclose after the generator wires its handler so the internal
        // await resolves and the generator exits cleanly instead of hanging.
        queueMicrotask(() => sock.onclose?.());
        return sock as unknown as WebSocketLike;
      },
    });
    const result = await admin.streamAdmin().next();
    expect(result.done).toBe(true);
    expect(captured).toBeUndefined();
  });
});

describe("RtDbAdminClient explain + slow-queries (ENH-019)", () => {
  it("explainQuery POSTs {query} to /admin/db/{db}/explain and returns the body", async () => {
    const result: ExplainResult = {
      sql: 'SELECT "doc" FROM "kanban"."items"',
      params: [],
      terminal: "collect",
      warnings: ["unindexed-filter:status"],
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(result));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    const q = { json: { table: "items" } };
    await expect(admin.explainQuery("kanban", q)).resolves.toEqual(result);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/db/kanban/explain");
    expect(init.method).toBe("POST");
    expect(init.headers.Authorization).toBe("Bearer k");
    expect(JSON.parse(init.body)).toEqual({ query: { table: "items" } });
  });

  it("explainQuery accepts a raw QueryJson (no .json wrapper)", async () => {
    const result: ExplainResult = {
      sql: "SELECT 1",
      params: [],
      terminal: "count",
      warnings: [],
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(result));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await admin.explainQuery("dbx", { table: "items" });
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({ query: { table: "items" } });
  });

  it("explainQuery URL-encodes the db name", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ sql: "", params: [], terminal: "get", warnings: [] }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await admin.explainQuery("my/db", { table: "t" });
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/db/my%2Fdb/explain");
  });

  it("getSlowQueries GETs /admin/slow-queries with db + limit and returns the body", async () => {
    const entries: SlowQueryEntry[] = [
      {
        startedAtMs: 1700000000000,
        durationMs: 42,
        db: "kanban",
        table: "projects",
        terminal: "collect",
        sql: "select ...",
        // params omitted by default (redacted)
      },
    ];
    const payload: SlowQueriesResponse = {
      queries: entries,
      thresholdMs: 25,
      capacity: 200,
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(payload));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.getSlowQueries({ db: "kanban", limit: 10 })).resolves.toEqual(payload);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/slow-queries?db=kanban&limit=10");
    expect(init.method).toBe("GET");
    expect(init.headers.Authorization).toBe("Bearer k");
  });

  it("getSlowQueries omits the query string when no opts are provided", async () => {
    const payload: SlowQueriesResponse = { queries: [], thresholdMs: 0, capacity: 200 };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(payload));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    await expect(admin.getSlowQueries()).resolves.toEqual(payload);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/slow-queries");
  });

  it("getSlowQueries surfaces params when the server includes them", async () => {
    // When RTDB_SLOW_QUERY_LOG_PARAMS=true the server emits params as string[].
    const payload: SlowQueriesResponse = {
      queries: [
        {
          startedAtMs: 1,
          durationMs: 2,
          db: "x",
          table: "t",
          terminal: "collect",
          sql: "select ...",
          params: ["active"],
        },
      ],
      thresholdMs: 0,
      capacity: 200,
    };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(payload));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });
    const got = await admin.getSlowQueries();
    expect(got.queries[0].params).toEqual(["active"]);
  });
});

describe("RtDbAdminClient workflows (FM-29)", () => {
  const spec: WorkflowSpec = {
    name: "onboard",
    steps: [{ txn: { steps: [{ op: "insert", table: "items", doc: {} }] } }],
  };
  const info = {
    id: "wf1",
    name: "onboard",
    status: "pending",
    currentStep: 0,
    stepCount: 1,
    attempts: 0,
    sleepUntil: 5,
    createdAt: 1,
    updatedAt: 1,
  };

  it("adminListWorkflows GETs the db workflows route with optional filters", async () => {
    // Fresh Response per call — a Response body can be read only once, so a
    // shared mockResolvedValue instance would fail the second call's json().
    const fetchMock = vi
      .fn()
      .mockImplementation(() => Promise.resolve(jsonResponse({ workflows: [info] })));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });

    await expect(admin.adminListWorkflows("kanban")).resolves.toEqual([info]);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/db/kanban/workflows");

    await admin.adminListWorkflows("kanban", { status: "running", limit: 5 });
    expect(fetchMock.mock.calls[1][0]).toBe(
      "http://h:8300/admin/db/kanban/workflows?status=running&limit=5",
    );
  });

  it("adminGetWorkflow GETs the full run row (info + stepOutcomes)", async () => {
    const full = { ...info, stepOutcomes: [] };
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(full));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });

    await expect(admin.adminGetWorkflow("kanban", "wf1")).resolves.toEqual(full);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/admin/db/kanban/workflows/wf1");
  });

  it("adminStartWorkflow POSTs the spec and returns the new id", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ id: "wf2" }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });

    await expect(admin.adminStartWorkflow("kanban", spec)).resolves.toEqual({ id: "wf2" });
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe("http://h:8300/admin/db/kanban/workflows");
    expect(init.method).toBe("POST");
    expect(JSON.parse(init.body)).toEqual(spec);
  });

  it("adminCancelWorkflow / adminDeleteWorkflow hit their routes and return ok", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ ok: true }))
      .mockResolvedValueOnce(jsonResponse({ ok: true }));
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k", fetch: fetchMock });

    await expect(admin.adminCancelWorkflow("kanban", "wf1")).resolves.toEqual({ ok: true });
    await expect(admin.adminDeleteWorkflow("kanban", "wf1")).resolves.toEqual({ ok: true });
    const calls = fetchMock.mock.calls;
    expect(calls[0][0]).toBe("http://h:8300/admin/db/kanban/workflows/wf1/cancel");
    expect(calls[0][1].method).toBe("POST");
    expect(calls[1][0]).toBe("http://h:8300/admin/db/kanban/workflows/wf1");
    expect(calls[1][1].method).toBe("DELETE");
  });
});

describe("RtDbAdminClient default fetch binding", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  // Browsers define fetch on Window and require Window as its receiver: a
  // reference read off the global and stored, then called unbound, throws
  // "Failed to execute 'fetch' on 'Window': Illegal invocation". The injected
  // `fetch: fetchMock` used by every other test never exercises this default
  // path, so a detached store regresses silently in Node-run suites while
  // breaking every browser consumer (the operator dashboard).
  it("invokes the default global fetch with globalThis as its receiver", async () => {
    const fetchMock = vi.fn(function (this: unknown) {
      if (this !== globalThis) {
        throw new TypeError("Failed to execute 'fetch' on 'Window': Illegal invocation");
      }
      return Promise.resolve(jsonResponse({ databases: ["db1"] }));
    });
    vi.stubGlobal("fetch", fetchMock);
    const admin = new RtDbAdminClient({ url: "http://h:8300", adminKey: "k" });

    await expect(admin.listDbs()).resolves.toEqual(["db1"]);
    expect(fetchMock).toHaveBeenCalledOnce();
  });
});
