import { describe, expect, it, vi } from "vitest";
import { RtDbAdminClient } from "../src/admin.js";
import { Migration } from "../src/migration.js";
import type { TransactionJson } from "../src/protocol.js";
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
    };
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
    const fetchMock = vi.fn().mockResolvedValue(
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
});
