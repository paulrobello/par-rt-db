import { describe, expect, it, vi } from "vitest";
import { RtDbAdminClient } from "../src/admin.js";
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
});
