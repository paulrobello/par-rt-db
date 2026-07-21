import { beforeAll, describe, expect, it } from "vitest";
import { createApi } from "../../src/query.js";
import { mutation } from "../../src/mutation.js";
import { defineSchema, defineTable, t } from "../../src/schema.js";
import { httpClient, provisionDb, testServer, wsClient } from "./harness.js";

const schema = defineSchema({
  items: defineTable({
    projectId: t.string(),
    title: t.string(),
    order: t.number(),
  }).index("by_project", ["projectId"]),
});
const api = createApi(schema);

const server = testServer();

describe.skipIf(server === null)("e2e against a running server", () => {
  // biome-ignore lint/style/noNonNullAssertion: guarded by describe.skipIf
  const srv = server!;
  let db = "";
  let token = "";

  beforeAll(async () => {
    const provisioned = await provisionDb(srv, schema);
    db = provisioned.db;
    token = provisioned.token;
  });

  it("HTTP insert then HTTP query round-trips with system fields", async () => {
    const http = httpClient(srv, db, token);
    const [id] = await http.mutate(
      mutation().insert("items", { projectId: "p1", title: "first", order: 1 }).build(),
    );
    expect(typeof id).toBe("string");

    const rows = await http.query(api.items.query().withIndex("by_project", ["p1"]).collect());
    expect(rows).toHaveLength(1);
    expect(rows[0]).toMatchObject({
      _id: id,
      projectId: "p1",
      title: "first",
      _version: expect.any(Number),
    });
  });

  it("delivers a live queryUpdate over WS when an HTTP mutation writes", async () => {
    const ws = wsClient(srv, db, token);
    const http = httpClient(srv, db, token);
    const updates: unknown[][] = [];

    ws.connect();
    await new Promise<void>((resolve) => {
      const off = ws.onAuthChange((state) => {
        if (state === "authenticated") {
          off();
          resolve();
        }
      });
    });

    ws.subscribe(api.items.query().withIndex("by_project", ["p2"]).collect(), (v) => {
      updates.push(v as unknown[]);
    });

    // Wait for the initial (empty) push.
    await waitFor(() => updates.length >= 1);
    expect(updates.at(-1)).toEqual([]);

    await http.mutate(
      mutation().insert("items", { projectId: "p2", title: "live", order: 1 }).build(),
    );

    await waitFor(() => (updates.at(-1)?.length ?? 0) === 1);
    expect((updates.at(-1) as Array<{ title: string }>)[0].title).toBe("live");

    ws.close();
  });
});

async function waitFor(predicate: () => boolean, timeoutMs = 5000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) {
      throw new Error("waitFor timed out");
    }
    await new Promise((r) => setTimeout(r, 25));
  }
}
