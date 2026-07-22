import { describe, expect, it, vi } from "vitest";
import { RtDbError } from "../src/errors.js";
import { RtDbHttpClient } from "../src/http.js";
import { mutation } from "../src/mutation.js";
import type { RtQuery } from "../src/query.js";

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

describe("RtDbHttpClient", () => {
  it("posts a query with db + bearer token and returns result", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ result: [{ _id: "a" }] }));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });
    const q: RtQuery<Array<{ _id: string }>> = { json: { table: "items" } };

    const result = await client.query(q);

    expect(result).toEqual([{ _id: "a" }]);
    const [calledUrl, init] = fetchMock.mock.calls[0];
    expect(calledUrl).toBe("http://h:8300/api/query");
    expect(init.method).toBe("POST");
    expect(init.headers.Authorization).toBe("Bearer tok");
    expect(JSON.parse(init.body)).toEqual({ db: "kanban", query: { table: "items" } });
  });

  it("posts a mutation and returns the results array", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ results: ["new-id"] }));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    const results = await client.mutate(mutation().insert("items", { title: "x" }).build());

    expect(results).toEqual(["new-id"]);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/api/mutate");
  });

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

  it("throws RtDbError from an error envelope on non-2xx", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ code: "PRECONDITION_FAILED", message: "stale" }, 409));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    await expect(client.mutate(mutation().build())).rejects.toMatchObject({
      name: "RtDbError",
      code: "PRECONDITION_FAILED",
      message: "stale",
    });
    await expect(client.mutate(mutation().build())).rejects.toBeInstanceOf(RtDbError);
  });
});
