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

  it("forwards opts.mutId as idempotencyKey in the request body when provided", async () => {
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
    expect(JSON.parse(init.body).idempotencyKey).toBe("caller-key-1");
  });

  it("omits idempotencyKey from the request body when not provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ results: ["new-id"] }));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    await client.mutate(mutation().insert("items", { title: "x" }).build());

    const [, init] = fetchMock.mock.calls[0];
    expect(JSON.parse(init.body)).not.toHaveProperty("idempotencyKey");
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

  it("validateSessionToken GETs /auth/validate with the presented bearer and returns the user", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        user: {
          kind: "user",
          email: "player@example.com",
          name: null,
          githubLogin: "player",
          githubId: 42,
        },
      }),
    );
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "client-own-token",
      fetch: fetchMock,
    });

    const user = await client.validateSessionToken("player-session-token");

    expect(user).toEqual({
      kind: "user",
      email: "player@example.com",
      name: null,
      githubLogin: "player",
      githubId: 42,
    });
    const [calledUrl, init] = fetchMock.mock.calls[0];
    expect(calledUrl).toBe("http://h:8300/auth/validate");
    expect(init.method).toBe("GET");
    // The validated token is the argument, not the client's own token.
    expect(init.headers.Authorization).toBe("Bearer player-session-token");
  });

  it("validateSessionToken surfaces a 401 as an RtDbError envelope", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ code: "UNAUTHORIZED", message: "invalid token" }, 401));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    await expect(client.validateSessionToken("not-a-real-token")).rejects.toMatchObject({
      name: "RtDbError",
      code: "UNAUTHORIZED",
      message: "invalid token",
    });
  });

  it("validateSessionToken tolerates a response omitting the github fields", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(jsonResponse({ user: { kind: "machine", email: null, name: null } }));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    const user = await client.validateSessionToken("mach-tok");

    expect(user.kind).toBe("machine");
    expect(user.githubLogin).toBeUndefined();
    expect(user.githubId).toBeUndefined();
  });
});
