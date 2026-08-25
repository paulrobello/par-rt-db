import { afterEach, describe, expect, it, vi } from "vitest";
import { RtDbError } from "../src/errors.js";
import { RtDbHttpClient } from "../src/http.js";
import { mutation } from "../src/mutation.js";
import { PROTOCOL_VERSION, type WorkflowSpec } from "../src/protocol.js";
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

  it("posts a query-batch with db + bearer + queries and returns aligned outcomes", async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        results: [
          { ok: true, result: [{ _id: "a" }] },
          { ok: false, error: { code: "NOT_FOUND", message: "no such table" } },
        ],
      }),
    );
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });
    const queries = [{ table: "items" }, { table: "noSuch" }];

    const results = await client.batchQuery(queries);

    expect(results).toEqual([
      { ok: true, result: [{ _id: "a" }] },
      { ok: false, error: { code: "NOT_FOUND", message: "no such table" } },
    ]);
    const [calledUrl, init] = fetchMock.mock.calls[0];
    expect(calledUrl).toBe("http://h:8300/api/query-batch");
    expect(init.method).toBe("POST");
    expect(init.headers.Authorization).toBe("Bearer tok");
    expect(JSON.parse(init.body)).toEqual({ db: "kanban", queries });
  });

  it("posts a mutation and returns the results array", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ results: [{ id: "new-id" }] }));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    const results = await client.mutate(mutation().insert("items", { title: "x" }).build());

    expect(results).toEqual([{ id: "new-id" }]);
    expect(fetchMock.mock.calls[0][0]).toBe("http://h:8300/api/mutate");
  });

  it("posts a schedule with db + when + txn and returns {id}", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ id: "job-1" }));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    const txn = mutation().insert("items", { title: "x" }).build();
    const result = await client.schedule(txn, { type: "afterMs", ms: 5000 });

    expect(result).toEqual({ id: "job-1" });
    const [calledUrl, init] = fetchMock.mock.calls[0];
    expect(calledUrl).toBe("http://h:8300/api/schedule");
    expect(init.method).toBe("POST");
    expect(init.headers.Authorization).toBe("Bearer tok");
    expect(JSON.parse(init.body)).toEqual({
      db: "kanban",
      when: { type: "afterMs", ms: 5000 },
      txn,
    });
  });

  it("posts cancel/pause/resume to the per-id routes and returns the body's ok", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ ok: true }))
      .mockResolvedValueOnce(jsonResponse({ ok: false }))
      .mockResolvedValueOnce(jsonResponse({ ok: true }));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    await expect(client.cancelSchedule("job-1")).resolves.toBe(true);
    // ok:false = unknown/already-terminal job: a 200 no-op, not an error.
    await expect(client.pauseSchedule("job-1")).resolves.toBe(false);
    await expect(client.resumeSchedule("job-1")).resolves.toBe(true);

    const routes = fetchMock.mock.calls.map((c) => c[0]);
    expect(routes).toEqual([
      "http://h:8300/api/schedule/job-1/cancel",
      "http://h:8300/api/schedule/job-1/pause",
      "http://h:8300/api/schedule/job-1/resume",
    ]);
    for (const [, init] of fetchMock.mock.calls) {
      expect(JSON.parse(init.body)).toEqual({ db: "kanban" });
    }
  });

  it("posts listSchedules and returns the schedules array", async () => {
    const schedules = [
      {
        id: "job-1",
        kind: "oneshot",
        dueAt: 5,
        status: "pending",
        createdAt: 1,
        firedCount: 0,
      },
    ];
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ schedules }));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    const result = await client.listSchedules();

    expect(result).toEqual(schedules);
    const [calledUrl, init] = fetchMock.mock.calls[0];
    expect(calledUrl).toBe("http://h:8300/api/schedules");
    expect(JSON.parse(init.body)).toEqual({ db: "kanban" });
  });

  it("forwards opts.mutId as idempotencyKey in the request body when provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ results: [{ id: "new-id" }] }));
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

  it("forwards opts.idempotencyKey in the request body (preferred alias for mutId)", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ results: [{ id: "new-id" }] }));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    await client.mutate(mutation().insert("items", { title: "x" }).build(), {
      idempotencyKey: "caller-key-2",
    });

    const [, init] = fetchMock.mock.calls[0];
    expect(JSON.parse(init.body).idempotencyKey).toBe("caller-key-2");
  });

  it("prefers opts.idempotencyKey over opts.mutId when both are supplied", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ results: [{ id: "new-id" }] }));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    await client.mutate(mutation().insert("items", { title: "x" }).build(), {
      idempotencyKey: "preferred",
      mutId: "alias",
    });

    const [, init] = fetchMock.mock.calls[0];
    expect(JSON.parse(init.body).idempotencyKey).toBe("preferred");
  });

  it("omits idempotencyKey from the request body when not provided", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ results: [{ id: "new-id" }] }));
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

  it("throws RtDbError INTERNAL when a 2xx body is not valid JSON", async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response("not-json", { status: 200 }));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });
    const q: RtQuery<unknown> = { json: { table: "items" } };

    await expect(client.query(q)).rejects.toBeInstanceOf(RtDbError);
    await expect(client.query(q)).rejects.toMatchObject({
      name: "RtDbError",
      code: "INTERNAL",
      message: "/api/query returned 2xx with no JSON object body",
    });
  });

  it("throws RtDbError INTERNAL when a 2xx body is empty or literal JSON null", async () => {
    for (const body of ["", "null"]) {
      const fetchMock = vi.fn().mockResolvedValue(new Response(body, { status: 200 }));
      const client = new RtDbHttpClient({
        url: "http://h:8300",
        db: "kanban",
        token: "tok",
        fetch: fetchMock,
      });
      const q: RtQuery<unknown> = { json: { table: "items" } };

      await expect(client.query(q)).rejects.toMatchObject({
        name: "RtDbError",
        code: "INTERNAL",
        message: "/api/query returned 2xx with no JSON object body",
      });
    }
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

  it("authMe GETs /auth/me with the client's own bearer and returns the user", async () => {
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

    const user = await client.authMe();

    expect(user).toEqual({
      kind: "user",
      email: "player@example.com",
      name: null,
      githubLogin: "player",
      githubId: 42,
    });
    const [calledUrl, init] = fetchMock.mock.calls[0];
    expect(calledUrl).toBe("http://h:8300/auth/me");
    expect(init.method).toBe("GET");
    // authMe uses the client's own token, not an argument like validateSessionToken.
    expect(init.headers.Authorization).toBe("Bearer client-own-token");
    // ARC-013: protocol header now rides on GET calls too.
    expect(init.headers["X-Rtdb-Protocol"]).toBe(String(PROTOCOL_VERSION));
  });

  it("authMe surfaces a 401 as an RtDbError envelope", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse({ code: "UNAUTHORIZED", message: "machine token rejected" }, 401),
      );
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    await expect(client.authMe()).rejects.toMatchObject({
      name: "RtDbError",
      code: "UNAUTHORIZED",
      message: "machine token rejected",
    });
  });

  it("mints a signed URL via GET /api/storage/{db}/{id}/signed-url", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        jsonResponse({ url: "http://h:8300/storage/f1?exp=100&sig=abc", expiresAt: 100 }),
      );
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    const res = await client.getSignedUrl("f1");

    expect(res).toEqual({ url: "http://h:8300/storage/f1?exp=100&sig=abc", expiresAt: 100 });
    const [calledUrl, init] = fetchMock.mock.calls[0];
    expect(calledUrl).toBe("http://h:8300/api/storage/kanban/f1/signed-url");
    expect(init.method).toBe("GET");
    expect(init.headers.Authorization).toBe("Bearer tok");
  });

  it("getSignedUrl appends ttlSeconds only when provided", async () => {
    const withTtl = vi.fn().mockResolvedValue(jsonResponse({ url: "u", expiresAt: 1 }));
    const c1 = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: withTtl,
    });
    await c1.getSignedUrl("f1", 120);
    expect(withTtl.mock.calls[0][0]).toBe(
      "http://h:8300/api/storage/kanban/f1/signed-url?ttlSeconds=120",
    );

    const noTtl = vi.fn().mockResolvedValue(jsonResponse({ url: "u", expiresAt: 1 }));
    const c2 = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: noTtl,
    });
    await c2.getSignedUrl("f1");
    expect(noTtl.mock.calls[0][0]).toBe("http://h:8300/api/storage/kanban/f1/signed-url");
  });
});

describe("RtDbHttpClient workflows (FM-29)", () => {
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

  it("posts startWorkflow to /api/workflows and returns {id}", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ id: "wf1" }));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    await expect(client.startWorkflow(spec)).resolves.toEqual({ id: "wf1" });
    const [calledUrl, init] = fetchMock.mock.calls[0];
    expect(calledUrl).toBe("http://h:8300/api/workflows");
    expect(init.method).toBe("POST");
    expect(init.headers.Authorization).toBe("Bearer tok");
    expect(JSON.parse(init.body)).toEqual({ db: "kanban", spec });
  });

  it("posts listWorkflows to /api/workflows/list with an optional status filter", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse({ workflows: [info] }))
      .mockResolvedValueOnce(jsonResponse({ workflows: [] }));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    await expect(client.listWorkflows()).resolves.toEqual([info]);
    expect(JSON.parse(fetchMock.mock.calls[0][1].body)).toEqual({ db: "kanban" });

    await expect(client.listWorkflows("failed")).resolves.toEqual([]);
    expect(fetchMock.mock.calls[1][0]).toBe("http://h:8300/api/workflows/list");
    expect(JSON.parse(fetchMock.mock.calls[1][1].body)).toEqual({
      db: "kanban",
      status: "failed",
    });
  });

  it("posts cancelWorkflow to the per-id route and returns cancelled", async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ cancelled: true }));
    const client = new RtDbHttpClient({
      url: "http://h:8300",
      db: "kanban",
      token: "tok",
      fetch: fetchMock,
    });

    await expect(client.cancelWorkflow("wf1")).resolves.toBe(true);
    const [calledUrl, init] = fetchMock.mock.calls[0];
    expect(calledUrl).toBe("http://h:8300/api/workflows/wf1/cancel");
    expect(JSON.parse(init.body)).toEqual({ db: "kanban" });
  });
});

describe("RtDbHttpClient default fetch binding", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  // See the twin test in admin.test.ts: browsers require Window as fetch's
  // receiver, so the default must never store a detached `globalThis.fetch`.
  it("invokes the default global fetch with globalThis as its receiver", async () => {
    const fetchMock = vi.fn(function (this: unknown) {
      if (this !== globalThis) {
        throw new TypeError("Failed to execute 'fetch' on 'Window': Illegal invocation");
      }
      return Promise.resolve(jsonResponse({ result: [{ _id: "a" }] }));
    });
    vi.stubGlobal("fetch", fetchMock);
    const client = new RtDbHttpClient({ url: "http://h:8300", db: "kanban", token: "tok" });
    const q: RtQuery<Array<{ _id: string }>> = { json: { table: "items" } };

    await expect(client.query(q)).resolves.toEqual([{ _id: "a" }]);
    expect(fetchMock).toHaveBeenCalledOnce();
  });
});
