import { describe, expect, it } from "vitest";
import { RtDbClient, type WebSocketLike } from "../src/client.js";
import { projectOptimisticUpdate } from "../src/optimistic.js";
import type { QueryJson, TransactionJson } from "../src/protocol.js";
import type { RtQuery } from "../src/query.js";

/** A doc as it appears in a result: user fields plus merged `_id`/`_creationTime`/`_version`. */
type Doc = Record<string, unknown> & { _id: string };

/** Wraps a QueryJson as an RtQuery for `subscribe`. */
const rq = (json: QueryJson): RtQuery<unknown> => ({ json });
const txn = (steps: TransactionJson["steps"]): TransactionJson => ({ steps });
const doc = (d: Doc): Doc => d;

// ---------------------------------------------------------------------------
// Pure projection
// ---------------------------------------------------------------------------

describe("projectOptimisticUpdate", () => {
  const clock = () => 1_000;

  it("overlays an insert on an unfiltered collect (append)", () => {
    const last = [doc({ _id: "a", n: 1 })];
    const out = projectOptimisticUpdate(
      { table: "items" },
      last,
      txn([{ op: "insert", table: "items", doc: { n: 2 } }]),
      clock,
    );
    expect(out.overlaid).toBe(true);
    const value = out.value as Doc[];
    expect(value).toHaveLength(2);
    expect(value[0]).toEqual({ _id: "a", n: 1 });
    expect(value[1]).toMatchObject({ n: 2, _creationTime: 1_000, _version: 1 });
    expect(typeof value[1]._id).toBe("string");
    expect(value[1]._id).toMatch(/^__optimistic__\d+$/);
  });

  it("does not overlay an insert that would overflow a full take window", () => {
    const last = [doc({ _id: "a", n: 1 })];
    const out = projectOptimisticUpdate(
      { table: "items", take: 1 },
      last,
      txn([{ op: "insert", table: "items", doc: { n: 2 } }]),
      clock,
    );
    expect(out.overlaid).toBe(false);
  });

  it("overlays a patch on a doc present in an unfiltered result", () => {
    const out = projectOptimisticUpdate(
      { table: "items" },
      [doc({ _id: "a", n: 1 })],
      txn([{ op: "patch", table: "items", id: "a", fields: { n: 9 } }]),
    );
    expect(out.overlaid).toBe(true);
    expect(out.value).toEqual([{ _id: "a", n: 9 }]);
  });

  it("treats a no-op patch (same value) as no overlay", () => {
    const out = projectOptimisticUpdate(
      { table: "items" },
      [doc({ _id: "a", n: 1 })],
      txn([{ op: "patch", table: "items", id: "a", fields: { n: 1 } }]),
    );
    expect(out.overlaid).toBe(false);
  });

  it("does not overlay a patch on a doc absent from the result", () => {
    const out = projectOptimisticUpdate(
      { table: "items" },
      [doc({ _id: "a", n: 1 })],
      txn([{ op: "patch", table: "items", id: "missing", fields: { n: 9 } }]),
    );
    expect(out.overlaid).toBe(false);
  });

  it("overlays a replace, preserving _id and _creationTime", () => {
    const out = projectOptimisticUpdate(
      { table: "items" },
      [doc({ _id: "a", _creationTime: 7, n: 1 })],
      txn([{ op: "replace", table: "items", id: "a", doc: { n: 2 } }]),
    );
    expect(out.overlaid).toBe(true);
    expect(out.value).toEqual([{ _id: "a", _creationTime: 7, n: 2 }]);
  });

  it("overlays a delete of a doc present in an unfiltered result", () => {
    const out = projectOptimisticUpdate(
      { table: "items" },
      [doc({ _id: "a", n: 1 }), doc({ _id: "b", n: 2 })],
      txn([{ op: "delete", table: "items", id: "a" }]),
    );
    expect(out.overlaid).toBe(true);
    expect(out.value).toEqual([{ _id: "b", n: 2 }]);
  });

  it("overlays a delete even on a filtered query (membership is unambiguous)", () => {
    const out = projectOptimisticUpdate(
      { table: "items", index: "by_n", eq: [1] },
      [doc({ _id: "a", n: 1 })],
      txn([{ op: "delete", table: "items", id: "a" }]),
    );
    expect(out.overlaid).toBe(true);
    expect(out.value).toEqual([]);
  });

  it("does not overlay an insert/patch on a filtered query (membership depends on the schema-bound filter)", () => {
    const last = [doc({ _id: "a", n: 1 })];
    expect(
      projectOptimisticUpdate(
        { table: "items", index: "by_n", eq: [1] },
        last,
        txn([{ op: "insert", table: "items", doc: { n: 1 } }]),
      ).overlaid,
    ).toBe(false);
    expect(
      projectOptimisticUpdate(
        { table: "items", index: "by_n", eq: [1] },
        last,
        txn([{ op: "patch", table: "items", id: "a", fields: { n: 2 } }]),
      ).overlaid,
    ).toBe(false);
  });

  it("does not overlay an upsert (insert-vs-patch path is ambiguous without the schema)", () => {
    const out = projectOptimisticUpdate(
      { table: "items" },
      [doc({ _id: "a", n: 1 })],
      txn([
        { op: "upsert", table: "items", index: "by_n", eq: [1], insert: { n: 1 }, patch: { n: 2 } },
      ]),
    );
    expect(out.overlaid).toBe(false);
  });

  it("overlays patch/replace/delete of the target id for a get(id) query", () => {
    expect(
      projectOptimisticUpdate(
        { table: "items", get: "a" },
        doc({ _id: "a", n: 1 }),
        txn([{ op: "patch", table: "items", id: "a", fields: { n: 9 } }]),
      ),
    ).toEqual({ overlaid: true, value: { _id: "a", n: 9 } });
    expect(
      projectOptimisticUpdate(
        { table: "items", get: "a" },
        doc({ _id: "a", n: 1 }),
        txn([{ op: "delete", table: "items", id: "a" }]),
      ),
    ).toEqual({ overlaid: true, value: null });
  });

  it("does not overlay a get(id) patch of a different id", () => {
    const out = projectOptimisticUpdate(
      { table: "items", get: "a" },
      doc({ _id: "a", n: 1 }),
      txn([{ op: "patch", table: "items", id: "other", fields: { n: 9 } }]),
    );
    expect(out.overlaid).toBe(false);
  });

  it("does not overlay count/first/unique/paginate queries", () => {
    const insert = txn([{ op: "insert", table: "items", doc: { n: 1 } }]);
    expect(projectOptimisticUpdate({ table: "items", count: true }, 5, insert).overlaid).toBe(
      false,
    );
    expect(projectOptimisticUpdate({ table: "items", first: true }, null, insert).overlaid).toBe(
      false,
    );
    expect(projectOptimisticUpdate({ table: "items", unique: true }, null, insert).overlaid).toBe(
      false,
    );
    expect(
      projectOptimisticUpdate({ table: "items", paginate: { numItems: 10 } }, { docs: [] }, insert)
        .overlaid,
    ).toBe(false);
  });

  it("ignores steps on other tables (no overlay)", () => {
    const out = projectOptimisticUpdate(
      { table: "items" },
      [doc({ _id: "a", n: 1 })],
      txn([{ op: "insert", table: "other", doc: { n: 2 } }]),
    );
    expect(out.overlaid).toBe(false);
  });

  it("applies multiple safe steps in one transaction", () => {
    const out = projectOptimisticUpdate(
      { table: "items" },
      [doc({ _id: "a", n: 1 }), doc({ _id: "b", n: 2 })],
      txn([
        { op: "patch", table: "items", id: "a", fields: { n: 10 } },
        { op: "delete", table: "items", id: "b" },
      ]),
    );
    expect(out.overlaid).toBe(true);
    expect(out.value).toEqual([{ _id: "a", n: 10 }]);
  });

  it("declines the whole projection if any single step is ambiguous (all-or-nothing)", () => {
    const out = projectOptimisticUpdate(
      { table: "items" },
      [doc({ _id: "a", n: 1 })],
      txn([
        { op: "patch", table: "items", id: "a", fields: { n: 10 } },
        { op: "upsert", table: "items", index: "by_n", eq: [1], insert: { n: 1 }, patch: { n: 2 } },
      ]),
    );
    expect(out.overlaid).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// Client integration (FakeSocket)
// ---------------------------------------------------------------------------

class FakeSocket implements WebSocketLike {
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: { code: number; reason: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  readonly sent: string[] = [];

  send(data: string): void {
    this.sent.push(data);
  }
  close(): void {}
  open(): void {
    this.onopen?.();
  }
  deliver(msg: unknown): void {
    this.onmessage?.({ data: JSON.stringify(msg) });
  }
  get sentParsed(): unknown[] {
    return this.sent.map((s) => JSON.parse(s));
  }
}

interface Harness {
  client: RtDbClient;
  socket: FakeSocket;
}

/** Connects + authenticates a client with a single ready socket. */
function harness(opts: { optimistic?: boolean } = {}): Harness {
  const sockets: FakeSocket[] = [];
  const client = new RtDbClient({
    url: "ws://h:8300",
    db: "kanban",
    getToken: () => "tok",
    webSocketFactory: () => {
      const s = new FakeSocket();
      sockets.push(s);
      return s;
    },
    heartbeatMs: 0,
    optimisticUpdates: opts.optimistic ?? false,
    now: () => 5_000,
    random: () => 0.5,
    setTimeoutImpl: () => 0 as unknown as ReturnType<typeof setTimeout>,
    clearTimeoutImpl: () => {},
  });
  client.connect();
  const socket = sockets[0]; // created synchronously by the factory during connect()
  socket.open();
  socket.deliver({ type: "authOk", user: { kind: "machine" } });
  return { client, socket };
}

function mutateFrame(socket: FakeSocket): { mutId: string } {
  return socket.sentParsed
    .filter(
      (m): m is { type: "mutate"; mutId: string } => (m as { type: string }).type === "mutate",
    )
    .at(-1) as { mutId: string };
}

function firstSubId(socket: FakeSocket): string {
  return (
    socket.sentParsed.find((m) => (m as { type: string }).type === "subscribe") as {
      queryId: string;
    }
  ).queryId;
}

describe("RtDbClient optimistic updates", () => {
  it("overlays an insert immediately and reconciles on the next queryUpdate", async () => {
    const { client, socket } = harness({ optimistic: true });
    const updates: unknown[] = [];
    client.subscribe(rq({ table: "items" }), (v) => updates.push(v));
    const subId = firstSubId(socket);
    socket.deliver({ type: "queryUpdate", queryId: subId, result: [{ _id: "a", n: 1 }] });
    expect(updates).toEqual([[{ _id: "a", n: 1 }]]);

    // Submit the mutation; the overlay must arrive synchronously, before any server reply.
    const pending = client.mutate(txn([{ op: "insert", table: "items", doc: { n: 2 } }]));
    expect(updates).toHaveLength(2);
    const overlaid = (updates[1] as Doc[])[1];
    expect(overlaid).toMatchObject({ n: 2, _creationTime: 5_000, _version: 1 });
    expect(overlaid._id).toMatch(/^__optimistic__/);

    // Authoritative server value replaces the overlay (server wins).
    socket.deliver({
      type: "queryUpdate",
      queryId: subId,
      result: [
        { _id: "a", n: 1 },
        { _id: "real", n: 2, _creationTime: 5_000, _version: 1 },
      ],
    });
    expect(updates[2]).toEqual([
      { _id: "a", n: 1 },
      { _id: "real", n: 2, _creationTime: 5_000, _version: 1 },
    ]);

    socket.deliver({
      type: "mutateOk",
      mutId: mutateFrame(socket).mutId,
      results: [{ id: "real" }],
    });
    await expect(pending).resolves.toEqual([{ id: "real" }]);
  });

  it("overlays a delete immediately and reconciles on the next queryUpdate", async () => {
    const { client, socket } = harness({ optimistic: true });
    const updates: unknown[] = [];
    client.subscribe(rq({ table: "items" }), (v) => updates.push(v));
    const subId = firstSubId(socket);
    socket.deliver({ type: "queryUpdate", queryId: subId, result: [{ _id: "a", n: 1 }] });

    const pending = client.mutate(txn([{ op: "delete", table: "items", id: "a" }]));
    expect(updates.at(-1)).toEqual([]); // optimistic: doc removed before the server replies

    socket.deliver({ type: "queryUpdate", queryId: subId, result: [] });
    expect(updates.at(-1)).toEqual([]); // authoritative reconcile — same value, no extra frame
    socket.deliver({ type: "mutateOk", mutId: mutateFrame(socket).mutId, results: [null] });
    await expect(pending).resolves.toEqual([null]);
  });

  it("reverts the overlay to the authoritative value when the mutation is rejected", async () => {
    const { client, socket } = harness({ optimistic: true });
    const updates: unknown[] = [];
    client.subscribe(rq({ table: "items" }), (v) => updates.push(v));
    const subId = firstSubId(socket);
    socket.deliver({ type: "queryUpdate", queryId: subId, result: [{ _id: "a", n: 1 }] });

    const pending = client.mutate(
      txn([{ op: "patch", table: "items", id: "a", fields: { n: 9 } }]),
    );
    expect(updates.at(-1)).toEqual([{ _id: "a", n: 9 }]); // optimistic overlay

    // Server rejects: no queryUpdate will ever come, so the overlay must roll back.
    socket.deliver({
      type: "mutateErr",
      mutId: mutateFrame(socket).mutId,
      error: { code: "PRECONDITION_FAILED", message: "stale" },
    });
    expect(updates.at(-1)).toEqual([{ _id: "a", n: 1 }]); // reverted to serverLast
    await expect(pending).rejects.toMatchObject({ code: "PRECONDITION_FAILED" });
  });

  it("does not overlay anything when optimisticUpdates is disabled (default)", () => {
    const { client, socket } = harness(); // optimisticUpdates defaults to false
    const updates: unknown[] = [];
    client.subscribe(rq({ table: "items" }), (v) => updates.push(v));
    const subId = firstSubId(socket);
    socket.deliver({ type: "queryUpdate", queryId: subId, result: [{ _id: "a", n: 1 }] });
    expect(updates).toHaveLength(1);

    client.mutate(txn([{ op: "insert", table: "items", doc: { n: 2 } }]));
    expect(updates).toHaveLength(1); // no synchronous overlay

    socket.deliver({
      type: "queryUpdate",
      queryId: subId,
      result: [
        { _id: "a", n: 1 },
        { _id: "b", n: 2 },
      ],
    });
    expect(updates.at(-1)).toEqual([
      { _id: "a", n: 1 },
      { _id: "b", n: 2 },
    ]);
  });

  it("falls back to no overlay for an ambiguous projection (filtered insert) and uses the server value", () => {
    const { client, socket } = harness({ optimistic: true });
    const updates: unknown[] = [];
    client.subscribe(rq({ table: "items", index: "by_n", eq: [1] }), (v) => updates.push(v));
    const subId = firstSubId(socket);
    socket.deliver({ type: "queryUpdate", queryId: subId, result: [{ _id: "a", n: 1 }] });
    expect(updates).toHaveLength(1);

    // Insert into a filtered query is ambiguous — no overlay.
    client.mutate(txn([{ op: "insert", table: "items", doc: { n: 1 } }]));
    expect(updates).toHaveLength(1);

    socket.deliver({
      type: "queryUpdate",
      queryId: subId,
      result: [
        { _id: "a", n: 1 },
        { _id: "b", n: 1 },
      ],
    });
    expect(updates.at(-1)).toEqual([
      { _id: "a", n: 1 },
      { _id: "b", n: 1 },
    ]);
  });

  it("does not get stuck: the overlay reverts and a later authoritative push still lands", async () => {
    const { client, socket } = harness({ optimistic: true });
    const updates: unknown[] = [];
    client.subscribe(rq({ table: "items" }), (v) => updates.push(v));
    const subId = firstSubId(socket);
    socket.deliver({ type: "queryUpdate", queryId: subId, result: [{ _id: "a", n: 1 }] });

    const pending = client.mutate(
      txn([{ op: "patch", table: "items", id: "a", fields: { n: 10 } }]),
    );
    expect(updates.at(-1)).toEqual([{ _id: "a", n: 10 }]);

    // Mutation fails → overlay reverts to the authoritative value.
    socket.deliver({
      type: "mutateErr",
      mutId: mutateFrame(socket).mutId,
      error: { code: "PRECONDITION_FAILED", message: "stale" },
    });
    expect(updates.at(-1)).toEqual([{ _id: "a", n: 1 }]);

    // A later authoritative push still lands correctly — nothing is wedged.
    socket.deliver({ type: "queryUpdate", queryId: subId, result: [{ _id: "a", n: 1 }] });
    await expect(pending).rejects.toMatchObject({ code: "PRECONDITION_FAILED" });
  });
});
