import { describe, expect, it } from "vitest";
import { RtDbClient, type WebSocketLike } from "../src/client.js";

/** A controllable fake socket. Records sent frames; lets the test drive open/message/close. */
class FakeSocket implements WebSocketLike {
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: { code: number; reason: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  readonly sent: string[] = [];
  closed = false;
  /** When set, the socket drops itself after this many sends — simulates an outage
   * landing mid-replay (sends after the drop still land in `sent` but go nowhere). */
  dropAfterSends?: number;
  private sends = 0;

  send(data: string): void {
    this.sent.push(data);
    if (this.dropAfterSends !== undefined && ++this.sends >= this.dropAfterSends) {
      this.closed = true;
      this.onclose?.({ code: 4000, reason: "simulated outage" });
    }
  }
  close(code = 1000, reason = ""): void {
    this.closed = true;
    this.onclose?.({ code, reason });
  }

  // test helpers
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
  sockets: FakeSocket[];
  runTimers: () => void;
  setNow: (t: number) => void;
}

function newClient(
  overrides: { offlineQueue?: boolean; offlineQueueTtlMs?: number } = {},
): Harness {
  const sockets: FakeSocket[] = [];
  const timers: Array<() => void> = [];
  const clock = { t: 0 };
  const client = new RtDbClient({
    url: "ws://h:8300",
    db: "kanban",
    getToken: () => "tok",
    webSocketFactory: () => {
      const s = new FakeSocket();
      sockets.push(s);
      return s;
    },
    heartbeatMs: 0, // disabled; only reconnect timers are queued
    now: () => clock.t,
    random: () => 0.5,
    setTimeoutImpl: (fn) => {
      timers.push(fn);
      return timers.length as unknown as ReturnType<typeof setTimeout>;
    },
    clearTimeoutImpl: () => {},
    ...(overrides.offlineQueue !== undefined ? { offlineQueue: overrides.offlineQueue } : {}),
    ...(overrides.offlineQueueTtlMs !== undefined
      ? { offlineQueueTtlMs: overrides.offlineQueueTtlMs }
      : {}),
  });
  const runTimers = () => {
    for (const fn of timers.splice(0)) {
      fn();
    }
  };
  return { client, sockets, runTimers, setNow: (t) => (clock.t = t) };
}

const frames = (s: FakeSocket) => s.sentParsed as Array<{ type: string; [k: string]: unknown }>;
const mutateFrames = (s: FakeSocket) =>
  frames(s).filter((m) => m.type === "mutate") as Array<{
    type: string;
    mutId: string;
    idempotencyKey?: string;
    txn: unknown;
  }>;

/** Connects, completes the auth handshake, then drops the socket without acking. */
function connectAndDrop(h: Harness): void {
  h.client.connect();
  h.sockets[0].open();
  h.sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });
  h.sockets[0].close(4000, "simulated outage");
}

describe("offline mutation queue", () => {
  it("queues a mutate issued while disconnected and lands it on the server after reconnect, in order", async () => {
    const h = newClient({ offlineQueue: true });
    connectAndDrop(h);

    const p1 = h.client.mutate({ steps: [{ op: "insert", table: "items", doc: { n: 1 } }] });
    const p2 = h.client.mutate(
      { steps: [{ op: "insert", table: "items", doc: { n: 2 } }] },
      { idempotencyKey: "caller-key-2" },
    );

    h.runTimers(); // fires the reconnect timer
    h.sockets[1].open();
    h.sockets[1].deliver({ type: "authOk", user: { kind: "machine" } });

    const sent = mutateFrames(h.sockets[1]);
    expect(sent).toHaveLength(2);
    expect(sent[0].txn).toEqual({ steps: [{ op: "insert", table: "items", doc: { n: 1 } }] });
    expect(sent[1].idempotencyKey).toBe("caller-key-2");
    // A queued mutation always carries a key — one is minted when the caller supplied none.
    expect(sent[0].idempotencyKey).toMatch(/^off-/);

    h.sockets[1].deliver({
      type: "mutateOk",
      mutId: sent[0].mutId,
      results: [{ id: "id-1" }],
    });
    h.sockets[1].deliver({
      type: "mutateOk",
      mutId: sent[1].mutId,
      results: [{ id: "id-2" }],
    });
    await expect(p1).resolves.toEqual([{ id: "id-1" }]);
    await expect(p2).resolves.toEqual([{ id: "id-2" }]);
  });

  it("replays with the original idempotency key after a forced mid-replay disconnect, so a dedup server applies each mutation once", async () => {
    const h = newClient({ offlineQueue: true });
    h.client.connect();
    h.sockets[0].open();
    h.sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    // Both mutations are dispatched live, then the connection drops before either is acked.
    const p1 = h.client.mutate(
      { steps: [{ op: "insert", table: "items", doc: { n: 1 } }] },
      { idempotencyKey: "k-1" },
    );
    const p2 = h.client.mutate(
      { steps: [{ op: "insert", table: "items", doc: { n: 2 } }] },
      { idempotencyKey: "k-2" },
    );
    h.sockets[0].close(4000, "drop before ack");

    // First replay: drop again after the FIRST mutate frame — m1 is in-flight (re-queued),
    // m2 was never sent (stays queued), and order must survive both paths.
    h.runTimers();
    h.sockets[1].dropAfterSends = 2; // auth is send 1; the first mutate send drops the socket
    h.sockets[1].open();
    h.sockets[1].deliver({ type: "authOk", user: { kind: "machine" } });
    expect(mutateFrames(h.sockets[1]).map((f) => f.idempotencyKey)).toEqual(["k-1"]);

    // Second replay: both mutations replay, in original order, with their ORIGINAL keys.
    h.runTimers();
    h.sockets[2].open();
    h.sockets[2].deliver({ type: "authOk", user: { kind: "machine" } });
    const replay = mutateFrames(h.sockets[2]);
    expect(replay.map((f) => f.idempotencyKey)).toEqual(["k-1", "k-2"]);

    // Server-side dedup contract: every wire attempt for a logical mutation shares one key,
    // so a dedup-aware server applies each exactly once — two attempts, two documents.
    const attemptsPerKey = new Map<string, number>();
    for (const s of h.sockets) {
      for (const f of mutateFrames(s)) {
        const key = f.idempotencyKey as string;
        attemptsPerKey.set(key, (attemptsPerKey.get(key) ?? 0) + 1);
      }
    }
    expect(attemptsPerKey.get("k-1")).toBeGreaterThanOrEqual(2); // was genuinely replayed
    expect(attemptsPerKey.size).toBe(2); // distinct keys = applied documents

    h.sockets[2].deliver({
      type: "mutateOk",
      mutId: replay[0].mutId,
      results: [{ id: "id-1" }],
    });
    h.sockets[2].deliver({
      type: "mutateOk",
      mutId: replay[1].mutId,
      results: [{ id: "id-2" }],
    });
    await expect(p1).resolves.toEqual([{ id: "id-1" }]);
    await expect(p2).resolves.toEqual([{ id: "id-2" }]);
  });

  it("exposes queue depth and oldest-entry age via getOfflineQueueStatus", () => {
    const h = newClient({ offlineQueue: true });
    h.client.connect(); // never completes the handshake — everything queues
    void h.client.mutate({ steps: [] });
    void h.client.mutate({ steps: [] });

    expect(h.client.getOfflineQueueStatus()).toEqual({ depth: 2, oldestAgeMs: 0 });

    h.setNow(5000);
    expect(h.client.getOfflineQueueStatus()).toEqual({ depth: 2, oldestAgeMs: 5000 });

    // Flushing on auth drains the queue and the status reflects it.
    h.sockets[0].open();
    h.sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });
    expect(h.client.getOfflineQueueStatus()).toEqual({ depth: 0, oldestAgeMs: null });
  });

  it("rejects entries past the TTL and never replays them", async () => {
    const h = newClient({ offlineQueue: true, offlineQueueTtlMs: 1000 });
    h.client.connect(); // stuck pre-auth
    const p = h.client.mutate(
      { steps: [{ op: "insert", table: "items", doc: {} }] },
      { idempotencyKey: "will-expire" },
    );
    h.setNow(2000); // past the 1000ms TTL

    // Reading the status is itself a prune point: the expired entry rejects here.
    expect(h.client.getOfflineQueueStatus()).toEqual({ depth: 0, oldestAgeMs: null });
    await expect(p).rejects.toMatchObject({ name: "RtDbError", code: "INTERNAL" });

    // The auth completes — no mutate frame may go out for the expired entry.
    h.sockets[0].open();
    h.sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });
    expect(mutateFrames(h.sockets[0])).toHaveLength(0);
  });

  it("close() rejects queued entries and clears the queue", async () => {
    const h = newClient({ offlineQueue: true });
    h.client.connect(); // stuck pre-auth
    const p = h.client.mutate({ steps: [] });
    h.client.close();
    await expect(p).rejects.toMatchObject({ name: "RtDbError", code: "INTERNAL" });
    expect(h.client.getOfflineQueueStatus()).toEqual({ depth: 0, oldestAgeMs: null });
  });

  it("does not mint or queue by default — the disabled path is unchanged", async () => {
    const h = newClient(); // offlineQueue off
    connectAndDrop(h);
    const p = h.client.mutate({ steps: [{ op: "insert", table: "items", doc: { n: 1 } }] });
    h.runTimers();
    h.sockets[1].open();
    h.sockets[1].deliver({ type: "authOk", user: { kind: "machine" } });

    const sent = mutateFrames(h.sockets[1]);
    expect(sent).toHaveLength(1);
    expect(sent[0]).not.toHaveProperty("idempotencyKey"); // no auto-minted key
    h.sockets[1].deliver({ type: "mutateOk", mutId: sent[0].mutId, results: [{ id: "id-1" }] });
    await expect(p).resolves.toEqual([{ id: "id-1" }]);
    expect(h.client.getOfflineQueueStatus()).toEqual({ depth: 0, oldestAgeMs: null });
  });
});
