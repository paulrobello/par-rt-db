/**
 * WS change-feed push channel: wire shapes, resume-on-reconnect, dedupe,
 * table filter, and terminal `feedErr` handling.
 */
import { describe, expect, it } from "vitest";
import { RtDbClient, type WebSocketLike } from "../src/client.js";
import { RtDbError } from "../src/errors.js";
import type { ChangeOp, ClientMessage, ServerMessage } from "../src/protocol.js";

class FakeSocket implements WebSocketLike {
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: { code: number; reason: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  readonly sent: string[] = [];
  send(data: string): void {
    this.sent.push(data);
  }
  close(code = 1000, reason = ""): void {
    this.onclose?.({ code, reason });
  }
  deliver(msg: ServerMessage): void {
    this.onmessage?.({ data: JSON.stringify(msg) });
  }
  parsed(): ClientMessage[] {
    return this.sent.map((s) => JSON.parse(s) as ClientMessage);
  }
}

const AUTH_OK = { type: "authOk", user: { id: "u", kind: "machine" } } as unknown as ServerMessage;

function setup() {
  const sockets: FakeSocket[] = [];
  const timers: Array<() => void> = [];
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
    setTimeoutImpl: (fn) => {
      timers.push(fn);
      return timers.length as unknown as ReturnType<typeof setTimeout>;
    },
    clearTimeoutImpl: () => {},
  });
  const connect = async (): Promise<FakeSocket> => {
    client.connect();
    await Promise.resolve();
    await Promise.resolve();
    const s = sockets[sockets.length - 1] as FakeSocket;
    s.onopen?.();
    s.deliver(AUTH_OK);
    return s;
  };
  const runTimers = () => {
    for (const fn of timers.splice(0)) {
      fn();
    }
  };
  return { client, sockets, connect, runTimers };
}

const op = (seq: number, table = "tasks"): ChangeOp => ({
  seq,
  table,
  docId: `d${seq}`,
  kind: "insert",
  doc: { n: seq },
  ts: 1000 + seq,
});
const EPOCH = "0a1b2c3d4e5f6071";
const events = (ops: ChangeOp[], head: number, feedId = "feed-1"): ServerMessage => ({
  type: "feedEvents",
  feedId,
  feedEpoch: EPOCH,
  ops,
  head,
});
const feedFrames = (s: FakeSocket) => s.parsed().filter((m) => m.type.startsWith("feed"));

describe("subscribeChanges", () => {
  it("sends feedSubscribe with the table filter and omits unset fields", async () => {
    const { client, connect } = setup();
    const s = await connect();
    client.subscribeChanges({ tables: ["tasks", "boards"] }, () => {});
    expect(feedFrames(s)).toEqual([
      { type: "feedSubscribe", feedId: "feed-1", tables: ["tasks", "boards"] },
    ]);
    client.subscribeChanges({}, () => {});
    expect(feedFrames(s)[1]).toEqual({ type: "feedSubscribe", feedId: "feed-2" });
  });

  it("delivers ops in order, acks the max seq, and skips the empty confirm frame", async () => {
    const { client, connect } = setup();
    const s = await connect();
    const batches: number[][] = [];
    client.subscribeChanges({ since: 0 }, (ops) => batches.push(ops.map((o) => o.seq)));
    s.deliver(events([], 4));
    s.deliver(events([op(1), op(2), op(3)], 4));
    expect(batches).toEqual([[1, 2, 3]]);
    expect(feedFrames(s).slice(-1)).toEqual([{ type: "feedAck", feedId: "feed-1", seq: 3 }]);
  });

  it("resumes with the last processed seq and epoch after a reconnect, and dedupes replays", async () => {
    const { client, sockets, connect, runTimers } = setup();
    const s1 = await connect();
    const seen: number[] = [];
    client.subscribeChanges({ tables: ["tasks"] }, (ops) => seen.push(...ops.map((o) => o.seq)));
    s1.deliver(events([], 5));
    s1.deliver(events([op(6), op(7)], 7));
    s1.close(1006, "drop");
    runTimers();
    await Promise.resolve();
    await Promise.resolve();
    const s2 = sockets[sockets.length - 1] as FakeSocket;
    expect(s2).not.toBe(s1);
    s2.onopen?.();
    s2.deliver(AUTH_OK);
    expect(feedFrames(s2)).toEqual([
      { type: "feedSubscribe", feedId: "feed-1", since: 7, feedEpoch: EPOCH, tables: ["tasks"] },
    ]);
    // Server replays 7 (at-least-once) plus new op 8.
    s2.deliver(events([op(7), op(8)], 8));
    expect(seen).toEqual([6, 7, 8]);
    // A fully replayed batch is not delivered but is still acked.
    s2.deliver(events([op(8)], 8));
    expect(seen).toEqual([6, 7, 8]);
    expect(feedFrames(s2).slice(-1)).toEqual([{ type: "feedAck", feedId: "feed-1", seq: 8 }]);
  });

  it("pins the cursor to head when started without since, so a reconnect does not skip ops", async () => {
    const { client, sockets, connect, runTimers } = setup();
    const s1 = await connect();
    client.subscribeChanges({}, () => {});
    s1.deliver(events([], 41));
    s1.close(1006, "drop");
    runTimers();
    await Promise.resolve();
    await Promise.resolve();
    const s2 = sockets[sockets.length - 1] as FakeSocket;
    s2.onopen?.();
    s2.deliver(AUTH_OK);
    expect(feedFrames(s2)[0]).toMatchObject({ since: 41, feedEpoch: EPOCH });
  });

  it("surfaces CURSOR_EXPIRED to onError and does not re-subscribe on reconnect", async () => {
    const { client, sockets, connect, runTimers } = setup();
    const s1 = await connect();
    const errors: RtDbError[] = [];
    client.subscribeChanges(
      { since: 3, feedEpoch: "old" },
      () => {},
      (e) => errors.push(e),
    );
    s1.deliver({
      type: "feedErr",
      feedId: "feed-1",
      error: { code: "CURSOR_EXPIRED", message: "resync" },
    });
    expect(errors).toHaveLength(1);
    expect(errors[0]).toBeInstanceOf(RtDbError);
    expect(errors[0]?.code).toBe("CURSOR_EXPIRED");
    s1.close(1006, "drop");
    runTimers();
    await Promise.resolve();
    await Promise.resolve();
    const s2 = sockets[sockets.length - 1] as FakeSocket;
    s2.onopen?.();
    s2.deliver(AUTH_OK);
    expect(feedFrames(s2)).toEqual([]);
  });

  it("unsubscribe sends feedUnsubscribe and stops delivery", async () => {
    const { client, connect } = setup();
    const s = await connect();
    const seen: number[] = [];
    const off = client.subscribeChanges({}, (ops) => seen.push(...ops.map((o) => o.seq)));
    off();
    expect(feedFrames(s).slice(-1)).toEqual([{ type: "feedUnsubscribe", feedId: "feed-1" }]);
    s.deliver(events([op(1)], 1));
    expect(seen).toEqual([]);
  });

  it("changes() yields batches and rejects on feedErr", async () => {
    const { client, connect } = setup();
    const s = await connect();
    const it = client.changes({ since: 0 });
    s.deliver(events([op(1)], 1));
    const first = await it.next();
    expect(first.value?.ops.map((o: ChangeOp) => o.seq)).toEqual([1]);
    s.deliver({
      type: "feedErr",
      feedId: "feed-1",
      error: { code: "CURSOR_EXPIRED", message: "x" },
    });
    await expect(it.next()).rejects.toMatchObject({ code: "CURSOR_EXPIRED" });
  });
});
