import { describe, expect, it } from "vitest";
import { RtDbClient, type WebSocketLike } from "../src/client.js";
import type { RtQuery } from "../src/query.js";

/** A controllable fake socket. Records sent frames; lets the test drive open/message/close. */
class FakeSocket implements WebSocketLike {
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: { code: number; reason: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  readonly sent: string[] = [];
  closed = false;

  send(data: string): void {
    this.sent.push(data);
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

/** Like FakeSocket, but rejects close codes a real WebSocket rejects (only 1000 or 3000–4999). */
class StrictFakeSocket extends FakeSocket {
  override close(code = 1000, reason = ""): void {
    if (code !== 1000 && (code < 3000 || code > 4999)) {
      throw new Error(`InvalidAccessError: close code ${code} is not allowed`);
    }
    super.close(code, reason);
  }
}

interface Harness {
  client: RtDbClient;
  sockets: FakeSocket[];
  runTimers: () => void;
}

function newClient(
  overrides: { heartbeatMs?: number; SocketClass?: typeof FakeSocket } = {},
): Harness & {
  setNow: (t: number) => void;
} {
  const sockets: FakeSocket[] = [];
  const timers: Array<() => void> = [];
  const clock = { t: 0 };
  const SocketClass = overrides.SocketClass ?? FakeSocket;
  const client = new RtDbClient({
    url: "ws://h:8300",
    db: "kanban",
    getToken: () => "tok",
    webSocketFactory: () => {
      const s = new SocketClass();
      sockets.push(s);
      return s;
    },
    heartbeatMs: overrides.heartbeatMs ?? 0, // disabled by default; only reconnect timers are queued
    now: () => clock.t,
    random: () => 0.5,
    setTimeoutImpl: (fn) => {
      timers.push(fn);
      return timers.length as unknown as ReturnType<typeof setTimeout>;
    },
    clearTimeoutImpl: () => {},
  });
  // Fires every currently-queued timer once (timers scheduled during the run are
  // left for the next call), so a test drives reconnect/heartbeat deterministically.
  const runTimers = () => {
    for (const fn of timers.splice(0)) {
      fn();
    }
  };
  return { client, sockets, runTimers, setNow: (t) => (clock.t = t) };
}

const frames = (s: FakeSocket) => s.sentParsed as Array<{ type: string; [k: string]: unknown }>;
const typeCount = (s: FakeSocket, type: string) => frames(s).filter((m) => m.type === type).length;

describe("RtDbClient", () => {
  it("sends auth on connect with the token field (not sessionToken)", () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    expect(sockets[0].sentParsed[0]).toEqual({ type: "auth", token: "tok", db: "kanban" });
  });

  it("subscribes after authOk and delivers queryUpdate to the listener", () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user", email: "a@b.com" } });

    const updates: unknown[] = [];
    const q: RtQuery<unknown> = { json: { table: "items" } };
    client.subscribe(q, (v) => updates.push(v));

    const subFrame = frames(sockets[0]).find((m) => m.type === "subscribe") as unknown as {
      queryId: string;
      query: unknown;
    };
    expect(subFrame.query).toEqual({ table: "items" });

    sockets[0].deliver({ type: "queryUpdate", queryId: subFrame.queryId, result: [{ _id: "x" }] });
    expect(updates).toEqual([[{ _id: "x" }]]);
  });

  it("resolves mutate on mutateOk and rejects on mutateErr", async () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    const okPromise = client.mutate({ steps: [{ op: "insert", table: "items", doc: {} }] });
    const okFrame = frames(sockets[0]).find((m) => m.type === "mutate") as unknown as {
      mutId: string;
    };
    sockets[0].deliver({ type: "mutateOk", mutId: okFrame.mutId, results: ["id-1"] });
    await expect(okPromise).resolves.toEqual(["id-1"]);

    const errPromise = client.mutate({ steps: [] });
    const errFrame = frames(sockets[0])
      .filter((m) => m.type === "mutate")
      .at(-1) as unknown as {
      mutId: string;
    };
    sockets[0].deliver({
      type: "mutateErr",
      mutId: errFrame.mutId,
      error: { code: "PRECONDITION_FAILED", message: "stale" },
    });
    await expect(errPromise).rejects.toMatchObject({
      name: "RtDbError",
      code: "PRECONDITION_FAILED",
    });
  });

  it("resubscribes all active queries after a reconnect", () => {
    const { client, sockets, runTimers } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    client.subscribe({ json: { table: "items" } }, () => {});
    client.subscribe({ json: { table: "projects" } }, () => {});

    // drop the connection; the client schedules a reconnect. Fire it, then a new
    // socket opens and, after auth, resubscribes both queries.
    sockets[0].close(1006, "gone");
    runTimers();
    expect(sockets.length).toBe(2);
    sockets[1].open();
    sockets[1].deliver({ type: "authOk", user: { kind: "user" } });

    const subscribed = frames(sockets[1])
      .filter((m) => m.type === "subscribe")
      .map((m) => (m as unknown as { query: { table: string } }).query.table)
      .sort();
    expect(subscribed).toEqual(["items", "projects"]);
  });

  it("shares one subscription across identical queries and unsubscribes when the last listener leaves", () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    const q: RtQuery<unknown> = { json: { table: "items" } };
    const off1 = client.subscribe(q, () => {});
    const off2 = client.subscribe(q, () => {});
    expect(typeCount(sockets[0], "subscribe")).toBe(1);

    off1();
    expect(typeCount(sockets[0], "unsubscribe")).toBe(0);
    off2();
    expect(typeCount(sockets[0], "unsubscribe")).toBe(1);
  });

  it("does not reconnect on a 4401 auth-failure close, even after timers fire", () => {
    const { client, sockets, runTimers } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    sockets[0].close(4401, "unauthorized");
    runTimers(); // no reconnect should have been scheduled
    expect(sockets.length).toBe(1);
    expect(client.getAuthState()).toBe("unauthenticated");
  });

  it("rejects in-flight mutations on close() and rejects mutate() issued after close()", async () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    const inFlight = client.mutate({ steps: [{ op: "insert", table: "items", doc: {} }] });
    client.close();
    await expect(inFlight).rejects.toMatchObject({ name: "RtDbError", code: "INTERNAL" });
    await expect(client.mutate({ steps: [] })).rejects.toMatchObject({ code: "INTERNAL" });
  });

  it("can reconnect via an explicit connect() after close()", () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    client.close();

    client.connect();
    expect(sockets.length).toBe(2);
    sockets[1].open();
    sockets[1].deliver({ type: "authOk", user: { kind: "user" } });
    expect(client.getAuthState()).toBe("authenticated");
  });

  it("setToken() cancels a pending reconnect so no duplicate socket opens", () => {
    const { client, sockets, runTimers } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    sockets[0].close(1006, "gone"); // schedules a reconnect timer
    client.setToken("tok2"); // opens socket[1] and must invalidate the pending reconnect
    runTimers(); // the stale reconnect must NOT open a third socket
    expect(sockets.length).toBe(2);
  });

  it("drops a subscribeErr'd query from both maps so it is not resent on reconnect", () => {
    const { client, sockets, runTimers } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    client.subscribe({ json: { table: "items", index: "nope", eq: [] } }, () => {});

    const badFrame = frames(sockets[0]).find((m) => m.type === "subscribe") as unknown as {
      queryId: string;
    };
    sockets[0].deliver({
      type: "subscribeErr",
      queryId: badFrame.queryId,
      error: { code: "BAD_REQUEST", message: "unknown index" },
    });

    sockets[0].close(1006, "gone");
    runTimers();
    sockets[1].open();
    sockets[1].deliver({ type: "authOk", user: { kind: "user" } });
    expect(typeCount(sockets[1], "subscribe")).toBe(0);
  });

  it("heartbeat timeout force-closes with a valid code and reconnects (no InvalidAccessError)", () => {
    const { client, sockets, runTimers, setNow } = newClient({
      heartbeatMs: 1000,
      SocketClass: StrictFakeSocket,
    });
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } }); // starts the heartbeat timer

    setNow(2000); // two intervals with no pong
    runTimers(); // beat() must force-close with a spec-valid code (StrictFakeSocket throws otherwise)
    runTimers(); // fire the reconnect that the close scheduled
    expect(sockets.length).toBe(2);
  });

  it("setToken(null) signs out: connects with no token and lands unauthenticated", () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    expect(client.getAuthState()).toBe("authenticated");

    client.setToken(null);
    sockets[1].open(); // onopen sees a null token -> unauthenticated, no auth frame
    expect(client.getAuthState()).toBe("unauthenticated");
    expect(typeCount(sockets[1], "auth")).toBe(0);
  });
});
