import { describe, expect, it } from "vitest";
import { type ConnectionState, RtDbClient, type WebSocketLike } from "../src/client.js";
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
    sockets[0].deliver({ type: "mutateOk", mutId: okFrame.mutId, results: [{ id: "id-1" }] });
    await expect(okPromise).resolves.toEqual([{ id: "id-1" }]);

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

  it("resolves schedule({id}) on scheduleOk and rejects on scheduleErr", async () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    const okPromise = client.schedule(
      { steps: [{ op: "insert", table: "items", doc: {} }] },
      { type: "afterMs", ms: 1000 },
    );
    const okFrame = frames(sockets[0]).find((m) => m.type === "schedule") as unknown as {
      scheduleId: string;
      when: unknown;
      txn: unknown;
    };
    expect(okFrame.when).toEqual({ type: "afterMs", ms: 1000 });
    expect(okFrame.scheduleId).toMatch(/^sch-\d+$/);
    sockets[0].deliver({ type: "scheduleOk", scheduleId: okFrame.scheduleId, id: "job-1" });
    await expect(okPromise).resolves.toEqual({ id: "job-1" });

    const errPromise = client.schedule(
      { steps: [{ op: "insert", table: "items", doc: {} }] },
      { type: "cron", expr: "not-valid" },
    );
    const errFrame = frames(sockets[0])
      .filter((m) => m.type === "schedule")
      .at(-1) as unknown as { scheduleId: string };
    sockets[0].deliver({
      type: "scheduleErr",
      scheduleId: errFrame.scheduleId,
      error: { code: "BAD_REQUEST", message: "bad cron" },
    });
    await expect(errPromise).rejects.toMatchObject({
      name: "RtDbError",
      code: "BAD_REQUEST",
    });
  });

  it("scheduleAck resolves true on ok:true, false on a bare ok:false no-op, rejects on error", async () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    const okPromise = client.cancelSchedule("job-1");
    const okFrame = frames(sockets[0]).find((m) => m.type === "cancelSchedule") as unknown as {
      scheduleId: string;
      id: string;
    };
    expect(okFrame.id).toBe("job-1");
    sockets[0].deliver({ type: "scheduleAck", scheduleId: okFrame.scheduleId, ok: true });
    await expect(okPromise).resolves.toBe(true);

    const errPromise = client.pauseSchedule("job-missing");
    const errFrame = frames(sockets[0]).find((m) => m.type === "pauseSchedule") as unknown as {
      scheduleId: string;
    };
    sockets[0].deliver({
      type: "scheduleAck",
      scheduleId: errFrame.scheduleId,
      ok: false,
      error: { code: "NOT_FOUND", message: "no such schedule" },
    });
    await expect(errPromise).rejects.toMatchObject({ name: "RtDbError", code: "NOT_FOUND" });

    // Bare ok:false (no error envelope) = unknown/already-terminal job: the
    // server's documented no-op ack, so the promise resolves false.
    const noopPromise = client.resumeSchedule("job-done");
    const noopFrame = frames(sockets[0]).find((m) => m.type === "resumeSchedule") as unknown as {
      scheduleId: string;
    };
    sockets[0].deliver({ type: "scheduleAck", scheduleId: noopFrame.scheduleId, ok: false });
    await expect(noopPromise).resolves.toBe(false);
  });

  it("resolves listSchedules with the schedules array on listSchedulesOk", async () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    const listPromise = client.listSchedules();
    const frame = frames(sockets[0]).find((m) => m.type === "listSchedules") as unknown as {
      scheduleId: string;
    };
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
    sockets[0].deliver({ type: "listSchedulesOk", scheduleId: frame.scheduleId, schedules });
    await expect(listPromise).resolves.toEqual(schedules);
  });

  it("rejects in-flight schedules on close() and schedule() issued after close()", async () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    const inFlight = client.schedule(
      { steps: [{ op: "insert", table: "items", doc: {} }] },
      { type: "afterMs", ms: 1000 },
    );
    client.close();
    await expect(inFlight).rejects.toMatchObject({ name: "RtDbError", code: "INTERNAL" });
    await expect(
      client.schedule({ steps: [] }, { type: "afterMs", ms: 1000 }),
    ).rejects.toMatchObject({ code: "INTERNAL" });
  });

  it("sends opts.mutId as idempotencyKey, keeping mutId as the internal correlation id", () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    client.mutate(
      { steps: [{ op: "insert", table: "items", doc: {} }] },
      { mutId: "caller-key-1" },
    );
    const frame = frames(sockets[0]).find((m) => m.type === "mutate") as unknown as {
      mutId: string;
      idempotencyKey?: string;
    };
    expect(frame.idempotencyKey).toBe("caller-key-1");
    expect(frame.mutId).toMatch(/^mut-\d+$/);
  });

  it("sends opts.idempotencyKey on the wire (preferred alias for mutId)", () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    client.mutate(
      { steps: [{ op: "insert", table: "items", doc: {} }] },
      { idempotencyKey: "caller-key-2" },
    );
    const frame = frames(sockets[0]).find((m) => m.type === "mutate") as unknown as {
      mutId: string;
      idempotencyKey?: string;
    };
    expect(frame.idempotencyKey).toBe("caller-key-2");
    // mutId stays the internal reply-correlation id (mut-<n>), unaffected.
    expect(frame.mutId).toMatch(/^mut-\d+$/);
  });

  it("prefers opts.idempotencyKey over opts.mutId when both are supplied", () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    client.mutate(
      { steps: [{ op: "insert", table: "items", doc: {} }] },
      { idempotencyKey: "preferred", mutId: "alias" },
    );
    const frame = frames(sockets[0]).find((m) => m.type === "mutate") as unknown as {
      idempotencyKey?: string;
    };
    expect(frame.idempotencyKey).toBe("preferred");
  });

  it("omits idempotencyKey from the wire frame when opts.mutId is not provided", () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });

    client.mutate({ steps: [{ op: "insert", table: "items", doc: {} }] });
    const frame = frames(sockets[0]).find((m) => m.type === "mutate") as unknown as {
      idempotencyKey?: string;
    };
    expect(frame).not.toHaveProperty("idempotencyKey");
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

  it("revives via an explicit connect() after a 4401 auth-failure", () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    sockets[0].close(4401, "unauthorized");
    expect(client.getAuthState()).toBe("unauthenticated");

    // connState was left "idle" (not wedged at "connected"), so connect() reopens.
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

  it("setToken(null) signs out without dialing a socket, landing unauthenticated", () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    expect(client.getAuthState()).toBe("authenticated");

    client.setToken(null);
    // A null token must NOT dial a new socket (that would spin the reconnect loop).
    expect(sockets.length).toBe(1);
    expect(client.getAuthState()).toBe("unauthenticated");
  });

  it("treats a rejected getToken() as no credential instead of hanging in connecting", async () => {
    const sockets: FakeSocket[] = [];
    const client = new RtDbClient({
      url: "ws://h:8300",
      db: "kanban",
      getToken: () => Promise.reject(new Error("token fetch failed")),
      webSocketFactory: () => {
        const s = new FakeSocket();
        sockets.push(s);
        return s;
      },
      heartbeatMs: 0,
      setTimeoutImpl: () => 0 as unknown as ReturnType<typeof setTimeout>,
      clearTimeoutImpl: () => {},
    });
    client.connect();
    await Promise.resolve();
    await Promise.resolve();
    expect(sockets.length).toBe(0); // never dialed
    expect(client.getAuthState()).toBe("unauthenticated");
  });
});

describe("connection state observation", () => {
  it("getConnectionState() is idle before connect", () => {
    const { client } = newClient();
    expect(client.getConnectionState()).toBe("idle");
  });

  it("onConnectionChange fires connecting then connected through the auth flow", () => {
    const { client, sockets } = newClient();
    const seen: ConnectionState[] = [];
    client.onConnectionChange((s) => seen.push(s));
    client.connect();
    expect(client.getConnectionState()).toBe("connecting");
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });
    expect(client.getConnectionState()).toBe("connected");
    expect(seen).toEqual(["connecting", "connected"]);
  });

  it("onConnectionChange returns an unsubscribe that stops further callbacks", () => {
    const { client, sockets } = newClient();
    const seen: ConnectionState[] = [];
    const off = client.onConnectionChange((s) => seen.push(s));
    client.connect(); // → connecting
    off();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } }); // → connected
    expect(seen).toEqual(["connecting"]);
  });

  it("does not fire onConnectionChange for a no-op (idempotent connect)", () => {
    const { client } = newClient();
    const seen: ConnectionState[] = [];
    client.onConnectionChange((s) => seen.push(s));
    client.connect(); // → connecting
    client.connect(); // already connecting → no-op
    expect(seen).toEqual(["connecting"]);
  });

  it("close() transitions to closed and fires onConnectionChange", () => {
    const { client, sockets } = newClient();
    const seen: ConnectionState[] = [];
    client.onConnectionChange((s) => seen.push(s));
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine" } });
    client.close();
    expect(client.getConnectionState()).toBe("closed");
    expect(seen[seen.length - 1]).toBe("closed");
  });

  it("a 4401 auth failure transitions to idle", () => {
    const { client, sockets } = newClient();
    const seen: ConnectionState[] = [];
    client.onConnectionChange((s) => seen.push(s));
    client.connect();
    sockets[0].open();
    sockets[0].deliver({
      type: "authErr",
      error: { code: "UNAUTHENTICATED", message: "no" },
    });
    expect(client.getConnectionState()).toBe("idle");
    expect(seen[seen.length - 1]).toBe("idle");
  });
});

describe("RtDbClient auth-unreachable signal", () => {
  /** Cookie-mode harness (no getToken) — the mode a non-allowlisted origin
   * breaks: the WS upgrade is 403'd, which the browser surfaces only as a
   * close(1006) with no authOk/authErr ever arriving. */
  function newCookieClient(overrides: { authUnreachableAfterAttempts?: number } = {}) {
    const sockets: FakeSocket[] = [];
    const timers: Array<() => void> = [];
    const client = new RtDbClient({
      url: "ws://h:8300",
      db: "app",
      webSocketFactory: () => {
        const s = new FakeSocket();
        sockets.push(s);
        return s;
      },
      heartbeatMs: 0,
      now: () => 0,
      random: () => 0.5,
      setTimeoutImpl: (fn) => {
        timers.push(fn);
        return timers.length as unknown as ReturnType<typeof setTimeout>;
      },
      clearTimeoutImpl: () => {},
      ...overrides,
    });
    const runTimers = () => {
      for (const fn of timers.splice(0)) {
        fn();
      }
    };
    return { client, sockets, runTimers };
  }

  it("reaches 'unreachable' after N consecutive pre-auth closes, and keeps retrying", () => {
    const { client, sockets, runTimers } = newCookieClient();
    client.connect();
    // Five rejected upgrades: close 1006 before any handshake completes.
    for (let i = 0; i < 5; i++) {
      sockets[sockets.length - 1].close(1006, "");
      expect(client.getAuthState()).toBe(i < 4 ? "authenticating" : "unreachable");
      runTimers(); // the reconnect timer dials the next socket
    }
    expect(sockets.length).toBe(6); // retries continue in "unreachable"

    // A later successful handshake clears the signal.
    sockets[sockets.length - 1].open();
    sockets[sockets.length - 1].deliver({
      type: "authOk",
      user: { kind: "machine", email: null, name: null },
    });
    expect(client.getAuthState()).toBe("authenticated");
  });

  it("a post-auth drop never counts toward unreachable", () => {
    const { client, sockets, runTimers } = newCookieClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "machine", email: null, name: null } });
    // Blips after a completed handshake reset the counter — as long as each
    // re-dial completes its handshake, the signal never fires.
    for (let i = 0; i < 6; i++) {
      sockets[sockets.length - 1].close(1006, "");
      expect(client.getAuthState()).toBe("authenticating");
      runTimers();
      sockets[sockets.length - 1].open();
      sockets[sockets.length - 1].deliver({
        type: "authOk",
        user: { kind: "machine", email: null, name: null },
      });
      expect(client.getAuthState()).toBe("authenticated");
    }
  });

  it("4401 stays 'unauthenticated' and resets the pre-auth counter", () => {
    const { client, sockets, runTimers } = newCookieClient();
    client.connect();
    for (let i = 0; i < 4; i++) {
      sockets[sockets.length - 1].close(1006, "");
      runTimers();
    }
    sockets[sockets.length - 1].open();
    sockets[sockets.length - 1].deliver({
      type: "authErr",
      error: { code: "UNAUTHENTICATED", message: "revoked" },
    });
    expect(client.getAuthState()).toBe("unauthenticated");
    // The next four pre-auth closes do not trip the signal (counter reset).
    for (let i = 0; i < 4; i++) {
      sockets[sockets.length - 1].close(1006, "");
      expect(client.getAuthState()).toBe("authenticating");
      runTimers();
    }
  });

  it("an explicit connect() after 'unreachable' starts a fresh attempt", () => {
    const { client, sockets, runTimers } = newCookieClient();
    client.connect();
    for (let i = 0; i < 5; i++) {
      sockets[sockets.length - 1].close(1006, "");
      runTimers();
    }
    expect(client.getAuthState()).toBe("unreachable");
    client.connect();
    expect(client.getAuthState()).toBe("authenticating");
    sockets[sockets.length - 1].close(1006, "");
    expect(client.getAuthState()).toBe("authenticating"); // failure 1 of a fresh N
  });

  it("authUnreachableAfterAttempts: 0 disables the signal", () => {
    const { client, sockets, runTimers } = newCookieClient({
      authUnreachableAfterAttempts: 0,
    });
    client.connect();
    for (let i = 0; i < 8; i++) {
      sockets[sockets.length - 1].close(1006, "");
      expect(client.getAuthState()).toBe("authenticating");
      runTimers();
    }
  });
});
