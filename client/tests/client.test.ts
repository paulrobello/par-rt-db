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

function newClient() {
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
    heartbeatMs: 0, // disable heartbeat in unit tests; only reconnect timers are queued
    now: () => 0,
    random: () => 0.5,
    setTimeoutImpl: (fn) => {
      timers.push(fn);
      return timers.length as unknown as ReturnType<typeof setTimeout>;
    },
    clearTimeoutImpl: () => {},
  });
  // Fires every queued timer (used to drive a scheduled reconnect deterministically).
  const runTimers = () => {
    for (const fn of timers.splice(0)) {
      fn();
    }
  };
  return { client, sockets, runTimers };
}

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

    const subFrame = sockets[0].sentParsed.find(
      (m) => (m as { type: string }).type === "subscribe",
    ) as {
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
    const okFrame = sockets[0].sentParsed.find(
      (m) => (m as { type: string }).type === "mutate",
    ) as {
      mutId: string;
    };
    sockets[0].deliver({ type: "mutateOk", mutId: okFrame.mutId, results: ["id-1"] });
    await expect(okPromise).resolves.toEqual(["id-1"]);

    const errPromise = client.mutate({ steps: [] });
    const errFrame = sockets[0].sentParsed
      .filter((m) => (m as { type: string }).type === "mutate")
      .at(-1) as { mutId: string };
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

    const subscribed = sockets[1].sentParsed
      .filter((m) => (m as { type: string }).type === "subscribe")
      .map((m) => (m as { query: { table: string } }).query.table)
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
    const subCount = sockets[0].sentParsed.filter(
      (m) => (m as { type: string }).type === "subscribe",
    ).length;
    expect(subCount).toBe(1);

    off1();
    expect(sockets[0].sentParsed.some((m) => (m as { type: string }).type === "unsubscribe")).toBe(
      false,
    );
    off2();
    expect(sockets[0].sentParsed.some((m) => (m as { type: string }).type === "unsubscribe")).toBe(
      true,
    );
  });

  it("does not reconnect on a 4401 auth-failure close", () => {
    const { client, sockets } = newClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    sockets[0].close(4401, "unauthorized");
    expect(sockets.length).toBe(1);
    expect(client.getAuthState()).toBe("unauthenticated");
  });
});
