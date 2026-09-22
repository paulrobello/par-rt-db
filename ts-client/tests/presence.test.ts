/**
 * Presence (ENH-015) — wire-type round-trip + reactive client frame routing.
 *
 * The wire shapes here are the four-implementations-identical contract pinned by
 * `server/src/protocol.rs`:
 *   - client -> server: `presence` / `presenceState` / `leavePresence`
 *   - server -> client: `presenceSnapshot` / `presenceErr`
 *   - `PresenceMember = { connectionId: string, user: AuthedUser, state: unknown }`
 *
 * CamelCase tags and field names are load-bearing — the asserts below pin them
 * against `JSON.stringify` output so a renamed field or tag drifts loudly. The
 * `satisfies ClientMessage` / `satisfies ServerMessage` clauses make a missing
 * or mis-typed variant a compile-time error, so this test fails to typecheck
 * until `protocol.ts` defines the variants.
 *
 * The reactive block exercises the per-room callback wiring: `presence()`
 * sends the wire frame and registers a per-room handler; inbound
 * `presenceSnapshot` fans out to that handler; `leavePresence()` drops both
 * the wire membership and the local listener (a subsequent snapshot is a
 * no-op, never reaches the handler). Mirrors how `subscribe`/`queryUpdate`
 * are tested.
 */
import { describe, expect, it } from "vitest";
import { RtDbClient, type WebSocketLike } from "../src/client.js";
import type { ClientMessage, PresenceMember, ServerMessage } from "../src/protocol.js";

/** Minimal fake socket: records sent frames, lets a test drive onopen/onmessage. */
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
  open(): void {
    this.onopen?.();
  }
  deliver(msg: unknown): void {
    this.onmessage?.({ data: JSON.stringify(msg) });
  }
  parsed(): unknown[] {
    return this.sent.map((s) => JSON.parse(s));
  }
}

function setupClient() {
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
  const runTimers = () => {
    for (const fn of timers.splice(0)) {
      fn();
    }
  };
  return { client, sockets, runTimers };
}

describe("presence wire types", () => {
  it("presence frame omits state when absent", () => {
    const f = { type: "presence", room: "doc:1" } satisfies ClientMessage;
    expect(JSON.parse(JSON.stringify(f))).toEqual({ type: "presence", room: "doc:1" });
  });

  it("presence frame includes state when present", () => {
    const f = { type: "presence", room: "doc:1", state: { x: 3, y: 4 } } satisfies ClientMessage;
    expect(JSON.parse(JSON.stringify(f))).toEqual({
      type: "presence",
      room: "doc:1",
      state: { x: 3, y: 4 },
    });
  });

  it("presenceState carries room + state", () => {
    const f = {
      type: "presenceState",
      room: "doc:1",
      state: { typing: true },
    } satisfies ClientMessage;
    expect(JSON.parse(JSON.stringify(f))).toEqual({
      type: "presenceState",
      room: "doc:1",
      state: { typing: true },
    });
    // ttlMs is optional: omitting it must keep the field absent on the wire.
    expect("ttlMs" in JSON.parse(JSON.stringify(f))).toBe(false);
  });

  it("presenceState carries optional ttlMs when set", () => {
    const f = {
      type: "presenceState",
      room: "doc:1",
      state: { typing: true },
      ttlMs: 3000,
    } satisfies ClientMessage;
    expect(JSON.parse(JSON.stringify(f))).toEqual({
      type: "presenceState",
      room: "doc:1",
      state: { typing: true },
      ttlMs: 3000,
    });
  });

  it("leavePresence carries only room", () => {
    const f = { type: "leavePresence", room: "doc:1" } satisfies ClientMessage;
    expect(JSON.parse(JSON.stringify(f))).toEqual({ type: "leavePresence", room: "doc:1" });
  });

  it("presenceSnapshot carries camelCase members", () => {
    const member: PresenceMember = {
      connectionId: "1",
      user: { kind: "user", email: "a@b.com" },
      state: { x: 1 },
    };
    const m = {
      type: "presenceSnapshot",
      room: "doc:1",
      members: [member],
    } satisfies ServerMessage;
    const round = JSON.parse(JSON.stringify(m)) as {
      type: string;
      members: PresenceMember[];
    };
    expect(round.type).toBe("presenceSnapshot");
    expect(round.members[0].connectionId).toBe("1");
    expect(round.members[0].state).toEqual({ x: 1 });
  });

  it("presenceErr carries room + error envelope", () => {
    const m = {
      type: "presenceErr",
      room: "doc:1",
      error: { code: "FORBIDDEN", message: "presence not enabled" },
    } satisfies ServerMessage;
    expect(JSON.parse(JSON.stringify(m))).toEqual({
      type: "presenceErr",
      room: "doc:1",
      error: { code: "FORBIDDEN", message: "presence not enabled" },
    });
  });
});

describe("RtDbClient presence", () => {
  it("sends the presence wire frame and routes the inbound snapshot", () => {
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    const seen: PresenceMember[][] = [];
    client.presence("doc:1", { cursor: { x: 0 } }, (m) => seen.push(m));

    const join = sockets[0]
      .parsed()
      .find((m) => (m as { type: string }).type === "presence") as ClientMessage;
    expect(join).toEqual({ type: "presence", room: "doc:1", state: { cursor: { x: 0 } } });

    const members: PresenceMember[] = [
      { connectionId: "1", user: { kind: "user" }, state: { cursor: { x: 0 } } },
    ];
    sockets[0].deliver({ type: "presenceSnapshot", room: "doc:1", members });
    expect(seen).toEqual([members]);

    // updatePresence sends presenceState; leavePresence sends leavePresence.
    client.updatePresence("doc:1", { cursor: { x: 5 } });
    client.leavePresence("doc:1");
    const updateFrame = sockets[0]
      .parsed()
      .find((m) => (m as { type: string }).type === "presenceState") as ClientMessage;
    const leaveFrame = sockets[0]
      .parsed()
      .find((m) => (m as { type: string }).type === "leavePresence") as ClientMessage;
    expect(updateFrame).toEqual({
      type: "presenceState",
      room: "doc:1",
      state: { cursor: { x: 5 } },
    });
    expect(leaveFrame).toEqual({ type: "leavePresence", room: "doc:1" });
  });

  it("updatePresence forwards ttlMs onto the wire and omits it when unset", () => {
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    client.presence("doc:1", undefined, () => {});

    // With ttlMs: the frame carries ttlMs alongside room + state.
    client.updatePresence("doc:1", { typing: true }, 3000);
    const withTtl = sockets[0]
      .parsed()
      .filter((m) => (m as { type: string }).type === "presenceState")
      .at(-1) as ClientMessage;
    expect(withTtl).toEqual({
      type: "presenceState",
      room: "doc:1",
      state: { typing: true },
      ttlMs: 3000,
    });

    // Without ttlMs: the field must be absent on the wire (optional, not `undefined`).
    client.updatePresence("doc:1", { typing: false });
    const withoutTtl = sockets[0]
      .parsed()
      .filter((m) => (m as { type: string }).type === "presenceState")
      .at(-1) as ClientMessage;
    expect(withoutTtl).toEqual({
      type: "presenceState",
      room: "doc:1",
      state: { typing: false },
    });
    expect("ttlMs" in withoutTtl).toBe(false);
  });

  it("unsub stops the snapshot callback; leavePresence drops the room entirely", () => {
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    const seen: PresenceMember[][] = [];
    const off = client.presence("doc:1", undefined, (m) => seen.push(m));

    // off() only removes the local listener — it does NOT send leavePresence.
    off();
    const leaveBefore = sockets[0]
      .parsed()
      .filter((m) => (m as { type: string }).type === "leavePresence").length;
    expect(leaveBefore).toBe(0);

    sockets[0].deliver({
      type: "presenceSnapshot",
      room: "doc:1",
      members: [{ connectionId: "x", user: { kind: "user" }, state: null }],
    });
    expect(seen).toHaveLength(0); // listener gone: no delivery
  });

  it("presenceErr drops local listeners for the room", () => {
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    const seen: PresenceMember[][] = [];
    client.presence("doc:1", undefined, (m) => seen.push(m));

    sockets[0].deliver({
      type: "presenceErr",
      room: "doc:1",
      error: { code: "FORBIDDEN", message: "presence not enabled" },
    });
    // Subsequent snapshots for that room must not reach the listener: the room
    // is dead to this client until it rejoins.
    sockets[0].deliver({
      type: "presenceSnapshot",
      room: "doc:1",
      members: [{ connectionId: "y", user: { kind: "user" }, state: null }],
    });
    expect(seen).toHaveLength(0);
  });

  it("routes snapshots to the right room (per-room keying)", () => {
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    const doc1: PresenceMember[][] = [];
    const doc2: PresenceMember[][] = [];
    client.presence("doc:1", undefined, (m) => doc1.push(m));
    client.presence("doc:2", undefined, (m) => doc2.push(m));

    sockets[0].deliver({
      type: "presenceSnapshot",
      room: "doc:1",
      members: [{ connectionId: "a", user: { kind: "user" }, state: null }],
    });
    expect(doc1).toHaveLength(1);
    expect(doc2).toHaveLength(0);
  });

  it("presence/updatePresence/leavePresence send NOTHING before authOk (auth gate parity with subscribe)", () => {
    // Regression: a direct caller doing `connect(); presence(...)` would
    // otherwise fire the join frame in the socket-open → authOk window, and
    // flushOnAuth would then replay it → the server sees a duplicate join.
    // All three presence sends must gate on `authState === "authenticated"`,
    // exactly like `subscribe`/`unsubscribe`/`dispatchMutate`/`dispatchSchedule`.
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();
    // STILL unauthenticated — no authOk delivered.

    client.presence("doc:1", { cursor: { x: 1 } });
    client.updatePresence("doc:1", { cursor: { x: 2 } });
    client.leavePresence("doc:1");

    const isPresenceFrame = (m: unknown): boolean =>
      typeof m === "object" &&
      m !== null &&
      ["presence", "presenceState", "leavePresence"].includes((m as { type: string }).type);
    expect(sockets[0].parsed().filter(isPresenceFrame)).toHaveLength(0);

    // Now authenticate: joinedRooms is empty (leavePresence cleared it), so
    // flushOnAuth replays nothing.
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    expect(sockets[0].parsed().filter(isPresenceFrame)).toHaveLength(0);
  });

  it("pre-auth presence() buffers exactly one join, replayed on authOk (no duplicate)", () => {
    // The fix's positive case: `presence()` called before auth buffers; on
    // authOk exactly ONE presence frame is sent (the replay). A direct caller
    // never sees a duplicate.
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();

    client.presence("doc:1", { cursor: { x: 0 } });
    const presenceFrames = (): unknown[] =>
      sockets[0].parsed().filter((m) => (m as { type: string }).type === "presence");

    expect(presenceFrames()).toHaveLength(0); // pre-auth: buffered, not sent

    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    expect(presenceFrames()).toEqual([
      { type: "presence", room: "doc:1", state: { cursor: { x: 0 } } },
    ]);
    expect(presenceFrames()).toHaveLength(1); // exactly one — not two
  });

  it("pre-auth leavePresence cancels a buffered join (no replay on authOk)", () => {
    // Caller joins then changes their mind before auth: joinedRooms is cleared,
    // so flushOnAuth does NOT replay the join. Local-state bookkeeping runs
    // regardless of the auth gate.
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();

    client.presence("doc:1", { cursor: { x: 0 } });
    client.leavePresence("doc:1");

    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    const presenceFrames = sockets[0]
      .parsed()
      .filter((m) => (m as { type: string }).type === "presence");
    const leaveFrames = sockets[0]
      .parsed()
      .filter((m) => (m as { type: string }).type === "leavePresence");
    expect(presenceFrames).toHaveLength(0); // buffered join cancelled by leave
    expect(leaveFrames).toHaveLength(0); // not authenticated → no wire send
  });

  it("pre-auth updatePresence does not send, but updates the buffered join state for replay", () => {
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();

    client.presence("doc:1", { cursor: { x: 0 } });
    client.updatePresence("doc:1", { cursor: { x: 99 } });

    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    const presenceFrames = sockets[0]
      .parsed()
      .filter((m) => (m as { type: string }).type === "presence");
    // Single replayed join, carrying the latest state — not the original.
    expect(presenceFrames).toEqual([
      { type: "presence", room: "doc:1", state: { cursor: { x: 99 } } },
    ]);
    const stateFrames = sockets[0]
      .parsed()
      .filter((m) => (m as { type: string }).type === "presenceState");
    expect(stateFrames).toHaveLength(0); // pre-auth update did not send
  });

  it("leavePresence is refcounted: only the last detach sends the wire leave", () => {
    // Regression: the server holds one membership per conn+room, so N listeners
    // on one client must produce exactly one wire leave — not one per detach.
    // Two presence() calls → first leavePresence() must NOT send the wire frame
    // or clear joinedRooms; the second one must.
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    client.presence("doc:1", { cursor: { x: 0 } });
    client.presence("doc:1", { cursor: { x: 1 } });

    const leaveFrames = (): unknown[] =>
      sockets[0].parsed().filter((m) => (m as { type: string }).type === "leavePresence");

    client.leavePresence("doc:1"); // first detach — still one listener held
    expect(leaveFrames()).toHaveLength(0);

    client.leavePresence("doc:1"); // last detach — wire leave fires now
    expect(leaveFrames()).toEqual([{ type: "leavePresence", room: "doc:1" }]);

    // A third call on a fully-left room is a no-op (no duplicate wire frame).
    client.leavePresence("doc:1");
    expect(leaveFrames()).toHaveLength(1);
  });
});

describe("RtDbClient presenceDelta (v3 fold)", () => {
  const c1 = { connectionId: "1", user: { kind: "user" as const }, state: null };
  const c2 = { connectionId: "2", user: { kind: "user" as const }, state: null };
  const c3 = { connectionId: "3", user: { kind: "user" as const }, state: null };

  it("presenceDelta carries camelCase fields and tolerates all-empty wire shape", () => {
    const m = {
      type: "presenceDelta",
      room: "doc:1",
      seq: 2,
      joined: [c1],
      left: ["2"],
      stateChanged: [c3],
    } satisfies ServerMessage;
    const round = JSON.parse(JSON.stringify(m)) as { type: string; seq: number };
    expect(round.type).toBe("presenceDelta");
    expect(round.seq).toBe(2);
    // The server omits empty arrays; the all-empty shape is a valid frame.
    const bare = { type: "presenceDelta", room: "r", seq: 1 } satisfies ServerMessage;
    expect(JSON.parse(JSON.stringify(bare))).toEqual({ type: "presenceDelta", room: "r", seq: 1 });
  });

  it("folds a delta into the snapshot: joined upserted, left removed, stateChanged replaced", () => {
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    const seen: PresenceMember[][] = [];
    client.presence("doc:1", undefined, (m) => seen.push(m));

    sockets[0].deliver({
      type: "presenceSnapshot",
      room: "doc:1",
      members: [c1, c2],
    });
    sockets[0].deliver({
      type: "presenceDelta",
      room: "doc:1",
      seq: 2,
      joined: [c3],
      left: ["2"],
      stateChanged: [{ ...c1, state: { cursor: { x: 9 } } }],
    });
    expect(seen).toHaveLength(2);
    expect(seen[1]).toEqual([{ ...c1, state: { cursor: { x: 9 } } }, c3]);
  });

  it("all-empty delta is a no-op fold and does NOT re-notify (push-on-change parity)", () => {
    // Choice: an empty delta changes nothing observable, so listeners keep
    // their previous data — mirroring the server's push-only-on-change
    // contract rather than causing a redundant re-render.
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    const seen: PresenceMember[][] = [];
    client.presence("doc:1", undefined, (m) => seen.push(m));
    sockets[0].deliver({ type: "presenceSnapshot", room: "doc:1", members: [c1] });
    sockets[0].deliver({ type: "presenceDelta", room: "doc:1", seq: 5 });
    expect(seen).toHaveLength(1);

    // The empty delta DID advance the seq baseline: a later contiguous
    // delta still folds (it is not treated as a gap).
    sockets[0].deliver({
      type: "presenceDelta",
      room: "doc:1",
      seq: 6,
      joined: [c2],
    });
    expect(seen).toHaveLength(2);
    expect(seen[1]).toEqual([c1, c2]);
  });

  it("delta for a room with no snapshot baseline yet is ignored (no crash, no fold)", () => {
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    const seen: PresenceMember[][] = [];
    client.presence("doc:1", undefined, (m) => seen.push(m));
    sockets[0].deliver({ type: "presenceDelta", room: "doc:1", seq: 3, joined: [c1] });
    expect(seen).toHaveLength(0);
  });

  it("delta for an unknown room is ignored", () => {
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    const seen: PresenceMember[][] = [];
    client.presence("doc:1", undefined, (m) => seen.push(m));
    sockets[0].deliver({ type: "presenceSnapshot", room: "doc:1", members: [c1] });
    sockets[0].deliver({ type: "presenceDelta", room: "other", seq: 4, joined: [c2] });
    expect(seen).toHaveLength(1);
  });

  it("seq gap discards the fold, re-sends the room join, and a later snapshot resyncs", () => {
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    const seen: PresenceMember[][] = [];
    client.presence("doc:1", { cursor: { x: 0 } }, (m) => seen.push(m));
    sockets[0].deliver({ type: "presenceSnapshot", room: "doc:1", members: [c1] });

    // First delta after the snapshot: accepted from any seq (the room's
    // counter ran while this connection caught up).
    sockets[0].deliver({ type: "presenceDelta", room: "doc:1", seq: 5, joined: [c2] });
    expect(seen).toHaveLength(2);

    // Gap (6 missing): seq 7 must NOT be applied; a fresh join frame goes out.
    sockets[0].deliver({ type: "presenceDelta", room: "doc:1", seq: 7, joined: [c3] });
    expect(seen).toHaveLength(2); // no fold, no notify
    const joins = sockets[0].parsed().filter((m) => (m as { type: string }).type === "presence");
    expect(joins).toEqual([
      { type: "presence", room: "doc:1", state: { cursor: { x: 0 } } }, // initial
      { type: "presence", room: "doc:1", state: { cursor: { x: 0 } } }, // gap re-join
    ]);

    // The re-join forces a full snapshot server-side; it restores a sane list.
    sockets[0].deliver({ type: "presenceSnapshot", room: "doc:1", members: [c1, c3] });
    sockets[0].deliver({ type: "presenceDelta", room: "doc:1", seq: 9, left: ["1"] });
    expect(seen).toHaveLength(4);
    expect(seen[3]).toEqual([c3]);
  });

  it("stale duplicate delta (seq <= lastSeq) is ignored", () => {
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    const seen: PresenceMember[][] = [];
    client.presence("doc:1", undefined, (m) => seen.push(m));
    sockets[0].deliver({ type: "presenceSnapshot", room: "doc:1", members: [c1] });
    sockets[0].deliver({ type: "presenceDelta", room: "doc:1", seq: 6, joined: [c2] });
    sockets[0].deliver({ type: "presenceDelta", room: "doc:1", seq: 6, joined: [c2] });
    expect(seen).toHaveLength(2);
    expect(seen[1]).toEqual([c1, c2]);
  });

  it("reconnect clears fold state: the replayed re-join is followed by a fresh snapshot", () => {
    const setup = setupClient();
    const { client, sockets } = setup;
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    client.presence("doc:1", { cursor: { x: 0 } }, () => {});
    sockets[0].deliver({ type: "presenceSnapshot", room: "doc:1", members: [c1] });
    sockets[0].deliver({ type: "presenceDelta", room: "doc:1", seq: 4, joined: [c2] });

    // Drop + reconnect (non-4401: reconnectable). close fires onclose; the
    // client schedules the reconnect on the queued timer.
    sockets[0].close(1006, "gone");
    const { runTimers } = setup;
    runTimers();
    sockets[1].open();
    sockets[1].deliver({ type: "authOk", user: { kind: "user" } });
    // flushOnAuth replayed the join on the new socket.
    const joins = sockets[1].parsed().filter((m) => (m as { type: string }).type === "presence");
    expect(joins).toEqual([{ type: "presence", room: "doc:1", state: { cursor: { x: 0 } } }]);

    // A delta on the new socket BEFORE its snapshot has no baseline → ignored;
    // the fresh snapshot re-seeds, and a delta folding from ANY seq works.
    const seen: PresenceMember[][] = [];
    // (listener re-registered post-reconnect, mirroring usePresence remount)
    client.presence("doc:1", undefined, (m) => seen.push(m));
    sockets[1].deliver({ type: "presenceDelta", room: "doc:1", seq: 99, joined: [c3] });
    expect(seen).toHaveLength(0);
    sockets[1].deliver({ type: "presenceSnapshot", room: "doc:1", members: [c1] });
    expect(seen).toEqual([[c1]]);
    sockets[1].deliver({ type: "presenceDelta", room: "doc:1", seq: 2, left: ["1"] });
    expect(seen).toEqual([[c1], []]);
  });

  it("leavePresence drops the fold state with the room", () => {
    const { client, sockets } = setupClient();
    client.connect();
    sockets[0].open();
    sockets[0].deliver({ type: "authOk", user: { kind: "user" } });

    const seen: PresenceMember[][] = [];
    client.presence("doc:1", undefined, (m) => seen.push(m));
    sockets[0].deliver({ type: "presenceSnapshot", room: "doc:1", members: [c1] });
    client.leavePresence("doc:1");
    // A late delta after leaving is ignored (no re-fold, no re-join frame).
    sockets[0].deliver({ type: "presenceDelta", room: "doc:1", seq: 7, joined: [c2] });
    const joinsAfterLeave = sockets[0]
      .parsed()
      .filter((m) => (m as { type: string }).type === "presence");
    expect(joinsAfterLeave).toHaveLength(1); // only the original join
    expect(seen).toHaveLength(1);
  });
});
