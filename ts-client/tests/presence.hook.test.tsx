/**
 * `usePresence` hook (ENH-015) — React reactivity test.
 *
 * Mirrors `react.test.tsx`'s `useQuery` pattern: a controllable `FakeSocket`
 * drives the auth handshake, then inbound `presenceSnapshot` frames fan out to
 * the hook's state. Asserts: join on auth, snapshot updates `members`,
 * `updatePresence`/`leavePresence` send the right wire frames, and unmount
 * tears down by leaving the room.
 */
import { act, fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { describe, expect, it } from "vitest";
import { RtDbClient, type WebSocketLike } from "../src/client.js";
import type { PresenceMember } from "../src/protocol.js";
import { RtDbProvider, usePresence } from "../src/react.js";

class FakeSocket implements WebSocketLike {
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: { code: number; reason: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  readonly sent: string[] = [];
  send(d: string): void {
    this.sent.push(d);
  }
  close() {}
  open() {
    this.onopen?.();
  }
  deliver(m: unknown) {
    this.onmessage?.({ data: JSON.stringify(m) });
  }
  parsed() {
    return this.sent.map((s) => JSON.parse(s));
  }
}

function setup() {
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
    setTimeoutImpl: () => 0 as unknown as ReturnType<typeof setTimeout>,
    clearTimeoutImpl: () => {},
  });
  return { client, sockets };
}

describe("usePresence", () => {
  it("joins on auth, reflects inbound snapshots, and sends updates", async () => {
    const { client, sockets } = setup();
    function View() {
      const r = usePresence("doc:1");
      return (
        <div>
          <span>count:{r.members.length}</span>
          <button type="button" onClick={() => r.updatePresence({ cursor: { x: 1 } })}>
            move
          </button>
          <button type="button" onClick={() => r.leavePresence()}>
            leave
          </button>
        </div>
      );
    }
    render(
      <RtDbProvider client={client} authBaseUrl="http://h:8300">
        <View />
      </RtDbProvider>,
    );

    await act(async () => {
      sockets[0].open();
      sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    });
    // The hook calls `presence()` on mount; after authOk the frame is sent.
    const join = sockets[0].parsed().find((m) => m.type === "presence");
    expect(join).toEqual({ type: "presence", room: "doc:1" });

    // Inbound snapshot -> state.
    const members: PresenceMember[] = [
      { connectionId: "self", user: { kind: "user" }, state: null },
      { connectionId: "peer", user: { kind: "user" }, state: null },
    ];
    await act(async () => {
      sockets[0].deliver({ type: "presenceSnapshot", room: "doc:1", members });
    });
    expect(screen.getByText("count:2")).toBeTruthy();

    // updatePresence -> presenceState frame.
    await act(async () => {
      fireEvent.click(screen.getByText("move"));
    });
    const update = sockets[0].parsed().find((m) => m.type === "presenceState");
    expect(update).toEqual({ type: "presenceState", room: "doc:1", state: { cursor: { x: 1 } } });
  });

  it("sends leavePresence on unmount", async () => {
    const { client, sockets } = setup();
    function View() {
      usePresence("doc:1");
      return <div>x</div>;
    }
    const { unmount } = render(
      <RtDbProvider client={client} authBaseUrl="http://h:8300">
        <View />
      </RtDbProvider>,
    );
    await act(async () => {
      sockets[0].open();
      sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    });
    const leavesBefore = sockets[0].parsed().filter((m) => m.type === "leavePresence").length;
    expect(leavesBefore).toBe(0);

    await act(async () => {
      unmount();
    });
    const leave = sockets[0].parsed().find((m) => m.type === "leavePresence");
    expect(leave).toEqual({ type: "leavePresence", room: "doc:1" });
  });

  it("re-subscribes when room changes (leaves old, joins new)", async () => {
    const { client, sockets } = setup();
    function View() {
      const [room, setRoom] = useState("doc:1");
      usePresence(room);
      return (
        <button type="button" onClick={() => setRoom("doc:2")}>
          switch
        </button>
      );
    }
    render(
      <RtDbProvider client={client} authBaseUrl="http://h:8300">
        <View />
      </RtDbProvider>,
    );
    await act(async () => {
      sockets[0].open();
      sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    });
    expect(sockets[0].parsed().filter((m) => m.type === "presence").length).toBe(1);
    expect(sockets[0].parsed().filter((m) => m.type === "leavePresence").length).toBe(0);

    // Switch room -> leaves doc:1, joins doc:2.
    await act(async () => {
      fireEvent.click(screen.getByText("switch"));
    });
    const presences = sockets[0]
      .parsed()
      .filter((m) => m.type === "presence")
      .map((m) => (m as { room: string }).room);
    const leaves = sockets[0]
      .parsed()
      .filter((m) => m.type === "leavePresence")
      .map((m) => (m as { room: string }).room);
    expect(presences).toEqual(["doc:1", "doc:2"]);
    expect(leaves).toEqual(["doc:1"]);
  });
});
