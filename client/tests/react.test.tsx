import { act, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { RtDbClient, type WebSocketLike } from "../src/client.js";
import {
  Authenticated,
  AuthLoading,
  RtDbProvider,
  signInWithGitHub,
  Unauthenticated,
  useQuery,
} from "../src/react.js";
import type { RtQuery } from "../src/query.js";

class FakeSocket implements WebSocketLike {
  onopen: (() => void) | null = null;
  onmessage: ((ev: { data: unknown }) => void) | null = null;
  onclose: ((ev: { code: number; reason: string }) => void) | null = null;
  onerror: (() => void) | null = null;
  readonly sent: string[] = [];
  send(d: string) {
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

describe("react bindings", () => {
  it("useQuery returns undefined then the pushed result", async () => {
    const { client, sockets } = setup();
    const q: RtQuery<Array<{ _id: string }>> = { json: { table: "items" } };

    function View() {
      const items = useQuery(q);
      return <div>{items === undefined ? "loading" : `count:${items.length}`}</div>;
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
    expect(screen.getByText("loading")).toBeTruthy();

    const sub = sockets[0].parsed().find((m) => m.type === "subscribe") as { queryId: string };
    await act(async () => {
      sockets[0].deliver({
        type: "queryUpdate",
        queryId: sub.queryId,
        result: [{ _id: "a" }, { _id: "b" }],
      });
    });
    expect(screen.getByText("count:2")).toBeTruthy();
  });

  it("renders auth gates by connection auth state", async () => {
    const { client, sockets } = setup();
    render(
      <RtDbProvider client={client} authBaseUrl="http://h:8300">
        <AuthLoading>loading</AuthLoading>
        <Authenticated>in</Authenticated>
        <Unauthenticated>out</Unauthenticated>
      </RtDbProvider>,
    );
    // authenticating -> AuthLoading
    expect(screen.getByText("loading")).toBeTruthy();
    await act(async () => {
      sockets[0].open();
      sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    });
    expect(screen.getByText("in")).toBeTruthy();
  });

  it('"skip" suppresses the subscription', async () => {
    const { client, sockets } = setup();
    function View() {
      const items = useQuery<unknown[]>("skip");
      return <div>{items === undefined ? "skipped" : "has"}</div>;
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
    expect(screen.getByText("skipped")).toBeTruthy();
    expect(sockets[0].parsed().some((m) => m.type === "subscribe")).toBe(false);
  });

  it("shows AuthLoading (not Unauthenticated) while a stored session reconnects", async () => {
    localStorage.setItem("rtdb-session-token", "stored-tok");
    const { client, sockets } = setup();
    render(
      <RtDbProvider client={client} authBaseUrl="http://h:8300">
        <AuthLoading>loading</AuthLoading>
        <Authenticated>in</Authenticated>
        <Unauthenticated>out</Unauthenticated>
      </RtDbProvider>,
    );
    // A returning user with a stored token must land on AuthLoading during the
    // connect+auth roundtrip, not flash Unauthenticated.
    expect(screen.getByText("loading")).toBeTruthy();
    await act(async () => {
      sockets[0].open();
      sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    });
    expect(screen.getByText("in")).toBeTruthy();
    localStorage.removeItem("rtdb-session-token");
  });
});

describe("signInWithGitHub", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  function deliverAuthMessage(token: string) {
    window.dispatchEvent(
      new MessageEvent("message", {
        origin: "http://h:8300",
        data: { type: "rtdb-auth", token },
      }),
    );
  }

  it("resolves with the token from a valid rtdb-auth message", async () => {
    const popup = { closed: false };
    vi.spyOn(window, "open").mockReturnValue(popup as unknown as Window);

    const promise = signInWithGitHub("http://h:8300");
    deliverAuthMessage("tok-1");

    await expect(promise).resolves.toBe("tok-1");
  });

  it("rejects immediately when the popup is blocked", async () => {
    vi.spyOn(window, "open").mockReturnValue(null);

    await expect(signInWithGitHub("http://h:8300")).rejects.toThrow("popup blocked");
  });

  it("rejects and cleans up the message listener when the popup closes before sign-in completes", async () => {
    vi.useFakeTimers();
    const popup = { closed: false };
    vi.spyOn(window, "open").mockReturnValue(popup as unknown as Window);
    const removeEventListenerSpy = vi.spyOn(window, "removeEventListener");

    const promise = signInWithGitHub("http://h:8300");
    const rejection = expect(promise).rejects.toThrow(/popup closed before completing sign-in/);

    popup.closed = true;
    await vi.advanceTimersByTimeAsync(1000);
    await rejection;

    expect(removeEventListenerSpy).toHaveBeenCalledWith("message", expect.any(Function));
  });

  it("does not reject if the popup closes after a valid message already resolved sign-in", async () => {
    vi.useFakeTimers();
    const popup = { closed: false };
    vi.spyOn(window, "open").mockReturnValue(popup as unknown as Window);

    const promise = signInWithGitHub("http://h:8300");
    deliverAuthMessage("tok-2");
    await expect(promise).resolves.toBe("tok-2");

    popup.closed = true;
    // Advancing well past the poll interval after resolution must not throw or
    // produce an unhandled rejection — the interval must already be cleared.
    await vi.advanceTimersByTimeAsync(5000);
  });
});
