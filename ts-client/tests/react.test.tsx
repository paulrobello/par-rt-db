import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { RtDbClient, type WebSocketLike } from "../src/client.js";
import type { QueryJson } from "../src/protocol.js";
import type { RtQuery } from "../src/query.js";
import {
  Authenticated,
  AuthLoading,
  RtDbProvider,
  signInWithGitHub,
  signInWithGoogle,
  Unauthenticated,
  useConnectionState,
  usePaginatedQuery,
  useQuery,
  useRtDbAuth,
} from "../src/react.js";

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

describe("signInWithGoogle", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("opens the /auth/google popup and resolves with the token from a valid rtdb-auth message", async () => {
    const popup = { closed: false };
    const openSpy = vi.spyOn(window, "open").mockReturnValue(popup as unknown as Window);

    const promise = signInWithGoogle("http://h:8300");
    window.dispatchEvent(
      new MessageEvent("message", {
        origin: "http://h:8300",
        data: { type: "rtdb-auth", token: "goog-tok" },
      }),
    );

    await expect(promise).resolves.toBe("goog-tok");
    expect(openSpy).toHaveBeenCalledWith(
      expect.stringContaining("/auth/google?origin="),
      "rtdb-auth",
      "width=600,height=700",
    );
  });

  it("rejects immediately when the popup is blocked", async () => {
    vi.spyOn(window, "open").mockReturnValue(null);
    await expect(signInWithGoogle("http://h:8300")).rejects.toThrow("popup blocked");
  });
});

describe("useRtDbAuth signIn routing", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    // Token-mode signIn writes the session token to localStorage on completion
    // (below); clear it so it cannot leak into later tests' assertions.
    localStorage.removeItem("rtdb-session-token");
  });

  it("signIn('google') opens the /auth/google popup", async () => {
    const { client } = setup();
    const popup = { closed: false };
    const openSpy = vi.spyOn(window, "open").mockReturnValue(popup as unknown as Window);
    let pending: Promise<void> | undefined;

    function View() {
      const { signIn } = useRtDbAuth();
      return (
        <button type="button" onClick={() => { pending = signIn("google"); }}>
          google
        </button>
      );
    }

    render(
      <RtDbProvider client={client} authBaseUrl="http://h:8300">
        <View />
      </RtDbProvider>,
    );

    await act(async () => {
      fireEvent.click(screen.getByText("google"));
    });

    expect(openSpy).toHaveBeenCalledWith(
      expect.stringContaining("/auth/google?origin="),
      "rtdb-auth",
      "width=600,height=700",
    );

    // Complete the OAuth flow so signInWithOAuth removes its message listener
    // (a pending, never-resolved signIn would otherwise leak the listener into
    // later tests and fire on their auth messages).
    await act(async () => {
      window.dispatchEvent(
        new MessageEvent("message", {
          origin: "http://h:8300",
          data: { type: "rtdb-auth", token: "tok" },
        }),
      );
      await pending;
    });
  });

  it("signIn() with no argument opens the /auth/github popup (default)", async () => {
    const { client } = setup();
    const popup = { closed: false };
    const openSpy = vi.spyOn(window, "open").mockReturnValue(popup as unknown as Window);
    let pending: Promise<void> | undefined;

    function View() {
      const { signIn } = useRtDbAuth();
      return (
        <button type="button" onClick={() => { pending = signIn(); }}>
          default
        </button>
      );
    }

    render(
      <RtDbProvider client={client} authBaseUrl="http://h:8300">
        <View />
      </RtDbProvider>,
    );

    await act(async () => {
      fireEvent.click(screen.getByText("default"));
    });

    expect(openSpy).toHaveBeenCalledWith(
      expect.stringContaining("/auth/github?origin="),
      "rtdb-auth",
      "width=600,height=700",
    );

    await act(async () => {
      window.dispatchEvent(
        new MessageEvent("message", {
          origin: "http://h:8300",
          data: { type: "rtdb-auth", token: "tok" },
        }),
      );
      await pending;
    });
  });
});

describe("useRtDbAuth cookie mode (SEC-002)", () => {
  afterEach(() => {
    vi.restoreAllMocks();
    localStorage.removeItem("rtdb-session-token");
  });

  // No `getToken` -> cookie mode: the HttpOnly `rtdb_session` cookie authenticates.
  function setupCookie() {
    const sockets: FakeSocket[] = [];
    const client = new RtDbClient({
      url: "ws://h:8300",
      db: "kanban",
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

  it("cookieMode is true when no getToken is supplied", () => {
    const { client } = setupCookie();
    expect(client.cookieMode).toBe(true);
  });

  it("signIn does not persist the session token to localStorage", async () => {
    const { client } = setupCookie();
    expect(client.cookieMode).toBe(true);
    const popup = { closed: false };
    vi.spyOn(window, "open").mockReturnValue(popup as unknown as Window);

    let pending: Promise<void> | undefined;
    function View() {
      const { signIn } = useRtDbAuth();
      return (
        <button type="button" onClick={() => { pending = signIn(); }}>
          in
        </button>
      );
    }
    render(
      <RtDbProvider client={client} authBaseUrl="http://h:8300">
        <View />
      </RtDbProvider>,
    );

    await act(async () => {
      fireEvent.click(screen.getByText("in"));
    });
    // Resolve the OAuth popup (the server has now set the HttpOnly cookie), then
    // await signIn's continuation so the assertion observes its final state.
    await act(async () => {
      window.dispatchEvent(
        new MessageEvent("message", {
          origin: "http://h:8300",
          data: { type: "rtdb-auth", token: "session-tok" },
        }),
      );
      await pending;
    });

    // SEC-002: the credential must NOT land in script-readable storage.
    expect(localStorage.getItem("rtdb-session-token")).toBeNull();
  });
});

describe("usePaginatedQuery", () => {
  /** Returns the latest subscribe frame not yet responded to. */
  function nextPendingSub(
    socket: FakeSocket,
    delivered: Set<string>,
  ): { queryId: string; query: QueryJson } {
    const frame = socket
      .parsed()
      .find(
        (m) => m.type === "subscribe" && !delivered.has((m as { queryId: string }).queryId),
      ) as {
      queryId: string;
      query: QueryJson;
    };
    if (!frame) {
      throw new Error("no pending subscribe frame");
    }
    delivered.add(frame.queryId);
    return frame;
  }

  it("loads the first page then stitches the next via loadMore", async () => {
    const { client, sockets } = setup();
    const delivered = new Set<string>();

    function View() {
      const r = usePaginatedQuery<{ _id: string }>(() => ({ table: "items" }), {
        pageSize: 2,
      });
      return (
        <div>
          <span>count:{r.data.length}</span>
          <span>loading:{String(r.loading)}</span>
          <span>hasNext:{String(r.hasNextPage)}</span>
          <button type="button" onClick={() => void r.loadMore()}>
            more
          </button>
        </div>
      );
    }

    render(
      <RtDbProvider client={client} authBaseUrl="http://h:8300">
        <View />
      </RtDbProvider>,
    );
    // First page is pending before the socket authenticates.
    expect(screen.getByText("loading:true")).toBeTruthy();

    await act(async () => {
      sockets[0].open();
      sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    });

    const sub1 = nextPendingSub(sockets[0], delivered);
    expect(sub1.query.paginate?.numItems).toBe(2);
    expect(sub1.query.paginate?.cursor).toBeUndefined();

    await act(async () => {
      sockets[0].deliver({
        type: "queryUpdate",
        queryId: sub1.queryId,
        result: { docs: [{ _id: "a" }, { _id: "b" }], nextCursor: "cur1" },
      });
    });
    expect(screen.getByText("count:2")).toBeTruthy();
    expect(screen.getByText("loading:false")).toBeTruthy();
    expect(screen.getByText("hasNext:true")).toBeTruthy();

    // loadMore subscribes page 2 at the returned cursor. The incremental
    // manager must NOT re-subscribe page 1, so only one new frame appears.
    await act(async () => {
      screen.getByText("more").click();
    });
    const sub2 = nextPendingSub(sockets[0], delivered);
    expect(sub2.query.paginate?.cursor).toBe("cur1");
    expect(sub2.query.paginate?.numItems).toBe(2);

    await act(async () => {
      sockets[0].deliver({
        type: "queryUpdate",
        queryId: sub2.queryId,
        result: { docs: [{ _id: "c" }] }, // no nextCursor -> last page
      });
    });
    expect(screen.getByText("count:3")).toBeTruthy();
    expect(screen.getByText("hasNext:false")).toBeTruthy();
    expect(screen.getByText("loading:false")).toBeTruthy();

    // loadMore on the last page is a no-op (no new subscribe).
    const before = sockets[0].parsed().filter((m) => m.type === "subscribe").length;
    await act(async () => {
      screen.getByText("more").click();
    });
    const after = sockets[0].parsed().filter((m) => m.type === "subscribe").length;
    expect(after).toBe(before);
  });

  it("does not subscribe when enabled is false", async () => {
    const { client, sockets } = setup();
    function View() {
      const r = usePaginatedQuery<{ _id: string }>(() => ({ table: "items" }), {
        enabled: false,
      });
      return <div>count:{r.data.length}</div>;
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
    expect(screen.getByText("count:0")).toBeTruthy();
    expect(sockets[0].parsed().some((m) => m.type === "subscribe")).toBe(false);
  });

  it("refetch drops deeper pages and re-subscribes the first page", async () => {
    const { client, sockets } = setup();
    const delivered = new Set<string>();
    function View() {
      const r = usePaginatedQuery<{ _id: string }>(() => ({ table: "items" }), {
        pageSize: 2,
      });
      return (
        <div>
          <span>count:{r.data.length}</span>
          <button type="button" onClick={() => void r.loadMore()}>
            more
          </button>
          <button type="button" onClick={() => void r.refetch()}>
            refetch
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

    const sub1 = nextPendingSub(sockets[0], delivered);
    await act(async () => {
      sockets[0].deliver({
        type: "queryUpdate",
        queryId: sub1.queryId,
        result: { docs: [{ _id: "a" }, { _id: "b" }], nextCursor: "cur1" },
      });
    });
    await act(async () => {
      screen.getByText("more").click();
    });
    const sub2 = nextPendingSub(sockets[0], delivered);
    await act(async () => {
      sockets[0].deliver({
        type: "queryUpdate",
        queryId: sub2.queryId,
        result: { docs: [{ _id: "c" }] },
      });
    });
    expect(screen.getByText("count:3")).toBeTruthy();

    // refetch resets to the first page; deeper pages are dropped.
    await act(async () => {
      screen.getByText("refetch").click();
    });
    expect(screen.getByText("count:0")).toBeTruthy();
    const subRefetch = nextPendingSub(sockets[0], delivered);
    await act(async () => {
      sockets[0].deliver({
        type: "queryUpdate",
        queryId: subRefetch.queryId,
        result: { docs: [{ _id: "a2" }, { _id: "b2" }] },
      });
    });
    expect(screen.getByText("count:2")).toBeTruthy();
  });
});

describe("useConnectionState", () => {
  it("tracks the idle → connecting → connected transitions", async () => {
    const { client, sockets } = setup();
    function View() {
      const conn = useConnectionState();
      return <div>conn:{conn}</div>;
    }
    render(
      <RtDbProvider client={client} authBaseUrl="http://h:8300">
        <View />
      </RtDbProvider>,
    );
    // Before the socket opens the client is not yet connected.
    expect(screen.getByText(/^conn:/).textContent).not.toBe("conn:connected");
    await act(async () => {
      sockets[0].open();
      sockets[0].deliver({ type: "authOk", user: { kind: "user" } });
    });
    expect(screen.getByText("conn:connected")).toBeTruthy();
  });
});
