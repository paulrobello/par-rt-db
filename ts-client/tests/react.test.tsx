import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { RtDbClient, type WebSocketLike } from "../src/client.js";
import type { QueryJson } from "../src/protocol.js";
import type { RtQuery } from "../src/query.js";
import {
  Authenticated,
  AuthLoading,
  OAUTH_POLL_INTERVAL_MS,
  OAUTH_POLL_TIMEOUT_MS,
  RtDbProvider,
  signInWithGitHub,
  signInWithGoogle,
  Unauthenticated,
  useConnectionState,
  usePaginatedQuery,
  useQuery,
  useRtDbAuth,
} from "../src/react.js";

/** Minimal Response stub for fetch-mocking the OAuth begin/state endpoints. */
function oauthResp(body: unknown, ok = true): Response {
  return { ok, json: async () => body } as unknown as Response;
}

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

describe("signInWithGitHub (begin + poll relay)", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("begins the flow, opens the authorize URL with noopener, and resolves with the polled token", async () => {
    vi.useFakeTimers();
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    fetchSpy.mockResolvedValueOnce(oauthResp({ authorizeUrl: "about:blank", state: "s1" }));
    fetchSpy.mockResolvedValueOnce(oauthResp({ status: "pending" }));
    fetchSpy.mockResolvedValueOnce(
      oauthResp({ status: "complete", token: "tok-1", user: { kind: "user" } }),
    );
    const openSpy = vi.spyOn(window, "open").mockReturnValue(null);

    const promise = signInWithGitHub("http://h:8300");
    // Drives the begin fetch → first poll (pending) → 800ms sleep → second poll (complete).
    await vi.advanceTimersByTimeAsync(OAUTH_POLL_INTERVAL_MS);

    await expect(promise).resolves.toBe("tok-1");
    expect(fetchSpy).toHaveBeenCalledWith(
      expect.stringContaining("/auth/github/begin?origin="),
      { credentials: "include" },
    );
    expect(fetchSpy).toHaveBeenCalledWith(expect.stringContaining("/auth/state?state=s1"));
    expect(openSpy).toHaveBeenCalledWith(
      "about:blank",
      "rtdb-auth",
      "noopener,noreferrer,width=600,height=700",
    );
  });

  it("rejects when a poll returns expired", async () => {
    vi.useFakeTimers();
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    fetchSpy.mockResolvedValueOnce(oauthResp({ authorizeUrl: "about:blank", state: "s1" }));
    fetchSpy.mockResolvedValueOnce(oauthResp({ status: "expired" }));

    const promise = signInWithGitHub("http://h:8300");
    // Attach the rejection handler before driving the poll so the rejection
    // doesn't surface as an unhandled rejection when the expired status lands.
    const rejection = expect(promise).rejects.toThrow("sign-in expired");
    await vi.advanceTimersByTimeAsync(OAUTH_POLL_INTERVAL_MS);
    await rejection;
  });

  it("rejects with 'sign-in timed out' after the timeout elapses", async () => {
    // Fake only setTimeout (used by `sleep`); leave Date.now real so we can
    // trip the deadline directly. Advancing the full 180s of fake time would
    // fire happy-dom's internal navigation timeout and emit noisy network
    // errors unrelated to the SDK.
    vi.useFakeTimers({ toFake: ["setTimeout", "clearTimeout"] });
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    fetchSpy.mockResolvedValueOnce(oauthResp({ authorizeUrl: "about:blank", state: "s1" }));
    // Every /auth/state poll stays pending until the deadline trips.
    fetchSpy.mockResolvedValue(oauthResp({ status: "pending" }));
    vi.spyOn(window, "open").mockReturnValue(null);
    // deadline = 0 + OAUTH_POLL_TIMEOUT_MS; the first loop check passes (0);
    // the check after one pending poll + sleep trips past the deadline.
    vi.spyOn(Date, "now")
      .mockReturnValueOnce(0)
      .mockReturnValueOnce(0)
      .mockReturnValue(OAUTH_POLL_TIMEOUT_MS + 1);

    const promise = signInWithGitHub("http://h:8300");
    const rejection = expect(promise).rejects.toThrow("sign-in timed out");
    await vi.advanceTimersByTimeAsync(OAUTH_POLL_INTERVAL_MS);
    await rejection;
  });
});

describe("signInWithGoogle (begin + poll relay)", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it("begins the google flow, opens the authorize URL with noopener, and resolves with the polled token", async () => {
    vi.useFakeTimers();
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    fetchSpy.mockResolvedValueOnce(oauthResp({ authorizeUrl: "about:blank", state: "s1" }));
    fetchSpy.mockResolvedValueOnce(
      oauthResp({ status: "complete", token: "goog-tok", user: { kind: "user" } }),
    );
    const openSpy = vi.spyOn(window, "open").mockReturnValue(null);

    const promise = signInWithGoogle("http://h:8300");
    await vi.advanceTimersByTimeAsync(OAUTH_POLL_INTERVAL_MS);

    await expect(promise).resolves.toBe("goog-tok");
    expect(fetchSpy).toHaveBeenCalledWith(
      expect.stringContaining("/auth/google/begin?origin="),
      { credentials: "include" },
    );
    expect(openSpy).toHaveBeenCalledWith(
      "about:blank",
      "rtdb-auth",
      "noopener,noreferrer,width=600,height=700",
    );
  });
});

describe("useRtDbAuth signIn routing", () => {
  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
    // Token-mode signIn writes the session token to localStorage on completion
    // (below); clear it so it cannot leak into later tests' assertions.
    localStorage.removeItem("rtdb-session-token");
  });

  it("signIn('google') begins the google flow and persists the token in token mode", async () => {
    vi.useFakeTimers();
    const { client } = setup();
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    fetchSpy.mockResolvedValueOnce(oauthResp({ authorizeUrl: "about:blank", state: "s1" }));
    fetchSpy.mockResolvedValueOnce(
      oauthResp({ status: "complete", token: "goog-tok", user: { kind: "user" } }),
    );
    const openSpy = vi.spyOn(window, "open").mockReturnValue(null);
    let pending: Promise<void> | undefined;

    function View() {
      const { signIn } = useRtDbAuth();
      return (
        <button
          type="button"
          onClick={() => {
            pending = signIn("google");
          }}
        >
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
    await act(async () => {
      await vi.advanceTimersByTimeAsync(OAUTH_POLL_INTERVAL_MS);
      await pending;
    });

    expect(fetchSpy).toHaveBeenCalledWith(
      expect.stringContaining("/auth/google/begin?origin="),
      { credentials: "include" },
    );
    expect(openSpy).toHaveBeenCalledWith(
      "about:blank",
      "rtdb-auth",
      "noopener,noreferrer,width=600,height=700",
    );
    // Token mode: the credential persists to localStorage for reconnect hydration.
    expect(localStorage.getItem("rtdb-session-token")).toBe("goog-tok");
  });

  it("signIn() with no argument begins the github flow (default)", async () => {
    vi.useFakeTimers();
    const { client } = setup();
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    fetchSpy.mockResolvedValueOnce(oauthResp({ authorizeUrl: "about:blank", state: "s1" }));
    fetchSpy.mockResolvedValueOnce(
      oauthResp({ status: "complete", token: "gh-tok", user: { kind: "user" } }),
    );
    vi.spyOn(window, "open").mockReturnValue(null);
    let pending: Promise<void> | undefined;

    function View() {
      const { signIn } = useRtDbAuth();
      return (
        <button
          type="button"
          onClick={() => {
            pending = signIn();
          }}
        >
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
    await act(async () => {
      await vi.advanceTimersByTimeAsync(OAUTH_POLL_INTERVAL_MS);
      await pending;
    });

    expect(fetchSpy).toHaveBeenCalledWith(
      expect.stringContaining("/auth/github/begin?origin="),
      { credentials: "include" },
    );
    expect(localStorage.getItem("rtdb-session-token")).toBe("gh-tok");
  });
});

describe("useRtDbAuth cookie mode (SEC-002)", () => {
  afterEach(() => {
    vi.useRealTimers();
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
    vi.useFakeTimers();
    const { client } = setupCookie();
    expect(client.cookieMode).toBe(true);
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    fetchSpy.mockResolvedValueOnce(oauthResp({ authorizeUrl: "about:blank", state: "s1" }));
    fetchSpy.mockResolvedValueOnce(
      oauthResp({ status: "complete", token: "session-tok", user: { kind: "user" } }),
    );
    vi.spyOn(window, "open").mockReturnValue(null);

    let pending: Promise<void> | undefined;
    function View() {
      const { signIn } = useRtDbAuth();
      return (
        <button
          type="button"
          onClick={() => {
            pending = signIn();
          }}
        >
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
    // Resolve the OAuth poll (the server has now set the HttpOnly cookie), then
    // await signIn's continuation so the assertion observes its final state.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(OAUTH_POLL_INTERVAL_MS);
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
