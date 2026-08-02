import { act, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SessionProvider, useSession } from "./session";

// SEC-001 invariant tests: no dashboard credential is ever held in JS — both
// login paths set an HttpOnly `rtdb_session` cookie, so the provider exposes only
// the auth method + user (never a token). These guard that invariant and verify
// the OAuth begin/poll relay + signOut hit the right endpoints.

function Probe() {
  const s = useSession();
  return (
    <div>
      <span data-testid="method">{s.method ?? "(null)"}</span>
      <span data-testid="user">{s.user?.email ?? "(null)"}</span>
      <button type="button" onClick={() => void s.signInWithAdminKey("test-key")}>
        sign-in-admin
      </button>
      <button type="button" onClick={() => void s.signInWithOAuth("github")}>
        sign-in-oauth
      </button>
      <button type="button" onClick={() => void s.signOut()}>
        sign-out
      </button>
    </div>
  );
}

function buildFetch(opts: { loginStatus?: number; meBody?: unknown } = {}) {
  const loginStatus = opts.loginStatus ?? 204;
  const meBody = opts.meBody ?? { email: "ops@example.com" };
  // The mount probe calls /auth/me first (no session yet -> 401); a later call
  // once the OAuth begin/poll flow completes resolves the user (200). The counter
  // models that.
  let meCalls = 0;
  return vi.fn(async (input: RequestInfo | URL, _init?: RequestInit) => {
    const url = typeof input === "string" ? input : input.toString();
    if (url === "/admin/login") {
      return new Response(null, { status: loginStatus });
    }
    if (url === "/admin/logout") {
      return new Response(null, { status: 204 });
    }
    if (url === "/auth/me") {
      meCalls += 1;
      if (meCalls === 1) return new Response(null, { status: 401 });
      return new Response(JSON.stringify(meBody), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }
    if (
      url.includes("/auth/github/begin") ||
      url.includes("/auth/google/begin") ||
      url.includes("/auth/gitlab/begin")
    ) {
      return new Response(JSON.stringify({ authorizeUrl: "about:blank", state: "s1" }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }
    if (url.includes("/auth/state")) {
      return new Response(JSON.stringify({ status: "complete" }), {
        status: 200,
        headers: { "content-type": "application/json" },
      });
    }
    if (url === "/auth/logout") {
      return new Response("{}", { status: 200 });
    }
    return new Response("not found", { status: 404 });
  }) as unknown as typeof fetch;
}

describe("SessionProvider", () => {
  let originalFetch: typeof globalThis.fetch;
  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });
  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
    vi.useRealTimers();
    localStorage.clear();
  });

  it("admin-key sign-in authenticates WITHOUT storing the key (cookie set server-side); signOut clears it", async () => {
    const setItemSpy = vi.spyOn(Storage.prototype, "setItem");
    globalThis.fetch = buildFetch();

    render(
      <SessionProvider>
        <Probe />
      </SessionProvider>,
    );
    expect(screen.getByTestId("method").textContent).toBe("(null)");

    await act(async () => {
      screen.getByText("sign-in-admin").click();
    });
    // SEC-001: no key in JS — only the method flips; the cookie authenticates.
    expect(screen.getByTestId("method").textContent).toBe("adminkey");
    expect(screen.getByTestId("user").textContent).toBe("(null)");

    await act(async () => {
      screen.getByText("sign-out").click();
    });
    expect(screen.getByTestId("method").textContent).toBe("(null)");

    // SEC-001 invariant: nothing token-like may ever be persisted.
    const persistedLooking = setItemSpy.mock.calls.filter(([k]) => {
      const key = String(k).toLowerCase();
      return key.includes("token") || key.includes("admin") || key.includes("rtdb");
    });
    expect(persistedLooking).toHaveLength(0);
  });

  it("OAuth begin+poll loads the user via /auth/me (cookie), signOut hits /auth/logout", async () => {
    const fetchMock = buildFetch({ meBody: { email: "oauth@example.com" } });
    globalThis.fetch = fetchMock;
    // noopener popup: window.open returns null (no popup handle) — the polling
    // timeout covers blocked/closed/abandoned. The spy lets us assert the call.
    vi.spyOn(window, "open").mockReturnValue(null);

    render(
      <SessionProvider>
        <Probe />
      </SessionProvider>,
    );

    await act(async () => {
      screen.getByText("sign-in-oauth").click();
    });

    // SEC-001: the begin callback sets an HttpOnly cookie (no token in JS); the
    // provider polls /auth/state, then loads the user via `/auth/me`.
    expect(screen.getByTestId("method").textContent).toBe("oauth");
    expect(screen.getByTestId("user").textContent).toBe("oauth@example.com");
    expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining("/auth/github/begin?origin="));
    expect(fetchMock).toHaveBeenCalledWith(expect.stringContaining("/auth/state?state=s1"));
    // SEC-012: the popup must open the begin-returned authorizeUrl with
    // noopener,noreferrer so the relay page can't reach back into this window.
    expect(window.open).toHaveBeenCalledWith(
      "about:blank",
      "rtdb-oauth",
      expect.stringContaining("noopener"),
    );
    expect(window.open).toHaveBeenCalledWith(
      "about:blank",
      "rtdb-oauth",
      expect.stringContaining("noreferrer"),
    );

    await act(async () => {
      screen.getByText("sign-out").click();
    });
    expect(screen.getByTestId("method").textContent).toBe("(null)");
    expect(fetchMock).toHaveBeenCalledWith(
      "/auth/logout",
      expect.objectContaining({ method: "POST" }),
    );
  });

  it("rejects an invalid admin key without authenticating", async () => {
    globalThis.fetch = buildFetch({ loginStatus: 401 });

    let captured: unknown = null;
    function ProbeErr() {
      const s = useSession();
      return (
        <button
          type="button"
          onClick={async () => {
            try {
              await s.signInWithAdminKey("bad");
            } catch (e) {
              captured = e;
            }
          }}
        >
          try-bad
        </button>
      );
    }

    render(
      <SessionProvider>
        <ProbeErr />
      </SessionProvider>,
    );

    await act(async () => {
      screen.getByText("try-bad").click();
    });

    expect(captured).toBeInstanceOf(Error);
    expect((captured as Error).message).toBe("invalid admin key");
  });
});
