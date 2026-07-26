import { act, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { SessionProvider, useSession } from "./session";

// SEC-001 invariant tests: no dashboard credential is ever held in JS — both
// login paths set an HttpOnly `rtdb_session` cookie, so the provider exposes only
// the auth method + user (never a token). These guard that invariant and verify
// the OAuth postMessage handshake + signOut hit the right endpoints.

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
  // after the OAuth callback resolves the user (200). The counter models that.
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

  it("OAuth handshake loads the user via /auth/me (cookie), signOut hits /auth/logout", async () => {
    const fetchMock = buildFetch({ meBody: { email: "oauth@example.com" } });
    globalThis.fetch = fetchMock;
    const popup = { closed: false, close: vi.fn() };
    vi.spyOn(window, "open").mockReturnValue(popup as unknown as Window);

    render(
      <SessionProvider>
        <Probe />
      </SessionProvider>,
    );

    await act(async () => {
      screen.getByText("sign-in-oauth").click();
    });

    // SEC-001 phase 2: the callback posts `{type:"rtdb-auth"}` with NO token;
    // the provider loads the user via `/auth/me` (the cookie authenticates).
    await act(async () => {
      window.dispatchEvent(
        new MessageEvent("message", {
          origin: window.location.origin,
          data: { type: "rtdb-auth" },
        }),
      );
    });

    expect(screen.getByTestId("method").textContent).toBe("oauth");
    expect(screen.getByTestId("user").textContent).toBe("oauth@example.com");
    expect(popup.close).toHaveBeenCalled();
    expect(fetchMock).toHaveBeenCalledWith("/auth/me");

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
