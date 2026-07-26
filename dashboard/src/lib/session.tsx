import { createContext, useContext, useState, type ReactNode } from "react";
import type { AuthedUser } from "@par-rt-db/client";

export type AuthMethod = "oauth" | "adminkey";

export interface SessionValue {
  token: string | null;
  method: AuthMethod | null;
  user: AuthedUser | null;
  loading: boolean;
  error: string | null;
  signInWithAdminKey: (key: string) => Promise<void>;
  signInWithOAuth: (provider: "github" | "google") => Promise<void>;
  signOut: () => Promise<void>;
}

const SessionContext = createContext<SessionValue | null>(null);

export function useSession(): SessionValue {
  const v = useContext(SessionContext);
  if (!v) throw new Error("useSession must be used within SessionProvider");
  return v;
}

export function SessionProvider({ children }: { children: ReactNode }) {
  // SEC-001: the admin key never lives in JS — `/admin/login` sets an HttpOnly
  // `rtdb_session` cookie the browser sends on same-origin requests (including
  // the `/admin/stream` WS upgrade), so XSS can't read it. `token` here is used
  // ONLY by the OAuth path, which still holds the session token in React state
  // until Phase 2 moves it into the same cookie; it is `null` for admin-key
  // logins, so `token` is never a persisted admin key.
  const [token, setToken] = useState<string | null>(null);
  const [method, setMethod] = useState<AuthMethod | null>(null);
  const [user, setUser] = useState<AuthedUser | null>(null);
  const [loading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function applyToken(t: string, m: AuthMethod): Promise<void> {
    setToken(t);
    setMethod(m);
    setError(null);
    if (m === "oauth") {
      const resp = await fetch("/auth/me", {
        headers: { authorization: `Bearer ${t}` },
      });
      if (!resp.ok) throw new Error("could not load session");
      setUser(await resp.json());
    } else {
      setUser(null);
    }
  }

  async function signInWithAdminKey(key: string): Promise<void> {
    setError(null);
    // SEC-001: validate via `/admin/login`, which sets an HttpOnly session
    // cookie. The key itself never lives in JS state — the cookie (sent
    // automatically on same-origin requests) authenticates `/admin/*` and
    // `/admin/stream` from here on.
    const resp = await fetch("/admin/login", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ adminKey: key }),
    });
    if (!resp.ok) {
      throw new Error(
        resp.status === 401 || resp.status === 403
          ? "invalid admin key"
          : `rejected (${resp.status})`,
      );
    }
    setToken(null);
    setMethod("adminkey");
    setUser(null);
  }

  function signInWithOAuth(provider: "github" | "google"): Promise<void> {
    const origin = window.location.origin;
    const url = `${origin}/auth/${provider}?origin=${encodeURIComponent(origin)}`;
    return new Promise<void>((resolve, reject) => {
      const popup = window.open(url, "rtdb-oauth", "popup,width=560,height=720");
      if (!popup) {
        reject(new Error("popup blocked — allow popups for this site"));
        return;
      }
      const cleanup = () => {
        window.removeEventListener("message", onMessage);
        clearInterval(poller);
      };
      const onMessage = (e: MessageEvent) => {
        if (e.origin !== origin) return;
        const data = e.data as { type?: string; token?: string } | null;
        if (data?.type === "rtdb-auth" && typeof data.token === "string") {
          cleanup();
          popup.close();
          applyToken(data.token, "oauth").then(resolve, reject);
        }
      };
      const poller = setInterval(() => {
        if (popup.closed) {
          cleanup();
          reject(new Error("sign-in cancelled"));
        }
      }, 500);
      window.addEventListener("message", onMessage);
    });
  }

  async function signOut(): Promise<void> {
    if (method === "oauth" && token) {
      try {
        await fetch("/auth/logout", {
          method: "POST",
          headers: { authorization: `Bearer ${token}` },
        });
      } catch {
        /* best-effort */
      }
    } else if (method === "adminkey") {
      // SEC-001: clear the HttpOnly session cookie server-side.
      try {
        await fetch("/admin/logout", { method: "POST" });
      } catch {
        /* best-effort */
      }
    }
    setToken(null);
    setMethod(null);
    setUser(null);
  }

  const value: SessionValue = {
    token,
    method,
    user,
    loading,
    error,
    signInWithAdminKey,
    signInWithOAuth,
    signOut,
  };

  return <SessionContext.Provider value={value}>{children}</SessionContext.Provider>;
}
