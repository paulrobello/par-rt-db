import { createContext, useContext, useState, type ReactNode } from "react";
import type { AuthedUser } from "@par-rt-db/client";

export type AuthMethod = "oauth" | "adminkey";

export interface SessionValue {
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
  // SEC-001: no dashboard credential is ever held in JS. Both login paths
  // (`/admin/login` for the admin key, the OAuth callback for a session token)
  // set an HttpOnly `rtdb_session` cookie the browser sends on same-origin
  // requests — including the `/admin/stream` and `/sync` WS upgrades — so XSS
  // can't read it. This component tracks only the auth METHOD and the resolved
  // user; it never sees the secret.
  const [method, setMethod] = useState<AuthMethod | null>(null);
  const [user, setUser] = useState<AuthedUser | null>(null);
  const [loading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function signInWithAdminKey(key: string): Promise<void> {
    setError(null);
    // SEC-001: validate via `/admin/login`, which sets the HttpOnly cookie. The
    // key itself never lives in JS state.
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
        // SEC-001 phase 2: the callback signals success only — `{type:"rtdb-auth"}`
        // with NO token. The session token arrived as the HttpOnly cookie on the
        // callback response; load the user via `/auth/me` (cookie sent same-origin).
        const data = e.data as { type?: string } | null;
        if (data?.type === "rtdb-auth") {
          cleanup();
          popup.close();
          fetch("/auth/me")
            .then((r) => (r.ok ? r.json() : Promise.reject(new Error("could not load session"))))
            .then((u: AuthedUser) => {
              setError(null);
              setUser(u);
              setMethod("oauth");
              resolve();
            })
            .catch(reject);
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
    // SEC-001: clear the HttpOnly cookie server-side — `/auth/logout` (OAuth)
    // and `/admin/logout` (admin key) both send a Set-Cookie that expires it.
    if (method) {
      const endpoint = method === "oauth" ? "/auth/logout" : "/admin/logout";
      try {
        await fetch(endpoint, { method: "POST" });
      } catch {
        /* best-effort */
      }
    }
    setMethod(null);
    setUser(null);
  }

  const value: SessionValue = {
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
