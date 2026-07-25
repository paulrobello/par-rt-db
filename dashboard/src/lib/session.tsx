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
  // SEC-001: the admin bearer token lives in React state ONLY — never written
  // to `localStorage` — so an XSS / malicious extension can't lift a persisted
  // admin key via `localStorage.getItem`. Tradeoff: a page reload clears the
  // token and the operator re-authenticates. Long-term this should be replaced
  // by an HttpOnly+Secure+SameSite=Strict cookie set by a server-issued
  // admin-login endpoint (out of scope for this remediation).
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
    const resp = await fetch("/admin/dbs", {
      headers: { authorization: `Bearer ${key}` },
    });
    if (!resp.ok) {
      throw new Error(
        resp.status === 401 || resp.status === 403
          ? "invalid admin key"
          : `rejected (${resp.status})`,
      );
    }
    await applyToken(key, "adminkey");
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
