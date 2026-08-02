import type { AuthedUser } from "@par-rt-db/client";
import { createContext, type ReactNode, useContext, useEffect, useState } from "react";

export type AuthMethod = "oauth" | "adminkey";

export interface SessionValue {
  method: AuthMethod | null;
  user: AuthedUser | null;
  loading: boolean;
  error: string | null;
  signInWithAdminKey: (key: string) => Promise<void>;
  signInWithOAuth: (provider: "github" | "google" | "gitlab") => Promise<void>;
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
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // SEC-001: the HttpOnly cookie survives a page reload, so on mount probe
  // `/auth/me` (the browser sends the cookie) and restore an OAuth session.
  // Admin-key sessions have no resolvable identity (the key isn't a user
  // session), so a reload re-prompts for the key — acceptable for an
  // operator-typed credential.
  useEffect(() => {
    let cancelled = false;
    fetch("/auth/me")
      .then((r) => (r.ok ? r.json() : null))
      .then((u: AuthedUser | null) => {
        if (cancelled) return;
        if (u) {
          setUser(u);
          setMethod("oauth");
        }
        setLoading(false);
      })
      .catch(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

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

  function signInWithOAuth(provider: "github" | "google" | "gitlab"): Promise<void> {
    const origin = window.location.origin;
    const beginUrl = `${origin}/auth/${provider}/begin?origin=${encodeURIComponent(origin)}`;
    const poll = (
      state: string,
      deadline: number,
      resolve: () => void,
      reject: (e: Error) => void,
    ) => {
      if (Date.now() > deadline) {
        reject(new Error("sign-in timed out"));
        return;
      }
      fetch(`${origin}/auth/state?state=${encodeURIComponent(state)}`)
        .then((r) => (r.ok ? r.json() : null))
        .then((data: { status?: string } | null) => {
          if (data?.status === "complete") {
            // The HttpOnly cookie was set by the callback; load the user.
            fetch("/auth/me")
              .then((r) => (r.ok ? r.json() : Promise.reject(new Error("could not load session"))))
              .then((u: AuthedUser) => {
                setError(null);
                setUser(u);
                setMethod("oauth");
                resolve();
              })
              .catch(reject);
          } else if (data?.status === "expired" || data?.status === "error") {
            reject(new Error(`sign-in ${data.status}`));
          } else {
            setTimeout(() => poll(state, deadline, resolve, reject), 800);
          }
        })
        .catch(() => setTimeout(() => poll(state, deadline, resolve, reject), 800));
    };
    return new Promise<void>((resolve, reject) => {
      fetch(beginUrl)
        .then((r) => (r.ok ? r.json() : Promise.reject(new Error("could not start sign-in"))))
        .then((b: { authorizeUrl: string; state: string }) => {
          // noopener: window.open returns null — no blocked-popup detect, no
          // closed-poll; the polling timeout covers blocked/closed/abandoned.
          window.open(
            b.authorizeUrl,
            "rtdb-oauth",
            "noopener,noreferrer,popup,width=560,height=720",
          );
          poll(b.state, Date.now() + 180_000, resolve, reject);
        })
        .catch(reject);
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
