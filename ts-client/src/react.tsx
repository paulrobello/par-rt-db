import {
  createContext,
  createElement,
  type ReactNode,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import type { AuthState, ConnectionState, RtDbClient } from "./client.js";
import type { StepResult } from "./mutation.js";
import type { AuthedUser, PresenceMember, TransactionJson } from "./protocol.js";
import type { RtQuery } from "./query.js";

export type { UsePaginatedQueryOptions, UsePaginatedQueryResult } from "./usePaginatedQuery.js";
export { usePaginatedQuery } from "./usePaginatedQuery.js";

const TOKEN_STORAGE_KEY = "rtdb-session-token";

interface RtDbContextValue {
  client: RtDbClient;
  authBaseUrl: string;
  state: AuthState;
  user: AuthedUser | null;
}

const RtDbContext = createContext<RtDbContextValue | null>(null);

function useContextValue(): RtDbContextValue {
  const ctx = useContext(RtDbContext);
  if (!ctx) {
    throw new Error("RtDb hooks must be used within <RtDbProvider>");
  }
  return ctx;
}

/** Required React context provider around every hook in this module. Tracks
 * the client's auth state (re-rendering on sign-in/out) and, in token mode,
 * hydrates/persists the session token from `localStorage`; `authBaseUrl` is
 * the server's HTTP origin used for the OAuth popup and logout. */
export function RtDbProvider(props: {
  client: RtDbClient;
  authBaseUrl: string;
  children: ReactNode;
}): ReactNode {
  const { client, authBaseUrl, children } = props;
  const [state, setState] = useState<AuthState>(() => client.getAuthState());
  const [user, setUser] = useState<AuthedUser | null>(() => client.getUser());

  useEffect(() => {
    // Attach the listener BEFORE setToken/connect so the synchronous
    // authState transitions they trigger are observed, then re-snapshot in
    // case state advanced before the listener was wired or the client was
    // already connected when this provider mounted.
    const off = client.onAuthChange((nextState, nextUser) => {
      setState(nextState);
      setUser(nextUser);
    });
    // Token mode hydrates from localStorage; cookie mode (no `getToken`) connects
    // tokenless and lets the browser's HttpOnly session cookie authenticate the
    // upgrade — it must never read script-readable storage (SEC-002).
    if (!client.cookieMode) {
      const stored =
        typeof localStorage !== "undefined" ? localStorage.getItem(TOKEN_STORAGE_KEY) : null;
      if (stored) {
        client.setToken(stored);
      }
    }
    client.connect();
    setState(client.getAuthState());
    setUser(client.getUser());
    return () => {
      off();
    };
  }, [client]);

  const value = useMemo<RtDbContextValue>(
    () => ({ client, authBaseUrl, state, user }),
    [client, authBaseUrl, state, user],
  );

  return createElement(RtDbContext.Provider, { value }, children);
}

export function useRtDbClient(): RtDbClient {
  return useContextValue().client;
}

/** Subscribe to a live query built by `createApi` (`api.items.query()...`).
 * Returns the query's current result, `undefined` until the first update
 * arrives, and re-renders on every push. Pass `"skip"` to hold the
 * subscription off (result stays `undefined`). Re-subscribes when the
 * serialized query shape changes; an identical shape does not. */
export function useQuery<R>(query: RtQuery<R> | "skip"): R | undefined {
  const { client } = useContextValue();
  const [value, setValue] = useState<R | undefined>(undefined);
  const key = query === "skip" ? "skip" : JSON.stringify(query.json);

  // `key` (the serialized query shape) is the real dependency; a new object
  // with the same shape must not resubscribe. `query`/`setValue` are read but
  // intentionally excluded — keyed re-subscription is the desired behavior.
  // biome-ignore lint/correctness/useExhaustiveDependencies: re-subscribe keyed by serialized query shape
  useEffect(() => {
    if (query === "skip") {
      setValue(undefined);
      return;
    }
    // Reset to `undefined` when the query changes so a prior query's result is
    // not shown under the new one; `subscribe` immediately replays a cached
    // value if this query already has one.
    setValue(undefined);
    const off = client.subscribe(query, (next) => setValue(next));
    return off;
  }, [client, key]);

  return value;
}

/** Returns a stable `mutate(txn)` function that runs a transaction over the
 * provider's client and resolves with one `StepResult` per step. */
export function useMutation(): (txn: TransactionJson) => Promise<StepResult[]> {
  const { client } = useContextValue();
  return useCallback((txn: TransactionJson) => client.mutate(txn), [client]);
}

/**
 * Subscribes to presence room `room`: joins on mount, returns the current
 * `members` list (re-rendered on every `presenceSnapshot`), and leaves on
 * unmount. Mirrors `useQuery`'s mount/subscribe/teardown lifecycle: `room` is
 * the real dependency, so changing it leaves the old room and joins the new.
 */
export function usePresence(room: string): {
  members: PresenceMember[];
  updatePresence: (state: unknown, ttlMs?: number) => void;
  leavePresence: () => void;
} {
  const { client } = useContextValue();
  const [members, setMembers] = useState<PresenceMember[]>([]);

  // `client` and `room` are both read inside the effect and listed as deps —
  // exhaustive. Changing `room` leaves the old room and joins the new.
  useEffect(() => {
    setMembers([]);
    const off = client.presence(room, undefined, setMembers);
    return () => {
      // Leave the room first (sends the wire frame AND drops local listeners),
      // then run the snapshot unsub — a no-op once `leavePresence` cleared the set.
      client.leavePresence(room);
      off();
    };
  }, [client, room]);

  const updatePresence = useCallback(
    (state: unknown, ttlMs?: number) => client.updatePresence(room, state, ttlMs),
    [client, room],
  );
  const leavePresence = useCallback(() => client.leavePresence(room), [client, room]);

  return { members, updatePresence, leavePresence };
}

export function useConnectionState(): ConnectionState {
  const { client } = useContextValue();
  const [state, setState] = useState<ConnectionState>(() => client.getConnectionState());
  useEffect(() => {
    const off = client.onConnectionChange((next) => setState(next));
    // Re-snapshot in case state advanced between the initial useState snapshot
    // and wiring the listener (e.g. the provider's connect() fired first).
    setState(client.getConnectionState());
    return off;
  }, [client]);
  return state;
}

/** Every OAuth provider the server ships. The union is the single source of
 *  truth so the signIn signature, the popup helper, and the convenience
 *  exports cannot drift apart. Keep this in sync with `server/src/auth`. */
export type OAuthProvider = "github" | "google" | "gitlab" | "microsoft" | "apple" | "oidc";

/** The auth surface for React: current `state` + `user`, plus `signIn`
 * (OAuth popup; provider defaults to GitHub), `signInAnonymous` (gated by
 * the server's anonymous flag), and `signOut`. Works in both cookie mode
 * (HttpOnly session cookie; no script-readable credential) and token mode
 * (`getToken` + storage). */
export function useRtDbAuth(): {
  state: AuthState;
  user: AuthedUser | null;
  signIn: (provider?: OAuthProvider) => Promise<void>;
  signInAnonymous: () => Promise<void>;
  signOut: () => Promise<void>;
} {
  const { client, authBaseUrl, state, user } = useContextValue();

  const signIn = useCallback(
    async (provider: OAuthProvider = "github") => {
      // The OAuth popup runs in both modes; the server sets the HttpOnly
      // session cookie on its callback regardless. Token mode persists the
      // posted-back token; cookie mode ignores it and re-dials tokenless so the
      // now-set cookie authenticates — no script-readable credential (SEC-002).
      // All providers share one `/auth/{provider}/begin` + `/auth/state` flow.
      const token = await signInWithOAuth(authBaseUrl, provider);
      if (client.cookieMode) {
        client.setToken(null);
      } else {
        if (typeof localStorage !== "undefined") {
          localStorage.setItem(TOKEN_STORAGE_KEY, token);
        }
        client.setToken(token);
      }
    },
    [client, authBaseUrl],
  );

  // Anonymous sign-in: a direct POST (no OAuth popup). The server mints an
  // ephemeral user + session when RTDB_AUTH_ANONYMOUS_ENABLED is on; the
  // response sets the HttpOnly cookie (cookie mode) AND returns the session
  // token (token mode). A disabled server replies 403, which rejects here.
  const signInAnonymous = useCallback(async () => {
    const resp = await fetch(`${authBaseUrl.replace(/\/+$/, "")}/auth/anonymous`, {
      method: "POST",
      credentials: "include",
    });
    if (!resp.ok) {
      throw new Error(`anonymous sign-in failed: ${resp.status}`);
    }
    if (client.cookieMode) {
      client.setToken(null);
    } else {
      const body = (await resp.json()) as { token: string };
      if (typeof localStorage !== "undefined") {
        localStorage.setItem(TOKEN_STORAGE_KEY, body.token);
      }
      client.setToken(body.token);
    }
  }, [client, authBaseUrl]);

  const signOut = useCallback(async () => {
    if (client.cookieMode) {
      // Cookie mode: the browser sends the HttpOnly cookie, the server clears
      // it, then re-dial tokenless (no cookie -> unauthenticated). No localStorage.
      await fetch(`${authBaseUrl.replace(/\/+$/, "")}/auth/logout`, {
        method: "POST",
        credentials: "include",
      }).catch(() => undefined);
      client.setToken(null);
    } else {
      const token =
        typeof localStorage !== "undefined" ? localStorage.getItem(TOKEN_STORAGE_KEY) : null;
      if (token) {
        await fetch(`${authBaseUrl.replace(/\/+$/, "")}/auth/logout`, {
          method: "POST",
          headers: { Authorization: `Bearer ${token}` },
        }).catch(() => undefined);
      }
      if (typeof localStorage !== "undefined") {
        localStorage.removeItem(TOKEN_STORAGE_KEY);
      }
      client.setToken(null);
    }
  }, [client, authBaseUrl]);

  return { state, user, signIn, signInAnonymous, signOut };
}

export function Authenticated(props: { children: ReactNode }): ReactNode {
  return useContextValue().state === "authenticated" ? props.children : null;
}

export function Unauthenticated(props: { children: ReactNode }): ReactNode {
  return useContextValue().state === "unauthenticated" ? props.children : null;
}

export function AuthLoading(props: { children: ReactNode }): ReactNode {
  return useContextValue().state === "authenticating" ? props.children : null;
}

export const OAUTH_POLL_INTERVAL_MS = 800;
export const OAUTH_POLL_TIMEOUT_MS = 180_000;

/** One poll of `/auth/state`; resolves with the token on `complete`.
 *  A transient fetch failure or malformed body keeps polling (returns
 *  `{ done: false }`); only a terminal `expired`/`error` status rejects. */
async function pollOAuthState(
  apiBase: string,
  state: string,
): Promise<{ done: true; token: string } | { done: false }> {
  let resp: Response;
  try {
    // SEC-121: send credentials so the `rtdb-oauth-state` cookie (set at /begin,
    // same value as `state`) is attached. Without it the server rejects the poll
    // as if the flow had expired — a leaked `state` URL alone cannot poll.
    resp = await fetch(`${apiBase}/auth/state?state=${encodeURIComponent(state)}`, {
      credentials: "include",
    });
  } catch {
    return { done: false };
  }
  if (!resp.ok) return { done: false };
  let data: { status?: string; token?: string };
  try {
    data = (await resp.json()) as { status?: string; token?: string };
  } catch {
    return { done: false };
  }
  if (data.status === "complete" && typeof data.token === "string") {
    return { done: true, token: data.token };
  }
  if (data.status === "expired" || data.status === "error") {
    throw new Error(data.status === "expired" ? "sign-in expired" : "sign-in failed");
  }
  return { done: false };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Begins a provider OAuth flow, opens the authorize URL in a `noopener`
 * popup (SEC-012 tabnabbing hardening), and polls `/auth/state` until the
 * session token is ready. `noopener` means `window.open` returns null, so the
 * old postMessage/closed-poll relay is replaced by this poll.
 */
function signInWithOAuth(baseUrl: string, provider: OAuthProvider): Promise<string> {
  const api = baseUrl.replace(/\/+$/, "");
  const spaOrigin = window.location.origin;
  return (async () => {
    const beginResp = await fetch(
      `${api}/auth/${provider}/begin?origin=${encodeURIComponent(spaOrigin)}`,
      { credentials: "include" },
    );
    if (!beginResp.ok) {
      throw new Error(`could not start sign-in (${beginResp.status})`);
    }
    const began = (await beginResp.json()) as { authorizeUrl: string; state: string };
    window.open(began.authorizeUrl, "rtdb-auth", "noopener,noreferrer,width=600,height=700");
    const deadline = Date.now() + OAUTH_POLL_TIMEOUT_MS;
    while (Date.now() < deadline) {
      const r = await pollOAuthState(api, began.state);
      if (r.done) return r.token;
      await sleep(OAUTH_POLL_INTERVAL_MS);
    }
    throw new Error("sign-in timed out");
  })();
}

/** Begins the GitHub OAuth flow and resolves with the session token polled from `/auth/state`. */
export function signInWithGitHub(baseUrl: string): Promise<string> {
  return signInWithOAuth(baseUrl, "github");
}

/** Begins the Google OAuth flow and resolves with the session token polled from `/auth/state`. */
export function signInWithGoogle(baseUrl: string): Promise<string> {
  return signInWithOAuth(baseUrl, "google");
}

/** Begins the generic OIDC flow and resolves with the session token polled from `/auth/state`.
 *  Active only when the server has `RTDB_OIDC_*` configured (any standards-compliant IdP). */
export function signInWithOidc(baseUrl: string): Promise<string> {
  return signInWithOAuth(baseUrl, "oidc");
}

/** Begins the GitLab OAuth flow and resolves with the session token polled from `/auth/state`. */
export function signInWithGitLab(baseUrl: string): Promise<string> {
  return signInWithOAuth(baseUrl, "gitlab");
}

/** Begins the Microsoft (Entra ID / Azure AD v2) OAuth flow and resolves with the
 *  session token polled from `/auth/state`. The server derives the authorize/token
 *  endpoints from `RTDB_MICROSOFT_TENANT`. */
export function signInWithMicrosoft(baseUrl: string): Promise<string> {
  return signInWithOAuth(baseUrl, "microsoft");
}

/** Begins the Apple OAuth flow and resolves with the session token polled from
 *  `/auth/state`. Apple uses `response_mode=form_post`, served by the server's
 *  dedicated POST `/auth/apple/callback`. */
export function signInWithApple(baseUrl: string): Promise<string> {
  return signInWithOAuth(baseUrl, "apple");
}
