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
import type { AuthedUser, TransactionJson } from "./protocol.js";
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

export function useMutation(): (txn: TransactionJson) => Promise<StepResult[]> {
  const { client } = useContextValue();
  return useCallback((txn: TransactionJson) => client.mutate(txn), [client]);
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

export function useRtDbAuth(): {
  state: AuthState;
  user: AuthedUser | null;
  signIn: (provider?: "github" | "google") => Promise<void>;
  signOut: () => Promise<void>;
} {
  const { client, authBaseUrl, state, user } = useContextValue();

  const signIn = useCallback(
    async (provider: "github" | "google" = "github") => {
      // The OAuth popup runs in both modes; the server sets the HttpOnly
      // session cookie on its callback regardless. Token mode persists the
      // posted-back token; cookie mode ignores it and re-dials tokenless so the
      // now-set cookie authenticates — no script-readable credential (SEC-002).
      const token =
        provider === "google"
          ? await signInWithGoogle(authBaseUrl)
          : await signInWithGitHub(authBaseUrl);
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

  return { state, user, signIn, signOut };
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

/** Opens the server's OAuth popup for `provider` and resolves with the session token it posts back. */
function signInWithOAuth(baseUrl: string, provider: "github" | "google"): Promise<string> {
  const origin = new URL(baseUrl).origin;
  const spaOrigin = window.location.origin;
  const popup = window.open(
    `${baseUrl.replace(/\/+$/, "")}/auth/${provider}?origin=${encodeURIComponent(spaOrigin)}`,
    "rtdb-auth",
    "width=600,height=700",
  );

  return new Promise<string>((resolve, reject) => {
    if (!popup) {
      reject(new Error("popup blocked"));
      return;
    }
    const cleanup = () => {
      window.removeEventListener("message", onMessage);
      clearInterval(closedPoll);
    };
    const onMessage = (event: MessageEvent) => {
      if (event.origin !== origin) {
        return;
      }
      const data = event.data as { type?: string; token?: string };
      if (data?.type !== "rtdb-auth" || typeof data.token !== "string") {
        return;
      }
      cleanup();
      resolve(data.token);
    };
    window.addEventListener("message", onMessage);
    const closedPoll = setInterval(() => {
      if (popup.closed) {
        cleanup();
        reject(new Error("popup closed before completing sign-in"));
      }
    }, 500);
  });
}

/** Opens the server's GitHub OAuth popup and resolves with the session token it posts back. */
export function signInWithGitHub(baseUrl: string): Promise<string> {
  return signInWithOAuth(baseUrl, "github");
}

/** Opens the server's Google OAuth popup and resolves with the session token it posts back. */
export function signInWithGoogle(baseUrl: string): Promise<string> {
  return signInWithOAuth(baseUrl, "google");
}
