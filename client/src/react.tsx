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
import type { AuthState, RtDbClient } from "./client.js";
import type { AuthedUser, TransactionJson } from "./protocol.js";
import type { RtQuery } from "./query.js";

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
    const stored =
      typeof localStorage !== "undefined" ? localStorage.getItem(TOKEN_STORAGE_KEY) : null;
    if (stored) {
      client.setToken(stored);
    }
    const off = client.onAuthChange((nextState, nextUser) => {
      setState(nextState);
      setUser(nextUser);
    });
    client.connect();
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
    const off = client.subscribe(query, (next) => setValue(next));
    return off;
  }, [client, key]);

  return value;
}

export function useMutation(): (txn: TransactionJson) => Promise<unknown[]> {
  const { client } = useContextValue();
  return useCallback((txn: TransactionJson) => client.mutate(txn), [client]);
}

export function useRtDbAuth(): {
  state: AuthState;
  user: AuthedUser | null;
  signIn: () => Promise<void>;
  signOut: () => Promise<void>;
} {
  const { client, authBaseUrl, state, user } = useContextValue();

  const signIn = useCallback(async () => {
    const token = await signInWithGitHub(authBaseUrl);
    if (typeof localStorage !== "undefined") {
      localStorage.setItem(TOKEN_STORAGE_KEY, token);
    }
    client.setToken(token);
  }, [client, authBaseUrl]);

  const signOut = useCallback(async () => {
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

/** Opens the server's GitHub OAuth popup and resolves with the session token it posts back. */
export function signInWithGitHub(baseUrl: string): Promise<string> {
  const origin = new URL(baseUrl).origin;
  const spaOrigin = window.location.origin;
  const popup = window.open(
    `${baseUrl.replace(/\/+$/, "")}/auth/github?origin=${encodeURIComponent(spaOrigin)}`,
    "rtdb-auth",
    "width=600,height=700",
  );

  return new Promise<string>((resolve, reject) => {
    if (!popup) {
      reject(new Error("popup blocked"));
      return;
    }
    const onMessage = (event: MessageEvent) => {
      if (event.origin !== origin) {
        return;
      }
      const data = event.data as { type?: string; token?: string };
      if (data?.type !== "rtdb-auth" || typeof data.token !== "string") {
        return;
      }
      window.removeEventListener("message", onMessage);
      resolve(data.token);
    };
    window.addEventListener("message", onMessage);
  });
}
