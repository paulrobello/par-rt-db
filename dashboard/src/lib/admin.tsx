import {
  createContext,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import type { AuthedUser, QueryJson, SchemaJson, TransactionJson } from "@par-rt-db/client";
import type { ConnectionState } from "../components/ui";
import { useSession } from "./session";
import type {
  AdminMutateResult,
  AdminQueryResult,
  ConfigResponse,
  DbStats,
  HotConfigPatch,
  MetricsSnapshot,
  OpEvent,
  RtDbErrorEnvelope,
  TokenRow,
} from "./types";

const enc = encodeURIComponent;

export class RtDbRequestError extends Error {
  code: string;
  status: number;
  constructor(code: string, status: number, message: string) {
    super(message);
    this.name = "RtDbRequestError";
    this.code = code;
    this.status = status;
  }
}

/** Admin/control-plane client. Same-origin; bearer read from the session. */
export class AdminClient {
  constructor(private readonly getToken: () => string | null) {}

  private async req<T>(path: string, init?: RequestInit): Promise<T> {
    const token = this.getToken();
    const headers: Record<string, string> = {
      "content-type": "application/json",
      ...((init?.headers as Record<string, string>) ?? {}),
    };
    if (token) headers.authorization = `Bearer ${token}`;
    const resp = await fetch(path, { ...init, headers });
    if (!resp.ok) {
      let code = "INTERNAL";
      let message = resp.statusText || `request failed (${resp.status})`;
      try {
        const body = (await resp.json()) as RtDbErrorEnvelope;
        if (body.code) code = body.code;
        if (body.message) message = body.message;
      } catch {
        /* keep defaults */
      }
      throw new RtDbRequestError(code, resp.status, message);
    }
    if (resp.status === 204) return undefined as T;
    return (await resp.json()) as T;
  }

  listDbs() {
    return this.req<{ databases: string[] }>("/admin/dbs");
  }
  createDb(name: string) {
    return this.req<{ ok: boolean }>("/admin/create-db", {
      method: "POST",
      body: JSON.stringify({ name }),
    });
  }
  getSchema(db: string) {
    return this.req<SchemaJson>(`/admin/dbs/${enc(db)}/schema`);
  }
  getStats(db: string) {
    return this.req<DbStats>(`/admin/dbs/${enc(db)}/stats`);
  }
  listTokens(db: string) {
    return this.req<{ tokens: TokenRow[] }>(`/admin/tokens?db=${enc(db)}`);
  }
  getMetrics() {
    return this.req<MetricsSnapshot>("/admin/metrics");
  }
  getConfig() {
    return this.req<ConfigResponse>("/admin/config");
  }
  patchConfig(patch: HotConfigPatch) {
    return this.req<ConfigResponse>("/admin/config", {
      method: "PATCH",
      body: JSON.stringify(patch),
    });
  }
  listAdmins() {
    return this.req<{ admins: ConfigResponse["admins"] }>("/admin/admins");
  }
  addAdmin(email: string, githubId?: number) {
    return this.req<{ ok: boolean }>("/admin/admins", {
      method: "POST",
      body: JSON.stringify({ email, githubId }),
    });
  }
  removeAdmin(email: string) {
    return this.req<{ ok: boolean }>("/admin/admins", {
      method: "DELETE",
      body: JSON.stringify({ email }),
    });
  }
  getOpsRecent(filter?: { db?: string; table?: string; n?: number }) {
    const q = new URLSearchParams();
    if (filter?.db) q.set("db", filter.db);
    if (filter?.table) q.set("table", filter.table);
    q.set("n", String(filter?.n ?? 100));
    return this.req<{ ops: OpEvent[] }>(`/admin/ops/recent?${q.toString()}`);
  }
  adminQuery(db: string, query: QueryJson): Promise<AdminQueryResult> {
    return this.req<AdminQueryResult>(`/admin/db/${enc(db)}/query`, {
      method: "POST",
      body: JSON.stringify({ query }),
    });
  }
  adminMutate(
    db: string,
    txn: TransactionJson,
    idempotencyKey?: string,
  ): Promise<AdminMutateResult> {
    return this.req<AdminMutateResult>(`/admin/db/${enc(db)}/mutate`, {
      method: "POST",
      body: JSON.stringify({ txn, idempotencyKey }),
    });
  }
  me() {
    return this.req<AuthedUser>("/auth/me");
  }
}

const OPS_CAP = 100;

export interface AdminValue {
  client: AdminClient;
  databases: string[];
  databasesLoading: boolean;
  databasesError: string | null;
  refreshDatabases: () => Promise<void>;
  /** Newest-first op events, capped. */
  ops: OpEvent[];
  metrics: MetricsSnapshot | null;
  connection: ConnectionState;
}

const AdminContext = createContext<AdminValue | null>(null);

export function useAdmin(): AdminValue {
  const v = useContext(AdminContext);
  if (!v) throw new Error("useAdmin must be used within AdminProvider");
  return v;
}

/**
 * Holds the admin client and the live, polled rails (databases, op feed,
 * metrics). The op feed and metrics poll at ~1s because `/admin/stream` is
 * header-gated at the WS upgrade and browsers cannot set headers on a WebSocket
 * — true streaming needs a server-side browser-auth seam (query-param or
 * subprotocol token), flagged for later. The data browser still gets true
 * realtime via `/sync` (in-band auth).
 */
export function AdminProvider({ children }: { children: ReactNode }) {
  const { token } = useSession();
  const client = useMemo(() => new AdminClient(() => token), [token]);

  const [databases, setDatabases] = useState<string[]>([]);
  const [databasesLoading, setDatabasesLoading] = useState(false);
  const [databasesError, setDatabasesError] = useState<string | null>(null);
  const [ops, setOps] = useState<OpEvent[]>([]);
  const [metrics, setMetrics] = useState<MetricsSnapshot | null>(null);
  const [connection, setConnection] = useState<ConnectionState>("idle");

  const refreshDatabases = useRef<() => Promise<void>>(async () => {});

  useEffect(() => {
    if (!token) {
      setDatabases([]);
      setOps([]);
      setMetrics(null);
      setConnection("idle");
      return;
    }
    let cancelled = false;
    setConnection("connecting");
    const ok = () => {
      if (!cancelled) setConnection("connected");
    };
    const fail = () => {
      if (!cancelled) setConnection("closed");
    };

    const loadDbs = async () => {
      try {
        setDatabases((await client.listDbs()).databases);
        ok();
      } catch {
        fail();
      }
    };
    const loadOps = async () => {
      try {
        const { ops: fresh } = await client.getOpsRecent({ n: OPS_CAP });
        if (!cancelled) setOps(fresh.slice().reverse());
        ok();
      } catch {
        fail();
      }
    };
    const loadMetrics = async () => {
      try {
        const m = await client.getMetrics();
        if (!cancelled) setMetrics(m);
      } catch {
        /* metrics are non-fatal for connection health */
      }
    };

    refreshDatabases.current = async () => {
      setDatabasesLoading(true);
      setDatabasesError(null);
      try {
        setDatabases((await client.listDbs()).databases);
        ok();
      } catch (e) {
        setDatabasesError(e instanceof Error ? e.message : String(e));
        fail();
      } finally {
        setDatabasesLoading(false);
      }
    };

    loadDbs();
    loadOps();
    loadMetrics();
    const dbsTimer = setInterval(loadDbs, 20000);
    const opsTimer = setInterval(loadOps, 1000);
    const metricsTimer = setInterval(loadMetrics, 1000);
    return () => {
      cancelled = true;
      clearInterval(dbsTimer);
      clearInterval(opsTimer);
      clearInterval(metricsTimer);
    };
  }, [token, client]);

  const value: AdminValue = {
    client,
    databases,
    databasesLoading,
    databasesError,
    refreshDatabases: () => refreshDatabases.current(),
    ops,
    metrics,
    connection,
  };

  return <AdminContext.Provider value={value}>{children}</AdminContext.Provider>;
}
