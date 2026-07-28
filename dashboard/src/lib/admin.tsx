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
  FileMeta,
  HotConfigPatch,
  MetricsSnapshot,
  OpEvent,
  RtDbErrorEnvelope,
  ScheduleInfo,
  ScheduleWhen,
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
  /** Delete a database (schema + all per-db state). `confirm` must equal `name`
   *  exactly — the server's typed guard against accidental deletion. */
  deleteDb(name: string, confirm: string) {
    return this.req<{ ok: boolean }>("/admin/delete-db", {
      method: "POST",
      body: JSON.stringify({ name, confirm }),
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
  listSchedules(db: string): Promise<ScheduleInfo[]> {
    return this.req<{ schedules: ScheduleInfo[] }>(`/admin/db/${enc(db)}/schedules`).then(
      (r) => r.schedules,
    );
  }
  createSchedule(db: string, when: ScheduleWhen, txn: TransactionJson): Promise<{ id: string }> {
    return this.req<{ id: string }>(`/admin/db/${enc(db)}/schedules`, {
      method: "POST",
      body: JSON.stringify({ when, txn }),
    });
  }
  cancelSchedule(db: string, id: string): Promise<{ ok: boolean }> {
    return this.req<{ ok: boolean }>(`/admin/db/${enc(db)}/schedules/${enc(id)}/cancel`, {
      method: "POST",
    });
  }
  pauseSchedule(db: string, id: string): Promise<{ ok: boolean }> {
    return this.req<{ ok: boolean }>(`/admin/db/${enc(db)}/schedules/${enc(id)}/pause`, {
      method: "POST",
    });
  }
  resumeSchedule(db: string, id: string): Promise<{ ok: boolean }> {
    return this.req<{ ok: boolean }>(`/admin/db/${enc(db)}/schedules/${enc(id)}/resume`, {
      method: "POST",
    });
  }
  me() {
    return this.req<AuthedUser>("/auth/me");
  }
  listFiles(db: string): Promise<FileMeta[]> {
    return this.req<{ files: FileMeta[] }>(`/admin/db/${enc(db)}/storage`).then((r) => r.files);
  }
  /** Upload raw bytes (POST body is the file, not JSON). The content-type is
   *  taken from the Blob's `.type` (a File keeps its MIME type) so the server
   *  stores and serves it back. The server enforces `maxFileSize` (413/error). */
  uploadFile(db: string, body: Blob | ArrayBuffer): Promise<{ id: string }> {
    const blob = body instanceof Blob ? body : new Blob([body]);
    return this.req<{ id: string }>(`/admin/db/${enc(db)}/storage`, {
      method: "POST",
      body: blob,
      headers: { "content-type": blob.type || "application/octet-stream" },
    });
  }
  deleteFile(db: string, id: string): Promise<{ ok: boolean }> {
    return this.req<{ ok: boolean }>(`/admin/db/${enc(db)}/storage/${enc(id)}`, {
      method: "DELETE",
    });
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
 * Holds the admin client and the live rails. The op feed and metrics stream over
 * a single WebSocket to `/admin/stream` — the admin bearer rides in the
 * `Sec-WebSocket-Protocol` subprotocol (`rtdb-admin.<token>`), since browsers
 * cannot set the Authorization header on a WS handshake. Databases poll every
 * 20s (the db list isn't part of the stream). The data browser gets its own
 * realtime via `/sync`.
 */
export function AdminProvider({ children }: { children: ReactNode }) {
  const { method } = useSession();
  // SEC-001: cookie-only — the HttpOnly `rtdb_session` authenticates `/admin/*`,
  // so AdminClient sends no Bearer header (getToken -> null).
  const client = useMemo(() => new AdminClient(() => null), []);

  const [databases, setDatabases] = useState<string[]>([]);
  const [databasesLoading, setDatabasesLoading] = useState(false);
  const [databasesError, setDatabasesError] = useState<string | null>(null);
  const [ops, setOps] = useState<OpEvent[]>([]);
  const [metrics, setMetrics] = useState<MetricsSnapshot | null>(null);
  const [connection, setConnection] = useState<ConnectionState>("idle");

  const refreshDatabases = useRef<() => Promise<void>>(async () => {});

  useEffect(() => {
    if (!method) {
      setDatabases([]);
      setOps([]);
      setMetrics(null);
      setConnection("idle");
      return;
    }
    let cancelled = false;
    let ws: WebSocket | null = null;
    let reconnect: ReturnType<typeof setTimeout> | null = null;
    let backoff = 1000;
    const ok = () => {
      if (!cancelled) setConnection("connected");
    };
    const fail = () => {
      if (!cancelled) setConnection("closed");
    };

    const loadDbs = async () => {
      try {
        setDatabases((await client.listDbs()).databases);
      } catch {
        /* outages surface via the stream's connection state */
      }
    };

    const connect = () => {
      if (cancelled) return;
      setConnection((c) => (c === "connected" ? c : "connecting"));
      const proto = window.location.protocol === "https:" ? "wss" : "ws";
      const streamUrl = `${proto}://${window.location.host}/admin/stream`;
      // SEC-001: the admin credential rides an HttpOnly cookie the browser sends
      // on the WS upgrade — no JS-readable token for either auth method, so no
      // subprotocol is offered. (CLI/automation still uses the Authorization
      // header or subprotocol; the dashboard is cookie-only.)
      ws = new WebSocket(streamUrl);
      ws.onopen = () => {
        if (cancelled) return;
        backoff = 1000;
        ok();
      };
      ws.onmessage = (e) => {
        if (cancelled) return;
        let msg: { kind: string; event?: OpEvent; gauges?: MetricsSnapshot };
        try {
          msg = JSON.parse(e.data as string);
        } catch {
          return;
        }
        if (msg.kind === "op" && msg.event) {
          const ev = msg.event;
          setOps((prev) => [ev, ...prev].slice(0, OPS_CAP));
        } else if (msg.kind === "gauges" && msg.gauges) {
          setMetrics(msg.gauges);
        }
      };
      ws.onclose = () => {
        if (cancelled) return;
        fail();
        reconnect = setTimeout(connect, backoff);
        backoff = Math.min(backoff * 2, 5000);
      };
      ws.onerror = () => {
        /* browsers hide the detail; onclose drives reconnect */
      };
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
    client
      .getMetrics()
      .then(setMetrics)
      .catch(() => {});
    connect();
    const dbsTimer = setInterval(loadDbs, 20000);
    return () => {
      cancelled = true;
      if (reconnect) clearTimeout(reconnect);
      ws?.close();
      clearInterval(dbsTimer);
    };
  }, [method, client]);

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
