import type {
  AuthedUser,
  MigrateRequestJson,
  MigrateResultJson,
  QueryJson,
  SchemaHistoryEntry,
  SchemaHistoryEntrySummary,
  SchemaJson,
  TransactionJson,
} from "@par-rt-db/client";
import {
  createContext,
  type ReactNode,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type { ConnectionState } from "../components/ui";
import { useSession } from "./session";
import type {
  AdminMutateResult,
  AdminQueryResult,
  AuditEntry,
  ConfigResponse,
  CreateWebhookOptions,
  DbStats,
  EditWebhookOptions,
  FileMeta,
  GetAuditOptions,
  HotConfigPatch,
  ListDeliveriesOptions,
  MetricsSnapshot,
  OpEvent,
  RtDbErrorEnvelope,
  ScheduleInfo,
  ScheduleWhen,
  SchemaDiff,
  SessionRow,
  SubscriptionsResponse,
  TokenRow,
  Webhook,
  WebhookDelivery,
} from "./types";

const enc = encodeURIComponent;

/** Build a request body from only the keys on `opts` whose value is not
 *  `undefined`. Used by webhook create/edit so omitted fields are absent on the
 *  wire (server default applies) while an explicit `null` (e.g. clearing a
 *  webhook's `table` to all-tables) is preserved as JSON `null`. Mirrors the
 *  ts-client's `pickDefined`. */
function pickDefined<T extends object>(opts: T): Partial<T> {
  const out: { [K in keyof T]: T[K] } = {} as Partial<T> as {
    [K in keyof T]: T[K];
  };
  for (const key of Object.keys(opts) as (keyof T)[]) {
    const v = opts[key];
    if (v !== undefined) out[key] = v;
  }
  return out;
}

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

export interface BackupFile {
  name: string;
  sizeBytes: number;
  createdMs: number;
}
export interface BackupsListResponse {
  running: boolean;
  backups: BackupFile[];
}
export interface RestoreResult {
  target: string;
  instructions: string;
}

/** Reads the readable (non-HttpOnly) `rtdb-admin-csrf` cookie set alongside
 *  the session cookie by `/admin/login` and the OAuth callback (SEC-106).
 *  Returns null when no admin session is active, or when running where
 *  `document` is undefined (SSR). The value is echoed in the `X-Rtdb-Csrf`
 *  header on admin requests — see `AdminClient.req`. */
function readAdminCsrfCookie(): string | null {
  if (typeof document === "undefined") return null;
  const match = document.cookie.match(/(?:^|;\s*)rtdb-admin-csrf=([^;]+)/);
  return match ? match[1] : null;
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
    // SEC-106: echo the readable admin-CSRF nonce on every admin request. The
    // server enforces it on cookie-authenticated mutating requests; sending it
    // on bearer-authenticated or GET requests is harmless (the server skips
    // those branches). A cross-site forge cannot read this cookie and so
    // cannot set the header — forcing a preflight that the CORS allowlist then
    // gates.
    const csrf = readAdminCsrfCookie();
    if (csrf) headers["x-rtdb-csrf"] = csrf;
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

  /** Bearer header for non-JSON fetches (e.g. blob downloads) — null token
   *  yields {} so the same-origin HttpOnly session cookie authenticates. */
  private authHeader(): Record<string, string> {
    const token = this.getToken();
    return token ? { authorization: `Bearer ${token}` } : {};
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
  /** Push a schema (additive-only; the server rejects drops/type-changes). */
  pushSchema(db: string, schema: SchemaJson) {
    return this.req<{ ok: boolean }>("/admin/push-schema", {
      method: "POST",
      body: JSON.stringify({ db, schema }),
    });
  }
  /** Preview an additive-only schema diff against the currently-applied schema.
   *  Pure/advisory — does NOT apply. Returns what the push would ADD and what
   *  it would REJECT (drops, type changes). */
  previewSchema(db: string, schema: SchemaJson): Promise<SchemaDiff> {
    return this.req<SchemaDiff>(`/admin/db/${enc(db)}/schema/preview`, {
      method: "POST",
      body: JSON.stringify({ schema }),
    });
  }
  /** Apply (or preview) a declarative schema migration
   *  (POST /admin/db/{db}/migrate). `req.dryRun` reports `affectedRows` and the
   *  derived `schema` without committing; a real run returns `applied: true`
   *  with the installed schema. */
  migrate(db: string, req: MigrateRequestJson): Promise<MigrateResultJson> {
    return this.req<MigrateResultJson>(`/admin/db/${enc(db)}/migrate`, {
      method: "POST",
      body: JSON.stringify(req),
    });
  }
  /** Schema snapshot history, newest-first
   *  (GET /admin/db/{db}/schema/history). Each entry is metadata-only (no
   *  schema blob); fetch the full snapshot with `getSchemaVersion`.
   *  `limit`/`offset` page. */
  getSchemaHistory(
    db: string,
    opts: { limit?: number; offset?: number } = {},
  ): Promise<SchemaHistoryEntrySummary[]> {
    const params = new URLSearchParams();
    if (opts.limit !== undefined) params.set("limit", String(opts.limit));
    if (opts.offset !== undefined) params.set("offset", String(opts.offset));
    const qs = params.toString();
    return this.req<{ entries: SchemaHistoryEntrySummary[] }>(
      `/admin/db/${enc(db)}/schema/history${qs ? `?${qs}` : ""}`,
    ).then((r) => r.entries);
  }
  /** One full schema snapshot
   *  (GET /admin/db/{db}/schema/history/{version}), including the `schema`
   *  blob. */
  getSchemaVersion(db: string, version: number): Promise<SchemaHistoryEntry> {
    return this.req<SchemaHistoryEntry>(`/admin/db/${enc(db)}/schema/history/${version}`);
  }
  /** Restore the live schema shape to a prior snapshot
   *  (POST /admin/db/{db}/schema/restore). `confirm` must equal the db name
   *  (typed guard, mirrors delete-db). The outgoing schema is captured first,
   *  so a restore is itself undoable. */
  restoreSchema(
    db: string,
    version: number,
    confirm: string,
  ): Promise<{ ok: boolean; restoredTo: number }> {
    return this.req(`/admin/db/${enc(db)}/schema/restore`, {
      method: "POST",
      body: JSON.stringify({ version, confirm }),
    });
  }
  getStats(db: string) {
    return this.req<DbStats>(`/admin/dbs/${enc(db)}/stats`);
  }
  listTokens(db: string) {
    return this.req<{ tokens: TokenRow[] }>(`/admin/tokens?db=${enc(db)}`);
  }
  /** Mint a scoped/time-limited machine token. Omitted capability fields fall
   *  back to server defaults (no expiry, read-write, all tables). The plaintext
   *  `token` is returned ONLY here — the server stores a hash, so it cannot be
   *  recovered; surface it for one-time copy in the UI. */
  mintToken(
    db: string,
    name: string,
    opts: { expiresAt?: number; readOnly?: boolean; tables?: string[] } = {},
  ): Promise<{ tokenId: string; token: string }> {
    return this.req<{ tokenId: string; token: string }>("/admin/mint-token", {
      method: "POST",
      body: JSON.stringify({ db, name, ...opts }),
    });
  }
  revokeToken(tokenId: string): Promise<{ ok: boolean }> {
    return this.req<{ ok: boolean }>("/admin/revoke-token", {
      method: "POST",
      body: JSON.stringify({ tokenId }),
    });
  }
  /** List active sessions server-wide, optionally narrowed by a user id or
   *  email substring (GET /admin/sessions?user=&limit=). `filter.user` is
   *  matched against both user id and email on the server. */
  listSessions(filter?: { user?: string; limit?: number }): Promise<{ sessions: SessionRow[] }> {
    const qs = new URLSearchParams();
    if (filter?.user) qs.set("user", filter.user);
    if (filter?.limit !== undefined) qs.set("limit", String(filter.limit));
    const suffix = qs.toString() ? `?${qs}` : "";
    return this.req<{ sessions: SessionRow[] }>(`/admin/sessions${suffix}`);
  }
  /** Revoke a single session by its token hash (DELETE /admin/sessions/{hash}).
   *  The cookie/token stops authenticating immediately. */
  revokeSession(tokenHash: string): Promise<{ ok: boolean }> {
    return this.req<{ ok: boolean }>(`/admin/sessions/${enc(tokenHash)}`, {
      method: "DELETE",
    });
  }
  /** Revoke every session belonging to a user id
   *  (DELETE /admin/sessions?user={userId}). Returns how many were revoked. */
  revokeUserSessions(userId: string): Promise<{ ok: boolean; revoked: number }> {
    return this.req<{ ok: boolean; revoked: number }>(`/admin/sessions?user=${enc(userId)}`, {
      method: "DELETE",
    });
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
  /** Trigger an on-demand backup (POST /admin/backup → 202). */
  backupNow(): Promise<void> {
    return this.req("/admin/backup", { method: "POST", body: "{}" });
  }
  /** List existing backup files + whether a backup is currently running. */
  listBackups(): Promise<BackupsListResponse> {
    return this.req<BackupsListResponse>("/admin/backups");
  }
  /** Download a backup as a binary blob and trigger a browser download. */
  async downloadBackup(name: string): Promise<void> {
    const resp = await fetch(`/admin/backups/${enc(name)}`, {
      headers: this.authHeader(),
    });
    if (!resp.ok) throw new Error(`download failed (${resp.status})`);
    const blob = await resp.blob();
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = name;
    a.click();
    URL.revokeObjectURL(url);
  }
  deleteBackup(name: string): Promise<void> {
    return this.req(`/admin/backups/${enc(name)}`, { method: "DELETE" });
  }
  /** Restore a backup by exact name (confirm must equal name). Returns the
   *  cutover instructions; the server writes nothing — restore is offline. */
  restoreBackup(name: string): Promise<RestoreResult> {
    return this.req<RestoreResult>("/admin/restore", {
      method: "POST",
      body: JSON.stringify({ name, confirm: name }),
    });
  }
  /** List webhooks registered for `db` (GET /admin/db/{db}/webhooks). Returns
   *  an empty array when webhooks are disabled at boot — the table may not
   *  exist. */
  listWebhooks(db: string): Promise<Webhook[]> {
    return this.req<{ webhooks: Webhook[] }>(`/admin/db/${enc(db)}/webhooks`).then(
      (r) => r.webhooks,
    );
  }
  /** Register a webhook for `db` (POST /admin/db/{db}/webhooks). Only the
   *  provided option keys are sent; the server defaults `table` to all-tables,
   *  `events` to `["*"]`, and `enabled` to true when their keys are absent.
   *  Returns the new id. */
  createWebhook(db: string, opts: CreateWebhookOptions): Promise<{ id: number }> {
    return this.req<{ id: number }>(`/admin/db/${enc(db)}/webhooks`, {
      method: "POST",
      body: JSON.stringify(pickDefined(opts)),
    });
  }
  /** Partial-edit a webhook (PUT /admin/db/{db}/webhooks/{id}). Each present
   *  field overwrites the stored value; absent fields are unchanged. `table`
   *  is a tri-state: omitted leaves it alone, `null` clears it to all-tables, a
   *  string sets it. Returns the updated webhook. */
  editWebhook(db: string, id: number, opts: EditWebhookOptions): Promise<Webhook> {
    return this.req<Webhook>(`/admin/db/${enc(db)}/webhooks/${enc(id)}`, {
      method: "PUT",
      body: JSON.stringify(pickDefined(opts)),
    });
  }
  /** Delete a webhook and cascade its pending deliveries
   *  (DELETE /admin/db/{db}/webhooks/{id}). */
  deleteWebhook(db: string, id: number): Promise<{ ok: boolean }> {
    return this.req<{ ok: boolean }>(`/admin/db/${enc(db)}/webhooks/${enc(id)}`, {
      method: "DELETE",
    });
  }
  /** List a webhook's delivery outbox newest-first
   *  (GET /admin/db/{db}/webhooks/{id}/deliveries?status=&limit=&offset=).
   *  `status` filters by `pending|retrying|delivered|failed`; `limit`/`offset`
   *  page. */
  listDeliveries(
    db: string,
    id: number,
    opts: ListDeliveriesOptions = {},
  ): Promise<WebhookDelivery[]> {
    const params = new URLSearchParams();
    if (opts.status) params.set("status", opts.status);
    if (opts.limit !== undefined) params.set("limit", String(opts.limit));
    if (opts.offset !== undefined) params.set("offset", String(opts.offset));
    const qs = params.toString();
    return this.req<{ deliveries: WebhookDelivery[] }>(
      `/admin/db/${enc(db)}/webhooks/${enc(id)}/deliveries${qs ? `?${qs}` : ""}`,
    ).then((r) => r.deliveries);
  }
  /** Durable audit-log entries, newest-first (GET /admin/audit). Each of
   *  `db`/`table`/`op`/`principal`/`source` is an optional equality filter
   *  (combined with AND); `limit`/`offset` page (`!== undefined` so an explicit
   *  0 survives). Returns an empty array when audit logging is disabled at boot
   *  — the table may not exist. */
  getAudit(opts: GetAuditOptions = {}): Promise<AuditEntry[]> {
    const params = new URLSearchParams();
    if (opts.db) params.set("db", opts.db);
    if (opts.table) params.set("table", opts.table);
    if (opts.op) params.set("op", opts.op);
    if (opts.principal) params.set("principal", opts.principal);
    if (opts.source) params.set("source", opts.source);
    if (opts.limit !== undefined) params.set("limit", String(opts.limit));
    if (opts.offset !== undefined) params.set("offset", String(opts.offset));
    const qs = params.toString();
    return this.req<{ entries: AuditEntry[] }>(`/admin/audit${qs ? `?${qs}` : ""}`).then(
      (r) => r.entries,
    );
  }
  /** Live subscription inspector (GET /admin/subscriptions). `db` is optional —
   *  omit it to list across every database. Returns the per-subscription rows
   *  plus the global and per-db invalidation counters that explain re-run
   *  behavior. */
  getSubscriptions(opts: { db?: string } = {}): Promise<SubscriptionsResponse> {
    const params = new URLSearchParams();
    if (opts.db) params.set("db", opts.db);
    const qs = params.toString();
    return this.req<SubscriptionsResponse>(`/admin/subscriptions${qs ? `?${qs}` : ""}`);
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
