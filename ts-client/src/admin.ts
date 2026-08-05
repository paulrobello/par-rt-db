import { RtDbError } from "./errors.js";
import type {
  MigrateRequestJson,
  MigrateResultJson,
  SchemaJson,
  TransactionJson,
} from "./protocol.js";
import type { WebSocketLike } from "./client.js";
import type { RtQuery } from "./query.js";
import type { SchemaDefinition } from "./schema.js";

export interface RtDbAdminClientOptions {
  url: string;
  adminKey: string;
  fetch?: typeof fetch;
  /** Injectable WebSocket constructor (browser/Node/bun). Defaults to the global
   *  `WebSocket`. The second arg carries the WS subprotocol(s) — `/admin/stream`
   *  authenticates via the `rtdb-admin.<token>` subprotocol. */
  webSocketFactory?: (url: string, protocols?: string | string[]) => WebSocketLike;
}

export interface AdminMember {
  email: string;
  githubId?: number;
}

export interface TableStat {
  name: string;
  rowCount: number;
  sizeBytes: number;
}
export interface DbStats {
  tables: TableStat[];
  totalSizeBytes: number;
}
export interface TokenInfo {
  id: string;
  name: string;
  createdAt: number;
  revoked: boolean;
  /** Server always sends these three; `null` means "no limit" (full access). */
  expiresAt: number | null;
  readOnly: boolean;
  tables: string[] | null;
}
/** Optional capabilities for `mintToken`. Omitted fields fall back to server
 *  defaults (full access: no expiry, read-write, all tables). */
export interface MintTokenOptions {
  expiresAt?: number;
  readOnly?: boolean;
  tables?: string[];
}
export interface LatencyStats {
  p50: number;
  p95: number;
  p99: number;
}
export interface MetricsSnapshot {
  queriesTotal: number;
  mutationsTotal: number;
  uploadsTotal: number;
  wsConnections: number;
  activeSubscriptions: number;
  poolSize: number;
  poolIdle: number;
  uptimeSeconds: number;
  queryLatency: LatencyStats;
  mutateLatency: LatencyStats;
  subscribeLatency: LatencyStats;
  /**
   * Subscription-invalidation effectiveness. Counted per subscription whose
   * table was written, so `reruns + skips` is the number of read-set decisions
   * made; a skip means the read set proved every written document irrelevant.
   * Skips are split by the class that proved it: `point` = `get(id)` reads,
   * `indexed` = count/collect/unique over an eq-prefix window, `ordered` =
   * take/first/paginate bounded by a top-N sort boundary.
   */
  subsRerunsTotal: number;
  subsSkipsPointTotal: number;
  subsSkipsIndexedTotal: number;
  subsSkipsOrderedTotal: number;
  /**
   * Sampled shadow verifications of skips (server-side `RTDB_SUBS_VERIFY_SKIP_EVERY`),
   * and how many found the skip was WRONG. `subsMissedPushesTotal > 0` means
   * invalidation under-approximated and a realtime update would have been
   * dropped — a correctness defect, not a tuning signal.
   */
  subsSkipVerificationsTotal: number;
  subsMissedPushesTotal: number;
}
export interface HotConfig {
  allowedOrigins: string[];
  sessionTtlDays: number;
  maxFileSize: number;
  idempotencyTtlMs: number;
}
export interface ConfigResponse {
  port: number;
  publicUrl: string;
  githubBaseUrl: string;
  githubApiUrl: string;
  databaseUrlConfigured: boolean;
  adminKeyConfigured: boolean;
  githubConfigured: boolean;
  googleConfigured: boolean;
  gitlabConfigured: boolean;
  oidcConfigured: boolean;
  hot: HotConfig;
  version: string;
  gitCommit: string;
  admins: AdminMember[];
}
export interface HotConfigPatch {
  allowedOrigins?: string[];
  sessionTtlDays?: number;
  maxFileSize?: number;
  idempotencyTtlMs?: number;
}
export type OpEventKind = "insert" | "patch" | "replace" | "delete" | "upsert";
export interface OpEvent {
  db: string;
  table: string;
  docId: string;
  kind: OpEventKind;
  ts: number;
  owner?: string | null;
}
/** A frame on the `/admin/stream` op-feed: a document op event (replay then live),
 *  or a ~1s server metrics snapshot. */
export type AdminStreamFrame =
  | { kind: "op"; event: OpEvent }
  | { kind: "gauges"; gauges: MetricsSnapshot };

/** One durable row from the audit log (`GET /admin/audit`). `op` is `null` for
 *  rows the server could not label with a kind; `principal` is `null` for
 *  system-initiated writes (TTL reaper, scheduled jobs) where there is no
 *  interactive user. */
export interface AuditEntry {
  id: number;
  tsMs: number;
  db: string;
  table: string;
  op: string | null;
  docId: string;
  principal: string | null;
  source: string;
}

/** Options for `getAudit`. All optional; omitted filters match all rows, and
 *  omitted paging falls back to server defaults (limit 100, offset 0). */
export interface GetAuditOptions {
  db?: string;
  table?: string;
  op?: string;
  principal?: string;
  source?: string;
  limit?: number;
  offset?: number;
}

/** One managed pg_dump file on disk (GET /admin/backups). */
export interface BackupFile {
  name: string;
  sizeBytes: number;
  createdMs: number;
}
/** Response from GET /admin/backups: existing dumps and whether one is running. */
export interface BackupsListResponse {
  running: boolean;
  backups: BackupFile[];
}
/** Response from POST /admin/restore: the fresh rtdb_restored_<stamp> DB and follow-up. */
export interface RestoreResult {
  target: string;
  instructions: string;
}

/** One registered webhook. `table: null` means "all tables"; `events` carries op
 *  names (`insert`/`patch`/`replace`/`delete`/`upsert`) or the single element
 *  `["*"]` to match every event. */
export interface Webhook {
  id: number;
  db: string;
  table: string | null;
  url: string;
  events: string[];
  createdAt: number;
  enabled: boolean;
}

/** One delivery row from a webhook's outbox (`GET .../webhooks/{id}/deliveries`).
 *  `payload` is the raw JSON body the worker POSTed (or will POST) — typed
 *  `unknown` because the server passes it through verbatim. */
export interface WebhookDelivery {
  id: number;
  attempts: number;
  status: string;
  nextAttempt: number;
  lastError: string | null;
  payload: unknown;
}

/** Options for `createWebhook`. `url` is required; the rest fall back to server
 *  defaults (all tables, `["*"]` events, enabled). */
export interface CreateWebhookOptions {
  url: string;
  table?: string | null;
  events?: string[];
  enabled?: boolean;
}

/** Options for `editWebhook`. Every field is optional: omitted = unchanged.
 *  `table` is a tri-state — omit to leave the filter alone, `null` to clear it
 *  to all-tables, or a string to set it. */
export interface EditWebhookOptions {
  url?: string;
  table?: string | null;
  events?: string[];
  enabled?: boolean;
}

/** Options for `listDeliveries`. All optional; omitted filters/limits mean: no
 *  status filter, server default limit, offset 0. */
export interface ListDeliveriesOptions {
  status?: string;
  limit?: number;
  offset?: number;
}

function toSchemaJson(schema: SchemaDefinition<any> | SchemaJson): SchemaJson {
  return "toJSON" in schema && typeof schema.toJSON === "function"
    ? schema.toJSON()
    : (schema as SchemaJson);
}

/** Build a request body from only the keys present on `opts` whose value is not
 *  `undefined`. Used by webhook create/edit so omitted fields are absent on the
 *  wire (server default applies) while an explicit `null` (e.g. clearing a
 *  webhook's `table`) is preserved as JSON `null`. */
function pickDefined<T extends object>(opts: T): Partial<T> {
  const out: { [K in keyof T]: T[K] } = {} as Partial<T> as { [K in keyof T]: T[K] };
  for (const key of Object.keys(opts) as (keyof T)[]) {
    const v = opts[key];
    if (v !== undefined) {
      out[key] = v;
    }
  }
  return out;
}

/** Parse one `/admin/stream` text frame into a typed `AdminStreamFrame`, or `null`
 *  for a malformed/non-string message (ignored, never breaks the stream). */
function parseAdminStreamFrame(data: unknown): AdminStreamFrame | null {
  if (typeof data !== "string") return null;
  let parsed: { kind?: string; event?: OpEvent; gauges?: MetricsSnapshot };
  try {
    parsed = JSON.parse(data);
  } catch {
    return null;
  }
  if (parsed.kind === "op" && parsed.event) {
    return { kind: "op", event: parsed.event };
  }
  if (parsed.kind === "gauges" && parsed.gauges) {
    return { kind: "gauges", gauges: parsed.gauges };
  }
  return null;
}

/** Control-plane client for `/admin/*`, authorized with the instance admin key. */
export class RtDbAdminClient {
  private readonly url: string;
  private readonly adminKey: string;
  private readonly fetchImpl: typeof fetch;
  private readonly webSocketFactory: (url: string, protocols?: string | string[]) => WebSocketLike;

  constructor(options: RtDbAdminClientOptions) {
    this.url = options.url.replace(/\/+$/, "");
    this.adminKey = options.adminKey;
    this.fetchImpl = options.fetch ?? globalThis.fetch;
    this.webSocketFactory =
      options.webSocketFactory ??
      ((url, protocols) => new WebSocket(url, protocols) as unknown as WebSocketLike);
  }

  async createDb(name: string): Promise<void> {
    await this.request("POST", "/admin/create-db", { name });
  }

  /** Delete a database (schema + all per-db state). `confirm` must equal `name`
   *  exactly — the server's typed guard against accidental deletion. */
  async deleteDb(name: string, confirm: string): Promise<void> {
    await this.request("POST", "/admin/delete-db", { name, confirm });
  }

  async pushSchema(db: string, schema: SchemaDefinition<any> | SchemaJson): Promise<void> {
    await this.request("POST", "/admin/push-schema", { db, schema: toSchemaJson(schema) });
  }

  async listDbs(): Promise<string[]> {
    const body = await this.request("GET", "/admin/dbs");
    return (body as { databases: string[] }).databases;
  }

  async mintToken(
    db: string,
    name: string,
    opts: MintTokenOptions = {},
  ): Promise<{ tokenId: string; token: string }> {
    const body = await this.request("POST", "/admin/mint-token", { db, name, ...opts });
    return body as { tokenId: string; token: string };
  }

  async revokeToken(tokenId: string): Promise<void> {
    await this.request("POST", "/admin/revoke-token", { tokenId });
  }

  async allowlistAdd(db: string, email: string): Promise<void> {
    await this.request("POST", "/admin/allowlist", { db, action: "add", email });
  }

  async allowlistRemove(db: string, email: string): Promise<void> {
    await this.request("POST", "/admin/allowlist", { db, action: "remove", email });
  }

  async allowlistList(db: string): Promise<string[]> {
    const body = await this.request("GET", `/admin/allowlist?db=${encodeURIComponent(db)}`);
    return (body as { emails: string[] }).emails;
  }

  /** Cookie-session login (POST /admin/login). Sets the server's HttpOnly `rtdb_session`
   *  cookie on 204. A browser auto-attaches the cookie thereafter; a Node caller must wire
   *  its own cookie jar onto the injected `fetch` to reuse the session. */
  async login(adminKey: string): Promise<void> {
    await this.request("POST", "/admin/login", { adminKey });
  }

  /** Clear the admin session cookie (POST /admin/logout, always 204). */
  async logout(): Promise<void> {
    await this.request("POST", "/admin/logout");
  }

  /** List server-wide dashboard admin emails (GET /admin/admins). */
  async adminsList(): Promise<AdminMember[]> {
    const body = await this.request("GET", "/admin/admins");
    return (body as { admins: AdminMember[] }).admins;
  }

  /** Add (or upsert) a dashboard admin (POST /admin/admins). */
  async addAdmin(email: string, githubId?: number): Promise<void> {
    await this.request(
      "POST",
      "/admin/admins",
      githubId === undefined ? { email } : { email, githubId },
    );
  }

  /** Remove a dashboard admin (DELETE /admin/admins, body-on-DELETE). */
  async removeAdmin(email: string): Promise<void> {
    await this.request("DELETE", "/admin/admins", { email });
  }

  /** Fetches `db`'s schema and every document as JSONL text (see server `snapshot::export_database`). */
  async exportDb(db: string): Promise<string> {
    const response = await this.fetchImpl(
      `${this.url}/admin/export-db?db=${encodeURIComponent(db)}`,
      {
        method: "GET",
        headers: { Authorization: `Bearer ${this.adminKey}` },
      },
    );
    if (!response.ok) {
      await this.throwFromResponse(response);
    }
    return await response.text();
  }

  /** Loads a JSONL snapshot from `exportDb` into `db` (see server `snapshot::import_database`). */
  async importDb(db: string, jsonl: string): Promise<void> {
    const response = await this.fetchImpl(
      `${this.url}/admin/import-db?db=${encodeURIComponent(db)}`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${this.adminKey}`,
          "content-type": "application/x-ndjson",
        },
        body: jsonl,
      },
    );
    if (!response.ok) {
      await this.throwFromResponse(response);
    }
  }

  /** Read a database's pushed schema (GET /admin/dbs/{db}/schema). */
  async getSchema(db: string): Promise<SchemaJson> {
    return (await this.request("GET", `/admin/dbs/${encodeURIComponent(db)}/schema`)) as SchemaJson;
  }

  /** Per-table row counts + storage sizes (GET /admin/dbs/{db}/stats). */
  async dbStats(db: string): Promise<DbStats> {
    return (await this.request("GET", `/admin/dbs/${encodeURIComponent(db)}/stats`)) as DbStats;
  }

  /** List tokens for a database, no secrets (GET /admin/tokens?db=). */
  async listTokens(db: string): Promise<TokenInfo[]> {
    const body = await this.request("GET", `/admin/tokens?db=${encodeURIComponent(db)}`);
    return (body as { tokens: TokenInfo[] }).tokens;
  }

  /** Server metrics snapshot (GET /admin/metrics). */
  async metrics(): Promise<MetricsSnapshot> {
    return (await this.request("GET", "/admin/metrics")) as MetricsSnapshot;
  }

  /** Redacted server config (GET /admin/config). Secrets surface as configured-bools, not values. */
  async getConfig(): Promise<ConfigResponse> {
    return (await this.request("GET", "/admin/config")) as ConfigResponse;
  }

  /** Patch hot-reloadable config (PATCH /admin/config). Each present field fully replaces the
   *  prior value; the server validates (sessionTtlDays>=1, maxFileSize within bounds, origin shape). */
  async patchConfig(patch: HotConfigPatch): Promise<ConfigResponse> {
    return (await this.request("PATCH", "/admin/config", patch)) as ConfigResponse;
  }

  /** Recent op-feed events, newest-first (GET /admin/ops/recent). All filter opts optional. */
  async opsRecent(opts?: { db?: string; table?: string; n?: number }): Promise<OpEvent[]> {
    const params = new URLSearchParams();
    if (opts?.db) params.set("db", opts.db);
    if (opts?.table) params.set("table", opts.table);
    if (opts?.n !== undefined) params.set("n", String(opts.n));
    const qs = params.toString();
    const body = await this.request("GET", `/admin/ops/recent${qs ? `?${qs}` : ""}`);
    return (body as { ops: OpEvent[] }).ops;
  }

  /** Durable audit-log entries, newest-first (GET /admin/audit). Each of
   *  `db`/`table`/`op`/`principal`/`source` is an optional equality filter
   *  (combined with AND); `limit`/`offset` page. Returns an empty array when
   *  audit logging is disabled at boot — the table may not exist. */
  async getAudit(opts?: GetAuditOptions): Promise<AuditEntry[]> {
    const params = new URLSearchParams();
    if (opts?.db) params.set("db", opts.db);
    if (opts?.table) params.set("table", opts.table);
    if (opts?.op) params.set("op", opts.op);
    if (opts?.principal) params.set("principal", opts.principal);
    if (opts?.source) params.set("source", opts.source);
    if (opts?.limit !== undefined) params.set("limit", String(opts.limit));
    if (opts?.offset !== undefined) params.set("offset", String(opts.offset));
    const qs = params.toString();
    const body = await this.request("GET", `/admin/audit${qs ? `?${qs}` : ""}`);
    return (body as { entries: AuditEntry[] }).entries;
  }

  /** Owner-bypass document read (POST /admin/db/{db}/query). Admin sees every row regardless
   *  of per-row ownerField. Body and result shapes match /api/query. */
  async adminQuery<R>(db: string, query: RtQuery<R>): Promise<R> {
    const body = await this.request("POST", `/admin/db/${encodeURIComponent(db)}/query`, {
      query: query.json,
    });
    return (body as { result: R }).result;
  }

  /** Owner-bypass document write (POST /admin/db/{db}/mutate). Body shapes match /api/mutate;
   *  `idempotencyKey` is the opt-in safe-retry key. Capped server-side by RTDB_MAX_AFFECTED_DOCS. */
  async adminMutate(
    db: string,
    txn: TransactionJson,
    opts?: { idempotencyKey?: string },
  ): Promise<unknown[]> {
    const body = await this.request("POST", `/admin/db/${encodeURIComponent(db)}/mutate`, {
      txn,
      idempotencyKey: opts?.idempotencyKey,
    });
    return (body as { results: unknown[] }).results;
  }

  /** Apply (or preview) a declarative schema migration (POST /admin/db/{db}/migrate).
   *  `req.dryRun` reports `affectedRows` and the derived `schema` without committing;
   *  a real run returns `applied: true` with the new installed schema. */
  async migrate(db: string, req: MigrateRequestJson): Promise<MigrateResultJson> {
    return (await this.request(
      "POST",
      `/admin/db/${encodeURIComponent(db)}/migrate`,
      req,
    )) as MigrateResultJson;
  }

  /** Trigger one pg_dump now (POST /admin/backup, 202 Accepted). Runs async server-side;
   *  poll `listBackups()` to see the result. */
  async backupNow(): Promise<void> {
    await this.request("POST", "/admin/backup", {});
  }

  /** List existing backups and whether one is currently running (GET /admin/backups). */
  async listBackups(): Promise<BackupsListResponse> {
    return (await this.request("GET", "/admin/backups")) as BackupsListResponse;
  }

  /** Download a dump as a raw binary `Response` (caller streams to a file). Bypasses the
   *  JSON `request` helper because the body is `application/octet-stream`. Error envelopes
   *  on non-OK still surface as `RtDbError` via `throwFromResponse`. */
  async downloadBackup(name: string): Promise<Response> {
    const response = await this.fetchImpl(`${this.url}/admin/backups/${encodeURIComponent(name)}`, {
      method: "GET",
      headers: { Authorization: `Bearer ${this.adminKey}` },
    });
    if (!response.ok) {
      await this.throwFromResponse(response);
    }
    return response;
  }

  /** Delete a backup file (DELETE /admin/backups/{name}, 204 No Content). */
  async deleteBackup(name: string): Promise<void> {
    await this.request("DELETE", `/admin/backups/${encodeURIComponent(name)}`);
  }

  /** Restore a dump into a fresh `rtdb_restored_<stamp>` DB (POST /admin/restore).
   *  `confirm` is sent equal to `name` — the server's typed guard against accidental
   *  restores. Returns the restored DB name and follow-up instructions. */
  async restoreBackup(name: string): Promise<RestoreResult> {
    return (await this.request("POST", "/admin/restore", {
      name,
      confirm: name,
    })) as RestoreResult;
  }

  /** List webhooks registered for `db` (GET /admin/db/{db}/webhooks). Returns an
   *  empty array when webhooks are disabled at boot — the table may not exist. */
  async listWebhooks(db: string): Promise<Webhook[]> {
    const body = await this.request("GET", `/admin/db/${encodeURIComponent(db)}/webhooks`);
    return (body as { webhooks: Webhook[] }).webhooks;
  }

  /** Register a webhook for `db` (POST /admin/db/{db}/webhooks). Only the provided
   *  option keys are sent; the server defaults `table` to all-tables, `events` to
   *  `["*"]`, and `enabled` to true when their keys are absent. Returns the new id. */
  async createWebhook(db: string, opts: CreateWebhookOptions): Promise<{ id: number }> {
    const body = await this.request(
      "POST",
      `/admin/db/${encodeURIComponent(db)}/webhooks`,
      pickDefined(opts),
    );
    return body as { id: number };
  }

  /** Partial-edit a webhook (PUT /admin/db/{db}/webhooks/{id}). Each present field
   *  overwrites the stored value; absent fields are unchanged. The `table` field
   *  is a tri-state on the wire: omitted leaves the filter alone, JSON `null`
   *  clears it to all-tables, a string sets it. Returns the updated webhook. */
  async editWebhook(db: string, id: number, opts: EditWebhookOptions): Promise<Webhook> {
    const body = await this.request(
      "PUT",
      `/admin/db/${encodeURIComponent(db)}/webhooks/${encodeURIComponent(id)}`,
      pickDefined(opts),
    );
    return body as Webhook;
  }

  /** Delete a webhook and cascade its pending deliveries (DELETE /admin/db/{db}/webhooks/{id}). */
  async deleteWebhook(db: string, id: number): Promise<void> {
    await this.request(
      "DELETE",
      `/admin/db/${encodeURIComponent(db)}/webhooks/${encodeURIComponent(id)}`,
    );
  }

  /** List a webhook's delivery outbox newest-first
   *  (GET /admin/db/{db}/webhooks/{id}/deliveries?status=&limit=&offset=). `status`
   *  filters by `pending|retrying|delivered|failed`; `limit`/`offset` page. */
  async listDeliveries(
    db: string,
    id: number,
    opts: ListDeliveriesOptions = {},
  ): Promise<WebhookDelivery[]> {
    const params = new URLSearchParams();
    if (opts.status) params.set("status", opts.status);
    if (opts.limit !== undefined) params.set("limit", String(opts.limit));
    if (opts.offset !== undefined) params.set("offset", String(opts.offset));
    const qs = params.toString();
    const body = await this.request(
      "GET",
      `/admin/db/${encodeURIComponent(db)}/webhooks/${encodeURIComponent(id)}/deliveries${qs ? `?${qs}` : ""}`,
    );
    return (body as { deliveries: WebhookDelivery[] }).deliveries;
  }

  /** Open the realtime op-feed over the `/admin/stream` WebSocket and yield frames
   *  as they arrive: document op events (a 200-row replay, then live) interleaved
   *  with ~1s server metrics snapshots. `db`/`table` filter both the replay and the
   *  live stream. The admin key is carried in the `rtdb-admin.<token>` WS subprotocol
   *  (the path browsers must use, since they cannot set headers on a WS handshake; the
   *  server echoes it back to complete the 101). Break out of `for await`, abort
   *  `signal`, or call `.return()` on the generator to close the socket. */
  async *streamAdmin(opts?: {
    db?: string;
    table?: string;
    signal?: AbortSignal;
  }): AsyncGenerator<AdminStreamFrame> {
    const params = new URLSearchParams();
    if (opts?.db) params.set("db", opts.db);
    if (opts?.table) params.set("table", opts.table);
    const qs = params.toString();
    const wsUrl = `${this.url.replace(/^http/, "ws")}/admin/stream${qs ? `?${qs}` : ""}`;

    const socket = this.webSocketFactory(wsUrl, `rtdb-admin.${this.adminKey}`);

    const queue: AdminStreamFrame[] = [];
    let resolveWaiter: (() => void) | null = null;
    let done = false;
    let streamError: Error | null = null;
    const wake = () => {
      const r = resolveWaiter;
      resolveWaiter = null;
      r?.();
    };

    socket.onmessage = (ev: { data: unknown }) => {
      const frame = parseAdminStreamFrame(ev.data);
      if (frame) {
        queue.push(frame);
        wake();
      }
    };
    socket.onerror = () => {
      streamError = new RtDbError("INTERNAL", "admin stream socket error");
      done = true;
      wake();
    };
    socket.onclose = () => {
      done = true;
      wake();
    };

    const onAbort = () => {
      done = true;
      try {
        socket.close(1000, "abort");
      } catch {
        /* socket may already be closed */
      }
      wake();
    };
    opts?.signal?.addEventListener("abort", onAbort);

    try {
      while (!done || queue.length > 0) {
        if (queue.length > 0) {
          yield queue.shift() as AdminStreamFrame;
        } else {
          await new Promise<void>((resolve) => {
            resolveWaiter = resolve;
          });
        }
      }
      if (streamError) throw streamError;
    } finally {
      opts?.signal?.removeEventListener("abort", onAbort);
      try {
        socket.close(1000, "cleanup");
      } catch {
        /* already closed */
      }
    }
  }

  private async throwFromResponse(response: Response): Promise<never> {
    const parsed: unknown = await response.json().catch(() => null);
    if (RtDbError.isEnvelope(parsed)) {
      throw RtDbError.fromEnvelope(parsed);
    }
    throw new RtDbError("INTERNAL", `admin request failed with status ${response.status}`);
  }

  private async request(
    method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE",
    path: string,
    payload?: unknown,
  ): Promise<unknown> {
    const response = await this.fetchImpl(`${this.url}${path}`, {
      method,
      headers: {
        Authorization: `Bearer ${this.adminKey}`,
        ...(payload === undefined ? {} : { "content-type": "application/json" }),
      },
      body: payload === undefined ? undefined : JSON.stringify(payload),
    });
    const parsed: unknown = await response.json().catch(() => null);
    if (!response.ok) {
      if (RtDbError.isEnvelope(parsed)) {
        throw RtDbError.fromEnvelope(parsed);
      }
      throw new RtDbError("INTERNAL", `admin request failed with status ${response.status}`);
    }
    return parsed;
  }
}
