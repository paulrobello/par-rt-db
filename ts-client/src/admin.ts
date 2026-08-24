import type { WebSocketLike } from "./client.js";
import { RtDbError } from "./errors.js";
import type { FileMetadata } from "./http.js";
import type {
  MigrateRequestJson,
  MigrateResultJson,
  QueryJson,
  ScheduleInfo,
  ScheduleWhen,
  SchemaHistoryEntry,
  SchemaHistoryEntrySummary,
  SchemaJson,
  TransactionJson,
  WorkflowInfo,
  WorkflowInfoFull,
  WorkflowSpec,
  WorkflowStatus,
} from "./protocol.js";
import type { RtQuery } from "./query.js";
import type { SchemaDefinition } from "./schema.js";

export interface RtDbAdminClientOptions {
  url: string;
  /** Instance admin key. When set, every request carries
   *  `Authorization: Bearer <adminKey>` (the CLI/automation path). When omitted,
   *  the client authenticates via the HttpOnly `rtdb_session` cookie: each
   *  request is sent with `credentials: "include"` and the readable
   *  `rtdb-admin-csrf` nonce is echoed as `X-Rtdb-Csrf` (the header the server's
   *  admin-CSRF guard requires on cookie-authenticated mutating `/admin/*`
   *  verbs). Cookie mode is how the operator dashboard — a same-origin SPA with
   *  no JS-readable admin key — consumes this client. */
  adminKey?: string;
  fetch?: typeof fetch;
  /** Injectable WebSocket constructor (browser/Node/bun). Defaults to the global
   *  `WebSocket`. The second arg carries the WS subprotocol(s) — `/admin/stream`
   *  authenticates via the `rtdb-admin.<token>` subprotocol in bearer mode; in
   *  cookie mode the subprotocol is omitted (the browser attaches the session
   *  cookie to the same-origin upgrade). */
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
  /** ENH-011 per-db quota/usage fields (0 = unlimited). Server always emits all six. */
  tablesQuota: number;
  tablesUsed: number;
  storageQuotaBytes: number;
  storageUsedBytes: number;
  subsQuota: number;
  subsUsed: number;
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
/** One active interactive session from `GET /admin/sessions`. `tokenHash` is a
 *  non-reversible sha256 digest (the plaintext token is never stored), safe to
 *  surface to an admin and used to target a row for revoke. `email`/`login` are
 *  `null` when the user has none (e.g. an anonymous session). */
export interface SessionInfo {
  tokenHash: string;
  userId: string;
  email: string | null;
  login: string | null;
  anonymous: boolean;
  createdAt: number;
  expiresAt: number;
}
/** A row skipped by the anon→real merge because the re-stamp would collide
 *  with an existing doc under a unique index (server `merge::MergeConflict`). */
export interface MergeConflict {
  table: string;
  id: string;
}
/** Per-database outcome of an anon→real merge: re-stamped-doc counts per
 *  table plus the rows skipped over unique-index conflicts. */
export interface MergeDbResult {
  tables: Record<string, number>;
  conflicts: MergeConflict[];
}
/** Full-instance anon→real merge outcome from `POST /admin/merge-users`:
 *  per-db doc re-stamps, storage blobs repointed, sessions repointed (an
 *  open WS or stored SDK token promotes to the real principal on its next
 *  op), and whether the anon user row was deleted. */
export interface MergeReport {
  dbs: Record<string, MergeDbResult>;
  storageRepointed: number;
  sessionsRepointed: number;
  anonDeleted: boolean;
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
  /** Per-db rollup of the subscription counters above, when the server
   *  includes it on `/admin/metrics`. Absent on older server builds. */
  perDbSubs?: DbSubCounters[];
}
export interface HotConfig {
  allowedOrigins: string[];
  sessionTtlDays: number;
  maxFileSize: number;
  idempotencyTtlMs: number;
  /** Per-db resource quotas (ENH-011). Zero means "no limit" on the server. */
  maxTablesPerDb: number;
  maxStorageBytesPerDb: number;
  maxSubsPerDb: number;
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
  /** Per-db resource quotas (ENH-011). Omit to leave unchanged. */
  maxTablesPerDb?: number;
  maxStorageBytesPerDb?: number;
  maxSubsPerDb?: number;
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

/** Identity of a subscriber, when it has an interactive principal. `null` for
 *  machine tokens, scheduled jobs, and admin-bypass subscriptions — anything
 *  with no user identity. */
export interface SubscriptionsPrincipal {
  userId: string | null;
  email: string | null;
}

/** One live subscription from `GET /admin/subscriptions`. `terminal` is the
 *  query-DSL terminal step kind; `readSetClass` is the invalidation class the
 *  committer uses to decide skip-vs-rerun (point/indexed/ordered/table). */
export interface SubscriptionInfo {
  db: string;
  table: string;
  terminal: string;
  readSetClass: string;
  principal: SubscriptionsPrincipal | null;
}

/** Per-db rollup of subscription-invalidation counters (the same fields the
 *  server totals across all dbs on `SubscriptionsResponse`). `skips` is the
 *  total across the three classes and `rerunRatio` is
 *  `reruns / max(1, reruns + skips)` — in [0, 1]; sustained above 0.5 means
 *  re-runs dominate this db's fan-out (ENH-024). */
export interface DbSubCounters {
  db: string;
  reruns: number;
  skipsPoint: number;
  skipsIndexed: number;
  skipsOrdered: number;
  missed: number;
  skips: number;
  rerunRatio: number;
}

/** Response from `GET /admin/subscriptions`: the live subscription list plus
 *  server-wide invalidation totals and a per-db rollup. */
export interface SubscriptionsResponse {
  subscriptions: SubscriptionInfo[];
  subsRerunsTotal: number;
  subsSkipsPointTotal: number;
  subsSkipsIndexedTotal: number;
  subsSkipsOrderedTotal: number;
  subsMissedPushesTotal: number;
  perDb: DbSubCounters[];
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
 *  `["*"]` to match every event. `secret` is the per-webhook HMAC key the
 *  server generates (SEC-115); the receiver uses it to verify each delivery's
 *  `X-Rtdb-Signature` header. Surfaced here so an operator can copy it to the
 *  receiver; `null` only before the boot backfill has run. */
export interface Webhook {
  id: number;
  db: string;
  table: string | null;
  url: string;
  events: string[];
  createdAt: number;
  enabled: boolean;
  secret: string | null;
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
 *  to all-tables, or a string to set it. `rotateSecret: true` generates a fresh
 *  server-side signing secret (SEC-115); the secret value itself is never
 *  accepted from the client. */
export interface EditWebhookOptions {
  url?: string;
  table?: string | null;
  events?: string[];
  enabled?: boolean;
  rotateSecret?: boolean;
}

/** Options for `listDeliveries`. All optional; omitted filters/limits mean: no
 *  status filter, server default limit, offset 0. */
export interface ListDeliveriesOptions {
  status?: string;
  limit?: number;
  offset?: number;
}

/** One new column reported by `previewSchema`. `fieldType` is the
 *  human-readable field type (e.g. `string`, `id<projects>`, `string?`). */
export interface SchemaPreviewColumnAdd {
  name: string;
  fieldType: string;
}
/** One new index reported by `previewSchema`. */
export interface SchemaPreviewIndexAdd {
  name: string;
  fields: string[];
}
/** One new table reported by `previewSchema`: its name plus the columns and
 *  indexes the additive-only push would add. */
export interface SchemaPreviewTableAdd {
  table: string;
  columns: SchemaPreviewColumnAdd[];
  indexes: SchemaPreviewIndexAdd[];
}
/** One rejection reported by `previewSchema`: a drop or type change the DDL
 *  layer will refuse. `item` is the bare column/index name. */
export interface SchemaPreviewRejection {
  table: string;
  item: string;
  reason: string;
}
/** Result of `previewSchema`: what an additive-only push would ADD and what it
 *  would REJECT (drops, type changes). Mirrors the server's `schema_diff`. */
export interface SchemaPreviewDiff {
  added: SchemaPreviewTableAdd[];
  rejected: SchemaPreviewRejection[];
}

/** Result of POST /admin/db/{db}/explain — the compiled SQL for a query, with
 *  bind values formatted as strings (numbers/booleans via Display) and any
 *  compile-time warnings (e.g. unindexed-filter). `terminal` is the query
 *  terminal kind (get|collect|count|unique|first|distinct|aggregate|paginate|
 *  search|vectorSearch|hybridSearch). */
export interface ExplainResult {
  sql: string;
  params: string[];
  terminal: string;
  warnings: string[];
}

/** One entry from GET /admin/slow-queries. `params` is omitted from the server
 *  JSON when redacted (the default) and present as string[] when
 *  `RTDB_SLOW_QUERY_LOG_PARAMS=true`. */
export interface SlowQueryEntry {
  startedAtMs: number;
  durationMs: number;
  db: string;
  table: string;
  terminal: string;
  sql: string;
  params?: string[];
}

/** Response envelope for GET /admin/slow-queries. `thresholdMs` is 0 when slow
 *  query logging is disabled. */
export interface SlowQueriesResponse {
  queries: SlowQueryEntry[];
  thresholdMs: number;
  capacity: number;
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

/** Reads the readable (non-HttpOnly) `rtdb-admin-csrf` cookie set alongside the
 *  session cookie by `/admin/login` and the OAuth callback (SEC-106). Returns
 *  `null` when no admin session is active, or when running where `document` is
 *  undefined (SSR/Node). The value is echoed in the `X-Rtdb-Csrf` header on
 *  cookie-mode admin requests — a cross-site forge cannot read this cookie and
 *  so cannot set the header, forcing a preflight the CORS allowlist then gates. */
function readAdminCsrfCookie(): string | null {
  if (typeof document === "undefined") return null;
  const match = document.cookie.match(/(?:^|;\s*)rtdb-admin-csrf=([^;]+)/);
  return match ? match[1] : null;
}

/** Control-plane client for `/admin/*`. Authorize either with the instance
 *  admin key (bearer mode — CLI/automation) or, when `adminKey` is omitted, via
 *  the HttpOnly session cookie (cookie mode — same-origin browser consumers like
 *  the operator dashboard). */
export class RtDbAdminClient {
  private readonly url: string;
  private readonly adminKey: string | undefined;
  /** True when no `adminKey` was supplied — requests ride the session cookie. */
  private readonly cookieMode: boolean;
  private readonly fetchImpl: typeof fetch;
  private readonly webSocketFactory: (url: string, protocols?: string | string[]) => WebSocketLike;

  constructor(options: RtDbAdminClientOptions) {
    this.url = options.url.replace(/\/+$/, "");
    this.adminKey = options.adminKey;
    this.cookieMode = !options.adminKey;
    // Bind the global: browsers require Window as fetch's receiver, and a
    // stored-unbound reference throws "Illegal invocation" when called.
    this.fetchImpl = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.webSocketFactory =
      options.webSocketFactory ??
      ((url, protocols) => new WebSocket(url, protocols) as unknown as WebSocketLike);
  }

  /** Auth + CSRF headers for the current mode. Bearer mode sets
   *  `Authorization: Bearer <key>`; cookie mode omits it and instead echoes the
   *  readable `rtdb-admin-csrf` nonce. Merged into every outgoing request. */
  private authHeaders(): Record<string, string> {
    if (this.adminKey) return { Authorization: `Bearer ${this.adminKey}` };
    const csrf = readAdminCsrfCookie();
    return csrf ? { "X-Rtdb-Csrf": csrf } : {};
  }

  /** `credentials` value for `fetch` init: cookie mode sends `"include"` so the
   *  browser attaches the HttpOnly session cookie; bearer mode leaves it
   *  absent (the default, matching pre-cookie-mode behavior). Returns a
   *  spreadable Partial<RequestInit> so the `credentials` key is omitted
   *  entirely in bearer mode — `exactOptionalPropertyTypes` (ARC-133) rejects
   *  passing `undefined` literally to fetch's RequestInit.credentials. */
  private get creds(): Pick<RequestInit, "credentials"> {
    return this.cookieMode ? { credentials: "include" } : {};
  }

  /** Creates a new, empty database with no schema pushed yet. */
  async createDb(name: string): Promise<void> {
    await this.request("POST", "/admin/create-db", { name });
  }

  /** Delete a database (schema + all per-db state). `confirm` must equal `name`
   *  exactly — the server's typed guard against accidental deletion. */
  async deleteDb(name: string, confirm: string): Promise<void> {
    await this.request("POST", "/admin/delete-db", { name, confirm });
  }

  /** Applies an additive-only schema push to `db`: new tables, fields, and
   *  indexes only — never drops or type changes (use {@link Migration} for
   *  those). Safe to call repeatedly with a superset schema. */
  async pushSchema(db: string, schema: SchemaDefinition<any> | SchemaJson): Promise<void> {
    await this.request("POST", "/admin/push-schema", { db, schema: toSchemaJson(schema) });
  }

  /** Preview an additive-only schema diff against the currently-applied schema
   *  (POST /admin/db/{db}/schema/preview). Pure/advisory — does NOT apply.
   *  Returns what the push would ADD and what it would REJECT (drops, type
   *  changes the DDL layer refuses). */
  async previewSchema(
    db: string,
    schema: SchemaDefinition<any> | SchemaJson,
  ): Promise<SchemaPreviewDiff> {
    return (await this.request("POST", `/admin/db/${encodeURIComponent(db)}/schema/preview`, {
      schema: toSchemaJson(schema),
    })) as SchemaPreviewDiff;
  }

  /** Lists every database on the instance. */
  async listDbs(): Promise<string[]> {
    const body = await this.request("GET", "/admin/dbs");
    return (body as { databases: string[] }).databases;
  }

  /** Mints a new machine token for `db`, named `name`. Returns the token id
   *  (for later revocation) and the plaintext token, which is shown only
   *  once — the server stores only its hash. */
  async mintToken(
    db: string,
    name: string,
    opts: MintTokenOptions = {},
  ): Promise<{ tokenId: string; token: string }> {
    const body = await this.request("POST", "/admin/mint-token", { db, name, ...opts });
    return body as { tokenId: string; token: string };
  }

  /** Revokes a machine token by id, invalidating it immediately. */
  async revokeToken(tokenId: string): Promise<void> {
    await this.request("POST", "/admin/revoke-token", { tokenId });
  }

  /** Adds `email` to `db`'s OAuth sign-in allowlist. */
  async allowlistAdd(db: string, email: string): Promise<void> {
    await this.request("POST", "/admin/allowlist", { db, action: "add", email });
  }

  /** Removes `email` from `db`'s OAuth sign-in allowlist. */
  async allowlistRemove(db: string, email: string): Promise<void> {
    await this.request("POST", "/admin/allowlist", { db, action: "remove", email });
  }

  /** Lists every email on `db`'s OAuth sign-in allowlist. */
  async allowlistList(db: string): Promise<string[]> {
    const body = await this.request("GET", `/admin/allowlist?db=${encodeURIComponent(db)}`);
    return (body as { emails: string[] }).emails;
  }

  /** Whether `db` has opted in to anonymous principal access
   *  (GET /admin/db/{db}/anonymous-access, SEC-103). Reports only the per-db
   *  flag; the instance-wide boot gate `RTDB_AUTH_ANONYMOUS_ENABLED` is a
   *  boot-time config and is not reflected here. */
  async getAnonymousAccess(db: string): Promise<{ enabled: boolean }> {
    return (await this.request("GET", `/admin/db/${encodeURIComponent(db)}/anonymous-access`)) as {
      enabled: boolean;
    };
  }

  /** Opt `db` in to (or out of) anonymous principal access
   *  (PATCH /admin/db/{db}/anonymous-access, SEC-103). The instance-wide boot
   *  gate must also be on for anon minting to work; this per-db flag is the
   *  additional gate checked at `authorize`. */
  async setAnonymousAccess(db: string, enabled: boolean): Promise<void> {
    await this.request("PATCH", `/admin/db/${encodeURIComponent(db)}/anonymous-access`, {
      enabled,
    });
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
        headers: this.authHeaders(),
        ...this.creds,
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
        headers: { ...this.authHeaders(), "content-type": "application/x-ndjson" },
        ...this.creds,
        body: jsonl,
      },
    );
    if (!response.ok) {
      await this.throwFromResponse(response);
    }
  }

  /** Clones `from` (schema + documents) into a freshly created `to` (see server `admin::dbs::clone_db`, ENH-009). */
  async cloneDb(from: string, to: string): Promise<void> {
    const response = await this.fetchImpl(
      `${this.url}/admin/clone-db?from=${encodeURIComponent(from)}&to=${encodeURIComponent(to)}`,
      {
        method: "POST",
        headers: this.authHeaders(),
        ...this.creds,
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

  /** List active interactive sessions server-wide, optionally filtered by user
   *  id or email (GET /admin/sessions?user=&limit=). Returns sessions
   *  newest-first. `limit` is clamped server-side to [1, 1000] (default 200). */
  async listSessions(filter?: { user?: string; limit?: number }): Promise<SessionInfo[]> {
    const params = new URLSearchParams();
    if (filter?.user) params.set("user", filter.user);
    if (filter?.limit !== undefined) params.set("limit", String(filter.limit));
    const qs = params.toString();
    const body = await this.request("GET", `/admin/sessions${qs ? `?${qs}` : ""}`);
    return (body as { sessions: SessionInfo[] }).sessions;
  }

  /** Revoke a single session by its `tokenHash` (DELETE /admin/sessions/{tokenHash}). */
  async revokeSession(tokenHash: string): Promise<void> {
    await this.request("DELETE", `/admin/sessions/${encodeURIComponent(tokenHash)}`);
  }

  /** Revoke every session for a user (DELETE /admin/sessions?user={userId}).
   *  Returns `{ ok, revoked }` where `revoked` is the count of sessions dropped. */
  async revokeUserSessions(userId: string): Promise<{ ok: boolean; revoked: number }> {
    return (await this.request("DELETE", `/admin/sessions?user=${encodeURIComponent(userId)}`)) as {
      ok: boolean;
      revoked: number;
    };
  }

  /** Revoke every EXPIRED session instance-wide (DELETE /admin/sessions?expired=true).
   *  Sweeps both OAuth/anonymous sessions and admin-key login rows — the
   *  dashboard's "remove all expired" action. Returns `{ ok, revoked }`. */
  async revokeExpiredSessions(): Promise<{ ok: boolean; revoked: number }> {
    return (await this.request("DELETE", "/admin/sessions?expired=true")) as {
      ok: boolean;
      revoked: number;
    };
  }

  /** Run the anon→real account merge synchronously and return the full report
   *  (POST /admin/merge-users). The server's typed guard is applied for you:
   *  `confirm` is sent as `realUserId` (same pattern as `deleteDb`). A 404
   *  means the anon user row does not exist (nothing to merge). */
  async mergeUsers(anonUserId: string, realUserId: string): Promise<MergeReport> {
    return (await this.request("POST", "/admin/merge-users", {
      anonUserId,
      realUserId,
      confirm: realUserId,
    })) as MergeReport;
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

  /** Live subscription inspector (GET /admin/subscriptions). Returns every
   *  active subscription across all dbs (or one `db` when filtered), plus
   *  server-wide invalidation totals and a per-db rollup. */
  async listSubscriptions(opts?: { db?: string }): Promise<SubscriptionsResponse> {
    const params = new URLSearchParams();
    if (opts?.db) params.set("db", opts.db);
    const qs = params.toString();
    const body = await this.request("GET", `/admin/subscriptions${qs ? `?${qs}` : ""}`);
    return body as SubscriptionsResponse;
  }

  /** Options for `adminQuery`. `includeDeleted` is an internal admin-route param
   *  (NOT a wire `Query` field): `true` also returns soft-deleted rows — rows a
   *  `softDelete` table stamped with `deleted_at` (FM-33) — so operators can see
   *  them in the data browser. Omitted/false keeps the live-rows-only default. */
  async adminQuery<R>(
    db: string,
    query: RtQuery<R>,
    opts?: { includeDeleted?: boolean },
  ): Promise<R>;
  async adminQuery(
    db: string,
    query: QueryJson,
    opts?: { includeDeleted?: boolean },
  ): Promise<unknown>;
  async adminQuery(
    db: string,
    query: RtQuery<unknown> | QueryJson,
    opts?: { includeDeleted?: boolean },
  ): Promise<unknown> {
    const json = "json" in query ? query.json : query;
    const payload: { query: QueryJson; includeDeleted?: boolean } = { query: json };
    if (opts?.includeDeleted) {
      payload.includeDeleted = true;
    }
    const body = await this.request("POST", `/admin/db/${encodeURIComponent(db)}/query`, payload);
    return (body as { result: unknown }).result;
  }

  /** Compile-time query introspection (POST /admin/db/{db}/explain). Accepts
   *  either a typed `RtQuery<R>` builder or a raw `QueryJson` and returns the
   *  compiled SQL (`$1`-bound, never interpolated literals), the bind params
   *  formatted as strings, the terminal kind, and any compile warnings. */
  async explainQuery(db: string, query: RtQuery<unknown> | QueryJson): Promise<ExplainResult> {
    const json = "json" in query ? query.json : query;
    return (await this.request("POST", `/admin/db/${encodeURIComponent(db)}/explain`, {
      query: json,
    })) as ExplainResult;
  }

  /** Slow-query log inspector (GET /admin/slow-queries). Returns recent slow
   *  queries across every db (or one `db` when filtered), the configured
   *  threshold (0 = logging disabled), and the ring-buffer capacity. `limit`
   *  caps the number of entries returned. */
  async getSlowQueries(opts?: { db?: string; limit?: number }): Promise<SlowQueriesResponse> {
    const params = new URLSearchParams();
    if (opts?.db) params.set("db", opts.db);
    if (opts?.limit !== undefined) params.set("limit", String(opts.limit));
    const qs = params.toString();
    return (await this.request(
      "GET",
      `/admin/slow-queries${qs ? `?${qs}` : ""}`,
    )) as SlowQueriesResponse;
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

  /** List scheduled jobs for `db` (GET /admin/db/{db}/schedules). */
  async listSchedules(db: string): Promise<ScheduleInfo[]> {
    const body = await this.request("GET", `/admin/db/${encodeURIComponent(db)}/schedules`);
    return (body as { schedules: ScheduleInfo[] }).schedules;
  }

  /** Create a scheduled job (POST /admin/db/{db}/schedules). `when` selects
   *  one-shot (`afterMs`/`runAt`) or recurring (`cron`); `txn` is the
   *  transaction the scheduler executes at the due time. Returns the new id. */
  async createSchedule(
    db: string,
    when: ScheduleWhen,
    txn: TransactionJson,
  ): Promise<{ id: string }> {
    const body = await this.request("POST", `/admin/db/${encodeURIComponent(db)}/schedules`, {
      when,
      txn,
    });
    return body as { id: string };
  }

  /** Cancel a scheduled job (POST /admin/db/{db}/schedules/{id}/cancel). */
  async cancelSchedule(db: string, id: string): Promise<{ ok: boolean }> {
    return (await this.request(
      "POST",
      `/admin/db/${encodeURIComponent(db)}/schedules/${encodeURIComponent(id)}/cancel`,
    )) as { ok: boolean };
  }

  /** Pause a scheduled job (POST /admin/db/{db}/schedules/{id}/pause). */
  async pauseSchedule(db: string, id: string): Promise<{ ok: boolean }> {
    return (await this.request(
      "POST",
      `/admin/db/${encodeURIComponent(db)}/schedules/${encodeURIComponent(id)}/pause`,
    )) as { ok: boolean };
  }

  /** Resume a paused scheduled job (POST /admin/db/{db}/schedules/{id}/resume). */
  async resumeSchedule(db: string, id: string): Promise<{ ok: boolean }> {
    return (await this.request(
      "POST",
      `/admin/db/${encodeURIComponent(db)}/schedules/${encodeURIComponent(id)}/resume`,
    )) as { ok: boolean };
  }

  /** List workflow runs for `db`, newest first (GET /admin/db/{db}/workflows).
   *  `status` filters by lifecycle; `limit` defaults to 100 and caps at 500. */
  async adminListWorkflows(
    db: string,
    opts: { status?: WorkflowStatus; limit?: number } = {},
  ): Promise<WorkflowInfo[]> {
    const params = new URLSearchParams();
    if (opts.status !== undefined) params.set("status", opts.status);
    if (opts.limit !== undefined) params.set("limit", String(opts.limit));
    const qs = params.toString();
    const body = await this.request(
      "GET",
      `/admin/db/${encodeURIComponent(db)}/workflows${qs ? `?${qs}` : ""}`,
    );
    return (body as { workflows: WorkflowInfo[] }).workflows;
  }

  /** Fetch one full run row — info plus the per-step outcome trail
   *  (GET /admin/db/{db}/workflows/{id}). */
  async adminGetWorkflow(db: string, id: string): Promise<WorkflowInfoFull> {
    return (await this.request(
      "GET",
      `/admin/db/${encodeURIComponent(db)}/workflows/${encodeURIComponent(id)}`,
    )) as WorkflowInfoFull;
  }

  /** Start a workflow run (POST /admin/db/{db}/workflows). The body is the
   *  bare `WorkflowSpec` (no wrapper); returns the new run's id. */
  async adminStartWorkflow(db: string, spec: WorkflowSpec): Promise<{ id: string }> {
    return (await this.request("POST", `/admin/db/${encodeURIComponent(db)}/workflows`, spec)) as {
      id: string;
    };
  }

  /** Cancel a non-terminal run (POST /admin/db/{db}/workflows/{id}/cancel).
   *  `ok:false` = unknown/terminal run (a no-op, not an error). */
  async adminCancelWorkflow(db: string, id: string): Promise<{ ok: boolean }> {
    return (await this.request(
      "POST",
      `/admin/db/${encodeURIComponent(db)}/workflows/${encodeURIComponent(id)}/cancel`,
    )) as { ok: boolean };
  }

  /** Deliver a signal to a waiting run (POST /admin/db/{db}/workflows/{id}/signal;
   *  latest-wins payload). Typed 404/409s on unknown id / not waiting / name
   *  mismatch; `ok:true` only on delivery. */
  async adminSignalWorkflow(
    db: string,
    id: string,
    name: string,
    payload?: unknown,
  ): Promise<{ ok: boolean }> {
    return (await this.request(
      "POST",
      `/admin/db/${encodeURIComponent(db)}/workflows/${encodeURIComponent(id)}/signal`,
      { name, ...(payload === undefined ? {} : { payload }) },
    )) as { ok: boolean };
  }

  /** Hard-delete one run row (DELETE /admin/db/{db}/workflows/{id}). Unlike
   *  cancel, this removes the row entirely — including its outcome trail. */
  async adminDeleteWorkflow(db: string, id: string): Promise<{ ok: boolean }> {
    return (await this.request(
      "DELETE",
      `/admin/db/${encodeURIComponent(db)}/workflows/${encodeURIComponent(id)}`,
    )) as { ok: boolean };
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

  /** Schema snapshot history, newest-first (GET /admin/db/{db}/schema/history).
   *  Each entry is metadata-only (no schema blob); fetch the full snapshot with
   *  `getSchemaVersion`. `limit`/`offset` page. */
  async getSchemaHistory(
    db: string,
    opts: { limit?: number; offset?: number } = {},
  ): Promise<SchemaHistoryEntrySummary[]> {
    const params = new URLSearchParams();
    if (opts.limit !== undefined) params.set("limit", String(opts.limit));
    if (opts.offset !== undefined) params.set("offset", String(opts.offset));
    const qs = params.toString();
    const body = await this.request(
      "GET",
      `/admin/db/${encodeURIComponent(db)}/schema/history${qs ? `?${qs}` : ""}`,
    );
    return (body as { entries: SchemaHistoryEntrySummary[] }).entries;
  }

  /** One full schema snapshot (GET /admin/db/{db}/schema/history/{version}),
   *  including the `schema` blob. */
  async getSchemaVersion(db: string, version: number): Promise<SchemaHistoryEntry> {
    return (await this.request(
      "GET",
      `/admin/db/${encodeURIComponent(db)}/schema/history/${version}`,
    )) as SchemaHistoryEntry;
  }

  /** Restore the live schema shape to a prior snapshot (POST /admin/db/{db}/schema/restore).
   *  `confirm` must equal the db name (typed guard, mirrors delete-db). The
   *  outgoing schema is captured first, so a restore is itself undoable. */
  async restoreSchema(
    db: string,
    version: number,
    confirm: string,
  ): Promise<{ ok: boolean; restoredTo: number }> {
    return (await this.request("POST", `/admin/db/${encodeURIComponent(db)}/schema/restore`, {
      version,
      confirm,
    })) as { ok: boolean; restoredTo: number };
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
      headers: this.authHeaders(),
      ...this.creds,
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

  /** List storage blobs for `db` (GET /admin/db/{db}/storage). */
  async listFiles(db: string): Promise<FileMetadata[]> {
    const body = await this.request("GET", `/admin/db/${encodeURIComponent(db)}/storage`);
    return (body as { files: FileMetadata[] }).files;
  }

  /** Upload raw bytes to `db`'s storage (POST /admin/db/{db}/storage). The body
   *  is the file itself (not JSON); the content-type is taken from the Blob's
   *  `.type` (a File keeps its MIME type) so the server stores and serves it
   *  back. The server enforces `maxFileSize` (413). Returns the new blob id. */
  async uploadFile(db: string, body: Blob | ArrayBuffer): Promise<{ id: string }> {
    const blob = body instanceof Blob ? body : new Blob([body]);
    const response = await this.fetchImpl(
      `${this.url}/admin/db/${encodeURIComponent(db)}/storage`,
      {
        method: "POST",
        headers: { ...this.authHeaders(), "content-type": blob.type || "application/octet-stream" },
        ...this.creds,
        body: blob,
      },
    );
    if (!response.ok) {
      await this.throwFromResponse(response);
    }
    return (await response.json()) as { id: string };
  }

  /** Delete a storage blob (DELETE /admin/db/{db}/storage/{id}). */
  async deleteFile(db: string, id: string): Promise<{ ok: boolean }> {
    return (await this.request(
      "DELETE",
      `/admin/db/${encodeURIComponent(db)}/storage/${encodeURIComponent(id)}`,
    )) as { ok: boolean };
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

    // Bearer mode rides the admin key in the `rtdb-admin.<token>` subprotocol
    // (browsers can't set headers on a WS handshake). Cookie mode omits it — the
    // browser attaches the HttpOnly session cookie to the same-origin upgrade.
    const socket = this.adminKey
      ? this.webSocketFactory(wsUrl, `rtdb-admin.${this.adminKey}`)
      : this.webSocketFactory(wsUrl);

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
      throw RtDbError.fromEnvelope(parsed, response.status);
    }
    throw new RtDbError(
      "INTERNAL",
      `admin request failed with status ${response.status}`,
      undefined,
      response.status,
    );
  }

  private async request(
    method: "GET" | "POST" | "PUT" | "PATCH" | "DELETE",
    path: string,
    payload?: unknown,
  ): Promise<unknown> {
    const response = await this.fetchImpl(`${this.url}${path}`, {
      method,
      headers: {
        ...this.authHeaders(),
        ...(payload === undefined ? {} : { "content-type": "application/json" }),
      },
      ...this.creds,
      ...(payload === undefined ? {} : { body: JSON.stringify(payload) }),
    });
    const parsed: unknown = await response.json().catch(() => null);
    if (!response.ok) {
      if (RtDbError.isEnvelope(parsed)) {
        throw RtDbError.fromEnvelope(parsed, response.status);
      }
      throw new RtDbError(
        "INTERNAL",
        `admin request failed with status ${response.status}`,
        undefined,
        response.status,
      );
    }
    // 202 (backupNow) and 204 (logout, backup delete) legitimately carry no
    // body — callers discard it. Any other 2xx must carry a JSON object:
    // returning null here (empty body, HTML gateway page, invalid JSON)
    // TypeErrors downstream when callers destructure fields instead of
    // surfacing an RtDbError.
    const bodylessOk = response.status === 202 || response.status === 204;
    if (!bodylessOk && (parsed === null || typeof parsed !== "object")) {
      throw new RtDbError(
        "INTERNAL",
        `admin request to ${path} returned 2xx with no JSON object body`,
        undefined,
        response.status,
      );
    }
    return parsed;
  }
}
