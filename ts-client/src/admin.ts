import { RtDbError } from "./errors.js";
import type {
  MigrateRequestJson,
  MigrateResultJson,
  SchemaJson,
  TransactionJson,
} from "./protocol.js";
import type { RtQuery } from "./query.js";
import type { SchemaDefinition } from "./schema.js";

export interface RtDbAdminClientOptions {
  url: string;
  adminKey: string;
  fetch?: typeof fetch;
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

function toSchemaJson(schema: SchemaDefinition<any> | SchemaJson): SchemaJson {
  return "toJSON" in schema && typeof schema.toJSON === "function"
    ? schema.toJSON()
    : (schema as SchemaJson);
}

/** Control-plane client for `/admin/*`, authorized with the instance admin key. */
export class RtDbAdminClient {
  private readonly url: string;
  private readonly adminKey: string;
  private readonly fetchImpl: typeof fetch;

  constructor(options: RtDbAdminClientOptions) {
    this.url = options.url.replace(/\/+$/, "");
    this.adminKey = options.adminKey;
    this.fetchImpl = options.fetch ?? globalThis.fetch;
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

  async mintToken(db: string, name: string): Promise<{ tokenId: string; token: string }> {
    const body = await this.request("POST", "/admin/mint-token", { db, name });
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

  private async throwFromResponse(response: Response): Promise<never> {
    const parsed: unknown = await response.json().catch(() => null);
    if (RtDbError.isEnvelope(parsed)) {
      throw RtDbError.fromEnvelope(parsed);
    }
    throw new RtDbError("INTERNAL", `admin request failed with status ${response.status}`);
  }

  private async request(
    method: "GET" | "POST" | "PATCH" | "DELETE",
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
