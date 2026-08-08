import { RtDbError } from "./errors.js";
import { parseStepResults, type StepResult } from "./mutation.js";
import type {
  AuthedUser,
  BatchQueryOutcomeJson,
  QueryJson,
  ScheduleInfo,
  ScheduleWhen,
  TransactionJson,
} from "./protocol.js";
import type { RtQuery } from "./query.js";

export interface RtDbHttpClientOptions {
  url: string;
  db: string;
  token: string;
  fetch?: typeof fetch;
}

/** Result of an upload: the server-assigned id, content digest, and size. */
export interface UploadResult {
  id: string;
  sha256: string;
  size: number;
  contentType?: string;
}

/** Metadata for a stored file. `creationTime` is epoch milliseconds. */
export interface FileMetadata {
  id: string;
  sha256: string;
  size: number;
  contentType?: string;
  creationTime: number;
}

/** A signed, time-limited storage URL: `url` works until `expiresAt` (epoch ms). */
export interface SignedUrl {
  url: string;
  expiresAt: number;
}

/** Image-transform query params appended to a storage serve URL (ENH-014). */
export interface TransformOpts {
  w?: number;
  h?: number;
  fit?: "cover" | "contain" | "scale-down";
  q?: number;
  format?: "jpeg" | "png" | "auto";
}

/** Append image-transform query params to a storage URL. Omits unset opts. */
export function appendImageParams(url: string, opts: TransformOpts): string {
  const parts: string[] = [];
  const push = (k: string, v: string | undefined) => {
    if (v !== undefined) parts.push(`${k}=${encodeURIComponent(v)}`);
  };
  push("w", opts.w?.toString());
  push("h", opts.h?.toString());
  push("fit", opts.fit);
  push("q", opts.q?.toString());
  // "auto" is the server default — omit so the URL stays minimal (rust parity).
  if (opts.format && opts.format !== "auto") push("format", opts.format);
  return parts.length ? `${url}?${parts.join("&")}` : url;
}

/** One-shot HTTP client for machine callers (or any Bearer token). */
export class RtDbHttpClient {
  private readonly url: string;
  private readonly db: string;
  private readonly token: string;
  private readonly fetchImpl: typeof fetch;

  constructor(options: RtDbHttpClientOptions) {
    this.url = options.url.replace(/\/+$/, "");
    this.db = options.db;
    this.token = options.token;
    this.fetchImpl = options.fetch ?? globalThis.fetch;
  }

  async query<R>(query: RtQuery<R>): Promise<R> {
    const body = await this.post("/api/query", { db: this.db, query: query.json });
    return (body as { result: R }).result;
  }

  /**
   * Fan out over many queries against this client's db in one round trip
   * (`POST /api/query-batch`). Auth and owner resolution run once for the whole
   * request; each query's outcome lands in its own aligned slot. A per-query
   * execution error becomes that slot's `{ok:false,error}` (standard envelope)
   * and never throws — only a db-level auth failure (or empty `queries`) throws
   * `RtDbError`. The returned array is length-aligned with the input order.
   */
  async batchQuery(queries: QueryJson[]): Promise<BatchQueryOutcomeJson[]> {
    const body = await this.post("/api/query-batch", { db: this.db, queries });
    return (body as { results: BatchQueryOutcomeJson[] }).results;
  }

  /**
   * `opts.idempotencyKey` is an idempotency key, not a display/tracking id:
   * supply the *same* value again to safely retry a mutation whose result you
   * never received instead of double-applying it. The server does not
   * fingerprint the transaction body, so reusing a key for a different
   * mutation replays the first one's cached result. Omit it for ordinary
   * one-shot calls.
   *
   * `opts.mutId` is a deprecated alias for `opts.idempotencyKey` and remains
   * accepted for backwards compatibility; it is unrelated to the wire-only
   * `mutId` reply-correlation field used on the WS transport.
   */
  async mutate(
    txn: TransactionJson,
    opts?: {
      idempotencyKey?: string;
      /** @deprecated use `idempotencyKey`. Unrelated to the WS reply-correlation field. */
      mutId?: string;
    },
  ): Promise<StepResult[]> {
    const body = await this.post("/api/mutate", {
      db: this.db,
      txn,
      idempotencyKey: opts?.idempotencyKey ?? opts?.mutId,
    });
    return parseStepResults((body as { results: unknown[] }).results);
  }

  /** Schedules `txn` for `when`; the server validates cron expressions. */
  async schedule(txn: TransactionJson, when: ScheduleWhen): Promise<{ id: string }> {
    const body = await this.post("/api/schedule", { db: this.db, when, txn });
    return { id: (body as { id: string }).id };
  }

  async cancelSchedule(id: string): Promise<void> {
    await this.post(`/api/schedule/${encodeURIComponent(id)}/cancel`, { db: this.db });
  }

  async pauseSchedule(id: string): Promise<void> {
    await this.post(`/api/schedule/${encodeURIComponent(id)}/pause`, { db: this.db });
  }

  async resumeSchedule(id: string): Promise<void> {
    await this.post(`/api/schedule/${encodeURIComponent(id)}/resume`, { db: this.db });
  }

  async listSchedules(): Promise<ScheduleInfo[]> {
    const body = await this.post("/api/schedules", { db: this.db });
    return (body as { schedules: ScheduleInfo[] }).schedules;
  }

  /**
   * Validate an arbitrary player-supplied session/machine token via
   * `GET /auth/validate`, returning the authed user. Unlike the client's own
   * configured token, the token to validate is passed as an argument and may
   * be either a session or a machine token. An invalid/expired token surfaces
   * as the standard `RtDbError` auth envelope.
   */
  async validateSessionToken(token: string): Promise<AuthedUser> {
    const body = await this.get("/auth/validate", token);
    return (body as { user: AuthedUser }).user;
  }

  /**
   * Resolve the principal the client is authenticated as, via `GET /auth/me`
   * with the client's own bearer. Session-only — unlike `validateSessionToken`,
   * which validates an arbitrary token passed as an argument. Machine tokens are
   * rejected by the server (401) and surface as the standard `RtDbError` envelope.
   */
  async authMe(): Promise<AuthedUser> {
    const body = await this.get("/auth/me", this.token);
    return (body as { user: AuthedUser }).user;
  }

  /** Upload raw bytes; the db is this client's db (injected into the path). */
  async upload(bytes: Uint8Array, contentType?: string): Promise<UploadResult> {
    const headers: Record<string, string> = { Authorization: `Bearer ${this.token}` };
    if (contentType) {
      headers["content-type"] = contentType;
    }
    const response = await this.fetchImpl(
      `${this.url}/api/storage/${encodeURIComponent(this.db)}`,
      // `bytes` is a valid BodyInit at runtime; the cast works around the TS
      // lib's `Uint8Array<ArrayBufferLike>` ↔ `BodyInit` variance.
      { method: "POST", headers, body: bytes as BodyInit },
    );
    return (await this.parse(response)) as UploadResult;
  }

  async deleteFile(id: string): Promise<void> {
    await this.fetchImpl(
      `${this.url}/api/storage/${encodeURIComponent(this.db)}/${encodeURIComponent(id)}`,
      { method: "DELETE", headers: { Authorization: `Bearer ${this.token}` } },
    ).then((r) => this.requireOk(r));
  }

  async getFileMetadata(id: string): Promise<FileMetadata> {
    const body = await this.get(
      `/api/storage/${encodeURIComponent(this.db)}/${encodeURIComponent(id)}/metadata`,
      this.token,
    );
    return body as FileMetadata;
  }

  /** Mint a signed, time-limited URL for `id` via the server (GET mint endpoint). */
  async getSignedUrl(id: string, ttlSeconds?: number): Promise<SignedUrl> {
    let path = `/api/storage/${encodeURIComponent(this.db)}/${encodeURIComponent(id)}/signed-url`;
    if (ttlSeconds !== undefined) {
      path += `?ttlSeconds=${ttlSeconds}`;
    }
    return (await this.get(path, this.token)) as SignedUrl;
  }

  /** The public serve URL for `id` — no fetch, the browser consumes it. */
  getUrl(id: string): string {
    return `${this.url}/storage/${encodeURIComponent(id)}`;
  }

  /** The public serve URL for `id` with image-transform params applied. */
  transformUrl(id: string, opts: TransformOpts): string {
    return appendImageParams(this.getUrl(id), opts);
  }

  private async post(path: string, payload: unknown): Promise<unknown> {
    const response = await this.fetchImpl(`${this.url}${path}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        Authorization: `Bearer ${this.token}`,
      },
      body: JSON.stringify(payload),
    });
    return this.parse(response);
  }

  private async get(path: string, bearer: string): Promise<unknown> {
    const response = await this.fetchImpl(`${this.url}${path}`, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${bearer}`,
      },
    });
    return this.parse(response);
  }

  /** Throws on a non-2xx `response` (envelope-aware); resolves nothing on success. */
  private async requireOk(response: Response): Promise<void> {
    if (response.ok) {
      return;
    }
    const parsed: unknown = await response.json().catch(() => null);
    if (RtDbError.isEnvelope(parsed)) {
      throw RtDbError.fromEnvelope(parsed);
    }
    throw new RtDbError("INTERNAL", `request failed with status ${response.status}`);
  }

  /** Parses `response.json()`, throwing an envelope-aware error on non-2xx. */
  private async parse(response: Response): Promise<unknown> {
    const parsed: unknown = await response.json().catch(() => null);
    if (!response.ok) {
      if (RtDbError.isEnvelope(parsed)) {
        throw RtDbError.fromEnvelope(parsed);
      }
      throw new RtDbError("INTERNAL", `request failed with status ${response.status}`);
    }
    return parsed;
  }
}
