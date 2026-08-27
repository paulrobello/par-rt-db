import { RtDbError } from "./errors.js";
import { parseStepResults, type StepResult } from "./mutation.js";
import type {
  AuthedUser,
  BatchQueryOutcomeJson,
  QueryJson,
  ScheduleInfo,
  ScheduleWhen,
  WorkflowInfo,
  WorkflowSpec,
  WorkflowStatus,
  TransactionJson,
} from "./protocol.js";
import { PROTOCOL_VERSION } from "./protocol.js";
import type { RtQuery } from "./query.js";

/**
 * Options for configuring the `RtDbHttpClient`.
 */
export interface RtDbHttpClientOptions {
  /**
   * Base URL of the Realtime Database instance.
   */
  url: string;
  /**
   * The target database name.
   */
  db: string;
  /**
   * The authorization Bearer token.
   */
  token: string;
  /**
   * Optional custom fetch implementation to override the global fetch.
   */
  fetch?: typeof fetch;
}

/** Result of an upload: the server-assigned id, content digest, and size. */
export interface UploadResult {
  id: string;
  sha256: string;
  size: number;
  contentType?: string;
}

/** Upload input accepted by {@link RtDbHttpClient.upload}: any `BodyInit`-compatible
 * value the underlying `fetch` can stream without buffering. `Blob` and
 * `ReadableStream` let a caller upload a multi-GB file without holding it in JS
 * memory (ENH-021); `Uint8Array` remains the canonical in-memory form. */
export type UploadInput = Uint8Array | Blob | ReadableStream<Uint8Array> | ArrayBuffer | string;

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
    // Bind the global: browsers require Window as fetch's receiver, and a
    // stored-unbound reference throws "Illegal invocation" when called.
    this.fetchImpl = options.fetch ?? globalThis.fetch.bind(globalThis);
  }

  /**
   * Executes a database query.
   */
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

  /** Cancels a scheduled job. Resolves `false` for an unknown/already-terminal
   * job (a no-op), `true` when a live job was cancelled. */
  async cancelSchedule(id: string): Promise<boolean> {
    const body = await this.post(`/api/schedule/${encodeURIComponent(id)}/cancel`, {
      db: this.db,
    });
    return (body as { ok: boolean }).ok;
  }

  /** Pauses a scheduled job until `resumeSchedule`. Same `ok` contract as
   * `cancelSchedule`. */
  async pauseSchedule(id: string): Promise<boolean> {
    const body = await this.post(`/api/schedule/${encodeURIComponent(id)}/pause`, {
      db: this.db,
    });
    return (body as { ok: boolean }).ok;
  }

  /** Resumes a paused scheduled job. Same `ok` contract as `cancelSchedule`. */
  async resumeSchedule(id: string): Promise<boolean> {
    const body = await this.post(`/api/schedule/${encodeURIComponent(id)}/resume`, {
      db: this.db,
    });
    return (body as { ok: boolean }).ok;
  }

  /**
   * Lists all workflows/tasks scheduled on the server.
   */
  async listSchedules(): Promise<ScheduleInfo[]> {
    const body = await this.post("/api/schedules", { db: this.db });
    return (body as { schedules: ScheduleInfo[] }).schedules;
  }

  /** Starts a durable workflow run from `spec` (FM-29). The HTTP route returns
   * only the new run's `{id}` — fetch the full row via the admin get route or
   * the WS client's `startWorkflow`, which resolves the whole `WorkflowInfo`. */
  async startWorkflow(spec: WorkflowSpec): Promise<{ id: string }> {
    const body = await this.post("/api/workflows", { db: this.db, spec });
    return { id: (body as { id: string }).id };
  }

  /** Cancels a pending/running workflow by id (FM-29). Resolves `false` for an
   * unknown/terminal run (a no-op), `true` when a live run was cancelled. */
  async cancelWorkflow(id: string): Promise<boolean> {
    const body = await this.post(`/api/workflows/${encodeURIComponent(id)}/cancel`, {
      db: this.db,
    });
    return (body as { cancelled: boolean }).cancelled;
  }

  /** Delivers an out-of-band signal to a waiting run's `awaitSignal` step
   * (latest-wins payload). Resolves `true` on delivery; typed failures reject
   * via the standard error envelope — unknown id (`NOT_FOUND`), not waiting /
   * name mismatch (`CONFLICT`). */
  async signalWorkflow(id: string, name: string, payload?: unknown): Promise<boolean> {
    const body = await this.post(`/api/workflows/${encodeURIComponent(id)}/signal`, {
      db: this.db,
      name,
      ...(payload === undefined ? {} : { payload }),
    });
    return (body as { delivered: boolean }).delivered;
  }

  /** Lists workflow runs, newest first, optionally filtered by `status` (FM-29). */
  async listWorkflows(status?: WorkflowStatus): Promise<WorkflowInfo[]> {
    const body = await this.post("/api/workflows/list", {
      db: this.db,
      ...(status === undefined ? {} : { status }),
    });
    return (body as { workflows: WorkflowInfo[] }).workflows;
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

  /** Upload a streaming-capable body (ENH-021); the db is this client's db
   *  (injected into the path). The input is forwarded to `fetch` verbatim so a
   *  `Blob`/`ReadableStream` streams without buffering in JS memory. */
  async upload(body: UploadInput, contentType?: string): Promise<UploadResult> {
    if (
      !(body instanceof Uint8Array) &&
      typeof Blob !== "undefined" &&
      !(body instanceof Blob) &&
      typeof ReadableStream !== "undefined" &&
      !(body instanceof ReadableStream) &&
      !(body instanceof ArrayBuffer) &&
      typeof body !== "string"
    ) {
      throw new RtDbError(
        "BAD_REQUEST",
        "upload body must be Uint8Array, Blob, ReadableStream, ArrayBuffer, or string",
      );
    }
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.token}`,
      // ARC-013: lets the server diagnose/reject a version mismatch instead
      // of a generic 400 from `deny_unknown_fields`.
      "X-Rtdb-Protocol": String(PROTOCOL_VERSION),
    };
    if (contentType) {
      headers["content-type"] = contentType;
    }
    const path = `/api/storage/${encodeURIComponent(this.db)}`;
    const response = await this.fetchImpl(`${this.url}${path}`, {
      method: "POST",
      headers,
      body: body as BodyInit,
    });
    return (await this.parse(response, path)) as UploadResult;
  }

  /**
   * Deletes a stored file by its unique identifier.
   */
  async deleteFile(id: string): Promise<void> {
    await this.fetchImpl(
      `${this.url}/api/storage/${encodeURIComponent(this.db)}/${encodeURIComponent(id)}`,
      {
        method: "DELETE",
        headers: {
          Authorization: `Bearer ${this.token}`,
          // ARC-013: lets the server diagnose/reject a version mismatch instead
          // of a generic 400 from `deny_unknown_fields`.
          "X-Rtdb-Protocol": String(PROTOCOL_VERSION),
        },
      },
    ).then((r) => this.requireOk(r));
  }

  /**
   * Retrieves metadata for a stored file.
   */
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
        // ARC-013: lets the server diagnose/reject a version mismatch instead
        // of a generic 400 from `deny_unknown_fields`.
        "X-Rtdb-Protocol": String(PROTOCOL_VERSION),
      },
      body: JSON.stringify(payload),
    });
    return this.parse(response, path);
  }

  private async get(path: string, bearer: string): Promise<unknown> {
    const response = await this.fetchImpl(`${this.url}${path}`, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${bearer}`,
        // ARC-013: lets the server diagnose/reject a version mismatch instead
        // of a generic 400 from `deny_unknown_fields`.
        "X-Rtdb-Protocol": String(PROTOCOL_VERSION),
      },
    });
    return this.parse(response, path);
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
  private async parse(response: Response, path: string): Promise<unknown> {
    const parsed: unknown = await response.json().catch(() => null);
    if (!response.ok) {
      if (RtDbError.isEnvelope(parsed)) {
        throw RtDbError.fromEnvelope(parsed);
      }
      throw new RtDbError("INTERNAL", `request failed with status ${response.status}`);
    }
    // A 2xx must carry a JSON object body. Returning null here (empty body,
    // HTML gateway page, invalid JSON) TypeErrors downstream when callers
    // destructure `.result` and friends instead of surfacing an RtDbError.
    if (parsed === null || typeof parsed !== "object") {
      throw new RtDbError("INTERNAL", `${path} returned 2xx with no JSON object body`);
    }
    return parsed;
  }
}
