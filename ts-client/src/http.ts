import { RtDbError } from "./errors.js";
import type { AuthedUser, ScheduleInfo, ScheduleWhen, TransactionJson } from "./protocol.js";
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
   * `opts.mutId` is an idempotency key, not a display/tracking id: supply the
   * *same* value again to safely retry a mutation whose result you never
   * received instead of double-applying it. The server does not fingerprint
   * the transaction body, so reusing a key for a different mutation replays
   * the first one's cached result. Omit it for ordinary one-shot calls.
   */
  async mutate(txn: TransactionJson, opts?: { mutId?: string }): Promise<unknown[]> {
    const body = await this.post("/api/mutate", {
      db: this.db,
      txn,
      idempotencyKey: opts?.mutId,
    });
    return (body as { results: unknown[] }).results;
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

  /** The public serve URL for `id` — no fetch, the browser consumes it. */
  getUrl(id: string): string {
    return `${this.url}/storage/${encodeURIComponent(id)}`;
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
