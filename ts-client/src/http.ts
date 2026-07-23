import { RtDbError } from "./errors.js";
import type { AuthedUser, ScheduleInfo, ScheduleWhen, TransactionJson } from "./protocol.js";
import type { RtQuery } from "./query.js";

export interface RtDbHttpClientOptions {
  url: string;
  db: string;
  token: string;
  fetch?: typeof fetch;
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

  private async post(path: string, payload: unknown): Promise<unknown> {
    const response = await this.fetchImpl(`${this.url}${path}`, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        Authorization: `Bearer ${this.token}`,
      },
      body: JSON.stringify(payload),
    });
    const parsed: unknown = await response.json().catch(() => null);
    if (!response.ok) {
      if (RtDbError.isEnvelope(parsed)) {
        throw RtDbError.fromEnvelope(parsed);
      }
      throw new RtDbError("INTERNAL", `request failed with status ${response.status}`);
    }
    return parsed;
  }

  private async get(path: string, bearer: string): Promise<unknown> {
    const response = await this.fetchImpl(`${this.url}${path}`, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${bearer}`,
      },
    });
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
