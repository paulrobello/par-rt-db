import { RtDbError } from "./errors.js";
import type { TransactionJson } from "./protocol.js";
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

  async mutate(txn: TransactionJson): Promise<unknown[]> {
    const body = await this.post("/api/mutate", { db: this.db, txn });
    return (body as { results: unknown[] }).results;
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
}
