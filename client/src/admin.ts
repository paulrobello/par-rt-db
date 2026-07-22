import { RtDbError } from "./errors.js";
import type { SchemaJson } from "./protocol.js";
import type { SchemaDefinition } from "./schema.js";

export interface RtDbAdminClientOptions {
  url: string;
  adminKey: string;
  fetch?: typeof fetch;
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

  /** Fetches `db`'s schema and every document as JSONL text (see server `snapshot::export_database`). */
  async exportDb(db: string): Promise<string> {
    const response = await this.fetchImpl(`${this.url}/admin/export-db?db=${encodeURIComponent(db)}`, {
      method: "GET",
      headers: { Authorization: `Bearer ${this.adminKey}` },
    });
    if (!response.ok) {
      await this.throwFromResponse(response);
    }
    return await response.text();
  }

  /** Loads a JSONL snapshot from `exportDb` into `db` (see server `snapshot::import_database`). */
  async importDb(db: string, jsonl: string): Promise<void> {
    const response = await this.fetchImpl(`${this.url}/admin/import-db?db=${encodeURIComponent(db)}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${this.adminKey}`,
        "content-type": "application/x-ndjson",
      },
      body: jsonl,
    });
    if (!response.ok) {
      await this.throwFromResponse(response);
    }
  }

  private async throwFromResponse(response: Response): Promise<never> {
    const parsed: unknown = await response.json().catch(() => null);
    if (RtDbError.isEnvelope(parsed)) {
      throw RtDbError.fromEnvelope(parsed);
    }
    throw new RtDbError("INTERNAL", `admin request failed with status ${response.status}`);
  }

  private async request(method: "GET" | "POST", path: string, payload?: unknown): Promise<unknown> {
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
