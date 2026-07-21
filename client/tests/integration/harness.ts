import { RtDbAdminClient } from "../../src/admin.js";
import { RtDbClient } from "../../src/client.js";
import { RtDbHttpClient } from "../../src/http.js";
import type { SchemaDefinition } from "../../src/schema.js";

export interface TestServer {
  url: string;
  adminKey: string;
}

/** Returns the configured server, or null when integration env is absent (suite should skip). */
export function testServer(): TestServer | null {
  const url = process.env.RTDB_TEST_SERVER_URL;
  const adminKey = process.env.RTDB_TEST_ADMIN_KEY;
  if (!url || !adminKey) {
    return null;
  }
  return { url, adminKey };
}

export function uniqueDbName(): string {
  return `t${process.hrtime.bigint().toString(36)}`;
}

/** Creates a fresh database, pushes `schema`, and mints a machine token for it. */
export async function provisionDb(
  server: TestServer,
  schema: SchemaDefinition<any>,
): Promise<{ db: string; token: string; admin: RtDbAdminClient }> {
  const admin = new RtDbAdminClient({ url: server.url, adminKey: server.adminKey });
  const db = uniqueDbName();
  await admin.createDb(db);
  await admin.pushSchema(db, schema);
  const { token } = await admin.mintToken(db, "integration");
  return { db, token, admin };
}

export function httpClient(server: TestServer, db: string, token: string): RtDbHttpClient {
  return new RtDbHttpClient({ url: server.url, db, token });
}

export function wsClient(server: TestServer, db: string, token: string): RtDbClient {
  return new RtDbClient({ url: server.url, db, getToken: () => token });
}
