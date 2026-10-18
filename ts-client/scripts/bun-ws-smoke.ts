/**
 * Bun-runtime WS smoke gate for the reactive client (manual, opt-in).
 *
 * Reproduces the filed defect: a reactive `RtDbClient` constructed WITHOUT a
 * `getToken` option, driven only by `setToken()` + `connect()` — the bun/CLI
 * shape with no cookie jar. Against the fix this reaches `authenticated`,
 * completes one full `mutate()` round trip over the live socket, and exits 0
 * with near-zero idle CPU; against the old behavior the tokenless dial was
 * answered 4401 and the client landed idle.
 *
 *   bun scripts/bun-ws-smoke.ts     # needs RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY
 *
 * Provisions a fresh `t`-prefixed database and deletes it in a `finally`, so a
 * failed run leaves nothing behind.
 */
import { RtDbClient } from "../src/client.js";
import { mutation } from "../src/mutation.js";
import { defineSchema, defineTable, t } from "../src/schema.js";
import { provisionDb, testServer, type TestServer } from "../tests/integration/harness.js";

const server: TestServer | null = testServer();
if (server === null) {
  console.error("bun-ws-smoke: set RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY (a real server)");
  process.exit(1);
}
const srv: TestServer = server;

const schema = defineSchema({
  smoke: defineTable({ label: t.string() }).index("by_label", ["label"]),
});

const provisioned = await provisionDb(srv, schema);
const client = new RtDbClient({ url: srv.url, db: provisioned.db });
try {
  // Deliberately NO getToken option: this is the cookie-mode constructor shape
  // whose dial must still carry the setToken() credential.
  client.setToken(provisioned.token);
  client.connect();

  const deadline = Date.now() + 15000;
  while (client.getAuthState() !== "authenticated") {
    if (Date.now() > deadline) {
      throw new Error(
        `never reached authenticated (state=${client.getAuthState()}, conn=${client.getConnectionState()})`,
      );
    }
    await new Promise((r) => setTimeout(r, 50));
  }

  const [inserted] = await client.mutate(
    mutation()
      .insert("smoke", { label: `bun-smoke-${Date.now()}` })
      .build(),
  );
  const id = (inserted as { id: string }).id;
  if (typeof id !== "string") {
    throw new Error(`insert returned ${JSON.stringify(inserted)}`);
  }
  await client.mutate(mutation().delete("smoke", id).build());
  console.log(
    `bun-ws-smoke: ok — authenticated + mutate round trip (${id}) under ${typeof Bun !== "undefined" ? `bun ${Bun.version}` : "node"}`,
  );
  const cpu = process.cpuUsage();
  console.log(`bun-ws-smoke: cpuUsage ${JSON.stringify(cpu)}`);
} finally {
  client.close();
  await provisioned.admin.deleteDb(provisioned.db, provisioned.db);
}
process.exit(0);
