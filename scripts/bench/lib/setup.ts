/** Shared bench-db bootstrap: one `items` table used by scenarios a, b, and d.
 * `createDb`/`pushSchema` are both idempotent (additive-only schema, and
 * create-db on an existing name is treated as already-there), so this is safe
 * to call once per server URL on every run. */

import { RtDbAdminClient, defineSchema, defineTable, t } from "@par-rt-db/client";

export const benchSchema = defineSchema({
  items: defineTable({
    seed: t.number(),
    text: t.string(),
    createdAtMs: t.number(),
  }).index("by_creation", ["createdAtMs"]),
});

export interface BenchTarget {
  url: string;
  admin: RtDbAdminClient;
  db: string;
  token: string;
}

/** Ensures `db` exists on the server at `url` with `benchSchema` applied, and
 * mints a fresh machine token scoped to it. */
export async function ensureBenchDb(
  url: string,
  adminKey: string,
  db: string,
): Promise<BenchTarget> {
  const admin = new RtDbAdminClient({ url, adminKey });
  try {
    await admin.createDb(db);
  } catch {
    // Already exists — createDb has no "if not exists" flag, so a repeat run
    // against the same db/server surfaces a Conflict here, which is fine.
  }
  await admin.pushSchema(db, benchSchema);
  const { token } = await admin.mintToken(db, `bench-${Date.now()}`);
  return { url, admin, db, token };
}
