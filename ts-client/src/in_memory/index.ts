/**
 * Public surface of the in-memory harness — re-exports the names the former
 * `src/in_memory.ts` monolith exported, from the modules it was decomposed
 * into (ARC-201; mirrors the rust-client's `in_memory/` layout):
 *
 * - `./store.ts` — the client core (rows, transactions, schedules, workflows,
 *   storage, presence, admin surface).
 * - `./query.ts` — the query engine (`executeQuery` dispatcher + one executor
 *   per terminal).
 * - `./migrate.ts` — the migration engine (one function per directive kind).
 * - `./validate.ts` — filter validation/evaluation + value predicates.
 */

export type { InMemoryRtDbClientOptions } from "./store.js";
export {
  InMemoryAdminClient,
  InMemoryRtDbClient,
  MAX_AFFECTED_ROWS_PER_TXN,
  MAX_BY_QUERY_STEPS_PER_TXN,
  MAX_STEPS,
  PresenceRooms,
  worstCaseAffected,
} from "./store.js";
export type { FieldMap } from "./validate.js";
export { evalFilterExpr, validateFilter } from "./validate.js";
