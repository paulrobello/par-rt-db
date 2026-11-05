/**
 * In-memory admin surface for the par-rt-db test harness (split out of
 * `./store.ts`, QA-003b): `InMemoryAdminClient` and its private query-shape
 * helpers, bound to an `InMemoryRtDbClient`'s clock and subscription registry.
 * Re-exported from `./store.ts`, which remains the module the public
 * `./index.ts` façade imports.
 */

import type {
  AuditEntry,
  DbSubCounters,
  GetAuditOptions,
  MergeReport,
  SessionInfo,
  SubscriptionInfo,
  SubscriptionsResponse,
} from "../admin.js";
import type { QueryJson } from "../protocol.js";
import type { Subscription } from "./store.js";

/** The single database name the in-memory harness models (it is single-db).
 *  Surfaced on audit rows and the subscription inspector for shape parity with
 *  `RtDbAdminClient`, which reads per-db data on a real multi-db server. */
const IN_MEMORY_DB = "db";

/** Terminal step kind a query resolves to (the `terminal` field the server
 *  reports on `GET /admin/subscriptions`). Best-effort derivation from the
 *  query JSON — sufficient for shape parity in the harness. */
function queryTerminal(q: QueryJson): string {
  if (q.get !== undefined) return "get";
  if (q.count) return "count";
  if (q.first) return "first";
  if (q.unique) return "unique";
  if (q.distinct) return "distinct";
  if (q.aggregate !== undefined) return "aggregate";
  // ENH-030: the ranked terminals outrank `paginate`, which composes with them
  // as a peer clause rather than replacing them — the server's `cq.terminal`
  // likewise stays `search`/`vectorSearch`/`hybridSearch` for a paginated
  // ranked query, so these three must be tested first.
  if (q.search !== undefined) return "search";
  if (q.vectorSearch !== undefined) return "vectorSearch";
  if (q.hybridSearch !== undefined) return "hybridSearch";
  if (q.paginate !== undefined) return "paginate";
  return "collect";
}

/** Invalidation class the committer would assign (the `readSetClass` field the
 *  server reports): point reads, indexed (eq/range) reads, ordered (take/first)
 *  reads, else table-level. Best-effort — the harness does not track the real
 *  skip logic, only the shape. */
function queryReadSetClass(q: QueryJson): string {
  if (q.get !== undefined) return "point";
  const hasIndex =
    q.index !== undefined ||
    (q.eq?.length ?? 0) > 0 ||
    q.gt !== undefined ||
    q.gte !== undefined ||
    q.lt !== undefined ||
    q.lte !== undefined;
  if (q.order !== undefined || q.first || q.take !== undefined) return "ordered";
  if (hasIndex) return "indexed";
  return "table";
}

/** In-memory admin surface mirroring `RtDbAdminClient`: a seedable durable
 *  audit log (`getAudit`) and the live subscription inspector
 *  (`listSubscriptions`). Bound to an `InMemoryRtDbClient`'s clock and
 *  subscription registry so admin-keyed consumers (audit backstops, quota
 *  inspection) are unit-testable with no network. The harness does not track
 *  invalidation counters, so those totals read zero; the audit log is seedable
 *  rather than auto-recorded (tests assert the read shape, not DocOp
 *  provenance). */
export class InMemoryAdminClient {
  private readonly auditLog: AuditEntry[] = [];
  private auditSeq = 0;

  constructor(private readonly backplane: { now: () => number; subs: readonly Subscription[] }) {}

  /** Seed audit rows directly (test affordance). `id` auto-increments when
   *  omitted; `tsMs` defaults to the host clock; `op`/`principal` default to
   *  null and the rest to empty strings, matching the `AuditEntry` shape. */
  seedAudit(rows: Array<Partial<AuditEntry>>): void {
    for (const row of rows) {
      const id = row.id ?? ++this.auditSeq;
      if (row.id !== undefined && row.id > this.auditSeq) this.auditSeq = row.id;
      this.auditLog.push({
        id,
        tsMs: row.tsMs ?? this.backplane.now(),
        db: row.db ?? IN_MEMORY_DB,
        table: row.table ?? "",
        op: row.op ?? null,
        docId: row.docId ?? "",
        principal: row.principal ?? null,
        source: row.source ?? "client",
      });
    }
  }

  /** Durable audit-log entries, newest-first, mirroring `GET /admin/audit`.
   *  Each of `db`/`table`/`op`/`principal`/`source` is an optional equality
   *  filter (combined AND); `limit`/`offset` page (defaults 100/0). */
  getAudit(opts?: GetAuditOptions): Promise<AuditEntry[]> {
    let rows = this.auditLog;
    if (opts?.db) rows = rows.filter((r) => r.db === opts.db);
    if (opts?.table) rows = rows.filter((r) => r.table === opts.table);
    if (opts?.op) rows = rows.filter((r) => r.op === opts.op);
    if (opts?.principal) rows = rows.filter((r) => r.principal === opts.principal);
    if (opts?.source) rows = rows.filter((r) => r.source === opts.source);
    const sorted = [...rows].sort((a, b) => (a.tsMs !== b.tsMs ? b.tsMs - a.tsMs : b.id - a.id));
    const offset = opts?.offset ?? 0;
    const limit = opts?.limit ?? 100;
    return Promise.resolve(sorted.slice(offset, offset + limit));
  }

  /** Live subscription inspector, mirroring `GET /admin/subscriptions`. Returns
   *  the currently-registered queries as `SubscriptionInfo` rows plus per-db
   *  rollup. Invalidation totals read zero (the harness does not track them). */
  listSubscriptions(opts?: { db?: string }): Promise<SubscriptionsResponse> {
    if (opts?.db && opts.db !== IN_MEMORY_DB) {
      return Promise.resolve(this.emptySubscriptions());
    }
    const subscriptions: SubscriptionInfo[] = this.backplane.subs.map((s) => ({
      db: IN_MEMORY_DB,
      table: s.table,
      terminal: queryTerminal(s.query),
      readSetClass: queryReadSetClass(s.query),
      principal: null,
    }));
    const perDb: DbSubCounters[] = subscriptions.length
      ? [
          {
            db: IN_MEMORY_DB,
            reruns: 0,
            skipsPoint: 0,
            skipsIndexed: 0,
            skipsOrdered: 0,
            missed: 0,
            skips: 0,
            rerunRatio: 0,
          },
        ]
      : [];
    return Promise.resolve({
      subscriptions,
      subsRerunsTotal: 0,
      subsSkipsPointTotal: 0,
      subsSkipsIndexedTotal: 0,
      subsSkipsOrderedTotal: 0,
      subsMissedPushesTotal: 0,
      perDb,
    });
  }

  /** Active interactive sessions. The harness mints no sessions, so this always
   *  reads empty — mirroring `RtDbAdminClient.listSessions` for admin consumers
   *  unit-tested with no network. */
  listSessions(_filter?: { user?: string; limit?: number }): Promise<SessionInfo[]> {
    return Promise.resolve([]);
  }

  /** Revoke one session by token hash. No-op in-memory; resolves void. */
  revokeSession(_tokenHash: string): Promise<void> {
    return Promise.resolve();
  }

  /** Revoke every session for a user. No-op in-memory; reports zero revoked. */
  revokeUserSessions(_userId: string): Promise<{ ok: boolean; revoked: number }> {
    return Promise.resolve({ ok: true, revoked: 0 });
  }

  /** Revoke every expired session. No-op in-memory (the harness mints no
   *  sessions); reports zero revoked. */
  revokeExpiredSessions(): Promise<{ ok: boolean; revoked: number }> {
    return Promise.resolve({ ok: true, revoked: 0 });
  }

  /** Anon→real account merge. No-op in-memory; resolves an empty report
   *  (nothing re-stamped, nothing repointed, no anon row deleted) — mirroring
   *  `RtDbAdminClient.mergeUsers` for admin consumers unit-tested with no
   *  network. */
  mergeUsers(_anonUserId: string, _realUserId: string): Promise<MergeReport> {
    return Promise.resolve({
      dbs: {},
      storageRepointed: 0,
      sessionsRepointed: 0,
      anonDeleted: false,
    });
  }

  private emptySubscriptions(): SubscriptionsResponse {
    return {
      subscriptions: [],
      subsRerunsTotal: 0,
      subsSkipsPointTotal: 0,
      subsSkipsIndexedTotal: 0,
      subsSkipsOrderedTotal: 0,
      subsMissedPushesTotal: 0,
      perDb: [],
    };
  }
}
