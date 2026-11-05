import Foundation

// The in-memory admin surface (store.ts `InMemoryAdminClient`), extracted
// from ``InMemoryEngine`` (QA-003b) and moved verbatim into a sibling file.

// MARK: - Admin surface

/// The single database name the in-memory engine models (it is single-db),
/// surfaced on audit rows and the subscription inspector (store.ts
/// `IN_MEMORY_DB`).
private let inMemoryDb = "db"

/// Terminal kind a query resolves to (store.ts `queryTerminal`) — the
/// `terminal` field the server reports on `GET /admin/subscriptions`.
func queryTerminal(_ query: Query) -> String {
    if query.get != nil {
        return "get"
    }
    if query.count {
        return "count"
    }
    if query.first {
        return "first"
    }
    if query.unique {
        return "unique"
    }
    if query.distinct {
        return "distinct"
    }
    if query.aggregate != nil {
        return "aggregate"
    }
    // ENH-030: the ranked terminals are checked BEFORE `paginate` — mirroring
    // server `Query::terminal_name` — because `paginate` now composes with all
    // three, and the server reports the ranked terminal's own name for such a
    // query. (The payload-shape discriminator in `HttpClient` deliberately
    // stays paginate-first: a ranked+paginate result IS the paginated
    // envelope.)
    if query.search != nil {
        return "search"
    }
    if query.vectorSearch != nil {
        return "vectorSearch"
    }
    if query.hybridSearch != nil {
        return "hybridSearch"
    }
    if query.paginate != nil {
        return "paginate"
    }
    return "collect"
}

/// Invalidation class the committer would assign (store.ts
/// `queryReadSetClass`): point, indexed (eq/range), ordered (take/first),
/// else table-level.
func queryReadSetClass(_ query: Query) -> String {
    if query.get != nil {
        return "point"
    }
    let hasIndex = query.index != nil || !query.eq.isEmpty
        || query.gt != nil || query.gte != nil || query.lt != nil || query.lte != nil
    if query.order != nil || query.first || query.take != nil {
        return "ordered"
    }
    if hasIndex {
        return "indexed"
    }
    return "table"
}

/// One row seeded through `InMemoryAdminClient.seedAudit` — every field
/// optional, defaulting exactly as the TS `Partial<AuditEntry>` does.
public struct InMemoryAuditSeedRow {
    /// Audit row id; auto-increments when nil.
    public var id: Int64?
    /// Write time, epoch ms; defaults to the engine clock.
    public var tsMs: Int64?
    /// Which database; defaults to the engine's single db name.
    public var db: String?
    /// Which table.
    public var table: String?
    /// The op; nil for system-initiated rows.
    public var op: String?
    /// Which document.
    public var docId: String?
    /// The per-row principal.
    public var principal: String?
    /// Tap arm; defaults to "client".
    public var source: String?

    public init(
        id: Int64? = nil, tsMs: Int64? = nil, db: String? = nil, table: String? = nil,
        op: String? = nil, docId: String? = nil, principal: String? = nil, source: String? = nil
    ) {
        self.id = id
        self.tsMs = tsMs
        self.db = db
        self.table = table
        self.op = op
        self.docId = docId
        self.principal = principal
        self.source = source
    }
}

/// In-memory admin surface (store.ts `InMemoryAdminClient`): a seedable
/// audit log and the live subscription inspector, bound to an engine's clock
/// and subscription registry. Invalidation counters read zero (the harness
/// does not track them); the audit log is seedable rather than auto-recorded.
/// Not Sendable — same-context test infra.
public final class InMemoryAdminClient {
    /// One live subscription, as the inspector reports it.
    public struct SubscriptionSnapshot {
        public var table: String
        public var terminal: String
        public var readSetClass: String
    }

    private let now: () -> Int64
    private let liveSubs: () -> [SubscriptionSnapshot]
    private var auditLog: [AuditEntry] = []
    private var auditSeq: Int64 = 0

    init(now: @escaping () -> Int64, subs: @escaping () -> [SubscriptionSnapshot]) {
        self.now = now
        liveSubs = subs
    }

    /// Seed audit rows directly (test affordance). `id` auto-increments when
    /// omitted; `tsMs` defaults to the engine clock; `op`/`principal`
    /// default to nil and the rest to empty strings.
    public func seedAudit(_ rows: [InMemoryAuditSeedRow]) {
        for row in rows {
            let id: Int64
            if let explicit = row.id {
                id = explicit
                if explicit > auditSeq {
                    auditSeq = explicit
                }
            } else {
                auditSeq += 1
                id = auditSeq
            }
            auditLog.append(
                AuditEntry(
                    id: id,
                    tsMs: row.tsMs ?? now(),
                    db: row.db ?? inMemoryDb,
                    table: row.table ?? "",
                    op: row.op,
                    docId: row.docId ?? "",
                    principal: row.principal,
                    source: row.source ?? "client"
                )
            )
        }
    }

    /// Durable audit-log entries, newest-first (store.ts `getAudit`); each
    /// option is an equality filter combined with AND; limit/offset page.
    public func getAudit(db: String? = nil, options: AuditQuery? = nil) -> [AuditEntry] {
        var rows = auditLog
        if let db {
            rows = rows.filter { $0.db == db }
        }
        if let table = options?.table {
            rows = rows.filter { $0.table == table }
        }
        if let op = options?.op {
            rows = rows.filter { $0.op == op }
        }
        if let principal = options?.principal {
            rows = rows.filter { $0.principal == principal }
        }
        if let source = options?.source {
            rows = rows.filter { $0.source == source }
        }
        let sorted = rows.sorted {
            if $0.tsMs != $1.tsMs {
                return $0.tsMs > $1.tsMs
            }
            return $0.id > $1.id
        }
        let offset = Int(options?.offset ?? 0)
        let limit = Int(options?.limit ?? 100)
        return Array(sorted.dropFirst(offset).prefix(limit))
    }

    /// Live subscription inspector, mirroring `GET /admin/subscriptions`. A
    /// db filter that isn't the engine's single db reads empty.
    public func listSubscriptions(db: String? = nil) -> SubscriptionsResponse {
        if let db, db != inMemoryDb {
            return SubscriptionsResponse(
                subscriptions: [], subsRerunsTotal: 0, subsSkipsPointTotal: 0,
                subsSkipsIndexedTotal: 0, subsSkipsOrderedTotal: 0, subsMissedPushesTotal: 0,
                perDb: []
            )
        }
        let subscriptions = liveSubs().map {
            SubscriptionInfo(
                db: inMemoryDb, table: $0.table, terminal: $0.terminal,
                readSetClass: $0.readSetClass, principal: nil
            )
        }
        let perDb: [DbSubCounters] = subscriptions.isEmpty ? [] : [
            DbSubCounters(
                db: inMemoryDb, reruns: 0, skipsPoint: 0, skipsIndexed: 0, skipsOrdered: 0,
                missed: 0, skips: 0, rerunRatio: 0
            )
        ]
        return SubscriptionsResponse(
            subscriptions: subscriptions, subsRerunsTotal: 0, subsSkipsPointTotal: 0,
            subsSkipsIndexedTotal: 0, subsSkipsOrderedTotal: 0, subsMissedPushesTotal: 0,
            perDb: perDb
        )
    }

    /// Active interactive sessions — the engine mints none; always empty.
    public func listSessions(filter: (user: String?, limit: Int?)? = nil) -> [SessionInfo] {
        _ = filter
        return []
    }

    /// Revoke one session by token hash — no-op in-memory.
    public func revokeSession(_ tokenHash: String) {
        _ = tokenHash
    }

    /// Revoke every session for a user — no-op; reports zero revoked.
    public func revokeUserSessions(_ userId: String) -> (ok: Bool, revoked: Int) {
        _ = userId
        return (true, 0)
    }

    /// Anon -> real account merge — no-op; resolves an empty report.
    public func mergeUsers(_ anonUserId: String, _ realUserId: String) -> MergeReport {
        _ = anonUserId
        _ = realUserId
        return MergeReport()
    }
}
