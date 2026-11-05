import Foundation

// Client-coupled supporting types for the in-memory engine, extracted from
// ``InMemoryEngine`` (QA-003b) and moved verbatim into a sibling file: the
// protocol limits, the stored-row model, the client options, and the
// FM-33 cascade context.

/// Protocol-limit constants mirroring store.ts's exported caps (and the
/// server constants they mirror).
public enum InMemoryLimits {
    /// Maximum steps in one transaction, counted recursively (server
    /// `txn::MAX_STEPS`; the same number as `MutationLimits.maxSteps`).
    public static let maxSteps = MutationLimits.maxSteps
    /// Hard cap on rows a single `patchByQuery`/`deleteByQuery` step may touch
    /// (server `txn::MAX_BY_QUERY_ROWS`); a larger match set is truncated.
    public static let maxByQueryRows = 1000
    /// SEC-104: hard cap on the count of by-query steps in one txn (server
    /// `txn::MAX_BY_QUERY_STEPS_PER_TXN`).
    public static let maxByQueryStepsPerTxn = 16
    /// SEC-104: hard ceiling on the worst-case total documents a single txn
    /// may touch (server `txn::MAX_AFFECTED_ROWS_PER_TXN`).
    public static let maxAffectedRowsPerTxn = 10000
    /// FM-33: hard cap on the rows one initiating delete may cascade through
    /// via `onDelete` (server `txn::MAX_CASCADE_ROWS`).
    public static let maxCascadeRows = 10000
    /// Approximate cron re-fire interval for the in-memory stub (real 5-field
    /// cron parsing stays server-side; the engine only needs crons to re-arm).
    static let cronStepMs: Int64 = 60000
    /// Upper bound on an interval job's `everyMs`: one year in ms (server
    /// `scheduler::MAX_EVERY_MS`). The schedule path rejects a non-positive
    /// or over-cap value with BAD_REQUEST.
    public static let maxEveryMs: Int64 = 365 * 24 * 60 * 60 * 1000
    /// FM-29: hard cap on steps per workflow spec (server
    /// `workflows::MAX_WORKFLOW_STEPS`).
    static let maxWorkflowSteps = 64
    /// awaitSignal: serialized-size cap on a signal payload (server
    /// `workflows::MAX_SIGNAL_PAYLOAD_BYTES`), enforced on delivery so every
    /// surface gets the same bound.
    static let maxSignalPayloadBytes = 64 * 1024
    /// SEC-007: hard ceiling on `search.query`'s UTF-8 byte length (server
    /// `search::MAX_SEARCH_QUERY_BYTES`), enforced right after the
    /// not-empty check.
    public static let maxSearchQueryBytes = 4096
}

/// SEC-104: total documents a txn could touch in the worst case (store.ts
/// `worstCaseAffected`). Per-id steps count 1 each; control-flow steps
/// (`schedule`/`cancelSchedule`/`startWorkflow`/`cancelWorkflow`) count 0;
/// each by-query step counts up to its `limit` (default and cap
/// `maxByQueryRows`).
public func worstCaseAffected(_ txn: Transaction) -> Int {
    var total = 0
    for step in txn.steps {
        switch step {
        case let .patchByQuery(_, _, _, limit), let .deleteByQuery(_, _, limit):
            total += min(Int(limit ?? UInt32(InMemoryLimits.maxByQueryRows)), InMemoryLimits.maxByQueryRows)
        case let .insertMany(_, docs), let .upsertMany(_, _, docs):
            total += docs.count
        case .expectBefore, .expectNoMatch, .schedule, .cancelSchedule, .startWorkflow, .cancelWorkflow:
            continue // control-flow: touches no documents
        default:
            total += 1
        }
    }
    return total
}

// MARK: - Stored rows

/// A stored row: the user doc plus its identity/history, kept separate so the
/// system fields (`_id`/`_creationTime`/`_version`) are merged in only at read
/// time — exactly as the server stores `doc` jsonb alongside `id`/`created_at`
/// /`version` columns (store.ts `StoredRow`). `deletedAt` is the FM-33
/// soft-delete stamp: present only on a softDelete table's deleted rows,
/// invisible to every read terminal and eq-lookup. A class because the TS row
/// is a shared mutable reference (updates stamp in place); not Sendable.
public final class StoredRow {
    /// Server-shaped opaque row id.
    public let id: String
    /// The document body without system fields.
    public var doc: [String: JSONValue]
    /// Creation timestamp, epoch milliseconds.
    public let createdAt: Int64
    /// Optimistic-concurrency version, bumped on every write.
    public var version: Int64
    /// FM-33 soft-delete stamp; nil = live.
    public var deletedAt: Int64?

    init(id: String, doc: [String: JSONValue], createdAt: Int64, version: Int64, deletedAt: Int64? = nil) {
        self.id = id
        self.doc = doc
        self.createdAt = createdAt
        self.version = version
        self.deletedAt = deletedAt
    }

    /// Deep copy for txn snapshots (JSONValue is a value type; a fresh row
    /// object detaches the snapshot from live mutation).
    func snapshotCopy() -> StoredRow {
        StoredRow(id: id, doc: doc, createdAt: createdAt, version: version, deletedAt: deletedAt)
    }
}

/// Per-table row storage. Swift dictionaries are copy-on-write, so nesting
/// `[String: StoredRow]` inside the client's tables map would hand readers a
/// snapshot that silently diverges from live writes; this box keeps the TS's
/// shared-live-map semantics. Not Sendable.
final class RowStore {
    var rows: [String: StoredRow] = [:]
    /// Next value of the table's `autoIncrementField` counter — the mirror of
    /// the server's per-table Postgres sequence (which starts at 1). Lives on
    /// the store so it follows table lifecycle: a migrate rename moves it, a
    /// drop discards it, and a recreated table starts fresh.
    var autoIncrementNext: Int64 = 1
}

// MARK: - Options

/// Options for `InMemoryRtDbClient` (store.ts `InMemoryRtDbClientOptions`).
/// `now`/`random` are the corpus-determinism levers — inject both to pin id
/// minting and `_creationTime`.
public struct InMemoryRtDbClientOptions {
    /// Injectable clock (epoch ms) for deterministic `_creationTime` and id
    /// minting. Defaults to the system clock.
    public var now: (@Sendable () -> Int64)?
    /// Injectable RNG in [0, 1) for deterministic id minting. Defaults to
    /// `Double.random(in: 0..<1)`.
    public var random: (@Sendable () -> Double)?
    /// Stable identity for this client in presence rooms. Auto-generated as
    /// `c1` when omitted (the TS's per-instance counter quirk — distinct in
    /// shape from document ids).
    public var connectionId: String?
    /// Display identity stamped on this client's presence entries. Defaults
    /// to a bare `{ kind: "user" }`.
    public var presenceUser: AuthedUser?
    /// Optional shared presence backing. Two clients that pass the same
    /// `PresenceRooms` instance see each other; without one a client gets a
    /// private instance and sees only itself.
    public var presenceRooms: PresenceRooms?

    public init(
        now: (@Sendable () -> Int64)? = nil,
        random: (@Sendable () -> Double)? = nil,
        connectionId: String? = nil,
        presenceUser: AuthedUser? = nil,
        presenceRooms: PresenceRooms? = nil
    ) {
        self.now = now
        self.random = random
        self.connectionId = connectionId
        self.presenceUser = presenceUser
        self.presenceRooms = presenceRooms
    }
}

// MARK: - Cascade context

/// Per-initiating-delete cascade context (FM-33; store.ts `CascadeCtx`),
/// mirroring the `visited` set and `rows` counter server
/// `txn::delete_row_cascade` threads through one step. A class so recursion
/// shares the budget (Swift has no inout forwarding through methods).
final class CascadeContext {
    var visited: Set<String> = []
    var rows = 0
    var touched: Set<String> = []
}
