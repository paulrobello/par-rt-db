import CryptoKit
import Foundation

// MARK: - Limits

// Mirrors ts-client/src/in_memory/store.ts — the in-memory engine's client
// core (mirrors rust-client/src/in_memory/mod.rs): stored rows, the
// transaction executor, schedules/workflows/storage/presence, and the admin
// surface. The query engine lives in `InMemoryQuery.swift`, the migration
// engine in `InMemoryMigrate.swift`, and value/filter validation in
// `InMemoryValidate.swift`.
//
// The server (`server/src/{txn,query,schema,protocol}.rs`) is the source of
// truth for the declarative DSL, step-result shapes, system fields, and
// query semantics; this engine mirrors them so tests can exercise
// query/txn/schema behavior with no network and no live Postgres.
//
// Concurrency: the engine is synchronous like the TS original and
// deliberately NOT Sendable — it is test infrastructure driven from one
// context. Time, RNG, and random-id minting flow through the injected
// `now`/`random` closures so corpus runs stay deterministic.

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
    /// FM-29: hard cap on steps per workflow spec (server
    /// `workflows::MAX_WORKFLOW_STEPS`).
    static let maxWorkflowSteps = 64
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
        case .schedule, .cancelSchedule, .startWorkflow, .cancelWorkflow:
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
}

// MARK: - Presence

/// Shared in-memory presence backing (store.ts `PresenceRooms`): a
/// `room -> connectionId -> member` map with a per-room subscriber set. Two
/// engines that share an instance see each other's joins/updates/leaves fan
/// out — one client, one connection, like the live server's per-ConnId
/// keying. Snapshot order is join order (a JS Map iterates insertion order;
/// the order array preserves it). Not Sendable: same-context test infra.
public final class PresenceRooms {
    private var members: [String: [String: PresenceMember]] = [:]
    private var memberOrder: [String: [String]] = [:]
    private var subs: [String: [Int: ([PresenceMember]) -> Void]] = [:]
    private var expiry: [String: [String: Int64]] = [:]
    private var nextSubToken = 0

    /// Returns a stable-order snapshot of `room`'s current members.
    public func snapshot(_ room: String) -> [PresenceMember] {
        (memberOrder[room] ?? []).compactMap { members[room]?[$0] }
    }

    /// Adds or replaces `member` in `room` and fans out a fresh snapshot.
    public func join(_ room: String, _ member: PresenceMember) {
        if members[room] == nil {
            members[room] = [:]
            memberOrder[room] = []
        }
        if members[room]?[member.connectionId] == nil {
            memberOrder[room]?.append(member.connectionId)
        }
        members[room]?[member.connectionId] = member
        fanOut(room)
    }

    /// Updates the member's state in `room` and fans out. No-op when the
    /// connection is not in the room. When `ttlMs` > 0, schedules an expiry
    /// sweep that nulls this member's `state` at `now + ttlMs` (the member
    /// stays listed); a refresh without one clears any pending expiry
    /// (store.ts `update`).
    public func update(
        _ room: String, _ connectionId: String, state: JSONValue, ttlMs: Int64?, now: Int64
    ) {
        guard var member = members[room]?[connectionId] else { return }
        member.state = state
        members[room]?[connectionId] = member
        if let ttlMs, ttlMs > 0 {
            expiry[room, default: [:]][connectionId] = now + ttlMs
        } else {
            expiry[room]?.removeValue(forKey: connectionId)
        }
        fanOut(room)
    }

    /// Removes the connection from `room` and fans out. No-op if absent.
    /// Clears any pending expiry so a re-join doesn't inherit a stale ttl.
    public func leave(_ room: String, _ connectionId: String) {
        guard members[room] != nil else { return }
        members[room]?.removeValue(forKey: connectionId)
        memberOrder[room]?.removeAll { $0 == connectionId }
        expiry[room]?.removeValue(forKey: connectionId)
        if members[room]?.isEmpty == true {
            members.removeValue(forKey: room)
            memberOrder.removeValue(forKey: room)
            expiry.removeValue(forKey: room)
        }
        fanOut(room)
    }

    /// Clears expired members' `state` to null (the member stays listed) and
    /// fans out each touched room once. Returns true if anything expired.
    @discardableResult
    public func expire(now: Int64) -> Bool {
        var any = false
        var touched: [String] = []
        for (room, roomExpiry) in expiry {
            guard let roomMap = members[room] else {
                expiry.removeValue(forKey: room)
                continue
            }
            var roomTouched = false
            for (connectionId, at) in roomExpiry where at <= now {
                if var member = roomMap[connectionId] {
                    member.state = .null
                    members[room]?[connectionId] = member
                    any = true
                    roomTouched = true
                }
                expiry[room]?.removeValue(forKey: connectionId)
            }
            if roomTouched {
                touched.append(room)
            }
        }
        for room in touched {
            fanOut(room)
        }
        return any
    }

    /// Registers `fn` for `room` snapshots and immediately fires it with the
    /// current snapshot (the server's first `presenceSnapshot` on join).
    /// Returns an unsubscribe.
    public func subscribe(
        _ room: String, _ fn: @escaping ([PresenceMember]) -> Void
    ) -> () -> Void {
        let token = nextSubToken
        nextSubToken += 1
        subs[room, default: [:]][token] = fn
        fn(snapshot(room))
        return { [weak self] in
            self?.subs[room]?.removeValue(forKey: token)
            if self?.subs[room]?.isEmpty == true {
                self?.subs.removeValue(forKey: room)
            }
        }
    }

    private func fanOut(_ room: String) {
        guard let listeners = subs[room] else { return }
        let snap = snapshot(room)
        for fn in listeners.values {
            fn(snap)
        }
    }
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

// MARK: - Schedules & workflows

/// A stored scheduled job (store.ts `ScheduledJob`): `tick` fires due
/// non-paused jobs by applying `txn` through the same atomic path as
/// `mutate`. A class so tick/pause/resume stamp in place.
final class ScheduledJob {
    let id: String
    let kind: ScheduleKind
    let txn: Transaction
    var dueAt: Int64
    var cron: String?
    var status: ScheduleStatus
    let createdAt: Int64
    var firedCount: Int64
    var lastError: String?

    init(
        id: String, kind: ScheduleKind, txn: Transaction, dueAt: Int64, cron: String?,
        status: ScheduleStatus, createdAt: Int64, firedCount: Int64, lastError: String? = nil
    ) {
        self.id = id
        self.kind = kind
        self.txn = txn
        self.dueAt = dueAt
        self.cron = cron
        self.status = status
        self.createdAt = createdAt
        self.firedCount = firedCount
        self.lastError = lastError
    }
}

/// FM-29 retry policy applied when a step spec omits `retry` (server
/// `protocol::StepRetry::default` — the Swift wire type already carries the
/// same defaults).
private let defaultStepRetry = StepRetry(maxAttempts: 3)

/// FM-29: exponential backoff after the `attempts`-th failure of a step —
/// `initialRetryMs * 2^(attempts-1)` (shift capped at 32), clamped to
/// `maxRetryMs` (store.ts `backoffMs`).
private func backoffMs(_ retry: StepRetry, _ attempts: Int) -> Int64 {
    let shift = UInt64(min(attempts - 1, 32))
    let doubled = retry.initialRetryMs << shift
    let capped = min(doubled, retry.maxRetryMs)
    return Int64(clamping: capped)
}

/// FM-29: submit-time spec validation — a port of server
/// `workflows::validate_spec` (store.ts `validateWorkflowSpec`), including
/// the recursive `maxSteps` gate over the spec's step txns.
private func validateWorkflowSpec(_ spec: WorkflowSpec) throws {
    if spec.steps.isEmpty {
        throw RtDbError(code: .badRequest, message: "workflow must have at least one step")
    }
    if spec.steps.count > InMemoryLimits.maxWorkflowSteps {
        throw RtDbError(
            code: .badRequest,
            message: "workflow exceeds \(InMemoryLimits.maxWorkflowSteps) steps"
        )
    }
    for (index, step) in spec.steps.enumerated() {
        guard let retry = step.retry else { continue }
        if retry.maxAttempts == 0 {
            throw RtDbError(
                code: .badRequest, message: "steps[\(index)].retry.maxAttempts must be >= 1"
            )
        }
        if retry.initialRetryMs == 0 || retry.maxRetryMs < retry.initialRetryMs {
            throw RtDbError(
                code: .badRequest,
                message: "steps[\(index)].retry requires initialRetryMs > 0 and "
                    + "maxRetryMs >= initialRetryMs"
            )
        }
    }
    let total = spec.steps.reduce(0) { $0 + countSteps($1.txn) }
    if total > InMemoryLimits.maxSteps {
        throw RtDbError(
            code: .badRequest,
            message: "workflow recursive step count \(total) exceeds MAX_STEPS "
                + "\(InMemoryLimits.maxSteps)"
        )
    }
}

/// FM-29: a stored workflow run (store.ts `WorkflowRun`); field semantics
/// mirror the server's `workflows` table. `sleepUntil` is always set. A class
/// so `advanceWorkflow` can identity-check the live entry between steps.
final class WorkflowRun {
    let id: String
    let spec: WorkflowSpec
    var status: WorkflowStatus
    var currentStep: Int
    var attempts: Int
    var sleepUntil: Int64
    var lastError: String?
    let createdAt: Int64
    var updatedAt: Int64
    var startedAt: Int64?
    var finishedAt: Int64?
    var stepOutcomes: [StepOutcome]
    /// Insertion sequence — `listWorkflows` ties break on it (the TS sort is
    /// stable over insertion order; Swift's is not).
    let seq: Int

    init(
        id: String, spec: WorkflowSpec, status: WorkflowStatus, currentStep: Int, attempts: Int,
        sleepUntil: Int64, createdAt: Int64, updatedAt: Int64, stepOutcomes: [StepOutcome], seq: Int
    ) {
        self.id = id
        self.spec = spec
        self.status = status
        self.currentStep = currentStep
        self.attempts = attempts
        self.sleepUntil = sleepUntil
        self.createdAt = createdAt
        self.updatedAt = updatedAt
        self.stepOutcomes = stepOutcomes
        self.seq = seq
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

// MARK: - Doc validation

// swiftlint:disable cyclomatic_complexity
/// Recursive value validator — a port of server `schema::validate_value`
/// (store.ts `validateValue`).
func validateValue(_ ty: FieldType, _ value: JSONValue) -> Bool {
    switch (ty, value) {
    case (.string, .string): return true
    case (.number, .int), (.number, .double): return true
    case (.boolean, .bool): return true
    case (.null, .null): return true
    case (.id, _): return isHexId(value)
    case let (.literal(accepted), actual): return jsonEq(accepted, actual)
    // Server: `Optional { inner } => value.is_null() || validate_value(inner,
    // value)` — null is accepted for ANY optional; stripUnsetOptionals then
    // drops the key when the inner type does not itself accept null.
    case (.optional, .null): return true
    case let (.optional(inner), actual): return validateValue(inner, actual)
    case let (.union(variants), _):
        return variants.contains { validateValue($0, value) }
    case let (.array(element), .array(items)):
        return items.allSatisfy { validateValue(element, $0) }
    case let (.object(fields), .object(doc)):
        // Unknown keys reject; absent keys must be optional.
        for key in doc.keys where fields[key] == nil {
            return false
        }
        for (field, fieldTy) in fields {
            if let actual = doc[field] {
                if !validateValue(fieldTy, actual) {
                    return false
                }
            } else if case .optional = fieldTy {
                // absent optional is fine
            } else {
                return false
            }
        }
        return true
    case (.int64, _): return isInt64String(value)
    case (.bytes, _): return isBase64String(value)
    case (.any, _): return true
    case let (.record(valueTy), .object(map)):
        return map.values.allSatisfy { validateValue(valueTy, $0) }
    case let (.vector(dimensions), .array(items)):
        guard items.count == Int(dimensions) else { return false }
        return items.allSatisfy { item in
            if case .int = item {
                return true
            }
            if case let .double(double) = item {
                return double.isFinite
            }
            return false
        }
    default: return false
    }
}

// swiftlint:enable cyclomatic_complexity

/// Full-document validation — a port of server `schema::validate_doc`
/// (store.ts `validateDoc`).
func validateDoc(_ table: TableDef, _ doc: [String: JSONValue]) throws {
    for key in doc.keys {
        if key.hasPrefix("_") {
            throw RtDbError(code: .schemaViolation, message: "field '\(key)' is reserved")
        }
        if table.fields[key] == nil {
            throw RtDbError(code: .schemaViolation, message: "unknown field '\(key)'")
        }
    }
    for (field, fieldTy) in table.fields {
        if let value = doc[field] {
            if !validateValue(fieldTy, value) {
                throw RtDbError(
                    code: .schemaViolation, message: "field '\(field)' has an invalid value"
                )
            }
        } else if case .optional = fieldTy {
            // absent optional is fine
        } else {
            throw RtDbError(code: .schemaViolation, message: "field '\(field)' is required")
        }
    }
}

/// Removes keys whose value is null for an optional field whose inner type
/// does not itself accept null — a port of server `strip_unset_optionals`
/// (store.ts `stripUnsetOptionals`), so an inserted-or-nulled optional lands
/// as "key absent".
func stripUnsetOptionals(_ table: TableDef, _ doc: [String: JSONValue]) -> [String: JSONValue] {
    var out: [String: JSONValue] = [:]
    for (key, value) in doc {
        if case .null = value, let fieldTy = table.fields[key] {
            if case let .optional(inner) = fieldTy, !validateValue(inner, .null) {
                continue
            }
        }
        out[key] = value
    }
    return out
}

/// Stamps the TTL field at insert when the table declares a
/// `defaultDurationMs` and the document omits the field (store.ts
/// `stampTtlDefault`) — before validation, so the stamped value satisfies a
/// required numeric field.
func stampTtlDefault(
    _ table: TableDef, _ doc: [String: JSONValue], _ now: Int64
) -> [String: JSONValue] {
    guard let ttl = table.ttl, let duration = ttl.defaultDurationMs, doc[ttl.field] == nil else {
        return doc
    }
    var out = doc
    out[ttl.field] = .int(now + duration)
    return out
}

/// Applies the table's push-time-validated `defaults` (FM-32) to a NEW
/// document (store.ts `applyDefaults`): every key the doc omits is stamped.
/// Runs after `stampTtlDefault` (a ttl default on the same field wins). Only
/// the new-document paths call it — insert, replace, upsert-insert; patch
/// never re-applies.
func applyDefaults(_ table: TableDef, _ doc: [String: JSONValue]) -> [String: JSONValue] {
    guard !table.defaults.isEmpty else { return doc }
    let missing = table.defaults.keys.filter { doc[$0] == nil }
    if missing.isEmpty {
        return doc
    }
    var out = doc
    // JSONValue is a value type — the default is never aliased into a doc.
    for field in missing {
        out[field] = table.defaults[field]
    }
    return out
}

/// Applies a patch's fields onto `doc` — a port of server `txn::apply_patch`
/// (store.ts `applyPatch`). A null on an optional field whose inner type
/// rejects null removes the key.
func applyPatch(
    _ table: TableDef, _ doc: [String: JSONValue], _ fields: [String: JSONValue]
) throws -> [String: JSONValue] {
    var merged = doc
    for (field, value) in fields {
        guard let fieldTy = table.fields[field] else {
            throw RtDbError(code: .schemaViolation, message: "unknown field '\(field)'")
        }
        if case .null = value, case let .optional(inner) = fieldTy {
            if !validateValue(inner, .null) {
                merged.removeValue(forKey: field)
                continue
            }
        }
        if !validateValue(fieldTy, value) {
            throw RtDbError(
                code: .schemaViolation, message: "field '\(field)' has an invalid value"
            )
        }
        merged[field] = value
    }
    try validateDoc(table, merged)
    return merged
}

// MARK: - Storage types

/// Result of `InMemoryRtDbClient.upload` — the server-computed file identity,
/// content hash, size, and (if recorded) content type. Defined locally like
/// the rust harness's copy, so the engine pulls in no HTTP surface.
public struct UploadResult: Equatable, Sendable {
    /// Engine-assigned opaque file id.
    public var id: String
    /// SHA-256 hex digest of the stored bytes.
    public var sha256: String
    /// Size in bytes.
    public var size: Int64
    /// The upload's content type, when recorded.
    public var contentType: String?

    public init(id: String, sha256: String, size: Int64, contentType: String? = nil) {
        self.id = id
        self.sha256 = sha256
        self.size = size
        self.contentType = contentType
    }
}

/// One stored file blob (store.ts's files map entry).
private struct StoredFile {
    var bytes: Data
    var contentType: String?
    var createdAt: Int64
    var sha256: String
}

// MARK: - The client

/// One reactive subscription (store.ts `Subscription`); the callback is
/// cleared by the unsubscribe handle so notify skips dead entries.
private final class EngineSubscription {
    let query: Query
    let table: String
    var onUpdate: ((JSONValue) -> Void)?
    var last: JSONValue?
    var hasValue = false

    init(query: Query, table: String) {
        self.query = query
        self.table = table
    }
}

/// In-memory par-rt-db engine for tests (store.ts `InMemoryRtDbClient`). No
/// network, no Postgres; mirrors server DSL/step-result/system-field
/// semantics. Not Sendable — synchronous test infrastructure driven from one
/// context, exactly like the TS original.
public final class InMemoryRtDbClient: MigrationStore {
    private let nowFn: @Sendable () -> Int64
    private let randomFn: @Sendable () -> Double
    private var schema: SchemaDef?
    var tables: [String: RowStore] = [:]
    private var idempotency: [String: [JSONValue]] = [:]
    private var subs: [EngineSubscription] = []
    private var schedules: [String: ScheduledJob] = [:]
    private var scheduleOrder: [String] = []
    private var workflows: [String: WorkflowRun] = [:]
    private var workflowOrder: [String] = []
    private var workflowSeq = 0
    private var files: [String: StoredFile] = [:]
    private let presenceRooms: PresenceRooms
    private let presenceUser: AuthedUser
    private var joinedRooms: Set<String> = []
    /// Unsubscribe handles for the per-room callbacks this client registered
    /// on `PresenceRooms`, so `leavePresence(room)` drops every local
    /// subscriber for that room. Keyed by token — closures are not
    /// comparable.
    private var presenceUnsubs: [String: [Int: () -> Void]] = [:]
    private var presenceSubSeq = 0
    private var idCounter: Int64 = 0
    private var _admin: InMemoryAdminClient?

    /// This client's stable identity in presence rooms (counter-prefixed,
    /// distinct in shape from document ids).
    public let connectionId: String

    public init(options: InMemoryRtDbClientOptions = InMemoryRtDbClientOptions()) {
        nowFn = options.now ?? { @Sendable in
            Int64((Date().timeIntervalSince1970 * 1000).rounded())
        }
        randomFn = options.random ?? { @Sendable in Double.random(in: 0 ..< 1) }
        presenceRooms = options.presenceRooms ?? PresenceRooms()
        presenceUser = options.presenceUser ?? AuthedUser(kind: .user)
        if let id = options.connectionId {
            connectionId = id
        } else {
            idCounter += 1
            connectionId = "c" + String(idCounter, radix: 36)
        }
    }

    /// In-memory admin surface (store.ts `InMemoryAdminClient`): a seedable
    /// audit log and the live subscription inspector.
    public var admin: InMemoryAdminClient {
        if let cached = _admin {
            return cached
        }
        let admin = InMemoryAdminClient(
            now: { [nowFn] in nowFn() },
            subs: { [weak self] in
                guard let self else { return [] }
                return subs.map {
                    InMemoryAdminClient.SubscriptionSnapshot(
                        table: $0.table,
                        terminal: queryTerminal($0.query),
                        readSetClass: queryReadSetClass($0.query)
                    )
                }
            }
        )
        _admin = admin
        return admin
    }

    // MARK: Schema

    /// Installs `schema` as this engine's sole schema (store.ts `pushSchema`).
    /// The first push seeds an empty doc store per table; a subsequent push
    /// must be additive (removed/retyped table/field/index is BAD_REQUEST)
    /// and keeps every existing row. Every push validates TTL/index rules and
    /// `onDelete` declarations.
    public func pushSchema(_ schema: SchemaDef) throws {
        try validateSchema(schema)
        try validateOnDelete(schema)
        if let current = self.schema {
            try detectDestructiveChanges(current, schema)
            // Additive: keep existing tables' rows; only brand-new tables
            // seed empty doc stores.
            for tableName in schema.tables.keys where tables[tableName] == nil {
                tables[tableName] = RowStore()
            }
        } else {
            for tableName in schema.tables.keys {
                tables[tableName] = RowStore()
            }
        }
        self.schema = schema
    }

    /// Applies (or previews) a declarative schema migration — a port of
    /// server `migrate::plan_migration` + `apply_migration` (store.ts
    /// `migrate`). A failed directive is atomic: every earlier structural and
    /// data effect rolls back. With `dryRun`, the plan validates and reports
    /// `affectedRows` against the derived schema but commits nothing.
    public func migrate(_ request: MigrateRequest) throws -> MigrateResult {
        let old = try requireSchema()
        var planned = old // value semantics — the TS clones here
        var touched: Set<String> = []
        let tableSnap = snapshotTables()
        var reports: [DirectiveReport] = []
        do {
            for directive in request.directives {
                let (report, table) = try applyMigrationDirective(&planned, directive, self)
                reports.append(report)
                if let table {
                    touched.insert(table)
                }
            }
        } catch {
            // Atomicity: a failed directive rolls back every earlier effect.
            restoreTables(tableSnap)
            throw error
        }
        if request.dryRun {
            restoreTables(tableSnap)
            return MigrateResult(applied: false, schema: planned, directives: reports)
        }
        schema = planned
        notifySubs(touched)
        return MigrateResult(applied: true, schema: planned, directives: reports)
    }

    // MARK: Queries & transactions

    /// One-shot query — same shape as the HTTP client's `query` (store.ts
    /// `query`). The raw JSONValue result is decodable via `parseResult`.
    public func query(_ query: Query) throws -> JSONValue {
        try executeQuery(query, requireTable(query.table)) { [self] table in
            rowsFor(table)
        }
    }

    /// Executes a transaction and returns one result per step, in order
    /// (store.ts `mutate`); `idempotencyKey` replays a cached result.
    public func mutate(
        _ txn: Transaction, idempotencyKey: String? = nil
    ) throws -> [StepResult] {
        if let idempotencyKey {
            if let cached = idempotency[idempotencyKey] {
                return try cached.map(parseStepResult)
            }
        }
        let results = try executeTransaction(txn)
        if let idempotencyKey {
            idempotency[idempotencyKey] = results
        }
        return try results.map(parseStepResult)
    }

    /// Decodes one raw step result into the typed `StepResult` (the TS
    /// `parseStepResults` mirror — the wire enum's own decode does the
    /// untagged shape matching).
    private func parseStepResult(_ raw: JSONValue) throws -> StepResult {
        let data = try JSONEncoder().encode(raw)
        return try JSONDecoder().decode(StepResult.self, from: data)
    }

    // swiftlint:disable function_body_length
    /// Synchronous atomic core shared by `mutate` and `tick`'s scheduled
    /// fires (store.ts `executeTransaction`): enforces the step caps,
    /// snapshots, applies every step (rolling back the whole txn on any
    /// error), then notifies subscriptions.
    private func executeTransaction(_ txn: Transaction) throws -> [JSONValue] {
        if countSteps(txn) > InMemoryLimits.maxSteps {
            throw RtDbError(
                code: .badRequest,
                message: "transaction exceeds maximum of \(InMemoryLimits.maxSteps) steps"
            )
        }
        // SEC-104: bound the worst-case row count before any step applies.
        var byQuerySteps = 0
        for step in txn.steps {
            switch step {
            case .patchByQuery, .deleteByQuery:
                byQuerySteps += 1
            default:
                break
            }
        }
        if byQuerySteps > InMemoryLimits.maxByQueryStepsPerTxn {
            throw RtDbError(
                code: .badRequest,
                message: "transaction has \(byQuerySteps) by-query steps, exceeding the limit "
                    + "of \(InMemoryLimits.maxByQueryStepsPerTxn)"
            )
        }
        let worst = worstCaseAffected(txn)
        if worst > InMemoryLimits.maxAffectedRowsPerTxn {
            throw RtDbError(
                code: .badRequest,
                message: "transaction could affect up to \(worst) documents, exceeding the "
                    + "limit of \(InMemoryLimits.maxAffectedRowsPerTxn)"
            )
        }
        let snapshot = snapshotTables()
        // FM-28/FM-29: a schedule/workflow control step mutates its store, so
        // a failed txn must roll it back with the docs. The snapshots are
        // shallow — the TS shares the job/run objects too.
        let schedulesSnapshot = schedules
        let scheduleOrderSnapshot = scheduleOrder
        let workflowsSnapshot = workflows
        let workflowOrderSnapshot = workflowOrder
        var results: [JSONValue] = []
        var writeSet: Set<String> = []
        do {
            for step in txn.steps {
                let execution = try executeStep(step)
                results.append(execution.result)
                if let table = execution.table {
                    writeSet.insert(table)
                }
                // FM-33: an onDelete cascade writes child tables beyond the
                // step's own.
                for extra in execution.extraTables {
                    writeSet.insert(extra)
                }
            }
        } catch {
            // Atomicity: any step's error rolls back everything already
            // applied.
            restoreTables(snapshot)
            schedules = schedulesSnapshot
            scheduleOrder = scheduleOrderSnapshot
            workflows = workflowsSnapshot
            workflowOrder = workflowOrderSnapshot
            throw error
        }
        notifySubs(writeSet)
        return results
    }

    // swiftlint:enable function_body_length

    // MARK: Subscriptions

    /// Reactive subscription (store.ts `subscribe`): recomputes and fires
    /// `onUpdate` on the initial value (synchronously) and again whenever a
    /// mutation changes the result. An invalid query throws from here, like
    /// the TS. Returns an unsubscribe handle.
    @discardableResult
    public func subscribe(
        _ query: Query, onUpdate: @escaping (JSONValue) -> Void
    ) throws -> () -> Void {
        let sub = EngineSubscription(query: query, table: query.table)
        subs.append(sub)
        sub.onUpdate = onUpdate
        let initial = try executeQuery(sub.query, requireTable(sub.table)) { [self] table in
            rowsFor(table)
        }
        sub.last = initial
        sub.hasValue = true
        onUpdate(initial)
        return { [weak sub, weak self] in
            sub?.onUpdate = nil
            guard let self, let sub else { return }
            subs.removeAll { $0 === sub }
        }
    }

    /// Recomputes every subscription touching `writeSet` and fires listeners
    /// on change (store.ts `notifySubs`). JSONValue equality is key-order
    /// independent — the TS canonicalizes for the same property.
    private func notifySubs(_ writeSet: Set<String>) {
        for sub in subs {
            guard writeSet.contains(sub.table), sub.onUpdate != nil else { continue }
            guard let next = try? executeQuery(sub.query, requireTable(sub.table), rowsFor)
            else { continue }
            if sub.hasValue, next == sub.last {
                continue
            }
            sub.last = next
            sub.hasValue = true
            sub.onUpdate?(next)
        }
    }

    // MARK: Presence

    /// Joins presence room `room` with optional initial state (store.ts
    /// `presence`). When `onUpdate` is supplied it fires with the current
    /// member list on join and on every local mutation. The returned handle
    /// stops listening but does NOT leave the room — call `leavePresence`.
    @discardableResult
    public func presence(
        _ room: String,
        state: JSONValue? = nil,
        onUpdate: (([PresenceMember]) -> Void)? = nil
    ) -> () -> Void {
        joinedRooms.insert(room)
        presenceRooms.join(
            room,
            PresenceMember(connectionId: connectionId, user: presenceUser, state: state ?? .null)
        )
        var token: Int?
        if let onUpdate {
            let next = presenceSubSeq
            presenceSubSeq += 1
            token = next
            presenceUnsubs[room, default: [:]][next] = presenceRooms.subscribe(room, onUpdate)
        }
        return { [weak self] in
            guard let self, let token else { return }
            presenceUnsubs[room]?.removeValue(forKey: token)
            if presenceUnsubs[room]?.isEmpty == true {
                presenceUnsubs.removeValue(forKey: room)
            }
        }
    }

    /// Broadcasts updated state for this connection in `room` (store.ts
    /// `updatePresence`). No-op when this client has not joined the room.
    public func updatePresence(_ room: String, state: JSONValue, ttlMs: Int64? = nil) {
        guard joinedRooms.contains(room) else { return }
        presenceRooms.update(room, connectionId, state: state, ttlMs: ttlMs, now: nowFn())
    }

    /// Leaves `room`: removes this connection, drops every local subscriber
    /// for it, and fans out to the remaining subscribers (store.ts
    /// `leavePresence`).
    public func leavePresence(_ room: String) {
        guard joinedRooms.remove(room) != nil else { return }
        if let handles = presenceUnsubs.removeValue(forKey: room) {
            for off in handles.values {
                off()
            }
        }
        presenceRooms.leave(room, connectionId)
    }

    // MARK: Schedules

    /// Stores `txn` scheduled for `when` and returns its id (store.ts
    /// `schedule`, whose `{ id }` result object collapses to the id here).
    @discardableResult
    public func schedule(_ txn: Transaction, when: ScheduleWhen) throws -> String {
        try scheduleJob(txn, when)
    }

    /// Sync core of `schedule` — the `Step.schedule` transaction step reuses
    /// it from the sync executeStep path.
    private func scheduleJob(_ txn: Transaction, _ when: ScheduleWhen) throws -> String {
        let id = newId()
        let now = nowFn()
        var kind = ScheduleKind.oneshot
        var cron: String?
        if case let .cron(expr) = when {
            kind = .cron
            cron = expr
        }
        let job = ScheduledJob(
            id: id,
            kind: kind,
            txn: txn,
            dueAt: dueAtFor(when, now),
            cron: cron,
            status: .pending,
            createdAt: now,
            firedCount: 0
        )
        schedules[id] = job
        scheduleOrder.append(id)
        return id
    }

    /// Cancels a scheduled job: true when a row was removed, false for an
    /// unknown id (a no-op, not an error) — the server's `scheduler::cancel`.
    @discardableResult
    public func cancelSchedule(_ id: String) -> Bool {
        guard schedules.removeValue(forKey: id) != nil else { return false }
        scheduleOrder.removeAll { $0 == id }
        return true
    }

    /// Pauses a pending job: false when missing or not pending.
    @discardableResult
    public func pauseSchedule(_ id: String) -> Bool {
        guard let job = schedules[id], job.status == .pending else { return false }
        job.status = .paused
        return true
    }

    /// Resumes a paused job: false when missing or not paused.
    @discardableResult
    public func resumeSchedule(_ id: String) -> Bool {
        guard let job = schedules[id], job.status == .paused else { return false }
        job.status = .pending
        return true
    }

    /// Lists scheduled jobs in creation order (store.ts `listSchedules`).
    public func listSchedules() -> [ScheduleInfo] {
        scheduleOrder.compactMap { schedules[$0].map(toScheduleInfo) }
    }

    private func dueAtFor(_ when: ScheduleWhen, _ now: Int64) -> Int64 {
        switch when {
        case let .afterMs(ms): now + ms
        case let .runAt(ms): ms
        case .cron: now + InMemoryLimits.cronStepMs
        }
    }

    private func toScheduleInfo(_ job: ScheduledJob) -> ScheduleInfo {
        ScheduleInfo(
            id: job.id,
            kind: job.kind,
            dueAt: job.dueAt,
            cron: job.cron,
            status: job.status,
            lastError: job.lastError,
            createdAt: job.createdAt,
            firedCount: job.firedCount
        )
    }

    // MARK: Workflows (FM-29)

    /// Starts a durable workflow run from `spec`, validating it like the
    /// server's `workflows::validate_spec` (store.ts `startWorkflow`);
    /// `tick` advances it.
    public func startWorkflow(_ spec: WorkflowSpec) throws -> WorkflowInfo {
        try validateWorkflowSpec(spec)
        return try toWorkflowInfo(startWorkflowJob(spec))
    }

    /// Cancels a pending/running run: false (a no-op) for an unknown or
    /// terminal run — the server's `workflows::cancel` contract.
    @discardableResult
    public func cancelWorkflow(_ id: String) -> Bool {
        guard let run = workflows[id], run.status == .pending || run.status == .running else {
            return false
        }
        run.status = .cancelled
        run.finishedAt = nowFn()
        run.updatedAt = nowFn()
        return true
    }

    /// Lists runs, newest first (createdAt DESC, insertion order on ties).
    public func listWorkflows(status: WorkflowStatus? = nil) -> [WorkflowInfo] {
        workflows.values
            .filter { status == nil || $0.status == status }
            .sorted {
                if $0.createdAt != $1.createdAt {
                    return $0.createdAt > $1.createdAt
                }
                return $0.seq > $1.seq
            }
            .map(toWorkflowInfo)
    }

    /// Fetches one full run row — info plus the per-step outcome trail.
    /// NOT_FOUND on unknown id.
    public func getWorkflow(_ id: String) throws -> WorkflowInfoFull {
        guard let run = workflows[id] else {
            throw RtDbError(code: .notFound, message: "workflow '\(id)' not found")
        }
        return WorkflowInfoFull(info: toWorkflowInfo(run), stepOutcomes: run.stepOutcomes)
    }

    /// Sync core of `startWorkflow` — the `Step.startWorkflow` transaction
    /// step reuses it (the server's `workflows::insert_on`).
    private func startWorkflowJob(_ spec: WorkflowSpec) throws -> WorkflowRun {
        let now = nowFn()
        let run = WorkflowRun(
            id: newId(),
            spec: spec,
            status: .pending,
            currentStep: 0,
            attempts: 0,
            sleepUntil: now + Int64(spec.steps.first?.sleepBeforeMs ?? 0),
            createdAt: now,
            updatedAt: now,
            stepOutcomes: [],
            seq: workflowSeq
        )
        workflowSeq += 1
        workflows[run.id] = run
        workflowOrder.append(run.id)
        return run
    }

    private func toWorkflowInfo(_ run: WorkflowRun) -> WorkflowInfo {
        WorkflowInfo(
            id: run.id,
            name: run.spec.name,
            status: run.status,
            currentStep: UInt32(run.currentStep),
            stepCount: UInt32(run.spec.steps.count),
            attempts: UInt32(run.attempts),
            sleepUntil: run.sleepUntil,
            lastError: run.lastError,
            createdAt: run.createdAt,
            updatedAt: run.updatedAt,
            startedAt: run.startedAt,
            finishedAt: run.finishedAt
        )
    }

    // MARK: File storage

    /// Stores `body` and returns a server-shaped upload result (store.ts
    /// `upload`). The id is a counter-prefixed token distinct in shape from
    /// document ids; the digest is a real SHA-256 of the bytes.
    public func upload(_ body: Data, contentType: String? = nil) -> UploadResult {
        idCounter += 1
        let id = "f" + String(idCounter, radix: 36)
        let digest = SHA256.hash(data: body)
        let sha256 = digest.map { String(format: "%02x", $0) }.joined()
        files[id] = StoredFile(
            bytes: body, contentType: contentType, createdAt: nowFn(), sha256: sha256
        )
        return UploadResult(id: id, sha256: sha256, size: Int64(body.count), contentType: contentType)
    }

    /// Deletes a stored file; NOT_FOUND for an unknown id.
    public func deleteFile(_ id: String) throws {
        guard files.removeValue(forKey: id) != nil else {
            throw RtDbError(code: .notFound, message: "unknown file")
        }
    }

    /// Stored metadata for a file (store.ts `getFileMetadata`); `sha256` is
    /// blank — only the HTTP client computes it for metadata reads.
    public func getFileMetadata(_ id: String) throws -> FileMetadata {
        guard let file = files[id] else {
            throw RtDbError(code: .notFound, message: "unknown file")
        }
        return FileMetadata(
            id: id, sha256: "", size: Int64(file.bytes.count), contentType: file.contentType,
            creationTime: file.createdAt
        )
    }

    /// Synthetic handle — no real byte stream (store.ts `getUrl`).
    public func getUrl(_ id: String) -> String {
        "memory://\(id)"
    }

    // MARK: Tick

    /// Fires every due non-paused job by applying its txn through the same
    /// atomic path as `mutate`; advances due workflow runs (FM-29); reaps
    /// expired TTL documents (FM-33-aware hard delete). Pass `nowMs` to drive
    /// the clock deterministically. Returns the count of documents reaped
    /// (store.ts `tick` — whose reaper cascade can throw here too, exactly
    /// like the TS propagates it out of `tick`).
    @discardableResult
    public func tick(nowMs: Int64? = nil) throws -> Int {
        let now = nowMs ?? nowFn()
        for id in scheduleOrder {
            guard let job = schedules[id] else { continue }
            if job.status == .paused || job.dueAt > now {
                continue
            }
            do {
                _ = try executeTransaction(job.txn)
                job.firedCount += 1
                if job.kind == .oneshot {
                    schedules.removeValue(forKey: id)
                    scheduleOrder.removeAll { $0 == id }
                } else {
                    job.dueAt = now + InMemoryLimits.cronStepMs
                    job.status = .pending
                }
            } catch {
                job.status = .error
                job.lastError = errorMessage(error)
                if job.kind == .cron {
                    job.dueAt = now + InMemoryLimits.cronStepMs
                }
            }
        }
        // FM-29: claim due pending runs (server `claim_due`), then advance.
        let due = workflowOrder.compactMap { workflows[$0] }.filter {
            $0.status == .pending && $0.sleepUntil <= now
        }
        for run in due {
            run.status = .running
            if run.startedAt == nil {
                run.startedAt = now
            }
            run.updatedAt = now
            advanceWorkflow(run, now: now)
        }
        return try reapTtl(now)
    }

    // swiftlint:disable function_body_length
    /// FM-29: drives one claimed run across step boundaries (store.ts
    /// `advanceWorkflow`). Success on the last step finalizes; success earlier
    /// moves to the next step and applies its `sleepBeforeMs` gate (a future
    /// gate re-pends the run; a `now` gate continues in the same tick);
    /// failure re-pends with exponential backoff or, once attempts are
    /// exhausted, marks the run failed with a terminal outcome.
    private func advanceWorkflow(_ run: WorkflowRun, now: Int64) {
        while true {
            // Per-boundary liveness check: a cancel (or terminal transition)
            // between steps ends the pass — the server re-checks the row each
            // boundary.
            guard workflows[run.id] === run, run.status == .running else { return }
            guard run.currentStep < run.spec.steps.count else { return }
            let step = run.spec.steps[run.currentStep]
            var execError: String?
            do {
                _ = try executeTransaction(step.txn)
            } catch {
                execError = errorMessage(error)
            }
            if execError == nil {
                let outcome = StepOutcome(
                    stepIndex: UInt32(run.currentStep),
                    status: .success,
                    attempts: UInt32(run.attempts + 1),
                    at: now
                )
                let isLast = run.currentStep + 1 >= run.spec.steps.count
                run.stepOutcomes.append(outcome)
                run.updatedAt = now
                if isLast {
                    run.status = .success
                    run.attempts = 0
                    run.lastError = nil
                    run.finishedAt = now
                    return
                }
                run.currentStep += 1
                run.attempts = 0
                let next = run.spec.steps[run.currentStep]
                let gate = now + Int64(next.sleepBeforeMs ?? 0)
                if gate > now {
                    run.status = .pending
                    run.sleepUntil = gate
                    run.updatedAt = now
                    return
                }
                continue
            }
            let retry = step.retry ?? defaultStepRetry
            run.attempts += 1
            if run.attempts < Int(retry.maxAttempts) {
                run.status = .pending
                run.sleepUntil = now + backoffMs(retry, run.attempts)
                run.updatedAt = now
                return
            }
            run.stepOutcomes.append(
                StepOutcome(
                    stepIndex: UInt32(run.currentStep),
                    status: .failed,
                    attempts: UInt32(run.attempts),
                    at: now,
                    error: execError
                )
            )
            run.status = .failed
            run.lastError = execError
            run.finishedAt = now
            run.updatedAt = now
            return
        }
    }

    // swiftlint:enable function_body_length

    /// Removes documents whose TTL field value is a number strictly less than
    /// `now` (store.ts `reapTtl`). The reaper always HARD-deletes — even rows
    /// on a softDelete table — expanding onDelete cascades with one shared
    /// visited set and budget across the sweep. Returns the count removed.
    private func reapTtl(_ now: Int64) throws -> Int {
        guard let schema else { return 0 }
        var removed = 0
        let context = CascadeContext()
        // Key snapshot: deleteRowCascade's lazy row-store creation may insert
        // into `tables` mid-sweep.
        for tableName in Array(tables.keys) {
            guard let ttl = schema.tables[tableName]?.ttl else { continue }
            let store = rowStore(tableName)
            for row in Array(store.rows.values) {
                guard let value = row.doc[ttl.field] else { continue }
                var expired = false
                if case let .int(int) = value, int < now {
                    expired = true
                }
                if case let .double(double) = value, double < Double(now) {
                    expired = true
                }
                guard expired else { continue }
                // An earlier expiry's cascade may already have removed this
                // row (live-map check, not the iteration snapshot).
                guard store.rows[row.id] != nil else { continue }
                try deleteRowCascade(tableName, row.id, context, forceHard: true)
                removed += 1
            }
        }
        if !context.touched.isEmpty {
            notifySubs(context.touched)
        }
        return removed
    }

    // MARK: Transaction steps

    /// One step's execution outcome: the raw result value, the primary table
    /// it wrote, and any extra tables an onDelete cascade touched.
    struct StepExecution {
        var result: JSONValue
        var table: String?
        var extraTables: [String]
    }

    // swiftlint:disable cyclomatic_complexity function_body_length
    /// Applies one step and returns its raw result, primary table, and any
    /// cascade-touched extra tables (store.ts `executeStep`).
    private func executeStep(_ step: Step) throws -> StepExecution {
        // The schedule/workflow control-flow steps target their own stores,
        // not a table; cancel mirrors the standalone ops (cancelled: false is
        // not an error).
        if case let .schedule(when, txn) = step {
            let id = try scheduleJob(txn, when)
            return StepExecution(
                result: .object(["scheduleId": .string(id)]), table: nil, extraTables: []
            )
        }
        if case let .cancelSchedule(id) = step {
            let cancelled = cancelSchedule(id)
            return StepExecution(
                result: .object(["cancelled": .bool(cancelled)]), table: nil, extraTables: []
            )
        }
        if case let .startWorkflow(spec) = step {
            try validateWorkflowSpec(spec)
            let run = try startWorkflowJob(spec)
            return StepExecution(
                result: .object(["workflowId": .string(run.id)]), table: nil, extraTables: []
            )
        }
        if case let .cancelWorkflow(id) = step {
            let cancelled = cancelWorkflow(id)
            return StepExecution(
                result: .object(["cancelled": .bool(cancelled)]), table: nil, extraTables: []
            )
        }
        switch step {
        case let .insert(table, doc):
            let tableDef = try requireTable(table)
            let id = try doInsert(table, tableDef, doc)
            return StepExecution(result: .object(["id": .string(id)]), table: table, extraTables: [])
        case let .patch(table, id, fields):
            let tableDef = try requireTable(table)
            try doPatch(tableDef, table, id, fields)
            return StepExecution(result: .null, table: table, extraTables: [])
        case let .replace(table, id, doc):
            let tableDef = try requireTable(table)
            try doReplace(tableDef, table, id, doc)
            return StepExecution(result: .null, table: table, extraTables: [])
        case let .delete(table, id):
            let tableDef = try requireTable(table)
            let extraTables = try doDelete(tableDef, table, id)
            return StepExecution(result: .null, table: table, extraTables: extraTables)
        case let .undelete(table, id):
            let tableDef = try requireTable(table)
            try doUndelete(tableDef, table, id)
            return StepExecution(result: .null, table: table, extraTables: [])
        case let .expectVersion(table, id, version):
            _ = try requireTable(table)
            try doExpectVersion(table, id, version)
            return StepExecution(result: .null, table: nil, extraTables: [])
        case let .expectAbsent(table, index, eq):
            let tableDef = try requireTable(table)
            let rows = try eqLookup(tableDef, table, index, eq)
            if !rows.isEmpty {
                throw RtDbError(
                    code: .preconditionFailed,
                    message: "index '\(index)' already has a matching document"
                )
            }
            return StepExecution(result: .null, table: nil, extraTables: [])
        case let .upsert(table, index, eq, insert, patch):
            let tableDef = try requireTable(table)
            let rows = try eqLookup(tableDef, table, index, eq)
            if rows.count > 1 {
                throw RtDbError(
                    code: .preconditionFailed, message: "upsert matched multiple documents"
                )
            }
            guard let row = rows.first else {
                let id = try doInsert(table, tableDef, insert)
                return StepExecution(
                    result: .object(["id": .string(id), "inserted": .bool(true)]),
                    table: table, extraTables: []
                )
            }
            let merged = try applyPatch(tableDef, row.doc, patch)
            try doUpdate(table, tableDef, row, merged)
            return StepExecution(
                result: .object(["id": .string(row.id), "inserted": .bool(false)]),
                table: table, extraTables: []
            )
        case let .patchByQuery(table, filter, patch, limit):
            let tableDef = try requireTable(table)
            let (rows, truncated) = try scanByQuery(tableDef, table, filter, limit)
            for row in rows {
                let merged = try applyPatch(tableDef, row.doc, patch)
                try doUpdate(table, tableDef, row, merged)
            }
            return StepExecution(
                result: .object(["patched": .int(Int64(rows.count)), "truncated": .bool(truncated)]),
                table: table, extraTables: []
            )
        case let .deleteByQuery(table, filter, limit):
            let tableDef = try requireTable(table)
            let (rows, truncated) = try scanByQuery(tableDef, table, filter, limit)
            // FM-33: every matched row deletes through the same onDelete-aware
            // path as a per-id delete, with ONE shared visited set and budget.
            if tableDef.softDelete {
                let now = nowFn()
                for row in rows {
                    row.deletedAt = now
                    row.version += 1
                }
                return StepExecution(
                    result: .object(["deleted": .int(Int64(rows.count)), "truncated": .bool(truncated)]),
                    table: table, extraTables: []
                )
            }
            let context = CascadeContext()
            for row in rows {
                try deleteRowCascade(table, row.id, context, forceHard: false)
            }
            return StepExecution(
                result: .object(["deleted": .int(Int64(rows.count)), "truncated": .bool(truncated)]),
                table: table, extraTables: Array(context.touched)
            )
        case .schedule, .cancelSchedule, .startWorkflow, .cancelWorkflow:
            throw RtDbError(code: .internal, message: "control-flow steps handled above")
        }
    }

    // swiftlint:enable cyclomatic_complexity function_body_length

    private func doInsert(
        _ tableName: String, _ tableDef: TableDef, _ doc: [String: JSONValue]
    ) throws -> String {
        var stamped = stampTtlDefault(tableDef, doc, nowFn())
        stamped = applyDefaults(tableDef, stamped)
        try validateDoc(tableDef, stamped)
        let stored = stripUnsetOptionals(tableDef, stamped)
        try checkUniqueIndexes(tableName, tableDef, stored)
        let id = newId()
        rowStore(tableName).rows[id] = StoredRow(
            id: id, doc: stored, createdAt: nowFn(), version: 1
        )
        return id
    }

    private func doPatch(
        _ tableDef: TableDef, _ tableName: String, _ id: String, _ fields: [String: JSONValue]
    ) throws {
        let row = try requireRow(tableName, id)
        let merged = try applyPatch(tableDef, row.doc, fields)
        try doUpdate(tableName, tableDef, row, merged)
    }

    private func doReplace(
        _ tableDef: TableDef, _ tableName: String, _ id: String, _ doc: [String: JSONValue]
    ) throws {
        let row = try requireRow(tableName, id)
        // Defaults re-apply on a full replace; the TTL default does NOT
        // (only insert stamps it — store.ts `doReplace`).
        let stamped = applyDefaults(tableDef, doc)
        try validateDoc(tableDef, stamped)
        let stored = stripUnsetOptionals(tableDef, stamped)
        try checkUniqueIndexes(tableName, tableDef, stored, excludeId: row.id)
        row.doc = stored
        row.version += 1
    }

    /// Per-id delete — FM-33-aware (store.ts `doDelete`). On a softDelete
    /// table this stamps `deletedAt` (+version bump) and never cascades;
    /// otherwise the row deletes through `deleteRowCascade`.
    private func doDelete(_ tableDef: TableDef, _ tableName: String, _ id: String) throws -> [String] {
        if tableDef.softDelete {
            let row = rowStore(tableName).rows[id]
            guard let row, row.deletedAt == nil else {
                throw RtDbError(code: .notFound, message: "document '\(id)' not found")
            }
            row.deletedAt = nowFn()
            row.version += 1
            return []
        }
        let context = CascadeContext()
        try deleteRowCascade(tableName, id, context, forceHard: false)
        return Array(context.touched)
    }

    /// Restores a soft-deleted row (store.ts `doUndelete`) — port of server
    /// `txn::step_undelete`. BadRequest on a table without softDelete;
    /// NotFound on an absent id; idempotent on a row already live. Restoring
    /// must not violate a unique index another live row now holds — checked
    /// BEFORE the stamp clears, surfacing as Conflict.
    private func doUndelete(_ tableDef: TableDef, _ tableName: String, _ id: String) throws {
        guard tableDef.softDelete else {
            throw RtDbError(
                code: .badRequest, message: "table '\(tableName)' does not declare softDelete"
            )
        }
        guard let row = rowStore(tableName).rows[id] else {
            throw RtDbError(code: .notFound, message: "document '\(id)' not found")
        }
        guard row.deletedAt != nil else { return }
        try checkUniqueIndexes(tableName, tableDef, row.doc, excludeId: row.id)
        row.deletedAt = nil
        row.version += 1
    }

    // swiftlint:disable cyclomatic_complexity function_body_length
    /// Hard delete with `onDelete` expansion — a port of server
    /// `txn::delete_row_cascade` (store.ts `deleteRowCascade`).
    /// Children-first-parent-last: `restrict` throws a Conflict naming
    /// `table.field` while a LIVE child references the row, `cascade`
    /// recurses (stamping when the CHILD table is softDelete), and `setNull`
    /// removes the child's field key. A softDelete PARENT row stamps instead
    /// of deleting unless `forceHard` (the TTL reaper always hard-deletes).
    private func deleteRowCascade(
        _ tableName: String, _ id: String, _ context: CascadeContext, forceHard: Bool
    ) throws {
        let key = "\(tableName) \(id)"
        if context.visited.contains(key) {
            return
        }
        context.visited.insert(key)
        if context.rows >= InMemoryLimits.maxCascadeRows {
            throw RtDbError(
                code: .conflict,
                message: "onDelete cascade exceeds the limit of \(InMemoryLimits.maxCascadeRows) rows"
            )
        }
        context.rows += 1
        context.touched.insert(tableName)

        let schema = try requireSchema()
        let tableDef = try requireTable(tableName)
        let row = rowStore(tableName).rows[id]
        guard let row, !(tableDef.softDelete && row.deletedAt != nil) else {
            throw RtDbError(code: .notFound, message: "document '\(id)' not found")
        }

        // A softDelete parent stamps and stops — a stamped row is never a
        // cascade trigger, so its own children are untouched.
        if tableDef.softDelete, !forceHard {
            row.deletedAt = nowFn()
            row.version += 1
            return
        }

        for childTableName in schema.tables.keys {
            let childTableDef = schema.tables[childTableName]!
            for (fieldName, fieldTy) in childTableDef.fields {
                guard let action = onDeleteRef(fieldTy, tableName) else { continue }
                let childIds = visibleChildIds(childTableName, fieldName, id)
                if action == .restrict {
                    if let first = childIds.first {
                        throw RtDbError(
                            code: .conflict,
                            message: "cannot delete '\(tableName)': '\(childTableName)."
                                + "\(fieldName)' is referenced by document '\(first)'"
                        )
                    }
                } else if action == .cascade {
                    for childId in childIds {
                        try deleteRowCascade(childTableName, childId, context, forceHard: forceHard)
                    }
                } else {
                    // setNull: remove the child's field key (a null-on-optional
                    // patch) and bump its version — one budget slot per child.
                    for childId in childIds {
                        if context.rows >= InMemoryLimits.maxCascadeRows {
                            throw RtDbError(
                                code: .conflict,
                                message: "onDelete cascade exceeds the limit of "
                                    + "\(InMemoryLimits.maxCascadeRows) rows"
                            )
                        }
                        context.rows += 1
                        guard let childRow = rowStore(childTableName).rows[childId] else {
                            continue // visibleChildIds returns only live rows
                        }
                        let merged = try applyPatch(childTableDef, childRow.doc, [fieldName: .null])
                        try doUpdate(childTableName, childTableDef, childRow, merged)
                        context.touched.insert(childTableName)
                    }
                }
            }
        }

        // Parent last.
        rowStore(tableName).rows.removeValue(forKey: id)
    }

    // swiftlint:enable cyclomatic_complexity function_body_length

    /// LIVE child rows whose `fieldName` references `parentId` (store.ts
    /// `visibleChildIds`): a soft-deleted child is invisible to every action.
    private func visibleChildIds(_ childTableName: String, _ fieldName: String, _ parentId: String) -> [String] {
        var ids: [String] = []
        for row in rowStore(childTableName).rows.values {
            if row.deletedAt != nil {
                continue
            }
            if case let .string(value)? = row.doc[fieldName], value == parentId {
                ids.append(row.id)
            }
        }
        return ids
    }

    private func doExpectVersion(_ tableName: String, _ id: String, _ expected: Int64) throws {
        let row = try requireRow(tableName, id)
        if row.version != expected {
            throw RtDbError(
                code: .preconditionFailed,
                message: "version mismatch: expected \(expected), actual \(row.version)"
            )
        }
    }

    /// Scans `table` for rows matching `filter`, ordered by `createdAt` then
    /// `id` (server `ORDER BY "created_at", "id"`), and applies the by-query
    /// `limit` (store.ts `scanByQuery`). Returns the selected rows and
    /// whether the match set exceeded the limit.
    private func scanByQuery(
        _ tableDef: TableDef, _ tableName: String, _ filter: FilterExpr, _ limitOpt: UInt32?
    ) throws -> (rows: [StoredRow], truncated: Bool) {
        try validateFilter(filter, tableDef)
        let limit = min(limitOpt.map(Int.init) ?? InMemoryLimits.maxByQueryRows,
                        InMemoryLimits.maxByQueryRows)
        var matched: [StoredRow] = []
        for row in rowStore(tableName).rows.values {
            if row.deletedAt != nil {
                continue
            } // FM-33: stamped rows are absent
            if evalFilterExpr(filter, row.doc, tableDef.fields) {
                matched.append(row)
            }
        }
        matched.sort { left, right in
            if left.createdAt != right.createdAt {
                return left.createdAt < right.createdAt
            }
            return left.id < right.id
        }
        let truncated = matched.count > limit
        return (truncated ? Array(matched.prefix(limit)) : matched, truncated)
    }

    /// Shared by `patch`, `replace`, and `upsert`'s patch path: writes the
    /// merged doc and bumps `version` (server `apply_update`).
    private func doUpdate(
        _ tableName: String, _ tableDef: TableDef, _ row: StoredRow, _ merged: [String: JSONValue]
    ) throws {
        try checkUniqueIndexes(tableName, tableDef, merged, excludeId: row.id)
        row.doc = merged
        row.version += 1
    }

    // swiftlint:disable cyclomatic_complexity
    /// Enforce `unique` indexes on a candidate write (store.ts
    /// `checkUniqueIndexes`, mirroring `CREATE UNIQUE INDEX`): for each unique
    /// index, no OTHER live row that satisfies the index's `where` predicate
    /// may share the candidate's key values. NULL/absent key fields disable
    /// the constraint for that row (Postgres UNIQUE treats NULLs as
    /// distinct). Throws CONFLICT on collision. Uniqueness is on `fields`
    /// only — never id or created_at.
    private func checkUniqueIndexes(
        _ tableName: String,
        _ tableDef: TableDef,
        _ candidateDoc: [String: JSONValue],
        excludeId: String? = nil
    ) throws {
        guard let indexes = tableDef.indexes, !indexes.isEmpty else { return }
        for index in indexes where index.unique {
            let predicate = index.whereClause
            // A partial unique index constrains only rows matching it.
            if let predicate, !evalFilterExpr(predicate, candidateDoc, tableDef.fields) {
                continue
            }
            let candidateKey = index.fields.map { candidateDoc[$0] ?? .null }
            // NULLs are distinct under Postgres UNIQUE.
            if candidateKey.contains(.null) {
                continue
            }
            for row in rowStore(tableName).rows.values {
                if row.deletedAt != nil {
                    continue
                } // stamped rows are outside unique indexes
                if let excludeId, row.id == excludeId {
                    continue
                }
                if let predicate, !evalFilterExpr(predicate, row.doc, tableDef.fields) {
                    continue
                }
                var collision = true
                for (position, field) in index.fields.enumerated() {
                    guard let rowValue = row.doc[field], rowValue != .null,
                          jsonEq(rowValue, candidateKey[position])
                    else {
                        collision = false
                        break
                    }
                }
                if collision {
                    throw RtDbError(
                        code: .conflict, message: "unique index '\(index.name)' violated"
                    )
                }
            }
        }
    }

    // swiftlint:enable cyclomatic_complexity

    /// Full-arity index lookup — a port of server `txn::eq_lookup` (store.ts
    /// `eqLookup`), shared by `expectAbsent` and `upsert`.
    private func eqLookup(
        _ tableDef: TableDef, _ tableName: String, _ indexName: String, _ eq: [JSONValue]
    ) throws -> [StoredRow] {
        let index = try requireIndex(tableDef, indexName)
        guard eq.count == index.fields.count else {
            throw RtDbError(
                code: .badRequest,
                message: "index '\(indexName)' expects \(index.fields.count) eq value(s), "
                    + "got \(eq.count)"
            )
        }
        let typed = try eq.enumerated().map { position, value in
            try coerceIndexValue(tableDef, index.fields[position], value)
        }
        var matches: [StoredRow] = []
        for row in rowStore(tableName).rows.values {
            // FM-33: a soft-deleted row is absent to eq-lookup.
            if row.deletedAt != nil {
                continue
            }
            let allMatch = index.fields.enumerated().allSatisfy { position, field in
                guard let value = row.doc[field], value != .null else { return false }
                return jsonEq(value, typed[position])
            }
            if allMatch {
                matches.append(row)
            }
        }
        return matches
    }

    // MARK: Helpers

    func rowsFor(_ tableName: String) -> [String: StoredRow] {
        rowStore(tableName).rows
    }

    func moveTableRows(from: String, to: String) {
        guard let store = tables.removeValue(forKey: from) else { return }
        tables[to] = store
    }

    func dropTableRows(_ name: String) {
        tables.removeValue(forKey: name)
    }

    /// The live per-table store, created on first touch (store.ts `rowsFor`).
    private func rowStore(_ tableName: String) -> RowStore {
        if let store = tables[tableName] {
            return store
        }
        let store = RowStore()
        tables[tableName] = store
        return store
    }

    private func requireSchema() throws -> SchemaDef {
        guard let schema else {
            throw RtDbError(code: .internal, message: "no schema pushed; call pushSchema first")
        }
        return schema
    }

    private func requireTable(_ name: String) throws -> TableDef {
        guard let def = try requireSchema().tables[name] else {
            throw RtDbError(code: .notFound, message: "table '\(name)' not found")
        }
        return def
    }

    private func requireRow(_ tableName: String, _ id: String) throws -> StoredRow {
        guard let row = rowStore(tableName).rows[id], row.deletedAt == nil else {
            // FM-33: a soft-deleted row is absent to every per-id write lookup.
            throw RtDbError(code: .notFound, message: "document '\(id)' not found")
        }
        return row
    }

    /// UUIDv7-shaped id (timestamp-prefixed for sort stability), 32 hex chars
    /// (store.ts `newId`). The counter suffix guarantees uniqueness even
    /// under a pinned `random: () => 0` — two ids minted in the same instant
    /// must never collide.
    private func newId() -> String {
        let hex = String(nowFn(), radix: 16)
        let padding = max(0, 12 - hex.count)
        let ts = (String(repeating: "0", count: padding) + hex).suffix(12)
        let counter = idCounter % 0x1000000
        idCounter += 1
        let rand = randomHex(13) + String(format: "%06x", counter)
        return "\(ts)7\(rand)"
    }

    /// `count` lowercase hex chars drawn from the injected RNG.
    private func randomHex(_ count: Int) -> String {
        var out = ""
        for _ in 0 ..< count {
            let digit = min(Int(randomFn() * 16), 15)
            out += String(digit, radix: 16)
        }
        return out
    }

    /// Deep table snapshot for txn rollback (store.ts `snapshotTables`).
    private func snapshotTables() -> [String: [String: StoredRow]] {
        tables.mapValues { store in
            store.rows.mapValues { $0.snapshotCopy() }
        }
    }

    private func restoreTables(_ snapshot: [String: [String: StoredRow]]) {
        tables = snapshot.mapValues { rows in
            let store = RowStore()
            store.rows = rows
            return store
        }
    }
}

/// Extracts a stable message from a thrown error (the TS reads `Error.message`).
private func errorMessage(_ error: Error) -> String {
    if let rtError = error as? RtDbError {
        return rtError.message
    }
    return String(describing: error)
}

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
    if query.paginate != nil {
        return "paginate"
    }
    if query.search != nil {
        return "search"
    }
    if query.vectorSearch != nil {
        return "vectorSearch"
    }
    if query.hybridSearch != nil {
        return "hybridSearch"
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
