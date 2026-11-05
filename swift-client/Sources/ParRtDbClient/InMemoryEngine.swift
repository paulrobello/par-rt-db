import CryptoKit
import Foundation

// Mirrors ts-client/src/in_memory/store.ts — the in-memory engine's client
// core (mirrors rust-client/src/in_memory/mod.rs): the engine class (stored
// state, init, admin view, file storage, and the shared lookup/snapshot
// helpers) plus the private workflow-spec validators. The client-coupled
// supporting types live in `InMemoryEngineTypes.swift`, presence in
// `InMemoryPresence.swift`, the schedule/workflow stores in
// `InMemorySchedules.swift` / `InMemoryWorkflows.swift`, doc validation and
// stamping in `InMemoryDocValidation.swift`, and the admin surface in
// `InMemoryAdmin.swift`. The query engine lives in `InMemoryQuery.swift`,
// the migration engine in `InMemoryMigrate.swift`, and value/filter
// validation in `InMemoryValidate.swift`.
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

/// FM-29 + awaitSignal: one spec step's submit-time checks — a port of the
/// per-step half of server `workflows::validate_spec`. A step carries exactly
/// one of txn/awaitSignal; the wait declaration is bounded (name 1..=256
/// chars, a present timeoutMs strictly positive); retry needs
/// maxAttempts >= 1 and maxRetryMs >= a positive initialRetryMs.
private func validateWorkflowStep(_ step: WorkflowStepSpec, index: Int) throws {
    switch (step.txn, step.awaitSignal) {
    case (.some, .some), (.none, .none):
        throw RtDbError(
            code: .badRequest,
            message: "steps[\(index)] must carry exactly one of txn or awaitSignal"
        )
    case (.some, nil):
        break
    case (nil, let .some(signal)):
        if signal.name.isEmpty || signal.name.count > 256 {
            throw RtDbError(
                code: .badRequest,
                message: "steps[\(index)].awaitSignal.name must be 1..=256 chars"
            )
        }
        if let timeoutMs = signal.timeoutMs, timeoutMs == 0 {
            throw RtDbError(
                code: .badRequest,
                message: "steps[\(index)].awaitSignal.timeoutMs must be > 0"
            )
        }
    }
    guard let retry = step.retry else { return }
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
        try validateWorkflowStep(step, index: index)
    }
    let total = spec.steps.reduce(0) { $0 + ($1.txn.map(countSteps) ?? 0) }
    if total > InMemoryLimits.maxSteps {
        throw RtDbError(
            code: .badRequest,
            message: "workflow recursive step count \(total) exceeds MAX_STEPS "
                + "\(InMemoryLimits.maxSteps)"
        )
    }
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
    var schema: SchemaDef?
    var tables: [String: RowStore] = [:]
    private var idempotency: [String: [JSONValue]] = [:]
    private var subs: [EngineSubscription] = []
    var schedules: [String: ScheduledJob] = [:]
    private var scheduleOrder: [String] = []
    var workflows: [String: WorkflowRun] = [:]
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
    // Per-database read-only freeze (server PATCH /admin/db/{db}/readonly).
    var readOnly = false

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
        var schema = schema
        // FM-30: mirrors server `ddl::push_schema` — grandfather `trgm` on a
        // search index that already existed before this push declared it,
        // before the (deliberately trgm-blind) destructive-change check.
        grandfatherTrgm(old: self.schema, next: &schema)
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
        repositionAutoIncrementCounters(from: self.schema, to: schema)
        self.schema = schema
    }

    /// A push that ADDS an `autoIncrementField` declaration to a table with
    /// existing rows positions the counter past the stored max (the server's
    /// DDL creates the sequence `START max+1`), so numbering continues past
    /// pre-declaration values instead of overwriting them. A re-push of an
    /// already-declared counter never repositions — the counter only moves
    /// forward.
    private func repositionAutoIncrementCounters(from current: SchemaDef?, to next: SchemaDef) {
        guard let current else { return } // first push: every store is empty
        for (name, table) in next.tables {
            guard let field = table.autoIncrementField,
                  current.tables[name]?.autoIncrementField == nil,
                  let store = tables[name] else { continue }
            var maxStored: Int64 = 0
            for row in store.rows.values {
                if case let .string(text)? = row.doc[field], let value = Int64(text), value > maxStored {
                    maxStored = value
                }
            }
            store.autoIncrementNext = max(store.autoIncrementNext, maxStored + 1)
        }
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
        // Directive folding must not be able to invalidate a computed entry —
        // e.g. `changeType` retyping a computed field so its expression's
        // static kind no longer fits. Re-validate the derived map here (pure),
        // mirroring plan_migration's post-folding validate_computed; the
        // restore covers the interleaved row effects the server's plan-first
        // ordering never commits.
        do {
            try validateComputed(planned)
        } catch {
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
    /// One-shot query — same shape as the HTTP client's `query` (store.ts
    /// `query`). The raw JSONValue result is decodable via `parseResult`.
    public func query(_ query: Query) throws -> JSONValue {
        try executeQuery(query, requireTable(query.table), { [self] table in
            rowsFor(table)
        }, { [self] table in
            try requireTable(table)
        })
    }

    /// Executes a transaction and returns one result per step, in order
    /// (store.ts `mutate`); `idempotencyKey` replays a cached result.
    public func mutate(
        _ txn: Transaction, idempotencyKey: String? = nil
    ) throws -> [StepResult] {
        if let idempotencyKey, let cached = idempotency[idempotencyKey] {
            return try cached.map(parseStepResult)
        }
        if readOnly {
            throw RtDbError(
                code: .readOnly,
                message: "database is frozen read-only; an operator can unfreeze it via "
                    + "PATCH /admin/db/{db}/readonly"
            )
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
    func executeTransaction(_ txn: Transaction) throws -> [JSONValue] {
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
            case .patchByQuery, .deleteByQuery, .expectNoMatch:
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
        try checkDeadlineGuards(txn)
        let snapshot = snapshotTables()
        // FM-28/FM-29: a schedule/workflow control step mutates its store, so
        // a failed txn must roll it back with the docs. The snapshots are
        // shallow — the TS shares the job/run objects too.
        let schedulesSnapshot = schedules
        let scheduleOrderSnapshot = scheduleOrder
        let workflowsSnapshot = WorkflowTransactionSnapshot(workflows, order: workflowOrder)
        var results: [JSONValue] = []
        var writeSet: Set<String> = []
        do {
            for step in txn.steps {
                try enforceInsertOnly(step)
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
            try checkDeadlineGuards(txn)
        } catch {
            // Atomicity: any step's error rolls back everything already
            // applied.
            restoreTables(snapshot)
            schedules = schedulesSnapshot
            scheduleOrder = scheduleOrderSnapshot
            workflowsSnapshot.restore(into: &workflows, order: &workflowOrder)
            throw error
        }
        notifySubs(writeSet)
        return results
    }

    // swiftlint:enable function_body_length

    private func checkDeadlineGuards(_ txn: Transaction) throws {
        let deadlines: [Int64] = txn.steps.compactMap { step in
            if case let .expectBefore(value) = step {
                return value
            }
            return nil
        }
        guard !deadlines.isEmpty else { return }
        for deadline in deadlines {
            guard (0 ... 9_007_199_254_740_991).contains(deadline) else {
                throw RtDbError(code: .badRequest, message: "deadlineMs must be a nonnegative safe integer")
            }
        }
        let now = nowFn()
        if deadlines.contains(where: { now >= $0 }) {
            throw RtDbError(code: .preconditionFailed, message: "transaction deadline expired")
        }
    }

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
        let initial = try executeQuery(sub.query, requireTable(sub.table), { [self] table in
            rowsFor(table)
        }, { [self] table in
            try requireTable(table)
        })
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
    /// independent — the TS canonicalizes for the same property. A projected
    /// subscription (`fields` set) compares through `diffCanonical`, which
    /// strips the volatile `_version` first; pushed payloads still carry it.
    private func notifySubs(_ writeSet: Set<String>) {
        for sub in subs {
            guard writeSet.contains(sub.table), sub.onUpdate != nil,
                  let next = try? executeQuery(
                      sub.query, requireTable(sub.table), rowsFor,
                      { [self] table in try requireTable(table) }
                  )
            else { continue }
            if sub.hasValue, let last = sub.last, diffCanonical(sub.query, next) == diffCanonical(sub.query, last) {
                continue
            }
            sub.last = next
            sub.hasValue = true
            sub.onUpdate?(next)
        }
    }

    /// The form a subscription's result diffs against for push decisions — a
    /// port of server `diff_canonical`. For an UNPROJECTED query this is the
    /// result as-is (unchanged push behavior). For a PROJECTED one the
    /// volatile `_version` is stripped from every doc: it bumps on every
    /// write, so an unstripped compare would push on any member write even
    /// when no projected field changed — defeating the payload-width point of
    /// a projected subscription. (A subscriber that must see every `_version`
    /// bump should use an unprojected subscription.)
    private func diffCanonical(_ query: Query, _ result: JSONValue) -> JSONValue {
        guard query.fields != nil else {
            return result
        }
        return mapResultDocs(query, result) { doc in
            guard case var .object(object) = doc else { return doc }
            object.removeValue(forKey: "_version")
            return .object(object)
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
    /// `external: true` never fires via `tick` — the harness does not model
    /// the claim surface; an application worker owns the job's execution.
    @discardableResult
    public func schedule(
        _ txn: Transaction, when: ScheduleWhen, external: Bool = false
    ) throws -> String {
        try scheduleJob(txn, when, external: external)
    }

    /// Sync core of `schedule` — the `Step.schedule` transaction step reuses
    /// it from the sync executeStep path.
    private func scheduleJob(
        _ txn: Transaction, _ when: ScheduleWhen, external: Bool = false
    ) throws -> String {
        let id = newId()
        let now = nowFn()
        var kind = ScheduleKind.oneshot
        var cron: String?
        var tz: String?
        var everyMs: Int64?
        if case let .cron(expr, cronTz) = when {
            kind = .cron
            cron = expr
            tz = cronTz
        }
        if case let .interval(ms) = when {
            // Server `scheduler::resolve_when`: everyMs must be positive and
            // at most MAX_EVERY_MS.
            if ms <= 0 {
                throw RtDbError(code: .badRequest, message: "everyMs must be positive")
            }
            if ms > InMemoryLimits.maxEveryMs {
                throw RtDbError(
                    code: .badRequest,
                    message: "everyMs must be at most \(InMemoryLimits.maxEveryMs)"
                )
            }
            kind = .interval
            everyMs = ms
        }
        let job = ScheduledJob(
            id: id,
            kind: kind,
            txn: txn,
            dueAt: dueAtFor(when, now),
            cron: cron,
            tz: tz,
            everyMs: everyMs,
            status: .pending,
            createdAt: now,
            firedCount: 0,
            external: external
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

    /// Resumes a paused job: false when missing or not paused. An interval
    /// job re-arms one full interval from resume (windows elapsed while
    /// paused are skipped, never backfilled — server `scheduler::set_paused`);
    /// a cron's next fire can't be recomputed engine-side (no cron parser —
    /// `cronStepMs` only approximates it at fire time), so its dueAt is
    /// left alone.
    @discardableResult
    public func resumeSchedule(_ id: String) -> Bool {
        guard let job = schedules[id], job.status == .paused else { return false }
        if job.kind == .interval, let ms = job.everyMs {
            job.dueAt = nowFn() + ms
        }
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
        case let .interval(ms): now + ms
        }
    }

    private func toScheduleInfo(_ job: ScheduledJob) -> ScheduleInfo {
        ScheduleInfo(
            id: job.id,
            kind: job.kind,
            dueAt: job.dueAt,
            cron: job.cron,
            tz: job.tz,
            everyMs: job.everyMs,
            status: job.status,
            lastError: job.lastError,
            createdAt: job.createdAt,
            firedCount: job.firedCount,
            external: job.external,
            missedCount: job.missedCount,
            lastMissedAt: job.lastMissedAt
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

    /// Cancels a pending/running/waiting run: false (a no-op) for an unknown
    /// or terminal run — the server's `workflows::cancel` contract. Waiting
    /// rows are cancellable; the wait columns drop with the flip (the
    /// leave-`waiting` rule).
    @discardableResult
    public func cancelWorkflow(_ id: String) -> Bool {
        guard let run = workflows[id],
              run.status == .pending || run.status == .running || run.status == .waiting
        else {
            return false
        }
        run.status = .cancelled
        run.waitName = nil
        run.waitedSince = nil
        run.signalPayload = nil
        run.finishedAt = nowFn()
        run.updatedAt = nowFn()
        return true
    }

    /// Delivers a named signal to a waiting run (the server's
    /// `workflows::deliver_signal`): latest-wins slot write + wake flip in one
    /// step — a `waiting` run AND a `pending` run whose `waitName` survived an
    /// earlier delivery both match (every delivery while the wait is
    /// unconsumed overwrites the slot; an ordinary pending run never matches).
    /// The flipped run is due on the next `tick`. Typed errors mirror the
    /// server's classification: NOT_FOUND unknown id, CONFLICT not-waiting,
    /// CONFLICT name mismatch naming both names. An omitted payload is
    /// delivered as JSON null — the slot is always set on a delivery (nil
    /// stays exclusively the no-signal state).
    public func signalWorkflow(_ id: String, name: String, payload: JSONValue? = nil) throws {
        if let payload {
            let encoded = try JSONEncoder().encode(payload)
            if encoded.count > InMemoryLimits.maxSignalPayloadBytes {
                throw RtDbError(
                    code: .badRequest,
                    message: "signal payload exceeds \(InMemoryLimits.maxSignalPayloadBytes) bytes"
                )
            }
        }
        guard let run = workflows[id] else {
            throw RtDbError(code: .notFound, message: "unknown workflow")
        }
        let now = nowFn()
        if run.status == .waiting || run.status == .pending, run.waitName == name {
            run.status = .pending
            run.sleepUntil = now
            run.signalPayload = payload ?? .null
            run.updatedAt = now
            return
        }
        if run.status == .waiting {
            throw RtDbError(
                code: .conflict,
                message: "workflow waiting on '\(run.waitName ?? "")', got '\(name)'"
            )
        }
        throw RtDbError(code: .conflict, message: "workflow is not waiting for a signal")
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
        // Wait columns project only while `waiting` — terminal rows never
        // leak stale wait state (leave-waiting ops clear it).
        let waiting = run.status == .waiting
        return WorkflowInfo(
            id: run.id,
            name: run.spec.name,
            status: run.status,
            currentStep: UInt32(run.currentStep),
            stepCount: UInt32(run.spec.steps.count),
            attempts: UInt32(run.attempts),
            sleepUntil: run.sleepUntil,
            lastError: run.lastError,
            waitingFor: waiting ? run.waitName : nil,
            waitedSince: waiting ? run.waitedSince : nil,
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

    /// Computes and accumulates the count of interval windows that elapsed
    /// before this fire (`prevDueAt` to `now`, exclusive of the fire itself).
    /// Exact for interval jobs, unlike cron, which this harness approximates
    /// on a fixed `cronStepMs` rather than real cron occurrence math. A no-op
    /// when nothing was missed (the common case: normal poll-cadence fires).
    private func recordMissedIntervalWindows(on job: ScheduledJob, prevDueAt: Int64, now: Int64, everyMs ms: Int64) {
        guard ms > 0 else { return }
        let delta = now - prevDueAt
        guard delta > 0 else { return }
        let missed = (delta - 1) / ms
        guard missed > 0 else { return }
        job.missedCount += missed
        job.lastMissedAt = now
    }

    /// Fires every due non-paused, non-external job by applying its txn
    /// through the same atomic path as `mutate`; advances due workflow runs
    /// (FM-29); reaps expired TTL documents (FM-33-aware hard delete). Pass
    /// `nowMs` to drive the clock deterministically. Returns the count of
    /// documents reaped (store.ts `tick` — whose reaper cascade can throw here
    /// too, exactly like the TS propagates it out of `tick`).
    @discardableResult
    public func tick(nowMs: Int64? = nil) throws -> Int {
        let now = nowMs ?? nowFn()
        for id in scheduleOrder {
            guard let job = schedules[id] else { continue }
            // External jobs are never internally executed — they sit until an
            // application worker claims them over HTTP (server `claim_due`
            // excludes `external` rows the same way).
            if job.external || job.status == .paused || job.dueAt > now {
                continue
            }
            do {
                let prevDueAt = job.dueAt
                _ = try executeTransaction(job.txn)
                job.firedCount += 1
                if job.kind == .oneshot {
                    schedules.removeValue(forKey: id)
                    scheduleOrder.removeAll { $0 == id }
                } else if job.kind == .interval, let ms = job.everyMs {
                    // Interval re-arms from each actual fire time (cron
                    // parity: windows missed during the fire's latency are
                    // skipped, not backfilled). The elapsed-window count is
                    // exact here (unlike cron, which this harness
                    // approximates on a fixed cronStepMs).
                    recordMissedIntervalWindows(on: job, prevDueAt: prevDueAt, now: now, everyMs: ms)
                    job.dueAt = now + ms
                    job.status = .pending
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
                // Interval re-arms after a failure too — recurring kinds keep
                // firing; only a one-shot records the error and stops.
                if job.kind == .interval, let ms = job.everyMs {
                    job.dueAt = now + ms
                }
            }
        }
        // FM-29: claim due pending runs (server `claim_due`), then advance.
        // A `waiting` run whose timeout gate (its `sleepUntil`) expired is
        // claimed too — that claim IS the timed-out attempt.
        let due = workflowOrder.compactMap { workflows[$0] }.filter {
            ($0.status == .pending || $0.status == .waiting) && $0.sleepUntil <= now
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

    /// Applies one step and returns its raw result, primary table, and any
    /// cascade-touched extra tables (store.ts `executeStep`). Dispatches to a
    /// per-step handler; the four control-flow steps target their own stores
    /// (schedule/workflow), not a table.
    private func executeStep(_ step: Step) throws -> StepExecution {
        // The control-flow steps target the schedule/workflow stores, not a
        // table; everything else dispatches through the table-op layer.
        switch step {
        case .expectBefore, .schedule, .cancelSchedule, .startWorkflow, .cancelWorkflow:
            try executeControlFlowStep(step)
        case .insert, .patch, .adjustCounter, .replace, .delete, .undelete,
             .expectVersion, .expectFields, .expectAbsent, .expectNoMatch, .upsert, .insertMany,
             .upsertMany, .patchByQuery,
             .deleteByQuery:
            try executeTableStep(step)
        }
    }

    /// The schedule/workflow control-flow half of `executeStep`.
    private func executeControlFlowStep(_ step: Step) throws -> StepExecution {
        switch step {
        case .expectBefore:
            StepExecution(result: .null, table: nil, extraTables: [])
        case let .schedule(when, txn, external):
            try stepSchedule(when, txn, external ?? false)
        case let .cancelSchedule(id):
            stepCancelSchedule(id)
        case let .startWorkflow(spec):
            try stepStartWorkflow(spec)
        case let .cancelWorkflow(id):
            stepCancelWorkflow(id)
        case .insert, .patch, .adjustCounter, .replace, .delete, .undelete,
             .expectVersion, .expectFields, .expectAbsent, .expectNoMatch, .upsert, .insertMany,
             .upsertMany, .patchByQuery,
             .deleteByQuery:
            throw RtDbError(code: .internal, message: "table steps handled by executeTableStep")
        }
    }

    /// The per-id table-op half of `executeStep` — one handler per variant.
    private func executeTableStep(_ step: Step) throws -> StepExecution {
        switch step {
        case let .insert(table, doc):
            return try stepInsert(table, doc)
        case let .insertMany(table, docs):
            return try stepInsertMany(table, docs)
        case let .upsertMany(table, index, docs):
            return try stepUpsertMany(table, index, docs)
        case let .patch(table, id, fields):
            return try stepPatch(table, id, fields)
        case let .adjustCounter(table, id, field, delta, minimum, maximum, expected):
            let adjustment = CounterAdjustment(
                field: field, delta: delta, min: minimum, max: maximum, expected: expected
            )
            return try stepAdjustCounter(table, id, adjustment)
        case let .replace(table, id, doc):
            return try stepReplace(table, id, doc)
        case let .delete(table, id):
            return try stepDelete(table, id)
        case let .undelete(table, id):
            return try stepUndelete(table, id)
        case .expectVersion, .expectFields, .expectAbsent, .expectNoMatch, .upsert, .patchByQuery,
             .deleteByQuery:
            return try executeLookupStep(step)
        case .expectBefore, .schedule, .cancelSchedule, .startWorkflow, .cancelWorkflow:
            throw RtDbError(code: .internal, message: "control-flow steps handled above")
        }
    }

    /// The lookup / by-query half of `executeStep` — the variants whose
    /// validation reads the index scan surface.
    private func executeLookupStep(_ step: Step) throws -> StepExecution {
        switch step {
        case let .expectVersion(table, id, version):
            return try stepExpectVersion(table, id, version)
        case let .expectFields(table, id, expected):
            let definition = try requireTable(table)
            let row = try requireRow(table, id)
            try checkFieldExpectations(definition, row.doc, expected)
            return StepExecution(result: .null, table: nil, extraTables: [])
        case let .expectAbsent(table, index, eq):
            return try stepExpectAbsent(table, index, eq)
        case let .expectNoMatch(table, filter):
            return try stepExpectNoMatch(table, filter)
        case let .upsert(table, index, eq, insert, patch):
            return try stepUpsert(table, index, eq, insert, patch)
        case let .patchByQuery(table, filter, patch, limit):
            return try stepPatchByQuery(table, filter, patch, limit)
        case let .deleteByQuery(table, filter, limit):
            return try stepDeleteByQuery(table, filter, limit)
        default:
            throw RtDbError(code: .internal, message: "lookup steps handled here only")
        }
    }

    private func stepSchedule(
        _ when: ScheduleWhen, _ txn: Transaction, _ external: Bool
    ) throws -> StepExecution {
        let id = try scheduleJob(txn, when, external: external)
        return StepExecution(
            result: .object(["scheduleId": .string(id)]), table: nil, extraTables: []
        )
    }

    private func stepCancelSchedule(_ id: String) -> StepExecution {
        let cancelled = cancelSchedule(id)
        return StepExecution(
            result: .object(["cancelled": .bool(cancelled)]), table: nil, extraTables: []
        )
    }

    private func stepStartWorkflow(_ spec: WorkflowSpec) throws -> StepExecution {
        try validateWorkflowSpec(spec)
        let run = try startWorkflowJob(spec)
        return StepExecution(
            result: .object(["workflowId": .string(run.id)]), table: nil, extraTables: []
        )
    }

    private func stepCancelWorkflow(_ id: String) -> StepExecution {
        let cancelled = cancelWorkflow(id)
        return StepExecution(
            result: .object(["cancelled": .bool(cancelled)]), table: nil, extraTables: []
        )
    }

    private func stepInsert(_ table: String, _ doc: [String: JSONValue]) throws -> StepExecution {
        let tableDef = try requireTable(table)
        let id = try doInsert(table, tableDef, doc)
        return StepExecution(result: .object(["id": .string(id)]), table: table, extraTables: [])
    }

    private func checkBulkLen(_ op: String, _ count: Int) throws {
        if count > InMemoryLimits.maxByQueryRows {
            throw RtDbError(
                code: .badRequest,
                message: "\(op) has \(count) docs, exceeding the limit of \(InMemoryLimits.maxByQueryRows)"
            )
        }
    }

    private func stepInsertMany(
        _ table: String, _ docs: [[String: JSONValue]]
    ) throws -> StepExecution {
        try checkBulkLen("insertMany", docs.count)
        let tableDef = try requireTable(table)
        var ids: [JSONValue] = []
        for doc in docs {
            try ids.append(.string(doInsert(table, tableDef, doc)))
        }
        return StepExecution(result: .object(["ids": .array(ids)]), table: table, extraTables: [])
    }

    private func stepUpsertMany(
        _ table: String, _ index: String, _ docs: [[String: JSONValue]]
    ) throws -> StepExecution {
        try checkBulkLen("upsertMany", docs.count)
        let tableDef = try requireTable(table)
        guard let indexDef = (tableDef.indexes ?? []).first(where: { $0.name == index }) else {
            throw RtDbError(code: .badRequest, message: "index '\(index)' not found")
        }
        var ids: [JSONValue] = []
        var flags: [JSONValue] = []
        for (docIndex, doc) in docs.enumerated() {
            var eq: [JSONValue] = []
            for field in indexDef.fields {
                guard let value = doc[field] else {
                    throw RtDbError(
                        code: .badRequest,
                        message: "upsertMany docs[\(docIndex)] is missing indexed field '\(field)' of index '\(index)'"
                    )
                }
                eq.append(value)
            }
            let step = try stepUpsert(table, index, eq, doc, doc)
            guard case let .object(result) = step.result, let id = result["id"],
                  let inserted = result["inserted"]
            else {
                throw RtDbError(code: .internal, message: "upsert returned an unexpected result")
            }
            ids.append(id)
            flags.append(inserted)
        }
        return StepExecution(
            result: .object(["ids": .array(ids), "inserted": .array(flags)]),
            table: table, extraTables: []
        )
    }

    private func stepPatch(
        _ table: String, _ id: String, _ fields: [String: JSONValue]
    ) throws -> StepExecution {
        let tableDef = try requireTable(table)
        try doPatch(tableDef, table, id, fields)
        return StepExecution(result: .null, table: table, extraTables: [])
    }

    private func stepAdjustCounter(
        _ table: String, _ id: String, _ adjustment: CounterAdjustment
    ) throws -> StepExecution {
        let tableDef = try requireTable(table)
        try doAdjustCounter(tableDef, table, id, adjustment)
        return StepExecution(result: .null, table: table, extraTables: [])
    }

    private func stepReplace(
        _ table: String, _ id: String, _ doc: [String: JSONValue]
    ) throws -> StepExecution {
        let tableDef = try requireTable(table)
        try doReplace(tableDef, table, id, doc)
        return StepExecution(result: .null, table: table, extraTables: [])
    }

    private func stepDelete(_ table: String, _ id: String) throws -> StepExecution {
        let tableDef = try requireTable(table)
        let extraTables = try doDelete(tableDef, table, id)
        return StepExecution(result: .null, table: table, extraTables: extraTables)
    }

    private func stepUndelete(_ table: String, _ id: String) throws -> StepExecution {
        let tableDef = try requireTable(table)
        try doUndelete(tableDef, table, id)
        return StepExecution(result: .null, table: table, extraTables: [])
    }

    private func stepExpectVersion(
        _ table: String, _ id: String, _ version: Int64
    ) throws -> StepExecution {
        _ = try requireTable(table)
        try doExpectVersion(table, id, version)
        return StepExecution(result: .null, table: nil, extraTables: [])
    }

    private func stepExpectAbsent(
        _ table: String, _ index: String, _ eq: [JSONValue]
    ) throws -> StepExecution {
        let tableDef = try requireTable(table)
        let rows = try eqLookup(tableDef, table, index, eq)
        if !rows.isEmpty {
            throw RtDbError(
                code: .preconditionFailed,
                message: "index '\(index)' already has a matching document"
            )
        }
        return StepExecution(result: .null, table: nil, extraTables: [])
    }

    /// `expectNoMatch`: aborts when any visible live row matches `filter`
    /// (same scan and soft-delete visibility as the by-query steps, so
    /// `olderThan` is accepted); writes nothing.
    private func stepExpectNoMatch(
        _ table: String, _ filter: FilterExpr
    ) throws -> StepExecution {
        let tableDef = try requireTable(table)
        let (rows, _) = try scanByQuery(tableDef, table, filter, 1)
        if !rows.isEmpty {
            throw RtDbError(
                code: .preconditionFailed, message: "filter matched an existing document"
            )
        }
        return StepExecution(result: .null, table: nil, extraTables: [])
    }

    private func stepUpsert(
        _ table: String, _ index: String, _ eq: [JSONValue],
        _ insert: [String: JSONValue], _ patch: [String: JSONValue]
    ) throws -> StepExecution {
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
        let merged = try applyPatch(
            tableDef, row.doc, stampUpdatedAt(tableDef, patch, nowFn()), now: nowFn()
        )
        try doUpdate(table, tableDef, row, merged)
        return StepExecution(
            result: .object(["id": .string(row.id), "inserted": .bool(false)]),
            table: table, extraTables: []
        )
    }

    private func stepPatchByQuery(
        _ table: String, _ filter: FilterExpr, _ patch: [String: JSONValue], _ limit: UInt32?
    ) throws -> StepExecution {
        let tableDef = try requireTable(table)
        let (rows, truncated) = try scanByQuery(tableDef, table, filter, limit)
        for row in rows {
            // Fresh stamp per row, exactly as the server restamps inside
            // its per-row loop.
            let merged = try applyPatch(
                tableDef, row.doc, stampUpdatedAt(tableDef, patch, nowFn()), now: nowFn()
            )
            try doUpdate(table, tableDef, row, merged)
        }
        return StepExecution(
            result: .object(["patched": .int(Int64(rows.count)), "truncated": .bool(truncated)]),
            table: table, extraTables: []
        )
    }

    private func stepDeleteByQuery(
        _ table: String, _ filter: FilterExpr, _ limit: UInt32?
    ) throws -> StepExecution {
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
    }

    private func doInsert(
        _ tableName: String, _ tableDef: TableDef, _ doc: [String: JSONValue]
    ) throws -> String {
        var stamped = stampTtlDefault(tableDef, doc, nowFn())
        stamped = stampUpdatedAt(tableDef, stamped, nowFn())
        stamped = applyDefaults(tableDef, stamped)
        stamped = stampAutoIncrement(rowStore(tableName), tableDef, stamped)
        // Computed stamping is the LAST insert stamp — after the ttl /
        // updatedAt / defaults / autoIncrement chain — so expressions see
        // final inputs, and before validateDoc so a client-supplied computed
        // value never reaches validation.
        stamped = try stampComputed(tableDef, stamped, nowFn())
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
        // The updatedAt stamp rides IN the patch payload, so the merged doc
        // carries it even when the patch does not mention the field.
        let stamped = stampUpdatedAt(tableDef, fields, nowFn())
        let merged = try applyPatch(tableDef, row.doc, stamped, now: nowFn())
        try doUpdate(tableName, tableDef, row, merged)
    }

    private func doAdjustCounter(
        _ tableDef: TableDef, _ tableName: String, _ id: String, _ adjustment: CounterAdjustment
    ) throws {
        try adjustment.validate(table: tableDef)
        let row = try requireRow(tableName, id)
        let value = try adjustment.value(in: row.doc)
        try doPatch(tableDef, tableName, id, [adjustment.field: value])
    }

    private func doReplace(
        _ tableDef: TableDef, _ tableName: String, _ id: String, _ doc: [String: JSONValue]
    ) throws {
        let row = try requireRow(tableName, id)
        // Defaults re-apply on a full replace; the TTL default does NOT
        // (only insert stamps it — store.ts `doReplace`). The updatedAt
        // stamp DOES restamp on replace.
        let stamped = stampUpdatedAt(tableDef, applyDefaults(tableDef, doc), nowFn())
        // A replace validates as a complete document, so the server-stamped
        // auto-increment value must be present: an omitted (or null) field is
        // filled from the stored row (preserved, never re-assigned), and a
        // supplied value must equal the stored one — round-trip replace
        // works, changing the counter does not. A stored doc that PREDATES
        // the declaration (written before the counter was added) has no
        // value to preserve, so a replace may set one — first-set, like an
        // insert (server `txn::do_replace`).
        var resolved = stamped
        if let auto = tableDef.autoIncrementField, let storedCounter = row.doc[auto] {
            if let supplied = resolved[auto] {
                if case .null = supplied {
                    resolved[auto] = storedCounter
                } else if supplied != storedCounter {
                    throw RtDbError(
                        code: .badRequest,
                        message: "autoIncrementField '\(auto)' cannot be changed"
                    )
                }
            } else {
                resolved[auto] = storedCounter
            }
        }
        // Client-supplied computed values are dropped before validation: the
        // stamp re-derives them from the final doc, and a wrong-typed client
        // value must not fail validateDoc first.
        resolved = try stampComputed(tableDef, resolved, nowFn())
        try validateDoc(tableDef, resolved)
        let stored = stripUnsetOptionals(tableDef, resolved)
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
                    // The patch payload carries the CHILD table's updatedAt
                    // stamp when it declares one.
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
                        let stamped = stampUpdatedAt(
                            childTableDef, [fieldName: .null], nowFn()
                        )
                        let merged = try applyPatch(
                            childTableDef, childRow.doc, stamped, now: nowFn()
                        )
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
        try validateFilter(filter, tableDef, allowRelativeTime: true)
        let limit = min(limitOpt.map(Int.init) ?? InMemoryLimits.maxByQueryRows,
                        InMemoryLimits.maxByQueryRows)
        // The by-query scan is the one surface admitting `olderThan`: its
        // cutoff is derived from the clock at execution, read once per scan
        // (server `older_than_lhs_and_bind` runs at compile == execution
        // time, once per scan's WHERE compile).
        let now = nowFn()
        var matched: [StoredRow] = []
        for row in rowStore(tableName).rows.values {
            if row.deletedAt != nil {
                continue
            } // FM-33: stamped rows are absent
            if evalFilterExpr(filter, row.doc, tableDef.fields, now: now) {
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
func errorMessage(_ error: Error) -> String {
    if let rtError = error as? RtDbError {
        return rtError.message
    }
    return String(describing: error)
}
