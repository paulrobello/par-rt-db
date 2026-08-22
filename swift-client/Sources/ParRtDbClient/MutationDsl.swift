import Foundation

// MARK: - MutationBuilder

/// Fluent builder producing a [`Transaction`] — the Swift mirror of
/// rust-client/src/mutation.rs `Mutation`. Every method returns a NEW builder
/// (value-semantics chaining over `let` storage), so branching a chain never
/// shares state; a struct makes that automatic under Sendable.
///
/// Deviations from the task brief, forced by the shipped wire structs and the
/// Task 8 house pattern:
/// - Document bodies (`doc`/`fields`/`insert`/`patch`) are
///   `[String: JSONValue]`, not `JSONValue`. Rust takes `Value` and coerces a
///   non-object to an empty map so the server rejects it; the Swift builder
///   makes a non-object body unrepresentable at compile time instead.
/// - `expectVersion` takes `Int` and the by-query `limit`s take `Int?`; the
///   pending step keeps them and `build()` converts exactly (or throws
///   badRequest) — the same deferred-conversion pattern as `TableQuery.Acc`.
/// - `build()` enforces the server's step cap: rust's builder does no cap
///   checking, but `MutationLimits.maxSteps` documents that builders should
///   refuse to construct a doomed request, so the cap uses the server's
///   recursive `count_steps` port and its verbatim message.
public struct MutationBuilder: Sendable {
    /// The accumulated steps, as builder-side pending values so numeric
    /// arguments stay `Int` and convert exactly (or throw) at `build()`.
    private let pending: [PendingStep]

    /// Start an empty transaction.
    public init() {
        pending = []
    }

    private init(pending: [PendingStep]) {
        self.pending = pending
    }

    private func adding(_ step: PendingStep) -> MutationBuilder {
        MutationBuilder(pending: pending + [step])
    }

    /// Queue an insert step; the step's result is the new document's id.
    public func insert(_ table: String, _ doc: [String: JSONValue]) -> MutationBuilder {
        adding(.insert(table: table, doc: doc))
    }

    /// Queue a patch step (merge `fields` into the row).
    public func patch(_ table: String, _ id: String, _ fields: [String: JSONValue]) -> MutationBuilder {
        adding(.patch(table: table, id: id, fields: fields))
    }

    /// Queue a replace step (overwrite the row).
    public func replace(_ table: String, _ id: String, _ doc: [String: JSONValue]) -> MutationBuilder {
        adding(.replace(table: table, id: id, doc: doc))
    }

    /// Queue a delete step.
    public func delete(_ table: String, _ id: String) -> MutationBuilder {
        adding(.delete(table: table, id: id))
    }

    /// Restore a soft-deleted row (only legal on a `softDelete` table).
    public func undelete(_ table: String, _ id: String) -> MutationBuilder {
        adding(.undelete(table: table, id: id))
    }

    /// Queue a version-precondition step (the row must be at exactly `version`).
    public func expectVersion(_ table: String, _ id: String, _ version: Int) -> MutationBuilder {
        // Int -> Int64 is a total conversion (Int is at most 64-bit wide).
        adding(.expectVersion(table: table, id: id, version: Int64(version)))
    }

    /// Queue an index-absence precondition step (no row may match the eq prefix).
    public func expectAbsent(_ table: String, _ index: String, _ eq: [JSONValue]) -> MutationBuilder {
        adding(.expectAbsent(table: table, index: index, eq: eq))
    }

    /// Queue an insert-or-patch step keyed by an index eq-prefix match.
    public func upsert(
        _ table: String,
        index: String,
        eq: [JSONValue],
        insert: [String: JSONValue],
        patch: [String: JSONValue]
    ) -> MutationBuilder {
        adding(.upsert(table: table, index: index, eq: eq, insert: insert, patch: patch))
    }

    /// Patch every row in `table` matching `filter`. `limit` defaults to the
    /// server cap (1000) when nil; a larger match set patches `limit` rows and
    /// reports `truncated: true` in the result.
    public func patchByQuery(
        _ table: String,
        filter: FilterExpr,
        patch: [String: JSONValue],
        limit: Int? = nil
    ) -> MutationBuilder {
        adding(.patchByQuery(table: table, filter: filter, patch: patch, limit: limit))
    }

    /// Delete every row in `table` matching `filter` (same `limit`/`truncated`
    /// semantics as `patchByQuery`).
    public func deleteByQuery(
        _ table: String,
        filter: FilterExpr,
        limit: Int? = nil
    ) -> MutationBuilder {
        adding(.deleteByQuery(table: table, filter: filter, limit: limit))
    }

    /// Queue `txn` to run later.
    public func schedule(_ when: ScheduleWhen, _ txn: Transaction) -> MutationBuilder {
        adding(.schedule(when: when, txn: txn))
    }

    /// Cancel a previously scheduled job.
    public func cancelSchedule(_ id: String) -> MutationBuilder {
        adding(.cancelSchedule(id: id))
    }

    /// Start a durable workflow run; the server snapshots `spec` per run and
    /// returns the run id as the step's result.
    public func startWorkflow(_ spec: WorkflowSpec) -> MutationBuilder {
        adding(.startWorkflow(spec: spec))
    }

    /// Cancel a workflow run (`false` in the result = already terminal, a
    /// no-op not an error).
    public func cancelWorkflow(_ id: String) -> MutationBuilder {
        adding(.cancelWorkflow(id: id))
    }

    /// Finish to the wire `Transaction`. Throws badRequest when the recursive
    /// step count (flat steps + scheduled/workflow-nested txns — the server's
    /// `count_steps`) exceeds `MutationLimits.maxSteps`, or when a numeric
    /// argument cannot convert exactly to its wire type.
    public func build() throws -> Transaction {
        let txn = try Transaction(steps: pending.map(toStep))
        guard countSteps(txn) <= MutationLimits.maxSteps else {
            throw RtDbError(
                code: .badRequest,
                message: "transaction exceeds maximum of \(MutationLimits.maxSteps) steps "
                    + "(counted recursively, including scheduled txns)"
            )
        }
        return txn
    }

    // swiftlint:disable:next cyclomatic_complexity
    private func toStep(_ pending: PendingStep) throws -> Step {
        switch pending {
        case let .insert(table, doc):
            .insert(table: table, doc: doc)
        case let .patch(table, id, fields):
            .patch(table: table, id: id, fields: fields)
        case let .replace(table, id, doc):
            .replace(table: table, id: id, doc: doc)
        case let .delete(table, id):
            .delete(table: table, id: id)
        case let .undelete(table, id):
            .undelete(table: table, id: id)
        case let .expectVersion(table, id, version):
            .expectVersion(table: table, id: id, version: version)
        case let .expectAbsent(table, index, eq):
            .expectAbsent(table: table, index: index, eq: eq)
        case let .upsert(table, index, eq, insert, patch):
            .upsert(table: table, index: index, eq: eq, insert: insert, patch: patch)
        case let .patchByQuery(table, filter, patch, limit):
            try .patchByQuery(
                table: table,
                filter: filter,
                patch: patch,
                limit: limit.map { try uint32($0, "patchByQuery limit") }
            )
        case let .deleteByQuery(table, filter, limit):
            try .deleteByQuery(
                table: table,
                filter: filter,
                limit: limit.map { try uint32($0, "deleteByQuery limit") }
            )
        case let .schedule(when, txn):
            .schedule(when: when, txn: txn)
        case let .cancelSchedule(id):
            .cancelSchedule(id: id)
        case let .startWorkflow(spec):
            .startWorkflow(spec: spec)
        case let .cancelWorkflow(id):
            .cancelWorkflow(id: id)
        }
    }
}

/// The builder's step accumulator: identical to `Step` except the by-query
/// `limit`s stay `Int?` so `build()` can reject out-of-UInt32-range values
/// with a badRequest instead of trapping at method time.
private enum PendingStep: Sendable {
    case insert(table: String, doc: [String: JSONValue])
    case patch(table: String, id: String, fields: [String: JSONValue])
    case replace(table: String, id: String, doc: [String: JSONValue])
    case delete(table: String, id: String)
    case undelete(table: String, id: String)
    case expectVersion(table: String, id: String, version: Int64)
    case expectAbsent(table: String, index: String, eq: [JSONValue])
    case upsert(
        table: String,
        index: String,
        eq: [JSONValue],
        insert: [String: JSONValue],
        patch: [String: JSONValue]
    )
    case patchByQuery(table: String, filter: FilterExpr, patch: [String: JSONValue], limit: Int?)
    case deleteByQuery(table: String, filter: FilterExpr, limit: Int?)
    case schedule(when: ScheduleWhen, txn: Transaction)
    case cancelSchedule(id: String)
    case startWorkflow(spec: WorkflowSpec)
    case cancelWorkflow(id: String)
}

/// Port of server/src/txn.rs `count_steps` — the step budget is counted
/// recursively: a schedule step costs 1 + its nested txn, a startWorkflow step
/// 1 + every spec step's txn (an `awaitSignal` step has no txn and counts 0
/// nested — the server's `s.txn.as_ref().map_or(0, count_steps)`).
/// `build()` enforces the cap against this count.
func countSteps(_ txn: Transaction) -> Int {
    txn.steps.reduce(0) { total, step in
        switch step {
        case let .schedule(_, nested):
            total + 1 + countSteps(nested)
        case let .startWorkflow(spec):
            total + 1 + spec.steps.reduce(0) { $0 + ($1.txn.map(countSteps) ?? 0) }
        default:
            total + 1
        }
    }
}

// MARK: - WorkflowStepSpec: awaitSignal builder

public extension WorkflowStepSpec {
    /// Build an `awaitSignal` wait step — park the run until a signal named
    /// `name` is delivered (`timeoutMs` bounds each wait attempt; nil waits
    /// indefinitely, cancel is the escape). The `WorkflowStepSpec(txn:)`
    /// initializer constructs the ordinary-step variant; together the two
    /// constructors enforce the exactly-one-of rule structurally.
    ///
    /// Throws badRequest eagerly for the two constraints the server's
    /// `validate_spec` checks on every `awaitSignal` step: `name` 1..=256
    /// chars and `timeoutMs` > 0 when present.
    static func awaitSignal(
        name: String, timeoutMs: UInt64? = nil, retry: StepRetry? = nil,
        sleepBeforeMs: UInt64? = nil
    ) throws -> WorkflowStepSpec {
        if name.isEmpty || name.count > 256 {
            throw RtDbError(code: .badRequest, message: "awaitSignal.name must be 1..=256 chars")
        }
        if let timeoutMs, timeoutMs == 0 {
            throw RtDbError(code: .badRequest, message: "awaitSignal.timeoutMs must be > 0")
        }
        return WorkflowStepSpec(
            txn: nil,
            awaitSignal: AwaitSignalSpec(name: name, timeoutMs: timeoutMs),
            retry: retry,
            sleepBeforeMs: sleepBeforeMs
        )
    }
}

// MARK: - Transaction: WireEncodable

/// `wireObject()` comes from `WireEncodable`'s Codable default implementation
/// (JSONValue.swift) — the Task 8 `Query` helper generalized in Task 9.
extension Transaction: WireEncodable {}
