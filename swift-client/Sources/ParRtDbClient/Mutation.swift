import Foundation

/// Protocol contract constants for mutations. `maxSteps` mirrors
/// server/src/txn.rs `MAX_STEPS` and rust-client/src/in_memory/mod.rs
/// `MAX_STEPS`; the wire-corpus `protocol_constants.max_steps` assertion pins
/// this number — change it only with the server and every client together.
public enum MutationLimits {
    /// Maximum steps in one transaction, counted recursively (scheduled txns
    /// included). The server rejects a larger txn; builders should refuse to
    /// construct one rather than send a doomed request.
    public static let maxSteps = 1024
}

/// Mirrors server/src/dsl.rs::Transaction (rust-client/src/mutation.rs::
/// Transaction) — an ordered list of steps applied atomically by the server's
/// committer. `steps` is required; unknown fields are tolerated (neither the
/// server nor the rust-client Transaction carries `deny_unknown_fields`).
/// The max-steps cap is NOT enforced here — `MutationLimits` (Task 6) and the
/// builder layer own it.
public struct Transaction: Equatable, Codable, Sendable {
    /// The steps, applied in order; any failure rolls the whole txn back.
    public var steps: [Step]

    public init(steps: [Step]) {
        self.steps = steps
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case steps
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        steps = try container.decode([Step].self, forKey: .steps)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(steps, forKey: .steps)
    }
}

/// Mirrors server/src/dsl.rs::Step (rust-client/src/mutation.rs::Step) — one
/// write/control step, internally tagged on `"op"`, camelCase tags and fields,
/// unknown fields rejected per variant. Document bodies are
/// `[String: JSONValue]` (serde `Map<String, Value>`).
public enum Step: Equatable, Codable, Sendable {
    /// Insert a new document; result is its id.
    case insert(table: String, doc: [String: JSONValue])
    /// Merge `fields` into an existing document; result is null.
    case patch(table: String, id: String, fields: [String: JSONValue])
    /// Overwrite the whole document; result is null.
    case replace(table: String, id: String, doc: [String: JSONValue])
    /// Delete a document; result is null.
    case delete(table: String, id: String)
    /// Precondition: the row must be at exactly `version`.
    case expectVersion(table: String, id: String, version: Int64)
    /// Precondition: no row may match the index eq-prefix.
    case expectAbsent(table: String, index: String, eq: [JSONValue])
    /// Insert-or-patch keyed by an index eq-prefix match.
    case upsert(
        table: String, index: String, eq: [JSONValue],
        insert: [String: JSONValue], patch: [String: JSONValue]
    )
    /// Patch every row matching `filter` (at most `limit`, default server cap).
    case patchByQuery(
        table: String, filter: FilterExpr, patch: [String: JSONValue], limit: UInt32?
    )
    /// Delete every row matching `filter` (same `limit` semantics).
    case deleteByQuery(table: String, filter: FilterExpr, limit: UInt32?)
    /// Schedule `txn` to run later.
    case schedule(when: ScheduleWhen, txn: Transaction)
    /// Cancel a previously scheduled job.
    case cancelSchedule(id: String)
    /// Start a durable workflow run.
    case startWorkflow(spec: WorkflowSpec)
    /// Cancel a workflow run.
    case cancelWorkflow(id: String)
    /// Restore a soft-deleted row (only legal on a `softDelete` table).
    case undelete(table: String, id: String)

    enum CodingKeys: String, CodingKey, CaseIterable {
        case op, table, doc, id, fields, version, index, eq, insert, patch
        case filter, limit, when, txn, spec
    }

    // swiftlint:disable:next cyclomatic_complexity function_body_length
    public init(from decoder: Decoder) throws {
        let payload = try taggedEnumPayload("Step", tagKey: "op", from: decoder)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        switch payload.tag {
        case "insert":
            try rejectUnknownVariantFields(
                "Step", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "table", "doc"]
            )
            self = try .insert(
                table: container.decode(String.self, forKey: .table),
                doc: container.decode([String: JSONValue].self, forKey: .doc)
            )
        case "patch":
            try rejectUnknownVariantFields(
                "Step", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "table", "id", "fields"]
            )
            self = try .patch(
                table: container.decode(String.self, forKey: .table),
                id: container.decode(String.self, forKey: .id),
                fields: container.decode([String: JSONValue].self, forKey: .fields)
            )
        case "replace":
            try rejectUnknownVariantFields(
                "Step", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "table", "id", "doc"]
            )
            self = try .replace(
                table: container.decode(String.self, forKey: .table),
                id: container.decode(String.self, forKey: .id),
                doc: container.decode([String: JSONValue].self, forKey: .doc)
            )
        case "delete":
            try rejectUnknownVariantFields(
                "Step", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "table", "id"]
            )
            self = try .delete(
                table: container.decode(String.self, forKey: .table),
                id: container.decode(String.self, forKey: .id)
            )
        case "expectVersion":
            try rejectUnknownVariantFields(
                "Step", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "table", "id", "version"]
            )
            self = try .expectVersion(
                table: container.decode(String.self, forKey: .table),
                id: container.decode(String.self, forKey: .id),
                version: container.decode(Int64.self, forKey: .version)
            )
        case "expectAbsent":
            try rejectUnknownVariantFields(
                "Step", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "table", "index", "eq"]
            )
            self = try .expectAbsent(
                table: container.decode(String.self, forKey: .table),
                index: container.decode(String.self, forKey: .index),
                eq: container.decode([JSONValue].self, forKey: .eq)
            )
        case "upsert":
            try rejectUnknownVariantFields(
                "Step", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "table", "index", "eq", "insert", "patch"]
            )
            self = try .upsert(
                table: container.decode(String.self, forKey: .table),
                index: container.decode(String.self, forKey: .index),
                eq: container.decode([JSONValue].self, forKey: .eq),
                insert: container.decode([String: JSONValue].self, forKey: .insert),
                patch: container.decode([String: JSONValue].self, forKey: .patch)
            )
        case "patchByQuery":
            try rejectUnknownVariantFields(
                "Step", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "table", "filter", "patch", "limit"]
            )
            self = try .patchByQuery(
                table: container.decode(String.self, forKey: .table),
                filter: container.decode(FilterExpr.self, forKey: .filter),
                patch: container.decode([String: JSONValue].self, forKey: .patch),
                limit: container.decodeIfPresent(UInt32.self, forKey: .limit)
            )
        case "deleteByQuery":
            try rejectUnknownVariantFields(
                "Step", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "table", "filter", "limit"]
            )
            self = try .deleteByQuery(
                table: container.decode(String.self, forKey: .table),
                filter: container.decode(FilterExpr.self, forKey: .filter),
                limit: container.decodeIfPresent(UInt32.self, forKey: .limit)
            )
        case "schedule":
            try rejectUnknownVariantFields(
                "Step", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "when", "txn"]
            )
            self = try .schedule(
                when: container.decode(ScheduleWhen.self, forKey: .when),
                txn: container.decode(Transaction.self, forKey: .txn)
            )
        case "cancelSchedule":
            try rejectUnknownVariantFields(
                "Step", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "id"]
            )
            self = try .cancelSchedule(id: container.decode(String.self, forKey: .id))
        case "startWorkflow":
            try rejectUnknownVariantFields(
                "Step", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "spec"]
            )
            self = try .startWorkflow(spec: container.decode(WorkflowSpec.self, forKey: .spec))
        case "cancelWorkflow":
            try rejectUnknownVariantFields(
                "Step", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "id"]
            )
            self = try .cancelWorkflow(id: container.decode(String.self, forKey: .id))
        case "undelete":
            try rejectUnknownVariantFields(
                "Step", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "table", "id"]
            )
            self = try .undelete(
                table: container.decode(String.self, forKey: .table),
                id: container.decode(String.self, forKey: .id)
            )
        case let unknown:
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: decoder.codingPath,
                    debugDescription: "Step: unknown op '\(unknown)'"
                )
            )
        }
    }

    // swiftlint:disable:next cyclomatic_complexity function_body_length
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case let .insert(table, doc):
            try container.encode("insert", forKey: .op)
            try container.encode(table, forKey: .table)
            try container.encode(doc, forKey: .doc)
        case let .patch(table, id, fields):
            try container.encode("patch", forKey: .op)
            try container.encode(table, forKey: .table)
            try container.encode(id, forKey: .id)
            try container.encode(fields, forKey: .fields)
        case let .replace(table, id, doc):
            try container.encode("replace", forKey: .op)
            try container.encode(table, forKey: .table)
            try container.encode(id, forKey: .id)
            try container.encode(doc, forKey: .doc)
        case let .delete(table, id):
            try container.encode("delete", forKey: .op)
            try container.encode(table, forKey: .table)
            try container.encode(id, forKey: .id)
        case let .expectVersion(table, id, version):
            try container.encode("expectVersion", forKey: .op)
            try container.encode(table, forKey: .table)
            try container.encode(id, forKey: .id)
            try container.encode(version, forKey: .version)
        case let .expectAbsent(table, index, eq):
            try container.encode("expectAbsent", forKey: .op)
            try container.encode(table, forKey: .table)
            try container.encode(index, forKey: .index)
            try container.encode(eq, forKey: .eq)
        case let .upsert(table, index, eq, insert, patch):
            try container.encode("upsert", forKey: .op)
            try container.encode(table, forKey: .table)
            try container.encode(index, forKey: .index)
            try container.encode(eq, forKey: .eq)
            try container.encode(insert, forKey: .insert)
            try container.encode(patch, forKey: .patch)
        case let .patchByQuery(table, filter, patch, limit):
            try container.encode("patchByQuery", forKey: .op)
            try container.encode(table, forKey: .table)
            try container.encode(filter, forKey: .filter)
            try container.encode(patch, forKey: .patch)
            try container.encodeIfPresent(limit, forKey: .limit)
        case let .deleteByQuery(table, filter, limit):
            try container.encode("deleteByQuery", forKey: .op)
            try container.encode(table, forKey: .table)
            try container.encode(filter, forKey: .filter)
            try container.encodeIfPresent(limit, forKey: .limit)
        case let .schedule(when, txn):
            try container.encode("schedule", forKey: .op)
            try container.encode(when, forKey: .when)
            try container.encode(txn, forKey: .txn)
        case let .cancelSchedule(id):
            try container.encode("cancelSchedule", forKey: .op)
            try container.encode(id, forKey: .id)
        case let .startWorkflow(spec):
            try container.encode("startWorkflow", forKey: .op)
            try container.encode(spec, forKey: .spec)
        case let .cancelWorkflow(id):
            try container.encode("cancelWorkflow", forKey: .op)
            try container.encode(id, forKey: .id)
        case let .undelete(table, id):
            try container.encode("undelete", forKey: .op)
            try container.encode(table, forKey: .table)
            try container.encode(id, forKey: .id)
        }
    }
}

/// Mirrors rust-client/src/mutation.rs::StepResult — UNTAGGED: one entry of
/// `mutateOk.results`, decoded by shape. Variant order is load-bearing:
/// `upsert` must precede `insert` because a variant matches when all ITS
/// fields decode (extra keys ignored, serde untagged parity) — with `insert`
/// first, `{"id","inserted"}` would be greedily captured by `insert`,
/// silently dropping `inserted`. `null` is the result of
/// patch/delete/expect*/undelete steps.
public enum StepResult: Equatable, Codable, Sendable {
    /// `{"id", "inserted"}` from an upsert step.
    case upsert(id: String, inserted: Bool)
    /// `{"id"}` from an insert step.
    case insert(id: String)
    /// `{"patched", "truncated"}` from a patchByQuery step.
    case patchByQuery(patched: UInt32, truncated: Bool)
    /// `{"deleted", "truncated"}` from a deleteByQuery step.
    case deleteByQuery(deleted: UInt32, truncated: Bool)
    /// `{"scheduleId"}` from a schedule step.
    case schedule(scheduleId: String)
    /// `{"cancelled"}` from a cancelSchedule/cancelWorkflow step.
    case cancelled(cancelled: Bool)
    /// `{"workflowId"}` from a startWorkflow step.
    case workflowId(workflowId: String)
    /// `null` — patch/delete/expect*/undelete results.
    case null

    public init(from decoder: Decoder) throws {
        let value = try JSONValue(from: decoder)
        guard case let .object(fields) = value else {
            if case .null = value {
                self = .null
                return
            }
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: decoder.codingPath,
                    debugDescription: "StepResult: no variant matches \(value)"
                )
            )
        }
        // Each shape is tried in rust's declaration order; first match wins.
        // A variant fails when a required field is missing or wrong-typed,
        // exactly like serde's untagged fall-through.
        if let id = fields["id"]?.stringValue, case let .bool(inserted) = fields["inserted"] {
            self = .upsert(id: id, inserted: inserted)
        } else if let id = fields["id"]?.stringValue {
            self = .insert(id: id)
        } else if let patched = fields["patched"]?.uint32Value, case let .bool(truncated) = fields["truncated"] {
            self = .patchByQuery(patched: patched, truncated: truncated)
        } else if let deleted = fields["deleted"]?.uint32Value, case let .bool(truncated) = fields["truncated"] {
            self = .deleteByQuery(deleted: deleted, truncated: truncated)
        } else if let scheduleId = fields["scheduleId"]?.stringValue {
            self = .schedule(scheduleId: scheduleId)
        } else if case let .bool(cancelled) = fields["cancelled"] {
            self = .cancelled(cancelled: cancelled)
        } else if let workflowId = fields["workflowId"]?.stringValue {
            self = .workflowId(workflowId: workflowId)
        } else {
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: decoder.codingPath,
                    debugDescription: "StepResult: no variant matches \(value)"
                )
            )
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.singleValueContainer()
        switch self {
        case let .upsert(id, inserted):
            try container.encode(JSONValue.object(["id": .string(id), "inserted": .bool(inserted)]))
        case let .insert(id):
            try container.encode(JSONValue.object(["id": .string(id)]))
        case let .patchByQuery(patched, truncated):
            try container.encode(
                JSONValue.object([
                    "patched": .int(Int64(patched)), "truncated": .bool(truncated)
                ])
            )
        case let .deleteByQuery(deleted, truncated):
            try container.encode(
                JSONValue.object([
                    "deleted": .int(Int64(deleted)), "truncated": .bool(truncated)
                ])
            )
        case let .schedule(scheduleId):
            try container.encode(JSONValue.object(["scheduleId": .string(scheduleId)]))
        case let .cancelled(cancelled):
            try container.encode(JSONValue.object(["cancelled": .bool(cancelled)]))
        case let .workflowId(workflowId):
            try container.encode(JSONValue.object(["workflowId": .string(workflowId)]))
        case .null:
            try container.encodeNil()
        }
    }
}

extension JSONValue {
    /// UInt32 view of an integral value, nil when negative/overflow/non-integral
    /// (serde `u32` rejects those on the wire). Used by StepResult shape checks.
    var uint32Value: UInt32? {
        if case let .int(int) = self {
            return UInt32(exactly: int)
        }
        return nil
    }
}
