import Foundation

struct CounterAdjustment: Sendable {
    private static let safeInteger = Int64(9_007_199_254_740_991)

    let field: String
    private let delta: Int64
    private let minimum: Int64?
    private let maximum: Int64?
    private let expected: [String: JSONValue]?

    init(
        field: String, delta: Int64, min: Int64?, max: Int64?,
        expected: [String: JSONValue]?
    ) {
        self.field = field
        self.delta = delta
        minimum = min
        maximum = max
        self.expected = expected
    }

    func validate(table: TableDef) throws {
        try validateBounds()
        try validateField(table: table)
        try validateExpectedFields(table: table)
    }

    func value(in document: [String: JSONValue]) throws -> JSONValue {
        try validateExpectedValues(in: document)
        let current = try currentValue(in: document)
        let (next, overflow) = current.addingReportingOverflow(delta)
        guard !overflow, next >= -Self.safeInteger, next <= Self.safeInteger else {
            throw RtDbError(code: .badRequest, message: "adjustCounter result must be a safe integer")
        }
        try validateResult(next)
        return .int(next)
    }

    private func validateBounds() throws {
        guard delta >= -Self.safeInteger, delta <= Self.safeInteger,
              minimum.map({ $0 >= -Self.safeInteger && $0 <= Self.safeInteger }) ?? true,
              maximum.map({ $0 >= -Self.safeInteger && $0 <= Self.safeInteger }) ?? true,
              minimum.map({ low in maximum.map { low <= $0 } ?? true }) ?? true
        else {
            throw RtDbError(
                code: .badRequest,
                message: "adjustCounter numeric arguments must be safe integers"
            )
        }
    }

    private func validateField(table: TableDef) throws {
        guard let type = table.fields[field] else {
            throw RtDbError(code: .schemaViolation, message: "unknown field '\(field)'")
        }
        guard !isServerManaged(table: table) else {
            throw RtDbError(code: .badRequest, message: "field '\(field)' is server-managed")
        }
        guard acceptsNumbers(type) else {
            throw RtDbError(
                code: .schemaViolation,
                message: "field '\(field)' must be number or optional(number)"
            )
        }
    }

    private func isServerManaged(table: TableDef) -> Bool {
        table.computed[field] != nil
            || table.autoIncrementField == field
            || table.updatedAtField == field
    }

    private func acceptsNumbers(_ type: FieldType) -> Bool {
        switch type {
        case .number: true
        case let .optional(inner): if case .number = inner {
                true
            } else {
                false
            }
        default: false
        }
    }

    private func validateExpectedFields(table: TableDef) throws {
        guard let expected else { return }
        if let unknown = expected.keys.filter({ table.fields[$0] == nil }).sorted().first {
            throw RtDbError(code: .schemaViolation, message: "unknown expected field '\(unknown)'")
        }
    }

    private func validateExpectedValues(in document: [String: JSONValue]) throws {
        guard let expected else { return }
        for (key, value) in expected {
            guard let stored = document[key], counterExpectedJSONEqual(stored, value) else {
                throw RtDbError(code: .preconditionFailed, message: "expected field values do not match")
            }
        }
    }

    private func currentValue(in document: [String: JSONValue]) throws -> Int64 {
        switch document[field] {
        case let .int(value)
            where value >= -Self.safeInteger && value <= Self.safeInteger:
            value
        case let .double(value)
            where value.isFinite && value.rounded(.towardZero) == value
            && abs(value) <= Double(Self.safeInteger):
            Int64(value)
        default:
            throw RtDbError(
                code: .badRequest,
                message: "field '\(field)' must contain a safe integer"
            )
        }
    }

    private func validateResult(_ value: Int64) throws {
        guard minimum.map({ value >= $0 }) ?? true,
              maximum.map({ value <= $0 }) ?? true
        else {
            throw RtDbError(
                code: .preconditionFailed,
                message: "adjustCounter result is outside the allowed bounds"
            )
        }
    }
}

private func counterExpectedJSONEqual(_ lhs: JSONValue, _ rhs: JSONValue) -> Bool {
    switch (lhs, rhs) {
    case let (.int(integer), .double(number)):
        exactInteger(number) == integer
    case let (.double(number), .int(integer)):
        exactInteger(number) == integer
    case let (.array(left), .array(right)):
        left.count == right.count && zip(left, right).allSatisfy(counterExpectedJSONEqual)
    case let (.object(left), .object(right)):
        left.count == right.count && left.allSatisfy { key, value in
            guard let other = right[key] else { return false }
            return counterExpectedJSONEqual(value, other)
        }
    default:
        lhs == rhs
    }
}

private func exactInteger(_ value: Double) -> Int64? {
    guard value.isFinite,
          value.rounded(.towardZero) == value,
          value >= Double(Int64.min),
          value < Double(Int64.max)
    else {
        return nil
    }
    return Int64(value)
}
