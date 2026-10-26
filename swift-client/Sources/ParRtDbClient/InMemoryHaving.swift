import Foundation

// MARK: - Aggregate having + group-cap marker

// The in-memory mirror of the server's aggregate `HAVING` and `LIMIT MAX_TAKE + 1`
// truncation detection (server/src/query/terminals.rs `build_having`): failing
// groups are dropped BEFORE the cap, and a capped result is wrapped as
// `{groups, truncated: true}` (an under-cap result stays the bare array).

extension HavingCmp {
    var hasBound: Bool {
        [eq, ne, gt, gte, lt, lte].contains { $0 != nil }
    }

    func matches(_ number: Double) -> Bool {
        (eq.map { number == $0 } ?? true) && (ne.map { number != $0 } ?? true)
            && (gt.map { number > $0 } ?? true) && (gte.map { number >= $0 } ?? true)
            && (lt.map { number < $0 } ?? true) && (lte.map { number <= $0 } ?? true)
    }
}

/// BAD_REQUEST gates for `having`: needs a grouped spec, a non-empty map,
/// known names, at least one bound each, and a numeric aggregate for the
/// field-bearing ops.
func validateHaving(
    _ having: [String: HavingCmp]?, ops: [(alias: String, op: AggregateOp)],
    grouped: Bool, numericAgg: Bool
) throws {
    guard let having else { return }
    if !grouped {
        throw RtDbError(code: .badRequest, message: "aggregate having requires groupBy")
    }
    if having.isEmpty {
        throw RtDbError(code: .badRequest, message: "aggregate having must not be empty")
    }
    for (name, cmp) in having {
        guard let entry = ops.first(where: { $0.alias == name }) else {
            throw RtDbError(
                code: .badRequest, message: "aggregate having references unknown aggregate '\(name)'"
            )
        }
        if !cmp.hasBound {
            throw RtDbError(
                code: .badRequest, message: "aggregate having '\(name)' needs at least one bound"
            )
        }
        if entry.op != .count, !numericAgg {
            throw RtDbError(
                code: .badRequest, message: "aggregate having '\(name)' requires a numeric aggregate"
            )
        }
    }
}

private func havingKeeps(_ row: JSONValue, _ having: [String: HavingCmp]) -> Bool {
    guard case let .object(fields) = row else { return false }
    for (name, cmp) in having {
        let cell: JSONValue? = if case let .object(values)? = fields["values"] {
            values[name]
        } else {
            fields["value"]
        }
        let number: Double? = switch cell {
        case let .string(text)?: Double(text)
        default: cell?.doubleValue
        }
        guard let number, cmp.matches(number) else { return false }
    }
    return true
}

/// Applies `having`, then the `maxQueryTake` cap; a capped result is
/// `{groups, truncated: true}`.
func finishGroups(_ rows: [JSONValue], having: [String: HavingCmp]?) -> JSONValue {
    let kept = having.map { filter in rows.filter { havingKeeps($0, filter) } } ?? rows
    if kept.count > maxQueryTake {
        return .object(["groups": .array(Array(kept.prefix(maxQueryTake))), "truncated": .bool(true)])
    }
    return .array(kept)
}
