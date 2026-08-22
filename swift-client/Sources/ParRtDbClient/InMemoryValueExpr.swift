import Foundation

// MARK: - Computed fields (ENH-028)

// The `ValueExpr` interpreter + write-path stamping for computed fields — the
// Swift mirror of server `value_expr.rs`'s `eval_value_expr` and `txn.rs`'s
// `stamp_computed` (ts-client store.ts `stampComputed`). Split out of
// InMemoryEngine.swift alongside InMemoryValidate/InMemoryQuery: the engine
// keeps the write-choke-point CALLS (doInsert/doReplace/applyPatch); this file
// carries the grammar evaluation they stamp through.

/// JSON value -> text, mirroring the interpreter's `to_text` (the SQL
/// `doc->>'field'` extraction convention): `nil` means SQL NULL (JSON null) —
/// only `.null` maps to nil. Numbers use their JSON number text form
/// (`jsNumberString`: integral values print without a fraction, the same form
/// JS `String(number)` produces); objects/arrays use COMPACT JSON with keys
/// sorted (serde_json's BTreeMap order — the convention the semantics table
/// pins for all five implementations).
func valueText(_ value: JSONValue) -> String? {
    switch value {
    case .null:
        return nil
    case let .string(string):
        return string
    case let .int(int):
        return String(int)
    case let .double(double):
        return jsNumberString(double)
    case let .bool(bool):
        return bool ? "true" : "false"
    case .array, .object:
        let encoder = JSONEncoder()
        encoder.outputFormatting = .sortedKeys
        guard let data = try? encoder.encode(value) else { return nil }
        return String(data: data, encoding: .utf8)
    }
}

/// JSON value -> Double for the arithmetic nodes, mirroring `to_numeric`:
/// `nil` means SQL NULL (propagation, not an error). Numbers yield their
/// double; strings are trimmed and strictly parsed; bool/object/array are
/// type errors.
func valueNumeric(_ value: JSONValue) throws -> Double? {
    switch value {
    case .null:
        return nil
    case let .int(int):
        return Double(int)
    case let .double(double):
        // A JSON number is finite by construction; the guard keeps a
        // hand-constructed non-finite .double from reaching arithmetic.
        return double.isFinite ? double : nil
    case let .string(string):
        let trimmed = string.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let number = Double(trimmed), number.isFinite else {
            throw RtDbError(code: .badRequest, message: "cannot cast '\(string)' to number")
        }
        return number
    case .bool, .array, .object:
        throw RtDbError(code: .badRequest, message: "cannot cast to number")
    }
}

/// IEEE double -> JSON number; a non-finite result (NaN, ±inf — overflow-shaped
/// arithmetic) is an error, mirroring `finite_number`.
func finiteJSONNumber(_ result: Double) throws -> JSONValue {
    guard result.isFinite else {
        throw RtDbError(code: .badRequest, message: "numeric result is not finite")
    }
    return jsonNumber(result)
}

/// `Cast.toInt64`: a `.double` payload is NOT integral even when mathematically
/// whole (serde_json's `as_i64` only succeeds on integer-backed numbers — the
/// semantics table pins "a float payload like 3.0 is not"); a `.string` is
/// trimmed and strictly parsed. The result is a JSON number — the int64
/// decimal-STRING wire convention applies only to stored int64 fields.
func castToInt64(_ value: JSONValue) throws -> JSONValue {
    switch value {
    case .null:
        return .null
    case let .int(int):
        return .int(int)
    case let .double(double):
        throw RtDbError(
            code: .badRequest, message: "cannot cast \(jsNumberString(double)) to int64"
        )
    case let .string(string):
        let trimmed = string.trimmingCharacters(in: .whitespacesAndNewlines)
        guard let int = parseI64(trimmed) else {
            throw RtDbError(code: .badRequest, message: "cannot cast '\(string)' to int64")
        }
        return .int(int)
    case .bool, .array, .object:
        throw RtDbError(code: .badRequest, message: "cannot cast to int64")
    }
}

// swiftlint:disable cyclomatic_complexity
/// `Cast.toBoolean`: bools pass through; numbers accept exactly `1`/`0`
/// (numeric equality — `.int(1)` and `.double(1.0)` agree); strings match
/// case-insensitively against Postgres's boolean literal set.
func castToBoolean(_ value: JSONValue) throws -> JSONValue {
    let trueWords: Set = ["true", "t", "yes", "on", "1"]
    let falseWords: Set = ["false", "f", "no", "off", "0"]
    switch value {
    case .null:
        return .null
    case let .bool(bool):
        return .bool(bool)
    case let .int(int):
        if int == 1 {
            return .bool(true)
        }
        if int == 0 {
            return .bool(false)
        }
        throw RtDbError(code: .badRequest, message: "cannot cast \(int) to boolean")
    case let .double(double):
        if double == 1.0 {
            return .bool(true)
        }
        if double == 0.0 {
            return .bool(false)
        }
        throw RtDbError(
            code: .badRequest, message: "cannot cast \(jsNumberString(double)) to boolean"
        )
    case let .string(string):
        if trueWords.contains(string.lowercased()) {
            return .bool(true)
        }
        if falseWords.contains(string.lowercased()) {
            return .bool(false)
        }
        throw RtDbError(code: .badRequest, message: "cannot cast '\(string)' to boolean")
    case .array, .object:
        throw RtDbError(code: .badRequest, message: "cannot cast to boolean")
    }
}

// swiftlint:enable cyclomatic_complexity

// swiftlint:disable cyclomatic_complexity function_body_length
/// In-memory `ValueExpr` interpreter — the per-write counterpart of the
/// server's SQL compiler, used by computed-field stamping (server
/// `value_expr::eval_value_expr`). Field reads are TEXT extraction
/// (`doc->>'field'`), arithmetic is IEEE doubles with SQL-NULL propagation
/// BEFORE the div-zero check, `caseExpr` predicates reuse the engine's
/// `evalFilterExpr` matcher (push validation rejects principal markers inside
/// computed exprs, so no principal is ever resolved here).
func evalValueExpr(
    _ ve: ValueExpr, _ doc: [String: JSONValue], _ nowMs: Int64, _ fields: FieldMap
) throws -> JSONValue {
    switch ve {
    case let .field(field):
        if let text = doc[field].flatMap(valueText) {
            return .string(text)
        }
        return .null
    case let .literal(value):
        return value
    case let .concat(parts):
        var out = ""
        for part in parts {
            // valueText is nil exactly for null parts — concat skips them
            // rather than nulling the result; all-null parts yield "".
            if let text = try valueText(evalValueExpr(part, doc, nowMs, fields)) {
                out += text
            }
        }
        return .string(out)
    case let .add(left, right), let .sub(left, right),
         let .mul(left, right), let .div(left, right):
        let lhs = try valueNumeric(evalValueExpr(left, doc, nowMs, fields))
        let rhs = try valueNumeric(evalValueExpr(right, doc, nowMs, fields))
        guard let leftNum = lhs, let rightNum = rhs else {
            // Either operand SQL-NULL -> NULL; propagation precedes the
            // zero-divisor and finiteness checks (null / 0 is null).
            return .null
        }
        if case .div = ve, rightNum == 0.0 {
            throw RtDbError(code: .badRequest, message: "division by zero")
        }
        let result: Double = switch ve {
        case .add: leftNum + rightNum
        case .sub: leftNum - rightNum
        case .mul: leftNum * rightNum
        default: leftNum / rightNum
        }
        return try finiteJSONNumber(result)
    case let .coalesce(parts):
        for part in parts {
            let value = try evalValueExpr(part, doc, nowMs, fields)
            if value != .null {
                return value
            }
        }
        return .null
    case let .lower(value):
        if let text = try valueText(evalValueExpr(value, doc, nowMs, fields)) {
            return .string(text.lowercased())
        }
        return .null
    case let .upper(value):
        if let text = try valueText(evalValueExpr(value, doc, nowMs, fields)) {
            return .string(text.uppercased())
        }
        return .null
    case let .trim(value):
        if let text = try valueText(evalValueExpr(value, doc, nowMs, fields)) {
            // Spaces only — Postgres btrim's default, not Unicode whitespace:
            // a leading tab survives.
            var trimmed = Substring(text)
            while trimmed.first == " " {
                trimmed.removeFirst()
            }
            while trimmed.last == " " {
                trimmed.removeLast()
            }
            return .string(String(trimmed))
        }
        return .null
    case let .cast(value, to):
        let inner = try evalValueExpr(value, doc, nowMs, fields)
        switch to {
        case .toString:
            if let text = valueText(inner) {
                return .string(text)
            }
            return .null
        case .toNumber:
            if let number = try valueNumeric(inner) {
                return try finiteJSONNumber(number)
            }
            return .null
        case .toInt64:
            return try castToInt64(inner)
        case .toBoolean:
            return try castToBoolean(inner)
        }
    case .now:
        return .int(nowMs)
    case let .caseExpr(whens, otherwise):
        for cw in whens where evalFilterExpr(cw.when, doc, fields) {
            return try evalValueExpr(cw.then, doc, nowMs, fields)
        }
        return try evalValueExpr(otherwise, doc, nowMs, fields)
    }
}

// swiftlint:enable cyclomatic_complexity function_body_length

/// Visits every field name a `ValueExpr` reads: each `field` node, every
/// `caseExpr` branch's `then`/`otherwise`, and every filter field inside
/// `Case.whens` — the same field set push validation and the migrate rename
/// rewrite operate on (server `value_expr::walk_value_expr_fields`).
func walkValueExprFields(_ ve: ValueExpr, _ visit: (String) -> Void) {
    switch ve {
    case let .field(field):
        visit(field)
    case .literal, .now:
        break
    case let .concat(parts), let .coalesce(parts):
        for part in parts {
            walkValueExprFields(part, visit)
        }
    case let .add(left, right), let .sub(left, right),
         let .mul(left, right), let .div(left, right):
        walkValueExprFields(left, visit)
        walkValueExprFields(right, visit)
    case let .lower(value), let .upper(value), let .trim(value), let .cast(value, _):
        walkValueExprFields(value, visit)
    case let .caseExpr(whens, otherwise):
        for cw in whens {
            walkFilterExprFieldNames(cw.when, visit)
            walkValueExprFields(cw.then, visit)
        }
        walkValueExprFields(otherwise, visit)
    }
}

/// The `FilterExpr` half of the walk: `and`/`or`/`not` recurse; every leaf
/// variant carries a field name (server `walk_filter_expr_fields`).
func walkFilterExprFieldNames(_ expr: FilterExpr, _ visit: (String) -> Void) {
    switch expr {
    case let .eq(field, _), let .neq(field, _), let .gt(field, _),
         let .gte(field, _), let .lt(field, _), let .lte(field, _),
         let .inValues(field, _), let .contains(field, _), let .exists(field),
         let .olderThan(field, _):
        visit(field)
    case let .and(exprs), let .or(exprs):
        for subExpr in exprs {
            walkFilterExprFieldNames(subExpr, visit)
        }
    case let .not(expr):
        walkFilterExprFieldNames(expr, visit)
    }
}

/// Stamps the table's computed fields — a port of server `txn::stamp_computed`
/// (store.ts `stampComputed`): every `computed` entry is re-evaluated against
/// the final doc and stored — a null result REMOVES the key (an unset optional
/// field is an absent key, `stripUnsetOptionals`' shape convention) and a
/// non-null result overwrites whatever is there (the ownerField authority
/// model: client-supplied values never survive). An evaluation error fails the
/// whole write as BAD_REQUEST, naming the field. Runs last in the stamp chain
/// — after the ttl default, updatedAt, defaults, and autoIncrement stamps, so
/// expressions see final inputs — and before `validateDoc` at every site:
/// `applyPatch` (patch, upsert's update branch, patchByQuery, cascade
/// setNull), `doInsert` (insert + upsert's insert branch), and `doReplace`.
func stampComputed(
    _ table: TableDef, _ doc: [String: JSONValue], _ now: Int64
) throws -> [String: JSONValue] {
    guard !table.computed.isEmpty else {
        return doc
    }
    var out = doc
    for (name, expr) in table.computed {
        let value: JSONValue
        do {
            value = try evalValueExpr(expr, out, now, table.fields)
        } catch let error as RtDbError {
            throw RtDbError(
                code: .badRequest, message: "computed field '\(name)': \(error.message)"
            )
        }
        if value == .null {
            out.removeValue(forKey: name)
        } else {
            out[name] = value
        }
    }
    return out
}
