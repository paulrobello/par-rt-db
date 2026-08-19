import Foundation

// MARK: - Value predicates

/// ASCII scalar digit check (0-9) — the regex `[0-9]` in validate.ts's
/// predicates, as a scalar compare.
func isASCIIDigit(_ character: Character) -> Bool {
    guard let ascii = character.asciiValue else { return false }
    return (48 ... 57).contains(ascii)
}

/// ASCII lowercase hex digit check (0-9a-f) — validate.ts `isHexId`'s class.
func isASCIILowerHex(_ character: Character) -> Bool {
    guard let ascii = character.asciiValue else { return false }
    return (48 ... 57).contains(ascii) || (97 ... 102).contains(ascii)
}

/// ASCII alphanumeric check — the `[A-Za-z0-9]` class of the FTS tokenizer
/// and the base64 alphabet.
func isASCIIAlphaNumeric(_ character: Character) -> Bool {
    guard let ascii = character.asciiValue else { return false }
    return (48 ... 57).contains(ascii) || (65 ... 90).contains(ascii) || (97 ... 122).contains(ascii)
}

// Mirrors ts-client/src/in_memory/validate.ts — value/filter validation for
// the in-memory engine: structural validation + evaluation of the query DSL's
// `FilterExpr` (server `query::compile_filter_node` / `field_lhs_and_bind` /
// `jsonb_lhs_and_bind`, including the SEC-126 value-kind checks), and the
// eq-bind typing of index values (`indexColumnType` / `coerceIndexValue`,
// server `eq_bind_for`).

/// Mirrors validate.ts `isHexId` — a 32-char lowercase hex document id.
public func isHexId(_ value: JSONValue) -> Bool {
    guard case let .string(string) = value, string.count == 32 else { return false }
    return string.allSatisfy(isASCIILowerHex)
}

/// Mirrors validate.ts `isInt64String` — a strict `-?digits` decimal string
/// within the i64 range (the canonical int64 wire form; a leading `+` is
/// rejected exactly like Rust's `i64::from_str`).
public func isInt64String(_ value: JSONValue) -> Bool {
    guard case let .string(string) = value else { return false }
    var digits = Substring(string)
    if digits.hasPrefix("-") {
        digits = digits.dropFirst()
    }
    guard !digits.isEmpty, digits.allSatisfy(isASCIIDigit) else { return false }
    return Int64(string) != nil
}

/// Mirrors validate.ts `isBase64String` — base64 alphabet with 0-2 trailing
/// `=` and a length that is a multiple of 4.
public func isBase64String(_ value: JSONValue) -> Bool {
    guard case let .string(string) = value, string.count % 4 == 0 else { return false }
    var padding = 0
    for character in string.reversed() {
        if character == "=" && padding < 2 {
            padding += 1
            continue
        }
        if padding > 0 {
            return false
        } // '=' is only trailing
        guard isASCIIAlphaNumeric(character) || character == "+" || character == "/" else {
            return false
        }
    }
    return true
}

/// Mirrors validate.ts `FieldMap` — a table's declared field map, keyed by
/// field name. Pass an empty map for type-less filter evaluation.
public typealias FieldMap = [String: FieldType]

// MARK: - Indexable column typing

/// Indexed-column storage type, mirroring server `indexed_column_type` and
/// validate.ts `PgType`.
public enum PgType: Equatable, Sendable {
    case text
    case number
    case boolean
    case int64
}

/// Mirrors validate.ts `IndexedType` — the storage kind plus nullability.
public struct IndexedType: Equatable, Sendable {
    public let pg: PgType
    public let nullable: Bool
}

// swiftlint:disable cyclomatic_complexity
/// The wire tag of a field type — used only in "not indexable" error messages.
func fieldTypeTag(_ ty: FieldType) -> String {
    switch ty {
    case .string: "string"
    case .number: "number"
    case .boolean: "boolean"
    case .null: "null"
    case .id: "id"
    case .literal: "literal"
    case .optional: "optional"
    case .union: "union"
    case .array: "array"
    case .object: "object"
    case .int64: "int64"
    case .bytes: "bytes"
    case .any: "any"
    case .record: "record"
    case .vector: "vector"
    }
}

// swiftlint:enable cyclomatic_complexity

// swiftlint:disable cyclomatic_complexity
/// Indexable column type — a port of server `schema::indexed_column_type`
/// (validate.ts `indexColumnType`). SCHEMA_VIOLATION for a non-indexable type.
public func indexColumnType(_ ty: FieldType) throws -> IndexedType {
    switch ty {
    case .string, .id:
        return IndexedType(pg: .text, nullable: false)
    case .number:
        return IndexedType(pg: .number, nullable: false)
    case .int64:
        return IndexedType(pg: .int64, nullable: false)
    case .boolean:
        return IndexedType(pg: .boolean, nullable: false)
    case let .literal(value):
        if case .string = value {
            return IndexedType(pg: .text, nullable: false)
        }
        throw RtDbError(
            code: .schemaViolation, message: "field type '\(fieldTypeTag(ty))' is not indexable"
        )
    case let .union(variants):
        let allStringLiterals = variants.allSatisfy { variant in
            if case let .literal(value) = variant, case .string = value {
                return true
            }
            return false
        }
        if allStringLiterals {
            return IndexedType(pg: .text, nullable: false)
        }
        throw RtDbError(
            code: .schemaViolation, message: "field type '\(fieldTypeTag(ty))' is not indexable"
        )
    case let .optional(inner):
        let resolved = try indexColumnType(inner)
        return IndexedType(pg: resolved.pg, nullable: true)
    default:
        throw RtDbError(
            code: .schemaViolation, message: "field type '\(fieldTypeTag(ty))' is not indexable"
        )
    }
}

// swiftlint:enable cyclomatic_complexity

/// JS `typeof value === "number"` — the engine treats `.int` and `.double` as
/// one number domain (JS has a single number type; the split exists only in
/// Swift's JSONValue).
func isJSONNumber(_ value: JSONValue) -> Bool {
    if case .int = value {
        return true
    }
    if case .double = value {
        return true
    }
    return false
}

// swiftlint:disable cyclomatic_complexity
/// JS `===` on JSON values: numbers compare by value across the `.int` /
/// `.double` split (5 === 5.0 in JS), everything else by case + payload.
func jsonEq(_ lhs: JSONValue, _ rhs: JSONValue) -> Bool {
    switch (lhs, rhs) {
    case let (.int(first), .int(second)): first == second
    case let (.int(first), .double(second)): Double(first) == second
    case let (.double(first), .int(second)): first == Double(second)
    case let (.double(first), .double(second)): first == second
    case let (.string(first), .string(second)): first == second
    case let (.bool(first), .bool(second)): first == second
    case (.null, .null): true
    case let (.array(first), .array(second)):
        first.count == second.count && zip(first, second).allSatisfy(jsonEq)
    case let (.object(first), .object(second)):
        first.count == second.count && first.allSatisfy { key, value in
            if let other = second[key] {
                return jsonEq(value, other)
            }
            return false
        }
    default: false
    }
}

// swiftlint:enable cyclomatic_complexity

/// Type-checks an eq/range bind value, mirroring server `eq_bind_for`
/// (validate.ts `coerceIndexValue`). The value passes through unchanged; only
/// its kind is validated against the field's indexed storage type.
public func coerceIndexValue(
    _ table: TableDef, _ fieldName: String, _ value: JSONValue
) throws -> JSONValue {
    guard let fieldTy = table.fields[fieldName] else {
        throw RtDbError(code: .internal, message: "index references unknown field '\(fieldName)'")
    }
    let pg = try indexColumnType(fieldTy).pg
    switch pg {
    case .text:
        guard case .string = value else {
            throw RtDbError(code: .badRequest, message: "eq value must be a string")
        }
        return value
    case .number:
        guard isJSONNumber(value) else {
            throw RtDbError(code: .badRequest, message: "eq value must be a number")
        }
        return value
    case .int64:
        // Canonical decimal string, validated exactly as on insert
        // (`isInt64String` mirrors the server's `i64::from_str`).
        guard isInt64String(value) else {
            throw RtDbError(code: .badRequest, message: "eq value must be an int64 string")
        }
        return value
    case .boolean:
        guard case .bool = value else {
            throw RtDbError(code: .badRequest, message: "eq value must be a boolean")
        }
        return value
    }
}

// MARK: - Filter validation

// swiftlint:disable cyclomatic_complexity
/// Structural validation of a `FilterExpr` against a table's declared fields,
/// mirroring server `query::compile_filter_node` / `field_lhs_and_bind`
/// (validate.ts `validateFilter`). Throws BAD_REQUEST for an unknown field, an
/// empty `and`/`or`, an empty `in`, a non-scalar leaf value, or — SEC-126 — a
/// value whose JSON kind does not match the field's declared type. Call once
/// before evaluating per row.
public func validateFilter(_ node: FilterExpr, _ table: TableDef) throws {
    switch node {
    case let .and(exprs), let .or(exprs):
        if exprs.isEmpty {
            let op = if case .and = node {
                "and"
            } else {
                "or"
            }
            throw RtDbError(code: .badRequest, message: "\(op) filter requires at least one expr")
        }
        for expr in exprs {
            try validateFilter(expr, table)
        }
    case let .inValues(field, values):
        if values.isEmpty {
            throw RtDbError(code: .badRequest, message: "in filter requires at least one value")
        }
        for value in values {
            try checkLeafValue(field, value, table)
        }
        guard let first = values.first else { break }
        let firstKind = inValueKind(first)
        for value in values.dropFirst() where inValueKind(value) != firstKind {
            throw RtDbError(
                code: .badRequest, message: "in filter values must all be the same type"
            )
        }
    case let .not(expr):
        try validateFilter(expr, table)
    case let .contains(field, value):
        try checkLeafValue(field, value, table)
    case let .exists(field):
        guard table.fields[field] != nil else {
            throw RtDbError(
                code: .badRequest, message: "filter references unknown field '\(field)'"
            )
        }
    case let .eq(field, value), let .neq(field, value), let .gt(field, value),
         let .gte(field, value), let .lt(field, value), let .lte(field, value):
        try checkLeafValue(field, value, table)
    }
}

// swiftlint:enable cyclomatic_complexity

private func checkLeafValue(_ field: String, _ value: JSONValue, _ table: TableDef) throws {
    guard table.fields[field] != nil else {
        throw RtDbError(code: .badRequest, message: "filter references unknown field '\(field)'")
    }
    switch value {
    case .string, .bool: break
    case .int, .double: break
    default:
        throw RtDbError(
            code: .badRequest, message: "filter value must be a string, number, or boolean"
        )
    }
    // SEC-126: reject a value whose JSON kind contradicts the declared field
    // type BEFORE evaluation (server `field_lhs_and_bind`). Indexed fields type
    // the value through the same eq-bind conversion as `query.eq` binds; other
    // declared fields get the jsonb kind check.
    let indexed = (table.indexes ?? []).contains { $0.fields.contains(field) }
    if indexed {
        _ = try coerceIndexValue(table, field, value)
    } else if let fieldTy = table.fields[field] {
        try validateJsonbComparisonValue(field, fieldTy, value)
    }
}

/// Mirrors server `validate_jsonb_comparison_value` (SEC-126): passes when
/// `value`'s JSON kind can be ordered against a declared-but-not-indexed field
/// of type `ty`; the `optional` wrapper is unwrapped first. The deliberate
/// asymmetry with the indexed path holds: a non-indexed int64 field takes a
/// JSON NUMBER (the jsonb float8 comparison) and rejects the decimal string
/// the typed bigint column binds.
private func validateJsonbComparisonValue(
    _ field: String, _ ty: FieldType, _ value: JSONValue
) throws {
    let inner: FieldType = if case let .optional(wrapped) = ty {
        wrapped
    } else {
        ty
    }
    let ok: Bool = switch inner {
    case .string, .id, .bytes:
        if case .string = value {
            true
        } else {
            false
        }
    case .number, .int64:
        isJSONNumber(value)
    case .boolean:
        if case .bool = value {
            true
        } else {
            false
        }
    default:
        // Any / Literal / Union / Array / Object / Record / Vector / Null:
        // no reliable static check; accept any scalar.
        switch value {
        case .string, .bool, .int, .double: true
        default: false
        }
    }
    if !ok {
        throw RtDbError(
            code: .badRequest,
            message: "filter on field '\(field)' value kind does not match declared field type"
        )
    }
}

private func inValueKind(_ value: JSONValue) -> String {
    switch value {
    case .string: "string"
    case .int, .double: "number"
    case .bool: "boolean"
    default: "other"
    }
}

// MARK: - Filter evaluation

// swiftlint:disable cyclomatic_complexity
/// Evaluate a `FilterExpr` predicate against a stored doc, mirroring server
/// `query::jsonb_lhs_and_bind` (validate.ts `evalFilterExpr`): the filter
/// value's kind picks the comparison domain — string compares the doc field's
/// `->>` text, number as `float8`, boolean as `boolean` — EXCEPT on a declared
/// `int64` field, where a string value compares numerically so decimal strings
/// order `-605 < -1 < 15`, not lexicographically (ENH-027). A null/absent
/// field never matches (SQL NULL exclusion). Assumes `validateFilter` passed.
public func evalFilterExpr(
    _ node: FilterExpr, _ doc: [String: JSONValue], _ fields: FieldMap
) -> Bool {
    switch node {
    case let .and(exprs):
        return exprs.allSatisfy { evalFilterExpr($0, doc, fields) }
    case let .or(exprs):
        return exprs.contains { evalFilterExpr($0, doc, fields) }
    case let .inValues(field, values):
        return values.contains { compareLeaf(.eq, field, $0, doc, fields) }
    case let .not(expr):
        return !evalFilterExpr(expr, doc, fields)
    case let .contains(field, value):
        guard case let .array(array) = doc[field] else { return false }
        return array.contains { jsonEq($0, value) }
    case let .exists(field):
        guard let value = doc[field] else { return false }
        return value != .null
    case let .eq(field, value):
        return compareLeaf(.eq, field, value, doc, fields)
    case let .neq(field, value):
        return compareLeaf(.neq, field, value, doc, fields)
    case let .gt(field, value):
        return compareLeaf(.gt, field, value, doc, fields)
    case let .gte(field, value):
        return compareLeaf(.gte, field, value, doc, fields)
    case let .lt(field, value):
        return compareLeaf(.lt, field, value, doc, fields)
    case let .lte(field, value):
        return compareLeaf(.lte, field, value, doc, fields)
    }
}

// swiftlint:enable cyclomatic_complexity

/// The six leaf comparison operators (validate.ts `FilterLeafOp`).
enum FilterLeafOp {
    case eq, neq, gt, gte, lt, lte
}

private func compareLeaf(
    _ op: FilterLeafOp,
    _ field: String,
    _ filterValue: JSONValue,
    _ doc: [String: JSONValue],
    _ fields: FieldMap
) -> Bool {
    guard let docVal = doc[field], docVal != .null else {
        return false
    }
    if case let .string(stringValue) = filterValue, isInt64Field(fields[field]) {
        // The server binds a string filter value on an int64 field as a typed
        // bigint, so any legal comparison is numeric. Parse both sides exactly
        // as i64; an unparseable value never matches.
        guard case let .string(docString) = docVal, let lhs = parseI64(docString) else {
            return false
        }
        guard let rhs = parseI64(stringValue) else { return false }
        return compareValues(op, lhs, rhs)
    }
    if case let .string(stringValue) = filterValue {
        return compareValues(op, docToText(docVal), stringValue)
    }
    if isJSONNumber(filterValue) {
        guard let lhs = docToNumber(docVal) else { return false }
        guard let rhs = filterValue.doubleValue else { return false }
        return compareValues(op, lhs, rhs)
    }
    if case let .bool(docBool) = docVal, case let .bool(filterBool) = filterValue {
        return compareValues(op, docBool, filterBool)
    }
    return false
}

/// Whether a declared field type is `int64` (an `optional<int64>` unwraps to
/// it — mirrors the server's `eq_bind_for` Optional unwrap).
private func isInt64Field(_ ty: FieldType?) -> Bool {
    guard let ty else { return false }
    if case .int64 = ty {
        return true
    }
    if case let .optional(inner) = ty, case .int64 = inner {
        return true
    }
    return false
}

/// Exact `i64::from_str` mirror: an optional `+`/`-` sign then one or more
/// ASCII digits, within the i64 range (validate.ts `parseI64`).
func parseI64(_ text: String) -> Int64? {
    var digits = Substring(text)
    if digits.hasPrefix("-") || digits.hasPrefix("+") {
        digits = digits.dropFirst()
    }
    guard !digits.isEmpty, digits.allSatisfy(isASCIIDigit) else { return nil }
    return Int64(text)
}

/// Mirrors Postgres `doc->>'field'`: the JSON text of the value
/// (validate.ts `docToText`).
func docToText(_ docVal: JSONValue) -> String {
    switch docVal {
    case let .string(string): return string
    case let .int(int): return String(int)
    case let .double(double): return jsNumberString(double)
    case let .bool(bool): return bool ? "true" : "false"
    case .null: return "null"
    case .array, .object:
        let data = (try? JSONEncoder().encode(docVal)) ?? Data()
        return String(data: data, encoding: .utf8) ?? "null"
    }
}

/// Mirrors Postgres `(doc->>'field')::float8`: a number, or a numeric string
/// (validate.ts `docToNumber`). A trimmed empty string is not numeric.
func docToNumber(_ docVal: JSONValue) -> Double? {
    switch docVal {
    case let .int(int): return Double(int)
    case let .double(double): return double.isFinite ? double : nil
    case let .string(string):
        let trimmed = string.trimmingCharacters(in: .whitespaces)
        guard !trimmed.isEmpty, let number = Double(trimmed) else { return nil }
        return number.isFinite ? number : nil
    default: return nil
    }
}

/// JS number-to-string: integral doubles print without a fraction (`5.0` is
/// `"5"`, matching `String(5)` in JS and `JSON.stringify`).
func jsNumberString(_ double: Double) -> String {
    if double.isFinite, double == double.rounded(), abs(double) < 1e15, let int = Int64(exactly: double) {
        return String(int)
    }
    return String(double)
}

private func compareValues(_ op: FilterLeafOp, _ lhs: String, _ rhs: String) -> Bool {
    switch op {
    case .eq: lhs == rhs
    case .neq: lhs != rhs
    case .gt: lhs > rhs
    case .gte: lhs >= rhs
    case .lt: lhs < rhs
    case .lte: lhs <= rhs
    }
}

private func compareValues(_ op: FilterLeafOp, _ lhs: Double, _ rhs: Double) -> Bool {
    switch op {
    case .eq: lhs == rhs
    case .neq: lhs != rhs
    case .gt: lhs > rhs
    case .gte: lhs >= rhs
    case .lt: lhs < rhs
    case .lte: lhs <= rhs
    }
}

private func compareValues(_ op: FilterLeafOp, _ lhs: Bool, _ rhs: Bool) -> Bool {
    switch op {
    case .eq: lhs == rhs
    case .neq: lhs != rhs
    case .gt: lhs == true && rhs == false
    case .gte: !(lhs == false && rhs == true)
    case .lt: lhs == false && rhs == true
    case .lte: !(lhs == true && rhs == false)
    }
}

private func compareValues(_ op: FilterLeafOp, _ lhs: Int64, _ rhs: Int64) -> Bool {
    switch op {
    case .eq: lhs == rhs
    case .neq: lhs != rhs
    case .gt: lhs > rhs
    case .gte: lhs >= rhs
    case .lt: lhs < rhs
    case .lte: lhs <= rhs
    }
}
