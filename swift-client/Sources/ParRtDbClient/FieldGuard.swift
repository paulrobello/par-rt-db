import Foundation

/// An exact value assertion at an object-key path from a declared root field.
public struct FieldExpectation: Equatable, Codable, Sendable {
    /// Object keys from a declared root field. Array indexing is not supported.
    public var path: [String]
    /// Exact required value. Missing fields never match, including for null.
    public var value: JSONValue

    public init(path: [String], value: JSONValue) {
        self.path = path
        self.value = value
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case path, value
    }

    public init(from decoder: Decoder) throws {
        try decoder.rejectUnknownKeys("FieldExpectation", as: CodingKeys.self)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        path = try container.decode([String].self, forKey: .path)
        value = try container.decode(JSONValue.self, forKey: .value)
    }
}

/// JSON object keys compare by UTF-8 bytes. Swift dictionary lookup normalizes Unicode.
private func fieldLookup<Value>(_ fields: [String: Value], _ key: String) -> Value? {
    fields.first { $0.key.utf8.elementsEqual(key.utf8) }?.value
}

private func fieldPathDeclared(_ fields: [String: FieldType], _ path: ArraySlice<String>) -> Bool {
    guard let key = path.first, let field = fieldLookup(fields, key) else { return false }
    return fieldChildDeclared(field, path.dropFirst())
}

private func fieldChildDeclared(_ field: FieldType, _ path: ArraySlice<String>) -> Bool {
    if path.isEmpty {
        return true
    }
    switch field {
    case .any: return true
    case let .optional(inner): return fieldChildDeclared(inner, path)
    case let .object(fields): return fieldPathDeclared(fields, path)
    case let .record(value): return fieldChildDeclared(value, path.dropFirst())
    case let .union(variants): return variants.contains { fieldChildDeclared($0, path) }
    default: return false
    }
}

private func fieldJSONEqual(_ left: JSONValue, _ right: JSONValue) -> Bool {
    switch (left, right) {
    case let (.int(integer), .double(number)): Int64(exactly: number) == integer
    case let (.double(number), .int(integer)): Int64(exactly: number) == integer
    case let (.string(leftText), .string(rightText)): leftText.utf8.elementsEqual(rightText.utf8)
    case let (.array(leftValues), .array(rightValues)):
        leftValues.count == rightValues.count && zip(leftValues, rightValues).allSatisfy(fieldJSONEqual)
    case let (.object(leftFields), .object(rightFields)):
        leftFields.count == rightFields.count && leftFields.allSatisfy { key, value in
            guard let other = fieldLookup(rightFields, key) else { return false }
            return fieldJSONEqual(value, other)
        }
    default: left == right
    }
}

private func fieldBudgetError() -> RtDbError {
    RtDbError(code: .badRequest, message: "expected field values exceed the depth, node or UTF-8 byte budget")
}

private func checkFieldValueBudget(
    _ value: JSONValue, _ depth: Int, _ nodes: inout Int, _ bytes: inout Int
) throws {
    nodes += 1
    guard depth <= 32, nodes <= 8192, bytes <= 65536 else { throw fieldBudgetError() }
    switch value {
    case let .string(text): bytes += text.utf8.count
    case let .array(values):
        for child in values {
            try checkFieldValueBudget(child, depth + 1, &nodes, &bytes)
        }
    case let .object(values):
        for (key, child) in values {
            bytes += key.utf8.count
            try checkFieldValueBudget(child, depth + 1, &nodes, &bytes)
        }
    case let .double(number): if !number.isFinite {
            throw fieldBudgetError()
        }
    default: break
    }
    if bytes > 65536 {
        throw fieldBudgetError()
    }
}

func checkFieldExpectations(
    _ table: TableDef, _ document: [String: JSONValue], _ expected: [FieldExpectation]
) throws {
    guard !expected.isEmpty, expected.count <= 64 else {
        throw RtDbError(code: .badRequest, message: "expected must contain 1-64 field assertions")
    }
    var paths = Set<[Data]>()
    var nodes = 0
    var bytes = 0
    for item in expected {
        guard !item.path.isEmpty, item.path.count <= 16,
              item.path.allSatisfy({ !$0.isEmpty && $0.utf8.count <= 256 })
        else {
            throw RtDbError(code: .badRequest, message: "field paths require 1-16 keys of 1-256 UTF-8 bytes")
        }
        guard paths.insert(item.path.map { Data($0.utf8) }).inserted else {
            throw RtDbError(code: .badRequest, message: "expected contains a duplicate field path")
        }
        bytes += item.path.reduce(0) { $0 + $1.utf8.count }
        try checkFieldValueBudget(item.value, 0, &nodes, &bytes)
        guard fieldPathDeclared(table.fields, item.path[...]) else {
            throw RtDbError(code: .schemaViolation, message: "expected field path is not declared")
        }
    }
    for item in expected {
        var value = JSONValue.object(document)
        for key in item.path {
            guard case let .object(fields) = value, let child = fieldLookup(fields, key) else {
                throw RtDbError(code: .preconditionFailed, message: "expected fields did not match")
            }
            value = child
        }
        guard fieldJSONEqual(value, item.value) else {
            throw RtDbError(code: .preconditionFailed, message: "expected fields did not match")
        }
    }
}
