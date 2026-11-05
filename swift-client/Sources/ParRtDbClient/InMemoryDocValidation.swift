import Foundation

// Document validation and write-time stamping (store.ts's validateValue/
// validateDoc/stamp* free functions), extracted from ``InMemoryEngine``
// (QA-003b) and moved verbatim into a sibling file. Query-DSL filter
// validation lives in `InMemoryValidate.swift`.

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
            try checkConstraintsForField(table, field, value)
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
        if case .null = value, let fieldTy = table.fields[key], case let .optional(inner) = fieldTy,
           !validateValue(inner, .null) {
            continue
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

/// Stamps the table's `updatedAtField` (FM-36) with the current epoch-ms,
/// overwriting any client-supplied value — the same authority model as the
/// ttl `defaultDurationMs` stamp (server `txn::stamp_updated_at`). Runs on
/// every version-bumping write path: insert, patch, replace, upsert (both
/// branches), patchByQuery, and cascade setNull. The value follows the
/// field's wire convention: a JSON number on `number`, a decimal string on
/// `int64` (int64 travels as strings end to end).
func stampUpdatedAt(
    _ table: TableDef, _ doc: [String: JSONValue], _ now: Int64
) -> [String: JSONValue] {
    guard let field = table.updatedAtField else {
        return doc
    }
    var out = doc
    if case .int64 = table.fields[field] {
        out[field] = .string(String(now))
    } else {
        out[field] = .int(now)
    }
    return out
}

/// Applies the table's push-time-validated `defaults` (FM-32) to a NEW
/// document (store.ts `applyDefaults`): every key the doc omits is stamped.
/// Runs after `stampTtlDefault` and `stampUpdatedAt` (both stamps win over a
/// default on the same field). Only the new-document paths call it — insert,
/// replace, upsert-insert; patch never re-applies.
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

/// Stamps the table's `autoIncrementField` (FM-37) with the next value of the
/// table's monotonic counter, overwriting any client-supplied value — the
/// same authority model as `stampUpdatedAt` (server `txn::stamp_auto_increment`,
/// whose per-table Postgres sequence the `RowStore` counter mirrors). Runs on
/// the two insert paths only (`insert` and upsert's insert branch), AFTER
/// `applyDefaults` (a defaults entry on the field always loses to the stamp);
/// after insert the field is immutable (`applyPatch` / `doReplace` reject
/// changes). The value is a decimal string, matching the int64 wire
/// convention. The counter advances only when stamped — including on an
/// insert that later fails validation, mirroring the server's
/// non-transactional `nextval` (monotonic, not gap-free).
func stampAutoIncrement(
    _ store: RowStore, _ table: TableDef, _ doc: [String: JSONValue]
) -> [String: JSONValue] {
    guard let field = table.autoIncrementField else {
        return doc
    }
    let next = store.autoIncrementNext
    store.autoIncrementNext += 1
    var out = doc
    out[field] = .string(String(next))
    return out
}

/// Applies a patch's fields onto `doc` — a port of server `txn::apply_patch`
/// (store.ts `applyPatch`). A null on an optional field whose inner type
/// rejects null removes the key.
func applyPatch(
    _ table: TableDef, _ doc: [String: JSONValue], _ fields: [String: JSONValue],
    now: Int64
) throws -> [String: JSONValue] {
    // The auto-increment field is server-assigned and immutable after insert.
    // A patch may carry it back unchanged (round-trip friendly), but any
    // different value — including a type-shifted form of the same number —
    // is rejected. Covers `patch`, upsert's update branch, `patchByQuery`,
    // and cascade setNull (every patch-shaped path funnels through here).
    if let auto = table.autoIncrementField, let value = fields[auto], doc[auto] != value {
        throw RtDbError(
            code: .badRequest,
            message: "autoIncrementField '\(auto)' cannot be changed"
        )
    }
    var merged = doc
    for (field, value) in fields {
        // A computed key in the patch is dropped, not merged: the stamp below
        // re-derives it from the final doc, so skipping here also keeps a
        // wrong-typed client value from failing validateValue first.
        if table.computed[field] != nil {
            continue
        }
        guard let fieldTy = table.fields[field] else {
            throw RtDbError(code: .schemaViolation, message: "unknown field '\(field)'")
        }
        if case .null = value, case let .optional(inner) = fieldTy, !validateValue(inner, .null) {
            merged.removeValue(forKey: field)
            continue
        }
        if !validateValue(fieldTy, value) {
            throw RtDbError(
                code: .schemaViolation, message: "field '\(field)' has an invalid value"
            )
        }
        merged[field] = value
    }
    let stamped = try stampComputed(table, merged, now)
    try validateDoc(table, stamped)
    return stamped
}
