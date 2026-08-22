import Foundation

// MARK: - Destructive-change detection

// Mirrors ts-client/src/in_memory/migrate.ts — the schema-migration engine
// for the in-memory engine: destructive-change detection, `onDelete` push
// validation, and the migration-directive interpreter (mirrors
// rust-client/src/in_memory/migrate.rs; one function per directive kind).

/// Returns the values a finite literal-union (or lone literal) accepts,
/// mirroring server `schema::literal_set` (migrate.ts `literalSet`): a lone
/// `literal` yields its single value; a `union` yields its variants' literal
/// values only when EVERY variant is a literal (and the union is non-empty).
/// Returns nil for any type that is not a finite set and cannot widen.
func literalSet(_ ty: FieldType) -> [JSONValue]? {
    switch ty {
    case let .literal(value):
        return [value]
    case let .union(variants):
        guard !variants.isEmpty else { return nil }
        var values: [JSONValue] = []
        for variant in variants {
            guard case let .literal(value) = variant else { return nil }
            values.append(value)
        }
        return values
    default:
        return nil
    }
}

/// True iff every value accepted by `old` is also accepted by `next` — a port
/// of server `schema::is_widening_of` (migrate.ts `isWideningOf`).
func isWideningOf(_ old: FieldType, _ next: FieldType) -> Bool {
    guard let oldValues = literalSet(old), let newValues = literalSet(next) else {
        return false
    }
    return oldValues.allSatisfy { value in
        newValues.contains { jsonEq($0, value) }
    }
}

// swiftlint:disable cyclomatic_complexity function_body_length
/// Rejects destructive schema changes — a port of server
/// `ddl::detect_destructive_changes` (migrate.ts `detectDestructiveChanges`).
/// A second push may only ADD tables, fields, and indexes; removing or
/// retyping any existing table/field/index is a BAD_REQUEST with the server's
/// message. Field types compare after stripping `onDelete` actions (FM-33:
/// adding or changing an action is additive), and a field-type change is
/// accepted when it is a safe widening.
public func detectDestructiveChanges(_ oldSchema: SchemaDef, _ newSchema: SchemaDef) throws {
    for (tableName, oldTable) in oldSchema.tables {
        guard let newTable = newSchema.tables[tableName] else {
            throw RtDbError(code: .badRequest, message: "removed table '\(tableName)'")
        }
        for (fieldName, oldFieldType) in oldTable.fields {
            guard let newFieldType = newTable.fields[fieldName] else {
                throw RtDbError(
                    code: .badRequest, message: "removed field '\(tableName).\(fieldName)'"
                )
            }
            let changed = stripOnDelete(newFieldType) != stripOnDelete(oldFieldType)
            if changed, !isWideningOf(oldFieldType, newFieldType) {
                throw RtDbError(
                    code: .badRequest, message: "changed type of field '\(tableName).\(fieldName)'"
                )
            }
        }
        for oldIndex in oldTable.indexes ?? [] {
            guard let newIndex = (newTable.indexes ?? []).first(where: { $0.name == oldIndex.name })
            else {
                throw RtDbError(code: .badRequest, message: "removed index '\(oldIndex.name)'")
            }
            if newIndex.fields != oldIndex.fields {
                throw RtDbError(
                    code: .badRequest, message: "changed fields of index '\(oldIndex.name)'"
                )
            }
            if newIndex.search != oldIndex.search {
                throw RtDbError(
                    code: .badRequest,
                    message: "changed kind of index '\(oldIndex.name)' (btree <-> search)"
                )
            }
            if newIndex.vector != oldIndex.vector {
                throw RtDbError(
                    code: .badRequest, message: "changed vector spec of index '\(oldIndex.name)'"
                )
            }
            if newIndex.unique != oldIndex.unique {
                throw RtDbError(
                    code: .badRequest, message: "changed uniqueness of index '\(oldIndex.name)'"
                )
            }
            if newIndex.whereClause != oldIndex.whereClause {
                throw RtDbError(
                    code: .badRequest,
                    message: "changed partial predicate of index '\(oldIndex.name)'"
                )
            }
            if newIndex.language != oldIndex.language {
                throw RtDbError(
                    code: .badRequest,
                    message: "changed language of search index '\(oldIndex.name)'"
                )
            }
        }
    }
}

// swiftlint:enable cyclomatic_complexity function_body_length

// MARK: - Push-time validation

// swiftlint:disable cyclomatic_complexity function_body_length
/// Push-time schema validation — the TTL, updatedAtField, autoIncrementField,
/// and index-field rules of server `schema::validate` (migrate.ts
/// `validateSchema`): index fields must be declared and indexable, search
/// indexes must cover text fields, a TTL must name a numeric field carrying a
/// single-field, non-unique, non-partial btree index, an `updatedAtField`
/// must name a declared numeric field differing from `ttl.field`, and an
/// `autoIncrementField` must name a declared `int64` field differing from
/// both. Deliberately a subset — identifier formats, owner/collaborators
/// fields, and `defaults` shapes stay server-side (`onDelete` has its own
/// `validateOnDelete` pass).
public func validateSchema(_ schema: SchemaDef) throws {
    for (tableName, table) in schema.tables {
        for index in table.indexes ?? [] {
            if index.fields.isEmpty {
                throw RtDbError(
                    code: .schemaViolation,
                    message: "index '\(index.name)' on table '\(tableName)' has no fields"
                )
            }
            // A vector index's `fields[0]` is a Vector column, not
            // btree-indexable — the server validates vector specs in their
            // own branch and skips the per-field loop.
            if index.vector != nil {
                continue
            }
            for fieldName in index.fields {
                guard let fieldType = table.fields[fieldName] else {
                    throw RtDbError(
                        code: .schemaViolation,
                        message: "index '\(index.name)' on table '\(tableName)' references "
                            + "unknown field '\(fieldName)'"
                    )
                }
                let pg = try indexColumnType(fieldType).pg
                if index.search, pg != .text {
                    throw RtDbError(
                        code: .schemaViolation,
                        message: "search index '\(index.name)' on table '\(tableName)' has "
                            + "non-text field '\(fieldName)'"
                    )
                }
            }
        }
        if let ttl = table.ttl {
            guard let fieldType = table.fields[ttl.field] else {
                throw RtDbError(
                    code: .schemaViolation,
                    message: "ttl.field '\(ttl.field)' is not a declared field"
                )
            }
            // No optional unwrap — the server's TTL check types the declared
            // field directly, exactly as the TS harness does.
            let tag = fieldTypeTag(fieldType)
            guard tag == "number" || tag == "int64" else {
                throw RtDbError(
                    code: .schemaViolation,
                    message: "ttl.field '\(ttl.field)' must be a number or bigint field"
                )
            }
            let hasTtlIndex = (table.indexes ?? []).contains { index in
                !index.search && index.vector == nil && !index.unique && index.whereClause == nil
                    && index.fields.count == 1 && index.fields[0] == ttl.field
            }
            guard hasTtlIndex else {
                throw RtDbError(
                    code: .schemaViolation,
                    message: "ttl.field '\(ttl.field)' requires a single-field, non-unique, "
                        + "non-partial btree index on it"
                )
            }
            if let duration = ttl.defaultDurationMs, duration <= 0 {
                throw RtDbError(
                    code: .schemaViolation,
                    message: "ttl.defaultDurationMs must be greater than 0"
                )
            }
        }
        if let field = table.updatedAtField {
            guard let fieldType = table.fields[field] else {
                throw RtDbError(
                    code: .schemaViolation,
                    message: "updatedAtField '\(field)' is not a declared field"
                )
            }
            let tag = fieldTypeTag(fieldType)
            guard tag == "number" || tag == "int64" else {
                throw RtDbError(
                    code: .schemaViolation,
                    message: "updatedAtField '\(field)' must be a number or bigint field"
                )
            }
            if let ttl = table.ttl, ttl.field == field {
                throw RtDbError(
                    code: .schemaViolation,
                    message: "updatedAtField '\(field)' must differ from ttl.field (both "
                        + "stamps write unconditionally; a shared field would drop the expiry)"
                )
            }
        }
        if let field = table.autoIncrementField {
            guard let fieldType = table.fields[field] else {
                throw RtDbError(
                    code: .schemaViolation,
                    message: "autoIncrementField '\(field)' is not a declared field"
                )
            }
            // Exactly int64 (server `validate_auto_increment`): the counter
            // produces int64 — a `number` would lose precision, an
            // `optional` would admit a missing counter.
            let tag = fieldTypeTag(fieldType)
            guard tag == "int64" else {
                throw RtDbError(
                    code: .schemaViolation,
                    message: "autoIncrementField '\(field)' must be an int64 field"
                )
            }
            if let ttl = table.ttl, ttl.field == field {
                throw RtDbError(
                    code: .schemaViolation,
                    message: "autoIncrementField '\(field)' must differ from ttl.field "
                        + "(the ttl reaper would delete counter rows)"
                )
            }
            if let updatedAt = table.updatedAtField, updatedAt == field {
                throw RtDbError(
                    code: .schemaViolation,
                    message: "autoIncrementField '\(field)' must differ from updatedAtField "
                        + "(the timestamp would overwrite the counter on every write)"
                )
            }
        }
        // ENH-028: computed-field rules (server `validate_structure` ->
        // `validate_computed`) — declared keys, non-stamped targets, declared
        // non-computed references, marker-free `caseExpr` whens, static-kind
        // fit, and authorize independence. BAD_REQUEST, matching the server
        // (the corpus pushError cases pin the code).
        try validateComputedTable(table, tableName)
    }
}

// swiftlint:enable cyclomatic_complexity function_body_length

/// Strips `onDelete` from id fields, recursing through every compositor — a
/// port of server `schema::strip_on_delete` (migrate.ts `stripOnDelete`), so
/// adding or changing an action never counts as a destructive field-type
/// change.
func stripOnDelete(_ ty: FieldType) -> FieldType {
    switch ty {
    case let .id(table, onDelete):
        onDelete != nil ? .id(table: table, onDelete: nil) : ty
    case let .optional(inner):
        .optional(inner: stripOnDelete(inner))
    case let .union(variants):
        .union(variants: variants.map(stripOnDelete))
    case let .array(element):
        .array(element: stripOnDelete(element))
    case let .object(fields):
        .object(fields: fields.mapValues(stripOnDelete))
    case let .record(value):
        .record(value: stripOnDelete(value))
    default:
        ty
    }
}

/// True iff an id field carrying `onDelete` appears anywhere in `ty` (at any
/// nesting depth) — the probe behind the FM-33 push rule that confines
/// `onDelete` to a top-level id or optional-id field (migrate.ts
/// `fieldHasNestedOnDelete`).
private func fieldHasNestedOnDelete(_ ty: FieldType) -> Bool {
    switch ty {
    case let .id(_, onDelete):
        onDelete != nil
    case let .optional(inner):
        fieldHasNestedOnDelete(inner)
    case let .union(variants):
        variants.contains(where: fieldHasNestedOnDelete)
    case let .array(element):
        fieldHasNestedOnDelete(element)
    case let .object(fields):
        fields.values.contains(where: fieldHasNestedOnDelete)
    case let .record(value):
        fieldHasNestedOnDelete(value)
    default:
        false
    }
}

/// The top-level id declaration under `ty` (an `optional` wrapper unwraps),
/// when it carries an `onDelete` action — nil otherwise (migrate.ts's inline
/// `topId?.onDelete` probe).
private func onDeleteDeclaration(_ ty: FieldType) -> (table: String, action: OnDeleteAction)? {
    var inner = ty
    if case let .optional(wrapped) = inner {
        inner = wrapped
    }
    if case let .id(table, onDelete) = inner, let action = onDelete {
        return (table, action)
    }
    return nil
}

// swiftlint:disable cyclomatic_complexity
/// Validates `onDelete` declarations at push time — a port of server
/// `schema::validate_on_delete` (FM-33; migrate.ts `validateOnDelete`). An
/// action is legal only on a top-level `id` field (or one `optional` wrapping
/// it — required for `setNull`); the referencing field needs a single-field,
/// non-unique, non-partial btree index; and the referenced table must exist.
public func validateOnDelete(_ schema: SchemaDef) throws {
    for (tableName, table) in schema.tables {
        for (fieldName, fieldTy) in table.fields {
            guard let declaration = onDeleteDeclaration(fieldTy) else {
                if fieldHasNestedOnDelete(fieldTy) {
                    throw RtDbError(
                        code: .schemaViolation,
                        message: "field '\(fieldName)' on table '\(tableName)': onDelete is "
                            + "legal only on a top-level id or optional-id field"
                    )
                }
                continue
            }
            if declaration.action == .setNull {
                var isOptional = false
                if case .optional = fieldTy {
                    isOptional = true
                }
                guard isOptional else {
                    throw RtDbError(
                        code: .schemaViolation,
                        message: "onDelete 'setNull' requires the id field to be optional"
                    )
                }
            }
            let hasIndex = (table.indexes ?? []).contains { index in
                !index.search && index.vector == nil && !index.unique && index.whereClause == nil
                    && index.fields.count == 1 && index.fields[0] == fieldName
            }
            guard hasIndex else {
                throw RtDbError(
                    code: .schemaViolation,
                    message: "onDelete field '\(fieldName)' on table '\(tableName)' requires a "
                        + "single-field, non-unique, non-partial btree index on it"
                )
            }
        }
    }
    // Second pass (server order): every referenced table must exist.
    for (tableName, table) in schema.tables {
        for (fieldName, fieldTy) in table.fields {
            guard let declaration = onDeleteDeclaration(fieldTy) else { continue }
            if schema.tables[declaration.table] == nil {
                throw RtDbError(
                    code: .schemaViolation,
                    message: "onDelete field '\(fieldName)' on table '\(tableName)' references "
                        + "unknown table '\(declaration.table)'"
                )
            }
        }
    }
}

// swiftlint:enable cyclomatic_complexity

/// The `onDelete` action `ty` declares against `parentTable`, if any — a port
/// of server `txn::on_delete_ref` (migrate.ts `onDeleteRef`). Only a top-level
/// id (or one `optional` wrapping it) can carry one; push validation keeps
/// every other shape from reaching this walk.
public func onDeleteRef(_ ty: FieldType, _ parentTable: String) -> OnDeleteAction? {
    if case let .id(table, onDelete) = ty {
        return table == parentTable ? onDelete : nil
    }
    if case let .optional(inner) = ty {
        return onDeleteRef(inner, parentTable)
    }
    return nil
}

// MARK: - Computed-field push validation (ENH-028)

/// The statically-known result kind of a `ValueExpr`, for the computed-field
/// push check (server `StaticKind`). nil means the result kind varies by
/// input — `field` (text extraction of any JSON value), `coalesce`/`caseExpr`
/// (whichever branch wins), and the null / object / array literals whose
/// runtime `validateDoc` check is the only guard.
private enum ComputedStaticKind: Equatable {
    case string
    case number
    case boolean

    /// The sample value the field's type must accept (server: "s" / 1 / true).
    var sample: JSONValue {
        switch self {
        case .string: .string("s")
        case .number: .int(1)
        case .boolean: .bool(true)
        }
    }

    /// The kind's name in the rejection message (server `as_str`).
    var name: String {
        switch self {
        case .string: "a string"
        case .number: "a number"
        case .boolean: "a boolean"
        }
    }
}

private func inferStaticKind(_ ve: ValueExpr) -> ComputedStaticKind? {
    switch ve {
    case .field, .coalesce, .caseExpr:
        nil
    case let .literal(value):
        switch value {
        case .string: .string
        case .int, .double: .number
        case .bool: .boolean
        case .null, .array, .object: nil
        }
    case .concat, .lower, .upper, .trim, .cast(_, .toString):
        .string
    case .add, .sub, .mul, .div, .cast(_, .toNumber), .cast(_, .toInt64), .now:
        .number
    case .cast(_, .toBoolean):
        .boolean
    }
}

/// Whether a filter value is a principal marker — `{"$user":true}` or
/// `{"$email":true}` (server `is_principal_marker`).
private func isPrincipalMarker(_ value: JSONValue) -> Bool {
    guard case let .object(map) = value, map.count == 1 else {
        return false
    }
    if case .bool(true)? = map["$user"] {
        return true
    }
    if case .bool(true)? = map["$email"] {
        return true
    }
    return false
}

/// Walks a computed expression's `caseExpr` nodes rejecting principal markers
/// in every `when` filter — computed exprs run on every write with no
/// interactive principal, so a `$user`/`$email` marker has no value to
/// resolve (server `validate_computed_case_whens`). Branch bodies recurse so
/// a `caseExpr` nested inside a `then`/`otherwise` is covered.
private func validateComputedCaseWhens(_ ve: ValueExpr) throws {
    switch ve {
    case let .caseExpr(whens, otherwise):
        for cw in whens {
            try rejectPrincipalMarkers(cw.when)
            try validateComputedCaseWhens(cw.then)
        }
        try validateComputedCaseWhens(otherwise)
    case let .concat(parts), let .coalesce(parts):
        for part in parts {
            try validateComputedCaseWhens(part)
        }
    case let .add(left, right), let .sub(left, right),
         let .mul(left, right), let .div(left, right):
        try validateComputedCaseWhens(left)
        try validateComputedCaseWhens(right)
    case let .lower(value), let .upper(value), let .trim(value), let .cast(value, _):
        try validateComputedCaseWhens(value)
    case .field, .literal, .now:
        break
    }
}

/// Rejects a principal marker in any leaf VALUE position of a filter
/// (eq/neq/gt/gte/lt/lte/contains values and `in` values) — the
/// marker-rejecting mode of the server's `validate_filter_expr_fields`.
private func rejectPrincipalMarkers(_ expr: FilterExpr) throws {
    switch expr {
    case let .eq(field, value), let .neq(field, value), let .gt(field, value),
         let .gte(field, value), let .lt(field, value), let .lte(field, value),
         let .contains(field, value):
        if isPrincipalMarker(value) {
            throw RtDbError(
                code: .badRequest,
                message: "principal markers ({\"$user\":true}/{\"$email\":true}) are not "
                    + "allowed in client filters (field '\(field)')"
            )
        }
    case let .inValues(field, values):
        for value in values where isPrincipalMarker(value) {
            throw RtDbError(
                code: .badRequest,
                message: "principal markers ({\"$user\":true}/{\"$email\":true}) are not "
                    + "allowed in client filters (field '\(field)')"
            )
        }
    case let .and(exprs), let .or(exprs):
        for subExpr in exprs {
            try rejectPrincipalMarkers(subExpr)
        }
    case let .not(expr):
        try rejectPrincipalMarkers(expr)
    case .exists:
        break
    }
}

// swiftlint:disable cyclomatic_complexity function_body_length
/// Computed-field push validation — a port of server
/// `schema::TableDef::validate_computed`. Rules, in order:
/// 1. every `computed` key names a declared field;
/// 2. the key is not one of the server-stamped declaration fields
///    (`ownerField`/`collaboratorsField`/`autoIncrementField`);
/// 3. every field the expression references (including `caseExpr.when` filter
///    fields) is declared and not itself computed (no chained or cyclic
///    evaluation);
/// 4. `caseExpr.when` filters reject principal markers;
/// 5. when the expression's result kind is statically known, the field's type
///    must accept a value of that kind (int64 accepts a String kind — a
///    decimal-string possibility — but never a Number kind);
/// 6. the table's `authorize` predicate references no computed field (authorize
///    runs pre-stamp on the insert paths, so such a predicate would evaluate
///    forgeable client input).
func validateComputedTable(_ table: TableDef, _ tableName: String) throws {
    for (field, expr) in table.computed {
        if table.fields[field] == nil {
            throw RtDbError(
                code: .badRequest,
                message: "computed field '\(tableName).\(field)' is not a declared field"
            )
        }
        if table.ownerField == field {
            throw RtDbError(
                code: .badRequest,
                message: "computed field '\(tableName).\(field)' must not be the table's "
                    + "ownerField"
            )
        }
        if table.collaboratorsField == field {
            throw RtDbError(
                code: .badRequest,
                message: "computed field '\(tableName).\(field)' must not be the table's "
                    + "collaboratorsField"
            )
        }
        if table.autoIncrementField == field {
            throw RtDbError(
                code: .badRequest,
                message: "computed field '\(tableName).\(field)' must not be the table's "
                    + "autoIncrementField"
            )
        }
        // First offense wins; the walk covers `field` nodes and every
        // `caseExpr.when` filter field.
        var offender: String?
        walkValueExprFields(expr) { referenced in
            guard offender == nil else {
                return
            }
            if table.fields[referenced] == nil {
                offender = "computed field '\(tableName).\(field)' references undeclared "
                    + "field '\(referenced)'"
            } else if table.computed[referenced] != nil {
                offender = "computed field '\(tableName).\(field)' references computed "
                    + "field '\(referenced)' (computed fields may not reference each other)"
            }
        }
        if let message = offender {
            throw RtDbError(code: .badRequest, message: message)
        }
        try validateComputedCaseWhens(expr)
        if let kind = inferStaticKind(expr) {
            // `validateValue` is the wire contract, but int64's wire form is a
            // decimal STRING: a Number-kind result can never validate
            // (arithmetic yields JSON numbers), while a String-kind one can
            // ("42") — decimal-ness stays a runtime `validateDoc` check.
            // Optional unwrapping admits the nullable spelling. Rule 1 above
            // guarantees the key is declared.
            guard let declared = table.fields[field] else {
                return
            }
            var inner = declared
            while case let .optional(wrapped) = inner {
                inner = wrapped
            }
            let accepts = validateValue(declared, kind.sample)
                || (inner == .int64 && kind == .string)
            if !accepts {
                throw RtDbError(
                    code: .badRequest,
                    message: "computed field '\(tableName).\(field)' produces \(kind.name), "
                        + "which the field type does not accept"
                )
            }
        }
    }
    // Rule 6: authorize runs pre-stamp on the insert paths, so a predicate
    // over a computed field would read client input.
    if let authorize = table.authorize {
        var offender: String?
        walkFilterExprFieldNames(authorize) { referenced in
            if offender == nil, table.computed[referenced] != nil {
                offender = referenced
            }
        }
        if let field = offender {
            throw RtDbError(
                code: .badRequest,
                message: "computed field '\(tableName).\(field)' must not be referenced by "
                    + "the table's authorize predicate (authorize predicates may not "
                    + "reference computed fields)"
            )
        }
    }
}

// swiftlint:enable cyclomatic_complexity function_body_length

/// Validates every table's computed-field map — the schema-level entry point
/// behind `validateSchema`'s per-table pass, also called by the engine's
/// `migrate` after directive folding so a `changeType` that invalidates a
/// computed entry fails at plan time (server `schema::validate_computed`,
/// called from `plan_migration`).
public func validateComputed(_ schema: SchemaDef) throws {
    for (tableName, table) in schema.tables {
        try validateComputedTable(table, tableName)
    }
}

// MARK: - Casts

/// True iff `cast` can coerce from `old` — a port of server
/// `migrate::cast_valid_for` (migrate.ts `castValidFor`).
func castValidFor(_ cast: Cast, _ old: FieldType) -> Bool {
    let tag = fieldTypeTag(old)
    switch cast {
    case .toString:
        return tag == "string" || tag == "number" || tag == "boolean" || tag == "int64"
    case .toNumber:
        return tag == "string" || tag == "boolean" || tag == "int64"
    case .toInt64:
        return tag == "string" || tag == "number"
    case .toBoolean:
        return tag == "string" || tag == "number"
    }
}

// swiftlint:disable cyclomatic_complexity
/// Pure coercion mirroring server `migrate::coerce_value` (migrate.ts
/// `coerceValue`). Returns nil when the value cannot be coerced under `cast` —
/// the caller then substitutes a (coerced) default or raises a row-named
/// BAD_REQUEST. `toInt64` emits the canonical decimal-string wire form;
/// `toNumber` emits a JSON number.
func coerceValue(_ cast: Cast, _ value: JSONValue) -> JSONValue? {
    switch cast {
    case .toString:
        switch value {
        case let .string(string): return .string(string)
        case let .int(int): return .string(String(int))
        case let .double(double): return .string(jsNumberString(double))
        case let .bool(bool): return .string(bool ? "true" : "false")
        default: return nil
        }
    case .toNumber:
        switch value {
        case let .string(string):
            guard let number = Double(string), number.isFinite else { return nil }
            return jsonNumber(number)
        case let .int(int): return .int(int)
        case let .double(double): return .double(double)
        case let .bool(bool): return .int(bool ? 1 : 0)
        default: return nil
        }
    case .toInt64:
        switch value {
        case let .string(string):
            // `isInt64String` validates the canonical decimal form and the
            // i64 range; the value passes through unchanged.
            return isInt64String(value) ? .string(string) : nil
        case let .int(int):
            return .string(String(int))
        case let .double(double):
            guard double == double.rounded(), double.isFinite else { return nil }
            guard let int = Int64(exactly: double) else { return nil }
            return .string(String(int))
        default: return nil
        }
    case .toBoolean:
        switch value {
        case let .string(string):
            if string == "true" || string == "1" {
                return .bool(true)
            }
            if string == "false" || string == "0" {
                return .bool(false)
            }
            return nil
        case let .int(int): return .bool(int != 0)
        case let .double(double): return .bool(double != 0)
        default: return nil
        }
    }
}

// swiftlint:enable cyclomatic_complexity

// MARK: - Directive interpreter

/// Doc-store handle the directive functions operate through (migrate.ts
/// `MigrationStore`): the lazy per-table row accessor from the engine core,
/// plus the two re-keying operations a rename/drop needs. The TS passes its
/// live `tables` map directly; Swift dictionaries are copy-on-write, so the
/// handle speaks in operations instead.
protocol MigrationStore: AnyObject {
    func rowsFor(_ table: String) -> [String: StoredRow]
    /// Move one table's rows to a new key (renameTable).
    func moveTableRows(from: String, to: String)
    /// Drop one table's rows entirely (dropTable).
    func dropTableRows(_ name: String)
}

/// Validates and applies one directive (migrate.ts
/// `applyMigrationDirective`): folds the structural effect into `planned`
/// (the working schema copy, passed inout) and rewrites the doc map. Thin
/// dispatcher — one function per directive kind below.
func applyMigrationDirective(
    _ planned: inout SchemaDef, _ directive: Directive, _ store: MigrationStore
) throws -> (report: DirectiveReport, table: String?) {
    switch directive {
    case let .renameField(table, from, to):
        return try applyRenameFieldDirective(&planned, table, from, to, store)
    case let .renameTable(from, to):
        return try applyRenameTableDirective(&planned, from, to, store)
    case let .changeType(table, field, to, cast, defaultValue):
        return try applyChangeTypeDirective(&planned, table, field, to, cast, defaultValue, store)
    case let .dropField(table, field):
        return try applyDropFieldDirective(&planned, table, field, store)
    case let .dropTable(name):
        return try applyDropTableDirective(&planned, name, store)
    case let .dropIndex(table, name):
        return try applyDropIndexDirective(&planned, table, name)
    case let .setDefault(table, field, value):
        return try applySetDefaultDirective(&planned, table, field, value, store)
    case .evalExpr:
        // No SQL engine in the harness — throw rather than silently misbehave
        // (both dual-accept arms, typed and legacy, for the same reason).
        throw RtDbError(code: .badRequest, message: "evalExpr unsupported in-memory")
    }
}

/// Guards that the working schema carries `name`, throwing the server-shaped
/// BAD_REQUEST when absent (migrate.ts `migrateTable` — Swift cannot return
/// an inout binding, so callers mutate `planned.tables[name]` directly).
private func requireMigrateTable(_ schema: SchemaDef, _ name: String) throws {
    guard schema.tables[name] != nil else {
        throw RtDbError(code: .badRequest, message: "table '\(name)' does not exist")
    }
}

// swiftlint:disable:next cyclomatic_complexity
private func applyRenameFieldDirective(
    _ planned: inout SchemaDef, _ table: String, _ from: String, _ to: String,
    _ store: MigrationStore
) throws -> (report: DirectiveReport, table: String?) {
    try requireMigrateTable(planned, table)
    if planned.tables[table]?.fields[to] != nil {
        throw RtDbError(
            code: .badRequest, message: "rename target '\(table).\(to)' already exists"
        )
    }
    guard let fieldType = planned.tables[table]?.fields[from] else {
        throw RtDbError(
            code: .badRequest, message: "renamed field '\(table).\(from)' does not exist"
        )
    }
    planned.tables[table]?.fields.removeValue(forKey: from)
    planned.tables[table]?.fields[to] = fieldType
    if var indexes = planned.tables[table]?.indexes {
        for position in indexes.indices {
            indexes[position].fields = indexes[position].fields.map { $0 == from ? to : $0 }
        }
        planned.tables[table]?.indexes = indexes
    }
    if planned.tables[table]?.ownerField == from {
        planned.tables[table]?.ownerField = to
    }
    if planned.tables[table]?.collaboratorsField == from {
        planned.tables[table]?.collaboratorsField = to
    }
    // ENH-028: the computed map follows the rename the way `defaults` does —
    // an entry KEYED on the renamed field moves to the new name (its declared
    // field moved; leaving it keyed on `from` would fail validateComputed's
    // declared-field rule on the derived schema), and every expression's
    // `field` references (including `caseExpr.whens` predicates) are rewritten
    // to read the renamed doc key. Input values are unchanged by the rename,
    // so stored computed values stay correct; the next write re-stamps.
    if var computed = planned.tables[table]?.computed {
        if let keyed = computed.removeValue(forKey: from) {
            computed[to] = keyed
        }
        for key in Array(computed.keys) {
            if let expr = computed[key] {
                computed[key] = renamedValueExpr(expr, from, to)
            }
        }
        planned.tables[table]?.computed = computed
    }
    var affected: Int64 = 0
    for row in store.rowsFor(table).values {
        guard let value = row.doc.removeValue(forKey: from) else { continue }
        row.doc[to] = value
        affected += 1
    }
    return (DirectiveReport(op: "renameField", affectedRows: affected), table)
}

// swiftlint:disable cyclomatic_complexity
/// The `FilterExpr` half of a field rename: rewrites every leaf `field` equal
/// to `from` to `to` (server `rename_filter_fields`). Recurses through
/// `and`/`or`/`not`.
private func renamedFilterExpr(_ expr: FilterExpr, _ from: String, _ to: String) -> FilterExpr {
    switch expr {
    case let .eq(name, value):
        .eq(field: name == from ? to : name, value: value)
    case let .neq(name, value):
        .neq(field: name == from ? to : name, value: value)
    case let .gt(name, value):
        .gt(field: name == from ? to : name, value: value)
    case let .gte(name, value):
        .gte(field: name == from ? to : name, value: value)
    case let .lt(name, value):
        .lt(field: name == from ? to : name, value: value)
    case let .lte(name, value):
        .lte(field: name == from ? to : name, value: value)
    case let .inValues(name, values):
        .inValues(field: name == from ? to : name, values: values)
    case let .contains(name, value):
        .contains(field: name == from ? to : name, value: value)
    case let .exists(name):
        .exists(field: name == from ? to : name)
    case let .and(exprs):
        .and(exprs: exprs.map { renamedFilterExpr($0, from, to) })
    case let .or(exprs):
        .or(exprs: exprs.map { renamedFilterExpr($0, from, to) })
    case let .not(inner):
        .not(expr: renamedFilterExpr(inner, from, to))
    }
}

// swiftlint:enable cyclomatic_complexity

// swiftlint:disable cyclomatic_complexity
/// The `ValueExpr` half of a field rename: rewrites every `field` reference
/// equal to `from` to `to` — the value-returning mirror of
/// `walkValueExprFields` (server `rename_value_expr_fields`). `caseExpr.whens`
/// predicates reuse `renamedFilterExpr` (the same rewrite `authorize` gets on
/// the server), so a rename carries computed expressions across intact. `to`
/// is fresh (renameField rejects an existing target), so no reference set can
/// collide.
private func renamedValueExpr(_ ve: ValueExpr, _ from: String, _ to: String) -> ValueExpr {
    switch ve {
    case let .field(name):
        name == from ? .field(field: to) : ve
    case .literal, .now:
        ve
    case let .concat(parts):
        .concat(parts: parts.map { renamedValueExpr($0, from, to) })
    case let .coalesce(parts):
        .coalesce(parts: parts.map { renamedValueExpr($0, from, to) })
    case let .add(left, right):
        .add(left: renamedValueExpr(left, from, to), right: renamedValueExpr(right, from, to))
    case let .sub(left, right):
        .sub(left: renamedValueExpr(left, from, to), right: renamedValueExpr(right, from, to))
    case let .mul(left, right):
        .mul(left: renamedValueExpr(left, from, to), right: renamedValueExpr(right, from, to))
    case let .div(left, right):
        .div(left: renamedValueExpr(left, from, to), right: renamedValueExpr(right, from, to))
    case let .lower(value):
        .lower(value: renamedValueExpr(value, from, to))
    case let .upper(value):
        .upper(value: renamedValueExpr(value, from, to))
    case let .trim(value):
        .trim(value: renamedValueExpr(value, from, to))
    case let .cast(value, castTo):
        .cast(value: renamedValueExpr(value, from, to), to: castTo)
    case let .caseExpr(whens, otherwise):
        .caseExpr(
            whens: whens.map { cw in
                CaseWhen(
                    when: renamedFilterExpr(cw.when, from, to),
                    then: renamedValueExpr(cw.then, from, to)
                )
            },
            otherwise: renamedValueExpr(otherwise, from, to)
        )
    }
}

// swiftlint:enable cyclomatic_complexity

private func applyRenameTableDirective(
    _ planned: inout SchemaDef, _ from: String, _ to: String, _ store: MigrationStore
) throws -> (report: DirectiveReport, table: String?) {
    if planned.tables[to] != nil {
        throw RtDbError(
            code: .badRequest, message: "rename target table '\(to)' already exists"
        )
    }
    guard let def = planned.tables.removeValue(forKey: from) else {
        throw RtDbError(code: .badRequest, message: "renamed table '\(from)' does not exist")
    }
    // Id references to `from` in other tables follow the rename. The onDelete
    // action is preserved — the rust engine's behavior; the TS port's
    // fresh-id rewrite silently drops it.
    for name in Array(planned.tables.keys) {
        for (fieldName, fieldTy) in planned.tables[name]?.fields ?? [:] {
            if case let .id(refTable, onDelete) = fieldTy, refTable == from {
                planned.tables[name]?.fields[fieldName] = .id(table: to, onDelete: onDelete)
            }
        }
    }
    planned.tables[to] = def
    store.moveTableRows(from: from, to: to)
    return (DirectiveReport(op: "renameTable", affectedRows: 0), to)
}

// swiftlint:disable function_parameter_count
private func applyChangeTypeDirective(
    _ planned: inout SchemaDef, _ table: String, _ field: String, _ to: FieldType,
    _ cast: Cast, _ defaultValue: JSONValue?, _ store: MigrationStore
) throws -> (report: DirectiveReport, table: String?) {
    try requireMigrateTable(planned, table)
    guard let oldTy = planned.tables[table]?.fields[field] else {
        throw RtDbError(
            code: .badRequest, message: "changed field '\(table).\(field)' does not exist"
        )
    }
    guard castValidFor(cast, oldTy) else {
        throw RtDbError(
            code: .badRequest, message: "cast \(cast.rawValue) is not valid for \(table).\(field)"
        )
    }
    let rows = Array(store.rowsFor(table).values)
    var affected: Int64 = 0
    for row in rows {
        guard let original = row.doc[field] else { continue }
        affected += 1
        if let coerced = coerceValue(cast, original) {
            row.doc[field] = coerced
            continue
        }
        if let fallback = defaultValue {
            row.doc[field] = coerceValue(cast, fallback) ?? fallback
            continue
        }
        throw RtDbError(
            code: .badRequest,
            message: "changeType cannot coerce value in \(table).\(row.id) (\(original)) "
                + "and no default given"
        )
    }
    planned.tables[table]?.fields[field] = to
    return (DirectiveReport(op: "changeType", affectedRows: affected), table)
}

// swiftlint:enable function_parameter_count

// swiftlint:disable:next cyclomatic_complexity
private func applyDropFieldDirective(
    _ planned: inout SchemaDef, _ table: String, _ field: String, _ store: MigrationStore
) throws -> (report: DirectiveReport, table: String?) {
    try requireMigrateTable(planned, table)
    guard planned.tables[table]?.fields[field] != nil else {
        throw RtDbError(
            code: .badRequest, message: "dropped field '\(table).\(field)' does not exist"
        )
    }
    planned.tables[table]?.fields.removeValue(forKey: field)
    if var indexes = planned.tables[table]?.indexes {
        for position in indexes.indices {
            indexes[position].fields.removeAll { $0 == field }
        }
        planned.tables[table]?.indexes = indexes
    }
    if planned.tables[table]?.ownerField == field {
        planned.tables[table]?.ownerField = nil
    }
    if planned.tables[table]?.collaboratorsField == field {
        planned.tables[table]?.collaboratorsField = nil
    }
    // ENH-028: a computed expression reading the dropped field would dangle —
    // every future write fails its stamp. Reject, naming the computed field,
    // so the caller amends the computed map first (a push removing the entry
    // leaves stored values in place).
    var computedOffender: String?
    for (computedField, expr) in planned.tables[table]?.computed ?? [:] {
        var referenced = false
        walkValueExprFields(expr) { name in
            if name == field {
                referenced = true
            }
        }
        if referenced {
            computedOffender = computedField
            break
        }
    }
    if let computedField = computedOffender {
        throw RtDbError(
            code: .badRequest,
            message: "cannot drop field '\(table).\(field)': it is referenced by computed "
                + "field '\(table).\(computedField)'; drop the computed field first"
        )
    }
    // An entry KEYED on the dropped field goes with it (the `defaults`
    // discipline): the applier removes the stored key from every doc, so
    // leaving the entry would fail validateComputed's declared-field rule on
    // the derived schema.
    planned.tables[table]?.computed.removeValue(forKey: field)
    var affected: Int64 = 0
    for row in store.rowsFor(table).values {
        let removed = row.doc.removeValue(forKey: field)
        if removed != nil {
            affected += 1
        }
    }
    return (DirectiveReport(op: "dropField", affectedRows: affected), table)
}

private func applyDropTableDirective(
    _ planned: inout SchemaDef, _ name: String, _ store: MigrationStore
) throws -> (report: DirectiveReport, table: String?) {
    guard planned.tables[name] != nil else {
        throw RtDbError(code: .badRequest, message: "dropped table '\(name)' does not exist")
    }
    let count = store.rowsFor(name).count
    planned.tables.removeValue(forKey: name)
    store.dropTableRows(name)
    return (DirectiveReport(op: "dropTable", affectedRows: Int64(count)), name)
}

private func applyDropIndexDirective(
    _ planned: inout SchemaDef, _ table: String, _ name: String
) throws -> (report: DirectiveReport, table: String?) {
    try requireMigrateTable(planned, table)
    guard let indexes = planned.tables[table]?.indexes,
          indexes.contains(where: { $0.name == name })
    else {
        throw RtDbError(
            code: .badRequest, message: "dropped index '\(table).\(name)' does not exist"
        )
    }
    planned.tables[table]?.indexes = indexes.filter { $0.name != name }
    return (DirectiveReport(op: "dropIndex", affectedRows: 0), table)
}

private func applySetDefaultDirective(
    _ planned: inout SchemaDef, _ table: String, _ field: String, _ value: JSONValue,
    _ store: MigrationStore
) throws -> (report: DirectiveReport, table: String?) {
    try requireMigrateTable(planned, table)
    guard planned.tables[table]?.fields[field] != nil else {
        throw RtDbError(
            code: .badRequest, message: "setDefault target '\(table).\(field)' does not exist"
        )
    }
    var affected: Int64 = 0
    for row in store.rowsFor(table).values {
        let missing = row.doc[field] == nil
        if missing {
            row.doc[field] = value
            affected += 1
        }
    }
    return (DirectiveReport(op: "setDefault", affectedRows: affected), table)
}
