package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.errors.badRequest
import dev.parrtdb.errors.schemaViolation
import dev.parrtdb.wire.Cast
import dev.parrtdb.wire.FilterExpr
import dev.parrtdb.wire.ValueExpr
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

private fun isPlainBtree(index: IndexDef): Boolean = !index.search && index.vector == null && !index.unique && index.whereClause == null

/**
 * Push-time schema validation (server schema::validate subset): authorize/index/TTL/updatedAt/
 * autoIncrement rules, then computed and constraint validation. Throws on the first violation.
 */
fun validateSchema(schema: SchemaDef) {
    for ((tableName, table) in schema.tables) {
        table.authorize?.let {
            rejectRelativeTimeFilter(it, ErrorCode.SCHEMA_VIOLATION, "olderThan filter is only allowed in patchByQuery/deleteByQuery filters")
        }
        for (index in table.indexes) {
            if (index.fields.isEmpty()) throw schemaViolation("index '${index.name}' on table '$tableName' has no fields")
            index.whereClause?.let {
                rejectRelativeTimeFilter(it, ErrorCode.BAD_REQUEST, "olderThan filter is not allowed in a partial-index predicate")
            }
            if (index.vector != null) continue
            for (fieldName in index.fields) {
                val fty =
                    table.fields[fieldName]
                        ?: throw schemaViolation("index '${index.name}' on table '$tableName' references unknown field '$fieldName'")
                val pg = indexColumnType(fty)
                if (index.search && pg.pg != PgType.Text) {
                    throw schemaViolation("search index '${index.name}' on table '$tableName' has non-text field '$fieldName'")
                }
            }
        }
        if (table.ttl != null) validateTtl(table)
        if (table.updatedAtField != null) validateUpdatedAtField(table)
        if (table.autoIncrementField != null) validateAutoIncrementField(table)
        validateComputedTable(table, tableName)
        validateConstraintsTable(table, tableName)
    }
}

private fun validateConstraintsTable(table: TableDef, tableName: String) {
    for ((field, fc) in table.constraints) {
        fun violate(detail: String): Nothing = throw schemaViolation("constraints on field '$field': $detail")
        var fty =
            table.fields[field]
                ?: throw schemaViolation("constraints key '$field' is not a declared field of table '$tableName'")
        while (fty.kind == "optional" && fty.inner != null) fty = fty.inner
        if (fc.min != null || fc.max != null) {
            if (fty.kind != "number" && fty.kind != "int64") violate("'min'/'max' is legal only on number/int64 fields")
            if (fty.kind == "int64") {
                for (b in listOf(fc.min, fc.max)) {
                    if (b != null && runCatching { jsonNumberI64(b) }.isFailure) {
                        violate("'min'/'max' bound must be an integer on an int64 field")
                    }
                }
            }
        }
        if ((fc.minLength != null || fc.maxLength != null || fc.pattern != null) && fty.kind != "string") {
            violate("'minLength'/'maxLength'/'pattern' is legal only on string fields")
        }
        if (fc.min != null && fc.max != null && jsonNumberF64(fc.min) > jsonNumberF64(fc.max)) violate("'min' exceeds 'max'")
        if (fc.minLength != null && fc.maxLength != null && fc.minLength > fc.maxLength) violate("'minLength' exceeds 'maxLength'")
        if (fc.pattern != null && compileRe2Pattern(fc.pattern) == null) violate("invalid pattern: '${fc.pattern}'")
    }
}

private fun validateTtl(table: TableDef) {
    val ttl = table.ttl!!
    val fty = table.fields[ttl.field] ?: throw schemaViolation("ttl.field '${ttl.field}' is not a declared field")
    if (fty.kind != "number" && fty.kind != "int64") throw schemaViolation("ttl.field '${ttl.field}' must be a number or bigint field")
    val hasIndex = table.indexes.any { isPlainBtree(it) && it.fields.size == 1 && it.fields[0] == ttl.field }
    if (!hasIndex) {
        throw schemaViolation("ttl.field '${ttl.field}' requires a single-field, non-unique, non-partial btree index on it")
    }
    if (ttl.defaultDurationMs != null && ttl.defaultDurationMs <= 0) throw schemaViolation("ttl.defaultDurationMs must be greater than 0")
}

private fun validateUpdatedAtField(table: TableDef) {
    val field = table.updatedAtField!!
    val fty = table.fields[field] ?: throw schemaViolation("updatedAtField '$field' is not a declared field")
    if (fty.kind != "number" && fty.kind != "int64") throw schemaViolation("updatedAtField '$field' must be a number or bigint field")
    if (table.ttl?.field == field) {
        throw schemaViolation(
            "updatedAtField '$field' must differ from ttl.field (both stamps write unconditionally; a shared field would drop the expiry)",
        )
    }
}

private fun validateAutoIncrementField(table: TableDef) {
    val field = table.autoIncrementField!!
    val fty = table.fields[field] ?: throw schemaViolation("autoIncrementField '$field' is not a declared field")
    if (fty.kind != "int64") throw schemaViolation("autoIncrementField '$field' must be an int64 field")
    if (table.ttl?.field == field) {
        throw schemaViolation("autoIncrementField '$field' must differ from ttl.field (the ttl reaper would delete counter rows)")
    }
    if (table.updatedAtField == field) {
        throw schemaViolation("autoIncrementField '$field' must differ from updatedAtField (the timestamp would overwrite the counter on every write)")
    }
}

private fun onDeleteDeclaration(ty: FieldType): Pair<String, OnDeleteAction>? {
    var inner = ty
    if (inner.kind == "optional" && inner.inner != null) inner = inner.inner
    return if (inner.kind == "id" && inner.onDelete != null) inner.table to inner.onDelete else null
}

/**
 * Validates onDelete declarations at push time (FM-30 sibling FM-33): legal only on a top-level id
 * field (or one optional wrapping it), the field needs a single-field plain btree index, setNull
 * needs an optional field, and the referenced table must exist.
 */
fun validateOnDelete(schema: SchemaDef) {
    for ((tableName, table) in schema.tables) {
        for ((fieldName, fieldTy) in table.fields) {
            val decl = onDeleteDeclaration(fieldTy)
            if (decl == null) {
                if (fieldHasNestedOnDelete(fieldTy)) {
                    throw schemaViolation(
                        "field '$fieldName' on table '$tableName': onDelete is legal only on a top-level id or optional-id field",
                    )
                }
                continue
            }
            if (decl.second == OnDeleteAction.SetNull && fieldTy.kind != "optional") {
                throw schemaViolation("onDelete 'setNull' requires the id field to be optional")
            }
            val hasIndex = table.indexes.any { isPlainBtree(it) && it.fields.size == 1 && it.fields[0] == fieldName }
            if (!hasIndex) {
                throw schemaViolation(
                    "onDelete field '$fieldName' on table '$tableName' requires a single-field, non-unique, non-partial btree index on it",
                )
            }
        }
    }
    for ((tableName, table) in schema.tables) {
        for ((fieldName, fieldTy) in table.fields) {
            val decl = onDeleteDeclaration(fieldTy) ?: continue
            if (decl.first !in schema.tables) {
                throw schemaViolation("onDelete field '$fieldName' on table '$tableName' references unknown table '${decl.first}'")
            }
        }
    }
}

/** True when an id field carrying onDelete appears anywhere in [ty] at any nesting depth. */
fun fieldHasNestedOnDelete(ty: FieldType): Boolean =
    when (ty.kind) {
        "id" -> ty.onDelete != null
        "optional", "array", "record" -> ty.inner != null && fieldHasNestedOnDelete(ty.inner)
        "union" -> ty.variants.any { fieldHasNestedOnDelete(it) }
        "object" -> ty.fields.values.any { fieldHasNestedOnDelete(it) }
        else -> false
    }

/** The onDelete action [ty] declares against [parentTable]; only a top-level id (or one optional wrapping it) can carry one. */
fun onDeleteRef(ty: FieldType, parentTable: String): OnDeleteAction? {
    if (ty.kind == "id" && ty.table == parentTable && ty.onDelete != null) return ty.onDelete
    if (ty.kind == "optional" && ty.inner != null) return onDeleteRef(ty.inner, parentTable)
    return null
}

/** True when ANY table declares an onDelete field referencing [parent] (the TTL reaper's bulk-vs-cascade branch). */
fun hasOnDeleteChildren(schema: SchemaDef, parent: String): Boolean =
    schema.tables.values.any { t -> t.fields.values.any { onDeleteRef(it, parent) != null } }

private enum class StaticKind(val sampleJson: String, val label: String) {
    Str("\"s\"", "a string"),
    Num("1", "a number"),
    Bool("true", "a boolean"),
}

private fun StaticKind.sample(): JsonElement =
    when (this) {
        StaticKind.Str -> dev.parrtdb.wire.jsonStr("s")
        StaticKind.Num -> dev.parrtdb.wire.jsonNum("1")
        StaticKind.Bool -> dev.parrtdb.wire.jsonBool(true)
    }

private fun inferStaticKind(ve: ValueExpr): StaticKind? =
    when (ve) {
        is ValueExpr.Field, is ValueExpr.Coalesce, is ValueExpr.Case -> null
        is ValueExpr.Literal ->
            when {
                strOf(ve.value) != null -> StaticKind.Str
                numOf(ve.value) != null -> StaticKind.Num
                boolOf(ve.value) != null -> StaticKind.Bool
                else -> null
            }
        is ValueExpr.Concat, is ValueExpr.Lower, is ValueExpr.Upper, is ValueExpr.Trim -> StaticKind.Str
        is ValueExpr.Add, is ValueExpr.Sub, is ValueExpr.Mul, is ValueExpr.Div, is ValueExpr.Now -> StaticKind.Num
        is ValueExpr.CastTo ->
            when (ve.to) {
                Cast.ToString -> StaticKind.Str
                Cast.ToNumber, Cast.ToInt64 -> StaticKind.Num
                Cast.ToBoolean -> StaticKind.Bool
            }
    }

private fun isPrincipalMarker(value: JsonElement): Boolean {
    val obj = value as? JsonObject ?: return false
    if (obj.size != 1) return false
    return boolOf(obj["\$user"]) == true || boolOf(obj["\$email"]) == true
}

/** Rejects the execution-time-relative `olderThan` leaf anywhere in [expr] with [code] and [message]. */
private fun rejectRelativeTimeFilter(expr: FilterExpr, code: ErrorCode, message: String) {
    fun found(e: FilterExpr): Boolean =
        when (e) {
            is FilterExpr.And -> e.exprs.any { found(it) }
            is FilterExpr.Or -> e.exprs.any { found(it) }
            is FilterExpr.Not -> found(e.expr)
            is FilterExpr.OlderThan -> true
            else -> false
        }
    if (found(expr)) throw RtDbException(code, message)
}

private fun validateComputedCaseWhens(ve: ValueExpr) {
    when (ve) {
        is ValueExpr.Case -> {
            for (cw in ve.whens) {
                rejectPrincipalMarkers(cw.whenExpr)
                rejectRelativeTimeFilter(cw.whenExpr, ErrorCode.BAD_REQUEST, "olderThan filter is only allowed in patchByQuery/deleteByQuery filters")
                validateComputedCaseWhens(cw.then)
            }
            validateComputedCaseWhens(ve.otherwise)
        }
        is ValueExpr.Concat -> ve.parts.forEach { validateComputedCaseWhens(it) }
        is ValueExpr.Coalesce -> ve.parts.forEach { validateComputedCaseWhens(it) }
        is ValueExpr.Add -> both(ve.left, ve.right)
        is ValueExpr.Sub -> both(ve.left, ve.right)
        is ValueExpr.Mul -> both(ve.left, ve.right)
        is ValueExpr.Div -> both(ve.left, ve.right)
        is ValueExpr.Lower -> validateComputedCaseWhens(ve.value)
        is ValueExpr.Upper -> validateComputedCaseWhens(ve.value)
        is ValueExpr.Trim -> validateComputedCaseWhens(ve.value)
        is ValueExpr.CastTo -> validateComputedCaseWhens(ve.value)
        else -> {}
    }
}

private fun both(l: ValueExpr, r: ValueExpr) {
    validateComputedCaseWhens(l)
    validateComputedCaseWhens(r)
}

private fun rejectPrincipalMarkers(expr: FilterExpr) {
    fun fail(field: String): Nothing =
        throw badRequest("principal markers ({\"\$user\":true}/{\"\$email\":true}) are not allowed in client filters (field '$field')")
    when (expr) {
        is FilterExpr.Eq -> if (isPrincipalMarker(expr.value)) fail(expr.field)
        is FilterExpr.Neq -> if (isPrincipalMarker(expr.value)) fail(expr.field)
        is FilterExpr.Gt -> if (isPrincipalMarker(expr.value)) fail(expr.field)
        is FilterExpr.Gte -> if (isPrincipalMarker(expr.value)) fail(expr.field)
        is FilterExpr.Lt -> if (isPrincipalMarker(expr.value)) fail(expr.field)
        is FilterExpr.Lte -> if (isPrincipalMarker(expr.value)) fail(expr.field)
        is FilterExpr.Contains -> if (isPrincipalMarker(expr.value)) fail(expr.field)
        is FilterExpr.In -> if (expr.values.any { isPrincipalMarker(it) }) fail(expr.field)
        is FilterExpr.And -> expr.exprs.forEach { rejectPrincipalMarkers(it) }
        is FilterExpr.Or -> expr.exprs.forEach { rejectPrincipalMarkers(it) }
        is FilterExpr.Not -> rejectPrincipalMarkers(expr.expr)
        else -> {}
    }
}

private fun walkValueExprFields(ve: ValueExpr, visit: (String) -> Unit) {
    when (ve) {
        is ValueExpr.Field -> visit(ve.field)
        is ValueExpr.Literal, is ValueExpr.Now -> {}
        is ValueExpr.Concat -> ve.parts.forEach { walkValueExprFields(it, visit) }
        is ValueExpr.Coalesce -> ve.parts.forEach { walkValueExprFields(it, visit) }
        is ValueExpr.Add -> { walkValueExprFields(ve.left, visit); walkValueExprFields(ve.right, visit) }
        is ValueExpr.Sub -> { walkValueExprFields(ve.left, visit); walkValueExprFields(ve.right, visit) }
        is ValueExpr.Mul -> { walkValueExprFields(ve.left, visit); walkValueExprFields(ve.right, visit) }
        is ValueExpr.Div -> { walkValueExprFields(ve.left, visit); walkValueExprFields(ve.right, visit) }
        is ValueExpr.Lower -> walkValueExprFields(ve.value, visit)
        is ValueExpr.Upper -> walkValueExprFields(ve.value, visit)
        is ValueExpr.Trim -> walkValueExprFields(ve.value, visit)
        is ValueExpr.CastTo -> walkValueExprFields(ve.value, visit)
        is ValueExpr.Case -> {
            for (cw in ve.whens) {
                walkFilterExprFieldNames(cw.whenExpr, visit)
                walkValueExprFields(cw.then, visit)
            }
            walkValueExprFields(ve.otherwise, visit)
        }
    }
}

/** Computed-field push validation (server TableDef::validate_computed). BAD_REQUEST on every rule. */
private fun validateComputedTable(table: TableDef, tableName: String) {
    for ((field, expr) in table.computed) {
        val label = "computed field '$tableName.$field'"
        if (field !in table.fields) throw badRequest("$label is not a declared field")
        if (table.ownerField == field) throw badRequest("$label must not be the table's ownerField")
        if (table.collaboratorsField == field) throw badRequest("$label must not be the table's collaboratorsField")
        if (table.autoIncrementField == field) throw badRequest("$label must not be the table's autoIncrementField")
        var offender: String? = null
        walkValueExprFields(expr) { ref ->
            if (offender == null) {
                if (ref !in table.fields) {
                    offender = "$label references undeclared field '$ref'"
                } else if (ref in table.computed) {
                    offender = "$label references computed field '$ref' (computed fields may not reference each other)"
                }
            }
        }
        offender?.let { throw badRequest(it) }
        validateComputedCaseWhens(expr)
        val kind = inferStaticKind(expr)
        if (kind != null) {
            val declared = table.fields[field] ?: continue
            var inner = declared
            while (inner.kind == "optional" && inner.inner != null) inner = inner.inner
            val accepts = validateValue(declared, kind.sample()) || (inner.kind == "int64" && kind == StaticKind.Str)
            if (!accepts) throw badRequest("$label produces ${kind.label}, which the field type does not accept")
        }
    }
    table.authorize?.let { auth ->
        var offender: String? = null
        walkFilterExprFieldNames(auth) { ref -> if (offender == null && ref in table.computed) offender = ref }
        offender?.let {
            throw badRequest(
                "computed field '$tableName.$it' must not be referenced by the table's authorize predicate " +
                    "(authorize predicates may not reference computed fields)",
            )
        }
    }
}

/** Validates every table's computed map; also used by migrate after directive folding. */
internal fun validateComputed(schema: SchemaDef) {
    for ((tableName, table) in schema.tables) validateComputedTable(table, tableName)
}
