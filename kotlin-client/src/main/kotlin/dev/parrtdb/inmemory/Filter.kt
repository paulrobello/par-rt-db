package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.errors.badRequest
import dev.parrtdb.wire.FilterExpr
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject

/** SEC-007 ceiling on and/or/not nesting (server filter::MAX_FILTER_DEPTH). */
private const val MAX_FILTER_DEPTH = 32

/** SEC-007 ceiling on an `in` list (server filter::MAX_IN_VALUES). */
private const val MAX_IN_VALUES = 1000

/** Indexed-column storage type (server indexed_column_type). */
enum class PgType { Text, Number, Boolean, Int64 }

/** Storage kind plus nullability of an indexable column. */
data class IndexedType(val pg: PgType, val nullable: Boolean)

/**
 * Read-context structural validation of a FilterExpr against a table's declared fields.
 * `olderThan` is rejected here; only by-query step filters admit it ([validateByQueryFilter]).
 */
fun validateFilterExpr(expr: FilterExpr, table: TableDef) = validateFilterExprAt(expr, table, false, 1)

/** The patchByQuery/deleteByQuery chokepoint, the one filter context that accepts `olderThan`. */
fun validateByQueryFilter(expr: FilterExpr, table: TableDef) = validateFilterExprAt(expr, table, true, 1)

private fun leafOf(e: FilterExpr): Pair<String, JsonElement>? =
    when (e) {
        is FilterExpr.Eq -> e.field to e.value
        is FilterExpr.Neq -> e.field to e.value
        is FilterExpr.Gt -> e.field to e.value
        is FilterExpr.Gte -> e.field to e.value
        is FilterExpr.Lt -> e.field to e.value
        is FilterExpr.Lte -> e.field to e.value
        else -> null
    }

private fun validateFilterExprAt(expr: FilterExpr, table: TableDef, allowRelativeTime: Boolean, depth: Int) {
    if (depth > MAX_FILTER_DEPTH) throw badRequest("filter nesting exceeds $MAX_FILTER_DEPTH levels")
    when (expr) {
        is FilterExpr.And -> {
            if (expr.exprs.isEmpty()) throw badRequest("and filter requires at least one expr")
            expr.exprs.forEach { validateFilterExprAt(it, table, allowRelativeTime, depth + 1) }
        }
        is FilterExpr.Or -> {
            if (expr.exprs.isEmpty()) throw badRequest("or filter requires at least one expr")
            expr.exprs.forEach { validateFilterExprAt(it, table, allowRelativeTime, depth + 1) }
        }
        is FilterExpr.Not -> validateFilterExprAt(expr.expr, table, allowRelativeTime, depth + 1)
        is FilterExpr.In -> {
            if (expr.values.isEmpty()) throw badRequest("in filter requires at least one value")
            if (expr.values.size > MAX_IN_VALUES) throw badRequest("in: at most $MAX_IN_VALUES values")
            expr.values.forEach { checkLeafValue(expr.field, it, table) }
            val first = inValueKind(expr.values[0])
            for (v in expr.values.drop(1)) {
                if (inValueKind(v) != first) throw badRequest("in filter values must all be the same type")
            }
            expr.values.forEach { checkLeafKind(expr.field, it, table) }
        }
        is FilterExpr.Eq, is FilterExpr.Neq, is FilterExpr.Gt, is FilterExpr.Gte, is FilterExpr.Lt, is FilterExpr.Lte -> {
            val (field, value) = leafOf(expr)!!
            checkLeaf(field, value, table)
        }
        is FilterExpr.Contains -> checkLeaf(expr.field, expr.value, table)
        is FilterExpr.OlderThan -> {
            if (!allowRelativeTime) throw badRequest("olderThan filter is only allowed in patchByQuery/deleteByQuery filters")
            if (expr.ms < 0) throw badRequest("olderThan ms must be >= 0")
            var fty = leafFieldType(expr.field, table)
            if (fty.kind == "optional") fty = fty.inner!!
            if (fty.kind != "number" && fty.kind != "int64") {
                throw badRequest("field '${expr.field}' must be a number or int64 field for olderThan")
            }
        }
        is FilterExpr.Exists -> leafFieldType(expr.field, table)
    }
}

private fun leafFieldType(field: String, table: TableDef): FieldType =
    table.fields[field] ?: throw badRequest("filter references unknown field '$field'")

private fun checkLeafValue(field: String, value: JsonElement, table: TableDef) {
    leafFieldType(field, table)
    if (strOf(value) == null && numOf(value) == null && boolOf(value) == null) {
        throw badRequest("filter value must be a string, number, or boolean")
    }
}

private fun checkLeaf(field: String, value: JsonElement, table: TableDef) {
    checkLeafValue(field, value, table)
    checkLeafKind(field, value, table)
}

/**
 * SEC-126 type check: an indexed field types the value through the same eq-bind conversion as index
 * eq prefixes; any other declared field requires only the value's JSON kind to match.
 */
private fun checkLeafKind(field: String, value: JsonElement, table: TableDef) {
    val fty = leafFieldType(field, table)
    for (idx in table.indexes) {
        if (field in idx.fields) {
            coerceIndexValue(table, field, value)
            return
        }
    }
    validateJsonbComparisonValue(field, fty, value)
}

private fun validateJsonbComparisonValue(field: String, ty: FieldType, value: JsonElement) {
    val inner = if (ty.kind == "optional") ty.inner!! else ty
    val ok =
        when (inner.kind) {
            "string", "id", "bytes" -> strOf(value) != null
            "number", "int64" -> isJsonNumber(value)
            "boolean" -> boolOf(value) != null
            else -> strOf(value) != null || numOf(value) != null || boolOf(value) != null
        }
    if (!ok) throw badRequest("filter on field '$field' value kind does not match declared field type")
}

private fun inValueKind(v: JsonElement): String =
    when {
        strOf(v) != null -> "string"
        numOf(v) != null -> "number"
        boolOf(v) != null -> "boolean"
        else -> "other"
    }

/** Resolves a field type's indexable storage column; SCHEMA_VIOLATION for a non-indexable type. */
fun indexColumnType(ty: FieldType): IndexedType {
    when (ty.kind) {
        "string", "id" -> return IndexedType(PgType.Text, false)
        "number" -> return IndexedType(PgType.Number, false)
        "int64" -> return IndexedType(PgType.Int64, false)
        "boolean" -> return IndexedType(PgType.Boolean, false)
        "literal" -> if (strOf(ty.value) != null) return IndexedType(PgType.Text, false)
        "union" -> {
            val all = ty.variants.isNotEmpty() && ty.variants.all { it.kind == "literal" && strOf(it.value) != null }
            if (all) return IndexedType(PgType.Text, false)
        }
        "optional" -> return indexColumnType(ty.inner!!).copy(nullable = true)
    }
    throw RtDbException(ErrorCode.SCHEMA_VIOLATION, "field type '${ty.kind}' is not indexable")
}

/** Type-checks an eq/range bind value against the field's indexed storage type; the value passes through unchanged. */
fun coerceIndexValue(table: TableDef, fieldName: String, value: JsonElement): JsonElement {
    val fty = table.fields[fieldName] ?: throw RtDbException(ErrorCode.INTERNAL, "index references unknown field '$fieldName'")
    when (indexColumnType(fty).pg) {
        PgType.Text -> if (strOf(value) == null) throw badRequest("eq value must be a string")
        PgType.Number -> if (!isJsonNumber(value)) throw badRequest("eq value must be a number")
        PgType.Int64 -> if (!isInt64String(value)) throw badRequest("eq value must be an int64 string")
        PgType.Boolean -> if (boolOf(value) == null) throw badRequest("eq value must be a boolean")
    }
    return value
}

/**
 * Evaluates a validated FilterExpr against a merged doc. A null/absent field never matches
 * (SQL NULL exclusion); `olderThan` fail-closes to false in this read-context entry point.
 */
fun matchesFilter(expr: FilterExpr, doc: JsonObject, fields: Map<String, FieldType>): Boolean =
    evalFilterWithClock(expr, doc, fields, 0, false)

/** By-query evaluator: an `olderThan` leaf compares the doc value against now minus ms (strict less-than). */
fun matchesFilterAt(expr: FilterExpr, doc: JsonObject, fields: Map<String, FieldType>, now: Long): Boolean =
    evalFilterWithClock(expr, doc, fields, now, true)

private enum class FilterOp { Eq, Neq, Gt, Gte, Lt, Lte }

private fun evalFilterWithClock(expr: FilterExpr, doc: JsonObject, fields: Map<String, FieldType>, now: Long, hasNow: Boolean): Boolean =
    when (expr) {
        is FilterExpr.And -> expr.exprs.all { evalFilterWithClock(it, doc, fields, now, hasNow) }
        is FilterExpr.Or -> expr.exprs.any { evalFilterWithClock(it, doc, fields, now, hasNow) }
        is FilterExpr.Not -> !evalFilterWithClock(expr.expr, doc, fields, now, hasNow)
        is FilterExpr.In -> expr.values.any { compareLeaf(FilterOp.Eq, expr.field, it, doc, fields) }
        is FilterExpr.Eq -> compareLeaf(FilterOp.Eq, expr.field, expr.value, doc, fields)
        is FilterExpr.Neq -> compareLeaf(FilterOp.Neq, expr.field, expr.value, doc, fields)
        is FilterExpr.Gt -> compareLeaf(FilterOp.Gt, expr.field, expr.value, doc, fields)
        is FilterExpr.Gte -> compareLeaf(FilterOp.Gte, expr.field, expr.value, doc, fields)
        is FilterExpr.Lt -> compareLeaf(FilterOp.Lt, expr.field, expr.value, doc, fields)
        is FilterExpr.Lte -> compareLeaf(FilterOp.Lte, expr.field, expr.value, doc, fields)
        is FilterExpr.Contains -> (doc[expr.field] as? JsonArray)?.any { jsonEq(it, expr.value) } ?: false
        is FilterExpr.Exists -> doc[expr.field].let { it != null && it !is JsonNull }
        is FilterExpr.OlderThan -> olderThan(expr, doc, fields, now, hasNow)
    }

private fun olderThan(e: FilterExpr.OlderThan, doc: JsonObject, fields: Map<String, FieldType>, now: Long, hasNow: Boolean): Boolean {
    if (!hasNow) return false
    var cutoff = now - e.ms
    if (cutoff > now) cutoff = Long.MIN_VALUE
    val dv = doc[e.field] ?: return false
    if (dv is JsonNull) return false
    if (isInt64Field(fields[e.field])) {
        val text = strOf(dv) ?: numOf(dv)?.content ?: return false
        return parseI64(text)?.let { it < cutoff } ?: false
    }
    val lhs = docToNumber(dv) ?: return false
    return lhs < cutoff.toDouble()
}

private fun compareLeaf(op: FilterOp, field: String, filterValue: JsonElement, doc: JsonObject, fields: Map<String, FieldType>): Boolean {
    val dv = doc[field] ?: return false
    if (dv is JsonNull) return false
    val fs = strOf(filterValue)
    if (fs != null && isInt64Field(fields[field])) {
        val docStr = strOf(dv) ?: return false
        val lhs = parseI64(docStr) ?: return false
        val rhs = parseI64(fs) ?: return false
        return cmp(op, lhs.compareTo(rhs))
    }
    if (fs != null) return cmp(op, compareUtf8(docToText(dv), fs))
    numOf(filterValue)?.let { fv ->
        val lhs = docToNumber(dv) ?: return false
        val rhs = jsonNumberF64(fv)
        // IEEE semantics: NaN compares false everywhere except neq.
        return when (op) {
            FilterOp.Eq -> lhs == rhs
            FilterOp.Neq -> lhs != rhs
            FilterOp.Gt -> lhs > rhs
            FilterOp.Gte -> lhs >= rhs
            FilterOp.Lt -> lhs < rhs
            FilterOp.Lte -> lhs <= rhs
        }
    }
    val fb = boolOf(filterValue) ?: return false
    val db = boolOf(dv) ?: return false
    return cmp(op, db.compareTo(fb))
}

private fun cmp(op: FilterOp, c: Int): Boolean =
    when (op) {
        FilterOp.Eq -> c == 0
        FilterOp.Neq -> c != 0
        FilterOp.Gt -> c > 0
        FilterOp.Gte -> c >= 0
        FilterOp.Lt -> c < 0
        FilterOp.Lte -> c <= 0
    }

private fun isInt64Field(ty: FieldType?): Boolean =
    ty != null && (ty.kind == "int64" || (ty.kind == "optional" && ty.inner?.kind == "int64"))

/** Postgres `doc->>'field'`: the JSON text of the value (integer-valued numbers render without a decimal point). */
private fun docToText(v: JsonElement): String =
    when {
        strOf(v) != null -> strOf(v)!!
        numOf(v) != null -> jsonNumberText(v)
        boolOf(v) != null -> if (boolOf(v)!!) "true" else "false"
        else -> compactJson(v)
    }

/** Postgres `(doc->>'field')::float8`: a finite number, or a parsed numeric string. */
private fun docToNumber(v: JsonElement): Double? {
    numOf(v)?.let {
        val f = jsonNumberF64(it)
        return if (f.isInfinite() || f.isNaN()) null else f
    }
    strOf(v)?.let { s ->
        val t = s.trim { c -> c.isWhitespace() }
        if (t.isEmpty()) return null
        val f = strictParseFloat(t) ?: return null
        return if (f.isInfinite() || f.isNaN()) null else f
    }
    return null
}

/** Visits every field name a FilterExpr references. */
fun walkFilterExprFieldNames(expr: FilterExpr, visit: (String) -> Unit) {
    when (expr) {
        is FilterExpr.And -> expr.exprs.forEach { walkFilterExprFieldNames(it, visit) }
        is FilterExpr.Or -> expr.exprs.forEach { walkFilterExprFieldNames(it, visit) }
        is FilterExpr.Not -> walkFilterExprFieldNames(expr.expr, visit)
        is FilterExpr.Exists -> visit(expr.field)
        is FilterExpr.OlderThan -> visit(expr.field)
        is FilterExpr.In -> visit(expr.field)
        is FilterExpr.Contains -> visit(expr.field)
        else -> leafOf(expr)?.first?.takeIf { it.isNotEmpty() }?.let(visit)
    }
}
