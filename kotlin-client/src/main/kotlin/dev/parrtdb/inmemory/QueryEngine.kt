package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.FilterExpr
import dev.parrtdb.wire.Order
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.jsonNum
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

private fun bad(message: String): RtDbException = RtDbException(ErrorCode.BAD_REQUEST, message)

/**
 * The single read entry the client surface and corpus harness call. Returns the terminal result:
 * get/first/unique give a merged doc or null, count a number, take/collect an array, paginate
 * `{docs, nextCursor?}`. Takes the store lock; filter validation runs before any row is touched.
 */
fun evalQuery(s: Store, q: Query): JsonElement = s.locked { evalQueryLocked(s, q) }

/** [evalQuery] for callers that already hold the store lock. */
fun evalQueryLocked(s: Store, q: Query): JsonElement {
    val table = requireTable(s, q.table)
    // Lookup and projection validation run before every early return (server compile_query).
    val aliases = validateLookups(s, q, table)
    q.fields?.let { validateProjection(table, it, aliases) }
    var result = executeQuery(s, q, table)
    if (!q.lookups.isNullOrEmpty()) result = expandLookups(s, q, result)
    if (q.fields != null) result = projectResult(result, q)
    return result
}

/** Looks up a table definition or throws NOT_FOUND. */
fun requireTable(s: Store, name: String): TableDef =
    s.tables[name] ?: throw RtDbException(ErrorCode.NOT_FOUND, "table '$name' does not exist")

/** Looks up an index on [table] or throws BAD_REQUEST. */
fun requireIndex(table: TableDef, name: String): IndexDef =
    table.indexes.firstOrNull { it.name == name } ?: throw bad("index '$name' not found")

/** Live (non-soft-deleted) merged docs of [table], in store order. */
fun collectAllLocked(s: Store, table: String): JsonArray {
    val out = ArrayList<JsonElement>()
    for ((key, row) in s.docs) {
        if (key.table != table || row.deletedAt != null) continue
        out += mergeDoc(row)
    }
    return JsonArray(out)
}

private fun executeQuery(s: Store, q: Query, table: TableDef): JsonElement {
    val hasRange = q.gt != null || q.gte != null || q.lt != null || q.lte != null
    checkQueryCombinations(q)
    if (q.take != null && q.take > maxTake) throw bad("take exceeds maximum of $maxTake")

    q.get?.let { id ->
        val row = s.docs[RowKey(q.table, id)]
        return if (row == null || row.deletedAt != null) JsonNull else mergeDoc(row)
    }
    q.vectorSearch?.let { return executeVectorSearchTerminal(s, q, it, table) }
    q.hybridSearch?.let { return executeHybridSearchTerminal(s, q, it) }
    q.search?.let { return executeSearchTerminal(s, q, it, table) }

    val plan = prepareScan(q, table, hasRange)
    val filtered = fetchFilteredRows(s, q, plan, table.fields)

    if (q.count) return jsonNum(formatI64(filtered.size.toLong()))
    if (q.distinct) return executeDistinctTerminal(table, plan, filtered)
    q.aggregate?.let { return executeAggregateTerminal(it, table, plan, filtered) }

    val dir = q.order ?: Order.Asc
    val sorted = sortFilteredRows(filtered, table, plan, dir)
    q.paginate?.let { return executePaginateTerminal(it, table, sorted, plan, dir) }
    return executeCollectTerminal(q, sorted)
}

/** Everything the row scan needs besides the query itself. */
internal class ScanPlan(
    val indexDef: IndexDef?,
    val typedEq: List<JsonElement>,
    val rangeField: String,
    val rangeFieldPg: PgType,
    val gt: JsonElement?,
    val gte: JsonElement?,
    val lt: JsonElement?,
    val lte: JsonElement?,
) {
    /** A field's indexed storage type; Text when undeclared or non-indexable (shared defensive fallback). */
    fun fieldPg(table: TableDef, field: String): PgType {
        val fty = table.fields[field] ?: return PgType.Text
        return try {
            indexColumnType(fty).pg
        } catch (e: RtDbException) {
            PgType.Text
        }
    }
}

/** Resolves the index, binds the eq prefix, coerces range bounds, validates the filter once. */
private fun prepareScan(q: Query, table: TableDef, hasRange: Boolean): ScanPlan {
    val indexDef = q.index?.let { requireIndex(table, it) }
    val eq = q.eq ?: emptyList()
    if (indexDef == null && eq.isNotEmpty()) throw bad("eq requires an index")
    if (indexDef != null && eq.size > indexDef.fields.size) {
        throw bad("index '${indexDef.name}' expects at most ${indexDef.fields.size} eq value(s), got ${eq.size}")
    }
    val typedEq = ArrayList<JsonElement>()
    if (indexDef != null) {
        for ((i, value) in eq.withIndex()) typedEq += coerceIndexValue(table, indexDef.fields[i], value)
    }
    var rangeField = ""
    var rangePg = PgType.Text
    var gt: JsonElement? = null
    var gte: JsonElement? = null
    var lt: JsonElement? = null
    var lte: JsonElement? = null
    if (hasRange) {
        if (indexDef == null) throw bad("range bound requires an index")
        if (eq.size >= indexDef.fields.size) throw bad("range bound requires a remaining index field after eq")
        rangeField = indexDef.fields[eq.size]
        val probe = ScanPlan(indexDef, typedEq, rangeField, PgType.Text, null, null, null, null)
        rangePg = probe.fieldPg(table, rangeField)
        gt = q.gt?.let { coerceIndexValue(table, rangeField, it) }
        gte = q.gte?.let { coerceIndexValue(table, rangeField, it) }
        lt = q.lt?.let { coerceIndexValue(table, rangeField, it) }
        lte = q.lte?.let { coerceIndexValue(table, rangeField, it) }
    }
    q.filter?.let { validateFilterExpr(it, table) }
    return ScanPlan(indexDef, typedEq, rangeField, rangePg, gt, gte, lt, lte)
}

/** Eq prefix, then range, then filter, over the live rows of the table. */
private fun fetchFilteredRows(s: Store, q: Query, plan: ScanPlan, fields: Map<String, FieldType>): List<StoredRow> {
    val filter: FilterExpr? = q.filter
    val out = ArrayList<StoredRow>()
    for ((key, row) in s.docs) {
        if (key.table != q.table || row.deletedAt != null) continue
        val idx = plan.indexDef
        if (idx != null) {
            var ok = true
            for ((i, tv) in plan.typedEq.withIndex()) {
                val v = row.doc[idx.fields[i]]
                if (v == null || isNullValue(v) || !jsonEq(v, tv)) {
                    ok = false
                    break
                }
            }
            if (!ok) continue
        }
        if (plan.rangeField.isNotEmpty()) {
            val v = row.doc[plan.rangeField]
            if (v == null || isNullValue(v)) continue
            val pg = plan.rangeFieldPg
            if (plan.gt != null && compareIndexValues(v, plan.gt, pg) <= 0) continue
            if (plan.gte != null && compareIndexValues(v, plan.gte, pg) < 0) continue
            if (plan.lt != null && compareIndexValues(v, plan.lt, pg) >= 0) continue
            if (plan.lte != null && compareIndexValues(v, plan.lte, pg) > 0) continue
        }
        if (filter != null && !matchesFilter(filter, row.doc, fields)) continue
        out += row
    }
    return out
}

private fun executeCollectTerminal(q: Query, filtered: List<StoredRow>): JsonElement {
    if (q.unique) {
        if (filtered.size > 1) throw RtDbException(ErrorCode.PRECONDITION_FAILED, "unique query matched multiple documents")
        return if (filtered.isEmpty()) JsonNull else mergeDoc(filtered[0])
    }
    if (q.first) return if (filtered.isEmpty()) JsonNull else mergeDoc(filtered[0])
    val limit = minOf(q.take ?: maxTake, maxTake).coerceAtLeast(0)
    return JsonArray(filtered.take(limit).map { mergeDoc(it) })
}

/** Orders by unbound index fields after the eq prefix, then _creationTime, then _id (total order). */
private fun sortFilteredRows(filtered: List<StoredRow>, table: TableDef, plan: ScanPlan, dir: Order): List<StoredRow> {
    val sortFields = plan.indexDef?.fields?.drop(plan.typedEq.size) ?: emptyList()
    val sortPgs = sortFields.map { plan.fieldPg(table, it) }
    val sign = if (dir == Order.Desc) -1 else 1
    return filtered.sortedWith { x, y ->
        for ((i, field) in sortFields.withIndex()) {
            val c = compareIndexValues(x.doc[field] ?: JsonNull, y.doc[field] ?: JsonNull, sortPgs[i])
            if (c != 0) return@sortedWith sign * c
        }
        if (x.createdAt != y.createdAt) return@sortedWith sign * (if (x.createdAt < y.createdAt) -1 else 1)
        sign * cmpStrings(x.id, y.id)
    }
}

/** Compares strings in UTF-8 byte order (code point order), matching Go's string comparison. */
internal fun cmpStrings(a: String, b: String): Int {
    var i = 0
    var j = 0
    while (i < a.length && j < b.length) {
        val ca = a.codePointAt(i)
        val cb = b.codePointAt(j)
        if (ca != cb) return if (ca < cb) -1 else 1
        i += Character.charCount(ca)
        j += Character.charCount(cb)
    }
    return when {
        i < a.length -> 1
        j < b.length -> -1
        else -> 0
    }
}

private fun cmpF64(a: Double, b: Double): Int = if (a < b) -1 else if (a > b) 1 else 0

/**
 * Null-sorts-last comparison for one index sort key: numbers numerically, strings
 * lexicographically, booleans false<true; int64 columns parse the decimal strings. Mixed kinds
 * compare equal. Returns -1, 0 or 1.
 */
fun compareIndexValues(a: JsonElement, b: JsonElement, pg: PgType): Int {
    val aNull = isNullValue(a)
    val bNull = isNullValue(b)
    if (aNull && bNull) return 0
    if (aNull) return 1
    if (bNull) return -1
    if (pg == PgType.Int64) {
        val an = (a as? JsonPrimitive)?.takeIf { it.isString }?.let { parseI64(it.content) } ?: Long.MIN_VALUE
        val bn = (b as? JsonPrimitive)?.takeIf { it.isString }?.let { parseI64(it.content) } ?: Long.MIN_VALUE
        return an.compareTo(bn).coerceIn(-1, 1)
    }
    if (a !is JsonPrimitive || b !is JsonPrimitive) return 0
    return when {
        a.isString -> if (b.isString) cmpStrings(a.content, b.content) else 0
        a.content == "true" || a.content == "false" ->
            if (b.isString || (b.content != "true" && b.content != "false")) {
                0
            } else {
                (a.content == "true").compareTo(b.content == "true").coerceIn(-1, 1)
            }
        else ->
            if (b.isString || !isJsonNumber(b)) 0 else cmpF64(jsonNumberF64(a), jsonNumberF64(b))
    }
}

private fun validateProjection(table: TableDef, fields: List<String>, lookupAliases: List<String>) {
    for (name in fields) {
        if (name == "_id" || name == "_creationTime" || name == "_version") continue
        if (table.fields.containsKey(name)) continue
        if (name in lookupAliases) continue
        throw bad("unknown projection field '$name'")
    }
}

/**
 * Applies [f] to every doc-bearing result shape by the query's terminal (never sniffed from the
 * value). Doc-less terminals pass through.
 */
private fun mapResultDocs(q: Query, result: JsonElement, f: (JsonObject) -> JsonObject): JsonElement {
    if (q.count || q.distinct || q.aggregate != null) return result
    if (q.paginate != null) {
        val obj = result as? JsonObject ?: return result
        val docs = obj["docs"] as? JsonArray ?: return result
        val mapped = JsonArray(docs.map { if (it is JsonObject) f(it) else it })
        return JsonObject(LinkedHashMap(obj).also { it["docs"] = mapped })
    }
    return when (result) {
        is JsonObject -> f(result)
        is JsonArray -> JsonArray(result.map { if (it is JsonObject) f(it) else it })
        else -> result
    }
}

/** Keeps each doc's `_`-prefixed keys (system and synthetic) plus the listed user fields. */
private fun projectResult(result: JsonElement, q: Query): JsonElement {
    val keep = q.fields?.toSet() ?: return result
    return mapResultDocs(q, result) { doc -> JsonObject(doc.filterKeys { it.startsWith("_") || it in keep }) }
}

internal fun executeDistinctTerminal(table: TableDef, plan: ScanPlan, filtered: List<StoredRow>): JsonElement {
    val idx = plan.indexDef
    if (idx == null || plan.typedEq.size >= idx.fields.size) throw bad("distinct requires an index field beyond the eq prefix")
    val field = idx.fields[plan.typedEq.size]
    val pg = plan.fieldPg(table, field)
    val seen = HashSet<String>()
    val values = ArrayList<JsonElement>()
    for (row in filtered) {
        val v = row.doc[field] ?: JsonNull
        if (seen.add(canonical(v))) values += v
    }
    val sorted = values.sortedWith { x, y -> compareIndexValues(x, y, pg) }
    return JsonArray(sorted.take(maxTake))
}
