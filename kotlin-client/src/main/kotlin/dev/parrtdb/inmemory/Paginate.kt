package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.Order
import dev.parrtdb.wire.Paginate
import dev.parrtdb.wire.WireException
import dev.parrtdb.wire.WireJson
import dev.parrtdb.wire.jsonNum
import dev.parrtdb.wire.jsonObj
import dev.parrtdb.wire.jsonStr
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.util.Base64

private fun bad(message: String): RtDbException = RtDbException(ErrorCode.BAD_REQUEST, message)

/** One keyset sort column: an indexed field or a tiebreaker. */
internal enum class SortKind { Index, CreatedAt, Id }

internal class SortCol(val kind: SortKind, val field: String = "")

/** Pages the sorted btree result over [unbound index fields..., _creationTime, _id]. */
internal fun executePaginateTerminal(
    pag: Paginate,
    table: TableDef,
    filtered: List<StoredRow>,
    plan: ScanPlan,
    dir: Order,
): JsonElement {
    val cols = ArrayList<SortCol>()
    plan.indexDef?.let { idx -> for (f in idx.fields.drop(plan.typedEq.size)) cols += SortCol(SortKind.Index, f) }
    cols += SortCol(SortKind.CreatedAt)
    cols += SortCol(SortKind.Id)
    val colTypes =
        cols.map {
            when (it.kind) {
                SortKind.Index -> plan.fieldPg(table, it.field)
                SortKind.CreatedAt -> PgType.Number
                SortKind.Id -> PgType.Text
            }
        }
    return paginateResult(pag, table, filtered, cols, colTypes, dir)
}

private fun paginateResult(
    pag: Paginate,
    table: TableDef,
    sorted: List<StoredRow>,
    cols: List<SortCol>,
    colTypes: List<PgType>,
    dir: Order,
): JsonElement {
    val numItems = minOf(pag.numItems, maxTake).coerceAtLeast(0)
    var cursorValues: List<JsonElement>? = null
    pag.cursor?.let { c ->
        val decoded = decodeCursorForEngine(c)
        if (decoded.size != cols.size) {
            throw bad("cursor has ${decoded.size} value(s) but this query sorts over ${cols.size} column(s)")
        }
        validateCursorValues(decoded, cols, table)
        cursorValues = decoded
    }
    val cv = cursorValues
    val rows = if (cv != null) sorted.filter { isAfterCursor(it, cv, cols, colTypes, dir) } else sorted
    val hasNext = rows.size > numItems
    val page = if (rows.size > numItems) rows.subList(0, numItems) else rows
    val docs = JsonArray(page.map { mergeDoc(it) })
    val out = LinkedHashMap<String, JsonElement>()
    out["docs"] = docs
    if (hasNext && page.isNotEmpty()) {
        val last = page.last()
        out["nextCursor"] = jsonStr(encodeCursorForEngine(cols.map { sortValue(last, it) }))
    }
    return JsonObject(out)
}

/** Wraps codec failures into the engine's BAD_REQUEST surface. */
internal fun decodeCursorForEngine(s: String): List<JsonElement> {
    try {
        val raw = Base64.getDecoder().decode(s)
        val v = WireJson.parse(String(raw, Charsets.UTF_8))
        return (v as? JsonArray)?.toList() ?: throw WireException("cursor payload is not a JSON array")
    } catch (e: IllegalArgumentException) {
        // WireException is an IllegalArgumentException, as is Base64's decode failure.
        throw bad("invalid cursor: ${e.message}")
    }
}

/**
 * Standard base64 of the compact JSON array. Go's json.Marshal escapes `<`, `>`, `&`, U+2028 and
 * U+2029, so the same escapes are applied here to keep cursors byte-identical.
 */
internal fun encodeCursorForEngine(values: List<JsonElement>): String {
    val json =
        WireJson.encode(JsonArray(values))
            .replace("<", "\\u003c")
            .replace(">", "\\u003e")
            .replace("&", "\\u0026")
            .replace(" ", "\\u2028")
            .replace(" ", "\\u2029")
    return Base64.getEncoder().encodeToString(json.toByteArray(Charsets.UTF_8))
}

private fun validateCursorValues(values: List<JsonElement>, cols: List<SortCol>, table: TableDef) {
    for ((i, c) in cols.withIndex()) {
        val v = values[i]
        when (c.kind) {
            SortKind.Index -> if (!isNullValue(v)) coerceIndexValue(table, c.field, v)
            SortKind.CreatedAt -> if (!isJsonNumber(v)) throw bad("cursor value for created_at must be a number")
            SortKind.Id -> if (!(v is JsonPrimitive && v.isString)) throw bad("cursor value for id must be a string")
        }
    }
}

private fun isAfterCursor(row: StoredRow, cursorValues: List<JsonElement>, cols: List<SortCol>, colTypes: List<PgType>, dir: Order): Boolean =
    keysetIsAfter(cols.map { sortValue(row, it) }, cursorValues, colTypes, dir)

/** Resume predicate: strictly after in the sort direction, lexicographic OR-of-AND over the columns. */
internal fun keysetIsAfter(rowValues: List<JsonElement>, cursorValues: List<JsonElement>, colTypes: List<PgType>, dir: Order): Boolean {
    for (i in rowValues.indices) {
        var prefixEqual = true
        for (j in 0 until i) {
            if (compareIndexValues(rowValues[j], cursorValues[j], colTypes[j]) != 0) {
                prefixEqual = false
                break
            }
        }
        if (!prefixEqual) continue
        val c = compareIndexValues(rowValues[i], cursorValues[i], colTypes[i])
        val ahead = if (dir == Order.Desc) c < 0 else c > 0
        if (ahead) return true
    }
    return false
}

private fun sortValue(row: StoredRow, c: SortCol): JsonElement =
    when (c.kind) {
        SortKind.CreatedAt -> jsonNum(formatI64(row.createdAt))
        SortKind.Id -> jsonStr(row.id)
        SortKind.Index -> row.doc[c.field] ?: JsonNull
    }

/** One ranked candidate (search/vectorSearch paging): the synthetic rank, tiebreakers, and the merged doc. */
internal class RankedRow(val rank: JsonElement?, val doc: JsonObject) {
    val createdAt: Long = (doc["_creationTime"]?.takeIf { isJsonNumber(it) } as? JsonPrimitive)?.let { parseI64(it.content) } ?: 0L
    val id: String = (doc["_id"] as? JsonPrimitive)?.takeIf { it.isString }?.content ?: ""

    fun keyset(hasRank: Boolean): List<JsonElement> {
        val out = ArrayList<JsonElement>(3)
        if (hasRank) out += rank ?: JsonNull
        out += jsonNum(formatI64(createdAt))
        out += jsonStr(id)
        return out
    }
}

/** Orders by rank desc, then _creationTime desc, then _id desc. */
internal fun sortRankedRows(rows: List<RankedRow>): List<RankedRow> =
    rows.sortedWith { x, y ->
        val c = compareIndexValues(x.rank ?: JsonNull, y.rank ?: JsonNull, PgType.Number)
        when {
            c != 0 -> -c
            x.createdAt != y.createdAt -> if (x.createdAt > y.createdAt) -1 else 1
            else -> -cmpStrings(x.id, y.id)
        }
    }

/** Pages an already-ranked candidate list over [rank?, created_at, id], all desc. */
internal fun paginateRanked(pag: Paginate, rows: List<RankedRow>, hasRank: Boolean): JsonElement {
    val numItems = minOf(pag.numItems, maxTake).coerceAtLeast(0)
    val colTypes = if (hasRank) listOf(PgType.Number, PgType.Number, PgType.Text) else listOf(PgType.Number, PgType.Text)
    var cursorValues: List<JsonElement>? = null
    pag.cursor?.let { c ->
        val decoded = decodeCursorForEngine(c)
        if (decoded.size != colTypes.size) {
            throw bad("cursor has ${decoded.size} value(s) but this query sorts over ${colTypes.size} column(s)")
        }
        validateRankedCursorValues(decoded, hasRank)
        cursorValues = decoded
    }
    val cv = cursorValues
    var page = ArrayList<RankedRow>()
    for (row in rows) {
        if (cv != null && !keysetIsAfter(row.keyset(hasRank), cv, colTypes, Order.Desc)) continue
        page += row
        if (page.size > numItems) break
    }
    val hasNext = page.size > numItems
    if (page.size > numItems) page = ArrayList(page.subList(0, numItems))
    val out = LinkedHashMap<String, JsonElement>()
    out["docs"] = JsonArray(page.map { it.doc })
    if (hasNext && page.isNotEmpty()) out["nextCursor"] = jsonStr(encodeCursorForEngine(page.last().keyset(hasRank)))
    return JsonObject(out)
}

private fun validateRankedCursorValues(values: List<JsonElement>, hasRank: Boolean) {
    var tail = 0
    if (hasRank) {
        if (!isJsonNumber(values[0])) throw bad("cursor value for rank must be a number")
        tail = 1
    }
    if (!isJsonNumber(values[tail])) throw bad("cursor value for created_at must be a number")
    val idv = values[tail + 1]
    if (!(idv is JsonPrimitive && idv.isString)) throw bad("cursor value for id must be a string")
}
