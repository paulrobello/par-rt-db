package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.Query
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

private fun bad(message: String): RtDbException = RtDbException(ErrorCode.BAD_REQUEST, message)

private const val MAX_LOOKUPS = 4
private const val MAX_LOOKUP_CAP = 1000
private const val DEFAULT_LOOKUP_CAP = 100
private const val MAX_JOINED_ROWS = 8192

/**
 * Compile-time lookup validation (server `validate_lookups`). Returns the aliases the projection
 * gate admits.
 */
internal fun validateLookups(s: Store, q: Query, table: TableDef): List<String> {
    val lookups = q.lookups
    if (lookups.isNullOrEmpty()) return emptyList()
    if (lookups.size > MAX_LOOKUPS) throw bad("at most $MAX_LOOKUPS lookups per query")
    val aliases = ArrayList<String>(lookups.size)
    for (lk in lookups) {
        requireTable(s, lk.table)
        val ty = table.fields[lk.field] ?: throw bad("lookup field '${lk.field}' is not declared on table '${q.table}'")
        var inner = ty
        if (inner.kind == "optional") inner = inner.inner!!
        if (inner.kind == "array") inner = inner.inner!!
        if (inner.kind != "id") throw bad("lookup field '${lk.field}' must be declared Id, Optional<Id>, or Array<Id>")
        if (inner.table != lk.table) {
            throw bad("lookup field '${lk.field}' declares Id table '${inner.table}' but the lookup names '${lk.table}'")
        }
        if (lk.alias.startsWith("_")) throw bad("lookup alias '${lk.alias}' must not start with '_'")
        if (table.fields.containsKey(lk.alias)) throw bad("lookup alias '${lk.alias}' collides with a declared field")
        if (lk.alias in aliases) throw bad("lookup alias '${lk.alias}' collides with another lookup alias")
        if (lk.cap != null && lk.cap > MAX_LOOKUP_CAP) throw bad("lookup cap exceeds maximum of $MAX_LOOKUP_CAP")
        aliases += lk.alias
    }
    return aliases
}

private fun isArrayLookup(ty: FieldType): Boolean {
    var inner = ty
    if (inner.kind == "optional") inner = inner.inner!!
    return inner.kind == "array"
}


private fun collectFieldIds(value: JsonElement?, ids: MutableList<String>, seen: MutableSet<String>) {
    when (value) {
        is JsonArray -> for (idv in value) strOf(idv)?.let { if (seen.add(it)) ids += it }
        is JsonPrimitive -> strOf(value)?.let { if (seen.add(it)) ids += it }
        else -> {}
    }
}

/** Routes by result shape like the Go port: paginate's `docs`, then single-doc terminals, then arrays. */
private fun lookupResultDocs(q: Query, result: JsonElement): List<JsonObject> {
    if (result is JsonObject) {
        val docs = result["docs"]
        if (docs is JsonArray) return docs.filterIsInstance<JsonObject>()
    }
    if (q.count || q.distinct || q.aggregate != null) return emptyList()
    if (q.get != null || q.unique || q.first) return if (result is JsonObject) listOf(result) else emptyList()
    return if (result is JsonArray) result.filterIsInstance<JsonObject>() else emptyList()
}

/** Rebuilds [result] with each doc replaced by its entry in [replaced] (same order as [lookupResultDocs]). */
private fun rebuildResult(q: Query, result: JsonElement, replaced: List<JsonObject>): JsonElement {
    var i = 0
    fun next(): JsonObject = replaced[i++]
    if (result is JsonObject) {
        val docs = result["docs"]
        if (docs is JsonArray) {
            val mapped = JsonArray(docs.map { if (it is JsonObject) next() else it })
            return JsonObject(LinkedHashMap(result).also { it["docs"] = mapped })
        }
        return next()
    }
    if (result is JsonArray) return JsonArray(result.map { if (it is JsonObject) next() else it })
    return result
}

/**
 * Execute-time lookup expansion (server `expand_lookups`): distinct ids per lookup, one fetch of
 * visible child rows, then fan out in declared order with dangling ids dropped, `cap`
 * truncation, and a deterministic joined-row ceiling in parent-result order.
 */
internal fun expandLookups(s: Store, q: Query, result: JsonElement): JsonElement {
    val original = lookupResultDocs(q, result)
    if (original.isEmpty()) return result
    var docs: List<JsonObject> = original
    val table = s.tables[q.table] ?: return result
    for (lk in q.lookups.orEmpty()) {
        val ty = table.fields[lk.field] ?: return result
        val arrayForm = isArrayLookup(ty)
        val ids = ArrayList<String>()
        val seen = HashSet<String>()
        for (doc in docs) collectFieldIds(doc[lk.field], ids, seen)
        val fetched = HashMap<String, JsonObject>()
        for (id in ids) {
            val row = s.docs[RowKey(lk.table, id)]
            if (row != null && row.deletedAt == null) fetched[id] = mergeDoc(row)
        }
        val cap = lk.cap ?: DEFAULT_LOOKUP_CAP
        var budget = MAX_JOINED_ROWS
        docs =
            docs.map { doc ->
                val v = doc[lk.field]
                val joined: JsonElement =
                    when {
                        v is JsonPrimitive && v.isString -> {
                            val j = if (budget > 0) fetched[v.content] else null
                            if (j != null) budget--
                            j ?: JsonNull
                        }
                        v is JsonArray -> {
                            val out = ArrayList<JsonElement>()
                            for (idv in v) {
                                if (out.size >= cap || budget == 0) break
                                val id = strOf(idv) ?: continue
                                fetched[id]?.let {
                                    out += it
                                    budget--
                                }
                            }
                            JsonArray(out)
                        }
                        arrayForm -> JsonArray(emptyList())
                        else -> JsonNull
                    }
                JsonObject(LinkedHashMap(doc).also { it[lk.alias] = joined })
            }
    }
    return rebuildResult(q, result, docs)
}
