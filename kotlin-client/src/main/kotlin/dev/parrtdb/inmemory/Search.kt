package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.FilterExpr
import dev.parrtdb.wire.HybridSearchQuery
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.SearchMode
import dev.parrtdb.wire.SearchQuery
import dev.parrtdb.wire.VectorSearchQuery
import dev.parrtdb.wire.jsonStr
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

private fun bad(message: String): RtDbException = RtDbException(ErrorCode.BAD_REQUEST, message)

/** A quoted phrase (non-null [phrase]) or a bare lowercased [term]. */
private class SearchOperand(val phrase: List<String>?, val term: String = "") {
    val isPhrase: Boolean get() = phrase != null
}

private class WebsearchQuery(val groups: List<List<SearchOperand>>, val excluded: List<SearchOperand>)

/** Go's unicode.IsSpace. */
private fun isSpace(c: Char): Boolean = c in '\t'..'\r' || c == '\u0085' || Character.isSpaceChar(c)

private fun fields(s: String): List<String> {
    val out = ArrayList<String>()
    val cur = StringBuilder()
    for (c in s) {
        if (isSpace(c)) {
            if (cur.isNotEmpty()) {
                out += cur.toString()
                cur.setLength(0)
            }
        } else {
            cur.append(c)
        }
    }
    if (cur.isNotEmpty()) out += cur.toString()
    return out
}

private fun byteLen(s: String): Int = s.toByteArray(Charsets.UTF_8).size

/**
 * Approximates the server's websearch_to_tsquery: quoted phrases need adjacent words, the bare
 * word `or` unions adjacent operands, `-term`/`-"phrase"` excludes, anything else is an ANDed term.
 */
private fun parseWebsearchQuery(text: String): WebsearchQuery {
    val groups = ArrayList<List<SearchOperand>>()
    val excluded = ArrayList<SearchOperand>()
    var current = ArrayList<SearchOperand>()
    var orPending = false
    val fs = fields(text)
    var i = 0
    while (i < fs.size) {
        var tok = fs[i]
        i++
        val negated = tok.startsWith("-")
        if (negated) {
            tok = tok.substring(1)
            if (tok.isEmpty()) continue
        }
        val operand: SearchOperand
        val quoted = tok.startsWith("\"")
        if (quoted) {
            val start = tok.removePrefix("\"")
            var phrase = start
            while (i < fs.size && (!phrase.endsWith("\"") || (phrase == start && !tok.endsWith("\"")))) {
                phrase += " " + fs[i]
                i++
            }
            phrase = phrase.removeSuffix("\"")
            val words = fields(phrase.lowercase())
            if (words.isEmpty()) continue
            operand = SearchOperand(words)
        } else {
            val word = tok.removePrefix("\"").removeSuffix("\"").lowercase()
            if (word.isEmpty()) continue
            operand = SearchOperand(null, word)
        }
        if (!negated && !operand.isPhrase && operand.term == "or") {
            orPending = current.isNotEmpty()
            continue
        }
        if (negated) {
            excluded += operand
            continue
        }
        if (orPending) {
            current += operand
            orPending = false
        } else {
            if (current.isNotEmpty()) {
                groups += current
                current = ArrayList()
            }
            current += operand
        }
    }
    if (current.isNotEmpty()) groups += current
    return WebsearchQuery(groups, excluded)
}

private fun operandMatches(op: SearchOperand, fieldTexts: List<String>): Boolean {
    val needle = if (op.isPhrase) op.phrase!!.joinToString(" ") else op.term
    return fieldTexts.any { it.contains(needle) }
}

private fun positiveLexemes(parsed: WebsearchQuery): List<String> =
    parsed.groups.flatMap { g -> g.flatMap { if (it.isPhrase) it.phrase!! else listOf(it.term) } }

private fun ftsStringify(v: JsonElement?): String = valueText(v) ?: ""

/** Maximal ASCII-alphanumeric runs of the lowercased text (the /[a-z0-9]+/g stand-in for lexemes). */
private fun ftsTokens(s: String): List<String> {
    val out = ArrayList<String>()
    val cur = StringBuilder()
    for (ch in s) {
        val c = if (ch in 'A'..'Z') ch + 32 else ch
        if (c in '0'..'9' || c in 'a'..'z') {
            cur.append(c)
        } else if (cur.isNotEmpty()) {
            out += cur.toString()
            cur.setLength(0)
        }
    }
    if (cur.isNotEmpty()) out += cur.toString()
    return out
}

/** Relevance stand-in every client engine pins: how many doc tokens are positive query lexemes. */
private fun tsqueryScore(doc: JsonObject, indexFields: List<String>, positives: Set<String>): Long {
    val source = indexFields.joinToString(" ") { ftsStringify(doc[it]) }
    return ftsTokens(source).count { it in positives }.toLong()
}

private fun searchFieldTexts(doc: JsonObject, indexFields: List<String>): List<String> =
    indexFields.mapNotNull { f -> (doc[f] as? JsonPrimitive)?.takeIf { it.isString }?.content?.lowercase() }

/** Writes `_searchSnippet`: a at-most-35-word excerpt centered on the first matched word, matches wrapped in `<mark>`. */
private fun attachSearchSnippets(rows: List<JsonObject>, indexFields: List<String>, parsed: WebsearchQuery): List<JsonObject> {
    val terms = positiveLexemes(parsed)
    return rows.map { doc ->
        val text = indexFields.mapNotNull { f -> (doc[f] as? JsonPrimitive)?.takeIf { it.isString }?.content }.joinToString(" ")
        fun marked(w: String): Boolean {
            val lw = w.lowercase()
            return terms.any { lw.contains(it) }
        }
        val words = fields(text)
        var first = 0
        for ((i, w) in words.withIndex()) {
            if (marked(w)) {
                first = i
                break
            }
        }
        val start = maxOf(first - 15, 0)
        val end = minOf(start + 35, words.size)
        val snippet = words.subList(start, end).joinToString(" ") { if (marked(it)) "<mark>$it</mark>" else it }
        JsonObject(LinkedHashMap(doc).also { it["_searchSnippet"] = jsonStr(snippet) })
    }
}

private fun filterDocs(rows: List<JsonObject>, expr: FilterExpr, fields: Map<String, FieldType>): List<JsonObject> =
    rows.filter { matchesFilter(expr, it, fields) }

private fun allDocs(s: Store, table: String): List<JsonObject> = collectAllLocked(s, table).map { it as JsonObject }

private class ScoredRow(val score: Double, val createdAt: Long, val id: String, val doc: JsonObject)

internal fun executeSearchTerminal(s: Store, q: Query, search: SearchQuery, table: TableDef): JsonElement {
    if (search.query.trim { isSpace(it) }.isEmpty()) throw bad("search query text must not be empty")
    if (byteLen(search.query) > maxSearchQueryBytes) throw bad("search query text must be at most $maxSearchQueryBytes bytes")
    val indexDef =
        table.indexes.firstOrNull { it.name == search.index && it.search } ?: throw bad("search index '${search.index}' not found")
    if (search.mode == SearchMode.Trgm && !indexDef.trgm) {
        throw bad("search index '${search.index}' does not declare trgm: true — trgm mode requires it")
    }
    val indexFields = indexDef.fields
    val snippet = search.snippet == true
    if (snippet && search.mode == SearchMode.Trgm) throw bad("snippet is only supported in tsquery mode")
    search.filter?.let { validateFilterExpr(it, table) }
    var rows = allDocs(s, q.table)
    if (search.mode == SearchMode.Trgm) {
        search.filter?.let { rows = filterDocs(rows, it, table.fields) }
        val needle = search.query.lowercase()
        val queryLen = byteLen(search.query)
        val scored = ArrayList<ScoredRow>()
        for (doc in rows) {
            var best: Double? = null
            for (field in indexFields) {
                val ts = (doc[field] as? JsonPrimitive)?.takeIf { it.isString }?.content ?: continue
                if (ts.lowercase().contains(needle)) {
                    val len = byteLen(ts)
                    val score = if (len > 0) queryLen.toDouble() / len.toDouble() else 0.0
                    if (best == null || score > best) best = score
                }
            }
            if (best == null) continue
            val ct = doc["_creationTime"]
            val createdAt = if (ct != null && isJsonNumber(ct)) parseI64((ct as JsonPrimitive).content) ?: 0L else Long.MIN_VALUE
            val id = (doc["_id"] as? JsonPrimitive)?.takeIf { it.isString }?.content ?: ""
            scored += ScoredRow(best, createdAt, id, doc)
        }
        val sorted =
            scored.sortedWith { a, b ->
                when {
                    a.score != b.score -> if (a.score > b.score) -1 else 1
                    a.createdAt != b.createdAt -> if (a.createdAt > b.createdAt) -1 else 1
                    else -> -cmpStrings(a.id, b.id)
                }
            }
        q.paginate?.let { pag ->
            return paginateRanked(pag, sorted.map { RankedRow(jsonNumberFromF64(it.score), it.doc) }, true)
        }
        val limit = (q.take ?: maxTake).coerceAtLeast(0)
        return JsonArray(sorted.take(limit).map { it.doc })
    }
    val parsed = parseWebsearchQuery(search.query)
    var kept = ArrayList<JsonObject>()
    for (doc in rows) {
        val texts = searchFieldTexts(doc, indexFields)
        if (parsed.excluded.any { operandMatches(it, texts) }) continue
        if (parsed.groups.all { g -> g.any { operandMatches(it, texts) } }) kept += doc
    }
    var result: List<JsonObject> = kept
    search.filter?.let { result = filterDocs(result, it, table.fields) }
    if (snippet) result = attachSearchSnippets(result, indexFields, parsed)
    q.paginate?.let { pag ->
        val positives = positiveLexemes(parsed).toSet()
        val ranked = result.map { RankedRow(dev.parrtdb.wire.jsonNum(formatI64(tsqueryScore(it, indexFields, positives))), it) }
        return paginateRanked(pag, sortRankedRows(ranked), true)
    }
    return JsonArray(result)
}

/** Every live doc is a candidate narrowed by the carried filter and capped at limit. */
internal fun executeVectorSearchTerminal(s: Store, q: Query, vector: VectorSearchQuery, table: TableDef): JsonElement {
    vector.filter?.let { validateFilterExpr(it, table) }
    var rows = allDocs(s, q.table)
    vector.filter?.let { rows = filterDocs(rows, it, table.fields) }
    q.paginate?.let { pag ->
        val ranked = sortRankedRows(rows.map { RankedRow(null, it) }).take(vector.limit.coerceAtLeast(0))
        return paginateRanked(pag, ranked, false)
    }
    return JsonArray(rows.take(vector.limit.coerceAtLeast(0)))
}

/** Over-approximates the fused rank; a paginated hybrid returns an empty page. */
internal fun executeHybridSearchTerminal(s: Store, q: Query, hybrid: HybridSearchQuery): JsonElement {
    q.paginate?.let { return paginateRanked(it, emptyList(), true) }
    return JsonArray(allDocs(s, q.table).take(hybrid.limit.coerceAtLeast(0)))
}
