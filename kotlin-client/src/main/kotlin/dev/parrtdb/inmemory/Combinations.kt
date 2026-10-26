package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.WireJson
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/** Caps take/collect/paginate page sizes (server MAX_TAKE). */
internal const val maxTake: Int = 4096

/** SEC-007 ceiling on raw search text, in UTF-8 bytes. */
internal const val maxSearchQueryBytes: Int = 4096

private class CombinationRule(
    val forbid: List<String>?,
    val atMostOne: List<String>?,
    val code: String,
    val message: String,
)

private const val RULES_RESOURCE = "query-combinations.json"

/** Loaded from the classpath copy of wire-corpus/query-combinations.json (Go's go:embed analogue). */
private val rules: List<CombinationRule> by lazy { loadRules() }

private fun loadRules(): List<CombinationRule> {
    val stream =
        CombinationRule::class.java.classLoader.getResourceAsStream(RULES_RESOURCE)
            ?: throw RtDbException(ErrorCode.INTERNAL, "missing resource $RULES_RESOURCE")
    val text = stream.use { String(it.readBytes(), Charsets.UTF_8) }
    val parsed =
        try {
            WireJson.parse(text) as JsonObject
        } catch (e: Exception) {
            throw RtDbException(ErrorCode.INTERNAL, "parse query-combinations.json: ${e.message}")
        }
    val arr = parsed["rules"] as? JsonArray ?: throw RtDbException(ErrorCode.INTERNAL, "query-combinations.json: missing rules")
    return arr.map { r ->
        val o = r as JsonObject
        val forbid = (o["forbid"] as? JsonArray)?.map { (it as JsonPrimitive).content }?.takeIf { it.isNotEmpty() }
        val atMost = (o["atMostOne"] as? JsonArray)?.map { (it as JsonPrimitive).content }?.takeIf { it.isNotEmpty() }
        if ((forbid != null) == (atMost != null)) {
            throw RtDbException(ErrorCode.INTERNAL, "query-combinations.json: a rule must declare exactly one of forbid/atMostOne")
        }
        CombinationRule(forbid, atMost, (o["code"] as JsonPrimitive).content, (o["message"] as JsonPrimitive).content)
    }
}

/** The canonical clause-presence set for a query (mirror of the server's query_clauses). */
private fun queryClauses(q: Query): Set<String> {
    val set = HashSet<String>()
    if (q.get != null) set += "get"
    if (q.index != null) set += "index"
    if (!q.eq.isNullOrEmpty()) set += "eq"
    if (q.gt != null) set += "gt"
    if (q.gte != null) set += "gte"
    if (q.lt != null) set += "lt"
    if (q.lte != null) set += "lte"
    if (q.order != null) set += "order"
    if (q.take != null) set += "take"
    if (q.unique) set += "unique"
    if (q.first) set += "first"
    if (q.count) set += "count"
    if (q.distinct) set += "distinct"
    if (q.aggregate != null) set += "aggregate"
    if (q.paginate != null) set += "paginate"
    if (q.filter != null) set += "filter"
    if (q.search != null) set += "search"
    if (q.vectorSearch != null) set += "vectorSearch"
    if (q.hybridSearch != null) set += "hybridSearch"
    if (!q.lookups.isNullOrEmpty()) set += "lookups"
    return set
}

private fun violation(rule: CombinationRule): RtDbException =
    RtDbException(ErrorCode.fromWire(rule.code) ?: ErrorCode.INTERNAL, rule.message)

/**
 * Evaluates every declared rule against the clause set: all forbid rules in declaration order,
 * then all atMostOne rules. Accept/reject is order-independent; only the winning message follows
 * the table's tie-break.
 */
internal fun checkQueryCombinations(q: Query) {
    val table = rules
    val present = queryClauses(q)
    for (rule in table) {
        if (rule.forbid != null && rule.forbid.all { it in present }) throw violation(rule)
    }
    for (rule in table) {
        if (rule.atMostOne != null && rule.atMostOne.count { it in present } > 1) throw violation(rule)
    }
}
