package dev.parrtdb.wire

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

enum class Order(val wire: String) {
    Asc("asc"),
    Desc("desc");

    companion object {
        fun fromWire(s: String): Order = entries.firstOrNull { it.wire == s } ?: wireFail("unknown order `$s`")
    }
}

enum class SearchMode(val wire: String) {
    Tsquery("tsquery"),
    Trgm("trgm");

    companion object {
        fun fromWire(s: String): SearchMode = entries.firstOrNull { it.wire == s } ?: wireFail("unknown search mode `$s`")
    }
}

enum class SearchRankMode(val wire: String) {
    TsRank("ts_rank"),
    TsRankCd("ts_rank_cd");

    companion object {
        fun fromWire(s: String): SearchRankMode = entries.firstOrNull { it.wire == s } ?: wireFail("unknown search rankMode `$s`")
    }
}

enum class AggregateOp(val wire: String) {
    Sum("sum"),
    Avg("avg"),
    Min("min"),
    Max("max"),
    Count("count");

    companion object {
        fun fromWire(s: String): AggregateOp = entries.firstOrNull { it.wire == s } ?: wireFail("unknown aggregate op `$s`")
    }
}

data class LookupSpec(val field: String, val table: String, val alias: String, val cap: Int? = null) {
    fun toJson() = jsonObj("field" to jsonStr(field), "table" to jsonStr(table), "as" to jsonStr(alias), "cap" to cap?.let { jsonNum(it) })

    companion object {
        fun fromJson(e: JsonElement): LookupSpec {
            val o = StrictObj.of(e, "lookup")
            return LookupSpec(o.str("field"), o.str("table"), o.str("as"), o.optInt("cap")).also { o.finish() }
        }
    }
}

data class Paginate(val numItems: Int, val cursor: String? = null) {
    fun toJson() = jsonObj("cursor" to cursor?.let { jsonStr(it) }, "numItems" to jsonNum(numItems))

    companion object {
        fun fromJson(e: JsonElement): Paginate {
            val o = StrictObj.of(e, "paginate")
            return Paginate(o.int("numItems"), o.optStr("cursor")).also { o.finish() }
        }
    }
}

data class SearchQuery(
    val index: String,
    val query: String,
    val filter: FilterExpr? = null,
    val mode: SearchMode? = null,
    val snippet: Boolean? = null,
    /** Tsquery-mode ranking function; omitted on the wire when null (the default `ts_rank`). */
    val rankMode: SearchRankMode? = null,
    /** Trgm-mode similarity floor in 0..1; omitted on the wire when null. */
    val minSimilarity: Double? = null,
) {
    fun toJson() =
        jsonObj(
            "index" to jsonStr(index),
            "query" to jsonStr(query),
            "filter" to filter?.toJson(),
            "mode" to mode?.let { jsonStr(it.wire) },
            "snippet" to snippet?.let { jsonBool(it) },
            "rankMode" to rankMode?.let { jsonStr(it.wire) },
            "minSimilarity" to minSimilarity?.let { jsonNum(if (it == Math.rint(it)) it.toLong().toString() else it.toString()) },
        )

    companion object {
        fun fromJson(e: JsonElement): SearchQuery {
            val o = StrictObj.of(e, "search")
            return SearchQuery(
                o.str("index"),
                o.str("query"),
                o.opt("filter")?.let { FilterExpr.fromJson(it) },
                o.optStr("mode")?.let { SearchMode.fromWire(it) },
                o.optBool("snippet"),
                o.optStr("rankMode")?.let { SearchRankMode.fromWire(it) },
                o.opt("minSimilarity")?.let { v ->
                    val p = v as? JsonPrimitive
                    if (p == null || p.isString) wireFail("search.minSimilarity: expected a number")
                    p.content.toDoubleOrNull() ?: wireFail("search.minSimilarity: expected a number")
                },
            ).also { o.finish() }
        }
    }
}

/** Float32 vector; serialized with the shortest float32 spelling and always a fractional part. */
fun vectorToJson(v: List<Float>): JsonArray = JsonArray(v.map { JsonPrimitive(it) })

fun vectorFromJson(e: JsonElement): List<Float> {
    val a = e as? JsonArray ?: wireFail("vector: expected an array")
    return a.map {
        val p = it as? JsonPrimitive
        if (p == null || p.isString || p is JsonNull) wireFail("vector: expected numbers")
        p.content.toFloatOrNull() ?: wireFail("vector: expected numbers")
    }
}

data class VectorSearchQuery(val index: String, val vector: List<Float>, val limit: Int, val filter: FilterExpr? = null) {
    fun toJson() =
        jsonObj(
            "index" to jsonStr(index),
            "vector" to vectorToJson(vector),
            "limit" to jsonNum(limit),
            "filter" to filter?.toJson(),
        )

    companion object {
        fun fromJson(e: JsonElement): VectorSearchQuery {
            val o = StrictObj.of(e, "vectorSearch")
            return VectorSearchQuery(
                o.str("index"),
                vectorFromJson(o.req("vector")),
                o.int("limit"),
                o.opt("filter")?.let { FilterExpr.fromJson(it) },
            ).also { o.finish() }
        }
    }
}

data class HybridSearchQuery(
    val query: String,
    val vector: List<Float>,
    val limit: Int,
    val searchIndex: String? = null,
    val vectorIndex: String? = null,
    val k: Int? = null,
) {
    fun toJson() =
        jsonObj(
            "query" to jsonStr(query),
            "vector" to vectorToJson(vector),
            "limit" to jsonNum(limit),
            "searchIndex" to searchIndex?.let { jsonStr(it) },
            "vectorIndex" to vectorIndex?.let { jsonStr(it) },
            "k" to k?.let { jsonNum(it) },
        )

    companion object {
        fun fromJson(e: JsonElement): HybridSearchQuery {
            val o = StrictObj.of(e, "hybridSearch")
            return HybridSearchQuery(
                o.str("query"),
                vectorFromJson(o.req("vector")),
                o.int("limit"),
                o.optStr("searchIndex"),
                o.optStr("vectorIndex"),
                o.optInt("k"),
            ).also { o.finish() }
        }
    }
}

/** `groupBy`: `false`/`true` or a list of field names. */
sealed interface GroupBy {
    data class Flag(val on: Boolean) : GroupBy

    data class Fields(val fields: List<String>) : GroupBy

    fun toJson(): JsonElement =
        when (this) {
            is Flag -> jsonBool(on)
            is Fields -> jsonStrArr(fields)
        }

    companion object {
        fun fromJson(e: JsonElement?): GroupBy =
            when {
                e == null || e is JsonNull -> Flag(false)
                e is JsonArray -> Fields(e.map { reqStr(it, "groupBy[]") })
                e is JsonPrimitive && !e.isString -> Flag(reqBool(e, "groupBy"))
                else -> wireFail("groupBy must be a bool or a string array")
            }
    }
}

/** One `having` entry: numeric bounds ANDed together; at least one is required (checked by the engine/server). */
data class HavingCmp(
    val eq: Double? = null,
    val ne: Double? = null,
    val gt: Double? = null,
    val gte: Double? = null,
    val lt: Double? = null,
    val lte: Double? = null,
) {
    fun hasBound(): Boolean = listOf(eq, ne, gt, gte, lt, lte).any { it != null }

    fun matches(v: Double): Boolean =
        (eq == null || v == eq) && (ne == null || v != ne) && (gt == null || v > gt) &&
            (gte == null || v >= gte) && (lt == null || v < lt) && (lte == null || v <= lte)

    fun toJson() =
        jsonObj(
            "eq" to eq?.let { havingNum(it) },
            "ne" to ne?.let { havingNum(it) },
            "gt" to gt?.let { havingNum(it) },
            "gte" to gte?.let { havingNum(it) },
            "lt" to lt?.let { havingNum(it) },
            "lte" to lte?.let { havingNum(it) },
        )

    companion object {
        fun fromJson(e: JsonElement): HavingCmp {
            val o = StrictObj.of(e, "having")
            fun bound(k: String): Double? =
                o.opt(k)?.let { v ->
                    val p = v as? JsonPrimitive
                    if (p == null || p.isString) wireFail("having.$k: expected a number")
                    p.content.toDoubleOrNull() ?: wireFail("having.$k: expected a number")
                }
            return HavingCmp(bound("eq"), bound("ne"), bound("gt"), bound("gte"), bound("lt"), bound("lte")).also { o.finish() }
        }
    }
}

private fun havingNum(d: Double): JsonElement =
    if (d == Math.rint(d) && Math.abs(d) < 1e15) jsonNum(d.toLong()) else jsonNum(d)

/** A grouped aggregate result that hit the group cap: `{"groups":[...],"truncated":true}`; under the cap it is the bare array. */
data class AggregateGroupsTruncated(val groups: List<JsonElement>, val truncated: Boolean = true) {
    fun toJson() = jsonObj("groups" to JsonArray(groups), "truncated" to jsonBool(truncated))
}

data class AggregateSpec(
    val op: AggregateOp? = null,
    val aggregates: Map<String, AggregateOp>? = null,
    val groupBy: GroupBy = GroupBy.Flag(false),
    /** Post-group filter: aggregate name (alias, or the op name for a single-op spec) to bounds. */
    val having: Map<String, HavingCmp>? = null,
) {
    fun toJson() =
        jsonObj(
            "op" to op?.let { jsonStr(it.wire) },
            "aggregates" to aggregates?.let { m -> JsonObject(m.mapValues { jsonStr(it.value.wire) }) },
            "groupBy" to groupBy.toJson(),
            "having" to having?.let { m -> JsonObject(m.mapValues { it.value.toJson() }) },
        )

    companion object {
        fun fromJson(e: JsonElement): AggregateSpec {
            val o = StrictObj.of(e, "aggregate")
            val op = o.optStr("op")?.let { AggregateOp.fromWire(it) }
            val aggs =
                o.opt("aggregates")?.let { a ->
                    val ao = a as? JsonObject ?: wireFail("aggregate.aggregates: expected an object")
                    ao.mapValues { AggregateOp.fromWire(reqStr(it.value, "aggregate.aggregates")) }
                }
            val gb = GroupBy.fromJson(o.raw("groupBy"))
            val having =
                o.opt("having")?.let { h ->
                    val ho = h as? JsonObject ?: wireFail("aggregate.having: expected an object")
                    ho.mapValues { HavingCmp.fromJson(it.value) }
                }
            return AggregateSpec(op, aggs, gb, having).also { o.finish() }
        }
    }
}

/** The read-query DSL (mirrors core Query). */
data class Query(
    val table: String,
    val get: String? = null,
    val index: String? = null,
    val eq: List<JsonElement>? = null,
    val gt: JsonElement? = null,
    val gte: JsonElement? = null,
    val lt: JsonElement? = null,
    val lte: JsonElement? = null,
    val order: Order? = null,
    val take: Int? = null,
    val unique: Boolean = false,
    val first: Boolean = false,
    val count: Boolean = false,
    val distinct: Boolean = false,
    val aggregate: AggregateSpec? = null,
    val paginate: Paginate? = null,
    val filter: FilterExpr? = null,
    val search: SearchQuery? = null,
    val fields: List<String>? = null,
    val vectorSearch: VectorSearchQuery? = null,
    val hybridSearch: HybridSearchQuery? = null,
    val lookups: List<LookupSpec>? = null,
) {
    fun toJson(): JsonObject =
        jsonObj(
            "table" to jsonStr(table),
            "get" to get?.let { jsonStr(it) },
            "index" to index?.let { jsonStr(it) },
            "eq" to eq?.let { jsonArr(it) },
            "gt" to gt,
            "gte" to gte,
            "lt" to lt,
            "lte" to lte,
            "order" to order?.let { jsonStr(it.wire) },
            "take" to take?.let { jsonNum(it) },
            "unique" to if (unique) jsonBool(true) else null,
            "first" to if (first) jsonBool(true) else null,
            "count" to if (count) jsonBool(true) else null,
            "distinct" to if (distinct) jsonBool(true) else null,
            "aggregate" to aggregate?.toJson(),
            "paginate" to paginate?.toJson(),
            "filter" to filter?.toJson(),
            "search" to search?.toJson(),
            "fields" to fields?.let { jsonStrArr(it) },
            "vectorSearch" to vectorSearch?.toJson(),
            "hybridSearch" to hybridSearch?.toJson(),
            "lookups" to lookups?.let { l -> jsonArr(l.map { it.toJson() }) },
        )

    companion object {
        fun fromJson(e: JsonElement): Query {
            val o = StrictObj.of(e, "query")
            return Query(
                table = o.str("table"),
                get = o.optStr("get"),
                index = o.optStr("index"),
                eq = o.optArr("eq")?.toList(),
                gt = o.opt("gt"),
                gte = o.opt("gte"),
                lt = o.opt("lt"),
                lte = o.opt("lte"),
                order = o.optStr("order")?.let { Order.fromWire(it) },
                take = o.optInt("take"),
                unique = o.optBool("unique") ?: false,
                first = o.optBool("first") ?: false,
                count = o.optBool("count") ?: false,
                distinct = o.optBool("distinct") ?: false,
                aggregate = o.opt("aggregate")?.let { AggregateSpec.fromJson(it) },
                paginate = o.opt("paginate")?.let { Paginate.fromJson(it) },
                filter = o.opt("filter")?.let { FilterExpr.fromJson(it) },
                search = o.opt("search")?.let { SearchQuery.fromJson(it) },
                fields = o.optStrList("fields"),
                vectorSearch = o.opt("vectorSearch")?.let { VectorSearchQuery.fromJson(it) },
                hybridSearch = o.opt("hybridSearch")?.let { HybridSearchQuery.fromJson(it) },
                lookups = o.optArr("lookups")?.map { LookupSpec.fromJson(it) },
            ).also { o.finish() }
        }
    }
}
