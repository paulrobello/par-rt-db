package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.AggregateOp
import dev.parrtdb.wire.AggregateSpec
import dev.parrtdb.wire.GroupBy
import dev.parrtdb.wire.jsonNum
import dev.parrtdb.wire.jsonStr
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.math.BigInteger

private fun bad(message: String): RtDbException = RtDbException(ErrorCode.BAD_REQUEST, message)

private class OpAlias(val alias: String, val op: AggregateOp)

/** Validated, normalized AggregateSpec; aggField is "" when every op is count. */
private class AggregatePlan(
    val ops: List<OpAlias>,
    val groupFields: List<String>,
    val groupPgs: List<PgType>,
    val legacy: Boolean,
    val aggField: String,
    val aggFieldPg: PgType,
)

private fun isAggAliasChar(c: Char): Boolean = c in 'a'..'z' || c in 'A'..'Z' || c in '0'..'9' || c == '_'

private fun planAggregate(agg: AggregateSpec, table: TableDef, plan: ScanPlan): AggregatePlan {
    val eqLen = plan.typedEq.size
    val groupBy = agg.groupBy
    val aggregates = agg.aggregates
    if (agg.op != null && !aggregates.isNullOrEmpty()) throw bad("aggregate op and aggregates are mutually exclusive")
    val ops: List<OpAlias> =
        when {
            !aggregates.isNullOrEmpty() -> aggregates.keys.sorted().map { OpAlias(it, aggregates.getValue(it)) }
            agg.op != null -> listOf(OpAlias(agg.op.wire, agg.op))
            else -> emptyList()
        }
    if (ops.isEmpty()) throw bad("aggregate requires op or aggregates")
    for (o in ops) {
        // Go's len() counts bytes; non-ASCII fails the charset check below either way.
        if (o.alias.toByteArray(Charsets.UTF_8).size !in 1..64) throw bad("aggregates alias must be 1-64 characters")
        if (!o.alias.all { isAggAliasChar(it) }) throw bad("aggregates alias may contain only [A-Za-z0-9_]")
    }
    val needsField = ops.any { it.op != AggregateOp.Count }
    val legacy = agg.op != null && aggregates.isNullOrEmpty() && groupBy is GroupBy.Flag

    val groupFields: List<String> =
        when (groupBy) {
            is GroupBy.Flag ->
                if (groupBy.on) {
                    val idx = plan.indexDef
                    if (idx == null || eqLen >= idx.fields.size) throw bad("aggregate groupBy requires an index field beyond the eq prefix")
                    listOf(idx.fields[eqLen])
                } else {
                    emptyList()
                }
            is GroupBy.Fields -> {
                if (groupBy.fields.isEmpty()) throw bad("aggregate groupBy list must not be empty")
                groupBy.fields
            }
        }
    if (groupFields.isNotEmpty()) {
        val idx = plan.indexDef ?: throw bad("aggregate groupBy requires an index field beyond the eq prefix")
        for (f in groupFields) if (f !in idx.fields) throw bad("aggregate groupBy field is not a declared index field")
    }

    var aggField = ""
    if (needsField) {
        val idx = plan.indexDef ?: throw bad("aggregate requires an index field beyond the eq prefix")
        aggField = idx.fields.drop(eqLen).firstOrNull { it !in groupFields } ?: ""
        if (aggField.isEmpty()) throw bad("aggregate requires an index field beyond the eq prefix")
        val pg = plan.fieldPg(table, aggField)
        for (o in ops) {
            if ((o.op == AggregateOp.Sum || o.op == AggregateOp.Avg) && pg != PgType.Number && pg != PgType.Int64) {
                throw bad("aggregate op ${o.op.wire} requires a numeric index field")
            }
        }
        if (groupBy is GroupBy.Flag && groupBy.on && eqLen + 1 >= idx.fields.size) {
            throw bad("aggregate groupBy requires two index fields beyond the eq prefix")
        }
    }
    val aggFieldPg = if (aggField.isNotEmpty()) plan.fieldPg(table, aggField) else PgType.Text
    return AggregatePlan(ops, groupFields, groupFields.map { plan.fieldPg(table, it) }, legacy, aggField, aggFieldPg)
}

/**
 * One path for the legacy single-`op` scalar/grouped shapes and the wire-v2 `aggregates` map /
 * composite `groupBy` shapes; dispatch happens only at the final result-shape choice.
 */
internal fun executeAggregateTerminal(agg: AggregateSpec, table: TableDef, plan: ScanPlan, filtered: List<StoredRow>): JsonElement {
    val ap = planAggregate(agg, table, plan)
    val evaluate: (List<StoredRow>, AggregateOp) -> JsonElement = { rows, op ->
        if (op == AggregateOp.Count) {
            jsonNum(formatI64(rows.size.toLong()))
        } else {
            val values = rows.mapNotNull { r -> r.doc[ap.aggField]?.takeUnless { isNullValue(it) } }
            if (values.isEmpty()) JsonNull else applyAggregate(op, values, ap.aggFieldPg)
        }
    }
    return if (ap.groupFields.isNotEmpty()) aggregateGrouped(ap, filtered, evaluate) else aggregateScalar(ap, filtered, evaluate)
}

private class GroupEntry(val keys: List<JsonElement>) {
    val rows = ArrayList<StoredRow>()
}

/** One row per group sorted by key ascending; legacy emits {key, value}, wire-v2 {keys, values}; capped by maxTake. */
private fun aggregateGrouped(ap: AggregatePlan, filtered: List<StoredRow>, evaluate: (List<StoredRow>, AggregateOp) -> JsonElement): JsonElement {
    val groups = ArrayList<GroupEntry>()
    val index = HashMap<String, GroupEntry>()
    for (row in filtered) {
        val keys = ap.groupFields.map { row.doc[it] ?: JsonNull }
        val key = keys.joinToString("\u001f", postfix = "\u001f") { canonical(it) }
        val g = index.getOrPut(key) { GroupEntry(keys).also { groups += it } }
        g.rows += row
    }
    val sorted =
        groups.sortedWith { a, b ->
            for (i in ap.groupFields.indices) {
                val c = compareIndexValues(a.keys[i], b.keys[i], ap.groupPgs[i])
                if (c != 0) return@sortedWith c
            }
            0
        }.take(maxTake)
    return JsonArray(
        sorted.map { g ->
            if (ap.legacy) {
                JsonObject(linkedMapOf("key" to g.keys[0], "value" to evaluate(g.rows, ap.ops[0].op)))
            } else {
                val values = LinkedHashMap<String, JsonElement>()
                for (o in ap.ops) values[o.alias] = evaluate(g.rows, o.op)
                JsonObject(linkedMapOf("keys" to JsonArray(g.keys), "values" to JsonObject(values)))
            }
        },
    )
}

private fun aggregateScalar(ap: AggregatePlan, filtered: List<StoredRow>, evaluate: (List<StoredRow>, AggregateOp) -> JsonElement): JsonElement {
    if (ap.legacy) return evaluate(filtered, ap.ops[0].op)
    val out = LinkedHashMap<String, JsonElement>()
    for (o in ap.ops) out[o.alias] = evaluate(filtered, o.op)
    return JsonObject(out)
}

/**
 * Applies one op over a non-empty value set. SUM over an int64 field reduces exactly over the
 * decimal strings (BigInteger) and returns the exact decimal string; other SUM/AVG reduce as
 * doubles; MIN/MAX pick the extreme per [compareIndexValues].
 */
private fun applyAggregate(op: AggregateOp, values: List<JsonElement>, pg: PgType): JsonElement =
    when (op) {
        AggregateOp.Sum ->
            if (pg == PgType.Int64) {
                var total = BigInteger.ZERO
                var exact = true
                for (v in values) {
                    val s = (v as? JsonPrimitive)?.takeIf { it.isString }?.content
                    val n = s?.let { runCatching { BigInteger(it) }.getOrNull() }
                    if (n == null) {
                        exact = false
                        break
                    }
                    total = total.add(n)
                }
                if (exact) jsonStr(total.toString()) else f64Aggregate(op, values, pg)
            } else {
                f64Aggregate(op, values, pg)
            }
        AggregateOp.Avg -> f64Aggregate(op, values, pg)
        AggregateOp.Min, AggregateOp.Max -> {
            var best = values[0]
            for (v in values.drop(1)) {
                val c = compareIndexValues(v, best, pg)
                if ((op == AggregateOp.Min && c < 0) || (op == AggregateOp.Max && c > 0)) best = v
            }
            best
        }
        AggregateOp.Count -> jsonNum(formatI64(values.size.toLong()))
    }

private fun f64Aggregate(op: AggregateOp, values: List<JsonElement>, pg: PgType): JsonElement {
    var sum = 0.0
    for (v in values) sum += numericValue(v, pg)
    val result = if (op == AggregateOp.Avg) sum / values.size else sum
    if (result.isNaN() || result.isInfinite()) return JsonNull
    return jsonNumberFromF64(result)
}

private fun numericValue(v: JsonElement, pg: PgType): Double {
    val p = v as? JsonPrimitive ?: return 0.0
    if (pg == PgType.Int64) {
        if (!p.isString) return 0.0
        return p.content.toDoubleOrNull() ?: 0.0
    }
    if (!isJsonNumber(p)) return 0.0
    return jsonNumberF64(p)
}
