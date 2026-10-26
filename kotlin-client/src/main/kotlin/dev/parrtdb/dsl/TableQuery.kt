package dev.parrtdb.dsl

import dev.parrtdb.wire.AggregateOp
import dev.parrtdb.wire.AggregateSpec
import dev.parrtdb.wire.FilterExpr
import dev.parrtdb.wire.GroupBy
import dev.parrtdb.wire.HybridSearchQuery
import dev.parrtdb.wire.Order
import dev.parrtdb.wire.Paginate
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.SearchQuery
import dev.parrtdb.wire.VectorSearchQuery

/**
 * Immutable fluent builder for the read-query DSL. Every method returns a new builder, so a
 * partially built query can be shared safely. Combination rules (e.g. `get` excludes `index`)
 * are enforced by the engine/server, not here.
 */
class TableQuery private constructor(private val q: Query) {
    constructor(table: String) : this(Query(table))

    fun get(id: String): TableQuery = TableQuery(q.copy(get = id))

    /** Selects an index and appends equality values (one per leading index field). */
    fun withIndex(name: String, vararg eq: Any?): TableQuery = TableQuery(q.copy(index = name, eq = (q.eq ?: emptyList()) + eq.map { json(it) }))

    fun eq(value: Any?): TableQuery = TableQuery(q.copy(eq = (q.eq ?: emptyList()) + json(value)))

    fun gt(value: Any?): TableQuery = TableQuery(q.copy(gt = json(value)))

    fun gte(value: Any?): TableQuery = TableQuery(q.copy(gte = json(value)))

    fun lt(value: Any?): TableQuery = TableQuery(q.copy(lt = json(value)))

    fun lte(value: Any?): TableQuery = TableQuery(q.copy(lte = json(value)))

    fun orderAsc(): TableQuery = TableQuery(q.copy(order = Order.Asc))

    fun orderDesc(): TableQuery = TableQuery(q.copy(order = Order.Desc))

    fun take(n: Int): TableQuery = TableQuery(q.copy(take = n))

    fun unique(): TableQuery = TableQuery(q.copy(unique = true))

    fun first(): TableQuery = TableQuery(q.copy(first = true))

    fun count(): TableQuery = TableQuery(q.copy(count = true))

    fun distinct(): TableQuery = TableQuery(q.copy(distinct = true))

    /** Single-op aggregate; `groupBy = true` groups by the index fields. */
    fun aggregate(op: AggregateOp, groupBy: Boolean = false): TableQuery =
        TableQuery(q.copy(aggregate = AggregateSpec(op = op, groupBy = GroupBy.Flag(groupBy))))

    fun aggregateGroupByFields(op: AggregateOp, vararg fields: String): TableQuery =
        TableQuery(q.copy(aggregate = AggregateSpec(op = op, groupBy = GroupBy.Fields(fields.toList()))))

    /** Named multi-op aggregate, e.g. `mapOf("total" to AggregateOp.Sum, "n" to AggregateOp.Count)`. */
    fun aggregates(aggregates: Map<String, AggregateOp>, groupBy: GroupBy = GroupBy.Flag(false)): TableQuery =
        TableQuery(q.copy(aggregate = AggregateSpec(aggregates = aggregates, groupBy = groupBy)))

    fun paginate(numItems: Int, cursor: String? = null): TableQuery = TableQuery(q.copy(paginate = Paginate(numItems, cursor?.takeIf { it.isNotEmpty() })))

    fun filter(f: FilterExpr): TableQuery = TableQuery(q.copy(filter = f))

    fun search(s: SearchQuery): TableQuery = TableQuery(q.copy(search = s))

    fun fields(vararg fields: String): TableQuery = TableQuery(q.copy(fields = fields.toList()))

    fun vectorSearch(v: VectorSearchQuery): TableQuery = TableQuery(q.copy(vectorSearch = v))

    fun hybridSearch(h: HybridSearchQuery): TableQuery = TableQuery(q.copy(hybridSearch = h))

    fun build(): Query = q
}

/** Entry point: `query("tasks").withIndex("by_status", "open").take(10).build()`. */
fun query(table: String): TableQuery = TableQuery(table)
