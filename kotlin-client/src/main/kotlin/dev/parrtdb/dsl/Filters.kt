package dev.parrtdb.dsl

import dev.parrtdb.wire.Cast
import dev.parrtdb.wire.CaseWhen
import dev.parrtdb.wire.FilterExpr
import dev.parrtdb.wire.ValueExpr
import kotlinx.serialization.json.JsonElement

/** Filter-expression constructors. Values are plain Kotlin values (see [json]) or [JsonElement]s. */
object Filters {
    fun eq(field: String, value: Any?): FilterExpr = FilterExpr.Eq(field, json(value))

    fun neq(field: String, value: Any?): FilterExpr = FilterExpr.Neq(field, json(value))

    fun gt(field: String, value: Any?): FilterExpr = FilterExpr.Gt(field, json(value))

    fun gte(field: String, value: Any?): FilterExpr = FilterExpr.Gte(field, json(value))

    fun lt(field: String, value: Any?): FilterExpr = FilterExpr.Lt(field, json(value))

    fun lte(field: String, value: Any?): FilterExpr = FilterExpr.Lte(field, json(value))

    fun inList(field: String, vararg values: Any?): FilterExpr = FilterExpr.In(field, values.map { json(it) })

    fun contains(field: String, value: Any?): FilterExpr = FilterExpr.Contains(field, json(value))

    fun exists(field: String): FilterExpr = FilterExpr.Exists(field)

    fun olderThan(field: String, ms: Long): FilterExpr = FilterExpr.OlderThan(field, ms)

    fun and(vararg exprs: FilterExpr): FilterExpr = FilterExpr.And(exprs.toList())

    fun or(vararg exprs: FilterExpr): FilterExpr = FilterExpr.Or(exprs.toList())

    fun not(expr: FilterExpr): FilterExpr = FilterExpr.Not(expr)
}

/** Computed-field / migrate value-expression constructors. */
object Exprs {
    fun field(name: String): ValueExpr = ValueExpr.Field(name)

    fun literal(value: Any?): ValueExpr = ValueExpr.Literal(json(value))

    fun concat(vararg parts: ValueExpr): ValueExpr = ValueExpr.Concat(parts.toList())

    fun add(left: ValueExpr, right: ValueExpr): ValueExpr = ValueExpr.Add(left, right)

    fun sub(left: ValueExpr, right: ValueExpr): ValueExpr = ValueExpr.Sub(left, right)

    fun mul(left: ValueExpr, right: ValueExpr): ValueExpr = ValueExpr.Mul(left, right)

    fun div(left: ValueExpr, right: ValueExpr): ValueExpr = ValueExpr.Div(left, right)

    fun coalesce(vararg parts: ValueExpr): ValueExpr = ValueExpr.Coalesce(parts.toList())

    fun lower(value: ValueExpr): ValueExpr = ValueExpr.Lower(value)

    fun upper(value: ValueExpr): ValueExpr = ValueExpr.Upper(value)

    fun trim(value: ValueExpr): ValueExpr = ValueExpr.Trim(value)

    fun cast(value: ValueExpr, to: Cast): ValueExpr = ValueExpr.CastTo(value, to)

    fun now(): ValueExpr = ValueExpr.Now

    fun case(whens: List<CaseWhen>, otherwise: ValueExpr): ValueExpr = ValueExpr.Case(whens, otherwise)

    fun whenThen(condition: FilterExpr, result: ValueExpr): CaseWhen = CaseWhen(condition, result)
}
