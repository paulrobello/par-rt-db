package dev.parrtdb.wire

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

/** Filter expression tagged by `op` (mirrors core FilterExpr). */
sealed interface FilterExpr {
    fun toJson(): JsonObject

    data class Eq(val field: String, val value: JsonElement) : FilterExpr {
        override fun toJson() = jsonObj("op" to jsonStr("eq"), "field" to jsonStr(field), "value" to value)
    }

    data class Neq(val field: String, val value: JsonElement) : FilterExpr {
        override fun toJson() = jsonObj("op" to jsonStr("neq"), "field" to jsonStr(field), "value" to value)
    }

    data class Gt(val field: String, val value: JsonElement) : FilterExpr {
        override fun toJson() = jsonObj("op" to jsonStr("gt"), "field" to jsonStr(field), "value" to value)
    }

    data class Gte(val field: String, val value: JsonElement) : FilterExpr {
        override fun toJson() = jsonObj("op" to jsonStr("gte"), "field" to jsonStr(field), "value" to value)
    }

    data class Lt(val field: String, val value: JsonElement) : FilterExpr {
        override fun toJson() = jsonObj("op" to jsonStr("lt"), "field" to jsonStr(field), "value" to value)
    }

    data class Lte(val field: String, val value: JsonElement) : FilterExpr {
        override fun toJson() = jsonObj("op" to jsonStr("lte"), "field" to jsonStr(field), "value" to value)
    }

    data class In(val field: String, val values: List<JsonElement>) : FilterExpr {
        override fun toJson() = jsonObj("op" to jsonStr("in"), "field" to jsonStr(field), "values" to jsonArr(values))
    }

    data class Contains(val field: String, val value: JsonElement) : FilterExpr {
        override fun toJson() = jsonObj("op" to jsonStr("contains"), "field" to jsonStr(field), "value" to value)
    }

    data class And(val exprs: List<FilterExpr>) : FilterExpr {
        override fun toJson() = jsonObj("op" to jsonStr("and"), "exprs" to jsonArr(exprs.map { it.toJson() }))
    }

    data class Or(val exprs: List<FilterExpr>) : FilterExpr {
        override fun toJson() = jsonObj("op" to jsonStr("or"), "exprs" to jsonArr(exprs.map { it.toJson() }))
    }

    data class Not(val expr: FilterExpr) : FilterExpr {
        override fun toJson() = jsonObj("op" to jsonStr("not"), "expr" to expr.toJson())
    }

    data class Exists(val field: String) : FilterExpr {
        override fun toJson() = jsonObj("op" to jsonStr("exists"), "field" to jsonStr(field))
    }

    data class OlderThan(val field: String, val ms: Long) : FilterExpr {
        override fun toJson() = jsonObj("op" to jsonStr("olderThan"), "field" to jsonStr(field), "ms" to jsonNum(ms))
    }

    companion object {
        fun fromJson(e: JsonElement): FilterExpr {
            val (tag, body) = peekTag(e, "op", "filter")
            val o = StrictObj(body, "filter.$tag")
            val r: FilterExpr =
                when (tag) {
                    "eq" -> Eq(o.str("field"), o.req("value"))
                    "neq" -> Neq(o.str("field"), o.req("value"))
                    "gt" -> Gt(o.str("field"), o.req("value"))
                    "gte" -> Gte(o.str("field"), o.req("value"))
                    "lt" -> Lt(o.str("field"), o.req("value"))
                    "lte" -> Lte(o.str("field"), o.req("value"))
                    "in" -> In(o.str("field"), o.arr("values").toList())
                    "contains" -> Contains(o.str("field"), o.req("value"))
                    "and" -> And(o.arr("exprs").map { fromJson(it) })
                    "or" -> Or(o.arr("exprs").map { fromJson(it) })
                    "not" -> Not(fromJson(o.req("expr")))
                    "exists" -> Exists(o.str("field"))
                    "olderThan" -> OlderThan(o.str("field"), o.long("ms"))
                    else -> wireFail("unknown filter op `$tag`")
                }
            o.finish()
            return r
        }
    }
}

enum class Cast(val wire: String) {
    ToString("toString"),
    ToNumber("toNumber"),
    ToInt64("toInt64"),
    ToBoolean("toBoolean");

    companion object {
        fun fromWire(s: String): Cast = entries.firstOrNull { it.wire == s } ?: wireFail("unknown cast `$s`")
    }
}

data class CaseWhen(val whenExpr: FilterExpr, val then: ValueExpr)

/** Computed-field / migrate expression tagged by `op` (mirrors core ValueExpr). */
sealed interface ValueExpr {
    fun toJson(): JsonObject

    data class Field(val field: String) : ValueExpr {
        override fun toJson() = jsonObj("op" to jsonStr("field"), "field" to jsonStr(field))
    }

    data class Literal(val value: JsonElement) : ValueExpr {
        override fun toJson() = jsonObj("op" to jsonStr("literal"), "value" to value)
    }

    data class Concat(val parts: List<ValueExpr>) : ValueExpr {
        override fun toJson() = jsonObj("op" to jsonStr("concat"), "parts" to jsonArr(parts.map { it.toJson() }))
    }

    data class Add(val left: ValueExpr, val right: ValueExpr) : ValueExpr {
        override fun toJson() = jsonObj("op" to jsonStr("add"), "left" to left.toJson(), "right" to right.toJson())
    }

    data class Sub(val left: ValueExpr, val right: ValueExpr) : ValueExpr {
        override fun toJson() = jsonObj("op" to jsonStr("sub"), "left" to left.toJson(), "right" to right.toJson())
    }

    data class Mul(val left: ValueExpr, val right: ValueExpr) : ValueExpr {
        override fun toJson() = jsonObj("op" to jsonStr("mul"), "left" to left.toJson(), "right" to right.toJson())
    }

    data class Div(val left: ValueExpr, val right: ValueExpr) : ValueExpr {
        override fun toJson() = jsonObj("op" to jsonStr("div"), "left" to left.toJson(), "right" to right.toJson())
    }

    data class Coalesce(val parts: List<ValueExpr>) : ValueExpr {
        override fun toJson() = jsonObj("op" to jsonStr("coalesce"), "parts" to jsonArr(parts.map { it.toJson() }))
    }

    data class Lower(val value: ValueExpr) : ValueExpr {
        override fun toJson() = jsonObj("op" to jsonStr("lower"), "value" to value.toJson())
    }

    data class Upper(val value: ValueExpr) : ValueExpr {
        override fun toJson() = jsonObj("op" to jsonStr("upper"), "value" to value.toJson())
    }

    data class Trim(val value: ValueExpr) : ValueExpr {
        override fun toJson() = jsonObj("op" to jsonStr("trim"), "value" to value.toJson())
    }

    data class CastTo(val value: ValueExpr, val to: Cast) : ValueExpr {
        override fun toJson() = jsonObj("op" to jsonStr("cast"), "value" to value.toJson(), "to" to jsonStr(to.wire))
    }

    data object Now : ValueExpr {
        override fun toJson() = jsonObj("op" to jsonStr("now"))
    }

    data class Case(val whens: List<CaseWhen>, val otherwise: ValueExpr) : ValueExpr {
        override fun toJson() =
            jsonObj(
                "op" to jsonStr("case"),
                "whens" to jsonArr(whens.map { jsonObj("when" to it.whenExpr.toJson(), "then" to it.then.toJson()) }),
                "otherwise" to otherwise.toJson(),
            )
    }

    companion object {
        fun fromJson(e: JsonElement): ValueExpr {
            val (tag, body) = peekTag(e, "op", "valueExpr")
            val o = StrictObj(body, "valueExpr.$tag")
            val r: ValueExpr =
                when (tag) {
                    "field" -> Field(o.str("field"))
                    "literal" -> Literal(o.req("value"))
                    "concat" -> Concat(o.arr("parts").map { fromJson(it) })
                    "add" -> Add(fromJson(o.req("left")), fromJson(o.req("right")))
                    "sub" -> Sub(fromJson(o.req("left")), fromJson(o.req("right")))
                    "mul" -> Mul(fromJson(o.req("left")), fromJson(o.req("right")))
                    "div" -> Div(fromJson(o.req("left")), fromJson(o.req("right")))
                    "coalesce" -> Coalesce(o.arr("parts").map { fromJson(it) })
                    "lower" -> Lower(fromJson(o.req("value")))
                    "upper" -> Upper(fromJson(o.req("value")))
                    "trim" -> Trim(fromJson(o.req("value")))
                    "cast" -> CastTo(fromJson(o.req("value")), Cast.fromWire(o.str("to")))
                    "now" -> Now
                    "case" ->
                        Case(
                            o.arr("whens").map { w ->
                                val wo = StrictObj.of(w, "valueExpr.case.when")
                                val cw = CaseWhen(FilterExpr.fromJson(wo.req("when")), fromJson(wo.req("then")))
                                wo.finish()
                                cw
                            },
                            fromJson(o.req("otherwise")),
                        )
                    else -> wireFail("unknown value-expression op `$tag`")
                }
            o.finish()
            return r
        }
    }
}
