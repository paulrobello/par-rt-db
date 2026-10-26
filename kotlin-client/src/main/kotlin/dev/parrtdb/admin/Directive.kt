package dev.parrtdb.admin

import dev.parrtdb.wire.Cast
import dev.parrtdb.wire.FilterExpr
import dev.parrtdb.wire.StrictObj
import dev.parrtdb.wire.ValueExpr
import dev.parrtdb.wire.jsonObj
import dev.parrtdb.wire.jsonStr
import dev.parrtdb.wire.peekTag
import dev.parrtdb.wire.wireFail
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * evalExpr's `expr`: a typed [ValueExpr] (safe path) or a legacy raw-SQL string
 * (deprecated, root admin key only). Exactly one side is set.
 */
data class ExprSource(val typed: ValueExpr? = null, val legacy: String? = null) {
    fun toJson(): JsonElement = typed?.toJson() ?: JsonPrimitive(legacy ?: "")

    companion object {
        fun fromJson(e: JsonElement): ExprSource =
            if (e is JsonPrimitive && e.isString) ExprSource(legacy = e.content) else ExprSource(typed = ValueExpr.fromJson(e))
    }
}

/** evalExpr's `where`: a typed [FilterExpr] or a legacy raw-SQL predicate string. */
data class CondSource(val typed: FilterExpr? = null, val legacy: String? = null) {
    fun toJson(): JsonElement = typed?.toJson() ?: JsonPrimitive(legacy ?: "")

    companion object {
        fun fromJson(e: JsonElement): CondSource =
            if (e is JsonPrimitive && e.isString) CondSource(legacy = e.content) else CondSource(typed = FilterExpr.fromJson(e))
    }
}

/** One schema-migration step, tagged by `op` (mirrors migrate.rs::Directive). */
sealed interface Directive {
    fun toJson(): JsonObject

    data class RenameField(val table: String, val from: String, val to: String) : Directive {
        override fun toJson() = jsonObj("op" to jsonStr("renameField"), "table" to jsonStr(table), "from" to jsonStr(from), "to" to jsonStr(to))
    }

    data class RenameTable(val from: String, val to: String) : Directive {
        override fun toJson() = jsonObj("op" to jsonStr("renameTable"), "from" to jsonStr(from), "to" to jsonStr(to))
    }

    /** [to] is a field-type value (`{"type":"string"}`); [default] substitutes for un-coercible rows (null = roll back). */
    data class ChangeType(
        val table: String,
        val field: String,
        val to: JsonElement,
        val cast: Cast,
        val default: JsonElement? = null,
    ) : Directive {
        override fun toJson() =
            jsonObj(
                "op" to jsonStr("changeType"),
                "table" to jsonStr(table),
                "field" to jsonStr(field),
                "to" to to,
                "cast" to jsonStr(cast.wire),
                "default" to default,
            )
    }

    data class DropField(val table: String, val field: String) : Directive {
        override fun toJson() = jsonObj("op" to jsonStr("dropField"), "table" to jsonStr(table), "field" to jsonStr(field))
    }

    data class DropTable(val name: String) : Directive {
        override fun toJson() = jsonObj("op" to jsonStr("dropTable"), "name" to jsonStr(name))
    }

    data class DropIndex(val table: String, val name: String) : Directive {
        override fun toJson() = jsonObj("op" to jsonStr("dropIndex"), "table" to jsonStr(table), "name" to jsonStr(name))
    }

    data class SetDefault(val table: String, val field: String, val value: JsonElement) : Directive {
        override fun toJson() = jsonObj("op" to jsonStr("setDefault"), "table" to jsonStr(table), "field" to jsonStr(field), "value" to value)
    }

    data class EvalExpr(val table: String, val set: String, val expr: ExprSource, val where: CondSource? = null) : Directive {
        override fun toJson() =
            jsonObj(
                "op" to jsonStr("evalExpr"),
                "table" to jsonStr(table),
                "set" to jsonStr(set),
                "expr" to expr.toJson(),
                "where" to where?.toJson(),
            )
    }

    companion object {
        /** Strict decode: unknown `op` or unknown field is rejected. */
        fun fromJson(e: JsonElement): Directive {
            val (tag, body) = peekTag(e, "op", "directive")
            val o = StrictObj(body, "directive.$tag")
            val r: Directive =
                when (tag) {
                    "renameField" -> RenameField(o.str("table"), o.str("from"), o.str("to"))
                    "renameTable" -> RenameTable(o.str("from"), o.str("to"))
                    "changeType" -> ChangeType(o.str("table"), o.str("field"), o.req("to"), Cast.fromWire(o.str("cast")), o.opt("default"))
                    "dropField" -> DropField(o.str("table"), o.str("field"))
                    "dropTable" -> DropTable(o.str("name"))
                    "dropIndex" -> DropIndex(o.str("table"), o.str("name"))
                    "setDefault" -> SetDefault(o.str("table"), o.str("field"), o.req("value"))
                    "evalExpr" -> EvalExpr(o.str("table"), o.str("set"), ExprSource.fromJson(o.req("expr")), o.opt("where")?.let { CondSource.fromJson(it) })
                    else -> wireFail("unknown directive op `$tag`")
                }
            o.finish()
            return r
        }
    }
}
