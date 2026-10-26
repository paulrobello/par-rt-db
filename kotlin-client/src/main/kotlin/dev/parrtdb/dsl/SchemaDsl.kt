package dev.parrtdb.dsl

import dev.parrtdb.wire.FilterExpr
import dev.parrtdb.wire.ValueExpr
import dev.parrtdb.wire.jsonArr
import dev.parrtdb.wire.jsonBool
import dev.parrtdb.wire.jsonNum
import dev.parrtdb.wire.jsonObj
import dev.parrtdb.wire.jsonStr
import dev.parrtdb.wire.jsonStrArr
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

/** A schema field type, held as its wire JSON. */
class FieldType internal constructor(val json: JsonObject) {
    companion object {
        private fun simple(tag: String) = FieldType(jsonObj("type" to jsonStr(tag)))

        fun string(): FieldType = simple("string")

        fun number(): FieldType = simple("number")

        fun boolean(): FieldType = simple("boolean")

        fun nullType(): FieldType = simple("null")

        /** int64 values travel as decimal strings. */
        fun int64(): FieldType = simple("int64")

        fun bytes(): FieldType = simple("bytes")

        fun any(): FieldType = simple("any")

        fun id(table: String, onDelete: OnDelete? = null): FieldType =
            FieldType(jsonObj("type" to jsonStr("id"), "table" to jsonStr(table), "onDelete" to onDelete?.let { jsonStr(it.wire) }))

        fun literal(value: Any?): FieldType = FieldType(jsonObj("type" to jsonStr("literal"), "value" to json(value)))

        fun optional(inner: FieldType): FieldType = FieldType(jsonObj("type" to jsonStr("optional"), "inner" to inner.json))

        fun union(vararg variants: FieldType): FieldType =
            FieldType(jsonObj("type" to jsonStr("union"), "variants" to jsonArr(variants.map { it.json })))

        fun array(element: FieldType): FieldType = FieldType(jsonObj("type" to jsonStr("array"), "element" to element.json))

        fun record(value: FieldType): FieldType = FieldType(jsonObj("type" to jsonStr("record"), "value" to value.json))

        fun obj(fields: Map<String, FieldType>): FieldType =
            FieldType(jsonObj("type" to jsonStr("object"), "fields" to JsonObject(fields.mapValues { it.value.json })))

        fun vector(dimensions: Int): FieldType = FieldType(jsonObj("type" to jsonStr("vector"), "dimensions" to jsonNum(dimensions)))
    }
}

enum class OnDelete(val wire: String) {
    Cascade("cascade"),
    Restrict("restrict"),
    SetNull("setNull"),
}

/** Builds a schema object: `defineSchema { table("tasks") { field("title", FieldType.string()) } }`. */
class SchemaBuilder {
    private val tables = LinkedHashMap<String, JsonElement>()

    fun table(name: String, build: TableBuilder.() -> Unit): SchemaBuilder {
        tables[name] = TableBuilder().apply(build).build()
        return this
    }

    fun build(): JsonObject = jsonObj("tables" to JsonObject(LinkedHashMap(tables)))
}

fun defineSchema(build: SchemaBuilder.() -> Unit): JsonObject = SchemaBuilder().apply(build).build()

class TableBuilder {
    private val fields = LinkedHashMap<String, JsonElement>()
    private val indexes = ArrayList<MutableMap<String, JsonElement>>()
    private val extra = LinkedHashMap<String, JsonElement>()

    fun field(name: String, type: FieldType): TableBuilder = also { fields[name] = type.json }

    fun index(name: String, vararg fields: String): TableBuilder = also {
        indexes += linkedMapOf("name" to jsonStr(name), "fields" to jsonStrArr(fields.toList()))
    }

    private fun lastIndex(what: String): MutableMap<String, JsonElement> =
        indexes.lastOrNull() ?: throw IllegalStateException("$what() must follow index()/searchIndex()")

    /** Marks the most recently declared index unique. */
    fun unique(): TableBuilder = also { lastIndex("unique")["unique"] = jsonBool(true) }

    /** Marks the most recently declared index as a trigram index. */
    fun trgm(): TableBuilder = also { lastIndex("trgm")["trgm"] = jsonBool(true) }

    /** Restricts the most recently declared index to rows matching [f] (partial index). */
    fun where(f: FilterExpr): TableBuilder = also { lastIndex("where")["where"] = f.toJson() }

    fun searchIndex(name: String, language: String? = null, vararg fields: String): TableBuilder = also {
        val idx = linkedMapOf("name" to jsonStr(name), "fields" to jsonStrArr(fields.toList()), "search" to jsonBool(true))
        language?.let { idx["language"] = jsonStr(it) }
        indexes += idx
    }

    fun vectorIndex(name: String, field: String, dimensions: Int, filterFields: List<String> = emptyList(), metric: String? = null): TableBuilder = also {
        val spec = linkedMapOf<String, JsonElement>("dimensions" to jsonNum(dimensions))
        if (filterFields.isNotEmpty()) spec["filterFields"] = jsonStrArr(filterFields)
        metric?.let { spec["metric"] = jsonStr(it) }
        indexes += linkedMapOf("name" to jsonStr(name), "fields" to jsonStrArr(listOf(field)), "vector" to JsonObject(spec))
    }

    fun ownerField(field: String): TableBuilder = also { extra["ownerField"] = jsonStr(field) }

    fun collaboratorsField(field: String): TableBuilder = also { extra["collaboratorsField"] = jsonStr(field) }

    fun authorize(predicate: FilterExpr): TableBuilder = also { extra["authorize"] = predicate.toJson() }

    fun ttl(field: String, defaultDurationMs: Long? = null): TableBuilder = also {
        extra["ttl"] = jsonObj("field" to jsonStr(field), "defaultDurationMs" to defaultDurationMs?.let { jsonNum(it) })
    }

    fun updatedAtField(field: String): TableBuilder = also { extra["updatedAtField"] = jsonStr(field) }

    fun autoIncrementField(field: String): TableBuilder = also { extra["autoIncrementField"] = jsonStr(field) }

    fun defaults(entries: Map<String, Any?>): TableBuilder = also { extra["defaults"] = json(entries) }

    /** Per-field constraints, e.g. `mapOf("age" to obj("min" to 0, "max" to 150))`. */
    fun constraints(entries: Map<String, Any?>): TableBuilder = also { extra["constraints"] = json(entries) }

    fun describe(tableDescription: String? = null, fieldDescriptions: Map<String, String> = emptyMap()): TableBuilder = also {
        if (!tableDescription.isNullOrEmpty()) extra["description"] = jsonStr(tableDescription)
        if (fieldDescriptions.isNotEmpty()) extra["fieldDescriptions"] = JsonObject(fieldDescriptions.mapValues { jsonStr(it.value) })
    }

    fun computed(name: String, expr: ValueExpr): TableBuilder = also {
        val cur = (extra["computed"] as? JsonObject)?.toMutableMap() ?: linkedMapOf()
        cur[name] = expr.toJson()
        extra["computed"] = JsonObject(cur)
    }

    fun softDelete(): TableBuilder = also { extra["softDelete"] = jsonBool(true) }

    fun build(): JsonObject {
        val out = LinkedHashMap<String, JsonElement>(extra)
        out["fields"] = JsonObject(LinkedHashMap(fields))
        if (indexes.isNotEmpty()) out["indexes"] = jsonArr(indexes.map { JsonObject(LinkedHashMap(it)) })
        return JsonObject(out)
    }
}
