package dev.parrtdb.inmemory

import dev.parrtdb.errors.badRequest
import dev.parrtdb.wire.FilterExpr
import dev.parrtdb.wire.ValueExpr
import dev.parrtdb.wire.WireException
import dev.parrtdb.wire.isNumberValue
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/** Deep structural equality of two field types (literal values compare numerically, not by spelling). */
fun FieldType.equalTo(other: FieldType): Boolean {
    if (kind != other.kind || table != other.table || dimensions != other.dimensions) return false
    if (onDelete != other.onDelete) return false
    if (!strictValueEq(value, other.value)) return false
    if ((inner == null) != (other.inner == null)) return false
    if (inner != null && !inner.equalTo(other.inner!!)) return false
    if (variants.size != other.variants.size) return false
    for (i in variants.indices) if (!variants[i].equalTo(other.variants[i])) return false
    if (fields.size != other.fields.size) return false
    for ((k, v) in fields) {
        val ov = other.fields[k] ?: return false
        if (!v.equalTo(ov)) return false
    }
    return true
}

private fun bad(msg: String): Nothing = throw badRequest(msg)

private fun JsonElement?.present(): JsonElement? = if (this == null || this is JsonNull) null else this

private fun rejectUnknownKeys(obj: JsonObject, what: String, vararg allowed: String) {
    val set = allowed.toSet()
    for (k in obj.keys) if (k !in set) bad("$what carries unknown key '$k'")
}

private fun optString(obj: JsonObject, key: String): String? {
    val v = obj[key].present() ?: return null
    return (v as? JsonPrimitive)?.takeIf { it.isString }?.content ?: bad("$key must be a string")
}

private fun <T> wireWrap(block: () -> T): T =
    try {
        block()
    } catch (e: WireException) {
        throw badRequest(e.message ?: "invalid expression")
    }

/**
 * Converts a wire schema JSON value (the SchemaBuilder output shape) into the typed model.
 * Unknown keys are rejected with BAD_REQUEST, mirroring the server's deny-unknown-fields parse.
 */
fun parseSchema(v: JsonElement): SchemaDef {
    val obj = v as? JsonObject ?: bad("schema must be a JSON object")
    val tablesV = obj["tables"] ?: bad("schema is missing the tables map")
    val tablesObj = tablesV as? JsonObject ?: bad("schema.tables must be an object")
    val out = LinkedHashMap<String, TableDef>()
    for ((name, tv) in tablesObj) out[name] = parseTableDef(tv)
    return SchemaDef(out)
}

private fun parseTableDef(v: JsonElement): TableDef {
    val obj = v as? JsonObject ?: bad("table definition must be a JSON object")
    rejectUnknownKeys(
        obj, "table", "fields", "indexes", "ownerField", "collaboratorsField", "ttl", "updatedAtField",
        "autoIncrementField", "authorize", "defaults", "constraints", "computed", "softDelete",
        "description", "fieldDescriptions",
    )
    val fieldsObj = obj["fields"] as? JsonObject ?: bad("table.fields must be an object")
    val fields = LinkedHashMap<String, FieldType>()
    for ((fname, fv) in fieldsObj) fields[fname] = parseFieldType(fv)

    val indexes = ArrayList<IndexDef>()
    obj["indexes"].present()?.let { iv ->
        val arr = iv as? JsonArray ?: bad("table.indexes must be an array")
        for (e in arr) indexes += parseIndexDef(e)
    }
    val ownerField = optString(obj, "ownerField")
    val collaboratorsField = optString(obj, "collaboratorsField")
    val updatedAtField = optString(obj, "updatedAtField")
    val autoIncrementField = optString(obj, "autoIncrementField")
    val description = optString(obj, "description")

    var fieldDescriptions: Map<String, String>? = null
    obj["fieldDescriptions"].present()?.let { fv ->
        val fdObj = fv as? JsonObject ?: bad("table.fieldDescriptions must be an object")
        val m = LinkedHashMap<String, String>()
        for ((k, e) in fdObj) m[k] = strOf(e) ?: bad("table.fieldDescriptions entries must be strings")
        fieldDescriptions = m
    }

    var softDelete = false
    obj["softDelete"]?.let { sv ->
        // Go: absent is false; present (even null) must be a bool.
        softDelete = boolOf(sv) ?: bad("table.softDelete must be a boolean")
    }

    val ttl = obj["ttl"].present()?.let { parseTtl(it) }

    val defaults = LinkedHashMap<String, JsonElement>()
    obj["defaults"].present()?.let { dv ->
        val dobj = dv as? JsonObject ?: bad("table.defaults must be an object")
        for ((k, e) in dobj) defaults[k] = e
    }

    val constraints = LinkedHashMap<String, FieldConstraints>()
    obj["constraints"].present()?.let { cv ->
        val cobj = cv as? JsonObject ?: bad("table.constraints must be an object")
        for ((fname, c) in cobj) {
            val fco = c as? JsonObject ?: bad("table.constraints entries must be objects")
            rejectUnknownKeys(fco, "constraints", "min", "max", "minLength", "maxLength", "pattern")
            var min: JsonElement? = null
            var max: JsonElement? = null
            var minLength: Long? = null
            var maxLength: Long? = null
            var pattern: String? = null
            fco["min"].present()?.let { min = numOf(it) ?: bad("constraints.min must be a number") }
            fco["max"].present()?.let { max = numOf(it) ?: bad("constraints.max must be a number") }
            fco["minLength"].present()?.let {
                val n = numOf(it) ?: bad("constraints.minLength must be a number")
                minLength = try { jsonNumberI64(n) } catch (_: Exception) { bad("constraints.minLength must be an integer") }
            }
            fco["maxLength"].present()?.let {
                val n = numOf(it) ?: bad("constraints.maxLength must be a number")
                maxLength = try { jsonNumberI64(n) } catch (_: Exception) { bad("constraints.maxLength must be an integer") }
            }
            fco["pattern"].present()?.let { pattern = strOf(it) ?: bad("constraints.pattern must be a string") }
            constraints[fname] = FieldConstraints(min, max, minLength, maxLength, pattern)
        }
    }

    val computed = LinkedHashMap<String, ValueExpr>()
    obj["computed"].present()?.let { cv ->
        val cobj = cv as? JsonObject ?: bad("table.computed must be an object")
        for ((k, e) in cobj) computed[k] = wireWrap { ValueExpr.fromJson(e) }
    }

    val authorize = obj["authorize"].present()?.let { wireWrap { FilterExpr.fromJson(it) } }

    return TableDef(
        fields = fields,
        indexes = indexes,
        ownerField = ownerField,
        collaboratorsField = collaboratorsField,
        ttl = ttl,
        updatedAtField = updatedAtField,
        autoIncrementField = autoIncrementField,
        authorize = authorize,
        defaults = defaults,
        constraints = constraints,
        computed = computed,
        softDelete = softDelete,
        description = description,
        fieldDescriptions = fieldDescriptions,
    )
}

private fun parseIndexDef(v: JsonElement): IndexDef {
    val obj = v as? JsonObject ?: bad("index definition must be a JSON object")
    rejectUnknownKeys(obj, "index", "name", "fields", "search", "trgm", "vector", "unique", "where", "language")
    val name = strOf(obj["name"]) ?: bad("index.name must be a string")
    val fieldsV = obj["fields"] as? JsonArray ?: bad("index.fields must be an array")
    val fields = fieldsV.map { strOf(it) ?: bad("index.fields entries must be strings") }
    var search = false
    var unique = false
    var trgm = false
    obj["search"].present()?.let { search = boolOf(it) ?: bad("index.search must be a boolean") }
    obj["unique"].present()?.let { unique = boolOf(it) ?: bad("index.unique must be a boolean") }
    obj["trgm"].present()?.let { trgm = boolOf(it) ?: bad("index.trgm must be a boolean") }
    var vector: VectorIndexSpec? = null
    obj["vector"].present()?.let { vv ->
        val vobj = vv as? JsonObject ?: bad("index.vector must be an object")
        rejectUnknownKeys(vobj, "vector", "dimensions", "filterFields", "metric")
        val dimsV = numOf(vobj["dimensions"]) ?: bad("vector.dimensions must be a number")
        val dims = try { jsonNumberI64(dimsV) } catch (_: Exception) { bad("vector.dimensions must be an integer") }
        var filterFields: List<String> = emptyList()
        vobj["filterFields"].present()?.let { ff ->
            val farr = ff as? JsonArray ?: bad("vector.filterFields must be an array")
            filterFields = farr.map { strOf(it) ?: bad("vector.filterFields entries must be strings") }
        }
        var metric = "cosine"
        vobj["metric"].present()?.let { metric = strOf(it) ?: bad("vector.metric must be a string") }
        vector = VectorIndexSpec(dims.toInt(), filterFields, metric)
    }
    val where = obj["where"].present()?.let { wireWrap { FilterExpr.fromJson(it) } }
    var language: String? = null
    obj["language"].present()?.let { language = strOf(it) ?: bad("index.language must be a string") }
    return IndexDef(name, fields, search, trgm, vector, unique, where, language)
}

private fun parseTtl(v: JsonElement): TtlDef {
    val obj = v as? JsonObject ?: bad("ttl must be an object")
    rejectUnknownKeys(obj, "ttl", "field", "defaultDurationMs")
    val field = strOf(obj["field"]) ?: bad("ttl.field must be a string")
    var dur: Long? = null
    obj["defaultDurationMs"].present()?.let {
        val n = numOf(it) ?: bad("ttl.defaultDurationMs must be a number")
        dur = try { jsonNumberI64(n) } catch (_: Exception) { bad("ttl.defaultDurationMs must be an integer") }
    }
    return TtlDef(field, dur)
}

/** Converts the tagged wire shape ({"type": ...}) into [FieldType]; unknown tags are rejected. */
internal fun parseFieldType(v: JsonElement): FieldType {
    val obj = v as? JsonObject ?: bad("field type must be a JSON object")
    val kind = strOf(obj["type"]) ?: bad("field type is missing the 'type' tag")
    val allowed: Map<String, List<String>> =
        mapOf(
            "string" to emptyList(), "number" to emptyList(), "boolean" to emptyList(), "null" to emptyList(),
            "int64" to emptyList(), "bytes" to emptyList(), "any" to emptyList(),
            "id" to listOf("table", "onDelete"),
            "literal" to listOf("value"),
            "optional" to listOf("inner"),
            "union" to listOf("variants"),
            "array" to listOf("element"),
            "object" to listOf("fields"),
            "record" to listOf("value"),
            "vector" to listOf("dimensions"),
        )
    val keys = allowed[kind] ?: bad("field type: unknown type '$kind'")
    rejectUnknownKeys(obj, "field type", "type", *keys.toTypedArray())
    return when (kind) {
        "id" -> {
            val table = strOf(obj["table"]) ?: bad("id field type requires 'table'")
            var od: OnDeleteAction? = null
            obj["onDelete"].present()?.let { odv ->
                val s = strOf(odv) ?: bad("onDelete must be a string")
                od = OnDeleteAction.entries.firstOrNull { it.wire == s } ?: bad("onDelete must be one of cascade|restrict|setNull")
            }
            FieldType(kind, table = table, onDelete = od)
        }
        "literal" -> FieldType(kind, value = obj["value"] ?: bad("literal field type requires 'value'"))
        "optional", "array", "record" -> {
            val key = when (kind) { "array" -> "element"; "record" -> "value"; else -> "inner" }
            val iv = obj[key] ?: bad("field type requires '$key'")
            FieldType(kind, inner = parseFieldType(iv))
        }
        "union" -> {
            val vs = obj["variants"] as? JsonArray ?: bad("union field type requires 'variants'")
            FieldType(kind, variants = vs.map { parseFieldType(it) })
        }
        "object" -> {
            val fo = obj["fields"] as? JsonObject ?: bad("object field type requires 'fields'")
            val m = LinkedHashMap<String, FieldType>()
            for ((k, fv) in fo) m[k] = parseFieldType(fv)
            FieldType(kind, fields = m)
        }
        "vector" -> {
            val dv = numOf(obj["dimensions"]) ?: bad("vector field type requires 'dimensions'")
            FieldType(kind, dimensions = jsonNumberI64(dv).toInt())
        }
        else -> FieldType(kind)
    }
}
