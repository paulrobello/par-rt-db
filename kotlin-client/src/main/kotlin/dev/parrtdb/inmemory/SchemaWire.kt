package dev.parrtdb.inmemory

import dev.parrtdb.wire.jsonArr
import dev.parrtdb.wire.jsonBool
import dev.parrtdb.wire.jsonNum
import dev.parrtdb.wire.jsonStr
import dev.parrtdb.wire.jsonStrArr
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

private fun sortedObj(entries: Map<String, JsonElement>): JsonObject =
    JsonObject(entries.entries.sortedWith { a, b -> compareUtf8(a.key, b.key) }.associate { it.key to it.value })

/** Serializes a schema to the wire shape accepted by [parseSchema]; table and field keys are sorted. */
fun schemaToWire(schema: SchemaDef): JsonElement {
    val tables = LinkedHashMap<String, JsonElement>()
    for (name in schema.tables.keys.sortedWith { a, b -> compareUtf8(a, b) }) tables[name] = tableDefToWire(schema.tables.getValue(name))
    return JsonObject(mapOf("tables" to JsonObject(tables)))
}

/** Serializes one table definition. Indexes are always emitted (the server's derived schema does too). */
fun tableDefToWire(td: TableDef): JsonElement {
    val out = LinkedHashMap<String, JsonElement>()
    out["fields"] = sortedObj(td.fields.mapValues { fieldTypeToWire(it.value) })
    out["indexes"] =
        jsonArr(
            td.indexes.map { idx ->
                val io = LinkedHashMap<String, JsonElement>()
                io["name"] = jsonStr(idx.name)
                io["fields"] = jsonStrArr(idx.fields)
                if (idx.search) io["search"] = jsonBool(true)
                if (idx.unique) io["unique"] = jsonBool(true)
                idx.vector?.let { v ->
                    val vo = LinkedHashMap<String, JsonElement>()
                    vo["dimensions"] = jsonNum(formatI64(v.dimensions.toLong()))
                    if (v.filterFields.isNotEmpty()) vo["filterFields"] = jsonStrArr(v.filterFields)
                    if (v.metric.isNotEmpty() && v.metric != "cosine") vo["metric"] = jsonStr(v.metric)
                    io["vector"] = sortedObj(vo)
                }
                idx.whereClause?.let { io["where"] = it.toJson() }
                idx.language?.let { io["language"] = jsonStr(it) }
                sortedObj(io)
            },
        )
    td.ownerField?.let { out["ownerField"] = jsonStr(it) }
    td.collaboratorsField?.let { out["collaboratorsField"] = jsonStr(it) }
    td.updatedAtField?.let { out["updatedAtField"] = jsonStr(it) }
    td.autoIncrementField?.let { out["autoIncrementField"] = jsonStr(it) }
    td.ttl?.let { ttl ->
        val to = LinkedHashMap<String, JsonElement>()
        to["field"] = jsonStr(ttl.field)
        ttl.defaultDurationMs?.let { to["defaultDurationMs"] = jsonNum(formatI64(it)) }
        out["ttl"] = sortedObj(to)
    }
    td.authorize?.let { out["authorize"] = it.toJson() }
    if (td.defaults.isNotEmpty()) out["defaults"] = sortedObj(td.defaults)
    if (td.constraints.isNotEmpty()) {
        out["constraints"] =
            sortedObj(
                td.constraints.mapValues { (_, fc) ->
                    val fo = LinkedHashMap<String, JsonElement>()
                    fc.min?.let { fo["min"] = it }
                    fc.max?.let { fo["max"] = it }
                    fc.minLength?.let { fo["minLength"] = jsonNum(formatI64(it)) }
                    fc.maxLength?.let { fo["maxLength"] = jsonNum(formatI64(it)) }
                    fc.pattern?.let { fo["pattern"] = jsonStr(it) }
                    sortedObj(fo)
                },
            )
    }
    if (td.computed.isNotEmpty()) out["computed"] = sortedObj(td.computed.mapValues { it.value.toJson() })
    if (td.softDelete) out["softDelete"] = jsonBool(true)
    td.description?.let { out["description"] = jsonStr(it) }
    td.fieldDescriptions?.takeIf { it.isNotEmpty() }?.let { fd -> out["fieldDescriptions"] = sortedObj(fd.mapValues { jsonStr(it.value) }) }
    return sortedObj(out)
}

/** Serializes a field type to its tagged wire shape. */
fun fieldTypeToWire(ty: FieldType): JsonElement {
    val out = LinkedHashMap<String, JsonElement>()
    out["type"] = jsonStr(ty.kind)
    when (ty.kind) {
        "id" -> {
            out["table"] = jsonStr(ty.table)
            ty.onDelete?.let { out["onDelete"] = jsonStr(it.wire) }
        }
        "literal" -> ty.value?.let { out["value"] = it }
        "optional" -> out["inner"] = fieldTypeToWire(ty.inner!!)
        "array" -> out["element"] = fieldTypeToWire(ty.inner!!)
        "record" -> out["value"] = fieldTypeToWire(ty.inner!!)
        "union" -> out["variants"] = jsonArr(ty.variants.map { fieldTypeToWire(it) })
        "object" -> out["fields"] = sortedObj(ty.fields.mapValues { fieldTypeToWire(it.value) })
        "vector" -> out["dimensions"] = jsonNum(formatI64(ty.dimensions.toLong()))
    }
    return sortedObj(out)
}
