package dev.parrtdb.dsl

import dev.parrtdb.wire.WireException
import dev.parrtdb.wire.jsonBool
import dev.parrtdb.wire.jsonNum
import dev.parrtdb.wire.jsonStr
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/**
 * Converts plain Kotlin values to wire JSON: null, Boolean, String, Int/Long/Short/Byte (exact
 * integer spelling), Double/Float (finite only), Map<String, *>, Iterable<*>, Array<*>, or an
 * existing [JsonElement]. int64 schema fields take decimal STRINGS on the wire, so pass `"42"`.
 */
fun json(value: Any?): JsonElement =
    when (value) {
        null -> JsonNull
        is JsonElement -> value
        is Boolean -> jsonBool(value)
        is String -> jsonStr(value)
        is Int -> jsonNum(value)
        is Long -> jsonNum(value)
        is Short -> jsonNum(value.toLong())
        is Byte -> jsonNum(value.toLong())
        is Double -> if (value.isFinite()) JsonPrimitive(value) else throw WireException("non-finite number")
        is Float -> if (value.isFinite()) JsonPrimitive(value.toDouble()) else throw WireException("non-finite number")
        is Map<*, *> -> JsonObject(value.entries.associate { (k, v) -> (k as? String ?: throw WireException("object keys must be strings")) to json(v) })
        is Iterable<*> -> JsonArray(value.map { json(it) })
        is Array<*> -> JsonArray(value.map { json(it) })
        else -> throw WireException("unsupported value type ${value::class.simpleName}")
    }

/** Builds a JSON object from key/value pairs of plain Kotlin values. */
fun obj(vararg entries: Pair<String, Any?>): JsonObject = json(mapOf(*entries)) as JsonObject
