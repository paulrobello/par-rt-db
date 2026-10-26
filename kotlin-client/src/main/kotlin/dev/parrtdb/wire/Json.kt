package dev.parrtdb.wire

import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.JsonUnquotedLiteral
import kotlinx.serialization.json.booleanOrNull
import kotlinx.serialization.json.contentOrNull

/** A wire decoding failure: unknown field, wrong type, unknown tag, trailing data. */
class WireException(message: String) : IllegalArgumentException(message)

fun wireFail(message: String): Nothing = throw WireException(message)

object WireJson {
    private val json = Json { isLenient = false }

    /** Parses one JSON value; trailing data is rejected. Number literals keep their spelling. */
    fun parse(text: String): JsonElement =
        try {
            json.parseToJsonElement(text)
        } catch (e: Exception) {
            wireFail("invalid JSON: ${e.message}")
        }

    fun encode(value: JsonElement): String = json.encodeToString(JsonElement.serializer(), value)
}

fun jsonNum(literal: String): JsonElement = JsonUnquotedLiteral(literal)

fun jsonNum(value: Long): JsonElement = JsonPrimitive(value)

fun jsonNum(value: Int): JsonElement = JsonPrimitive(value)

fun jsonNum(value: Double): JsonElement = JsonPrimitive(value)

fun jsonStr(value: String): JsonElement = JsonPrimitive(value)

fun jsonBool(value: Boolean): JsonElement = JsonPrimitive(value)

fun jsonObj(vararg entries: Pair<String, JsonElement?>): JsonObject {
    val m = LinkedHashMap<String, JsonElement>()
    for ((k, v) in entries) if (v != null) m[k] = v
    return JsonObject(m)
}

fun jsonArr(items: Iterable<JsonElement>): JsonArray = JsonArray(items.toList())

fun jsonStrArr(items: Iterable<String>): JsonArray = JsonArray(items.map { JsonPrimitive(it) })

val JsonElement.isNull: Boolean get() = this is JsonNull

/** A string primitive (as opposed to a number/bool literal). */
val JsonElement.isStringValue: Boolean get() = this is JsonPrimitive && this.isString

val JsonElement.isNumberValue: Boolean
    get() = this is JsonPrimitive && !this.isString && this !is JsonNull && this.booleanOrNull == null

val JsonElement.stringValue: String? get() = if (this is JsonPrimitive && this.isString) this.contentOrNull else null

/** Strips the tag key from an object copy. */
internal fun JsonObject.without(key: String): JsonObject = JsonObject(this.filterKeys { it != key })

/**
 * Strict object reader with serde `deny_unknown_fields` semantics: every key
 * read is recorded and [finish] rejects any key that was never read.
 */
class StrictObj(val obj: JsonObject, private val what: String) {
    private val seen = HashSet<String>()

    companion object {
        fun of(e: JsonElement, what: String): StrictObj {
            if (e !is JsonObject) wireFail("$what: expected a JSON object")
            return StrictObj(e, what)
        }
    }

    fun has(key: String): Boolean = obj.containsKey(key)

    /** Raw value or null when absent (an explicit JSON null is returned as JsonNull). */
    fun raw(key: String): JsonElement? {
        seen += key
        return obj[key]
    }

    fun req(key: String): JsonElement = raw(key) ?: wireFail("$what: missing field `$key`")

    /** Optional value: absent or JSON null both yield null. */
    fun opt(key: String): JsonElement? = raw(key)?.takeUnless { it is JsonNull }

    fun str(key: String): String = reqStr(req(key), "$what.$key")

    fun optStr(key: String): String? = opt(key)?.let { reqStr(it, "$what.$key") }

    fun long(key: String): Long = reqLong(req(key), "$what.$key")

    fun optLong(key: String): Long? = opt(key)?.let { reqLong(it, "$what.$key") }

    fun int(key: String): Int = long(key).let { if (it > Int.MAX_VALUE || it < Int.MIN_VALUE) wireFail("$what.$key out of range") else it.toInt() }

    fun optInt(key: String): Int? = optLong(key)?.let { it.toInt() }

    fun bool(key: String): Boolean = reqBool(req(key), "$what.$key")

    fun optBool(key: String): Boolean? = opt(key)?.let { reqBool(it, "$what.$key") }

    fun arr(key: String): JsonArray = req(key).let { it as? JsonArray ?: wireFail("$what.$key: expected an array") }

    fun optArr(key: String): JsonArray? = opt(key)?.let { it as? JsonArray ?: wireFail("$what.$key: expected an array") }

    fun objOf(key: String): JsonObject = req(key).let { it as? JsonObject ?: wireFail("$what.$key: expected an object") }

    fun strList(key: String): List<String> = arr(key).map { reqStr(it, "$what.$key[]") }

    fun optStrList(key: String): List<String>? = optArr(key)?.map { reqStr(it, "$what.$key[]") }

    fun finish() {
        val extra = obj.keys.filter { it !in seen }
        if (extra.isNotEmpty()) wireFail("$what: unknown field `${extra.first()}`")
    }
}

internal fun reqStr(e: JsonElement, what: String): String =
    (e as? JsonPrimitive)?.takeIf { it.isString }?.content ?: wireFail("$what: expected a string")

internal fun reqBool(e: JsonElement, what: String): Boolean =
    (e as? JsonPrimitive)?.takeIf { !it.isString }?.booleanOrNull ?: wireFail("$what: expected a boolean")

/** Strict integer: the literal must be a plain (optionally signed) decimal integer that fits in Long. */
internal fun reqLong(e: JsonElement, what: String): Long {
    val p = e as? JsonPrimitive
    if (p == null || p.isString || p is JsonNull) wireFail("$what: expected an integer")
    return p.content.toLongOrNull() ?: wireFail("$what: expected an integer")
}

/** Reads an object that is tagged by `tagKey`, returning the tag and the object without it. */
internal fun peekTag(e: JsonElement, tagKey: String, what: String): Pair<String, JsonObject> {
    val o = e as? JsonObject ?: wireFail("$what: expected a JSON object")
    val tag = o[tagKey] ?: wireFail("$what: missing `$tagKey` tag")
    val t = (tag as? JsonPrimitive)?.takeIf { it.isString }?.content ?: wireFail("$what: `$tagKey` must be a string")
    return t to o.without(tagKey)
}
