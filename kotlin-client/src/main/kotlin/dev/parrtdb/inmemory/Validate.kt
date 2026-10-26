package dev.parrtdb.inmemory

import dev.parrtdb.errors.RtDbException
import dev.parrtdb.errors.badRequest
import dev.parrtdb.errors.schemaViolation
import dev.parrtdb.wire.jsonNum
import dev.parrtdb.wire.jsonStr
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/** True for a 32-char lowercase hex string (an `_id` shape). */
fun isHexId(v: JsonElement): Boolean {
    val s = strOf(v) ?: return false
    return s.length == 32 && s.all { it in '0'..'9' || it in 'a'..'f' }
}

/** True for a strict `-?digits` decimal string within the i64 range (the canonical int64 wire form). */
fun isInt64String(v: JsonElement): Boolean = strOf(v)?.let { parseI64(it) != null } ?: false

/** True for base64 alphabet with 0-2 trailing `=` and a length that is a multiple of 4. */
fun isBase64String(v: JsonElement): Boolean {
    val s = strOf(v) ?: return false
    if (s.length % 4 != 0) return false
    var eq = 0
    for (i in s.length - 1 downTo 0) {
        val c = s[i]
        if (c == '=') {
            eq++
            continue
        }
        if (eq > 2) return false
        if (!(c in 'A'..'Z' || c in 'a'..'z' || c in '0'..'9' || c == '+' || c == '/')) return false
    }
    return eq <= 2
}

/** Recursive value validator (server validate_value): true when [value] satisfies [ty]. */
fun validateValue(ty: FieldType, value: JsonElement): Boolean =
    when (ty.kind) {
        "string" -> strOf(value) != null
        "number" -> isJsonNumber(value)
        "boolean" -> boolOf(value) != null
        "null" -> value is JsonNull
        "id" -> isHexId(value)
        "literal" -> jsonEq(value, ty.value)
        "optional" -> value is JsonNull || validateValue(ty.inner!!, value)
        "union" -> ty.variants.any { validateValue(it, value) }
        "array" -> value is JsonArray && value.all { validateValue(ty.inner!!, it) }
        "object" -> {
            if (value !is JsonObject) {
                false
            } else if (value.keys.any { it !in ty.fields }) {
                false
            } else {
                ty.fields.all { (name, fty) ->
                    val v = value[name]
                    if (v != null) validateValue(fty, v) else fty.kind == "optional"
                }
            }
        }
        "int64" -> isInt64String(value)
        "bytes" -> isBase64String(value)
        "any" -> true
        "record" -> value is JsonObject && value.values.all { validateValue(ty.inner!!, it) }
        "vector" ->
            value is JsonArray && value.size == ty.dimensions &&
                value.all { numOf(it)?.let { n -> jsonNumberF64(n).let { f -> !f.isNaN() && !f.isInfinite() } } ?: false }
        else -> false
    }

/**
 * Full-document validator: reserved (`_`-prefixed) and unknown fields are rejected, every declared
 * field is present-and-valid or absent-and-optional. Throws SCHEMA_VIOLATION on the first violation.
 */
fun validateDoc(table: TableDef, doc: JsonObject) {
    for (key in doc.keys) {
        if (key.startsWith("_")) throw schemaViolation("field '$key' is reserved")
        if (key !in table.fields) throw schemaViolation("unknown field '$key'")
    }
    for ((field, fty) in table.fields) {
        val v = doc[field]
        if (v != null) {
            if (!validateValue(fty, v)) throw schemaViolation("field '$field' has an invalid value")
            checkConstraintsForField(table, field, v)
        } else if (fty.kind != "optional") {
            throw schemaViolation("field '$field' is required")
        }
    }
}

/**
 * Compiles a constraint pattern. Java regex is a superset of RE2, so constructs RE2 rejects
 * (lookaround, backreferences) are refused here to keep push-time acceptance aligned with the server.
 */
internal fun compileRe2Pattern(pattern: String): Regex? {
    if (Regex("""\(\?<?[=!]|\\[1-9]|\\k<""").containsMatchIn(pattern)) return null
    return try {
        Regex(pattern)
    } catch (_: Exception) {
        null
    }
}

/**
 * Write-path constraint check for one present field value (server check_constraints_for_field).
 * Null never violates; bounds are inclusive (exact i64 compare on int64 decimal strings); lengths
 * count Unicode scalar values; `pattern` is an unanchored search.
 */
fun checkConstraintsForField(table: TableDef, field: String, value: JsonElement) {
    val fc = table.constraints[field]
    if (fc == null || isNullValue(value)) return
    fun violate(label: String): Nothing = throw schemaViolation("field '$field' violates its '$label' constraint")
    var fty = table.fields[field] ?: return
    while (fty.kind == "optional" && fty.inner != null) fty = fty.inner
    when (fty.kind) {
        "number" -> {
            val vn = numOf(value) ?: return
            val v = jsonNumberF64(vn)
            if (fc.min != null && v < jsonNumberF64(fc.min)) violate("min")
            if (fc.max != null && v > jsonNumberF64(fc.max)) violate("max")
        }
        "int64" -> {
            val v = strOf(value)?.let { parseI64(it) } ?: return
            if (fc.min != null) runCatching { jsonNumberI64(fc.min) }.getOrNull()?.let { if (v < it) violate("min") }
            if (fc.max != null) runCatching { jsonNumberI64(fc.max) }.getOrNull()?.let { if (v > it) violate("max") }
        }
        "string" -> {
            val s = strOf(value) ?: return
            val n = s.codePointCount(0, s.length)
            if (fc.minLength != null && n < fc.minLength) violate("minLength")
            if (fc.maxLength != null && n > fc.maxLength) violate("maxLength")
            if (fc.pattern != null) {
                val re = compileRe2Pattern(fc.pattern)
                if (re != null && !re.containsMatchIn(s)) violate("pattern")
            }
        }
    }
}

/** True for JSON null (absent is not representable here). */
fun isNullValue(v: JsonElement?): Boolean = v == null || v is JsonNull

/**
 * Removes keys whose value is null for an Optional field whose inner type does not itself accept
 * null (server strip_unset_optionals).
 */
fun stripUnsetOptionals(table: TableDef, doc: JsonObject): JsonObject {
    val out = LinkedHashMap<String, JsonElement>()
    for ((key, value) in doc) {
        if (isNullValue(value)) {
            val fty = table.fields[key]
            if (fty != null && fty.kind == "optional" && !validateValue(fty.inner!!, value)) continue
        }
        out[key] = value
    }
    return JsonObject(out)
}

private fun JsonObject.with(key: String, value: JsonElement): JsonObject {
    val m = LinkedHashMap<String, JsonElement>(this)
    m[key] = value
    return JsonObject(m)
}

/** Stamps the TTL field at insert when the table declares a defaultDurationMs and the doc omits the field. */
fun stampTtlDefault(table: TableDef, doc: JsonObject, now: Long): JsonObject {
    val ttl = table.ttl ?: return doc
    val dur = ttl.defaultDurationMs ?: return doc
    if (doc.containsKey(ttl.field)) return doc
    return doc.with(ttl.field, jsonNum((now + dur).toString()))
}

/** Stamps the table's updatedAtField with [now], overwriting any client-supplied value. */
fun stampUpdatedAt(table: TableDef, doc: JsonObject, now: Long): JsonObject {
    val f = table.updatedAtField ?: return doc
    val isInt64 = table.fields[f]?.kind == "int64"
    return doc.with(f, if (isInt64) jsonStr(now.toString()) else jsonNum(now.toString()))
}

/** Applies the table's defaults to a NEW doc: every key the doc omits is stamped from the schema. */
fun applyDefaults(table: TableDef, doc: JsonObject): JsonObject {
    if (table.defaults.isEmpty()) return doc
    val out = LinkedHashMap<String, JsonElement>(doc)
    for ((field, value) in table.defaults) if (!out.containsKey(field)) out[field] = value
    return JsonObject(out)
}

/**
 * Re-evaluates every computed entry over the final doc: a null result removes the key, a non-null
 * result overwrites it. An evaluation error fails the write, naming the field.
 */
fun stampComputed(table: TableDef, doc: JsonObject, now: Long): JsonObject {
    if (table.computed.isEmpty()) return doc
    // Go evaluates each entry against the progressively updated doc (map order); entries may not
    // reference each other (push-validated), so evaluating against `out` is equivalent.
    var out = doc
    for ((name, expr) in table.computed) {
        val value =
            try {
                evalValueExpr(expr, out, now, table.fields)
            } catch (e: RtDbException) {
                throw RtDbException(e.code, "computed field '$name': ${e.message}")
            }
        out =
            if (isNullValue(value)) {
                JsonObject(LinkedHashMap(out).also { it.remove(name) })
            } else {
                out.with(name, value)
            }
    }
    return out
}

/**
 * Merges a patch onto [doc] and re-validates the result (server txn::apply_patch). A computed key
 * in the patch is dropped, the autoIncrement field is immutable, and a null onto an Optional whose
 * inner rejects null deletes the key.
 */
fun applyPatch(table: TableDef, doc: JsonObject, fields: JsonObject, now: Long): JsonObject {
    table.autoIncrementField?.let { auto ->
        val value = fields[auto]
        if (value != null) {
            val existing = doc[auto]
            if (existing == null || !strictValueEq(existing, value)) {
                throw badRequest("autoIncrementField '$auto' cannot be changed")
            }
        }
    }
    val merged = LinkedHashMap<String, JsonElement>(doc)
    for ((field, value) in fields) {
        if (field in table.computed) continue
        val fty = table.fields[field] ?: throw schemaViolation("unknown field '$field'")
        if (fty.kind == "optional" && isNullValue(value) && !validateValue(fty.inner!!, value)) {
            merged.remove(field)
            continue
        }
        if (!validateValue(fty, value)) throw schemaViolation("field '$field' has an invalid value")
        merged[field] = value
    }
    val stamped = stampComputed(table, JsonObject(merged), now)
    validateDoc(table, stamped)
    return stamped
}

/** Compact JSON with object keys sorted recursively and `<`, `>`, `&` escaped (Go json.Marshal shape). */
fun canonical(v: JsonElement): String = StringBuilder().also { writeCompact(it, v, true) }.toString()
