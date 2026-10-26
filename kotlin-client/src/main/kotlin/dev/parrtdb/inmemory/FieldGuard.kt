package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.errors.badRequest
import dev.parrtdb.errors.notFound
import dev.parrtdb.errors.schemaViolation
import dev.parrtdb.wire.Step
import dev.parrtdb.wire.StepResult
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.math.BigDecimal
import java.math.BigInteger

private fun fieldPathDeclared(fields: Map<String, FieldType>, path: List<String>): Boolean {
    val field = fields[path[0]] ?: return false
    return fieldChildDeclared(field, path.drop(1))
}

private fun fieldChildDeclared(field: FieldType, path: List<String>): Boolean {
    if (path.isEmpty()) return true
    return when (field.kind) {
        "any" -> true
        "optional" -> field.inner != null && fieldChildDeclared(field.inner, path)
        "object" -> fieldPathDeclared(field.fields, path)
        "record" -> field.inner != null && fieldChildDeclared(field.inner, path.drop(1))
        "union" -> field.variants.any { fieldChildDeclared(it, path) }
        else -> false
    }
}

/** Exact integer value (when the literal is integral and below 2^127) plus its double approximation. */
private fun fieldNumber(value: JsonElement): Pair<BigInteger?, Double> {
    val text = (value as JsonPrimitive).content
    text.toLongOrNull()?.let { return BigInteger.valueOf(it) to it.toDouble() }
    if (text.isNotEmpty() && text.all { it in '0'..'9' }) {
        val big = BigInteger(text)
        if (big.bitLength() <= 64) return big to big.toDouble()
    }
    val number = text.toDoubleOrNull() ?: 0.0
    if (!number.isInfinite() && !number.isNaN() && number == Math.floor(number) && Math.abs(number) < Math.pow(2.0, 127.0)) {
        return BigDecimal(number).toBigInteger() to number
    }
    return null to number
}

internal fun fieldJsonEqual(left: JsonElement?, right: JsonElement?): Boolean {
    val l = left ?: JsonNull
    val r = right ?: JsonNull
    if (isJsonNumber(l)) {
        if (!isJsonNumber(r)) return false
        val (ai, af) = fieldNumber(l)
        val (bi, bf) = fieldNumber(r)
        if (ai != null && bi != null) return ai == bi
        return af == bf
    }
    if (l is JsonArray) {
        if (r !is JsonArray || l.size != r.size) return false
        return l.indices.all { fieldJsonEqual(l[it], r[it]) }
    }
    if (l is JsonObject) {
        if (r !is JsonObject || l.size != r.size) return false
        return l.all { (k, v) -> r.containsKey(k) && fieldJsonEqual(v, r[k]) }
    }
    return jsonEq(l, r)
}

private fun validUtf16(s: String): Boolean {
    var i = 0
    while (i < s.length) {
        val c = s[i]
        if (Character.isHighSurrogate(c)) {
            if (i + 1 >= s.length || !Character.isLowSurrogate(s[i + 1])) return false
            i += 2
            continue
        }
        if (Character.isLowSurrogate(c)) return false
        i++
    }
    return true
}

private fun utf8Len(s: String): Int = s.toByteArray(Charsets.UTF_8).size

private const val FIELD_BUDGET_MSG = "expected field values exceed the depth, node or UTF-8 byte budget"
private const val FIELD_PATH_MSG = "field paths require 1-16 keys of 1-256 UTF-8 bytes"

private class FieldBudget {
    var nodes = 0
    var bytes = 0

    fun check(value: JsonElement, depth: Int) {
        nodes++
        if (depth > 32 || nodes > 8192 || bytes > 65536) throw badRequest(FIELD_BUDGET_MSG)
        when (value) {
            is JsonNull -> {}
            is JsonArray -> value.forEach { check(it, depth + 1) }
            is JsonObject ->
                for ((k, child) in value) {
                    if (!validUtf16(k)) throw badRequest(FIELD_BUDGET_MSG)
                    bytes += utf8Len(k)
                    check(child, depth + 1)
                }
            is JsonPrimitive ->
                when {
                    value.isString -> {
                        if (!validUtf16(value.content)) throw badRequest(FIELD_BUDGET_MSG)
                        bytes += utf8Len(value.content)
                    }
                    value.content == "true" || value.content == "false" -> {}
                    else -> {
                        val number = value.content.toDoubleOrNull()
                        if (number == null || number.isInfinite() || number.isNaN()) throw badRequest(FIELD_BUDGET_MSG)
                    }
                }
        }
        if (bytes > 65536) throw badRequest(FIELD_BUDGET_MSG)
    }
}

/** expectFields: validates the assertion budget and declared paths, then asserts every path equals its value. */
internal fun stepExpectFields(s: Store, step: Step.ExpectFields): StepOut {
    val table = requireTable(s, step.table)
    val row = s.docs[RowKey(step.table, step.id)]
    if (row == null || row.deletedAt != null) throw notFound("document '${step.id}' not found")
    if (step.expected.isEmpty() || step.expected.size > 64) throw badRequest("expected must contain 1-64 field assertions")
    val paths = HashSet<List<String>>()
    val budget = FieldBudget()
    for (item in step.expected) {
        if (item.path.isEmpty() || item.path.size > 16) throw badRequest(FIELD_PATH_MSG)
        for (key in item.path) {
            if (key.isEmpty() || utf8Len(key) > 256 || !validUtf16(key)) throw badRequest(FIELD_PATH_MSG)
            budget.bytes += utf8Len(key)
        }
        if (!paths.add(item.path)) throw badRequest("expected contains a duplicate field path")
        budget.check(item.value, 0)
        if (!fieldPathDeclared(table.fields, item.path)) throw schemaViolation("expected field path is not declared")
    }
    val mismatch = { RtDbException(ErrorCode.PRECONDITION_FAILED, "expected fields did not match") }
    for (item in step.expected) {
        var value: JsonElement = row.doc
        for (key in item.path) {
            val obj = value as? JsonObject ?: throw mismatch()
            value = obj[key] ?: throw mismatch()
        }
        if (!fieldJsonEqual(value, item.value)) throw mismatch()
    }
    return StepOut(StepResult())
}
