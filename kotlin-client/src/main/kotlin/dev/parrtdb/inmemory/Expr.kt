package dev.parrtdb.inmemory

import dev.parrtdb.errors.RtDbException
import dev.parrtdb.errors.badRequest
import dev.parrtdb.wire.Cast
import dev.parrtdb.wire.ValueExpr
import dev.parrtdb.wire.isNumberValue
import dev.parrtdb.wire.jsonNum
import dev.parrtdb.wire.jsonStr
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.math.BigDecimal

/** String payload of a string primitive, else null. */
internal fun strOf(v: JsonElement?): String? = (v as? JsonPrimitive)?.takeIf { it.isString }?.content

/** Boolean payload of a boolean primitive, else null. */
internal fun boolOf(v: JsonElement?): Boolean? =
    (v as? JsonPrimitive)?.takeIf { !it.isString && it !is JsonNull }?.content?.let {
        when (it) {
            "true" -> true
            "false" -> false
            else -> null
        }
    }

/** The primitive when it is a number literal, else null. */
internal fun numOf(v: JsonElement?): JsonPrimitive? = (v as? JsonPrimitive)?.takeIf { it.isNumberValue }

/** Go string ordering: UTF-8 byte order, which equals code point order. */
internal fun compareUtf8(a: String, b: String): Int {
    var i = 0
    var j = 0
    while (i < a.length && j < b.length) {
        val ca = a.codePointAt(i)
        val cb = b.codePointAt(j)
        if (ca != cb) return if (ca < cb) -1 else 1
        i += Character.charCount(ca)
        j += Character.charCount(cb)
    }
    return (a.length - i).compareTo(b.length - j)
}

/** JS `===` on JSON values: numbers compare by value across spellings, everything else by kind and payload. */
fun jsonEq(lhs: JsonElement?, rhs: JsonElement?): Boolean {
    if (lhs == null) return rhs == null
    if (rhs == null) return false
    return when {
        lhs is JsonNull -> rhs is JsonNull
        lhs is JsonArray -> {
            if (rhs !is JsonArray || lhs.size != rhs.size) return false
            lhs.indices.all { jsonEq(lhs[it], rhs[it]) }
        }
        lhs is JsonObject -> {
            if (rhs !is JsonObject || lhs.size != rhs.size) return false
            lhs.all { (k, lv) -> rhs[k]?.let { jsonEq(lv, it) } ?: false }
        }
        lhs is JsonPrimitive && lhs.isString -> rhs is JsonPrimitive && rhs.isString && lhs.content == rhs.content
        boolOf(lhs) != null -> boolOf(rhs) == boolOf(lhs)
        lhs is JsonPrimitive -> {
            val r = numOf(rhs) ?: return false
            lhs.content == r.content || jsonNumberF64(lhs) == jsonNumberF64(r)
        }
        else -> false
    }
}

/**
 * serde_json Value equality: numbers are equal only within their own backing class
 * (integer-spelled with integer value, float-spelled with float value); 1 and 1.0 are UNEQUAL.
 */
fun strictValueEq(lhs: JsonElement?, rhs: JsonElement?): Boolean {
    val ln = numOf(lhs)
    val rn = numOf(rhs)
    if (ln != null && rn != null) {
        val li = parseI64(ln.content)
        val ri = parseI64(rn.content)
        if ((li == null) != (ri == null)) return false
        if (li != null) return li == ri
        return jsonNumberF64(ln) == jsonNumberF64(rn)
    }
    return jsonEq(lhs, rhs)
}

/** True when [v] is a JSON number. */
fun isJsonNumber(v: JsonElement?): Boolean = v != null && v.isNumberValue

/** Exact `i64::from_str` mirror: optional sign then ASCII digits, within the Long range. */
fun parseI64(text: String): Long? {
    var neg = false
    var start = 0
    if (text.startsWith("-")) {
        neg = true
        start = 1
    } else if (text.startsWith("+")) {
        start = 1
    }
    if (start >= text.length) return null
    val limit = if (neg) 9223372036854775808UL else 9223372036854775807UL
    var acc = 0UL
    for (i in start until text.length) {
        val c = text[i]
        if (c < '0' || c > '9') return null
        val d = (c - '0').toULong()
        if (acc > (limit - d) / 10UL) return null
        acc = acc * 10UL + d
    }
    return if (neg) (0UL - acc).toLong() else acc.toLong()
}

/** Go `strconv.FormatFloat(f, 'g', -1, 64)`: shortest digits, exponent form when exp < -4 or >= 21 digits rule (eprec 6). */
private fun goFormatG(f: Double): String {
    if (f.isNaN()) return "NaN"
    if (f.isInfinite()) return if (f > 0) "+Inf" else "-Inf"
    if (f == 0.0) return if (1.0 / f < 0) "-0" else "0"
    val neg = f < 0
    val bd = BigDecimal(java.lang.Double.toString(Math.abs(f))).stripTrailingZeros()
    val digits = bd.unscaledValue().toString()
    val nd = digits.length
    val dp = nd - bd.scale() // value = 0.digits * 10^dp
    val exp = dp - 1
    val sb = StringBuilder()
    if (neg) sb.append('-')
    if (exp < -4 || exp >= 6) {
        sb.append(digits[0])
        if (nd > 1) sb.append('.').append(digits, 1, nd)
        sb.append('e').append(if (exp < 0) '-' else '+')
        val ae = Math.abs(exp)
        if (ae < 10) sb.append('0')
        sb.append(ae)
    } else if (dp <= 0) {
        sb.append("0.")
        repeat(-dp) { sb.append('0') }
        sb.append(digits)
    } else if (dp >= nd) {
        sb.append(digits)
        repeat(dp - nd) { sb.append('0') }
    } else {
        sb.append(digits, 0, dp).append('.').append(digits, dp, nd)
    }
    return sb.toString()
}

/** Renders a double the way the Go engine does: integral values below 1e15 print without a fraction. */
fun jsNumberString(f: Double): String {
    if (f == Math.floor(f) && !f.isInfinite() && Math.abs(f) < 1e15) return f.toLong().toString()
    return goFormatG(f)
}

private val FLOAT_RE = Regex("""[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?""")

/** Strict decimal float parse (Go ParseFloat subset): null when the text is not a plain decimal float. */
internal fun strictParseFloat(s: String): Double? = if (FLOAT_RE.matches(s)) s.toDoubleOrNull() else null

/** A number literal's value as a double (any spelling); unparseable text yields 0. */
fun jsonNumberF64(n: JsonElement): Double = (n as? JsonPrimitive)?.content?.let { strictParseFloat(it) } ?: 0.0

/** A number literal's exact integer value; a float-spelled literal ("3.0") fails BAD_REQUEST. */
fun jsonNumberI64(n: JsonElement): Long {
    val s = (n as? JsonPrimitive)?.content ?: throw badRequest("expected an integer")
    val digits = s.removePrefix("-").removePrefix("+")
    if (digits.isEmpty() || !digits.all { it in '0'..'9' }) throw badRequest("expected an integer")
    return parseI64(s) ?: throw badRequest("expected an integer")
}

/** Renders a finite double as a JSON number literal (integral values lose the ".0"). */
fun jsonNumberFromF64(f: Double): JsonElement = jsonNum(jsNumberString(f))

/** Decimal rendering of a Long. */
fun formatI64(v: Long): String = v.toString()

internal fun jsonNumberText(n: JsonElement): String {
    val s = (n as JsonPrimitive).content
    return strictParseFloat(s)?.let { jsNumberString(it) } ?: s
}

private fun appendJsonString(sb: StringBuilder, s: String, escapeHtml: Boolean) {
    sb.append('"')
    for (c in s) {
        when {
            c == '"' -> sb.append("\\\"")
            c == '\\' -> sb.append("\\\\")
            c == '\n' -> sb.append("\\n")
            c == '\r' -> sb.append("\\r")
            c == '\t' -> sb.append("\\t")
            c == '\b' -> sb.append("\\b")
            c == '\u000c' -> sb.append("\\f")
            c < ' ' -> sb.append(String.format("\\u%04x", c.code))
            escapeHtml && (c == '<' || c == '>' || c == '&') -> sb.append(String.format("\\u%04x", c.code))
            c == ' ' || c == ' ' -> sb.append(String.format("\\u%04x", c.code))
            else -> sb.append(c)
        }
    }
    sb.append('"')
}

internal fun writeCompact(sb: StringBuilder, v: JsonElement?, escapeHtml: Boolean) {
    when {
        v == null || v is JsonNull -> sb.append("null")
        v is JsonArray -> {
            sb.append('[')
            v.forEachIndexed { i, e ->
                if (i > 0) sb.append(',')
                writeCompact(sb, e, escapeHtml)
            }
            sb.append(']')
        }
        v is JsonObject -> {
            sb.append('{')
            var first = true
            for (k in v.keys.sortedWith { a, b -> compareUtf8(a, b) }) {
                if (!first) sb.append(',')
                first = false
                appendJsonString(sb, k, escapeHtml)
                sb.append(':')
                writeCompact(sb, v[k], escapeHtml)
            }
            sb.append('}')
        }
        v is JsonPrimitive && v.isString -> appendJsonString(sb, v.content, escapeHtml)
        v is JsonPrimitive -> sb.append(v.content)
    }
}

/** Compact JSON with object keys sorted recursively, without HTML escaping (rust serde to_string shape). */
internal fun compactJson(v: JsonElement?): String = StringBuilder().also { writeCompact(it, v, false) }.toString()

/**
 * SQL `doc->>'field'` text extraction: null means SQL NULL (JSON null). Objects and arrays
 * render as compact JSON with keys sorted.
 */
fun valueText(v: JsonElement?): String? =
    when {
        v == null || v is JsonNull -> null
        strOf(v) != null -> strOf(v)
        numOf(v) != null -> jsonNumberText(v)
        boolOf(v) != null -> if (boolOf(v) == true) "true" else "false"
        else -> compactJson(v)
    }

private class NumRes(val value: Double?, val err: RtDbException?) {
    val nullProp: Boolean get() = value == null && err == null
}

/** `(doc->>'field')::float8` for arithmetic: null propagation, strict string parse, type errors otherwise. */
private fun valueNumeric(v: JsonElement?): NumRes {
    if (v == null || v is JsonNull) return NumRes(null, null)
    numOf(v)?.let {
        val f = jsonNumberF64(it)
        return if (f.isInfinite() || f.isNaN()) NumRes(null, null) else NumRes(f, null)
    }
    strOf(v)?.let { s ->
        val trimmed = s.trim(' ', '\t', '\n', '\r')
        val f = strictParseFloat(trimmed)
        return if (f == null || f.isInfinite() || f.isNaN()) {
            NumRes(null, badRequest("cannot cast '$s' to number"))
        } else {
            NumRes(f, null)
        }
    }
    return NumRes(null, badRequest("cannot cast to number"))
}

private fun finiteJsonNumber(f: Double): JsonElement {
    if (f.isInfinite() || f.isNaN()) throw badRequest("numeric result is not finite")
    return jsonNumberFromF64(f)
}

private fun castToInt64(v: JsonElement?): JsonElement {
    if (v == null || v is JsonNull) return JsonNull
    numOf(v)?.let {
        val i = parseI64(it.content) ?: throw badRequest("cannot cast ${jsonNumberText(it)} to int64")
        return jsonNum(i.toString())
    }
    strOf(v)?.let { s ->
        val i = parseI64(s.trim(' ', '\t', '\n', '\r')) ?: throw badRequest("cannot cast '$s' to int64")
        return jsonNum(i.toString())
    }
    throw badRequest("cannot cast to int64")
}

private val TRUE_WORDS = setOf("true", "t", "yes", "on", "1")
private val FALSE_WORDS = setOf("false", "f", "no", "off", "0")

private fun castToBoolean(v: JsonElement?): JsonElement {
    if (v == null || v is JsonNull) return JsonNull
    boolOf(v)?.let { return JsonPrimitive(it) }
    numOf(v)?.let {
        val f = jsonNumberF64(it)
        if (f == 1.0) return JsonPrimitive(true)
        if (f == 0.0) return JsonPrimitive(false)
        throw badRequest("cannot cast ${jsonNumberText(it)} to boolean")
    }
    strOf(v)?.let { s ->
        val l = s.lowercase()
        if (l in TRUE_WORDS) return JsonPrimitive(true)
        if (l in FALSE_WORDS) return JsonPrimitive(false)
        throw badRequest("cannot cast '$s' to boolean")
    }
    throw badRequest("cannot cast to boolean")
}

/**
 * Evaluates a ValueExpr against a doc (the per-write counterpart of the server's SQL compiler,
 * used by computed-field stamping). Field reads are text extraction, arithmetic is IEEE doubles
 * with SQL-NULL propagation before the div-zero check, and case predicates reuse [matchesFilter].
 */
fun evalValueExpr(ve: ValueExpr, doc: JsonObject, nowMs: Long, fields: Map<String, FieldType>): JsonElement {
    fun ev(e: ValueExpr): JsonElement = evalValueExpr(e, doc, nowMs, fields)

    fun arith(l: ValueExpr, r: ValueExpr, op: (Double, Double) -> Double, isDiv: Boolean = false): JsonElement {
        val lhs = valueNumeric(ev(l))
        val rhs = valueNumeric(ev(r))
        if (lhs.nullProp || rhs.nullProp) return JsonNull
        lhs.err?.let { throw it }
        rhs.err?.let { throw it }
        val a = lhs.value!!
        val b = rhs.value!!
        if (isDiv && b == 0.0) throw badRequest("division by zero")
        return finiteJsonNumber(op(a, b))
    }

    return when (ve) {
        is ValueExpr.Field -> doc[ve.field]?.let { valueText(it) }?.let { jsonStr(it) } ?: JsonNull
        is ValueExpr.Literal -> ve.value
        is ValueExpr.Concat -> {
            val sb = StringBuilder()
            for (p in ve.parts) valueText(ev(p))?.let { sb.append(it) }
            jsonStr(sb.toString())
        }
        is ValueExpr.Add -> arith(ve.left, ve.right, { a, b -> a + b })
        is ValueExpr.Sub -> arith(ve.left, ve.right, { a, b -> a - b })
        is ValueExpr.Mul -> arith(ve.left, ve.right, { a, b -> a * b })
        is ValueExpr.Div -> arith(ve.left, ve.right, { a, b -> a / b }, isDiv = true)
        is ValueExpr.Coalesce -> {
            for (p in ve.parts) {
                val pv = ev(p)
                if (pv !is JsonNull) return pv
            }
            JsonNull
        }
        is ValueExpr.Lower -> valueText(ev(ve.value))?.let { jsonStr(it.lowercase()) } ?: JsonNull
        is ValueExpr.Upper -> valueText(ev(ve.value))?.let { jsonStr(it.uppercase()) } ?: JsonNull
        is ValueExpr.Trim -> valueText(ev(ve.value))?.let { jsonStr(it.trim(' ')) } ?: JsonNull
        is ValueExpr.CastTo -> {
            val iv = ev(ve.value)
            when (ve.to) {
                Cast.ToString -> valueText(iv)?.let { jsonStr(it) } ?: JsonNull
                Cast.ToNumber -> {
                    val n = valueNumeric(iv)
                    n.err?.let { throw it }
                    n.value?.let { finiteJsonNumber(it) } ?: JsonNull
                }
                Cast.ToInt64 -> castToInt64(iv)
                Cast.ToBoolean -> castToBoolean(iv)
            }
        }
        is ValueExpr.Now -> jsonNum(nowMs.toString())
        is ValueExpr.Case -> {
            for (cw in ve.whens) {
                if (matchesFilter(cw.whenExpr, doc, fields)) return ev(cw.then)
            }
            ev(ve.otherwise)
        }
    }
}
