package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.FilterExpr
import dev.parrtdb.wire.ValueExpr
import dev.parrtdb.wire.WireJson
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

class AValueTest {
    private fun j(s: String) = WireJson.parse(s)

    private fun ty(s: String): FieldType = parseFieldType(j(s))

    private fun obj(s: String) = j(s) as JsonObject

    @Test
    fun int64AndBytes() {
        val i = ty("""{"type":"int64"}""")
        assertTrue(validateValue(i, j("\"9223372036854775807\"")))
        assertTrue(validateValue(i, j("\"-9223372036854775808\"")))
        assertFalse(validateValue(i, j("\"9223372036854775808\"")))
        assertFalse(validateValue(i, j("5")))
        assertFalse(validateValue(i, j("\"1.5\"")))
        val b = ty("""{"type":"bytes"}""")
        assertTrue(validateValue(b, j("\"aGVsbG8=\"")))
        assertTrue(validateValue(b, j("\"\"")))
        assertFalse(validateValue(b, j("\"aGVsbG8\"")))
        assertFalse(validateValue(b, j("\"a===\"")))
    }

    @Test
    fun literalUnionOptionalObject() {
        val lit = ty("""{"type":"literal","value":1}""")
        assertTrue(validateValue(lit, j("1.0")))
        assertFalse(validateValue(lit, j("\"1\"")))
        val u = ty("""{"type":"union","variants":[{"type":"string"},{"type":"number"}]}""")
        assertTrue(validateValue(u, j("3")))
        assertFalse(validateValue(u, j("true")))
        val o = ty("""{"type":"object","fields":{"a":{"type":"string"},"b":{"type":"optional","inner":{"type":"number"}}}}""")
        assertTrue(validateValue(o, j("""{"a":"x"}""")))
        assertTrue(validateValue(o, j("""{"a":"x","b":null}""")))
        assertFalse(validateValue(o, j("""{"b":1}""")))
        assertFalse(validateValue(o, j("""{"a":"x","z":1}""")))
        val v = ty("""{"type":"vector","dimensions":2}""")
        assertTrue(validateValue(v, j("[1,2.5]")))
        assertFalse(validateValue(v, j("[1]")))
        assertFalse(validateValue(v, j("[1,\"a\"]")))
        assertTrue(validateValue(ty("""{"type":"id","table":"t"}"""), j("\"" + "a".repeat(32) + "\"")))
        assertFalse(validateValue(ty("""{"type":"id","table":"t"}"""), j("\"" + "A".repeat(32) + "\"")))
    }

    private val table =
        TableDef(
            fields =
                mapOf(
                    "n" to ty("""{"type":"number"}"""),
                    "s" to ty("""{"type":"string"}"""),
                    "big" to ty("""{"type":"int64"}"""),
                    "b" to ty("""{"type":"boolean"}"""),
                    "tags" to ty("""{"type":"array","element":{"type":"string"}}"""),
                    "o" to ty("""{"type":"optional","inner":{"type":"string"}}"""),
                ),
            indexes = listOf(IndexDef("by_n", listOf("n"))),
        )

    private fun m(filter: String, doc: String, now: Long? = null): Boolean {
        val f = FilterExpr.fromJson(j(filter))
        val d = obj(doc)
        return if (now == null) matchesFilter(f, d, table.fields) else matchesFilterAt(f, d, table.fields, now)
    }

    @Test
    fun filterMatching() {
        assertTrue(m("""{"op":"eq","field":"n","value":5}""", """{"n":5.0}"""))
        assertTrue(m("""{"op":"gt","field":"n","value":4.999999}""", """{"n":5}"""))
        assertTrue(m("""{"op":"lte","field":"n","value":5}""", """{"n":"5"}""")) // string doc value parses numerically
        assertFalse(m("""{"op":"eq","field":"n","value":5}""", """{"n":null}"""))
        assertFalse(m("""{"op":"neq","field":"n","value":5}""", """{}""")) // absent never matches
        assertTrue(m("""{"op":"gt","field":"big","value":"9007199254740993"}""", """{"big":"9007199254740994"}"""))
        assertFalse(m("""{"op":"gt","field":"big","value":"9007199254740994"}""", """{"big":"9007199254740993"}"""))
        assertTrue(m("""{"op":"lt","field":"big","value":"10"}""", """{"big":"9"}""")) // numeric not lexicographic
        assertTrue(m("""{"op":"gt","field":"s","value":"9"}""", """{"s":"a"}"""))
        assertTrue(m("""{"op":"gt","field":"b","value":false}""", """{"b":true}"""))
        assertTrue(m("""{"op":"in","field":"s","values":["x","y"]}""", """{"s":"y"}"""))
        assertTrue(m("""{"op":"contains","field":"tags","value":"a"}""", """{"tags":["b","a"]}"""))
        assertTrue(m("""{"op":"exists","field":"o"}""", """{"o":"v"}"""))
        assertFalse(m("""{"op":"exists","field":"o"}""", """{"o":null}"""))
        assertTrue(m("""{"op":"not","expr":{"op":"eq","field":"s","value":"a"}}""", """{"s":"b"}"""))
        assertFalse(m("""{"op":"olderThan","field":"n","ms":100}""", """{"n":1}""")) // fail-closed without a clock
        assertTrue(m("""{"op":"olderThan","field":"n","ms":100}""", """{"n":899}""", 1000))
        assertFalse(m("""{"op":"olderThan","field":"n","ms":100}""", """{"n":900}""", 1000))
    }

    @Test
    fun filterValidation() {
        fun v(f: String, byQuery: Boolean = false) {
            val e = FilterExpr.fromJson(j(f))
            if (byQuery) validateByQueryFilter(e, table) else validateFilterExpr(e, table)
        }
        fun bad(f: String) = assertEquals(ErrorCode.BAD_REQUEST, assertFailsWith<RtDbException> { v(f) }.code)
        v("""{"op":"eq","field":"n","value":1}""")
        bad("""{"op":"eq","field":"zz","value":1}""")
        bad("""{"op":"eq","field":"n","value":"x"}""") // indexed: number required
        bad("""{"op":"eq","field":"s","value":1}""")
        bad("""{"op":"in","field":"s","values":[]}""")
        bad("""{"op":"in","field":"s","values":["a",1]}""")
        bad("""{"op":"and","exprs":[]}""")
        bad("""{"op":"olderThan","field":"n","ms":1}""")
        v("""{"op":"olderThan","field":"n","ms":1}""", byQuery = true)
        var deep = """{"op":"eq","field":"n","value":1}"""
        repeat(32) { deep = """{"op":"not","expr":$deep}""" }
        bad(deep)
        assertEquals(ErrorCode.SCHEMA_VIOLATION, assertFailsWith<RtDbException> { indexColumnType(ty("""{"type":"any"}""")) }.code)
        assertEquals(IndexedType(PgType.Int64, true), indexColumnType(ty("""{"type":"optional","inner":{"type":"int64"}}""")))
        val seen = mutableListOf<String>()
        walkFilterExprFieldNames(FilterExpr.fromJson(j("""{"op":"or","exprs":[{"op":"eq","field":"a","value":1},{"op":"exists","field":"b"}]}"""))) { seen += it }
        assertEquals(listOf("a", "b"), seen)
    }

    private fun ev(expr: String, doc: String = "{}", now: Long = 42): String =
        WireJson.encode(evalValueExpr(ValueExpr.fromJson(j(expr)), obj(doc), now, table.fields))

    @Test
    fun valueExprEval() {
        assertEquals("\"ab1\"", ev("""{"op":"concat","parts":[{"op":"field","field":"s"},{"op":"literal","value":"b"},{"op":"literal","value":1.0}]}""", """{"s":"a"}"""))
        assertEquals("3", ev("""{"op":"add","left":{"op":"field","field":"n"},"right":{"op":"literal","value":"2"}}""", """{"n":1}"""))
        assertEquals("null", ev("""{"op":"add","left":{"op":"field","field":"n"},"right":{"op":"literal","value":1}}"""))
        assertEquals("0.5", ev("""{"op":"div","left":{"op":"literal","value":1},"right":{"op":"literal","value":2}}"""))
        val e = assertFailsWith<RtDbException> { ev("""{"op":"div","left":{"op":"literal","value":1},"right":{"op":"literal","value":0}}""") }
        assertEquals(ErrorCode.BAD_REQUEST, e.code)
        assertEquals("null", ev("""{"op":"div","left":{"op":"field","field":"n"},"right":{"op":"literal","value":0}}"""))
        assertEquals("\"X\"", ev("""{"op":"upper","value":{"op":"trim","value":{"op":"literal","value":"  x "}}}"""))
        assertEquals("true", ev("""{"op":"cast","to":"toBoolean","value":{"op":"literal","value":"YES"}}"""))
        assertEquals("12", ev("""{"op":"cast","to":"toInt64","value":{"op":"literal","value":" 12 "}}"""))
        assertFailsWith<RtDbException> { ev("""{"op":"cast","to":"toInt64","value":{"op":"literal","value":1.0}}""") }
        assertEquals("\"5\"", ev("""{"op":"cast","to":"toString","value":{"op":"literal","value":5.0}}"""))
        assertEquals("42", ev("""{"op":"now"}"""))
        assertEquals("\"b\"", ev("""{"op":"case","whens":[{"when":{"op":"eq","field":"s","value":"a"},"then":{"op":"literal","value":"A"}}],"otherwise":{"op":"literal","value":"b"}}""", """{"s":"z"}"""))
        assertEquals("\"A\"", ev("""{"op":"case","whens":[{"when":{"op":"eq","field":"s","value":"a"},"then":{"op":"literal","value":"A"}}],"otherwise":{"op":"literal","value":"b"}}""", """{"s":"a"}"""))
        assertEquals("\"{\\\"a\\\":1,\\\"b\\\":2}\"", ev("""{"op":"field","field":"s"}""", """{"s":{"b":2,"a":1}}"""))
    }

    @Test
    fun scalarHelpers() {
        assertEquals(Long.MIN_VALUE, parseI64("-9223372036854775808"))
        assertNull(parseI64("9223372036854775808"))
        assertNull(parseI64("-"))
        assertNull(parseI64("1.0"))
        assertEquals(5L, parseI64("+5"))
        assertTrue(jsonEq(j("5"), j("5.0")))
        assertFalse(strictValueEq(j("5"), j("5.0")))
        assertTrue(strictValueEq(j("5.5"), j("5.50")))
        assertEquals("5", jsNumberString(5.0))
        assertEquals("1.5", jsNumberString(1.5))
        assertEquals("1e+15", jsNumberString(1e15))
        assertEquals("1.2345675e+06", jsNumberString(1234567.5))
        assertEquals("1e-05", jsNumberString(0.00001))
        assertEquals("0.0001", jsNumberString(0.0001))
        assertEquals(3L, jsonNumberI64(j("3")))
        assertFailsWith<RtDbException> { jsonNumberI64(j("3.0")) }
        assertEquals("""{"a":[1,{"b":null,"c":"<"}],"z":true}""".replace("<", "\\u003c"), canonical(j("""{"z":true,"a":[1,{"c":"<","b":null}]}""")))
        assertEquals(null, valueText(JsonNull))
    }

    @Test
    fun docStamps() {
        val t =
            table.copy(
                constraints = mapOf("s" to FieldConstraints(minLength = 2, maxLength = 4, pattern = "^a"), "n" to FieldConstraints(min = j("0"), max = j("10"))),
                updatedAtField = "n",
                defaults = mapOf("b" to j("true")),
            )
        val d = applyDefaults(t, obj("""{"n":1,"s":"ab","big":"1"}"""))
        assertEquals(j("true"), d["b"])
        validateDoc(t, stripUnsetOptionals(t, obj("""{"n":1,"s":"ab","big":"1","b":true,"tags":[],"o":null}""")))
        fun viol(doc: String) = assertEquals(ErrorCode.SCHEMA_VIOLATION, assertFailsWith<RtDbException> { validateDoc(t, obj(doc)) }.code)
        viol("""{"n":11,"s":"ab","big":"1","b":true,"tags":[]}""")
        viol("""{"n":1,"s":"ba","big":"1","b":true,"tags":[]}""")
        viol("""{"n":1,"s":"abcde","big":"1","b":true,"tags":[]}""")
        viol("""{"n":1,"s":"ab","big":"1","b":true}""")
        viol("""{"_id":"x","n":1,"s":"ab","big":"1","b":true,"tags":[]}""")
        assertEquals(j("500"), stampUpdatedAt(t, obj("{}"), 500)["n"])
        val t2 = t.copy(ttl = TtlDef("big", 100))
        assertEquals(j("\"1100\""), stampTtlDefault(t2, obj("{}"), 1000)["big"].let { j("\"" + (it as kotlinx.serialization.json.JsonPrimitive).content + "\"") })
        val p = applyPatch(t, obj("""{"n":1,"s":"ab","big":"1","b":true,"tags":[]}"""), obj("""{"s":"ac","o":null}"""), 0)
        assertEquals(j("\"ac\""), p["s"])
    }
}
