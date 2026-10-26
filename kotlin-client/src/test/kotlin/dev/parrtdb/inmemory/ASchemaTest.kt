package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.WireJson
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ASchemaTest {
    private fun parse(json: String): SchemaDef = parseSchema(WireJson.parse(json))

    private fun code(block: () -> Unit): ErrorCode = assertFailsWith<RtDbException> { block() }.code

    private val rich =
        """
        {"tables":{"posts":{
          "fields":{
            "title":{"type":"string"},
            "n":{"type":"int64"},
            "owner":{"type":"id","table":"users","onDelete":"cascade"},
            "tags":{"type":"array","element":{"type":"string"}},
            "kind":{"type":"union","variants":[{"type":"literal","value":"a"},{"type":"literal","value":"b"}]},
            "meta":{"type":"optional","inner":{"type":"object","fields":{"x":{"type":"number"}}}},
            "emb":{"type":"vector","dimensions":3},
            "score":{"type":"number"}
          },
          "indexes":[{"name":"by_title","fields":["title"],"search":true},
                     {"name":"by_owner","fields":["owner"]},
                     {"name":"v","fields":["emb"],"vector":{"dimensions":3,"metric":"l2","filterFields":["kind"]}}],
          "softDelete":true,
          "description":"d",
          "fieldDescriptions":{"title":"t"},
          "constraints":{"score":{"min":0,"max":10.5},"title":{"minLength":1,"maxLength":9,"pattern":"^a"}},
          "defaults":{"score":1},
          "computed":{"title":{"op":"lower","value":{"op":"field","field":"kind"}}}
        },"users":{"fields":{"name":{"type":"string"}}}}}
        """.trimIndent()

    @Test
    fun acceptsRichSchemaAndRoundTrips() {
        val s = parse(rich)
        val wire1 = schemaToWire(s)
        val s2 = parseSchema(wire1)
        assertEquals(wire1, schemaToWire(s2))
        val posts = s2.tables.getValue("posts")
        assertTrue(posts.softDelete)
        assertEquals("l2", posts.indexes[2].vector?.metric)
        assertEquals(OnDeleteAction.Cascade, posts.fields.getValue("owner").onDelete)
        assertTrue(posts.fields.getValue("n").equalTo(s.tables.getValue("posts").fields.getValue("n")))
    }

    @Test
    fun rejectsUnknownKeys() {
        assertEquals(ErrorCode.BAD_REQUEST, code { parse("""{"tables":{"t":{"fields":{},"bogus":1}}}""") })
        assertEquals(ErrorCode.BAD_REQUEST, code { parse("""{"tables":{"t":{"fields":{"a":{"type":"string","x":1}}}}}""") })
        assertEquals(ErrorCode.BAD_REQUEST, code { parse("""{"tables":{"t":{"fields":{"a":{"type":"nope"}}}}}""") })
        assertEquals(ErrorCode.BAD_REQUEST, code { parse("""{"tables":{"t":{"fields":{},"indexes":[{"name":"i","fields":[],"zz":1}]}}}""") })
        assertEquals(ErrorCode.BAD_REQUEST, code { parse("""{"nope":{}}""") })
        assertEquals(ErrorCode.BAD_REQUEST, code { parse("[]") })
        assertEquals(ErrorCode.BAD_REQUEST, code { parse("""{"tables":{"t":{"fields":{"a":{"type":"id","table":"x","onDelete":"zap"}}}}}""") })
        assertEquals(ErrorCode.BAD_REQUEST, code { parse("""{"tables":{"t":{"fields":{"a":{"type":"vector","dimensions":3.5}}}}}""") })
    }

    @Test
    fun literalEqualityIgnoresSpelling() {
        val a = parse("""{"tables":{"t":{"fields":{"a":{"type":"literal","value":1}}}}}""").tables.getValue("t").fields.getValue("a")
        val b = parse("""{"tables":{"t":{"fields":{"a":{"type":"literal","value":1}}}}}""").tables.getValue("t").fields.getValue("a")
        val c = parse("""{"tables":{"t":{"fields":{"a":{"type":"literal","value":1.0}}}}}""").tables.getValue("t").fields.getValue("a")
        assertTrue(a.equalTo(b))
        assertFalse(a.equalTo(c)) // strict serde equality: 1 != 1.0
    }

    @Test
    fun grandfatherTrgmFlipsExistingSearchIndexOnly() {
        val old = parse("""{"tables":{"t":{"fields":{"a":{"type":"string"}},"indexes":[{"name":"s","fields":["a"],"search":true}]}}}""")
        val next =
            parse(
                """{"tables":{"t":{"fields":{"a":{"type":"string"}},"indexes":[
                {"name":"s","fields":["a"],"search":true},{"name":"s2","fields":["a"],"search":true}]}}}""",
            )
        val adj = grandfatherTrgm(old, next)
        val idx = adj.tables.getValue("t").indexes
        assertTrue(idx[0].trgm)
        assertFalse(idx[1].trgm)
        assertEquals(next, grandfatherTrgm(null, next))
    }

    @Test
    fun detectDestructiveChanges() {
        val old = parse("""{"tables":{"t":{"fields":{"a":{"type":"string"},"k":{"type":"literal","value":"x"}},"indexes":[{"name":"i","fields":["a"]}]}}}""")
        fun check(next: String): String = assertFailsWith<RtDbException> { detectDestructiveChanges(old, parse(next)) }.message
        assertTrue(check("""{"tables":{}}""").contains("removed table 't'"))
        assertTrue(check("""{"tables":{"t":{"fields":{"k":{"type":"literal","value":"x"}}}}}""").contains("removed field 't.a'"))
        assertTrue(check("""{"tables":{"t":{"fields":{"a":{"type":"number"},"k":{"type":"literal","value":"x"}},"indexes":[{"name":"i","fields":["a"]}]}}}""").contains("changed type"))
        assertTrue(check("""{"tables":{"t":{"fields":{"a":{"type":"string"},"k":{"type":"literal","value":"x"}}}}}""").contains("removed index 'i'"))
        // literal widening is accepted
        detectDestructiveChanges(
            old,
            parse(
                """{"tables":{"t":{"fields":{"a":{"type":"string"},"k":{"type":"union","variants":[{"type":"literal","value":"x"},{"type":"literal","value":"y"}]}},
                "indexes":[{"name":"i","fields":["a"]}]}}}""",
            ),
        )
    }

    @Test
    fun validateSchemaRules() {
        assertEquals(ErrorCode.SCHEMA_VIOLATION, code { validateSchema(parse("""{"tables":{"t":{"fields":{"a":{"type":"any"}},"indexes":[{"name":"i","fields":["a"]}]}}}""")) })
        assertEquals(ErrorCode.SCHEMA_VIOLATION, code { validateSchema(parse("""{"tables":{"t":{"fields":{"a":{"type":"number"}},"ttl":{"field":"a"}}}}""")) })
        assertEquals(ErrorCode.SCHEMA_VIOLATION, code { validateSchema(parse("""{"tables":{"t":{"fields":{"a":{"type":"number"}},"constraints":{"a":{"min":5,"max":1}}}}}""")) })
        assertEquals(ErrorCode.BAD_REQUEST, code { validateSchema(parse("""{"tables":{"t":{"fields":{"a":{"type":"number"}},"computed":{"a":{"op":"field","field":"zz"}}}}}""")) })
        assertEquals(ErrorCode.SCHEMA_VIOLATION, code { validateOnDelete(parse("""{"tables":{"t":{"fields":{"a":{"type":"id","table":"u","onDelete":"cascade"}}}}}""")) })
        validateSchema(parse(rich))
        validateOnDelete(parse(rich))
        assertTrue(hasOnDeleteChildren(parse(rich), "users"))
        assertFalse(hasOnDeleteChildren(parse(rich), "posts"))
    }

    @Test
    fun pushSchemaIsAdditive() {
        val s = Store()
        s.pushSchema(WireJson.parse("""{"tables":{"t":{"fields":{"a":{"type":"string"}}}}}"""))
        s.pushSchema(WireJson.parse("""{"tables":{"t":{"fields":{"a":{"type":"string"},"b":{"type":"optional","inner":{"type":"string"}}}}}}"""))
        assertEquals(setOf("a", "b"), s.tables.getValue("t").fields.keys)
        assertEquals(ErrorCode.BAD_REQUEST, code { s.pushSchema(WireJson.parse("""{"tables":{}}""")) })
    }
}
