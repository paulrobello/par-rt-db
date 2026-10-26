package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.inmemory.BSupport.ids
import dev.parrtdb.inmemory.BSupport.json
import dev.parrtdb.inmemory.BSupport.put
import dev.parrtdb.wire.AggregateOp
import dev.parrtdb.wire.AggregateSpec
import dev.parrtdb.wire.GroupBy
import dev.parrtdb.wire.LookupSpec
import dev.parrtdb.wire.Order
import dev.parrtdb.wire.Paginate
import dev.parrtdb.wire.Query
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class BQueryTest {
    private fun seeded(): Store {
        val s = BSupport.store()
        put(s, "items", "a1", 10, """{"name":"alpha","status":"todo","score":3,"big":"9007199254740993"}""")
        put(s, "items", "a2", 20, """{"name":"beta","status":"todo","score":1,"big":"9007199254740994"}""")
        put(s, "items", "a3", 30, """{"name":"gamma","status":"done","score":2,"big":"5"}""")
        put(s, "items", "a4", 40, """{"name":"delta","status":"todo","score":2,"big":"7"}""")
        put(s, "items", "a5", 50, """{"name":"gone","status":"todo","score":9,"big":"1"}""", deleted = true)
        return s
    }

    private fun code(block: () -> Unit): ErrorCode = assertFailsWith<RtDbException> { block() }.code

    @Test
    fun indexEqOrderTake() {
        val s = seeded()
        val q = Query("items", index = "by_status_score", eq = listOf(json("\"todo\"")))
        assertEquals(listOf("a2", "a4", "a1"), ids(evalQuery(s, q)))
        assertEquals(listOf("a1", "a4", "a2"), ids(evalQuery(s, q.copy(order = Order.Desc))))
        assertEquals(listOf("a2", "a4"), ids(evalQuery(s, q.copy(take = 2))))
    }

    @Test
    fun rangeBounds() {
        val s = seeded()
        val q = Query("items", index = "by_status_score", eq = listOf(json("\"todo\"")), gt = json("1"), lte = json("3"))
        assertEquals(listOf("a4", "a1"), ids(evalQuery(s, q)))
        assertEquals(ErrorCode.BAD_REQUEST, code { evalQuery(s, Query("items", gt = json("1"))) })
    }

    @Test
    fun getSkipsSoftDeleted() {
        val s = seeded()
        assertEquals(JsonNull, evalQuery(s, Query("items", get = "a5")))
        val doc = evalQuery(s, Query("items", get = "a1")) as JsonObject
        assertEquals("alpha", (doc["name"] as JsonPrimitive).content)
        assertEquals(ErrorCode.NOT_FOUND, code { evalQuery(s, Query("nope")) })
    }

    @Test
    fun uniqueFirstCountDistinct() {
        val s = seeded()
        val base = Query("items", index = "by_status_score", eq = listOf(json("\"todo\"")))
        assertEquals(ErrorCode.PRECONDITION_FAILED, code { evalQuery(s, base.copy(unique = true)) })
        val one = Query("items", index = "by_status_score", eq = listOf(json("\"done\"")), unique = true)
        assertEquals("a3", ((evalQuery(s, one) as JsonObject)["_id"] as JsonPrimitive).content)
        assertEquals("a2", ((evalQuery(s, base.copy(first = true)) as JsonObject)["_id"] as JsonPrimitive).content)
        assertEquals("3", (evalQuery(s, base.copy(count = true)) as JsonPrimitive).content)
        assertEquals(json("[1,2,3]"), evalQuery(s, base.copy(distinct = true)))
        assertEquals(ErrorCode.BAD_REQUEST, code { evalQuery(s, Query("items", first = true, count = true)) })
    }

    @Test
    fun aggregateScalarGroupedMulti() {
        val s = seeded()
        val base = Query("items", index = "by_status_score", eq = listOf(json("\"todo\"")))
        fun agg(spec: AggregateSpec) = evalQuery(s, base.copy(aggregate = spec))
        assertEquals(json("6"), agg(AggregateSpec(op = AggregateOp.Sum)))
        assertEquals(json("2"), agg(AggregateSpec(op = AggregateOp.Avg)))
        assertEquals(json("1"), agg(AggregateSpec(op = AggregateOp.Min)))
        assertEquals(json("3"), agg(AggregateSpec(op = AggregateOp.Max)))
        assertEquals(json("3"), agg(AggregateSpec(op = AggregateOp.Count)))
        assertEquals(
            json("""{"max":3,"sum":6}"""),
            agg(AggregateSpec(aggregates = mapOf("sum" to AggregateOp.Sum, "max" to AggregateOp.Max))),
        )
        val grouped = Query("items", index = "by_status_score", aggregate = AggregateSpec(op = AggregateOp.Count, groupBy = GroupBy.Flag(true)))
        assertEquals(
            json("""[{"key":"done","value":1},{"key":"todo","value":3}]"""),
            evalQuery(s, grouped),
        )
        val v2 = AggregateSpec(aggregates = mapOf("n" to AggregateOp.Count), groupBy = GroupBy.Fields(listOf("status")))
        assertEquals(
            json("""[{"keys":["done"],"values":{"n":1}},{"keys":["todo"],"values":{"n":3}}]"""),
            evalQuery(s, grouped.copy(aggregate = v2)),
        )
        assertEquals(
            ErrorCode.BAD_REQUEST,
            code { evalQuery(s, base.copy(aggregate = AggregateSpec(op = AggregateOp.Sum, aggregates = mapOf("x" to AggregateOp.Sum)))) },
        )
    }

    @Test
    fun aggregateInt64IsExact() {
        val s = seeded()
        val q = Query("items", index = "by_status_big", eq = listOf(json("\"todo\"")), aggregate = AggregateSpec(op = AggregateOp.Sum))
        // 9007199254740993 + 9007199254740994 + 7 = 18014398509481994 (not representable as a double sum)
        assertEquals(json("\"18014398509481994\""), evalQuery(s, q))
        val max = q.copy(aggregate = AggregateSpec(op = AggregateOp.Max))
        assertEquals(json("\"9007199254740994\""), evalQuery(s, max))
    }

    @Test
    fun paginateCursorRoundTrip() {
        val s = seeded()
        val base = Query("items", index = "by_status_score", eq = listOf(json("\"todo\"")))
        val seen = ArrayList<String>()
        var cursor: String? = null
        var pages = 0
        while (true) {
            val r = evalQuery(s, base.copy(paginate = Paginate(2, cursor))) as JsonObject
            seen += ids(r["docs"]!!)
            pages++
            val next = r["nextCursor"] as? JsonPrimitive ?: break
            cursor = next.content
            assertTrue(pages < 5)
        }
        assertEquals(listOf("a2", "a4", "a1"), seen)
        assertEquals(2, pages)
        // Desc paging
        val d = evalQuery(s, base.copy(order = Order.Desc, paginate = Paginate(2))) as JsonObject
        assertEquals(listOf("a1", "a4"), ids(d["docs"]!!))
        val d2 = evalQuery(s, base.copy(order = Order.Desc, paginate = Paginate(2, (d["nextCursor"] as JsonPrimitive).content))) as JsonObject
        assertEquals(listOf("a2"), ids(d2["docs"]!!))
        assertEquals(ErrorCode.BAD_REQUEST, code { evalQuery(s, base.copy(paginate = Paginate(2, "!!notbase64"))) })
    }

    @Test
    fun cursorFormatMatchesGo() {
        // Go: base64(json.Marshal([2,"20","a2"])) with number literals preserved.
        val enc = encodeCursorForEngine(listOf(json("2"), json("20"), json("\"a<2\"")))
        val raw = String(java.util.Base64.getDecoder().decode(enc), Charsets.UTF_8)
        assertEquals("[2,20,\"a" + "\\" + "u003c2\"]", raw)
        assertEquals(3, decodeCursorForEngine(enc).size)
    }

    @Test
    fun projectionAndLookups() {
        val s = seeded()
        put(s, "owners", "o1", 1, """{"label":"ann"}""")
        put(s, "owners", "o2", 2, """{"label":"bob"}""")
        put(s, "items", "a6", 60, """{"name":"zed","status":"x","score":1,"big":"1","owner":"o1","tags":["o2","missing","o1"]}""")
        val q = Query("items", get = "a6", fields = listOf("name", "owner_doc"), lookups = listOf(LookupSpec("owner", "owners", "owner_doc")))
        val r = evalQuery(s, q) as JsonObject
        assertEquals(setOf("_id", "_creationTime", "_version", "name", "owner_doc"), r.keys)
        assertEquals("ann", (((r["owner_doc"]) as JsonObject)["label"] as JsonPrimitive).content)
        val arr = evalQuery(s, Query("items", get = "a6", lookups = listOf(LookupSpec("tags", "owners", "t", cap = 1)))) as JsonObject
        assertEquals(1, (arr["t"] as JsonArray).size)
        assertEquals(ErrorCode.BAD_REQUEST, code { evalQuery(s, Query("items", get = "a1", fields = listOf("bogus"))) })
        assertEquals(
            ErrorCode.BAD_REQUEST,
            code { evalQuery(s, Query("items", get = "a1", lookups = listOf(LookupSpec("name", "owners", "x")))) },
        )
    }

    @Test
    fun compareIndexValuesNullsLast() {
        assertEquals(1, compareIndexValues(JsonNull, json("1"), PgType.Number))
        assertEquals(-1, compareIndexValues(json("1"), JsonNull, PgType.Number))
        assertEquals(-1, compareIndexValues(json("\"9\""), json("\"10\""), PgType.Int64).let { if (it < 0) 1 else -1 }.let { -it })
        assertEquals(1, compareIndexValues(json("true"), json("false"), PgType.Boolean))
        assertEquals(0, compareIndexValues(json("1"), json("\"x\""), PgType.Number))
    }

    @Test
    fun takeCapAndCombos() {
        val s = seeded()
        assertEquals(ErrorCode.BAD_REQUEST, code { evalQuery(s, Query("items", take = 5000)) })
    }
}
