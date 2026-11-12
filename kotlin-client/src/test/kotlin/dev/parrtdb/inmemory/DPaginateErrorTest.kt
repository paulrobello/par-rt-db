package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.Paginate
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.Step
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.WireJson
import dev.parrtdb.wire.jsonNum
import dev.parrtdb.wire.jsonStr
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Pagination boundary and cursor error-path tests over [InMemoryClient] (ported paginated-query error cases). */
class DPaginateErrorTest {
    private fun client(n: Int = 5): InMemoryClient {
        val c = InMemoryClient(InMemoryOptions(now = { 1_700_000_000_000L }, random = { 0.0 }))
        c.pushSchema(
            WireJson.parse(
                """{"tables":{"items":{"fields":{"name":{"type":"string"},"n":{"type":"number"}},
                   "indexes":[{"name":"by_name","fields":["name"]}]}}}""",
            ),
        )
        c.mutate(
            Transaction(
                (1..n).map { i ->
                    Step.Insert("items", WireJson.parse("""{"name":"i$i","n":$i}"""))
                },
            ),
        )
        return c
    }

    private fun code(block: () -> Unit): ErrorCode = assertFailsWith<RtDbException> { block() }.code

    @Test
    fun numItemsZeroReturnsEmptyPageWithoutCursor() {
        val c = client()
        val r = c.query(Query(table = "items", paginate = Paginate(0))) as kotlinx.serialization.json.JsonObject
        assertEquals(0, (r["docs"] as kotlinx.serialization.json.JsonArray).size)
        assertNull(r["nextCursor"])
    }

    @Test
    fun hugeNumItemsClampsToMaxTakeAndReturnsEverythingWithoutCursor() {
        val c = client()
        val r = c.query(Query(table = "items", paginate = Paginate(1_000_000))) as kotlinx.serialization.json.JsonObject
        assertEquals(5, (r["docs"] as kotlinx.serialization.json.JsonArray).size)
        assertNull(r["nextCursor"])
    }

    @Test
    fun negativeNumItemsBehavesLikeZero() {
        val c = client()
        val r = c.query(Query(table = "items", paginate = Paginate(-3))) as kotlinx.serialization.json.JsonObject
        assertEquals(0, (r["docs"] as kotlinx.serialization.json.JsonArray).size)
        assertNull(r["nextCursor"])
    }

    @Test
    fun cursorColumnCountMismatchIsRejected() {
        val c = client()
        // A 3-value cursor against this 2-column sort (created_at, id): decode checks the count first.
        val threeValues = encodeCursorForEngine(listOf(jsonNum("1"), jsonNum("2"), jsonStr("x")))
        val e = assertFailsWith<RtDbException> { c.query(Query(table = "items", paginate = Paginate(2, threeValues))) }
        assertEquals(ErrorCode.BAD_REQUEST, e.code)
        assertTrue(e.message.contains("cursor has 3 value(s) but this query sorts over 2 column(s)"))
    }

    @Test
    fun cursorValueOfWrongKindIsRejected() {
        val c = client()
        // created_at column requires a number; a string value is rejected before paging.
        val bad = encodeCursorForEngine(listOf(jsonStr("not-a-number"), jsonStr("x")))
        val e = assertFailsWith<RtDbException> { c.query(Query(table = "items", paginate = Paginate(2, bad))) }
        assertEquals(ErrorCode.BAD_REQUEST, e.code)
        assertTrue(e.message.contains("cursor value for created_at must be a number"))
    }

    @Test
    fun cursorForUnknownIndexFieldStillPagesOnTiebreakersOnly() {
        val c = client()
        // No index in the query: cols are created_at + id (2 columns), a well-formed cursor pages fine.
        val page1 = c.query(Query(table = "items", paginate = Paginate(2))) as kotlinx.serialization.json.JsonObject
        assertEquals(2, (page1["docs"] as kotlinx.serialization.json.JsonArray).size)
        val cursor = (page1["nextCursor"] as JsonPrimitive).content
        val page2 = c.query(Query(table = "items", paginate = Paginate(2, cursor))) as kotlinx.serialization.json.JsonObject
        assertEquals(2, (page2["docs"] as kotlinx.serialization.json.JsonArray).size)
        val page3 = c.query(Query(table = "items", paginate = Paginate(2, (page2["nextCursor"] as JsonPrimitive).content))) as kotlinx.serialization.json.JsonObject
        assertEquals(1, (page3["docs"] as kotlinx.serialization.json.JsonArray).size)
        assertNull(page3["nextCursor"])
    }
}
