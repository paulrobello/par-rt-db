package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.inmemory.BSupport.ids
import dev.parrtdb.inmemory.BSupport.json
import dev.parrtdb.inmemory.BSupport.put
import dev.parrtdb.wire.Paginate
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.SearchMode
import dev.parrtdb.wire.SearchQuery
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class BSearchTest {
    private fun seeded(): Store {
        val s = BSupport.store()
        put(s, "items", "s1", 100, """{"name":"alpha document here","status":"t","score":1,"big":"1"}""")
        put(s, "items", "s2", 200, """{"name":"beta paper alpha","status":"t","score":1,"big":"1"}""")
        put(s, "items", "s3", 300, """{"name":"gamma unrelated","status":"t","score":1,"big":"1"}""")
        return s
    }

    private fun search(s: Store, text: String, mode: SearchMode? = null, snippet: Boolean? = null, pag: Paginate? = null, take: Int? = null) =
        evalQuery(s, Query("items", search = SearchQuery("by_content", text, mode = mode, snippet = snippet), paginate = pag, take = take))

    @Test
    fun tsqueryTermsPhraseOrExclude() {
        val s = seeded()
        assertEquals(setOf("s1", "s2"), ids(search(s, "alpha")).toSet())
        assertEquals(listOf("s2"), ids(search(s, "alpha beta")))
        assertEquals(listOf("s2"), ids(search(s, "\"beta paper\"")))
        assertEquals(emptyList(), ids(search(s, "\"paper beta\"")))
        assertEquals(setOf("s1", "s3"), ids(search(s, "document or gamma")).toSet())
        assertEquals(listOf("s1"), ids(search(s, "alpha -beta")))
    }

    @Test
    fun snippetMarksMatches() {
        val s = seeded()
        val r = search(s, "paper", snippet = true) as JsonArray
        val doc = r[0] as JsonObject
        assertEquals("beta <mark>paper</mark> alpha", (doc["_searchSnippet"] as JsonPrimitive).content)
    }

    @Test
    fun trgmRanksBySimilarity() {
        val s = seeded()
        // "alpha" is 5/21 of "alpha document here" and 5/16 of "beta paper alpha": shorter text ranks first.
        assertEquals(listOf("s2", "s1"), ids(search(s, "ALPHA", mode = SearchMode.Trgm)))
        assertEquals(listOf("s2"), ids(search(s, "ALPHA", mode = SearchMode.Trgm, take = 1)))
    }

    @Test
    fun searchValidation() {
        val s = seeded()
        fun code(f: () -> Unit) = assertFailsWith<RtDbException> { f() }.code
        assertEquals(ErrorCode.BAD_REQUEST, code { search(s, "   ") })
        assertEquals(ErrorCode.BAD_REQUEST, code { search(s, "a".repeat(4097)) })
        assertEquals(ErrorCode.BAD_REQUEST, code { evalQuery(s, Query("items", search = SearchQuery("nope", "x"))) })
        assertEquals(ErrorCode.BAD_REQUEST, code { search(s, "x", mode = SearchMode.Trgm, snippet = true) })
    }

    @Test
    fun rankedPaginationRoundTrip() {
        val s = seeded()
        val seen = ArrayList<String>()
        var cursor: String? = null
        var guard = 0
        while (guard++ < 5) {
            val r = search(s, "alpha", pag = Paginate(1, cursor)) as JsonObject
            seen += ids(r["docs"]!!)
            cursor = (r["nextCursor"] as? JsonPrimitive)?.content
            if (cursor == null) break
        }
        // equal tsquery scores: _creationTime desc breaks the tie.
        assertEquals(listOf("s2", "s1"), seen)
        assertTrue(cursor == null)
    }

    @Test
    fun vectorAndHybridApproximations() {
        val s = seeded()
        val v = evalQuery(s, Query("items", vectorSearch = dev.parrtdb.wire.VectorSearchQuery("v", listOf(1f), 2))) as JsonArray
        assertEquals(2, v.size)
        val h = evalQuery(s, Query("items", hybridSearch = dev.parrtdb.wire.HybridSearchQuery("q", listOf(1f), 10))) as JsonArray
        assertEquals(3, h.size)
        val hp = evalQuery(s, Query("items", hybridSearch = dev.parrtdb.wire.HybridSearchQuery("q", listOf(1f), 10), paginate = Paginate(5))) as JsonObject
        assertEquals(0, (hp["docs"] as JsonArray).size)
        assertEquals(json("[]"), hp["docs"])
    }
}
