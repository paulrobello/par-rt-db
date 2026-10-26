package dev.parrtdb.net

import dev.parrtdb.admin.DbStats
import dev.parrtdb.admin.Directive
import dev.parrtdb.admin.MigrateResult
import dev.parrtdb.admin.OpEvent
import dev.parrtdb.wire.StrictObj
import dev.parrtdb.wire.WireException
import dev.parrtdb.wire.WireJson
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

/** Corpus-pinned admin DTOs: strict decode, then re-encode value-identically. */
class CorpusDtoTest {
    private val corpus: JsonObject by lazy {
        var dir: File? = File("").absoluteFile
        while (dir != null && !File(dir, "wire-corpus/wire-corpus.json").isFile) dir = dir.parentFile
        val f = File(dir ?: error("wire-corpus not found above ${File("").absolutePath}"), "wire-corpus/wire-corpus.json")
        WireJson.parse(f.readText()) as JsonObject
    }

    private fun section(name: String): List<JsonElement> = (corpus[name] as JsonArray).toList()

    /** Key order is not significant; compare as parsed value trees. */
    private fun assertSame(expected: JsonElement, actual: JsonElement) = assertEquals(expected, actual)

    @Test
    fun `migrate requests decode and re-encode`() {
        val cases = section("migrate_requests")
        assertTrue(cases.isNotEmpty())
        for (c in cases) {
            val o = c as JsonObject
            val ds = (o["directives"] as JsonArray).map { Directive.fromJson(it) }
            assertSame(o["directives"]!!, JsonArray(ds.map { it.toJson() }))
        }
    }

    @Test
    fun `migrate results decode and re-encode`() {
        val cases = section("migrate_results")
        assertTrue(cases.isNotEmpty())
        for (c in cases) assertSame(c, MigrateResult.fromJson(c).toJson())
    }

    @Test
    fun `admin op events decode and re-encode`() {
        val cases = section("admin_op_events")
        assertTrue(cases.isNotEmpty())
        for (c in cases) assertSame(c, OpEvent.fromJson(c).toJson())
    }

    @Test
    fun `db stats decode and re-encode`() {
        val cases = section("db_stats")
        assertTrue(cases.isNotEmpty())
        for (c in cases) {
            val clean = JsonObject((c as JsonObject).filterKeys { it != "\$comment" })
            assertSame(clean, DbStats.fromJson(clean).toJson())
        }
    }

    @Test
    fun `strict decode rejects unknown fields`() {
        assertFailsWith<WireException> { Directive.fromJson(WireJson.parse("""{"op":"dropTable","name":"x","extra":1}""")) }
        assertFailsWith<WireException> { Directive.fromJson(WireJson.parse("""{"op":"nope"}""")) }
        assertFailsWith<WireException> { MigrateResult.fromJson(WireJson.parse("""{"applied":true,"schema":{},"directives":[],"x":1}""")) }
        assertFailsWith<WireException> { OpEvent.fromJson(WireJson.parse("""{"db":"a","table":"t","docId":"d","kind":"insert","ts":1,"owner":null,"seq":1,"feedEpoch":"e","x":1}""")) }
        assertFailsWith<WireException> { DbStats.fromJson(WireJson.parse("""{"tables":[{"name":"a","rowCount":1,"sizeBytes":1,"x":2}]}""")) }
    }

    @Test
    fun `directive report omits empty lists`() {
        val r = dev.parrtdb.admin.DirectiveReport("dropField", 1, emptyList(), null).toJson()
        assertEquals(setOf("op", "affectedRows"), r.keys)
        val _unused: StrictObj? = null
    }
}
