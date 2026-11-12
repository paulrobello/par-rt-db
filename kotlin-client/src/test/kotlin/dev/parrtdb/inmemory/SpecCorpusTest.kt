package dev.parrtdb.inmemory

import dev.parrtdb.corpus.CorpusPaths
import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.WireJson
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlinx.serialization.json.boolean
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * ENH-052 family 1 — spec-table corpus runner: the FieldType -> Postgres
 * column-type mapping (wire-corpus/spec/index-column-types.json).
 *
 * Asserts the engine's `indexColumnType` against every fixture row — decoded
 * through the engine's own `parseFieldType`, the same wire spelling pushSchema
 * parses — and that the engine's FieldType variant set is covered by the
 * fixture. `columnType` carries the server's raw Postgres string; the engine's
 * four-value `PgType` maps back to it.
 */
class SpecCorpusTest {
    private fun rows(): List<JsonObject> {
        val doc = WireJson.parse(CorpusPaths.read("spec", "index-column-types.json")) as JsonObject
        val arr = doc["rows"] as? JsonArray ?: error("spec corpus has no rows array")
        return arr.map { it as JsonObject }
    }

    /** Canonical JSON (object keys sorted recursively) for row lookup. */
    private fun canonical(v: JsonElement): String =
        when (v) {
            is JsonArray -> v.joinToString(",", "[", "]") { canonical(it) }
            is JsonObject ->
                v.entries
                    .sortedBy { it.key }
                    .joinToString(",", "{", "}") { (k, value) -> "\"$k\":${canonical(value)}" }
            else -> v.toString()
        }

    /** The engine's four-value storage enum mapped back to the server's raw column strings. */
    private fun rawPg(pg: PgType): String =
        when (pg) {
            PgType.Text -> "text"
            PgType.Number -> "double precision"
            PgType.Boolean -> "boolean"
            PgType.Int64 -> "bigint"
        }

    private fun ftObj(vararg kvs: Pair<String, JsonElement>): JsonObject =
        JsonObject(kvs.associate { it })

    private fun str(v: String): JsonElement = JsonPrimitive(v)

    /** One representative per FieldType variant this engine's indexColumnType
     *  distinguishes, in the wire spelling (the corpus README's own-variant
     *  coverage rule: a variant without a fixture row fails). */
    private fun ownVariants(): List<JsonElement> =
        listOf(
            ftObj("type" to str("string")),
            ftObj("type" to str("number")),
            ftObj("type" to str("boolean")),
            ftObj("type" to str("int64")),
            ftObj("type" to str("id"), "table" to str("t")),
            ftObj("type" to str("id"), "table" to str("t"), "onDelete" to str("cascade")),
            ftObj("type" to str("literal"), "value" to str("x")),
            ftObj("type" to str("literal"), "value" to JsonPrimitive(42)),
            ftObj("type" to str("optional"), "inner" to ftObj("type" to str("string"))),
            ftObj(
                "type" to str("union"),
                "variants" to JsonArray(
                    listOf(
                        ftObj("type" to str("literal"), "value" to str("a")),
                        ftObj("type" to str("literal"), "value" to str("b")),
                    ),
                ),
            ),
            ftObj("type" to str("union"), "variants" to JsonArray(emptyList())),
            ftObj(
                "type" to str("union"),
                "variants" to JsonArray(
                    listOf(
                        ftObj("type" to str("literal"), "value" to str("a")),
                        ftObj("type" to str("number")),
                    ),
                ),
            ),
            ftObj("type" to str("null")),
            ftObj("type" to str("array"), "element" to ftObj("type" to str("string"))),
            ftObj("type" to str("object"), "fields" to ftObj("a" to ftObj("type" to str("string")))),
            ftObj("type" to str("bytes")),
            ftObj("type" to str("any")),
            ftObj("type" to str("record"), "value" to ftObj("type" to str("string"))),
            ftObj("type" to str("vector"), "dimensions" to JsonPrimitive(3)),
        )

    @Test
    fun everyOwnVariantHasAFixtureRow() {
        val rowKeys = rows().map { canonical(it["fieldType"]!!) }.toSet()
        for (own in ownVariants()) {
            assertTrue(rowKeys.contains(canonical(own)), "no fixture row for $own")
        }
    }

    @Test
    fun everyFixtureRowMatchesThisEngine() {
        for ((i, row) in rows().withIndex()) {
            val ty = parseFieldType(row["fieldType"]!!)
            val isError = row["error"] is JsonPrimitive && (row["error"] as JsonPrimitive).boolean
            if (isError) {
                val ei =
                    assertFailsWith<RtDbException>("row $i: expected SCHEMA_VIOLATION") {
                        indexColumnType(ty)
                    }
                assertEquals(ErrorCode.SCHEMA_VIOLATION, ei.code, "row $i error code")
                continue
            }
            val resolved = indexColumnType(ty)
            assertEquals((row["columnType"] as JsonPrimitive).content, rawPg(resolved.pg), "row $i columnType")
            val nullable = (row["nullable"] as JsonPrimitive).boolean
            assertEquals(nullable, resolved.nullable, "row $i nullable")
        }
    }
}
