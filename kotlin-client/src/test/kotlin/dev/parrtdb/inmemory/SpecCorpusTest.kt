package dev.parrtdb.inmemory

import dev.parrtdb.corpus.Compare
import dev.parrtdb.corpus.CorpusPaths
import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.ValueExpr
import dev.parrtdb.wire.WireJson
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
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

/**
 * ENH-052 family 2 — spec-table corpus runner: computed-field stamping
 * (wire-corpus/spec/computed-stamping.json).
 *
 * Asserts the engine's `stampComputed` port of server `txn::stamp_computed` +
 * `value_expr::eval_value_expr` against every fixture row: each row's wire
 * schema fragment (`fields` + `computed`) is decoded through the engine's own
 * `parseSchema`, stamped with the engine, and asserted per key with
 * `Compare.equalNumeric` — a JSON-null expected value means the key must be
 * ABSENT after stamping. A semantics change regenerates the fixture in the
 * same commit (`make gen-spec-computed-stamping`).
 */
class ComputedStampingSpecCorpusTest {
    private fun rows(): List<JsonObject> {
        val doc = WireJson.parse(CorpusPaths.read("spec", "computed-stamping.json")) as? JsonObject
            ?: error("computed stamping corpus is not a JSON object")
        val arr = doc["rows"] as? JsonArray ?: error("spec corpus has no rows array")
        return arr.map { it as JsonObject }
    }

    /** The wire op tags this engine's evalValueExpr distinguishes — the
     *  corpus README's own-op coverage rule: a grammar node without a fixture
     *  row fails. */
    private val ownOps: Set<String> =
        setOf(
            "field", "literal", "concat", "add", "sub", "mul", "div",
            "coalesce", "lower", "upper", "trim", "cast", "now", "case",
        )

    private fun fragmentTable(row: JsonObject): TableDef {
        val schema = WireJson.parse(
            WireJson.encode(
                JsonObject(
                    mapOf(
                        "tables" to JsonObject(
                            mapOf(
                                "t" to JsonObject(
                                    mapOf(
                                        "fields" to row["fields"]!!,
                                        "computed" to row["computed"]!!,
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )
        return parseSchema(schema).tables.getValue("t")
    }

    @Test
    fun everyOpHasAFixtureRow() {
        val seen = mutableSetOf<String>()
        for (row in rows()) {
            val computed = row["computed"] as JsonObject
            for (expr in computed.values) {
                seen.add(
                    when (ValueExpr.fromJson(expr)) {
                        is ValueExpr.Field -> "field"
                        is ValueExpr.Literal -> "literal"
                        is ValueExpr.Concat -> "concat"
                        is ValueExpr.Add -> "add"
                        is ValueExpr.Sub -> "sub"
                        is ValueExpr.Mul -> "mul"
                        is ValueExpr.Div -> "div"
                        is ValueExpr.Coalesce -> "coalesce"
                        is ValueExpr.Lower -> "lower"
                        is ValueExpr.Upper -> "upper"
                        is ValueExpr.Trim -> "trim"
                        is ValueExpr.CastTo -> "cast"
                        is ValueExpr.Now -> "now"
                        is ValueExpr.Case -> "case"
                    },
                )
            }
        }
        for (op in ownOps) {
            assertTrue(op in seen, "no fixture row exercises op '$op'")
        }
    }

    @Test
    fun everyFixtureRowMatchesThisEngine() {
        for ((i, row) in rows().withIndex()) {
            val table = fragmentTable(row)
            val doc = row["inputDoc"] as JsonObject
            val expect = row["expect"] as JsonObject
            if (expect["error"] != null) {
                val ei =
                    assertFailsWith<RtDbException>("row $i: expected the stamp to fail") {
                        stampComputed(table, doc, row["now"]!!.jsonLong())
                    }
                assertEquals(ErrorCode.BAD_REQUEST, ei.code, "row $i error code")
                continue
            }
            val nowV = row["now"] as JsonPrimitive
            val stamped = stampComputed(table, doc, nowV.jsonLong())
            val stampedValues = expect["stampedValues"] as JsonObject
            for ((name, expected) in stampedValues) {
                if (expected is JsonNull) {
                    assertTrue(
                        name !in stamped,
                        "row $i: key '$name' must be absent after stamping",
                    )
                    continue
                }
                val got = stamped[name]
                    ?: throw AssertionError("row $i: missing computed key '$name'")
                assertTrue(
                    Compare.equalNumeric(got, expected),
                    "row $i: stamped value of '$name' is $got, want $expected",
                )
            }
        }
    }
}

/** Reads a JSON number primitive as a Long (epoch-ms / small fixture values). */
private fun JsonElement.jsonLong(): Long = (this as JsonPrimitive).content.toLong()
