package dev.parrtdb.inmemory

import dev.parrtdb.corpus.Compare
import dev.parrtdb.corpus.CorpusPaths
import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.Step
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.ValueExpr
import dev.parrtdb.wire.WireJson
import dev.parrtdb.wire.jsonArr
import dev.parrtdb.wire.jsonObj
import dev.parrtdb.wire.jsonStr
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

/**
 * ENH-052 family 3 — aggregate-terminals spec-table runner:
 * wire-corpus/spec/aggregate-terminals.json. Each row pushes its schema
 * fragment, inserts the docs through the real write path, runs the
 * aggregate query through `InMemoryClient.query`, and asserts the bare wire
 * value (`expect.error` rows assert the failure).
 */
class AggregateTerminalsSpecCorpusTest {
    private fun rows(): List<JsonObject> {
        val doc = WireJson.parse(CorpusPaths.read("spec", "aggregate-terminals.json")) as? JsonObject
            ?: error("aggregate terminals corpus is not a JSON object")
        val arr = doc["rows"] as? JsonArray ?: error("spec corpus has no rows array")
        return arr.map { it as JsonObject }
    }

    private fun newClient(): InMemoryClient {
        var clock = 1_700_000_000_000L
        return InMemoryClient(InMemoryOptions(now = { ++clock }, random = { 0.0 }))
    }

    private fun aggOp(row: JsonObject, op: String): Boolean {
        val agg = row["aggregate"] as JsonObject
        (agg["op"] as? JsonPrimitive)?.let { if (it.content == op) return true }
        (agg["aggregates"] as? JsonObject)?.forEach { (_, v) ->
            if ((v as JsonPrimitive).content == op) return true
        }
        return false
    }

    /** The corpus README's own-op coverage rule. */
    @Test
    fun everyAggregateOpHasAFixtureRow() {
        for (op in listOf("sum", "avg", "min", "max", "count")) {
            assertTrue(rows().any { aggOp(it, op) }, "no fixture row exercises aggregate op '$op'")
        }
    }

    @Test
    fun everyRowMatchesThisEngine() {
        for ((i, row) in rows().withIndex()) {
            val id = (row["id"] as JsonPrimitive).content
            val index = row["index"] as JsonObject
            val client = newClient()
            client.pushSchema(
                jsonObj(
                    "tables" to jsonObj(
                        "items" to jsonObj(
                            "fields" to row["fields"]!!,
                            "indexes" to jsonArr(
                                listOf(jsonObj("name" to index["name"]!!, "fields" to index["fields"]!!)),
                            ),
                        ),
                    ),
                ),
            )
            val docs = (row["docs"] as JsonArray).toList()
            client.mutate(Transaction(listOf(Step.InsertMany("items", docs))))
            val q =
                Query.fromJson(
                    jsonObj(
                        "table" to jsonStr("items"),
                        "index" to index["name"]!!,
                        "eq" to row["eq"]!!,
                        "aggregate" to row["aggregate"]!!,
                    ),
                )
            val expect = row["expect"]!!
            val isErrRow = expect is JsonObject && expect.containsKey("error")
            if (isErrRow) {
                assertFailsWith<RtDbException>("row $i ($id): expected the aggregate to fail") {
                    client.query(q)
                }
                continue
            }
            assertEquals(expect, client.query(q), "row $i ($id): aggregate result")
        }
    }
}

/**
 * ENH-052 family 4 — pagination spec-table runner:
 * wire-corpus/spec/pagination.json. Each row pushes its schema fragment,
 * inserts the docs through the real write path, encodes the RAW cursor
 * values through this engine's codec, runs the paginate query through
 * `InMemoryClient.query`, and asserts the page envelope —
 * `nextCursorPrefix` is the unbound-index prefix of the decoded minted
 * cursor (`expect.error` rows assert the failure with its code).
 */
class PaginationSpecCorpusTest {
    private fun rows(): List<JsonObject> {
        val doc = WireJson.parse(CorpusPaths.read("spec", "pagination.json")) as? JsonObject
            ?: error("pagination corpus is not a JSON object")
        val arr = doc["rows"] as? JsonArray ?: error("pagination corpus has no rows array")
        return arr.map { it as JsonObject }
    }

    private fun newClient(): InMemoryClient {
        var clock = 1_700_000_000_000L
        return InMemoryClient(InMemoryOptions(now = { ++clock }, random = { 0.0 }))
    }

    /** The corpus README's own-coverage rule: every Order spelling and keyset
     *  value kind this engine distinguishes must be exercised by a row. */
    @Test
    fun everyOrderAndKindHasAFixtureRow() {
        val orders = HashSet<String>()
        val kinds = HashSet<String>()
        for (row in rows()) {
            orders.add((row["order"] as JsonPrimitive).content)
            val fields = row["fields"] as JsonObject
            val v = fields["v"] as? JsonObject
            kinds.add((v?.get("type") as? JsonPrimitive)?.content ?: "string")
        }
        for (order in listOf("asc", "desc")) {
            assertTrue(order in orders, "no fixture row exercises order '$order'")
        }
        for (kind in listOf("number", "int64", "boolean", "optional")) {
            assertTrue(kind in kinds, "no fixture row exercises value kind '$kind'")
        }
    }

    @Test
    fun everyRowMatchesThisEngine() {
        for ((i, row) in rows().withIndex()) {
            val id = (row["id"] as JsonPrimitive).content
            val index = row["index"] as JsonObject
            val client = newClient()
            client.pushSchema(
                jsonObj(
                    "tables" to jsonObj(
                        "items" to jsonObj(
                            "fields" to row["fields"]!!,
                            "indexes" to jsonArr(
                                listOf(jsonObj("name" to index["name"]!!, "fields" to index["fields"]!!)),
                            ),
                        ),
                    ),
                ),
            )
            val docs = (row["docs"] as JsonArray).toList()
            client.mutate(Transaction(listOf(Step.InsertMany("items", docs))))
            val pagJson = (row["paginate"] as JsonObject).toMutableMap()
            (pagJson["cursor"] as? JsonArray)?.let { raw ->
                pagJson["cursor"] = JsonPrimitive(encodeCursorForEngine(raw.toList()))
            }
            val q =
                Query.fromJson(
                    jsonObj(
                        "table" to jsonStr("items"),
                        "index" to index["name"]!!,
                        "eq" to row["eq"]!!,
                        "order" to row["order"]!!,
                        "paginate" to JsonObject(pagJson),
                    ),
                )
            val expect = row["expect"] as JsonObject
            val wantCode = (expect["error"] as? JsonObject)?.get("code")?.let {
                (it as JsonPrimitive).content
            }
            if (wantCode != null) {
                val err = assertFailsWith<RtDbException>("row $i ($id): expected the query to fail") {
                    client.query(q)
                }
                assertEquals(ErrorCode.fromWire(wantCode), err.code, "row $i ($id): rejection code")
                continue
            }
            val result = client.query(q) as JsonObject
            val gotDocs = (result["docs"] as JsonArray).map { doc ->
                (doc as JsonObject).toMutableMap().apply {
                    remove("_id")
                    remove("_creationTime")
                    remove("_version")
                }.let { JsonObject(it) }
            }
            assertEquals(
                expect["docs"],
                JsonArray(gotDocs),
                "row $i ($id): page docs",
            )
            val prefix = expect["nextCursorPrefix"] as? JsonArray
            if (prefix == null) {
                assertTrue(
                    result.containsKey("nextCursor").not(),
                    "row $i ($id): expected no next cursor",
                )
                continue
            }
            val encoded = result["nextCursor"] as? JsonPrimitive
                ?: error("row $i ($id): expected a next cursor")
            val decoded = decodeCursorForEngine(encoded.content)
            assertEquals(
                prefix.size + 2,
                decoded.size,
                "row $i ($id): minted cursor arity",
            )
            assertEquals(
                prefix,
                JsonArray(decoded.take(prefix.size)),
                "row $i ($id): minted cursor prefix",
            )
            assertTrue(
                decoded[decoded.size - 2] is JsonPrimitive && (decoded[decoded.size - 2] as JsonPrimitive).content.toDoubleOrNull() != null,
                "row $i ($id): created_at cursor kind",
            )
            assertTrue(
                decoded.last() is JsonPrimitive && (decoded.last() as JsonPrimitive).isString,
                "row $i ($id): id cursor kind",
            )
        }
    }
}
