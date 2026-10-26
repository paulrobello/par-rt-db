package dev.parrtdb.corpus

import dev.parrtdb.admin.DbStats
import dev.parrtdb.admin.Directive
import dev.parrtdb.admin.MigrateResult
import dev.parrtdb.admin.OpEvent
import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.inmemory.InMemoryClient
import dev.parrtdb.inmemory.InMemoryOptions
import dev.parrtdb.inmemory.evalQuery
import dev.parrtdb.wire.AuthedUser
import dev.parrtdb.wire.ChangeFeedResponse
import dev.parrtdb.wire.ClientMessage
import dev.parrtdb.wire.ErrorEnvelope
import dev.parrtdb.wire.MAX_STEPS
import dev.parrtdb.wire.PROTOCOL_VERSION
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.ScheduleInfo
import dev.parrtdb.wire.ScheduleWhen
import dev.parrtdb.wire.ServerMessage
import dev.parrtdb.wire.WireException
import dev.parrtdb.wire.WireJson
import dev.parrtdb.wire.WorkflowSpec
import dev.parrtdb.wire.Transaction
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.junit.jupiter.api.DynamicTest
import org.junit.jupiter.api.TestFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.test.fail

/** Runs every semantics-corpus case; any `skip.kotlin` entry or failing case fails the build. */
class SemanticsCorpusTest {
    @TestFactory
    fun everySemanticsCase(): List<DynamicTest> =
        SemanticsRunner.caseFiles().map { f ->
            DynamicTest.dynamicTest(f.name.removeSuffix(".json")) {
                when (val o = SemanticsRunner.run(f)) {
                    is SemanticsRunner.Outcome.Skipped -> fail("${f.name}: skipped (${o.reason}); the kotlin runner allows zero skips")
                    SemanticsRunner.Outcome.Executed -> Unit
                }
            }
        }

    @Test
    fun corpusIsNotEmptyAndNothingSkipsKotlin() {
        val files = SemanticsRunner.caseFiles()
        assertTrue(files.size >= 100, "semantics corpus unexpectedly small: ${files.size}")
        var skipped = 0
        var executed = 0
        for (f in files) {
            when (SemanticsRunner.run(f)) {
                is SemanticsRunner.Outcome.Skipped -> skipped++
                SemanticsRunner.Outcome.Executed -> executed++
            }
        }
        println("SEMANTICS-CORPUS executed=$executed skipped=$skipped total=${files.size}")
        assertEquals(0, skipped)
        assertEquals(files.size, executed)
    }
}

/** wire-corpus/golden-vector.json against the in-memory engine. */
class GoldenVectorTest {
    private fun str(e: JsonElement?): String? = (e as? JsonPrimitive)?.takeIf { it.isString }?.content

    private fun fieldType(shorthand: String): JsonObject {
        fun t(tag: String) = JsonObject(mapOf("type" to JsonPrimitive(tag)))
        return when {
            shorthand == "string" -> t("string")
            shorthand == "number" -> t("number")
            shorthand == "optional(string)" -> JsonObject(mapOf("type" to JsonPrimitive("optional"), "inner" to t("string")))
            shorthand == "array(string)" -> JsonObject(mapOf("type" to JsonPrimitive("array"), "element" to t("string")))
            shorthand.startsWith("vector(") -> JsonObject(mapOf("type" to JsonPrimitive("vector"), "dimensions" to WireJson.parse(shorthand.removePrefix("vector(").removeSuffix(")"))))
            else -> error("fixture field type not implemented: $shorthand")
        }
    }

    private fun project(doc: JsonElement): JsonElement {
        val o = doc as? JsonObject ?: return doc
        return JsonObject(listOf("name", "status", "order").associateWith { o[it] ?: JsonNull })
    }

    @Test
    fun goldenVectorParity() {
        val fx = CorpusPaths.parse("golden-vector.json") as JsonObject
        val table = str(fx["schema_table"])!!
        val fields = (fx["schema_fields"] as JsonObject).mapValues { fieldType((it.value as JsonPrimitive).content) }
        val indexes =
            (fx["schema_indexes"] as JsonArray).map { ix ->
                val o = ix as JsonObject
                val m = linkedMapOf<String, JsonElement>("name" to o["name"]!!, "fields" to o["fields"]!!)
                if ((o["search"] as? JsonPrimitive)?.content == "true") m["search"] = JsonPrimitive(true)
                (o["vector"] as? JsonObject)?.let { m["vector"] = it }
                JsonObject(m)
            }
        val schema = JsonObject(mapOf("tables" to JsonObject(mapOf(table to JsonObject(mapOf("fields" to JsonObject(fields), "indexes" to JsonArray(indexes)))))))
        var clock = 1_700_000_000_000L
        val client = InMemoryClient(InMemoryOptions(now = { ++clock }, random = { 0.0 }))
        client.pushSchema(schema)
        for (doc in fx["seed"] as JsonArray) {
            client.mutate(Transaction(listOf(dev.parrtdb.wire.Step.Insert(table, doc))))
        }
        val cases = fx["cases"] as JsonArray
        assertTrue(cases.size >= 38)
        for (c in cases) {
            val co = c as JsonObject
            val id = str(co["id"])!!
            val result = evalQuery(client.store, Query.fromJson(co["query"]!!))
            val unordered = (co["expected_unordered"] as? JsonPrimitive)?.content == "true"
            fun numEq(a: JsonElement, b: JsonElement) = Compare.equalNumeric(a, b)
            when {
                co["expected_scalar"] != null -> assertTrue(numEq(result, co["expected_scalar"]!!), "$id: count ${Compare.canonical(result)}")
                co["expected_value"] != null -> assertTrue(numEq(result, co["expected_value"]!!), "$id: aggregate ${Compare.canonical(result)}")
                co["expected_groups"] != null -> assertTrue(numEq(result, co["expected_groups"]!!), "$id: groups ${Compare.canonical(result)} vs ${Compare.canonical(co["expected_groups"]!!)}")
                co["expected_distinct"] != null -> assertTrue(numEq(result, co["expected_distinct"]!!), "$id: distinct ${Compare.canonical(result)}")
                (co["expected_has_next_cursor"] as? JsonPrimitive)?.content == "true" -> {
                    val page = result as JsonObject
                    val docs = JsonArray((page["docs"] as JsonArray).map { project(it) })
                    assertTrue(numEq(docs, co["expected"]!!), "$id: page ${Compare.canonical(docs)}")
                    assertTrue(page["nextCursor"].let { it != null && it !is JsonNull }, "$id: expected nextCursor")
                }
                co["expected"] != null -> {
                    val want = co["expected"]!!
                    if (want is JsonArray) {
                        var got: JsonElement = JsonArray((result as JsonArray).map { project(it) })
                        var w: JsonElement = want
                        if (unordered) {
                            got = JsonArray((got as JsonArray).sortedBy { str((it as JsonObject)["name"]) ?: "" })
                            w = JsonArray(want.sortedBy { str((it as JsonObject)["name"]) ?: "" })
                        }
                        assertTrue(numEq(got, w), "$id: mismatch\n got ${Compare.canonical(got)}\nwant ${Compare.canonical(w)}")
                    } else {
                        assertTrue(numEq(project(result), want), "$id: single doc ${Compare.canonical(result)}")
                    }
                }
                else -> fail("$id: case has no expected shape")
            }
        }
        println("GOLDEN-VECTOR cases=${cases.size}")
    }
}

/** wire-corpus/wire-corpus.json: strict decode -> re-encode -> structural equality for every section. */
class WireCorpusTest {
    private val corpus: JsonObject by lazy { CorpusPaths.parse("wire-corpus.json") as JsonObject }

    private fun section(name: String): List<JsonElement> {
        val arr = corpus[name] as? JsonArray ?: fail("corpus is missing the $name section")
        assertTrue(arr.isNotEmpty(), "$name: empty section")
        return arr
    }

    private fun stripComment(e: JsonElement): JsonElement = if (e is JsonObject) JsonObject(e.filterKeys { it != "\$comment" }) else e

    private fun roundTrip(sectionName: String, decodeEncode: (JsonElement) -> JsonElement) {
        section(sectionName).forEachIndexed { i, raw ->
            val fixture = stripComment(raw)
            val again =
                try {
                    decodeEncode(fixture)
                } catch (e: WireException) {
                    fail("$sectionName #$i: strict decode failed: ${e.message}")
                }
            assertTrue(
                Compare.equalNumeric(fixture, again) && Compare.canonical(fixture) == Compare.canonical(again),
                "$sectionName #$i: round-trip diverged\n fixture ${Compare.canonical(fixture)}\nremarshal ${Compare.canonical(again)}",
            )
        }
    }

    @Test fun clientMessages() = roundTrip("client_messages") { ClientMessage.fromJson(it).toJson() }

    @Test fun serverMessages() = roundTrip("server_messages") { ServerMessage.fromJson(it).toJson() }

    @Test fun authedUsers() = roundTrip("authed_users") { AuthedUser.fromJson(it).toJson() }

    @Test fun scheduleWhens() = roundTrip("schedule_whens") { ScheduleWhen.fromJson(it).toJson() }

    @Test fun scheduleInfos() = roundTrip("schedule_infos") { ScheduleInfo.fromJson(it).toJson() }

    @Test fun queries() = roundTrip("queries") { Query.fromJson(it).toJson() }

    @Test fun errorEnvelopes() = roundTrip("error_envelopes") { ErrorEnvelope.fromJson(it).toJson() }

    @Test fun changeFeedResponses() = roundTrip("change_feed_responses") { ChangeFeedResponse.fromJson(it).toJson() }

    @Test fun adminOpEvents() = roundTrip("admin_op_events") { OpEvent.fromJson(it).toJson() }

    @Test fun dbStats() = roundTrip("db_stats") { DbStats.fromJson(it).toJson() }

    @Test fun migrateResults() = roundTrip("migrate_results") { MigrateResult.fromJson(it).toJson() }

    @Test
    fun migrateRequests() {
        section("migrate_requests").forEachIndexed { i, raw ->
            val req = raw as JsonObject
            for ((j, d) in (req["directives"] as JsonArray).withIndex()) {
                val again = Directive.fromJson(d).toJson()
                assertEquals(Compare.canonical(d), Compare.canonical(again), "migrate_requests #$i directive #$j")
            }
        }
    }

    /** query_results is deliberately untyped on the wire; the raw value must re-serialize byte-stably. */
    @Test
    fun queryResultsRawRoundTrip() {
        section("query_results").forEach { raw ->
            val again = WireJson.parse(WireJson.encode(raw))
            assertEquals(Compare.canonical(raw), Compare.canonical(again))
        }
    }

    @Test
    fun rejectsUnknownFields() {
        val cases: List<Pair<String, (JsonElement) -> Unit>> =
            listOf(
                "rejects_client_message_unknown_field" to { e -> ClientMessage.fromJson(e) },
                "rejects_schedule_when_unknown_field" to { e -> ScheduleWhen.fromJson(e) },
                "rejects_workflow_spec_unknown_field" to { e -> WorkflowSpec.fromJson(e) },
                "rejects_authed_user_unknown_kind" to { e -> AuthedUser.fromJson(e) },
                "rejects_schedule_info_unknown_kind" to { e -> ScheduleInfo.fromJson(e) },
                "rejects_schedule_info_unknown_status" to { e -> ScheduleInfo.fromJson(e) },
            )
        for ((sec, decode) in cases) {
            section(sec).forEachIndexed { i, raw ->
                assertFailsWith<WireException>("$sec #$i: strict decode must reject this fixture") { decode(raw) }
            }
        }
    }

    @Test
    fun protocolConstants() {
        val c = corpus["protocol_constants"] as JsonObject
        assertEquals((c["max_steps"] as JsonPrimitive).content.toInt(), MAX_STEPS)
        assertEquals((c["protocol_version"] as JsonPrimitive).content.toInt(), PROTOCOL_VERSION)
    }

    /** Every top-level section must be consumed by a test above (the README's coverage rule). */
    @Test
    fun everySectionHasAConsumer() {
        val consumed =
            setOf(
                "client_messages", "server_messages", "authed_users", "schedule_whens", "schedule_infos", "query_results", "error_envelopes",
                "queries", "migrate_requests", "migrate_results", "db_stats", "change_feed_responses", "admin_op_events", "protocol_constants",
                "rejects_client_message_unknown_field", "rejects_schedule_when_unknown_field", "rejects_workflow_spec_unknown_field",
                "rejects_authed_user_unknown_kind", "rejects_schedule_info_unknown_kind", "rejects_schedule_info_unknown_status",
            )
        val actual = corpus.keys.filter { it != "\$comment" }.toSet()
        assertEquals(emptySet(), actual - consumed, "corpus sections without a Kotlin consumer")
        assertEquals(emptySet(), consumed - actual, "consumer names a section the corpus no longer has")
    }

    @Test
    fun errorCodesMatchCorpus() {
        val fx = CorpusPaths.parse("error-codes.json") as JsonObject
        val rows = (fx["codes"] as JsonArray).map { it as JsonObject }
        assertEquals(rows.size, ErrorCode.entries.size)
        for (row in rows) {
            val code = (row["code"] as JsonPrimitive).content
            val status = (row["httpStatus"] as JsonPrimitive).content.toInt()
            val ec = ErrorCode.fromWire(code) ?: fail("corpus code $code missing from the client")
            assertEquals(status, ec.httpStatus, "httpStatus for $code")
        }
        assertEquals(rows.map { (it["code"] as JsonPrimitive).content }, ErrorCode.entries.map { it.wire }, "declaration order")
        assertNull(ErrorCode.fromWire("NOPE"))
    }
}
