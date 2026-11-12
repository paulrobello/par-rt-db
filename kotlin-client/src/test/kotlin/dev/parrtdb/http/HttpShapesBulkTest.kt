package dev.parrtdb.http

import dev.parrtdb.wire.WireException
import dev.parrtdb.wire.WireJson
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject

/** HTTP-shape drift verification for the ARC-005 bulk-delete surface over
 * `wire-corpus/http-shapes.json` (repo root — the single source of truth;
 * the server-side parity test `http_api::bulk::tests::bulk_http_shapes_fixture_parity`
 * constructs the real wire structs against it, and every client mirrors it).
 *
 * Strict-decodes the fixture's [BulkOpAccepted] and [BulkOpInfo] instances
 * (unknown fields reject via [StrictObj.finish]), asserts every field, and
 * verifies the absent-optional contract: a running op carries no `error` key
 * at all (server `skip_serializing_if`), while the failed instance carries
 * it. */
class HttpShapesBulkTest {
    private val shapes: JsonObject by lazy {
        var dir: File? = File("").absoluteFile
        while (dir != null && !File(dir, "wire-corpus/wire-corpus.json").isFile) dir = dir.parentFile
        val root = dir ?: error("wire-corpus not found above ${File("").absolutePath}")
        WireJson.parse(File(root, "wire-corpus/http-shapes.json").readText()) as JsonObject
    }

    @Test
    fun bulkDeleteAcceptedDecodesEveryField() {
        val accepted = BulkOpAccepted.fromJson(shapes["bulkDeleteAccepted"]!!)
        assertEquals("bulk_01JENQ9Z7K3M2V8R5T4W6X8Y0A", accepted.opId)
        assertEquals(1500L, accepted.matched)
        assertEquals("running", accepted.state)
    }

    @Test
    fun bulkOpInfoRunningDecodesEveryFieldAndErrorIsAbsent() {
        val info = BulkOpInfo.fromJson(shapes["bulkOpInfoRunning"]!!)
        assertEquals("bulk_01JENQ9Z7K3M2V8R5T4W6X8Y0A", info.id)
        assertEquals("running", info.state)
        assertEquals(1500L, info.matched)
        assertEquals(500L, info.deleted)
        assertNull(info.error)
    }

    @Test
    fun bulkOpInfoFailedDecodesError() {
        val info = BulkOpInfo.fromJson(shapes["bulkOpInfoFailed"]!!)
        assertEquals("bulk_01JENQ9Z7K3M2V8R5T4W6X8Y0A", info.id)
        assertEquals("failed", info.state)
        assertEquals(1500L, info.matched)
        assertEquals(500L, info.deleted)
        assertEquals("chunk turn failed: schema changed mid-op", info.error)
    }

    @Test
    fun bulkOpAcceptedRejectsUnknownField() {
        val bogus = JsonObject(shapes["bulkDeleteAccepted"]!!.jsonObject + ("bogusKey" to WireJson.parse("1")))
        val failure = assertFailsWith<WireException> { BulkOpAccepted.fromJson(bogus) }
        assertIs<WireException>(failure)
    }

    @Test
    fun bulkOpInfoRejectsUnknownField() {
        val bogus = JsonObject(shapes["bulkOpInfoRunning"]!!.jsonObject + ("bogusKey" to WireJson.parse("1")))
        val failure = assertFailsWith<WireException> { BulkOpInfo.fromJson(bogus) }
        assertIs<WireException>(failure)
    }
}
