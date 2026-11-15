package dev.parrtdb.http

import dev.parrtdb.wire.ScheduleInfo
import dev.parrtdb.wire.StepResult
import dev.parrtdb.wire.StrictObj
import dev.parrtdb.wire.ScheduleKind
import dev.parrtdb.wire.ScheduleStatus
import dev.parrtdb.wire.WireException
import dev.parrtdb.wire.WireJson
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import java.io.File
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonObject

/** HTTP-shape drift verification (kotlin-client view) over
 * `wire-corpus/http-shapes.json` (repo root — the single source of truth; the
 * server-side parity test `protocol::tests::http_shapes_fixture_parity`
 * constructs the real wire structs against it, and every client mirrors it).
 *
 * Strict-decodes the fixture's [ClaimedSchedule] and [ScheduleInfo]
 * instances (unknown fields reject via [StrictObj.finish]), asserts every
 * field, and verifies the absent-optional contract (oneshot carries no
 * cron/tz/everyMs keys). The fixture exists because the ClaimedSchedule.tz
 * drift shipped undetected (fixed in 8a55d03); the WS corpus does not cover
 * HTTP-only shapes. */
class HttpShapesTest {
    private val shapes: JsonObject by lazy {
        var dir: File? = File("").absoluteFile
        while (dir != null && !File(dir, "wire-corpus/wire-corpus.json").isFile) dir = dir.parentFile
        val root = dir ?: error("wire-corpus not found above ${File("").absolutePath}")
        WireJson.parse(File(root, "wire-corpus/http-shapes.json").readText()) as JsonObject
    }

    @Test
    fun claimedScheduleCronDecodesEveryField() {
        val job = ClaimedSchedule.fromJson(shapes["claimedScheduleCron"]!!)
        assertEquals("job_cron_1", job.id)
        assertEquals(ScheduleKind.Cron, job.kind)
        assertEquals(1794470400000L, job.dueAt)
        assertEquals(0, job.txn.steps.size)
        assertEquals("0 5 * * *", job.cron)
        assertEquals("America/Los_Angeles", job.tz)
        assertNull(job.everyMs)
        assertEquals(7L, job.leaseGeneration)
        assertEquals(1794470500000L, job.leaseDeadlineMs)
    }

    @Test
    fun claimedScheduleOneshotToleratesAbsentOptionals() {
        val job = ClaimedSchedule.fromJson(shapes["claimedScheduleOneshot"]!!)
        assertEquals("job_oneshot_1", job.id)
        assertEquals(ScheduleKind.Oneshot, job.kind)
        assertNull(job.cron)
        assertNull(job.tz)
        assertNull(job.everyMs)
        assertEquals(8L, job.leaseGeneration)
        assertEquals(1794470500000L, job.leaseDeadlineMs)
    }

    @Test
    fun claimedScheduleRejectsUnknownField() {
        val bogus = JsonObject(shapes["claimedScheduleCron"]!!.jsonObject + ("bogusKey" to WireJson.parse("1")))
        val failure = assertFailsWith<WireException> { ClaimedSchedule.fromJson(bogus) }
        assertIs<WireException>(failure)
    }

    @Test
    fun scheduleInfoCronDecodesEveryField() {
        val info = ScheduleInfo.fromJson(shapes["scheduleInfoCron"]!!)
        assertEquals("job_cron_1", info.id)
        assertEquals(ScheduleKind.Cron, info.kind)
        assertEquals(1794470400000L, info.dueAt)
        assertEquals("0 5 * * *", info.cron)
        assertEquals("America/Los_Angeles", info.tz)
        assertEquals(ScheduleStatus.Paused, info.status)
        assertEquals("connection reset", info.lastError)
        assertEquals(1794470000000L, info.createdAt)
        assertEquals(3L, info.firedCount)
        assertEquals(true, info.external)
        assertEquals(2L, info.missedCount)
        assertEquals(1794470300000L, info.lastMissedAt)
    }

    @Test
    fun scheduleInfoMinimalDecodesDefaults() {
        val info = ScheduleInfo.fromJson(shapes["scheduleInfoMinimal"]!!)
        assertEquals("job_oneshot_1", info.id)
        assertEquals(ScheduleKind.Oneshot, info.kind)
        assertNull(info.cron)
        assertNull(info.tz)
        assertEquals(ScheduleStatus.Pending, info.status)
        assertNull(info.lastError)
        assertEquals(0L, info.firedCount)
        assertFalse(info.external)
        assertEquals(0L, info.missedCount)
        assertNull(info.lastMissedAt)
    }

    @Test
    fun claimSchedulesResponseDecodes() {
        val resp = shapes["claimSchedulesResponse"]!!.jsonObject
        val jobs = (resp["jobs"] as kotlinx.serialization.json.JsonArray).map { ClaimedSchedule.fromJson(it) }
        assertEquals(1, jobs.size)
        assertEquals("job_cron_1", jobs[0].id)
        assertEquals("America/Los_Angeles", jobs[0].tz)
    }

    @Test
    fun queryBatchResponseSlotsParseLikeTheClient() {
        // Mirrors RtDbHttpClient.queryBatch's StrictObj slot parse over the
        // fixture: exact keys, omit-when-None pairing (an ok slot carries no
        // `error` key; an errored slot no `result` key).
        val resp = shapes["queryBatchResponse"]!!
        val o = StrictObj.of(resp, "queryBatchResponse")
        val slots = o.arr("results").map { s ->
            val so = StrictObj.of(s, "querySlot")
            val err = so.opt("error")
            val ok = so.bool("ok")
            val res = so.raw("result")
            so.finish()
            if (err != null) QuerySlot(false) else if (ok) QuerySlot(true, result = res) else QuerySlot(false)
        }
        assertEquals(2, slots.size)
        assertEquals(true, slots[0].ok)
        assertEquals(2, (slots[0].result as kotlinx.serialization.json.JsonArray).size)
        assertNull(slots[0].error)
        assertEquals(false, slots[1].ok)
        assertNull(slots[1].result)
        // The errored slot carries the {code, message} envelope only.
        val errSlot = (shapes["queryBatchResponse"]!!.jsonObject["results"] as kotlinx.serialization.json.JsonArray)[1].jsonObject
        assertEquals(setOf("ok", "error"), errSlot.keys)
        assertEquals(setOf("code", "message"), errSlot["error"]!!.jsonObject.keys)
    }

    @Test
    fun mutateBatchResponseSlotsParseLikeTheClient() {
        val resp = shapes["mutateBatchResponse"]!!
        val o = StrictObj.of(resp, "mutateBatchResponse")
        val slots = o.arr("results").map { s ->
            val so = StrictObj.of(s, "mutateBatchSlot")
            val ok = so.bool("ok")
            val results = so.optArr("results")?.map { StepResult.fromJson(it) }
            val err = so.opt("error")
            so.finish()
            MutateBatchSlot(ok, if (err == null) results else null)
        }
        assertEquals(2, slots.size)
        assertEquals(true, slots[0].ok)
        assertEquals(2, slots[0].results?.size)
        assertNull(slots[0].error)
        assertEquals(false, slots[1].ok)
        assertNull(slots[1].results)
    }

    @Test
    fun batchRequestShapesCarryExactKeys() {
        val qb = shapes["queryBatchRequest"]!!.jsonObject
        assertEquals(setOf("db", "queries"), qb.keys)
        val mb = shapes["mutateBatchRequest"]!!.jsonObject
        assertEquals(setOf("db", "txns"), mb.keys)
        val entries = mb["txns"] as kotlinx.serialization.json.JsonArray
        assertEquals(setOf("txn", "idempotencyKey"), entries[0].jsonObject.keys)
        assertEquals(setOf("txn"), entries[1].jsonObject.keys)
    }
}
