package dev.parrtdb.inmemory

import dev.parrtdb.admin.Directive
import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.inmemory.CSupport.assertCode
import dev.parrtdb.inmemory.CSupport.j
import dev.parrtdb.inmemory.CSupport.str
import dev.parrtdb.wire.AuthedUser
import dev.parrtdb.wire.Cast
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.ScheduleWhen
import dev.parrtdb.wire.Step
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.UserKind
import dev.parrtdb.wire.jsonStr
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.runBlocking
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Scheduler, TTL, presence, subscription fan-out and migration tests over [InMemoryClient]. */
class CEngineTest {
    private val schema =
        """{"tables":{
          "items":{"fields":{"name":{"type":"string"},"n":{"type":"number"},"exp":{"type":"number"}},
                   "indexes":[{"name":"by_name","fields":["name"]},{"name":"by_exp","fields":["exp"]}],"ttl":{"field":"exp"}}
        }}"""

    private fun client(clock: () -> Long = { 1_000L }): InMemoryClient {
        var salt = 0.0
        val c = InMemoryClient(InMemoryOptions(now = clock, random = { salt = (salt + 0.0371) % 1.0; salt }))
        c.pushSchema(j(schema))
        return c
    }

    private fun ins(name: String, n: Int = 1, exp: Long = 9_000_000_000L) =
        Step.Insert("items", j("""{"name":"$name","n":$n,"exp":$exp}"""))

    private fun count(c: InMemoryClient): Int = (c.query(Query(table = "items")) as JsonArray).size

    @Test
    fun oneShotScheduleFiresOnceOnTick() {
        val c = client()
        val r = c.mutate(Transaction(listOf(Step.Schedule(ScheduleWhen.AfterMs(500), Transaction(listOf(ins("sched")))))))
        assertTrue(r.single().scheduleId != null)
        c.tick(1_400)
        assertEquals(0, count(c))
        c.tick(1_500)
        assertEquals(1, count(c))
        assertEquals(0, c.store.scheduledJobs.size)
        c.tick(9_999)
        assertEquals(1, count(c))
    }

    @Test
    fun intervalJobRearmsAndCancelRemovesIt() {
        val c = client()
        val id = c.mutate(Transaction(listOf(Step.Schedule(ScheduleWhen.Interval(100), Transaction(listOf(ins("tick")))))))[0].scheduleId!!
        c.tick(1_100)
        c.tick(1_200)
        assertEquals(2, count(c))
        assertEquals(1_300L, c.store.scheduledJobs.single().dueAt)
        val cancelled = c.mutate(Transaction(listOf(Step.CancelSchedule(id))))[0].cancelled
        assertEquals(true, cancelled)
        assertEquals(false, c.mutate(Transaction(listOf(Step.CancelSchedule(id))))[0].cancelled)
        assertCode(ErrorCode.BAD_REQUEST) {
            c.mutate(Transaction(listOf(Step.Schedule(ScheduleWhen.Interval(0), Transaction(emptyList())))))
        }
    }

    @Test
    fun failingJobIsMarkedErrorAndExternalJobsNeverRun() {
        val c = client()
        c.mutate(Transaction(listOf(Step.Schedule(ScheduleWhen.AfterMs(0), Transaction(listOf(Step.Patch("items", "nope", j("{}"))))))))
        c.mutate(Transaction(listOf(Step.Schedule(ScheduleWhen.AfterMs(0), Transaction(listOf(ins("ext"))), external = true))))
        c.tick(5_000)
        assertEquals(0, count(c))
        val jobs = c.store.scheduledJobs
        assertEquals(ScheduleStatusMem.Error, jobs[0].status)
        assertEquals(ScheduleStatusMem.Pending, jobs[1].status)
    }

    @Test
    fun ttlReapRemovesOnlyExpiredDocs() {
        val c = client()
        c.mutate(Transaction(listOf(ins("old", exp = 1_500), ins("fresh", exp = 9_000), ins("none"))))
        assertEquals(1, c.tick(2_000))
        assertEquals(2, count(c))
    }

    @Test
    fun subscriptionFiresInitialThenOncePerRealChangeOutsideTheLock() {
        val c = client()
        val seen = ArrayList<JsonArray>()
        var heldLockDuringCallback = true
        val sub =
            c.subscribe(Query(table = "items")) {
                heldLockDuringCallback = c.store.lock.isHeldByCurrentThread
                seen += it as JsonArray
            }
        assertEquals(1, seen.size)
        assertEquals(0, seen[0].size)
        c.mutate(Transaction(listOf(ins("a"))))
        assertEquals(2, seen.size)
        assertEquals(false, heldLockDuringCallback)
        // A write to the same table that leaves the result identical must not fire.
        c.mutate(Transaction(listOf(Step.ExpectBefore(9_007_199_254_740_991L))))
        assertEquals(2, seen.size)
        c.mutate(Transaction(listOf(ins("b"))))
        assertEquals(3, seen.size)
        sub.unsubscribe()
        c.mutate(Transaction(listOf(ins("c"))))
        assertEquals(3, seen.size)
        assertEquals(0, c.store.subscribers.size)
    }

    @Test
    fun subscriptionCallbackMayReenterTheStore() {
        val c = client()
        var inner = -1
        c.subscribe(Query(table = "items")) { inner = (c.query(Query(table = "items")) as JsonArray).size }
        c.mutate(Transaction(listOf(ins("a"))))
        assertEquals(1, inner)
    }

    @Test
    fun subscribeFlowEmitsSnapshotsAndUnsubscribesOnCancel() = runBlocking {
        val c = client()
        c.mutate(Transaction(listOf(ins("a"))))
        val flow = c.subscribeFlow(Query(table = "items"))
        assertEquals(1, (flow.first() as JsonArray).size)
        assertEquals(0, c.store.subscribers.size)
        val collected = ArrayList<Int>()
        flow.take(2).collect {
            collected += (it as JsonArray).size
            if (collected.size == 1) c.mutate(Transaction(listOf(ins("b"))))
        }
        assertEquals(listOf(1, 2), collected)
        assertEquals(0, c.store.subscribers.size)
    }

    @Test
    fun presenceJoinUpdateLeaveAndExpire() {
        val rooms = PresenceRooms()
        var clock = 100L
        val a = InMemoryClient(InMemoryOptions(now = { clock }, connectionId = "a", presenceRooms = rooms))
        val b = InMemoryClient(InMemoryOptions(now = { clock }, connectionId = "b", presenceRooms = rooms, presenceUser = AuthedUser(UserKind.User, name = "Bee")))
        val aSeen = ArrayList<Int>()
        a.joinPresence("room", jsonStr("editing")) { aSeen += it.size }
        assertEquals(listOf(1), aSeen)
        b.joinPresence("room", jsonStr("viewing"))
        assertEquals(listOf(1, 2), aSeen)
        assertEquals(listOf("a", "b"), a.presenceSnapshot("room").map { it.connectionId })
        b.updatePresence("room", jsonStr("away"), 50)
        assertEquals("away", (a.presenceSnapshot("room")[1].state as JsonPrimitive).content)
        assertEquals(false, rooms.expire(149))
        assertEquals(true, rooms.expire(150))
        assertEquals(JsonNull, a.presenceSnapshot("room")[1].state)
        assertEquals(2, a.presenceSnapshot("room").size)
        b.leavePresence("room")
        assertEquals(listOf("a"), a.presenceSnapshot("room").map { it.connectionId })
        a.leavePresence("room")
        val before = aSeen.size
        b.joinPresence("room", jsonStr("x"))
        assertEquals(before, aSeen.size)
        // Update by a non-member is a no-op.
        a.updatePresence("room", jsonStr("ghost"))
        assertEquals(1, a.presenceSnapshot("room").size)
    }

    private fun migrateClient(): InMemoryClient {
        var salt = 0.0
        val c = InMemoryClient(InMemoryOptions(now = { 1_000L }, random = { salt = (salt + 0.0371) % 1.0; salt }))
        c.pushSchema(j("""{"tables":{"users":{"fields":{"name":{"type":"string"},"age":{"type":"string"},"nick":{"type":"optional","inner":{"type":"string"}}}}}}"""))
        c.mutate(Transaction(listOf(Step.Insert("users", j("""{"name":"Ada","age":"36"}""")), Step.Insert("users", j("""{"name":"Bo","age":"x"}""")))))
        return c
    }

    private fun userField(c: InMemoryClient, field: String): List<String?> =
        (c.query(Query(table = "users")) as JsonArray).map { (it as JsonObject)[field]?.let { v -> (v as? JsonPrimitive)?.content } }

    @Test
    fun migrationRenameDropSetDefaultChangeType() {
        val c = migrateClient()
        val r =
            c.applyMigration(
                listOf(
                    Directive.RenameField("users", "name", "fullName"),
                    Directive.SetDefault("users", "nick", jsonStr("anon")),
                    Directive.ChangeType("users", "age", j("""{"type":"number"}"""), Cast.ToNumber, default = j("0")),
                ),
                dryRun = false,
            )
        assertTrue(r.applied)
        assertEquals(listOf(2L, 2L, 2L), r.directives.map { it.affectedRows })
        assertEquals(setOf("Ada", "Bo"), userField(c, "fullName").toSet())
        assertEquals(listOf("anon", "anon"), userField(c, "nick"))
        assertEquals(setOf("36", "0"), userField(c, "age").toSet())
        c.applyMigration(listOf(Directive.DropField("users", "nick")), dryRun = false)
        assertEquals(listOf(null, null), userField(c, "nick"))
    }

    @Test
    fun migrationDryRunRollsBackAndFailureIsAtomic() {
        val c = migrateClient()
        val dry = c.applyMigration(listOf(Directive.RenameField("users", "name", "fullName")), dryRun = true)
        assertEquals(false, dry.applied)
        assertEquals(2L, dry.directives.single().affectedRows)
        assertEquals(setOf("Ada", "Bo"), userField(c, "name").toSet())
        assertEquals(null, c.store.tables["users"]!!.fields["fullName"])
        // changeType without a default cannot coerce "x": the earlier rename must not stick.
        assertCode(ErrorCode.BAD_REQUEST) {
            c.applyMigration(
                listOf(
                    Directive.RenameField("users", "name", "fullName"),
                    Directive.ChangeType("users", "age", j("""{"type":"number"}"""), Cast.ToNumber),
                ),
                dryRun = false,
            )
        }
        assertEquals(setOf("Ada", "Bo"), userField(c, "name").toSet())
        assertNull(c.store.tables["users"]!!.fields["fullName"])
    }

    @Test
    fun migrationEvalExprIsUnsupportedInMemory() {
        val c = migrateClient()
        assertCode(ErrorCode.BAD_REQUEST) {
            c.applyMigration(listOf(Directive.EvalExpr("users", "age", dev.parrtdb.admin.ExprSource(legacy = "1"))), dryRun = false)
        }
    }

    @Test
    fun migrationNotifiesSubscribersOfTouchedTables() {
        val c = migrateClient()
        val seen = ArrayList<JsonArray>()
        c.subscribe(Query(table = "users")) { seen += it as JsonArray }
        c.applyMigration(listOf(Directive.SetDefault("users", "nick", jsonStr("anon"))), dryRun = false)
        assertEquals(2, seen.size)
        assertEquals("anon", str(seen[1][0] as JsonObject, "nick"))
    }
}
