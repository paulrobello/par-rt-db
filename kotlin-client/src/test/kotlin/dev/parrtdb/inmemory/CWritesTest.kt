package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.inmemory.CSupport.assertCode
import dev.parrtdb.inmemory.CSupport.deterministicStore
import dev.parrtdb.inmemory.CSupport.get
import dev.parrtdb.inmemory.CSupport.insert
import dev.parrtdb.inmemory.CSupport.j
import dev.parrtdb.inmemory.CSupport.liveCount
import dev.parrtdb.inmemory.CSupport.obj
import dev.parrtdb.inmemory.CSupport.run
import dev.parrtdb.inmemory.CSupport.txn
import dev.parrtdb.wire.FieldExpectation
import dev.parrtdb.wire.FilterExpr
import dev.parrtdb.wire.Step
import dev.parrtdb.wire.jsonStr
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Write-path tests driving applyTxn over a hand-pushed schema. They need Agent A's pushSchema,
 * validators and filter matcher; Agent B's evalQuery is only used by the subscription tests.
 */
class CWritesTest {
    private val schema =
        """{"tables":{
          "authors":{"fields":{"name":{"type":"string"}}},
          "books":{"fields":{"title":{"type":"string"},"author":{"type":"id","table":"authors","onDelete":"cascade"},"n":{"type":"number"}},
                   "indexes":[{"name":"by_title","fields":["title"],"unique":true},{"name":"by_author","fields":["author"]}]},
          "notes":{"fields":{"body":{"type":"string"},"author":{"type":"optional","inner":{"type":"id","table":"authors","onDelete":"setNull"}}},"indexes":[{"name":"by_author","fields":["author"]}]},
          "locks":{"fields":{"who":{"type":"id","table":"authors","onDelete":"restrict"}},"indexes":[{"name":"by_who","fields":["who"]}]},
          "trash":{"fields":{"v":{"type":"string"}},"softDelete":true,"indexes":[{"name":"by_v","fields":["v"],"unique":true}]},
          "counters":{"fields":{"n":{"type":"number"},"tag":{"type":"string"}}}
        }}"""

    private fun store(): Store = deterministicStore(schema)

    @Test
    fun insertMintsDeterministicIdAndSystemFields() {
        val s = store()
        val id = insert(s, "authors", """{"name":"Ada"}""")
        assertEquals(32, id.length)
        // First id: clock reads 1_700_000_000_000 for stampers/ids; random is constant 0.0 so the tail is all zeros.
        assertTrue(id.substring(12) == "7" + "0".repeat(19))
        val doc = get(s, "authors", id)!!
        assertEquals("Ada", (doc["name"] as JsonPrimitive).content)
        assertEquals(id, (doc["_id"] as JsonPrimitive).content)
        assertEquals("1", (doc["_version"] as JsonPrimitive).content)
    }

    @Test
    fun patchReplaceDeleteAndExpectSteps() {
        val s = store()
        val id = insert(s, "counters", """{"n":1,"tag":"a"}""")
        run(s, Step.Patch("counters", id, j("""{"n":2}""")))
        assertEquals("2", (get(s, "counters", id)!!["_version"] as JsonPrimitive).content)
        run(s, Step.ExpectVersion("counters", id, 2))
        assertCode(ErrorCode.PRECONDITION_FAILED) { run(s, Step.ExpectVersion("counters", id, 9)) }
        run(s, Step.Replace("counters", id, j("""{"n":5,"tag":"z"}""")))
        assertEquals("z", (get(s, "counters", id)!!["tag"] as JsonPrimitive).content)
        run(s, Step.ExpectFields("counters", id, listOf(FieldExpectation(listOf("n"), j("5")))))
        assertCode(ErrorCode.PRECONDITION_FAILED) {
            run(s, Step.ExpectFields("counters", id, listOf(FieldExpectation(listOf("n"), j("6")))))
        }
        assertCode(ErrorCode.SCHEMA_VIOLATION) {
            run(s, Step.ExpectFields("counters", id, listOf(FieldExpectation(listOf("ghost"), j("6")))))
        }
        run(s, Step.Delete("counters", id))
        assertNull(get(s, "counters", id))
        assertCode(ErrorCode.NOT_FOUND) { run(s, Step.Patch("counters", id, j("""{"n":2}"""))) }
    }

    @Test
    fun failedStepRollsBackWholeTransaction() {
        val s = store()
        assertCode(ErrorCode.NOT_FOUND) {
            run(s, Step.Insert("authors", j("""{"name":"x"}""")), Step.Patch("authors", "nope", j("""{"name":"y"}""")))
        }
        assertEquals(0, liveCount(s, "authors"))
    }

    @Test
    fun upsertInsertsThenPatches() {
        val s = store()
        val first = run(s, Step.Upsert("books", "by_title", listOf(jsonStr("T")), j("""{"title":"T","author":"${authorId(s)}","n":1}"""), j("""{"n":2}"""))).single()
        assertEquals(true, first.inserted)
        val second = run(s, Step.Upsert("books", "by_title", listOf(jsonStr("T")), j("{}"), j("""{"n":2}"""))).single()
        assertEquals(false, second.inserted)
        assertEquals(first.id, second.id)
        assertEquals(1, liveCount(s, "books"))
    }

    @Test
    fun upsertManyReportsPerDocFlagsAndUniqueConflicts() {
        val s = store()
        val a = authorId(s)
        val r = run(s, Step.UpsertMany("books", "by_title", listOf(j("""{"title":"A","author":"$a","n":1}"""), j("""{"title":"A","author":"$a","n":2}""")))).single()
        assertEquals(listOf(true, false), r.insertedMany)
        assertCode(ErrorCode.CONFLICT) {
            run(s, Step.Insert("books", j("""{"title":"A","author":"$a","n":3}""")))
        }
    }

    @Test
    fun patchByQueryAndDeleteByQueryHonourLimitAndTruncation() {
        val s = store()
        repeat(5) { insert(s, "counters", """{"n":$it,"tag":"t"}""") }
        val r = run(s, Step.PatchByQuery("counters", FilterExpr.Eq("tag", jsonStr("t")), j("""{"tag":"u"}"""), limit = 2)).single()
        assertEquals(2L, r.patched)
        assertEquals(true, r.truncated)
        val d = run(s, Step.DeleteByQuery("counters", FilterExpr.Eq("tag", jsonStr("u")))).single()
        assertEquals(2L, d.deleted)
        assertEquals(false, d.truncated)
        assertEquals(3, liveCount(s, "counters"))
        assertCode(ErrorCode.BAD_REQUEST) { run(s, Step.DeleteByQuery("counters", FilterExpr.Eq("tag", jsonStr("t")), limit = -1)) }
    }

    @Test
    fun byQueryCapsRejectOversizedTransactions() {
        val s = store()
        val steps = (0..16).map { Step.PatchByQuery("counters", FilterExpr.Eq("tag", jsonStr("t")), j("""{"tag":"u"}"""), 1) }
        assertCode(ErrorCode.BAD_REQUEST) { applyTxn(s, txn(*steps.toTypedArray()), null) }
        val docs = (0..1000).map { j("""{"n":1,"tag":"t"}""") }
        assertCode(ErrorCode.BAD_REQUEST) { run(s, Step.InsertMany("counters", docs)) }
    }

    @Test
    fun adjustCounterAppliesDeltaAndChecksBounds() {
        val s = store()
        val id = insert(s, "counters", """{"n":5,"tag":"a"}""")
        run(s, Step.AdjustCounter("counters", id, "n", 3, min = 0, max = 10))
        assertEquals("8", (get(s, "counters", id)!!["n"] as JsonPrimitive).content)
        assertCode(ErrorCode.PRECONDITION_FAILED) { run(s, Step.AdjustCounter("counters", id, "n", 5, max = 10)) }
        assertCode(ErrorCode.PRECONDITION_FAILED) { run(s, Step.AdjustCounter("counters", id, "n", 1, expected = obj("""{"tag":"zzz"}"""))) }
        assertCode(ErrorCode.SCHEMA_VIOLATION) { run(s, Step.AdjustCounter("counters", id, "tag", 1)) }
        assertCode(ErrorCode.BAD_REQUEST) { run(s, Step.AdjustCounter("counters", id, "n", 1, min = 5, max = 1)) }
    }

    @Test
    fun cascadeRestrictAndSetNull() {
        val s = store()
        val a = authorId(s)
        val book = insert(s, "books", """{"title":"B","author":"$a","n":1}""")
        val note = insert(s, "notes", """{"body":"x","author":"$a"}""")
        run(s, Step.Delete("authors", a))
        assertNull(get(s, "books", book))
        val cleared = get(s, "notes", note)!!["author"]
        assertTrue(cleared == null || cleared is JsonNull) // patch(null) on an optional clears the field

        val b = authorId(s)
        insert(s, "locks", """{"who":"$b"}""")
        assertCode(ErrorCode.CONFLICT) { run(s, Step.Delete("authors", b)) }
        assertNotNull(get(s, "authors", b))
    }

    @Test
    fun softDeleteHidesRowAndUndeleteRestoresIt() {
        val s = store()
        val id = insert(s, "trash", """{"v":"one"}""")
        run(s, Step.Delete("trash", id))
        assertNull(get(s, "trash", id))
        assertCode(ErrorCode.NOT_FOUND) { run(s, Step.Delete("trash", id)) }
        // Unique key is free while soft deleted, and undelete re-applies it.
        insert(s, "trash", """{"v":"one"}""")
        assertCode(ErrorCode.CONFLICT) { run(s, Step.Undelete("trash", id)) }
        assertCode(ErrorCode.BAD_REQUEST) { run(s, Step.Undelete("counters", id)) }
    }

    @Test
    fun expectAbsentAndExpectNoMatch() {
        val s = store()
        val a = authorId(s)
        run(s, Step.ExpectAbsent("books", "by_title", listOf(jsonStr("Q"))))
        insert(s, "books", """{"title":"Q","author":"$a","n":1}""")
        assertCode(ErrorCode.PRECONDITION_FAILED) { run(s, Step.ExpectAbsent("books", "by_title", listOf(jsonStr("Q")))) }
        assertCode(ErrorCode.PRECONDITION_FAILED) { run(s, Step.ExpectNoMatch("books", FilterExpr.Eq("title", jsonStr("Q")))) }
        run(s, Step.ExpectNoMatch("books", FilterExpr.Eq("title", jsonStr("other"))))
    }

    @Test
    fun idempotencyKeyReplaysCachedResultsWithoutReexecuting() {
        val s = store()
        val first = run(s, Step.Insert("authors", j("""{"name":"A"}""")), key = "k1")
        val again = run(s, Step.Insert("authors", j("""{"name":"A"}""")), key = "k1")
        assertEquals(first, again)
        assertEquals(1, liveCount(s, "authors"))
    }

    @Test
    fun readOnlyFreezeRejectsWritesButNotReplays() {
        val s = store()
        run(s, Step.Insert("authors", j("""{"name":"A"}""")), key = "k1")
        s.setReadOnly(true)
        assertCode(ErrorCode.READ_ONLY) { run(s, Step.Insert("authors", j("""{"name":"B"}"""))) }
        run(s, Step.Insert("authors", j("""{"name":"A"}""")), key = "k1")
        s.setReadOnly(false)
        run(s, Step.Insert("authors", j("""{"name":"B"}""")))
        assertEquals(2, liveCount(s, "authors"))
    }

    @Test
    fun expectBeforeDeadlineIsEnforced() {
        val s = store()
        run(s, Step.ExpectBefore(9_007_199_254_740_991L))
        assertCode(ErrorCode.PRECONDITION_FAILED) { run(s, Step.ExpectBefore(1)) }
        assertCode(ErrorCode.BAD_REQUEST) { run(s, Step.ExpectBefore(-1)) }
    }

    @Test
    fun workflowStepsAreRejected() {
        val s = store()
        assertCode(ErrorCode.INTERNAL) { run(s, Step.CancelWorkflow("x")) }
    }

    @Test
    fun diffCanonicalStripsVersionForProjectedQueries() {
        val q = dev.parrtdb.wire.Query(table = "t", fields = listOf("a"))
        val a = diffCanonical(j("""[{"a":1,"_version":1}]"""), q)
        val b = diffCanonical(j("""[{"a":1,"_version":2}]"""), q)
        assertEquals(a, b)
        val plain = dev.parrtdb.wire.Query(table = "t")
        assertFalse(diffCanonical(j("""[{"a":1,"_version":1}]"""), plain) == diffCanonical(j("""[{"a":1,"_version":2}]"""), plain))
    }

    private fun authorId(s: Store): String = insert(s, "authors", """{"name":"n"}""")
}
