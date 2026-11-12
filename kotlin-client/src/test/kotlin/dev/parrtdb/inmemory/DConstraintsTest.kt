package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.Step
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.WireJson
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Port of ts-client tests/field-constraints.test.ts: push-time and write-path constraint enforcement. */
class DConstraintsTest {
    private val schema =
        """{"tables":{"tasks":{"fields":{"name":{"type":"string"},"score":{"type":"number"},"counter":{"type":"int64"},"note":{"type":"optional","inner":{"type":"string"}}},
           "indexes":[{"name":"by_name","fields":["name"]}],
           "constraints":{"score":{"min":1,"max":10},"counter":{"min":1,"max":100},"name":{"minLength":2,"maxLength":5,"pattern":"^[a-z]+$"}}}}}"""

    private fun client(): InMemoryClient {
        val c = InMemoryClient(InMemoryOptions(now = { 1_700_000_000_000L }, random = { 0.0 }))
        c.pushSchema(WireJson.parse(schema))
        return c
    }

    private fun push(j: String) {
        val c = InMemoryClient(InMemoryOptions(now = { 1_700_000_000_000L }, random = { 0.0 }))
        c.pushSchema(WireJson.parse(j))
    }

    private fun violation(j: String): RtDbException =
        assertFailsWith<RtDbException> { push(j) }

    private fun ins(doc: String) = Step.Insert("tasks", WireJson.parse(doc))

    private fun docs(c: InMemoryClient): List<JsonObject> = (c.query(Query(table = "tasks")) as kotlinx.serialization.json.JsonArray).map { it as JsonObject }

    @Test
    fun pushTimeRejectsMisappliedConstraintNamingTheField() {
        val e =
            violation(
                """{"tables":{"tasks":{"fields":{"name":{"type":"string"}},"indexes":[{"name":"by_name","fields":["name"]}],"constraints":{"name":{"min":1}}}}}""",
            )
        assertEquals(ErrorCode.SCHEMA_VIOLATION, e.code)
        assertTrue(e.message.contains("'name'"))
        assertTrue(e.message.contains("number/int64"))
    }

    @Test
    fun pushTimeRejectsInvalidPattern() {
        val e =
            violation(
                """{"tables":{"tasks":{"fields":{"name":{"type":"string"}},"indexes":[{"name":"by_name","fields":["name"]}],"constraints":{"name":{"pattern":"([a-z"}}}}}""",
            )
        assertEquals(ErrorCode.SCHEMA_VIOLATION, e.code)
        assertTrue(e.message.contains("invalid pattern"))
    }

    @Test
    fun pushTimeRejectsInt64BoundsThatAreNotIntegers() {
        val e =
            violation(
                """{"tables":{"tasks":{"fields":{"counter":{"type":"int64"}},"constraints":{"counter":{"min":1.5}}}}}""",
            )
        assertEquals(ErrorCode.SCHEMA_VIOLATION, e.code)
        assertTrue(e.message.contains("must be an integer"))
    }

    @Test
    fun pushTimeRejectsConstraintsKeyThatIsNotADeclaredField() {
        val e =
            violation(
                """{"tables":{"tasks":{"fields":{"name":{"type":"string"}},"constraints":{"zz":{"minLength":1}}}}}""",
            )
        assertEquals(ErrorCode.SCHEMA_VIOLATION, e.code)
        assertTrue(e.message.contains("not a declared field"))
    }

    @Test
    fun pushTimeRejectsMinExceedingMaxAndLengthsInverted() {
        assertEquals(
            ErrorCode.SCHEMA_VIOLATION,
            violation("""{"tables":{"tasks":{"fields":{"n":{"type":"number"}},"constraints":{"n":{"min":5,"max":1}}}}}""").code,
        )
        assertEquals(
            ErrorCode.SCHEMA_VIOLATION,
            violation("""{"tables":{"tasks":{"fields":{"n":{"type":"string"}},"constraints":{"n":{"minLength":5,"maxLength":1}}}}}""").code,
        )
    }

    @Test
    fun pushTimeRejectsLengthBoundsOnNonStringField() {
        assertEquals(
            ErrorCode.SCHEMA_VIOLATION,
            violation("""{"tables":{"tasks":{"fields":{"n":{"type":"number"}},"constraints":{"n":{"minLength":1}}}}}""").code,
        )
    }

    @Test
    fun writePathAcceptsInclusiveBoundaryValues() {
        val c = client()
        c.mutate(
            Transaction(
                listOf(
                    ins("""{"name":"aa","score":1,"counter":"1"}"""),
                    ins("""{"name":"abcde","score":10,"counter":"100"}"""),
                ),
            ),
        )
        assertEquals(2, docs(c).size)
    }

    @Test
    fun writePathInsertBelowMinFailsAndRollsBackTheTransaction() {
        val c = client()
        val e =
            assertFailsWith<RtDbException> {
                c.mutate(
                    Transaction(
                        listOf(
                            ins("""{"name":"good","score":5,"counter":"1"}"""),
                            ins("""{"name":"bad","score":0.5,"counter":"1"}"""),
                        ),
                    ),
                )
            }
        assertEquals(ErrorCode.SCHEMA_VIOLATION, e.code)
        assertEquals(0, docs(c).size)
    }

    @Test
    fun writePathEnforcesLengthAndPatternOnStrings() {
        val c = client()
        fun bad(doc: String): String = assertFailsWith<RtDbException> { c.mutate(Transaction(listOf(ins(doc)))) }.message
        assertTrue(bad("""{"name":"a","score":5,"counter":"1"}""").contains("'minLength' constraint"))
        assertTrue(bad("""{"name":"abcdef","score":5,"counter":"1"}""").contains("'maxLength' constraint"))
        assertTrue(bad("""{"name":"ABC","score":5,"counter":"1"}""").contains("'pattern' constraint"))
    }

    @Test
    fun writePathEnforcesConstraintsThroughPatchReplaceUpsertAndPatchByQuery() {
        val c = client()
        val id = c.mutate(Transaction(listOf(ins("""{"name":"ok","score":5,"counter":"1"}"""))))[0].id!!
        fun bad(steps: List<Step>): String =
            assertFailsWith<RtDbException> { c.mutate(Transaction(steps)) }.message
        assertTrue(bad(listOf(Step.Patch("tasks", id, WireJson.parse("""{"score":50}""")))).contains("'max' constraint"))
        assertFailsWith<RtDbException> {
            c.mutate(Transaction(listOf(Step.Replace("tasks", id, WireJson.parse("""{"name":"ab","score":99,"counter":"2"}""")))))
        }
        assertTrue(
            bad(
                listOf(
                    Step.Upsert(
                        "tasks",
                        "by_name",
                        listOf(WireJson.parse("\"ok\"")),
                        WireJson.parse("""{"name":"ok","score":5,"counter":"1"}"""),
                        WireJson.parse("""{"counter":"101"}"""),
                    ),
                ),
            ).contains("'max' constraint"),
        )
        assertTrue(
            bad(
                listOf(
                    Step.PatchByQuery(
                        "tasks",
                        dev.parrtdb.wire.FilterExpr.fromJson(WireJson.parse("""{"op":"eq","field":"name","value":"ok"}""")),
                        WireJson.parse("""{"score":99}"""),
                    ),
                ),
            ).contains("'max' constraint"),
        )
        // Nothing survived: the stored doc is untouched.
        val stored = docs(c).single()
        assertEquals(id, (stored["_id"] as JsonPrimitive).content)
        assertEquals("1", (stored["counter"] as JsonPrimitive).content)
        assertEquals(5.0, (stored["score"] as JsonPrimitive).content.toDouble())
    }

    @Test
    fun nullNeverViolatesAConstraintAndAbsentOptionalStaysOptional() {
        val c = client()
        c.mutate(Transaction(listOf(ins("""{"name":"ok","score":5,"counter":"1","note":null}"""))))
        assertEquals(1, docs(c).size)
    }

    @Test
    fun stripUnsetOptionalsDropsNullValuesForOptionalsWhoseInnerRejectsNull() {
        val table =
            TableDef(
                fields =
                    mapOf(
                        "note" to FieldType("optional", inner = FieldType("string")),
                        "flag" to FieldType("boolean"),
                    ),
            )
        val stripped =
            stripUnsetOptionals(
                table,
                WireJson.parse("""{"note":null,"flag":null}""") as JsonObject,
            )
        assertEquals(setOf("flag"), stripped.keys)
    }
}
