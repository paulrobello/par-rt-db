package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.Cast
import dev.parrtdb.wire.Step
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.ValueExpr
import dev.parrtdb.wire.WireJson
import dev.parrtdb.wire.jsonStr
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * In-memory engine error paths (ported from ts errors.test.ts / rust in-memory error suites):
 * evalValueExpr edge behavior, migration coercion rejections, and bulk-write error paths.
 *
 * QA-006 guard coverage note: of the eight `requireNotNull` sites, the three in SchemaValidate.kt
 * get direct reflection tests in [DSchemaValidationTest]; the Expr.kt pair above is pinned at its
 * reachable behavior; the Writes.kt pair (upsertMany id/inserted) and the Filter.kt leafOf guard
 * are unreachable for ANY input to their enclosing functions — executeUpsert always sets id and
 * inserted, and leafOf is non-null exactly when the calling branch matches — so no test can fire
 * them short of bytecode manipulation.
 */
class DErrorPathTest {
    private fun j(s: String) = WireJson.parse(s)

    private fun obj(s: String) = j(s) as kotlinx.serialization.json.JsonObject

    private fun code(block: () -> Unit): ErrorCode = assertFailsWith<RtDbException> { block() }.code

    private fun ev(expr: String, doc: String = "{}"): String =
        WireJson.encode(evalValueExpr(ValueExpr.fromJson(j(expr)), obj(doc), 42, emptyMap()))

    // --- QA-006 guard reachability (Expr.kt lines 320-321) ---
    //
    // The two `requireNotNull` guards in evalValueExpr's arith() are provably unreachable:
    // NumRes(null, null) IS nullProp (the property is defined as value==null && err==null),
    // so the SQL-NULL propagation check immediately above the guards intercepts every input
    // that would otherwise reach them. The tests below pin the reachable behavior of that
    // path — non-finite numbers propagate SQL NULL — which is the contract the guards backstop.

    @Test
    fun nonFiniteNumberLiteralPropagatesSqlNullOnBothSides() {
        // A 1e400 JSON number literal overflows double to +Infinity -> NumRes(null, null).
        assertEquals("null", ev("""{"op":"add","left":{"op":"literal","value":1e400},"right":{"op":"literal","value":1}}"""))
        assertEquals("null", ev("""{"op":"add","left":{"op":"literal","value":1},"right":{"op":"literal","value":1e400}}"""))
    }

    @Test
    fun outOfRangeNumberStringIsABadRequestNotAnOverflow() {
        // Field reads extract text, and the strict float parse rejects out-of-range exponents:
        // the string path surfaces a proper BAD_REQUEST instead of null propagation.
        val e =
            assertFailsWith<RtDbException> {
                ev("""{"op":"mul","left":{"op":"field","field":"x"},"right":{"op":"literal","value":2}}""", """{"x":"1e400"}""")
            }
        assertEquals(ErrorCode.BAD_REQUEST, e.code)
        assertTrue(e.message.contains("cannot cast '1e400' to number"))
    }

    // --- evalValueExpr error paths not covered by AValueTest ---

    @Test
    fun arithmeticOnNonNumericStringIsRejected() {
        val e =
            assertFailsWith<RtDbException> {
                ev("""{"op":"add","left":{"op":"field","field":"s"},"right":{"op":"literal","value":1}}""", """{"s":"abc"}""")
            }
        assertEquals(ErrorCode.BAD_REQUEST, e.code)
        assertTrue(e.message.contains("cannot cast 'abc' to number"))
    }

    @Test
    fun castToBooleanOfObjectIsRejected() {
        val e =
            assertFailsWith<RtDbException> {
                ev("""{"op":"cast","to":"toBoolean","value":{"op":"field","field":"o"}}""", """{"o":{"a":1}}""")
            }
        assertEquals(ErrorCode.BAD_REQUEST, e.code)
        assertTrue(e.message.contains("cannot cast") && e.message.contains("to boolean"))
    }

    // --- migration coercion (Migrate.kt coerceValue) ---

    @Test
    fun coerceValueRejections() {
        assertNull(coerceValue(Cast.ToNumber, jsonStr("abc")))
        assertNull(coerceValue(Cast.ToNumber, j("""{"a":1}""")))
        assertNull(coerceValue(Cast.ToInt64, jsonStr("1.5")))
        assertNull(coerceValue(Cast.ToInt64, j("""[1]""")))
        assertNull(coerceValue(Cast.ToBoolean, j("""{"a":1}""")))
        assertEquals("1", WireJson.encode(coerceValue(Cast.ToNumber, jsonStr(" 1 "))!!))
    }

    // --- bulk-write error paths (Writes.kt) ---

    @Test
    fun upsertManyDocMissingIndexedFieldIsRejected() {
        val c =
            InMemoryClient(InMemoryOptions(now = { 1_700_000_000_000L }, random = { 0.0 })).also {
                it.pushSchema(
                    j(
                        """{"tables":{"tasks":{"fields":{"name":{"type":"string"},"score":{"type":"number"}},
                           "indexes":[{"name":"by_name","fields":["name"]}]}}}""",
                    ),
                )
            }
        val e =
            assertFailsWith<RtDbException> {
                c.mutate(
                    Transaction(
                        listOf(
                            Step.UpsertMany(
                                "tasks",
                                "by_name",
                                listOf(j("""{"score":1}""")),
                            ),
                        ),
                    ),
                )
            }
        assertEquals(ErrorCode.BAD_REQUEST, e.code)
        assertTrue(e.message.contains("is missing indexed field 'name'"))
    }

    @Test
    fun upsertManyReportsPerDocFlagsAndIds() {
        val c =
            InMemoryClient(InMemoryOptions(now = { 1_700_000_000_000L }, random = { 0.0 })).also {
                it.pushSchema(
                    j(
                        """{"tables":{"tasks":{"fields":{"name":{"type":"string"},"score":{"type":"number"}},
                           "indexes":[{"name":"by_name","fields":["name"]}]}}}""",
                    ),
                )
            }
        val r =
            c.mutate(
                Transaction(
                    listOf(
                        Step.UpsertMany(
                            "tasks",
                            "by_name",
                            listOf(j("""{"name":"a","score":1}"""), j("""{"name":"a","score":2}""")),
                        ),
                    ),
                ),
            )[0]
        assertEquals(listOf(true, false), r.insertedMany)
        assertEquals(2, r.ids!!.size)
        assertEquals(r.ids[0], r.ids[1])
    }
}
