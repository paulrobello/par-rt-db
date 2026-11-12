package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.WireJson
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Port of the schema-validation error-path suites from ts/rust clients (schema.test.ts validation
 * subset, rust in-memory validate tests): TTL, updatedAt, autoIncrement, computed, index, and
 * onDelete push rules, plus the QA-006 `requireNotNull` guards in SchemaValidate.kt invoked
 * directly through reflection (validateSchema only calls them when the field is non-null, so the
 * guards are otherwise unreachable).
 */
class DSchemaValidationTest {
    private fun parse(json: String): SchemaDef = parseSchema(WireJson.parse(json))

    private fun validate(j: String): Unit = validateSchema(parse(j))

    private fun violation(j: String): RtDbException = assertFailsWith<RtDbException> { validate(j) }

    private fun code(j: String): ErrorCode = violation(j).code

    // --- ttl rules ---

    @Test
    fun ttlFieldMustBeDeclared() {
        assertEquals(ErrorCode.SCHEMA_VIOLATION, code("""{"tables":{"t":{"fields":{"n":{"type":"number"}},"ttl":{"field":"zz"}}}}"""))
    }

    @Test
    fun ttlFieldMustBeNumberOrInt64() {
        val e = violation("""{"tables":{"t":{"fields":{"n":{"type":"string"}},"ttl":{"field":"n"}}}}""")
        assertEquals(ErrorCode.SCHEMA_VIOLATION, e.code)
        assertTrue(e.message.contains("must be a number or bigint field"))
    }

    @Test
    fun ttlFieldRequiresASinglePlainBtreeIndex() {
        val e =
            violation(
                """{"tables":{"t":{"fields":{"n":{"type":"number"}},"indexes":[{"name":"i","fields":["n"],"unique":true}],"ttl":{"field":"n"}}}}""",
            )
        assertEquals(ErrorCode.SCHEMA_VIOLATION, e.code)
        assertTrue(e.message.contains("requires a single-field, non-unique, non-partial btree index"))
    }

    @Test
    fun ttlDefaultDurationMustBePositive() {
        assertEquals(
            ErrorCode.SCHEMA_VIOLATION,
            code(
                """{"tables":{"t":{"fields":{"n":{"type":"number"}},"indexes":[{"name":"i","fields":["n"]}],"ttl":{"field":"n","defaultDurationMs":0}}}}""",
            ),
        )
    }

    // --- updatedAtField rules ---

    @Test
    fun updatedAtFieldMustBeDeclared() {
        assertEquals(ErrorCode.SCHEMA_VIOLATION, code("""{"tables":{"t":{"fields":{"n":{"type":"number"}},"updatedAtField":"zz"}}}"""))
    }

    @Test
    fun updatedAtFieldMustBeNumberOrInt64() {
        assertEquals(
            ErrorCode.SCHEMA_VIOLATION,
            code("""{"tables":{"t":{"fields":{"n":{"type":"boolean"}},"updatedAtField":"n"}}}"""),
        )
    }

    @Test
    fun updatedAtFieldMustDifferFromTtlField() {
        val e =
            violation(
                """{"tables":{"t":{"fields":{"n":{"type":"number"}},"indexes":[{"name":"i","fields":["n"]}],"ttl":{"field":"n"},"updatedAtField":"n"}}}""",
            )
        assertEquals(ErrorCode.SCHEMA_VIOLATION, e.code)
        assertTrue(e.message.contains("must differ from ttl.field"))
    }

    // --- autoIncrementField rules ---

    @Test
    fun autoIncrementFieldMustBeDeclared() {
        assertEquals(ErrorCode.SCHEMA_VIOLATION, code("""{"tables":{"t":{"fields":{"n":{"type":"number"}},"autoIncrementField":"zz"}}}"""))
    }

    @Test
    fun autoIncrementFieldMustBeInt64() {
        val e = violation("""{"tables":{"t":{"fields":{"n":{"type":"number"}},"autoIncrementField":"n"}}}""")
        assertEquals(ErrorCode.SCHEMA_VIOLATION, e.code)
        assertTrue(e.message.contains("must be an int64 field"))
    }

    @Test
    fun autoIncrementFieldMustDifferFromTtlAndUpdatedAt() {
        assertEquals(
            ErrorCode.SCHEMA_VIOLATION,
            code(
                """{"tables":{"t":{"fields":{"n":{"type":"int64"}},"indexes":[{"name":"i","fields":["n"]}],"ttl":{"field":"n"},"autoIncrementField":"n"}}}""",
            ),
        )
        assertEquals(
            ErrorCode.SCHEMA_VIOLATION,
            code(
                """{"tables":{"t":{"fields":{"n":{"type":"int64"},"u":{"type":"number"}},"updatedAtField":"u","autoIncrementField":"u"}}}""",
            ),
        )
    }

    // --- computed rules ---

    @Test
    fun computedFieldMustBeDeclaredAndNotACollisionTarget() {
        assertEquals(
            ErrorCode.BAD_REQUEST,
            code("""{"tables":{"t":{"fields":{"n":{"type":"number"}},"computed":{"zz":{"op":"field","field":"n"}}}}}"""),
        )
        assertEquals(
            ErrorCode.BAD_REQUEST,
            code("""{"tables":{"t":{"fields":{"n":{"type":"number"}},"ownerField":"n","computed":{"n":{"op":"literal","value":"x"}}}}}"""),
        )
        assertEquals(
            ErrorCode.BAD_REQUEST,
            code("""{"tables":{"t":{"fields":{"n":{"type":"int64"}},"autoIncrementField":"n","computed":{"n":{"op":"literal","value":"x"}}}}}"""),
        )
    }

    @Test
    fun computedFieldMustNotReferenceUndeclaredOrComputedFields() {
        assertEquals(
            ErrorCode.BAD_REQUEST,
            code("""{"tables":{"t":{"fields":{"n":{"type":"number"}},"computed":{"c":{"op":"field","field":"zz"}}}}}"""),
        )
        assertEquals(
            ErrorCode.BAD_REQUEST,
            code(
                """{"tables":{"t":{"fields":{"n":{"type":"number"},"a":{"type":"string"},"b":{"type":"string"}},
                   "computed":{"a":{"op":"literal","value":"x"},"b":{"op":"field","field":"a"}}}}}""".replace("\n", ""),
            ),
        )
    }

    @Test
    fun authorizePredicateMustNotReferenceComputedFields() {
        val e =
            violation(
                """{"tables":{"t":{"fields":{"n":{"type":"number"},"c":{"type":"string"}},"computed":{"c":{"op":"literal","value":"x"}},
                   "authorize":{"op":"eq","field":"c","value":"x"}}}}""".replace("\n", ""),
            )
        assertEquals(ErrorCode.BAD_REQUEST, e.code)
        assertTrue(e.message.contains("must not be referenced by the table's authorize predicate"))
    }

    @Test
    fun computedStaticKindMustMatchDeclaredFieldType() {
        // concat produces a string; a number field cannot accept it.
        assertEquals(
            ErrorCode.BAD_REQUEST,
            code(
                """{"tables":{"t":{"fields":{"n":{"type":"number"},"s":{"type":"string"}},"computed":{"n":{"op":"concat","parts":[{"op":"field","field":"s"},{"op":"literal","value":"!"}]}}}}}""",
            ),
        )
    }

    // --- index and authorize rules ---

    @Test
    fun indexRules() {
        assertEquals(
            ErrorCode.SCHEMA_VIOLATION,
            code("""{"tables":{"t":{"fields":{"n":{"type":"string"}},"indexes":[{"name":"i","fields":[]}]}}}"""),
        )
        assertEquals(
            ErrorCode.SCHEMA_VIOLATION,
            code("""{"tables":{"t":{"fields":{"n":{"type":"string"}},"indexes":[{"name":"i","fields":["zz"]}]}}}"""),
        )
        assertEquals(
            ErrorCode.SCHEMA_VIOLATION,
            code("""{"tables":{"t":{"fields":{"n":{"type":"number"}},"indexes":[{"name":"i","fields":["n"],"search":true}]}}}"""),
        )
    }

    @Test
    fun olderThanIsRejectedInAuthorizeAndPartialIndexPredicates() {
        val e =
            violation(
                """{"tables":{"t":{"fields":{"n":{"type":"number"}},"authorize":{"op":"olderThan","field":"n","ms":1000}}}}""",
            )
        assertEquals(ErrorCode.SCHEMA_VIOLATION, e.code)
        assertEquals(
            ErrorCode.BAD_REQUEST,
            code(
                """{"tables":{"t":{"fields":{"n":{"type":"number"},"done":{"type":"boolean"}},"indexes":[{"name":"i","fields":["n"],"where":{"op":"olderThan","field":"n","ms":1000}}]}}}""",
            ),
        )
    }

    // --- onDelete rules ---

    @Test
    fun onDeleteRules() {
        fun vCode(j: String): ErrorCode = assertFailsWith<RtDbException> { validateOnDelete(parse(j)) }.code
        // onDelete nested inside an object field is rejected.
        assertEquals(
            ErrorCode.SCHEMA_VIOLATION,
            vCode("""{"tables":{"t":{"fields":{"o":{"type":"object","fields":{"p":{"type":"id","table":"u","onDelete":"cascade"}}}}}}}"""),
        )
        // setNull requires an optional id field.
        assertEquals(
            ErrorCode.SCHEMA_VIOLATION,
            vCode("""{"tables":{"t":{"fields":{"p":{"type":"id","table":"u","onDelete":"setNull"}}}}}"""),
        )
        // a declared onDelete field requires an index.
        assertEquals(
            ErrorCode.SCHEMA_VIOLATION,
            vCode("""{"tables":{"t":{"fields":{"p":{"type":"id","table":"u","onDelete":"cascade"}}}}}"""),
        )
        // an unknown referenced table is rejected.
        assertEquals(
            ErrorCode.SCHEMA_VIOLATION,
            vCode("""{"tables":{"t":{"fields":{"p":{"type":"id","table":"u","onDelete":"cascade"}},"indexes":[{"name":"i","fields":["p"]}]}}}"""),
        )
    }

    @Test
    fun validateOnDeleteAcceptsLegalShapes() {
        validateOnDelete(
            parse(
                """{"tables":{"u":{"fields":{"title":{"type":"string"}}},
                   "c":{"fields":{"p":{"type":"optional","inner":{"type":"id","table":"u","onDelete":"setNull"}}},"indexes":[{"name":"i","fields":["p"]}]}}}""".replace(
                    "\n",
                    "",
                ),
            ),
        )
    }

    // --- QA-006 requireNotNull guards (SchemaValidate.kt) ---

    private fun guardMessage(function: String, table: TableDef): String {
        val m = Class.forName("dev.parrtdb.inmemory.SchemaValidateKt").getDeclaredMethod(function, TableDef::class.java)
        m.isAccessible = true
        try {
            m.invoke(null, table)
            throw IllegalStateException("guard did not fire: $function")
        } catch (e: java.lang.reflect.InvocationTargetException) {
            val cause = e.cause ?: throw e
            return assertFailsWith<IllegalArgumentException> { throw cause }.message ?: ""
        }
    }

    @Test
    fun ttlGuardThrowsWithHouseMessage() {
        val table = TableDef(fields = mapOf("a" to FieldType("number")), ttl = null)
        assertEquals("table.ttl required for ttl validation", guardMessage("validateTtl", table))
    }

    @Test
    fun updatedAtGuardThrowsWithHouseMessage() {
        val table = TableDef(fields = mapOf("a" to FieldType("number")), updatedAtField = null)
        assertEquals("table.updatedAtField required for updatedAtField validation", guardMessage("validateUpdatedAtField", table))
    }

    @Test
    fun autoIncrementGuardThrowsWithHouseMessage() {
        val table = TableDef(fields = mapOf("a" to FieldType("number")), autoIncrementField = null)
        assertEquals("table.autoIncrementField required for autoIncrementField validation", guardMessage("validateAutoIncrementField", table))
    }
}
