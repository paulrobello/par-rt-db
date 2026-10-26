package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.Step
import dev.parrtdb.wire.StepResult
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.WireJson
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/** Shared helpers for the Agent C write-path, scheduler, presence and migration tests. */
internal object CSupport {
    fun j(text: String): JsonElement = WireJson.parse(text)

    fun obj(text: String): JsonObject = j(text) as JsonObject

    /** Post-increment clock from the corpus runner's base, constant-0.0 random. */
    fun deterministicStore(schemaJson: String): Store {
        val s = Store()
        var t = 1_700_000_000_000L
        s.withClock { t++ }
        s.withRandom { 0.0 }
        s.pushSchema(j(schemaJson))
        return s
    }

    fun clockStore(schemaJson: String, clock: () -> Long): Store {
        val s = Store()
        s.withClock(clock)
        s.withRandom { 0.5 }
        s.pushSchema(j(schemaJson))
        return s
    }

    fun txn(vararg steps: Step) = Transaction(steps.toList())

    fun run(s: Store, vararg steps: Step, key: String? = null): List<StepResult> = applyTxn(s, txn(*steps), key)

    fun insert(s: Store, table: String, doc: String): String = run(s, Step.Insert(table, j(doc)))[0].id!!

    fun get(s: Store, table: String, id: String): JsonObject? =
        s.docs[RowKey(table, id)]?.takeIf { it.deletedAt == null }?.let { mergeDoc(it) }

    fun liveCount(s: Store, table: String): Int = s.docs.entries.count { it.key.table == table && it.value.deletedAt == null }

    fun code(block: () -> Unit): ErrorCode = assertFailsWith<RtDbException> { block() }.code

    fun assertCode(expected: ErrorCode, block: () -> Unit) = assertEquals(expected, code(block))

    fun str(o: JsonObject, k: String): String = (o[k] as JsonPrimitive).content
}
