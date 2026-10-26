package dev.parrtdb

import dev.parrtdb.admin.Directive
import dev.parrtdb.dsl.FieldType
import dev.parrtdb.dsl.Filters
import dev.parrtdb.dsl.defineSchema
import dev.parrtdb.dsl.mutation
import dev.parrtdb.dsl.obj
import dev.parrtdb.dsl.query
import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.inmemory.InMemoryClient
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Real DSL builders driving the in-memory engine end to end, including reactive subscriptions. */
class SmokeTest {
    private val schema =
        defineSchema {
            table("tasks") {
                field("title", FieldType.string())
                field("status", FieldType.string())
                field("points", FieldType.number())
                index("by_status", "status")
                index("by_title", "title")
                unique()
            }
        }

    private fun client(): InMemoryClient = InMemoryClient().also { it.pushSchema(schema) }

    private fun titles(r: JsonElement): List<String> = (r as JsonArray).map { ((it as JsonObject)["title"] as JsonPrimitive).content }

    @Test
    fun dslRoundTripThroughEngine() {
        val c = client()
        c.mutate(
            mutation()
                .insert("tasks", obj("title" to "a", "status" to "open", "points" to 3))
                .insert("tasks", obj("title" to "b", "status" to "open", "points" to 5))
                .insert("tasks", obj("title" to "c", "status" to "done", "points" to 1))
                .build(),
        )
        val open = c.query(query("tasks").withIndex("by_status", "open").build())
        assertEquals(setOf("a", "b"), titles(open).toSet())
        val count = c.query(query("tasks").withIndex("by_status", "open").count().build())
        assertEquals("2", (count as JsonPrimitive).content)
        val filtered = c.query(query("tasks").filter(Filters.gt("points", 2)).build())
        assertEquals(setOf("a", "b"), titles(filtered).toSet())
        val ex = assertFailsWith<RtDbException> { c.mutate(mutation().insert("tasks", obj("title" to "a", "status" to "x", "points" to 0)).build()) }
        assertEquals(ErrorCode.CONFLICT, ex.code)
    }

    @Test
    fun reactiveSubscriptionFlowEmitsSnapshotThenChanges() =
        runBlocking {
            val c = client()
            val q = query("tasks").withIndex("by_status", "open").build()
            val updates = Channel<JsonElement>(Channel.UNLIMITED)
            val job = launch(Dispatchers.Default) { c.subscribeFlow(q).collect { updates.send(it) } }
            try {
                withTimeout(10_000) {
                    assertEquals(0, (updates.receive() as JsonArray).size, "initial snapshot is empty")
                    c.mutate(mutation().insert("tasks", obj("title" to "t1", "status" to "open", "points" to 1)).build())
                    assertEquals(listOf("t1"), titles(updates.receive()))
                    c.mutate(mutation().insert("tasks", obj("title" to "t2", "status" to "done", "points" to 1)).build())
                    c.mutate(mutation().insert("tasks", obj("title" to "t3", "status" to "open", "points" to 1)).build())
                    assertEquals(setOf("t1", "t3"), titles(updates.receive()).toSet(), "unrelated write must not emit")
                }
            } finally {
                job.cancelAndJoin()
            }
        }

    @Test
    fun migrationDirectivesRunInMemory() {
        val c = client()
        c.mutate(mutation().insert("tasks", obj("title" to "a", "status" to "open", "points" to 3)).build())
        val result = c.applyMigration(listOf(Directive.RenameField("tasks", "points", "score")), dryRun = false)
        assertTrue(result.applied)
        val rows = c.query(query("tasks").build()) as JsonArray
        assertTrue((rows[0] as JsonObject).containsKey("score"))
    }
}
