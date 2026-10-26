package dev.parrtdb.inmemory

import dev.parrtdb.wire.WireJson
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/** Builds a Store with `items` (and optionally `owners`) and rows inserted directly, bypassing Writes. */
internal object BSupport {
    fun items(): TableDef =
        TableDef(
            fields =
                mapOf(
                    "name" to FieldType("string"),
                    "status" to FieldType("string"),
                    "score" to FieldType("number"),
                    "big" to FieldType("int64"),
                    "owner" to FieldType("optional", inner = FieldType("id", table = "owners")),
                    "tags" to FieldType("array", inner = FieldType("id", table = "owners")),
                ),
            indexes =
                listOf(
                    IndexDef("by_status_score", listOf("status", "score")),
                    IndexDef("by_status_big", listOf("status", "big")),
                    IndexDef("by_content", listOf("name"), search = true, trgm = true),
                ),
        )

    fun store(): Store {
        val s = Store()
        s.tables["items"] = items()
        s.tables["owners"] = TableDef(fields = mapOf("label" to FieldType("string")))
        return s
    }

    fun json(text: String): JsonElement = WireJson.parse(text)

    fun put(s: Store, table: String, id: String, created: Long, doc: String, deleted: Boolean = false) {
        val row = StoredRow(id, json(doc) as JsonObject, 1, created, if (deleted) 1L else null)
        s.docs[RowKey(table, id)] = row
    }

    fun ids(v: JsonElement): List<String> = (v as JsonArray).map { ((it as JsonObject)["_id"] as JsonPrimitive).content }
}
