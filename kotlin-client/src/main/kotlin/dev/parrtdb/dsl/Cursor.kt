package dev.parrtdb.dsl

import dev.parrtdb.wire.WireException
import dev.parrtdb.wire.WireJson
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import java.util.Base64

/** Pagination cursors are standard base64 of a compact JSON array of keyset values. */
object Cursor {
    fun encode(values: List<JsonElement>): String =
        Base64.getEncoder().encodeToString(WireJson.encode(JsonArray(values)).toByteArray(Charsets.UTF_8))

    fun decode(s: String): List<JsonElement> {
        val raw =
            try {
                Base64.getDecoder().decode(s)
            } catch (e: IllegalArgumentException) {
                throw WireException("invalid cursor base64: ${e.message}")
            }
        val v = WireJson.parse(String(raw, Charsets.UTF_8))
        return (v as? JsonArray)?.toList() ?: throw WireException("cursor payload is not a JSON array")
    }
}
