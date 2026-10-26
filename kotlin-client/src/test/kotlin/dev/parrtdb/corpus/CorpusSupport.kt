package dev.parrtdb.corpus

import dev.parrtdb.wire.WireJson
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.io.File

/** Locates `wire-corpus/` by walking up from the Gradle project dir. */
object CorpusPaths {
    val root: File by lazy {
        var dir: File? = File(System.getProperty("user.dir")).absoluteFile
        while (dir != null) {
            if (File(dir, "wire-corpus/wire-corpus.json").isFile) return@lazy File(dir, "wire-corpus")
            dir = dir.parentFile
        }
        error("wire-corpus/ not found above ${System.getProperty("user.dir")}")
    }

    fun read(vararg parts: String): String = File(root, parts.joinToString("/")).readText(Charsets.UTF_8)

    fun parse(vararg parts: String): JsonElement = WireJson.parse(read(*parts))
}

/** Corpus comparison helpers: recursive key projection, canonical form, numeric-tolerant equality. */
object Compare {
    val defaultNormalize: List<String> = listOf("_id", "_creationTime", "_version")

    fun project(node: JsonElement, keys: Collection<String>): JsonElement =
        when (node) {
            is JsonObject -> JsonObject(node.filterKeys { it !in keys }.mapValues { project(it.value, keys) })
            is JsonArray -> JsonArray(node.map { project(it, keys) })
            else -> node
        }

    /** Compact JSON with object keys sorted recursively (the multiset-sort form). */
    fun canonical(v: JsonElement): String =
        when (v) {
            is JsonObject -> v.entries.sortedBy { it.key }.joinToString(",", "{", "}") { "${quote(it.key)}:${canonical(it.value)}" }
            is JsonArray -> v.joinToString(",", "[", "]") { canonical(it) }
            is JsonNull -> "null"
            is JsonPrimitive -> if (v.isString) quote(v.content) else v.content
        }

    private fun quote(s: String): String = WireJson.encode(JsonPrimitive(s))

    /** Numbers compare by double value (6 == 6.0); strings never equal numbers. */
    fun equalNumeric(a: JsonElement, b: JsonElement): Boolean =
        when {
            a is JsonObject && b is JsonObject ->
                a.size == b.size && a.all { (k, v) -> b[k]?.let { equalNumeric(v, it) } ?: false }
            a is JsonArray && b is JsonArray -> a.size == b.size && a.indices.all { equalNumeric(a[it], b[it]) }
            a is JsonNull || b is JsonNull -> a is JsonNull && b is JsonNull
            a is JsonPrimitive && b is JsonPrimitive -> {
                if (!a.isString && !b.isString && a.content != "true" && a.content != "false" && b.content != "true" && b.content != "false") {
                    val x = a.content.toDoubleOrNull()
                    val y = b.content.toDoubleOrNull()
                    if (x != null && y != null) x == y || a.content == b.content else a.content == b.content
                } else {
                    a.isString == b.isString && a.content == b.content
                }
            }
            else -> false
        }

    /** Returns null when equal, else a failure description. */
    fun check(got: JsonElement, want: JsonElement, normalize: Collection<String>, unordered: Boolean): String? {
        val g = project(got, normalize)
        val w = project(want, normalize)
        if (equalNumeric(g, w)) return null
        if (!unordered) return "result mismatch\n got ${canonical(g)}\nwant ${canonical(w)}"
        if (g !is JsonArray || w !is JsonArray) return "unordered comparison requires arrays: got ${canonical(g)}, want ${canonical(w)}"
        if (g.size != w.size) return "row count mismatch (unordered): got ${g.size}, want ${w.size}"
        val gs = g.sortedBy { canonical(it) }
        val ws = w.sortedBy { canonical(it) }
        for (i in gs.indices) {
            if (!equalNumeric(gs[i], ws[i])) return "row $i mismatch (unordered)\n got ${canonical(gs[i])}\nwant ${canonical(ws[i])}"
        }
        return null
    }
}
