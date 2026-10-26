package dev.parrtdb.admin

import dev.parrtdb.wire.StrictObj
import dev.parrtdb.wire.jsonArr
import dev.parrtdb.wire.jsonBool
import dev.parrtdb.wire.jsonNum
import dev.parrtdb.wire.jsonObj
import dev.parrtdb.wire.jsonStr
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject

/** One row that failed coercion. */
data class CastFailure(val id: String, val value: JsonElement) {
    fun toJson() = jsonObj("id" to jsonStr(id), "value" to value)

    companion object {
        fun fromJson(e: JsonElement): CastFailure {
            val o = StrictObj.of(e, "castFailure")
            return CastFailure(o.str("id"), o.raw("value") ?: JsonNull).also { o.finish() }
        }
    }
}

/** One before/after sample. */
data class SampleChange(val id: String, val before: JsonElement, val after: JsonElement) {
    fun toJson() = jsonObj("id" to jsonStr(id), "before" to before, "after" to after)

    companion object {
        fun fromJson(e: JsonElement): SampleChange {
            val o = StrictObj.of(e, "sampleChange")
            return SampleChange(o.str("id"), o.raw("before") ?: JsonNull, o.raw("after") ?: JsonNull).also { o.finish() }
        }
    }
}

/** One directive's outcome. Empty or absent lists are omitted on encode (Go `omitempty`). */
data class DirectiveReport(
    val op: String,
    val affectedRows: Long,
    val castFailures: List<CastFailure>? = null,
    val sampleChanges: List<SampleChange>? = null,
) {
    fun toJson() =
        jsonObj(
            "op" to jsonStr(op),
            "affectedRows" to jsonNum(affectedRows),
            "castFailures" to castFailures?.takeIf { it.isNotEmpty() }?.let { l -> jsonArr(l.map { it.toJson() }) },
            "sampleChanges" to sampleChanges?.takeIf { it.isNotEmpty() }?.let { l -> jsonArr(l.map { it.toJson() }) },
        )

    companion object {
        fun fromJson(e: JsonElement): DirectiveReport {
            val o = StrictObj.of(e, "directiveReport")
            return DirectiveReport(
                o.str("op"),
                o.long("affectedRows"),
                o.optArr("castFailures")?.map { CastFailure.fromJson(it) },
                o.optArr("sampleChanges")?.map { SampleChange.fromJson(it) },
            ).also { o.finish() }
        }
    }
}

/** MigrateSchema's response; [schema] is the derived post-migration schema, returned even on dry-run. */
data class MigrateResult(val applied: Boolean, val schema: JsonElement, val directives: List<DirectiveReport>) {
    fun toJson(): JsonObject =
        jsonObj("applied" to jsonBool(applied), "schema" to schema, "directives" to jsonArr(directives.map { it.toJson() }))

    companion object {
        fun fromJson(e: JsonElement): MigrateResult {
            val o = StrictObj.of(e, "migrateResult")
            return MigrateResult(o.bool("applied"), o.req("schema"), o.arr("directives").map { DirectiveReport.fromJson(it) }).also { o.finish() }
        }
    }
}
