package dev.parrtdb.http

import dev.parrtdb.wire.ScheduleKind
import dev.parrtdb.wire.StrictObj
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.jsonObj
import dev.parrtdb.wire.jsonStr
import dev.parrtdb.wire.wireFail
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

/**
 * Versioned host-scoped capability `{"type":"parMeshV1","host":"macdaddy"}` carried by machine
 * tokens and enrollment grants. Decoding is strict: unknown type, unknown field and malformed
 * host all fail closed.
 */
data class HostCapability(val type: String, val host: String) {
    fun toJson(): JsonObject = jsonObj("type" to jsonStr(type), "host" to jsonStr(host))

    companion object {
        const val PAR_MESH_V1: String = "parMeshV1"
        private val HOST_RE = Regex("^[a-z0-9][a-z0-9_-]{0,31}$")

        fun parMeshV1(host: String): HostCapability = HostCapability(PAR_MESH_V1, host)

        fun validateHost(host: String) {
            if (!HOST_RE.matches(host)) wireFail("invalid host `$host`: expected ^[a-z0-9][a-z0-9-_]{0,31}$")
        }

        fun fromJson(e: JsonElement): HostCapability {
            val o = StrictObj.of(e, "hostCapability")
            val type = o.str("type")
            if (type != PAR_MESH_V1) wireFail("unknown capability type `$type`")
            val host = o.str("host")
            validateHost(host)
            o.finish()
            return HostCapability(type, host)
        }
    }
}

/** POST /auth/enroll's body: the minted machine token (shown once), its database and capability. */
data class EnrollResult(val tokenId: String, val token: String, val db: String, val capability: HostCapability) {
    companion object {
        fun fromJson(e: JsonElement): EnrollResult {
            val o = StrictObj.of(e, "enrollResult")
            return EnrollResult(o.str("tokenId"), o.str("token"), o.str("db"), HostCapability.fromJson(o.req("capability"))).also { o.finish() }
        }
    }
}

/** One externally-claimed job from POST /api/schedule/claim; [leaseGeneration] is the fencing token. */
data class ClaimedSchedule(
    val id: String,
    val kind: ScheduleKind,
    val dueAt: Long,
    val txn: Transaction,
    val cron: String? = null,
    val tz: String? = null,
    val everyMs: Long? = null,
    val leaseGeneration: Long,
    val leaseDeadlineMs: Long,
) {
    companion object {
        fun fromJson(e: JsonElement): ClaimedSchedule {
            val o = StrictObj.of(e, "claimedSchedule")
            return ClaimedSchedule(
                o.str("id"),
                ScheduleKind.fromWire(o.str("kind")),
                o.long("dueAt"),
                Transaction.fromJson(o.req("txn")),
                o.optStr("cron"),
                o.optStr("tz"),
                o.optLong("everyMs"),
                o.long("leaseGeneration"),
                o.long("leaseDeadlineMs"),
            ).also { o.finish() }
        }
    }
}

/** POST /api/storage/{db} response. */
data class UploadResult(val id: String, val sha256: String, val size: Long, val contentType: String? = null) {
    companion object {
        fun fromJson(e: JsonElement): UploadResult {
            val o = StrictObj.of(e, "uploadResult")
            return UploadResult(o.str("id"), o.str("sha256"), o.long("size"), o.optStr("contentType"))
        }
    }
}

/** Stored blob metadata. */
data class FileMetadata(
    val id: String,
    val sha256: String,
    val size: Long,
    val contentType: String? = null,
    val creationTime: Long,
) {
    companion object {
        fun fromJson(e: JsonElement): FileMetadata {
            val o = StrictObj.of(e, "fileMetadata")
            return FileMetadata(o.str("id"), o.str("sha256"), o.long("size"), o.optStr("contentType"), o.long("creationTime"))
        }
    }
}

/** A time-limited signed serve URL. */
data class SignedUrl(val url: String, val expiresAt: Long) {
    companion object {
        fun fromJson(e: JsonElement): SignedUrl {
            val o = StrictObj.of(e, "signedUrl")
            return SignedUrl(o.str("url"), o.long("expiresAt"))
        }
    }
}

enum class Fit(val wire: String) { Cover("cover"), Contain("contain"), Inside("inside"), Outside("outside") }

enum class OutFormat(val wire: String) { Jpeg("jpeg"), Png("png"), WebP("webp"), Avif("avif") }

/** Image transform parameters; only non-null fields reach the query string. */
data class TransformOpts(
    val w: Int? = null,
    val h: Int? = null,
    val fit: Fit? = null,
    val q: Int? = null,
    val format: OutFormat? = null,
) {
    internal fun parts(): List<Pair<String, String>> =
        listOfNotNull(
            w?.let { "w" to it.toString() },
            h?.let { "h" to it.toString() },
            fit?.let { "fit" to it.wire },
            q?.let { "q" to it.toString() },
            format?.let { "format" to it.wire },
        )
}

/** One aligned slot of [RtDbHttpClient.queryBatch]: exactly one of [result] / [error] is set. */
data class QuerySlot(val ok: Boolean, val result: JsonElement? = null, val error: dev.parrtdb.errors.RtDbException? = null)

/** One aligned slot of [RtDbHttpClient.mutateBatch]. */
data class MutateBatchSlot(
    val ok: Boolean,
    val results: List<dev.parrtdb.wire.StepResult>? = null,
    val error: dev.parrtdb.errors.RtDbException? = null,
)
