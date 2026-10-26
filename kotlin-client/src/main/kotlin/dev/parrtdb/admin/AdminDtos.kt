package dev.parrtdb.admin

import dev.parrtdb.http.HostCapability
import dev.parrtdb.wire.StrictObj
import dev.parrtdb.wire.jsonArr
import dev.parrtdb.wire.jsonBool
import dev.parrtdb.wire.jsonNum
import dev.parrtdb.wire.jsonObj
import dev.parrtdb.wire.jsonStr
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject

// Admin response DTOs decode like Go's StrictUnmarshal: unknown fields are rejected, absent
// fields take their zero value (older servers omit newer counters). The helpers below
// implement that rule.

internal fun StrictObj.l(k: String): Long = optLong(k) ?: 0L

internal fun StrictObj.s(k: String): String = optStr(k) ?: ""

internal fun StrictObj.b(k: String): Boolean = optBool(k) ?: false

internal fun StrictObj.d(k: String): Double =
    (opt(k) as? kotlinx.serialization.json.JsonPrimitive)?.content?.toDoubleOrNull() ?: 0.0

internal fun <T> StrictObj.list(k: String, f: (JsonElement) -> T): List<T> = optArr(k)?.map(f) ?: emptyList()

internal fun StrictObj.strs(k: String): List<String> = optStrList(k) ?: emptyList()

internal fun nullable(v: String?): JsonElement = v?.let { jsonStr(it) } ?: JsonNull

private inline fun <T> decode(e: JsonElement, what: String, f: (StrictObj) -> T): T {
    val o = StrictObj.of(e, what)
    return f(o).also { o.finish() }
}

/** MintToken response. */
data class MintedToken(val tokenId: String, val token: String) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "mintedToken") { MintedToken(it.s("tokenId"), it.s("token")) }
    }
}

/** One admin allowlist row. */
data class AdminMember(val email: String, val githubId: Long?) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "adminMember") { AdminMember(it.s("email"), it.optLong("githubId")) }
    }
}

/** One row of [DbStats.tables]. */
data class TableStat(val name: String, val rowCount: Long, val sizeBytes: Long) {
    fun toJson() = jsonObj("name" to jsonStr(name), "rowCount" to jsonNum(rowCount), "sizeBytes" to jsonNum(sizeBytes))

    companion object {
        fun fromJson(e: JsonElement) = decode(e, "tableStat") { TableStat(it.s("name"), it.l("rowCount"), it.l("sizeBytes")) }
    }
}

/** GET /admin/dbs/{db}/stats: per-table counts plus quota/usage (0 = unlimited). */
data class DbStats(
    val tables: List<TableStat>,
    val totalSizeBytes: Long,
    val tablesQuota: Long,
    val tablesUsed: Long,
    val storageQuotaBytes: Long,
    val storageUsedBytes: Long,
    val subsQuota: Long,
    val subsUsed: Long,
) {
    fun toJson(): JsonObject =
        jsonObj(
            "tables" to jsonArr(tables.map { it.toJson() }),
            "totalSizeBytes" to jsonNum(totalSizeBytes),
            "tablesQuota" to jsonNum(tablesQuota),
            "tablesUsed" to jsonNum(tablesUsed),
            "storageQuotaBytes" to jsonNum(storageQuotaBytes),
            "storageUsedBytes" to jsonNum(storageUsedBytes),
            "subsQuota" to jsonNum(subsQuota),
            "subsUsed" to jsonNum(subsUsed),
        )

    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "dbStats") {
                DbStats(
                    it.list("tables") { t -> TableStat.fromJson(t) },
                    it.l("totalSizeBytes"),
                    it.l("tablesQuota"),
                    it.l("tablesUsed"),
                    it.l("storageQuotaBytes"),
                    it.l("storageUsedBytes"),
                    it.l("subsQuota"),
                    it.l("subsUsed"),
                )
            }
    }
}

/** One machine token from ListTokens; [hostCapability] null = legacy token without one. */
data class TokenInfo(
    val id: String,
    val name: String,
    val createdAt: Long,
    val revoked: Boolean,
    val expiresAt: Long?,
    val readOnly: Boolean,
    val tables: List<String>,
    val hostCapability: HostCapability?,
) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "tokenInfo") {
                TokenInfo(
                    it.s("id"), it.s("name"), it.l("createdAt"), it.b("revoked"), it.optLong("expiresAt"), it.b("readOnly"),
                    it.strs("tables"), it.opt("hostCapability")?.let { h -> HostCapability.fromJson(h) },
                )
            }
    }
}

/** One interactive session; [tokenHash] is the revoke target. */
data class SessionInfo(
    val tokenHash: String,
    val userId: String,
    val email: String?,
    val name: String?,
    val login: String?,
    val anonymous: Boolean,
    val createdAt: Long,
    val expiresAt: Long,
) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "sessionInfo") {
                SessionInfo(it.s("tokenHash"), it.s("userId"), it.optStr("email"), it.optStr("name"), it.optStr("login"), it.b("anonymous"), it.l("createdAt"), it.l("expiresAt"))
            }
    }
}

/** `{ok, revoked}` body of the session sweep routes. */
data class RevokeUserSessionsResponse(val ok: Boolean, val revoked: Long) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "revokeSessions") { RevokeUserSessionsResponse(it.b("ok"), it.l("revoked")) }
    }
}

data class MergeConflict(val table: String, val id: String) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "mergeConflict") { MergeConflict(it.s("table"), it.s("id")) }
    }
}

data class MergeDbResult(val tables: Map<String, Long>, val conflicts: List<MergeConflict>) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "mergeDbResult") {
                MergeDbResult(
                    it.optObj("tables")?.mapValues { (k, v) -> dev.parrtdb.wire.reqLong(v, "tables.$k") } ?: emptyMap(),
                    it.list("conflicts") { c -> MergeConflict.fromJson(c) },
                )
            }
    }
}

/** Full-instance anon-to-real merge outcome. */
data class MergeReport(
    val dbs: Map<String, MergeDbResult>,
    val storageRepointed: Long,
    val sessionsRepointed: Long,
    val anonDeleted: Boolean,
) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "mergeReport") {
                MergeReport(it.optObj("dbs")?.mapValues { (_, v) -> MergeDbResult.fromJson(v) } ?: emptyMap(), it.l("storageRepointed"), it.l("sessionsRepointed"), it.b("anonDeleted"))
            }
    }
}

internal fun StrictObj.optObj(k: String): JsonObject? = opt(k)?.let { it as? JsonObject ?: dev.parrtdb.wire.wireFail("${k}: expected an object") }

data class BackupFile(val name: String, val sizeBytes: Long, val createdMs: Long) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "backupFile") { BackupFile(it.s("name"), it.l("sizeBytes"), it.l("createdMs")) }
    }
}

data class BackupsListResponse(val running: Boolean, val backups: List<BackupFile>) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "backups") { BackupsListResponse(it.b("running"), it.list("backups") { x -> BackupFile.fromJson(x) }) }
    }
}

data class RestoreResult(val target: String, val instructions: String) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "restoreResult") { RestoreResult(it.s("target"), it.s("instructions")) }
    }
}

/** Compiled SQL plus ordered binds, byte-identical to what execution runs. */
data class ExplainResult(val sql: String, val params: List<String>, val terminal: String, val warnings: List<String>) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "explainResult") { ExplainResult(it.s("sql"), it.strs("params"), it.s("terminal"), it.strs("warnings")) }
    }
}

data class SlowQueryEntry(
    val startedAtMs: Long,
    val durationMs: Long,
    val db: String,
    val table: String,
    val terminal: String,
    val sql: String,
    val params: List<String>,
) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "slowQuery") { SlowQueryEntry(it.l("startedAtMs"), it.l("durationMs"), it.s("db"), it.s("table"), it.s("terminal"), it.s("sql"), it.strs("params")) }
    }
}

data class SlowQueriesResponse(val queries: List<SlowQueryEntry>, val thresholdMs: Long, val capacity: Long) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "slowQueries") { SlowQueriesResponse(it.list("queries") { x -> SlowQueryEntry.fromJson(x) }, it.l("thresholdMs"), it.l("capacity")) }
    }
}

/**
 * One recent document-op event. [seq] is monotonic within one [feedEpoch]; an epoch change means
 * the counter reset (server restart), a gap within an epoch means dropped events.
 */
data class OpEvent(
    val db: String,
    val table: String,
    val docId: String,
    val kind: String,
    val ts: Long,
    val owner: String?,
    val seq: Long,
    val feedEpoch: String,
) {
    fun toJson(): JsonObject =
        JsonObject(
            linkedMapOf(
                "db" to jsonStr(db), "table" to jsonStr(table), "docId" to jsonStr(docId), "kind" to jsonStr(kind),
                "ts" to jsonNum(ts), "owner" to nullable(owner), "seq" to jsonNum(seq), "feedEpoch" to jsonStr(feedEpoch),
            ),
        )

    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "opEvent") { OpEvent(it.s("db"), it.s("table"), it.s("docId"), it.s("kind"), it.l("ts"), it.optStr("owner"), it.l("seq"), it.s("feedEpoch")) }
    }
}

data class AuditEntry(
    val id: Long,
    val tsMs: Long,
    val db: String,
    val table: String,
    val op: String?,
    val docId: String,
    val principal: String?,
    val source: String,
) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "auditEntry") { AuditEntry(it.l("id"), it.l("tsMs"), it.s("db"), it.s("table"), it.optStr("op"), it.s("docId"), it.optStr("principal"), it.s("source")) }
    }
}

/** A registered webhook; [table] null = all tables, [secret] is the per-webhook HMAC key. */
data class Webhook(
    val id: Long,
    val db: String,
    val table: String?,
    val url: String,
    val events: List<String>,
    val createdAt: Long,
    val enabled: Boolean,
    val secret: String?,
) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "webhook") { Webhook(it.l("id"), it.s("db"), it.optStr("table"), it.s("url"), it.strs("events"), it.l("createdAt"), it.b("enabled"), it.optStr("secret")) }
    }
}

data class WebhookDelivery(
    val id: Long,
    val attempts: Long,
    val status: String,
    val nextAttempt: Long,
    val lastError: String?,
    val payload: JsonElement,
) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "webhookDelivery") { WebhookDelivery(it.l("id"), it.l("attempts"), it.s("status"), it.l("nextAttempt"), it.optStr("lastError"), it.raw("payload") ?: JsonNull) }
    }
}

/** CreateEnrollmentGrant response: the one-time plaintext [grant] plus the capability it fixes. */
data class EnrollmentGrant(val grantId: String, val grant: String, val db: String, val capability: HostCapability, val expiresAt: Long) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "enrollmentGrant") { EnrollmentGrant(it.s("grantId"), it.s("grant"), it.s("db"), HostCapability.fromJson(it.req("capability")), it.l("expiresAt")) }
    }
}

internal fun boolJson(v: Boolean) = jsonBool(v)
