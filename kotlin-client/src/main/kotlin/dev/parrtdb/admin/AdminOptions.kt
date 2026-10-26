package dev.parrtdb.admin

import dev.parrtdb.http.HostCapability
import dev.parrtdb.wire.WorkflowStatus
import dev.parrtdb.wire.jsonBool
import dev.parrtdb.wire.jsonNum
import dev.parrtdb.wire.jsonObj
import dev.parrtdb.wire.jsonStr
import dev.parrtdb.wire.jsonStrArr
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject

/** Optional mint capabilities; omitted fields take the server defaults (no expiry, read-write, all tables). */
data class MintTokenOptions(
    val expiresAt: Long? = null,
    val readOnly: Boolean? = null,
    val tables: List<String>? = null,
    val hostCapability: HostCapability? = null,
) {
    internal fun fields(): List<Pair<String, JsonElement?>> =
        listOf(
            "expiresAt" to expiresAt?.let { jsonNum(it) },
            "readOnly" to readOnly?.let { jsonBool(it) },
            "tables" to tables?.takeIf { it.isNotEmpty() }?.let { jsonStrArr(it) },
            "hostCapability" to hostCapability?.toJson(),
        )
}

data class SessionListOptions(val user: String? = null, val limit: Long? = null)

data class WorkflowListOptions(val status: WorkflowStatus? = null, val limit: Long? = null)

data class ListDeliveriesOptions(val status: String? = null, val limit: Long? = null, val offset: Long? = null)

/** GetAudit filters, AND-combined server-side. */
data class AuditQuery(
    val table: String? = null,
    val op: String? = null,
    val principal: String? = null,
    val source: String? = null,
    val limit: Long? = null,
    val offset: Long? = null,
)

/** CreateWebhook body; omitted fields take the server defaults (all tables, `["*"]` events, enabled). */
data class CreateWebhookOptions(
    val url: String,
    val table: String? = null,
    val events: List<String>? = null,
    val enabled: Boolean? = null,
) {
    fun toJson(): JsonObject =
        jsonObj(
            "url" to jsonStr(url),
            "table" to table?.let { jsonStr(it) },
            "events" to events?.takeIf { it.isNotEmpty() }?.let { jsonStrArr(it) },
            "enabled" to enabled?.let { jsonBool(it) },
        )
}

/** Tri-state webhook table filter for [WebhookEditOptions]: leave unchanged (null field), clear, or scope. */
sealed interface TableFilter {
    /** Clears the filter to all tables (JSON null). */
    data object Clear : TableFilter

    data class Scope(val table: String) : TableFilter
}

/** EditWebhook body; absent means "leave unchanged". */
data class WebhookEditOptions(
    val url: String? = null,
    val table: TableFilter? = null,
    val events: List<String>? = null,
    val enabled: Boolean? = null,
    val rotateSecret: Boolean? = null,
) {
    fun toJson(): JsonObject =
        jsonObj(
            "url" to url?.let { jsonStr(it) },
            "table" to
                when (table) {
                    null -> null
                    TableFilter.Clear -> JsonNull
                    is TableFilter.Scope -> jsonStr(table.table)
                },
            "events" to events?.takeIf { it.isNotEmpty() }?.let { jsonStrArr(it) },
            "enabled" to enabled?.let { jsonBool(it) },
            "rotateSecret" to rotateSecret?.let { jsonBool(it) },
        )
}

/** PATCH /admin/config body; omitted fields are left unchanged. */
data class HotConfigPatch(
    val allowedOrigins: List<String>? = null,
    val sessionTtlDays: Long? = null,
    val maxFileSize: Long? = null,
    val idempotencyTtlMs: Long? = null,
    val changeLogMaxRows: Long? = null,
    val maxTablesPerDb: Long? = null,
    val maxStorageBytesPerDb: Long? = null,
    val maxSubsPerDb: Long? = null,
    val perTokenRpm: Long? = null,
    val perDbRpm: Long? = null,
) {
    fun toJson(): JsonObject =
        jsonObj(
            "allowedOrigins" to allowedOrigins?.takeIf { it.isNotEmpty() }?.let { jsonStrArr(it) },
            "sessionTtlDays" to sessionTtlDays?.let { jsonNum(it) },
            "maxFileSize" to maxFileSize?.let { jsonNum(it) },
            "idempotencyTtlMs" to idempotencyTtlMs?.let { jsonNum(it) },
            "changeLogMaxRows" to changeLogMaxRows?.let { jsonNum(it) },
            "maxTablesPerDb" to maxTablesPerDb?.let { jsonNum(it) },
            "maxStorageBytesPerDb" to maxStorageBytesPerDb?.let { jsonNum(it) },
            "maxSubsPerDb" to maxSubsPerDb?.let { jsonNum(it) },
            "perTokenRpm" to perTokenRpm?.let { jsonNum(it) },
            "perDbRpm" to perDbRpm?.let { jsonNum(it) },
        )
}
