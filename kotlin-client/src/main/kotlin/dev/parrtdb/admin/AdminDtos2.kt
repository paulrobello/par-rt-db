package dev.parrtdb.admin

import dev.parrtdb.wire.StrictObj
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull

private inline fun <T> decode(e: JsonElement, what: String, f: (StrictObj) -> T): T {
    val o = StrictObj.of(e, what)
    return f(o).also { o.finish() }
}

data class LatencyStats(val p50: Long, val p95: Long, val p99: Long) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "latencyStats") { LatencyStats(it.l("p50"), it.l("p95"), it.l("p99")) }
    }
}

data class PresenceRoomInspect(
    val room: String,
    val memberCount: Long,
    val localMemberCount: Long,
    val stateBytes: Long,
    val oldestMemberAgeMs: Long,
    val mergedWithPeers: Boolean,
) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "presenceRoom") { PresenceRoomInspect(it.s("room"), it.l("memberCount"), it.l("localMemberCount"), it.l("stateBytes"), it.l("oldestMemberAgeMs"), it.b("mergedWithPeers")) }
    }
}

data class PresenceRoomsResponse(val rooms: List<PresenceRoomInspect>) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "presenceRooms") { PresenceRoomsResponse(it.list("rooms") { r -> PresenceRoomInspect.fromJson(r) }) }
    }
}

data class SubscriptionsPrincipal(val userId: String?, val email: String?) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "subscriptionsPrincipal") { SubscriptionsPrincipal(it.optStr("userId"), it.optStr("email")) }
    }
}

data class SubscriptionInfo(val db: String, val table: String, val terminal: String, val readSetClass: String, val principal: SubscriptionsPrincipal?) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "subscriptionInfo") {
                SubscriptionInfo(it.s("db"), it.s("table"), it.s("terminal"), it.s("readSetClass"), it.opt("principal")?.let { p -> SubscriptionsPrincipal.fromJson(p) })
            }
    }
}

data class DbSubCounters(
    val db: String,
    val reruns: Long,
    val skipsPoint: Long,
    val skipsIndexed: Long,
    val skipsOrdered: Long,
    val missed: Long,
    val skips: Long,
    val rerunRatio: Double,
) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "dbSubCounters") { DbSubCounters(it.s("db"), it.l("reruns"), it.l("skipsPoint"), it.l("skipsIndexed"), it.l("skipsOrdered"), it.l("missed"), it.l("skips"), it.d("rerunRatio")) }
    }
}

data class DbQuotaCounters(val db: String, val tables: Long, val storage: Long, val subs: Long) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "dbQuotaCounters") { DbQuotaCounters(it.s("db"), it.l("tables"), it.l("storage"), it.l("subs")) }
    }
}

data class DbWorkflowStatusCounts(
    val db: String,
    val pending: Long,
    val running: Long,
    val waiting: Long,
    val success: Long,
    val failed: Long,
    val cancelled: Long,
) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "dbWorkflowCounts") { DbWorkflowStatusCounts(it.s("db"), it.l("pending"), it.l("running"), it.l("waiting"), it.l("success"), it.l("failed"), it.l("cancelled")) }
    }
}

/** GET /admin/metrics. Counter groups absent on older servers decode as zero/empty. */
data class MetricsSnapshot(
    val queriesTotal: Long,
    val mutationsTotal: Long,
    val uploadsTotal: Long,
    val wsConnections: Long,
    val activeSubscriptions: Long,
    val poolSize: Long,
    val poolIdle: Long,
    val uptimeSeconds: Long,
    val queryLatency: LatencyStats,
    val mutateLatency: LatencyStats,
    val subscribeLatency: LatencyStats,
    val subsRerunsTotal: Long,
    val subsSkipsPointTotal: Long,
    val subsSkipsIndexedTotal: Long,
    val subsSkipsOrderedTotal: Long,
    val subsSkipVerificationsTotal: Long,
    val subsMissedPushesTotal: Long,
    val perDbSubs: List<DbSubCounters>,
    val presenceDetail: List<PresenceRoomInspect>,
    val presenceRooms: Long,
    val presenceSessions: Long,
    val quotaRejectionsTablesTotal: Long,
    val quotaRejectionsStorageTotal: Long,
    val quotaRejectionsSubsTotal: Long,
    val adminAuthFailuresTotal: Long,
    val perDbQuota: List<DbQuotaCounters>,
    val workflowStepsSuccessTotal: Long,
    val workflowStepsRetryTotal: Long,
    val workflowStepsFailTotal: Long,
    val perDbWorkflows: List<DbWorkflowStatusCounts>,
) {
    companion object {
        private val ZERO = LatencyStats(0, 0, 0)

        fun fromJson(e: JsonElement) =
            decode(e, "metrics") {
                MetricsSnapshot(
                    it.l("queriesTotal"), it.l("mutationsTotal"), it.l("uploadsTotal"), it.l("wsConnections"), it.l("activeSubscriptions"),
                    it.l("poolSize"), it.l("poolIdle"), it.l("uptimeSeconds"),
                    it.opt("queryLatency")?.let { x -> LatencyStats.fromJson(x) } ?: ZERO,
                    it.opt("mutateLatency")?.let { x -> LatencyStats.fromJson(x) } ?: ZERO,
                    it.opt("subscribeLatency")?.let { x -> LatencyStats.fromJson(x) } ?: ZERO,
                    it.l("subsRerunsTotal"), it.l("subsSkipsPointTotal"), it.l("subsSkipsIndexedTotal"), it.l("subsSkipsOrderedTotal"),
                    it.l("subsSkipVerificationsTotal"), it.l("subsMissedPushesTotal"),
                    it.list("perDbSubs") { x -> DbSubCounters.fromJson(x) },
                    it.list("presenceDetail") { x -> PresenceRoomInspect.fromJson(x) },
                    it.l("presenceRooms"), it.l("presenceSessions"),
                    it.l("quotaRejectionsTablesTotal"), it.l("quotaRejectionsStorageTotal"), it.l("quotaRejectionsSubsTotal"), it.l("adminAuthFailuresTotal"),
                    it.list("perDbQuota") { x -> DbQuotaCounters.fromJson(x) },
                    it.l("workflowStepsSuccessTotal"), it.l("workflowStepsRetryTotal"), it.l("workflowStepsFailTotal"),
                    it.list("perDbWorkflows") { x -> DbWorkflowStatusCounts.fromJson(x) },
                )
            }
    }
}

/** GET /admin/subscriptions: live inspector with server-wide totals and the per-db breakdown. */
data class SubscriptionsResponse(
    val subscriptions: List<SubscriptionInfo>,
    val subsRerunsTotal: Long,
    val subsSkipsPointTotal: Long,
    val subsSkipsIndexedTotal: Long,
    val subsSkipsOrderedTotal: Long,
    val subsMissedPushesTotal: Long,
    val perDb: List<DbSubCounters>,
) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "subscriptions") {
                SubscriptionsResponse(
                    it.list("subscriptions") { x -> SubscriptionInfo.fromJson(x) },
                    it.l("subsRerunsTotal"), it.l("subsSkipsPointTotal"), it.l("subsSkipsIndexedTotal"), it.l("subsSkipsOrderedTotal"), it.l("subsMissedPushesTotal"),
                    it.list("perDb") { x -> DbSubCounters.fromJson(x) },
                )
            }
    }
}

/** The runtime-mutable config subset. */
data class HotConfig(
    val allowedOrigins: List<String>,
    val sessionTtlDays: Long,
    val maxFileSize: Long,
    val idempotencyTtlMs: Long,
    val changeLogMaxRows: Long,
    val maxTablesPerDb: Long,
    val maxStorageBytesPerDb: Long,
    val maxSubsPerDb: Long,
    val perTokenRpm: Long,
    val perDbRpm: Long,
) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "hotConfig") {
                HotConfig(
                    it.strs("allowedOrigins"), it.l("sessionTtlDays"), it.l("maxFileSize"), it.l("idempotencyTtlMs"), it.l("changeLogMaxRows"),
                    it.l("maxTablesPerDb"), it.l("maxStorageBytesPerDb"), it.l("maxSubsPerDb"), it.l("perTokenRpm"), it.l("perDbRpm"),
                )
            }
    }
}

/** Read-only boot-pinned signed-URL storage scope: [mode] is `off`, `all` or `databases`; [databases] is empty unless `databases`. */
data class StorageSignedUrls(val mode: String, val databases: List<String>) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "storageSignedUrls") { StorageSignedUrls(it.s("mode"), it.strs("databases")) }
    }
}

/** GET /admin/config: redacted boot config, hot config, build identity and the admin allowlist. */
data class ConfigResponse(
    val port: Long,
    val publicUrl: String,
    val githubBaseUrl: String,
    val githubApiUrl: String,
    val databaseUrlConfigured: Boolean,
    val adminKeyConfigured: Boolean,
    val githubConfigured: Boolean,
    val googleConfigured: Boolean,
    val gitlabConfigured: Boolean,
    val oidcConfigured: Boolean,
    val storageSignedUrls: StorageSignedUrls,
    val hot: HotConfig,
    val version: String,
    val gitCommit: String,
    val admins: List<AdminMember>,
) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "config") {
                ConfigResponse(
                    it.l("port"), it.s("publicUrl"), it.s("githubBaseUrl"), it.s("githubApiUrl"),
                    it.b("databaseUrlConfigured"), it.b("adminKeyConfigured"), it.b("githubConfigured"), it.b("googleConfigured"), it.b("gitlabConfigured"), it.b("oidcConfigured"),
                    it.opt("storageSignedUrls")?.let { s -> StorageSignedUrls.fromJson(s) } ?: StorageSignedUrls("off", emptyList()),
                    it.opt("hot")?.let { h -> HotConfig.fromJson(h) } ?: HotConfig(emptyList(), 0, 0, 0, 0, 0, 0, 0, 0, 0),
                    it.s("version"), it.s("gitCommit"), it.list("admins") { x -> AdminMember.fromJson(x) },
                )
            }
    }
}

data class SchemaHistorySummary(val version: Long, val capturedAt: Long, val source: String, val principal: String?) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "schemaHistorySummary") { SchemaHistorySummary(it.l("version"), it.l("capturedAt"), it.s("source"), it.optStr("principal")) }
    }
}

data class SchemaHistoryEntry(val version: Long, val capturedAt: Long, val source: String, val principal: String?, val schema: JsonElement) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "schemaHistoryEntry") { SchemaHistoryEntry(it.l("version"), it.l("capturedAt"), it.s("source"), it.optStr("principal"), it.raw("schema") ?: JsonNull) }
    }
}

data class SchemaPreviewColumnAdd(val name: String, val fieldType: String) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "schemaPreviewColumn") { SchemaPreviewColumnAdd(it.s("name"), it.s("fieldType")) }
    }
}

data class SchemaPreviewIndexAdd(val name: String, val fields: List<String>) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "schemaPreviewIndex") { SchemaPreviewIndexAdd(it.s("name"), it.strs("fields")) }
    }
}

data class SchemaPreviewTableAdd(val table: String, val columns: List<SchemaPreviewColumnAdd>, val indexes: List<SchemaPreviewIndexAdd>) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "schemaPreviewTable") {
                SchemaPreviewTableAdd(it.s("table"), it.list("columns") { x -> SchemaPreviewColumnAdd.fromJson(x) }, it.list("indexes") { x -> SchemaPreviewIndexAdd.fromJson(x) })
            }
    }
}

data class SchemaPreviewRejection(val table: String, val item: String, val reason: String) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "schemaPreviewRejection") { SchemaPreviewRejection(it.s("table"), it.s("item"), it.s("reason")) }
    }
}

/** PreviewSchema result: what an additive-only push would add and what it would reject. */
data class SchemaPreviewDiff(val added: List<SchemaPreviewTableAdd>, val rejected: List<SchemaPreviewRejection>) {
    companion object {
        fun fromJson(e: JsonElement) =
            decode(e, "schemaPreviewDiff") {
                SchemaPreviewDiff(it.list("added") { x -> SchemaPreviewTableAdd.fromJson(x) }, it.list("rejected") { x -> SchemaPreviewRejection.fromJson(x) })
            }
    }
}

/** Preview phase of [AdminClient.purgePreview]: the matching count plus the token to confirm with. */
data class PurgePreview(val matching: Long, val confirmToken: String, val limit: Int) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "purgePreview") { PurgePreview(it.l("matching"), it.s("confirmToken"), it.int("limit")) }
    }
}

/** Confirm phase of [AdminClient.purge]; re-run the call while [truncated] is true. */
data class PurgeResult(val deleted: Long, val truncated: Boolean) {
    companion object {
        fun fromJson(e: JsonElement) = decode(e, "purgeResult") { PurgeResult(it.l("deleted"), it.b("truncated")) }
    }
}
