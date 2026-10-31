package dev.parrtdb.admin

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.http.FileMetadata
import dev.parrtdb.http.HostCapability
import dev.parrtdb.http.HttpCore
import dev.parrtdb.http.SignedUrl
import dev.parrtdb.http.TransformOpts
import dev.parrtdb.http.okOf
import dev.parrtdb.http.pathEscape
import dev.parrtdb.http.stepResults
import dev.parrtdb.http.withQuery
import dev.parrtdb.wire.ChangeFeedResponse
import dev.parrtdb.wire.FilterExpr
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.ScheduleInfo
import dev.parrtdb.wire.ScheduleWhen
import dev.parrtdb.wire.StepResult
import dev.parrtdb.wire.StrictObj
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.WireJson
import dev.parrtdb.wire.WorkflowInfo
import dev.parrtdb.wire.WorkflowSpec
import dev.parrtdb.wire.jsonArr
import dev.parrtdb.wire.jsonBool
import dev.parrtdb.wire.jsonNum
import dev.parrtdb.wire.jsonObj
import dev.parrtdb.wire.jsonStr
import dev.parrtdb.transport.HttpTransport
import dev.parrtdb.transport.JdkTransport
import java.io.InputStream
import java.net.http.HttpClient
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

/**
 * The admin control-plane client (all /admin routes). Every call carries the instance admin key as the bearer.
 * Dynamic path segments are injected raw (like Go), so callers must pass trusted db names and ids.
 * Failures with status >= 400 throw [RtDbException].
 */
class AdminClient(baseUrl: String, adminKey: String, transport: HttpTransport = JdkTransport()) {
    /** JVM convenience: runs over the given `java.net.http` client. */
    constructor(baseUrl: String, adminKey: String, http: HttpClient) : this(baseUrl, adminKey, JdkTransport(http))

    internal val core = HttpCore(baseUrl, adminKey, transport)

    private suspend fun ack(method: String, path: String, body: JsonElement? = null) {
        if (!okOf(core.json(method, path, body), "admin ack")) throw RtDbException(ErrorCode.INTERNAL, "admin request returned ok=false")
    }

    private suspend fun get(path: String, params: List<Pair<String, String>> = emptyList()): JsonElement = core.json("GET", withQuery(path, params))

    private fun opt(k: String, v: Any?): Pair<String, String>? = v?.let { k to it.toString() }

    // ---- databases, schema, tokens ----------------------------------------------------

    /** Creates the database and returns its creation identity (pass it to [deleteDb] as [expectedCreationId]). */
    suspend fun createDb(name: String): String {
        val resp = core.json("POST", "/admin/create-db", jsonObj("name" to jsonStr(name)))
        if (!okOf(resp, "admin ack")) throw RtDbException(ErrorCode.INTERNAL, "admin request returned ok=false")
        return StrictObj.of(resp, "createDb").str("creationId")
    }

    /**
     * The server rejects unless [confirm] equals [name] exactly. A non-null [expectedCreationId] that differs
     * from the database's current creation identity fails with CONFLICT and deletes nothing.
     */
    suspend fun deleteDb(name: String, confirm: String, expectedCreationId: String? = null) =
        ack(
            "POST",
            "/admin/delete-db",
            jsonObj(
                "name" to jsonStr(name),
                "confirm" to jsonStr(confirm),
                *listOfNotNull(expectedCreationId?.let { "expectedCreationId" to jsonStr(it) }).toTypedArray(),
            ),
        )

    suspend fun pushSchema(db: String, schema: JsonElement) = ack("POST", "/admin/push-schema", jsonObj("db" to jsonStr(db), "schema" to schema))

    /** Validates and diffs without applying. */
    suspend fun previewSchema(db: String, schema: JsonElement): SchemaPreviewDiff =
        SchemaPreviewDiff.fromJson(core.json("POST", "/admin/db/$db/schema/preview", jsonObj("schema" to schema)))

    /** On [dryRun] nothing commits; the returned schema previews the derived shape. */
    suspend fun migrateSchema(db: String, directives: List<Directive>, dryRun: Boolean): MigrateResult =
        MigrateResult.fromJson(
            core.json("POST", "/admin/db/$db/migrate", jsonObj("directives" to jsonArr(directives.map { it.toJson() }), "dryRun" to jsonBool(dryRun))),
        )

    suspend fun listDbs(): List<String> = StrictObj.of(get("/admin/dbs"), "dbs").strs("databases")

    /** Mints a token; omitted [options] take the server defaults. */
    suspend fun mintToken(db: String, name: String, options: MintTokenOptions = MintTokenOptions()): MintedToken =
        MintedToken.fromJson(core.json("POST", "/admin/mint-token", jsonObj("db" to jsonStr(db), "name" to jsonStr(name), *options.fields().toTypedArray())))

    suspend fun revokeToken(tokenId: String) = ack("POST", "/admin/revoke-token", jsonObj("tokenId" to jsonStr(tokenId)))

    suspend fun allowlistAdd(db: String, email: String) = allowlist(db, "add", email)

    suspend fun allowlistRemove(db: String, email: String) = allowlist(db, "remove", email)

    private suspend fun allowlist(db: String, action: String, email: String) =
        ack("POST", "/admin/allowlist", jsonObj("db" to jsonStr(db), "action" to jsonStr(action), "email" to jsonStr(email)))

    suspend fun allowlistList(db: String): List<String> = StrictObj.of(get("/admin/allowlist", listOf("db" to db)), "allowlist").strs("emails")

    suspend fun adminsList(): List<AdminMember> = StrictObj.of(get("/admin/admins"), "admins").list("admins") { AdminMember.fromJson(it) }

    suspend fun adminsAdd(email: String, githubId: Long? = null) =
        ack("POST", "/admin/admins", jsonObj("email" to jsonStr(email), "githubId" to githubId?.let { jsonNum(it) }))

    suspend fun adminsRemove(email: String) = ack("DELETE", "/admin/admins", jsonObj("email" to jsonStr(email)))

    suspend fun getSchema(db: String): JsonElement = get("/admin/dbs/$db/schema")

    suspend fun getJsonSchema(db: String): JsonElement = get("/admin/db/$db/schema/json-schema")

    suspend fun schemaHistory(db: String, limit: Long? = null, offset: Long? = null): List<SchemaHistorySummary> =
        StrictObj.of(get("/admin/db/$db/schema/history", listOfNotNull(opt("limit", limit), opt("offset", offset))), "schemaHistory")
            .list("entries") { SchemaHistorySummary.fromJson(it) }

    suspend fun schemaHistoryGet(db: String, version: Long): SchemaHistoryEntry = SchemaHistoryEntry.fromJson(get("/admin/db/$db/schema/history/$version"))

    /** Restores a snapshot and returns the restored version; [confirm] must equal the db name. */
    suspend fun restoreSchema(db: String, version: Long, confirm: String): Long {
        val resp = core.json("POST", "/admin/db/$db/schema/restore", jsonObj("version" to jsonNum(version), "confirm" to jsonStr(confirm)))
        return StrictObj.of(resp, "restoreSchema").l("restoredTo")
    }

    suspend fun dbStats(db: String): DbStats = DbStats.fromJson(get("/admin/dbs/$db/stats"))

    suspend fun listTokens(db: String): List<TokenInfo> = StrictObj.of(get("/admin/tokens", listOf("db" to db)), "tokens").list("tokens") { TokenInfo.fromJson(it) }

    /** The schema plus every document as JSONL. */
    suspend fun exportDb(db: String): String = String(core.bytes("GET", withQuery("/admin/export-db", listOf("db" to db))), Charsets.UTF_8)

    /** Imports an [exportDb] snapshot (`application/x-ndjson`). */
    suspend fun importDb(db: String, jsonl: String) {
        val data = core.bytes("POST", withQuery("/admin/import-db", listOf("db" to db)), jsonl.toByteArray(Charsets.UTF_8), "application/x-ndjson")
        if (!okOf(WireJson.parse(String(data, Charsets.UTF_8)), "import ack")) throw RtDbException(ErrorCode.INTERNAL, "admin request returned ok=false")
    }

    /**
     * Streaming companion to [exportDb]: the same JSONL bytes, returned as a stream read as they arrive from the
     * server so a large database is never held in memory. The caller must close the stream. Prefer this over
     * [exportDb] for big databases. Requires a [dev.parrtdb.transport.StreamingHttpTransport] (both bundled
     * transports qualify), otherwise throws [UnsupportedOperationException].
     */
    suspend fun exportDbStream(db: String): InputStream = core.stream("GET", withQuery("/admin/export-db", listOf("db" to db)))

    /**
     * Streaming companion to [importDb]: uploads the JSONL read from [jsonl] as it is sent, with no full-size
     * buffer. [jsonl] is closed when the call ends, success or failure. Same transport requirement as [exportDbStream].
     */
    suspend fun importDbStream(db: String, jsonl: InputStream) {
        val ack = core.stream("POST", withQuery("/admin/import-db", listOf("db" to db)), jsonl, "application/x-ndjson")
        val data = withContext(Dispatchers.IO) { ack.use { it.readAllBytes() } }
        if (!okOf(WireJson.parse(String(data, Charsets.UTF_8)), "import ack")) throw RtDbException(ErrorCode.INTERNAL, "admin request returned ok=false")
    }

    /** Clones schema and documents server-side; blobs and scheduled transactions are not copied. */
    suspend fun cloneDb(from: String, to: String) = ack("POST", withQuery("/admin/clone-db", listOf("from" to from, "to" to to)))

    // ---- workflows, schedules, files, flags ----------------------------------------------

    suspend fun listWorkflows(db: String, options: WorkflowListOptions? = null): List<WorkflowInfo> =
        StrictObj.of(get("/admin/db/$db/workflows", listOfNotNull(opt("status", options?.status?.wire), opt("limit", options?.limit))), "workflows")
            .list("workflows") { WorkflowInfo.fromJson(it) }

    /** The full run row including the per-step outcome trail, as dynamic JSON. */
    suspend fun getWorkflow(db: String, id: String): JsonElement = get("/admin/db/$db/workflows/$id")

    suspend fun startWorkflow(db: String, spec: WorkflowSpec): String =
        StrictObj.of(core.json("POST", "/admin/db/$db/workflows", spec.toJson()), "startWorkflow").str("id")

    suspend fun cancelWorkflow(db: String, id: String): Boolean = okOf(core.json("POST", "/admin/db/$db/workflows/$id/cancel"), "cancelWorkflow")

    suspend fun signalWorkflow(db: String, id: String, name: String, payload: JsonElement? = null): Boolean =
        okOf(core.json("POST", "/admin/db/$db/workflows/$id/signal", jsonObj("name" to jsonStr(name), "payload" to payload)), "signalWorkflow")

    suspend fun deleteWorkflow(db: String, id: String): Boolean = okOf(core.json("DELETE", "/admin/db/$db/workflows/$id"), "deleteWorkflow")

    suspend fun listSchedules(db: String): List<ScheduleInfo> =
        StrictObj.of(get("/admin/db/$db/schedules"), "schedules").list("schedules") { ScheduleInfo.fromJson(it) }

    suspend fun createSchedule(db: String, whenSpec: ScheduleWhen, txn: Transaction, external: Boolean = false): String {
        val body = jsonObj("when" to whenSpec.toJson(), "txn" to txn.toJson(), "external" to if (external) jsonBool(true) else null)
        return StrictObj.of(core.json("POST", "/admin/db/$db/schedules", body), "createSchedule").str("id")
    }

    suspend fun cancelSchedule(db: String, id: String): Boolean = manageSchedule(db, id, "cancel")

    suspend fun pauseSchedule(db: String, id: String): Boolean = manageSchedule(db, id, "pause")

    suspend fun resumeSchedule(db: String, id: String): Boolean = manageSchedule(db, id, "resume")

    private suspend fun manageSchedule(db: String, id: String, op: String): Boolean = okOf(core.json("POST", "/admin/db/$db/schedules/$id/$op"), "manageSchedule")

    suspend fun listFiles(db: String): List<FileMetadata> =
        StrictObj.of(get("/admin/db/$db/storage"), "files").list("files") { FileMetadata.fromJson(it) }

    /** Uploads raw bytes (not JSON) and returns the new blob id. */
    suspend fun uploadFile(db: String, data: ByteArray, contentType: String? = null): String =
        StrictObj.of(WireJson.parse(String(core.bytes("POST", "/admin/db/$db/storage", data, contentType), Charsets.UTF_8)), "upload").str("id")

    suspend fun deleteFile(db: String, id: String) = ack("DELETE", "/admin/db/$db/storage/$id")

    /** Transform options ride the mint so the server binds them into the signature; format `auto` is the default and not sent. */
    suspend fun getSignedUrl(db: String, id: String, opts: TransformOpts? = null): SignedUrl =
        SignedUrl.fromJson(get("/api/storage/$db/$id/signed-url", opts?.let { transformParams(it) } ?: emptyList()))

    private fun transformParams(o: TransformOpts): List<Pair<String, String>> = o.parts()

    suspend fun getAnonymousAccess(db: String): Boolean = StrictObj.of(get("/admin/db/$db/anonymous-access"), "anon").b("enabled")

    suspend fun setAnonymousAccess(db: String, enabled: Boolean) = ack("PATCH", "/admin/db/$db/anonymous-access", jsonObj("enabled" to jsonBool(enabled)))

    suspend fun getReadOnly(db: String): Boolean = StrictObj.of(get("/admin/db/$db/readonly"), "readonly").b("readOnly")

    /** Freezes (or unfreezes) the database; client-plane writes are rejected with READ_ONLY while frozen. */
    suspend fun setReadOnly(db: String, readOnly: Boolean) = ack("PATCH", "/admin/db/$db/readonly", jsonObj("readOnly" to jsonBool(readOnly)))

    // ---- backups, webhooks, audit, sessions ---------------------------------------------------

    suspend fun backupNow() = ack("POST", "/admin/backup", jsonObj())

    suspend fun listBackups(): BackupsListResponse = BackupsListResponse.fromJson(get("/admin/backups"))

    suspend fun downloadBackup(name: String): ByteArray = core.bytes("GET", "/admin/backups/$name")

    suspend fun deleteBackup(name: String) = core.discard("DELETE", "/admin/backups/$name")

    /** Restores into a fresh `rtdb_restored_<stamp>` database; the live db is never touched. Sends `confirm == name`. */
    suspend fun restoreBackup(name: String): RestoreResult =
        RestoreResult.fromJson(core.json("POST", "/admin/restore", jsonObj("name" to jsonStr(name), "confirm" to jsonStr(name))))

    suspend fun listWebhooks(db: String): List<Webhook> =
        StrictObj.of(get("/admin/db/$db/webhooks"), "webhooks").list("webhooks") { Webhook.fromJson(it) }

    suspend fun createWebhook(db: String, options: CreateWebhookOptions): Long =
        StrictObj.of(core.json("POST", "/admin/db/$db/webhooks", options.toJson()), "createWebhook").l("id")

    suspend fun editWebhook(db: String, id: Long, options: WebhookEditOptions): Webhook =
        Webhook.fromJson(core.json("PUT", "/admin/db/$db/webhooks/$id", options.toJson()))

    suspend fun deleteWebhook(db: String, id: Long) = ack("DELETE", "/admin/db/$db/webhooks/$id")

    suspend fun listDeliveries(db: String, id: Long, options: ListDeliveriesOptions? = null): List<WebhookDelivery> =
        StrictObj.of(
            get("/admin/db/$db/webhooks/$id/deliveries", listOfNotNull(opt("status", options?.status), opt("limit", options?.limit), opt("offset", options?.offset))),
            "deliveries",
        ).list("deliveries") { WebhookDelivery.fromJson(it) }

    /** Re-enqueues a failed delivery; active deliveries answer CONFLICT. */
    suspend fun redeliverDelivery(db: String, id: Long, deliveryId: Long): WebhookDelivery =
        WebhookDelivery.fromJson(core.json("POST", "/admin/db/$db/webhooks/$id/deliveries/$deliveryId/redeliver", jsonObj()))

    suspend fun redeliverFailed(db: String, id: Long): List<WebhookDelivery> =
        StrictObj.of(core.json("POST", "/admin/db/$db/webhooks/$id/deliveries/redeliver-failed", jsonObj()), "redeliver")
            .list("deliveries") { WebhookDelivery.fromJson(it) }

    /** Durable audit rows, newest first; `db` is always sent, other filters only when set. */
    suspend fun getAudit(db: String, query: AuditQuery? = null): List<AuditEntry> {
        val params = listOfNotNull("db" to db, opt("table", query?.table), opt("op", query?.op), opt("principal", query?.principal), opt("source", query?.source), opt("limit", query?.limit), opt("offset", query?.offset))
        return StrictObj.of(get("/admin/audit", params), "audit").list("entries") { AuditEntry.fromJson(it) }
    }

    suspend fun listSessions(options: SessionListOptions? = null): List<SessionInfo> =
        StrictObj.of(get("/admin/sessions", listOfNotNull(opt("user", options?.user), opt("limit", options?.limit))), "sessions")
            .list("sessions") { SessionInfo.fromJson(it) }

    suspend fun revokeSession(tokenHash: String) = ack("DELETE", "/admin/sessions/$tokenHash")

    suspend fun revokeUserSessions(userId: String): RevokeUserSessionsResponse =
        RevokeUserSessionsResponse.fromJson(core.json("DELETE", withQuery("/admin/sessions", listOf("user" to userId))))

    suspend fun revokeExpiredSessions(): RevokeUserSessionsResponse =
        RevokeUserSessionsResponse.fromJson(core.json("DELETE", withQuery("/admin/sessions", listOf("expired" to "true"))))

    /** Merges an anonymous user into a real one; `confirm` is sent as [realUserId]. */
    suspend fun mergeUsers(anonUserId: String, realUserId: String): MergeReport =
        MergeReport.fromJson(
            core.json("POST", "/admin/merge-users", jsonObj("anonUserId" to jsonStr(anonUserId), "realUserId" to jsonStr(realUserId), "confirm" to jsonStr(realUserId))),
        )

    // ---- observability, config, owner-bypass data plane -----------------------------------

    suspend fun metrics(): MetricsSnapshot = MetricsSnapshot.fromJson(get("/admin/metrics"))

    suspend fun presenceRooms(): PresenceRoomsResponse = PresenceRoomsResponse.fromJson(get("/admin/presence"))

    suspend fun listSubscriptions(db: String? = null): SubscriptionsResponse =
        SubscriptionsResponse.fromJson(get("/admin/subscriptions", listOfNotNull(opt("db", db))))

    suspend fun getConfig(): ConfigResponse = ConfigResponse.fromJson(get("/admin/config"))

    suspend fun patchConfig(patch: HotConfigPatch): ConfigResponse = ConfigResponse.fromJson(core.json("PATCH", "/admin/config", patch.toJson()))

    suspend fun opsRecent(db: String, table: String? = null, n: Long? = null): List<OpEvent> =
        StrictObj.of(get("/admin/ops/recent", listOfNotNull("db" to db, opt("table", table), opt("n", n))), "ops").list("ops") { OpEvent.fromJson(it) }

    /** The retained durable change feed under the admin plane (no per-table allowlist filtering). */
    suspend fun adminChanges(db: String, since: Long, table: String? = null, limit: Long? = null): ChangeFeedResponse =
        ChangeFeedResponse.fromJson(get("/admin/db/$db/changes", listOfNotNull("since" to since.toString(), opt("table", table), opt("limit", limit))))

    /** Owner-bypass read; [includeDeleted] true surfaces soft-deleted rows. */
    suspend fun adminQuery(db: String, query: Query, includeDeleted: Boolean? = null): JsonElement {
        val body = jsonObj("query" to query.toJson(), "includeDeleted" to includeDeleted?.let { jsonBool(it) })
        return StrictObj.of(core.json("POST", "/admin/db/$db/query", body), "adminQuery").req("result")
    }

    suspend fun explainQuery(db: String, query: Query): ExplainResult =
        ExplainResult.fromJson(core.json("POST", "/admin/db/$db/explain", jsonObj("query" to query.toJson())))

    suspend fun getSlowQueries(db: String? = null, limit: Long? = null): SlowQueriesResponse =
        SlowQueriesResponse.fromJson(get("/admin/slow-queries", listOfNotNull(opt("db", db), opt("limit", limit))))

    /** Owner-bypass write. */
    suspend fun adminMutate(db: String, txn: Transaction, idempotencyKey: String? = null): List<StepResult> =
        stepResults(core.json("POST", "/admin/db/$db/mutate", jsonObj("txn" to txn.toJson(), "idempotencyKey" to idempotencyKey?.takeIf { it.isNotEmpty() }?.let { jsonStr(it) })), "adminMutate")

    /**
     * Phase one of the two-phase purge of an insertOnly table: counts the rows matching [filter]
     * (the same wire shape a [Query]'s `where` uses; omit to match the whole table) and returns
     * [PurgePreview.confirmToken]. Nothing is deleted.
     */
    suspend fun purgePreview(db: String, table: String, filter: FilterExpr? = null, limit: Int? = null): PurgePreview =
        PurgePreview.fromJson(core.json("POST", "/admin/db/$db/purge", purgeBody(table, filter, limit, null)))

    /**
     * Phase two: with the [confirmToken] a [purgePreview] returned, deletes up to `limit` rows
     * (server-capped at 10,000) per call. Re-run while [PurgeResult.truncated] is true. A stale
     * token fails with PRECONDITION_FAILED (409) — re-preview to get a fresh one.
     */
    suspend fun purge(db: String, table: String, confirmToken: String, filter: FilterExpr? = null, limit: Int? = null): PurgeResult =
        PurgeResult.fromJson(core.json("POST", "/admin/db/$db/purge", purgeBody(table, filter, limit, confirmToken)))

    private fun purgeBody(table: String, filter: FilterExpr?, limit: Int?, confirm: String?): JsonObject =
        jsonObj(
            "table" to jsonStr(table),
            "filter" to filter?.toJson(),
            "limit" to limit?.let { jsonNum(it) },
            "confirm" to confirm?.let { jsonStr(it) },
        )

    // ---- enrollment ------------------------------------------------------------------------

    /** Creates a one-time enrollment grant; the plaintext grant is only in this response. */
    suspend fun createEnrollmentGrant(db: String, name: String, capability: HostCapability, expiresAt: Long, tokenExpiresAt: Long? = null): EnrollmentGrant =
        EnrollmentGrant.fromJson(
            core.json(
                "POST",
                "/admin/enrollment-grants",
                jsonObj(
                    "db" to jsonStr(db),
                    "name" to jsonStr(name),
                    "capability" to capability.toJson(),
                    "expiresAt" to jsonNum(expiresAt),
                    "tokenExpiresAt" to tokenExpiresAt?.let { jsonNum(it) },
                ),
            ),
        )

    suspend fun revokeEnrollmentGrant(grantId: String) = ack("POST", "/admin/revoke-enrollment-grant", jsonObj("grantId" to jsonStr(grantId)))
}
