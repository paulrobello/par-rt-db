package dev.parrtdb.http

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.transport.HttpTransport
import dev.parrtdb.transport.JdkTransport
import dev.parrtdb.wire.AuthedUser
import dev.parrtdb.wire.ChangeFeedResponse
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.ScheduleInfo
import dev.parrtdb.wire.ScheduleWhen
import dev.parrtdb.wire.StepResult
import dev.parrtdb.wire.StrictObj
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.WireException
import dev.parrtdb.wire.WorkflowInfo
import dev.parrtdb.wire.WorkflowSpec
import dev.parrtdb.wire.WorkflowStatus
import dev.parrtdb.wire.jsonArr
import dev.parrtdb.wire.jsonBool
import dev.parrtdb.wire.jsonNum
import dev.parrtdb.wire.jsonObj
import dev.parrtdb.wire.jsonStr
import dev.parrtdb.wire.wireFail
import java.net.http.HttpClient
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject

/**
 * One-shot HTTP client for a single par-rt-db database. Failures with status >= 400 throw
 * [RtDbException]; malformed 2xx bodies throw [WireException]. Transport failures surface as
 * the underlying `java.io.IOException`.
 */
class RtDbHttpClient(
    baseUrl: String,
    val db: String,
    token: String,
    transport: HttpTransport = JdkTransport(),
) {
    /** JVM convenience: runs over the given `java.net.http` client. */
    constructor(baseUrl: String, db: String, token: String, http: HttpClient) : this(baseUrl, db, token, JdkTransport(http))

    private val core = HttpCore(baseUrl, token, transport)
    private val baseUrl: String get() = core.baseUrl

    // ---- queries -------------------------------------------------------------------------

    /** POST /api/query → the dynamic `result` tree. */
    suspend fun query(q: Query): JsonElement {
        val resp = core.json("POST", "/api/query", jsonObj("db" to jsonStr(db), "query" to q.toJson()))
        return StrictObj.of(resp, "queryResponse").req("result")
    }

    /** POST /api/query-batch → one slot per query, aligned with the input order. */
    suspend fun queryBatch(queries: List<Query>): List<QuerySlot> {
        val resp = core.json("POST", "/api/query-batch", jsonObj("db" to jsonStr(db), "queries" to jsonArr(queries.map { it.toJson() })))
        val o = StrictObj.of(resp, "queryBatchResponse")
        val slots = o.arr("results").map { s ->
            val so = StrictObj.of(s, "querySlot")
            val err = so.opt("error")
            val ok = so.bool("ok")
            val res = so.raw("result")
            if (err != null) QuerySlot(false, error = slotError(err)) else if (ok) QuerySlot(true, result = res) else QuerySlot(false)
        }
        return slots
    }

    // ---- mutations -----------------------------------------------------------------------

    /** POST /api/mutate → one [StepResult] per step. A non-empty [idempotencyKey] engages the server cache. */
    suspend fun mutate(txn: Transaction, idempotencyKey: String? = null): List<StepResult> {
        val body = jsonObj(
            "db" to jsonStr(db),
            "idempotencyKey" to idempotencyKey?.takeIf { it.isNotEmpty() }?.let { jsonStr(it) },
            "txn" to txn.toJson(),
        )
        return stepResults(core.json("POST", "/api/mutate", body), "mutateResponse")
    }

    /**
     * POST /api/mutate-batch: independent transactions in one round trip (not atomic across entries).
     * [idempotencyKeys], when given, must match [txns] in length.
     */
    suspend fun mutateBatch(txns: List<Transaction>, idempotencyKeys: List<String>? = null): List<MutateBatchSlot> {
        if (idempotencyKeys != null && idempotencyKeys.size != txns.size) {
            throw IllegalArgumentException("idempotencyKeys length ${idempotencyKeys.size} does not match txns length ${txns.size}")
        }
        val entries = txns.mapIndexed { i, t ->
            jsonObj("txn" to t.toJson(), "idempotencyKey" to idempotencyKeys?.get(i)?.takeIf { it.isNotEmpty() }?.let { jsonStr(it) })
        }
        val resp = core.json("POST", "/api/mutate-batch", jsonObj("db" to jsonStr(db), "txns" to jsonArr(entries)))
        val o = StrictObj.of(resp, "mutateBatchResponse")
        val slots = o.arr("results").map { s ->
            val so = StrictObj.of(s, "mutateBatchSlot")
            val ok = so.bool("ok")
            val results = so.optArr("results")?.map { StepResult.fromJson(it) }
            val err = so.opt("error")?.let { slotError(it) }
            MutateBatchSlot(ok, if (err == null) results else null, err)
        }
        return slots
    }

    /** Re-runs [build] on PRECONDITION_FAILED, up to [attempts] total tries. */
    suspend fun mutateWithRetry(attempts: Int, build: () -> Transaction): List<StepResult> {
        var last: RtDbException? = null
        repeat(attempts) {
            try {
                return mutate(build())
            } catch (e: RtDbException) {
                if (e.code != ErrorCode.PRECONDITION_FAILED) throw e
                last = e
            }
        }
        throw last ?: RtDbException(ErrorCode.INTERNAL, "mutate_with_retry: no attempts made")
    }

    // ---- schedules -----------------------------------------------------------------------

    /** POST /api/schedule → the minted job id; [external] creates a worker-claimed job. */
    suspend fun schedule(whenSpec: ScheduleWhen, txn: Transaction, external: Boolean = false): String {
        val body = jsonObj(
            "db" to jsonStr(db),
            "when" to whenSpec.toJson(),
            "txn" to txn.toJson(),
            "external" to if (external) jsonBool(true) else null,
        )
        return idOf(core.json("POST", "/api/schedule", body), "scheduleResponse")
    }

    /** POST /api/schedule/claim → up to [limit] due external jobs under a [leaseMs] lease. */
    suspend fun claimSchedules(limit: Long? = null, leaseMs: Long? = null): List<ClaimedSchedule> {
        val body = jsonObj("db" to jsonStr(db), "limit" to limit?.let { jsonNum(it) }, "leaseMs" to leaseMs?.let { jsonNum(it) })
        val o = StrictObj.of(core.json("POST", "/api/schedule/claim", body), "claimResponse")
        return o.arr("jobs").map { ClaimedSchedule.fromJson(it) }
    }

    /** POST /api/schedule/{id}/complete with the claim's fencing token. */
    suspend fun completeSchedule(id: String, lease: Long) {
        core.discard("POST", "/api/schedule/${pathEscape(id)}/complete", jsonObj("db" to jsonStr(db), "lease" to jsonNum(lease)))
    }

    /** POST /api/schedule/{id}/retry: re-arms at now + [retryAfterMs] (server default 60s) recording [errMsg]. */
    suspend fun retrySchedule(id: String, lease: Long, retryAfterMs: Long? = null, errMsg: String = "") {
        val body = jsonObj(
            "db" to jsonStr(db),
            "lease" to jsonNum(lease),
            "delayMs" to retryAfterMs?.let { jsonNum(it) },
            "error" to errMsg.takeIf { it.isNotEmpty() }?.let { jsonStr(it) },
        )
        core.discard("POST", "/api/schedule/${pathEscape(id)}/retry", body)
    }

    /** POST /api/schedule/{id}/fail: terminal failure recorded as [errMsg]. */
    suspend fun failSchedule(id: String, lease: Long, errMsg: String) {
        core.discard("POST", "/api/schedule/${pathEscape(id)}/fail", jsonObj("db" to jsonStr(db), "lease" to jsonNum(lease), "error" to jsonStr(errMsg)))
    }

    /** Cancels a job; false = unknown or already terminal. */
    suspend fun cancelSchedule(id: String): Boolean = manageSchedule(id, "cancel")

    suspend fun pauseSchedule(id: String): Boolean = manageSchedule(id, "pause")

    suspend fun resumeSchedule(id: String): Boolean = manageSchedule(id, "resume")

    private suspend fun manageSchedule(id: String, op: String): Boolean =
        okOf(core.json("POST", "/api/schedule/${pathEscape(id)}/$op", jsonObj("db" to jsonStr(db))), "scheduleManage")

    /** POST /api/schedules → the db's jobs. */
    suspend fun listSchedules(): List<ScheduleInfo> {
        val o = StrictObj.of(core.json("POST", "/api/schedules", jsonObj("db" to jsonStr(db))), "schedulesResponse")
        return o.arr("schedules").map { ScheduleInfo.fromJson(it) }
    }

    // ---- workflows -----------------------------------------------------------------------

    /** POST /api/workflows → the run id. */
    suspend fun startWorkflow(spec: WorkflowSpec): String =
        idOf(core.json("POST", "/api/workflows", jsonObj("db" to jsonStr(db), "spec" to spec.toJson())), "startWorkflowResponse")

    /** Cancels a run; false = unknown or already terminal. */
    suspend fun cancelWorkflow(id: String): Boolean =
        okOf(core.json("POST", "/api/workflows/${pathEscape(id)}/cancel", jsonObj("db" to jsonStr(db))), "cancelWorkflow")

    /** Delivers a named signal to a waiting run; false = not delivered. */
    suspend fun signalWorkflow(id: String, name: String, payload: JsonElement? = null): Boolean {
        val body = jsonObj("db" to jsonStr(db), "name" to jsonStr(name), "payload" to payload)
        return okOf(core.json("POST", "/api/workflows/${pathEscape(id)}/signal", body), "signalWorkflow")
    }

    /** POST /api/workflows/list, optionally filtered by [status]. */
    suspend fun listWorkflows(status: WorkflowStatus? = null): List<WorkflowInfo> {
        val body = jsonObj("db" to jsonStr(db), "status" to status?.let { jsonStr(it.wire) })
        val o = StrictObj.of(core.json("POST", "/api/workflows/list", body), "listWorkflowsResponse")
        return o.arr("workflows").map { WorkflowInfo.fromJson(it) }
    }

    // ---- storage -------------------------------------------------------------------------

    /** POST /api/storage/{db} with the raw bytes as the body. */
    suspend fun upload(contentType: String?, data: ByteArray): UploadResult =
        UploadResult.fromJson(parseBody(core.bytes("POST", "/api/storage/$db", data, contentType)))

    /** GET /storage/{id} (the public serve route) → the file bytes. */
    suspend fun download(id: String): ByteArray = core.bytes("GET", "/storage/$id")

    /** DELETE /api/storage/{db}/{id} (idempotent). */
    suspend fun deleteFile(id: String) {
        if (!okOf(core.json("DELETE", "/api/storage/$db/$id"), "deleteFile")) {
            throw RtDbException(ErrorCode.INTERNAL, "delete file returned ok=false")
        }
    }

    suspend fun getFileMetadata(id: String): FileMetadata = FileMetadata.fromJson(core.json("GET", "/api/storage/$db/$id/metadata"))

    /** GET /api/storage/{db}/{id}/signed-url?ttlSeconds= */
    suspend fun getSignedUrl(id: String, ttlSeconds: Int? = null): SignedUrl {
        val path = "/api/storage/$db/$id/signed-url" + (ttlSeconds?.let { "?ttlSeconds=$it" } ?: "")
        return SignedUrl.fromJson(core.json("GET", path))
    }

    /** The public serve URL for a file (no request is made). */
    fun getUrl(id: String): String = "$baseUrl/storage/$id"

    /** The public serve URL with transform query parameters, in Go's fixed order w,h,fit,q,format. */
    fun transformUrl(id: String, opts: TransformOpts): String {
        val parts = opts.parts()
        return getUrl(id) + if (parts.isEmpty()) "" else "?" + parts.joinToString("&") { "${it.first}=${it.second}" }
    }

    // ---- misc ----------------------------------------------------------------------------

    /**
     * GET /api/db/{db}/changes: committed ops strictly after [since], oldest first. [table] null = all;
     * [limit] <= 0 takes the server default. CURSOR_EXPIRED means resync from 0.
     */
    suspend fun changes(since: Long, table: String? = null, limit: Int = 0): ChangeFeedResponse {
        val params = buildList {
            add("since" to since.toString())
            if (!table.isNullOrEmpty()) add("table" to table)
            if (limit > 0) add("limit" to limit.toString())
        }
        return ChangeFeedResponse.fromJson(core.json("GET", withQuery("/api/db/${pathEscape(db)}/changes", params)))
    }

    /** GET /auth/me: the principal behind this client's credential (user sessions only). */
    suspend fun authMe(): AuthedUser = userOf(core.json("GET", "/auth/me"))

    /** GET /auth/validate with [token] as the bearer. An empty token is rejected locally (BAD_REQUEST). */
    suspend fun authValidate(token: String): AuthedUser {
        if (token.isEmpty()) throw RtDbException(ErrorCode.BAD_REQUEST, "auth validate requires a non-empty token")
        return userOf(core.json("GET", "/auth/validate", null, token))
    }

    /** POST /auth/enroll: public, unauthenticated redemption of a one-time enrollment grant. */
    suspend fun enroll(grant: String): EnrollResult =
        EnrollResult.fromJson(parseBody(core.bytes("POST", "/auth/enroll", dev.parrtdb.wire.WireJson.encode(jsonObj("grant" to jsonStr(grant))).toByteArray(), "application/json", null)))

    // ---- decode helpers ------------------------------------------------------------------

    private fun userOf(resp: JsonElement): AuthedUser {
        val o = StrictObj.of(resp, "authResponse")
        return AuthedUser.fromJson(o.req("user"))
    }
}

internal fun parseBody(b: ByteArray): JsonElement = dev.parrtdb.wire.WireJson.parse(String(b, Charsets.UTF_8))

internal fun stepResults(resp: JsonElement, what: String): List<StepResult> {
    val o = StrictObj.of(resp, what)
    return o.arr("results").map { StepResult.fromJson(it) }
}

internal fun idOf(resp: JsonElement, what: String): String {
    val o = StrictObj.of(resp, what)
    return o.str("id")
}

internal fun okOf(resp: JsonElement, what: String): Boolean {
    val o = StrictObj.of(resp, what)
    return o.bool("ok")
}
