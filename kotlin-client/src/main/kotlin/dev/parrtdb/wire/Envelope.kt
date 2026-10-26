package dev.parrtdb.wire

import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject

/** Mirrors server `protocol::PROTOCOL_VERSION`. */
const val PROTOCOL_VERSION: Int = 3

/** Mirrors server `txn::MAX_STEPS`. */
const val MAX_STEPS: Int = 1024

data class ErrorEnvelope(val code: String, val message: String, val retryAfter: Double? = null) {
    fun toJson() =
        jsonObj(
            "code" to jsonStr(code),
            "message" to jsonStr(message),
            "retryAfter" to retryAfter?.let { jsonNum(it) },
        )

    companion object {
        fun fromJson(e: JsonElement): ErrorEnvelope {
            val o = StrictObj.of(e, "error")
            val ra = o.opt("retryAfter")?.let { (it as? kotlinx.serialization.json.JsonPrimitive)?.content?.toDoubleOrNull() ?: wireFail("error.retryAfter: expected a number") }
            return ErrorEnvelope(o.str("code"), o.str("message"), ra).also { o.finish() }
        }
    }
}

enum class UserKind(val wire: String) {
    User("user"),
    Machine("machine");

    companion object {
        fun fromWire(s: String): UserKind = entries.firstOrNull { it.wire == s } ?: wireFail("unknown user kind `$s`")
    }
}

/** `email`/`name` are always emitted (null when absent); the github/user ids are omitted when absent. */
data class AuthedUser(
    val kind: UserKind,
    val email: String? = null,
    val name: String? = null,
    val githubLogin: String? = null,
    val githubId: Long? = null,
    val userId: String? = null,
) {
    fun toJson(): JsonObject {
        val m = LinkedHashMap<String, JsonElement>()
        m["kind"] = jsonStr(kind.wire)
        m["email"] = email?.let { jsonStr(it) } ?: JsonNull
        m["name"] = name?.let { jsonStr(it) } ?: JsonNull
        githubLogin?.let { m["githubLogin"] = jsonStr(it) }
        githubId?.let { m["githubId"] = jsonNum(it) }
        userId?.let { m["userId"] = jsonStr(it) }
        return JsonObject(m)
    }

    companion object {
        fun fromJson(e: JsonElement): AuthedUser {
            val o = StrictObj.of(e, "user")
            return AuthedUser(
                UserKind.fromWire(o.str("kind")),
                o.optStr("email"),
                o.optStr("name"),
                o.optStr("githubLogin"),
                o.optLong("githubId"),
                o.optStr("userId"),
            ).also { o.finish() }
        }
    }
}

data class PresenceMember(val connectionId: String, val user: AuthedUser, val state: JsonElement) {
    fun toJson() = jsonObj("connectionId" to jsonStr(connectionId), "user" to user.toJson(), "state" to state)

    companion object {
        fun fromJson(e: JsonElement): PresenceMember {
            val o = StrictObj.of(e, "presenceMember")
            return PresenceMember(o.str("connectionId"), AuthedUser.fromJson(o.req("user")), o.req("state")).also { o.finish() }
        }
    }
}

enum class WorkflowStatus(val wire: String) {
    Pending("pending"),
    Running("running"),
    Waiting("waiting"),
    Success("success"),
    Failed("failed"),
    Cancelled("cancelled");

    companion object {
        fun fromWire(s: String): WorkflowStatus = entries.firstOrNull { it.wire == s } ?: wireFail("unknown workflow status `$s`")
    }
}

data class WorkflowInfo(
    val id: String,
    val name: String,
    val status: WorkflowStatus,
    val currentStep: Int,
    val stepCount: Int,
    val attempts: Int,
    val sleepUntil: Long? = null,
    val lastError: String? = null,
    val waitingFor: String? = null,
    val waitedSince: Long? = null,
    val createdAt: Long,
    val updatedAt: Long,
    val startedAt: Long? = null,
    val finishedAt: Long? = null,
) {
    fun toJson() =
        jsonObj(
            "id" to jsonStr(id),
            "name" to jsonStr(name),
            "status" to jsonStr(status.wire),
            "currentStep" to jsonNum(currentStep),
            "stepCount" to jsonNum(stepCount),
            "attempts" to jsonNum(attempts),
            "sleepUntil" to sleepUntil?.let { jsonNum(it) },
            "lastError" to lastError?.let { jsonStr(it) },
            "waitingFor" to waitingFor?.let { jsonStr(it) },
            "waitedSince" to waitedSince?.let { jsonNum(it) },
            "createdAt" to jsonNum(createdAt),
            "updatedAt" to jsonNum(updatedAt),
            "startedAt" to startedAt?.let { jsonNum(it) },
            "finishedAt" to finishedAt?.let { jsonNum(it) },
        )

    companion object {
        fun fromJson(e: JsonElement): WorkflowInfo = readFrom(StrictObj.of(e, "workflowInfo")).also { }

        internal fun readFrom(o: StrictObj): WorkflowInfo =
            WorkflowInfo(
                o.str("id"),
                o.str("name"),
                WorkflowStatus.fromWire(o.str("status")),
                o.int("currentStep"),
                o.int("stepCount"),
                o.int("attempts"),
                o.optLong("sleepUntil"),
                o.optStr("lastError"),
                o.optStr("waitingFor"),
                o.optLong("waitedSince"),
                o.long("createdAt"),
                o.long("updatedAt"),
                o.optLong("startedAt"),
                o.optLong("finishedAt"),
            ).also { o.finish() }
    }
}

enum class ScheduleKind(val wire: String) {
    Oneshot("oneshot"),
    Cron("cron"),
    Interval("interval");

    companion object {
        fun fromWire(s: String): ScheduleKind = entries.firstOrNull { it.wire == s } ?: wireFail("unknown schedule kind `$s`")
    }
}

enum class ScheduleStatus(val wire: String) {
    Pending("pending"),
    Running("running"),
    Paused("paused"),
    Error("error");

    companion object {
        fun fromWire(s: String): ScheduleStatus = entries.firstOrNull { it.wire == s } ?: wireFail("unknown schedule status `$s`")
    }
}

data class ScheduleInfo(
    val id: String,
    val kind: ScheduleKind,
    val dueAt: Long,
    val cron: String? = null,
    val tz: String? = null,
    val everyMs: Long? = null,
    val status: ScheduleStatus,
    val lastError: String? = null,
    val createdAt: Long,
    val firedCount: Long,
    val external: Boolean = false,
    val missedCount: Long = 0,
    val lastMissedAt: Long? = null,
) {
    fun toJson() =
        jsonObj(
            "id" to jsonStr(id),
            "kind" to jsonStr(kind.wire),
            "dueAt" to jsonNum(dueAt),
            "cron" to cron?.let { jsonStr(it) },
            "tz" to tz?.let { jsonStr(it) },
            "everyMs" to everyMs?.let { jsonNum(it) },
            "status" to jsonStr(status.wire),
            "lastError" to lastError?.let { jsonStr(it) },
            "createdAt" to jsonNum(createdAt),
            "firedCount" to jsonNum(firedCount),
            "external" to if (external) jsonBool(true) else null,
            "missedCount" to if (missedCount != 0L) jsonNum(missedCount) else null,
            "lastMissedAt" to lastMissedAt?.let { jsonNum(it) },
        )

    companion object {
        fun fromJson(e: JsonElement): ScheduleInfo {
            val o = StrictObj.of(e, "scheduleInfo")
            return ScheduleInfo(
                o.str("id"),
                ScheduleKind.fromWire(o.str("kind")),
                o.long("dueAt"),
                o.optStr("cron"),
                o.optStr("tz"),
                o.optLong("everyMs"),
                ScheduleStatus.fromWire(o.str("status")),
                o.optStr("lastError"),
                o.long("createdAt"),
                o.long("firedCount"),
                o.optBool("external") ?: false,
                o.optLong("missedCount") ?: 0L,
                o.optLong("lastMissedAt"),
            ).also { o.finish() }
        }
    }
}

enum class ChangeKind(val wire: String) {
    Insert("insert"),
    Patch("patch"),
    Replace("replace"),
    Delete("delete"),
    Upsert("upsert");

    companion object {
        fun fromWire(s: String): ChangeKind = entries.firstOrNull { it.wire == s } ?: wireFail("unknown change kind `$s`")
    }
}

data class ChangeOp(val seq: Long, val table: String, val docId: String, val kind: ChangeKind, val doc: JsonElement, val ts: Long) {
    fun toJson() =
        jsonObj(
            "seq" to jsonNum(seq),
            "table" to jsonStr(table),
            "docId" to jsonStr(docId),
            "kind" to jsonStr(kind.wire),
            "doc" to doc,
            "ts" to jsonNum(ts),
        )

    companion object {
        fun fromJson(e: JsonElement): ChangeOp {
            val o = StrictObj.of(e, "changeOp")
            return ChangeOp(o.long("seq"), o.str("table"), o.str("docId"), ChangeKind.fromWire(o.str("kind")), o.raw("doc") ?: JsonNull, o.long("ts"))
                .also { o.finish() }
        }
    }
}

data class ChangeFeedResponse(val ops: List<ChangeOp>, val nextSeq: Long, val head: Long, val logId: String) {
    fun toJson() =
        jsonObj(
            "ops" to jsonArr(ops.map { it.toJson() }),
            "nextSeq" to jsonNum(nextSeq),
            "head" to jsonNum(head),
            "logId" to jsonStr(logId),
        )

    companion object {
        fun fromJson(e: JsonElement): ChangeFeedResponse {
            val o = StrictObj.of(e, "changeFeed")
            return ChangeFeedResponse(o.arr("ops").map { ChangeOp.fromJson(it) }, o.long("nextSeq"), o.long("head"), o.str("logId"))
                .also { o.finish() }
        }
    }
}

/** Client -> server frames, tagged by `type`. */
sealed interface ClientMessage {
    fun toJson(): JsonObject

    data class Auth(val token: String?, val db: String, val protocolVersion: Int? = null) : ClientMessage {
        override fun toJson() =
            jsonObj(
                "type" to jsonStr("auth"),
                "token" to token?.let { jsonStr(it) },
                "db" to jsonStr(db),
                "protocolVersion" to protocolVersion?.let { jsonNum(it) },
            )
    }

    data class Subscribe(val queryId: String, val query: Query) : ClientMessage {
        override fun toJson() = jsonObj("type" to jsonStr("subscribe"), "queryId" to jsonStr(queryId), "query" to query.toJson())
    }

    data class Unsubscribe(val queryId: String) : ClientMessage {
        override fun toJson() = jsonObj("type" to jsonStr("unsubscribe"), "queryId" to jsonStr(queryId))
    }

    data class Mutate(val mutId: String, val txn: Transaction, val idempotencyKey: String? = null) : ClientMessage {
        override fun toJson() =
            jsonObj(
                "type" to jsonStr("mutate"),
                "mutId" to jsonStr(mutId),
                "idempotencyKey" to idempotencyKey?.let { jsonStr(it) },
                "txn" to txn.toJson(),
            )
    }

    data class Schedule(val scheduleId: String, val whenSpec: ScheduleWhen, val txn: Transaction, val external: Boolean? = null) : ClientMessage {
        override fun toJson() =
            jsonObj(
                "type" to jsonStr("schedule"),
                "scheduleId" to jsonStr(scheduleId),
                "when" to whenSpec.toJson(),
                "txn" to txn.toJson(),
                "external" to external?.let { jsonBool(it) },
            )
    }

    data class CancelSchedule(val scheduleId: String, val id: String) : ClientMessage {
        override fun toJson() = jsonObj("type" to jsonStr("cancelSchedule"), "scheduleId" to jsonStr(scheduleId), "id" to jsonStr(id))
    }

    data class PauseSchedule(val scheduleId: String, val id: String) : ClientMessage {
        override fun toJson() = jsonObj("type" to jsonStr("pauseSchedule"), "scheduleId" to jsonStr(scheduleId), "id" to jsonStr(id))
    }

    data class ResumeSchedule(val scheduleId: String, val id: String) : ClientMessage {
        override fun toJson() = jsonObj("type" to jsonStr("resumeSchedule"), "scheduleId" to jsonStr(scheduleId), "id" to jsonStr(id))
    }

    data class ListSchedules(val scheduleId: String) : ClientMessage {
        override fun toJson() = jsonObj("type" to jsonStr("listSchedules"), "scheduleId" to jsonStr(scheduleId))
    }

    data class StartWorkflow(val workflowId: String, val spec: WorkflowSpec) : ClientMessage {
        override fun toJson() = jsonObj("type" to jsonStr("startWorkflow"), "workflowId" to jsonStr(workflowId), "spec" to spec.toJson())
    }

    data class CancelWorkflow(val workflowId: String, val id: String) : ClientMessage {
        override fun toJson() = jsonObj("type" to jsonStr("cancelWorkflow"), "workflowId" to jsonStr(workflowId), "id" to jsonStr(id))
    }

    data class SignalWorkflow(val workflowId: String, val id: String, val name: String, val payload: JsonElement? = null) : ClientMessage {
        override fun toJson() =
            jsonObj(
                "type" to jsonStr("signalWorkflow"),
                "workflowId" to jsonStr(workflowId),
                "id" to jsonStr(id),
                "name" to jsonStr(name),
                "payload" to payload,
            )
    }

    data class ListWorkflows(val workflowId: String, val status: WorkflowStatus? = null) : ClientMessage {
        override fun toJson() =
            jsonObj("type" to jsonStr("listWorkflows"), "workflowId" to jsonStr(workflowId), "status" to status?.let { jsonStr(it.wire) })
    }

    data class Presence(val room: String, val state: JsonElement? = null) : ClientMessage {
        override fun toJson() = jsonObj("type" to jsonStr("presence"), "room" to jsonStr(room), "state" to state)
    }

    data class PresenceState(val room: String, val state: JsonElement, val ttlMs: Long? = null) : ClientMessage {
        override fun toJson() =
            jsonObj("type" to jsonStr("presenceState"), "room" to jsonStr(room), "state" to state, "ttlMs" to ttlMs?.let { jsonNum(it) })
    }

    data class LeavePresence(val room: String) : ClientMessage {
        override fun toJson() = jsonObj("type" to jsonStr("leavePresence"), "room" to jsonStr(room))
    }

    data object Ping : ClientMessage {
        override fun toJson() = jsonObj("type" to jsonStr("ping"))
    }

    companion object {
        fun fromJson(e: JsonElement): ClientMessage {
            val (tag, body) = peekTag(e, "type", "clientMessage")
            val o = StrictObj(body, "clientMessage.$tag")
            val r: ClientMessage =
                when (tag) {
                    "auth" -> Auth(o.optStr("token"), o.str("db"), o.optInt("protocolVersion"))
                    "subscribe" -> Subscribe(o.str("queryId"), Query.fromJson(o.req("query")))
                    "unsubscribe" -> Unsubscribe(o.str("queryId"))
                    "mutate" -> Mutate(o.str("mutId"), Transaction.fromJson(o.req("txn")), o.optStr("idempotencyKey"))
                    "schedule" ->
                        Schedule(o.str("scheduleId"), ScheduleWhen.fromJson(o.req("when")), Transaction.fromJson(o.req("txn")), o.optBool("external"))
                    "cancelSchedule" -> CancelSchedule(o.str("scheduleId"), o.str("id"))
                    "pauseSchedule" -> PauseSchedule(o.str("scheduleId"), o.str("id"))
                    "resumeSchedule" -> ResumeSchedule(o.str("scheduleId"), o.str("id"))
                    "listSchedules" -> ListSchedules(o.str("scheduleId"))
                    "startWorkflow" -> StartWorkflow(o.str("workflowId"), WorkflowSpec.fromJson(o.req("spec")))
                    "cancelWorkflow" -> CancelWorkflow(o.str("workflowId"), o.str("id"))
                    "signalWorkflow" -> SignalWorkflow(o.str("workflowId"), o.str("id"), o.str("name"), o.opt("payload"))
                    "listWorkflows" -> ListWorkflows(o.str("workflowId"), o.optStr("status")?.let { WorkflowStatus.fromWire(it) })
                    "presence" -> Presence(o.str("room"), o.opt("state"))
                    "presenceState" -> PresenceState(o.str("room"), o.req("state"), o.optLong("ttlMs"))
                    "leavePresence" -> LeavePresence(o.str("room"))
                    "ping" -> Ping
                    else -> wireFail("unknown client tag `$tag`")
                }
            o.finish()
            return r
        }
    }
}

/** Server -> client frames, tagged by `type`. */
sealed interface ServerMessage {
    fun toJson(): JsonObject

    data class AuthOk(val user: AuthedUser, val protocolVersion: Int? = null) : ServerMessage {
        override fun toJson() = jsonObj("type" to jsonStr("authOk"), "user" to user.toJson(), "protocolVersion" to protocolVersion?.let { jsonNum(it) })
    }

    data class AuthErr(val error: ErrorEnvelope) : ServerMessage {
        override fun toJson() = jsonObj("type" to jsonStr("authErr"), "error" to error.toJson())
    }

    data class QueryUpdate(val queryId: String, val result: JsonElement) : ServerMessage {
        override fun toJson() = jsonObj("type" to jsonStr("queryUpdate"), "queryId" to jsonStr(queryId), "result" to result)
    }

    data class MutateOk(val mutId: String, val results: List<JsonElement>) : ServerMessage {
        override fun toJson() = jsonObj("type" to jsonStr("mutateOk"), "mutId" to jsonStr(mutId), "results" to jsonArr(results))
    }

    data class MutateErr(val mutId: String, val error: ErrorEnvelope) : ServerMessage {
        override fun toJson() = jsonObj("type" to jsonStr("mutateErr"), "mutId" to jsonStr(mutId), "error" to error.toJson())
    }

    data class SubscribeErr(val queryId: String, val error: ErrorEnvelope) : ServerMessage {
        override fun toJson() = jsonObj("type" to jsonStr("subscribeErr"), "queryId" to jsonStr(queryId), "error" to error.toJson())
    }

    data class ScheduleOk(val scheduleId: String, val id: String) : ServerMessage {
        override fun toJson() = jsonObj("type" to jsonStr("scheduleOk"), "scheduleId" to jsonStr(scheduleId), "id" to jsonStr(id))
    }

    data class ScheduleErr(val scheduleId: String, val error: ErrorEnvelope) : ServerMessage {
        override fun toJson() = jsonObj("type" to jsonStr("scheduleErr"), "scheduleId" to jsonStr(scheduleId), "error" to error.toJson())
    }

    data class ScheduleAck(val scheduleId: String, val ok: Boolean, val error: ErrorEnvelope? = null) : ServerMessage {
        override fun toJson() =
            jsonObj("type" to jsonStr("scheduleAck"), "scheduleId" to jsonStr(scheduleId), "ok" to jsonBool(ok), "error" to error?.toJson())
    }

    data class ListSchedulesOk(val scheduleId: String, val schedules: List<ScheduleInfo>) : ServerMessage {
        override fun toJson() =
            jsonObj("type" to jsonStr("listSchedulesOk"), "scheduleId" to jsonStr(scheduleId), "schedules" to jsonArr(schedules.map { it.toJson() }))
    }

    data class StartWorkflowOk(val workflowId: String, val info: WorkflowInfo) : ServerMessage {
        override fun toJson() = jsonObj("type" to jsonStr("startWorkflowOk"), "workflowId" to jsonStr(workflowId), "info" to info.toJson())
    }

    data class StartWorkflowErr(val workflowId: String, val error: ErrorEnvelope) : ServerMessage {
        override fun toJson() = jsonObj("type" to jsonStr("startWorkflowErr"), "workflowId" to jsonStr(workflowId), "error" to error.toJson())
    }

    data class WorkflowAck(val workflowId: String, val ok: Boolean, val error: ErrorEnvelope? = null) : ServerMessage {
        override fun toJson() =
            jsonObj("type" to jsonStr("workflowAck"), "workflowId" to jsonStr(workflowId), "ok" to jsonBool(ok), "error" to error?.toJson())
    }

    data class ListWorkflowsOk(val workflowId: String, val workflows: List<WorkflowInfo>) : ServerMessage {
        override fun toJson() =
            jsonObj("type" to jsonStr("listWorkflowsOk"), "workflowId" to jsonStr(workflowId), "workflows" to jsonArr(workflows.map { it.toJson() }))
    }

    data class PresenceSnapshot(val room: String, val members: List<PresenceMember>) : ServerMessage {
        override fun toJson() = jsonObj("type" to jsonStr("presenceSnapshot"), "room" to jsonStr(room), "members" to jsonArr(members.map { it.toJson() }))
    }

    data class PresenceDelta(
        val room: String,
        val seq: Long,
        val joined: List<PresenceMember> = emptyList(),
        val left: List<String> = emptyList(),
        val stateChanged: List<PresenceMember> = emptyList(),
    ) : ServerMessage {
        override fun toJson() =
            jsonObj(
                "type" to jsonStr("presenceDelta"),
                "room" to jsonStr(room),
                "seq" to jsonNum(seq),
                "joined" to joined.takeIf { it.isNotEmpty() }?.let { l -> jsonArr(l.map { it.toJson() }) },
                "left" to left.takeIf { it.isNotEmpty() }?.let { jsonStrArr(it) },
                "stateChanged" to stateChanged.takeIf { it.isNotEmpty() }?.let { l -> jsonArr(l.map { it.toJson() }) },
            )
    }

    data class PresenceErr(val room: String, val error: ErrorEnvelope) : ServerMessage {
        override fun toJson() = jsonObj("type" to jsonStr("presenceErr"), "room" to jsonStr(room), "error" to error.toJson())
    }

    data object Pong : ServerMessage {
        override fun toJson() = jsonObj("type" to jsonStr("pong"))
    }

    companion object {
        fun fromJson(e: JsonElement): ServerMessage {
            val (tag, body) = peekTag(e, "type", "serverMessage")
            val o = StrictObj(body, "serverMessage.$tag")
            val r: ServerMessage =
                when (tag) {
                    "authOk" -> AuthOk(AuthedUser.fromJson(o.req("user")), o.optInt("protocolVersion"))
                    "authErr" -> AuthErr(ErrorEnvelope.fromJson(o.req("error")))
                    "queryUpdate" -> QueryUpdate(o.str("queryId"), o.raw("result") ?: wireFail("queryUpdate: missing field `result`"))
                    "mutateOk" -> MutateOk(o.str("mutId"), o.arr("results").toList())
                    "mutateErr" -> MutateErr(o.str("mutId"), ErrorEnvelope.fromJson(o.req("error")))
                    "subscribeErr" -> SubscribeErr(o.str("queryId"), ErrorEnvelope.fromJson(o.req("error")))
                    "scheduleOk" -> ScheduleOk(o.str("scheduleId"), o.str("id"))
                    "scheduleErr" -> ScheduleErr(o.str("scheduleId"), ErrorEnvelope.fromJson(o.req("error")))
                    "scheduleAck" -> ScheduleAck(o.str("scheduleId"), o.bool("ok"), o.opt("error")?.let { ErrorEnvelope.fromJson(it) })
                    "listSchedulesOk" -> ListSchedulesOk(o.str("scheduleId"), o.arr("schedules").map { ScheduleInfo.fromJson(it) })
                    "startWorkflowOk" -> StartWorkflowOk(o.str("workflowId"), WorkflowInfo.fromJson(o.req("info")))
                    "startWorkflowErr" -> StartWorkflowErr(o.str("workflowId"), ErrorEnvelope.fromJson(o.req("error")))
                    "workflowAck" -> WorkflowAck(o.str("workflowId"), o.bool("ok"), o.opt("error")?.let { ErrorEnvelope.fromJson(it) })
                    "listWorkflowsOk" -> ListWorkflowsOk(o.str("workflowId"), o.arr("workflows").map { WorkflowInfo.fromJson(it) })
                    "presenceSnapshot" -> PresenceSnapshot(o.str("room"), o.arr("members").map { PresenceMember.fromJson(it) })
                    "presenceDelta" ->
                        PresenceDelta(
                            o.str("room"),
                            o.long("seq"),
                            o.optArr("joined")?.map { PresenceMember.fromJson(it) } ?: emptyList(),
                            o.optStrList("left") ?: emptyList(),
                            o.optArr("stateChanged")?.map { PresenceMember.fromJson(it) } ?: emptyList(),
                        )
                    "presenceErr" -> PresenceErr(o.str("room"), ErrorEnvelope.fromJson(o.req("error")))
                    "pong" -> Pong
                    else -> wireFail("unknown server tag `$tag`")
                }
            o.finish()
            return r
        }
    }
}
