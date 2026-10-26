package dev.parrtdb.wire

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/** Schedule trigger tagged by `type` (camelCase variants). */
sealed interface ScheduleWhen {
    fun toJson(): JsonObject

    data class AfterMs(val ms: Long) : ScheduleWhen {
        override fun toJson() = jsonObj("type" to jsonStr("afterMs"), "ms" to jsonNum(ms))
    }

    data class RunAt(val ms: Long) : ScheduleWhen {
        override fun toJson() = jsonObj("type" to jsonStr("runAt"), "ms" to jsonNum(ms))
    }

    data class Cron(val expr: String, val tz: String? = null) : ScheduleWhen {
        override fun toJson() = jsonObj("type" to jsonStr("cron"), "expr" to jsonStr(expr), "tz" to tz?.let { jsonStr(it) })
    }

    data class Interval(val everyMs: Long) : ScheduleWhen {
        override fun toJson() = jsonObj("type" to jsonStr("interval"), "everyMs" to jsonNum(everyMs))
    }

    companion object {
        fun fromJson(e: JsonElement): ScheduleWhen {
            val (tag, body) = peekTag(e, "type", "scheduleWhen")
            val o = StrictObj(body, "scheduleWhen.$tag")
            val r: ScheduleWhen =
                when (tag) {
                    "afterMs" -> AfterMs(o.long("ms"))
                    "runAt" -> RunAt(o.long("ms"))
                    "cron" -> Cron(o.str("expr"), o.optStr("tz"))
                    "interval" -> Interval(o.long("everyMs"))
                    else -> wireFail("unknown when tag `$tag`")
                }
            o.finish()
            return r
        }
    }
}

data class StepRetry(val maxAttempts: Int, val initialRetryMs: Long, val maxRetryMs: Long) {
    fun toJson() =
        jsonObj(
            "maxAttempts" to jsonNum(maxAttempts),
            "initialRetryMs" to jsonNum(initialRetryMs),
            "maxRetryMs" to jsonNum(maxRetryMs),
        )

    companion object {
        fun fromJson(e: JsonElement): StepRetry {
            val o = StrictObj.of(e, "retry")
            return StepRetry(o.int("maxAttempts"), o.long("initialRetryMs"), o.long("maxRetryMs")).also { o.finish() }
        }
    }
}

data class AwaitSignalSpec(val name: String, val timeoutMs: Long? = null) {
    fun toJson() = jsonObj("name" to jsonStr(name), "timeoutMs" to timeoutMs?.let { jsonNum(it) })

    companion object {
        fun fromJson(e: JsonElement): AwaitSignalSpec {
            val o = StrictObj.of(e, "awaitSignal")
            return AwaitSignalSpec(o.str("name"), o.optLong("timeoutMs")).also { o.finish() }
        }
    }
}

data class WorkflowStepSpec(
    val txn: Transaction? = null,
    val awaitSignal: AwaitSignalSpec? = null,
    val retry: StepRetry? = null,
    val sleepBeforeMs: Long? = null,
) {
    fun toJson() =
        jsonObj(
            "txn" to txn?.toJson(),
            "awaitSignal" to awaitSignal?.toJson(),
            "retry" to retry?.toJson(),
            "sleepBeforeMs" to sleepBeforeMs?.let { jsonNum(it) },
        )

    companion object {
        fun fromJson(e: JsonElement): WorkflowStepSpec {
            val o = StrictObj.of(e, "workflowStep")
            return WorkflowStepSpec(
                o.opt("txn")?.let { Transaction.fromJson(it) },
                o.opt("awaitSignal")?.let { AwaitSignalSpec.fromJson(it) },
                o.opt("retry")?.let { StepRetry.fromJson(it) },
                o.optLong("sleepBeforeMs"),
            ).also { o.finish() }
        }
    }
}

data class WorkflowSpec(val name: String, val steps: List<WorkflowStepSpec>) {
    fun toJson() = jsonObj("name" to jsonStr(name), "steps" to jsonArr(steps.map { it.toJson() }))

    companion object {
        fun fromJson(e: JsonElement): WorkflowSpec {
            val o = StrictObj.of(e, "workflowSpec")
            return WorkflowSpec(o.str("name"), o.arr("steps").map { WorkflowStepSpec.fromJson(it) }).also { o.finish() }
        }
    }
}

data class FieldExpectation(val path: List<String>, val value: JsonElement) {
    fun toJson() = jsonObj("path" to jsonStrArr(path), "value" to value)

    companion object {
        fun fromJson(e: JsonElement): FieldExpectation {
            val o = StrictObj.of(e, "fieldExpectation")
            val path = o.req("path") as? JsonArray ?: wireFail("field path must be an array")
            return FieldExpectation(path.map { reqStr(it, "field path keys") }, o.req("value")).also { o.finish() }
        }
    }
}

/** One transaction step tagged by `op` (mirrors core Step). */
sealed interface Step {
    fun toJson(): JsonObject

    data class Insert(val table: String, val doc: JsonElement) : Step {
        override fun toJson() = jsonObj("op" to jsonStr("insert"), "table" to jsonStr(table), "doc" to doc)
    }

    data class Patch(val table: String, val id: String, val fields: JsonElement) : Step {
        override fun toJson() = jsonObj("op" to jsonStr("patch"), "table" to jsonStr(table), "id" to jsonStr(id), "fields" to fields)
    }

    data class AdjustCounter(
        val table: String,
        val id: String,
        val field: String,
        val delta: Long,
        val min: Long? = null,
        val max: Long? = null,
        val expected: JsonElement? = null,
    ) : Step {
        override fun toJson() =
            jsonObj(
                "op" to jsonStr("adjustCounter"),
                "table" to jsonStr(table),
                "id" to jsonStr(id),
                "field" to jsonStr(field),
                "delta" to jsonNum(delta),
                "min" to min?.let { jsonNum(it) },
                "max" to max?.let { jsonNum(it) },
                "expected" to expected,
            )
    }

    data class Replace(val table: String, val id: String, val doc: JsonElement) : Step {
        override fun toJson() = jsonObj("op" to jsonStr("replace"), "table" to jsonStr(table), "id" to jsonStr(id), "doc" to doc)
    }

    data class Delete(val table: String, val id: String) : Step {
        override fun toJson() = jsonObj("op" to jsonStr("delete"), "table" to jsonStr(table), "id" to jsonStr(id))
    }

    data class Undelete(val table: String, val id: String) : Step {
        override fun toJson() = jsonObj("op" to jsonStr("undelete"), "table" to jsonStr(table), "id" to jsonStr(id))
    }

    data class ExpectVersion(val table: String, val id: String, val version: Long) : Step {
        override fun toJson() =
            jsonObj("op" to jsonStr("expectVersion"), "table" to jsonStr(table), "id" to jsonStr(id), "version" to jsonNum(version))
    }

    data class ExpectFields(val table: String, val id: String, val expected: List<FieldExpectation>) : Step {
        override fun toJson() =
            jsonObj(
                "op" to jsonStr("expectFields"),
                "table" to jsonStr(table),
                "id" to jsonStr(id),
                "expected" to jsonArr(expected.map { it.toJson() }),
            )
    }

    data class ExpectBefore(val deadlineMs: Long) : Step {
        override fun toJson() = jsonObj("op" to jsonStr("expectBefore"), "deadlineMs" to jsonNum(deadlineMs))
    }

    data class ExpectAbsent(val table: String, val index: String, val eq: List<JsonElement>) : Step {
        override fun toJson() =
            jsonObj("op" to jsonStr("expectAbsent"), "table" to jsonStr(table), "index" to jsonStr(index), "eq" to jsonArr(eq))
    }

    data class ExpectNoMatch(val table: String, val filter: FilterExpr) : Step {
        override fun toJson() = jsonObj("op" to jsonStr("expectNoMatch"), "table" to jsonStr(table), "filter" to filter.toJson())
    }

    data class InsertMany(val table: String, val docs: List<JsonElement>) : Step {
        override fun toJson() = jsonObj("op" to jsonStr("insertMany"), "table" to jsonStr(table), "docs" to jsonArr(docs))
    }

    data class UpsertMany(val table: String, val index: String, val docs: List<JsonElement>) : Step {
        override fun toJson() =
            jsonObj("op" to jsonStr("upsertMany"), "table" to jsonStr(table), "index" to jsonStr(index), "docs" to jsonArr(docs))
    }

    data class Upsert(val table: String, val index: String, val eq: List<JsonElement>, val insert: JsonElement, val patch: JsonElement) : Step {
        override fun toJson() =
            jsonObj(
                "op" to jsonStr("upsert"),
                "table" to jsonStr(table),
                "index" to jsonStr(index),
                "eq" to jsonArr(eq),
                "insert" to insert,
                "patch" to patch,
            )
    }

    data class PatchByQuery(val table: String, val filter: FilterExpr, val patch: JsonElement, val limit: Int? = null) : Step {
        override fun toJson() =
            jsonObj(
                "op" to jsonStr("patchByQuery"),
                "table" to jsonStr(table),
                "filter" to filter.toJson(),
                "patch" to patch,
                "limit" to limit?.let { jsonNum(it) },
            )
    }

    data class DeleteByQuery(val table: String, val filter: FilterExpr, val limit: Int? = null) : Step {
        override fun toJson() =
            jsonObj("op" to jsonStr("deleteByQuery"), "table" to jsonStr(table), "filter" to filter.toJson(), "limit" to limit?.let { jsonNum(it) })
    }

    data class Schedule(val whenSpec: ScheduleWhen, val txn: Transaction, val external: Boolean? = null) : Step {
        override fun toJson() =
            jsonObj("op" to jsonStr("schedule"), "when" to whenSpec.toJson(), "txn" to txn.toJson(), "external" to external?.let { jsonBool(it) })
    }

    data class CancelSchedule(val id: String) : Step {
        override fun toJson() = jsonObj("op" to jsonStr("cancelSchedule"), "id" to jsonStr(id))
    }

    data class StartWorkflow(val spec: WorkflowSpec) : Step {
        override fun toJson() = jsonObj("op" to jsonStr("startWorkflow"), "spec" to spec.toJson())
    }

    data class CancelWorkflow(val id: String) : Step {
        override fun toJson() = jsonObj("op" to jsonStr("cancelWorkflow"), "id" to jsonStr(id))
    }

    companion object {
        fun fromJson(e: JsonElement): Step {
            val (tag, body) = peekTag(e, "op", "step")
            val o = StrictObj(body, "step.$tag")
            val r: Step =
                when (tag) {
                    "insert" -> Insert(o.str("table"), o.req("doc"))
                    "patch" -> Patch(o.str("table"), o.str("id"), o.req("fields"))
                    "adjustCounter" ->
                        AdjustCounter(
                            o.str("table"),
                            o.str("id"),
                            o.str("field"),
                            o.long("delta"),
                            o.optLong("min"),
                            o.optLong("max"),
                            o.raw("expected"),
                        )
                    "replace" -> Replace(o.str("table"), o.str("id"), o.req("doc"))
                    "delete" -> Delete(o.str("table"), o.str("id"))
                    "undelete" -> Undelete(o.str("table"), o.str("id"))
                    "expectVersion" -> ExpectVersion(o.str("table"), o.str("id"), o.long("version"))
                    "expectFields" ->
                        ExpectFields(o.str("table"), o.str("id"), o.arr("expected").map { FieldExpectation.fromJson(it) })
                    "expectBefore" -> ExpectBefore(o.long("deadlineMs"))
                    "expectAbsent" -> ExpectAbsent(o.str("table"), o.str("index"), o.arr("eq").toList())
                    "expectNoMatch" -> ExpectNoMatch(o.str("table"), FilterExpr.fromJson(o.req("filter")))
                    "insertMany" -> InsertMany(o.str("table"), o.arr("docs").toList())
                    "upsertMany" -> UpsertMany(o.str("table"), o.str("index"), o.arr("docs").toList())
                    "upsert" -> Upsert(o.str("table"), o.str("index"), o.arr("eq").toList(), o.req("insert"), o.req("patch"))
                    "patchByQuery" -> PatchByQuery(o.str("table"), FilterExpr.fromJson(o.req("filter")), o.req("patch"), o.optInt("limit"))
                    "deleteByQuery" -> DeleteByQuery(o.str("table"), FilterExpr.fromJson(o.req("filter")), o.optInt("limit"))
                    "schedule" -> Schedule(ScheduleWhen.fromJson(o.req("when")), Transaction.fromJson(o.req("txn")), o.optBool("external"))
                    "cancelSchedule" -> CancelSchedule(o.str("id"))
                    "startWorkflow" -> StartWorkflow(WorkflowSpec.fromJson(o.req("spec")))
                    "cancelWorkflow" -> CancelWorkflow(o.str("id"))
                    else -> wireFail("unknown step op `$tag`")
                }
            o.finish()
            return r
        }
    }
}

data class Transaction(val steps: List<Step>) {
    fun toJson() = jsonObj("steps" to jsonArr(steps.map { it.toJson() }))

    companion object {
        fun fromJson(e: JsonElement): Transaction {
            val o = StrictObj.of(e, "txn")
            return Transaction(o.arr("steps").map { Step.fromJson(it) }).also { o.finish() }
        }
    }
}

/**
 * A step result. The wire form is untagged: insert `{id}`, upsert `{id,inserted}`,
 * patchByQuery `{patched,truncated}`, deleteByQuery `{deleted,truncated}`, schedule
 * `{scheduleId}`, cancel `{cancelled}`, workflow `{workflowId}`, bulk `{ids,inserted?}`,
 * and `null` for every other step.
 */
data class StepResult(
    val id: String? = null,
    val inserted: Boolean? = null,
    val patched: Long? = null,
    val deleted: Long? = null,
    val truncated: Boolean? = null,
    val cancelled: Boolean? = null,
    val scheduleId: String? = null,
    val workflowId: String? = null,
    val ids: List<String>? = null,
    val insertedMany: List<Boolean>? = null,
) {
    fun toJson(): JsonElement {
        if (ids != null) {
            return jsonObj("ids" to jsonStrArr(ids), "inserted" to insertedMany?.let { l -> jsonArr(l.map { jsonBool(it) }) })
        }
        val empty = id == null && inserted == null && patched == null && deleted == null && truncated == null &&
            cancelled == null && scheduleId == null && workflowId == null
        if (empty) return JsonNull
        return jsonObj(
            "id" to id?.let { jsonStr(it) },
            "inserted" to inserted?.let { jsonBool(it) },
            "patched" to patched?.let { jsonNum(it) },
            "deleted" to deleted?.let { jsonNum(it) },
            "truncated" to truncated?.let { jsonBool(it) },
            "cancelled" to cancelled?.let { jsonBool(it) },
            "scheduleId" to scheduleId?.let { jsonStr(it) },
            "workflowId" to workflowId?.let { jsonStr(it) },
        )
    }

    companion object {
        fun fromJson(e: JsonElement): StepResult {
            if (e is JsonNull) return StepResult()
            val o = e as? JsonObject ?: wireFail("step result: expected object or null")
            val ids = o["ids"]
            if (ids is JsonArray) {
                return StepResult(
                    ids = ids.map { reqStr(it, "ids[]") },
                    insertedMany = (o["inserted"] as? JsonArray)?.map { reqBool(it, "inserted[]") },
                )
            }
            return StepResult(
                id = (o["id"] as? JsonPrimitive)?.takeIf { it.isString }?.content,
                inserted = (o["inserted"] as? JsonPrimitive)?.let { reqBool(it, "inserted") },
                patched = o["patched"]?.let { reqLong(it, "patched") },
                deleted = o["deleted"]?.let { reqLong(it, "deleted") },
                truncated = (o["truncated"] as? JsonPrimitive)?.let { reqBool(it, "truncated") },
                cancelled = (o["cancelled"] as? JsonPrimitive)?.let { reqBool(it, "cancelled") },
                scheduleId = (o["scheduleId"] as? JsonPrimitive)?.takeIf { it.isString }?.content,
                workflowId = (o["workflowId"] as? JsonPrimitive)?.takeIf { it.isString }?.content,
            )
        }
    }
}
