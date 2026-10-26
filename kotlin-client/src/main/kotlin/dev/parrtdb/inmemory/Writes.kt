package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.errors.badRequest
import dev.parrtdb.errors.notFound
import dev.parrtdb.wire.MAX_STEPS
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.Step
import dev.parrtdb.wire.StepResult
import dev.parrtdb.wire.Transaction
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

internal const val MAX_AFFECTED_ROWS_PER_TXN = 10_000
internal const val MAX_CASCADE_ROWS = 10_000
internal const val MAX_BY_QUERY_ROWS = 1000
internal const val MAX_BY_QUERY_STEPS_PER_TXN = 16
internal const val CRON_STEP_MS = 60_000L
internal const val MAX_EVERY_MS = 365L * 24 * 60 * 60 * 1000
internal const val MAX_SAFE_INTEGER = 9_007_199_254_740_991L

/** One executed step: its result plus every table it wrote (the notify fan-out is table-keyed). */
internal class StepOut(val result: StepResult, val written: List<String> = emptyList())

/**
 * Executes [txn] atomically and returns one [StepResult] per step. A previously seen [mutId]
 * short-circuits with the cached results. Subscriber callbacks run after the lock is released.
 */
fun applyTxn(s: Store, txn: Transaction, mutId: String?): List<StepResult> {
    var failure: RtDbException? = null
    var results: List<StepResult> = emptyList()
    val fires: List<NotifyFire>
    s.lock.lock()
    try {
        if (!mutId.isNullOrEmpty()) {
            s.idempotency[mutId]?.let { return it }
        }
        if (s.readOnly) {
            throw RtDbException(
                ErrorCode.READ_ONLY,
                "database is frozen read-only; an operator can unfreeze it via PATCH /admin/db/{db}/readonly",
            )
        }
        try {
            results = executeTransaction(s, txn)
            if (!mutId.isNullOrEmpty()) s.idempotency[mutId] = results
        } catch (e: RtDbException) {
            failure = e
        }
        fires = s.takePendingFires()
    } finally {
        s.lock.unlock()
    }
    for (f in fires) f.callback(f.value)
    failure?.let { throw it }
    return results
}

private fun countSteps(txn: Transaction): Int {
    var n = 0
    for (step in txn.steps) n += if (step is Step.Schedule) 1 + countSteps(step.txn) else 1
    return n
}

/** Over-approximates the documents [txn] could touch (server SEC-104); must never under-approximate. */
private fun worstCaseAffected(txn: Transaction): Int {
    var total = 0
    for (step in txn.steps) {
        total +=
            when (step) {
                is Step.InsertMany -> step.docs.size
                is Step.UpsertMany -> step.docs.size
                is Step.PatchByQuery -> byQueryLimit(step.limit)
                is Step.DeleteByQuery -> byQueryLimit(step.limit)
                is Step.ExpectBefore, is Step.ExpectNoMatch, is Step.Schedule, is Step.CancelSchedule,
                is Step.StartWorkflow, is Step.CancelWorkflow,
                -> 0
                else -> 1
            }
    }
    return total
}

private fun byQueryLimit(limit: Int?): Int = if (limit != null && limit >= 0 && limit < MAX_BY_QUERY_ROWS) limit else MAX_BY_QUERY_ROWS

/** Runs [txn]; the caller holds the store lock. */
fun executeTransaction(s: Store, txn: Transaction): List<StepResult> {
    var deadline: Long? = null
    for (step in txn.steps) {
        if (step is Step.ExpectBefore) {
            val value = step.deadlineMs
            if (value < 0 || value > MAX_SAFE_INTEGER) throw badRequest("deadlineMs must be a nonnegative safe integer")
            if (deadline == null || value < deadline) deadline = value
        }
    }
    if (countSteps(txn) > MAX_STEPS) throw badRequest("transaction exceeds maximum of $MAX_STEPS steps")
    val byQuerySteps = txn.steps.count { it is Step.PatchByQuery || it is Step.DeleteByQuery || it is Step.ExpectNoMatch }
    if (byQuerySteps > MAX_BY_QUERY_STEPS_PER_TXN) {
        throw badRequest("transaction has $byQuerySteps by-query steps, exceeding the limit of $MAX_BY_QUERY_STEPS_PER_TXN")
    }
    val worst = worstCaseAffected(txn)
    if (worst > MAX_AFFECTED_ROWS_PER_TXN) {
        throw badRequest("transaction could affect up to $worst documents, exceeding the limit of $MAX_AFFECTED_ROWS_PER_TXN")
    }
    if (deadline != null && s.now() >= deadline) {
        throw RtDbException(ErrorCode.PRECONDITION_FAILED, "transaction deadline expired")
    }
    val snapshot = snapshotDocs(s)
    val schedulesSnapshot = cloneSchedules(s.scheduledJobs)
    val results = ArrayList<StepResult>(txn.steps.size)
    val writeSet = LinkedHashSet<String>()
    try {
        for (step in txn.steps) {
            val out = executeStep(s, step)
            results += out.result
            writeSet.addAll(out.written)
        }
        if (deadline != null && s.now() >= deadline) {
            throw RtDbException(ErrorCode.PRECONDITION_FAILED, "transaction deadline expired")
        }
    } catch (e: Throwable) {
        s.docs = snapshot
        s.scheduledJobs = schedulesSnapshot
        throw e
    }
    notifySubs(s, writeSet)
    return results
}

internal fun snapshotDocs(s: Store): LinkedHashMap<RowKey, StoredRow> {
    val out = LinkedHashMap<RowKey, StoredRow>(s.docs.size * 2)
    for ((k, row) in s.docs) out[k] = StoredRow(row.id, row.doc, row.version, row.createdAt, row.deletedAt)
    return out
}

private fun cloneSchedules(jobs: List<ScheduledJob>): ArrayList<ScheduledJob> {
    val out = ArrayList<ScheduledJob>(jobs.size)
    for (j in jobs) {
        out +=
            ScheduledJob(
                j.id, j.kind, j.txn, j.dueAt, j.cron, j.tz, j.everyMs, j.status, j.createdAt,
                j.firedCount, j.lastError, j.external, j.missedCount, j.lastMissedAt,
            )
    }
    return out
}

/**
 * Re-runs each live subscriber whose table is in [writeSet] and queues its callback iff the
 * canonical result changed. The lock-releasing caller flushes the queue. Query errors are
 * suppressed so a failing subscriber query cannot abort the write.
 */
fun notifySubs(s: Store, writeSet: Set<String>) {
    val live = ArrayList<StoreSubscription>()
    for (sub in s.subscribers) {
        if (!sub.alive) continue
        live += sub
        if (sub.table !in writeSet) continue
        val next =
            try {
                evalQueryLocked(s, sub.query)
            } catch (e: RtDbException) {
                continue
            }
        val nextCanon = diffCanonical(next, sub.query)
        if (sub.hasLast && sub.last == nextCanon) continue
        sub.last = nextCanon
        sub.hasLast = true
        s.pendingFires.add(NotifyFire(sub.callback, next))
    }
    s.subscribers.clear()
    s.subscribers.addAll(live)
}

/** Push-decision form of a result: for a projected query the volatile `_version` is dropped from every doc first. */
fun diffCanonical(result: JsonElement, q: Query): String {
    if (q.fields == null) return canonical(result)
    return canonical(stripVersion(result, q))
}

private fun stripDocVersion(doc: JsonElement): JsonElement =
    if (doc is JsonObject) JsonObject(doc.filterKeys { it != "_version" }) else doc

private fun stripVersion(result: JsonElement, q: Query): JsonElement {
    if (q.count || q.distinct || q.aggregate != null) return result
    if (q.paginate != null) {
        val obj = result as? JsonObject ?: return result
        val docs = obj["docs"] as? JsonArray ?: return result
        return JsonObject(obj.toMutableMap().also { it["docs"] = JsonArray(docs.map(::stripDocVersion)) })
    }
    return when (result) {
        is JsonObject -> stripDocVersion(result)
        is JsonArray -> JsonArray(result.map(::stripDocVersion))
        else -> result
    }
}

/** Layers `_id`/`_creationTime`/`_version` onto a copy of the stored doc. */
fun mergeDoc(row: StoredRow): JsonObject {
    val out = LinkedHashMap<String, JsonElement>(row.doc)
    out["_id"] = JsonPrimitive(row.id)
    out["_creationTime"] = JsonPrimitive(row.createdAt)
    out["_version"] = JsonPrimitive(row.version)
    return JsonObject(out)
}

internal fun wireObject(v: JsonElement): JsonObject = v as? JsonObject ?: throw badRequest("expected a JSON object")

/** The table of a step that modifies or removes an existing row, else null. */
private fun existingRowWriteTable(step: Step): String? =
    when (step) {
        is Step.Patch -> step.table
        is Step.AdjustCounter -> step.table
        is Step.Replace -> step.table
        is Step.Delete -> step.table
        is Step.Undelete -> step.table
        is Step.Upsert -> step.table
        is Step.UpsertMany -> step.table
        is Step.PatchByQuery -> step.table
        is Step.DeleteByQuery -> step.table
        else -> null
    }

/** insertOnly gate; schema-driven removals (onDelete cascade, TTL) never pass through here. */
private fun enforceInsertOnly(s: Store, step: Step) {
    val table = existingRowWriteTable(step) ?: return
    if (s.tables[table]?.insertOnly == true) {
        throw RtDbException(ErrorCode.SCHEMA_VIOLATION, "table '$table' is insertOnly: only insert and insertMany are allowed")
    }
}

private fun executeStep(s: Store, step: Step): StepOut {
    enforceInsertOnly(s, step)
    return executeStepUnchecked(s, step)
}

private fun executeStepUnchecked(s: Store, step: Step): StepOut =
    when (step) {
        is Step.Insert -> stepInsert(s, step)
        is Step.InsertMany -> stepInsertMany(s, step)
        is Step.UpsertMany -> stepUpsertMany(s, step)
        is Step.Patch -> stepPatch(s, step)
        is Step.AdjustCounter -> executeAdjustCounter(s, step)
        is Step.Replace -> stepReplace(s, step)
        is Step.Delete -> stepDelete(s, step)
        is Step.Undelete -> stepUndelete(s, step)
        is Step.ExpectVersion -> {
            requireTable(s, step.table)
            doExpectVersion(s, step.table, step.id, step.version)
            StepOut(StepResult())
        }
        is Step.ExpectFields -> stepExpectFields(s, step)
        is Step.ExpectBefore -> StepOut(StepResult())
        is Step.ExpectAbsent -> stepExpectAbsent(s, step)
        is Step.ExpectNoMatch -> stepExpectNoMatch(s, step)
        is Step.Upsert -> executeUpsert(s, step)
        is Step.PatchByQuery -> stepPatchByQuery(s, step)
        is Step.DeleteByQuery -> stepDeleteByQuery(s, step)
        is Step.Schedule -> {
            val id = scheduleJob(s, step.txn, step.whenSpec, step.external == true)
            StepOut(StepResult(scheduleId = id))
        }
        is Step.CancelSchedule -> StepOut(StepResult(cancelled = cancelScheduleLocked(s, step.id)))
        is Step.StartWorkflow, is Step.CancelWorkflow ->
            throw RtDbException(ErrorCode.INTERNAL, "workflow steps are not supported by the in-memory harness")
    }

private fun stepInsert(s: Store, t: Step.Insert): StepOut {
    val tableDef = requireTable(s, t.table)
    val doc = wireObject(t.doc)
    val id = doInsert(s, t.table, tableDef, doc)
    return StepOut(StepResult(id = id), listOf(t.table))
}

private fun checkBulkLen(op: String, n: Int) {
    if (n > MAX_BY_QUERY_ROWS) throw badRequest("$op has $n docs, exceeding the limit of $MAX_BY_QUERY_ROWS")
}

private fun stepInsertMany(s: Store, t: Step.InsertMany): StepOut {
    checkBulkLen("insertMany", t.docs.size)
    val tableDef = requireTable(s, t.table)
    val ids = ArrayList<String>(t.docs.size)
    for (d in t.docs) ids += doInsert(s, t.table, tableDef, wireObject(d))
    return StepOut(StepResult(ids = ids), listOf(t.table))
}

private fun stepUpsertMany(s: Store, t: Step.UpsertMany): StepOut {
    checkBulkLen("upsertMany", t.docs.size)
    val tableDef = requireTable(s, t.table)
    val index = requireIndex(tableDef, t.index)
    val ids = ArrayList<String>(t.docs.size)
    val flags = ArrayList<Boolean>(t.docs.size)
    for ((n, d) in t.docs.withIndex()) {
        val doc = wireObject(d)
        val eq =
            index.fields.map { f ->
                doc[f] ?: throw badRequest("upsertMany docs[$n] is missing indexed field '$f' of index '${t.index}'")
            }
        val res = executeUpsert(s, Step.Upsert(t.table, t.index, eq, doc, doc)).result
        ids += res.id!!
        flags += res.inserted!!
    }
    return StepOut(StepResult(ids = ids, insertedMany = flags), listOf(t.table))
}

private fun stepPatch(s: Store, t: Step.Patch): StepOut {
    val tableDef = requireTable(s, t.table)
    doPatch(s, tableDef, t.table, t.id, wireObject(t.fields))
    return StepOut(StepResult(), listOf(t.table))
}

private fun stepReplace(s: Store, t: Step.Replace): StepOut {
    val tableDef = requireTable(s, t.table)
    doReplace(s, tableDef, t.table, t.id, wireObject(t.doc))
    return StepOut(StepResult(), listOf(t.table))
}

private fun stepDelete(s: Store, t: Step.Delete): StepOut {
    val tableDef = requireTable(s, t.table)
    val touched = ArrayList<String>()
    if (tableDef.softDelete) {
        doSoftDelete(s, t.table, t.id)
        touched += t.table
    } else {
        deleteRowCascade(s, t.table, t.id, HashSet(), CascadeBudget(), false, touched)
    }
    return StepOut(StepResult(), touched)
}

private fun stepUndelete(s: Store, t: Step.Undelete): StepOut {
    val tableDef = requireTable(s, t.table)
    doUndelete(s, tableDef, t.table, t.id)
    return StepOut(StepResult(), listOf(t.table))
}

private fun stepExpectAbsent(s: Store, t: Step.ExpectAbsent): StepOut {
    val tableDef = requireTable(s, t.table)
    if (eqLookup(s, tableDef, t.table, t.index, t.eq).isNotEmpty()) {
        throw RtDbException(ErrorCode.PRECONDITION_FAILED, "index '${t.index}' already has a matching document")
    }
    return StepOut(StepResult())
}

/** Aborts PRECONDITION_FAILED when any visible live row matches; writes nothing. */
private fun stepExpectNoMatch(s: Store, t: Step.ExpectNoMatch): StepOut {
    val tableDef = requireTable(s, t.table)
    val (ids, _) = scanIdsByFilter(s, tableDef, t.table, t.filter, 1)
    if (ids.isNotEmpty()) throw RtDbException(ErrorCode.PRECONDITION_FAILED, "filter matched an existing document")
    return StepOut(StepResult())
}

private fun stepPatchByQuery(s: Store, t: Step.PatchByQuery): StepOut {
    val tableDef = requireTable(s, t.table)
    if (t.limit != null && t.limit < 0) throw badRequest("limit must be >= 0")
    val patch = wireObject(t.patch)
    val (ids, truncated) = scanIdsByFilter(s, tableDef, t.table, t.filter, t.limit)
    for (id in ids) doPatch(s, tableDef, t.table, id, patch)
    return StepOut(StepResult(patched = ids.size.toLong(), truncated = truncated), listOf(t.table))
}

private fun stepDeleteByQuery(s: Store, t: Step.DeleteByQuery): StepOut {
    val tableDef = requireTable(s, t.table)
    if (t.limit != null && t.limit < 0) throw badRequest("limit must be >= 0")
    val (ids, truncated) = scanIdsByFilter(s, tableDef, t.table, t.filter, t.limit)
    val touched = ArrayList<String>()
    val visited = HashSet<RowKey>()
    val budget = CascadeBudget()
    for (id in ids) deleteRowCascade(s, t.table, id, visited, budget, false, touched)
    return StepOut(StepResult(deleted = ids.size.toLong(), truncated = truncated), touched)
}

/**
 * Stamps the table's autoIncrementField with the next counter value (lazy sequence positioned
 * past the stored max; monotonic, so a rolled-back txn does not reclaim its number).
 */
internal fun stampAutoIncrement(s: Store, tableName: String, tableDef: TableDef, doc: JsonObject): JsonObject {
    val field = tableDef.autoIncrementField ?: return doc
    val next: Long
    val last = s.autoIncrementCounters[tableName]
    if (last != null) {
        next = last + 1
    } else {
        var max = 0L
        for ((key, row) in s.docs) {
            if (key.table != tableName) continue
            val str = row.doc[field] as? JsonPrimitive
            if (str != null && str.isString) {
                val v = parseI64(str.content)
                if (v != null && v > max) max = v
            }
        }
        next = max + 1
    }
    s.autoIncrementCounters[tableName] = next
    val out = LinkedHashMap<String, JsonElement>(doc)
    out[field] = JsonPrimitive(formatI64(next))
    return JsonObject(out)
}

/** Insert: the stamp chain runs ttl, updatedAt, defaults, autoIncrement, computed; then validate, strip, unique-check, mint id. */
internal fun doInsert(s: Store, tableName: String, tableDef: TableDef, doc: JsonObject): String {
    val now = s.now()
    var stamped = stampTtlDefault(tableDef, doc, now)
    stamped = stampUpdatedAt(tableDef, stamped, now)
    stamped = applyDefaults(tableDef, stamped)
    stamped = stampAutoIncrement(s, tableName, tableDef, stamped)
    stamped = stampComputed(tableDef, stamped, now)
    validateDoc(tableDef, stamped)
    val stored = stripUnsetOptionals(tableDef, stamped)
    checkUniqueIndexes(s, tableDef, tableName, stored, "")
    val id = s.newIdLocked()
    s.docs[RowKey(tableName, id)] = StoredRow(id, stored, 1, s.now())
    return id
}

/** A soft-deleted row is absent to the lookup. */
internal fun doPatch(s: Store, tableDef: TableDef, tableName: String, id: String, fields: JsonObject) {
    val row = s.docs[RowKey(tableName, id)]
    if (row == null || row.deletedAt != null) throw notFound("document '$id' not found")
    val now = s.now()
    val stampedFields = stampUpdatedAt(tableDef, fields, now)
    val merged = applyPatch(tableDef, row.doc, stampedFields, now)
    doUpdate(s, tableDef, tableName, id, merged)
}

/** Replaces a doc whole: defaults but no ttl stamp, updatedAt stamp, autoIncrement preserve-or-reject, computed re-derivation. */
internal fun doReplace(s: Store, tableDef: TableDef, tableName: String, id: String, doc: JsonObject) {
    val row = s.docs[RowKey(tableName, id)]
    if (row == null || row.deletedAt != null) throw notFound("document '$id' not found")
    val prevDoc = row.doc
    val stampedBase = stampUpdatedAt(tableDef, applyDefaults(tableDef, doc), s.now())
    val stamped = LinkedHashMap<String, JsonElement>(stampedBase)
    val auto = tableDef.autoIncrementField
    if (auto != null) {
        val prev = prevDoc[auto]
        if (prev != null) {
            val value = stamped[auto]
            if (value == null || isNullValue(value)) {
                stamped[auto] = prev
            } else if (!jsonEq(value, prev)) {
                throw badRequest("autoIncrementField '$auto' cannot be changed")
            }
        }
    }
    for (name in tableDef.computed.keys) stamped.remove(name)
    val computed = stampComputed(tableDef, JsonObject(stamped), s.now())
    validateDoc(tableDef, computed)
    val stored = stripUnsetOptionals(tableDef, computed)
    checkUniqueIndexes(s, tableDef, tableName, stored, id)
    row.doc = stored
    row.version++
}

/** Shared write-back: unique-check, then write and bump the version. */
internal fun doUpdate(s: Store, tableDef: TableDef, tableName: String, id: String, merged: JsonObject) {
    checkUniqueIndexes(s, tableDef, tableName, merged, id)
    s.docs[RowKey(tableName, id)]?.let {
        it.doc = merged
        it.version++
    }
}

/** Stamps the row soft-deleted and bumps the version; an absent or already-stamped row is NOT_FOUND. */
internal fun doSoftDelete(s: Store, tableName: String, id: String) {
    val row = s.docs[RowKey(tableName, id)]
    if (row == null || row.deletedAt != null) throw notFound("document '$id' not found")
    row.deletedAt = s.now()
    row.version++
}

/** Restores a soft-deleted row: idempotent on a live row; unique indexes re-apply on restore. */
internal fun doUndelete(s: Store, tableDef: TableDef, tableName: String, id: String) {
    if (!tableDef.softDelete) throw badRequest("table '$tableName' does not declare softDelete")
    val row = s.docs[RowKey(tableName, id)] ?: throw notFound("document '$id' not found")
    if (row.deletedAt == null) return
    checkUniqueIndexes(s, tableDef, tableName, row.doc, id)
    row.deletedAt = null
    row.version++
}

/** Asserts a doc's current `_version`; a soft-deleted row is absent. */
internal fun doExpectVersion(s: Store, tableName: String, id: String, expected: Long) {
    val row = s.docs[RowKey(tableName, id)]
    if (row == null || row.deletedAt != null) throw notFound("document '$id' not found")
    if (row.version != expected) {
        throw RtDbException(ErrorCode.PRECONDITION_FAILED, "version mismatch: expected $expected, actual ${row.version}")
    }
}
