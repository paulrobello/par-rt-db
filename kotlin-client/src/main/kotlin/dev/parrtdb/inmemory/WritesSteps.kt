package dev.parrtdb.inmemory

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.errors.badRequest
import dev.parrtdb.errors.notFound
import dev.parrtdb.errors.schemaViolation
import dev.parrtdb.wire.FilterExpr
import dev.parrtdb.wire.ScheduleWhen
import dev.parrtdb.wire.Step
import dev.parrtdb.wire.StepResult
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.jsonNum
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive

/** The shared onDelete cascade budget for one initiating delete (or one by-query step). */
internal class CascadeBudget(var rows: Int = 0)

private fun unsafeInt(v: Long): Boolean = v > MAX_SAFE_INTEGER || v < -MAX_SAFE_INTEGER

/**
 * Counter-adjust: safe-integer bounds, number-or-optional-number field, not computed /
 * autoIncrement / updatedAt, expected-field preconditions, then patch the new value via [doPatch].
 */
internal fun executeAdjustCounter(s: Store, t: Step.AdjustCounter): StepOut {
    val tableDef = requireTable(s, t.table)
    if (unsafeInt(t.delta) || (t.min != null && unsafeInt(t.min)) || (t.max != null && unsafeInt(t.max)) ||
        (t.min != null && t.max != null && t.min > t.max)
    ) {
        throw badRequest("counter delta and bounds must be safe integers with min <= max")
    }
    val fty = tableDef.fields[t.field] ?: throw schemaViolation("unknown field '${t.field}'")
    if (t.field in tableDef.computed) throw badRequest("computed field '${t.field}' cannot be adjusted")
    if (tableDef.autoIncrementField == t.field) throw badRequest("autoIncrementField '${t.field}' cannot be changed")
    if (tableDef.updatedAtField == t.field) throw badRequest("updatedAtField '${t.field}' cannot be adjusted")
    val numeric = fty.kind == "number" || (fty.kind == "optional" && fty.inner?.kind == "number")
    if (!numeric) throw schemaViolation("counter field '${t.field}' must be number or optional(number)")
    val row = s.docs[RowKey(t.table, t.id)]
    if (row == null || row.deletedAt != null) throw notFound("document '${t.id}' not found")
    val expected = t.expected
    if (expected is JsonObject) {
        for ((fieldName, wanted) in expected) {
            if (fieldName !in tableDef.fields) throw schemaViolation("unknown expected field '$fieldName'")
            val actual = row.doc[fieldName]
            if (actual == null || !expectedJsonEq(actual, wanted)) {
                throw RtDbException(ErrorCode.PRECONDITION_FAILED, "expected field '$fieldName' did not match")
            }
        }
    }
    val inner = if (fty.kind == "optional" && fty.inner != null) fty.inner else fty
    val notInt = { badRequest("counter field '${t.field}' must contain a safe integer") }
    if (inner.kind != "number") throw notInt()
    val currentValue = row.doc[t.field] ?: throw notInt()
    if (!isJsonNumber(currentValue)) throw notInt()
    val f = jsonNumberF64(currentValue)
    if (f.isNaN() || f.isInfinite() || f != Math.floor(f) || f < -MAX_SAFE_INTEGER.toDouble() || f > MAX_SAFE_INTEGER.toDouble()) {
        throw notInt()
    }
    val current = f.toLong()
    val next = current + t.delta
    if (unsafeInt(next)) throw badRequest("counter result is outside the safe integer range")
    if ((t.min != null && next < t.min) || (t.max != null && next > t.max)) {
        throw RtDbException(ErrorCode.PRECONDITION_FAILED, "counter result is outside the configured bounds")
    }
    doPatch(s, tableDef, t.table, t.id, JsonObject(mapOf(t.field to jsonNum(next))))
    return StepOut(StepResult(), listOf(t.table))
}

/** Integer-valued numbers compare exactly as integers across spellings; other numbers as doubles. */
internal fun expectedJsonEq(left: JsonElement, right: JsonElement): Boolean {
    if (isJsonNumber(left) && isJsonNumber(right)) {
        val ln = (left as JsonPrimitive).content
        val rn = (right as JsonPrimitive).content
        val li = parseI64(ln)
        val ri = parseI64(rn)
        if (li != null && ri != null) return li == ri
        val lf = ln.toDoubleOrNull()
        val rf = rn.toDoubleOrNull()
        if ((li != null) == (ri != null)) {
            if (li != null) return false
            if (lf != null && rf != null) return lf == rf
        }
        return jsonNumberF64(left) == jsonNumberF64(right)
    }
    return jsonEq(left, right)
}

/** Insert-or-patch keyed by a full-arity index eq match; the update branch restamps updatedAtField. */
internal fun executeUpsert(s: Store, t: Step.Upsert): StepOut {
    val tableDef = requireTable(s, t.table)
    val rows = eqLookup(s, tableDef, t.table, t.index, t.eq)
    if (rows.size > 1) throw RtDbException(ErrorCode.PRECONDITION_FAILED, "upsert matched multiple documents")
    if (rows.size == 1) {
        val row = rows[0]
        var patch = wireObject(t.patch)
        val now = s.now()
        patch = stampUpdatedAt(tableDef, patch, now)
        val merged = applyPatch(tableDef, row.doc, patch, now)
        doUpdate(s, tableDef, t.table, row.id, merged)
        return StepOut(StepResult(id = row.id, inserted = false), listOf(t.table))
    }
    val id = doInsert(s, t.table, tableDef, wireObject(t.insert))
    return StepOut(StepResult(id = id, inserted = true), listOf(t.table))
}

/**
 * Deletes one row expanding the app-level onDelete rules: children first (restrict conflicts,
 * cascade recurses, setNull patches), parent last. SoftDelete tables stamp and stop unless
 * [forceHard]; [visited] guards cycles and [budget] is the shared row budget.
 */
internal fun deleteRowCascade(
    s: Store,
    tableName: String,
    id: String,
    visited: MutableSet<RowKey>,
    budget: CascadeBudget,
    forceHard: Boolean,
    touched: MutableList<String>,
) {
    val tableDef = requireTable(s, tableName)
    val key = RowKey(tableName, id)
    if (!visited.add(key)) return
    if (budget.rows >= MAX_CASCADE_ROWS) throw RtDbException(ErrorCode.CONFLICT, "onDelete cascade exceeds the limit of $MAX_CASCADE_ROWS rows")
    budget.rows++
    if (tableDef.softDelete && !forceHard) {
        doSoftDelete(s, tableName, id)
        touched += tableName
        return
    }
    val schema = s.schema ?: throw RtDbException(ErrorCode.INTERNAL, "schema not pushed")
    class Plan(val childTable: String, val childDef: TableDef, val fieldName: String, val action: OnDeleteAction)
    val plan = ArrayList<Plan>()
    for (childTable in schema.tables.keys.sorted()) {
        val childDef = schema.tables.getValue(childTable)
        for ((fieldName, fieldTy) in childDef.fields) {
            val action = onDeleteRef(fieldTy, tableName) ?: continue
            plan += Plan(childTable, childDef, fieldName, action)
        }
    }
    for (p in plan) {
        when (p.action) {
            OnDeleteAction.Restrict -> {
                val hits = visibleChildIds(s, p.childTable, p.fieldName, id, true)
                if (hits.isNotEmpty()) {
                    throw RtDbException(
                        ErrorCode.CONFLICT,
                        "cannot delete '$tableName': '${p.childTable}.${p.fieldName}' is referenced by document '${hits[0]}'",
                    )
                }
            }
            OnDeleteAction.Cascade ->
                for (childId in visibleChildIds(s, p.childTable, p.fieldName, id, false)) {
                    deleteRowCascade(s, p.childTable, childId, visited, budget, forceHard, touched)
                }
            OnDeleteAction.SetNull ->
                for (childId in visibleChildIds(s, p.childTable, p.fieldName, id, false)) {
                    if (budget.rows >= MAX_CASCADE_ROWS) {
                        throw RtDbException(ErrorCode.CONFLICT, "onDelete cascade exceeds the limit of $MAX_CASCADE_ROWS rows")
                    }
                    budget.rows++
                    doPatch(s, p.childDef, p.childTable, childId, JsonObject(mapOf(p.fieldName to JsonNull)))
                    touched += p.childTable
                }
        }
    }
    if (s.docs.remove(key) == null) throw notFound("document '$id' not found")
    touched += tableName
}

/** Live child rows referencing [parentId], ids ascending; [limitOne] probes a single hit, otherwise capped at the cascade budget plus one. */
private fun visibleChildIds(s: Store, childTable: String, field: String, parentId: String, limitOne: Boolean): List<String> {
    val cap = if (limitOne) 1 else MAX_CASCADE_ROWS + 1
    val ids = ArrayList<String>()
    for ((key, row) in s.docs) {
        if (key.table != childTable || row.deletedAt != null) continue
        val v = row.doc[field] as? JsonPrimitive
        if (v != null && v.isString && v.content == parentId) ids += key.id
    }
    ids.sort()
    return if (ids.size > cap) ids.subList(0, cap).toList() else ids
}

/**
 * Matching live rows ordered by (created_at, id) as the server's by-query scan does, capped at
 * the limit; the second value is the truncation flag. The clock is read once per step.
 */
internal fun scanIdsByFilter(s: Store, tableDef: TableDef, tableName: String, filter: FilterExpr, limitOpt: Int?): Pair<List<String>, Boolean> {
    validateByQueryFilter(filter, tableDef)
    val now = s.now()
    val limit = if (limitOpt != null && limitOpt < MAX_BY_QUERY_ROWS) limitOpt else MAX_BY_QUERY_ROWS
    val matching = ArrayList<StoredRow>()
    for ((key, row) in s.docs) {
        if (key.table != tableName || row.deletedAt != null) continue
        if (matchesFilterAt(filter, row.doc, tableDef.fields, now)) matching += row
    }
    matching.sortWith(compareBy<StoredRow> { it.createdAt }.thenBy { it.id })
    val truncated = matching.size > limit
    val ids = matching.take(limit).map { it.id }
    return ids to truncated
}

private fun collectIndexKey(fields: List<String>, doc: JsonObject): List<JsonElement>? {
    val key = ArrayList<JsonElement>(fields.size)
    for (field in fields) {
        val v = doc[field]
        if (v == null || isNullValue(v)) return null
        key += v
    }
    return key
}

/**
 * Enforces unique indexes on a candidate write: no other live row satisfying the partial
 * predicate may share the key. NULL/absent key fields disable the constraint.
 */
internal fun checkUniqueIndexes(s: Store, tableDef: TableDef, tableName: String, candidateDoc: JsonObject, excludeId: String) {
    for (index in tableDef.indexes) {
        if (!index.unique) continue
        val where = index.whereClause
        if (where != null && !matchesFilter(where, candidateDoc, tableDef.fields)) continue
        val candidateKey = collectIndexKey(index.fields, candidateDoc) ?: continue
        for ((key, row) in s.docs) {
            if (key.table != tableName) continue
            if (excludeId.isNotEmpty() && row.id == excludeId) continue
            if (row.deletedAt != null) continue
            if (where != null && !matchesFilter(where, row.doc, tableDef.fields)) continue
            val rowKeyVals = collectIndexKey(index.fields, row.doc) ?: continue
            if (candidateKey.indices.all { jsonEq(candidateKey[it], rowKeyVals[it]) }) {
                throw RtDbException(ErrorCode.CONFLICT, "unique index '${index.name}' violated")
            }
        }
    }
}

/** Full-arity index eq lookup shared by expectAbsent and upsert: soft-deleted rows absent, null index fields never match. Returns copies. */
fun eqLookup(s: Store, tableDef: TableDef, tableName: String, indexName: String, eq: List<JsonElement>): List<StoredRow> {
    val index = requireIndex(tableDef, indexName)
    if (eq.size != index.fields.size) {
        throw badRequest("index '$indexName' expects ${index.fields.size} eq value(s), got ${eq.size}")
    }
    val typed = eq.mapIndexed { i, v -> coerceIndexValue(tableDef, index.fields[i], v) }
    val matches = ArrayList<StoredRow>()
    for ((key, row) in s.docs) {
        if (key.table != tableName || row.deletedAt != null) continue
        val allMatch =
            index.fields.indices.all { i ->
                val v = row.doc[index.fields[i]]
                v != null && !isNullValue(v) && jsonEq(v, typed[i])
            }
        if (allMatch) matches += StoredRow(row.id, row.doc, row.version, row.createdAt, row.deletedAt)
    }
    return matches
}

/** Enqueues a scheduled txn; interval validates its everyMs and dueAt follows the engine's due-time math. */
internal fun scheduleJob(s: Store, txn: Transaction, whenSpec: ScheduleWhen, external: Boolean): String {
    var everyMs: Long? = null
    if (whenSpec is ScheduleWhen.Interval) {
        if (whenSpec.everyMs <= 0) throw badRequest("everyMs must be positive")
        if (whenSpec.everyMs > MAX_EVERY_MS) throw badRequest("everyMs must be at most $MAX_EVERY_MS")
        everyMs = whenSpec.everyMs
    }
    val id = s.newIdLocked()
    val now = s.now()
    var kind = ScheduleKindMem.OneShot
    var cron: String? = null
    var tz: String? = null
    when (whenSpec) {
        is ScheduleWhen.Cron -> {
            kind = ScheduleKindMem.Cron
            cron = whenSpec.expr
            tz = whenSpec.tz
        }
        is ScheduleWhen.Interval -> kind = ScheduleKindMem.Interval
        else -> {}
    }
    val dueAt =
        when (whenSpec) {
            is ScheduleWhen.AfterMs -> now + whenSpec.ms
            is ScheduleWhen.RunAt -> whenSpec.ms
            is ScheduleWhen.Cron -> now + CRON_STEP_MS
            is ScheduleWhen.Interval -> now + whenSpec.everyMs
        }
    s.scheduledJobs.add(
        ScheduledJob(id = id, kind = kind, txn = txn, dueAt = dueAt, cron = cron, tz = tz, everyMs = everyMs, createdAt = now, external = external),
    )
    return id
}

/** Removes the job; false when the id is missing. */
internal fun cancelScheduleLocked(s: Store, id: String): Boolean = s.scheduledJobs.removeIf { it.id == id }
