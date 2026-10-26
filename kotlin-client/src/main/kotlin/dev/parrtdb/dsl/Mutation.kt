package dev.parrtdb.dsl

import dev.parrtdb.wire.FieldExpectation
import dev.parrtdb.wire.FilterExpr
import dev.parrtdb.wire.ScheduleWhen
import dev.parrtdb.wire.Step
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.WorkflowSpec

/** Immutable fluent builder for an atomic transaction (a list of [Step]s). */
class Mutation private constructor(private val steps: List<Step>) {
    constructor() : this(emptyList())

    private fun add(step: Step): Mutation = Mutation(steps + step)

    fun insert(table: String, doc: Any?): Mutation = add(Step.Insert(table, json(doc)))

    fun insertMany(table: String, docs: List<Any?>): Mutation = add(Step.InsertMany(table, docs.map { json(it) }))

    fun upsertMany(table: String, index: String, docs: List<Any?>): Mutation = add(Step.UpsertMany(table, index, docs.map { json(it) }))

    fun patch(table: String, id: String, fields: Any?): Mutation = add(Step.Patch(table, id, json(fields)))

    fun adjustCounter(table: String, id: String, field: String, delta: Long, min: Long? = null, max: Long? = null, expected: Any? = null): Mutation =
        add(Step.AdjustCounter(table, id, field, delta, min, max, expected?.let { json(it) }))

    fun replace(table: String, id: String, doc: Any?): Mutation = add(Step.Replace(table, id, json(doc)))

    fun delete(table: String, id: String): Mutation = add(Step.Delete(table, id))

    fun undelete(table: String, id: String): Mutation = add(Step.Undelete(table, id))

    fun expectVersion(table: String, id: String, version: Long): Mutation = add(Step.ExpectVersion(table, id, version))

    fun expectFields(table: String, id: String, vararg expected: FieldExpectation): Mutation = add(Step.ExpectFields(table, id, expected.toList()))

    fun expectBefore(deadlineMs: Long): Mutation = add(Step.ExpectBefore(deadlineMs))

    fun expectAbsent(table: String, index: String, vararg eq: Any?): Mutation = add(Step.ExpectAbsent(table, index, eq.map { json(it) }))

    fun expectNoMatch(table: String, filter: FilterExpr): Mutation = add(Step.ExpectNoMatch(table, filter))

    /** Conditional insert: aborts the transaction when [filter] already matches, otherwise inserts [doc]. */
    fun insertIf(table: String, filter: FilterExpr, doc: Any?): Mutation = expectNoMatch(table, filter).insert(table, doc)

    fun upsert(table: String, index: String, eq: List<Any?>, insert: Any?, patch: Any?): Mutation =
        add(Step.Upsert(table, index, eq.map { json(it) }, json(insert), json(patch)))

    fun patchByQuery(table: String, filter: FilterExpr, patch: Any?, limit: Int? = null): Mutation = add(Step.PatchByQuery(table, filter, json(patch), limit))

    fun deleteByQuery(table: String, filter: FilterExpr, limit: Int? = null): Mutation = add(Step.DeleteByQuery(table, filter, limit))

    fun schedule(whenSpec: ScheduleWhen, txn: Mutation): Mutation = add(Step.Schedule(whenSpec, txn.build()))

    /** Schedules a job claimed by an external worker instead of the server's own executor. */
    fun scheduleExternal(whenSpec: ScheduleWhen, txn: Mutation): Mutation = add(Step.Schedule(whenSpec, txn.build(), external = true))

    fun cancelSchedule(id: String): Mutation = add(Step.CancelSchedule(id))

    fun startWorkflow(spec: WorkflowSpec): Mutation = add(Step.StartWorkflow(spec))

    fun cancelWorkflow(id: String): Mutation = add(Step.CancelWorkflow(id))

    fun build(): Transaction = Transaction(steps)
}

/** Entry point: `mutation().insert("tasks", obj("title" to "x")).build()`. */
fun mutation(): Mutation = Mutation()
