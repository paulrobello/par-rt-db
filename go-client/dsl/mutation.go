// go-client/dsl/mutation.go
package dsl

// Mirrors rust-client/src/mutation.rs::Mutation — the chainable builder
// producing wire.Transaction. Consume it as
// dsl.NewMutation().Insert(...).Build().

import (
	"encoding/json"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// Mutation is the chainable transaction builder.
type Mutation struct {
	t wire.Transaction
}

// NewMutation starts an empty transaction.
func NewMutation() Mutation {
	return Mutation{}
}

// Insert queues an insert step; result is the new id.
func (m Mutation) Insert(table string, doc wire.JSONValue) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepInsert{Table: table, Doc: doc})
	return m
}

// InsertMany queues a bulk insert (at most 1000 docs; atomic); result
// {"ids": [...]}.
func (m Mutation) InsertMany(table string, docs []wire.JSONValue) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepInsertMany{Table: table, Docs: docs})
	return m
}

// UpsertMany queues a bulk upsert resolved per doc through the full equality
// on index (each doc must carry the indexed fields); result
// {"ids": [...], "inserted": [...]}.
func (m Mutation) UpsertMany(table, index string, docs []wire.JSONValue) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepUpsertMany{Table: table, Index: index, Docs: docs})
	return m
}

// Patch queues a merge-into step; result null.
func (m Mutation) Patch(table, id string, fields wire.JSONValue) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepPatch{Table: table, ID: id, Fields: fields})
	return m
}

// AdjustCounter queues an atomic counter delta; result null.
func (m Mutation) AdjustCounter(table, id, field string, delta int64, min, max *int64, expected wire.JSONValue) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepAdjustCounter{
		Table: table, ID: id, Field: field, Delta: delta, Min: min, Max: max, Expected: expected,
	})
	return m
}

// Replace queues a full-document overwrite; result null.
func (m Mutation) Replace(table, id string, doc wire.JSONValue) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepReplace{Table: table, ID: id, Doc: doc})
	return m
}

// Delete queues a delete step; result null.
func (m Mutation) Delete(table, id string) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepDelete{Table: table, ID: id})
	return m
}

// Undelete queues a restore-of-soft-deleted-row step; result null.
func (m Mutation) Undelete(table, id string) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepUndelete{Table: table, ID: id})
	return m
}

// ExpectVersion queues the version precondition.
func (m Mutation) ExpectVersion(table, id string, version int64) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepExpectVersion{Table: table, ID: id, Version: version})
	return m
}

// ExpectFields queues exact field assertions without writing the live row.
func (m Mutation) ExpectFields(table, id string, expected ...wire.FieldExpectation) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepExpectFields{Table: table, ID: id, Expected: expected})
	return m
}

// ExpectBefore requires execution and the final commit decision before an exclusive server-clock epoch-ms deadline.
func (m Mutation) ExpectBefore(deadlineMs int64) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepExpectBefore{DeadlineMs: deadlineMs})
	return m
}

// ExpectAbsent queues the no-matching-row precondition on an index prefix.
func (m Mutation) ExpectAbsent(table, index string, eq ...wire.JSONValue) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepExpectAbsent{Table: table, Index: index, Eq: eq})
	return m
}

// ExpectNoMatch queues the filter-absence precondition: no visible live row
// in table may match filter, else the transaction aborts PRECONDITION_FAILED.
func (m Mutation) ExpectNoMatch(table string, filter wire.FilterExpr) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepExpectNoMatch{Table: table, Filter: filter})
	return m
}

// InsertIf queues a conditional insert: ExpectNoMatch(table, filter) followed
// by an insert of doc. A doc that is not JSON-encodable is a programming
// error and panics.
func (m Mutation) InsertIf(table string, filter wire.FilterExpr, doc map[string]any) Mutation {
	return m.ExpectNoMatch(table, filter).Insert(table, anyObject(doc))
}

// Upsert queues the insert-or-patch step keyed by an index eq-prefix.
func (m Mutation) Upsert(table, index string, eq []wire.JSONValue, insert, patch wire.JSONValue) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepUpsert{Table: table, Index: index, Eq: eq, Insert: insert, Patch: patch})
	return m
}

// PatchByQuery queues a filtered patch; at most limit rows (nil = server default).
func (m Mutation) PatchByQuery(table string, filter wire.FilterExpr, patch wire.JSONValue, limit *int) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepPatchByQuery{Table: table, Filter: filter, Patch: patch, Limit: limit})
	return m
}

// DeleteByQuery queues a filtered delete; same limit semantics.
func (m Mutation) DeleteByQuery(table string, filter wire.FilterExpr, limit *int) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepDeleteByQuery{Table: table, Filter: filter, Limit: limit})
	return m
}

// Schedule queues a scheduled nested transaction.
func (m Mutation) Schedule(when wire.ScheduleWhen, txn Mutation) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepSchedule{When: when, Txn: txn.Build()})
	return m
}

// ScheduleExternal queues an external scheduled nested transaction: one the
// server never executes — an application worker claims it via
// POST /api/schedule/claim and finalizes it with the returned
// leaseGeneration fencing token.
func (m Mutation) ScheduleExternal(when wire.ScheduleWhen, txn Mutation) Mutation {
	external := true
	m.t.Steps = append(m.t.Steps, wire.StepSchedule{When: when, Txn: txn.Build(), External: &external})
	return m
}

// CancelSchedule queues a schedule cancellation.
func (m Mutation) CancelSchedule(id string) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepCancelSchedule{ID: id})
	return m
}

// StartWorkflow queues a workflow run start.
func (m Mutation) StartWorkflow(spec wire.WorkflowSpec) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepStartWorkflow{Spec: spec})
	return m
}

// CancelWorkflow queues a workflow run cancellation.
func (m Mutation) CancelWorkflow(id string) Mutation {
	m.t.Steps = append(m.t.Steps, wire.StepCancelWorkflow{ID: id})
	return m
}

// Build returns the wire transaction (shallow copy).
func (m Mutation) Build() wire.Transaction {
	return m.t
}

// anyObject converts a plain Go map to a wire object via its JSON encoding.
func anyObject(doc map[string]any) wire.JSONValue {
	b, err := json.Marshal(doc)
	if err != nil {
		panic("dsl: InsertIf doc is not JSON-encodable: " + err.Error())
	}
	v, err := wire.UnmarshalJSON(b)
	if err != nil {
		panic("dsl: InsertIf doc: " + err.Error())
	}
	return v
}
