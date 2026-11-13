// go-client/dsl/mutation_extra_test.go
package dsl

import (
	"encoding/json"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// InsertMany queues one bulk-insert step.
func TestMutationInsertMany(t *testing.T) {
	txn := NewMutation().InsertMany("items", []wire.JSONValue{
		wire.Object{"name": wire.String("a")},
		wire.Object{"name": wire.String("b")},
	}).Build()
	assertWireEqual(t, txn, `{"steps":[{"op":"insertMany","table":"items","docs":[`+
		`{"name":"a"},{"name":"b"}]}]}`)
}

// UpsertMany resolves each doc through the index.
func TestMutationUpsertMany(t *testing.T) {
	txn := NewMutation().UpsertMany("items", "by_name", []wire.JSONValue{
		wire.Object{"name": wire.String("a")},
	}).Build()
	assertWireEqual(t, txn, `{"steps":[{"op":"upsertMany","table":"items","index":"by_name","docs":[{"name":"a"}]}]}`)
}

// AdjustCounter carries the delta, optional min/max and expected value.
func TestMutationAdjustCounter(t *testing.T) {
	min := int64(0)
	max := int64(100)
	txn := NewMutation().AdjustCounter("counters", "c1", "n", 5, &min, &max, wire.Number("10")).Build()
	assertWireEqual(t, txn, `{"steps":[{"op":"adjustCounter","table":"counters","id":"c1","field":"n",`+
		`"delta":5,"min":0,"max":100,"expected":10}]}`)

	// nil min/max/expected are omitted from the wire.
	txn = NewMutation().AdjustCounter("counters", "c1", "n", -1, nil, nil, nil).Build()
	assertWireEqual(t, txn, `{"steps":[{"op":"adjustCounter","table":"counters","id":"c1","field":"n","delta":-1}]}`)
}

// Replace overwrites the full document.
func TestMutationReplace(t *testing.T) {
	txn := NewMutation().Replace("items", "i1", wire.Object{"n": wire.Number("2")}).Build()
	assertWireEqual(t, txn, `{"steps":[{"op":"replace","table":"items","id":"i1","doc":{"n":2}}]}`)
}

// Undelete restores a soft-deleted row.
func TestMutationUndelete(t *testing.T) {
	txn := NewMutation().Undelete("items", "i1").Build()
	assertWireEqual(t, txn, `{"steps":[{"op":"undelete","table":"items","id":"i1"}]}`)
}

// Version and field preconditions queue as steps.
func TestMutationExpectVersionAndFields(t *testing.T) {
	txn := NewMutation().
		ExpectVersion("items", "i1", 7).
		ExpectFields("items", "i1",
			wire.FieldExpectation{Path: []string{"n"}, Value: wire.Number("1")},
			wire.FieldExpectation{Path: []string{"nested", "x"}, Value: wire.Bool(true)}).
		Build()
	assertWireEqual(t, txn, `{"steps":[`+
		`{"op":"expectVersion","table":"items","id":"i1","version":7},`+
		`{"op":"expectFields","table":"items","id":"i1","expected":[`+
		`{"path":["n"],"value":1},{"path":["nested","x"],"value":true}]}]}`)
}

// ExpectBefore pins the commit deadline.
func TestMutationExpectBefore(t *testing.T) {
	txn := NewMutation().ExpectBefore(1234567890).Build()
	assertWireEqual(t, txn, `{"steps":[{"op":"expectBefore","deadlineMs":1234567890}]}`)
}

// Upsert queues insert-or-patch keyed by an index eq-prefix.
func TestMutationUpsert(t *testing.T) {
	txn := NewMutation().Upsert("items", "by_name",
		[]wire.JSONValue{wire.String("a")},
		wire.Object{"name": wire.String("a"), "n": wire.Number("1")},
		wire.Object{"n": wire.Number("2")}).Build()
	assertWireEqual(t, txn, `{"steps":[{"op":"upsert","table":"items","index":"by_name","eq":["a"],`+
		`"insert":{"name":"a","n":1},"patch":{"n":2}}]}`)
}

// PatchByQuery/DeleteByQuery carry filter and optional limit; nil limit is
// omitted.
func TestMutationByQuery(t *testing.T) {
	limit := 5
	txn := NewMutation().PatchByQuery("items", Eq("done", wire.Bool(true)),
		wire.Object{"done": wire.Bool(false)}, &limit).Build()
	assertWireEqual(t, txn, `{"steps":[{"op":"patchByQuery","table":"items",`+
		`"filter":{"op":"eq","field":"done","value":true},"patch":{"done":false},"limit":5}]}`)

	txn = NewMutation().DeleteByQuery("items", Lt("n", wire.Number("3")), nil).Build()
	assertWireEqual(t, txn, `{"steps":[{"op":"deleteByQuery","table":"items",`+
		`"filter":{"op":"lt","field":"n","value":3}}]}`)
}

// Schedule nests the built transaction of the inner mutation.
func TestMutationScheduleNestsTxn(t *testing.T) {
	inner := NewMutation().Insert("items", wire.Object{"name": wire.String("a")})
	txn := NewMutation().Schedule(wire.WhenAfterMs{Ms: 500}, inner).Build()
	assertWireEqual(t, txn, `{"steps":[{"op":"schedule","when":{"type":"afterMs","ms":500},`+
		`"txn":{"steps":[{"op":"insert","table":"items","doc":{"name":"a"}}]}}]}`)
}

// CancelSchedule, StartWorkflow and CancelWorkflow queue their ops.
func TestMutationScheduleAndWorkflowOps(t *testing.T) {
	txn := NewMutation().
		CancelSchedule("s1").
		StartWorkflow(wire.WorkflowSpec{Name: "wf", Steps: []wire.WorkflowStepSpec{
			{Txn: &wire.Transaction{Steps: []wire.Step{
				wire.StepInsert{Table: "items", Doc: wire.Object{"name": wire.String("a")}},
			}}},
		}}).
		CancelWorkflow("w1").
		Build()
	assertWireEqual(t, txn, `{"steps":[`+
		`{"op":"cancelSchedule","id":"s1"},`+
		`{"op":"startWorkflow","spec":{"name":"wf","steps":[{"txn":{"steps":[{"op":"insert","table":"items","doc":{"name":"a"}}]}}]}},`+
		`{"op":"cancelWorkflow","id":"w1"}]}`)
}

// Chaining preserves step order across every queued op; each step's wire
// body carries its tagged op.
func TestMutationStepOrderPreserved(t *testing.T) {
	txn := NewMutation().
		Insert("t", wire.Object{}).
		Patch("t", "a", wire.Object{}).
		Delete("t", "b").
		Undelete("t", "b").
		ExpectVersion("t", "a", 1).
		Build()
	want := []string{"insert", "patch", "delete", "undelete", "expectVersion"}
	if len(txn.Steps) != len(want) {
		t.Fatalf("step count %d, want %d", len(txn.Steps), len(want))
	}
	for i, w := range want {
		b, err := json.Marshal(txn.Steps[i])
		if err != nil {
			t.Fatal(err)
		}
		if !bytesHas(b, `"op":"`+w+`"`) {
			t.Fatalf("step %d: want op %q in %s", i, w, b)
		}
	}
}
