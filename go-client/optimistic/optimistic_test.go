// go-client/optimistic/optimistic_test.go
package optimistic

import (
	"context"
	"errors"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func doc(id string, n wire.JSONValue) wire.Object {
	return wire.Object(map[string]wire.JSONValue{"_id": wire.String(id), "n": n})
}

func TestInsertOverlaysCachedArray(t *testing.T) {
	q := wire.Query{Table: "items"}
	cached := wire.Array{doc("a", wire.Number("1"))}
	txn := wire.Transaction{Steps: []wire.Step{
		wire.StepInsert{Table: "items", Doc: wire.Object(map[string]wire.JSONValue{"n": wire.Number("2")})},
	}}
	proj := Project(q, cached, txn, 1000)
	if proj.Kind != ProjectionOverlaid {
		t.Fatalf("kind %v", proj.Kind)
	}
	arr := proj.Value.(wire.Array)
	if len(arr) != 2 {
		t.Fatalf("len %d", len(arr))
	}
	newDoc := arr[1].(wire.Object)
	id, _ := newDoc["_id"].(wire.String)
	if id == "" || id == "a" {
		t.Fatalf("synthetic id missing: %v", newDoc["_id"])
	}
	if _, ok := newDoc["_creationTime"].(wire.Number); !ok {
		t.Fatalf("_creationTime missing: %v", newDoc)
	}
}

func TestDeleteRemovesAndFailedApplierRollsBack(t *testing.T) {
	q := wire.Query{Table: "items"}
	cached := wire.Array{doc("a", wire.Number("1")), doc("b", wire.Number("2"))}
	txn := wire.Transaction{Steps: []wire.Step{
		wire.StepDelete{Table: "items", ID: "b"},
	}}
	proj := Project(q, cached, txn, 1000)
	arr := proj.Value.(wire.Array)
	if len(arr) != 1 {
		t.Fatalf("projected len %d", len(arr))
	}
	// Layer: applier error must roll the cache back
	boom := errors.New("boom")
	layer := New(failingApplier{err: boom})
	layer.Observe(q, cached)
	_, err := layer.Apply(context.Background(), txn, []wire.Query{q}, "")
	if !errors.Is(err, boom) {
		t.Fatalf("apply err %v", err)
	}
	layer.mu.Lock()
	got := layer.cache[mustKey(t, q)]
	layer.mu.Unlock()
	arr2 := got.(wire.Array)
	if len(arr2) != 2 {
		t.Fatalf("rollback lost rows: %d", len(arr2))
	}
}

func TestNonArrayQuerySkips(t *testing.T) {
	q := wire.Query{Table: "items", Count: true}
	cached := wire.Number("5")
	txn := wire.Transaction{Steps: []wire.Step{
		wire.StepInsert{Table: "items", Doc: wire.Object(map[string]wire.JSONValue{})},
	}}
	if proj := Project(q, cached, txn, 1000); proj.Kind != ProjectionSkip {
		t.Fatalf("count query must skip, got %v", proj.Kind)
	}
}

type failingApplier struct{ err error }

func (f failingApplier) Mutate(ctx context.Context, txn wire.Transaction, idem string) ([]wire.StepResult, error) {
	return nil, f.err
}

func mustKey(t *testing.T, q wire.Query) string {
	t.Helper()
	k, err := canonicalKey(q)
	if err != nil {
		t.Fatal(err)
	}
	return k
}
