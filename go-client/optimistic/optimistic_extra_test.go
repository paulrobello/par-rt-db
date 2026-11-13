// go-client/optimistic/optimistic_extra_test.go
package optimistic

// Covers the projection branches the main test does not hit: expectBefore
// skip, get point-read overlays (patch/replace/delete + mismatched ids),
// filtered-array delete-only overlays and its skip arms, unfiltered-array
// skip arms (non-array cache, take window, non-object doc, bulk steps), the
// stepTable default arm, and the by-id helpers against non-object/missing
// rows.

import (
	"context"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func obj(pairs ...any) wire.Object {
	o := wire.Object{}
	for i := 0; i+1 < len(pairs); i += 2 {
		o[pairs[i].(string)] = pairs[i+1].(wire.JSONValue)
	}
	return o
}

func pStr(s string) *string { return &s }

func TestExpectBeforeSkips(t *testing.T) {
	q := wire.Query{Table: "items"}
	txn := wire.Transaction{Steps: []wire.Step{
		wire.StepExpectBefore{DeadlineMs: 100},
	}}
	if proj := Project(q, wire.Array{}, txn, 1000); proj.Kind != ProjectionSkip {
		t.Fatalf("expectBefore must skip, got %v", proj.Kind)
	}
}

func TestGetPointReadOverlays(t *testing.T) {
	cached := obj("_id", wire.String("a"), "n", wire.Number("1"), "keep", wire.String("yes"))

	t.Run("patch merges fields for the matching id", func(t *testing.T) {
		q := wire.Query{Table: "items", Get: pStr("a")}
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepPatch{Table: "items", ID: "a", Fields: obj("n", wire.Number("9"))},
		}}
		proj := Project(q, cached, txn, 1000)
		if proj.Kind != ProjectionOverlaid {
			t.Fatalf("kind %v", proj.Kind)
		}
		got := proj.Value.(wire.Object)
		if got["n"] != wire.JSONValue(wire.Number("9")) || got["keep"] != wire.JSONValue(wire.String("yes")) {
			t.Fatalf("merge wrong: %v", got)
		}
	})

	t.Run("patch for another id leaves the point read untouched", func(t *testing.T) {
		q := wire.Query{Table: "items", Get: pStr("a")}
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepPatch{Table: "items", ID: "other", Fields: obj("n", wire.Number("9"))},
			wire.StepInsert{Table: "items", Doc: obj("n", wire.Number("2"))},
		}}
		proj := Project(q, cached, txn, 1000)
		if proj.Kind != ProjectionOverlaid {
			t.Fatalf("kind %v", proj.Kind)
		}
		got := proj.Value.(wire.Object)
		if got["n"] != wire.JSONValue(wire.Number("1")) {
			t.Fatalf("point read mutated: %v optimistic", got)
		}
	})

	t.Run("replace swaps the whole body", func(t *testing.T) {
		q := wire.Query{Table: "items", Get: pStr("a")}
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepReplace{Table: "items", ID: "a", Doc: obj("n", wire.Number("7"))},
		}}
		proj := Project(q, cached, txn, 1000)
		if proj.Kind != ProjectionOverlaid {
			t.Fatalf("kind %v", proj.Kind)
		}
		got := proj.Value.(wire.Object)
		if len(got) != 1 || got["n"] != wire.JSONValue(wire.Number("7")) {
			t.Fatalf("replace wrong: %v", got)
		}
	})

	t.Run("replace with a non-object doc skips", func(t *testing.T) {
		q := wire.Query{Table: "items", Get: pStr("a")}
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepReplace{Table: "items", ID: "a", Doc: wire.Array{wire.String("nope")}},
		}}
		if proj := Project(q, cached, txn, 1000); proj.Kind != ProjectionSkip {
			t.Fatalf("non-object replace must skip, got %v", proj.Kind)
		}
	})

	t.Run("delete nulls the point read", func(t *testing.T) {
		q := wire.Query{Table: "items", Get: pStr("a")}
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepDelete{Table: "items", ID: "a"},
		}}
		proj := Project(q, cached, txn, 1000)
		if proj.Kind != ProjectionOverlaid || proj.Value != wire.JSONValue(wire.Null{}) {
			t.Fatalf("delete must project null, got %v %v", proj.Kind, proj.Value)
		}
	})

	t.Run("delete for another id keeps the doc", func(t *testing.T) {
		q := wire.Query{Table: "items", Get: pStr("a")}
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepDelete{Table: "items", ID: "other"},
		}}
		proj := Project(q, cached, txn, 1000)
		if proj.Kind != ProjectionOverlaid {
			t.Fatalf("kind %v", proj.Kind)
		}
		if _, ok := proj.Value.(wire.Object)["n"]; !ok {
			t.Fatalf("wrong-id delete must keep the doc: %v", proj.Value)
		}
	})

	t.Run("bulk steps skip", func(t *testing.T) {
		q := wire.Query{Table: "items", Get: pStr("a")}
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepUpsert{Table: "items", Index: "n", Eq: []wire.JSONValue{wire.Number("1")}, Insert: obj("n", wire.Number("1")), Patch: obj("m", wire.Number("2"))},
		}}
		if proj := Project(q, cached, txn, 1000); proj.Kind != ProjectionSkip {
			t.Fatalf("upsert on a point read must skip, got %v", proj.Kind)
		}
	})

	t.Run("non-object cache skips", func(t *testing.T) {
		q := wire.Query{Table: "items", Get: pStr("a")}
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepPatch{Table: "items", ID: "a", Fields: obj("n", wire.Number("9"))},
		}}
		if proj := Project(q, wire.String("not-an-object"), txn, 1000); proj.Kind != ProjectionSkip {
			t.Fatalf("non-object cache must skip, got %v", proj.Kind)
		}
	})
}

func TestFilteredArrayOverlays(t *testing.T) {
	cached := wire.Array{doc("a", wire.Number("1")), doc("b", wire.Number("2"))}
	eqFilter := wire.FilterEq{Field: "n", Value: wire.Number("1")}
	q := wire.Query{Table: "items", Eq: []wire.JSONValue{wire.Number("1")}}

	t.Run("delete of a cached doc overlays", func(t *testing.T) {
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepDelete{Table: "items", ID: "b"},
		}}
		proj := Project(q, cached, txn, 1000)
		if proj.Kind != ProjectionOverlaid {
			t.Fatalf("kind %v", proj.Kind)
		}
		if arr := proj.Value.(wire.Array); len(arr) != 1 || arr[0].(wire.Object)["_id"] != wire.JSONValue(wire.String("a")) {
			t.Fatalf("delete wrong: %v", arr)
		}
	})

	t.Run("non-delete steps skip", func(t *testing.T) {
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepPatch{Table: "items", ID: "a", Fields: obj("n", wire.Number("9"))},
		}}
		if proj := Project(q, cached, txn, 1000); proj.Kind != ProjectionSkip {
			t.Fatalf("patch under a filter must skip, got %v", proj.Kind)
		}
	})

	t.Run("non-array cache skips", func(t *testing.T) {
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepDelete{Table: "items", ID: "a"},
		}}
		if proj := Project(q, wire.Object{"_id": wire.String("a")}, txn, 1000); proj.Kind != ProjectionSkip {
			t.Fatalf("non-array cache must skip, got %v", proj.Kind)
		}
	})

	_ = eqFilter
}

func TestUnfilteredArraySkipArms(t *testing.T) {
	cached := wire.Array{doc("a", wire.Number("1"))}
	take := 1

	t.Run("non-array cache skips", func(t *testing.T) {
		q := wire.Query{Table: "items"}
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepInsert{Table: "items", Doc: obj("n", wire.Number("2"))},
		}}
		if proj := Project(q, wire.Number("3"), txn, 1000); proj.Kind != ProjectionSkip {
			t.Fatalf("non-array cache must skip, got %v", proj.Kind)
		}
	})

	t.Run("full take window declines insert", func(t *testing.T) {
		q := wire.Query{Table: "items", Take: &take}
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepInsert{Table: "items", Doc: obj("n", wire.Number("2"))},
		}}
		if proj := Project(q, cached, txn, 1000); proj.Kind != ProjectionSkip {
			t.Fatalf("full take window must skip, got %v", proj.Kind)
		}
	})

	t.Run("insert with a non-object doc skips", func(t *testing.T) {
		q := wire.Query{Table: "items"}
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepInsert{Table: "items", Doc: wire.Array{wire.String("nope")}},
		}}
		if proj := Project(q, cached, txn, 1000); proj.Kind != ProjectionSkip {
			t.Fatalf("non-object insert doc must skip, got %v", proj.Kind)
		}
	})

	t.Run("bulk and counter steps skip", func(t *testing.T) {
		q := wire.Query{Table: "items"}
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepUpsert{Table: "items", Index: "n", Eq: []wire.JSONValue{wire.Number("1")}, Insert: obj("n", wire.Number("1")), Patch: obj("m", wire.Number("2"))},
			wire.StepUpsertMany{Table: "items", Index: "n", Docs: []wire.JSONValue{obj("n", wire.Number("1"))}},
			wire.StepInsertMany{Table: "items", Docs: []wire.JSONValue{obj("n", wire.Number("5"))}},
			wire.StepAdjustCounter{Table: "items", ID: "a", Field: "c", Delta: 1},
		}}
		if proj := Project(q, cached, txn, 1000); proj.Kind != ProjectionSkip {
			t.Fatalf("bulk steps must skip, got %v", proj.Kind)
		}
	})
}

func TestUnfilteredArrayOverlays(t *testing.T) {
	cached := wire.Array{doc("a", wire.Number("1")), doc("b", wire.Number("2"))}

	t.Run("patch merges by id in place", func(t *testing.T) {
		q := wire.Query{Table: "items"}
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepPatch{Table: "items", ID: "a", Fields: obj("n", wire.Number("9"), "extra", wire.String("e"))},
			wire.StepPatch{Table: "other", ID: "a", Fields: obj("boom", wire.String("1"))},
		}}
		proj := Project(q, cached, txn, 1000)
		if proj.Kind != ProjectionOverlaid {
			t.Fatalf("kind %v", proj.Kind)
		}
		arr := proj.Value.(wire.Array)
		if len(arr) != 2 {
			t.Fatalf("len %d", len(arr))
		}
		a := arr[0].(wire.Object)
		if a["n"] != wire.JSONValue(wire.Number("9")) || a["extra"] != wire.JSONValue(wire.String("e")) || a["_id"] != wire.JSONValue(wire.String("a")) {
			t.Fatalf("patch wrong: %v", a)
		}
		if b := arr[1].(wire.Object); b["n"] != wire.JSONValue(wire.Number("2")) {
			t.Fatalf("sibling mutated: %v", b)
		}
	})

	t.Run("replace keeps the original id", func(t *testing.T) {
		q := wire.Query{Table: "items"}
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepReplace{Table: "items", ID: "a", Doc: obj("n", wire.Number("7"))},
		}}
		proj := Project(q, cached, txn, 1000)
		a := proj.Value.(wire.Array)[0].(wire.Object)
		if a["_id"] != wire.JSONValue(wire.String("a")) || len(a) != 2 {
			t.Fatalf("replace wrong: %v", a)
		}
	})

	t.Run("undelete leaves the cache untouched", func(t *testing.T) {
		q := wire.Query{Table: "items"}
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepUndelete{Table: "items", ID: "gone"},
		}}
		proj := Project(q, cached, txn, 1000)
		if proj.Kind != ProjectionOverlaid || len(proj.Value.(wire.Array)) != 2 {
			t.Fatalf("undelete must not disturb the cache: %+v", proj)
		}
	})

	t.Run("insert stamps synthetic system fields", func(t *testing.T) {
		q := wire.Query{Table: "items"}
		txn := wire.Transaction{Steps: []wire.Step{
			wire.StepInsert{Table: "items", Doc: obj("n", wire.Number("3"), "_id", wire.String("evil"))},
		}}
		proj := Project(q, cached, txn, 12345)
		arr := proj.Value.(wire.Array)
		ins := arr[len(arr)-1].(wire.Object)
		if id, _ := ins["_id"].(wire.String); string(id) == "evil" || id == "" {
			t.Fatalf("synthetic id not stamped: %v", ins["_id"])
		}
		if ins["_creationTime"] != wire.JSONValue(wire.Number("12345")) {
			t.Fatalf("_creationTime not stamped: %v", ins["_creationTime"])
		}
		if ins["_version"] != wire.JSONValue(wire.Number("1")) {
			t.Fatalf("_version not stamped: %v", ins["_version"])
		}
		if ins["n"] != wire.JSONValue(wire.Number("3")) {
			t.Fatalf("doc body lost: %v", ins)
		}
	})
}

func TestStepTableUnknownStep(t *testing.T) {
	// Steps with no table (expect guards) fall through stepTable's default
	// arm; Project routes expectBefore out earlier, so drive expectNoMatch
	// straight through the projection loop where it is simply skipped.
	q := wire.Query{Table: "items"}
	txn := wire.Transaction{Steps: []wire.Step{
		wire.StepExpectNoMatch{Table: "items", Filter: wire.FilterEq{Field: "n", Value: wire.Number("9")}},
		wire.StepDelete{Table: "items", ID: "a"},
	}}
	proj := Project(q, wire.Array{doc("a", wire.Number("1"))}, txn, 1000)
	if proj.Kind != ProjectionOverlaid || len(proj.Value.(wire.Array)) != 0 {
		t.Fatalf("expectNoMatch must pass through: %+v", proj)
	}
}

func TestByIDHelpersEdgeCases(t *testing.T) {
	mixed := wire.Array{
		wire.String("scalar"),
		obj("_id", wire.String("a"), "n", wire.Number("1")),
		obj("no-id", wire.String("x")),
	}

	t.Run("mergeByID ignores non-object rows and misses", func(t *testing.T) {
		clone := make(wire.Array, len(mixed))
		copy(clone, mixed)
		mergeByID(clone, "missing", obj("x", wire.Number("1")))
		if len(clone) != 3 || clone[0] != wire.JSONValue(wire.String("scalar")) {
			t.Fatalf("miss must not disturb: %v", clone)
		}
		mergeByID(clone, "a", obj("n", wire.Number("9")))
		if clone[1].(wire.Object)["n"] != wire.JSONValue(wire.Number("9")) {
			t.Fatalf("merge missed: %v", clone[1])
		}
	})

	t.Run("mergeInto ignores non-object fields", func(t *testing.T) {
		target := obj("k", wire.String("v"))
		mergeInto(target, wire.String("not-an-object"))
		if len(target) != 1 {
			t.Fatalf("non-object fields must be ignored: %v", target)
		}
	})

	t.Run("replaceByID ignores non-object rows and misses", func(t *testing.T) {
		clone := make(wire.Array, len(mixed))
		copy(clone, mixed)
		replaceByID(clone, "missing", obj("x", wire.Number("1")))
		replaceByID(clone, "a", wire.String("not-an-object"))
		if clone[1].(wire.Object)["n"] != wire.JSONValue(wire.Number("1")) {
			t.Fatalf("failed replace must not disturb: %v", clone[1])
		}
		replaceByID(clone, "a", obj("n", wire.Number("7")))
		if clone[1].(wire.Object)["n"] != wire.JSONValue(wire.Number("7")) || clone[1].(wire.Object)["_id"] != wire.JSONValue(wire.String("a")) {
			t.Fatalf("replace wrong: %v", clone[1])
		}
	})

	t.Run("removeByID keeps non-object rows", func(t *testing.T) {
		out := removeByID(mixed, "a")
		if len(out) != 2 || out[0] != wire.JSONValue(wire.String("scalar")) {
			t.Fatalf("remove wrong: %v", out)
		}
	})
}

func TestLayerObserveAndApply(t *testing.T) {
	layer := New(recordingApplier{results: []wire.StepResult{{ID: pStr("doc1")}}})
	q := wire.Query{Table: "items"}
	cached := wire.Array{doc("a", wire.Number("1"))}

	// An unmarshalable query is impossible with wire.Query today; the
	// Observe error branch cannot be triggered without one, so Observe's
	// happy path plus Apply's overlay-success and unknown-query paths are
	// exercised here.
	layer.Observe(q, cached)
	txn := wire.Transaction{Steps: []wire.Step{
		wire.StepPatch{Table: "items", ID: "a", Fields: obj("n", wire.Number("9"))},
	}}
	results, err := layer.Apply(context.Background(), txn, []wire.Query{q, {Table: "untracked"}}, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].ID == nil || *results[0].ID != "doc1" {
		t.Fatalf("results %+v", results)
	}
	// The overlay stuck in the cache.
	layer.mu.Lock()
	got := layer.cache[mustKey(t, q)]
	layer.mu.Unlock()
	if arr := got.(wire.Array); arr[0].(wire.Object)["n"] != wire.JSONValue(wire.Number("9")) {
		t.Fatalf("overlay not cached: %v", got)
	}
}

type recordingApplier struct{ results []wire.StepResult }

func (r recordingApplier) Mutate(ctx context.Context, txn wire.Transaction, idem string) ([]wire.StepResult, error) {
	return r.results, nil
}
