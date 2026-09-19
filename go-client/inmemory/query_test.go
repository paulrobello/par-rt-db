// Query evaluator tests — representative ports of rust in_memory/tests/query.rs
// (the exhaustive semantics gate is the wire-corpus runner, Task 28).
package inmemory

import (
	"strings"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/dsl"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func mustEval(t *testing.T, s *Store, q wire.Query) wire.JSONValue {
	t.Helper()
	v, err := EvalQuery(s, q)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	return v
}

func TestQueryCollectOrdersByIndexThenCreation(t *testing.T) {
	s := newTestStore(t)
	seedThreeRows(t, s)
	v := mustEval(t, s, wire.Query{Table: "items"})
	arr, ok := v.(wire.Array)
	if !ok || len(arr) != 3 {
		t.Fatalf("collect: %v", v)
	}
	// No index: the sort falls back to _creationTime then _id — the rows were
	// seeded at 100/200/300, so creation order (n3, n1, n2) differs from the
	// `order` value order and catches a wrong-column sort.
	want := []string{"n3", "n1", "n2"}
	for i, name := range want {
		got, _ := arr[i].(wire.Object)["name"]
		if !jsonEq(got, wire.String(name)) {
			t.Fatalf("row %d: got %v want %s", i, got, name)
		}
	}
	// With the composite index and eq prefix, the unbound `order` field sorts.
	v = mustEval(t, s, wire.Query{Table: "items", Index: strPtr("by_status_and_order"),
		Eq: []wire.JSONValue{wire.String("todo")}})
	arr = v.(wire.Array)
	sorted := []string{"n1", "n2", "n3"}
	for i, name := range sorted {
		got, _ := arr[i].(wire.Object)["name"]
		if !jsonEq(got, wire.String(name)) {
			t.Fatalf("indexed row %d: got %v want %s", i, got, name)
		}
	}
	// System fields are layered on every doc.
	first := arr[0].(wire.Object)
	if _, ok := first["_id"].(wire.String); !ok {
		t.Fatal("_id missing")
	}
	if _, ok := first["_creationTime"].(wire.Number); !ok {
		t.Fatal("_creationTime missing")
	}
	if _, ok := first["_version"].(wire.Number); !ok {
		t.Fatal("_version missing")
	}
}

func TestQueryEqPrefixAndRange(t *testing.T) {
	s := newTestStore(t)
	seedThreeRows(t, s)
	seedRow(t, s, "items", 400, docObj("name", "n9", "status", "done", "order", int64(9)))
	v := mustEval(t, s, wire.Query{Table: "items", Index: strPtr("by_status_and_order"),
		Eq: []wire.JSONValue{wire.String("done")}})
	arr := v.(wire.Array)
	if len(arr) != 1 {
		t.Fatalf("eq prefix: %v", arr)
	}
	// Range on the next index field after the eq prefix.
	v = mustEval(t, s, wire.Query{Table: "items", Index: strPtr("by_status_and_order"),
		Eq: []wire.JSONValue{wire.String("todo")}, Gte: jsonPtr(wire.Number("2"))})
	arr = v.(wire.Array)
	if len(arr) != 2 {
		t.Fatalf("gte range: %v", arr)
	}
	// int64-style decimal strings in an int64 index compare numerically.
	s2 := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("nums", func(tb *dsl.TableBuilder) {
			tb.Field("bucket", dsl.Str()).Field("n", dsl.Int64()).Index("by_bucket_n", "bucket", "n")
		})
	})
	if err := NewStore().PushSchema(s2); err != nil {
		t.Fatalf("push int64 schema: %v", err)
	}
	st := NewStore()
	if err := st.PushSchema(s2); err != nil {
		t.Fatalf("push: %v", err)
	}
	seedRow(t, st, "nums", 10, docObj("bucket", "b", "n", "100"))
	seedRow(t, st, "nums", 20, docObj("bucket", "b", "n", "20"))
	seedRow(t, st, "nums", 30, docObj("bucket", "b", "n", "3"))
	v = mustEval(t, st, wire.Query{Table: "nums", Index: strPtr("by_bucket_n"),
		Eq: []wire.JSONValue{wire.String("b")}})
	// Numeric order 3 < 20 < 100, not lexicographic 100 < 20 < 3.
	arr = v.(wire.Array)
	got := []string{}
	for _, d := range arr {
		n, _ := d.(wire.Object)["n"].(wire.String)
		got = append(got, string(n))
	}
	if got[0] != "3" || got[1] != "20" || got[2] != "100" {
		t.Fatalf("int64 numeric order: %v", got)
	}
}

func TestQueryOrderDescAndTake(t *testing.T) {
	s := newTestStore(t)
	seedThreeRows(t, s)
	desc := wire.OrderDesc
	two := 2
	v := mustEval(t, s, wire.Query{Table: "items", Order: &desc, Take: &two})
	arr := v.(wire.Array)
	if len(arr) != 2 {
		t.Fatalf("take: %v", arr)
	}
	// No index: desc is by _creationTime — n2 (seeded last) leads.
	if got, _ := arr[0].(wire.Object)["name"]; !jsonEq(got, wire.String("n2")) {
		t.Fatalf("desc first: %v", got)
	}
}

func TestQueryFilterCombos(t *testing.T) {
	s := newTestStore(t)
	seedThreeRows(t, s)
	f := dsl.And(dsl.Eq("status", wire.String("todo")), dsl.Gt("order", wire.Number("1")))
	v := mustEval(t, s, wire.Query{Table: "items", Filter: f})
	if len(v.(wire.Array)) != 2 {
		t.Fatalf("and filter: %v", v)
	}
	in := dsl.In("order", wire.Number("1"), wire.Number("3"))
	v = mustEval(t, s, wire.Query{Table: "items", Filter: in})
	if len(v.(wire.Array)) != 2 {
		t.Fatalf("in filter: %v", v)
	}
	ne := dsl.Neq("status", wire.String("todo"))
	v = mustEval(t, s, wire.Query{Table: "items", Filter: ne})
	if len(v.(wire.Array)) != 0 {
		t.Fatalf("neq filter: %v", v)
	}
}

func TestQueryRejectsOlderThanInReadFilter(t *testing.T) {
	s := newTestStore(t)
	seedThreeRows(t, s)
	_, err := EvalQuery(s, wire.Query{Table: "items", Filter: dsl.OlderThan("order", 5)})
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("got: %v", err)
	}
	if !strings.Contains(re.Message, "olderThan filter is only allowed in patchByQuery/deleteByQuery filters") {
		t.Fatalf("got: %s", re.Message)
	}
}

func TestQueryCountDistinctAggregate(t *testing.T) {
	s := newTestStore(t)
	seedThreeRows(t, s)
	v := mustEval(t, s, wire.Query{Table: "items", Count: true})
	if !jsonEq(v, wire.Number("3")) {
		t.Fatalf("count: %v", v)
	}
	v = mustEval(t, s, wire.Query{Table: "items", Distinct: true, Index: strPtr("by_status_and_order"),
		Eq: []wire.JSONValue{wire.String("todo")}})
	arr := v.(wire.Array)
	if len(arr) != 3 {
		t.Fatalf("distinct: %v", arr)
	}
	// sum over the field after the eq prefix.
	v = mustEval(t, s, wire.Query{Table: "items", Index: strPtr("by_status_and_order"),
		Eq:        []wire.JSONValue{wire.String("todo")},
		Aggregate: &wire.AggregateSpec{Op: wire.AggSum}})
	if !jsonEq(v, wire.Number("6")) {
		t.Fatalf("sum: %v", v)
	}
	// groupBy count: group by status's successor field.
	v = mustEval(t, s, wire.Query{Table: "items", Index: strPtr("by_status_and_order"),
		Eq:        []wire.JSONValue{wire.String("todo")},
		Aggregate: &wire.AggregateSpec{Op: wire.AggCount, GroupBy: true}})
	arr = v.(wire.Array)
	if len(arr) != 3 {
		t.Fatalf("groupBy count: %v", arr)
	}
	g0 := arr[0].(wire.Object)
	if !jsonEq(g0["key"], wire.Number("1")) || !jsonEq(g0["value"], wire.Number("1")) {
		t.Fatalf("groupBy row 0: %v", g0)
	}
}

func TestQueryPaginateCursorStability(t *testing.T) {
	s := newTestStore(t)
	seedThreeRows(t, s)
	v := mustEval(t, s, wire.Query{Table: "items", Paginate: &wire.Paginate{NumItems: 2}})
	page1, ok := v.(wire.Object)
	if !ok {
		t.Fatalf("paginate envelope: %v", v)
	}
	docs := page1["docs"].(wire.Array)
	if len(docs) != 2 {
		t.Fatalf("page 1: %v", docs)
	}
	cur, ok := page1["nextCursor"].(wire.String)
	if !ok {
		t.Fatalf("nextCursor missing: %v", page1)
	}
	curStr := string(cur)
	v = mustEval(t, s, wire.Query{Table: "items", Paginate: &wire.Paginate{NumItems: 2, Cursor: &curStr}})
	page2 := v.(wire.Object)
	docs2 := page2["docs"].(wire.Array)
	if len(docs2) != 1 {
		t.Fatalf("page 2: %v", docs2)
	}
	// No overlap between pages.
	if jsonEq(docs[0], docs2[0]) {
		t.Fatal("page 2 repeats page 1 rows")
	}
	if _, has := page2["nextCursor"]; has {
		t.Fatal("final page must not carry nextCursor")
	}
}

func TestQueryUniqueTerminal(t *testing.T) {
	s := newTestStore(t)
	seedThreeRows(t, s)
	_, err := EvalQuery(s, wire.Query{Table: "items", Unique: true})
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodePreconditionFailed {
		t.Fatalf("got: %v", err)
	}
	seedRow(t, s, "items", 500, docObj("name", "solo", "status", "done", "order", int64(1)))
	f := dsl.Eq("status", wire.String("done"))
	v := mustEval(t, s, wire.Query{Table: "items", Unique: true, Filter: f})
	obj := v.(wire.Object)
	if !jsonEq(obj["name"], wire.String("solo")) {
		t.Fatalf("unique: %v", obj)
	}
}

func TestQueryTerminalExclusivity(t *testing.T) {
	s := newTestStore(t)
	seedThreeRows(t, s)
	// count + distinct at once → BAD_REQUEST (terminal clique).
	_, err := EvalQuery(s, wire.Query{Table: "items", Count: true, Distinct: true})
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("count+distinct: %v", err)
	}
	// order + unique → its own forbid rule.
	desc := wire.OrderDesc
	_, err = EvalQuery(s, wire.Query{Table: "items", Unique: true, Order: &desc})
	re, ok = err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("order+unique: %v", err)
	}
	// get + index → forbidden pair.
	id := "0123456789abcdef0123456789abcdef"
	_, err = EvalQuery(s, wire.Query{Table: "items", Get: &id, Index: strPtr("by_name")})
	re, ok = err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("get+index: %v", err)
	}
}

func TestQueryProjection(t *testing.T) {
	s := newTestStore(t)
	seedThreeRows(t, s)
	v := mustEval(t, s, wire.Query{Table: "items", Fields: []string{"name"}})
	arr := v.(wire.Array)
	row := arr[0].(wire.Object)
	if _, has := row["status"]; has {
		t.Fatalf("unlisted user field survived: %v", row)
	}
	if _, has := row["name"]; !has {
		t.Fatal("listed field dropped")
	}
	if _, has := row["_id"]; !has {
		t.Fatal("system field dropped")
	}
	_, err := EvalQuery(s, wire.Query{Table: "items", Fields: []string{"ghost"}})
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("unknown projection: %v", err)
	}
}

func TestQueryUnknownTableAndEqErrors(t *testing.T) {
	s := newTestStore(t)
	_, err := EvalQuery(s, wire.Query{Table: "ghost"})
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeNotFound {
		t.Fatalf("unknown table: %v", err)
	}
	// eq without an index.
	_, err = EvalQuery(s, wire.Query{Table: "items", Eq: []wire.JSONValue{wire.String("x")}})
	re, ok = err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("eq w/o index: %v", err)
	}
	// eq arity overrun.
	_, err = EvalQuery(s, wire.Query{Table: "items", Index: strPtr("by_name"),
		Eq: []wire.JSONValue{wire.String("a"), wire.String("b")}})
	re, ok = err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("eq arity: %v", err)
	}
}

func strPtr(s string) *string                  { return &s }
func jsonPtr(v wire.JSONValue) *wire.JSONValue { return &v }
