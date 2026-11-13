// go-client/dsl/query_extra_test.go
package dsl

import (
	"encoding/json"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// Get targets a point read; eq from WithIndex is still emitted.
func TestTableQueryGet(t *testing.T) {
	b, err := json.Marshal(NewTableQuery("items").WithIndex("by_name", wire.String("a")).Get("i1").Build())
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"get":"i1"`) || !bytesHas(b, `"eq":["a"]`) {
		t.Fatalf("get shape: %s", b)
	}
}

// Eq appends binds beyond those bound by WithIndex.
func TestTableQueryEqAppend(t *testing.T) {
	b, err := json.Marshal(NewTableQuery("items").WithIndex("by_a_b", wire.String("a")).Eq(wire.String("b")).Build())
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"eq":["a","b"]`) {
		t.Fatalf("eq append shape: %s", b)
	}
}

// Exclusive bounds marshal under gt/lt and stay distinct from gte/lte.
func TestTableQueryExclusiveBounds(t *testing.T) {
	gt := wire.Number("0")
	lt := wire.Number("10")
	b, err := json.Marshal(NewTableQuery("items").WithIndex("by_n").Gt(gt).Lt(lt).Build())
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"gt":0`) || !bytesHas(b, `"lt":10`) {
		t.Fatalf("exclusive bounds shape: %s", b)
	}
	if bytesHas(b, `"gte":`) || bytesHas(b, `"lte":`) {
		t.Fatalf("inclusive bounds must be omitted: %s", b)
	}
}

// OrderDesc emits the explicit desc order.
func TestTableQueryOrderDesc(t *testing.T) {
	b, err := json.Marshal(NewTableQuery("items").WithIndex("by_n").OrderDesc().Build())
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"order":"desc"`) {
		t.Fatalf("order desc shape: %s", b)
	}
}

// Count/First/Unique/Distinct set their boolean terminals.
func TestTableQueryTerminals(t *testing.T) {
	q := NewTableQuery("items").Unique().First().Count().Distinct().Build()
	if !q.Unique || !q.First || !q.Count || !q.Distinct {
		t.Fatalf("terminals: %+v", q)
	}
}

// Aggregate builds the legacy single-op spec with boolean groupBy.
func TestTableQueryAggregateLegacy(t *testing.T) {
	grouped := NewTableQuery("items").WithIndex("by_status").Aggregate(wire.AggSum, true).Build()
	if grouped.Aggregate == nil || grouped.Aggregate.Op != wire.AggSum ||
		grouped.Aggregate.GroupBy != wire.GroupByBool(true) {
		t.Fatalf("grouped aggregate: %+v", grouped.Aggregate)
	}
	ungrouped := NewTableQuery("items").WithIndex("by_status").Aggregate(wire.AggCount, false).Build()
	if ungrouped.Aggregate == nil || ungrouped.Aggregate.GroupBy != wire.GroupByBool(false) {
		t.Fatalf("ungrouped aggregate: %+v", ungrouped.Aggregate)
	}
}

// Having adds bounds keyed by aggregate name across calls (a repeated name
// replaces its bounds — the map is keyed by name); it is a no-op before any
// aggregate terminal.
func TestTableQueryHaving(t *testing.T) {
	// No aggregate yet: Having must not set one.
	none := NewTableQuery("items").Having("total", wire.HavingCmp{}).Build()
	if none.Aggregate != nil {
		t.Fatalf("having without aggregate must be a no-op: %+v", none.Aggregate)
	}

	gt := 3.0
	lte := 100.0
	q := NewTableQuery("items").
		WithIndex("by_status", wire.String("todo")).
		Aggregates(map[string]wire.AggregateOp{"total": wire.AggSum, "n": wire.AggCount}, nil).
		Having("total", wire.HavingCmp{Gt: &gt}).
		Having("n", wire.HavingCmp{Lte: &lte}).
		Build()
	spec := q.Aggregate
	if spec == nil || spec.Having == nil {
		t.Fatalf("aggregate spec: %+v", spec)
	}
	totalCmp, ok := spec.Having["total"]
	if !ok || totalCmp.Gt == nil || *totalCmp.Gt != 3 {
		t.Fatalf("total having: %+v", spec.Having["total"])
	}
	nCmp, ok := spec.Having["n"]
	if !ok || nCmp.Lte == nil || *nCmp.Lte != 100 {
		t.Fatalf("n having: %+v", spec.Having["n"])
	}
	b, err := json.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"having":{"n":{"lte":100},"total":{"gt":3}}`) {
		t.Fatalf("having wire shape: %s", b)
	}

	// Same-name bounds replace: last call wins.
	replaced := NewTableQuery("items").
		WithIndex("by_status").
		Aggregates(map[string]wire.AggregateOp{"total": wire.AggSum}, nil).
		Having("total", wire.HavingCmp{Gt: &gt}).
		Having("total", wire.HavingCmp{Lte: &lte}).
		Build()
	final := replaced.Aggregate.Having["total"]
	if final.Gt != nil || final.Lte == nil || *final.Lte != 100 {
		t.Fatalf("same-name having must replace: %+v", final)
	}
}

// Filter overrides (last call wins) and lands under the filter key.
func TestTableQueryFilterOverrides(t *testing.T) {
	q := NewTableQuery("items").
		Filter(Eq("a", wire.Bool(true))).
		Filter(Eq("b", wire.Bool(true))).
		Build()
	b, err := json.Marshal(q)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		Filter map[string]any `json:"filter"`
	}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.Filter["field"] != "b" {
		t.Fatalf("last filter must win: %s", b)
	}
}

// Search, VectorSearch and HybridSearch terminals land under their keys.
func TestTableQuerySearchTerminals(t *testing.T) {
	b, err := json.Marshal(NewTableQuery("docs").
		Search(wire.SearchQuery{Index: "by_text", Query: "needle"}).
		Build())
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"search":{"index":"by_text","query":"needle"}`) {
		t.Fatalf("search shape: %s", b)
	}
	if bytesHas(b, "vectorSearch") {
		t.Fatalf("search must not leak vectorSearch: %s", b)
	}

	b, err = json.Marshal(NewTableQuery("docs").
		VectorSearch(wire.VectorSearchQuery{Index: "by_v", Vector: wire.Vector{1, 2}, Limit: 5}).
		Build())
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"vectorSearch":{"index":"by_v","vector":[1.0,2.0],"limit":5}`) {
		t.Fatalf("vectorSearch shape: %s", b)
	}

	k := 4
	searchIdx := "t"
	b, err = json.Marshal(NewTableQuery("docs").
		HybridSearch(wire.HybridSearchQuery{Query: "q", Vector: wire.Vector{1}, Limit: 3, SearchIndex: &searchIdx, K: &k}).
		Build())
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"hybridSearch":{"query":"q","vector":[1.0],"limit":3,"searchIndex":"t","k":4}`) {
		t.Fatalf("hybridSearch shape: %s", b)
	}
}

// Fields projects the named user fields.
func TestTableQueryFields(t *testing.T) {
	b, err := json.Marshal(NewTableQuery("items").Fields("a", "b").Build())
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"fields":["a","b"]`) {
		t.Fatalf("fields shape: %s", b)
	}
}
