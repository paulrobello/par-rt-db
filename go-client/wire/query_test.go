// go-client/wire/query_test.go
package wire

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestQueryExactJSON(t *testing.T) {
	take := 10
	b, err := json.Marshal(Query{Table: "items", Take: &take})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"table":"items","take":10}` {
		t.Fatalf("got %s", b)
	}
	b, err = json.Marshal(Query{Table: "items", Unique: true, VectorSearch: &VectorSearchQuery{
		Index:  "by_v",
		Vector: []float32{0.1, 0.2},
		Limit:  5,
	}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytesContains(b, `"vectorSearch":`) {
		t.Fatalf("camelCase rename lost: %s", b)
	}
	b, err = json.Marshal(Query{Table: "items"})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"table":"items"}` {
		t.Fatalf("omitempty leaked: %s", b)
	}
}

func TestQueryCasingPins(t *testing.T) {
	desc := OrderDesc
	cursor := "c1"
	num := 5
	b, err := json.Marshal(Query{Table: "items", Order: &desc, Paginate: &Paginate{Cursor: &cursor, NumItems: num}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytesContains(b, `"order":"desc"`) || !bytesContains(b, `"numItems":5`) || !bytesContains(b, `"cursor":"c1"`) {
		t.Fatalf("order/paginate casing wrong: %s", b)
	}
	b, err = json.Marshal(Query{Table: "items", Paginate: &Paginate{NumItems: 5}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytesContains(b, `"paginate":{"numItems":5}`) {
		t.Fatalf("absent cursor must be omitted: %s", b)
	}
	b, err = json.Marshal(Query{Table: "items", Aggregate: &AggregateSpec{Op: AggSum}})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"table":"items","aggregate":{"op":"sum","groupBy":false}}` {
		t.Fatalf("groupBy must always serialize: %s", b)
	}
}

func TestQueryRoundTrip(t *testing.T) {
	gtv := JSONValue(Number("3"))
	desc := OrderDesc
	in := Query{
		Table: "items",
		Index: strptr("by_n"),
		Eq:    []JSONValue{Number("1"), Number("2")},
		Gt:    &gtv,
		Order: &desc,
		Take:  intptr(10),
		Filter: FilterAnd{Exprs: []FilterExpr{
			FilterEq{Field: "a", Value: Number("1")},
			FilterOlderThan{Field: "completedAt", Ms: 1000},
		}},
		VectorSearch: &VectorSearchQuery{Index: "by_v", Vector: []float32{1, 2}, Limit: 3,
			Filter: FilterExists{Field: "ok"}},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out Query
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	rb, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != string(rb) {
		t.Fatalf("query round trip drifted:\n%s\n%s", b, rb)
	}
}

func TestStrictDecodePins(t *testing.T) {
	var p Paginate
	if err := json.Unmarshal([]byte(`{"numItems":5,"bogus":1}`), &p); err == nil {
		t.Fatal("Paginate must reject unknown fields")
	}
	var agg AggregateGroup
	if err := json.Unmarshal([]byte(`{"key":"k1","value":42}`), &agg); err != nil {
		t.Fatal(err)
	}
	if agg.Key != JSONValue(String("k1")) || agg.Value != JSONValue(Number("42")) {
		t.Fatalf("group row decode: %+v", agg)
	}
	var h HybridSearchQuery
	if err := json.Unmarshal([]byte(`{"query":"q","vector":[1],"limit":3,"bogus":1}`), &h); err == nil {
		t.Fatal("HybridSearchQuery must reject unknown fields")
	}
}

func strptr(s string) *string { return &s }
func intptr(i int) *int       { return &i }
func bytesContains(b []byte, s string) bool {
	return bytes.Contains(b, []byte(s))
}
