// go-client/dsl/query_test.go
package dsl

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestTableQueryIndexOrderTake(t *testing.T) {
	b, err := json.Marshal(NewTableQuery("items").WithIndex("by_n").OrderAsc().Take(10).Build())
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"index":"by_n"`) || !bytesHas(b, `"order":"asc"`) || !bytesHas(b, `"take":10`) {
		t.Fatalf("index/order/take shape: %s", b)
	}
	if bytesHas(b, `"eq":[]`) {
		t.Fatalf("empty eq must be omitted: %s", b)
	}
}

func TestTableQueryRange(t *testing.T) {
	gte := wire.Number("3")
	lte := wire.Number("7")
	b, err := json.Marshal(NewTableQuery("items").WithIndex("by_n").Gte(gte).Lte(lte).Build())
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"gte":3`) || !bytesHas(b, `"lte":7`) {
		t.Fatalf("range shape: %s", b)
	}
	if bytesHas(b, `"gt":`) || bytesHas(b, `"lt":`) {
		t.Fatalf("absent bounds must be omitted: %s", b)
	}
}

func TestTableQueryFilterTake(t *testing.T) {
	b, err := json.Marshal(NewTableQuery("items").Take(5).
		Filter(wire.FilterEq{Field: "done", Value: wire.Bool(true)}).Build())
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"take":5`) || !bytesHas(b, `"filter":{"field":"done","op":"eq","value":true}`) {
		t.Fatalf("filter shape: %s", b)
	}
}

func TestTableQueryPaginateCursor(t *testing.T) {
	b, err := json.Marshal(NewTableQuery("items").PaginateCursor("", 20).Build())
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"paginate":{"numItems":20}`) {
		t.Fatalf("first page must omit cursor: %s", b)
	}
	b, err = json.Marshal(NewTableQuery("items").PaginateCursor("abc", 20).Build())
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"paginate":{"cursor":"abc","numItems":20}`) {
		t.Fatalf("cursor page shape: %s", b)
	}
}

func bytesHas(b []byte, s string) bool { return bytes.Contains(b, []byte(s)) }
