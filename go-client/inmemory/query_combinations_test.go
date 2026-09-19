// The QA-001/QA-002 cross-client combination matrix — a generated Go
// transcription of rust-client/tests/query_combinations.rs's 124 cases
// (case-for-case; the ts and server runners carry the same matrix). Each
// case mutates a base {"table":"items"} query, strict-decodes it into a
// wire.Query, and runs it on the seeded in-memory engine. Accept = no
// error; Reject = BAD_REQUEST; any other error fails the test (a cascade
// gap must surface as BAD_REQUEST, not an internal fault).

package inmemory

import (
	"encoding/json"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/dsl"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

const qcID = "0123456789abcdef0123456789abcdef"

func matrixSchema() wire.JSONValue {
	return buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("title", dsl.Str()).
				Field("body", dsl.Str()).
				Field("count", dsl.Num()).
				Field("embedding", dsl.VectorF(3)).
				Index("by_title", "title").
				Index("by_title_count", "title", "count").
				SearchIndex("search_body", "", "title", "body").
				VectorIndex("by_embedding", "embedding", 3, nil, "cosine")
		})
	})
}

func newMatrixClient(t *testing.T) *Store {
	t.Helper()
	s := NewStore().WithRandom(func() float64 { return 0.0 })
	if err := s.PushSchema(matrixSchema()); err != nil {
		t.Fatalf("push matrix schema: %v", err)
	}
	return s
}

type qcStmt struct {
	set bool
	key string
	val any
}

var qcCases = []struct {
	name   string
	stmts  []qcStmt
	accept bool
}{

	{
		name: "solo: get",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
		},
		accept: true,
	},
	{
		name:   "solo: collect",
		stmts:  []qcStmt{},
		accept: true,
	},
	{
		name: "solo: index",
		stmts: []qcStmt{
			{set: true, key: "index", val: "by_title"},
		},
		accept: true,
	},
	{
		name: "solo: eq",
		stmts: []qcStmt{
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "eq", val: []any{"x"}},
		},
		accept: true,
	},
	{
		name: "solo: gt",
		stmts: []qcStmt{
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "gt", val: "x"},
		},
		accept: true,
	},
	{
		name: "solo: gte",
		stmts: []qcStmt{
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "gte", val: "x"},
		},
		accept: true,
	},
	{
		name: "solo: lt",
		stmts: []qcStmt{
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "lt", val: "x"},
		},
		accept: true,
	},
	{
		name: "solo: lte",
		stmts: []qcStmt{
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "lte", val: "x"},
		},
		accept: true,
	},
	{
		name: "solo: order",
		stmts: []qcStmt{
			{set: true, key: "order", val: "asc"},
		},
		accept: true,
	},
	{
		name: "solo: take",
		stmts: []qcStmt{
			{set: true, key: "take", val: float64(1)},
		},
		accept: true,
	},
	{
		name: "solo: unique",
		stmts: []qcStmt{
			{set: true, key: "unique", val: true},
		},
		accept: true,
	},
	{
		name: "solo: first",
		stmts: []qcStmt{
			{set: true, key: "first", val: true},
		},
		accept: true,
	},
	{
		name: "solo: count",
		stmts: []qcStmt{
			{set: true, key: "count", val: true},
		},
		accept: true,
	},
	{
		name: "solo: distinct",
		stmts: []qcStmt{
			{set: true, key: "distinct", val: true},
			{set: true, key: "index", val: "by_title"},
		},
		accept: true,
	},
	{
		name: "solo: paginate",
		stmts: []qcStmt{
			{set: true, key: "paginate", val: map[string]any{"numItems": 1}},
		},
		accept: true,
	},
	{
		name: "solo: filter",
		stmts: []qcStmt{
			{set: true, key: "filter", val: map[string]any{"op": "eq", "field": "title", "value": "x"}},
		},
		accept: true,
	},
	{
		name: "solo: search",
		stmts: []qcStmt{
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
		},
		accept: true,
	},
	{
		name: "solo: vectorSearch",
		stmts: []qcStmt{
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
		},
		accept: true,
	},
	{
		name: "solo: hybridSearch",
		stmts: []qcStmt{
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
		},
		accept: true,
	},
	{
		name: "get+index",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
			{set: true, key: "index", val: "by_title"},
		},
		accept: false,
	},
	{
		name: "get+eq",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
			{set: true, key: "eq", val: []any{"x"}},
		},
		accept: false,
	},
	{
		name: "get+gt",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
			{set: true, key: "gt", val: "x"},
		},
		accept: false,
	},
	{
		name: "get+gte",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
			{set: true, key: "gte", val: "x"},
		},
		accept: false,
	},
	{
		name: "get+lt",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
			{set: true, key: "lt", val: "x"},
		},
		accept: false,
	},
	{
		name: "get+lte",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
			{set: true, key: "lte", val: "x"},
		},
		accept: false,
	},
	{
		name: "get+order",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
			{set: true, key: "order", val: "asc"},
		},
		accept: false,
	},
	{
		name: "get+take",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
			{set: true, key: "take", val: float64(1)},
		},
		accept: false,
	},
	{
		name: "get+unique",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
			{set: true, key: "unique", val: true},
		},
		accept: false,
	},
	{
		name: "get+first",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
			{set: true, key: "first", val: true},
		},
		accept: false,
	},
	{
		name: "get+count",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
			{set: true, key: "count", val: true},
		},
		accept: false,
	},
	{
		name: "get+paginate",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
			{set: true, key: "paginate", val: map[string]any{"numItems": 1}},
		},
		accept: false,
	},
	{
		name: "get+filter",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
			{set: true, key: "filter", val: map[string]any{"op": "eq", "field": "title", "value": "x"}},
		},
		accept: false,
	},
	{
		name: "get+search",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
		},
		accept: false,
	},
	{
		name: "get+vectorSearch",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
		},
		accept: false,
	},
	{
		name: "get+hybridSearch",
		stmts: []qcStmt{
			{set: true, key: "get", val: qcID},
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
		},
		accept: false,
	},
	{
		name: "unique+take",
		stmts: []qcStmt{
			{set: true, key: "unique", val: true},
			{set: true, key: "take", val: float64(1)},
		},
		accept: false,
	},
	{
		name: "unique+order",
		stmts: []qcStmt{
			{set: true, key: "unique", val: true},
			{set: true, key: "order", val: "asc"},
		},
		accept: false,
	},
	{
		name: "first+unique",
		stmts: []qcStmt{
			{set: true, key: "first", val: true},
			{set: true, key: "unique", val: true},
		},
		accept: false,
	},
	{
		name: "first+take",
		stmts: []qcStmt{
			{set: true, key: "first", val: true},
			{set: true, key: "take", val: float64(1)},
		},
		accept: false,
	},
	{
		name: "count+unique",
		stmts: []qcStmt{
			{set: true, key: "count", val: true},
			{set: true, key: "unique", val: true},
		},
		accept: false,
	},
	{
		name: "count+take",
		stmts: []qcStmt{
			{set: true, key: "count", val: true},
			{set: true, key: "take", val: float64(1)},
		},
		accept: false,
	},
	{
		name: "count+first",
		stmts: []qcStmt{
			{set: true, key: "count", val: true},
			{set: true, key: "first", val: true},
		},
		accept: false,
	},
	{
		name: "count+order",
		stmts: []qcStmt{
			{set: true, key: "count", val: true},
			{set: true, key: "order", val: "asc"},
		},
		accept: false,
	},
	{
		name: "count+distinct",
		stmts: []qcStmt{
			{set: true, key: "count", val: true},
			{set: true, key: "distinct", val: true},
		},
		accept: false,
	},
	{
		name: "distinct+get",
		stmts: []qcStmt{
			{set: true, key: "distinct", val: true},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "get", val: qcID},
		},
		accept: false,
	},
	{
		name: "distinct+take",
		stmts: []qcStmt{
			{set: true, key: "distinct", val: true},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "take", val: float64(1)},
		},
		accept: false,
	},
	{
		name: "distinct+unique",
		stmts: []qcStmt{
			{set: true, key: "distinct", val: true},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "unique", val: true},
		},
		accept: false,
	},
	{
		name: "distinct+first",
		stmts: []qcStmt{
			{set: true, key: "distinct", val: true},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "first", val: true},
		},
		accept: false,
	},
	{
		name: "distinct+count",
		stmts: []qcStmt{
			{set: true, key: "distinct", val: true},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "count", val: true},
		},
		accept: false,
	},
	{
		name: "distinct+order",
		stmts: []qcStmt{
			{set: true, key: "distinct", val: true},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "order", val: "asc"},
		},
		accept: false,
	},
	{
		name: "distinct+paginate",
		stmts: []qcStmt{
			{set: true, key: "distinct", val: true},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "paginate", val: map[string]any{"numItems": 1}},
		},
		accept: false,
	},
	{
		name: "distinct+search",
		stmts: []qcStmt{
			{set: true, key: "distinct", val: true},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
		},
		accept: false,
	},
	{
		name: "distinct+vectorSearch",
		stmts: []qcStmt{
			{set: true, key: "distinct", val: true},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
		},
		accept: false,
	},
	{
		name: "distinct+hybridSearch",
		stmts: []qcStmt{
			{set: true, key: "distinct", val: true},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
		},
		accept: false,
	},
	{
		name: "solo: aggregate",
		stmts: []qcStmt{
			{set: true, key: "aggregate", val: map[string]any{"op": "min"}},
			{set: true, key: "index", val: "by_title"},
		},
		accept: true,
	},
	{
		name: "aggregate+get",
		stmts: []qcStmt{
			{set: true, key: "aggregate", val: map[string]any{"op": "min"}},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "get", val: qcID},
		},
		accept: false,
	},
	{
		name: "aggregate+take",
		stmts: []qcStmt{
			{set: true, key: "aggregate", val: map[string]any{"op": "min"}},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "take", val: float64(1)},
		},
		accept: false,
	},
	{
		name: "aggregate+unique",
		stmts: []qcStmt{
			{set: true, key: "aggregate", val: map[string]any{"op": "min"}},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "unique", val: true},
		},
		accept: false,
	},
	{
		name: "aggregate+first",
		stmts: []qcStmt{
			{set: true, key: "aggregate", val: map[string]any{"op": "min"}},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "first", val: true},
		},
		accept: false,
	},
	{
		name: "aggregate+count",
		stmts: []qcStmt{
			{set: true, key: "aggregate", val: map[string]any{"op": "min"}},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "count", val: true},
		},
		accept: false,
	},
	{
		name: "aggregate+distinct",
		stmts: []qcStmt{
			{set: true, key: "aggregate", val: map[string]any{"op": "min"}},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "distinct", val: true},
		},
		accept: false,
	},
	{
		name: "aggregate+order",
		stmts: []qcStmt{
			{set: true, key: "aggregate", val: map[string]any{"op": "min"}},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "order", val: "asc"},
		},
		accept: false,
	},
	{
		name: "aggregate+paginate",
		stmts: []qcStmt{
			{set: true, key: "aggregate", val: map[string]any{"op": "min"}},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "paginate", val: map[string]any{"numItems": 1}},
		},
		accept: false,
	},
	{
		name: "aggregate+search",
		stmts: []qcStmt{
			{set: true, key: "aggregate", val: map[string]any{"op": "min"}},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
		},
		accept: false,
	},
	{
		name: "aggregate+vectorSearch",
		stmts: []qcStmt{
			{set: true, key: "aggregate", val: map[string]any{"op": "min"}},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
		},
		accept: false,
	},
	{
		name: "aggregate+hybridSearch",
		stmts: []qcStmt{
			{set: true, key: "aggregate", val: map[string]any{"op": "min"}},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
		},
		accept: false,
	},
	{
		name: "compose: aggregate+eq",
		stmts: []qcStmt{
			{set: true, key: "aggregate", val: map[string]any{"op": "sum"}},
			{set: true, key: "index", val: "by_title_count"},
			{set: true, key: "eq", val: []any{"x"}},
		},
		accept: true,
	},
	{
		name: "compose: aggregate+filter",
		stmts: []qcStmt{
			{set: true, key: "aggregate", val: map[string]any{"op": "min"}},
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "filter", val: map[string]any{"op": "eq", "field": "title", "value": "x"}},
		},
		accept: true,
	},
	{
		name: "paginate+count",
		stmts: []qcStmt{
			{set: true, key: "paginate", val: map[string]any{"numItems": 1}},
			{set: true, key: "count", val: true},
		},
		accept: false,
	},
	{
		name: "paginate+unique",
		stmts: []qcStmt{
			{set: true, key: "paginate", val: map[string]any{"numItems": 1}},
			{set: true, key: "unique", val: true},
		},
		accept: false,
	},
	{
		name: "paginate+first",
		stmts: []qcStmt{
			{set: true, key: "paginate", val: map[string]any{"numItems": 1}},
			{set: true, key: "first", val: true},
		},
		accept: false,
	},
	{
		name: "paginate+take",
		stmts: []qcStmt{
			{set: true, key: "paginate", val: map[string]any{"numItems": 1}},
			{set: true, key: "take", val: float64(1)},
		},
		accept: false,
	},
	{
		name: "gt+gte",
		stmts: []qcStmt{
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "gt", val: "x"},
			{set: true, key: "gte", val: "x"},
		},
		accept: false,
	},
	{
		name: "lt+lte",
		stmts: []qcStmt{
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "lt", val: "x"},
			{set: true, key: "lte", val: "x"},
		},
		accept: false,
	},
	{
		name: "vectorSearch+index",
		stmts: []qcStmt{
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "index", val: "by_title"},
		},
		accept: false,
	},
	{
		name: "vectorSearch+eq",
		stmts: []qcStmt{
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "eq", val: []any{"x"}},
		},
		accept: false,
	},
	{
		name: "vectorSearch+gt",
		stmts: []qcStmt{
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "gt", val: "x"},
		},
		accept: false,
	},
	{
		name: "vectorSearch+gte",
		stmts: []qcStmt{
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "gte", val: "x"},
		},
		accept: false,
	},
	{
		name: "vectorSearch+lt",
		stmts: []qcStmt{
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "lt", val: "x"},
		},
		accept: false,
	},
	{
		name: "vectorSearch+lte",
		stmts: []qcStmt{
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "lte", val: "x"},
		},
		accept: false,
	},
	{
		name: "vectorSearch+order",
		stmts: []qcStmt{
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "order", val: "asc"},
		},
		accept: false,
	},
	{
		name: "vectorSearch+unique",
		stmts: []qcStmt{
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "unique", val: true},
		},
		accept: false,
	},
	{
		name: "vectorSearch+first",
		stmts: []qcStmt{
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "first", val: true},
		},
		accept: false,
	},
	{
		name: "vectorSearch+count",
		stmts: []qcStmt{
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "count", val: true},
		},
		accept: false,
	},
	{
		name: "vectorSearch+paginate",
		stmts: []qcStmt{
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "paginate", val: map[string]any{"numItems": 1}},
		},
		accept: true,
	},
	{
		name: "vectorSearch+filter",
		stmts: []qcStmt{
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "filter", val: map[string]any{"op": "eq", "field": "title", "value": "x"}},
		},
		accept: false,
	},
	{
		name: "vectorSearch+search",
		stmts: []qcStmt{
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
		},
		accept: false,
	},
	{
		name: "vectorSearch+take",
		stmts: []qcStmt{
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "take", val: float64(1)},
		},
		accept: false,
	},
	{
		name: "vectorSearch+hybridSearch",
		stmts: []qcStmt{
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
		},
		accept: false,
	},
	{
		name: "search+index",
		stmts: []qcStmt{
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
			{set: true, key: "index", val: "by_title"},
		},
		accept: false,
	},
	{
		name: "search+eq",
		stmts: []qcStmt{
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
			{set: true, key: "eq", val: []any{"x"}},
		},
		accept: false,
	},
	{
		name: "search+gt",
		stmts: []qcStmt{
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
			{set: true, key: "gt", val: "x"},
		},
		accept: false,
	},
	{
		name: "search+gte",
		stmts: []qcStmt{
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
			{set: true, key: "gte", val: "x"},
		},
		accept: false,
	},
	{
		name: "search+lt",
		stmts: []qcStmt{
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
			{set: true, key: "lt", val: "x"},
		},
		accept: false,
	},
	{
		name: "search+lte",
		stmts: []qcStmt{
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
			{set: true, key: "lte", val: "x"},
		},
		accept: false,
	},
	{
		name: "search+order",
		stmts: []qcStmt{
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
			{set: true, key: "order", val: "asc"},
		},
		accept: false,
	},
	{
		name: "search+unique",
		stmts: []qcStmt{
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
			{set: true, key: "unique", val: true},
		},
		accept: false,
	},
	{
		name: "search+first",
		stmts: []qcStmt{
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
			{set: true, key: "first", val: true},
		},
		accept: false,
	},
	{
		name: "search+count",
		stmts: []qcStmt{
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
			{set: true, key: "count", val: true},
		},
		accept: false,
	},
	{
		name: "search+paginate",
		stmts: []qcStmt{
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
			{set: true, key: "paginate", val: map[string]any{"numItems": 1}},
		},
		accept: true,
	},
	{
		name: "search+filter",
		stmts: []qcStmt{
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
			{set: true, key: "filter", val: map[string]any{"op": "eq", "field": "title", "value": "x"}},
		},
		accept: false,
	},
	{
		name: "search+vectorSearch",
		stmts: []qcStmt{
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
			{set: true, key: "vectorSearch", val: map[string]any{"index": "by_embedding", "vector": []float32{0, 0, 0}, "limit": 1}},
		},
		accept: false,
	},
	{
		name: "search+hybridSearch",
		stmts: []qcStmt{
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
		},
		accept: false,
	},
	{
		name: "hybridSearch+index",
		stmts: []qcStmt{
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "index", val: "by_title"},
		},
		accept: false,
	},
	{
		name: "hybridSearch+eq",
		stmts: []qcStmt{
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "eq", val: []any{"x"}},
		},
		accept: false,
	},
	{
		name: "hybridSearch+gt",
		stmts: []qcStmt{
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "gt", val: "x"},
		},
		accept: false,
	},
	{
		name: "hybridSearch+gte",
		stmts: []qcStmt{
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "gte", val: "x"},
		},
		accept: false,
	},
	{
		name: "hybridSearch+lt",
		stmts: []qcStmt{
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "lt", val: "x"},
		},
		accept: false,
	},
	{
		name: "hybridSearch+lte",
		stmts: []qcStmt{
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "lte", val: "x"},
		},
		accept: false,
	},
	{
		name: "hybridSearch+order",
		stmts: []qcStmt{
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "order", val: "asc"},
		},
		accept: false,
	},
	{
		name: "hybridSearch+unique",
		stmts: []qcStmt{
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "unique", val: true},
		},
		accept: false,
	},
	{
		name: "hybridSearch+first",
		stmts: []qcStmt{
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "first", val: true},
		},
		accept: false,
	},
	{
		name: "hybridSearch+count",
		stmts: []qcStmt{
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "count", val: true},
		},
		accept: false,
	},
	{
		name: "hybridSearch+distinct",
		stmts: []qcStmt{
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "distinct", val: true},
		},
		accept: false,
	},
	{
		name: "hybridSearch+aggregate",
		stmts: []qcStmt{
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "aggregate", val: map[string]any{"op": "min"}},
		},
		accept: false,
	},
	{
		name: "hybridSearch+paginate",
		stmts: []qcStmt{
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "paginate", val: map[string]any{"numItems": 1}},
		},
		accept: true,
	},
	{
		name: "hybridSearch+filter",
		stmts: []qcStmt{
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "filter", val: map[string]any{"op": "eq", "field": "title", "value": "x"}},
		},
		accept: false,
	},
	{
		name: "hybridSearch+take",
		stmts: []qcStmt{
			{set: true, key: "hybridSearch", val: map[string]any{"query": "x", "vector": []float32{0, 0, 0}, "limit": 1}},
			{set: true, key: "take", val: float64(1)},
		},
		accept: false,
	},
	{
		name: "compose: search+take",
		stmts: []qcStmt{
			{set: true, key: "search", val: map[string]any{"index": "search_body", "query": "x"}},
			{set: true, key: "take", val: float64(1)},
		},
		accept: true,
	},
	{
		name: "compose: index+take",
		stmts: []qcStmt{
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "take", val: float64(1)},
		},
		accept: true,
	},
	{
		name: "compose: index+eq+take",
		stmts: []qcStmt{
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "eq", val: []any{"x"}},
			{set: true, key: "take", val: float64(1)},
		},
		accept: true,
	},
	{
		name: "compose: index+order",
		stmts: []qcStmt{
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "order", val: "asc"},
		},
		accept: true,
	},
	{
		name: "compose: index+gt+lt",
		stmts: []qcStmt{
			{set: true, key: "index", val: "by_title"},
			{set: true, key: "gt", val: "a"},
			{set: true, key: "lt", val: "z"},
		},
		accept: true,
	},
	{
		name: "compose: take+filter",
		stmts: []qcStmt{
			{set: true, key: "take", val: float64(1)},
			{set: true, key: "filter", val: map[string]any{"op": "eq", "field": "title", "value": "x"}},
		},
		accept: true,
	},
}

func TestQueryCombinationMatrix(t *testing.T) {
	s := newMatrixClient(t)
	for _, tc := range qcCases {
		t.Run(tc.name, func(t *testing.T) {
			q := map[string]any{"table": "items"}
			for _, st := range tc.stmts {
				if st.set {
					q[st.key] = st.val
				} else {
					delete(q, st.key)
				}
			}
			blob, err := json.Marshal(q)
			if err != nil {
				t.Fatal(err)
			}
			var query wire.Query
			if err := json.Unmarshal(blob, &query); err != nil {
				t.Fatalf("query does not parse: %v (%s)", err, blob)
			}
			_, err = EvalQuery(s, query)
			if tc.accept {
				if err != nil {
					t.Fatalf("must accept, got: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatal("must reject")
			}
			re, ok := err.(*rtdberrors.RtDbError)
			if !ok || re.Code != rtdberrors.CodeBadRequest {
				t.Fatalf("reject must be BAD_REQUEST, got: %v", err)
			}
		})
	}
}
