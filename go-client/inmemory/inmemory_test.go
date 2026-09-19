// Cross-task fixtures for the in-memory engine tests, mirroring rust
// in_memory/tests/mod.rs (test_schema, items_table, the deterministic
// clock/RNG client constructor).
package inmemory

import (
	"fmt"
	"sync"

	"github.com/paulrobello/par-rt-db/go-client/dsl"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// buildSchema applies the caller's table declarations to one builder.
func buildSchema(fn func(b *dsl.SchemaBuilder)) wire.JSONValue {
	b := dsl.DefineSchema()
	fn(b)
	return b.Build()
}

// testSchema mirrors ts-client's in_memory.test.ts fixture: one items table
// with two string fields, a number, an optional, three btree indexes, and a
// search index.
func testSchema() wire.JSONValue {
	return buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(t *dsl.TableBuilder) {
			t.Field("name", dsl.Str()).
				Field("status", dsl.Str()).
				Field("order", dsl.Num()).
				Field("note", dsl.Optional(dsl.Str())).
				Index("by_name", "name").
				Index("by_status", "status").
				Index("by_status_and_order", "status", "order").
				SearchIndex("by_content", "", "name")
		})
	})
}

// parsedTestSchema is the typed view of testSchema.
func parsedTestSchema(t testingTB) *SchemaDef {
	s, err := parseSchema(testSchema())
	if err != nil {
		t.Fatalf("parse test schema: %v", err)
	}
	return s
}

func itemsTable(t testingTB, schema *SchemaDef) *TableDef {
	td, ok := schema.Tables["items"]
	if !ok {
		t.Fatalf("items table missing from schema")
	}
	return td
}

// testingTB is the subset of testing.TB the fixtures need (keeps helper
// signatures honest for both T and B).
type testingTB interface {
	Helper()
	Fatalf(format string, args ...any)
}

// newTestStore is the deterministic-clock store: post-incrementing
// epoch-millis clock from 1.7e12 and a constant 0 RNG, with testSchema
// pushed.
func newTestStore(t testingTB) *Store {
	var mu sync.Mutex
	counter := int64(1_700_000_000_000)
	s := NewStore().
		WithClock(func() int64 {
			mu.Lock()
			defer mu.Unlock()
			v := counter
			counter++
			return v
		}).
		WithRandom(func() float64 { return 0.0 })
	if err := s.PushSchema(testSchema()); err != nil {
		t.Fatalf("push test schema: %v", err)
	}
	return s
}

// docObj builds a wire.Object from pairs.
func docObj(kv ...any) wire.Object {
	out := wire.Object{}
	for i := 0; i+1 < len(kv); i += 2 {
		out[kv[i].(string)] = mustJSON(kv[i+1])
	}
	return out
}

// mustJSON converts Go literals (string, bool, int64, float64, nil,
// []any, map[string]any) into wire values.
func mustJSON(v any) wire.JSONValue {
	switch t := v.(type) {
	case nil:
		return wire.Null{}
	case bool:
		return wire.Bool(t)
	case string:
		return wire.String(t)
	case int:
		return wire.Number(formatI64(int64(t)))
	case int64:
		return wire.Number(formatI64(t))
	case float64:
		return wire.Number(floatToJSONText(t))
	default:
		panic("mustJSON: unsupported kind")
	}
}

func floatToJSONText(f float64) string {
	if f == float64(int64(f)) {
		return formatI64(int64(f))
	}
	return formatFloatText(f)
}

// seedRow inserts a stored row directly — the evaluator tests exercise
// EvalQuery in isolation; the write path gets its own end-to-end tests with
// the writes task (rust's ports go through mutate, which does not exist
// until then; the row shape here is mergeDoc's input contract).
var seedCounter uint64

func seedRow(t testingTB, s *Store, table string, createdAt int64, doc wire.Object) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	seedCounter++
	// 32-char lowercase-hex id, unique per seeded row (the real clock +
	// constant RNG would collide same-millisecond rows in the docs map).
	id := fmt.Sprintf("%032x", seedCounter)
	s.docs[rowKey{Table: table, ID: id}] = &StoredRow{
		ID:        id,
		Doc:       doc,
		Version:   1,
		CreatedAt: createdAt,
	}
	return id
}

// seedThreeRows mirrors tests/mod.rs seed_query_rows: order 3, 1, 2 so an
// ascending sort differs from insertion order.
func seedThreeRows(t testingTB, s *Store) {
	seedRow(t, s, "items", 100, docObj("name", "n3", "status", "todo", "order", int64(3)))
	seedRow(t, s, "items", 200, docObj("name", "n1", "status", "todo", "order", int64(1)))
	seedRow(t, s, "items", 300, docObj("name", "n2", "status", "todo", "order", int64(2)))
}
