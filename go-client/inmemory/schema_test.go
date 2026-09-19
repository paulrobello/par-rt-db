// Schema-push tests — the Go port of rust in_memory/tests/schema.rs (the
// push half; the additive-push-preserves-docs case moves to the write tests
// since it needs Mutate/Query).
package inmemory

import (
	"strings"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/dsl"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestPushSchemaStoresTheSchema(t *testing.T) {
	s := NewStore()
	if err := s.PushSchema(testSchema()); err != nil {
		t.Fatalf("push: %v", err)
	}
	stored := s.SchemaSnapshot()
	if stored == nil {
		t.Fatal("schema not installed")
	}
	if _, ok := stored.Tables["items"]; !ok {
		t.Fatal("items table missing after push")
	}
}

func TestPushSchemaRejectsADestructiveSecondPush(t *testing.T) {
	s := newTestStore(t)
	onlyOther := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("solo", func(tb *dsl.TableBuilder) { tb.Field("x", dsl.Num()) })
	})
	err := s.PushSchema(onlyOther)
	if err == nil {
		t.Fatal("expected error")
	}
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("got code: %v", err)
	}
	if !strings.Contains(re.Message, "removed table 'items'") {
		t.Fatalf("got: %s", re.Message)
	}
	// The rejected push left the prior schema in place.
	stored := s.SchemaSnapshot()
	if _, ok := stored.Tables["items"]; !ok {
		t.Fatal("prior schema was clobbered")
	}
	if _, ok := stored.Tables["solo"]; ok {
		t.Fatal("rejected push partially applied")
	}
}

func TestPushSchemaAllowsWideningALiteralUnion(t *testing.T) {
	narrow := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("title", dsl.Str()).
				Field("status", dsl.Union(dsl.FieldLiteral(wire.String("backlog")), dsl.FieldLiteral(wire.String("done"))))
		})
	})
	s := NewStore()
	if err := s.PushSchema(narrow); err != nil {
		t.Fatalf("first push: %v", err)
	}
	wide := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("title", dsl.Str()).
				Field("status", dsl.Union(
					dsl.FieldLiteral(wire.String("backlog")),
					dsl.FieldLiteral(wire.String("done")),
					dsl.FieldLiteral(wire.String("archived"))))
		})
	})
	if err := s.PushSchema(wide); err != nil {
		t.Fatalf("widening push: %v", err)
	}
	status, ok := s.SchemaSnapshot().Tables["items"].Fields["status"]
	if !ok || status.Kind != "union" || len(status.Variants) != 3 {
		t.Fatalf("widened field not folded: %+v", status)
	}
}

func TestPushSchemaRejectsNarrowingALiteralUnion(t *testing.T) {
	wide := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("title", dsl.Str()).
				Field("status", dsl.Union(
					dsl.FieldLiteral(wire.String("backlog")),
					dsl.FieldLiteral(wire.String("done")),
					dsl.FieldLiteral(wire.String("archived"))))
		})
	})
	s := NewStore()
	if err := s.PushSchema(wide); err != nil {
		t.Fatalf("first push: %v", err)
	}
	narrow := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("title", dsl.Str()).
				Field("status", dsl.Union(dsl.FieldLiteral(wire.String("backlog")), dsl.FieldLiteral(wire.String("done"))))
		})
	})
	err := s.PushSchema(narrow)
	if err == nil {
		t.Fatal("expected error")
	}
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("got code: %v", err)
	}
	if !strings.Contains(re.Message, "changed type of field 'items.status'") {
		t.Fatalf("got: %s", re.Message)
	}
	status := s.SchemaSnapshot().Tables["items"].Fields["status"]
	if status.Kind != "union" || len(status.Variants) != 3 {
		t.Fatal("rejected push clobbered the stored schema")
	}
}

// itemsSchemaVariant rebuilds testSchema omitting the named indexes and then
// applying the caller's modifier (which re-adds a mutated form of one).
func itemsSchemaVariant(omit map[string]bool, modify func(*dsl.TableBuilder)) wire.JSONValue {
	return buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).
				Field("status", dsl.Str()).
				Field("order", dsl.Num()).
				Field("note", dsl.Optional(dsl.Str()))
			if !omit["by_name"] {
				tb.Index("by_name", "name")
			}
			if !omit["by_status"] {
				tb.Index("by_status", "status")
			}
			if !omit["by_status_and_order"] {
				tb.Index("by_status_and_order", "status", "order")
			}
			if !omit["by_content"] {
				tb.SearchIndex("by_content", "", "name")
			}
			if modify != nil {
				modify(tb)
			}
		})
	})
}

func expectPushError(t *testing.T, schema wire.JSONValue, wantCode rtdberrors.ErrorCode, wantMsg string) {
	t.Helper()
	s := newTestStore(t)
	err := s.PushSchema(schema)
	if err == nil {
		t.Fatalf("expected error containing %q", wantMsg)
	}
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok {
		t.Fatalf("not an RtDbError: %v", err)
	}
	if re.Code != wantCode {
		t.Fatalf("code: got %v want %v (%s)", re.Code, wantCode, re.Message)
	}
	if !strings.Contains(re.Message, wantMsg) {
		t.Fatalf("message: got %q want contains %q", re.Message, wantMsg)
	}
}

func TestPushSchemaRejectsFlippingAnIndexToUnique(t *testing.T) {
	expectPushError(t, itemsSchemaVariant(map[string]bool{"by_name": true}, func(tb *dsl.TableBuilder) {
		tb.Index("by_name", "name").Unique()
	}), rtdberrors.CodeBadRequest, "changed uniqueness of index 'by_name'")
}

func TestPushSchemaRejectsChangingAnIndexPartialPredicate(t *testing.T) {
	expectPushError(t, itemsSchemaVariant(map[string]bool{"by_status": true}, func(tb *dsl.TableBuilder) {
		tb.Index("by_status", "status").Where(dsl.Eq("status", wire.String("todo")))
	}), rtdberrors.CodeBadRequest, "changed partial predicate of index 'by_status'")
}

func TestPushSchemaRejectsChangingASearchIndexLanguage(t *testing.T) {
	expectPushError(t, itemsSchemaVariant(map[string]bool{"by_content": true}, func(tb *dsl.TableBuilder) {
		tb.SearchIndex("by_content", "spanish", "name")
	}), rtdberrors.CodeBadRequest, "changed language of search index 'by_content'")
}

func TestPushSchemaRejectsTTLOnANonNumericField(t *testing.T) {
	bad := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).
				Field("order", dsl.Num()).
				Index("by_order", "order").
				TTL("name", nil)
		})
	})
	expectPushError(t, bad, rtdberrors.CodeSchemaViolation, "ttl.field 'name' must be a number or bigint field")
}

func TestPushSchemaRejectsTTLWithoutAMatchingBtreeIndex(t *testing.T) {
	bad := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).
				Field("order", dsl.Num()).
				Index("by_name", "name").
				TTL("order", nil)
		})
	})
	expectPushError(t, bad, rtdberrors.CodeSchemaViolation,
		"ttl.field 'order' requires a single-field, non-unique, non-partial btree index on it")
}

func TestPushSchemaRejectsAnIndexOverANonIndexableField(t *testing.T) {
	bad := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).
				Field("tags", dsl.ArrayOf(dsl.Str())).
				Index("by_name", "name").
				Index("by_tags", "tags")
		})
	})
	expectPushError(t, bad, rtdberrors.CodeSchemaViolation, "field type 'array' is not indexable")
}
