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

func TestUpdatedAtFieldStampsInt64AsString(t *testing.T) {
	// Pins the T20-review Critical: an int64 updatedAtField takes the
	// decimal-STRING wire form on every stamp (rust stamp_updated_at); a
	// JSON number would fail validateValue's int64 arm.
	schema := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("t", func(tb *dsl.TableBuilder) {
			tb.Field("n", dsl.Int64()).UpdatedAtField("n")
		})
	})
	s := NewStore()
	if err := s.PushSchema(schema); err != nil {
		t.Fatalf("push: %v", err)
	}
	table := s.SchemaSnapshot().Tables["t"]
	doc, err := doInsert(s, "t", table, docObj())
	if err != nil {
		t.Fatalf("insert: %v (updatedAt=%v n-kind=%v)", err, table.UpdatedAtField, table.Fields["n"].Kind)
	}
	row := s.docs[rowKey{Table: "t", ID: doc}]
	stamped, ok := row.Doc["n"].(wire.String)
	if !ok {
		t.Fatalf("int64 updatedAtField must store a decimal string, got %T", row.Doc["n"])
	}
	if _, valid := parseI64(string(stamped)); !valid {
		t.Fatalf("stamp not a decimal string: %s", stamped)
	}
}

func TestApplyPatchRejectsAutoIncrementValueReshuffle(t *testing.T) {
	// Pins the T20-review Important: rust's auto-immutability check is
	// strict serde equality — an integer-spelled stored counter does not
	// equal a float-spelled 5.0, so re-submitting it reshaped must fail.
	s := NewStore()
	if err := s.PushSchema(buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("t", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).Field("seq", dsl.Int64()).AutoIncrementField("seq")
		})
	})); err != nil {
		t.Fatalf("push: %v", err)
	}
	table := s.SchemaSnapshot().Tables["t"]
	id, err := doInsert(s, "t", table, docObj("name", "a"))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	row := s.docs[rowKey{Table: "t", ID: id}]
	// Round-trip the exact stored spelling: accepted.
	if _, err := applyPatch(table, row.Doc, docObj("name", "b"), 0); err != nil {
		t.Fatalf("round-trip patch must pass: %v", err)
	}
	// A float-spelled reshuffle of the same number: rejected.
	if _, err := applyPatch(table, row.Doc, docObj("seq", 5.0), 0); err == nil {
		t.Fatal("float-spelled autoIncrement patch must be rejected")
	}
}
