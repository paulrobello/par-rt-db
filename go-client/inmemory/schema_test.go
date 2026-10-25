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

// versionsSchema builds the artifacts fixture: `versions` is array<object>
// with a required `tag`; `size` is optional only after the widening — the
// par-artifacts consumer shape. Mirrors the server integration test.
func versionsSchema(withSize bool) wire.JSONValue {
	element := map[string]dsl.FieldType{"tag": dsl.Str()}
	if withSize {
		element["size"] = dsl.Optional(dsl.Num())
	}
	return buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("artifacts", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).
				Field("versions", dsl.ArrayOf(dsl.ObjectOf(element)))
		})
	})
}

func artifactVersions(t *testing.T, s *Store, id string) wire.Array {
	t.Helper()
	get := id
	doc, err := EvalQuery(s, wire.Query{Table: "artifacts", Get: &get})
	if err != nil {
		t.Fatalf("query artifact: %v", err)
	}
	obj, ok := doc.(wire.Object)
	if !ok {
		t.Fatalf("expected object doc, got %T", doc)
	}
	versions, ok := obj["versions"].(wire.Array)
	if !ok {
		t.Fatalf("expected versions array, got %T", obj["versions"])
	}
	return versions
}

func TestPushSchemaAcceptsAddingOptionalFieldInsideArrayObjectElements(t *testing.T) {
	// Monotonic clock: two same-millisecond inserts with a constant random
	// would mint colliding uuidv7 ids.
	counter := int64(1_700_000_000_000)
	s := NewStore().WithClock(func() int64 { counter++; return counter }).
		WithRandom(func() float64 { return 0.0 })
	if err := s.PushSchema(versionsSchema(false)); err != nil {
		t.Fatalf("first push: %v", err)
	}
	// Old row: elements lack `size`.
	oldID, err := doInsert(s, "artifacts", s.SchemaSnapshot().Tables["artifacts"],
		docObj("name", "a", "versions", wire.Array{wire.Object{"tag": wire.String("v1")}}))
	if err != nil {
		t.Fatalf("old-row insert: %v", err)
	}
	// Widened push: additive, non-destructive.
	if err := s.PushSchema(versionsSchema(true)); err != nil {
		t.Fatalf("widened push: %v", err)
	}
	// Old row still validates (optional-absent passes) and reads back.
	oldVersions := artifactVersions(t, s, oldID)
	if len(oldVersions) != 1 {
		t.Fatalf("expected 1 version, got %d", len(oldVersions))
	}
	if el, ok := oldVersions[0].(wire.Object)["tag"].(wire.String); !ok || el != "v1" {
		t.Fatalf("old row versions clobbered: %v", oldVersions)
	}
	if _, has := oldVersions[0].(wire.Object)["size"]; has {
		t.Fatal("old row unexpectedly carries size")
	}
	// New docs validate with and without the new field.
	table := s.SchemaSnapshot().Tables["artifacts"]
	withID, err := doInsert(s, "artifacts", table,
		docObj("name", "b", "versions", wire.Array{wire.Object{"tag": wire.String("v2"), "size": wire.Number("1024")}}))
	if err != nil {
		t.Fatalf("insert with size: %v", err)
	}
	withoutID, err := doInsert(s, "artifacts", table,
		docObj("name", "c", "versions", wire.Array{wire.Object{"tag": wire.String("v2")}}))
	if err != nil {
		t.Fatalf("insert without size: %v", err)
	}
	withEl := artifactVersions(t, s, withID)[0].(wire.Object)
	if _, has := withEl["size"]; !has {
		t.Fatal("doc with size lost it")
	}
	if tag, ok := withEl["tag"].(wire.String); !ok || tag != "v2" {
		t.Fatalf("doc with size tag wrong: %v", withEl)
	}
	withoutEl := artifactVersions(t, s, withoutID)[0].(wire.Object)
	if _, has := withoutEl["size"]; has {
		t.Fatal("doc without size gained one")
	}
}

func TestPushSchemaRejectsRemovingAnElementObjectField(t *testing.T) {
	s := NewStore()
	if err := s.PushSchema(versionsSchema(true)); err != nil {
		t.Fatalf("first push: %v", err)
	}
	err := s.PushSchema(versionsSchema(false))
	if err == nil {
		t.Fatal("expected error")
	}
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("got code: %v", err)
	}
	if !strings.Contains(re.Message, "changed type of field 'artifacts.versions'") {
		t.Fatalf("got: %s", re.Message)
	}
}

func TestPushSchemaRejectsRetypingAnElementObjectField(t *testing.T) {
	base := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("things", func(tb *dsl.TableBuilder) {
			tb.Field("v", dsl.ArrayOf(dsl.ObjectOf(map[string]dsl.FieldType{"size": dsl.Optional(dsl.Num())})))
		})
	})
	swapped := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("things", func(tb *dsl.TableBuilder) {
			tb.Field("v", dsl.ArrayOf(dsl.ObjectOf(map[string]dsl.FieldType{"size": dsl.Optional(dsl.Str())})))
		})
	})
	s := NewStore()
	if err := s.PushSchema(base); err != nil {
		t.Fatalf("first push: %v", err)
	}
	err := s.PushSchema(swapped)
	if err == nil {
		t.Fatal("expected error")
	}
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("got code: %v", err)
	}
	if !strings.Contains(re.Message, "changed type of field 'things.v'") {
		t.Fatalf("got: %s", re.Message)
	}
}

func TestPushSchemaRejectsAddingRequiredFieldInsideArrayObjectElements(t *testing.T) {
	base := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("things", func(tb *dsl.TableBuilder) {
			tb.Field("v", dsl.ArrayOf(dsl.ObjectOf(map[string]dsl.FieldType{"tag": dsl.Str()})))
		})
	})
	widened := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("things", func(tb *dsl.TableBuilder) {
			tb.Field("v", dsl.ArrayOf(dsl.ObjectOf(map[string]dsl.FieldType{"tag": dsl.Str(), "size": dsl.Num()})))
		})
	})
	s := NewStore()
	if err := s.PushSchema(base); err != nil {
		t.Fatalf("first push: %v", err)
	}
	err := s.PushSchema(widened)
	if err == nil {
		t.Fatal("expected error")
	}
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("got code: %v", err)
	}
	if !strings.Contains(re.Message, "changed type of field 'things.v'") {
		t.Fatalf("got: %s", re.Message)
	}
}

func TestPushSchemaRejectsNarrowingUnionInsideElementObject(t *testing.T) {
	base := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("things", func(tb *dsl.TableBuilder) {
			tb.Field("v", dsl.ArrayOf(dsl.ObjectOf(map[string]dsl.FieldType{
				"state": dsl.Union(dsl.FieldLiteral(wire.String("a")), dsl.FieldLiteral(wire.String("b")), dsl.FieldLiteral(wire.String("c"))),
			})))
		})
	})
	narrowed := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("things", func(tb *dsl.TableBuilder) {
			tb.Field("v", dsl.ArrayOf(dsl.ObjectOf(map[string]dsl.FieldType{
				"state": dsl.Union(dsl.FieldLiteral(wire.String("a")), dsl.FieldLiteral(wire.String("b"))),
			})))
		})
	})
	s := NewStore()
	if err := s.PushSchema(base); err != nil {
		t.Fatalf("first push: %v", err)
	}
	err := s.PushSchema(narrowed)
	if err == nil {
		t.Fatal("expected error")
	}
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("got code: %v", err)
	}
	if !strings.Contains(re.Message, "changed type of field 'things.v'") {
		t.Fatalf("got: %s", re.Message)
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

func TestPushSchemaGrandfathersTrgmForPreexistingSearchIndexOnRepush(t *testing.T) {
	// FM-30 grandfather: a search index that predates the trgm flag (declared
	// with no trgm key at all) gets Trgm silently flipped to true on the next
	// push if that push still omits it too (mirrors rust
	// push_schema_grandfathers_trgm_for_preexisting_search_index_on_repush).
	noTrgm := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).SearchIndex("by_content", "", "name")
		})
	})
	s := NewStore()
	if err := s.PushSchema(noTrgm); err != nil {
		t.Fatalf("first push: %v", err)
	}
	if s.SchemaSnapshot().Tables["items"].Indexes[0].Trgm {
		t.Fatal("fixture must start trgm:false")
	}
	// Routine re-push, same schema: grandfather flips it.
	if err := s.PushSchema(noTrgm); err != nil {
		t.Fatalf("second push: %v", err)
	}
	if !s.SchemaSnapshot().Tables["items"].Indexes[0].Trgm {
		t.Fatal("existing search index should be grandfathered to trgm:true")
	}
	// A search index NEW in this push stays opt-in.
	withNew := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).Field("note", dsl.Str()).
				SearchIndex("by_content", "", "name").
				SearchIndex("by_note", "", "note")
		})
	})
	if err := s.PushSchema(withNew); err != nil {
		t.Fatalf("third push: %v", err)
	}
	idx := s.SchemaSnapshot().Tables["items"].Indexes
	if !idx[0].Trgm || idx[1].Trgm {
		t.Fatalf("grandfather scope: by_content=%v by_note=%v", idx[0].Trgm, idx[1].Trgm)
	}
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

// Descriptions are pure metadata: stored verbatim by push, editable by an
// ordinary additive push (the destructive-change gate compares fields and
// indexes only), and decoded tolerantly (a description-less table carries
// neither key — mirroring the server's serde parse).
func TestPushSchemaStoresDescriptions(t *testing.T) {
	s := NewStore()
	if err := s.PushSchema(buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).Field("qty", dsl.Num()).
				Describe("Inventory items", map[string]string{"name": "Display name"})
		})
	})); err != nil {
		t.Fatalf("push: %v", err)
	}
	stored := s.SchemaSnapshot().Tables["items"]
	if stored.Description == nil || *stored.Description != "Inventory items" {
		t.Fatalf("description not stored: %v", stored.Description)
	}
	if stored.FieldDescriptions["name"] != "Display name" {
		t.Fatalf("fieldDescriptions not stored: %v", stored.FieldDescriptions)
	}

	// Editing the description is an ordinary additive push.
	if err := s.PushSchema(buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).Field("qty", dsl.Num()).
				Describe("Inventory items v2", map[string]string{"qty": "On hand"})
		})
	})); err != nil {
		t.Fatalf("description edit push: %v", err)
	}
	stored = s.SchemaSnapshot().Tables["items"]
	if stored.Description == nil || *stored.Description != "Inventory items v2" {
		t.Fatalf("description edit not applied: %v", stored.Description)
	}

	// An undescribed table decodes with both members unset.
	if err := s.PushSchema(buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).Field("qty", dsl.Num())
		})
	})); err != nil {
		t.Fatalf("undescribed push: %v", err)
	}
	stored = s.SchemaSnapshot().Tables["items"]
	if stored.Description != nil || stored.FieldDescriptions != nil {
		t.Fatalf("unset descriptions must decode to nil, got %v / %v",
			stored.Description, stored.FieldDescriptions)
	}
}

// A mis-typed description payload is rejected, mirroring the server's
// deny-unknown-fields + typed serde parse.
func TestPushSchemaRejectsMisTypedDescriptions(t *testing.T) {
	s := NewStore()
	table := func(v wire.JSONValue) wire.JSONValue {
		return wire.Object(map[string]wire.JSONValue{"tables": wire.Object(map[string]wire.JSONValue{"items": v})})
	}
	badDesc := table(wire.Object(map[string]wire.JSONValue{
		"fields":      wire.Object(map[string]wire.JSONValue{"name": wire.Object(map[string]wire.JSONValue{"type": wire.String("string")})}),
		"description": wire.Number("1"),
	}))
	if err := s.PushSchema(badDesc); err == nil {
		t.Fatal("non-string description must be rejected")
	}
	badFieldDesc := table(wire.Object(map[string]wire.JSONValue{
		"fields": wire.Object(map[string]wire.JSONValue{"name": wire.Object(map[string]wire.JSONValue{"type": wire.String("string")})}),
		"fieldDescriptions": wire.Object(map[string]wire.JSONValue{
			"name": wire.Number("1"),
		}),
	}))
	if err := s.PushSchema(badFieldDesc); err == nil {
		t.Fatal("non-string fieldDescriptions entry must be rejected")
	}
}
