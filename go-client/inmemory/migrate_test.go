// Migration tests — the plan-mandated corpus behaviors (add-field backfill
// via setDefault, int64→float64 widening changeType, error envelope on a
// failed directive) plus the atomicity ruling.
package inmemory

import (
	"strings"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/dsl"

	"github.com/paulrobello/par-rt-db/go-client/admin"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func migrateStore(t *testing.T) *Store {
	s := NewStore()
	if err := s.PushSchema(buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).
				Field("n", dsl.Int64()).
				Index("by_name", "name").
				Index("by_n", "n")
		})
	})); err != nil {
		t.Fatalf("push: %v", err)
	}
	return s
}

func TestMigrateChangeTypeWidensAndBackfills(t *testing.T) {
	s := migrateStore(t)
	schema := s.SchemaSnapshot()
	id, _ := doInsert(s, "items", schema.Tables["items"], docObj("name", "a", "n", "42"))
	_ = id
	// changeType int64 → number via toString... exercise the widening path:
	// number field's stored decimal string coerces through toNumber.
	res, err := ApplyMigration(s, []admin.Directive{admin.DirectiveChangeType{
		Table: "items", Field: "n", Cast: wire.CastToNumber,
		To: docObj("type", "number"),
	}}, false)
	if err != nil {
		t.Fatalf("changeType: %v", err)
	}
	if !res.Applied || len(res.Directives) != 1 || res.Directives[0].AffectedRows != 1 {
		t.Fatalf("report: %+v", res)
	}
	for _, row := range s.docs {
		if _, isStr := row.Doc["n"].(wire.String); isStr {
			t.Fatalf("n still a string: %v", row.Doc["n"])
		}
	}
	// The derived schema is installed and returned.
	nn, ok := s.SchemaSnapshot().Tables["items"].Fields["n"]
	if !ok || nn.Kind != "number" {
		t.Fatalf("schema not retyped: %+v", nn)
	}
	if _, ok := res.Schema.(wire.Object); !ok {
		t.Fatal("result schema not a JSON object")
	}
	// Validate the engine still accepts the retyped field on insert.
	if _, err := doInsert(s, "items", s.SchemaSnapshot().Tables["items"], docObj("name", "b", "n", 3.5)); err != nil {
		t.Fatalf("insert after retyping: %v", err)
	}
}

func TestMigrateSetDefaultBackfillsRowsOnly(t *testing.T) {
	s := migrateStore(t)
	schema := s.SchemaSnapshot()
	// Add an optional field, insert a row without it, then setDefault.
	if err := s.PushSchema(buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).
				Field("n", dsl.Int64()).
				Field("tag", dsl.Optional(dsl.Str())).
				Index("by_name", "name").
				Index("by_n", "n")
		})
	})); err != nil {
		t.Fatalf("push 2: %v", err)
	}
	schema = s.SchemaSnapshot()
	doInsert(s, "items", schema.Tables["items"], docObj("name", "a", "n", "1"))
	res, err := ApplyMigration(s, []admin.Directive{admin.DirectiveSetDefault{
		Table: "items", Field: "tag", Value: wire.String("none"),
	}}, false)
	if err != nil {
		t.Fatalf("setDefault: %v", err)
	}
	if res.Directives[0].AffectedRows != 1 {
		t.Fatalf("affected: %+v", res.Directives[0])
	}
	for _, row := range s.docs {
		if !jsonEq(row.Doc["tag"], wire.String("none")) {
			t.Fatalf("row not backfilled: %v", row.Doc)
		}
	}
	// Rust parity: the directive does NOT fold into the schema defaults map.
	if _, has := s.SchemaSnapshot().Tables["items"].Defaults["tag"]; has {
		t.Fatal("setDefault must not add a schema default (rust parity)")
	}
}

func TestMigrateFailureIsErrorEnvelopeAndAtomic(t *testing.T) {
	s := migrateStore(t)
	schema := s.SchemaSnapshot()
	id, _ := doInsert(s, "items", schema.Tables["items"], docObj("name", "a", "n", "42"))
	before := len(s.docs)
	// A cast invalid for the field's type → BAD_REQUEST before any row is
	// touched (toBoolean is not valid for an int64 field).
	_, err := ApplyMigration(s, []admin.Directive{admin.DirectiveChangeType{
		Table: "items", Field: "n", Cast: wire.CastToBoolean,
		To: docObj("type", "boolean"),
	}}, false)
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("want error envelope, got: %v", err)
	}
	if !strings.Contains(re.Message, "is not valid for") {
		t.Fatalf("msg: %s", re.Message)
	}
	// A valid cast with a non-coercible row and no default → the row-named
	// BAD_REQUEST (name string "a" cannot coerce to number).
	_, err = ApplyMigration(s, []admin.Directive{admin.DirectiveChangeType{
		Table: "items", Field: "name", Cast: wire.CastToNumber,
		To: docObj("type", "number"),
	}}, false)
	re, ok = err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest || !strings.Contains(re.Message, "cannot coerce value") {
		t.Fatalf("row-level: %v", err)
	}
	if len(s.docs) != before {
		t.Fatal("docs mutated on failed migration")
	}
	_ = id
}

func TestMigrateDropFieldRejectsComputedDependency(t *testing.T) {
	s := NewStore()
	if err := s.PushSchema(buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("posts", func(tb *dsl.TableBuilder) {
			tb.Field("title", dsl.Str()).
				Field("slug", dsl.Optional(dsl.Str())).
				Computed("slug", wire.ValueLower{Value: wire.ValueField{Field: "title"}})
		})
	})); err != nil {
		t.Fatalf("push: %v", err)
	}
	_, err := ApplyMigration(s, []admin.Directive{admin.DirectiveDropField{
		Table: "posts", Field: "title",
	}}, false)
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("got: %v", err)
	}
	if !strings.Contains(re.Message, "referenced by computed field") {
		t.Fatalf("msg: %s", re.Message)
	}
}

func TestMigrateRenameFieldRewritesReferences(t *testing.T) {
	s := migrateStore(t)
	schema := s.SchemaSnapshot()
	id, _ := doInsert(s, "items", schema.Tables["items"], docObj("name", "a", "n", "1"))
	res, err := ApplyMigration(s, []admin.Directive{admin.DirectiveRenameField{
		Table: "items", From: "n", To: "count",
	}}, false)
	if err != nil {
		t.Fatalf("rename: %v", err)
	}
	_ = res
	row := s.docs[rowKey{Table: "items", ID: id}]
	if _, has := row.Doc["n"]; has {
		t.Fatal("old key still present")
	}
	if _, has := row.Doc["count"]; !has {
		t.Fatal("new key missing")
	}
	// The by_n index followed the rename.
	var idx *IndexDef
	for i := range s.SchemaSnapshot().Tables["items"].Indexes {
		if s.SchemaSnapshot().Tables["items"].Indexes[i].Name == "by_n" {
			idx = &s.SchemaSnapshot().Tables["items"].Indexes[i]
		}
	}
	if idx == nil || idx.Fields[0] != "count" {
		t.Fatalf("index not renamed: %+v", idx)
	}
}

func TestMigrateDryRunCommitsNothing(t *testing.T) {
	s := migrateStore(t)
	before := len(s.docs)
	res, err := ApplyMigration(s, []admin.Directive{admin.DirectiveRenameTable{
		From: "items", To: "things",
	}}, true)
	if err != nil {
		t.Fatalf("dryRun: %v", err)
	}
	if res.Applied {
		t.Fatal("dryRun must report applied=false")
	}
	if len(s.docs) != before {
		t.Fatal("dryRun mutated docs")
	}
	if s.SchemaSnapshot().Tables["items"] == nil {
		t.Fatal("dryRun installed the schema")
	}
	// The derived preview shows the rename.
	if _, has := res.Schema.(wire.Object); !has {
		t.Fatal("preview schema missing")
	}
}

func TestMigrateEvalExprUnsupported(t *testing.T) {
	s := migrateStore(t)
	_, err := ApplyMigration(s, []admin.Directive{admin.DirectiveEvalExpr{
		Table: "items", Set: "n", Expr: admin.ExprSource{},
	}}, false)
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest || !strings.Contains(re.Message, "unsupported in-memory") {
		t.Fatalf("got: %v", err)
	}
}
