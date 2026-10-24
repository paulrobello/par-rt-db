// Declarative per-field write constraints (server TableDef::constraints):
// the .Constraints() builder wire shape plus in-memory push-time and
// write-path enforcement. Cross-engine agreement is pinned by the
// constraint-* / error-constraint-* semantics-corpus cases; these unit tests
// pin the DSL builder surface, mirroring ts-client/tests/
// field-constraints.test.ts's coverage.
package inmemory

import (
	"context"
	"strings"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/dsl"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func constraintsSchema() wire.JSONValue {
	return buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("tasks", func(t *dsl.TableBuilder) {
			t.Field("name", dsl.Str()).
				Field("score", dsl.Num()).
				Field("counter", dsl.Int64()).
				Field("note", dsl.Optional(dsl.Str())).
				Index("by_name", "name").
				Constraints(map[string]wire.JSONValue{
					"score": wire.Object{
						"min": wire.Number("1"), "max": wire.Number("10"),
					},
					"counter": wire.Object{
						"min": wire.Number("1"), "max": wire.Number("100"),
					},
					"name": wire.Object{
						"minLength": wire.Number("2"), "maxLength": wire.Number("5"),
						"pattern": wire.String("^[a-z]+$"),
					},
				})
		})
	})
}

func constraintsClient(t *testing.T) *Client {
	t.Helper()
	c := New(Options{Now: func() int64 { return 1_700_000_000_000 }})
	if err := c.PushSchema(constraintsSchema()); err != nil {
		t.Fatalf("push constraints schema: %v", err)
	}
	return c
}

func constraintErr(t *testing.T, err error) *rtdberrors.RtDbError {
	t.Helper()
	if err == nil {
		t.Fatalf("expected an error, got nil")
	}
	e, ok := err.(*rtdberrors.RtDbError)
	if !ok {
		t.Fatalf("expected *rtdberrors.RtDbError, got %T: %v", err, err)
	}
	return e
}

// parseConstraintsSchema parses the built schema JSON into the typed model
// (the typed view of the wire the engine enforces against).
func parseConstraintsSchema(t *testing.T, schema wire.JSONValue) *SchemaDef {
	t.Helper()
	s, err := parseSchema(schema)
	if err != nil {
		t.Fatalf("parse constraints schema: %v", err)
	}
	return s
}

func TestConstraintsWireShape(t *testing.T) {
	s := parseConstraintsSchema(t, constraintsSchema())
	tasks := s.Tables["tasks"]
	if len(tasks.Constraints) != 3 {
		t.Fatalf("constraints count: %d", len(tasks.Constraints))
	}
	score := tasks.Constraints["score"]
	if score.Min == nil || string(*score.Min) != "1" ||
		score.Max == nil || string(*score.Max) != "10" {
		t.Fatalf("score constraints: %+v", score)
	}
	name := tasks.Constraints["name"]
	if name.MinLength == nil || *name.MinLength != 2 ||
		name.MaxLength == nil || *name.MaxLength != 5 ||
		name.Pattern == nil || *name.Pattern != "^[a-z]+$" {
		t.Fatalf("name constraints: %+v", name)
	}

	// A table without constraints leaves the typed field empty (the wire key
	// is omitted entirely by the builder).
	plain := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("plain", func(t *dsl.TableBuilder) {
			t.Field("name", dsl.Str())
		})
	})
	ps := parseConstraintsSchema(t, plain)
	if len(ps.Tables["plain"].Constraints) != 0 {
		t.Fatalf("plain table constraints: %+v", ps.Tables["plain"].Constraints)
	}
}

func TestConstraintsPushValidationMisapplied(t *testing.T) {
	schema := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("tasks", func(t *dsl.TableBuilder) {
			t.Field("name", dsl.Str()).Index("by_name", "name").
				Constraints(map[string]wire.JSONValue{
					"name": wire.Object{"min": wire.Number("1")},
				})
		})
	})
	c := New(Options{})
	e := constraintErr(t, c.PushSchema(schema))
	if e.Code != rtdberrors.CodeSchemaViolation {
		t.Fatalf("code: %s", e.Code)
	}
	if !strings.Contains(e.Message, "'name'") || !strings.Contains(e.Message, "number/int64") {
		t.Fatalf("message: %s", e.Message)
	}
}

func TestConstraintsPushValidationUnknownField(t *testing.T) {
	schema := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("tasks", func(t *dsl.TableBuilder) {
			t.Field("name", dsl.Str()).
				Constraints(map[string]wire.JSONValue{
					"ghost": wire.Object{"minLength": wire.Number("1")},
				})
		})
	})
	c := New(Options{})
	e := constraintErr(t, c.PushSchema(schema))
	if e.Code != rtdberrors.CodeSchemaViolation ||
		!strings.Contains(e.Message, "'ghost'") {
		t.Fatalf("error: %s", e)
	}
}

func TestConstraintsPushValidationInt64BoundsMustBeIntegers(t *testing.T) {
	schema := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("tasks", func(t *dsl.TableBuilder) {
			t.Field("counter", dsl.Int64()).
				Constraints(map[string]wire.JSONValue{
					"counter": wire.Object{"min": wire.Number("1.5")},
				})
		})
	})
	c := New(Options{})
	e := constraintErr(t, c.PushSchema(schema))
	if e.Code != rtdberrors.CodeSchemaViolation ||
		!strings.Contains(e.Message, "integer") {
		t.Fatalf("error: %s", e)
	}
}

func TestConstraintsPushValidationCrossedBounds(t *testing.T) {
	schema := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("tasks", func(t *dsl.TableBuilder) {
			t.Field("name", dsl.Str()).Field("score", dsl.Num()).
				Constraints(map[string]wire.JSONValue{
					"score": wire.Object{"min": wire.Number("10"), "max": wire.Number("1")},
					"name":  wire.Object{"minLength": wire.Number("5"), "maxLength": wire.Number("2")},
				})
		})
	})
	c := New(Options{})
	if err := c.PushSchema(schema); err == nil {
		t.Fatalf("crossed bounds must fail the push")
	}
}

func TestConstraintsPushValidationInvalidRegex(t *testing.T) {
	schema := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("tasks", func(t *dsl.TableBuilder) {
			t.Field("name", dsl.Str()).
				Constraints(map[string]wire.JSONValue{
					"name": wire.Object{"pattern": wire.String("([a-z")},
				})
		})
	})
	c := New(Options{})
	e := constraintErr(t, c.PushSchema(schema))
	if e.Code != rtdberrors.CodeSchemaViolation ||
		!strings.Contains(e.Message, "invalid pattern") {
		t.Fatalf("error: %s", e)
	}
}

func TestConstraintsWritePathBoundaries(t *testing.T) {
	c := constraintsClient(t)
	_, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{
		wire.StepInsert{Table: "tasks", Doc: docObj("name", "aa", "score", 1, "counter", "1")},
		wire.StepInsert{Table: "tasks", Doc: docObj("name", "abcde", "score", 10, "counter", "100")},
	}}, "")
	if err != nil {
		t.Fatalf("boundary inserts: %v", err)
	}
}

func TestConstraintsWritePathMinViolatedRollsBack(t *testing.T) {
	c := constraintsClient(t)
	_, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{
		wire.StepInsert{Table: "tasks", Doc: docObj("name", "good", "score", 5, "counter", "1")},
		wire.StepInsert{Table: "tasks", Doc: docObj("name", "bad", "score", 0.5, "counter", "1")},
	}}, "")
	e := constraintErr(t, err)
	if e.Code != rtdberrors.CodeSchemaViolation {
		t.Fatalf("code: %s", e.Code)
	}
	v, err := c.Query(context.Background(), dsl.NewTableQuery("tasks"))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if arr, ok := v.(wire.Array); !ok || len(arr) != 0 {
		t.Fatalf("txn did not roll back: %v", v)
	}
}

func TestConstraintsWritePathStringLengthPattern(t *testing.T) {
	c := constraintsClient(t)
	cases := []struct {
		name    string
		docName string
		label   string
	}{
		{"too short", "a", "'minLength' constraint"},
		{"too long", "abcdef", "'maxLength' constraint"},
		{"pattern mismatch", "ABC", "'pattern' constraint"},
	}
	for _, tc := range cases {
		_, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{
			wire.StepInsert{Table: "tasks", Doc: docObj("name", tc.docName, "score", 5, "counter", "1")},
		}}, "")
		e := constraintErr(t, err)
		if e.Code != rtdberrors.CodeSchemaViolation || !strings.Contains(e.Message, tc.label) {
			t.Fatalf("%s: error: %s", tc.name, e)
		}
	}
}

func TestConstraintsWritePathInt64Bounds(t *testing.T) {
	c := constraintsClient(t)
	_, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{
		wire.StepInsert{Table: "tasks", Doc: docObj("name", "ok", "score", 5, "counter", "101")},
	}}, "")
	e := constraintErr(t, err)
	if e.Code != rtdberrors.CodeSchemaViolation || !strings.Contains(e.Message, "'max' constraint") {
		t.Fatalf("error: %s", e)
	}
}

func TestConstraintsEnforcedOnEveryWritePath(t *testing.T) {
	c := constraintsClient(t)
	results, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{
		wire.StepInsert{Table: "tasks", Doc: docObj("name", "ok", "score", 5, "counter", "1")},
	}}, "")
	if err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if results[0].ID == nil || *results[0].ID == "" {
		t.Fatalf("insert id: %#v", results[0])
	}
	id := *results[0].ID

	// patch above max.
	if _, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{
		wire.StepPatch{Table: "tasks", ID: id, Fields: docObj("score", 50)},
	}}, ""); !rtdberrors.IsCode(err, rtdberrors.CodeSchemaViolation) {
		t.Fatalf("patch error: %v", err)
	}
	// replace with an out-of-bounds doc.
	if _, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{
		wire.StepReplace{Table: "tasks", ID: id, Doc: docObj("name", "ab", "score", 99, "counter", "2")},
	}}, ""); !rtdberrors.IsCode(err, rtdberrors.CodeSchemaViolation) {
		t.Fatalf("replace error: %v", err)
	}
	// upsert's update branch above max.
	if _, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{
		wire.StepUpsert{
			Table: "tasks", Index: "by_name", Eq: []wire.JSONValue{wire.String("ok")},
			Insert: docObj("name", "ok", "score", 5, "counter", "1"),
			Patch:  docObj("counter", "101"),
		},
	}}, ""); !rtdberrors.IsCode(err, rtdberrors.CodeSchemaViolation) {
		t.Fatalf("upsert error: %v", err)
	}
	// patchByQuery above max.
	if _, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{
		wire.StepPatchByQuery{
			Table:  "tasks",
			Filter: wire.FilterEq{Field: "name", Value: wire.String("ok")},
			Patch:  docObj("score", 99),
		},
	}}, ""); !rtdberrors.IsCode(err, rtdberrors.CodeSchemaViolation) {
		t.Fatalf("patchByQuery error: %v", err)
	}

	// Nothing survived: the stored doc is untouched.
	v, err := c.Query(context.Background(), dsl.NewTableQuery("tasks"))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	arr, ok := v.(wire.Array)
	if !ok || len(arr) != 1 {
		t.Fatalf("query result: %v", v)
	}
	doc, ok := arr[0].(wire.Object)
	if !ok {
		t.Fatalf("row shape: %v", arr[0])
	}
	if !jsonEq(doc["score"], wire.Number("5")) || !jsonEq(doc["counter"], wire.String("1")) {
		t.Fatalf("row mutated by a failed write: %v", doc)
	}
}

func TestConstraintsNullAndAbsentNeverViolate(t *testing.T) {
	c := constraintsClient(t)
	if _, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{
		wire.StepInsert{Table: "tasks", Doc: docObj("name", "ok", "score", 5, "counter", "1", "note", nil)},
	}}, ""); err != nil {
		t.Fatalf("null optional insert: %v", err)
	}
}

// schemaToWire (the migrate-result wire form) carries constraints back and
// re-parses to the identical typed schema.
func TestConstraintsSchemaToWireRoundTrip(t *testing.T) {
	s := parseConstraintsSchema(t, constraintsSchema())
	w, err := schemaToWire(s)
	if err != nil {
		t.Fatalf("schemaToWire: %v", err)
	}
	tables := w.(wire.Object)["tables"].(wire.Object)
	tasks := tables["tasks"].(wire.Object)
	co, ok := tasks["constraints"].(wire.Object)
	if !ok || len(co) != 3 {
		t.Fatalf("wire constraints missing: %v", tasks["constraints"])
	}
	name := co["name"].(wire.Object)
	if string(name["minLength"].(wire.Number)) != "2" ||
		string(name["maxLength"].(wire.Number)) != "5" ||
		string(name["pattern"].(wire.String)) != "^[a-z]+$" {
		t.Fatalf("name constraint round-trip: %v", name)
	}
	again, err := parseSchema(w)
	if err != nil {
		t.Fatalf("re-parse: %v", err)
	}
	if len(again.Tables["tasks"].Constraints) != 3 {
		t.Fatalf("re-parsed constraints: %+v", again.Tables["tasks"].Constraints)
	}
}

// Lengths count Unicode scalar values, not bytes: a 3-rune (4-byte) string
// passes a maxLength 3, while a 4-byte interpretation would reject it.
func TestConstraintsLengthCountsRunes(t *testing.T) {
	c := New(Options{Now: func() int64 { return 1_700_000_000_000 }})
	if err := c.PushSchema(buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("labels", func(t *dsl.TableBuilder) {
			t.Field("tag", dsl.Str()).
				Constraints(map[string]wire.JSONValue{
					"tag": wire.Object{"maxLength": wire.Number("3")},
				})
		})
	})); err != nil {
		t.Fatalf("push: %v", err)
	}
	// "éab" is 3 runes but 4 bytes; a byte count would reject it.
	if _, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{
		wire.StepInsert{Table: "labels", Doc: docObj("tag", "éab")},
	}}, ""); err != nil {
		t.Fatalf("3-rune (4-byte) tag rejected: %v", err)
	}
	// 4 runes violate it.
	if _, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{
		wire.StepInsert{Table: "labels", Doc: docObj("tag", "éabc")},
	}}, ""); !rtdberrors.IsCode(err, rtdberrors.CodeSchemaViolation) {
		t.Fatalf("4-rune tag must violate maxLength 3: %v", err)
	}
}
