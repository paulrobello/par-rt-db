// EvalValueExpr sanity cases — the deep computed-field coverage arrives with
// the write-step tests (rust in_memory/tests/computed.rs); these pin the
// interpreter's core semantics the stamp chain depends on.
package inmemory

import (
	"strings"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/dsl"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func evalExpr(t *testing.T, expr wire.ValueExpr, doc wire.Object, now int64) wire.JSONValue {
	t.Helper()
	v, err := EvalValueExpr(expr, doc, now, nil)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	return v
}

func TestEvalValueExprBasics(t *testing.T) {
	doc := docObj("name", "Ada", "n", int64(2), "empty", nil)
	if got := evalExpr(t, wire.ValueField{Field: "name"}, doc, 5); !jsonEq(got, wire.String("Ada")) {
		t.Fatalf("field text extraction: %v", got)
	}
	if got := evalExpr(t, wire.ValueField{Field: "missing"}, doc, 5); !jsonEq(got, wire.Null{}) {
		t.Fatalf("missing field: %v", got)
	}
	if got := evalExpr(t, wire.ValueField{Field: "empty"}, doc, 5); !jsonEq(got, wire.Null{}) {
		t.Fatalf("null field: %v", got)
	}
	if got := evalExpr(t, wire.ValueLiteral{Value: wire.String("x")}, doc, 5); !jsonEq(got, wire.String("x")) {
		t.Fatalf("literal: %v", got)
	}
	if got := evalExpr(t, wire.ValueNow{}, doc, 5); !jsonEq(got, wire.Number("5")) {
		t.Fatalf("now: %v", got)
	}
	concat := wire.ValueConcat{Parts: []wire.ValueExpr{
		wire.ValueField{Field: "name"}, wire.ValueLiteral{Value: wire.String("!")},
		wire.ValueField{Field: "empty"},
	}}
	if got := evalExpr(t, concat, doc, 5); !jsonEq(got, wire.String("Ada!")) {
		t.Fatalf("concat skips null parts: %v", got)
	}
	add := wire.ValueAdd{Left: wire.ValueField{Field: "n"}, Right: wire.ValueLiteral{Value: wire.Number("3")}}
	if got := evalExpr(t, add, doc, 5); !jsonEq(got, wire.Number("5")) {
		t.Fatalf("add: %v", got)
	}
	// SQL-NULL propagation precedes the div-zero check (null / 0 is null).
	divNull := wire.ValueDiv{Left: wire.ValueField{Field: "empty"}, Right: wire.ValueLiteral{Value: wire.Number("0")}}
	if got := evalExpr(t, divNull, doc, 5); !jsonEq(got, wire.Null{}) {
		t.Fatalf("null propagation: %v", got)
	}
	coalesce := wire.ValueCoalesce{Parts: []wire.ValueExpr{
		wire.ValueField{Field: "empty"}, wire.ValueLiteral{Value: wire.Number("7")},
	}}
	if got := evalExpr(t, coalesce, doc, 5); !jsonEq(got, wire.Number("7")) {
		t.Fatalf("coalesce: %v", got)
	}
	casted := wire.ValueCast{Value: wire.ValueLiteral{Value: wire.String("42")}, To: wire.CastToInt64}
	if got := evalExpr(t, casted, doc, 5); !jsonEq(got, wire.Number("42")) {
		t.Fatalf("cast toInt64: %v", got)
	}
	// A float-spelled number is NOT integral for toInt64, even when whole.
	floatCast := wire.ValueCast{Value: wire.ValueLiteral{Value: wire.Number("3.0")}, To: wire.CastToInt64}
	if _, err := EvalValueExpr(floatCast, doc, 5, nil); err == nil {
		t.Fatal("cast 3.0 to int64 should fail")
	}
}

func TestStampComputedRemovesNullAndOverwrites(t *testing.T) {
	table := &TableDef{
		Fields: map[string]FieldType{
			"a":     {Kind: "number"},
			"b":     {Kind: "number"},
			"shout": {Kind: "optional", Inner: ptrField(FieldType{Kind: "string"})},
		},
		Computed: map[string]wire.ValueExpr{
			"shout": wire.ValueUpper{Value: wire.ValueField{Field: "a"}},
			"doubled": wire.ValueMul{
				Left: wire.ValueField{Field: "b"}, Right: wire.ValueLiteral{Value: wire.Number("2")},
			},
		},
	}
	doc := docObj("a", nil, "b", int64(3), "shout", "client-noise")
	out, err := stampComputed(table, doc, 5)
	if err != nil {
		t.Fatalf("stamp: %v", err)
	}
	if _, present := out["shout"]; present {
		t.Fatal("null computed result must remove the key (client-supplied value included)")
	}
	if got := out["doubled"]; !jsonEq(got, wire.Number("6")) {
		t.Fatalf("doubled: %v", got)
	}
}

func TestComputedPushValidationRejectsUndeclaredReference(t *testing.T) {
	bad := buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("items", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).
				Field("shout", dsl.Optional(dsl.Str())).
				Computed("shout", wire.ValueUpper{Value: wire.ValueField{Field: "ghost"}})
		})
	})
	_, err := parseSchema(bad)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	schema := parseSchemaOrDie(t, bad)
	err = validateSchema(schema)
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("got: %v", err)
	}
	if !strings.Contains(re.Message, "references undeclared field 'ghost'") {
		t.Fatalf("got: %s", re.Message)
	}
}
