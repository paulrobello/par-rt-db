// go-client/dsl/valueexpr_test.go
package dsl

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// Each constructor must emit the exact tagged op the server routes on; the
// compare is structural so key order is not part of the contract.
func TestValueExprWireShapes(t *testing.T) {
	cases := []struct {
		name string
		expr wire.ValueExpr
		want string
	}{
		{"field", Field("n"), `{"op":"field","field":"n"}`},
		{"literal", Literal(wire.Number("42")), `{"op":"literal","value":42}`},
		{
			"concat",
			Concat(Field("a"), Literal(wire.String("x"))),
			`{"op":"concat","parts":[{"op":"field","field":"a"},{"op":"literal","value":"x"}]}`,
		},
		{
			"add",
			Add(Field("a"), Literal(wire.Number("1"))),
			`{"op":"add","left":{"op":"field","field":"a"},"right":{"op":"literal","value":1}}`,
		},
		{
			"sub",
			Sub(Field("a"), Literal(wire.Number("1"))),
			`{"op":"sub","left":{"op":"field","field":"a"},"right":{"op":"literal","value":1}}`,
		},
		{
			"mul",
			Mul(Field("a"), Literal(wire.Number("2"))),
			`{"op":"mul","left":{"op":"field","field":"a"},"right":{"op":"literal","value":2}}`,
		},
		{
			"div",
			Div(Field("a"), Literal(wire.Number("2"))),
			`{"op":"div","left":{"op":"field","field":"a"},"right":{"op":"literal","value":2}}`,
		},
		{
			"coalesce",
			Coalesce(Field("a"), Literal(wire.Null{})),
			`{"op":"coalesce","parts":[{"op":"field","field":"a"},{"op":"literal","value":null}]}`,
		},
		{"lower", Lower(Field("a")), `{"op":"lower","value":{"op":"field","field":"a"}}`},
		{"upper", Upper(Field("a")), `{"op":"upper","value":{"op":"field","field":"a"}}`},
		{"trim", Trim(Field("a")), `{"op":"trim","value":{"op":"field","field":"a"}}`},
		{"cast", Cast(Field("n"), wire.CastToInt64), `{"op":"cast","value":{"op":"field","field":"n"},"to":"toInt64"}`},
		{"now", Now(), `{"op":"now"}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertWireEqual(t, tc.expr, tc.want)
		})
	}
}

// Case composes When branches with an otherwise fallback.
func TestCaseExprComposesWhenBranches(t *testing.T) {
	e := Case(
		[]wire.CaseWhen{
			When(Gte("n", wire.Number("10")), Literal(wire.String("big"))),
			When(Gte("n", wire.Number("5")), Literal(wire.String("mid"))),
		},
		Literal(wire.String("small")),
	)
	assertWireEqual(t, e, `{"op":"case","whens":[`+
		`{"when":{"op":"gte","field":"n","value":10},"then":{"op":"literal","value":"big"}},`+
		`{"when":{"op":"gte","field":"n","value":5},"then":{"op":"literal","value":"mid"}}],`+
		`"otherwise":{"op":"literal","value":"small"}}`)
}

// assertWireEqual marshals got and structurally compares against wantJSON.
func assertWireEqual(t *testing.T, got any, wantJSON string) {
	t.Helper()
	blob, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	var gotAny, wantAny any
	if err := json.Unmarshal(blob, &gotAny); err != nil {
		t.Fatalf("marshal produced invalid JSON: %s", blob)
	}
	if err := json.Unmarshal([]byte(wantJSON), &wantAny); err != nil {
		t.Fatalf("test fixture invalid: %s", wantJSON)
	}
	if !reflect.DeepEqual(gotAny, wantAny) {
		t.Fatalf("wire mismatch:\n got %s\nwant %s", blob, wantJSON)
	}
}
