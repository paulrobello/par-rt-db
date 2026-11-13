// go-client/dsl/filter_test.go
package dsl

import (
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// Comparison predicates carry field/value plus their tagged op.
func TestFilterComparisonShapes(t *testing.T) {
	cases := []struct {
		name string
		expr wire.FilterExpr
		want string
	}{
		{"eq", Eq("done", wire.Bool(true)), `{"op":"eq","field":"done","value":true}`},
		{"neq", Neq("done", wire.Bool(false)), `{"op":"neq","field":"done","value":false}`},
		{"gt", Gt("n", wire.Number("3")), `{"op":"gt","field":"n","value":3}`},
		{"gte", Gte("n", wire.Number("3")), `{"op":"gte","field":"n","value":3}`},
		{"lt", Lt("n", wire.Number("7")), `{"op":"lt","field":"n","value":7}`},
		{"lte", Lte("n", wire.Number("7")), `{"op":"lte","field":"n","value":7}`},
		{
			"in",
			In("status", wire.String("a"), wire.String("b")),
			`{"op":"in","field":"status","values":["a","b"]}`,
		},
		{
			"contains",
			Contains("tags", wire.String("x")),
			`{"op":"contains","field":"tags","value":"x"}`,
		},
		{"exists", Exists("deletedAt"), `{"op":"exists","field":"deletedAt"}`},
		{"olderThan", OlderThan("completedAt", 604800000), `{"op":"olderThan","field":"completedAt","ms":604800000}`},
		{
			"and",
			And(Eq("a", wire.Bool(true)), Eq("b", wire.Bool(false))),
			`{"op":"and","exprs":[{"op":"eq","field":"a","value":true},{"op":"eq","field":"b","value":false}]}`,
		},
		{
			"or",
			Or(Eq("a", wire.Bool(true)), Eq("b", wire.Bool(false))),
			`{"op":"or","exprs":[{"op":"eq","field":"a","value":true},{"op":"eq","field":"b","value":false}]}`,
		},
		{
			"not",
			Not(Eq("done", wire.Bool(true))),
			`{"op":"not","expr":{"op":"eq","field":"done","value":true}}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertWireEqual(t, tc.expr, tc.want)
		})
	}
}

// Combinators nest arbitrarily.
func TestFilterCombinatorsNest(t *testing.T) {
	e := And(
		Or(Eq("a", wire.Bool(true)), Eq("b", wire.Bool(true))),
		Not(Exists("deletedAt")),
	)
	assertWireEqual(t, e, `{"op":"and","exprs":[`+
		`{"op":"or","exprs":[{"op":"eq","field":"a","value":true},{"op":"eq","field":"b","value":true}]},`+
		`{"op":"not","expr":{"op":"exists","field":"deletedAt"}}]}`)
}
