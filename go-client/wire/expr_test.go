// go-client/wire/expr_test.go
package wire

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestFilterOlderThanTagIsCamelCase(t *testing.T) {
	b, err := json.Marshal(FilterOlderThan{Field: "completedAt", Ms: 604800000})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"op":"olderThan"`)) || !bytes.Contains(b, []byte(`"ms":604800000`)) {
		t.Fatalf("olderThan shape wrong: %s", b)
	}
}

func TestFilterAndNests(t *testing.T) {
	e := FilterAnd{Exprs: []FilterExpr{
		FilterEq{Field: "a", Value: Number("1")},
		FilterNot{Expr: FilterExists{Field: "b"}},
	}}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"op":"and"`)) || !bytes.Contains(b, []byte(`"op":"not"`)) {
		t.Fatalf("nested filter tags wrong: %s", b)
	}
}

func TestValueExprCastCamelCase(t *testing.T) {
	e := ValueCast{Value: ValueField{Field: "n"}, To: CastToString}
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"op":"cast"`)) || !bytes.Contains(b, []byte(`"to":"toString"`)) {
		t.Fatalf("cast shape wrong: %s", b)
	}
}

func TestUnmarshalFilterExprRoundTrip(t *testing.T) {
	in := FilterAnd{Exprs: []FilterExpr{
		FilterEq{Field: "a", Value: Number("1")},
		FilterOr{Exprs: []FilterExpr{
			FilterOlderThan{Field: "completedAt", Ms: 604800000},
			FilterNot{Expr: FilterExists{Field: "b"}},
		}},
	}}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalFilterExpr(b)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != string(rb) {
		t.Fatalf("filter round trip drifted:\n%s\n%s", b, rb)
	}
}

func TestUnmarshalValueExprRoundTrip(t *testing.T) {
	in := ValueCase{
		Whens: []CaseWhen{
			{When: FilterEq{Field: "a", Value: Number("1")}, Then: ValueField{Field: "x"}},
			{When: FilterOlderThan{Field: "t", Ms: 1}, Then: ValueNow{}},
		},
		Otherwise: ValueCast{Value: ValueField{Field: "n"}, To: CastToInt64},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	out, err := UnmarshalValueExpr(b)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != string(rb) {
		t.Fatalf("valueexpr round trip drifted:\n%s\n%s", b, rb)
	}
}

func TestUnknownOpsRejected(t *testing.T) {
	if _, err := UnmarshalFilterExpr([]byte(`{"op":"nope"}`)); err == nil {
		t.Fatal("unknown filter op must be rejected")
	}
	if _, err := UnmarshalValueExpr([]byte(`{"op":"nope"}`)); err == nil {
		t.Fatal("unknown value op must be rejected")
	}
}
