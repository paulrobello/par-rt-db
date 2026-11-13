// go-client/wire/filter_test.go
package wire

import (
	"encoding/json"
	"fmt"
	"testing"
)

// TestFilterAllVariantsRoundTrip pins the exact op tag of every FilterExpr
// variant and requires marshal → UnmarshalFilterExpr → re-marshal to be
// byte-identical.
func TestFilterAllVariantsRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		expr FilterExpr
		tag  string
	}{
		{"eq", FilterEq{Field: "a", Value: Number("1")}, `"op":"eq"`},
		{"neq", FilterNeq{Field: "a", Value: String("x")}, `"op":"neq"`},
		{"gt", FilterGt{Field: "a", Value: Number("0")}, `"op":"gt"`},
		{"gte", FilterGte{Field: "a", Value: Number("0")}, `"op":"gte"`},
		{"lt", FilterLt{Field: "a", Value: Number("9")}, `"op":"lt"`},
		{"lte", FilterLte{Field: "a", Value: Number("9")}, `"op":"lte"`},
		{"in", FilterIn{Field: "a", Values: []JSONValue{Number("1"), String("b"), Null{}}}, `"op":"in"`},
		{"contains", FilterContains{Field: "a", Value: String("x")}, `"op":"contains"`},
		{"and", FilterAnd{Exprs: []FilterExpr{FilterEq{Field: "a", Value: Number("1")}, FilterExists{Field: "b"}}}, `"op":"and"`},
		{"or", FilterOr{Exprs: []FilterExpr{FilterEq{Field: "a", Value: Number("1")}, FilterExists{Field: "b"}}}, `"op":"or"`},
		{"not", FilterNot{Expr: FilterExists{Field: "b"}}, `"op":"not"`},
		{"exists", FilterExists{Field: "b"}, `"op":"exists"`},
		{"olderThan", FilterOlderThan{Field: "completedAt", Ms: 604800000}, `"op":"olderThan"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.expr)
			if err != nil {
				t.Fatal(err)
			}
			if !bytesHas(b, tc.tag) {
				t.Fatalf("tag wrong: %s", b)
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
				t.Fatalf("round trip drifted:\n%s\n%s", b, rb)
			}
		})
	}
}

// TestFilterExactWireShapes pins byte-exact marshaled JSON (keys sort
// alphabetically because MarshalTagged injects the tag via a map).
func TestFilterExactWireShapes(t *testing.T) {
	exact := []struct {
		expr FilterExpr
		want string
	}{
		{FilterExists{Field: "b"}, `{"field":"b","op":"exists"}`},
		{FilterOlderThan{Field: "completedAt", Ms: 123}, `{"field":"completedAt","ms":123,"op":"olderThan"}`},
		{FilterEq{Field: "a", Value: Null{}}, `{"field":"a","op":"eq","value":null}`},
	}
	for _, tc := range exact {
		b, err := json.Marshal(tc.expr)
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != tc.want {
			t.Fatalf("wire shape wrong:\n got %s\nwant %s", b, tc.want)
		}
	}
}

// TestFilterDecodeErrors exercises decode error branches across the filter
// grammar: missing/non-string/unknown tags, strict unknown-field rejection
// per variant, and nested decode failures.
func TestFilterDecodeErrors(t *testing.T) {
	// Tag dispatch failures.
	if _, err := UnmarshalFilterExpr([]byte(`{"field":"a"}`)); err == nil {
		t.Fatal("missing op tag must be rejected")
	}
	if _, err := UnmarshalFilterExpr([]byte(`{"op":7}`)); err == nil {
		t.Fatal("non-string op tag must be rejected")
	}
	// Scalar filters: strict rejection, then missing-value decode failure.
	scalars := []string{"eq", "neq", "gt", "gte", "lt", "lte", "contains"}
	for _, op := range scalars {
		in := fmt.Sprintf(`{"op":%q,"field":"a","value":1,"zz":1}`, op)
		if _, err := UnmarshalFilterExpr([]byte(in)); err == nil {
			t.Fatalf("unknown field must be rejected: %s", in)
		}
		missing := fmt.Sprintf(`{"op":%q,"field":"a"}`, op)
		if _, err := UnmarshalFilterExpr([]byte(missing)); err == nil {
			t.Fatalf("missing value must be rejected: %s", missing)
		}
	}
	// in: strict rejection and element decode failure.
	if _, err := UnmarshalFilterExpr([]byte(`{"op":"in","field":"a","values":[],"zz":1}`)); err == nil {
		t.Fatal("in with unknown field must be rejected")
	}
	if out, err := UnmarshalFilterExpr([]byte(`{"op":"in","field":"a"}`)); err != nil {
		t.Fatalf("missing values decodes empty: %v", err)
	} else if f, ok := out.(FilterIn); !ok || len(f.Values) != 0 {
		t.Fatalf("expected empty values, got %#v", out)
	}
	// and/or: strict rejection and nested decode failure.
	for _, op := range []string{"and", "or"} {
		in := fmt.Sprintf(`{"op":%q,"exprs":[],"zz":1}`, op)
		if _, err := UnmarshalFilterExpr([]byte(in)); err == nil {
			t.Fatalf("unknown field must be rejected: %s", in)
		}
		bad := fmt.Sprintf(`{"op":%q,"exprs":[{"op":"nope"}]}`, op)
		if _, err := UnmarshalFilterExpr([]byte(bad)); err == nil {
			t.Fatalf("nested bad op must propagate: %s", bad)
		}
	}
	if _, err := UnmarshalFilterExpr([]byte(`{"op":"not","expr":{"op":"nope"}}`)); err == nil {
		t.Fatal("not with bad nested op must propagate")
	}
	if _, err := UnmarshalFilterExpr([]byte(`{"op":"not","zz":1}`)); err == nil {
		t.Fatal("not with unknown field must be rejected")
	}
	// exists / olderThan strict rejection.
	if _, err := UnmarshalFilterExpr([]byte(`{"op":"exists","field":"b","zz":1}`)); err == nil {
		t.Fatal("exists with unknown field must be rejected")
	}
	if _, err := UnmarshalFilterExpr([]byte(`{"op":"olderThan","field":"t","ms":1,"zz":1}`)); err == nil {
		t.Fatal("olderThan with unknown field must be rejected")
	}
	if _, err := UnmarshalFilterExpr([]byte(`not json`)); err == nil {
		t.Fatal("malformed JSON must be rejected")
	}
}
