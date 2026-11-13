// go-client/wire/valueexpr_test.go
package wire

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

// TestValueExprAllVariantsRoundTrip pins the exact op-tag casing of every
// ValueExpr variant, then requires marshal → UnmarshalValueExpr → re-marshal
// to be byte-identical (the wire-parity contract against serde output).
func TestValueExprAllVariantsRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		expr ValueExpr
		tag  string
	}{
		{"field", ValueField{Field: "amount"}, `"op":"field"`},
		{"literal", ValueLiteral{Value: Number("9007199254740993")}, `"op":"literal"`},
		{"concat", ValueConcat{Parts: []ValueExpr{ValueField{Field: "a"}, ValueLiteral{Value: String("x")}}}, `"op":"concat"`},
		{"add", ValueAdd{Left: ValueField{Field: "a"}, Right: ValueLiteral{Value: Number("1")}}, `"op":"add"`},
		{"sub", ValueSub{Left: ValueField{Field: "a"}, Right: ValueLiteral{Value: Number("1")}}, `"op":"sub"`},
		{"mul", ValueMul{Left: ValueField{Field: "a"}, Right: ValueLiteral{Value: Number("2")}}, `"op":"mul"`},
		{"div", ValueDiv{Left: ValueField{Field: "a"}, Right: ValueLiteral{Value: Number("2")}}, `"op":"div"`},
		{"coalesce", ValueCoalesce{Parts: []ValueExpr{ValueField{Field: "a"}, ValueLiteral{Value: Null{}}}}, `"op":"coalesce"`},
		{"lower", ValueLower{Value: ValueField{Field: "a"}}, `"op":"lower"`},
		{"upper", ValueUpper{Value: ValueField{Field: "a"}}, `"op":"upper"`},
		{"trim", ValueTrim{Value: ValueField{Field: "a"}}, `"op":"trim"`},
		{"cast", ValueCast{Value: ValueField{Field: "n"}, To: CastToBoolean}, `"op":"cast"`},
		{"now", ValueNow{}, `"op":"now"`},
		{"case", ValueCase{
			Whens:     []CaseWhen{{When: FilterEq{Field: "a", Value: Number("1")}, Then: ValueField{Field: "x"}}},
			Otherwise: ValueNow{},
		}, `"op":"case"`},
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
			out, err := UnmarshalValueExpr(b)
			if err != nil {
				t.Fatal(err)
			}
			if reflect.TypeOf(out) != reflect.TypeOf(tc.expr) {
				t.Fatalf("decoded %T, want %T", out, tc.expr)
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

// TestValueExprExactWireShapes pins byte-exact marshaled JSON for
// representative variants (map-key ordering is alphabetical: MarshalTagged
// injects the tag through a map, so keys sort).
func TestValueExprExactWireShapes(t *testing.T) {
	exact := []struct {
		expr ValueExpr
		want string
	}{
		{ValueNow{}, `{"op":"now"}`},
		{ValueField{Field: "n"}, `{"field":"n","op":"field"}`},
		{ValueCast{Value: ValueField{Field: "n"}, To: CastToInt64}, `{"op":"cast","to":"toInt64","value":{"field":"n","op":"field"}}`},
		{ValueLiteral{Value: Bool(true)}, `{"op":"literal","value":true}`},
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

// TestValueExprDecodeErrors exercises the decode error branches: missing or
// non-string tag, unknown op, strict unknown-field rejection, and failed
// nested conversion.
func TestValueExprDecodeErrors(t *testing.T) {
	if _, err := UnmarshalValueExpr([]byte(`{"field":"a"}`)); err == nil {
		t.Fatal("missing op tag must be rejected")
	}
	if _, err := UnmarshalValueExpr([]byte(`{"op":1}`)); err == nil {
		t.Fatal("non-string op tag must be rejected")
	}
	if _, err := UnmarshalValueExpr([]byte(`{"op":"field","field":"a","nope":1}`)); err == nil {
		t.Fatal("unknown field must be rejected")
	}
	if _, err := UnmarshalValueExpr([]byte(`{"op":"literal"}`)); err == nil {
		t.Fatal("literal without value must be rejected")
	}
	if _, err := UnmarshalValueExpr([]byte(`{"op":"add","left":{"op":"field","field":"a"}}`)); err == nil {
		t.Fatal("add without right must be rejected")
	}
	if _, err := UnmarshalValueExpr([]byte(`{"op":"now","extra":1}`)); err == nil {
		t.Fatal("now must reject unknown fields")
	}
	if _, err := UnmarshalValueExpr([]byte(`{"op":"concat","parts":[{"op":"nope"}]}`)); err == nil {
		t.Fatal("nested bad op must propagate")
	}
	// Left-side and right-side decode failures for the binary ops.
	for _, op := range []string{"add", "sub", "mul", "div"} {
		in := fmt.Sprintf(`{"op":%q,"left":{"op":"nope"},"right":{"op":"now"}}`, op)
		if _, err := UnmarshalValueExpr([]byte(in)); err == nil {
			t.Fatalf("%s with bad left must propagate", op)
		}
		in = fmt.Sprintf(`{"op":%q,"left":{"op":"now"},"right":{"op":"nope"}}`, op)
		if _, err := UnmarshalValueExpr([]byte(in)); err == nil {
			t.Fatalf("%s with bad right must propagate", op)
		}
	}
	// Nested-decode failures for the unary ops and cast.
	for _, op := range []string{"lower", "upper", "trim", "cast"} {
		in := fmt.Sprintf(`{"op":%q,"value":{"op":"nope"}}`, op)
		if _, err := UnmarshalValueExpr([]byte(in)); err == nil {
			t.Fatalf("%s with bad nested value must propagate", op)
		}
	}
	// Array-element decode failures.
	for _, op := range []string{"concat", "coalesce"} {
		in := fmt.Sprintf(`{"op":%q,"parts":[{"op":"field","field":"a","nope":1}]}`, op)
		if _, err := UnmarshalValueExpr([]byte(in)); err == nil {
			t.Fatalf("%s with strict-rejected part must propagate", op)
		}
	}
	if _, err := UnmarshalValueExpr([]byte(`{"op":"case","whens":[],"otherwise":{"op":"nope"}}`)); err == nil {
		t.Fatal("bad otherwise must propagate")
	}
	if _, err := UnmarshalValueExpr([]byte(`{}`)); err == nil {
		t.Fatal("empty object must be rejected")
	}
	// Per-variant strict-decode rejection: unknown field on every op.
	for _, in := range []string{
		`{"op":"field","field":"a","zz":1}`,
		`{"op":"literal","value":1,"zz":1}`,
		`{"op":"concat","parts":[],"zz":1}`,
		`{"op":"coalesce","parts":[],"zz":1}`,
		`{"op":"add","left":{"op":"now"},"right":{"op":"now"},"zz":1}`,
		`{"op":"sub","left":{"op":"now"},"right":{"op":"now"},"zz":1}`,
		`{"op":"mul","left":{"op":"now"},"right":{"op":"now"},"zz":1}`,
		`{"op":"div","left":{"op":"now"},"right":{"op":"now"},"zz":1}`,
		`{"op":"lower","value":{"op":"now"},"zz":1}`,
		`{"op":"upper","value":{"op":"now"},"zz":1}`,
		`{"op":"trim","value":{"op":"now"},"zz":1}`,
		`{"op":"cast","value":{"op":"now"},"to":"toString","zz":1}`,
		`{"op":"case","whens":[],"otherwise":{"op":"now"},"zz":1}`,
	} {
		if _, err := UnmarshalValueExpr([]byte(in)); err == nil {
			t.Fatalf("unknown field must be rejected: %s", in)
		}
	}
	if _, err := UnmarshalValueExpr([]byte(`not json`)); err == nil {
		t.Fatal("malformed JSON must be rejected")
	}
	if _, err := UnmarshalValueExpr([]byte(`[]`)); err == nil {
		t.Fatal("non-object must be rejected")
	}
}

// TestValueExprSealedMarkers invokes the sealed-interface marker methods so
// the compiler-enforced closure of the grammar is exercised, not just
// compile-time-checked.
func TestValueExprSealedMarkers(t *testing.T) {
	ValueField{}.isValueExpr()
	ValueLiteral{}.isValueExpr()
	ValueConcat{}.isValueExpr()
	ValueAdd{}.isValueExpr()
	ValueSub{}.isValueExpr()
	ValueMul{}.isValueExpr()
	ValueDiv{}.isValueExpr()
	ValueCoalesce{}.isValueExpr()
	ValueLower{}.isValueExpr()
	ValueUpper{}.isValueExpr()
	ValueTrim{}.isValueExpr()
	ValueCast{}.isValueExpr()
	ValueNow{}.isValueExpr()
	ValueCase{}.isValueExpr()
}

// TestCaseWhenDecode pins the CaseWhen {when, then} wire shape and its
// unknown-field rejection.
func TestCaseWhenDecode(t *testing.T) {
	in := []byte(`{"when":{"op":"eq","field":"a","value":1},"then":{"op":"field","field":"x"}}`)
	var cw CaseWhen
	if err := json.Unmarshal(in, &cw); err != nil {
		t.Fatal(err)
	}
	if _, ok := cw.When.(FilterEq); !ok {
		t.Fatalf("when not decoded: %T", cw.When)
	}
	if _, ok := cw.Then.(ValueField); !ok {
		t.Fatalf("then not decoded: %T", cw.Then)
	}
	b, err := json.Marshal(cw)
	if err != nil {
		t.Fatal(err)
	}
	// Struct field order (when, then) with tag-injected inner keys sorted.
	want := `{"when":{"field":"a","op":"eq","value":1},"then":{"field":"x","op":"field"}}`
	if string(b) != want {
		t.Fatalf("CaseWhen shape wrong:\n got %s\nwant %s", b, want)
	}
	if err := json.Unmarshal([]byte(`{"when":{"op":"eq","field":"a","value":1},"then":{"op":"field","field":"x"},"extra":1}`), &cw); err == nil {
		t.Fatal("unknown field must be rejected")
	}
	if err := json.Unmarshal([]byte(`{"when":{"op":"nope"},"then":{"op":"field","field":"x"}}`), &cw); err == nil {
		t.Fatal("bad when must propagate")
	}
	if err := json.Unmarshal([]byte(`{"when":{"op":"eq","field":"a","value":1},"then":{"op":"nope"}}`), &cw); err == nil {
		t.Fatal("bad then must propagate")
	}
}

// TestValueCaseDecodeErrors covers the ValueCase decode arms: missing
// otherwise fails; missing whens decodes to an empty slice (the code's
// actual behavior); a bad when element propagates.
func TestValueCaseDecodeErrors(t *testing.T) {
	if _, err := UnmarshalValueExpr([]byte(`{"op":"case","whens":[]}`)); err == nil {
		t.Fatal("case without otherwise must be rejected")
	}
	if _, err := UnmarshalValueExpr([]byte(`{"op":"case","whens":[{"when":{"op":"nope"},"then":{"op":"now"}}],"otherwise":{"op":"now"}}`)); err == nil {
		t.Fatal("bad when element must propagate")
	}
	out, err := UnmarshalValueExpr([]byte(`{"op":"case","otherwise":{"op":"now"}}`))
	if err != nil {
		t.Fatalf("missing whens decodes empty: %v", err)
	}
	if c, ok := out.(ValueCase); !ok || len(c.Whens) != 0 {
		t.Fatalf("expected empty whens, got %#v", out)
	}
}
