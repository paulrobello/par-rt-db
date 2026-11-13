// go-client/admin/directive_test.go
package admin

// Directive union coverage: every variant marshals to its tagged wire form
// and round-trips through UnmarshalDirective; unknown ops and malformed
// payloads are rejected; the dual-accept Expr/Cond sources marshal and
// decode both the typed expression and the legacy raw-SQL string.

import (
	"encoding/json"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestDirectiveMarshalRoundTrip(t *testing.T) {
	directives := []Directive{
		DirectiveRenameField{Table: "t", From: "a", To: "b"},
		DirectiveRenameTable{From: "t1", To: "t2"},
		DirectiveChangeType{Table: "t", Field: "f", To: wire.Number("5"), Cast: wire.CastToNumber},
		DirectiveChangeType{Table: "t", Field: "f", To: wire.Number("5"), Cast: wire.CastToNumber, Default: wire.Number("0")},
		DirectiveDropField{Table: "t", Field: "f"},
		DirectiveDropTable{Name: "t"},
		DirectiveDropIndex{Table: "t", Name: "ix"},
		DirectiveSetDefault{Table: "t", Field: "f", Value: wire.Number("7")},
		DirectiveEvalExpr{Table: "t", Set: "s", Expr: ExprSource{Legacy: "SELECT 1"}},
		DirectiveEvalExpr{Table: "t", Set: "s", Expr: ExprSource{Typed: wire.ValueField{Field: "a"}},
			Where: &CondSource{Typed: wire.FilterEq{Field: "k", Value: wire.String("v")}}},
	}
	for _, d := range directives {
		b, err := json.Marshal(d)
		if err != nil {
			t.Fatalf("marshal %T: %v", d, err)
		}
		if !jsonValidObjectWithTag(t, b, "op") {
			t.Fatalf("%T: not a tagged object: %s", d, b)
		}
		back, err := UnmarshalDirective(b)
		if err != nil {
			t.Fatalf("UnmarshalDirective %s: %v", b, err)
		}
		backB, err := json.Marshal(back)
		if err != nil {
			t.Fatalf("re-marshal %T: %v", back, err)
		}
		if !bodyEqual(backB, string(b)) {
			t.Fatalf("%T round trip drifted: %s vs %s", d, backB, b)
		}
	}
}

// jsonValidObjectWithTag asserts b is a JSON object carrying the tag key.
func jsonValidObjectWithTag(t *testing.T, b []byte, tagKey string) bool {
	t.Helper()
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return false
	}
	_, ok := m[tagKey]
	return ok
}

// Every variant must satisfy the Directive interface; exercising the
// concrete marker keeps the interface assertion honest.
func TestDirectiveVariantsImplementInterface(t *testing.T) {
	DirectiveRenameField{}.isDirective()
	DirectiveRenameTable{}.isDirective()
	DirectiveChangeType{}.isDirective()
	DirectiveDropField{}.isDirective()
	DirectiveDropTable{}.isDirective()
	DirectiveDropIndex{}.isDirective()
	DirectiveSetDefault{}.isDirective()
	DirectiveEvalExpr{}.isDirective()
}

func TestUnmarshalDirectiveRoutes(t *testing.T) {
	bodies := map[string]string{
		"renameField": `{"op":"renameField","table":"t","from":"a","to":"b"}`,
		"renameTable": `{"op":"renameTable","from":"t1","to":"t2"}`,
		"changeType":  `{"op":"changeType","table":"t","field":"f","to":5,"cast":"toNumber"}`,
		"dropField":   `{"op":"dropField","table":"t","field":"f"}`,
		"dropTable":   `{"op":"dropTable","name":"t"}`,
		"dropIndex":   `{"op":"dropIndex","table":"t","name":"ix"}`,
		"setDefault":  `{"op":"setDefault","table":"t","field":"f","value":7}`,
		"evalExpr":    `{"op":"evalExpr","table":"t","set":"s","expr":{"op":"field","field":"a"}}`,
	}
	wantTypes := map[string]any{
		"renameField": DirectiveRenameField{},
		"renameTable": DirectiveRenameTable{},
		"changeType":  DirectiveChangeType{},
		"dropField":   DirectiveDropField{},
		"dropTable":   DirectiveDropTable{},
		"dropIndex":   DirectiveDropIndex{},
		"setDefault":  DirectiveSetDefault{},
		"evalExpr":    DirectiveEvalExpr{},
	}
	for tag, body := range bodies {
		d, err := UnmarshalDirective([]byte(body))
		if err != nil {
			t.Fatalf("%s: %v", tag, err)
		}
		if wantTypes[tag] == nil {
			t.Fatalf("missing want type for %s", tag)
		}
		// Same concrete type: compare via reflect-free round trip — re-marshal
		// and diff against the input body.
		b, err := json.Marshal(d)
		if err != nil {
			t.Fatalf("%s marshal: %v", tag, err)
		}
		if !bodyEqual(b, body) {
			t.Fatalf("%s: %s vs %s", tag, b, body)
		}
	}
}

func TestUnmarshalDirectiveErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"unknown_op", `{"op":"dropDatabase","name":"x"}`},
		{"missing_tag", `{"table":"t"}`},
		{"not_an_object", `[1,2]`},
		{"malformed", `{"op":`},
		{"variant_unknown_field", `{"op":"setDefault","table":"t","field":"f","value":1,"zz":2}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := UnmarshalDirective([]byte(tc.body)); err == nil {
				t.Fatalf("want error for %s", tc.body)
			}
		})
	}
}

func TestExprSourceMarshal(t *testing.T) {
	// Typed expression marshals as the tagged expr object.
	b, err := json.Marshal(ExprSource{Typed: wire.ValueField{Field: "a"}})
	if err != nil {
		t.Fatal(err)
	}
	if !bodyEqual(b, `{"op":"field","field":"a"}`) {
		t.Fatalf("typed: %s", b)
	}
	// Legacy string marshals as a quoted string.
	b, err = json.Marshal(ExprSource{Legacy: "SELECT 1"})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `"SELECT 1"` {
		t.Fatalf("legacy: %s", b)
	}
}

func TestExprSourceUnmarshal(t *testing.T) {
	var e ExprSource
	if err := json.Unmarshal([]byte(`"SELECT 1"`), &e); err != nil {
		t.Fatal(err)
	}
	if e.Typed != nil || e.Legacy != "SELECT 1" {
		t.Fatalf("legacy decode: %+v", e)
	}
	if err := json.Unmarshal([]byte(`{"op":"field","field":"a"}`), &e); err != nil {
		t.Fatal(err)
	}
	if e.Legacy != "" || e.Typed == nil {
		t.Fatalf("typed decode: %+v", e)
	}
	if _, err := json.Marshal(e); err != nil {
		t.Fatalf("re-marshal typed: %v", err)
	}
	// A malformed typed expression must error.
	if err := json.Unmarshal([]byte(`{"op":"zzUnknown"}`), &e); err == nil {
		t.Fatal("unknown expr op must be rejected")
	}
	// A non-string, non-object scalar must error too.
	if err := json.Unmarshal([]byte(`42`), &e); err == nil {
		t.Fatal("scalar expr must be rejected")
	}
	// A truncated quoted string errors inside the legacy branch.
	if err := json.Unmarshal([]byte(`"abc`), &e); err == nil {
		t.Fatal("truncated string must be rejected")
	}
}

func TestCondSourceMarshal(t *testing.T) {
	b, err := json.Marshal(CondSource{Typed: wire.FilterEq{Field: "k", Value: wire.String("v")}})
	if err != nil {
		t.Fatal(err)
	}
	if !bodyEqual(b, `{"op":"eq","field":"k","value":"v"}`) {
		t.Fatalf("typed: %s", b)
	}
	b, err = json.Marshal(CondSource{Legacy: "age > 3"})
	if err != nil {
		t.Fatal(err)
	}
	// Compare as a decoded string: json.Marshal HTML-escapes '>'.
	var got string
	if json.Unmarshal(b, &got) != nil || got != "age > 3" {
		t.Fatalf("legacy: %s decoded %q", b, got)
	}
}

func TestCondSourceUnmarshal(t *testing.T) {
	var c CondSource
	if err := json.Unmarshal([]byte(`"age > 3"`), &c); err != nil {
		t.Fatal(err)
	}
	if c.Typed != nil || c.Legacy != "age > 3" {
		t.Fatalf("legacy decode: %+v", c)
	}
	if err := json.Unmarshal([]byte(`{"op":"eq","field":"k","value":"v"}`), &c); err != nil {
		t.Fatal(err)
	}
	if c.Legacy != "" || c.Typed == nil {
		t.Fatalf("typed decode: %+v", c)
	}
	if _, err := json.Marshal(c); err != nil {
		t.Fatalf("re-marshal typed: %v", err)
	}
	if err := json.Unmarshal([]byte(`{"op":"zzUnknown"}`), &c); err == nil {
		t.Fatal("unknown filter op must be rejected")
	}
	if err := json.Unmarshal([]byte(`true`), &c); err == nil {
		t.Fatal("bool predicate must be rejected")
	}
	// A truncated quoted string errors inside the legacy branch.
	if err := json.Unmarshal([]byte(`"abc`), &c); err == nil {
		t.Fatal("truncated string must be rejected")
	}
}

// DirectiveChangeType's custom decoder: the to/default blobs decode through
// wire.UnmarshalJSON, default stays nil when absent, unknown fields and
// malformed payloads are rejected. The strict struct carries no "op" key —
// DecodeTagged strips the tag before this UnmarshalJSON sees the body.
func TestDirectiveChangeTypeUnmarshal(t *testing.T) {
	var v DirectiveChangeType
	body := `{"table":"t","field":"f","to":5,"cast":"toNumber"}`
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatal(err)
	}
	if v.Table != "t" || v.Field != "f" || v.Cast != wire.CastToNumber || v.Default != nil || v.To != wire.Number("5") {
		t.Fatalf("got to=%#v default=%#v %+v", v.To, v.Default, v)
	}

	// A present default decodes too.
	v = DirectiveChangeType{}
	if err := json.Unmarshal([]byte(`{"table":"t","field":"f","to":5,"cast":"toNumber","default":0}`), &v); err != nil {
		t.Fatal(err)
	}
	if v.Default != wire.Number("0") {
		t.Fatalf("got default %#v", v.Default)
	}

	for _, bad := range []string{
		`{"table":"t","field":"f","to":5,"cast":"toNumber","zz":1}`,
		`{"table":"t","field":"f","to":nope,"cast":"toNumber"}`,
		`{"table":"t","field":"f","to":5,"cast":"toNumber","default":nope}`,
	} {
		if err := json.Unmarshal([]byte(bad), &v); err == nil {
			t.Fatalf("want error for %s", bad)
		}
	}
}

// DirectiveSetDefault's custom decoder: same strict shape without cast.
func TestDirectiveSetDefaultUnmarshal(t *testing.T) {
	var v DirectiveSetDefault
	if err := json.Unmarshal([]byte(`{"table":"t","field":"f","value":{"a":[1,true,null]}}`), &v); err != nil {
		t.Fatal(err)
	}
	arr, ok := v.Value.(wire.Object)
	if !ok {
		t.Fatalf("got %#v", v.Value)
	}
	wa, ok := arr["a"].(wire.Array)
	if !ok || len(wa) != 3 {
		t.Fatalf("got %#v", v.Value)
	}
	for _, bad := range []string{
		`{"table":"t","field":"f","value":1,"zz":2}`,
		`{"table":"t","field":"f","value":nope}`,
	} {
		if err := json.Unmarshal([]byte(bad), &v); err == nil {
			t.Fatalf("want error for %s", bad)
		}
	}
}
