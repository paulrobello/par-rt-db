// go-client/wire/jsonvalue_extra_test.go
package wire

import (
	"encoding/json"
	"testing"
)

// TestJSONValueSealedMarkers invokes the sealed-interface marker methods so
// the closure of the JSONValue union is exercised, not just compile-time-
// checked. (These are zero-statement methods; coverage tools report them
// 0% regardless — the call asserts they exist and are wired.)
func TestJSONValueSealedMarkers(t *testing.T) {
	Null{}.isJSONValue()
	Bool(false).isJSONValue()
	Number("1").isJSONValue()
	String("s").isJSONValue()
	Array(nil).isJSONValue()
	Object(nil).isJSONValue()
}

// TestNumberMarshalJSON pins the raw-literal behavior: exact spelling is
// preserved (json.Number semantics), and the empty literal is an error.
func TestNumberMarshalJSON(t *testing.T) {
	b, err := json.Marshal(Number("9007199254740993"))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "9007199254740993" {
		t.Fatalf("big literal spelling lost: %s", b)
	}
	b, err = json.Marshal(Number("-1.5e3"))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "-1.5e3" {
		t.Fatalf("float spelling lost: %s", b)
	}
	if _, err := json.Marshal(Number("")); err == nil {
		t.Fatal("empty number literal must be an error")
	}
}

// TestJSONValueDecodeErrors covers the decode failure arms of UnmarshalJSON
// and the Int64 helpers.
func TestJSONValueDecodeErrors(t *testing.T) {
	if _, err := UnmarshalJSON([]byte(`{`)); err == nil {
		t.Fatal("truncated JSON must be rejected")
	}
	if _, err := UnmarshalJSON(nil); err == nil {
		t.Fatal("empty input must be rejected")
	}
	if _, err := ParseInt64(Int64("nope")); err == nil {
		t.Fatal("invalid decimal must be rejected")
	}
	// Null and Bool marshal to exact literals.
	b, err := json.Marshal(Null{})
	if err != nil || string(b) != "null" {
		t.Fatalf("null literal: %s %v", b, err)
	}
	b, err = json.Marshal(Bool(true))
	if err != nil || string(b) != "true" {
		t.Fatalf("bool literal: %s %v", b, err)
	}
	// Int64 always quotes.
	b, err = json.Marshal(Int64("42"))
	if err != nil || string(b) != `"42"` {
		t.Fatalf("int64 quoting: %s %v", b, err)
	}
}
