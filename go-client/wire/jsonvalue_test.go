// go-client/wire/jsonvalue_test.go
package wire

import (
	"encoding/json"
	"testing"
)

func TestJSONValueRoundTrip(t *testing.T) {
	in := Object(map[string]JSONValue{
		"name":  String("x"),
		"n":     Number("6"),
		"big":   String("9007199254740993"),
		"tags":  Array([]JSONValue{String("a"), String("b")}),
		"inner": Object(map[string]JSONValue{"ok": Bool(true), "z": Null{}}),
	})
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	// Go forbids methods on interface types, so *JSONValue cannot satisfy
	// json.Unmarshaler; decode via the package-level UnmarshalJSON.
	out, err := UnmarshalJSON(b)
	if err != nil {
		t.Fatal(err)
	}
	rb, _ := json.Marshal(out)
	if string(b) != string(rb) {
		t.Fatalf("round trip drifted:\n%s\n%s", b, rb)
	}
}

func TestInt64SerializesAsDecimalString(t *testing.T) {
	b, err := json.Marshal(Int64("9007199254740993"))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `"9007199254740993"` {
		t.Fatalf("int64 must be a quoted decimal string, got %s", b)
	}
	v, err := ParseInt64(Int64("42"))
	if err != nil || v != 42 {
		t.Fatalf("ParseInt64: %v %v", v, err)
	}
}
