// go-client/wire/tagged_test.go
package wire

import "testing"

type toyVariant struct {
	Field string `json:"field"`
}

func TestTaggedCodec(t *testing.T) {
	b, err := MarshalTagged("op", "insert", toyVariant{Field: "v"})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"field":"v","op":"insert"}` && string(b) != `{"op":"insert","field":"v"}` {
		t.Fatalf("tag not injected: %s", b)
	}
	if _, err := DecodeTagged[toyVariant](b, "op"); err != nil {
		t.Fatal(err)
	}
	if _, err := StrictUnmarshal[toyVariant]([]byte(`{"field":"v","nope":1}`)); err == nil {
		t.Fatal("unknown field must be rejected")
	}
	if _, err := StrictUnmarshal[toyVariant]([]byte(`{"field":"v"}{}`)); err == nil {
		t.Fatal("trailing data must be rejected")
	}
	if tag, _ := PeekTag(b, "op"); tag != "insert" {
		t.Fatalf("PeekTag got %q", tag)
	}
}
