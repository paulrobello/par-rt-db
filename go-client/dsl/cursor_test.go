// go-client/dsl/cursor_test.go
package dsl

import (
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestCursorRoundTrip(t *testing.T) {
	values := []wire.JSONValue{
		wire.String("p1"),
		wire.String("backlog"),
		wire.Number("1700000000000"),
		wire.String("id1"),
	}
	s, err := EncodeCursor(values)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := DecodeCursor(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 4 {
		t.Fatalf("want 4 values, got %d", len(raw))
	}
	if raw[3] != wire.JSONValue(wire.String("id1")) {
		t.Fatalf("last keyset value drifted: %v", raw[3])
	}
}

func TestCursorDecodeRejectsGarbage(t *testing.T) {
	if _, err := DecodeCursor("!!!not-base64!!!"); err == nil {
		t.Fatal("non-base64 cursor must be rejected")
	}
}

func TestCursorDecodeRejectsNonArray(t *testing.T) {
	// base64 of `"hello"` — a JSON string, not an array
	if _, err := DecodeCursor("ImhlbGxvIg=="); err == nil {
		t.Fatal("non-array cursor payload must be rejected")
	}
}
