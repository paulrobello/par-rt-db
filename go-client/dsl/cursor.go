// go-client/dsl/cursor.go
package dsl

// Mirrors rust-client/src/cursor.rs — the pagination cursor codec. Server
// cursors are standard base64 of a JSON array [indexValues..., createdAt,
// id]; the client normally passes them through opaquely.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// EncodeCursor base64-encodes a keyset [indexValues..., createdAt, id]
// into the opaque server cursor format.
func EncodeCursor(values []wire.JSONValue) (string, error) {
	b, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

// DecodeCursor decodes an opaque cursor into its keyset values. Errors on
// non-base64 or non-JSON-array input.
func DecodeCursor(s string) ([]wire.JSONValue, error) {
	raw, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor base64: %w", err)
	}
	v, err := wire.UnmarshalJSON(raw)
	if err != nil {
		return nil, fmt.Errorf("invalid cursor json: %w", err)
	}
	arr, ok := v.(wire.Array)
	if !ok {
		return nil, fmt.Errorf("cursor payload is not a JSON array")
	}
	return []wire.JSONValue(arr), nil
}
