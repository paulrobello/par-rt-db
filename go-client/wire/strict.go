// go-client/wire/strict.go
package wire

import (
	"encoding/json"
	"fmt"
	"io"
)

// StrictUnmarshal decodes data into T with serde `deny_unknown_fields`
// semantics: unknown fields are rejected, and trailing bytes after the
// first JSON value are rejected. Every wire type decodes through this.
func StrictUnmarshal[T any](data []byte) (T, error) {
	var out T
	dec := json.NewDecoder(bytesReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		return out, err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			return out, fmt.Errorf("wire: trailing data after JSON value")
		}
		return out, fmt.Errorf("wire: trailing data after JSON value: %w", err)
	}
	return out, nil
}
