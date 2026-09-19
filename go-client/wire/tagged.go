// go-client/wire/tagged.go
package wire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// The serde internally-tagged enum analog. Every union type on the wire
// (ClientMessage, ServerMessage, FilterExpr, ValueExpr, Step, ScheduleWhen)
// serializes as a variant struct plus one injected tag key, and decodes by
// reading the tag key, stripping it, and strict-decoding the remainder.

// MarshalTagged marshals v (whose struct has NO tag field) and injects
// tagKey: tag into the resulting object.
func MarshalTagged(tagKey, tag string, v any) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	m[tagKey] = json.RawMessage(mustQuote(tag))
	return json.Marshal(m)
}

// DecodeTagged strips tagKey from the object, then strict-decodes the
// remainder into T. The tag value itself is validated by the caller's
// dispatch switch before DecodeTagged is reached.
func DecodeTagged[T any](data []byte, tagKey string) (T, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		var zero T
		return zero, err
	}
	delete(m, tagKey)
	b, err := json.Marshal(m)
	if err != nil {
		var zero T
		return zero, err
	}
	return StrictUnmarshal[T](b)
}

// PeekTag reads tagKey's string value from a wire object without
// decoding the rest. Used by the union dispatchers to route to a variant.
func PeekTag(data []byte, tagKey string) (string, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return "", err
	}
	raw, ok := m[tagKey]
	if !ok {
		return "", fmt.Errorf("wire: missing %q tag", tagKey)
	}
	var tag string
	if err := json.Unmarshal(raw, &tag); err != nil {
		return "", fmt.Errorf("wire: %q tag is not a string: %w", tagKey, err)
	}
	return tag, nil
}

func mustQuote(s string) []byte {
	b, _ := json.Marshal(s)
	return b
}

// bytesReader is a tiny local alias so strict.go avoids importing bytes
// for one constructor.
func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }
