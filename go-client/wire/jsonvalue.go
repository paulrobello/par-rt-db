// go-client/wire/jsonvalue.go
package wire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

// Mirrors serde_json::Value as used across core/src/wire.rs.
//
// The six variants are a sealed set: each implements the unexported
// marker isJSONValue, so no external type can satisfy JSONValue.
//
// Number wraps json.Number so integer literals beyond float64 precision
// survive decode/encode verbatim. Null needs an explicit MarshalJSON
// (a zero-value struct would otherwise encode as "{}"), and Number needs
// one too (the encoding/json raw-number fast path applies only to the
// exact type json.Number, not a defined type based on it).
type JSONValue interface {
	isJSONValue()
}

// Mirrors serde_json::Value::Null as used across core/src/wire.rs.
type Null struct{}

// MarshalJSON emits the literal null.
func (Null) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

func (Null) isJSONValue() {}

// Mirrors serde_json::Value::Bool as used across core/src/wire.rs.
type Bool bool

func (Bool) isJSONValue() {}

// Mirrors serde_json::Value::Number as used across core/src/wire.rs.
type Number json.Number

// MarshalJSON emits the literal's raw bytes, unquoted, preserving
// arbitrary-precision spellings exactly as received.
func (n Number) MarshalJSON() ([]byte, error) {
	if len(n) == 0 {
		return nil, fmt.Errorf("wire: empty JSON number literal")
	}
	return []byte(n), nil
}

func (Number) isJSONValue() {}

// Mirrors serde_json::Value::String as used across core/src/wire.rs.
type String string

func (String) isJSONValue() {}

// Mirrors serde_json::Value::Array as used across core/src/wire.rs.
type Array []JSONValue

func (Array) isJSONValue() {}

// Mirrors serde_json::Value::Object as used across core/src/wire.rs.
type Object map[string]JSONValue

func (Object) isJSONValue() {}

// UnmarshalJSON decodes JSON into a JSONValue tree. Integers and floats
// decode as Number (json.Number) via UseNumber, so int64-scale literals
// never lose precision.
func UnmarshalJSON(data []byte) (JSONValue, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var a any
	if err := dec.Decode(&a); err != nil {
		return nil, err
	}
	return jsonValueFromAny(a)
}

func jsonValueFromAny(a any) (JSONValue, error) {
	switch x := a.(type) {
	case nil:
		return Null{}, nil
	case bool:
		return Bool(x), nil
	case json.Number:
		return Number(x), nil
	case string:
		return String(x), nil
	case []any:
		arr := make(Array, len(x))
		for i, e := range x {
			v, err := jsonValueFromAny(e)
			if err != nil {
				return nil, err
			}
			arr[i] = v
		}
		return arr, nil
	case map[string]any:
		obj := make(Object, len(x))
		for k, e := range x {
			v, err := jsonValueFromAny(e)
			if err != nil {
				return nil, err
			}
			obj[k] = v
		}
		return obj, nil
	default:
		return nil, fmt.Errorf("wire: unsupported JSON value kind %T", a)
	}
}

// Mirrors the i64-as-decimal-string wire convention used across
// core/src/mutation.rs and core/src/wire.rs: int64 index values travel
// quoted, never as bare JSON numbers.
type Int64 string

// MarshalJSON always emits a quoted decimal string.
func (i Int64) MarshalJSON() ([]byte, error) { return json.Marshal(string(i)) }

// ParseInt64 converts a wire Int64 to a Go int64.
func ParseInt64(i Int64) (int64, error) {
	v, err := strconv.ParseInt(string(i), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("wire: invalid int64 %q: %w", string(i), err)
	}
	return v, nil
}
