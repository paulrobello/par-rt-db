// go-client/wire/filter.go
package wire

// Mirrors core/src/wire.rs::FilterExpr — the db-side predicate grammar.
// Tag key "op", lowercase tags, with the camelCase exception "olderThan".
// Strict decode; unknown fields rejected; unknown op is an error.

import (
	"encoding/json"
	"fmt"
)

// FilterExpr is the db-side predicate grammar appended to a query's WHERE
// clause (and to patchByQuery/deleteByQuery step filters).
type FilterExpr interface {
	isFilterExpr()
}

// Mirrors core/src/wire.rs::FilterExpr::Eq
type FilterEq struct {
	Field string    `json:"field"`
	Value JSONValue `json:"value"`
}

func (FilterEq) isFilterExpr() {}

func (v FilterEq) MarshalJSON() ([]byte, error) {
	type alias FilterEq
	return MarshalTagged("op", "eq", alias(v))
}

func (v *FilterEq) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Field string          `json:"field"`
		Value json.RawMessage `json:"value"`
	}](b)
	if err != nil {
		return err
	}
	val, err := UnmarshalJSON(r.Value)
	if err != nil {
		return err
	}
	v.Field, v.Value = r.Field, val
	return nil
}

// Mirrors core/src/wire.rs::FilterExpr::Neq
type FilterNeq struct {
	Field string    `json:"field"`
	Value JSONValue `json:"value"`
}

func (FilterNeq) isFilterExpr() {}

func (v FilterNeq) MarshalJSON() ([]byte, error) {
	type alias FilterNeq
	return MarshalTagged("op", "neq", alias(v))
}

func (v *FilterNeq) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Field string          `json:"field"`
		Value json.RawMessage `json:"value"`
	}](b)
	if err != nil {
		return err
	}
	val, err := UnmarshalJSON(r.Value)
	if err != nil {
		return err
	}
	v.Field, v.Value = r.Field, val
	return nil
}

// Mirrors core/src/wire.rs::FilterExpr::Gt
type FilterGt struct {
	Field string    `json:"field"`
	Value JSONValue `json:"value"`
}

func (FilterGt) isFilterExpr() {}

func (v FilterGt) MarshalJSON() ([]byte, error) {
	type alias FilterGt
	return MarshalTagged("op", "gt", alias(v))
}

func (v *FilterGt) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Field string          `json:"field"`
		Value json.RawMessage `json:"value"`
	}](b)
	if err != nil {
		return err
	}
	val, err := UnmarshalJSON(r.Value)
	if err != nil {
		return err
	}
	v.Field, v.Value = r.Field, val
	return nil
}

// Mirrors core/src/wire.rs::FilterExpr::Gte
type FilterGte struct {
	Field string    `json:"field"`
	Value JSONValue `json:"value"`
}

func (FilterGte) isFilterExpr() {}

func (v FilterGte) MarshalJSON() ([]byte, error) {
	type alias FilterGte
	return MarshalTagged("op", "gte", alias(v))
}

func (v *FilterGte) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Field string          `json:"field"`
		Value json.RawMessage `json:"value"`
	}](b)
	if err != nil {
		return err
	}
	val, err := UnmarshalJSON(r.Value)
	if err != nil {
		return err
	}
	v.Field, v.Value = r.Field, val
	return nil
}

// Mirrors core/src/wire.rs::FilterExpr::Lt
type FilterLt struct {
	Field string    `json:"field"`
	Value JSONValue `json:"value"`
}

func (FilterLt) isFilterExpr() {}

func (v FilterLt) MarshalJSON() ([]byte, error) {
	type alias FilterLt
	return MarshalTagged("op", "lt", alias(v))
}

func (v *FilterLt) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Field string          `json:"field"`
		Value json.RawMessage `json:"value"`
	}](b)
	if err != nil {
		return err
	}
	val, err := UnmarshalJSON(r.Value)
	if err != nil {
		return err
	}
	v.Field, v.Value = r.Field, val
	return nil
}

// Mirrors core/src/wire.rs::FilterExpr::Lte
type FilterLte struct {
	Field string    `json:"field"`
	Value JSONValue `json:"value"`
}

func (FilterLte) isFilterExpr() {}

func (v FilterLte) MarshalJSON() ([]byte, error) {
	type alias FilterLte
	return MarshalTagged("op", "lte", alias(v))
}

func (v *FilterLte) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Field string          `json:"field"`
		Value json.RawMessage `json:"value"`
	}](b)
	if err != nil {
		return err
	}
	val, err := UnmarshalJSON(r.Value)
	if err != nil {
		return err
	}
	v.Field, v.Value = r.Field, val
	return nil
}

// Mirrors core/src/wire.rs::FilterExpr::In
type FilterIn struct {
	Field  string      `json:"field"`
	Values []JSONValue `json:"values"`
}

func (FilterIn) isFilterExpr() {}

func (v FilterIn) MarshalJSON() ([]byte, error) {
	type alias FilterIn
	return MarshalTagged("op", "in", alias(v))
}

func (v *FilterIn) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Field  string            `json:"field"`
		Values []json.RawMessage `json:"values"`
	}](b)
	if err != nil {
		return err
	}
	vals := make([]JSONValue, len(r.Values))
	for i := range r.Values {
		iv, err := UnmarshalJSON(r.Values[i])
		if err != nil {
			return err
		}
		vals[i] = iv
	}
	v.Field, v.Values = r.Field, vals
	return nil
}

// Mirrors core/src/wire.rs::FilterExpr::Contains
type FilterContains struct {
	Field string    `json:"field"`
	Value JSONValue `json:"value"`
}

func (FilterContains) isFilterExpr() {}

func (v FilterContains) MarshalJSON() ([]byte, error) {
	type alias FilterContains
	return MarshalTagged("op", "contains", alias(v))
}

func (v *FilterContains) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Field string          `json:"field"`
		Value json.RawMessage `json:"value"`
	}](b)
	if err != nil {
		return err
	}
	val, err := UnmarshalJSON(r.Value)
	if err != nil {
		return err
	}
	v.Field, v.Value = r.Field, val
	return nil
}

// Mirrors core/src/wire.rs::FilterExpr::And
type FilterAnd struct {
	Exprs []FilterExpr `json:"exprs"`
}

func (FilterAnd) isFilterExpr() {}

func (v FilterAnd) MarshalJSON() ([]byte, error) {
	type alias FilterAnd
	return MarshalTagged("op", "and", alias(v))
}

func (v *FilterAnd) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Exprs []json.RawMessage `json:"exprs"`
	}](b)
	if err != nil {
		return err
	}
	exprs := make([]FilterExpr, len(r.Exprs))
	for i := range r.Exprs {
		e, err := UnmarshalFilterExpr(r.Exprs[i])
		if err != nil {
			return err
		}
		exprs[i] = e
	}
	v.Exprs = exprs
	return nil
}

// Mirrors core/src/wire.rs::FilterExpr::Or
type FilterOr struct {
	Exprs []FilterExpr `json:"exprs"`
}

func (FilterOr) isFilterExpr() {}

func (v FilterOr) MarshalJSON() ([]byte, error) {
	type alias FilterOr
	return MarshalTagged("op", "or", alias(v))
}

func (v *FilterOr) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Exprs []json.RawMessage `json:"exprs"`
	}](b)
	if err != nil {
		return err
	}
	exprs := make([]FilterExpr, len(r.Exprs))
	for i := range r.Exprs {
		e, err := UnmarshalFilterExpr(r.Exprs[i])
		if err != nil {
			return err
		}
		exprs[i] = e
	}
	v.Exprs = exprs
	return nil
}

// Mirrors core/src/wire.rs::FilterExpr::Not
type FilterNot struct {
	Expr FilterExpr `json:"expr"`
}

func (FilterNot) isFilterExpr() {}

func (v FilterNot) MarshalJSON() ([]byte, error) {
	type alias FilterNot
	return MarshalTagged("op", "not", alias(v))
}

func (v *FilterNot) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Expr json.RawMessage `json:"expr"`
	}](b)
	if err != nil {
		return err
	}
	e, err := UnmarshalFilterExpr(r.Expr)
	if err != nil {
		return err
	}
	v.Expr = e
	return nil
}

// Mirrors core/src/wire.rs::FilterExpr::Exists
type FilterExists struct {
	Field string `json:"field"`
}

func (FilterExists) isFilterExpr() {}

func (v FilterExists) MarshalJSON() ([]byte, error) {
	type alias FilterExists
	return MarshalTagged("op", "exists", alias(v))
}

func (v *FilterExists) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Field string `json:"field"`
	}](b)
	if err != nil {
		return err
	}
	v.Field = r.Field
	return nil
}

// Mirrors core/src/wire.rs::FilterExpr::OlderThan — the camelCase tag
// exception on an otherwise-lowercase enum. ms is a JSON number (i64),
// NOT the quoted int64-string convention (that applies to index values).
type FilterOlderThan struct {
	Field string `json:"field"`
	Ms    int64  `json:"ms"`
}

func (FilterOlderThan) isFilterExpr() {}

func (v FilterOlderThan) MarshalJSON() ([]byte, error) {
	type alias FilterOlderThan
	return MarshalTagged("op", "olderThan", alias(v))
}

func (v *FilterOlderThan) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Field string `json:"field"`
		Ms    int64  `json:"ms"`
	}](b)
	if err != nil {
		return err
	}
	v.Field, v.Ms = r.Field, r.Ms
	return nil
}

// UnmarshalFilterExpr routes a tagged filter object to its variant.
// Unknown op is an error, mirroring serde's closed enum.
func UnmarshalFilterExpr(data []byte) (FilterExpr, error) {
	tag, err := PeekTag(data, "op")
	if err != nil {
		return nil, err
	}
	switch tag {
	case "eq":
		return DecodeTagged[FilterEq](data, "op")
	case "neq":
		return DecodeTagged[FilterNeq](data, "op")
	case "gt":
		return DecodeTagged[FilterGt](data, "op")
	case "gte":
		return DecodeTagged[FilterGte](data, "op")
	case "lt":
		return DecodeTagged[FilterLt](data, "op")
	case "lte":
		return DecodeTagged[FilterLte](data, "op")
	case "in":
		return DecodeTagged[FilterIn](data, "op")
	case "and":
		return DecodeTagged[FilterAnd](data, "op")
	case "or":
		return DecodeTagged[FilterOr](data, "op")
	case "not":
		return DecodeTagged[FilterNot](data, "op")
	case "contains":
		return DecodeTagged[FilterContains](data, "op")
	case "exists":
		return DecodeTagged[FilterExists](data, "op")
	case "olderThan":
		return DecodeTagged[FilterOlderThan](data, "op")
	default:
		return nil, fmt.Errorf("wire: unknown filter op %q", tag)
	}
}
