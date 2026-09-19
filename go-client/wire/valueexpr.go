// go-client/wire/valueexpr.go
package wire

// Mirrors core/src/wire.rs::ValueExpr — the closed typed expression grammar
// for computed fields and migrate backfill. Tag key "op", camelCase tags,
// camelCase Cast values. Strict decode; interface-typed fields decode
// through per-variant UnmarshalJSON methods.

import (
	"encoding/json"
	"fmt"
)

// Mirrors core/src/wire.rs::Cast — closed scalar coercions, camelCase.
type Cast string

const (
	CastToString  Cast = "toString"
	CastToNumber  Cast = "toNumber"
	CastToInt64   Cast = "toInt64"
	CastToBoolean Cast = "toBoolean"
)

// ValueExpr is the closed, typed expression grammar. Variants:
// ValueField/Literal/Concat/Add/Sub/Mul/Div/Coalesce/Lower/Upper/Trim/Cast/
// Now/Case. There is deliberately no subquery, no function-call-by-name,
// and no raw-SQL node — the grammar is closed.
type ValueExpr interface {
	isValueExpr()
}

// Mirrors core/src/wire.rs::ValueExpr::Field
type ValueField struct {
	Field string `json:"field"`
}

func (ValueField) isValueExpr() {}

func (v ValueField) MarshalJSON() ([]byte, error) {
	type alias ValueField
	return MarshalTagged("op", "field", alias(v))
}

func (v *ValueField) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Field string `json:"field"`
	}](b)
	if err != nil {
		return err
	}
	v.Field = r.Field
	return nil
}

// Mirrors core/src/wire.rs::ValueExpr::Literal
type ValueLiteral struct {
	Value JSONValue `json:"value"`
}

func (ValueLiteral) isValueExpr() {}

func (v ValueLiteral) MarshalJSON() ([]byte, error) {
	type alias ValueLiteral
	return MarshalTagged("op", "literal", alias(v))
}

func (v *ValueLiteral) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Value json.RawMessage `json:"value"`
	}](b)
	if err != nil {
		return err
	}
	val, err := UnmarshalJSON(r.Value)
	if err != nil {
		return err
	}
	v.Value = val
	return nil
}

// Mirrors core/src/wire.rs::ValueExpr::Concat
type ValueConcat struct {
	Parts []ValueExpr `json:"parts"`
}

func (ValueConcat) isValueExpr() {}

func (v ValueConcat) MarshalJSON() ([]byte, error) {
	type alias ValueConcat
	return MarshalTagged("op", "concat", alias(v))
}

func (v *ValueConcat) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Parts []json.RawMessage `json:"parts"`
	}](b)
	if err != nil {
		return err
	}
	parts, err := decodeValueExprs(r.Parts)
	if err != nil {
		return err
	}
	v.Parts = parts
	return nil
}

// decodeValueExprs converts raw JSON objects into ValueExpr variants.
func decodeValueExprs(raws []json.RawMessage) ([]ValueExpr, error) {
	out := make([]ValueExpr, len(raws))
	for i := range raws {
		e, err := UnmarshalValueExpr(raws[i])
		if err != nil {
			return nil, err
		}
		out[i] = e
	}
	return out, nil
}

// Mirrors core/src/wire.rs::ValueExpr::Add
type ValueAdd struct {
	Left  ValueExpr `json:"left"`
	Right ValueExpr `json:"right"`
}

func (ValueAdd) isValueExpr() {}

func (v ValueAdd) MarshalJSON() ([]byte, error) {
	type alias ValueAdd
	return MarshalTagged("op", "add", alias(v))
}

func (v *ValueAdd) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Left  json.RawMessage `json:"left"`
		Right json.RawMessage `json:"right"`
	}](b)
	if err != nil {
		return err
	}
	left, err := UnmarshalValueExpr(r.Left)
	if err != nil {
		return err
	}
	right, err := UnmarshalValueExpr(r.Right)
	if err != nil {
		return err
	}
	v.Left, v.Right = left, right
	return nil
}

// Mirrors core/src/wire.rs::ValueExpr::Sub
type ValueSub struct {
	Left  ValueExpr `json:"left"`
	Right ValueExpr `json:"right"`
}

func (ValueSub) isValueExpr() {}

func (v ValueSub) MarshalJSON() ([]byte, error) {
	type alias ValueSub
	return MarshalTagged("op", "sub", alias(v))
}

func (v *ValueSub) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Left  json.RawMessage `json:"left"`
		Right json.RawMessage `json:"right"`
	}](b)
	if err != nil {
		return err
	}
	left, err := UnmarshalValueExpr(r.Left)
	if err != nil {
		return err
	}
	right, err := UnmarshalValueExpr(r.Right)
	if err != nil {
		return err
	}
	v.Left, v.Right = left, right
	return nil
}

// Mirrors core/src/wire.rs::ValueExpr::Mul
type ValueMul struct {
	Left  ValueExpr `json:"left"`
	Right ValueExpr `json:"right"`
}

func (ValueMul) isValueExpr() {}

func (v ValueMul) MarshalJSON() ([]byte, error) {
	type alias ValueMul
	return MarshalTagged("op", "mul", alias(v))
}

func (v *ValueMul) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Left  json.RawMessage `json:"left"`
		Right json.RawMessage `json:"right"`
	}](b)
	if err != nil {
		return err
	}
	left, err := UnmarshalValueExpr(r.Left)
	if err != nil {
		return err
	}
	right, err := UnmarshalValueExpr(r.Right)
	if err != nil {
		return err
	}
	v.Left, v.Right = left, right
	return nil
}

// Mirrors core/src/wire.rs::ValueExpr::Div
type ValueDiv struct {
	Left  ValueExpr `json:"left"`
	Right ValueExpr `json:"right"`
}

func (ValueDiv) isValueExpr() {}

func (v ValueDiv) MarshalJSON() ([]byte, error) {
	type alias ValueDiv
	return MarshalTagged("op", "div", alias(v))
}

func (v *ValueDiv) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Left  json.RawMessage `json:"left"`
		Right json.RawMessage `json:"right"`
	}](b)
	if err != nil {
		return err
	}
	left, err := UnmarshalValueExpr(r.Left)
	if err != nil {
		return err
	}
	right, err := UnmarshalValueExpr(r.Right)
	if err != nil {
		return err
	}
	v.Left, v.Right = left, right
	return nil
}

// Mirrors core/src/wire.rs::ValueExpr::Coalesce
type ValueCoalesce struct {
	Parts []ValueExpr `json:"parts"`
}

func (ValueCoalesce) isValueExpr() {}

func (v ValueCoalesce) MarshalJSON() ([]byte, error) {
	type alias ValueCoalesce
	return MarshalTagged("op", "coalesce", alias(v))
}

func (v *ValueCoalesce) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Parts []json.RawMessage `json:"parts"`
	}](b)
	if err != nil {
		return err
	}
	parts, err := decodeValueExprs(r.Parts)
	if err != nil {
		return err
	}
	v.Parts = parts
	return nil
}

// Mirrors core/src/wire.rs::ValueExpr::Lower
type ValueLower struct {
	Value ValueExpr `json:"value"`
}

func (ValueLower) isValueExpr() {}

func (v ValueLower) MarshalJSON() ([]byte, error) {
	type alias ValueLower
	return MarshalTagged("op", "lower", alias(v))
}

func (v *ValueLower) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Value json.RawMessage `json:"value"`
	}](b)
	if err != nil {
		return err
	}
	e, err := UnmarshalValueExpr(r.Value)
	if err != nil {
		return err
	}
	v.Value = e
	return nil
}

// Mirrors core/src/wire.rs::ValueExpr::Upper
type ValueUpper struct {
	Value ValueExpr `json:"value"`
}

func (ValueUpper) isValueExpr() {}

func (v ValueUpper) MarshalJSON() ([]byte, error) {
	type alias ValueUpper
	return MarshalTagged("op", "upper", alias(v))
}

func (v *ValueUpper) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Value json.RawMessage `json:"value"`
	}](b)
	if err != nil {
		return err
	}
	e, err := UnmarshalValueExpr(r.Value)
	if err != nil {
		return err
	}
	v.Value = e
	return nil
}

// Mirrors core/src/wire.rs::ValueExpr::Trim
type ValueTrim struct {
	Value ValueExpr `json:"value"`
}

func (ValueTrim) isValueExpr() {}

func (v ValueTrim) MarshalJSON() ([]byte, error) {
	type alias ValueTrim
	return MarshalTagged("op", "trim", alias(v))
}

func (v *ValueTrim) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Value json.RawMessage `json:"value"`
	}](b)
	if err != nil {
		return err
	}
	e, err := UnmarshalValueExpr(r.Value)
	if err != nil {
		return err
	}
	v.Value = e
	return nil
}

// Mirrors core/src/wire.rs::ValueExpr::Cast
type ValueCast struct {
	Value ValueExpr `json:"value"`
	To    Cast      `json:"to"`
}

func (ValueCast) isValueExpr() {}

func (v ValueCast) MarshalJSON() ([]byte, error) {
	type alias ValueCast
	return MarshalTagged("op", "cast", alias(v))
}

func (v *ValueCast) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Value json.RawMessage `json:"value"`
		To    Cast            `json:"to"`
	}](b)
	if err != nil {
		return err
	}
	e, err := UnmarshalValueExpr(r.Value)
	if err != nil {
		return err
	}
	v.Value, v.To = e, r.To
	return nil
}

// Mirrors core/src/wire.rs::ValueExpr::Now — unit variant, wire form
// {"op":"now"}.
type ValueNow struct{}

func (ValueNow) isValueExpr() {}

func (v ValueNow) MarshalJSON() ([]byte, error) { return MarshalTagged("op", "now", struct{}{}) }

func (v *ValueNow) UnmarshalJSON(b []byte) error {
	_, err := StrictUnmarshal[struct{}](b)
	return err
}

// Mirrors core/src/wire.rs::CaseWhen — one branch of Case. Wire shape
// {when, then}.
type CaseWhen struct {
	When FilterExpr `json:"when"`
	Then ValueExpr  `json:"then"`
}

func (c *CaseWhen) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		When json.RawMessage `json:"when"`
		Then json.RawMessage `json:"then"`
	}](b)
	if err != nil {
		return err
	}
	w, err := UnmarshalFilterExpr(r.When)
	if err != nil {
		return err
	}
	t, err := UnmarshalValueExpr(r.Then)
	if err != nil {
		return err
	}
	c.When, c.Then = w, t
	return nil
}

// Mirrors core/src/wire.rs::ValueExpr::Case
type ValueCase struct {
	Whens     []CaseWhen `json:"whens"`
	Otherwise ValueExpr  `json:"otherwise"`
}

func (ValueCase) isValueExpr() {}

func (v ValueCase) MarshalJSON() ([]byte, error) {
	type alias ValueCase
	return MarshalTagged("op", "case", alias(v))
}

func (v *ValueCase) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Whens     []json.RawMessage `json:"whens"`
		Otherwise json.RawMessage   `json:"otherwise"`
	}](b)
	if err != nil {
		return err
	}
	whens := make([]CaseWhen, len(r.Whens))
	for i := range r.Whens {
		if err := json.Unmarshal(r.Whens[i], &whens[i]); err != nil {
			return err
		}
	}
	otherwise, err := UnmarshalValueExpr(r.Otherwise)
	if err != nil {
		return err
	}
	v.Whens, v.Otherwise = whens, otherwise
	return nil
}

// UnmarshalValueExpr routes a tagged expr object to its variant. Unknown
// op is an error, mirroring serde's closed enum.
func UnmarshalValueExpr(data []byte) (ValueExpr, error) {
	tag, err := PeekTag(data, "op")
	if err != nil {
		return nil, err
	}
	switch tag {
	case "field":
		return DecodeTagged[ValueField](data, "op")
	case "literal":
		return DecodeTagged[ValueLiteral](data, "op")
	case "concat":
		return DecodeTagged[ValueConcat](data, "op")
	case "add":
		return DecodeTagged[ValueAdd](data, "op")
	case "sub":
		return DecodeTagged[ValueSub](data, "op")
	case "mul":
		return DecodeTagged[ValueMul](data, "op")
	case "div":
		return DecodeTagged[ValueDiv](data, "op")
	case "coalesce":
		return DecodeTagged[ValueCoalesce](data, "op")
	case "lower":
		return DecodeTagged[ValueLower](data, "op")
	case "upper":
		return DecodeTagged[ValueUpper](data, "op")
	case "trim":
		return DecodeTagged[ValueTrim](data, "op")
	case "cast":
		return DecodeTagged[ValueCast](data, "op")
	case "now":
		return DecodeTagged[ValueNow](data, "op")
	case "case":
		return DecodeTagged[ValueCase](data, "op")
	default:
		return nil, fmt.Errorf("wire: unknown value op %q", tag)
	}
}
