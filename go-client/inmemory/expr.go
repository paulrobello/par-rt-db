// ValueExpr evaluation, JSON equality, and the small scalar helpers shared by
// the schema model, validators, and query evaluator. Ports swift
// InMemoryValueExpr.swift / rust in_memory/value_expr.rs (server
// value_expr.rs::eval_value_expr).
package inmemory

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
	"strings"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// jsonEq is JS `===` on JSON values: numbers compare by value across spellings
// (5 == 5.0), everything else by kind + payload.
func jsonEq(lhs, rhs wire.JSONValue) bool {
	switch l := lhs.(type) {
	case nil:
		return rhs == nil
	case wire.Null:
		_, ok := rhs.(wire.Null)
		return ok
	case wire.Bool:
		r, ok := rhs.(wire.Bool)
		return ok && l == r
	case wire.String:
		r, ok := rhs.(wire.String)
		return ok && l == r
	case wire.Number:
		r, ok := rhs.(wire.Number)
		if !ok {
			return false
		}
		if string(l) == string(r) {
			return true
		}
		return jsonNumberF64(l) == jsonNumberF64(r)
	case wire.Array:
		r, ok := rhs.(wire.Array)
		if !ok || len(l) != len(r) {
			return false
		}
		for i := range l {
			if !jsonEq(l[i], r[i]) {
				return false
			}
		}
		return true
	case wire.Object:
		r, ok := rhs.(wire.Object)
		if !ok || len(l) != len(r) {
			return false
		}
		for k, lv := range l {
			rv, ok := r[k]
			if !ok || !jsonEq(lv, rv) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// strictValueEq is serde_json Value equality (rust PartialEq): numbers are
// equal only within their own backing class — integer-spelled with integer
// value equal, float-spelled with float value equal; 1 and 1.0 are UNEQUAL.
// Used where rust compares Values strictly (autoIncrement immutability,
// FieldType::Literal equality); JS-style jsonEq stays for the surfaces the
// other references treat JS-equal (literal validation, widening, contains).
func strictValueEq(lhs, rhs wire.JSONValue) bool {
	ln, lok := lhs.(wire.Number)
	rn, rok := rhs.(wire.Number)
	if lok && rok {
		li, lokInt := parseI64(string(ln))
		ri, rokInt := parseI64(string(rn))
		if lokInt != rokInt {
			return false
		}
		if lokInt {
			return li == ri
		}
		return jsonNumberF64(ln) == jsonNumberF64(rn)
	}
	return jsonEq(lhs, rhs)
}

// isJSONNumber reports whether v is a JSON number kind.
func isJSONNumber(v wire.JSONValue) bool {
	_, ok := v.(wire.Number)
	return ok
}

// parseI64 is the exact i64::from_str mirror: optional sign then ASCII
// digits, within the i64 range (including i64::MIN).
func parseI64(text string) (int64, bool) {
	neg := false
	digits := text
	if strings.HasPrefix(digits, "-") {
		neg = true
		digits = digits[1:]
	} else if strings.HasPrefix(digits, "+") {
		digits = digits[1:]
	}
	if digits == "" {
		return 0, false
	}
	var limit uint64 = 9223372036854775807
	if neg {
		limit = 9223372036854775808
	}
	var acc uint64
	for i := 0; i < len(digits); i++ {
		c := digits[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		d := uint64(c - '0')
		if acc > (limit-d)/10 {
			return 0, false
		}
		acc = acc*10 + d
	}
	if neg {
		return -int64(acc), true
	}
	return int64(acc), true
}

// jsNumberString renders a finite double the way JS String(number) does:
// integral values print without a fraction.
func jsNumberString(f float64) string {
	if f == math.Trunc(f) && math.Abs(f) < 1e15 {
		return strconv.FormatInt(int64(f), 10)
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

// compactJSON renders a value as compact JSON WITHOUT HTML escaping —
// encoding/json escapes <, >, & by default, which rust's serde to_string does
// not, so text extraction must not either (M6).
func compactJSON(v wire.JSONValue) (string, bool) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "", false
	}
	return strings.TrimSuffix(buf.String(), "\n"), true
}

// valueText is the SQL `doc->>'field'` text extraction: nil means SQL NULL
// (JSON null). Objects/arrays render as COMPACT JSON with keys sorted
// (encoding/json marshals maps in sorted key order — the convention the
// semantics corpus pins).
func valueText(v wire.JSONValue) (string, bool) {
	switch t := v.(type) {
	case wire.Null, nil:
		return "", false
	case wire.String:
		return string(t), true
	case wire.Number:
		return jsonNumberText(t), true
	case wire.Bool:
		if t {
			return "true", true
		}
		return "false", true
	default:
		text, ok := compactJSON(v)
		return text, ok
	}
}

// jsonNumberText renders a wire.Number in its canonical text form
// (integer-valued floats without the decimal point).
func jsonNumberText(n wire.Number) string {
	if f, err := strconv.ParseFloat(string(n), 64); err == nil {
		return jsNumberString(f)
	}
	return string(n)
}

// valueNumeric is the `(doc->>'field')::float8` cast for arithmetic nodes:
// nil means SQL NULL propagation; strings are trimmed and strictly parsed;
// bool/object/array are type errors.
func valueNumeric(v wire.JSONValue) (float64, error) {
	switch t := v.(type) {
	case wire.Null, nil:
		return 0, errNullProp
	case wire.Number:
		f := jsonNumberF64(t)
		if math.IsInf(f, 0) || math.IsNaN(f) {
			return 0, errNullProp
		}
		return f, nil
	case wire.String:
		trimmed := strings.Trim(string(t), " \t\n\r")
		f, err := strconv.ParseFloat(trimmed, 64)
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return 0, rtdberrors.New(rtdberrors.CodeBadRequest, "cannot cast '"+string(t)+"' to number")
		}
		return f, nil
	default:
		return 0, rtdberrors.New(rtdberrors.CodeBadRequest, "cannot cast to number")
	}
}

// errNullProp marks SQL-NULL propagation inside arithmetic (not an error to
// report; evalValueExpr turns it into a null result).
var errNullProp = rtdberrors.New(rtdberrors.CodeInternal, "null propagation")

func isNullProp(err error) bool {
	return err == errNullProp
}

// finiteJSONNumber converts an arithmetic result back to a JSON number; a
// non-finite result (overflow-shaped) is a BAD_REQUEST, mirroring
// finite_number.
func finiteJSONNumber(f float64) (wire.JSONValue, error) {
	if math.IsInf(f, 0) || math.IsNaN(f) {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "numeric result is not finite")
	}
	return jsonNumberFromF64(f), nil
}

// castToInt64 mirrors Cast.toInt64: a float-spelled number is NOT integral
// even when mathematically whole (serde_json's as_i64 only succeeds on
// integer-backed numbers); a string is trimmed and strictly parsed. The
// result is a JSON number — the int64 decimal-STRING convention applies only
// to stored int64 fields.
func castToInt64(v wire.JSONValue) (wire.JSONValue, error) {
	switch t := v.(type) {
	case wire.Null, nil:
		return wire.Null{}, nil
	case wire.Number:
		if i, ok := parseI64(string(t)); ok {
			return wire.Number(strconv.FormatInt(i, 10)), nil
		}
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"cannot cast "+jsonNumberText(t)+" to int64")
	case wire.String:
		trimmed := strings.Trim(string(t), " \t\n\r")
		if i, ok := parseI64(trimmed); ok {
			return wire.Number(strconv.FormatInt(i, 10)), nil
		}
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "cannot cast '"+string(t)+"' to int64")
	default:
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "cannot cast to int64")
	}
}

// castToBoolean mirrors Cast.toBoolean: bools pass through; numbers accept
// exactly 1/0; strings match case-insensitively against the Postgres boolean
// literal set.
func castToBoolean(v wire.JSONValue) (wire.JSONValue, error) {
	trueWords := map[string]bool{"true": true, "t": true, "yes": true, "on": true, "1": true}
	falseWords := map[string]bool{"false": true, "f": true, "no": true, "off": true, "0": true}
	lower := func(s wire.String) string { return strings.ToLower(string(s)) }
	switch t := v.(type) {
	case wire.Null, nil:
		return wire.Null{}, nil
	case wire.Bool:
		return t, nil
	case wire.Number:
		f := jsonNumberF64(t)
		if f == 1 {
			return wire.Bool(true), nil
		}
		if f == 0 {
			return wire.Bool(false), nil
		}
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"cannot cast "+jsonNumberText(t)+" to boolean")
	case wire.String:
		if trueWords[lower(t)] {
			return wire.Bool(true), nil
		}
		if falseWords[lower(t)] {
			return wire.Bool(false), nil
		}
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "cannot cast '"+string(t)+"' to boolean")
	default:
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "cannot cast to boolean")
	}
}

// EvalValueExpr evaluates a ValueExpr against a doc — the per-write
// counterpart of the server's SQL compiler, used by computed-field stamping.
// Field reads are TEXT extraction (doc->>'field'), arithmetic is IEEE doubles
// with SQL-NULL propagation before the div-zero check, and case predicates
// reuse the engine filter matcher.
func EvalValueExpr(ve wire.ValueExpr, doc wire.Object, nowMs int64, fields map[string]FieldType) (wire.JSONValue, error) {
	switch e := ve.(type) {
	case wire.ValueField:
		if v, ok := doc[e.Field]; ok {
			if text, ok := valueText(v); ok {
				return wire.String(text), nil
			}
		}
		return wire.Null{}, nil
	case wire.ValueLiteral:
		return e.Value, nil
	case wire.ValueConcat:
		out := ""
		for _, part := range e.Parts {
			pv, err := EvalValueExpr(part, doc, nowMs, fields)
			if err != nil {
				return nil, err
			}
			if text, ok := valueText(pv); ok {
				out += text
			}
		}
		return wire.String(out), nil
	case wire.ValueAdd, wire.ValueSub, wire.ValueMul, wire.ValueDiv:
		var lhsV, rhsV wire.ValueExpr
		var op string
		switch a := e.(type) {
		case wire.ValueAdd:
			lhsV, rhsV, op = a.Left, a.Right, "add"
		case wire.ValueSub:
			lhsV, rhsV, op = a.Left, a.Right, "sub"
		case wire.ValueMul:
			lhsV, rhsV, op = a.Left, a.Right, "mul"
		case wire.ValueDiv:
			lhsV, rhsV, op = a.Left, a.Right, "div"
		}
		lhs, err := EvalValueExpr(lhsV, doc, nowMs, fields)
		if err != nil {
			return nil, err
		}
		rhs, err := EvalValueExpr(rhsV, doc, nowMs, fields)
		if err != nil {
			return nil, err
		}
		l, lerr := valueNumeric(lhs)
		r, rerr := valueNumeric(rhs)
		if isNullProp(lerr) || isNullProp(rerr) {
			return wire.Null{}, nil
		}
		if lerr != nil {
			return nil, lerr
		}
		if rerr != nil {
			return nil, rerr
		}
		if op == "div" && r == 0 {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "division by zero")
		}
		var result float64
		switch op {
		case "add":
			result = l + r
		case "sub":
			result = l - r
		case "mul":
			result = l * r
		default:
			result = l / r
		}
		return finiteJSONNumber(result)
	case wire.ValueCoalesce:
		for _, part := range e.Parts {
			pv, err := EvalValueExpr(part, doc, nowMs, fields)
			if err != nil {
				return nil, err
			}
			if _, isNull := pv.(wire.Null); !isNull {
				return pv, nil
			}
		}
		return wire.Null{}, nil
	case wire.ValueLower:
		iv, err := EvalValueExpr(e.Value, doc, nowMs, fields)
		if err != nil {
			return nil, err
		}
		if text, ok := valueText(iv); ok {
			return wire.String(strings.ToLower(text)), nil
		}
		return wire.Null{}, nil
	case wire.ValueUpper:
		iv, err := EvalValueExpr(e.Value, doc, nowMs, fields)
		if err != nil {
			return nil, err
		}
		if text, ok := valueText(iv); ok {
			return wire.String(strings.ToUpper(text)), nil
		}
		return wire.Null{}, nil
	case wire.ValueTrim:
		iv, err := EvalValueExpr(e.Value, doc, nowMs, fields)
		if err != nil {
			return nil, err
		}
		if text, ok := valueText(iv); ok {
			return wire.String(strings.Trim(text, " ")), nil
		}
		return wire.Null{}, nil
	case wire.ValueCast:
		iv, err := EvalValueExpr(e.Value, doc, nowMs, fields)
		if err != nil {
			return nil, err
		}
		switch e.To {
		case wire.CastToString:
			if text, ok := valueText(iv); ok {
				return wire.String(text), nil
			}
			return wire.Null{}, nil
		case wire.CastToNumber:
			if n, nerr := valueNumeric(iv); nerr == nil {
				return finiteJSONNumber(n)
			} else if !isNullProp(nerr) {
				return nil, nerr
			}
			return wire.Null{}, nil
		case wire.CastToInt64:
			return castToInt64(iv)
		case wire.CastToBoolean:
			return castToBoolean(iv)
		}
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "unknown cast target")
	case wire.ValueNow:
		return wire.Number(strconv.FormatInt(nowMs, 10)), nil
	case wire.ValueCase:
		for _, cw := range e.Whens {
			if matchesFilter(cw.When, doc, fields) {
				return EvalValueExpr(cw.Then, doc, nowMs, fields)
			}
		}
		return EvalValueExpr(e.Otherwise, doc, nowMs, fields)
	default:
		return nil, rtdberrors.New(rtdberrors.CodeInternal, "unknown value expression kind")
	}
}
