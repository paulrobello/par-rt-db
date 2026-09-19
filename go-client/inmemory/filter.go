// FilterExpr structural validation and per-row evaluation — the port of
// swift InMemoryValidate.swift's filter half / rust in_memory/validate.rs
// (server query::compile_filter + jsonb_lhs_and_bind, including the SEC-126
// kind checks and the SEC-007 depth/length caps).
package inmemory

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// maxFilterDepth is the SEC-007 ceiling on and/or/not nesting (server
// filter::MAX_FILTER_DEPTH).
const maxFilterDepth = 32

// maxInValues is the SEC-007 ceiling on an in list (server filter::MAX_IN_VALUES).
const maxInValues = 1000

// validateFilterExpr is the read-context structural validation of a FilterExpr
// against a table's declared fields (allowRelativeTime = false rejects the
// execution-time-relative olderThan leaf; only by-query step filters admit
// it). Call once before evaluating per row.
func validateFilterExpr(expr wire.FilterExpr, table *TableDef) error {
	return validateFilterExprAt(expr, table, false, 1)
}

// validateByQueryFilter is the patchByQuery/deleteByQuery chokepoint — the
// one filter context that accepts olderThan.
func validateByQueryFilter(expr wire.FilterExpr, table *TableDef) error {
	return validateFilterExprAt(expr, table, true, 1)
}

func validateFilterExprAt(expr wire.FilterExpr, table *TableDef, allowRelativeTime bool, depth int) error {
	if depth > maxFilterDepth {
		return rtdberrors.New(rtdberrors.CodeBadRequest,
			"filter nesting exceeds "+strconv.Itoa(maxFilterDepth)+" levels")
	}
	switch e := expr.(type) {
	case wire.FilterAnd:
		if len(e.Exprs) == 0 {
			return rtdberrors.New(rtdberrors.CodeBadRequest, "and filter requires at least one expr")
		}
		for _, sub := range e.Exprs {
			if err := validateFilterExprAt(sub, table, allowRelativeTime, depth+1); err != nil {
				return err
			}
		}
	case wire.FilterOr:
		if len(e.Exprs) == 0 {
			return rtdberrors.New(rtdberrors.CodeBadRequest, "or filter requires at least one expr")
		}
		for _, sub := range e.Exprs {
			if err := validateFilterExprAt(sub, table, allowRelativeTime, depth+1); err != nil {
				return err
			}
		}
	case wire.FilterNot:
		return validateFilterExprAt(e.Expr, table, allowRelativeTime, depth+1)
	case wire.FilterIn:
		if len(e.Values) == 0 {
			return rtdberrors.New(rtdberrors.CodeBadRequest, "in filter requires at least one value")
		}
		if len(e.Values) > maxInValues {
			return rtdberrors.New(rtdberrors.CodeBadRequest,
				"in: at most "+strconv.Itoa(maxInValues)+" values")
		}
		for _, v := range e.Values {
			if err := checkLeafValue(e.Field, v, table); err != nil {
				return err
			}
		}
		first := inValueKind(e.Values[0])
		for _, v := range e.Values[1:] {
			if inValueKind(v) != first {
				return rtdberrors.New(rtdberrors.CodeBadRequest, "in filter values must all be the same type")
			}
		}
		for _, v := range e.Values {
			if err := checkLeafKind(e.Field, v, table); err != nil {
				return err
			}
		}
	case wire.FilterEq, wire.FilterNeq, wire.FilterGt, wire.FilterGte, wire.FilterLt, wire.FilterLte:
		field, value := leafField(expr)
		return checkLeaf(field, value, table)
	case wire.FilterContains:
		return checkLeaf(e.Field, e.Value, table)
	case wire.FilterOlderThan:
		if !allowRelativeTime {
			return rtdberrors.New(rtdberrors.CodeBadRequest,
				"olderThan filter is only allowed in patchByQuery/deleteByQuery filters")
		}
		if e.Ms < 0 {
			return rtdberrors.New(rtdberrors.CodeBadRequest, "olderThan ms must be >= 0")
		}
		fty, err := leafFieldType(e.Field, table)
		if err != nil {
			return err
		}
		inner := fty
		if inner.Kind == "optional" {
			inner = inner.Inner
		}
		if inner.Kind != "number" && inner.Kind != "int64" {
			return rtdberrors.New(rtdberrors.CodeBadRequest,
				"field '"+e.Field+"' must be a number or int64 field for olderThan")
		}
	case wire.FilterExists:
		if _, err := leafFieldType(e.Field, table); err != nil {
			return err
		}
	default:
		return rtdberrors.New(rtdberrors.CodeInternal, "unknown filter kind")
	}
	return nil
}

// leafField extracts (field, value) from the six comparison leaves.
func leafField(expr wire.FilterExpr) (string, wire.JSONValue) {
	switch e := expr.(type) {
	case wire.FilterEq:
		return e.Field, e.Value
	case wire.FilterNeq:
		return e.Field, e.Value
	case wire.FilterGt:
		return e.Field, e.Value
	case wire.FilterGte:
		return e.Field, e.Value
	case wire.FilterLt:
		return e.Field, e.Value
	case wire.FilterLte:
		return e.Field, e.Value
	}
	return "", nil
}

func leafFieldType(field string, table *TableDef) (*FieldType, error) {
	ft, ok := table.Fields[field]
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"filter references unknown field '"+field+"'")
	}
	return &ft, nil
}

func checkLeafValue(field string, value wire.JSONValue, table *TableDef) error {
	if _, err := leafFieldType(field, table); err != nil {
		return err
	}
	switch value.(type) {
	case wire.String, wire.Number, wire.Bool:
	default:
		return rtdberrors.New(rtdberrors.CodeBadRequest,
			"filter value must be a string, number, or boolean")
	}
	return nil
}

func checkLeaf(field string, value wire.JSONValue, table *TableDef) error {
	if err := checkLeafValue(field, value, table); err != nil {
		return err
	}
	return checkLeafKind(field, value, table)
}

// checkLeafKind is the SEC-126 type check: an indexed field types the value
// through the same eq-bind conversion as index eq prefixes; any other
// declared field requires only the value's JSON kind to match the declared
// type.
func checkLeafKind(field string, value wire.JSONValue, table *TableDef) error {
	fty, err := leafFieldType(field, table)
	if err != nil {
		return err
	}
	for _, idx := range table.Indexes {
		for _, f := range idx.Fields {
			if f == field {
				_, err := coerceIndexValue(table, field, value)
				return err
			}
		}
	}
	return validateJSONBComparisonValue(field, fty, value)
}

func validateJSONBComparisonValue(field string, ty *FieldType, value wire.JSONValue) error {
	inner := ty
	if inner.Kind == "optional" {
		inner = inner.Inner
	}
	var ok bool
	switch inner.Kind {
	case "string", "id", "bytes":
		_, ok = value.(wire.String)
	case "number", "int64":
		ok = isJSONNumber(value)
	case "boolean":
		_, ok = value.(wire.Bool)
	default:
		switch value.(type) {
		case wire.String, wire.Number, wire.Bool:
			ok = true
		}
	}
	if !ok {
		return rtdberrors.New(rtdberrors.CodeBadRequest,
			"filter on field '"+field+"' value kind does not match declared field type")
	}
	return nil
}

func inValueKind(v wire.JSONValue) string {
	switch t := v.(type) {
	case wire.String:
		return "string"
	case wire.Number:
		return "number"
	case wire.Bool:
		_ = t
		return "boolean"
	default:
		return "other"
	}
}

// PgType is the indexed-column storage type (server indexed_column_type).
type PgType string

const (
	PgText    PgType = "text"
	PgNumber  PgType = "number"
	PgBoolean PgType = "boolean"
	PgInt64   PgType = "int64"
)

// IndexedType is the storage kind plus nullability of an indexable column.
type IndexedType struct {
	Pg       PgType
	Nullable bool
}

func fieldTypeTag(ty FieldType) string { return ty.Kind }

// indexColumnType resolves a field type's indexable storage column;
// SCHEMA_VIOLATION for a non-indexable type.
func indexColumnType(ty FieldType) (IndexedType, error) {
	switch ty.Kind {
	case "string", "id":
		return IndexedType{Pg: PgText}, nil
	case "number":
		return IndexedType{Pg: PgNumber}, nil
	case "int64":
		return IndexedType{Pg: PgInt64}, nil
	case "boolean":
		return IndexedType{Pg: PgBoolean}, nil
	case "literal":
		if _, ok := ty.Value.(wire.String); ok {
			return IndexedType{Pg: PgText}, nil
		}
	case "union":
		all := len(ty.Variants) > 0
		for _, v := range ty.Variants {
			if v.Kind != "literal" {
				all = false
				break
			}
			if _, ok := v.Value.(wire.String); !ok {
				all = false
				break
			}
		}
		if all {
			return IndexedType{Pg: PgText}, nil
		}
	case "optional":
		resolved, err := indexColumnType(*ty.Inner)
		if err != nil {
			return IndexedType{}, err
		}
		resolved.Nullable = true
		return resolved, nil
	}
	return IndexedType{}, rtdberrors.New(rtdberrors.CodeSchemaViolation,
		"field type '"+ty.Kind+"' is not indexable")
}

// coerceIndexValue type-checks an eq/range bind value against the field's
// indexed storage type (server eq_bind_for). The value passes through
// unchanged.
func coerceIndexValue(table *TableDef, fieldName string, value wire.JSONValue) (wire.JSONValue, error) {
	fty, ok := table.Fields[fieldName]
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeInternal,
			"index references unknown field '"+fieldName+"'")
	}
	pg, err := indexColumnType(fty)
	if err != nil {
		return nil, err
	}
	switch pg.Pg {
	case PgText:
		if _, ok := value.(wire.String); !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "eq value must be a string")
		}
	case PgNumber:
		if !isJSONNumber(value) {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "eq value must be a number")
		}
	case PgInt64:
		if !isInt64String(value) {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "eq value must be an int64 string")
		}
	case PgBoolean:
		if _, ok := value.(wire.Bool); !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "eq value must be a boolean")
		}
	}
	return value, nil
}

// matchesFilter evaluates a validated FilterExpr against a merged doc. A
// null/absent field never matches (SQL NULL exclusion); olderThan fail-closes
// to false in this read-context entry point.
func matchesFilter(expr wire.FilterExpr, doc wire.Object, fields map[string]FieldType) bool {
	return evalFilterWithClock(expr, doc, fields, 0, false)
}

// matchesFilterAt is the by-query evaluator: identical to matchesFilter
// except that an olderThan leaf compares the doc value against now − ms
// (strict less-than), with now the engine clock read once per scan.
func matchesFilterAt(expr wire.FilterExpr, doc wire.Object, fields map[string]FieldType, now int64) bool {
	return evalFilterWithClock(expr, doc, fields, now, true)
}

func evalFilterWithClock(expr wire.FilterExpr, doc wire.Object, fields map[string]FieldType, now int64, hasNow bool) bool {
	switch e := expr.(type) {
	case wire.FilterAnd:
		for _, sub := range e.Exprs {
			if !evalFilterWithClock(sub, doc, fields, now, hasNow) {
				return false
			}
		}
		return true
	case wire.FilterOr:
		for _, sub := range e.Exprs {
			if evalFilterWithClock(sub, doc, fields, now, hasNow) {
				return true
			}
		}
		return false
	case wire.FilterNot:
		return !evalFilterWithClock(e.Expr, doc, fields, now, hasNow)
	case wire.FilterIn:
		for _, v := range e.Values {
			if compareLeaf(opEq, e.Field, v, doc, fields) {
				return true
			}
		}
		return false
	case wire.FilterEq:
		return compareLeaf(opEq, e.Field, e.Value, doc, fields)
	case wire.FilterNeq:
		return compareLeaf(opNeq, e.Field, e.Value, doc, fields)
	case wire.FilterGt:
		return compareLeaf(opGt, e.Field, e.Value, doc, fields)
	case wire.FilterGte:
		return compareLeaf(opGte, e.Field, e.Value, doc, fields)
	case wire.FilterLt:
		return compareLeaf(opLt, e.Field, e.Value, doc, fields)
	case wire.FilterLte:
		return compareLeaf(opLte, e.Field, e.Value, doc, fields)
	case wire.FilterContains:
		arr, ok := doc[e.Field].(wire.Array)
		if !ok {
			return false
		}
		for _, item := range arr {
			if jsonEq(item, e.Value) {
				return true
			}
		}
		return false
	case wire.FilterExists:
		v, ok := doc[e.Field]
		if !ok {
			return false
		}
		_, isNull := v.(wire.Null)
		return !isNull
	case wire.FilterOlderThan:
		if !hasNow {
			return false
		}
		cutoff := now - e.Ms
		if cutoff > now {
			cutoff = -9223372036854775808
		}
		dv, ok := doc[e.Field]
		if !ok {
			return false
		}
		if _, isNull := dv.(wire.Null); isNull {
			return false
		}
		if isInt64Field2(fields[e.Field]) {
			switch t := dv.(type) {
			case wire.String:
				if lhs, ok := parseI64(string(t)); ok {
					return lhs < cutoff
				}
				return false
			case wire.Number:
				if lhs, ok := parseI64(string(t)); ok {
					return lhs < cutoff
				}
				return false
			default:
				return false
			}
		}
		lhs, ok := docToNumber(dv)
		if !ok {
			return false
		}
		return lhs < float64(cutoff)
	default:
		return false
	}
}

type filterOp int

const (
	opEq filterOp = iota
	opNeq
	opGt
	opGte
	opLt
	opLte
)

// compareLeaf is the per-leaf comparison: the filter value's kind picks the
// comparison domain, except that a string value against a declared int64
// field compares numerically (ENH-027 — decimal strings order numerically,
// not lexicographically).
func compareLeaf(op filterOp, field string, filterValue wire.JSONValue, doc wire.Object, fields map[string]FieldType) bool {
	dv, ok := doc[field]
	if !ok {
		return false
	}
	if _, isNull := dv.(wire.Null); isNull {
		return false
	}
	if s, isStr := filterValue.(wire.String); isStr && isInt64Field2(fields[field]) {
		docStr, ok := dv.(wire.String)
		if !ok {
			return false
		}
		lhs, ok := parseI64(string(docStr))
		if !ok {
			return false
		}
		rhs, ok := parseI64(string(s))
		if !ok {
			return false
		}
		return compareI64(op, lhs, rhs)
	}
	switch fv := filterValue.(type) {
	case wire.String:
		return compareStrings(op, docToText(dv), string(fv))
	case wire.Number:
		lhs, ok := docToNumber(dv)
		if !ok {
			return false
		}
		return compareF64(op, lhs, jsonNumberF64(fv))
	case wire.Bool:
		db, ok := dv.(wire.Bool)
		if !ok {
			return false
		}
		return compareBool(op, bool(db), bool(fv))
	default:
		return false
	}
}

// isInt64Field2 reports whether a declared field type (looked up in the
// fields map; absent = false) is int64 — an optional<int64> unwraps to it.
func isInt64Field2(ty FieldType) bool {
	if ty.Kind == "int64" {
		return true
	}
	if ty.Kind == "optional" && ty.Inner != nil && ty.Inner.Kind == "int64" {
		return true
	}
	return false
}

// docToText mirrors Postgres doc->>'field': the JSON text of the value
// (integer-valued numbers render without a decimal point).
func docToText(v wire.JSONValue) string {
	switch t := v.(type) {
	case wire.String:
		return string(t)
	case wire.Number:
		return jsonNumberText(t)
	case wire.Bool:
		if t {
			return "true"
		}
		return "false"
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "null"
		}
		return string(b)
	}
}

// docToNumber mirrors Postgres (doc->>'field')::float8: a finite number, or a
// parsed numeric string.
func docToNumber(v wire.JSONValue) (float64, bool) {
	switch t := v.(type) {
	case wire.Number:
		f := jsonNumberF64(t)
		if math.IsInf(f, 0) || math.IsNaN(f) {
			return 0, false
		}
		return f, true
	case wire.String:
		trimmed := strings.TrimSpace(string(t))
		if trimmed == "" {
			return 0, false
		}
		f, err := strconv.ParseFloat(trimmed, 64)
		if err != nil || math.IsInf(f, 0) || math.IsNaN(f) {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

func compareStrings(op filterOp, l, r string) bool {
	switch op {
	case opEq:
		return l == r
	case opNeq:
		return l != r
	case opGt:
		return l > r
	case opGte:
		return l >= r
	case opLt:
		return l < r
	default:
		return l <= r
	}
}

func compareF64(op filterOp, l, r float64) bool {
	switch op {
	case opEq:
		return l == r
	case opNeq:
		return l != r
	case opGt:
		return l > r
	case opGte:
		return l >= r
	case opLt:
		return l < r
	default:
		return l <= r
	}
}

func compareI64(op filterOp, l, r int64) bool {
	switch op {
	case opEq:
		return l == r
	case opNeq:
		return l != r
	case opGt:
		return l > r
	case opGte:
		return l >= r
	case opLt:
		return l < r
	default:
		return l <= r
	}
}

func compareBool(op filterOp, l, r bool) bool {
	switch op {
	case opEq:
		return l == r
	case opNeq:
		return l != r
	case opGt:
		return l == true && r == false
	case opGte:
		return !(l == false && r == true)
	case opLt:
		return l == false && r == true
	default:
		return !(l == true && r == false)
	}
}

// walkFilterExprFieldNames visits every field name a FilterExpr references.
func walkFilterExprFieldNames(expr wire.FilterExpr, visit func(string)) {
	switch e := expr.(type) {
	case wire.FilterAnd:
		for _, sub := range e.Exprs {
			walkFilterExprFieldNames(sub, visit)
		}
	case wire.FilterOr:
		for _, sub := range e.Exprs {
			walkFilterExprFieldNames(sub, visit)
		}
	case wire.FilterNot:
		walkFilterExprFieldNames(e.Expr, visit)
	case wire.FilterExists:
		visit(e.Field)
	case wire.FilterOlderThan:
		visit(e.Field)
	case wire.FilterIn:
		visit(e.Field)
	case wire.FilterContains:
		visit(e.Field)
	default:
		field, _ := leafField(expr)
		if field != "" {
			visit(field)
		}
	}
}
