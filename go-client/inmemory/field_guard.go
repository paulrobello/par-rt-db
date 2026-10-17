package inmemory

import (
	"encoding/json"
	"math"
	"math/big"
	"strconv"
	"unicode/utf8"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func fieldPathDeclared(fields map[string]FieldType, path []string) bool {
	field, ok := fields[path[0]]
	return ok && fieldChildDeclared(field, path[1:])
}

func fieldChildDeclared(field FieldType, path []string) bool {
	if len(path) == 0 {
		return true
	}
	switch field.Kind {
	case "any":
		return true
	case "optional":
		return field.Inner != nil && fieldChildDeclared(*field.Inner, path)
	case "object":
		return fieldPathDeclared(field.Fields, path)
	case "record":
		return field.Inner != nil && fieldChildDeclared(*field.Inner, path[1:])
	case "union":
		for _, child := range field.Variants {
			if fieldChildDeclared(child, path) {
				return true
			}
		}
	}
	return false
}

func fieldNumber(value wire.Number) (*big.Int, float64) {
	text := string(value)
	if integer, err := strconv.ParseInt(text, 10, 64); err == nil {
		return big.NewInt(integer), float64(integer)
	}
	if integer, err := strconv.ParseUint(text, 10, 64); err == nil {
		return new(big.Int).SetUint64(integer), float64(integer)
	}
	number, _ := strconv.ParseFloat(text, 64)
	if !math.IsInf(number, 0) && !math.IsNaN(number) && math.Trunc(number) == number && math.Abs(number) < math.Exp2(127) {
		integer, _ := new(big.Float).SetFloat64(number).Int(nil)
		return integer, number
	}
	return nil, number
}

func fieldJSONEqual(left, right wire.JSONValue) bool {
	if left == nil {
		left = wire.Null{}
	}
	if right == nil {
		right = wire.Null{}
	}
	switch a := left.(type) {
	case wire.Number:
		b, ok := right.(wire.Number)
		if !ok {
			return false
		}
		ai, af := fieldNumber(a)
		bi, bf := fieldNumber(b)
		if ai != nil && bi != nil {
			return ai.Cmp(bi) == 0
		}
		return af == bf
	case wire.Array:
		b, ok := right.(wire.Array)
		if !ok || len(a) != len(b) {
			return false
		}
		for i := range a {
			if !fieldJSONEqual(a[i], b[i]) {
				return false
			}
		}
		return true
	case wire.Object:
		b, ok := right.(wire.Object)
		if !ok || len(a) != len(b) {
			return false
		}
		for key, value := range a {
			other, present := b[key]
			if !present || !fieldJSONEqual(value, other) {
				return false
			}
		}
		return true
	default:
		return jsonEq(left, right)
	}
}

func stepExpectFields(s *Store, step wire.StepExpectFields) (wire.StepResult, []string, error) {
	table, err := requireTable(s, step.Table)
	if err != nil {
		return wire.StepResult{}, nil, err
	}
	row, found := s.docs[rowKey{Table: step.Table, ID: step.ID}]
	if !found || row.DeletedAt != nil {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeNotFound, "document '"+step.ID+"' not found")
	}
	if len(step.Expected) == 0 || len(step.Expected) > 64 {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeBadRequest, "expected must contain 1-64 field assertions")
	}
	paths := map[string]bool{}
	nodes, bytes := 0, 0
	budgetError := func() error {
		return rtdberrors.New(rtdberrors.CodeBadRequest, "expected field values exceed the depth, node or UTF-8 byte budget")
	}
	var budget func(wire.JSONValue, int) error
	budget = func(value wire.JSONValue, depth int) error {
		nodes++
		if depth > 32 || nodes > 8192 || bytes > 65536 {
			return budgetError()
		}
		switch value := value.(type) {
		case nil, wire.Null, wire.Bool:
		case wire.Number:
			number, err := strconv.ParseFloat(string(value), 64)
			if err != nil || math.IsInf(number, 0) || math.IsNaN(number) {
				return budgetError()
			}
		case wire.String:
			if !utf8.ValidString(string(value)) {
				return budgetError()
			}
			bytes += len(value)
		case wire.Array:
			for _, child := range value {
				if err := budget(child, depth+1); err != nil {
					return err
				}
			}
		case wire.Object:
			for key, child := range value {
				if !utf8.ValidString(key) {
					return budgetError()
				}
				bytes += len(key)
				if err := budget(child, depth+1); err != nil {
					return err
				}
			}
		default:
			return budgetError()
		}
		if bytes > 65536 {
			return budgetError()
		}
		return nil
	}
	for _, item := range step.Expected {
		if len(item.Path) == 0 || len(item.Path) > 16 {
			return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeBadRequest, "field paths require 1-16 keys of 1-256 UTF-8 bytes")
		}
		for _, key := range item.Path {
			if key == "" || len(key) > 256 || !utf8.ValidString(key) {
				return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeBadRequest, "field paths require 1-16 keys of 1-256 UTF-8 bytes")
			}
			bytes += len(key)
		}
		key, _ := json.Marshal(item.Path)
		if paths[string(key)] {
			return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeBadRequest, "expected contains a duplicate field path")
		}
		paths[string(key)] = true
		if err := budget(item.Value, 0); err != nil {
			return wire.StepResult{}, nil, err
		}
		if !fieldPathDeclared(table.Fields, item.Path) {
			return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeSchemaViolation, "expected field path is not declared")
		}
	}
	for _, item := range step.Expected {
		var value wire.JSONValue = row.Doc
		for _, key := range item.Path {
			object, ok := value.(wire.Object)
			if !ok {
				return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodePreconditionFailed, "expected fields did not match")
			}
			value, found = object[key]
			if !found {
				return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodePreconditionFailed, "expected fields did not match")
			}
		}
		if !fieldJSONEqual(value, item.Value) {
			return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodePreconditionFailed, "expected fields did not match")
		}
	}
	return wire.StepResult{}, nil, nil
}
