// Value/document validators and the write-path stamp helpers — the port of
// rust in_memory/validate.rs + swift InMemoryValidate.swift's value half
// (server schema.rs::validate_value/validate_doc, txn.rs stamps).
package inmemory

import (
	"encoding/json"
	"strconv"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// isHexID reports a 32-char lowercase hex string (an _id shape).
func isHexID(v wire.JSONValue) bool {
	s, ok := v.(wire.String)
	if !ok || len(s) != 32 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

// isInt64String reports a strict `-?digits` decimal string within the i64
// range (the canonical int64 wire form).
func isInt64String(v wire.JSONValue) bool {
	s, ok := v.(wire.String)
	if !ok {
		return false
	}
	_, valid := parseI64(string(s))
	return valid
}

// isBase64String reports base64 alphabet with 0-2 trailing '=' and a length
// that is a multiple of 4.
func isBase64String(v wire.JSONValue) bool {
	s, ok := v.(wire.String)
	if !ok || len(s)%4 != 0 {
		return false
	}
	eq := 0
	for i := len(s) - 1; i >= 0; i-- {
		c := s[i]
		if c == '=' {
			eq++
			continue
		}
		if eq > 2 {
			return false
		}
		if !((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '+' || c == '/') {
			return false
		}
	}
	return eq <= 2
}

// validateValue is the recursive value validator (server validate_value) —
// true when value satisfies the declared field type.
func validateValue(ty FieldType, value wire.JSONValue) bool {
	switch ty.Kind {
	case "string":
		_, ok := value.(wire.String)
		return ok
	case "number":
		return isJSONNumber(value)
	case "boolean":
		_, ok := value.(wire.Bool)
		return ok
	case "null":
		_, ok := value.(wire.Null)
		return ok || value == nil
	case "id":
		return isHexID(value)
	case "literal":
		return jsonEq(value, ty.Value)
	case "optional":
		if _, isNull := value.(wire.Null); isNull || value == nil {
			return true
		}
		return validateValue(*ty.Inner, value)
	case "union":
		for i := range ty.Variants {
			if validateValue(ty.Variants[i], value) {
				return true
			}
		}
		return false
	case "array":
		arr, ok := value.(wire.Array)
		if !ok {
			return false
		}
		for _, item := range arr {
			if !validateValue(*ty.Inner, item) {
				return false
			}
		}
		return true
	case "object":
		obj, ok := value.(wire.Object)
		if !ok {
			return false
		}
		for k := range obj {
			if _, declared := ty.Fields[k]; !declared {
				return false
			}
		}
		for name, fty := range ty.Fields {
			v, present := obj[name]
			if present {
				if !validateValue(fty, v) {
					return false
				}
			} else if fty.Kind != "optional" {
				return false
			}
		}
		return true
	case "int64":
		return isInt64String(value)
	case "bytes":
		return isBase64String(value)
	case "any":
		return true
	case "record":
		obj, ok := value.(wire.Object)
		if !ok {
			return false
		}
		for _, v := range obj {
			if !validateValue(*ty.Inner, v) {
				return false
			}
		}
		return true
	case "vector":
		arr, ok := value.(wire.Array)
		if !ok || len(arr) != ty.Dimensions {
			return false
		}
		for _, item := range arr {
			n, ok := item.(wire.Number)
			if !ok {
				return false
			}
			f := jsonNumberF64(n)
			if isInfOrNaN(f) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func isInfOrNaN(f float64) bool {
	return f != f || f > 1.7976931348623157e308 || f < -1.7976931348623157e308
}

// validateDoc is the full-document validator: reserved (`_`-prefixed) and
// unknown fields are rejected, every declared field is either
// present-and-valid or absent-and-optional. SCHEMA_VIOLATION on the first
// violation.
func validateDoc(table *TableDef, doc wire.Object) error {
	for key := range doc {
		if len(key) > 0 && key[0] == '_' {
			return rtdberrors.New(rtdberrors.CodeSchemaViolation, "field '"+key+"' is reserved")
		}
		if _, declared := table.Fields[key]; !declared {
			return rtdberrors.New(rtdberrors.CodeSchemaViolation, "unknown field '"+key+"'")
		}
	}
	for field, fty := range table.Fields {
		v, present := doc[field]
		if present {
			if !validateValue(fty, v) {
				return rtdberrors.New(rtdberrors.CodeSchemaViolation,
					"field '"+field+"' has an invalid value")
			}
		} else if fty.Kind != "optional" {
			return rtdberrors.New(rtdberrors.CodeSchemaViolation, "field '"+field+"' is required")
		}
	}
	return nil
}

// stripUnsetOptionals removes keys whose value is null for an Optional field
// whose inner type does not itself accept null — one representation of an
// unset optional (server strip_unset_optionals).
func stripUnsetOptionals(table *TableDef, doc wire.Object) wire.Object {
	out := wire.Object{}
	for key, value := range doc {
		if isNullValue(value) {
			if fty, declared := table.Fields[key]; declared && fty.Kind == "optional" {
				if !validateValue(*fty.Inner, value) {
					continue
				}
			}
		}
		out[key] = value
	}
	return out
}

func isNullValue(v wire.JSONValue) bool {
	_, isNull := v.(wire.Null)
	return isNull || v == nil
}

// stampTTLDefault stamps the TTL field at insert when the table declares a
// defaultDurationMs and the doc omits the field. Runs BEFORE validation.
func stampTTLDefault(table *TableDef, doc wire.Object, now int64) wire.Object {
	if table.TTL == nil || table.TTL.DefaultDurationMs == nil {
		return doc
	}
	if _, present := doc[table.TTL.Field]; present {
		return doc
	}
	out := cloneObject(doc)
	out[table.TTL.Field] = wire.Number(strconv.FormatInt(now+*table.TTL.DefaultDurationMs, 10))
	return out
}

// stampUpdatedAt stamps the table's updatedAtField with now, overwriting any
// client-supplied value (server stamp_updated_at). Runs BEFORE validation.
func stampUpdatedAt(table *TableDef, doc wire.Object, now int64) wire.Object {
	if table.UpdatedAtField == nil {
		return doc
	}
	out := cloneObject(doc)
	if fty, declared := table.Fields[*table.UpdatedAtField]; declared && fty.Kind == "int64" {
		out[*table.UpdatedAtField] = wire.Number(strconv.FormatInt(now, 10))
	} else {
		out[*table.UpdatedAtField] = wire.Number(strconv.FormatInt(now, 10))
	}
	return out
}

// applyDefaults applies the table's push-validated defaults to a NEW doc:
// every key the doc omits is stamped from the schema. New-document paths
// only (insert/replace/upsert-insert); patch never re-applies.
func applyDefaults(table *TableDef, doc wire.Object) wire.Object {
	if len(table.Defaults) == 0 {
		return doc
	}
	out := cloneObject(doc)
	for field, value := range table.Defaults {
		if _, present := out[field]; !present {
			out[field] = value
		}
	}
	return out
}

// stampComputed re-evaluates every computed entry over the final doc and
// stores the result — a null result REMOVES the key; a non-null result
// overwrites whatever is there. An evaluation error fails the whole write as
// BAD_REQUEST, naming the field. Runs last in the stamp chain, before
// validateDoc at every site.
func stampComputed(table *TableDef, doc wire.Object, now int64) (wire.Object, error) {
	if len(table.Computed) == 0 {
		return doc, nil
	}
	out := doc
	cloned := false
	for name, expr := range table.Computed {
		value, err := EvalValueExpr(expr, out, now, table.Fields)
		if err != nil {
			if re, ok := err.(*rtdberrors.RtDbError); ok {
				return nil, rtdberrors.New(re.Code, "computed field '"+name+"': "+re.Message)
			}
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "computed field '"+name+"': "+err.Error())
		}
		if !cloned {
			out = cloneObject(doc)
			cloned = true
		}
		if isNullValue(value) {
			delete(out, name)
		} else {
			out[name] = value
		}
	}
	return out, nil
}

// cloneObject deep-copies a doc object (docs are pure JSON).
func cloneObject(doc wire.Object) wire.Object {
	out := make(wire.Object, len(doc))
	for k, v := range doc {
		out[k] = cloneValue(v)
	}
	return out
}

func cloneValue(v wire.JSONValue) wire.JSONValue {
	switch t := v.(type) {
	case wire.Object:
		return cloneObject(t)
	case wire.Array:
		out := make(wire.Array, len(t))
		for i, item := range t {
			out[i] = cloneValue(item)
		}
		return out
	default:
		return v
	}
}

// applyPatch merges a patch's fields onto doc and re-validates the whole
// result (server txn::apply_patch). A computed key in the patch is dropped —
// the stamp re-derives it. The autoIncrement field is immutable after
// insert. A null onto an Optional whose inner rejects null deletes the key.
func applyPatch(table *TableDef, doc wire.Object, fields wire.Object, now int64) (wire.Object, error) {
	if table.AutoIncrementField != nil {
		auto := *table.AutoIncrementField
		if value, present := fields[auto]; present {
			existing, had := doc[auto]
			if !had || !jsonEq(existing, value) {
				return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
					"autoIncrementField '"+auto+"' cannot be changed")
			}
		}
	}
	merged := cloneObject(doc)
	for field, value := range fields {
		if _, computed := table.Computed[field]; computed {
			continue
		}
		fty, declared := table.Fields[field]
		if !declared {
			return nil, rtdberrors.New(rtdberrors.CodeSchemaViolation, "unknown field '"+field+"'")
		}
		if fty.Kind == "optional" && isNullValue(value) && !validateValue(*fty.Inner, value) {
			delete(merged, field)
			continue
		}
		if !validateValue(fty, value) {
			return nil, rtdberrors.New(rtdberrors.CodeSchemaViolation,
				"field '"+field+"' has an invalid value")
		}
		merged[field] = value
	}
	stamped, err := stampComputed(table, merged, now)
	if err != nil {
		return nil, err
	}
	if err := validateDoc(table, stamped); err != nil {
		return nil, err
	}
	return stamped, nil
}

// canonical renders a value with sorted object keys — the
// key-order-independent change-detection form (serde_json's BTreeMap
// ordering mirror; encoding/json marshals maps in sorted key order).
func canonical(v wire.JSONValue) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}
