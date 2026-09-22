// Package inmemory is the in-memory par-rt-db engine: a local harness with
// the server's semantics (schema validation, queries, transactions,
// migration) for unit tests, mirroring rust-client/src/in_memory/ and the
// swift InMemory* files. The wire-corpus semantics runner drives it in Task
// 28, so every behavior here must match the server's exactly.
package inmemory

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// OnDeleteAction is the referential action applied to child rows when the
// referenced parent row is hard-deleted (FM-33), carried on the child table's
// id field.
type OnDeleteAction string

const (
	OnDeleteCascade  OnDeleteAction = "cascade"
	OnDeleteRestrict OnDeleteAction = "restrict"
	OnDeleteSetNull  OnDeleteAction = "setNull"
)

// FieldType is a field's declared type — the 15-variant schema grammar
// parsed from the wire shape (internally tagged "type"). The flat struct with
// a Kind discriminant replaces rust's enum; only the payloads of the Kind are
// meaningful.
type FieldType struct {
	Kind       string               // string number boolean null id literal optional union array object int64 bytes any record vector
	Table      string               // id
	OnDelete   *OnDeleteAction      // id
	Value      wire.JSONValue       // literal
	Inner      *FieldType           // optional, array element, record value
	Variants   []FieldType          // union
	Fields     map[string]FieldType // object
	Dimensions int                  // vector
}

// Equal reports deep structural equality of two field types.
func (f FieldType) Equal(other FieldType) bool {
	return f.equal(&other)
}

func (f *FieldType) equal(o *FieldType) bool {
	if f.Kind != o.Kind || f.Table != o.Table || f.Dimensions != o.Dimensions {
		return false
	}
	if (f.OnDelete == nil) != (o.OnDelete == nil) {
		return false
	}
	if f.OnDelete != nil && *f.OnDelete != *o.OnDelete {
		return false
	}
	if !strictValueEq(f.Value, o.Value) {
		return false
	}
	if (f.Inner == nil) != (o.Inner == nil) {
		return false
	}
	if f.Inner != nil && !f.Inner.equal(o.Inner) {
		return false
	}
	if len(f.Variants) != len(o.Variants) {
		return false
	}
	for i := range f.Variants {
		if !f.Variants[i].equal(&o.Variants[i]) {
			return false
		}
	}
	if len(f.Fields) != len(o.Fields) {
		return false
	}
	for k, v := range f.Fields {
		ov, ok := o.Fields[k]
		if !ok || !v.equal(&ov) {
			return false
		}
	}
	return true
}

// IndexDef is one declared index on a table (btree, search, vector, unique,
// partial). Mirrors swift IndexDef; nil vector means not a vector index.
// Trgm (FM-30) marks a search index as also carrying a trigram GIN, which
// the search terminal's mode: "trgm" requires; omitted on the wire when
// false and grandfathered on re-push (see grandfatherTrgm).
type IndexDef struct {
	Name        string
	Fields      []string
	Search      bool
	Trgm        bool
	Vector      *VectorIndexSpec
	Unique      bool
	WhereClause wire.FilterExpr
	Language    *string
}

// VectorIndexSpec declares a vector (approximate nearest-neighbor) index.
type VectorIndexSpec struct {
	Dimensions   int
	FilterFields []string
	Metric       string // cosine | l2 | ip; empty == cosine
}

// TtlDef declares document TTL: a numeric field holding each doc's absolute
// epoch-ms expiry, with an optional default stamped at insert.
type TtlDef struct {
	Field             string
	DefaultDurationMs *int64
}

// TableDef is one table: fields, indexes, and the opt-in per-row rules.
type TableDef struct {
	Fields             map[string]FieldType
	Indexes            []IndexDef
	OwnerField         *string
	CollaboratorsField *string
	TTL                *TtlDef
	UpdatedAtField     *string
	AutoIncrementField *string
	Authorize          wire.FilterExpr
	Defaults           map[string]wire.JSONValue
	Computed           map[string]wire.ValueExpr
	SoftDelete         bool
}

// SchemaDef is a whole schema: named tables.
type SchemaDef struct {
	Tables map[string]*TableDef
}

// parseSchema converts a wire schema JSON value (the dsl.SchemaBuilder output
// shape) into the typed model. Unknown top-level keys are rejected, mirroring
// the server's serde deny-unknown-fields parse.
func parseSchema(v wire.JSONValue) (*SchemaDef, error) {
	obj, ok := v.(wire.Object)
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "schema must be a JSON object")
	}
	tablesV, ok := obj["tables"]
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "schema is missing the tables map")
	}
	tablesObj, ok := tablesV.(wire.Object)
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "schema.tables must be an object")
	}
	out := &SchemaDef{Tables: map[string]*TableDef{}}
	for name, tv := range tablesObj {
		td, err := parseTableDef(tv)
		if err != nil {
			return nil, err
		}
		out.Tables[name] = td
	}
	return out, nil
}

func parseTableDef(v wire.JSONValue) (*TableDef, error) {
	obj, ok := v.(wire.Object)
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "table definition must be a JSON object")
	}
	if err := rejectUnknownKeys(obj, "table", "fields", "indexes", "ownerField",
		"collaboratorsField", "ttl", "updatedAtField", "autoIncrementField",
		"authorize", "defaults", "computed", "softDelete"); err != nil {
		return nil, err
	}
	td := &TableDef{Fields: map[string]FieldType{}, Defaults: map[string]wire.JSONValue{}, Computed: map[string]wire.ValueExpr{}}
	fieldsObj, ok := obj["fields"].(wire.Object)
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "table.fields must be an object")
	}
	for fname, fv := range fieldsObj {
		ft, err := parseFieldType(fv)
		if err != nil {
			return nil, err
		}
		td.Fields[fname] = *ft
	}
	if idxV, ok := obj["indexes"]; ok && idxV != nil {
		arr, ok := idxV.(wire.Array)
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "table.indexes must be an array")
		}
		for _, iv := range arr {
			idx, err := parseIndexDef(iv)
			if err != nil {
				return nil, err
			}
			td.Indexes = append(td.Indexes, *idx)
		}
	}
	var err error
	if td.OwnerField, err = optString(obj, "ownerField"); err != nil {
		return nil, err
	}
	if td.CollaboratorsField, err = optString(obj, "collaboratorsField"); err != nil {
		return nil, err
	}
	if td.UpdatedAtField, err = optString(obj, "updatedAtField"); err != nil {
		return nil, err
	}
	if td.AutoIncrementField, err = optString(obj, "autoIncrementField"); err != nil {
		return nil, err
	}
	if v, ok := obj["softDelete"]; ok {
		b, ok := v.(wire.Bool)
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "table.softDelete must be a boolean")
		}
		td.SoftDelete = bool(b)
	}
	if v, ok := obj["ttl"]; ok && v != nil {
		ttl, err := parseTTL(v)
		if err != nil {
			return nil, err
		}
		td.TTL = ttl
	}
	if v, ok := obj["defaults"]; ok && v != nil {
		dobj, ok := v.(wire.Object)
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "table.defaults must be an object")
		}
		for k, dv := range dobj {
			td.Defaults[k] = dv
		}
	}
	if v, ok := obj["computed"]; ok && v != nil {
		cobj, ok := v.(wire.Object)
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "table.computed must be an object")
		}
		for k, cv := range cobj {
			raw, err := json.Marshal(cv)
			if err != nil {
				return nil, err
			}
			expr, err := wire.UnmarshalValueExpr(raw)
			if err != nil {
				return nil, err
			}
			td.Computed[k] = expr
		}
	}
	if v, ok := obj["authorize"]; ok && v != nil {
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		fe, err := wire.UnmarshalFilterExpr(raw)
		if err != nil {
			return nil, err
		}
		td.Authorize = fe
	}
	return td, nil
}

func parseIndexDef(v wire.JSONValue) (*IndexDef, error) {
	obj, ok := v.(wire.Object)
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "index definition must be a JSON object")
	}
	if err := rejectUnknownKeys(obj, "index", "name", "fields", "search", "trgm", "vector",
		"unique", "where", "language"); err != nil {
		return nil, err
	}
	idx := &IndexDef{}
	nameV, ok := obj["name"].(wire.String)
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "index.name must be a string")
	}
	idx.Name = string(nameV)
	fieldsV, ok := obj["fields"].(wire.Array)
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "index.fields must be an array")
	}
	for _, fv := range fieldsV {
		fs, ok := fv.(wire.String)
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "index.fields entries must be strings")
		}
		idx.Fields = append(idx.Fields, string(fs))
	}
	if v, ok := obj["search"]; ok && v != nil {
		b, ok := v.(wire.Bool)
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "index.search must be a boolean")
		}
		idx.Search = bool(b)
	}
	if v, ok := obj["unique"]; ok && v != nil {
		b, ok := v.(wire.Bool)
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "index.unique must be a boolean")
		}
		idx.Unique = bool(b)
	}
	if v, ok := obj["trgm"]; ok && v != nil {
		b, ok := v.(wire.Bool)
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "index.trgm must be a boolean")
		}
		idx.Trgm = bool(b)
	}
	if v, ok := obj["vector"]; ok && v != nil {
		vobj, ok := v.(wire.Object)
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "index.vector must be an object")
		}
		if err := rejectUnknownKeys(vobj, "vector", "dimensions", "filterFields", "metric"); err != nil {
			return nil, err
		}
		dimsV, ok := vobj["dimensions"].(wire.Number)
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "vector.dimensions must be a number")
		}
		dims, err := jsonNumberI64(dimsV)
		if err != nil {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "vector.dimensions must be an integer")
		}
		spec := &VectorIndexSpec{Dimensions: int(dims), Metric: "cosine"}
		if ff, ok := vobj["filterFields"]; ok && ff != nil {
			farr, ok := ff.(wire.Array)
			if !ok {
				return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "vector.filterFields must be an array")
			}
			for _, f := range farr {
				fs, ok := f.(wire.String)
				if !ok {
					return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "vector.filterFields entries must be strings")
				}
				spec.FilterFields = append(spec.FilterFields, string(fs))
			}
		}
		if m, ok := vobj["metric"]; ok && m != nil {
			ms, ok := m.(wire.String)
			if !ok {
				return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "vector.metric must be a string")
			}
			spec.Metric = string(ms)
		}
		idx.Vector = spec
	}
	if v, ok := obj["where"]; ok && v != nil {
		raw, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		fe, err := wire.UnmarshalFilterExpr(raw)
		if err != nil {
			return nil, err
		}
		idx.WhereClause = fe
	}
	if v, ok := obj["language"]; ok && v != nil {
		ls, ok := v.(wire.String)
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "index.language must be a string")
		}
		s := string(ls)
		idx.Language = &s
	}
	return idx, nil
}

func parseTTL(v wire.JSONValue) (*TtlDef, error) {
	obj, ok := v.(wire.Object)
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "ttl must be an object")
	}
	if err := rejectUnknownKeys(obj, "ttl", "field", "defaultDurationMs"); err != nil {
		return nil, err
	}
	fv, ok := obj["field"].(wire.String)
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "ttl.field must be a string")
	}
	ttl := &TtlDef{Field: string(fv)}
	if dv, ok := obj["defaultDurationMs"]; ok && dv != nil {
		dn, ok := dv.(wire.Number)
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "ttl.defaultDurationMs must be a number")
		}
		d, err := jsonNumberI64(dn)
		if err != nil {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "ttl.defaultDurationMs must be an integer")
		}
		ttl.DefaultDurationMs = &d
	}
	return ttl, nil
}

func rejectUnknownKeys(obj wire.Object, what string, allowed ...string) error {
	set := map[string]bool{}
	for _, k := range allowed {
		set[k] = true
	}
	for k := range obj {
		if !set[k] {
			return rtdberrors.New(rtdberrors.CodeBadRequest, what+" carries unknown key '"+k+"'")
		}
	}
	return nil
}

func optString(obj wire.Object, key string) (*string, error) {
	v, ok := obj[key]
	if !ok || v == nil {
		return nil, nil
	}
	s, ok := v.(wire.String)
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, key+" must be a string")
	}
	out := string(s)
	return &out, nil
}

// jsonNumberI64 reads a wire.Number's exact integer value; a float-spelled
// literal ("3.0") fails, mirroring serde_json's as_i64.
func jsonNumberI64(n wire.Number) (int64, error) {
	s := string(n)
	digits := s
	if len(digits) > 0 && (digits[0] == '-' || digits[0] == '+') {
		digits = digits[1:]
	}
	if digits == "" {
		return 0, rtdberrors.New(rtdberrors.CodeBadRequest, "expected an integer")
	}
	for i := 0; i < len(digits); i++ {
		if digits[i] < '0' || digits[i] > '9' {
			return 0, rtdberrors.New(rtdberrors.CodeBadRequest, "expected an integer")
		}
	}
	return strconv.ParseInt(s, 10, 64)
}

// jsonNumberF64 reads a wire.Number as a float64 (any spelling).
func jsonNumberF64(n wire.Number) float64 {
	f, _ := strconv.ParseFloat(string(n), 64)
	return f
}

// jsonNumberFromF64 renders a finite float64 back to its canonical JSON
// spelling (integer-valued doubles lose the trailing ".0", matching the
// other engines' JSON number handling).
func jsonNumberFromF64(f float64) wire.Number {
	if f == math.Trunc(f) && math.Abs(f) < 1e15 {
		return wire.Number(strconv.FormatInt(int64(f), 10))
	}
	return wire.Number(strconv.FormatFloat(f, 'g', -1, 64))
}

func stringPtr(s string) *string { return &s }

// parseFieldType converts the tagged wire shape ({"type": ...}) into the
// typed model. Unknown type tags are rejected, mirroring serde's closed enum.
func parseFieldType(v wire.JSONValue) (*FieldType, error) {
	obj, ok := v.(wire.Object)
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "field type must be a JSON object")
	}
	tagV, ok := obj["type"].(wire.String)
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "field type is missing the 'type' tag")
	}
	ft := &FieldType{Kind: string(tagV)}
	allowed := map[string][]string{
		"string": {}, "number": {}, "boolean": {}, "null": {}, "int64": {},
		"bytes": {}, "any": {},
		"id":       {"table", "onDelete"},
		"literal":  {"value"},
		"optional": {"inner"},
		"union":    {"variants"},
		"array":    {"element"},
		"object":   {"fields"},
		"record":   {"value"},
		"vector":   {"dimensions"},
	}
	keys, ok := allowed[ft.Kind]
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			fmt.Sprintf("field type: unknown type '%s'", ft.Kind))
	}
	if err := rejectUnknownKeys(obj, "field type", append([]string{"type"}, keys...)...); err != nil {
		return nil, err
	}
	switch ft.Kind {
	case "id":
		table, ok := obj["table"].(wire.String)
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "id field type requires 'table'")
		}
		ft.Table = string(table)
		if odv, ok := obj["onDelete"]; ok && odv != nil {
			ods, ok := odv.(wire.String)
			if !ok {
				return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "onDelete must be a string")
			}
			switch OnDeleteAction(ods) {
			case OnDeleteCascade, OnDeleteRestrict, OnDeleteSetNull:
				action := OnDeleteAction(ods)
				ft.OnDelete = &action
			default:
				return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
					"onDelete must be one of cascade|restrict|setNull")
			}
		}
	case "literal":
		lv, ok := obj["value"]
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "literal field type requires 'value'")
		}
		ft.Value = lv
	case "optional", "array", "record":
		key := "inner"
		if ft.Kind == "array" {
			key = "element"
		} else if ft.Kind == "record" {
			key = "value"
		}
		iv, ok := obj[key]
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "field type requires '"+key+"'")
		}
		inner, err := parseFieldType(iv)
		if err != nil {
			return nil, err
		}
		ft.Inner = inner
	case "union":
		variantsV, ok := obj["variants"].(wire.Array)
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "union field type requires 'variants'")
		}
		for _, vv := range variantsV {
			pv, err := parseFieldType(vv)
			if err != nil {
				return nil, err
			}
			ft.Variants = append(ft.Variants, *pv)
		}
	case "object":
		fieldsV, ok := obj["fields"].(wire.Object)
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "object field type requires 'fields'")
		}
		ft.Fields = map[string]FieldType{}
		for k, fv := range fieldsV {
			pv, err := parseFieldType(fv)
			if err != nil {
				return nil, err
			}
			ft.Fields[k] = *pv
		}
	case "vector":
		dimsV, ok := obj["dimensions"].(wire.Number)
		if !ok {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "vector field type requires 'dimensions'")
		}
		dims, err := jsonNumberI64(dimsV)
		if err != nil {
			return nil, err
		}
		ft.Dimensions = int(dims)
	}
	return ft, nil
}
