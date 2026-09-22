// go-client/dsl/schema.go
package dsl

// Mirrors the schema DSL of rust-client/src/schema.rs + swift
// SchemaDsl.swift, producing the exact SchemaDef JSON consumed by
// POST /admin/push-schema. Shapes port core/src/schema.rs: FieldType is
// the 15-variant tagged union ({"type": ...}), TableDef carries fields,
// indexes, and the opt-in per-row rules. Build() emits wire.JSONValue per
// the plan's interface.

import (
	"encoding/json"
	"strconv"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// FieldType is an opaque built field-type JSON value.
type FieldType struct{ v wire.JSONValue }

// The 15 scalar/structural field types, mirroring core/src/schema.rs::FieldType.
func Str() FieldType {
	return FieldType{wire.Object(map[string]wire.JSONValue{"type": wire.String("string")})}
}
func Num() FieldType {
	return FieldType{wire.Object(map[string]wire.JSONValue{"type": wire.String("number")})}
}
func Bool() FieldType {
	return FieldType{wire.Object(map[string]wire.JSONValue{"type": wire.String("boolean")})}
}
func Null() FieldType {
	return FieldType{wire.Object(map[string]wire.JSONValue{"type": wire.String("null")})}
}
func Int64() FieldType {
	return FieldType{wire.Object(map[string]wire.JSONValue{"type": wire.String("int64")})}
}
func Bytes() FieldType {
	return FieldType{wire.Object(map[string]wire.JSONValue{"type": wire.String("bytes")})}
}
func Any() FieldType {
	return FieldType{wire.Object(map[string]wire.JSONValue{"type": wire.String("any")})}
}

// IdRef references a document in another table.
func IdRef(table string) FieldType {
	return FieldType{wire.Object(map[string]wire.JSONValue{
		"type": wire.String("id"), "table": wire.String(table),
	})}
}

// OnDelete is the referential action for an id field.
type OnDelete string

// OnDelete actions, mirroring core/src/schema.rs::OnDeleteAction.
const (
	OnDeleteCascade  OnDelete = "cascade"
	OnDeleteRestrict OnDelete = "restrict"
	OnDeleteSetNull  OnDelete = "setNull"
)

// IdRefWithOnDelete references another table with a referential action.
func IdRefWithOnDelete(table string, action OnDelete) FieldType {
	return FieldType{wire.Object(map[string]wire.JSONValue{
		"type": wire.String("id"), "table": wire.String(table), "onDelete": wire.String(action),
	})}
}

// FieldLiteral accepts exactly one literal value (FieldType constructor;
// renamed from Literal to avoid the ValueExpr constructor of the same name).
func FieldLiteral(v wire.JSONValue) FieldType {
	return FieldType{wire.Object(map[string]wire.JSONValue{
		"type": wire.String("literal"), "value": v,
	})}
}

// Optional wraps a type as T | null.
func Optional(inner FieldType) FieldType {
	return FieldType{wire.Object(map[string]wire.JSONValue{
		"type": wire.String("optional"), "inner": inner.v,
	})}
}

// Union accepts any of the variants.
func Union(variants ...FieldType) FieldType {
	members := make([]wire.JSONValue, len(variants))
	for i, v := range variants {
		members[i] = v.v
	}
	return FieldType{wire.Object(map[string]wire.JSONValue{
		"type": wire.String("union"), "variants": wire.Array(members),
	})}
}

// ArrayOf accepts arrays of element.
func ArrayOf(element FieldType) FieldType {
	return FieldType{wire.Object(map[string]wire.JSONValue{
		"type": wire.String("array"), "element": element.v,
	})}
}

// RecordOf accepts dynamic-key maps with a uniform value type.
func RecordOf(value FieldType) FieldType {
	return FieldType{wire.Object(map[string]wire.JSONValue{
		"type": wire.String("record"), "value": value.v,
	})}
}

// ObjectOf accepts fixed-shape nested objects.
func ObjectOf(fields map[string]FieldType) FieldType {
	m := map[string]wire.JSONValue{}
	for k, v := range fields {
		m[k] = v.v
	}
	return FieldType{wire.Object(map[string]wire.JSONValue{
		"type": wire.String("object"), "fields": wire.Object(m),
	})}
}

// VectorF is an embedding vector of fixed dimensions.
func VectorF(dimensions int) FieldType {
	return FieldType{wire.Object(map[string]wire.JSONValue{
		"type": wire.String("vector"), "dimensions": wire.Number(strconv.Itoa(dimensions)),
	})}
}

// SchemaBuilder assembles tables.
type SchemaBuilder struct {
	tables map[string]wire.JSONValue
	order  []string
}

// DefineSchema starts a schema.
func DefineSchema() *SchemaBuilder {
	return &SchemaBuilder{tables: map[string]wire.JSONValue{}}
}

// Table opens a table builder; the closure registers the table when it
// returns, mirroring rust's OnceTable form.
func (s *SchemaBuilder) Table(name string, build func(*TableBuilder)) *SchemaBuilder {
	t := &TableBuilder{name: name, fields: map[string]wire.JSONValue{}}
	build(t)
	s.add(name, t.Build())
	return s
}

func (s *SchemaBuilder) add(name string, t wire.JSONValue) {
	s.tables[name] = t
}

// Build emits the push-schema payload {tables: {name: tableDef}}.
func (s *SchemaBuilder) Build() wire.JSONValue {
	tables := map[string]wire.JSONValue{}
	for k, v := range s.tables {
		tables[k] = v
	}
	return wire.Object(map[string]wire.JSONValue{"tables": wire.Object(tables)})
}

// TableBuilder assembles one table's wire shape.
type TableBuilder struct {
	name      string
	fields    map[string]wire.JSONValue
	indexes   []wire.JSONValue
	lastIndex map[string]wire.JSONValue
	tableJSON map[string]wire.JSONValue
}

// Field declares a field.
func (t *TableBuilder) Field(name string, ft FieldType) *TableBuilder {
	t.fields[name] = ft.v
	return t
}

// Index declares a btree index.
func (t *TableBuilder) Index(name string, fields ...string) *TableBuilder {
	idx := map[string]wire.JSONValue{
		"name": wire.String(name), "fields": strArray(fields),
	}
	t.indexes = append(t.indexes, wire.Object(idx))
	t.lastIndex = idx
	return t
}

// Unique marks the most recently declared index unique.
func (t *TableBuilder) Unique() *TableBuilder {
	if t.lastIndex != nil {
		t.lastIndex["unique"] = wire.Bool(true)
	}
	return t
}

// Trgm opts the most recently declared search index into a trigram GIN,
// enabling the search terminal's mode: "trgm" (substring matching) on it
// (FM-30). Legal only on a search index; the server rejects trgm on a
// btree/vector index at push time. No-op if no index has been declared yet.
func (t *TableBuilder) Trgm() *TableBuilder {
	if t.lastIndex != nil {
		t.lastIndex["trgm"] = wire.Bool(true)
	}
	return t
}

// Where adds a partial-index predicate to the most recent index (wire key
// "where").
func (t *TableBuilder) Where(f wire.FilterExpr) *TableBuilder {
	if t.lastIndex != nil {
		t.lastIndex["where"] = exprToJSON(f)
	}
	return t
}

// SearchIndex declares a full-text search index with an optional language
// ("" = server default english).
func (t *TableBuilder) SearchIndex(name string, language string, fields ...string) *TableBuilder {
	idx := map[string]wire.JSONValue{
		"name": wire.String(name), "fields": strArray(fields), "search": wire.Bool(true),
	}
	if language != "" {
		idx["language"] = wire.String(language)
	}
	t.indexes = append(t.indexes, wire.Object(idx))
	t.lastIndex = idx
	return t
}

// VectorIndex declares a vector index over a vector field.
func (t *TableBuilder) VectorIndex(name, field string, dims int, filterFields []string, metric string) *TableBuilder {
	spec := map[string]wire.JSONValue{"dimensions": wire.Number(strconv.Itoa(dims))}
	if len(filterFields) > 0 {
		spec["filterFields"] = strArray(filterFields)
	}
	if metric != "" {
		spec["metric"] = wire.String(metric)
	}
	idx := map[string]wire.JSONValue{
		"name": wire.String(name), "fields": strArray([]string{field}), "vector": wire.Object(spec),
	}
	t.indexes = append(t.indexes, wire.Object(idx))
	return t
}

// OwnerField enables owner-scoped per-row authorization.
func (t *TableBuilder) OwnerField(field string) *TableBuilder {
	return t.optStr("ownerField", field)
}

// CollaboratorsField enables collaborator access on top of ownerField.
func (t *TableBuilder) CollaboratorsField(field string) *TableBuilder {
	return t.optStr("collaboratorsField", field)
}

// Authorize installs a per-row predicate DSL.
func (t *TableBuilder) Authorize(predicate wire.FilterExpr) *TableBuilder {
	t.ensure()["authorize"] = exprToJSON(predicate)
	return t
}

// TTL declares document expiry with an optional default duration in ms.
func (t *TableBuilder) TTL(field string, defaultDurationMs *int64) *TableBuilder {
	ttl := map[string]wire.JSONValue{"field": wire.String(field)}
	if defaultDurationMs != nil {
		ttl["defaultDurationMs"] = wire.Number(strconv.FormatInt(*defaultDurationMs, 10))
	}
	t.ensure()["ttl"] = wire.Object(ttl)
	return t
}

// UpdatedAtField names the auto-stamped modified-time field.
func (t *TableBuilder) UpdatedAtField(field string) *TableBuilder {
	return t.optStr("updatedAtField", field)
}

// AutoIncrementField names the auto-incremented counter field.
func (t *TableBuilder) AutoIncrementField(field string) *TableBuilder {
	return t.optStr("autoIncrementField", field)
}

// Defaults stamps insert-time defaults for the named fields.
func (t *TableBuilder) Defaults(entries map[string]wire.JSONValue) *TableBuilder {
	t.ensure()["defaults"] = wire.Object(entries)
	return t
}

// Computed declares a computed field from a ValueExpr.
func (t *TableBuilder) Computed(name string, expr wire.ValueExpr) *TableBuilder {
	m, ok := t.ensure()["computed"].(wire.Object)
	if !ok {
		m = wire.Object(map[string]wire.JSONValue{})
	}
	m[name] = exprToJSON(expr)
	t.ensure()["computed"] = m
	return t
}

// exprToJSON converts a typed expr (FilterExpr/ValueExpr) into the dynamic
// tree via a marshal round-trip (deterministic bytes).
func exprToJSON(v any) wire.JSONValue {
	b, err := json.Marshal(v)
	if err != nil {
		return wire.Null{}
	}
	out, err := wire.UnmarshalJSON(b)
	if err != nil {
		return wire.Null{}
	}
	return out
}

// SoftDelete enables soft deletion.
func (t *TableBuilder) SoftDelete() *TableBuilder {
	t.ensure()["softDelete"] = wire.Bool(true)
	return t
}

// Build finalizes the table wire shape.
func (t *TableBuilder) Build() wire.JSONValue {
	out := t.ensure()
	out["fields"] = wire.Object(t.fields)
	if len(t.indexes) > 0 {
		out["indexes"] = wire.Array(t.indexes)
	}
	return wire.Object(out)
}

func (t *TableBuilder) ensure() map[string]wire.JSONValue {
	if t.tableJSON == nil {
		t.tableJSON = map[string]wire.JSONValue{}
	}
	return t.tableJSON
}

func (t *TableBuilder) optStr(key, field string) *TableBuilder {
	t.ensure()[key] = wire.String(field)
	return t
}

func strArray(ss []string) wire.Array {
	out := make(wire.Array, len(ss))
	for i, s := range ss {
		out[i] = wire.String(s)
	}
	return out
}
