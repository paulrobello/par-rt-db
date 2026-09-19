// Push-time schema validation and destructive-change detection — the port of
// swift InMemoryMigrate.swift's validation half / server schema.rs::validate
// and ddl.rs::detect_destructive_changes. The semantics corpus pins the error
// CODES of every rejection (its pushError cases).
package inmemory

import (
	"encoding/json"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// literalSet returns the values a finite literal-union (or lone literal)
// accepts; nil when the type is not a finite set (server schema::literal_set).
func literalSet(ty FieldType) []wire.JSONValue {
	switch ty.Kind {
	case "literal":
		return []wire.JSONValue{ty.Value}
	case "union":
		if len(ty.Variants) == 0 {
			return nil
		}
		var values []wire.JSONValue
		for _, v := range ty.Variants {
			if v.Kind != "literal" {
				return nil
			}
			values = append(values, v.Value)
		}
		return values
	default:
		return nil
	}
}

// isWideningOf reports whether every value accepted by old is also accepted
// by next (server schema::is_widening_of) — the one legal field-type change.
func isWideningOf(old, next FieldType) bool {
	oldValues := literalSet(old)
	newValues := literalSet(next)
	if oldValues == nil || newValues == nil {
		return false
	}
	for _, ov := range oldValues {
		found := false
		for _, nv := range newValues {
			if jsonEq(nv, ov) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// stripOnDelete returns ty with onDelete stripped from id fields, recursing
// through every compositor — adding or changing an action is additive.
func stripOnDelete(ty FieldType) FieldType {
	switch ty.Kind {
	case "id":
		if ty.OnDelete == nil {
			return ty
		}
		return FieldType{Kind: "id", Table: ty.Table}
	case "optional":
		return FieldType{Kind: "optional", Inner: ptrField(stripOnDelete(*ty.Inner))}
	case "union":
		out := FieldType{Kind: "union"}
		for _, v := range ty.Variants {
			out.Variants = append(out.Variants, stripOnDelete(v))
		}
		return out
	case "array":
		return FieldType{Kind: "array", Inner: ptrField(stripOnDelete(*ty.Inner))}
	case "object":
		out := FieldType{Kind: "object", Fields: map[string]FieldType{}}
		for k, v := range ty.Fields {
			out.Fields[k] = stripOnDelete(v)
		}
		return out
	case "record":
		return FieldType{Kind: "record", Inner: ptrField(stripOnDelete(*ty.Inner))}
	default:
		return ty
	}
}

func ptrField(f FieldType) *FieldType { return &f }

// detectDestructiveChanges rejects a second push that removes or retypes any
// existing table/field/index (server ddl.rs::detect_destructive_changes).
// Field types compare after stripping onDelete; a change is accepted only as
// a safe literal-union widening.
func detectDestructiveChanges(oldSchema, newSchema *SchemaDef) error {
	for tableName, oldTable := range oldSchema.Tables {
		newTable, ok := newSchema.Tables[tableName]
		if !ok {
			return rtdberrors.New(rtdberrors.CodeBadRequest, "removed table '"+tableName+"'")
		}
		for fieldName, oldType := range oldTable.Fields {
			newType, ok := newTable.Fields[fieldName]
			if !ok {
				return rtdberrors.New(rtdberrors.CodeBadRequest,
					"removed field '"+tableName+"."+fieldName+"'")
			}
			changed := !stripOnDelete(newType).Equal(stripOnDelete(oldType))
			if changed && !isWideningOf(oldType, newType) {
				return rtdberrors.New(rtdberrors.CodeBadRequest,
					"changed type of field '"+tableName+"."+fieldName+"'")
			}
		}
		for _, oldIndex := range oldTable.Indexes {
			var newIndex *IndexDef
			for i := range newTable.Indexes {
				if newTable.Indexes[i].Name == oldIndex.Name {
					newIndex = &newTable.Indexes[i]
					break
				}
			}
			if newIndex == nil {
				return rtdberrors.New(rtdberrors.CodeBadRequest, "removed index '"+oldIndex.Name+"'")
			}
			if !equalStrings(newIndex.Fields, oldIndex.Fields) {
				return rtdberrors.New(rtdberrors.CodeBadRequest,
					"changed fields of index '"+oldIndex.Name+"'")
			}
			if newIndex.Search != oldIndex.Search {
				return rtdberrors.New(rtdberrors.CodeBadRequest,
					"changed kind of index '"+oldIndex.Name+"' (btree <-> search)")
			}
			if !equalVectorSpec(newIndex.Vector, oldIndex.Vector) {
				return rtdberrors.New(rtdberrors.CodeBadRequest,
					"changed vector spec of index '"+oldIndex.Name+"'")
			}
			if newIndex.Unique != oldIndex.Unique {
				return rtdberrors.New(rtdberrors.CodeBadRequest,
					"changed uniqueness of index '"+oldIndex.Name+"'")
			}
			if !jsonEq(whereToValue(newIndex.WhereClause), whereToValue(oldIndex.WhereClause)) {
				return rtdberrors.New(rtdberrors.CodeBadRequest,
					"changed partial predicate of index '"+oldIndex.Name+"'")
			}
			if !equalStrPtr(newIndex.Language, oldIndex.Language) {
				return rtdberrors.New(rtdberrors.CodeBadRequest,
					"changed language of search index '"+oldIndex.Name+"'")
			}
		}
	}
	return nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStrPtr(a, b *string) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	return a == nil || *a == *b
}

func equalVectorSpec(a, b *VectorIndexSpec) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == nil {
		return true
	}
	return a.Dimensions == b.Dimensions && a.Metric == b.Metric &&
		equalStrings(a.FilterFields, b.FilterFields)
}

// whereToValue renders a where predicate to a comparable value (nil when
// absent). Filter equality is by structural JSON form.
func whereToValue(f wire.FilterExpr) wire.JSONValue {
	if f == nil {
		return nil
	}
	v, err := filterToJSONValue(f)
	if err != nil {
		return nil
	}
	return v
}

func filterToJSONValue(f wire.FilterExpr) (wire.JSONValue, error) {
	b, err := json.Marshal(f)
	if err != nil {
		return nil, err
	}
	return wire.UnmarshalJSON(b)
}

func valueExprToJSONValue(v wire.ValueExpr) (wire.JSONValue, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return wire.UnmarshalJSON(b)
}

// validateSchema is the push-time schema validation — the TTL,
// updatedAtField, autoIncrementField, and index-field rules of server
// schema::validate. Deliberately a subset (identifier formats,
// owner/collaborators fields, and defaults shapes stay server-side); computed
// validation and onDelete have their own passes.
func validateSchema(schema *SchemaDef) error {
	for tableName, table := range schema.Tables {
		if table.Authorize != nil {
			if err := rejectRelativeTimeFilter(table.Authorize, rtdberrors.CodeSchemaViolation,
				"olderThan filter is only allowed in patchByQuery/deleteByQuery filters"); err != nil {
				return err
			}
		}
		for _, index := range table.Indexes {
			if len(index.Fields) == 0 {
				return rtdberrors.New(rtdberrors.CodeSchemaViolation,
					"index '"+index.Name+"' on table '"+tableName+"' has no fields")
			}
			if index.WhereClause != nil {
				if err := rejectRelativeTimeFilter(index.WhereClause, rtdberrors.CodeBadRequest,
					"olderThan filter is not allowed in a partial-index predicate"); err != nil {
					return err
				}
			}
			if index.Vector != nil {
				continue
			}
			for _, fieldName := range index.Fields {
				fty, declared := table.Fields[fieldName]
				if !declared {
					return rtdberrors.New(rtdberrors.CodeSchemaViolation,
						"index '"+index.Name+"' on table '"+tableName+"' references unknown field '"+fieldName+"'")
				}
				pg, err := indexColumnType(fty)
				if err != nil {
					return err
				}
				if index.Search && pg.Pg != PgText {
					return rtdberrors.New(rtdberrors.CodeSchemaViolation,
						"search index '"+index.Name+"' on table '"+tableName+"' has non-text field '"+fieldName+"'")
				}
			}
		}
		if table.TTL != nil {
			if err := validateTTL(table); err != nil {
				return err
			}
		}
		if table.UpdatedAtField != nil {
			if err := validateUpdatedAtField(table); err != nil {
				return err
			}
		}
		if table.AutoIncrementField != nil {
			if err := validateAutoIncrementField(table); err != nil {
				return err
			}
		}
		if err := validateComputedTable(table, tableName); err != nil {
			return err
		}
	}
	return nil
}

func validateTTL(table *TableDef) error {
	ttl := table.TTL
	fty, declared := table.Fields[ttl.Field]
	if !declared {
		return rtdberrors.New(rtdberrors.CodeSchemaViolation,
			"ttl.field '"+ttl.Field+"' is not a declared field")
	}
	if fty.Kind != "number" && fty.Kind != "int64" {
		return rtdberrors.New(rtdberrors.CodeSchemaViolation,
			"ttl.field '"+ttl.Field+"' must be a number or bigint field")
	}
	hasTTLIndex := false
	for _, index := range table.Indexes {
		if !index.Search && index.Vector == nil && !index.Unique && index.WhereClause == nil &&
			len(index.Fields) == 1 && index.Fields[0] == ttl.Field {
			hasTTLIndex = true
			break
		}
	}
	if !hasTTLIndex {
		return rtdberrors.New(rtdberrors.CodeSchemaViolation,
			"ttl.field '"+ttl.Field+"' requires a single-field, non-unique, non-partial btree index on it")
	}
	if ttl.DefaultDurationMs != nil && *ttl.DefaultDurationMs <= 0 {
		return rtdberrors.New(rtdberrors.CodeSchemaViolation,
			"ttl.defaultDurationMs must be greater than 0")
	}
	return nil
}

func validateUpdatedAtField(table *TableDef) error {
	field := *table.UpdatedAtField
	fty, declared := table.Fields[field]
	if !declared {
		return rtdberrors.New(rtdberrors.CodeSchemaViolation,
			"updatedAtField '"+field+"' is not a declared field")
	}
	if fty.Kind != "number" && fty.Kind != "int64" {
		return rtdberrors.New(rtdberrors.CodeSchemaViolation,
			"updatedAtField '"+field+"' must be a number or bigint field")
	}
	if table.TTL != nil && table.TTL.Field == field {
		return rtdberrors.New(rtdberrors.CodeSchemaViolation,
			"updatedAtField '"+field+"' must differ from ttl.field (both "+
				"stamps write unconditionally; a shared field would drop the expiry)")
	}
	return nil
}

func validateAutoIncrementField(table *TableDef) error {
	field := *table.AutoIncrementField
	fty, declared := table.Fields[field]
	if !declared {
		return rtdberrors.New(rtdberrors.CodeSchemaViolation,
			"autoIncrementField '"+field+"' is not a declared field")
	}
	if fty.Kind != "int64" {
		return rtdberrors.New(rtdberrors.CodeSchemaViolation,
			"autoIncrementField '"+field+"' must be an int64 field")
	}
	if table.TTL != nil && table.TTL.Field == field {
		return rtdberrors.New(rtdberrors.CodeSchemaViolation,
			"autoIncrementField '"+field+"' must differ from ttl.field "+
				"(the ttl reaper would delete counter rows)")
	}
	if table.UpdatedAtField != nil && *table.UpdatedAtField == field {
		return rtdberrors.New(rtdberrors.CodeSchemaViolation,
			"autoIncrementField '"+field+"' must differ from updatedAtField "+
				"(the timestamp would overwrite the counter on every write)")
	}
	return nil
}

// validateOnDelete validates onDelete declarations at push time (server
// schema::validate_on_delete, FM-33): an action is legal only on a top-level
// id field (or one optional wrapping it — required for setNull); the
// referencing field needs a single-field, non-unique, non-partial btree
// index; and the referenced table must exist.
func validateOnDelete(schema *SchemaDef) error {
	for tableName, table := range schema.Tables {
		for fieldName, fieldTy := range table.Fields {
			declaration := onDeleteDeclaration(fieldTy)
			if declaration == nil {
				if fieldHasNestedOnDelete(fieldTy) {
					return rtdberrors.New(rtdberrors.CodeSchemaViolation,
						"field '"+fieldName+"' on table '"+tableName+"': onDelete is "+
							"legal only on a top-level id or optional-id field")
				}
				continue
			}
			if declaration.action == OnDeleteSetNull && fieldTy.Kind != "optional" {
				return rtdberrors.New(rtdberrors.CodeSchemaViolation,
					"onDelete 'setNull' requires the id field to be optional")
			}
			hasIndex := false
			for _, index := range table.Indexes {
				if !index.Search && index.Vector == nil && !index.Unique && index.WhereClause == nil &&
					len(index.Fields) == 1 && index.Fields[0] == fieldName {
					hasIndex = true
					break
				}
			}
			if !hasIndex {
				return rtdberrors.New(rtdberrors.CodeSchemaViolation,
					"onDelete field '"+fieldName+"' on table '"+tableName+"' requires a "+
						"single-field, non-unique, non-partial btree index on it")
			}
		}
	}
	for tableName, table := range schema.Tables {
		for fieldName, fieldTy := range table.Fields {
			declaration := onDeleteDeclaration(fieldTy)
			if declaration == nil {
				continue
			}
			if _, ok := schema.Tables[declaration.table]; !ok {
				return rtdberrors.New(rtdberrors.CodeSchemaViolation,
					"onDelete field '"+fieldName+"' on table '"+tableName+"' references "+
						"unknown table '"+declaration.table+"'")
			}
		}
	}
	return nil
}

type onDeleteDecl struct {
	table  string
	action OnDeleteAction
}

// onDeleteDeclaration extracts the top-level id declaration under ty (an
// optional wrapper unwraps) when it carries an onDelete action.
func onDeleteDeclaration(ty FieldType) *onDeleteDecl {
	inner := ty
	if inner.Kind == "optional" && inner.Inner != nil {
		inner = *inner.Inner
	}
	if inner.Kind == "id" && inner.OnDelete != nil {
		return &onDeleteDecl{table: inner.Table, action: *inner.OnDelete}
	}
	return nil
}

// fieldHasNestedOnDelete reports whether an id field carrying onDelete
// appears anywhere in ty at any nesting depth.
func fieldHasNestedOnDelete(ty FieldType) bool {
	switch ty.Kind {
	case "id":
		return ty.OnDelete != nil
	case "optional", "array", "record":
		return ty.Inner != nil && fieldHasNestedOnDelete(*ty.Inner)
	case "union":
		for _, v := range ty.Variants {
			if fieldHasNestedOnDelete(v) {
				return true
			}
		}
		return false
	case "object":
		for _, v := range ty.Fields {
			if fieldHasNestedOnDelete(v) {
				return true
			}
		}
		return false
	default:
		return false
	}
}

// onDeleteRef returns the onDelete action ty declares against parentTable —
// only a top-level id (or one optional wrapping it) can carry one.
func onDeleteRef(ty FieldType, parentTable string) (OnDeleteAction, bool) {
	if ty.Kind == "id" && ty.Table == parentTable && ty.OnDelete != nil {
		return *ty.OnDelete, true
	}
	if ty.Kind == "optional" && ty.Inner != nil {
		return onDeleteRef(*ty.Inner, parentTable)
	}
	return "", false
}

// hasOnDeleteChildren reports whether ANY table declares an onDelete field
// referencing parent (the TTL reaper's bulk-vs-cascade branch).
func hasOnDeleteChildren(schema *SchemaDef, parent string) bool {
	for _, table := range schema.Tables {
		for _, ty := range table.Fields {
			if _, ok := onDeleteRef(ty, parent); ok {
				return true
			}
		}
	}
	return false
}

// computedStaticKind is the statically-known result kind of a ValueExpr for
// the computed-field push check (server StaticKind); nil means it varies by
// input.
type computedStaticKind int

const (
	kindString computedStaticKind = iota
	kindNumber
	kindBoolean
)

func (k computedStaticKind) sample() wire.JSONValue {
	switch k {
	case kindString:
		return wire.String("s")
	case kindNumber:
		return wire.Number("1")
	default:
		return wire.Bool(true)
	}
}

func (k computedStaticKind) name() string {
	switch k {
	case kindString:
		return "a string"
	case kindNumber:
		return "a number"
	default:
		return "a boolean"
	}
}

func inferStaticKind(ve wire.ValueExpr) *computedStaticKind {
	var k computedStaticKind
	switch e := ve.(type) {
	case wire.ValueField, wire.ValueCoalesce, wire.ValueCase:
		return nil
	case wire.ValueLiteral:
		switch e.Value.(type) {
		case wire.String:
			k = kindString
		case wire.Number:
			k = kindNumber
		case wire.Bool:
			k = kindBoolean
		default:
			return nil
		}
	case wire.ValueConcat, wire.ValueLower, wire.ValueUpper, wire.ValueTrim:
		k = kindString
	case wire.ValueAdd, wire.ValueSub, wire.ValueMul, wire.ValueDiv, wire.ValueNow:
		k = kindNumber
	case wire.ValueCast:
		switch e.To {
		case wire.CastToString:
			k = kindString
		case wire.CastToNumber, wire.CastToInt64:
			k = kindNumber
		default:
			k = kindBoolean
		}
	default:
		return nil
	}
	return &k
}

func isPrincipalMarker(value wire.JSONValue) bool {
	obj, ok := value.(wire.Object)
	if !ok || len(obj) != 1 {
		return false
	}
	if b, ok := obj["$user"].(wire.Bool); ok && bool(b) {
		return true
	}
	if b, ok := obj["$email"].(wire.Bool); ok && bool(b) {
		return true
	}
	return false
}

// rejectRelativeTimeFilter rejects the execution-time-relative olderThan leaf
// anywhere in expr (the allow_relative_time = false mode of the server's
// validate_filter_expr_fields at its push-time call sites).
func rejectRelativeTimeFilter(expr wire.FilterExpr, code rtdberrors.ErrorCode, message string) error {
	found := false
	var walk func(wire.FilterExpr)
	walk = func(e wire.FilterExpr) {
		if found {
			return
		}
		switch t := e.(type) {
		case wire.FilterAnd:
			for _, sub := range t.Exprs {
				walk(sub)
			}
		case wire.FilterOr:
			for _, sub := range t.Exprs {
				walk(sub)
			}
		case wire.FilterNot:
			walk(t.Expr)
		case wire.FilterOlderThan:
			found = true
		}
	}
	walk(expr)
	if found {
		return rtdberrors.New(code, message)
	}
	return nil
}

// validateComputedCaseWhens walks a computed expression's case nodes
// rejecting principal markers and olderThan in every when filter; branch
// bodies recurse so a nested case is covered.
func validateComputedCaseWhens(ve wire.ValueExpr) error {
	switch e := ve.(type) {
	case wire.ValueCase:
		for _, cw := range e.Whens {
			if err := rejectPrincipalMarkers(cw.When); err != nil {
				return err
			}
			if err := rejectRelativeTimeFilter(cw.When, rtdberrors.CodeBadRequest,
				"olderThan filter is only allowed in patchByQuery/deleteByQuery filters"); err != nil {
				return err
			}
			if err := validateComputedCaseWhens(cw.Then); err != nil {
				return err
			}
		}
		return validateComputedCaseWhens(e.Otherwise)
	case wire.ValueConcat:
		for _, part := range e.Parts {
			if err := validateComputedCaseWhens(part); err != nil {
				return err
			}
		}
	case wire.ValueCoalesce:
		for _, part := range e.Parts {
			if err := validateComputedCaseWhens(part); err != nil {
				return err
			}
		}
	case wire.ValueAdd:
		return bothComputedWhens(e.Left, e.Right)
	case wire.ValueSub:
		return bothComputedWhens(e.Left, e.Right)
	case wire.ValueMul:
		return bothComputedWhens(e.Left, e.Right)
	case wire.ValueDiv:
		return bothComputedWhens(e.Left, e.Right)
	case wire.ValueLower:
		return validateComputedCaseWhens(e.Value)
	case wire.ValueUpper:
		return validateComputedCaseWhens(e.Value)
	case wire.ValueTrim:
		return validateComputedCaseWhens(e.Value)
	case wire.ValueCast:
		return validateComputedCaseWhens(e.Value)
	}
	return nil
}

func bothComputedWhens(l, r wire.ValueExpr) error {
	if err := validateComputedCaseWhens(l); err != nil {
		return err
	}
	return validateComputedCaseWhens(r)
}

// rejectPrincipalMarkers rejects a principal marker in any leaf VALUE
// position of a filter.
func rejectPrincipalMarkers(expr wire.FilterExpr) error {
	markerMsg := "principal markers ({\"$user\":true}/{\"$email\":true}) are not " +
		"allowed in client filters (field '"
	switch e := expr.(type) {
	case wire.FilterEq, wire.FilterNeq, wire.FilterGt, wire.FilterGte, wire.FilterLt, wire.FilterLte, wire.FilterContains:
		field, value := leafField(expr)
		if field == "" {
			if c, ok := expr.(wire.FilterContains); ok {
				field, value = c.Field, c.Value
			}
		}
		if isPrincipalMarker(value) {
			return rtdberrors.New(rtdberrors.CodeBadRequest, markerMsg+field+"')")
		}
	case wire.FilterIn:
		for _, value := range e.Values {
			if isPrincipalMarker(value) {
				return rtdberrors.New(rtdberrors.CodeBadRequest, markerMsg+e.Field+"')")
			}
		}
	case wire.FilterAnd:
		for _, sub := range e.Exprs {
			if err := rejectPrincipalMarkers(sub); err != nil {
				return err
			}
		}
	case wire.FilterOr:
		for _, sub := range e.Exprs {
			if err := rejectPrincipalMarkers(sub); err != nil {
				return err
			}
		}
	case wire.FilterNot:
		return rejectPrincipalMarkers(e.Expr)
	}
	return nil
}

// walkValueExprFields visits every field name a ValueExpr reads: each field
// node, every case branch's then/otherwise, and every filter field inside
// case whens (server value_expr::walk_value_expr_fields).
func walkValueExprFields(ve wire.ValueExpr, visit func(string)) {
	switch e := ve.(type) {
	case wire.ValueField:
		visit(e.Field)
	case wire.ValueLiteral, wire.ValueNow:
	case wire.ValueConcat:
		for _, part := range e.Parts {
			walkValueExprFields(part, visit)
		}
	case wire.ValueCoalesce:
		for _, part := range e.Parts {
			walkValueExprFields(part, visit)
		}
	case wire.ValueAdd:
		walkValueExprFields(e.Left, visit)
		walkValueExprFields(e.Right, visit)
	case wire.ValueSub:
		walkValueExprFields(e.Left, visit)
		walkValueExprFields(e.Right, visit)
	case wire.ValueMul:
		walkValueExprFields(e.Left, visit)
		walkValueExprFields(e.Right, visit)
	case wire.ValueDiv:
		walkValueExprFields(e.Left, visit)
		walkValueExprFields(e.Right, visit)
	case wire.ValueLower:
		walkValueExprFields(e.Value, visit)
	case wire.ValueUpper:
		walkValueExprFields(e.Value, visit)
	case wire.ValueTrim:
		walkValueExprFields(e.Value, visit)
	case wire.ValueCast:
		walkValueExprFields(e.Value, visit)
	case wire.ValueCase:
		for _, cw := range e.Whens {
			walkFilterExprFieldNames(cw.When, visit)
			walkValueExprFields(cw.Then, visit)
		}
		walkValueExprFields(e.Otherwise, visit)
	}
}

// validateComputedTable is the computed-field push validation (server
// schema::TableDef::validate_computed, ENH-028). BAD_REQUEST on every rule,
// matching the server (the corpus pushError cases pin the code).
func validateComputedTable(table *TableDef, tableName string) error {
	for field, expr := range table.Computed {
		if _, declared := table.Fields[field]; !declared {
			return rtdberrors.New(rtdberrors.CodeBadRequest,
				"computed field '"+tableName+"."+field+"' is not a declared field")
		}
		if table.OwnerField != nil && *table.OwnerField == field {
			return rtdberrors.New(rtdberrors.CodeBadRequest,
				"computed field '"+tableName+"."+field+"' must not be the table's ownerField")
		}
		if table.CollaboratorsField != nil && *table.CollaboratorsField == field {
			return rtdberrors.New(rtdberrors.CodeBadRequest,
				"computed field '"+tableName+"."+field+"' must not be the table's collaboratorsField")
		}
		if table.AutoIncrementField != nil && *table.AutoIncrementField == field {
			return rtdberrors.New(rtdberrors.CodeBadRequest,
				"computed field '"+tableName+"."+field+"' must not be the table's autoIncrementField")
		}
		var offender string
		walkValueExprFields(expr, func(referenced string) {
			if offender != "" {
				return
			}
			if _, declared := table.Fields[referenced]; !declared {
				offender = "computed field '" + tableName + "." + field + "' references undeclared field '" + referenced + "'"
			} else if _, computed := table.Computed[referenced]; computed {
				offender = "computed field '" + tableName + "." + field + "' references computed field '" +
					referenced + "' (computed fields may not reference each other)"
			}
		})
		if offender != "" {
			return rtdberrors.New(rtdberrors.CodeBadRequest, offender)
		}
		if err := validateComputedCaseWhens(expr); err != nil {
			return err
		}
		if kind := inferStaticKind(expr); kind != nil {
			declared, declaredOK := table.Fields[field]
			if !declaredOK {
				continue
			}
			inner := declared
			for inner.Kind == "optional" && inner.Inner != nil {
				inner = *inner.Inner
			}
			accepts := validateValue(declared, kind.sample()) ||
				(inner.Kind == "int64" && *kind == kindString)
			if !accepts {
				return rtdberrors.New(rtdberrors.CodeBadRequest,
					"computed field '"+tableName+"."+field+"' produces "+kind.name()+
						", which the field type does not accept")
			}
		}
	}
	if table.Authorize != nil {
		var offender string
		walkFilterExprFieldNames(table.Authorize, func(referenced string) {
			if offender == "" {
				if _, computed := table.Computed[referenced]; computed {
					offender = referenced
				}
			}
		})
		if offender != "" {
			return rtdberrors.New(rtdberrors.CodeBadRequest,
				"computed field '"+tableName+"."+offender+"' must not be referenced by "+
					"the table's authorize predicate (authorize predicates may not "+
					"reference computed fields)")
		}
	}
	return nil
}

// validateComputed validates every table's computed map — also called by the
// engine's migrate after directive folding so a changeType that invalidates a
// computed entry fails at plan time.
func validateComputed(schema *SchemaDef) error {
	for tableName, table := range schema.Tables {
		if err := validateComputedTable(table, tableName); err != nil {
			return err
		}
	}
	return nil
}
