// The schema-migration engine — the Go port of rust in_memory/migrate.rs /
// swift InMemoryMigrate.swift: destructive-change detection (schema.go),
// the migration-directive interpreter (one function per directive kind,
// folding a working schema copy while rewriting the doc store), and the
// atomic plan/apply wrapper mirroring server migrate::plan/apply_migration.
package inmemory

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"

	"github.com/paulrobello/par-rt-db/go-client/admin"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// ApplyMigration plans and applies a declarative migration. Structural
// directives fold into a working schema copy; data directives rewrite the
// doc map. A failed directive is atomic: the doc store is restored wholesale
// and the working schema was never installed. With dryRun the full plan is
// validated and affected_rows reported, but nothing commits.
func ApplyMigration(s *Store, directives []admin.Directive, dryRun bool) (admin.MigrateResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.schema == nil {
		return admin.MigrateResult{}, rtdberrors.New(rtdberrors.CodeBadRequest, "no schema pushed for migration")
	}
	old := cloneSchema(s.schema)
	planned := old
	touched := map[string]bool{}
	reports := make([]admin.DirectiveReport, 0, len(directives))
	snapshot := snapshotDocs(s)

	for _, d := range directives {
		report, table, err := applyMigrationDirective(s, &planned, d)
		if err != nil {
			s.docs = snapshot
			return admin.MigrateResult{}, err
		}
		reports = append(reports, report)
		if table != "" {
			touched[table] = true
		}
	}
	// ENH-028: directive folding must not invalidate a computed entry —
	// re-validate the derived schema (pure) before anything commits.
	if err := validateSchema(planned); err != nil {
		s.docs = snapshot
		return admin.MigrateResult{}, err
	}
	schemaJSON, err := schemaToWire(planned)
	if err != nil {
		s.docs = snapshot
		return admin.MigrateResult{}, err
	}
	if dryRun {
		s.docs = snapshot
		return admin.MigrateResult{Applied: false, Schema: schemaJSON, Directives: reports}, nil
	}
	s.schema = planned
	s.tables = map[string]*TableDef{}
	for name, def := range planned.Tables {
		s.tables[name] = def
	}
	notifySubs(s, touched)
	return admin.MigrateResult{Applied: true, Schema: schemaJSON, Directives: reports}, nil
}

// cloneSchema deep-copies a typed schema (directives mutate the working copy).
func cloneSchema(in *SchemaDef) *SchemaDef {
	out := &SchemaDef{Tables: map[string]*TableDef{}}
	for name, td := range in.Tables {
		out.Tables[name] = cloneTableDef(td)
	}
	return out
}

func cloneTableDef(in *TableDef) *TableDef {
	out := &TableDef{
		Fields:     map[string]FieldType{},
		Defaults:   map[string]wire.JSONValue{},
		Computed:   map[string]wire.ValueExpr{},
		SoftDelete: in.SoftDelete,
	}
	for k, v := range in.Fields {
		out.Fields[k] = cloneFieldType(v)
	}
	out.Indexes = append(out.Indexes, in.Indexes...)
	for _, idx := range out.Indexes {
		i := idx
		_ = i
	}
	for i := range out.Indexes {
		if in.Indexes[i].Vector != nil {
			v := *in.Indexes[i].Vector
			v.FilterFields = append([]string(nil), in.Indexes[i].Vector.FilterFields...)
			out.Indexes[i].Vector = &v
		}
		if in.Indexes[i].Language != nil {
			l := *in.Indexes[i].Language
			out.Indexes[i].Language = &l
		}
		if in.Indexes[i].WhereClause != nil {
			out.Indexes[i].WhereClause = cloneFilterExpr(in.Indexes[i].WhereClause)
		}
	}
	if in.OwnerField != nil {
		v := *in.OwnerField
		out.OwnerField = &v
	}
	if in.CollaboratorsField != nil {
		v := *in.CollaboratorsField
		out.CollaboratorsField = &v
	}
	if in.UpdatedAtField != nil {
		v := *in.UpdatedAtField
		out.UpdatedAtField = &v
	}
	if in.AutoIncrementField != nil {
		v := *in.AutoIncrementField
		out.AutoIncrementField = &v
	}
	if in.Authorize != nil {
		out.Authorize = cloneFilterExpr(in.Authorize)
	}
	for k, v := range in.Defaults {
		out.Defaults[k] = v
	}
	for k, v := range in.Computed {
		out.Computed[k] = cloneValueExpr(v)
	}
	return out
}

func cloneFieldType(in FieldType) FieldType {
	out := in
	if in.OnDelete != nil {
		v := *in.OnDelete
		out.OnDelete = &v
	}
	if in.Inner != nil {
		v := cloneFieldType(*in.Inner)
		out.Inner = &v
	}
	out.Variants = append([]FieldType(nil), in.Variants...)
	for i := range out.Variants {
		out.Variants[i] = cloneFieldType(out.Variants[i])
	}
	out.Fields = map[string]FieldType{}
	for k, v := range in.Fields {
		out.Fields[k] = cloneFieldType(v)
	}
	return out
}

func cloneFilterExpr(in wire.FilterExpr) wire.FilterExpr {
	b, err := json.Marshal(in)
	if err != nil {
		return in
	}
	out, err := wire.UnmarshalFilterExpr(b)
	if err != nil {
		return in
	}
	return out
}

func cloneValueExpr(in wire.ValueExpr) wire.ValueExpr {
	b, err := json.Marshal(in)
	if err != nil {
		return in
	}
	out, err := wire.UnmarshalValueExpr(b)
	if err != nil {
		return in
	}
	return out
}

// applyMigrationDirective validates and applies one directive: folds the
// structural effect into planned and rewrites the doc map. Returns the
// report and the touched table (for the notify fan-out).
func applyMigrationDirective(s *Store, planned **SchemaDef, d admin.Directive) (admin.DirectiveReport, string, error) {
	switch t := d.(type) {
	case admin.DirectiveRenameField:
		return applyRenameField(s, planned, t.Table, t.From, t.To)
	case admin.DirectiveRenameTable:
		return applyRenameTable(s, planned, t.From, t.To)
	case admin.DirectiveChangeType:
		return applyChangeType(s, planned, t)
	case admin.DirectiveDropField:
		return applyDropField(s, planned, t.Table, t.Field)
	case admin.DirectiveDropTable:
		return applyDropTable(s, planned, t.Name)
	case admin.DirectiveDropIndex:
		return applyDropIndex(s, planned, t.Table, t.Name)
	case admin.DirectiveSetDefault:
		return applySetDefault(s, planned, t.Table, t.Field, t.Value)
	case admin.DirectiveEvalExpr:
		return admin.DirectiveReport{}, "", rtdberrors.New(rtdberrors.CodeBadRequest, "evalExpr unsupported in-memory")
	default:
		return admin.DirectiveReport{}, "", rtdberrors.New(rtdberrors.CodeInternal, "unknown directive kind")
	}
}

func requireMigrateTable(schema *SchemaDef, name string) error {
	if _, ok := schema.Tables[name]; !ok {
		return rtdberrors.New(rtdberrors.CodeBadRequest, "table '"+name+"' does not exist")
	}
	return nil
}

// renameFieldRefs rewrites every reference to field `from` on `table` to
// `to`: index field lists, owner/collaborators/autoIncrement/updatedAt, and
// the ttl field (server migrate::rename_field_refs).
func renameFieldRefs(schema *SchemaDef, table, from, to string) {
	td := schema.Tables[table]
	if td == nil {
		return
	}
	for i := range td.Indexes {
		for j, f := range td.Indexes[i].Fields {
			if f == from {
				td.Indexes[i].Fields[j] = to
			}
		}
	}
	if td.OwnerField != nil && *td.OwnerField == from {
		td.OwnerField = stringPtr(to)
	}
	if td.CollaboratorsField != nil && *td.CollaboratorsField == from {
		td.CollaboratorsField = stringPtr(to)
	}
	if td.AutoIncrementField != nil && *td.AutoIncrementField == from {
		td.AutoIncrementField = stringPtr(to)
	}
	if td.UpdatedAtField != nil && *td.UpdatedAtField == from {
		td.UpdatedAtField = stringPtr(to)
	}
	if td.TTL != nil && td.TTL.Field == from {
		td.TTL.Field = to
	}
	if td.Authorize != nil {
		td.Authorize = renamedFilterExpr(td.Authorize, from, to)
	}
}

func applyRenameField(s *Store, planned **SchemaDef, table, from, to string) (admin.DirectiveReport, string, error) {
	if err := requireMigrateTable(*planned, table); err != nil {
		return admin.DirectiveReport{}, "", err
	}
	td := (*planned).Tables[table]
	if _, exists := td.Fields[to]; exists {
		return admin.DirectiveReport{}, "", rtdberrors.New(rtdberrors.CodeBadRequest,
			"rename target '"+table+"."+to+"' already exists")
	}
	fty, ok := td.Fields[from]
	if !ok {
		return admin.DirectiveReport{}, "", rtdberrors.New(rtdberrors.CodeBadRequest,
			"renamed field '"+table+"."+from+"' does not exist")
	}
	delete(td.Fields, from)
	td.Fields[to] = fty
	renameFieldRefs(*planned, table, from, to)
	// ENH-028: the computed map follows the rename — the entry keyed on the
	// renamed field moves, and every expression reference is rewritten.
	if keyed, had := td.Computed[from]; had {
		delete(td.Computed, from)
		td.Computed[to] = keyed
	}
	for k, expr := range td.Computed {
		td.Computed[k] = renamedValueExpr(expr, from, to)
	}
	// QA-002: defaults is keyed by field name the same way.
	if keyed, had := td.Defaults[from]; had {
		delete(td.Defaults, from)
		td.Defaults[to] = keyed
	}
	affected := int64(0)
	for key, row := range s.docs {
		if key.Table != table {
			continue
		}
		value, present := row.Doc[from]
		if !present {
			continue
		}
		delete(row.Doc, from)
		row.Doc[to] = value
		affected++
	}
	return admin.DirectiveReport{Op: "renameField", AffectedRows: affected}, table, nil
}

// renamedFilterExpr rewrites every leaf field equal to from to to (server
// rename_filter_fields).
func renamedFilterExpr(expr wire.FilterExpr, from, to string) wire.FilterExpr {
	switch e := expr.(type) {
	case wire.FilterAnd:
		out := wire.FilterAnd{}
		for _, sub := range e.Exprs {
			out.Exprs = append(out.Exprs, renamedFilterExpr(sub, from, to))
		}
		return out
	case wire.FilterOr:
		out := wire.FilterOr{}
		for _, sub := range e.Exprs {
			out.Exprs = append(out.Exprs, renamedFilterExpr(sub, from, to))
		}
		return out
	case wire.FilterNot:
		return wire.FilterNot{Expr: renamedFilterExpr(e.Expr, from, to)}
	case wire.FilterIn:
		if e.Field == from {
			e.Field = to
		}
		return e
	case wire.FilterContains:
		if e.Field == from {
			e.Field = to
		}
		return e
	case wire.FilterExists:
		if e.Field == from {
			e.Field = to
		}
		return e
	case wire.FilterOlderThan:
		if e.Field == from {
			e.Field = to
		}
		return e
	default:
		field, value := leafField(expr)
		if field == from {
			switch expr.(type) {
			case wire.FilterEq:
				return wire.FilterEq{Field: to, Value: value}
			case wire.FilterNeq:
				return wire.FilterNeq{Field: to, Value: value}
			case wire.FilterGt:
				return wire.FilterGt{Field: to, Value: value}
			case wire.FilterGte:
				return wire.FilterGte{Field: to, Value: value}
			case wire.FilterLt:
				return wire.FilterLt{Field: to, Value: value}
			case wire.FilterLte:
				return wire.FilterLte{Field: to, Value: value}
			}
		}
		return expr
	}
}

// renamedValueExpr rewrites every field reference equal to from to to — the
// value-returning mirror of walkValueExprFields (server
// rename_value_expr_fields); case whens reuse renamedFilterExpr.
func renamedValueExpr(ve wire.ValueExpr, from, to string) wire.ValueExpr {
	switch e := ve.(type) {
	case wire.ValueField:
		if e.Field == from {
			return wire.ValueField{Field: to}
		}
		return e
	case wire.ValueLiteral, wire.ValueNow:
		return ve
	case wire.ValueConcat:
		out := wire.ValueConcat{}
		for _, p := range e.Parts {
			out.Parts = append(out.Parts, renamedValueExpr(p, from, to))
		}
		return out
	case wire.ValueCoalesce:
		out := wire.ValueCoalesce{}
		for _, p := range e.Parts {
			out.Parts = append(out.Parts, renamedValueExpr(p, from, to))
		}
		return out
	case wire.ValueAdd:
		return wire.ValueAdd{Left: renamedValueExpr(e.Left, from, to), Right: renamedValueExpr(e.Right, from, to)}
	case wire.ValueSub:
		return wire.ValueSub{Left: renamedValueExpr(e.Left, from, to), Right: renamedValueExpr(e.Right, from, to)}
	case wire.ValueMul:
		return wire.ValueMul{Left: renamedValueExpr(e.Left, from, to), Right: renamedValueExpr(e.Right, from, to)}
	case wire.ValueDiv:
		return wire.ValueDiv{Left: renamedValueExpr(e.Left, from, to), Right: renamedValueExpr(e.Right, from, to)}
	case wire.ValueLower:
		return wire.ValueLower{Value: renamedValueExpr(e.Value, from, to)}
	case wire.ValueUpper:
		return wire.ValueUpper{Value: renamedValueExpr(e.Value, from, to)}
	case wire.ValueTrim:
		return wire.ValueTrim{Value: renamedValueExpr(e.Value, from, to)}
	case wire.ValueCast:
		return wire.ValueCast{Value: renamedValueExpr(e.Value, from, to), To: e.To}
	case wire.ValueCase:
		out := wire.ValueCase{}
		for _, cw := range e.Whens {
			out.Whens = append(out.Whens, wire.CaseWhen{
				When: renamedFilterExpr(cw.When, from, to),
				Then: renamedValueExpr(cw.Then, from, to),
			})
		}
		out.Otherwise = renamedValueExpr(e.Otherwise, from, to)
		return out
	default:
		return ve
	}
}

func applyRenameTable(s *Store, planned **SchemaDef, from, to string) (admin.DirectiveReport, string, error) {
	if _, exists := (*planned).Tables[to]; exists {
		return admin.DirectiveReport{}, "", rtdberrors.New(rtdberrors.CodeBadRequest,
			"rename target table '"+to+"' already exists")
	}
	def, ok := (*planned).Tables[from]
	if !ok {
		return admin.DirectiveReport{}, "", rtdberrors.New(rtdberrors.CodeBadRequest,
			"renamed table '"+from+"' does not exist")
	}
	delete((*planned).Tables, from)
	// Id references to `from` in other tables follow the rename (onDelete
	// preserved — the rust engine's behavior).
	var names []string
	for name := range (*planned).Tables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		td := (*planned).Tables[name]
		for fieldName, fieldTy := range td.Fields {
			if fieldTy.Kind == "id" && fieldTy.Table == from {
				nt := fieldTy
				nt.Table = to
				td.Fields[fieldName] = nt
			}
		}
	}
	(*planned).Tables[to] = def
	// Move the rows.
	var keys []rowKey
	for key := range s.docs {
		if key.Table == from {
			keys = append(keys, key)
		}
	}
	for _, key := range keys {
		row := s.docs[key]
		delete(s.docs, key)
		s.docs[rowKey{Table: to, ID: key.ID}] = row
	}
	return admin.DirectiveReport{Op: "renameTable", AffectedRows: 0}, to, nil
}

// castValidFor reports whether cast can coerce from old (server
// migrate::cast_valid_for).
func castValidFor(cast wire.Cast, old FieldType) bool {
	tag := old.Kind
	switch cast {
	case wire.CastToString:
		return tag == "string" || tag == "number" || tag == "boolean" || tag == "int64"
	case wire.CastToNumber:
		return tag == "string" || tag == "boolean" || tag == "int64"
	case wire.CastToInt64:
		return tag == "string" || tag == "number"
	case wire.CastToBoolean:
		return tag == "string" || tag == "number"
	default:
		return false
	}
}

// coerceValue is the pure coercion (server migrate::coerce_value): nil when
// the value cannot coerce under cast; toInt64 emits the canonical decimal
// string, toNumber a JSON number.
func coerceValue(cast wire.Cast, value wire.JSONValue) wire.JSONValue {
	out, err := func() (wire.JSONValue, error) {
		switch cast {
		case wire.CastToString:
			if text, ok := valueText(value); ok {
				return wire.String(text), nil
			}
			return nil, rtdberrors.New(rtdberrors.CodeInternal, "not coercible")
		case wire.CastToNumber:
			switch t := value.(type) {
			case wire.String:
				f, err := strconvParseFloatStrict(string(t))
				if err != nil {
					return nil, rtdberrors.New(rtdberrors.CodeInternal, "not coercible")
				}
				return jsonNumberFromF64(f), nil
			case wire.Number:
				return value, nil
			case wire.Bool:
				if bool(t) {
					return wire.Number("1"), nil
				}
				return wire.Number("0"), nil
			}
			return nil, rtdberrors.New(rtdberrors.CodeInternal, "not coercible")
		case wire.CastToInt64:
			switch t := value.(type) {
			case wire.String:
				if isInt64String(t) {
					return t, nil
				}
			case wire.Number:
				if i, ok := parseI64(string(t)); ok {
					return wire.String(formatI64(i)), nil
				}
			}
			return nil, rtdberrors.New(rtdberrors.CodeInternal, "not coercible")
		case wire.CastToBoolean:
			switch t := value.(type) {
			case wire.String:
				switch strings.ToLower(string(t)) {
				case "true", "1":
					return wire.Bool(true), nil
				case "false", "0":
					return wire.Bool(false), nil
				}
			case wire.Number:
				f := jsonNumberF64(t)
				return wire.Bool(f != 0), nil
			}
			return nil, rtdberrors.New(rtdberrors.CodeInternal, "not coercible")
		default:
			return nil, rtdberrors.New(rtdberrors.CodeInternal, "unknown cast")
		}
	}()
	if err != nil {
		return nil
	}
	return out
}

func strconvParseFloatStrict(s string) (float64, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return 0, rtdberrors.New(rtdberrors.CodeInternal, "empty")
	}
	f, err := strconv.ParseFloat(trimmed, 64)
	if err != nil || isInfOrNaN(f) {
		return 0, rtdberrors.New(rtdberrors.CodeInternal, "not numeric")
	}
	return f, nil
}

func applyChangeType(s *Store, planned **SchemaDef, t admin.DirectiveChangeType) (admin.DirectiveReport, string, error) {
	if err := requireMigrateTable(*planned, t.Table); err != nil {
		return admin.DirectiveReport{}, "", err
	}
	td := (*planned).Tables[t.Table]
	oldTy, ok := td.Fields[t.Field]
	if !ok {
		return admin.DirectiveReport{}, "", rtdberrors.New(rtdberrors.CodeBadRequest,
			"changed field '"+t.Table+"."+t.Field+"' does not exist")
	}
	if !castValidFor(t.Cast, oldTy) {
		return admin.DirectiveReport{}, "", rtdberrors.New(rtdberrors.CodeBadRequest,
			"cast "+string(t.Cast)+" is not valid for "+t.Table+"."+t.Field)
	}
	newTy, err := parseFieldType(t.To)
	if err != nil {
		return admin.DirectiveReport{}, "", err
	}
	var rows []rowKey
	for key := range s.docs {
		if key.Table == t.Table {
			rows = append(rows, key)
		}
	}
	sort.Slice(rows, func(a, b int) bool { return rows[a].ID < rows[b].ID })
	affected := int64(0)
	for _, key := range rows {
		row := s.docs[key]
		original, present := row.Doc[t.Field]
		if !present {
			continue
		}
		affected++
		if coerced := coerceValue(t.Cast, original); coerced != nil {
			row.Doc[t.Field] = coerced
			continue
		}
		if t.Default != nil {
			fallback := coerceValue(t.Cast, t.Default)
			if fallback == nil {
				fallback = t.Default
			}
			row.Doc[t.Field] = fallback
			continue
		}
		return admin.DirectiveReport{}, "", rtdberrors.New(rtdberrors.CodeBadRequest,
			"changeType cannot coerce value in "+t.Table+"."+row.ID+
				" ("+canonical(original)+") and no default given")
	}
	td.Fields[t.Field] = *newTy
	return admin.DirectiveReport{Op: "changeType", AffectedRows: affected}, t.Table, nil
}

func applyDropField(s *Store, planned **SchemaDef, table, field string) (admin.DirectiveReport, string, error) {
	if err := requireMigrateTable(*planned, table); err != nil {
		return admin.DirectiveReport{}, "", err
	}
	td := (*planned).Tables[table]
	if _, ok := td.Fields[field]; !ok {
		return admin.DirectiveReport{}, "", rtdberrors.New(rtdberrors.CodeBadRequest,
			"dropped field '"+table+"."+field+"' does not exist")
	}
	delete(td.Fields, field)
	for i := range td.Indexes {
		kept := td.Indexes[i].Fields[:0]
		for _, f := range td.Indexes[i].Fields {
			if f != field {
				kept = append(kept, f)
			}
		}
		td.Indexes[i].Fields = kept
	}
	if td.OwnerField != nil && *td.OwnerField == field {
		td.OwnerField = nil
	}
	if td.CollaboratorsField != nil && *td.CollaboratorsField == field {
		td.CollaboratorsField = nil
	}
	// ENH-028: a computed expression reading the dropped field would dangle —
	// reject, naming the computed field, so the caller amends the map first.
	var computedOffender string
	for name, expr := range td.Computed {
		referenced := false
		walkValueExprFields(expr, func(name2 string) {
			if name2 == field {
				referenced = true
			}
		})
		if referenced {
			computedOffender = name
			break
		}
	}
	if computedOffender != "" {
		return admin.DirectiveReport{}, "", rtdberrors.New(rtdberrors.CodeBadRequest,
			"cannot drop field '"+table+"."+field+"': it is referenced by computed field '"+
				table+"."+computedOffender+"'; drop the computed field first")
	}
	delete(td.Computed, field)
	affected := int64(0)
	for key, row := range s.docs {
		if key.Table != table {
			continue
		}
		if _, present := row.Doc[field]; present {
			delete(row.Doc, field)
			affected++
		}
	}
	return admin.DirectiveReport{Op: "dropField", AffectedRows: affected}, table, nil
}

func applyDropTable(s *Store, planned **SchemaDef, name string) (admin.DirectiveReport, string, error) {
	if err := requireMigrateTable(*planned, name); err != nil {
		return admin.DirectiveReport{}, "", err
	}
	count := int64(0)
	for key := range s.docs {
		if key.Table == name {
			count++
		}
	}
	delete((*planned).Tables, name)
	for key := range s.docs {
		if key.Table == name {
			delete(s.docs, key)
		}
	}
	return admin.DirectiveReport{Op: "dropTable", AffectedRows: count}, name, nil
}

func applyDropIndex(s *Store, planned **SchemaDef, table, name string) (admin.DirectiveReport, string, error) {
	if err := requireMigrateTable(*planned, table); err != nil {
		return admin.DirectiveReport{}, "", err
	}
	td := (*planned).Tables[table]
	found := false
	for _, idx := range td.Indexes {
		if idx.Name == name {
			found = true
			break
		}
	}
	if !found {
		return admin.DirectiveReport{}, "", rtdberrors.New(rtdberrors.CodeBadRequest,
			"dropped index '"+table+"."+name+"' does not exist")
	}
	kept := td.Indexes[:0]
	for _, idx := range td.Indexes {
		if idx.Name != name {
			kept = append(kept, idx)
		}
	}
	td.Indexes = kept
	return admin.DirectiveReport{Op: "dropIndex", AffectedRows: 0}, table, nil
}

func applySetDefault(s *Store, planned **SchemaDef, table, field string, value wire.JSONValue) (admin.DirectiveReport, string, error) {
	if err := requireMigrateTable(*planned, table); err != nil {
		return admin.DirectiveReport{}, "", err
	}
	td := (*planned).Tables[table]
	if _, ok := td.Fields[field]; !ok {
		return admin.DirectiveReport{}, "", rtdberrors.New(rtdberrors.CodeBadRequest,
			"setDefault target '"+table+"."+field+"' does not exist")
	}
	affected := int64(0)
	for key, row := range s.docs {
		if key.Table != table {
			continue
		}
		if _, present := row.Doc[field]; !present {
			row.Doc[field] = value
			affected++
		}
	}
	return admin.DirectiveReport{Op: "setDefault", AffectedRows: affected}, table, nil
}

// schemaToWire renders the typed schema back to its wire JSON shape (the
// inverse of parseSchema) for MigrateResult.schema.
func schemaToWire(schema *SchemaDef) (wire.JSONValue, error) {
	tables := wire.Object{}
	names := make([]string, 0, len(schema.Tables))
	for name := range schema.Tables {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		td, err := tableDefToWire(schema.Tables[name])
		if err != nil {
			return nil, err
		}
		tables[name] = td
	}
	return wire.Object{"tables": tables}, nil
}

func tableDefToWire(td *TableDef) (wire.JSONValue, error) {
	out := wire.Object{}
	fields := wire.Object{}
	for name, fty := range td.Fields {
		fv, err := fieldTypeToWire(fty)
		if err != nil {
			return nil, err
		}
		fields[name] = fv
	}
	out["fields"] = fields
	if len(td.Indexes) > 0 {
		indexes := wire.Array{}
		for _, idx := range td.Indexes {
			io := wire.Object{"name": wire.String(idx.Name), "fields": strArrayWire(idx.Fields)}
			if idx.Search {
				io["search"] = wire.Bool(true)
			}
			if idx.Unique {
				io["unique"] = wire.Bool(true)
			}
			if idx.Vector != nil {
				vo := wire.Object{"dimensions": wire.Number(formatI64(int64(idx.Vector.Dimensions)))}
				if len(idx.Vector.FilterFields) > 0 {
					vo["filterFields"] = strArrayWire(idx.Vector.FilterFields)
				}
				if idx.Vector.Metric != "" && idx.Vector.Metric != "cosine" {
					vo["metric"] = wire.String(idx.Vector.Metric)
				}
				io["vector"] = vo
			}
			if idx.WhereClause != nil {
				fv, err := filterToJSONValue(idx.WhereClause)
				if err != nil {
					return nil, err
				}
				io["where"] = fv
			}
			if idx.Language != nil {
				io["language"] = wire.String(*idx.Language)
			}
			indexes = append(indexes, io)
		}
		out["indexes"] = indexes
	}
	if td.OwnerField != nil {
		out["ownerField"] = wire.String(*td.OwnerField)
	}
	if td.CollaboratorsField != nil {
		out["collaboratorsField"] = wire.String(*td.CollaboratorsField)
	}
	if td.UpdatedAtField != nil {
		out["updatedAtField"] = wire.String(*td.UpdatedAtField)
	}
	if td.AutoIncrementField != nil {
		out["autoIncrementField"] = wire.String(*td.AutoIncrementField)
	}
	if td.TTL != nil {
		to := wire.Object{"field": wire.String(td.TTL.Field)}
		if td.TTL.DefaultDurationMs != nil {
			to["defaultDurationMs"] = wire.Number(formatI64(*td.TTL.DefaultDurationMs))
		}
		out["ttl"] = to
	}
	if td.Authorize != nil {
		fv, err := filterToJSONValue(td.Authorize)
		if err != nil {
			return nil, err
		}
		out["authorize"] = fv
	}
	if len(td.Defaults) > 0 {
		defaults := wire.Object{}
		for k, v := range td.Defaults {
			defaults[k] = v
		}
		out["defaults"] = defaults
	}
	if len(td.Computed) > 0 {
		computed := wire.Object{}
		for k, v := range td.Computed {
			cv, err := valueExprToJSONValue(v)
			if err != nil {
				return nil, err
			}
			computed[k] = cv
		}
		out["computed"] = computed
	}
	if td.SoftDelete {
		out["softDelete"] = wire.Bool(true)
	}
	return out, nil
}

func fieldTypeToWire(fty FieldType) (wire.JSONValue, error) {
	out := wire.Object{"type": wire.String(fty.Kind)}
	switch fty.Kind {
	case "id":
		out["table"] = wire.String(fty.Table)
		if fty.OnDelete != nil {
			out["onDelete"] = wire.String(string(*fty.OnDelete))
		}
	case "literal":
		out["value"] = fty.Value
	case "optional":
		v, err := fieldTypeToWire(*fty.Inner)
		if err != nil {
			return nil, err
		}
		out["inner"] = v
	case "array":
		v, err := fieldTypeToWire(*fty.Inner)
		if err != nil {
			return nil, err
		}
		out["element"] = v
	case "record":
		v, err := fieldTypeToWire(*fty.Inner)
		if err != nil {
			return nil, err
		}
		out["value"] = v
	case "union":
		variants := wire.Array{}
		for _, v := range fty.Variants {
			vv, err := fieldTypeToWire(v)
			if err != nil {
				return nil, err
			}
			variants = append(variants, vv)
		}
		out["variants"] = variants
	case "object":
		fields := wire.Object{}
		for k, v := range fty.Fields {
			vv, err := fieldTypeToWire(v)
			if err != nil {
				return nil, err
			}
			fields[k] = vv
		}
		out["fields"] = fields
	case "vector":
		out["dimensions"] = wire.Number(formatI64(int64(fty.Dimensions)))
	}
	return out, nil
}

func strArrayWire(ss []string) wire.Array {
	out := make(wire.Array, len(ss))
	for i, s := range ss {
		out[i] = wire.String(s)
	}
	return out
}
