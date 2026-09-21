// The read evaluator — the Go port of rust in_memory/query.rs's run_query
// and its terminal executors (search/vector/hybrid terminals land with the
// search task). Ports the ts/python/rust engines' shared ScanPlan shape.
package inmemory

import (
	"math/big"
	"sort"
	"strconv"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// EvalQuery is the single query entry the client surface and corpus harness
// call. Returns the terminal result as a JSONValue: get/first/unique → merged
// doc or null; count → number; take/collect → array; paginate → {docs,
// nextCursor?}. Filter validation runs once before any row is touched.
func EvalQuery(s *Store, q wire.Query) (wire.JSONValue, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return evalQueryLocked(s, q)
}

func evalQueryLocked(s *Store, q wire.Query) (wire.JSONValue, error) {
	table, err := requireTable(s, q.Table)
	if err != nil {
		return nil, err
	}
	// Projection validation runs before every early return — the same
	// compile-time gate the server runs in compile_query.
	if q.Fields != nil {
		if err := validateProjection(table, q.Fields); err != nil {
			return nil, err
		}
	}
	result, err := executeQuery(s, &q, table)
	if err != nil {
		return nil, err
	}
	if q.Fields != nil {
		result = projectResult(result, &q)
	}
	return result, nil
}

func requireTable(s *Store, name string) (*TableDef, error) {
	td, ok := s.tables[name]
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeNotFound, "table '"+name+"' does not exist")
	}
	return td, nil
}

func executeQuery(s *Store, q *wire.Query, table *TableDef) (wire.JSONValue, error) {
	hasRange := q.Gt != nil || q.Gte != nil || q.Lt != nil || q.Lte != nil

	if err := checkQueryCombinations(q); err != nil {
		return nil, err
	}
	if q.Take != nil && *q.Take > maxTake {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"take exceeds maximum of "+strconv.Itoa(maxTake))
	}

	// get terminal — exclusive of every other clause. Direct row lookup:
	// the engine already holds s.mu here (Store.Get re-locks and would
	// self-deadlock on the non-reentrant mutex).
	if q.Get != nil {
		row, ok := s.docs[rowKey{Table: q.Table, ID: *q.Get}]
		if !ok || row.DeletedAt != nil {
			return wire.Null{}, nil
		}
		return mergeDoc(row), nil
	}

	if q.VectorSearch != nil {
		return executeVectorSearchTerminal(s, q, q.VectorSearch, table)
	}
	if q.HybridSearch != nil {
		return executeHybridSearchTerminal(s, q, q.HybridSearch)
	}
	if q.Search != nil {
		return executeSearchTerminal(s, q, q.Search, table)
	}

	plan, err := prepareScan(q, table, hasRange)
	if err != nil {
		return nil, err
	}
	filtered := fetchFilteredRows(s, q, plan, table.Fields)

	if q.Count {
		return wire.Number(formatI64(int64(len(filtered)))), nil
	}
	if q.Distinct {
		return executeDistinctTerminal(table, plan, filtered)
	}
	if q.Aggregate != nil {
		return executeAggregateTerminal(q.Aggregate, table, plan, filtered)
	}

	dir := wire.OrderAsc
	if q.Order != nil {
		dir = *q.Order
	}
	sortFilteredRows(filtered, table, plan, dir)

	if q.Paginate != nil {
		return executePaginateTerminal(q.Paginate, table, filtered, plan, dir)
	}
	return executeCollectTerminal(q, filtered)
}

// fetchFilteredRows runs eq prefix → range → filter over the live rows of the
// table (soft-deleted rows are absent to every read terminal).
func fetchFilteredRows(s *Store, q *wire.Query, plan *scanPlan, fields map[string]FieldType) []*StoredRow {
	var filtered []*StoredRow
	for key, row := range s.docs {
		if key.Table != q.Table || row.DeletedAt != nil {
			continue
		}
		if plan.indexDef != nil {
			ok := true
			for i, tv := range plan.typedEq {
				v, present := row.Doc[plan.indexDef.Fields[i]]
				if !present || isNullValue(v) || !jsonEq(v, tv) {
					ok = false
					break
				}
			}
			if !ok {
				continue
			}
		}
		if plan.rangeField != "" {
			v, present := row.Doc[plan.rangeField]
			if !present || isNullValue(v) {
				continue
			}
			if plan.gt != nil && compareIndexValues(v, plan.gt, plan.rangeFieldPg) != cmpGreater {
				continue
			}
			if plan.gte != nil && compareIndexValues(v, plan.gte, plan.rangeFieldPg) == cmpLess {
				continue
			}
			if plan.lt != nil && compareIndexValues(v, plan.lt, plan.rangeFieldPg) != cmpLess {
				continue
			}
			if plan.lte != nil && compareIndexValues(v, plan.lte, plan.rangeFieldPg) == cmpGreater {
				continue
			}
		}
		if q.Filter != nil && !matchesFilter(q.Filter, row.Doc, fields) {
			continue
		}
		filtered = append(filtered, row)
	}
	return filtered
}

func executeCollectTerminal(q *wire.Query, filtered []*StoredRow) (wire.JSONValue, error) {
	if q.Unique {
		if len(filtered) > 1 {
			return nil, rtdberrors.New(rtdberrors.CodePreconditionFailed,
				"unique query matched multiple documents")
		}
		if len(filtered) == 0 {
			return wire.Null{}, nil
		}
		return mergeDoc(filtered[0]), nil
	}
	if q.First {
		if len(filtered) == 0 {
			return wire.Null{}, nil
		}
		return mergeDoc(filtered[0]), nil
	}
	limit := maxTake
	if q.Take != nil {
		limit = *q.Take
	}
	if limit > maxTake {
		limit = maxTake
	}
	out := make(wire.Array, 0, len(filtered))
	for i, row := range filtered {
		if i >= limit {
			break
		}
		out = append(out, mergeDoc(row))
	}
	return out, nil
}

// scanPlan carries everything the row scan needs besides the query itself.
type scanPlan struct {
	indexDef     *IndexDef
	typedEq      []wire.JSONValue
	rangeField   string
	rangeFieldPg PgType
	gt, gte      wire.JSONValue
	lt, lte      wire.JSONValue
}

// prepareScan resolves the index, binds the eq prefix, coerces the range
// bounds, and validates the filter once — everything before a row is touched.
func prepareScan(q *wire.Query, table *TableDef, hasRange bool) (*scanPlan, error) {
	plan := &scanPlan{}
	var err error
	if q.Index != nil {
		plan.indexDef, err = requireIndex(table, *q.Index)
		if err != nil {
			return nil, err
		}
	} else if len(q.Eq) > 0 {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "eq requires an index")
	}
	if plan.indexDef != nil && len(q.Eq) > len(plan.indexDef.Fields) {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"index '"+plan.indexDef.Name+"' expects at most "+
				strconv.Itoa(len(plan.indexDef.Fields))+" eq value(s), got "+strconv.Itoa(len(q.Eq)))
	}
	if plan.indexDef != nil {
		for i, value := range q.Eq {
			tv, err := coerceIndexValue(table, plan.indexDef.Fields[i], value)
			if err != nil {
				return nil, err
			}
			plan.typedEq = append(plan.typedEq, tv)
		}
	}
	if hasRange {
		if plan.indexDef == nil {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "range bound requires an index")
		}
		if len(q.Eq) >= len(plan.indexDef.Fields) {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
				"range bound requires a remaining index field after eq")
		}
		plan.rangeField = plan.indexDef.Fields[len(q.Eq)]
		plan.rangeFieldPg = plan.fieldPg(table, plan.rangeField)
		bind := func(v *wire.JSONValue) (wire.JSONValue, error) {
			if v == nil {
				return nil, nil
			}
			return coerceIndexValue(table, plan.rangeField, *v)
		}
		if plan.gt, err = bind(q.Gt); err != nil {
			return nil, err
		}
		if plan.gte, err = bind(q.Gte); err != nil {
			return nil, err
		}
		if plan.lt, err = bind(q.Lt); err != nil {
			return nil, err
		}
		if plan.lte, err = bind(q.Lte); err != nil {
			return nil, err
		}
	}
	if q.Filter != nil {
		if err := validateFilterExpr(q.Filter, table); err != nil {
			return nil, err
		}
	}
	return plan, nil
}

// fieldPg resolves a field's indexed storage type (Text when undeclared or
// non-indexable — the defensive fallback the other engines share).
func (p *scanPlan) fieldPg(table *TableDef, field string) PgType {
	fty, ok := table.Fields[field]
	if !ok {
		return PgText
	}
	it, err := indexColumnType(fty)
	if err != nil {
		return PgText
	}
	return it.Pg
}

func requireIndex(table *TableDef, name string) (*IndexDef, error) {
	for i := range table.Indexes {
		if table.Indexes[i].Name == name {
			return &table.Indexes[i], nil
		}
	}
	return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "index '"+name+"' not found")
}

// sortFilteredRows orders the set by unbound index fields after the eq
// prefix, then _creationTime, then _id — a total order via the id tiebreaker.
func sortFilteredRows(filtered []*StoredRow, table *TableDef, plan *scanPlan, dir wire.Order) {
	var sortFields []string
	var sortPgs []PgType
	if plan.indexDef != nil {
		for _, f := range plan.indexDef.Fields[len(plan.typedEq):] {
			sortFields = append(sortFields, f)
			sortPgs = append(sortPgs, plan.fieldPg(table, f))
		}
	}
	sort.SliceStable(filtered, func(a, b int) bool {
		x, y := filtered[a], filtered[b]
		for i, field := range sortFields {
			av, aok := x.Doc[field]
			bv, bok := y.Doc[field]
			if !aok {
				av = wire.Null{}
			}
			if !bok {
				bv = wire.Null{}
			}
			if c := compareIndexValues(av, bv, sortPgs[i]); c != cmpEqual {
				return dirLess(c, dir)
			}
		}
		if x.CreatedAt != y.CreatedAt {
			if x.CreatedAt < y.CreatedAt {
				return dirLess(cmpLess, dir)
			}
			return dirLess(cmpGreater, dir)
		}
		return dirLess(cmpStrings(x.ID, y.ID), dir)
	})
}

// compareOrder is the three-way result of compareIndexValues.
type compareOrder int

const (
	cmpLess compareOrder = iota - 1
	cmpEqual
	cmpGreater
)

func dirLess(c compareOrder, dir wire.Order) bool {
	if dir == wire.OrderDesc {
		return c == cmpGreater
	}
	return c == cmpLess
}

func cmpStrings(a, b string) compareOrder {
	switch {
	case a < b:
		return cmpLess
	case a > b:
		return cmpGreater
	default:
		return cmpEqual
	}
}

func cmpF64(a, b float64) compareOrder {
	switch {
	case a < b:
		return cmpLess
	case a > b:
		return cmpGreater
	default:
		return cmpEqual
	}
}

func cmpI64(a, b int64) compareOrder {
	switch {
	case a < b:
		return cmpLess
	case a > b:
		return cmpGreater
	default:
		return cmpEqual
	}
}

// compareIndexValues is the null-sorts-last comparison for one index sort
// key: numbers numerically, strings lexicographically, booleans false<true;
// int64 columns parse the decimal strings so they order numerically. Mixed
// kinds compare equal (single-type columns by schema).
func compareIndexValues(a, b wire.JSONValue, pg PgType) compareOrder {
	aNull := isNullValue(a)
	bNull := isNullValue(b)
	if aNull && bNull {
		return cmpEqual
	}
	if aNull {
		return cmpGreater
	}
	if bNull {
		return cmpLess
	}
	if pg == PgInt64 {
		as, _ := a.(wire.String)
		bs, _ := b.(wire.String)
		an, aok := parseI64(string(as))
		bn, bok := parseI64(string(bs))
		if !aok {
			an = -9223372036854775808
		}
		if !bok {
			bn = -9223372036854775808
		}
		return cmpI64(an, bn)
	}
	switch av := a.(type) {
	case wire.Number:
		bv, ok := b.(wire.Number)
		if !ok {
			return cmpEqual
		}
		return cmpF64(jsonNumberF64(av), jsonNumberF64(bv))
	case wire.String:
		bv, ok := b.(wire.String)
		if !ok {
			return cmpEqual
		}
		return cmpStrings(string(av), string(bv))
	case wire.Bool:
		bv, ok := b.(wire.Bool)
		if !ok {
			return cmpEqual
		}
		ab, bb := bool(av), bool(bv)
		switch {
		case !ab && bb:
			return cmpLess
		case ab && !bb:
			return cmpGreater
		default:
			return cmpEqual
		}
	default:
		return cmpEqual
	}
}

// projectionSystemFields are always included; listing one is a no-op.
var projectionSystemFields = [3]string{"_id", "_creationTime", "_version"}

func validateProjection(table *TableDef, fields []string) error {
	for _, name := range fields {
		if name == "_id" || name == "_creationTime" || name == "_version" {
			continue
		}
		if _, declared := table.Fields[name]; declared {
			continue
		}
		return rtdberrors.New(rtdberrors.CodeBadRequest, "unknown projection field '"+name+"'")
	}
	return nil
}

// mapResultDocs applies f to every doc-bearing result shape, per the query's
// terminal — never sniffed from the value (a grouped aggregate's rows must
// not be mistaken for docs). Doc-less terminals pass through.
func mapResultDocs(q *wire.Query, result wire.JSONValue, f func(wire.Object)) {
	if q.Count || q.Distinct || q.Aggregate != nil {
		return
	}
	if q.Paginate != nil {
		if obj, ok := result.(wire.Object); ok {
			if docs, ok := obj["docs"].(wire.Array); ok {
				for _, doc := range docs {
					if m, ok := doc.(wire.Object); ok {
						f(m)
					}
				}
			}
		}
		return
	}
	switch t := result.(type) {
	case wire.Object:
		f(t)
	case wire.Array:
		for _, doc := range t {
			if m, ok := doc.(wire.Object); ok {
				f(m)
			}
		}
	}
}

// projectResult applies the fields projection: each doc keeps its
// `_`-prefixed keys (system fields plus synthetics like _searchSnippet) and
// the listed user fields. Sorting and cursors were computed on unprojected
// rows, so cursors still work.
func projectResult(result wire.JSONValue, q *wire.Query) wire.JSONValue {
	if q.Fields == nil {
		return result
	}
	keep := map[string]bool{}
	for _, f := range q.Fields {
		keep[f] = true
	}
	mapResultDocs(q, result, func(doc wire.Object) {
		for k := range doc {
			if stringsHasPrefix(k, "_") || keep[k] {
				continue
			}
			delete(doc, k)
		}
	})
	return result
}

func stringsHasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}

func executeDistinctTerminal(table *TableDef, plan *scanPlan, filtered []*StoredRow) (wire.JSONValue, error) {
	if plan.indexDef == nil || len(plan.typedEq) >= len(plan.indexDef.Fields) {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"distinct requires an index field beyond the eq prefix")
	}
	field := plan.indexDef.Fields[len(plan.typedEq)]
	fieldPg := plan.fieldPg(table, field)
	seen := map[string]bool{}
	var values wire.Array
	for _, row := range filtered {
		v, present := row.Doc[field]
		if !present {
			v = wire.Null{}
		}
		key := canonical(v)
		if !seen[key] {
			seen[key] = true
			values = append(values, v)
		}
	}
	sort.SliceStable(values, func(a, b int) bool {
		return compareIndexValues(values[a], values[b], fieldPg) == cmpLess
	})
	if len(values) > maxTake {
		values = values[:maxTake]
	}
	return values, nil
}

// aggregateOpAlias is one (alias, op) pair to evaluate — a single-`op` spec
// aliases its one entry by the lowercase op name (matches the server/ts
// `aliased_ops` convention; only load-bearing for the wire-v2 result shapes).
type aggregateOpAlias struct {
	alias string
	op    wire.AggregateOp
}

// isAggAliasChar mirrors the server's/ts's `[A-Za-z0-9_]` alias charset.
func isAggAliasChar(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
}

// executeAggregateTerminal ports ts-client's unified `executeAggregateTerminal`
// (in_memory/query.ts): one code path handles the legacy single-`op` scalar/
// grouped shapes and the wire-v2 `aggregates` map / composite `groupBy`
// shapes, dispatching only at the final result-shape choice.
func executeAggregateTerminal(agg *wire.AggregateSpec, table *TableDef, plan *scanPlan, filtered []*StoredRow) (wire.JSONValue, error) {
	eqLen := len(plan.typedEq)
	// A caller-constructed AggregateSpec (not decoded from JSON) may leave
	// GroupBy nil; normalize to the legacy default the wire form always
	// carries (mirrors AggregateSpec.MarshalJSON's nil -> GroupByBool(false)).
	groupBy := agg.GroupBy
	if groupBy == nil {
		groupBy = wire.GroupByBool(false)
	}

	if agg.Op != "" && len(agg.Aggregates) > 0 {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"aggregate op and aggregates are mutually exclusive")
	}
	var ops []aggregateOpAlias
	switch {
	case len(agg.Aggregates) > 0:
		aliases := make([]string, 0, len(agg.Aggregates))
		for alias := range agg.Aggregates {
			aliases = append(aliases, alias)
		}
		sort.Strings(aliases)
		for _, alias := range aliases {
			ops = append(ops, aggregateOpAlias{alias: alias, op: agg.Aggregates[alias]})
		}
	case agg.Op != "":
		ops = []aggregateOpAlias{{alias: string(agg.Op), op: agg.Op}}
	}
	if len(ops) == 0 {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "aggregate requires op or aggregates")
	}
	for _, o := range ops {
		if len(o.alias) == 0 || len(o.alias) > 64 {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
				"aggregates alias must be 1-64 characters")
		}
		for i := 0; i < len(o.alias); i++ {
			if !isAggAliasChar(o.alias[i]) {
				return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
					"aggregates alias may contain only [A-Za-z0-9_]")
			}
		}
	}
	needsField := false
	for _, o := range ops {
		if o.op != wire.AggCount {
			needsField = true
			break
		}
	}
	_, legacyGroupBool := groupBy.(wire.GroupByBool)
	legacy := agg.Op != "" && len(agg.Aggregates) == 0 && legacyGroupBool

	var groupFields []string
	switch gb := groupBy.(type) {
	case wire.GroupByBool:
		if bool(gb) {
			if plan.indexDef == nil || eqLen >= len(plan.indexDef.Fields) {
				return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
					"aggregate groupBy requires an index field beyond the eq prefix")
			}
			groupFields = []string{plan.indexDef.Fields[eqLen]}
		}
	case wire.GroupByFields:
		groupFields = []string(gb)
		if len(groupFields) == 0 {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
				"aggregate groupBy list must not be empty")
		}
	}
	if len(groupFields) > 0 {
		if plan.indexDef == nil {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
				"aggregate groupBy requires an index field beyond the eq prefix")
		}
		for _, field := range groupFields {
			found := false
			for _, f := range plan.indexDef.Fields {
				if f == field {
					found = true
					break
				}
			}
			if !found {
				return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
					"aggregate groupBy field is not a declared index field")
			}
		}
	}

	var aggField string
	if needsField {
		if plan.indexDef == nil {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
				"aggregate requires an index field beyond the eq prefix")
		}
		for _, f := range plan.indexDef.Fields[eqLen:] {
			inGroup := false
			for _, g := range groupFields {
				if g == f {
					inGroup = true
					break
				}
			}
			if !inGroup {
				aggField = f
				break
			}
		}
		if aggField == "" {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
				"aggregate requires an index field beyond the eq prefix")
		}
		aggFieldPg := plan.fieldPg(table, aggField)
		for _, o := range ops {
			if (o.op == wire.AggSum || o.op == wire.AggAvg) && aggFieldPg != PgNumber && aggFieldPg != PgInt64 {
				return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
					"aggregate op "+string(o.op)+" requires a numeric index field")
			}
		}
		if gb, ok := groupBy.(wire.GroupByBool); ok && bool(gb) &&
			eqLen+1 >= len(plan.indexDef.Fields) {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
				"aggregate groupBy requires two index fields beyond the eq prefix")
		}
	}
	aggFieldPg := PgText
	if aggField != "" {
		aggFieldPg = plan.fieldPg(table, aggField)
	}

	evaluate := func(rows []*StoredRow, op wire.AggregateOp) wire.JSONValue {
		if op == wire.AggCount {
			return wire.Number(formatI64(int64(len(rows))))
		}
		var values wire.Array
		for _, row := range rows {
			if v, present := row.Doc[aggField]; present && !isNullValue(v) {
				values = append(values, v)
			}
		}
		if len(values) == 0 {
			return wire.Null{}
		}
		return applyAggregate(op, values, aggFieldPg)
	}

	if len(groupFields) > 0 {
		groupPgs := make([]PgType, len(groupFields))
		for i, f := range groupFields {
			groupPgs[i] = plan.fieldPg(table, f)
		}
		type groupEntry struct {
			keys []wire.JSONValue
			rows []*StoredRow
		}
		var groups []groupEntry
		groupIndex := map[string]int{}
		for _, row := range filtered {
			keys := make([]wire.JSONValue, len(groupFields))
			var keyParts []byte
			for i, f := range groupFields {
				v, present := row.Doc[f]
				if !present {
					v = wire.Null{}
				}
				keys[i] = v
				keyParts = append(keyParts, canonical(v)...)
				keyParts = append(keyParts, 0x1f)
			}
			key := string(keyParts)
			i, seen := groupIndex[key]
			if !seen {
				i = len(groups)
				groupIndex[key] = i
				groups = append(groups, groupEntry{keys: keys})
			}
			groups[i].rows = append(groups[i].rows, row)
		}
		sort.SliceStable(groups, func(a, b int) bool {
			for i := range groupFields {
				c := compareIndexValues(groups[a].keys[i], groups[b].keys[i], groupPgs[i])
				if c != cmpEqual {
					return c == cmpLess
				}
			}
			return false
		})
		if len(groups) > maxTake {
			groups = groups[:maxTake]
		}
		if legacy {
			out := make(wire.Array, 0, len(groups))
			for _, g := range groups {
				out = append(out, wire.Object{"key": g.keys[0], "value": evaluate(g.rows, ops[0].op)})
			}
			return out, nil
		}
		out := make(wire.Array, 0, len(groups))
		for _, g := range groups {
			values := wire.Object{}
			for _, o := range ops {
				values[o.alias] = evaluate(g.rows, o.op)
			}
			out = append(out, wire.Object{"keys": wire.Array(g.keys), "values": values})
		}
		return out, nil
	}
	if legacy {
		return evaluate(filtered, ops[0].op), nil
	}
	out := wire.Object{}
	for _, o := range ops {
		out[o.alias] = evaluate(filtered, o.op)
	}
	return out, nil
}

// applyAggregate applies one aggregate op over a non-empty value set:
// SUM over an int64 field reduces exactly over the decimal strings and
// projects the exact decimal string (the int64 wire convention — the server
// casts OP("col")::text; a JSON number is an IEEE-754 double past 2^53);
// other SUM/AVG reduce numerically (int64 values parsed from decimal
// strings); MIN/MAX pick the extreme per compareIndexValues.
func applyAggregate(op wire.AggregateOp, values wire.Array, pg PgType) wire.JSONValue {
	switch op {
	case wire.AggSum:
		if pg == PgInt64 {
			// math/big is unbounded, matching the server's numeric exactly.
			total := new(big.Int)
			for _, v := range values {
				s, ok := v.(wire.String)
				if !ok {
					return f64Aggregate(op, values, pg)
				}
				n, ok := new(big.Int).SetString(string(s), 10)
				if !ok {
					return f64Aggregate(op, values, pg)
				}
				total.Add(total, n)
			}
			return wire.String(total.String())
		}
		return f64Aggregate(op, values, pg)
	case wire.AggAvg:
		return f64Aggregate(op, values, pg)
	case wire.AggMin, wire.AggMax:
		best := values[0]
		for _, v := range values[1:] {
			c := compareIndexValues(v, best, pg)
			if (op == wire.AggMin && c == cmpLess) || (op == wire.AggMax && c == cmpGreater) {
				best = v
			}
		}
		return best
	default: // count over provided values
		return wire.Number(formatI64(int64(len(values))))
	}
}

// f64Aggregate is the numeric reduction shared by number-field SUM/AVG and
// int64 AVG (a fractional mean has no int64 representation — documented f64
// form).
func f64Aggregate(op wire.AggregateOp, values wire.Array, pg PgType) wire.JSONValue {
	var sum float64
	for _, v := range values {
		sum += numericValue(v, pg)
	}
	result := sum
	if op == wire.AggAvg {
		result = sum / float64(len(values))
	}
	if isInfOrNaN(result) {
		return wire.Null{}
	}
	return jsonNumberFromF64(result)
}

func numericValue(v wire.JSONValue, pg PgType) float64 {
	if pg == PgInt64 {
		s, ok := v.(wire.String)
		if !ok {
			return 0
		}
		f, err := strconv.ParseFloat(string(s), 64)
		if err != nil {
			return 0
		}
		return f
	}
	n, ok := v.(wire.Number)
	if !ok {
		return 0
	}
	return jsonNumberF64(n)
}
