// The read evaluator — the Go port of rust in_memory/query.rs's run_query
// and its terminal executors (search/vector/hybrid terminals land with the
// search task). Ports the ts/python/rust engines' shared ScanPlan shape.
package inmemory

import (
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

func executeAggregateTerminal(agg *wire.AggregateSpec, table *TableDef, plan *scanPlan, filtered []*StoredRow) (wire.JSONValue, error) {
	if plan.indexDef == nil {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"aggregate requires an index field beyond the eq prefix")
	}
	eqLen := len(plan.typedEq)
	// count consumes no aggregate field (server AggregateOp::needs_field).
	if agg.Op == wire.AggCount {
		if agg.GroupBy {
			if eqLen >= len(plan.indexDef.Fields) {
				return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
					"aggregate groupBy requires an index field beyond the eq prefix")
			}
			groupField := plan.indexDef.Fields[eqLen]
			groupPg := plan.fieldPg(table, groupField)
			type groupEntry struct {
				key   wire.JSONValue
				count int64
			}
			var groups []groupEntry
			groupIndex := map[string]int{}
			for _, row := range filtered {
				k, present := row.Doc[groupField]
				if !present {
					k = wire.Null{}
				}
				key := canonical(k)
				i, seen := groupIndex[key]
				if !seen {
					i = len(groups)
					groupIndex[key] = i
					groups = append(groups, groupEntry{key: k})
				}
				groups[i].count++
			}
			out := make(wire.Array, 0, len(groups))
			for _, g := range groups {
				out = append(out, wire.Object{
					"key":   g.key,
					"value": wire.Number(formatI64(g.count)),
				})
			}
			sort.SliceStable(out, func(a, b int) bool {
				return compareIndexValues(out[a].(wire.Object)["key"], out[b].(wire.Object)["key"], groupPg) == cmpLess
			})
			if len(out) > maxTake {
				out = out[:maxTake]
			}
			return out, nil
		}
		return wire.Number(formatI64(int64(len(filtered)))), nil
	}
	var groupField, aggField string
	if agg.GroupBy {
		if eqLen+1 >= len(plan.indexDef.Fields) {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
				"aggregate groupBy requires two index fields beyond the eq prefix")
		}
		groupField = plan.indexDef.Fields[eqLen]
		aggField = plan.indexDef.Fields[eqLen+1]
	} else {
		if eqLen >= len(plan.indexDef.Fields) {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
				"aggregate requires an index field beyond the eq prefix")
		}
		aggField = plan.indexDef.Fields[eqLen]
	}
	aggFieldPg := plan.fieldPg(table, aggField)
	if (agg.Op == wire.AggSum || agg.Op == wire.AggAvg) && aggFieldPg != PgNumber && aggFieldPg != PgInt64 {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"aggregate op "+string(agg.Op)+" requires a numeric index field")
	}
	if groupField != "" {
		groupPg := plan.fieldPg(table, groupField)
		type groupEntry struct {
			key    wire.JSONValue
			values wire.Array
		}
		var groups []groupEntry
		groupIndex := map[string]int{}
		for _, row := range filtered {
			k, present := row.Doc[groupField]
			if !present {
				k = wire.Null{}
			}
			key := canonical(k)
			i, seen := groupIndex[key]
			if !seen {
				i = len(groups)
				groupIndex[key] = i
				groups = append(groups, groupEntry{key: k})
			}
			if v, present := row.Doc[aggField]; present && !isNullValue(v) {
				groups[i].values = append(groups[i].values, v)
			}
		}
		out := make(wire.Array, 0, len(groups))
		for _, g := range groups {
			var value wire.JSONValue = wire.Null{}
			if len(g.values) > 0 {
				value = applyAggregate(agg.Op, g.values, aggFieldPg)
			}
			out = append(out, wire.Object{"key": g.key, "value": value})
		}
		sort.SliceStable(out, func(a, b int) bool {
			return compareIndexValues(out[a].(wire.Object)["key"], out[b].(wire.Object)["key"], groupPg) == cmpLess
		})
		if len(out) > maxTake {
			out = out[:maxTake]
		}
		return out, nil
	}
	var values wire.Array
	for _, row := range filtered {
		if v, present := row.Doc[aggField]; present && !isNullValue(v) {
			values = append(values, v)
		}
	}
	if len(values) == 0 {
		return wire.Null{}, nil
	}
	return applyAggregate(agg.Op, values, aggFieldPg), nil
}

// applyAggregate applies one aggregate op over a non-empty value set:
// SUM/AVG reduce numerically (int64 values parsed from decimal strings);
// MIN/MAX pick the extreme per compareIndexValues.
func applyAggregate(op wire.AggregateOp, values wire.Array, pg PgType) wire.JSONValue {
	switch op {
	case wire.AggSum, wire.AggAvg:
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
