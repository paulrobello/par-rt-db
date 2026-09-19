// Cursor keyset pagination — the Go port of rust in_memory/query.rs's
// paginate half (ts paginateResult): one cursor value per sort column, the
// OR-of-AND resume predicate, and the {docs, nextCursor?} envelope. The
// ranked-terminal paging (search/vectorSearch) shares the same codec and
// predicate.
package inmemory

import (
	"sort"

	"github.com/paulrobello/par-rt-db/go-client/dsl"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// sortCol is one keyset sort column: an indexed field or a tiebreaker.
type sortCol struct {
	field string // empty for the synthetic columns
	kind  string // "index" | "createdAt" | "id"
}

// executePaginateTerminal pages the sorted btree result over [unbound index
// fields..., _creationTime, _id].
func executePaginateTerminal(pag *wire.Paginate, table *TableDef, filtered []*StoredRow, plan *scanPlan, dir wire.Order) (wire.JSONValue, error) {
	cols := []sortCol{}
	if plan.indexDef != nil {
		for _, f := range plan.indexDef.Fields[len(plan.typedEq):] {
			cols = append(cols, sortCol{field: f, kind: "index"})
		}
	}
	cols = append(cols, sortCol{kind: "createdAt"}, sortCol{kind: "id"})
	colTypes := make([]PgType, len(cols))
	for i, c := range cols {
		switch c.kind {
		case "index":
			colTypes[i] = plan.fieldPg(table, c.field)
		case "createdAt":
			colTypes[i] = PgNumber
		default:
			colTypes[i] = PgText
		}
	}
	return paginateResult(pag, table, filtered, cols, colTypes, dir)
}

func paginateResult(pag *wire.Paginate, table *TableDef, sorted []*StoredRow, cols []sortCol, colTypes []PgType, dir wire.Order) (wire.JSONValue, error) {
	numItems := pag.NumItems
	if numItems > maxTake {
		numItems = maxTake
	}
	var cursorValues []wire.JSONValue
	if pag.Cursor != nil {
		decoded, err := decodeCursorForEngine(*pag.Cursor)
		if err != nil {
			return nil, err
		}
		if len(decoded) != len(cols) {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
				"cursor has "+formatI64(int64(len(decoded)))+" value(s) but this query sorts over "+
					formatI64(int64(len(cols)))+" column(s)")
		}
		if err := validateCursorValues(decoded, cols, table); err != nil {
			return nil, err
		}
		cursorValues = decoded
	}
	rows := sorted
	if cursorValues != nil {
		rows = nil
		for _, row := range sorted {
			if isAfterCursor(row, cursorValues, cols, colTypes, dir) {
				rows = append(rows, row)
			}
		}
	}
	hasNext := len(rows) > numItems
	page := rows
	if len(page) > numItems {
		page = page[:numItems]
	}
	docs := make(wire.Array, 0, len(page))
	for _, row := range page {
		docs = append(docs, mergeDoc(row))
	}
	out := wire.Object{"docs": docs}
	if hasNext && len(page) > 0 {
		last := page[len(page)-1]
		keyset := make([]wire.JSONValue, 0, len(cols))
		for _, c := range cols {
			keyset = append(keyset, sortValue(last, c))
		}
		encoded, err := encodeCursorForEngine(keyset)
		if err != nil {
			return nil, err
		}
		out["nextCursor"] = wire.String(encoded)
	}
	return out, nil
}

// decodeCursorForEngine wraps the dsl codec's INTERNAL-shaped errors into the
// engine's BAD_REQUEST surface.
func decodeCursorForEngine(s string) ([]wire.JSONValue, error) {
	values, err := dsl.DecodeCursor(s)
	if err != nil {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "invalid cursor: "+err.Error())
	}
	return values, nil
}

func encodeCursorForEngine(values []wire.JSONValue) (string, error) {
	encoded, err := dsl.EncodeCursor(values)
	if err != nil {
		return "", rtdberrors.New(rtdberrors.CodeBadRequest, "invalid cursor: "+err.Error())
	}
	return encoded, nil
}

func validateCursorValues(values []wire.JSONValue, cols []sortCol, table *TableDef) error {
	for i, c := range cols {
		v := values[i]
		switch c.kind {
		case "index":
			if !isNullValue(v) {
				if _, err := coerceIndexValue(table, c.field, v); err != nil {
					return err
				}
			}
		case "createdAt":
			if _, ok := v.(wire.Number); !ok {
				return rtdberrors.New(rtdberrors.CodeBadRequest, "cursor value for created_at must be a number")
			}
		default:
			if _, ok := v.(wire.String); !ok {
				return rtdberrors.New(rtdberrors.CodeBadRequest, "cursor value for id must be a string")
			}
		}
	}
	return nil
}

func isAfterCursor(row *StoredRow, cursorValues []wire.JSONValue, cols []sortCol, colTypes []PgType, dir wire.Order) bool {
	rowValues := make([]wire.JSONValue, len(cols))
	for i, c := range cols {
		rowValues[i] = sortValue(row, c)
	}
	return keysetIsAfter(rowValues, cursorValues, colTypes, dir)
}

// keysetIsAfter is the resume predicate: strictly-after in the sort
// direction, lexicographic OR-of-AND over the columns.
func keysetIsAfter(rowValues, cursorValues []wire.JSONValue, colTypes []PgType, dir wire.Order) bool {
	for i := range rowValues {
		prefixEqual := true
		for j := 0; j < i; j++ {
			if compareIndexValues(rowValues[j], cursorValues[j], colTypes[j]) != cmpEqual {
				prefixEqual = false
				break
			}
		}
		if !prefixEqual {
			continue
		}
		c := compareIndexValues(rowValues[i], cursorValues[i], colTypes[i])
		ahead := c == cmpGreater
		if dir == wire.OrderDesc {
			ahead = c == cmpLess
		}
		if ahead {
			return true
		}
	}
	return false
}

func sortValue(row *StoredRow, c sortCol) wire.JSONValue {
	switch c.kind {
	case "createdAt":
		return wire.Number(formatI64(row.CreatedAt))
	case "id":
		return wire.String(row.ID)
	default:
		if v, present := row.Doc[c.field]; present {
			return v
		}
		return wire.Null{}
	}
}

// rankedRow is one ranked candidate (search/vectorSearch paging): the
// synthetic rank, the tiebreakers, and the merged doc.
type rankedRow struct {
	rank      wire.JSONValue // nil = no rank
	createdAt int64
	id        string
	doc       wire.Object
}

func newRankedRow(rank wire.JSONValue, doc wire.Object) *rankedRow {
	r := &rankedRow{rank: rank, doc: doc}
	if v, ok := doc["_creationTime"].(wire.Number); ok {
		if n, ok := parseI64(string(v)); ok {
			r.createdAt = n
		}
	}
	if v, ok := doc["_id"].(wire.String); ok {
		r.id = string(v)
	}
	return r
}

func (r *rankedRow) keyset(hasRank bool) []wire.JSONValue {
	out := make([]wire.JSONValue, 0, 3)
	if hasRank {
		if r.rank == nil {
			out = append(out, wire.Null{})
		} else {
			out = append(out, r.rank)
		}
	}
	out = append(out, wire.Number(formatI64(r.createdAt)), wire.String(r.id))
	return out
}

// sortRankedRows orders by rank desc, then _creationTime desc, then _id desc.
func sortRankedRows(rows []*rankedRow) {
	sort.SliceStable(rows, func(a, b int) bool {
		x, y := rows[a], rows[b]
		if c := compareIndexValues(x.rank, y.rank, PgNumber); c != cmpEqual {
			return c == cmpGreater
		}
		if x.createdAt != y.createdAt {
			return x.createdAt > y.createdAt
		}
		return x.id > y.id
	})
}

// paginateRanked pages an already-ranked candidate list over
// [rank?, created_at, id], all desc. hasRank selects the keyset shape.
func paginateRanked(pag *wire.Paginate, rows []*rankedRow, hasRank bool) (wire.JSONValue, error) {
	numItems := pag.NumItems
	if numItems > maxTake {
		numItems = maxTake
	}
	colTypes := []PgType{PgNumber, PgNumber, PgText}
	if !hasRank {
		colTypes = []PgType{PgNumber, PgText}
	}
	var cursorValues []wire.JSONValue
	if pag.Cursor != nil {
		decoded, err := decodeCursorForEngine(*pag.Cursor)
		if err != nil {
			return nil, err
		}
		if len(decoded) != len(colTypes) {
			return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
				"cursor has "+formatI64(int64(len(decoded)))+" value(s) but this query sorts over "+
					formatI64(int64(len(colTypes)))+" column(s)")
		}
		if err := validateRankedCursorValues(decoded, hasRank); err != nil {
			return nil, err
		}
		cursorValues = decoded
	}
	var page []*rankedRow
	for _, row := range rows {
		if cursorValues != nil && !keysetIsAfter(row.keyset(hasRank), cursorValues, colTypes, wire.OrderDesc) {
			continue
		}
		page = append(page, row)
		if len(page) > numItems {
			break
		}
	}
	hasNext := len(page) > numItems
	if len(page) > numItems {
		page = page[:numItems]
	}
	docs := make(wire.Array, 0, len(page))
	for _, row := range page {
		docs = append(docs, row.doc)
	}
	out := wire.Object{"docs": docs}
	if hasNext && len(page) > 0 {
		encoded, err := encodeCursorForEngine(page[len(page)-1].keyset(hasRank))
		if err != nil {
			return nil, err
		}
		out["nextCursor"] = wire.String(encoded)
	}
	return out, nil
}

func validateRankedCursorValues(values []wire.JSONValue, hasRank bool) error {
	tail := 0
	if hasRank {
		if _, ok := values[0].(wire.Number); !ok {
			return rtdberrors.New(rtdberrors.CodeBadRequest, "cursor value for rank must be a number")
		}
		tail = 1
	}
	if _, ok := values[tail].(wire.Number); !ok {
		return rtdberrors.New(rtdberrors.CodeBadRequest, "cursor value for created_at must be a number")
	}
	if _, ok := values[tail+1].(wire.String); !ok {
		return rtdberrors.New(rtdberrors.CodeBadRequest, "cursor value for id must be a string")
	}
	return nil
}
