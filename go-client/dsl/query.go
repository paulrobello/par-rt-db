// go-client/dsl/query.go
package dsl

// Mirrors rust-client/src/query.rs::Query builder — a chainable value-type
// builder producing wire.Query.

import "github.com/paulrobello/par-rt-db/go-client/wire"

// TableQuery builds a wire.Query fluently.
type TableQuery struct {
	q wire.Query
}

// NewTableQuery starts a query on a table.
func NewTableQuery(table string) TableQuery {
	return TableQuery{q: wire.Query{Table: table}}
}

// Get targets a point read by id; excludes all range clauses.
func (q TableQuery) Get(id string) TableQuery {
	q.q.Get = &id
	return q
}

// WithIndex selects the index and binds the eq prefix values.
func (q TableQuery) WithIndex(name string, eq ...wire.JSONValue) TableQuery {
	q.q.Index = &name
	q.q.Eq = append(q.q.Eq, eq...)
	return q
}

// Eq appends one eq-prefix bind.
func (q TableQuery) Eq(v wire.JSONValue) TableQuery {
	q.q.Eq = append(q.q.Eq, v)
	return q
}

// Gt sets an exclusive lower bound.
func (q TableQuery) Gt(v wire.JSONValue) TableQuery {
	q.q.Gt = &v
	return q
}

// Gte sets an inclusive lower bound (mutually exclusive with Gt).
func (q TableQuery) Gte(v wire.JSONValue) TableQuery {
	q.q.Gte = &v
	return q
}

// Lt sets an exclusive upper bound.
func (q TableQuery) Lt(v wire.JSONValue) TableQuery {
	q.q.Lt = &v
	return q
}

// Lte sets an inclusive upper bound (mutually exclusive with Lt).
func (q TableQuery) Lte(v wire.JSONValue) TableQuery {
	q.q.Lte = &v
	return q
}

// OrderAsc orders ascending (the server default; explicit on the wire).
func (q TableQuery) OrderAsc() TableQuery {
	o := wire.OrderAsc
	q.q.Order = &o
	return q
}

// OrderDesc orders descending.
func (q TableQuery) OrderDesc() TableQuery {
	o := wire.OrderDesc
	q.q.Order = &o
	return q
}

// Take caps the result set.
func (q TableQuery) Take(n int) TableQuery {
	q.q.Take = &n
	return q
}

// Unique reads a single row via a unique index.
func (q TableQuery) Unique() TableQuery {
	q.q.Unique = true
	return q
}

// First returns Doc(Some)/Doc(None).
func (q TableQuery) First() TableQuery {
	q.q.First = true
	return q
}

// Count is the count terminal.
func (q TableQuery) Count() TableQuery {
	q.q.Count = true
	return q
}

// Distinct is the distinct terminal.
func (q TableQuery) Distinct() TableQuery {
	q.q.Distinct = true
	return q
}

// Aggregate is the aggregate terminal; groupBy shifts to grouped rows.
func (q TableQuery) Aggregate(op wire.AggregateOp, groupBy bool) TableQuery {
	q.q.Aggregate = &wire.AggregateSpec{Op: op, GroupBy: groupBy}
	return q
}

// PaginateCursor enables pagination with a prior cursor ("" = first page).
func (q TableQuery) PaginateCursor(cursor string, numItems int) TableQuery {
	p := wire.Paginate{NumItems: numItems}
	if cursor != "" {
		p.Cursor = &cursor
	}
	q.q.Paginate = &p
	return q
}

// Filter appends the db-side predicate.
func (q TableQuery) Filter(f wire.FilterExpr) TableQuery {
	q.q.Filter = f
	return q
}

// Search is the full-text search terminal.
func (q TableQuery) Search(s wire.SearchQuery) TableQuery {
	q.q.Search = &s
	return q
}

// Fields projects the named user fields.
func (q TableQuery) Fields(fields ...string) TableQuery {
	q.q.Fields = fields
	return q
}

// VectorSearch is the vector-similarity terminal.
func (q TableQuery) VectorSearch(v wire.VectorSearchQuery) TableQuery {
	q.q.VectorSearch = &v
	return q
}

// HybridSearch is the RRF-fused text+vector terminal.
func (q TableQuery) HybridSearch(h wire.HybridSearchQuery) TableQuery {
	q.q.HybridSearch = &h
	return q
}

// Build returns the wire shape (shallow copy).
func (q TableQuery) Build() wire.Query {
	return q.q
}
