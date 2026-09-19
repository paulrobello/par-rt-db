// go-client/wire/query.go
package wire

// Mirrors server/src/dsl.rs — the read-clause wire vocabulary. Bare keys
// with two explicit camelCase renames (vectorSearch, hybridSearch);
// omitempty only where the server has skip_serializing_if. Interface-typed
// fields decode through the raw-conversion UnmarshalJSON below.

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

// Mirrors server/src/dsl.rs::Order — lowercase values ("asc"/"desc").
type Order string

const (
	OrderAsc  Order = "asc"
	OrderDesc Order = "desc"
)

// Mirrors server/src/dsl.rs::Query — the single declarative read shape.
// Field order follows the server struct. Distinct is a bool; Paginate.cursor
// is optional-omitted; AggregateSpec.groupBy always serializes (server file
// wins over the plan's sketches — ledger R5-R7).
type Query struct {
	Table        string             `json:"table"`
	Get          *string            `json:"get,omitempty"`
	Index        *string            `json:"index,omitempty"`
	Eq           []JSONValue        `json:"eq,omitempty"`
	Gt           *JSONValue         `json:"gt,omitempty"`
	Gte          *JSONValue         `json:"gte,omitempty"`
	Lt           *JSONValue         `json:"lt,omitempty"`
	Lte          *JSONValue         `json:"lte,omitempty"`
	Order        *Order             `json:"order,omitempty"`
	Take         *int               `json:"take,omitempty"`
	Unique       bool               `json:"unique,omitempty"`
	First        bool               `json:"first,omitempty"`
	Count        bool               `json:"count,omitempty"`
	Distinct     bool               `json:"distinct,omitempty"`
	Aggregate    *AggregateSpec     `json:"aggregate,omitempty"`
	Paginate     *Paginate          `json:"paginate,omitempty"`
	Filter       FilterExpr         `json:"filter,omitempty"`
	Search       *SearchQuery       `json:"search,omitempty"`
	Fields       []string           `json:"fields,omitempty"`
	VectorSearch *VectorSearchQuery `json:"vectorSearch,omitempty"`
	HybridSearch *HybridSearchQuery `json:"hybridSearch,omitempty"`
}

// UnmarshalJSON decodes a Query with interface-typed fields converted
// recursively (Eq, gt/gte/lt/lte bounds, filter, and the filter carried by
// search/vectorSearch).
func (q *Query) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Table        string             `json:"table"`
		Get          *string            `json:"get,omitempty"`
		Index        *string            `json:"index,omitempty"`
		Eq           []json.RawMessage  `json:"eq,omitempty"`
		Gt           json.RawMessage    `json:"gt,omitempty"`
		Gte          json.RawMessage    `json:"gte,omitempty"`
		Lt           json.RawMessage    `json:"lt,omitempty"`
		Lte          json.RawMessage    `json:"lte,omitempty"`
		Order        *Order             `json:"order,omitempty"`
		Take         *int               `json:"take,omitempty"`
		Unique       bool               `json:"unique,omitempty"`
		First        bool               `json:"first,omitempty"`
		Count        bool               `json:"count,omitempty"`
		Distinct     bool               `json:"distinct,omitempty"`
		Aggregate    *AggregateSpec     `json:"aggregate,omitempty"`
		Paginate     *Paginate          `json:"paginate,omitempty"`
		Filter       json.RawMessage    `json:"filter,omitempty"`
		Search       *SearchQuery       `json:"search,omitempty"`
		Fields       []string           `json:"fields,omitempty"`
		VectorSearch *VectorSearchQuery `json:"vectorSearch,omitempty"`
		HybridSearch *HybridSearchQuery `json:"hybridSearch,omitempty"`
	}](b)
	if err != nil {
		return err
	}
	*q = Query{
		Table:        r.Table,
		Get:          r.Get,
		Index:        r.Index,
		Order:        r.Order,
		Take:         r.Take,
		Unique:       r.Unique,
		First:        r.First,
		Count:        r.Count,
		Distinct:     r.Distinct,
		Aggregate:    r.Aggregate,
		Paginate:     r.Paginate,
		Search:       r.Search,
		Fields:       r.Fields,
		VectorSearch: r.VectorSearch,
		HybridSearch: r.HybridSearch,
	}
	if len(r.Eq) > 0 {
		eq := make([]JSONValue, len(r.Eq))
		for i := range r.Eq {
			v, err := UnmarshalJSON(r.Eq[i])
			if err != nil {
				return err
			}
			eq[i] = v
		}
		q.Eq = eq
	}
	bounds := []struct {
		raw json.RawMessage
		dst **JSONValue
	}{{r.Gt, &q.Gt}, {r.Gte, &q.Gte}, {r.Lt, &q.Lt}, {r.Lte, &q.Lte}}
	for _, bd := range bounds {
		if len(bd.raw) == 0 {
			continue
		}
		v, err := UnmarshalJSON(bd.raw)
		if err != nil {
			return err
		}
		*bd.dst = &v
	}
	if len(r.Filter) > 0 {
		f, err := UnmarshalFilterExpr(r.Filter)
		if err != nil {
			return err
		}
		q.Filter = f
	}
	return nil
}

// Mirrors server/src/dsl.rs::Paginate (camelCase). Cursor is
// optional-omitted; numItems is required.
type Paginate struct {
	Cursor   *string `json:"cursor,omitempty"`
	NumItems int     `json:"numItems"`
}

// Mirrors server/src/dsl.rs::SearchMode — lowercase wire values; omitted
// on the wire when the caller does not opt in (zero value = absent).
type SearchMode string

const (
	SearchModeTsquery SearchMode = "tsquery"
	SearchModeTrgm    SearchMode = "trgm"
)

// Mirrors server/src/dsl.rs::SearchQuery (camelCase).
type SearchQuery struct {
	Index   string     `json:"index"`
	Query   string     `json:"query"`
	Filter  FilterExpr `json:"filter,omitempty"`
	Mode    SearchMode `json:"mode,omitempty"`
	Snippet *bool      `json:"snippet,omitempty"`
}

// Mirrors server/src/dsl.rs::VectorSearchQuery (camelCase).
type VectorSearchQuery struct {
	Index  string     `json:"index"`
	Vector Vector     `json:"vector"`
	Limit  int        `json:"limit"`
	Filter FilterExpr `json:"filter,omitempty"`
}

// Mirrors server/src/dsl.rs::HybridSearchQuery (camelCase).
type HybridSearchQuery struct {
	Query       string  `json:"query"`
	Vector      Vector  `json:"vector"`
	Limit       int     `json:"limit"`
	SearchIndex *string `json:"searchIndex,omitempty"`
	VectorIndex *string `json:"vectorIndex,omitempty"`
	K           *int    `json:"k,omitempty"`
}

// Mirrors server/src/dsl.rs::AggregateOp — lowercase values.
type AggregateOp string

const (
	AggSum   AggregateOp = "sum"
	AggAvg   AggregateOp = "avg"
	AggMin   AggregateOp = "min"
	AggMax   AggregateOp = "max"
	AggCount AggregateOp = "count"
)

// Mirrors server/src/dsl.rs::AggregateSpec — groupBy is a bool that ALWAYS
// serializes (the server has no skip_serializing_if on it).
type AggregateSpec struct {
	Op      AggregateOp `json:"op"`
	GroupBy bool        `json:"groupBy"`
}

// Mirrors server/src/dsl.rs::AggregateGroup — one {key, value} row from a
// grouped aggregate.
type AggregateGroup struct {
	Key   JSONValue `json:"key"`
	Value JSONValue `json:"value"`
}

// Mirrors server/src/dsl.rs::PaginatedResult — the paginate terminal's
// result shape (camelCase, nextCursor omitted when absent).
type PaginatedResult struct {
	Docs       []JSONValue `json:"docs"`
	NextCursor *string     `json:"nextCursor,omitempty"`
}

// UnmarshalJSON decodes a PaginatedResult (Docs holds dynamic values).
func (p *PaginatedResult) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Docs       []json.RawMessage `json:"docs"`
		NextCursor *string           `json:"nextCursor,omitempty"`
	}](b)
	if err != nil {
		return err
	}
	docs := make([]JSONValue, len(r.Docs))
	for i := range r.Docs {
		d, err := UnmarshalJSON(r.Docs[i])
		if err != nil {
			return err
		}
		docs[i] = d
	}
	p.Docs, p.NextCursor = docs, r.NextCursor
	return nil
}

// UnmarshalJSON decodes a SearchQuery (filter converts recursively).
func (s *SearchQuery) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Index   string          `json:"index"`
		Query   string          `json:"query"`
		Filter  json.RawMessage `json:"filter,omitempty"`
		Mode    SearchMode      `json:"mode,omitempty"`
		Snippet *bool           `json:"snippet,omitempty"`
	}](b)
	if err != nil {
		return err
	}
	s.Index, s.Query, s.Mode, s.Snippet = r.Index, r.Query, r.Mode, r.Snippet
	if len(r.Filter) > 0 {
		f, err := UnmarshalFilterExpr(r.Filter)
		if err != nil {
			return err
		}
		s.Filter = f
	}
	return nil
}

// UnmarshalJSON decodes a VectorSearchQuery (filter converts recursively).
func (v *VectorSearchQuery) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Index  string          `json:"index"`
		Vector json.RawMessage `json:"vector"`
		Limit  int             `json:"limit"`
		Filter json.RawMessage `json:"filter,omitempty"`
	}](b)
	if err != nil {
		return err
	}
	var vec Vector
	if err := json.Unmarshal(r.Vector, &vec); err != nil {
		return err
	}
	v.Index, v.Vector, v.Limit = r.Index, vec, r.Limit
	if len(r.Filter) > 0 {
		f, err := UnmarshalFilterExpr(r.Filter)
		if err != nil {
			return err
		}
		v.Filter = f
	}
	return nil
}

// --- strict-decode wrappers for scalar-only wire structs (unknown-field
// rejection on every wire type; decode still invokes nested methods). ---

// UnmarshalJSON rejects unknown fields.
func (p *Paginate) UnmarshalJSON(b []byte) error {
	type alias Paginate
	v, err := StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*p = Paginate(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (a *AggregateSpec) UnmarshalJSON(b []byte) error {
	type alias AggregateSpec
	v, err := StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*a = AggregateSpec(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (h *HybridSearchQuery) UnmarshalJSON(b []byte) error {
	type alias HybridSearchQuery
	v, err := StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*h = HybridSearchQuery(v)
	return nil
}

// UnmarshalJSON converts the dynamic key/value cells.
func (g *AggregateGroup) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Key   json.RawMessage `json:"key"`
		Value json.RawMessage `json:"value"`
	}](b)
	if err != nil {
		return err
	}
	k, err := UnmarshalJSON(r.Key)
	if err != nil {
		return err
	}
	val, err := UnmarshalJSON(r.Value)
	if err != nil {
		return err
	}
	g.Key, g.Value = k, val
	return nil
}

// Vector is a pgvector embedding: f32 values on the wire. Integral values
// marshal with a trailing ".0" — serde_json's ryu rendering of an f32 keeps
// the wire bytes identical to the server's output (a bare "1" diverges).
type Vector []float32

func (v Vector) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('[')
	for i, f := range v {
		if i > 0 {
			buf.WriteByte(',')
		}
		s := strconv.FormatFloat(float64(f), 'g', -1, 32)
		if !strings.ContainsAny(s, ".eE") {
			s += ".0"
		}
		buf.WriteString(s)
	}
	buf.WriteByte(']')
	return buf.Bytes(), nil
}

func (v *Vector) UnmarshalJSON(b []byte) error {
	var fs []float32
	if err := json.Unmarshal(b, &fs); err != nil {
		return err
	}
	*v = fs
	return nil
}
