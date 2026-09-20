// go-client/wire/query.go
package wire

// Mirrors server/src/dsl.rs — the read-clause wire vocabulary. Bare keys
// with two explicit camelCase renames (vectorSearch, hybridSearch);
// omitempty only where the server has skip_serializing_if. Interface-typed
// fields decode through the raw-conversion UnmarshalJSON below.

import (
	"bytes"
	"encoding/json"
	"fmt"
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

// Mirrors server/src/dsl.rs::AggregateSpec. Wire v2 (ARC-013): exactly one
// of Op (legacy single scalar op) or Aggregates (alias->op map, evaluated in
// one pass) must be set — both or neither is BadRequest, enforced server-side
// (this mirror does not duplicate the check). GroupBy widens from a bool to
// bool|[]string: GroupByBool(true) is the legacy `groupBy: true` shape;
// GroupByFields groups by the listed declared index fields, returning the
// wire-v2 AggregateMultiGroups rows. GroupBy ALWAYS serializes as a bare
// `groupBy: false`/`true`/`[...]` (the server has no skip_serializing_if on
// it, and this mirror's always-serialized convention predates v2, so a nil
// GroupBy on construction defaults to GroupByBool(false)); Aggregates is
// omitted when nil.
type AggregateSpec struct {
	Op         AggregateOp            `json:"op,omitempty"`
	Aggregates map[string]AggregateOp `json:"aggregates,omitempty"`
	GroupBy    GroupBy                `json:"groupBy"`
}

// MarshalJSON encodes AggregateSpec, defaulting a nil GroupBy to the legacy
// `false` (mirrors the server's `#[serde(default)]` on an absent groupBy).
func (a AggregateSpec) MarshalJSON() ([]byte, error) {
	type alias AggregateSpec
	out := alias(a)
	if out.GroupBy == nil {
		out.GroupBy = GroupByBool(false)
	}
	return json.Marshal(out)
}

// UnmarshalJSON decodes AggregateSpec, rejecting unknown fields and resolving
// the untagged `groupBy` shape (bool or string array) into a GroupBy value.
func (a *AggregateSpec) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Op         AggregateOp            `json:"op,omitempty"`
		Aggregates map[string]AggregateOp `json:"aggregates,omitempty"`
		GroupBy    json.RawMessage        `json:"groupBy,omitempty"`
	}](b)
	if err != nil {
		return err
	}
	groupBy, err := unmarshalGroupBy(r.GroupBy)
	if err != nil {
		return err
	}
	a.Op, a.Aggregates, a.GroupBy = r.Op, r.Aggregates, groupBy
	return nil
}

// GroupBy is the wire-v2 widening of AggregateSpec.groupBy: either a legacy
// bool (GroupByBool) or an explicit list of declared index fields
// (GroupByFields), grouping by that composite key.
type GroupBy interface {
	isGroupBy()
}

// GroupByBool is the legacy `groupBy: false`/`true` form.
type GroupByBool bool

func (GroupByBool) isGroupBy() {}

// GroupByFields is the wire-v2 `groupBy: [field, …]` composite-grouping form.
type GroupByFields []string

func (GroupByFields) isGroupBy() {}

// unmarshalGroupBy decodes a raw `groupBy` value into GroupByBool or
// GroupByFields depending on its JSON shape (untagged, mirroring the
// server's `#[serde(untagged)]` GroupBy enum). An absent field defaults to
// GroupByBool(false), matching the server's `#[serde(default)]`.
func unmarshalGroupBy(raw json.RawMessage) (GroupBy, error) {
	if len(raw) == 0 {
		return GroupByBool(false), nil
	}
	var b bool
	if err := json.Unmarshal(raw, &b); err == nil {
		return GroupByBool(b), nil
	}
	var fields []string
	if err := json.Unmarshal(raw, &fields); err != nil {
		return nil, fmt.Errorf("wire: groupBy must be a bool or a string array: %w", err)
	}
	return GroupByFields(fields), nil
}

// Mirrors server/src/dsl.rs::AggregateGroup — one {key, value} row from a
// legacy grouped aggregate (`groupBy: true`).
type AggregateGroup struct {
	Key   JSONValue `json:"key"`
	Value JSONValue `json:"value"`
}

// Mirrors server/src/dsl.rs::AggregateMultiGroup — one {keys, values} row
// from a wire-v2 grouped aggregate (`aggregates` map and/or an explicit
// `groupBy` field list).
type AggregateMultiGroup struct {
	Keys   []JSONValue          `json:"keys"`
	Values map[string]JSONValue `json:"values"`
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
