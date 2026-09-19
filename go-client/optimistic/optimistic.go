// go-client/optimistic/optimistic.go
package optimistic

// Mirrors rust-client/src/optimistic.rs — projecting a transaction onto a
// cached query result so the UI can overlay the expected effect before the
// server round-trip, then reconcile against the authoritative update.

import (
	"fmt"
	"sync/atomic"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// ProjectionKind is the outcome of projecting a txn onto a cached result.
type ProjectionKind uint8

// Projection outcomes.
const (
	ProjectionSkip ProjectionKind = iota
	ProjectionOverlaid
)

// Projection is the result of Project.
type Projection struct {
	Kind  ProjectionKind
	Value wire.JSONValue
}

var syntheticCounter atomic.Uint64

// syntheticID brands a temporary id for an optimistically-inserted doc,
// replaced on reconcile with the server-assigned id.
func syntheticID() string {
	return fmt.Sprintf("__optimistic__%d", syntheticCounter.Add(1))
}

// Project projects txn onto last (the cached result for query). Routes to
// one of three shapes — unfiltered array, filtered array (delete-only), or
// get point-read — or Skip when the shape makes the effect ambiguous.
func Project(query wire.Query, last wire.JSONValue, txn wire.Transaction, now int64) Projection {
	if query.Get != nil {
		return projectGet(query, last, txn)
	}
	if !isArrayQuery(query) {
		return Projection{Kind: ProjectionSkip}
	}
	if hasFilter(query) {
		return projectFilteredArray(query, last, txn)
	}
	return projectUnfilteredArray(query, last, txn, now)
}

// isArrayQuery reports whether the query's result is a plain doc array.
// get/unique/first/count/distinct/paginate/search/vectorSearch are
// non-array or rank-based shapes we cannot project.
func isArrayQuery(q wire.Query) bool {
	return q.Get == nil &&
		!q.Unique && !q.First && !q.Count && !q.Distinct &&
		q.Paginate == nil && q.Search == nil && q.VectorSearch == nil
}

// hasFilter reports whether membership depends on a predicate we cannot
// evaluate without the schema. Only deletes of cached docs are unambiguous
// under such a filter.
func hasFilter(q wire.Query) bool {
	return q.Index != nil || len(q.Eq) > 0 ||
		q.Gt != nil || q.Gte != nil || q.Lt != nil || q.Lte != nil ||
		q.Filter != nil
}

func stepTable(s wire.Step) (string, bool) {
	switch st := s.(type) {
	case wire.StepInsert:
		return st.Table, true
	case wire.StepPatch:
		return st.Table, true
	case wire.StepAdjustCounter:
		return st.Table, true
	case wire.StepReplace:
		return st.Table, true
	case wire.StepDelete:
		return st.Table, true
	case wire.StepUndelete:
		return st.Table, true
	case wire.StepUpsert:
		return st.Table, true
	default:
		return "", false
	}
}

// projectUnfilteredArray overlays insert/patch/replace/delete on a
// full-table cached array.
func projectUnfilteredArray(query wire.Query, last wire.JSONValue, txn wire.Transaction, now int64) Projection {
	arr, ok := last.(wire.Array)
	if !ok {
		return Projection{Kind: ProjectionSkip}
	}
	working := make(wire.Array, len(arr))
	copy(working, arr)
	for _, step := range txn.Steps {
		table, ok := stepTable(step)
		if !ok || table != query.Table {
			continue
		}
		switch st := step.(type) {
		case wire.StepInsert:
			if query.Take != nil && len(working) >= *query.Take {
				// a full window would evict an unknown doc — decline
				return Projection{Kind: ProjectionSkip}
			}
			doc, ok := st.Doc.(wire.Object)
			if !ok {
				return Projection{Kind: ProjectionSkip}
			}
			d := wire.Object{}
			for k, v := range doc {
				d[k] = v
			}
			d["_id"] = wire.String(syntheticID())
			d["_creationTime"] = wire.Number(fmt.Sprintf("%d", now))
			d["_version"] = wire.Number("1")
			working = append(working, d)
		case wire.StepPatch:
			mergeByID(working, st.ID, st.Fields)
		case wire.StepReplace:
			replaceByID(working, st.ID, st.Doc)
		case wire.StepDelete:
			working = removeByID(working, st.ID)
		case wire.StepUndelete:
			// the restored body is not in the cache; the authoritative
			// update delivers the restored row
		case wire.StepUpsert, wire.StepAdjustCounter:
			return Projection{Kind: ProjectionSkip}
		}
	}
	return Projection{Kind: ProjectionOverlaid, Value: working}
}

// projectFilteredArray overlays ONLY deletes of already-cached docs —
// membership under a predicate is not evaluable without the schema.
func projectFilteredArray(query wire.Query, last wire.JSONValue, txn wire.Transaction) Projection {
	arr, ok := last.(wire.Array)
	if !ok {
		return Projection{Kind: ProjectionSkip}
	}
	working := make(wire.Array, len(arr))
	copy(working, arr)
	for _, step := range txn.Steps {
		table, ok := stepTable(step)
		if !ok || table != query.Table {
			continue
		}
		switch st := step.(type) {
		case wire.StepDelete:
			working = removeByID(working, st.ID)
		default:
			return Projection{Kind: ProjectionSkip}
		}
	}
	return Projection{Kind: ProjectionOverlaid, Value: working}
}

// projectGet overlays patch/replace/delete on a cached point read.
func projectGet(query wire.Query, last wire.JSONValue, txn wire.Transaction) Projection {
	doc, ok := last.(wire.Object)
	if !ok {
		return Projection{Kind: ProjectionSkip}
	}
	id := *query.Get
	working := wire.Object{}
	for k, v := range doc {
		working[k] = v
	}
	for _, step := range txn.Steps {
		table, ok := stepTable(step)
		if !ok || table != query.Table {
			continue
		}
		switch st := step.(type) {
		case wire.StepPatch:
			if st.ID == id {
				mergeInto(working, st.Fields)
			}
		case wire.StepReplace:
			if st.ID == id {
				rep, ok := st.Doc.(wire.Object)
				if !ok {
					return Projection{Kind: ProjectionSkip}
				}
				working = wire.Object{}
				for k, v := range rep {
					working[k] = v
				}
			}
		case wire.StepDelete:
			if st.ID == id {
				working = nil
			}
		case wire.StepInsert:
			// a different doc; the point read is unaffected
		case wire.StepUpsert, wire.StepAdjustCounter:
			return Projection{Kind: ProjectionSkip}
		}
	}
	var out wire.JSONValue
	if working == nil {
		out = wire.Null{}
	} else {
		out = working
	}
	return Projection{Kind: ProjectionOverlaid, Value: out}
}

// mergeByID merges fields into the doc with the given id in place.
func mergeByID(arr wire.Array, id string, fields wire.JSONValue) {
	for i, item := range arr {
		obj, ok := item.(wire.Object)
		if !ok {
			continue
		}
		if idv, ok := obj["_id"].(wire.String); ok && string(idv) == id {
			merged := wire.Object{}
			for k, v := range obj {
				merged[k] = v
			}
			mergeInto(merged, fields)
			arr[i] = merged
			return
		}
	}
}

// mergeInto merges a fields object into a target object.
func mergeInto(target wire.Object, fields wire.JSONValue) {
	f, ok := fields.(wire.Object)
	if !ok {
		return
	}
	for k, v := range f {
		target[k] = v
	}
}

// replaceByID swaps the doc with the given id for a new body.
func replaceByID(arr wire.Array, id string, doc wire.JSONValue) {
	for i, item := range arr {
		obj, ok := item.(wire.Object)
		if !ok {
			continue
		}
		if idv, ok := obj["_id"].(wire.String); ok && string(idv) == id {
			rep, ok := doc.(wire.Object)
			if !ok {
				return
			}
			next := wire.Object{}
			for k, v := range rep {
				next[k] = v
			}
			next["_id"] = wire.String(id)
			arr[i] = next
			return
		}
	}
}

// removeByID drops the doc with the given id.
func removeByID(arr wire.Array, id string) wire.Array {
	out := make(wire.Array, 0, len(arr))
	for _, item := range arr {
		obj, ok := item.(wire.Object)
		if !ok {
			out = append(out, item)
			continue
		}
		if idv, ok := obj["_id"].(wire.String); ok && string(idv) == id {
			continue
		}
		out = append(out, item)
	}
	return out
}
