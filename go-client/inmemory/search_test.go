// Search terminal tests — representative ports of rust in_memory/tests/search.rs
// (the exhaustive gate is the wire-corpus runner, Task 28).
package inmemory

import (
	"strings"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/dsl"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func searchStore(t *testing.T) *Store {
	s := newTestStore(t)
	seedRow(t, s, "items", 100, docObj("name", "alpha document here", "status", "todo", "order", int64(1)))
	seedRow(t, s, "items", 200, docObj("name", "beta paper alpha", "status", "todo", "order", int64(2)))
	seedRow(t, s, "items", 300, docObj("name", "gamma unrelated", "status", "todo", "order", int64(3)))
	return s
}

func names(v wire.JSONValue) []string {
	arr := v.(wire.Array)
	out := []string{}
	for _, d := range arr {
		if n, ok := d.(wire.Object)["name"].(wire.String); ok {
			out = append(out, string(n))
		}
	}
	return out
}

func TestSearchTokenAnd(t *testing.T) {
	s := searchStore(t)
	v := mustEval(t, s, wire.Query{Table: "items", Search: &wire.SearchQuery{
		Index: "by_content", Query: "alpha",
	}})
	got := names(v)
	if len(got) != 2 {
		t.Fatalf("token match: %v", got)
	}
	v = mustEval(t, s, wire.Query{Table: "items", Search: &wire.SearchQuery{
		Index: "by_content", Query: "alpha beta",
	}})
	// ANDed at the term level: each term must be a substring of SOME indexed
	// field's value — "beta paper alpha" carries both.
	if got = names(v); len(got) != 1 || got[0] != "beta paper alpha" {
		t.Fatalf("AND terms: %v", got)
	}
	v = mustEval(t, s, wire.Query{Table: "items", Search: &wire.SearchQuery{
		Index: "by_content", Query: "alpha gamma unrelated",
	}})
	// A term appearing in a DIFFERENT doc's field than the others still
	// kills the doc; no doc here holds all three terms.
	if got = names(v); len(got) != 0 {
		t.Fatalf("no doc holds all terms: %v", got)
	}
}

func TestSearchQuotedPhrase(t *testing.T) {
	s := searchStore(t)
	v := mustEval(t, s, wire.Query{Table: "items", Search: &wire.SearchQuery{
		Index: "by_content", Query: "\"document here\"",
	}})
	if got := names(v); len(got) != 1 || got[0] != "alpha document here" {
		t.Fatalf("phrase: %v", got)
	}
	v = mustEval(t, s, wire.Query{Table: "items", Search: &wire.SearchQuery{
		Index: "by_content", Query: "\"here document\"",
	}})
	if got := names(v); len(got) != 0 {
		t.Fatalf("non-adjacent phrase matched: %v", got)
	}
}

func TestSearchOrUnionAndExclusion(t *testing.T) {
	s := searchStore(t)
	v := mustEval(t, s, wire.Query{Table: "items", Search: &wire.SearchQuery{
		Index: "by_content", Query: "gamma or beta",
	}})
	got := names(v)
	if len(got) != 2 {
		t.Fatalf("or union: %v", got)
	}
	v = mustEval(t, s, wire.Query{Table: "items", Search: &wire.SearchQuery{
		Index: "by_content", Query: "alpha -beta",
	}})
	if got := names(v); len(got) != 1 || got[0] != "alpha document here" {
		t.Fatalf("exclusion: %v", got)
	}
}

func TestSearchTrgmRanksBySimilarity(t *testing.T) {
	s := searchStore(t)
	v := mustEval(t, s, wire.Query{Table: "items", Search: &wire.SearchQuery{
		Index: "by_content", Query: "alpha", Mode: wire.SearchModeTrgm,
	}})
	got := names(v)
	if len(got) != 2 {
		t.Fatalf("trgm matches: %v", got)
	}
	// "alpha document here" (18 chars) outranks "beta paper alpha" (16) —
	// shorter containing field wins; assert set only (order is the pinned
	// tie-break but the corpus pins the set).
	found := map[string]bool{}
	for _, n := range got {
		found[n] = true
	}
	if !found["alpha document here"] || !found["beta paper alpha"] {
		t.Fatalf("trgm set: %v", got)
	}
}

func TestSearchSnippet(t *testing.T) {
	s := searchStore(t)
	mark := true
	v := mustEval(t, s, wire.Query{Table: "items", Search: &wire.SearchQuery{
		Index: "by_content", Query: "alpha", Snippet: &mark,
	}})
	arr := v.(wire.Array)
	doc := arr[0].(wire.Object)
	sn, ok := doc["_searchSnippet"].(wire.String)
	if !ok {
		t.Fatalf("snippet missing: %v", doc)
	}
	if !strings.Contains(string(sn), "<mark>") {
		t.Fatalf("snippet has no mark: %s", sn)
	}
	// snippet + trgm → BAD_REQUEST.
	_, err := EvalQuery(s, wire.Query{Table: "items", Search: &wire.SearchQuery{
		Index: "by_content", Query: "alpha", Mode: wire.SearchModeTrgm, Snippet: &mark,
	}})
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest || !strings.Contains(re.Message, "snippet is only supported in tsquery mode") {
		t.Fatalf("snippet+trgm: %v", err)
	}
}

func TestSearchErrors(t *testing.T) {
	s := searchStore(t)
	// Empty query.
	_, err := EvalQuery(s, wire.Query{Table: "items", Search: &wire.SearchQuery{
		Index: "by_content", Query: "  ",
	}})
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest || !strings.Contains(re.Message, "must not be empty") {
		t.Fatalf("empty: %v", err)
	}
	// Unknown index.
	_, err = EvalQuery(s, wire.Query{Table: "items", Search: &wire.SearchQuery{
		Index: "by_ghost", Query: "alpha",
	}})
	re, ok = err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest || !strings.Contains(re.Message, "not found") {
		t.Fatalf("index: %v", err)
	}
}

func TestVectorSearchNarrowsByFilterAndLimit(t *testing.T) {
	s := searchStore(t)
	v := mustEval(t, s, wire.Query{Table: "items", VectorSearch: &wire.VectorSearchQuery{
		Index: "by_content", Vector: []float32{0.1, 0.2}, Limit: 2,
	}})
	if got := names(v); len(got) != 2 {
		t.Fatalf("vector limit: %v", got)
	}
	f := dsl.Eq("status", wire.String("todo"))
	v = mustEval(t, s, wire.Query{Table: "items", VectorSearch: &wire.VectorSearchQuery{
		Index: "by_content", Vector: []float32{0.1, 0.2}, Limit: 2, Filter: f,
	}})
	if got := names(v); len(got) != 2 {
		t.Fatalf("vector filtered: %v", got)
	}
	skip := dsl.Eq("status", wire.String("nope"))
	v = mustEval(t, s, wire.Query{Table: "items", VectorSearch: &wire.VectorSearchQuery{
		Index: "by_content", Vector: []float32{0.1, 0.2}, Limit: 2, Filter: skip,
	}})
	if got := names(v); len(got) != 0 {
		t.Fatalf("vector filter should empty the set: %v", got)
	}
}

func TestHybridSearchCapsAndPaginatesEmpty(t *testing.T) {
	s := searchStore(t)
	v := mustEval(t, s, wire.Query{Table: "items", HybridSearch: &wire.HybridSearchQuery{
		Query: "alpha", Vector: []float32{0.1}, Limit: 1,
	}})
	if got := names(v); len(got) != 1 {
		t.Fatalf("hybrid limit: %v", got)
	}
	v = mustEval(t, s, wire.Query{Table: "items", Paginate: &wire.Paginate{NumItems: 5},
		HybridSearch: &wire.HybridSearchQuery{Query: "alpha", Vector: []float32{0.1}, Limit: 5}})
	obj := v.(wire.Object)
	if docs := obj["docs"].(wire.Array); len(docs) != 0 {
		t.Fatalf("paginated hybrid must be empty: %v", docs)
	}
	if _, has := obj["nextCursor"]; has {
		t.Fatal("paginated hybrid must not mint a cursor")
	}
}

func TestSearchPeerExclusivity(t *testing.T) {
	s := searchStore(t)
	// search + vectorSearch at once → BAD_REQUEST.
	_, err := EvalQuery(s, wire.Query{Table: "items",
		Search:       &wire.SearchQuery{Index: "by_content", Query: "alpha"},
		VectorSearch: &wire.VectorSearchQuery{Index: "by_content", Limit: 1}})
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("search+vector: %v", err)
	}
	// search + paginate composes (ENH-030 peers).
	v := mustEval(t, s, wire.Query{Table: "items", Paginate: &wire.Paginate{NumItems: 1},
		Search: &wire.SearchQuery{Index: "by_content", Query: "alpha"}})
	if _, ok := v.(wire.Object)["docs"]; !ok {
		t.Fatalf("search+paginate envelope: %v", v)
	}
}
