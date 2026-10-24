// Lookup-expansion tests — the Go port of the lookup coverage the ts and
// server suites carry (server/tests/lookup_test.rs; the exhaustive gate is
// the wire-corpus runner's lookup-* cases, driven by TestSemanticsCorpus).
// These pin the engine-level pieces the corpus only exercises transitively:
// the compile-time validation table and the joined-row fan-out details.
package inmemory

import (
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/dsl"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func lookupSchema() wire.JSONValue {
	return buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("users", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).
				Field("tagIds", dsl.ArrayOf(dsl.IdRef("tags")))
		})
		b.Table("tags", func(tb *dsl.TableBuilder) {
			tb.Field("label", dsl.Str())
		})
		b.Table("posts", func(tb *dsl.TableBuilder) {
			tb.Field("title", dsl.Str()).
				Field("authorId", dsl.Optional(dsl.IdRef("users"))).
				Field("tagIds", dsl.ArrayOf(dsl.IdRef("tags"))).
				Index("by_title", "title")
		})
	})
}

func newLookupClient(t *testing.T) *Store {
	t.Helper()
	s := NewStore().WithRandom(func() float64 { return 0.0 })
	if err := s.PushSchema(lookupSchema()); err != nil {
		t.Fatalf("push lookup schema: %v", err)
	}
	return s
}

func lookupEval(t *testing.T, s *Store, q wire.Query) wire.JSONValue {
	t.Helper()
	v, err := EvalQuery(s, q)
	if err != nil {
		t.Fatalf("eval: %v", err)
	}
	return v
}

func lookupWantCode(t *testing.T, s *Store, q wire.Query, want rtdberrors.ErrorCode) {
	t.Helper()
	_, err := EvalQuery(s, q)
	if err == nil {
		t.Fatalf("must reject with %s, got success", want)
	}
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != want {
		t.Fatalf("must reject with %s, got: %v", want, err)
	}
}

func TestLookupSingleJoinCarriesChildSystemFields(t *testing.T) {
	s := newLookupClient(t)
	u1 := seedRow(t, s, "users", 100, docObj("name", "Ann"))
	seedRow(t, s, "users", 200, docObj("name", "Bo"))
	seedRow(t, s, "posts", 300, docObj("title", "p1", "authorId", u1))
	seedRow(t, s, "posts", 400, docObj("title", "p2", "authorId", nil))

	v := lookupEval(t, s, wire.Query{
		Table:   "posts",
		Lookups: []wire.LookupSpec{{Field: "authorId", Table: "users", As: "author"}},
	})
	arr, ok := v.(wire.Array)
	if !ok || len(arr) != 2 {
		t.Fatalf("collect: %v", v)
	}
	// Creation order: p1 (300) before p2 (400).
	first := arr[0].(wire.Object)
	author, ok := first["author"].(wire.Object)
	if !ok {
		t.Fatalf("p1 author must be the joined doc, got %v", first["author"])
	}
	if got, _ := author["name"].(wire.String); string(got) != "Ann" {
		t.Fatalf("joined author name: %v", author["name"])
	}
	// Joined docs carry the child table's system fields so a client can
	// patch/subscribe against the joined doc's identity.
	if got, _ := author["_id"].(wire.String); string(got) != u1 {
		t.Fatalf("joined _id: %v want %s", author["_id"], u1)
	}
	if _, ok := author["_creationTime"].(wire.Number); !ok {
		t.Fatal("joined _creationTime missing")
	}
	if _, ok := author["_version"].(wire.Number); !ok {
		t.Fatal("joined _version missing")
	}
	// A null/absent optional Id reads as author:null.
	second := arr[1].(wire.Object)
	if _, isNull := second["author"].(wire.Null); !isNull {
		t.Fatalf("p2 author must be null, got %v", second["author"])
	}
}

func TestLookupDanglingReadsNullAndArrayDrops(t *testing.T) {
	s := newLookupClient(t)
	alpha := seedRow(t, s, "tags", 100, docObj("label", "alpha"))
	seedRow(t, s, "posts", 200, docObj("title", "p1", "authorId", "e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0"))
	seedRow(t, s, "users", 300, docObj("name", "U",
		"tagIds", wire.Array{wire.String("d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1"), wire.String(alpha)}))

	v := lookupEval(t, s, wire.Query{
		Table:   "posts",
		Lookups: []wire.LookupSpec{{Field: "authorId", Table: "users", As: "author"}},
	})
	first := v.(wire.Array)[0].(wire.Object)
	if _, isNull := first["author"].(wire.Null); !isNull {
		t.Fatalf("dangling single-form lookup must read null, got %v", first["author"])
	}

	v = lookupEval(t, s, wire.Query{
		Table:   "users",
		Lookups: []wire.LookupSpec{{Field: "tagIds", Table: "tags", As: "tags"}},
	})
	users := v.(wire.Array)
	if len(users) != 1 {
		t.Fatalf("users: %v", users)
	}
	tags, ok := users[0].(wire.Object)["tags"].(wire.Array)
	if !ok || len(tags) != 1 {
		t.Fatalf("dangling array-form lookup must drop the dangling id: %v", users[0])
	}
	if got, _ := tags[0].(wire.Object)["label"].(wire.String); string(got) != "alpha" {
		t.Fatalf("joined tag label: %v", tags[0])
	}
}

func TestLookupArrayPreservesIdOrderAndCapTruncates(t *testing.T) {
	s := newLookupClient(t)
	t3 := seedRow(t, s, "tags", 100, docObj("label", "t3"))
	t1 := seedRow(t, s, "tags", 200, docObj("label", "t1"))
	t2 := seedRow(t, s, "tags", 300, docObj("label", "t2"))
	// The id-array's declared order (t3, t1, t2) differs from creation order.
	seedRow(t, s, "posts", 400, docObj("title", "p1",
		"tagIds", wire.Array{wire.String(t3), wire.String("d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1d1"), wire.String(t1), wire.String(t2)}))

	v := lookupEval(t, s, wire.Query{
		Table:   "posts",
		Lookups: []wire.LookupSpec{{Field: "tagIds", Table: "tags", As: "tags", Cap: intPtr(2)}},
	})
	tags := v.(wire.Array)[0].(wire.Object)["tags"].(wire.Array)
	// Declared order, dangling dropped, truncated to cap 2.
	want := []string{"t3", "t1"}
	if len(tags) != len(want) {
		t.Fatalf("cap 2 must truncate to %v, got %v", want, tags)
	}
	for i, label := range want {
		got, _ := tags[i].(wire.Object)["label"].(wire.String)
		if string(got) != label {
			t.Fatalf("row %d: got %v want %s", i, tags[i], label)
		}
	}
}

func TestLookupProjectionMayNameAlias(t *testing.T) {
	s := newLookupClient(t)
	u1 := seedRow(t, s, "users", 100, docObj("name", "Ann"))
	seedRow(t, s, "posts", 200, docObj("title", "p1", "authorId", u1))

	keep := lookupEval(t, s, wire.Query{
		Table:   "posts",
		Fields:  []string{"title", "author"},
		Lookups: []wire.LookupSpec{{Field: "authorId", Table: "users", As: "author"}},
	})
	row := keep.(wire.Array)[0].(wire.Object)
	if _, ok := row["author"]; !ok {
		t.Fatal("fields naming the alias keeps the joined key")
	}
	if _, ok := row["authorId"]; ok {
		t.Fatal("fields not naming the id field drops it")
	}

	drop := lookupEval(t, s, wire.Query{
		Table:   "posts",
		Fields:  []string{"title"},
		Lookups: []wire.LookupSpec{{Field: "authorId", Table: "users", As: "author"}},
	})
	row = drop.(wire.Array)[0].(wire.Object)
	if _, ok := row["author"]; ok {
		t.Fatal("fields omitting the alias drops the joined docs")
	}
}

func TestLookupGetTerminalExpands(t *testing.T) {
	s := newLookupClient(t)
	u1 := seedRow(t, s, "users", 100, docObj("name", "Ann"))
	p1 := seedRow(t, s, "posts", 200, docObj("title", "p1", "authorId", u1))

	v := lookupEval(t, s, wire.Query{
		Table:   "posts",
		Get:     &p1,
		Lookups: []wire.LookupSpec{{Field: "authorId", Table: "users", As: "author"}},
	})
	author, ok := v.(wire.Object)["author"].(wire.Object)
	if !ok {
		t.Fatalf("get terminal must expand lookups, got %v", v)
	}
	if got, _ := author["name"].(wire.String); string(got) != "Ann" {
		t.Fatalf("joined author name: %v", author["name"])
	}
}

func TestLookupValidationTable(t *testing.T) {
	s := newLookupClient(t)
	seedRow(t, s, "users", 100, docObj("name", "Ann"))

	cases := []struct {
		name string
		q    wire.Query
		want rtdberrors.ErrorCode
	}{
		{
			name: "unknown lookup table",
			q: wire.Query{Table: "posts", Lookups: []wire.LookupSpec{
				{Field: "authorId", Table: "ghosts", As: "author"}}},
			want: rtdberrors.CodeNotFound,
		},
		{
			name: "undeclared field",
			q: wire.Query{Table: "posts", Lookups: []wire.LookupSpec{
				{Field: "ghostId", Table: "users", As: "author"}}},
			want: rtdberrors.CodeBadRequest,
		},
		{
			name: "field not Id-shaped",
			q: wire.Query{Table: "posts", Lookups: []wire.LookupSpec{
				{Field: "title", Table: "users", As: "author"}}},
			want: rtdberrors.CodeBadRequest,
		},
		{
			name: "declared Id table mismatch",
			q: wire.Query{Table: "posts", Lookups: []wire.LookupSpec{
				{Field: "authorId", Table: "tags", As: "author"}}},
			want: rtdberrors.CodeBadRequest,
		},
		{
			name: "alias with _ prefix",
			q: wire.Query{Table: "posts", Lookups: []wire.LookupSpec{
				{Field: "authorId", Table: "users", As: "_author"}}},
			want: rtdberrors.CodeBadRequest,
		},
		{
			name: "alias collides with declared field",
			q: wire.Query{Table: "posts", Lookups: []wire.LookupSpec{
				{Field: "authorId", Table: "users", As: "title"}}},
			want: rtdberrors.CodeBadRequest,
		},
		{
			name: "duplicate alias",
			q: wire.Query{Table: "posts", Lookups: []wire.LookupSpec{
				{Field: "authorId", Table: "users", As: "x"},
				{Field: "authorId", Table: "users", As: "x"}}},
			want: rtdberrors.CodeBadRequest,
		},
		{
			name: "cap above 1000",
			q: wire.Query{Table: "posts", Lookups: []wire.LookupSpec{
				{Field: "tagIds", Table: "tags", As: "tags", Cap: intPtr(1001)}}},
			want: rtdberrors.CodeBadRequest,
		},
		{
			name: "five lookups",
			q: wire.Query{Table: "posts", Lookups: []wire.LookupSpec{
				{Field: "authorId", Table: "users", As: "a"},
				{Field: "authorId", Table: "users", As: "b"},
				{Field: "authorId", Table: "users", As: "c"},
				{Field: "authorId", Table: "users", As: "d"},
				{Field: "authorId", Table: "users", As: "e"}}},
			want: rtdberrors.CodeBadRequest,
		},
		{
			name: "lookups+count",
			q: wire.Query{Table: "posts", Count: true, Lookups: []wire.LookupSpec{
				{Field: "authorId", Table: "users", As: "author"}}},
			want: rtdberrors.CodeBadRequest,
		},
		{
			name: "lookups+distinct",
			q: wire.Query{Table: "posts", Distinct: true, Index: strPtr("by_title"),
				Lookups: []wire.LookupSpec{
					{Field: "authorId", Table: "users", As: "author"}}},
			want: rtdberrors.CodeBadRequest,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lookupWantCode(t, s, tc.q, tc.want)
		})
	}

	// The positive boundary: 4 lookups with cap 1000 are legal.
	lookupEval(t, s, wire.Query{Table: "posts", Lookups: []wire.LookupSpec{
		{Field: "authorId", Table: "users", As: "a"},
		{Field: "authorId", Table: "users", As: "b"},
		{Field: "tagIds", Table: "tags", As: "c", Cap: intPtr(1000)},
		{Field: "tagIds", Table: "tags", As: "d"},
	}})
}

func intPtr(v int) *int { return &v }
