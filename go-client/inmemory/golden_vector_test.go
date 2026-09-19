// Golden-vector parity — the Go view of wire-corpus/golden-vector.json
// (port of rust tests/golden_vector.rs): one shared dataset, each case's
// wire-shape query compared canonicalized with the fixture expectation.
// System fields are projected so id-minting order never diverges; the audit
// point is sort-comparator / boundary / terminal-cascade behavior.
package inmemory

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/dsl"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

type gvFixture struct {
	SchemaTable   string            `json:"schema_table"`
	SchemaFields  map[string]string `json:"schema_fields"`
	SchemaIndexes []struct {
		Name   string   `json:"name"`
		Fields []string `json:"fields"`
		Search bool     `json:"search"`
		Vector *struct {
			Dimensions int `json:"dimensions"`
		} `json:"vector,omitempty"`
	} `json:"schema_indexes"`
	Seed  []json.RawMessage `json:"seed"`
	Cases []gvCase          `json:"cases"`
}

type gvCase struct {
	ID       string          `json:"id"`
	Query    json.RawMessage `json:"query"`
	Expected json.RawMessage `json:"expected"`
	// Scalar count; distinct from a JSON-null aggregate via pointer vs raw.
	ExpectedScalar    *int64          `json:"expected_scalar"`
	ExpectedValue     json.RawMessage `json:"expected_value"`
	ExpectedGroups    json.RawMessage `json:"expected_groups"`
	ExpectedDistinct  json.RawMessage `json:"expected_distinct"`
	ExpectedUnordered bool            `json:"expected_unordered"`
	ExpectedHasCursor bool            `json:"expected_has_next_cursor"`
}

// corpusFixturePath resolves a wire-corpus fixture from this test file's dir.
func corpusFixturePath(t *testing.T, parts ...string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve fixture path")
	}
	return filepath.Join(append([]string{filepath.Dir(thisFile), "..", "..", "wire-corpus"}, parts...)...)
}

// gvFieldType translates the fixture's field-type shorthand into the dsl
// builders; new types the fixture uses must be added here (same discipline
// as the rust runner).
func gvFieldType(shorthand string) dsl.FieldType {
	switch shorthand {
	case "string":
		return dsl.Str()
	case "number":
		return dsl.Num()
	case "optional(string)":
		return dsl.Optional(dsl.Str())
	case "array(string)":
		return dsl.ArrayOf(dsl.Str())
	}
	if rest, ok := strings.CutPrefix(shorthand, "vector("); ok {
		dims := 0
		for _, c := range strings.TrimSuffix(rest, ")") {
			if c >= '0' && c <= '9' {
				dims = dims*10 + int(c-'0')
			}
		}
		return dsl.VectorF(dims)
	}
	panic("fixture field type not implemented: " + shorthand)
}

func gvSchema(t *testing.T, fx *gvFixture) wire.JSONValue {
	t.Helper()
	return buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table(fx.SchemaTable, func(tb *dsl.TableBuilder) {
			names := make([]string, 0, len(fx.SchemaFields))
			for name := range fx.SchemaFields {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				tb.Field(name, gvFieldType(fx.SchemaFields[name]))
			}
			for _, ix := range fx.SchemaIndexes {
				switch {
				case ix.Search:
					tb.SearchIndex(ix.Name, "", ix.Fields...)
				case ix.Vector != nil:
					tb.VectorIndex(ix.Name, ix.Fields[0], ix.Vector.Dimensions, nil, "cosine")
				default:
					tb.Index(ix.Name, ix.Fields...)
				}
			}
		})
	})
}

func gvLoad(t *testing.T) *gvFixture {
	t.Helper()
	raw, err := os.ReadFile(corpusFixturePath(t, "golden-vector.json"))
	if err != nil {
		t.Fatal(err)
	}
	var fx gvFixture
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	return &fx
}

func gvClient(t *testing.T, fx *gvFixture) *Store {
	t.Helper()
	var clock int64 = 1_700_000_000_000
	s := NewStore().WithClock(func() int64 { clock++; return clock }).WithRandom(func() float64 { return 0.0 })
	if err := s.PushSchema(gvSchema(t, fx)); err != nil {
		t.Fatalf("push_schema: %v", err)
	}
	table := s.SchemaSnapshot().Tables[fx.SchemaTable]
	for i, doc := range fx.Seed {
		v, err := wire.UnmarshalJSON(doc)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := doInsert(s, fx.SchemaTable, table, v.(wire.Object)); err != nil {
			t.Fatalf("seed #%d: %v", i, err)
		}
	}
	return s
}

// gvProject drops system fields (name/status/order only) so id-minting
// order never diverges.
func gvProject(doc wire.JSONValue) wire.JSONValue {
	obj, ok := doc.(wire.Object)
	if !ok {
		return doc
	}
	out := wire.Object{}
	for _, k := range []string{"name", "status", "order"} {
		if v, has := obj[k]; has {
			out[k] = v
		} else {
			out[k] = wire.Null{}
		}
	}
	return out
}

// jsonEqNumeric is numeric-tolerant equality (6 == 6.0 across the SQL
// numeric server result and the f64 client aggregate), recursive.
func jsonEqNumeric(a, b wire.JSONValue) bool {
	an, aok := a.(wire.Number)
	bn, bok := b.(wire.Number)
	if aok && bok {
		af := jsonNumberF64(an)
		bf := jsonNumberF64(bn)
		return math.Abs(af-bf) < 2.220446049250313e-16 || af == bf
	}
	switch av := a.(type) {
	case wire.Array:
		bv, ok := b.(wire.Array)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !jsonEqNumeric(av[i], bv[i]) {
				return false
			}
		}
		return true
	case wire.Object:
		bv, ok := b.(wire.Object)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			ov, ok := bv[k]
			if !ok || !jsonEqNumeric(v, ov) {
				return false
			}
		}
		return true
	default:
		return jsonEq(a, b)
	}
}

func mustJSONText(v wire.JSONValue) string {
	b, err := json.Marshal(v)
	if err != nil {
		return "<unmarshalable>"
	}
	return string(b)
}

func gvDocName(v wire.JSONValue) string {
	if obj, ok := v.(wire.Object); ok {
		if s, ok := obj["name"].(wire.String); ok {
			return string(s)
		}
	}
	return ""
}

func TestGoldenVectorParity(t *testing.T) {
	fx := gvLoad(t)
	s := gvClient(t, fx)
	for _, c := range fx.Cases {
		t.Run(c.ID, func(t *testing.T) {
			var q wire.Query
			if err := json.Unmarshal(c.Query, &q); err != nil {
				t.Fatalf("parse Query from fixture: %v", err)
			}
			result, err := EvalQuery(s, q)
			if err != nil {
				t.Fatalf("run_query: %v", err)
			}
			wantValue := len(c.ExpectedValue) > 0
			wantGroups := len(c.ExpectedGroups) > 0
			wantDistinct := len(c.ExpectedDistinct) > 0
			wantExpected := len(c.Expected) > 0

			switch {
			case c.ExpectedScalar != nil:
				if !jsonEqNumeric(result, wire.Number(formatI64(*c.ExpectedScalar))) {
					t.Fatalf("expected count %d, got %s", *c.ExpectedScalar, mustJSONText(result))
				}

			case wantValue:
				// aggregate scalar: a bare number, or null over an empty set.
				want, _ := wire.UnmarshalJSON(c.ExpectedValue)
				if !jsonEqNumeric(result, want) {
					t.Fatalf("aggregate scalar mismatch: got %s, want %s", mustJSONText(result), c.ExpectedValue)
				}

			case wantGroups:
				got, ok := result.(wire.Array)
				if !ok {
					t.Fatal("aggregate groupBy must return array")
				}
				wantAny, _ := wire.UnmarshalJSON(c.ExpectedGroups)
				wantGroups, ok := wantAny.(wire.Array)
				if !ok {
					t.Fatal("expected_groups requires array")
				}
				if len(got) != len(wantGroups) {
					t.Fatalf("group count mismatch: %d vs %d", len(got), len(wantGroups))
				}
				for i := range got {
					g, ok := got[i].(wire.Object)
					if !ok {
						t.Fatalf("group %d not an object", i)
					}
					w, ok := wantGroups[i].(wire.Object)
					if !ok {
						t.Fatalf("group %d expectation not an object", i)
					}
					if !jsonEq(g["key"], w["key"]) {
						t.Fatalf("group %d key mismatch: %v vs %v", i, g["key"], w["key"])
					}
					if !jsonEqNumeric(g["value"], w["value"]) {
						t.Fatalf("group %d value mismatch: %v vs %v", i, g["value"], w["value"])
					}
				}

			case wantDistinct:
				got, ok := result.(wire.Array)
				if !ok {
					t.Fatal("distinct must return array")
				}
				wantAny, _ := wire.UnmarshalJSON(c.ExpectedDistinct)
				want, ok := wantAny.(wire.Array)
				if !ok {
					t.Fatal("expected_distinct requires array")
				}
				_ = ok
				if len(got) != len(want) {
					t.Fatalf("distinct count mismatch: %d vs %d", len(got), len(want))
				}
				for i := range got {
					if !jsonEqNumeric(got[i], want[i]) {
						t.Fatalf("distinct[%d] mismatch: %v vs %v", i, got[i], want[i])
					}
				}

			case c.ExpectedUnordered:
				gotArr, ok := result.(wire.Array)
				if !ok {
					t.Fatal("expected array")
				}
				got := make(wire.Array, 0, len(gotArr))
				for _, d := range gotArr {
					got = append(got, gvProject(d))
				}
				sort.SliceStable(got, func(a, b int) bool { return gvDocName(got[a]) < gvDocName(got[b]) })
				wantAny, _ := wire.UnmarshalJSON(c.Expected)
				want, ok := wantAny.(wire.Array)
				if !ok {
					t.Fatal("expected_unordered requires array")
				}
				sort.SliceStable(want, func(a, b int) bool { return gvDocName(want[a]) < gvDocName(want[b]) })
				if !jsonEq(got, want) {
					t.Fatalf("unordered mismatch:\n got %s\nwant %s", mustJSONText(got), mustJSONText(want))
				}

			case c.ExpectedHasCursor:
				page, ok := result.(wire.Object)
				if !ok {
					t.Fatal("expected PaginatedResult object")
				}
				docs, ok := page["docs"].(wire.Array)
				if !ok {
					t.Fatal("PaginatedResult.docs array")
				}
				got := make(wire.Array, 0, len(docs))
				for _, d := range docs {
					got = append(got, gvProject(d))
				}
				wantAny, _ := wire.UnmarshalJSON(c.Expected)
				want, ok := wantAny.(wire.Array)
				if !ok {
					t.Fatal("paginate requires array expected")
				}
				if !jsonEq(got, want) {
					t.Fatalf("page mismatch:\n got %s\nwant %s", mustJSONText(got), mustJSONText(want))
				}
				nc, has := page["nextCursor"]
				if !has || isNullValue(nc) {
					t.Fatal("expected nextCursor present")
				}

			case wantExpected:
				expected, err2 := wire.UnmarshalJSON(c.Expected)
				if err2 != nil {
					t.Fatal(err2)
				}
				if _, isArr := expected.(wire.Array); isArr {
					gotArr, ok := result.(wire.Array)
					if !ok {
						t.Fatal("expected array")
					}
					got := make(wire.Array, 0, len(gotArr))
					for _, d := range gotArr {
						got = append(got, gvProject(d))
					}
					if !jsonEq(got, expected) {
						t.Fatalf("ordered mismatch:\n got %s\nwant %s", mustJSONText(got), c.Expected)
					}
					return
				}
				got := gvProject(result)
				if !jsonEq(got, expected) {
					t.Fatalf("single-doc mismatch:\n got %s\nwant %s", mustJSONText(got), c.Expected)
				}

			default:
				t.Fatalf("case has no expected shape")
			}
		})
	}
}
