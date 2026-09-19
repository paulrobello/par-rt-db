// Value comparison under the corpus contract: recursive normalize
// projection, canonical (key-sorted) serialization for the unordered
// multiset compare, and numeric-tolerant leaf equality (6 == 6.0). The Go
// port of rust tests/semantics_corpus.rs's helpers.
package corpus

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// DefaultNormalize is the projection applied when a case carries no
// normalize list: the volatile system fields.
var DefaultNormalize = []string{"_id", "_creationTime", "_version"}

// Canonical renders a value with sorted object keys, recursively — the
// multiset-sort form (encoding/json marshals maps sorted).
func Canonical(v wire.JSONValue) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// projectRecursive removes every key in keys from every object in the tree.
func projectRecursive(node wire.JSONValue, keys []string) {
	switch t := node.(type) {
	case wire.Object:
		for _, k := range keys {
			delete(t, k)
		}
		for _, v := range t {
			projectRecursive(v, keys)
		}
	case wire.Array:
		for _, v := range t {
			projectRecursive(v, keys)
		}
	}
}

// jsonEqNumeric compares two values with JSON-number tolerance: numbers
// compare by their float representation, everything else structurally.
func jsonEqNumeric(a, b wire.JSONValue) bool {
	an, aok := a.(wire.Number)
	bn, bok := b.(wire.Number)
	if aok && bok {
		af, aerr := parseFloat64(string(an))
		bf, berr := parseFloat64(string(bn))
		if aerr == nil && berr == nil {
			return absF64(af-bf) < 2.220446049250313e-16 || af == bf
		}
		return string(an) == string(bn)
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
			ov, present := bv[k]
			if !present || !jsonEqNumeric(v, ov) {
				return false
			}
		}
		return true
	default:
		return jsonEqLite(a, b)
	}
}

func jsonEqLite(a, b wire.JSONValue) bool {
	switch av := a.(type) {
	case nil:
		return b == nil
	case wire.Null:
		_, ok := b.(wire.Null)
		return ok
	case wire.Bool:
		r, ok := b.(wire.Bool)
		return ok && av == r
	case wire.String:
		r, ok := b.(wire.String)
		return ok && av == r
	default:
		return false
	}
}

func parseFloat64(s string) (float64, error) {
	return strconv.ParseFloat(s, 64)
}

func absF64(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}

// CompareValues asserts got == want after the normalize projection:
// unordered compares arrays as multisets sorted by canonical JSON, otherwise
// in place; leaves compare numeric-tolerant.
func CompareValues(got, want wire.JSONValue, normalize []string, unordered bool) error {
	projectRecursive(got, normalize)
	projectRecursive(want, normalize)
	if jsonEqNumeric(got, want) {
		return nil
	}
	if !unordered {
		return fmt.Errorf("result mismatch\n got %s\nwant %s", Canonical(got), Canonical(want))
	}
	g, gok := got.(wire.Array)
	w, wok := want.(wire.Array)
	if !gok || !wok {
		return fmt.Errorf("unordered comparison requires arrays — got %s, want %s", Canonical(got), Canonical(want))
	}
	if len(g) != len(w) {
		return fmt.Errorf("row count mismatch (unordered) — got %d, want %d", len(g), len(w))
	}
	gs := append([]wire.JSONValue(nil), g...)
	ws := append([]wire.JSONValue(nil), w...)
	sort.SliceStable(gs, func(a, b int) bool { return Canonical(gs[a]) < Canonical(gs[b]) })
	sort.SliceStable(ws, func(a, b int) bool { return Canonical(ws[a]) < Canonical(ws[b]) })
	for i := range gs {
		if !jsonEqNumeric(gs[i], ws[i]) {
			return fmt.Errorf("row %d mismatch (unordered compare)\n got %s\nwant %s", i, Canonical(got), Canonical(want))
		}
	}
	return nil
}
