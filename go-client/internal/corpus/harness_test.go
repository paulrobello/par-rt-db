// Harness unit tests against hand-written mini-cases: seed labeling +
// $idRef substitution, normalize projection, unordered multiset compare,
// numeric tolerance, the $prev paginate sentinel, a pushError case, and the
// failing-compare error message.
package corpus

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func writeCase(t *testing.T, name, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func runOne(t *testing.T, body string) {
	t.Helper()
	dir := writeCase(t, "mini.json", body)
	cases, err := LoadSemanticsDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	RunSemanticsCase(t, &cases[0])
}

func TestHarnessSeedLabelAndIdRef(t *testing.T) {
	runOne(t, `{
		"name": "mini",
		"schema": {"tables": {"items": {"fields": {"name": {"type": "string"}, "n": {"type": "number"}}}}},
		"seed": [
			{"table": "items", "doc": {"name": "keep", "n": 1}, "$id": "target"},
			{"name": "other", "n": 2}
		],
		"op": {"query": {"table": "items", "get": {"$idRef": "target"}}},
		"expect": {"_id": "whatever", "_creationTime": 0, "_version": 1, "name": "keep", "n": 1}
	}`)
}

func TestHarnessNormalizeAndNumericTolerance(t *testing.T) {
	// n is 1 (stored); expect 1.0 — numeric tolerance must agree; the
	// normalize default strips _id/_creationTime/_version.
	runOne(t, `{
		"name": "mini",
		"schema": {"tables": {"items": {"fields": {"name": {"type": "string"}, "n": {"type": "number"}}}}},
		"seed": [{"table": "items", "doc": {"name": "keep", "n": 1}, "$id": "row"}],
		"op": {"query": {"table": "items", "get": {"$idRef": "row"}}},
		"expect": {"name": "keep", "n": 1.0}
	}`)
}

func TestHarnessUnorderedMultiset(t *testing.T) {
	runOne(t, `{
		"name": "mini",
		"schema": {"tables": {"items": {"fields": {"name": {"type": "string"}, "n": {"type": "number"}}, "indexes": [{"name": "by_n", "fields": ["n"]}]}}},
		"seed": [
			{"name": "a", "n": 1},
			{"name": "b", "n": 2},
			{"name": "c", "n": 3}
		],
		"op": {"query": {"table": "items"}},
		"unordered": true,
		"expect": [
			{"name": "c", "n": 3},
			{"name": "a", "n": 1},
			{"name": "b", "n": 2}
		]
	}`)
}

func TestHarnessPushError(t *testing.T) {
	runOne(t, `{
		"name": "mini",
		"schema": {"tables": {"items": {"fields": {"name": {"type": "string"}, "tags": {"type": "array", "element": {"type": "string"}}}, "indexes": [{"name": "by_tags", "fields": ["tags"]}]}}},
		"pushError": {"code": "SCHEMA_VIOLATION"}
	}`)
}

func TestHarnessErrorCaseAssertsCodeOnly(t *testing.T) {
	runOne(t, `{
		"name": "mini",
		"schema": {"tables": {"items": {"fields": {"name": {"type": "string"}, "n": {"type": "number"}}}}},
		"seed": [{"name": "a", "n": 1}, {"name": "b", "n": 2}],
		"op": {"query": {"table": "items", "unique": true}},
		"expect": {"error": {"code": "PRECONDITION_FAILED"}}
	}`)
}

func TestHarnessPrevCursorSentinel(t *testing.T) {
	runOne(t, `{
		"name": "mini",
		"schema": {"tables": {"items": {"fields": {"name": {"type": "string"}, "n": {"type": "number"}}, "indexes": [{"name": "by_n", "fields": ["n"]}]}}},
		"seed": [
			{"name": "a", "n": 1},
			{"name": "b", "n": 2},
			{"name": "c", "n": 3}
		],
		"op": {"query": {"table": "items", "index": "by_n", "eq": [], "paginate": {"numItems": 2, "cursor": "$prev"}}},
		"expect": {"docs": [{"name": "c", "n": 3}]},
		"expect_next_cursor": false
	}`)
}

func TestHarnessThenFollowUp(t *testing.T) {
	runOne(t, `{
		"name": "mini",
		"schema": {"tables": {"items": {"fields": {"name": {"type": "string"}, "n": {"type": "number"}}}}},
		"seed": [{"table": "items", "doc": {"name": "a", "n": 1}, "$id": "row"}],
		"op": {"txn": {"steps": [{"op": "patch", "table": "items", "id": {"$idRef": "row"}, "fields": {"n": 9}}]}},
		"expect": [null],
		"then": {"query": {"table": "items", "get": {"$idRef": "row"}}, "expect": {"name": "a", "n": 9}}
	}`)
}

type fakeTB struct {
	fatalMsg string
	fataled  bool
}

func (f *fakeTB) Helper()                  {}
func (f *fakeTB) Skipf(_ string, _ ...any) {}
func (f *fakeTB) Fatalf(format string, args ...any) {
	f.fataled = true
	f.fatalMsg = fmt.Sprintf(format, args...)
	panic(f.fatalMsg)
}

func TestHarnessFailingCompareNamesTheCase(t *testing.T) {
	// A mismatching case must fail LOUDLY with a compare error naming the
	// case — asserted through a fake TB so the test itself stays green.
	dir := writeCase(t, "bad.json", `{
		"name": "bad",
		"schema": {"tables": {"items": {"fields": {"name": {"type": "string"}, "n": {"type": "number"}}}}},
		"seed": [{"name": "a", "n": 1}],
		"op": {"query": {"table": "items", "count": true}},
		"expect": 42
	}`)
	cases, err := LoadSemanticsDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	fb := &fakeTB{}
	func() {
		defer func() { _ = recover() }()
		RunSemanticsCase(fb, &cases[0])
	}()
	if !fb.fataled {
		t.Fatal("a mismatching case must Fatal")
	}
	if msg := fb.fatalMsg; msg == "" || msg[:3] != "bad" {
		t.Fatalf("failure must name the case: %q", msg[:min2(60, len(msg))])
	}
}

func min2(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type skipTB struct {
	fakeTB
	skipReason string
	skipped    bool
}

func (s *skipTB) Skipf(format string, args ...any) {
	s.skipped = true
	s.skipReason = fmt.Sprintf(format, args...)
	panic(s.skipReason)
}

func TestHarnessGoSkip(t *testing.T) {
	// The named-runner skip must fire Skipf with the fixture's reason —
	// asserted through a recording TB so the test itself stays green.
	dir := writeCase(t, "skipped.json", `{
		"name": "skipped",
		"schema": {"tables": {"items": {"fields": {"name": {"type": "string"}, "n": {"type": "number"}}}}},
		"seed": [],
		"op": {"query": {"table": "items"}},
		"expect": [],
		"skip": {"go": "not wired yet"}
	}`)
	cases, err := LoadSemanticsDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	stb := &skipTB{}
	func() {
		defer func() { _ = recover() }()
		RunSemanticsCase(stb, &cases[0])
	}()
	if !stb.skipped {
		t.Fatal("the go skip entry must Skipf")
	}
	// RunSemanticsCase renders the reason through tb.Skipf("go: %s", reason).
	if stb.skipReason != "go: not wired yet" {
		t.Fatalf("skip reason: %q", stb.skipReason)
	}
}
