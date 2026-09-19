// The embedded rule table must stay byte-identical with the repo's
// wire-corpus fixture — the fixture is the single source of truth; this test
// is the drift alarm (mirrors ts-client's query-combinations drift guard).
package inmemory

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestEmbeddedRuleTableMatchesCorpusFixture(t *testing.T) {
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve fixture path")
	}
	path := filepath.Join(filepath.Dir(thisFile), "..", "..", "wire-corpus", "query-combinations.json")
	fixture, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read corpus fixture (run from the repo checkout): %v", err)
	}
	if !bytes.Equal(embeddedRulesJSON, fixture) {
		t.Fatal("embedded query_combinations.json drifted from wire-corpus/query-combinations.json — re-copy the fixture into go-client/inmemory/")
	}
}
