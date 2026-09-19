// The wire-corpus semantics runner — the Go engine executes every case in
// wire-corpus/semantics/*.json through the shared corpus harness. Zero skips
// expected: a genuine engine gap gets a loud Skip["go"] fixture entry ADDED
// TO THE FIXTURE only with an accompanying card filed — any silent divergence
// here is an engine bug to fix, never a fixture edit.
//
// External test package: internal/corpus imports inmemory, so this file
// cannot live in the internal test package.
package inmemory_test

import (
	"path/filepath"
	"runtime"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/internal/corpus"
)

func semanticsDir(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve corpus path")
	}
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "wire-corpus", "semantics")
}

func TestSemanticsCorpus(t *testing.T) {
	cases, err := corpus.LoadSemanticsDir(semanticsDir(t))
	if err != nil {
		t.Fatal(err)
	}
	for i := range cases {
		c := cases[i]
		t.Run(c.Name, func(t *testing.T) {
			corpus.RunSemanticsCase(t, &c)
		})
	}
}
