// Query-combination rule evaluator — the Go port of core/src/query_combinations.rs,
// driven by the same wire-corpus/query-combinations.json table the server and
// rust client embed. The corpus file lives outside this module, so it is
// resolved relative to this source file at first use (the same mechanism the
// corpus runners use); a missing or malformed table is a loud INTERNAL error,
// never a silent pass.
package inmemory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"sync"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// maxTake caps take/collect/paginate page sizes (server MAX_TAKE).
const maxTake = 4096

// maxSearchQueryBytes is the SEC-007 ceiling on raw search text.
const maxSearchQueryBytes = 4096

type combinationRule struct {
	Forbid    []string `json:"forbid"`
	AtMostOne []string `json:"atMostOne"`
	Code      string   `json:"code"`
	Message   string   `json:"message"`
	hasForbid bool
	hasAtMost bool
}

type ruleTable struct {
	Rules []combinationRule `json:"rules"`
}

var (
	rulesOnce sync.Once
	rules     *ruleTable
	rulesErr  error
)

func loadRules() (*ruleTable, error) {
	rulesOnce.Do(func() {
		_, thisFile, _, ok := runtime.Caller(0)
		if !ok {
			rulesErr = rtdberrors.New(rtdberrors.CodeInternal, "cannot resolve query-combinations.json path")
			return
		}
		path := filepath.Join(filepath.Dir(thisFile), "..", "..", "wire-corpus", "query-combinations.json")
		data, err := os.ReadFile(path)
		if err != nil {
			rulesErr = rtdberrors.New(rtdberrors.CodeInternal, "read query-combinations.json: "+err.Error())
			return
		}
		var t ruleTable
		if err := json.Unmarshal(data, &t); err != nil {
			rulesErr = rtdberrors.New(rtdberrors.CodeInternal, "parse query-combinations.json: "+err.Error())
			return
		}
		for i := range t.Rules {
			t.Rules[i].hasForbid = len(t.Rules[i].Forbid) > 0
			t.Rules[i].hasAtMost = len(t.Rules[i].AtMostOne) > 0
			if t.Rules[i].hasForbid == t.Rules[i].hasAtMost {
				rulesErr = rtdberrors.New(rtdberrors.CodeInternal,
					"query-combinations.json: a rule must declare exactly one of forbid/atMostOne")
				return
			}
		}
		rules = &t
	})
	return rules, rulesErr
}

// queryClauses builds the canonical clause-presence set for a Query (mirror
// of the server's query_clauses).
func queryClauses(q *wire.Query) map[string]bool {
	set := map[string]bool{}
	if q.Get != nil {
		set["get"] = true
	}
	if q.Index != nil {
		set["index"] = true
	}
	if len(q.Eq) > 0 {
		set["eq"] = true
	}
	if q.Gt != nil {
		set["gt"] = true
	}
	if q.Gte != nil {
		set["gte"] = true
	}
	if q.Lt != nil {
		set["lt"] = true
	}
	if q.Lte != nil {
		set["lte"] = true
	}
	if q.Order != nil {
		set["order"] = true
	}
	if q.Take != nil {
		set["take"] = true
	}
	if q.Unique {
		set["unique"] = true
	}
	if q.First {
		set["first"] = true
	}
	if q.Count {
		set["count"] = true
	}
	if q.Distinct {
		set["distinct"] = true
	}
	if q.Aggregate != nil {
		set["aggregate"] = true
	}
	if q.Paginate != nil {
		set["paginate"] = true
	}
	if q.Filter != nil {
		set["filter"] = true
	}
	if q.Search != nil {
		set["search"] = true
	}
	if q.VectorSearch != nil {
		set["vectorSearch"] = true
	}
	if q.HybridSearch != nil {
		set["hybridSearch"] = true
	}
	return set
}

// checkQueryCombinations evaluates every declared rule against the clause
// set: every forbid rule (declaration order), then every atMostOne rule —
// accept/reject is order-independent, only the winning message follows the
// table's tie-break.
func checkQueryCombinations(q *wire.Query) error {
	table, err := loadRules()
	if err != nil {
		return err
	}
	present := queryClauses(q)
	for i := range table.Rules {
		rule := &table.Rules[i]
		if rule.hasForbid && allPresent(rule.Forbid, present) {
			return ruleViolation(rule)
		}
	}
	for i := range table.Rules {
		rule := &table.Rules[i]
		if rule.hasAtMost && countPresent(rule.AtMostOne, present) > 1 {
			return ruleViolation(rule)
		}
	}
	return nil
}

func allPresent(clauses []string, present map[string]bool) bool {
	for _, c := range clauses {
		if !present[c] {
			return false
		}
	}
	return true
}

func countPresent(clauses []string, present map[string]bool) int {
	n := 0
	for _, c := range clauses {
		if present[c] {
			n++
		}
	}
	return n
}

func ruleViolation(rule *combinationRule) error {
	code := rtdberrors.ErrorCode(rule.Code)
	return rtdberrors.New(code, rule.Message)
}
