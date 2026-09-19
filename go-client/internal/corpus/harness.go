// The wire-corpus semantics runner harness — the Go port of rust
// tests/semantics_corpus.rs's run_case (the contract in
// wire-corpus/README.md "How a runner executes a case"). Fresh engine per
// case, push (or pushError), seed through the real insert path with $id
// labels recorded, placeholder substitution, the $prev two-pass paginate
// run, and normalize/unordered/numeric-tolerant comparison. The runner never
// ticks schedulers or advances time (README determinism ruling).
package corpus

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/paulrobello/par-rt-db/go-client/admin"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/inmemory"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// SemanticsCase is one semantics/*.json fixture, kept raw (typed accessors
// decode lazily so authoring errors name the case, not a struct field).
type SemanticsCase struct {
	Name             string
	Raw              map[string]json.RawMessage `json:"-"`
	SourceFile       string                     `json:"-"`
	ParseError       error                      `json:"-"` // set when the file did not parse
	semanticTaggedID string
}

// LoadSemanticsDir reads every *.json fixture under dir, sorted by filename.
func LoadSemanticsDir(dir string) ([]SemanticsCase, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return nil, fmt.Errorf("%s contains no fixture files", dir)
	}
	out := make([]SemanticsCase, 0, len(names))
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		c := SemanticsCase{SourceFile: filepath.Join(dir, name)}
		if err != nil {
			c.Name = strings.TrimSuffix(name, ".json")
			c.ParseError = err
			out = append(out, c)
			continue
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(raw, &fields); err != nil {
			c.Name = strings.TrimSuffix(name, ".json")
			c.ParseError = err
			out = append(out, c)
			continue
		}
		c.Raw = fields
		if n, ok := fields["name"]; ok {
			var s string
			if json.Unmarshal(n, &s) == nil {
				c.Name = s
			}
		}
		out = append(out, c)
	}
	return out, nil
}

// caseTB is the failure surface the harness needs (satisfied by *testing.T
// and *testing.B, and by fakes in tests).
type caseTB interface {
	Helper()
	Fatalf(format string, args ...any)
	Skipf(format string, args ...any)
}

// runClient is the deterministic engine the runner drives (post-increment
// clock from the rust runner's epoch, constant 0 RNG).
func newRunClient() *inmemory.Client {
	var clock int64 = 1_700_000_000_000
	return inmemory.New(inmemory.Options{
		Now:    func() int64 { clock++; return clock },
		Random: func() float64 { return 0.0 },
	})
}

// RunSemanticsCase executes one case end to end. Skip["go"] skips loudly;
// parse failures and authoring errors fail the case by name.
func RunSemanticsCase(tb caseTB, c *SemanticsCase) {
	tb.Helper()
	if c.ParseError != nil {
		tb.Fatalf("%s: fixture did not parse: %v", c.Name, c.ParseError)
	}
	if c.Raw == nil {
		tb.Fatalf("%s: fixture has no fields", c.Name)
	}
	if reason, ok := skipReason(c.Raw, "go"); ok {
		tb.Skipf("go: %s", reason)
	}
	name := c.Name
	if stem := strings.TrimSuffix(filepath.Base(c.SourceFile), ".json"); name != "" && stem != "" && name != stem {
		tb.Fatalf("%s: case `name` must equal the filename stem", stem)
	}
	client := newRunClient()
	store := client.Store()

	// pushError: the push is the whole case.
	if pushRaw, ok := c.Raw["pushError"]; ok {
		for _, stray := range []string{"seed", "op", "then", "expect"} {
			if _, has := c.Raw[stray]; has {
				tb.Fatalf("%s: a pushError case must not carry `%s` — push is the whole case", name, stray)
			}
		}
		var want struct {
			Code string `json:"code"`
		}
		if err := json.Unmarshal(pushRaw, &want); err != nil {
			tb.Fatalf("%s: pushError.code does not parse: %v", name, err)
		}
		err := client.PushSchema(mustValue(tb, name, c.Raw["schema"]))
		if err == nil {
			tb.Fatalf("%s: pushError case — the push must fail", name)
		}
		assertErrorCode(tb, name, err, want.Code)
		return
	}

	schemaValue := mustValue(tb, name, c.Raw["schema"])
	if err := client.PushSchema(schemaValue); err != nil {
		tb.Fatalf("%s: push_schema: %v", name, err)
	}
	singleTable := singleTableName(schemaValue)

	ids, err := seedCase(client, name, c.Raw["seed"], singleTable)
	if err != nil {
		tb.Fatalf("%s: %v", name, err)
	}

	expectRaw, hasExpect := c.Raw["expect"]
	if !hasExpect {
		tb.Fatalf("%s: missing expect", name)
	}
	var expectTree wire.JSONValue
	expectsError := false
	var wantErrCode string
	if e := mustValue2(tb, name, expectRaw); e != nil {
		expectTree = e
	}
	if errTree, ok := errorTree(expectTree); ok {
		expectsError = true
		wantErrCode = errTree
	}
	caseKeys := normalizeKeys(tb, name, c.Raw, "")

	opRaw, hasOp := c.Raw["op"]
	if !hasOp {
		tb.Fatalf("%s: missing op", name)
	}
	var opFields map[string]json.RawMessage
	if err := json.Unmarshal(opRaw, &opFields); err != nil {
		tb.Fatalf("%s: op does not parse: %v", name, err)
	}
	opResult, err := executeOp(tb, client, store, name, opFields, ids)
	if err != nil {
		if !expectsError {
			tb.Fatalf("%s: unexpected op error: %v", name, err)
		}
		assertErrorCode(tb, name, err, wantErrCode)
		return // a failed op has no then follow-up
	}
	if expectsError {
		tb.Fatalf("%s: expected error %s, got success %s", name, wantErrCode, Canonical(opResult))
	}
	assertResult(tb, name, opResult, expectTree, caseKeys, boolField(c.Raw, "unordered"), boolField(c.Raw, "expect_next_cursor"))

	// Follow-up read after a successful op.
	if thenRaw, ok := c.Raw["then"]; ok {
		var then map[string]json.RawMessage
		if err := json.Unmarshal(thenRaw, &then); err != nil {
			tb.Fatalf("%s: then does not parse: %v", name, err)
		}
		qRaw, ok := then["query"]
		if !ok {
			tb.Fatalf("%s: then requires query", name)
		}
		qv, err := substituteJSON(mustValue(tb, name, qRaw), ids)
		if err != nil {
			tb.Fatalf("%s: then substitute: %v", name, err)
		}
		var q wire.Query
		if err := json.Unmarshal([]byte(Canonical(qv)), &q); err != nil {
			tb.Fatalf("%s: then.query does not parse: %v", name, err)
		}
		actual, err := inmemory.EvalQuery(store, q)
		if err != nil {
			tb.Fatalf("%s: then.query: %v", name, err)
		}
		keys := caseKeys
		if _, has := then["normalize"]; has {
			keys = normalizeKeys(tb, name, then, "")
		}
		assertResult(tb, name, actual, mustValue(tb, name, then["expect"]), keys, boolField(then, "unordered"), boolField(then, "expect_next_cursor"))
	}
}

func mustValue(tb caseTB, name string, raw json.RawMessage) wire.JSONValue {
	tb.Helper()
	v, err := wire.UnmarshalJSON(raw)
	if err != nil {
		tb.Fatalf("%s: JSON does not parse: %v", name, err)
	}
	return v
}

func mustValue2(tb caseTB, name string, raw json.RawMessage) wire.JSONValue {
	return mustValue(tb, name, raw)
}

func errorTree(v wire.JSONValue) (string, bool) {
	obj, ok := v.(wire.Object)
	if !ok {
		return "", false
	}
	errObj, ok := obj["error"].(wire.Object)
	if !ok {
		return "", false
	}
	code, ok := errObj["code"].(wire.String)
	if !ok {
		return "", false
	}
	return string(code), true
}

func boolField(fields map[string]json.RawMessage, key string) bool {
	raw, ok := fields[key]
	if !ok {
		return false
	}
	var b bool
	if json.Unmarshal(raw, &b) != nil {
		return false
	}
	return b
}

func skipReason(fields map[string]json.RawMessage, runner string) (string, bool) {
	raw, ok := fields["skip"]
	if !ok {
		return "", false
	}
	var skips map[string]string
	if err := json.Unmarshal(raw, &skips); err != nil {
		return "", false
	}
	reason, ok := skips[runner]
	return reason, ok
}

func normalizeKeys(tb caseTB, name string, fields map[string]json.RawMessage, _ string) []string {
	tb.Helper()
	raw, ok := fields["normalize"]
	if !ok {
		return append([]string(nil), DefaultNormalize...)
	}
	var keys []string
	if err := json.Unmarshal(raw, &keys); err != nil {
		tb.Fatalf("%s: normalize entries must be strings: %v", name, err)
	}
	return keys
}

func assertErrorCode(tb caseTB, name string, err error, want string) {
	tb.Helper()
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok {
		tb.Fatalf("%s: error is not an RtDbError: %v", name, err)
	}
	if string(re.Code) != want {
		tb.Fatalf("%s: error code mismatch — want %s, got %s (engine message: %s)", name, want, re.Code, re.Message)
	}
}

// seedCase inserts each seed entry through the real insert path, recording
// label → minted id for $id-labeled entries.
// singleTableName returns the schema's sole table name when exactly one is
// declared (the plain-doc seed's implied table), "" otherwise.
func singleTableName(schema wire.JSONValue) string {
	obj, ok := schema.(wire.Object)
	if !ok {
		return ""
	}
	tables, ok := obj["tables"].(wire.Object)
	if !ok || len(tables) != 1 {
		return ""
	}
	for name := range tables {
		return name
	}
	return ""
}

func seedCase(client *inmemory.Client, name string, seedRaw json.RawMessage, singleTable string) (map[string]string, error) {
	ids := map[string]string{}
	if seedRaw == nil {
		return ids, fmt.Errorf("seed must be an array")
	}
	var entries []json.RawMessage
	if err := json.Unmarshal(seedRaw, &entries); err != nil {
		return nil, fmt.Errorf("seed must be an array: %w", err)
	}
	for i, entry := range entries {
		table, doc, label, err := parseSeedEntry(entry, singleTable)
		if err != nil {
			return nil, fmt.Errorf("seed #%d: %w", i, err)
		}
		results, err := client.Mutate(nil, wire.Transaction{Steps: []wire.Step{wire.StepInsert{
			Table: table,
			Doc:   doc,
		}}}, "")
		if err != nil {
			return nil, fmt.Errorf("seed #%d into '%s': %v", i, table, err)
		}
		if results[0].ID == nil {
			return nil, fmt.Errorf("seed #%d: expected an Insert result", i)
		}
		if label != "" {
			ids[label] = *results[0].ID
		}
	}
	return ids, nil
}

// parseSeedEntry resolves one seed entry: wrapped (object with a doc key
// whose value is an object) or plain (single-table schemas only).
func parseSeedEntry(raw json.RawMessage, singleTable string) (table string, doc wire.JSONValue, label string, err error) {
	v, err := wire.UnmarshalJSON(raw)
	if err != nil {
		return "", nil, "", err
	}
	obj, ok := v.(wire.Object)
	if !ok {
		return "", nil, "", fmt.Errorf("seed entry must be a JSON object")
	}
	if docV, has := obj["doc"]; has {
		if _, isObj := docV.(wire.Object); !isObj {
			return "", nil, "", fmt.Errorf("wrapped seed entry's doc must be an object")
		}
		t := singleTable
		if tv, has := obj["table"]; has {
			ts, ok := tv.(wire.String)
			if !ok {
				return "", nil, "", fmt.Errorf("seed table must be a string")
			}
			t = string(ts)
		}
		if t == "" {
			return "", nil, "", fmt.Errorf("wrapped seed entry without `table` requires a single-table schema")
		}
		if idv, has := obj["$id"]; has {
			ls, ok := idv.(wire.String)
			if !ok {
				return "", nil, "", fmt.Errorf("$id label must be a string")
			}
			label = string(ls)
		}
		// Strip the $id label (and the table member) from the stored doc —
		// labels are never sent (README step 2).
		stripped := wire.Object{}
		for k, val := range docV.(wire.Object) {
			if k == "$id" || k == "table" {
				continue
			}
			stripped[k] = val
		}
		return t, stripped, label, nil
	}
	if singleTable == "" {
		return "", nil, "", fmt.Errorf("plain-doc seed requires a single-table schema")
	}
	return singleTable, obj, "", nil
}

// executeOp dispatches the query / txn / migrate op (README step 3-5).
func executeOp(tb caseTB, client *inmemory.Client, store *inmemory.Store, name string, fields map[string]json.RawMessage, ids map[string]string) (wire.JSONValue, error) {
	tb.Helper()
	if txnRaw, ok := fields["txn"]; ok {
		tv, err := substituteJSON(mustValue(tb, name, txnRaw), ids)
		if err != nil {
			tb.Fatalf("%s: %v", name, err)
		}
		var txn wire.Transaction
		if err := json.Unmarshal([]byte(Canonical(tv)), &txn); err != nil {
			tb.Fatalf("%s: op.txn does not parse: %v", name, err)
		}
		results, err := client.Mutate(nil, txn, "")
		if err != nil {
			return nil, err
		}
		return stepResultsToWire(results), nil
	}
	if qRaw, ok := fields["query"]; ok {
		return executeQueryOp(tb, client, store, name, qRaw, ids)
	}
	if migrateRaw, ok := fields["migrate"]; ok {
		mv, err := substituteJSON(mustValue(tb, name, migrateRaw), ids)
		if err != nil {
			tb.Fatalf("%s: %v", name, err)
		}
		var req struct {
			Directives []json.RawMessage `json:"directives"`
			DryRun     bool              `json:"dryRun"`
		}
		if err := json.Unmarshal([]byte(Canonical(mv)), &req); err != nil {
			tb.Fatalf("%s: op.migrate does not parse: %v", name, err)
		}
		directives := make([]admin.Directive, 0, len(req.Directives))
		for i, raw := range req.Directives {
			d, err := admin.UnmarshalDirective(raw)
			if err != nil {
				tb.Fatalf("%s: directive #%d does not parse: %v", name, i, err)
			}
			directives = append(directives, d)
		}
		result, err := client.ApplyMigration(directives, req.DryRun)
		if err != nil {
			return nil, err
		}
		out, err := json.Marshal(result)
		if err != nil {
			tb.Fatalf("%s: serialize migrate result: %v", name, err)
		}
		return wire.UnmarshalJSON(out)
	}
	return nil, fmt.Errorf("%s: op must carry `query`, `txn`, or `migrate`", name)
}

// executeQueryOp substitutes placeholders and resolves the $prev paginate
// cursor sentinel: run the cursor-less query, take its nextCursor (failing
// loudly when absent), then run the query with it — expect describes the
// SECOND page.
func executeQueryOp(tb caseTB, client *inmemory.Client, store *inmemory.Store, name string, qRaw json.RawMessage, ids map[string]string) (wire.JSONValue, error) {
	tb.Helper()
	raw := mustValue(tb, name, qRaw)
	prev := ""
	if obj, ok := raw.(wire.Object); ok {
		if pag, ok := obj["paginate"].(wire.Object); ok {
			if cur, ok := pag["cursor"].(wire.String); ok && string(cur) == "$prev" {
				firstObj := cloneJSON(raw).(wire.Object)
				firstPag := cloneJSON(firstObj["paginate"]).(wire.Object)
				delete(firstPag, "cursor")
				firstObj["paginate"] = firstPag
				var first wire.Query
				if err := json.Unmarshal([]byte(Canonical(firstObj)), &first); err != nil {
					tb.Fatalf("%s: $prev first-page query does not parse: %v", name, err)
				}
				firstResult, err := inmemory.EvalQuery(store, first)
				if err != nil {
					tb.Fatalf("%s: $prev first page: %v", name, err)
				}
				fObj, ok := firstResult.(wire.Object)
				if !ok {
					tb.Fatalf("%s: $prev: first page result is not an object", name)
				}
				nc, ok := fObj["nextCursor"].(wire.String)
				if !ok {
					tb.Fatalf("%s: $prev: first page has no nextCursor", name)
				}
				prev = string(nc)
			}
		}
	}
	substituted, err := Substitute(raw, ids, prev)
	if err != nil {
		tb.Fatalf("%s: %v", name, err)
	}
	var q wire.Query
	if err := json.Unmarshal([]byte(Canonical(substituted)), &q); err != nil {
		tb.Fatalf("%s: op.query does not parse: %v", name, err)
	}
	return inmemory.EvalQuery(store, q)
}

// stepResultsToWire renders the untagged StepResult serialization (insert
// {"id"}; upsert {"id","inserted"}; patchByQuery {"patched","truncated"};
// deleteByQuery {"deleted","truncated"}; schedule/cancel/workflow singles;
// null otherwise) — the README's txn expect shape.
func stepResultsToWire(results []wire.StepResult) wire.JSONValue {
	out := make(wire.Array, 0, len(results))
	for _, r := range results {
		switch {
		case r.ID != nil && r.Inserted != nil:
			out = append(out, wire.Object{"id": wire.String(*r.ID), "inserted": wire.Bool(*r.Inserted)})
		case r.ID != nil:
			out = append(out, wire.Object{"id": wire.String(*r.ID)})
		case r.Patched != nil:
			out = append(out, wire.Object{"patched": wire.Number(formatInt(*r.Patched)), "truncated": wire.Bool(boolVal(r.Truncated))})
		case r.Deleted != nil:
			out = append(out, wire.Object{"deleted": wire.Number(formatInt(*r.Deleted)), "truncated": wire.Bool(boolVal(r.Truncated))})
		case r.ScheduleID != nil:
			out = append(out, wire.Object{"scheduleId": wire.String(*r.ScheduleID)})
		case r.Cancelled != nil:
			out = append(out, wire.Object{"cancelled": wire.Bool(*r.Cancelled)})
		case r.WorkflowID != nil:
			out = append(out, wire.Object{"workflowId": wire.String(*r.WorkflowID)})
		default:
			out = append(out, wire.Null{})
		}
	}
	return out
}

func boolVal(b *bool) bool { return b != nil && *b }

func formatInt(v int64) string { return strconv.FormatInt(v, 10) }

// substituteJSON resolves $idRef objects (and nothing else) in an op tree;
// the $prev cursor sentinel is resolved by the query executor's two-pass run.
func substituteJSON(v wire.JSONValue, ids map[string]string) (wire.JSONValue, error) {
	return Substitute(v, ids, "")
}

func cloneJSON(v wire.JSONValue) wire.JSONValue {
	switch t := v.(type) {
	case wire.Object:
		out := wire.Object{}
		for k, val := range t {
			out[k] = cloneJSON(val)
		}
		return out
	case wire.Array:
		out := make(wire.Array, len(t))
		for i, item := range t {
			out[i] = cloneJSON(item)
		}
		return out
	default:
		return v
	}
}

// assertResult applies the normalize projection, the nextCursor structural
// assert, and the ordered/unordered compare.
func assertResult(tb caseTB, name string, actual, expect wire.JSONValue, keys []string, unordered, expectNextCursor bool) {
	tb.Helper()
	got := actual
	want := expect
	if expectNextCursor {
		obj, ok := got.(wire.Object)
		if !ok {
			tb.Fatalf("%s: nextCursor assert requires an object result, got %s", name, Canonical(got))
		}
		_, has := obj["nextCursor"]
		if has != expectNextCursor {
			tb.Fatalf("%s: nextCursor presence mismatch (got %v, want %v)", name, has, expectNextCursor)
		}
		projected := append(append([]string(nil), keys...), "nextCursor")
		projectRecursive(got, projected)
		projectRecursive(want, projected)
	} else {
		projectRecursive(got, keys)
		projectRecursive(want, keys)
	}
	if err := CompareValues(got, want, nil, unordered); err != nil {
		tb.Fatalf("%s: %v", name, err)
	}
}
