// Cross-client wire-parity corpus test — the Go view of
// wire-corpus/wire-corpus.json (the port of ts-client's
// tests/wire-corpus.test.ts; the rust/python runners are the same contract).
// For every typed section: strict-decode the fixture entry (unknown fields
// reject), re-marshal, and require parsed-structural equality with the
// fixture. The rejects_* sections assert the strict decode FAILS.
package wire_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/admin"
	"github.com/paulrobello/par-rt-db/go-client/inmemory"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func corpusPath(t *testing.T, parts ...string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot resolve the corpus path")
	}
	// go-client/wire/wire_corpus_test.go → repo root is two levels up.
	path := filepath.Join(append([]string{filepath.Dir(thisFile), "..", ".."}, parts...)...)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("corpus fixture missing at %s: %v", path, err)
	}
	return path
}

func loadCorpus(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	raw, err := os.ReadFile(corpusPath(t, "wire-corpus", "wire-corpus.json"))
	if err != nil {
		t.Fatalf("read wire-corpus.json: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("parse wire-corpus.json: %v", err)
	}
	return fields
}

// roundTripTyped strict-decodes one raw entry into dst (an UnmarshalJSON
// implementor), re-marshals, and requires parsed-structural equality with
// the fixture.
func roundTripTyped(t *testing.T, section string, idx int, raw json.RawMessage, dst json.Unmarshaler) {
	t.Helper()
	if err := json.Unmarshal(raw, dst); err != nil {
		t.Fatalf("%s #%d: strict decode failed: %v", section, idx, err)
	}
	re, err := json.Marshal(dst)
	if err != nil {
		t.Fatalf("%s #%d: re-marshal failed: %v", section, idx, err)
	}
	fixture, ferr := wire.UnmarshalJSON(raw)
	round, rerr := wire.UnmarshalJSON(re)
	if ferr != nil || rerr != nil {
		t.Fatalf("%s #%d: fixture/re-marshal JSON parse failed: %v/%v", section, idx, ferr, rerr)
	}
	if !jsonValueEqual(fixture, round) {
		fb, _ := json.Marshal(fixture)
		rb, _ := json.Marshal(round)
		t.Fatalf("%s #%d: round-trip diverged\n fixture %s\n remarshal %s", section, idx, fb, rb)
	}
}

// jsonValueEqual is deep equality over parsed JSON trees (map order
// insensitive; number spellings compared exactly — the corpus fixture is the
// contract and Go re-marshals int64/float spellings verbatim).
func jsonValueEqual(a, b wire.JSONValue) bool {
	switch av := a.(type) {
	case wire.Object:
		bv, ok := b.(wire.Object)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			bv2, ok := bv[k]
			if !ok || !jsonValueEqual(v, bv2) {
				return false
			}
		}
		return true
	case wire.Array:
		bv, ok := b.(wire.Array)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !jsonValueEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	default:
		// Strings, numbers (exact spelling), bools, null — compare via their
		// canonical JSON rendering.
		ab, _ := json.Marshal(a)
		bb, _ := json.Marshal(b)
		return string(ab) == string(bb)
	}
}

func entries(t *testing.T, corpus map[string]json.RawMessage, section string) []json.RawMessage {
	t.Helper()
	raw, ok := corpus[section]
	if !ok {
		t.Fatalf("corpus is missing the %s section", section)
	}
	var out []json.RawMessage
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("%s: not an array: %v", section, err)
	}
	if len(out) == 0 {
		t.Fatalf("%s: empty section", section)
	}
	return out
}

func TestWireCorpusClientMessages(t *testing.T) {
	for idx, raw := range entries(t, loadCorpus(t), "client_messages") {
		msg, err := wire.UnmarshalClientMessage(raw)
		if err != nil {
			t.Fatalf("client_messages #%d: strict decode failed: %v", idx, err)
		}
		roundTripTyped(t, "client_messages", idx, raw, &typedClientMessage{msg})
	}
}

// typedClientMessage adapts an already-decoded ClientMessage to json.Unmarshaler.
type typedClientMessage struct{ msg wire.ClientMessage }

func (t *typedClientMessage) UnmarshalJSON(b []byte) error {
	m, err := wire.UnmarshalClientMessage(b)
	if err != nil {
		return err
	}
	t.msg = m
	return nil
}

func (t *typedClientMessage) MarshalJSON() ([]byte, error) { return json.Marshal(t.msg) }

func TestWireCorpusServerMessages(t *testing.T) {
	for idx, raw := range entries(t, loadCorpus(t), "server_messages") {
		msg, err := wire.UnmarshalServerMessage(raw)
		if err != nil {
			t.Fatalf("server_messages #%d: strict decode failed: %v", idx, err)
		}
		roundTripTyped(t, "server_messages", idx, raw, &typedServerMessage{msg})
	}
}

type typedServerMessage struct{ msg wire.ServerMessage }

func (t *typedServerMessage) UnmarshalJSON(b []byte) error {
	m, err := wire.UnmarshalServerMessage(b)
	if err != nil {
		return err
	}
	t.msg = m
	return nil
}

func (t *typedServerMessage) MarshalJSON() ([]byte, error) { return json.Marshal(t.msg) }

func TestWireCorpusAuthedUsers(t *testing.T) {
	for idx, raw := range entries(t, loadCorpus(t), "authed_users") {
		roundTripTyped(t, "authed_users", idx, raw, &wire.AuthedUser{})
	}
}

func TestWireCorpusScheduleWhens(t *testing.T) {
	for idx, raw := range entries(t, loadCorpus(t), "schedule_whens") {
		roundTripTyped(t, "schedule_whens", idx, raw, &typedScheduleWhen{})
	}
}

type typedScheduleWhen struct{ v wire.ScheduleWhen }

func (t *typedScheduleWhen) UnmarshalJSON(b []byte) error {
	v, err := wire.UnmarshalScheduleWhen(b)
	if err != nil {
		return err
	}
	t.v = v
	return nil
}

func (t *typedScheduleWhen) MarshalJSON() ([]byte, error) { return json.Marshal(t.v) }

func TestWireCorpusScheduleInfos(t *testing.T) {
	for idx, raw := range entries(t, loadCorpus(t), "schedule_infos") {
		roundTripTyped(t, "schedule_infos", idx, raw, &wire.ScheduleInfo{})
	}
}

func TestWireCorpusQueries(t *testing.T) {
	for idx, raw := range entries(t, loadCorpus(t), "queries") {
		roundTripTyped(t, "queries", idx, raw, &wire.Query{})
	}
}

func TestWireCorpusChangeFeedResponses(t *testing.T) {
	for idx, raw := range entries(t, loadCorpus(t), "change_feed_responses") {
		roundTripTyped(t, "change_feed_responses", idx, raw, &wire.ChangeFeedResponse{})
	}
}

func TestWireCorpusAdminOpEvents(t *testing.T) {
	for idx, raw := range entries(t, loadCorpus(t), "admin_op_events") {
		roundTripTyped(t, "admin_op_events", idx, raw, &admin.OpEvent{})
	}
}

func TestWireCorpusRawSectionsRoundTrip(t *testing.T) {
	// query_results / error_envelopes are raw JSON values (QueryResult is
	// untagged on the wire; the error envelope model lives in errors) —
	// JSON round-trip only, like the ts runner's untyped sections.
	corpus := loadCorpus(t)
	for _, section := range []string{"query_results", "error_envelopes"} {
		for idx, raw := range entries(t, corpus, section) {
			v, err := wire.UnmarshalJSON(raw)
			if err != nil {
				t.Fatalf("%s #%d: parse: %v", section, idx, err)
			}
			_ = v // parse == round-trip for a raw section
		}
	}
}

func TestWireCorpusMigrateRequests(t *testing.T) {
	for idx, raw := range entries(t, loadCorpus(t), "migrate_requests") {
		var req struct {
			Directives []json.RawMessage `json:"directives"`
			DryRun     bool              `json:"dryRun"`
		}
		if err := json.Unmarshal(raw, &req); err != nil {
			t.Fatalf("migrate_requests #%d: decode failed: %v", idx, err)
		}
		if len(req.Directives) == 0 {
			t.Fatalf("migrate_requests #%d: no directives", idx)
		}
		for di, d := range req.Directives {
			dir, err := admin.UnmarshalDirective(d)
			if err != nil {
				t.Fatalf("migrate_requests #%d directive #%d: strict decode failed: %v", idx, di, err)
			}
			re, err := json.Marshal(dir)
			if err != nil {
				t.Fatalf("migrate_requests #%d directive #%d: re-marshal: %v", idx, di, err)
			}
			fixture, ferr := wire.UnmarshalJSON(d)
			round, rerr := wire.UnmarshalJSON(re)
			if ferr != nil || rerr != nil || !jsonValueEqual(fixture, round) {
				t.Fatalf("migrate_requests #%d directive #%d: round-trip diverged (%v/%v)", idx, di, ferr, rerr)
			}
		}
	}
}

func TestWireCorpusMigrateResults(t *testing.T) {
	for idx, raw := range entries(t, loadCorpus(t), "migrate_results") {
		var res admin.MigrateResult
		if err := json.Unmarshal(raw, &res); err != nil {
			t.Fatalf("migrate_results #%d: decode failed: %v", idx, err)
		}
		round, err := json.Marshal(res)
		if err != nil {
			t.Fatalf("migrate_results #%d: re-marshal: %v", idx, err)
		}
		fixture, ferr := wire.UnmarshalJSON(raw)
		got, gerr := wire.UnmarshalJSON(round)
		if ferr != nil || gerr != nil || !jsonValueEqual(fixture, got) {
			t.Fatalf("migrate_results #%d: round-trip diverged", idx)
		}
	}
}

func TestWireCorpusRejectsUnknownFields(t *testing.T) {
	// Reject fixtures document inputs the strict parsers reject; Go's
	// wire types strict-decode, so each must FAIL to decode.
	corpus := loadCorpus(t)
	cases := []struct {
		section string
		decode  func(raw json.RawMessage) error
	}{
		{"rejects_client_message_unknown_field", func(raw json.RawMessage) error {
			_, err := wire.UnmarshalClientMessage(raw)
			return err
		}},
		{"rejects_schedule_when_unknown_field", func(raw json.RawMessage) error {
			_, err := wire.UnmarshalScheduleWhen(raw)
			return err
		}},
		{"rejects_workflow_spec_unknown_field", func(raw json.RawMessage) error {
			var ws wire.WorkflowSpec
			return json.Unmarshal(raw, &ws)
		}},
		{"rejects_authed_user_unknown_kind", func(raw json.RawMessage) error {
			var u wire.AuthedUser
			return json.Unmarshal(raw, &u)
		}},
		{"rejects_schedule_info_unknown_kind", func(raw json.RawMessage) error {
			var si wire.ScheduleInfo
			return json.Unmarshal(raw, &si)
		}},
		{"rejects_schedule_info_unknown_status", func(raw json.RawMessage) error {
			var si wire.ScheduleInfo
			return json.Unmarshal(raw, &si)
		}},
	}
	for _, tc := range cases {
		for idx, raw := range entries(t, corpus, tc.section) {
			if err := tc.decode(raw); err == nil {
				t.Fatalf("%s #%d: strict decode must reject this fixture", tc.section, idx)
			}
		}
	}
}

func TestWireCorpusProtocolConstants(t *testing.T) {
	// ARC-104: MAX_STEPS and PROTOCOL_VERSION are part of the cross-client
	// contract; the corpus records the canonical values every client asserts
	// against.
	var constants struct {
		MaxSteps        int64  `json:"max_steps"`
		ProtocolVersion uint32 `json:"protocol_version"`
	}
	if err := json.Unmarshal(loadCorpus(t)["protocol_constants"], &constants); err != nil {
		t.Fatalf("protocol_constants: %v", err)
	}
	if int64(inmemory.MaxSteps) != constants.MaxSteps {
		t.Fatalf("max_steps: engine %d, corpus %d", inmemory.MaxSteps, constants.MaxSteps)
	}
	if wire.PROTOCOL_VERSION != constants.ProtocolVersion {
		t.Fatalf("protocol_version: client %d, corpus %d", wire.PROTOCOL_VERSION, constants.ProtocolVersion)
	}
}

// Structural pins over the fixture surface (the ts runner's typed blocks,
// asserted here over parsed JSON so a corpus drift on a load-bearing surface
// fails loudly).

func TestWireCorpusExternalJobClaimEntries(t *testing.T) {
	var frames []struct {
		Type     string          `json:"type"`
		External *bool           `json:"external,omitempty"`
		When     json.RawMessage `json:"when"`
		Txn      json.RawMessage `json:"txn"`
	}
	all := []json.RawMessage{}
	for _, raw := range entries(t, loadCorpus(t), "client_messages") {
		all = append(all, raw)
	}
	blob, _ := json.Marshal(all)
	if err := json.Unmarshal(blob, &frames); err != nil {
		t.Fatal(err)
	}
	externalCount := 0
	withoutFlag := 0
	for _, f := range frames {
		if f.Type != "schedule" {
			continue
		}
		if f.External != nil && *f.External {
			externalCount++
			if !strings.Contains(string(f.When), `"afterMs"`) || !strings.Contains(string(f.Txn), `"steps"`) {
				t.Fatalf("external schedule frame: when/txn shape drifted: %s %s", f.When, f.Txn)
			}
		} else if f.External == nil {
			withoutFlag++
		}
	}
	if externalCount != 1 || withoutFlag == 0 {
		t.Fatalf("external claim frames: %d external, %d without flag", externalCount, withoutFlag)
	}
}

func TestWireCorpusOlderThanEntries(t *testing.T) {
	corpus := loadCorpus(t)
	// The by-query steps: patchByQuery with a bare olderThan and
	// deleteByQuery with an and-wrapped one (limit 500).
	found := map[string]bool{}
	for _, raw := range entries(t, corpus, "client_messages") {
		var msg struct {
			Type string `json:"type"`
			Txn  struct {
				Steps []struct {
					Op     string `json:"op"`
					Filter json.RawMessage
					Limit  *int `json:"limit"`
				} `json:"steps"`
			} `json:"txn"`
		}
		if json.Unmarshal(raw, &msg) != nil || msg.Type != "mutate" {
			continue
		}
		for _, s := range msg.Txn.Steps {
			if s.Op == "patchByQuery" || s.Op == "deleteByQuery" {
				found[s.Op] = true
				if !strings.Contains(string(s.Filter), "olderThan") {
					t.Fatalf("%s step must carry an olderThan filter: %s", s.Op, s.Filter)
				}
				if s.Op == "deleteByQuery" && (s.Limit == nil || *s.Limit != 500) {
					t.Fatalf("deleteByQuery limit pin: %v", s.Limit)
				}
			}
		}
	}
	if !found["patchByQuery"] || !found["deleteByQuery"] {
		t.Fatalf("olderThan by-query steps missing: %v", found)
	}
	// The bare olderThan query entry.
	var queries []struct {
		Table  string `json:"table"`
		Filter struct {
			Op    string `json:"op"`
			Field string `json:"field"`
			Ms    int64  `json:"ms"`
		} `json:"filter"`
	}
	blob, _ := json.Marshal(entries(t, corpus, "queries"))
	if json.Unmarshal(blob, &queries) != nil {
		t.Fatal("queries shape")
	}
	olderThan := 0
	for _, q := range queries {
		if q.Filter.Op == "olderThan" {
			olderThan++
			if q.Table != "workItems" || q.Filter.Field != "completedAt" || q.Filter.Ms != 604800000 {
				t.Fatalf("olderThan query pin drifted: %+v", q)
			}
		}
	}
	if olderThan != 1 {
		t.Fatalf("olderThan query entries: %d", olderThan)
	}
}

func osRead(path string) ([]byte, error) { return os.ReadFile(path) }
