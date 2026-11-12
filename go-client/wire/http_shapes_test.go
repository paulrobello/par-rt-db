// HTTP-shape drift verification (Go view) over wire-corpus/http-shapes.json
// (repo root — the single source of truth; the server-side parity test
// protocol::tests::http_shapes_fixture_parity
// constructs the real wire structs against it, and every client mirrors it).
//
// Strict-decodes the fixture's ClaimedSchedule / ScheduleInfo instances
// (unknown fields reject on ClaimedSchedule), asserts every field, and
// re-marshals for canonical structural parity with the fixture. The fixture
// exists because the ClaimedSchedule.tz drift shipped undetected (fixed in
// 8a55d03); the WS corpus does not cover HTTP-only shapes.
package wire_test

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func loadHttpShapes(t *testing.T) map[string]json.RawMessage {
	t.Helper()
	raw, err := os.ReadFile(corpusPath(t, "wire-corpus", "http-shapes.json"))
	if err != nil {
		t.Fatalf("read http-shapes.json: %v", err)
	}
	var shapes map[string]json.RawMessage
	if err := json.Unmarshal(raw, &shapes); err != nil {
		t.Fatalf("parse http-shapes.json: %v", err)
	}
	return shapes
}

// canonicalStruct marshals v, re-decodes into a generic tree, then
// canonicalizes — canonicalJSON only sorts decoded maps, so structs must
// round-trip first (the wire_corpus_test.go pattern).
func canonicalStruct(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return canonicalJSON(t, json.RawMessage(b))
}

func TestHttpShapesClaimedSchedule(t *testing.T) {
	shapes := loadHttpShapes(t)

	var cron wire.ClaimedSchedule
	if err := json.Unmarshal(shapes["claimedScheduleCron"], &cron); err != nil {
		t.Fatalf("strict decode claimedScheduleCron: %v", err)
	}
	if cron.ID != "job_cron_1" {
		t.Errorf("ID = %q", cron.ID)
	}
	if cron.Kind != wire.ScheduleKindCron {
		t.Errorf("Kind = %q", cron.Kind)
	}
	if cron.DueAt != 1794470400000 {
		t.Errorf("DueAt = %d", cron.DueAt)
	}
	if len(cron.Txn.Steps) != 0 {
		t.Errorf("Txn.Steps = %v", cron.Txn.Steps)
	}
	if cron.Cron == nil || *cron.Cron != "0 5 * * *" {
		t.Errorf("Cron = %v", cron.Cron)
	}
	if cron.Tz == nil || *cron.Tz != "America/Los_Angeles" {
		t.Errorf("Tz = %v", cron.Tz)
	}
	if cron.EveryMs != nil {
		t.Errorf("EveryMs = %v", cron.EveryMs)
	}
	if cron.LeaseGeneration != 7 {
		t.Errorf("LeaseGeneration = %d", cron.LeaseGeneration)
	}
	if cron.LeaseDeadlineMs != 1794470500000 {
		t.Errorf("LeaseDeadlineMs = %d", cron.LeaseDeadlineMs)
	}
	// Round-trip parity: re-marshal and require canonical structural
	// equality with the fixture instance (key order irrelevant, omitempty
	// parity exact).
	if got, want := canonicalStruct(t, cron), canonicalJSON(t, json.RawMessage(shapes["claimedScheduleCron"])); got != want {
		t.Errorf("claimedScheduleCron re-marshal drift:\n got  %s\n want %s", got, want)
	}

	var oneshot wire.ClaimedSchedule
	if err := json.Unmarshal(shapes["claimedScheduleOneshot"], &oneshot); err != nil {
		t.Fatalf("strict decode claimedScheduleOneshot: %v", err)
	}
	if oneshot.Cron != nil || oneshot.Tz != nil || oneshot.EveryMs != nil {
		t.Errorf("oneshot optionals = %v %v %v", oneshot.Cron, oneshot.Tz, oneshot.EveryMs)
	}
	if oneshot.LeaseGeneration != 8 {
		t.Errorf("oneshot LeaseGeneration = %d", oneshot.LeaseGeneration)
	}
	if got, want := canonicalStruct(t, oneshot), canonicalJSON(t, json.RawMessage(shapes["claimedScheduleOneshot"])); got != want {
		t.Errorf("claimedScheduleOneshot re-marshal drift:\n got  %s\n want %s", got, want)
	}

	// Strict decoder contract: one bogus extra key MUST fail.
	bogus := make(map[string]any)
	if err := json.Unmarshal(shapes["claimedScheduleCron"], &bogus); err != nil {
		t.Fatalf("fixture decode to map: %v", err)
	}
	bogus["bogusKey"] = 1
	withBogus, err := json.Marshal(bogus)
	if err != nil {
		t.Fatalf("marshal bogus: %v", err)
	}
	if err := json.Unmarshal(withBogus, &oneshot); err == nil {
		t.Errorf("ClaimedSchedule accepted an unknown field (strict decoder contract broken)")
	}

	// claimSchedulesResponse.jobs[0] equals the cron claim instance.
	var resp struct {
		Jobs []wire.ClaimedSchedule `json:"jobs"`
	}
	if err := json.Unmarshal(shapes["claimSchedulesResponse"], &resp); err != nil {
		t.Fatalf("decode claimSchedulesResponse: %v", err)
	}
	if len(resp.Jobs) != 1 {
		t.Fatalf("claimSchedulesResponse.jobs: got %d entries, want 1", len(resp.Jobs))
	}
	if got, want := canonicalStruct(t, resp.Jobs[0]), canonicalStruct(t, cron); got != want {
		t.Errorf("claimSchedulesResponse.jobs[0] mismatch:\n got  %s\n want %s", got, want)
	}
}

func TestHttpShapesScheduleInfo(t *testing.T) {
	shapes := loadHttpShapes(t)

	var cron wire.ScheduleInfo
	if err := json.Unmarshal(shapes["scheduleInfoCron"], &cron); err != nil {
		t.Fatalf("decode scheduleInfoCron: %v", err)
	}
	if cron.ID != "job_cron_1" {
		t.Errorf("ID = %q", cron.ID)
	}
	if cron.Kind != wire.ScheduleKindCron {
		t.Errorf("Kind = %q", cron.Kind)
	}
	if cron.Cron == nil || *cron.Cron != "0 5 * * *" {
		t.Errorf("Cron = %v", cron.Cron)
	}
	if cron.Tz == nil || *cron.Tz != "America/Los_Angeles" {
		t.Errorf("Tz = %v", cron.Tz)
	}
	if cron.Status != wire.ScheduleStatusPaused {
		t.Errorf("Status = %q", cron.Status)
	}
	if cron.LastError == nil || *cron.LastError != "connection reset" {
		t.Errorf("LastError = %v", cron.LastError)
	}
	if !cron.External {
		t.Errorf("External = false, want true")
	}
	if cron.MissedCount != 2 {
		t.Errorf("MissedCount = %d", cron.MissedCount)
	}
	if cron.LastMissedAt == nil || *cron.LastMissedAt != 1794470300000 {
		t.Errorf("LastMissedAt = %v", cron.LastMissedAt)
	}
	if got, want := canonicalStruct(t, cron), canonicalJSON(t, json.RawMessage(shapes["scheduleInfoCron"])); got != want {
		t.Errorf("scheduleInfoCron re-marshal drift:\n got  %s\n want %s", got, want)
	}

	var minimal wire.ScheduleInfo
	if err := json.Unmarshal(shapes["scheduleInfoMinimal"], &minimal); err != nil {
		t.Fatalf("decode scheduleInfoMinimal: %v", err)
	}
	if minimal.Cron != nil || minimal.Tz != nil || minimal.EveryMs != nil || minimal.LastError != nil || minimal.LastMissedAt != nil {
		t.Errorf("minimal optionals not nil: %+v", minimal)
	}
	if minimal.External || minimal.MissedCount != 0 {
		t.Errorf("External = %v, MissedCount = %d, want false/0", minimal.External, minimal.MissedCount)
	}
	if got, want := canonicalStruct(t, minimal), canonicalJSON(t, json.RawMessage(shapes["scheduleInfoMinimal"])); got != want {
		t.Errorf("scheduleInfoMinimal re-marshal drift:\n got  %s\n want %s", got, want)
	}
}
