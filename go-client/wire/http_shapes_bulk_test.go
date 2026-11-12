// HTTP-shape drift verification (Go view) for the ARC-005 background bulk
// delete over wire-corpus/http-shapes.json (repo root — the single source of
// truth). Strict-decodes the fixture's bulkDeleteAccepted / bulkOpInfoRunning
// / bulkOpInfoFailed instances (unknown fields reject), asserts every field,
// requires error to be structurally ABSENT on the running instance, and
// re-marshals for canonical structural parity with the fixture.
package wire_test

import (
	"encoding/json"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestHttpShapesBulkDelete(t *testing.T) {
	shapes := loadHttpShapes(t)

	var accepted wire.BulkOpAccepted
	if err := json.Unmarshal(shapes["bulkDeleteAccepted"], &accepted); err != nil {
		t.Fatalf("strict decode bulkDeleteAccepted: %v", err)
	}
	if accepted.OpID != "bulk_01JENQ9Z7K3M2V8R5T4W6X8Y0A" {
		t.Errorf("OpID = %q", accepted.OpID)
	}
	if accepted.Matched != 1500 {
		t.Errorf("Matched = %d", accepted.Matched)
	}
	if accepted.State != "running" {
		t.Errorf("State = %q", accepted.State)
	}
	if got, want := canonicalStruct(t, accepted), canonicalJSON(t, json.RawMessage(shapes["bulkDeleteAccepted"])); got != want {
		t.Errorf("bulkDeleteAccepted re-marshal drift:\n got  %s\n want %s", got, want)
	}

	// Strict decoder contract: one bogus extra key MUST fail.
	bogusAccepted := make(map[string]any)
	if err := json.Unmarshal(shapes["bulkDeleteAccepted"], &bogusAccepted); err != nil {
		t.Fatalf("fixture decode to map: %v", err)
	}
	bogusAccepted["bogusKey"] = 1
	withBogus, err := json.Marshal(bogusAccepted)
	if err != nil {
		t.Fatalf("marshal bogus: %v", err)
	}
	if err := json.Unmarshal(withBogus, &accepted); err == nil {
		t.Errorf("BulkOpAccepted accepted an unknown field (strict decoder contract broken)")
	}

	var running wire.BulkOpInfo
	if err := json.Unmarshal(shapes["bulkOpInfoRunning"], &running); err != nil {
		t.Fatalf("strict decode bulkOpInfoRunning: %v", err)
	}
	if running.ID != "bulk_01JENQ9Z7K3M2V8R5T4W6X8Y0A" {
		t.Errorf("ID = %q", running.ID)
	}
	if running.State != "running" {
		t.Errorf("State = %q", running.State)
	}
	if running.Matched != 1500 {
		t.Errorf("Matched = %d", running.Matched)
	}
	if running.Deleted != 500 {
		t.Errorf("Deleted = %d", running.Deleted)
	}
	if running.Error != nil {
		t.Errorf("Error = %v, want nil on a running op", running.Error)
	}
	if got, want := canonicalStruct(t, running), canonicalJSON(t, json.RawMessage(shapes["bulkOpInfoRunning"])); got != want {
		t.Errorf("bulkOpInfoRunning re-marshal drift:\n got  %s\n want %s", got, want)
	}

	// error must be structurally ABSENT (not merely nil) on a running op:
	// decode the fixture instance into a generic map and inspect its keys.
	var runningKeys map[string]any
	if err := json.Unmarshal(shapes["bulkOpInfoRunning"], &runningKeys); err != nil {
		t.Fatalf("decode bulkOpInfoRunning to map: %v", err)
	}
	if _, present := runningKeys["error"]; present {
		t.Errorf("bulkOpInfoRunning carries an error key; the server omits it on running ops")
	}

	var failed wire.BulkOpInfo
	if err := json.Unmarshal(shapes["bulkOpInfoFailed"], &failed); err != nil {
		t.Fatalf("strict decode bulkOpInfoFailed: %v", err)
	}
	if failed.ID != "bulk_01JENQ9Z7K3M2V8R5T4W6X8Y0A" {
		t.Errorf("ID = %q", failed.ID)
	}
	if failed.State != "failed" {
		t.Errorf("State = %q", failed.State)
	}
	if failed.Matched != 1500 {
		t.Errorf("Matched = %d", failed.Matched)
	}
	if failed.Deleted != 500 {
		t.Errorf("Deleted = %d", failed.Deleted)
	}
	if failed.Error == nil || *failed.Error != "chunk turn failed: schema changed mid-op" {
		t.Errorf("Error = %v", failed.Error)
	}
	if got, want := canonicalStruct(t, failed), canonicalJSON(t, json.RawMessage(shapes["bulkOpInfoFailed"])); got != want {
		t.Errorf("bulkOpInfoFailed re-marshal drift:\n got  %s\n want %s", got, want)
	}

	bogusInfo := make(map[string]any)
	if err := json.Unmarshal(shapes["bulkOpInfoFailed"], &bogusInfo); err != nil {
		t.Fatalf("fixture decode to map: %v", err)
	}
	bogusInfo["bogusKey"] = 1
	withBogus, err = json.Marshal(bogusInfo)
	if err != nil {
		t.Fatalf("marshal bogus: %v", err)
	}
	if err := json.Unmarshal(withBogus, &failed); err == nil {
		t.Errorf("BulkOpInfo accepted an unknown field (strict decoder contract broken)")
	}
}
