// go-client/wire/mutation_test.go
package wire

import (
	"bytes"
	"encoding/json"
	"testing"
)

func obj(m map[string]JSONValue) JSONValue { return Object(m) }

func TestTransactionExactJSON(t *testing.T) {
	txn := Transaction{Steps: []Step{
		StepInsert{Table: "items", Doc: obj(map[string]JSONValue{"n": Number("1")})},
		StepPatch{Table: "items", ID: "i1", Fields: obj(map[string]JSONValue{"done": Bool(true)})},
		StepDelete{Table: "items", ID: "i2"},
	}}
	b, err := json.Marshal(txn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"op":"insert"`) || !bytesHas(b, `"doc":{"n":1}`) {
		t.Fatalf("insert step shape wrong: %s", b)
	}
	if !bytesHas(b, `"op":"patch"`) || !bytesHas(b, `"fields":{"done":true}`) {
		t.Fatalf("patch step shape wrong: %s", b)
	}
}

func TestStepScheduleWithInterval(t *testing.T) {
	ext := true
	txn := Transaction{Steps: []Step{
		StepSchedule{
			When:     WhenInterval{EveryMs: 60000},
			Txn:      Transaction{Steps: []Step{StepInsert{Table: "items", Doc: obj(map[string]JSONValue{})}}},
			External: &ext,
		},
	}}
	b, err := json.Marshal(txn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"op":"schedule"`) || !bytesHas(b, `"type":"interval"`) || !bytesHas(b, `"everyMs":60000`) || !bytesHas(b, `"external":true`) {
		t.Fatalf("schedule shape wrong: %s", b)
	}
}

func TestStepResultDecode(t *testing.T) {
	var results []StepResult
	if err := json.Unmarshal([]byte(`[{"id":"i1","inserted":true},null]`), &results); err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("want 2 results, got %d", len(results))
	}
	if results[0].ID == nil || *results[0].ID != "i1" || results[0].Inserted == nil || !*results[0].Inserted {
		t.Fatalf("upsert result wrong: %+v", results[0])
	}
	var nullResult StepResult
	if err := json.Unmarshal([]byte(`null`), &nullResult); err != nil {
		t.Fatal(err)
	}
	if nullResult.ID != nil {
		t.Fatalf("null result must decode to zero value: %+v", nullResult)
	}
}

func bytesHas(b []byte, s string) bool { return bytes.Contains(b, []byte(s)) }
