// go-client/wire/mutation_test.go
package wire

import (
	"bytes"
	"encoding/json"
	"reflect"
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

func TestStepExpectNoMatchRoundTrip(t *testing.T) {
	txn := Transaction{Steps: []Step{
		StepExpectNoMatch{Table: "items", Filter: FilterEq{Field: "name", Value: String("a")}},
	}}
	b, err := json.Marshal(txn)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"steps":[{"op":"expectNoMatch","table":"items","filter":{"op":"eq","field":"name","value":"a"}}]}`
	var got, wantV map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(want), &wantV); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, wantV) {
		t.Fatalf("wire mismatch:\n got %s\nwant %s", b, want)
	}
	step, err := UnmarshalStep([]byte(`{"op":"expectNoMatch","table":"items","filter":{"op":"eq","field":"name","value":"a"}}`))
	if err != nil {
		t.Fatal(err)
	}
	dec, ok := step.(StepExpectNoMatch)
	if !ok || dec.Table != "items" {
		t.Fatalf("decoded %#v", step)
	}
	if _, ok := dec.Filter.(FilterEq); !ok {
		t.Fatalf("filter not decoded: %#v", dec.Filter)
	}
}

func TestStepExpectNoMatchRejectsUnknownField(t *testing.T) {
	_, err := UnmarshalStep([]byte(`{"op":"expectNoMatch","table":"items","filter":{"op":"eq","field":"a","value":1},"extra":1}`))
	if err == nil {
		t.Fatal("unknown field must be rejected")
	}
	if _, err := UnmarshalStep([]byte(`{"op":"noSuchStep"}`)); err == nil {
		t.Fatal("unknown op must still error")
	}
}

func TestStepInsertManyUpsertManyRoundTrip(t *testing.T) {
	txn := Transaction{Steps: []Step{
		StepInsertMany{Table: "items", Docs: []JSONValue{Object{"name": String("a")}}},
		StepUpsertMany{Table: "items", Index: "by_name", Docs: []JSONValue{Object{"name": String("a")}}},
	}}
	b, err := json.Marshal(txn)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"steps":[{"op":"insertMany","table":"items","docs":[{"name":"a"}]},{"op":"upsertMany","table":"items","index":"by_name","docs":[{"name":"a"}]}]}`
	var got, wantV map[string]any
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(want), &wantV); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, wantV) {
		t.Fatalf("wire mismatch:\n got %s\nwant %s", b, want)
	}
	var back Transaction
	if err := json.Unmarshal([]byte(want), &back); err != nil {
		t.Fatal(err)
	}
	if _, ok := back.Steps[0].(StepInsertMany); !ok {
		t.Fatalf("decoded %#v", back.Steps[0])
	}
	if _, err := UnmarshalStep([]byte(`{"op":"insertMany","table":"items","docs":[],"extra":1}`)); err == nil {
		t.Fatal("unknown field must be rejected")
	}
	var r StepResult
	if err := json.Unmarshal([]byte(`{"ids":["x","y"],"inserted":[true,false]}`), &r); err != nil || len(r.IDs) != 2 || len(r.InsertedMany) != 2 {
		t.Fatalf("result decode: %v %+v", err, r)
	}
}
