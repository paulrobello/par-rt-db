// go-client/wire/mutation_extra_test.go
package wire

import (
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
)

// TestStepAllVariantsRoundTrip pins the op tag of every Step variant and
// requires marshal → UnmarshalStep → re-marshal to be byte-identical.
func TestStepAllVariantsRoundTrip(t *testing.T) {
	lim := 5
	minv, maxv := int64(0), int64(10)
	cases := []struct {
		name string
		step Step
		tag  string
	}{
		{"insert", StepInsert{Table: "items", Doc: obj(map[string]JSONValue{"n": Number("1")})}, `"op":"insert"`},
		{"patch", StepPatch{Table: "items", ID: "i1", Fields: obj(map[string]JSONValue{"d": Bool(true)})}, `"op":"patch"`},
		{"adjustCounter", StepAdjustCounter{Table: "items", ID: "i1", Field: "n", Delta: 2, Min: &minv, Max: &maxv, Expected: Number("5")}, `"op":"adjustCounter"`},
		{"replace", StepReplace{Table: "items", ID: "i1", Doc: obj(map[string]JSONValue{"n": Number("2")})}, `"op":"replace"`},
		{"delete", StepDelete{Table: "items", ID: "i2"}, `"op":"delete"`},
		{"undelete", StepUndelete{Table: "items", ID: "i2"}, `"op":"undelete"`},
		{"expectVersion", StepExpectVersion{Table: "items", ID: "i1", Version: 7}, `"op":"expectVersion"`},
		{"expectFields", StepExpectFields{Table: "items", ID: "i1", Expected: []FieldExpectation{{Path: []string{"a", "b"}, Value: Number("1")}}}, `"op":"expectFields"`},
		{"expectBefore", StepExpectBefore{DeadlineMs: 123}, `"op":"expectBefore"`},
		{"expectAbsent", StepExpectAbsent{Table: "items", Index: "by_a", Eq: []JSONValue{String("x")}}, `"op":"expectAbsent"`},
		{"expectNoMatch", StepExpectNoMatch{Table: "items", Filter: FilterEq{Field: "a", Value: Number("1")}}, `"op":"expectNoMatch"`},
		{"insertMany", StepInsertMany{Table: "items", Docs: []JSONValue{Object{"a": Number("1")}}}, `"op":"insertMany"`},
		{"upsertMany", StepUpsertMany{Table: "items", Index: "by_a", Docs: []JSONValue{Object{"a": Number("1")}}}, `"op":"upsertMany"`},
		{"upsert", StepUpsert{Table: "items", Index: "by_a", Eq: []JSONValue{Number("1")}, Insert: Object{"a": Number("1")}, Patch: Object{"b": Bool(true)}}, `"op":"upsert"`},
		{"patchByQuery", StepPatchByQuery{Table: "items", Filter: FilterEq{Field: "a", Value: Number("1")}, Patch: Object{"b": Bool(true)}, Limit: &lim}, `"op":"patchByQuery"`},
		{"deleteByQuery", StepDeleteByQuery{Table: "items", Filter: FilterEq{Field: "a", Value: Number("1")}}, `"op":"deleteByQuery"`},
		{"schedule", StepSchedule{
			When: WhenInterval{EveryMs: 60000},
			Txn:  Transaction{Steps: []Step{StepDelete{Table: "items", ID: "i2"}}},
		}, `"op":"schedule"`},
		{"cancelSchedule", StepCancelSchedule{ID: "s1"}, `"op":"cancelSchedule"`},
		{"startWorkflow", StepStartWorkflow{Spec: WorkflowSpec{Name: "wf", Steps: []WorkflowStepSpec{{Txn: &Transaction{Steps: []Step{StepDelete{Table: "items", ID: "i1"}}}}}}}, `"op":"startWorkflow"`},
		{"cancelWorkflow", StepCancelWorkflow{ID: "w1"}, `"op":"cancelWorkflow"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.step)
			if err != nil {
				t.Fatal(err)
			}
			if !bytesHas(b, tc.tag) {
				t.Fatalf("tag wrong: %s", b)
			}
			out, err := UnmarshalStep(b)
			if err != nil {
				t.Fatal(err)
			}
			if reflect.TypeOf(out) != reflect.TypeOf(tc.step) {
				t.Fatalf("decoded %T, want %T", out, reflect.TypeOf(tc.step))
			}
			rb, err := json.Marshal(out)
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != string(rb) {
				t.Fatalf("round trip drifted:\n%s\n%s", b, rb)
			}
		})
	}
}

// TestScheduleWhenAllVariants pins the "type" tag of every ScheduleWhen
// variant, byte-exact shapes, and round trips through UnmarshalScheduleWhen.
func TestScheduleWhenAllVariants(t *testing.T) {
	tz := "UTC"
	cases := []struct {
		name string
		when ScheduleWhen
		want string
	}{
		{"afterMs", WhenAfterMs{Ms: 5}, `{"ms":5,"type":"afterMs"}`},
		{"runAt", WhenRunAt{Ms: 6}, `{"ms":6,"type":"runAt"}`},
		{"cron-with-tz", WhenCron{Expr: "0 * * * *", Tz: &tz}, `{"expr":"0 * * * *","type":"cron","tz":"UTC"}`},
		{"cron-no-tz", WhenCron{Expr: "0 * * * *"}, `{"expr":"0 * * * *","type":"cron"}`},
		{"interval", WhenInterval{EveryMs: 10}, `{"everyMs":10,"type":"interval"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.when)
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != tc.want {
				t.Fatalf("when shape wrong:\n got %s\nwant %s", b, tc.want)
			}
			out, err := UnmarshalScheduleWhen(b)
			if err != nil {
				t.Fatal(err)
			}
			rb, err := json.Marshal(out)
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != string(rb) {
				t.Fatalf("round trip drifted:\n%s\n%s", b, rb)
			}
		})
	}
	// Optional tz omitted vs present.
	if _, err := UnmarshalScheduleWhen([]byte(`{"type":"cron","expr":"0 * * * *"}`)); err != nil {
		t.Fatal(err)
	}
	// Tag dispatch failures.
	if _, err := UnmarshalScheduleWhen([]byte(`{"expr":"x"}`)); err == nil {
		t.Fatal("missing type tag must be rejected")
	}
	if _, err := UnmarshalScheduleWhen([]byte(`{"type":3}`)); err == nil {
		t.Fatal("non-string type tag must be rejected")
	}
	if _, err := UnmarshalScheduleWhen([]byte(`{"type":"nope"}`)); err == nil {
		t.Fatal("unknown when tag must be rejected")
	}
	// Per-variant strict rejection and missing required fields.
	if _, err := UnmarshalScheduleWhen([]byte(`{"type":"afterMs","ms":1,"zz":1}`)); err == nil {
		t.Fatal("afterMs unknown field must be rejected")
	}
	if _, err := UnmarshalScheduleWhen([]byte(`{"type":"runAt","zz":1}`)); err == nil {
		t.Fatal("runAt unknown field must be rejected")
	}
	if _, err := UnmarshalScheduleWhen([]byte(`{"type":"cron","expr":"x","tz":"UTC","zz":1}`)); err == nil {
		t.Fatal("cron unknown field must be rejected")
	}
	if _, err := UnmarshalScheduleWhen([]byte(`{"type":"interval","everyMs":1,"zz":1}`)); err == nil {
		t.Fatal("interval unknown field must be rejected")
	}
}

// TestStepResultWireShapes pins StepResult marshal/decode behavior: null for
// the empty result, the ids/inserted pair for bulk steps, scalar passthrough.
func TestStepResultWireShapes(t *testing.T) {
	// Empty result marshals as JSON null.
	b, err := json.Marshal(StepResult{})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `null` {
		t.Fatalf("empty result must be null, got %s", b)
	}
	// Bulk result: ids + parallel inserted flags.
	ids := []string{"a", "b"}
	b, err = json.Marshal(StepResult{IDs: ids, InsertedMany: []bool{true, false}})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"ids":["a","b"],"inserted":[true,false]}` {
		t.Fatalf("bulk result shape wrong: %s", b)
	}
	// Bulk without inserted flags.
	b, err = json.Marshal(StepResult{IDs: ids})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"ids":["a","b"]}` {
		t.Fatalf("ids-only shape wrong: %s", b)
	}
	// Scalar result passthrough.
	s := "i1"
	p := int64(3)
	b, err = json.Marshal(StepResult{ID: &s, Patched: &p})
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"id":"i1"`) || !bytesHas(b, `"patched":3`) {
		t.Fatalf("scalar result wrong: %s", b)
	}
	// Decode: ids+inserted, ids-only, null, scalar.
	var r StepResult
	if err := json.Unmarshal([]byte(`{"ids":["a"],"inserted":[true]}`), &r); err != nil || len(r.IDs) != 1 || len(r.InsertedMany) != 1 {
		t.Fatalf("bulk decode: %v %+v", err, r)
	}
	r = StepResult{}
	if err := json.Unmarshal([]byte(`{"ids":["a"]}`), &r); err != nil || len(r.IDs) != 1 || r.InsertedMany != nil {
		t.Fatalf("ids-only decode: %v %+v", err, r)
	}
	r = StepResult{}
	if err := json.Unmarshal([]byte(`null`), &r); err != nil || r.IDs != nil {
		t.Fatalf("null decode: %v %+v", err, r)
	}
	if err := json.Unmarshal([]byte(`{"ids":["a"],"inserted":"no"}`), &r); err == nil {
		t.Fatal("bad inserted flags must be rejected")
	}
}

// TestFieldExpectationWire pins FieldExpectation marshal/decode: nil path
// marshals as [], and the decode-side required-field validation errors.
func TestFieldExpectationWire(t *testing.T) {
	// Nil path marshals as empty array.
	b, err := json.Marshal(FieldExpectation{Path: nil, Value: Number("1")})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"path":[],"value":1}` {
		t.Fatalf("nil path shape wrong: %s", b)
	}
	// Valid decode round trip.
	var fe FieldExpectation
	valid := []byte(`{"path":["a","b"],"value":{"x":1}}`)
	if err := json.Unmarshal(valid, &fe); err != nil {
		t.Fatal(err)
	}
	if len(fe.Path) != 2 || fe.Path[1] != "b" {
		t.Fatalf("path wrong: %#v", fe.Path)
	}
	rb, err := json.Marshal(fe)
	if err != nil {
		t.Fatal(err)
	}
	if string(rb) != `{"path":["a","b"],"value":{"x":1}}` {
		t.Fatalf("round trip wrong: %s", rb)
	}
	// Required-field validation errors.
	for _, in := range []string{
		`{"path":null,"value":1}`,
		`{"value":1}`,
		`{"path":["a"]}`,
	} {
		var fe FieldExpectation
		if err := json.Unmarshal([]byte(in), &fe); err == nil {
			t.Fatalf("must be rejected: %s", in)
		}
	}
	// Empty path array is accepted (decodes to an empty path).
	var feEmpty FieldExpectation
	if err := json.Unmarshal([]byte(`{"path":[],"value":1}`), &feEmpty); err != nil || len(feEmpty.Path) != 0 {
		t.Fatalf("empty path array decodes empty: %v %#v", err, feEmpty)
	}
	// Path must be an array; keys must be strings.
	var fe2 FieldExpectation
	if err := json.Unmarshal([]byte(`{"path":"a","value":1}`), &fe2); err == nil {
		t.Fatal("non-array path must be rejected")
	}
	if err := json.Unmarshal([]byte(`{"path":[1],"value":1}`), &fe2); err == nil {
		t.Fatal("non-string path key must be rejected")
	}
	// Strict unknown-field rejection.
	if err := json.Unmarshal([]byte(`{"path":["a"],"value":1,"zz":1}`), &fe2); err == nil {
		t.Fatal("unknown field must be rejected")
	}
}

// TestExpectStepDecodeErrors covers expectFields/expectBefore decode
// validation and strict rejection.
func TestExpectStepDecodeErrors(t *testing.T) {
	if _, err := UnmarshalStep([]byte(`{"op":"expectFields","expected":[]}`)); err == nil {
		t.Fatal("expectFields without table/id/expected presence must be rejected")
	}
	if _, err := UnmarshalStep([]byte(`{"op":"expectFields","table":"t","id":"i","expected":[],"zz":1}`)); err == nil {
		t.Fatal("expectFields unknown field must be rejected")
	}
	if _, err := UnmarshalStep([]byte(`{"op":"expectBefore"}`)); err == nil {
		t.Fatal("expectBefore without deadlineMs must be rejected")
	}
	if _, err := UnmarshalStep([]byte(`{"op":"expectBefore","deadlineMs":1,"zz":1}`)); err == nil {
		t.Fatal("expectBefore unknown field must be rejected")
	}
	// adjustCounter: optional fields survive round trip; expected omitted when absent.
	var s Step
	in := []byte(`{"op":"adjustCounter","table":"t","id":"i","field":"n","delta":1,"min":0,"max":9}`)
	s, err := UnmarshalStep(in)
	if err != nil {
		t.Fatal(err)
	}
	ac, ok := s.(StepAdjustCounter)
	if !ok || ac.Min == nil || ac.Max == nil || ac.Expected != nil {
		t.Fatalf("adjustCounter decode wrong: %#v", s)
	}
	rb, err := json.Marshal(ac)
	if err != nil {
		t.Fatal(err)
	}
	if string(rb) != `{"delta":1,"field":"n","id":"i","max":9,"min":0,"op":"adjustCounter","table":"t"}` {
		t.Fatalf("adjustCounter shape wrong: %s", rb)
	}
	// expectAbsent element decode failure: eq element not decodable is impossible
	// with well-formed JSON; strict rejection is the reachable branch.
	if _, err := UnmarshalStep([]byte(`{"op":"expectAbsent","table":"t","index":"i","eq":[],"zz":1}`)); err == nil {
		t.Fatal("expectAbsent unknown field must be rejected")
	}
	// upsert strict rejection and missing-field failure.
	if _, err := UnmarshalStep([]byte(`{"op":"upsert","table":"t","index":"i","eq":[],"insert":{},"patch":{},"zz":1}`)); err == nil {
		t.Fatal("upsert unknown field must be rejected")
	}
	if _, err := UnmarshalStep([]byte(`{"op":"upsert","table":"t","index":"i","eq":[],"insert":{}}`)); err == nil {
		t.Fatal("upsert without patch must be rejected")
	}
	// patchByQuery / deleteByQuery strict rejection and bad filter.
	for _, op := range []string{"patchByQuery", "deleteByQuery"} {
		in := fmt.Sprintf(`{"op":%q,"table":"t","filter":{"op":"eq","field":"a","value":1},"patch":{},"limit":2,"zz":1}`, op)
		if _, err := UnmarshalStep([]byte(in)); err == nil {
			t.Fatalf("unknown field must be rejected: %s", in)
		}
		bad := fmt.Sprintf(`{"op":%q,"table":"t","filter":{"op":"nope"}}`, op)
		if _, err := UnmarshalStep([]byte(bad)); err == nil {
			t.Fatalf("bad filter must propagate: %s", bad)
		}
	}
	// schedule: strict rejection and bad nested when.
	if _, err := UnmarshalStep([]byte(`{"op":"schedule","when":{"type":"interval","everyMs":1},"txn":{"steps":[]},"zz":1}`)); err == nil {
		t.Fatal("schedule unknown field must be rejected")
	}
	if _, err := UnmarshalStep([]byte(`{"op":"schedule","when":{"type":"nope"},"txn":{"steps":[]}}`)); err == nil {
		t.Fatal("bad when must propagate")
	}
	// startWorkflow: strict rejection, spec strict rejection, spec required.
	if _, err := UnmarshalStep([]byte(`{"op":"startWorkflow","spec":{"name":"w","steps":[]},"zz":1}`)); err == nil {
		t.Fatal("startWorkflow unknown field must be rejected")
	}
	if _, err := UnmarshalStep([]byte(`{"op":"startWorkflow","spec":{"name":"w","steps":[],"zz":1}}`)); err == nil {
		t.Fatal("spec unknown field must be rejected")
	}
	if _, err := UnmarshalStep([]byte(`{"op":"startWorkflow"}`)); err == nil {
		t.Fatal("missing spec must be rejected")
	}
	// cancelSchedule/cancelWorkflow strict rejection.
	for _, op := range []string{"cancelSchedule", "cancelWorkflow"} {
		in := fmt.Sprintf(`{"op":%q,"id":"x","zz":1}`, op)
		if _, err := UnmarshalStep([]byte(in)); err == nil {
			t.Fatalf("unknown field must be rejected: %s", in)
		}
	}
	// Transaction decode: strict rejection at the transaction level and
	// propagation of a bad step.
	var txn Transaction
	if err := json.Unmarshal([]byte(`{"steps":[],"zz":1}`), &txn); err == nil {
		t.Fatal("transaction unknown field must be rejected")
	}
	if err := json.Unmarshal([]byte(`{"steps":[{"op":"nope"}]}`), &txn); err == nil {
		t.Fatal("bad step must propagate")
	}
	// UnmarshalStep dispatch failures.
	if _, err := UnmarshalStep([]byte(`{}`)); err == nil {
		t.Fatal("missing op tag must be rejected")
	}
	if _, err := UnmarshalStep([]byte(`{"op":9}`)); err == nil {
		t.Fatal("non-string op tag must be rejected")
	}
	// Per-variant strict unknown-field rejection for the remaining ops.
	// (Variants whose other fields are plain strings/ints have no
	// required-field checks — a missing field decodes to its zero value.)
	perOp := []struct{ op, strict, missing string }{
		{"insert", `{"op":"insert","table":"t","doc":{},"zz":1}`, `{"op":"insert","table":"t"}`},
		{"patch", `{"op":"patch","table":"t","id":"i","fields":{},"zz":1}`, `{"op":"patch","table":"t","id":"i"}`},
		{"adjustCounter", `{"op":"adjustCounter","table":"t","id":"i","field":"n","delta":1,"zz":1}`, ""},
		{"delete", `{"op":"delete","table":"t","id":"i","zz":1}`, ""},
		{"replace", `{"op":"replace","table":"t","id":"i","doc":{},"zz":1}`, `{"op":"replace","table":"t","id":"i"}`},
		{"undelete", `{"op":"undelete","table":"t","id":"i","zz":1}`, ""},
		{"expectVersion", `{"op":"expectVersion","table":"t","id":"i","version":1,"zz":1}`, ""},
		{"expectNoMatch", `{"op":"expectNoMatch","table":"t","filter":{"op":"eq","field":"a","value":1},"zz":1}`, `{"op":"expectNoMatch","table":"t"}`},
		{"insertMany", `{"op":"insertMany","table":"t","docs":[],"zz":1}`, ""},
		{"upsertMany", `{"op":"upsertMany","table":"t","index":"i","docs":[],"zz":1}`, ""},
		{"upsert", `{"op":"upsert","table":"t","index":"i","eq":[],"insert":{},"patch":{},"zz":1}`, `{"op":"upsert","table":"t","index":"i","eq":[],"insert":{}}`},
	}
	for _, tc := range perOp {
		if _, err := UnmarshalStep([]byte(tc.strict)); err == nil {
			t.Fatalf("%s: unknown field must be rejected", tc.op)
		}
		if tc.missing == "" {
			continue
		}
		if _, err := UnmarshalStep([]byte(tc.missing)); err == nil {
			t.Fatalf("%s: missing required field must be rejected", tc.op)
		}
	}
	// patchByQuery / deleteByQuery: missing raw-typed required fields fail.
	if _, err := UnmarshalStep([]byte(`{"op":"patchByQuery","table":"t","filter":{"op":"eq","field":"a","value":1}}`)); err == nil {
		t.Fatal("patchByQuery without patch must be rejected")
	}
	if _, err := UnmarshalStep([]byte(`{"op":"deleteByQuery","table":"t"}`)); err == nil {
		t.Fatal("deleteByQuery without filter must be rejected")
	}
	// Nil-slice marshal normalization: docs/expected serialize as [] not null.
	b, err := json.Marshal(StepExpectFields{Table: "t", ID: "i"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"expected":[]`) {
		t.Fatalf("nil expected must marshal as []: %s", b)
	}
	b, err = json.Marshal(StepInsertMany{Table: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"docs":[]`) {
		t.Fatalf("nil docs must marshal as []: %s", b)
	}
	b, err = json.Marshal(StepUpsertMany{Table: "t", Index: "i"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"docs":[]`) {
		t.Fatalf("nil docs must marshal as []: %s", b)
	}
	// upsert: missing insert raw blob fails.
	if _, err := UnmarshalStep([]byte(`{"op":"upsert","table":"t","index":"i","eq":[]}`)); err == nil {
		t.Fatal("upsert without insert must be rejected")
	}
	// StepRetry strict rejection.
	var retry StepRetry
	if err := json.Unmarshal([]byte(`{"maxAttempts":1,"initialRetryMs":0,"maxRetryMs":0,"zz":1}`), &retry); err == nil {
		t.Fatal("StepRetry unknown field must be rejected")
	}
	// schedule: a non-object txn blob fails the nested Transaction decode.
	if _, err := UnmarshalStep([]byte(`{"op":"schedule","when":{"type":"interval","everyMs":1},"txn":5}`)); err == nil {
		t.Fatal("schedule with non-object txn must be rejected")
	}
	// StepResult decode: valid JSON with a type mismatch fails in the
	// alias fallback (syntactically invalid JSON is rejected by
	// encoding/json before UnmarshalJSON is even called).
	var r StepResult
	if err := json.Unmarshal([]byte(`{"id":5}`), &r); err == nil {
		t.Fatal("type-mismatched result must be rejected")
	}
}
