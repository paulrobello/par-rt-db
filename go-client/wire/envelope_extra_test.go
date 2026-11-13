// go-client/wire/envelope_extra_test.go
package wire

import (
	"encoding/json"
	"reflect"
	"testing"
)

var (
	envTok    = "t"
	envVer    = uint32(3)
	envIdem   = "k"
	envExt    = true
	envSince  = int64(7)
	envStatus = WorkflowStatusRunning
	envTxn    = Transaction{Steps: []Step{StepDelete{Table: "items", ID: "i1"}}}
	envSpec   = WorkflowSpec{Name: "wf", Steps: []WorkflowStepSpec{{Txn: &envTxn}}}
)

// TestClientMessageAllVariantsRoundTrip marshals every ClientMessage
// variant, pins its "type" tag, and requires dispatcher decode → re-marshal
// to be byte-identical.
func TestClientMessageAllVariantsRoundTrip(t *testing.T) {
	cases := []struct {
		name string
		msg  ClientMessage
		tag  string
	}{
		{"auth", ClientAuth{DB: "d1", Token: &envTok, ProtocolVersion: &envVer}, `"type":"auth"`},
		{"subscribe", ClientSubscribe{QueryID: "q1", Query: Query{Table: "items"}}, `"type":"subscribe"`},
		{"unsubscribe", ClientUnsubscribe{QueryID: "q1"}, `"type":"unsubscribe"`},
		{"mutate", ClientMutate{MutID: "m1", IdempotencyKey: &envIdem, Txn: envTxn}, `"type":"mutate"`},
		{"schedule", ClientSchedule{ScheduleID: "c1", When: WhenAfterMs{Ms: 5}, Txn: envTxn, External: &envExt}, `"type":"schedule"`},
		{"cancelSchedule", ClientCancelSchedule{ScheduleID: "c1", ID: "s1"}, `"type":"cancelSchedule"`},
		{"pauseSchedule", ClientPauseSchedule{ScheduleID: "c1", ID: "s1"}, `"type":"pauseSchedule"`},
		{"resumeSchedule", ClientResumeSchedule{ScheduleID: "c1", ID: "s1"}, `"type":"resumeSchedule"`},
		{"listSchedules", ClientListSchedules{ScheduleID: "c1"}, `"type":"listSchedules"`},
		{"startWorkflow", ClientStartWorkflow{WorkflowID: "w1", Spec: envSpec}, `"type":"startWorkflow"`},
		{"cancelWorkflow", ClientCancelWorkflow{WorkflowID: "w1", ID: "r1"}, `"type":"cancelWorkflow"`},
		{"signalWorkflow", ClientSignalWorkflow{WorkflowID: "w1", ID: "r1", Name: "sig"}, `"type":"signalWorkflow"`},
		{"listWorkflows", ClientListWorkflows{WorkflowID: "w1", Status: &envStatus}, `"type":"listWorkflows"`},
		{"presence", ClientPresence{Room: "doc:1", State: Object{"x": Number("1")}}, `"type":"presence"`},
		{"presenceState", ClientPresenceState{Room: "doc:1", State: Object{"x": Number("1")}}, `"type":"presenceState"`},
		{"leavePresence", ClientLeavePresence{Room: "doc:1"}, `"type":"leavePresence"`},
		{"feedSubscribe", ClientFeedSubscribe{FeedID: "f1", Since: &envSince, Tables: []string{"items"}}, `"type":"feedSubscribe"`},
		{"feedAck", ClientFeedAck{FeedID: "f1", Seq: 3}, `"type":"feedAck"`},
		{"feedUnsubscribe", ClientFeedUnsubscribe{FeedID: "f1"}, `"type":"feedUnsubscribe"`},
		{"ping", ClientPing{}, `"type":"ping"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.msg)
			if err != nil {
				t.Fatal(err)
			}
			if !bytesHas(b, tc.tag) {
				t.Fatalf("tag wrong: %s", b)
			}
			out, err := UnmarshalClientMessage(b)
			if err != nil {
				t.Fatal(err)
			}
			if reflect.TypeOf(out) != reflect.TypeOf(tc.msg) {
				t.Fatalf("decoded %T, want %T", out, tc.msg)
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

// TestServerMessageAllVariantsRoundTrip does the same for ServerMessage.
func TestServerMessageAllVariantsRoundTrip(t *testing.T) {
	pv := uint32(3)
	sleepUntil := int64(99)
	errEnv := ErrorEnvelope{Code: "BAD", Message: "no"}
	sched := ScheduleInfo{ID: "s1", Kind: ScheduleKindOneshot, DueAt: 1, Status: ScheduleStatusPending, CreatedAt: 2}
	info := WorkflowInfo{ID: "r1", Name: "wf", Status: WorkflowStatusRunning, CurrentStep: 0, StepCount: 2, Attempts: 1, CreatedAt: 1, UpdatedAt: 2, SleepUntil: &sleepUntil}
	member := PresenceMember{ConnectionID: "c1", User: AuthedUser{Kind: UserKindMachine}, State: Null{}}
	cases := []struct {
		name string
		msg  ServerMessage
		tag  string
	}{
		{"authOk", ServerAuthOk{User: AuthedUser{Kind: UserKindUser}, ProtocolVersion: &pv}, `"type":"authOk"`},
		{"authErr", ServerAuthErr{Error: errEnv}, `"type":"authErr"`},
		{"queryUpdate", ServerQueryUpdate{QueryID: "q1", Result: Object{"docs": Array{}}}, `"type":"queryUpdate"`},
		{"mutateOk", ServerMutateOk{MutID: "m1", Results: []JSONValue{Null{}}}, `"type":"mutateOk"`},
		{"mutateErr", ServerMutateErr{MutID: "m1", Error: errEnv}, `"type":"mutateErr"`},
		{"subscribeErr", ServerSubscribeErr{QueryID: "q1", Error: errEnv}, `"type":"subscribeErr"`},
		{"scheduleOk", ServerScheduleOk{ScheduleID: "c1", ID: "s1"}, `"type":"scheduleOk"`},
		{"scheduleErr", ServerScheduleErr{ScheduleID: "c1", Error: errEnv}, `"type":"scheduleErr"`},
		{"scheduleAck", ServerScheduleAck{ScheduleID: "c1", OK: true}, `"type":"scheduleAck"`},
		{"listSchedulesOk", ServerListSchedulesOk{ScheduleID: "c1", Schedules: []ScheduleInfo{sched}}, `"type":"listSchedulesOk"`},
		{"startWorkflowOk", ServerStartWorkflowOk{WorkflowID: "w1", Info: info}, `"type":"startWorkflowOk"`},
		{"startWorkflowErr", ServerStartWorkflowErr{WorkflowID: "w1", Error: errEnv}, `"type":"startWorkflowErr"`},
		{"workflowAck", ServerWorkflowAck{WorkflowID: "w1", OK: true}, `"type":"workflowAck"`},
		{"listWorkflowsOk", ServerListWorkflowsOk{WorkflowID: "w1", Workflows: []WorkflowInfo{info}}, `"type":"listWorkflowsOk"`},
		{"presenceSnapshot", ServerPresenceSnapshot{Room: "doc:1", Members: []PresenceMember{member}}, `"type":"presenceSnapshot"`},
		{"presenceDelta", ServerPresenceDelta{Room: "doc:1", Seq: 1, Left: []string{"c1"}}, `"type":"presenceDelta"`},
		{"presenceErr", ServerPresenceErr{Room: "doc:1", Error: errEnv}, `"type":"presenceErr"`},
		{"feedEvents", ServerFeedEvents{FeedID: "f1", FeedEpoch: "e1", Head: 3}, `"type":"feedEvents"`},
		{"feedErr", ServerFeedErr{FeedID: "f1", Error: errEnv}, `"type":"feedErr"`},
		{"pong", ServerPong{}, `"type":"pong"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.msg)
			if err != nil {
				t.Fatal(err)
			}
			if !bytesHas(b, tc.tag) {
				t.Fatalf("tag wrong: %s", b)
			}
			out, err := UnmarshalServerMessage(b)
			if err != nil {
				t.Fatal(err)
			}
			if reflect.TypeOf(out) != reflect.TypeOf(tc.msg) {
				t.Fatalf("decoded %T, want %T", out, tc.msg)
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

// TestEnvelopeDecodeErrors exercises the dispatcher and per-variant decode
// error branches across both vocabularies.
func TestEnvelopeDecodeErrors(t *testing.T) {
	// Dispatcher failures: missing tag, non-string tag.
	if _, err := UnmarshalClientMessage([]byte(`{"db":"d"}`)); err == nil {
		t.Fatal("missing type tag must be rejected")
	}
	if _, err := UnmarshalClientMessage([]byte(`{"type":1}`)); err == nil {
		t.Fatal("non-string type tag must be rejected")
	}
	if _, err := UnmarshalServerMessage([]byte(`{"seq":1}`)); err == nil {
		t.Fatal("missing type tag must be rejected")
	}
	if _, err := UnmarshalServerMessage([]byte(`{"type":true}`)); err == nil {
		t.Fatal("non-string type tag must be rejected")
	}
	// mutate: strict rejection and txn type mismatch.
	if _, err := UnmarshalClientMessage([]byte(`{"type":"mutate","mutId":"m","txn":{"steps":[]},"zz":1}`)); err == nil {
		t.Fatal("mutate unknown field must be rejected")
	}
	if _, err := UnmarshalClientMessage([]byte(`{"type":"mutate","mutId":"m","txn":5}`)); err == nil {
		t.Fatal("mutate with non-object txn must be rejected")
	}
	// schedule: bad when and bad txn propagate.
	if _, err := UnmarshalClientMessage([]byte(`{"type":"schedule","scheduleId":"c","when":{"type":"nope"},"txn":{"steps":[]}}`)); err == nil {
		t.Fatal("schedule with bad when must be rejected")
	}
	if _, err := UnmarshalClientMessage([]byte(`{"type":"schedule","scheduleId":"c","when":{"type":"afterMs","ms":1},"txn":7}`)); err == nil {
		t.Fatal("schedule with non-object txn must be rejected")
	}
	// signalWorkflow: payload decode failure and unknown field.
	if _, err := UnmarshalClientMessage([]byte(`{"type":"signalWorkflow","workflowId":"w","id":"r","name":"n","payload":{}}`)); err != nil {
		t.Fatalf("signalWorkflow without payload decodes: %v", err)
	}
	if _, err := UnmarshalClientMessage([]byte(`{"type":"signalWorkflow","workflowId":"w","id":"r","name":"n","zz":1}`)); err == nil {
		t.Fatal("signalWorkflow unknown field must be rejected")
	}
	// presence: optional state, then unknown field rejection.
	if _, err := UnmarshalClientMessage([]byte(`{"type":"presence","room":"r"}`)); err != nil {
		t.Fatalf("presence without state decodes: %v", err)
	}
	if _, err := UnmarshalClientMessage([]byte(`{"type":"presence","room":"r","zz":1}`)); err == nil {
		t.Fatal("presence unknown field must be rejected")
	}
	// presenceState: state required; unknown field rejected; ttlMs round trips.
	if _, err := UnmarshalClientMessage([]byte(`{"type":"presenceState","room":"r"}`)); err == nil {
		t.Fatal("presenceState without state must be rejected")
	}
	if _, err := UnmarshalClientMessage([]byte(`{"type":"presenceState","room":"r","state":{},"ttlMs":1000}`)); err != nil {
		t.Fatalf("presenceState with ttlMs decodes: %v", err)
	}
	if _, err := UnmarshalClientMessage([]byte(`{"type":"presenceState","room":"r","state":{},"zz":1}`)); err == nil {
		t.Fatal("presenceState unknown field must be rejected")
	}
	// ping: unit variant rejects unknown fields.
	if _, err := UnmarshalClientMessage([]byte(`{"type":"ping","zz":1}`)); err == nil {
		t.Fatal("ping unknown field must be rejected")
	}
	// queryUpdate: result required.
	if _, err := UnmarshalServerMessage([]byte(`{"type":"queryUpdate","queryId":"q"}`)); err == nil {
		t.Fatal("queryUpdate without result must be rejected")
	}
	// mutateOk: strict rejection.
	if _, err := UnmarshalServerMessage([]byte(`{"type":"mutateOk","mutId":"m","results":[],"zz":1}`)); err == nil {
		t.Fatal("mutateOk unknown field must be rejected")
	}
	// feedEvents: nil ops normalizes to [] in both directions.
	b, err := json.Marshal(ServerFeedEvents{FeedID: "f", FeedEpoch: "e", Head: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"ops":[]`) {
		t.Fatalf("nil ops must marshal as []: %s", b)
	}
	fe, err := UnmarshalServerMessage([]byte(`{"type":"feedEvents","feedId":"f","feedEpoch":"e","ops":null,"head":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if ev, ok := fe.(ServerFeedEvents); !ok || ev.Ops == nil {
		t.Fatalf("nil ops must decode as [], got %#v", fe)
	}
}

// TestEnvelopeDecodeErrorsCornerCases covers the last reachable dispatcher
// and variant error arms.
func TestEnvelopeDecodeErrorsCornerCases(t *testing.T) {
	if _, err := UnmarshalClientMessage([]byte(`{"type":"nope"}`)); err == nil {
		t.Fatal("unknown client tag must be rejected")
	}
	if _, err := UnmarshalClientMessage([]byte(`{"type":"schedule","scheduleId":"c","txn":{"steps":[]}}`)); err == nil {
		t.Fatal("schedule without when must be rejected")
	}
	if _, err := UnmarshalServerMessage([]byte(`{"type":"feedEvents","feedId":"f","feedEpoch":"e","ops":[],"head":1,"zz":1}`)); err == nil {
		t.Fatal("feedEvents unknown field must be rejected")
	}
}
