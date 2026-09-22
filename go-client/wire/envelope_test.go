// go-client/wire/envelope_test.go
package wire

import (
	"encoding/json"
	"testing"
)

func TestAuthFrameExactJSON(t *testing.T) {
	db := "d1"
	b, err := json.Marshal(ClientAuth{DB: db})
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"type":"auth"`) || !bytesHas(b, `"db":"d1"`) {
		t.Fatalf("auth frame: %s", b)
	}
	if bytesHas(b, "token") {
		t.Fatalf("absent token must be omitted: %s", b)
	}
	tok := "t"
	ver := uint32(1)
	b, err = json.Marshal(ClientAuth{DB: db, Token: &tok, ProtocolVersion: &ver})
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"token":"t"`) || !bytesHas(b, `"protocolVersion":1`) {
		t.Fatalf("token/protocolVersion lost: %s", b)
	}
}

func TestAuthedUserNameNullVsGithubOmitted(t *testing.T) {
	b, err := json.Marshal(AuthedUser{Kind: UserKindMachine})
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"name":null`) {
		t.Fatalf("name must serialize as null: %s", b)
	}
	if bytesHas(b, "githubLogin") || bytesHas(b, "githubId") {
		t.Fatalf("github fields must be omitted when absent: %s", b)
	}
}

func TestQueryUpdateDecode(t *testing.T) {
	frame := []byte(`{"type":"queryUpdate","queryId":"q1","result":{"docs":[{"_id":"i1"}]}}`)
	msg, err := UnmarshalServerMessage(frame)
	if err != nil {
		t.Fatal(err)
	}
	qu, ok := msg.(ServerQueryUpdate)
	if !ok {
		t.Fatalf("wrong variant %T", msg)
	}
	if qu.QueryID != "q1" {
		t.Fatalf("queryId %q", qu.QueryID)
	}
	docs, ok := qu.Result.(Object)
	if !ok {
		t.Fatalf("result kind %T", qu.Result)
	}
	arr, ok := docs["docs"].(Array)
	if !ok || len(arr) != 1 {
		t.Fatalf("docs shape %T", docs["docs"])
	}
}

func TestServerFrameRejectsUnknownField(t *testing.T) {
	_, err := UnmarshalServerMessage([]byte(`{"type":"pong","bogus":1}`))
	if err == nil {
		t.Fatal("unknown field on pong must be rejected")
	}
	_, err = UnmarshalServerMessage([]byte(`{"type":"nope"}`))
	if err == nil {
		t.Fatal("unknown tag must be rejected")
	}
}

func TestScheduleAckErrorOmittedWhenOK(t *testing.T) {
	b, err := json.Marshal(ServerScheduleAck{ScheduleID: "corr-1", OK: true})
	if err != nil {
		t.Fatal(err)
	}
	if bytesHas(b, "error") {
		t.Fatalf("ok ack must omit error: %s", b)
	}
	msg, err := UnmarshalServerMessage(b)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := msg.(ServerScheduleAck); !ok {
		t.Fatalf("wrong variant %T", msg)
	}
}

func TestPresenceDeltaOmitEmptyAndDecode(t *testing.T) {
	// All-empty buckets must be omitted (server skip_serializing_if parity):
	// the corpus pins the all-empty shape as room+seq+type only.
	b, err := json.Marshal(ServerPresenceDelta{Room: "doc:1", Seq: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"type":"presenceDelta"`) || !bytesHas(b, `"room":"doc:1"`) || !bytesHas(b, `"seq":1`) {
		t.Fatalf("all-empty delta: %s", b)
	}
	if bytesHas(b, "joined") || bytesHas(b, "left") || bytesHas(b, "stateChanged") {
		t.Fatalf("empty buckets must be omitted: %s", b)
	}
	msg, err := UnmarshalServerMessage(b)
	if err != nil {
		t.Fatal(err)
	}
	delta, ok := msg.(ServerPresenceDelta)
	if !ok {
		t.Fatalf("wrong variant %T", msg)
	}
	if delta.Room != "doc:1" || delta.Seq != 1 || delta.Joined != nil || delta.Left != nil || delta.StateChanged != nil {
		t.Fatalf("decode mismatch: %+v", delta)
	}

	// Full shape: buckets present, camelCase stateChanged preserved.
	full := ServerPresenceDelta{
		Room: "doc:1",
		Seq:  2,
		Joined: []PresenceMember{{
			ConnectionID: "c3",
			User:         AuthedUser{Kind: UserKindUser},
			State:        Null{},
		}},
		Left:         []string{"c2"},
		StateChanged: []PresenceMember{{ConnectionID: "c1", User: AuthedUser{Kind: UserKindMachine}, State: Null{}}},
	}
	b, err = json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"joined":[`, `"left":["c2"]`, `"stateChanged":[`} {
		if !bytesHas(b, key) {
			t.Fatalf("missing %s in %s", key, b)
		}
	}
	msg, err = UnmarshalServerMessage(b)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := msg.(ServerPresenceDelta); !ok {
		t.Fatalf("wrong variant %T", msg)
	}
}
