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
