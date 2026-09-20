// go-client/wire/changes_test.go
package wire_test

import (
	"encoding/json"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestChangeFeedResponseStrictDecode(t *testing.T) {
	for _, bad := range []string{
		`{"ops":[],"nextSeq":1,"head":1,"logId":"x","extra":1}`,
		`{"ops":[{"seq":1,"table":"t","docId":"d","kind":"insert","doc":null,"ts":1,"extra":1}],"nextSeq":1,"head":1,"logId":"x"}`,
	} {
		var cf wire.ChangeFeedResponse
		if err := json.Unmarshal([]byte(bad), &cf); err == nil {
			t.Fatalf("must reject unknown field: %s", bad)
		}
	}
}

func TestChangeOpDocAlwaysEmitted(t *testing.T) {
	b, err := json.Marshal(wire.ChangeOp{Seq: 1, Table: "t", DocID: "d", Kind: wire.ChangeKindDelete, Ts: 2})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"seq":1,"table":"t","docId":"d","kind":"delete","doc":null,"ts":2}` {
		t.Fatalf("marshal: %s", b)
	}
	var cf wire.ChangeFeedResponse
	if err := json.Unmarshal([]byte(`{"ops":[],"nextSeq":5,"head":5,"logId":"x"}`), &cf); err != nil {
		t.Fatal(err)
	}
	if re, _ := json.Marshal(cf); string(re) != `{"ops":[],"nextSeq":5,"head":5,"logId":"x"}` {
		t.Fatalf("empty ops re-marshal: %s", re)
	}
}
