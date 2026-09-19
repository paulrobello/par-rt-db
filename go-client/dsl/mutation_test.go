// go-client/dsl/mutation_test.go
package dsl

import (
	"encoding/json"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestMutationBuilderChain(t *testing.T) {
	txn := NewMutation().
		Insert("items", wire.Object(map[string]wire.JSONValue{"n": wire.Number("1")})).
		Patch("items", "i1", wire.Object(map[string]wire.JSONValue{"done": wire.Bool(true)})).
		Delete("items", "i2").
		Build()
	b, err := json.Marshal(txn)
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"op":"insert"`) || !bytesHas(b, `"doc":{"n":1}`) {
		t.Fatalf("insert shape: %s", b)
	}
	if !bytesHas(b, `"op":"patch"`) || !bytesHas(b, `"fields":{"done":true}`) {
		t.Fatalf("patch shape: %s", b)
	}
	if !bytesHas(b, `"op":"delete"`) {
		t.Fatalf("delete shape: %s", b)
	}
}
