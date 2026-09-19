// go-client/dsl/mutation_test.go
package dsl

import (
	"encoding/json"
	"reflect"
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

func TestExpectAbsentCarriesTable(t *testing.T) {
	// Pins the final-review I1: the step's table field is required by the
	// server wire shape (core/src/mutation.rs::Step::ExpectAbsent); a dropped
	// table emitted `"table":""` and the server rejected it.
	txn := NewMutation().ExpectAbsent("items", "by_name", wire.String("ghost")).Build()
	blob, err := json.Marshal(txn)
	if err != nil {
		t.Fatal(err)
	}
	// Structural compare (key order is not part of the contract).
	var got, want map[string]any
	if err := json.Unmarshal(blob, &got); err != nil {
		t.Fatal(err)
	}
	wantJSON := `{"steps":[{"op":"expectAbsent","table":"items","index":"by_name","eq":["ghost"]}]}`
	if err := json.Unmarshal([]byte(wantJSON), &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("wire mismatch:\n got %s\nwant %s", blob, wantJSON)
	}
}
