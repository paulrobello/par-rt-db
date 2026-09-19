// go-client/dsl/constructors_test.go
package dsl

import (
	"encoding/json"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestOlderThanShape(t *testing.T) {
	b, err := json.Marshal(OlderThan("completedAt", 604800000))
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"op":"olderThan"`) || !bytesHas(b, `"ms":604800000`) {
		t.Fatalf("olderThan shape: %s", b)
	}
}

func TestValueExprCompose(t *testing.T) {
	e := Cast(Field("n"), wire.CastToInt64)
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if !bytesHas(b, `"op":"cast"`) || !bytesHas(b, `"to":"toInt64"`) {
		t.Fatalf("cast shape: %s", b)
	}
}
