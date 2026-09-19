// go-client/guard_test.go
package parrtdb_test

// The stdlib-only rule from spec §3: importing the root package and the
// non-transport packages must never transitively pull coder/websocket.
// Mechanism: `go list -deps` (the plan sanctions the shell form when
// go/build's transitive walk proves insufficient).

import (
	"os/exec"
	"strings"
	"testing"
)

func TestNoWebsocketDepOutsideWsclient(t *testing.T) {
	pkgs := []string{
		"github.com/paulrobello/par-rt-db/go-client",
		"github.com/paulrobello/par-rt-db/go-client/wire",
		"github.com/paulrobello/par-rt-db/go-client/dsl",
		"github.com/paulrobello/par-rt-db/go-client/errors",
		"github.com/paulrobello/par-rt-db/go-client/httpclient",
		"github.com/paulrobello/par-rt-db/go-client/inmemory",
	}
	out, err := exec.Command("go", append([]string{"list", "-deps"}, pkgs...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps failed: %v\n%s", err, out)
	}
	for _, line := range strings.Split(string(out), "\n") {
		if strings.Contains(line, "coder/websocket") {
			t.Fatalf("stdlib-only package transitively imports %s", line)
		}
	}
}
