//go:build live

// Env-gated live integration tests against a running par-rt-db server.
// Requires RTDB_TEST_SERVER_URL and RTDB_TEST_ADMIN_KEY; the whole package
// skips cleanly when either is unset (mirror rust-client/tests/common/mod.rs).
// Setup mints a uniquely-named "t"-prefixed database per run and tears it
// down via t.Cleanup; no db whose name doesn't start with "t" is touched.
package live

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/admin"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

type liveEnv struct {
	t        *testing.T
	admin    *admin.AdminClient
	baseURL  string
	db       string
	httpURL  string
	wsURL    string
	token    string
	sceneCtx context.Context
}

func envOrSkip(t *testing.T) *liveEnv {
	t.Helper()
	base := os.Getenv("RTDB_TEST_SERVER_URL")
	key := os.Getenv("RTDB_TEST_ADMIN_KEY")
	if base == "" || key == "" {
		t.Skip("live integration tests require RTDB_TEST_SERVER_URL and RTDB_TEST_ADMIN_KEY")
	}
	return &liveEnv{t: t, admin: admin.NewAdminClient(base, key), baseURL: base}
}

// setup mints the per-run database, pushes the two-field fixture schema, and
// mints a machine token for the http/ws clients.
func (e *liveEnv) setup() {
	e.t.Helper()
	ctx := context.Background()
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		e.t.Fatalf("rand: %v", err)
	}
	e.db = "t" + hex.EncodeToString(suffix)
	if err := e.admin.CreateDB(ctx, e.db); err != nil {
		e.t.Fatalf("CreateDB(%s): %v", e.db, err)
	}
	e.t.Cleanup(func() {
		_ = e.admin.DeleteDB(context.Background(), e.db, e.db)
	})
	schema := wire.Object{"tables": wire.Object{
		"items": wire.Object{
			"fields": wire.Object{
				"name": wire.Object{"type": wire.String("string")},
				"n":    wire.Object{"type": wire.String("int64")},
			},
			"indexes": wire.Array{wire.Object{"name": wire.String("by_n"), "fields": wire.Array{wire.String("n")}}},
		},
	}}
	if err := e.admin.PushSchema(ctx, e.db, schema); err != nil {
		e.t.Fatalf("PushSchema: %v", err)
	}
	minted, err := e.admin.MintToken(ctx, e.db, "live-tests")
	if err != nil {
		e.t.Fatalf("MintToken: %v", err)
	}
	e.token = minted.Token
	e.httpURL = e.baseURL
	e.wsURL = wsURLFrom(e.baseURL)
}

// wsURLFrom converts an http(s) base URL to its ws(s) /sync endpoint (the
// server's WebSocket route; rust sync_url keeps any explicit path, so a base
// that already ends in /sync passes through unchanged).
func wsURLFrom(base string) string {
	out := base
	for _, pair := range [][2]string{{"https://", "wss://"}, {"http://", "ws://"}} {
		if len(base) >= len(pair[0]) && base[:len(pair[0])] == pair[0] {
			out = pair[1] + base[len(pair[0]):]
			break
		}
	}
	out = strings.TrimSuffix(out, "/")
	if strings.HasSuffix(out, "/sync") {
		return out
	}
	return out + "/sync"
}
