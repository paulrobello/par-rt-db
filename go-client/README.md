# go-client

Go client for [par-rt-db](../README.md) — the sixth implementation of the
par-rt-db wire contract (after the TypeScript, Rust, Python, and Swift
clients). Speaks the server's declarative query/transaction DSL over one-shot
HTTP and reactive WebSocket, with an admin control-plane client and an
in-memory test engine. No codegen: you build a schema value that serializes
to the exact server `SchemaDef`, and query results decode into your own Go
types.

Module name: `github.com/paulrobello/par-rt-db/go-client` (Go 1.23+,
stdlib-only outside `wsclient` — the only external dependency is
`github.com/coder/websocket`, and the `guard_test.go` gate keeps it that
way).

## Packages

| Package | Surface |
| --- | --- |
| `wire` | The wire vocabulary: strict-decoding JSONValue, envelope types (client/server messages), `Query`/`FilterExpr`/`ValueExpr`, `Transaction`/`Step`, schedules, errors-adjacent shapes. Unknown fields are rejected on decode, mirroring the server's `deny_unknown_fields`. |
| `dsl` | Builders: `TableQuery`, filter/value-expr constructors, `Mutation`, schema DSL, cursor codec. |
| `errors` | The eleven `ErrorCode` constants, `{code, message}` `RtDbError`, `HTTPStatus`. |
| `httpclient` | One-shot HTTP: typed `Query[T]`/`QueryRaw`, `Mutate` (+ retry helper), `authMe`, schedules, storage upload/download/delete, and the exported `Call`/`RawCall` seams. |
| `wsclient` | Reactive WebSocket client over `/sync`: live-query subscriptions with snapshot channels, presence, mutate-over-WS, reconnect/backoff/dedupe. |
| `admin` | The `/admin/*` control plane: db lifecycle, schema push/preview, tokens, sessions, webhooks, backups, ops feed, admin query/mutate, workflows, schedules, storage, anonymous access. |
| `inmemory` | The in-memory engine (`inmemory.Client`) — the same usage surface as httpclient/wsclient with server-identical semantics for tests (schema validation, queries, transactions, migrate, scheduler, presence, subscriptions). |
| `internal/corpus` | The wire-corpus semantics runner harness (internal; drives the engine in the repo's corpus tests). |

## Install

No release tag exists yet, so consume it as a local replace directive:

```
go mod edit -replace github.com/paulrobello/par-rt-db/go-client=../par-rt-db/go-client
go get github.com/paulrobello/par-rt-db/go-client
```

## Quick start (HTTP)

```go
package main

import (
	"context"
	"fmt"

	"github.com/paulrobello/par-rt-db/go-client/dsl"
	"github.com/paulrobello/par-rt-db/go-client/httpclient"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

type Item struct {
	Name string `json:"name"`
	N    *string `json:"n"` // int64 fields travel as decimal strings
	ID   string `json:"_id"`
}

func main() {
	c := httpclient.NewClient("http://127.0.0.1:8300", "mydb", "my-token")
	ctx := context.Background()

	// Insert.
	c.Mutate(ctx, wire.Transaction{Steps: []wire.Step{wire.StepInsert{
		Table: "items",
		Doc:   wire.Object{"name": wire.String("first"), "n": wire.String("7")},
	}}}, "")

	// Query (typed).
	q := dsl.NewTableQuery("items").WithIndex("by_n", wire.String("7"))
	items, err := httpclient.Query[[]Item](ctx, c, q.Build())
	if err != nil {
		panic(err)
	}
	fmt.Println(items[0].Name)
}
```

## Subscribe (WebSocket)

```go
ws := wsclient.NewClient("ws://127.0.0.1:8300/sync", "mydb",
	func(context.Context) (string, error) { return "my-token", nil })
if err := ws.Connect(ctx); err != nil {
	panic(err)
}
defer ws.Close()

sub, err := ws.Subscribe(ctx, wire.Query{Table: "items"})
if err != nil {
	panic(err)
}
for snap := range sub.Updates() {
	fmt.Println(snap.Kind, snap.Value)
}
```

## Admin

```go
adm := admin.NewAdminClient("http://127.0.0.1:8300", "admin-key")
adm.CreateDB(ctx, "mydb")
adm.PushSchema(ctx, "mydb", schema)
minted, _ := adm.MintToken(ctx, "mydb", "app")
```

## In-memory engine (tests)

The same usage surface with no network: schema-push validation that matches
the server's `SCHEMA_VIOLATION`/`BAD_REQUEST` codes, the full query DSL,
atomic transactions, migrations, a deterministic `Tick`, presence, and
subscriptions. The wire-corpus semantics runner drives exactly this engine
(164 cases, zero Go skips), so harness behavior is pinned to the server's.

```go
c := inmemory.New(inmemory.Options{})
c.PushSchema(schema)
c.Mutate(ctx, txn, "idem-1")
rows, _ := c.Query(ctx, dsl.NewTableQuery("items"))
```

## Wire contract

`wire/` is one of the six implementations of the par-rt-db protocol and must
stay byte-identical with the server (and the other clients' wire files).
The [`wire-corpus/`](../wire-corpus/README.md) fixtures pin the envelope
shapes, the error codes, the golden query vectors, and 164 semantics cases —
all executed by this repo's `make test` via the Go runners.

## Develop

```sh
make go-client-install   # go mod download
make go-client-checkall  # fmt-check + vet + dep guard + tests
go test ./...            # from go-client/, the plain suite
go test -tags live ./tests/live/   # needs RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY
```
