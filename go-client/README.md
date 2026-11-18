# go-client

Go client for [par-rt-db](../README.md) — the sixth implementation of the
par-rt-db wire contract (after the TypeScript, Rust, Python, and Swift
clients). Speaks the server's declarative query/transaction DSL over one-shot
HTTP and reactive WebSocket, with an admin control-plane client and an
in-memory test engine. No codegen: you build a schema value that serializes
to the exact server `SchemaDef`, and query results decode into your own Go
types.

Module name: `github.com/paulrobello/par-rt-db/go-client` (Go 1.27+, per `go.mod`;
stdlib-only outside `wsclient` — the only external dependency is
`github.com/coder/websocket`, and the `guard_test.go` gate keeps it that
way).

## Contents

- [Packages](#packages)
- [Install](#install)
- [Quick start (HTTP)](#quick-start-http)
- [Subscribe (WebSocket)](#subscribe-websocket)
- [Admin](#admin)
- [In-memory engine (tests)](#in-memory-engine-tests)
- [Row-field guards](#row-field-guards)
- [Transaction deadlines](#transaction-deadlines)
- [Wire contract](#wire-contract)
- [Develop](#develop)

## Packages

| Package | Surface |
| --- | --- |
| `wire` | The wire vocabulary: strict-decoding JSONValue, envelope types (client/server messages), `Query`/`FilterExpr`/`ValueExpr`, `Transaction`/`Step`, schedules, errors-adjacent shapes. Unknown fields are rejected on decode, mirroring the server's `deny_unknown_fields`. |
| `dsl` | Builders: `TableQuery`, filter/value-expr constructors, `Mutation`, schema DSL, cursor codec. |
| `errors` | The thirteen `ErrorCode` constants, `{code, message}` `RtDbError`, `HTTPStatus`. |
| `httpclient` | One-shot HTTP: typed `Query[T]`/`QueryRaw`, `Mutate` (+ retry helper), `AuthMe`, schedules, storage upload/download/delete, and the exported `Call`/`RawCall` seams. |
| `wsclient` | Reactive WebSocket client over `/sync`: live-query subscriptions with snapshot channels, presence, mutate-over-WS, reconnect/backoff/dedupe. |
| `admin` | The `/admin/*` control plane: db lifecycle, schema push/preview, tokens, sessions, webhooks, backups, ops feed, admin query/mutate, workflows, schedules, storage, anonymous access. |
| `inmemory` | The in-memory engine (`inmemory.Client`) — a similar API to httpclient/wsclient (method names differ; there is no shared interface) with server-identical semantics for tests (schema validation, queries, transactions, migrate, scheduler, presence, subscriptions). |
| `optimistic` | Optimistic-update layer: compose over any `MutationApplier` (httpclient or wsclient) with `optimistic.New`; the wsclient composition is manual (no wsclient option yet). |
| `internal/corpus` | The wire-corpus semantics runner harness (internal; drives the engine in the repo's corpus tests). |

## Install

No release tag exists yet, so consume it as a local replace directive:

```
go mod edit -replace github.com/paulrobello/par-rt-db/go-client=../par-rt-db/go-client
go get github.com/paulrobello/par-rt-db/go-client
```

## Quick start (HTTP)

The client talks to a database that already has a schema and a machine token.
Provision both once with the admin client (admin key from the server's
`RTDB_ADMIN_KEY`), then use the minted token for app traffic:

```go
package main

import (
	"context"
	"fmt"

	"github.com/paulrobello/par-rt-db/go-client/admin"
	"github.com/paulrobello/par-rt-db/go-client/dsl"
	"github.com/paulrobello/par-rt-db/go-client/httpclient"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

type Item struct {
	Name string  `json:"name"`
	N    *string `json:"n"` // int64 fields travel as decimal strings
	ID   string  `json:"_id"`
}

func main() {
	ctx := context.Background()
	baseURL := "http://127.0.0.1:8300"

	// One-time provisioning: database, schema, machine token.
	adm := admin.NewAdminClient(baseURL, "admin-key")
	if _, err := adm.CreateDB(ctx, "mydb"); err != nil {
		panic(err)
	}
	schema := dsl.DefineSchema().Table("items", func(t *dsl.TableBuilder) {
		t.Field("name", dsl.Str()).Field("n", dsl.Int64()).Index("by_n", "n")
	}).Build()
	if err := adm.PushSchema(ctx, "mydb", schema); err != nil {
		panic(err)
	}
	minted, err := adm.MintToken(ctx, "mydb", "app")
	if err != nil {
		panic(err)
	}

	c := httpclient.NewClient(baseURL, "mydb", minted.Token)

	// Insert.
	if _, err := c.Mutate(ctx, wire.Transaction{Steps: []wire.Step{wire.StepInsert{
		Table: "items",
		Doc:   wire.Object{"name": wire.String("first"), "n": wire.String("7")},
	}}}, ""); err != nil {
		panic(err)
	}

	// Query (typed).
	q := dsl.NewTableQuery("items").WithIndex("by_n", wire.String("7"))
	items, err := httpclient.Query[[]Item](ctx, c, q.Build())
	if err != nil {
		panic(err)
	}
	if len(items) == 0 {
		panic("no items returned")
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

A similar surface with no network (method names may differ from the HTTP/WS clients; there is no shared interface): schema-push validation that matches
the server's `SCHEMA_VIOLATION`/`BAD_REQUEST` codes, the full query DSL,
atomic transactions, migrations, a deterministic `Tick`, presence, and
subscriptions. The wire-corpus semantics runner drives exactly this engine
(every semantics case, zero Go skips; see
[`wire-corpus/README.md`](../wire-corpus/README.md) for current counts), so
harness behavior is pinned to the server's.

```go
c := inmemory.New(inmemory.Options{})
c.PushSchema(schema)
c.Mutate(ctx, txn, "idem-1")
rows, _ := c.Query(ctx, dsl.NewTableQuery("items"))
```

## Row-field guards

Transactions support `.ExpectFields(table, id, expected...)` with `FieldExpectation` assertions. The guard checks selected values without writing the live row or advancing its version. Unasserted fields may change. See [the shared contract](../docs/transaction-field-guards.md) for paths, bounds, visibility, deadlines and replay behavior.

## Transaction deadlines

Transactions support `.ExpectBefore(deadlineMs)` to require the authoritative clock to precede an exclusive epoch-ms deadline at execution and the final commit decision. Expiry rolls back all effects with `PRECONDITION_FAILED`. Pair a stored lease deadline with version/ownership guards. The in-memory engine uses its injected clock. See [transaction deadlines](../docs/transaction-deadlines.md).

## Wire contract

`wire/` is one of the seven implementations of the par-rt-db protocol and must
stay byte-identical with the server (and the other clients' wire files).
The [`wire-corpus/`](../wire-corpus/README.md) fixtures pin the envelope
shapes, the error codes, the golden query vectors, and every semantics case —
all executed by this repo's `make test` via the Go runners.

## Develop

```sh
make go-client-install   # go mod download
make go-client-checkall  # fmt-check + vet + dep guard + tests
go test ./...            # from go-client/, the plain suite
go test -tags live ./tests/live/   # needs RTDB_TEST_SERVER_URL + RTDB_TEST_ADMIN_KEY
```
