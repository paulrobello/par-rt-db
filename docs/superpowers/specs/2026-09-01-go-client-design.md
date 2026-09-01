# go-client — Go client library design

- **Date:** 2026-09-01
- **Status:** Design approved in session; spec awaiting owner review
- **Card:** kanban `01a016ad7ff37f52ad3f9b6bdc7c561d` — "Add Go client library (go-client)"
- **Precedent:** `docs/superpowers/specs/2026-08-18-swift-client-design.md` (fifth wire implementation). This spec adds the sixth. It supersedes the card's original "deferred until external demand" note; the owner requested implementation on 2026-09-01.

## 1. Goals and non-goals

**Goals**

- A Go module implementing the par-rt-db client contract at machine-client parity with `rust-client`: wire types, query DSL, mutation DSL, schema DSL, HTTP one-shot transport, WS `/sync` live subscriptions, presence, schedules/workflows, file storage, admin routes, optimistic updates.
- A Go in-memory engine plus corpus runners, making Go a **full sixth runner** of `wire-corpus/` (`wire-corpus.json`, `semantics/`, `golden-vector.json`, `query-combinations.json`, `error-codes.json`). No permanent runner skips.
- Zero third-party dependencies everywhere except the WS transport package.

**Non-goals**

- Browser OAuth login/logout flows and `/admin/stream` — the machine-client boundary shared by rust/python/swift (ts-client is the only client carrying them).
- UI bindings — no React/SwiftUI analog exists for Go.
- Publishing to a registry at implementation time. Tagging (and therefore public Go-proxy availability — see §9) is a separate owner decision aligned with ENH-031.

## 2. Inherited context

The four existing clients define the conventions this client must match. Authoritative references:

| Concern | Reference |
|---|---|
| Wire contract source of truth | `server/src/protocol.rs`, `core/src/wire.rs`, `core/src/mutation.rs`, `server/src/dsl.rs` |
| Machine-client surface + docs style | `rust-client/` (README feature table, `src/wire.rs`, `src/admin/mod.rs`) |
| Newest full-parity client (engine shape) | `swift-client/` (five `InMemory*.swift` files ≈ 5,950 lines) |
| Corpus runner contract | `wire-corpus/README.md` ("How a runner executes a case") |
| Test conventions | Live tests env-gated on `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY`; harness provisions its own `t<uuid>` database; corpus tests offline |

Scale expectation from the four predecessors: engine ≈ 6k lines, runners ≈ 1.5k, client core ≈ 8–10k. Total ≈ 15–18k lines of Go.

## 3. Module and package layout

Module `github.com/paulrobello/par-rt-db/go-client` (go.mod inside `go-client/`), Go 1.23 floor. The root package is named `parrtdb` (package name need not match the directory; the package clause wins) and re-exports the stdlib-only surface so common use needs one import.

```
go-client/
  *.go        (package parrtdb)  re-exports wire + dsl + errors + httpclient — stdlib only
  wire/       casing-exact wire types, tagged-union codec helper, JSONValue, Int64
  dsl/        TableQuery builder, Mutation builder, schema DSL, filter/value helpers
  errors/     RtDbError envelope, the 11 codes, HTTP-status table
  httpclient/ one-shot HTTP + storage + schedules/workflows (net/http only)
  wsclient/   /sync websocket: auth, heartbeat, reconnect, subscriptions, presence
              — the ONLY package importing github.com/coder/websocket
  admin/      machine admin surface, parity with rust-client's admin module
  optimistic/ optimistic update layer
  inmemory/   engine + corpus-runner support — stdlib only
```

**Dependency rules (enforced by review; there is no cargo-feature analog):**

- `wire`, `dsl`, `errors`, `httpclient`, `inmemory` import nothing outside the standard library.
- Importing the root package never transitively pulls `coder/websocket`: the root must not import `wsclient`, `admin`, `optimistic`, or `inmemory`.
- Import graph (acyclic): `dsl`→`wire`,`errors`; `httpclient`→`wire`,`dsl`,`errors`; `wsclient`→`wire`,`dsl`,`errors`,`optimistic`; `admin`→`httpclient`,`wire`; `inmemory`→`wire`,`dsl`,`errors`.
- No shared interfaces package. Consumers declare the minimal interface they need and rely on Go's structural typing: the corpus runner and `optimistic` each declare small `Querier`/`Mutator`/`Subscriber`-shaped interfaces that `httpclient`, `wsclient`, and `inmemory` satisfy without knowing about them. (Go proverb: interfaces live where they are used.)

**WS dependency choice:** `github.com/coder/websocket` (formerly nhooyr.io/websocket; MIT, context-native, small). gorilla/websocket was the alternative; coder/websocket wins on `context.Context` integration, which the whole API leans on.

## 4. Wire fidelity — the casing contract

Go struct tags are per-field, so non-uniform casing is directly expressible. The rule from `protocol.rs`/`dsl.rs` applies unchanged: replicate every serde tag and field name exactly; do not normalize. The concrete contract, reproduced from the server sources:

| Type | Serialization shape |
|---|---|
| `ClientMessage` / `ServerMessage` | internally tagged by `"type"`, camelCase variant names (`authOk`, `queryUpdate`, …) and camelCase fields (`queryId`, `mutId`, `idempotencyKey`, `protocolVersion`, `ttlMs`, `connectionId`, …). Unknown fields rejected. |
| `Query` | flat struct, bare field names (`table`, `get`, `index`, `eq`, `gt`…`lte`, `order`, `take`, `unique`, `first`, `count`, `distinct`, `aggregate`, `paginate`, `filter`, `search`, `fields`) **plus two explicit camelCase renames** `vectorSearch` and `hybridSearch`. Optionals omitted when absent; bools omitted when false; `eq` omitted when empty. Unknown fields rejected. |
| `FilterExpr` | internally tagged by `"op"`, lowercase tags (`eq neq gt gte lt lte in and or not contains exists`) **with the camelCase exception `olderThan`**. Unknown fields rejected. |
| `ValueExpr` | internally tagged by `"op"`, camelCase tags (`field literal concat add sub mul div coalesce lower upper trim cast now case`); `Cast` values camelCase (`toString`, `toNumber`, `toInt64`, `toBoolean`). |
| `Step` (txn) | internally tagged by `"op"`, camelCase, 14 variants: `insert patch replace delete expectVersion expectAbsent upsert patchByQuery deleteByQuery schedule cancelSchedule startWorkflow cancelWorkflow undelete`. |
| `ScheduleWhen` | internally tagged by `"type"`, camelCase variants `afterMs{ms}` / `runAt{ms}` / `cron{expr}` / `interval{everyMs}` — note `ms` bare vs `everyMs` renamed in the same enum. |
| `Order`, `OutcomeStatus` | lowercase values (`asc`/`desc`; `success`/`failed`). |
| `UserKind`, `ScheduleKind`, `WorkflowStatus` | lowercase compound values (`user`/`machine`; `oneshot`/`cron`/`interval`; `pending running waiting success failed cancelled`). |
| `AuthedUser` | `name` serializes as `null` when absent (no omission) while `githubLogin`/`githubId` are omitted entirely — same struct, two behaviors. |
| `Paginate`/`Paginated` | `numItems`, `nextCursor` camelCase alongside bare `docs`, `cursor`. |
| Error envelope | `{"code":"SCREAMING_SNAKE_CASE","message":"…"}` with `retryAfter` (seconds) on `RATE_LIMITED`; the 11 codes pinned by `wire-corpus/error-codes.json`. |

**Mechanics in Go:**

- Internally-tagged unions get hand-written `MarshalJSON`/`UnmarshalJSON` backed by one shared generic helper in `wire/tagged.go`: decode into `map[string]json.RawMessage`, read the tag key, switch, then strictly decode the remaining fields into the variant struct.
- Unknown-field rejection mirrors serde `deny_unknown_fields` / pydantic `extra="forbid"`: strict decoding via `json.Decoder.DisallowUnknownFields` on every wire type, per union variant.
- Omission semantics: `omitempty` on fields that serde skips; explicit non-`omitempty` nullable pointer for `AuthedUser.Name`.
- int64 travels as decimal strings end-to-end: `wire.Int64` (string kind) with parse helpers; bytes travel base64.
- Step results are untagged JSON matched by shape: `insert → {id}`, `upsert → {id, inserted}`, `patchByQuery → {patched, truncated}`, `deleteByQuery → {deleted, truncated}`, `schedule → {scheduleId}`, `cancelSchedule → {cancelled}`, `startWorkflow → {workflowId}`, `patch/replace/delete/expect*/undelete → null`.
- `PROTOCOL_VERSION = 1`: every HTTP request carries `Authorization: Bearer <token>` + `X-Rtdb-Protocol: 1` through a single seam in `httpclient` (the analog of rust's `http_common.rs`); the WS `auth` frame carries optional `protocolVersion` and tolerates `authOk` echoing it only when sent.

**HTTP one-shot shapes** (mirror `rust-client/src/http.rs`):

- `POST /api/query` `{"db","query"}` → `{"result": <value>}`
- `POST /api/mutate` `{"db","txn","idempotencyKey"?}` → `{"results":[…]}`
- `POST /api/query-batch` `{"db","queries":[…]}` → `{"results":[{ok, result|error}]}`
- Admin routes under `/admin/*` with the admin key as bearer.

## 5. Public API surface

`context.Context` on every operation replaces the sync/async client twins python ships. Two API shapes per read where decoding matters, because Go methods cannot carry type parameters:

```go
// Package-level generic functions decode into caller types:
rows, err := rtdb.Query[[]Item](ctx, httpc, dsl.NewTableQuery("items").
    WithIndex("by_n").Order(dsl.Asc).Take(10))

// Methods return wire.JSONValue when the caller prefers dynamic values:
v, err := httpc.Query(ctx, q)
```

**httpclient** — `New(url, db, token)` plus functional options (HTTP client override, retry policy). Covers: query, query-batch, mutate (+ idempotency key, `mutate_with_retry`/`retryOnPrecondition` analog), schedules (create/cancel/pause/resume/list), workflows (start/cancel/signal/list), full storage surface (upload, streaming upload, download/`GetURL`, metadata, signed URL, transform URL, delete), `authMe`.

**wsclient** — `New(url, db, tokenProvider)` where tokenProvider is `func(ctx) (string, error)` (refresh-capable, mirroring rust's provider closure). A manager goroutine owns the connection: auth handshake, heartbeat, exponential-backoff reconnect (defaults mirroring rust: base 500ms, max 15s, heartbeat 20s), replay on reconnect (re-auth, re-subscribe all live queries), dedupe of subscriptions by canonical query shape. Subscriptions are channel-based and callback-free:

```go
sub, err := wsc.Subscribe(dsl.NewTableQuery("items").WithIndex("by_n").Order(dsl.Asc).Take(10))
for snap := range sub.Updates() {
    if snap.Kind == wsclient.SnapshotValue { use(snap.Value) }
}
sub.Close() // refcount-aware; last handle sends {"type":"unsubscribe"}
```

`Snapshot` mirrors rust's `Pending | Value | Error`. Presence: `JoinPresence(room, state)`, `LeavePresence`, snapshots delivered as `presenceSnapshot` frames. Mutations over WS with idempotency keys. Optimistic updates via an option that wires in the `optimistic` package.

**dsl** — `TableQuery` builder with the full terminal set (`Get`, `WithIndex`, `Eq/Gt/Gte/Lt/Lte`, `Order`, `Take`, `Unique`, `First`, `Count`, `Distinct`, `Aggregate`, `Paginate`, `Filter`, `Search`, `Fields`, `VectorSearch`, `HybridSearch`); `Mutation` builder with all 14 step ops; schema DSL mirroring the field types, indexes (incl. search/vector/unique/partial), `ownerField`/`collaboratorsField`/`authorize`, TTL, defaults, computed fields, soft delete, `onDelete`.

**admin** — machine admin surface at parity with `rust-client/src/admin/mod.rs` (create/delete/list databases, push/preview schema, mint/revoke tokens, config get/patch, backups, schema-history + restore, session list/revoke, migrate). `docs/clients.md`'s rust column is the checklist.

**errors** — `RtDbError{Code, Message, RetryAfter}` implementing `error`; code constants for the 11 codes; `errors.Is` support; the code→HTTP-status table from `error-codes.json`.

## 6. In-memory engine and corpus runners

The engine mirrors the shape the other four shipped (swift's is the newest reference): store + document/schema/index state, full read-clause query evaluator, migrate, schema validation, value-expr evaluation, presence. Deliberately-unmodeled arms follow rust's convention: return `INTERNAL`, loudly, never silently succeed with wrong semantics. The engine does not advance time and does not tick schedulers/TTL reapers (corpus determinism rules).

Runner contract (from `wire-corpus/README.md`, implemented verbatim):

1. Fresh engine per case.
2. Push `schema` via the normal path; if the case carries `pushError`, assert its code and stop.
3. Insert each `seed` entry, stripping `$id` labels and recording label → minted id.
4. Substitute placeholders anywhere in `op`/`then`: `{"$idRef": "<label>"}` and `$prev` (= first page's `nextCursor`) in `op.query.paginate.cursor`.
5. Execute `op` (query / txn / migrate).
6. Compare against `expect` with `normalize` key projection (default `["_id","_creationTime","_version"]`), unordered multiset compare, numeric-tolerant equality (`6 == 6.0`).
7. Optionally run `then.query` after success. Error cases assert `code` only, never messages.
8. No generated values in expectations; order asserted only where deterministic.

Runners live as Go tests in `inmemory/` reading `../wire-corpus/`: `wire_corpus_test.go`, `semantics_corpus_test.go` (walks `semantics/*.json`), `golden_vector_test.go`, `query_combinations_test.go`, plus `error_codes_test.go` asserting the code set. The corpus `skip` map vocabulary gains a `go` key (used only for transient, reason-stamped skips; the goal is zero).

Engine unit tests mirror rust's 17-file `in_memory` suite: filter, aggregate, cascade, computed, migrate, paginate, presence, query, relative_filter, scheduler, schema, search, storage, subscribe, unique, validate, writes.

## 7. Testing

Three layers, matching house convention:

1. **Unit** — wire round-trips and exact-JSON assertions per builder call (rust's `query.rs` test idiom), DSL shape tests, engine tests above. Offline.
2. **Corpus** — the runners in §6. Offline; no server, no database.
3. **Live integration** — env-gated on `RTDB_TEST_SERVER_URL` + `RTDB_TEST_ADMIN_KEY`; tests skip cleanly when unset. The harness creates a `t<uuid>` database, pushes a small schema, mints a machine token, and deletes its database on exit — identical to the other four clients. Never touches a database it didn't create.

`make go-client-test` runs layers 1–2. Live tests run via `go test -tags=live ./…` with the env set (a build tag keeps them out of CI's default invocation while remaining discoverable, matching the `#[ignore]`-plus-`--ignored` rust convention).

## 8. Repository integration

**Makefile** — targets `go-client-install` (`go mod download`), `go-client-fmt` (gofmt + goimports), `go-client-fmt-check`, `go-client-lint` (`go vet ./…` + staticcheck), `go-client-test`, `go-client-checkall`; lines added to the `build`/`fmt`/`fmt-check`/`lint`/`typecheck`(`go vet`)/`test` sweeps; `.PHONY` updated. Go runs on linux, so CI adds `setup-go` to the existing ubuntu `checkall` job — no swift-style separate job. No `docs-api` target: godoc has no build artifact; pkg.go.dev self-services from the module path once tagged (the pages job is unaffected).

**Not applicable** (verified): Dockerfile stub-list (go-client is not a cargo member, so it creates no `[[test]]` targets), env-drift-check (scans `server/src` only).

**Docs to update in the same change** (every place the codebase enumerates implementations):

- `README.md` — packages table (~line 97), Make-targets section (~1029), Clients section (~1213), parallel snippets in feature sections this client spans (pagination, workflows).
- `docs/clients.md` — at-a-glance row, surface-comparison column, parity-contract file list (sixth wire file), implementation counts.
- `wire-corpus/README.md` — runner list, `skip` vocabulary (`go` key), error-code client-file list.
- `CONTRIBUTING.md` — package table, test commands.
- `docs/RELEASING.md` — lockstep-version list; go.mod has no version field (the tag is the version, the SPM precedent).
- `FEATURE_MATRIX.md` — rows whose notes enumerate clients.
- `CLAUDE.md` — workspace table, wire-contract implementation list.
- `.pre-commit-config.yaml` — golang fmt/lint hooks.
- `go-client/README.md` — new; README feature-table style of rust-client (coverage table, install via git tag / replace directive, examples).

## 9. Versioning and publishing interplay

All packages version in lockstep. A Go module in a subdirectory is versioned by tags of the form `go-client/vX.Y.Z`. **Owner decision flag:** unlike npm/crates.io/PyPI, pushing such a tag to the public repo makes the module fetchable through the public Go proxy automatically — there is no publish step to withhold. ENH-031 (tag-driven publish pipeline, currently blocked on owner approval of exactly this class of action) should subsume `go-client/v*` tags so the first Go release is a deliberate act, not an incidental `v*` push. Until then, no `go-client/v*` tag gets pushed.

## 10. Assumptions and decisions

- **Scope: full sixth runner** (owner choice, 2026-09-01) — engine + all five corpus artifacts, no permanent skips.
- **Approach: subpackage module, stdlib-first** (owner choice, 2026-09-01) — dependency isolation as the analog of rust-client's cargo features.
- Machine client: no browser login flows, no `/admin/stream`.
- Go 1.23 floor; `coder/websocket` as the single third-party dependency.
- No shared interfaces package; consumers declare minimal interfaces (structural typing).
- The engine's emulation scope matches the established engine convention (rust/swift): unmodeled arms return `INTERNAL`; any corpus case that needs a skip gets a loud, reason-stamped `go` skip key and a follow-up card.

## 11. Success criteria

Mirrors the kanban card's acceptance criteria (card `01a016ad7ff37f52ad3f9b6bdc7c561d`):

1. Wire types byte-compatible with `server/src/protocol.rs` for every covered surface, proven by the `wire-corpus.json` + `error-codes.json` runners passing and cross-checked against the four client mirrors.
2. All five corpus runners (`wire-corpus`, `semantics`, `golden-vector`, `query-combinations`, `error-codes`) pass offline with zero `go` skips.
3. `go-client` wired into the root Makefile; `make checkall` passes with it included.
4. Tests follow house convention: unit + corpus offline; live-server tests env-gated.
5. Docs updated everywhere §8 enumerates.
