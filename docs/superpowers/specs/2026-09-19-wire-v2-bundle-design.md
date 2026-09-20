# Wire v2 bundle — four protocol-bump features (2026-09-19)

Four backlog cards each change the wire vocabulary, so they ship together as
one coordinated `PROTOCOL_VERSION` 1 → 2 (ARC-013). One shared bump means one
mirror pass across the six wire implementations instead of four. Cards:

| Card | Feature |
| --- | --- |
| 01a0bb2005 | Mutate batch endpoint — `POST /api/mutate-batch` |
| 01a0b05256 | Multi-op aggregates in one query |
| 01a0bb2070 | Multi-field groupBy for aggregates |
| 01a0bb2071 | Cron timezone support (`tz` on cron schedules) |

The aggregate pair's design was agreed in the 2026-09-17 backlog session and
is recorded on the cards; this spec consolidates all four.

## 1. Version bump semantics (D1)

`PROTOCOL_VERSION = 2` on the server and in all five client SDKs. The
negotiation mechanism is unchanged: a client whose declared version is greater
than the server's is rejected at auth (`UNSUPPORTED_PROTOCOL`) — so a v2
client against an OLD server fails cleanly there instead of tripping
`deny_unknown_fields` on the first new field. A v1 client (every existing
vendored SDK) against a v2 server keeps working: every wire change below is
additive/optional. There is no per-message enforcement — the server's own
parser is the only vocabulary gate; the version number exists so peers fail
loudly at handshake when one is behind.

## 2. Multi-op aggregates (D2)

`AggregateSpec` (server `dsl.rs`, mirrored in five SDKs) gains:

```jsonc
{ "aggregates": { "revenue": "sum", "orders": "count" } }   // alias → AggregateOp
```

- Optional, skip-serialized when absent; `BTreeMap` semantics (deterministic
  alias order in SQL and results). Exactly one of `op` / `aggregates` — both
  or neither is `BadRequest`.
- Alias rules: non-empty, ≤ 64 chars, `[A-Za-z0-9_]` only (they become JSON
  result keys and `jsonb_build_object` labels, so the charset keeps SQL
  construction trivially safe). Violations are `BadRequest`.
- All field-needing ops share the ONE index-derived aggregate field (current
  resolution logic unchanged); per-alias numeric validation loops the existing
  `is_numeric_index_field` check; `count` consumes no field.
- Postgres form: ONE query — N aggregate expressions in one GROUP BY pass,
  `SELECT to_jsonb(jsonb_build_object('alias', COALESCE(to_jsonb(OP(col)),
  'null'::jsonb), …))`.

New result shapes (new `QueryResult` variants; `QueryResult` is
`#[serde(untagged)]` serialize-only, so this is server→client only):

- No group → **`aggregateMulti`**: one object, alias → value, e.g.
  `{"revenue": 1250.5, "orders": 42}`. A NULL aggregate surfaces as JSON null
  per alias (mirrors the scalar path's COALESCE).
- Grouped → **`aggregateMultiGroups`**: rows `[{ "keys": [...],
  "values": {…} }]`, ordered by group keys ascending, capped by `MAX_TAKE`.

Existing single-op shapes stay byte-identical (`aggregate` scalar,
`aggregateGroups` `[{key, value}]`).

## 3. Multi-field groupBy (D3)

`groupBy` widens from `bool` to `bool | [field, …]` (Rust: untagged
`GroupBy::Bool(bool) | GroupBy::Fields(Vec<String>)`, default `Bool(false)`;
`false` and `true` serialize exactly as today, so every existing wire form is
byte-identical — including the Go mirror's always-serialized `groupBy: false`).

- `Bool(true)` = legacy: group = `index.fields[eq.len()]`, aggregate field =
  `[eq.len()+1]`. Unchanged.
- `Fields(list)`: each entry must be a declared index field at position
  ≥ `eq_len` (`BadRequest` otherwise); non-empty; no duplicates. Group keys
  return as the `keys` array of `aggregateMultiGroups` rows.
- Aggregate field for field-needing ops = the first index field at position
  ≥ `eq_len` NOT in the group list; none available → `BadRequest`.
- Composes with `aggregates` and with single-`op`: single-op + explicit-list
  grouping uses the `aggregateMultiGroups` shape with `values` keyed by the
  lowercase op name (`{"sum": 42}`); single-op + `groupBy: true` keeps the
  legacy `{key, value}` shape.

## 4. Mutate batch endpoint (D4)

`POST /api/mutate-batch`, modeled on `/api/query-batch`:

```jsonc
// request
{ "db": "app", "txns": [ { "txn": {"steps": [...]}, "idempotencyKey": "k1" }, … ] }
// response
{ "results": [ { "ok": true, "results": [/* step results */] },
               { "ok": false, "error": {"code": "...", "message": "..."} }, … ] }
```

- Sequentially through `Committers::mutate` — no new write path; the
  single-writer invariant is untouched. **Deliberately NOT atomic** — a
  single atomic multi-step write is what the txn DSL is for; this is
  transport efficiency only. One failing entry never rolls back the others.
- Same auth, read-only-token rejection, and rate-limit gates as `/api/mutate`;
  empty/oversized batches rejected pre-auth (shared `MAX_BATCH_QUERIES = 64`
  cap). Per-entry `idempotencyKey` rides the existing per-db dedup table
  (replay per key).
- No WS surface (WS multiplexes mutations already). Mirrored in all five SDK
  HTTP surfaces, plus `batch_mutate` on the rust client and an `rtdb` CLI
  subcommand.

## 5. Cron timezone support (D5)

`ScheduleWhen::Cron` (in `par-rt-db-core::mutation`) gains an optional
`tz: Option<String>` — skip-serialized when absent, so no-tz wire bytes are
identical.

- Validation `tz.parse::<chrono_tz::Tz>()` → `BadRequest("unknown timezone")`
  inside `resolve_when`, which every create surface already funnels through
  (WS, HTTP, txn step, admin CRUD) — one check covers all. No silent UTC
  fallback.
- `next_fire(expr, now_ms, tz)`: with a tz, the search instant is converted
  into the zone (`now_utc.with_timezone(&tz)`) and croner (2.2.0,
  `find_next_occurrence<Tz: TimeZone>`) evaluates the wall-clock fields on
  that zone's local timeline; DST gaps are resolved by croner's zoned
  conversion. Result converts back to UTC epoch ms.
- Persistence: `scheduled_txns` gains a `tz text` column — added to the fresh
  `CREATE TABLE` and via idempotent `ALTER TABLE … ADD COLUMN IF NOT EXISTS`
  for existing databases. `ResolvedWhen` becomes a 5-tuple; `insert()`, the
  resume recompute, the external-finalize recompute, and the internal finalize
  recompute all pass `tz` through so recurring jobs re-arm in the same zone.
- `ScheduleInfo`, `ClaimedJob`, and `ClaimedSchedule` gain optional `tz`
  (skip-when-absent).

## 6. Corpus plan

Per the authoring rule, each behavior change ships its pinning case in the
same change. The corpus is synchronous-only (runners never advance time), so
cron firing itself stays in server integration tests; its corpus coverage is
wire-shape and validation:

- New semantics cases: `aggregate-multi-op`, `aggregate-multi-op-grouped`,
  `aggregate-groupby-fields`, `error-aggregate-op-and-aggregates`,
  `error-aggregate-groupby-unknown-field`, `error-schedule-cron-unknown-tz`.
- `wire-corpus.json`: `schedule_whens` gains a cron-with-tz entry; `queries`
  gains its first aggregate entries (map-only, grouped multi-op, groupBy
  list) with parallel `query_results`.
- No new query-combination rules: the existing terminal-clique rules key on
  the `aggregate` clause being set, and `aggregates` lives inside it. The
  op/aggregates exclusivity is an `AggregateSpec`-level rule pinned by the
  dedicated error case.
- No new `ErrorCode` variants → `error-codes.json` unchanged.

## 7. Client mirroring and rollout

Every change mirrors into all five SDKs: wire types + `PROTOCOL_VERSION = 2`,
DSL builders (`aggregates` map, `groupBy` list, cron `tz`), in-memory engine
support for the new aggregate shapes (engines re-arm crons by fixed step and
parse no cron, so tz is wire+`ScheduleInfo`+`ScheduledJob`-struct only),
`mutateBatch` HTTP method, and the per-client corpus runners.

Rollout is deployment-controlled: this bundle lands on `main` and is pushed,
but **not deployed** — dependent projects re-vendor the SDKs (now declaring
v2) and the server deploys v2 at the same time. Until that deploy, prod
speaks v1 and every old client is unaffected.
