# wire-corpus — shared parity fixtures

This directory holds the fixture data that keeps the server and the client
implementations honest against each other. Three artifacts live here:

- [`golden-vector.json`](golden-vector.json) — the **wire/behavior parity
  vector** for the query DSL over one shared seeded dataset (added by
  QA-001/QA-103). Catches wire-shape, sort-comparator, boundary, terminal, and
  filter-semantics divergence across the five implementations.
- [`semantics/`](semantics/) — the **behavioral-semantics corpus** (ENH-023):
  one JSON file per case, each self-contained (own schema + seed + operation +
  expected result), covering behavior the golden vector does not — transactions
  with per-step results, write-then-read visibility, defaults, soft delete, TTL,
  cursors, error codes, schema-push validation (`pushError` cases), and migrate
  interplay (`op.migrate` cases).
- [`wire-corpus.json`](wire-corpus.json) — the **wire-shape parity corpus**
  (ARC-008): client/server messages, authed users, schedule whens/infos, and
  queries that every wire implementation must encode/decode value-identically —
  and the same unknown fields must be rejected. Run by the server and all four
  clients (ts/rust/python/swift), so a drifted wire type fails whichever
  package drifted.
- [`error-codes.json`](error-codes.json) — the **error-code parity corpus**
  (ARC-017): the full `{code, httpStatus}` list for every `RtDbError` wire
  code, generated from `server/src/error.rs`'s `ErrorCode` enum. The
  `ErrorCode` enum is a sixth hand-mirrored surface (server + four clients);
  this corpus is what checks the five lists actually agree, so a code added to
  the server can't silently go unknown to a client. See
  [Error-code parity](#error-code-parity) below.

**Runner scope:** the server and all four client in-memory engines
(`ts-client`, `rust-client`, `python-client`, `swift-client`) execute
`golden-vector.json` and `semantics/`; every client also runs
`wire-corpus.json` (the swift client has since it landed; the semantics and
golden-vector runners shipped with its in-memory engine on 2026-08-19).

The server is the source of truth for every expected value in both behavioral
layers. A divergence between any client engine and these fixtures is a bug in the client
(or, if the server moved, a stale fixture — fix the fixture in the same change).

## Table of contents

- [The authoring rule](#the-authoring-rule)
- [Semantics corpus format](#semantics-corpus-format)
- [How a runner executes a case](#how-a-runner-executes-a-case)
- [Determinism rulings](#determinism-rulings)
- [Substitution placeholders](#substitution-placeholders)
- [Adding a case](#adding-a-case)
- [Error-code parity](#error-code-parity)

## The authoring rule

**Every change to server behavior that alters a query result, a step result, an
error code, or document visibility ships with at least one semantics case
exercising the new behavior — in the same change.** This mirrors the
long-standing golden-vector convention (see the PR checklist in
[`CONTRIBUTING.md`](../CONTRIBUTING.md)): the corpus is the source of truth for
cross-engine agreement, and an uncovered behavior is a standing regression risk
for the other four engines. A semantics change without a fixture fails review
the same way an untested code change does.

When a behavior is already pinned by a case, CHANGE the existing case in the
same commit as the server change — all five runners consume the files directly,
so a deliberate semantic flip turns exactly the cases that pinned the old
behavior red until the fixture is updated. Never delete a case to make a runner
green; either the client is wrong or the fixture is stale.

## Semantics corpus format

One case per file under `semantics/`, kebab-case filename equal to the case's
`name` (e.g. `paginate-last-full-page-no-cursor.json`). Every file is a single
JSON object:

| Field | Required | Meaning |
| --- | --- | --- |
| `name` | yes | Case slug; must equal the filename stem. |
| `$comment` | no | Author note: what the case pins, and where the expected value was derived from when it is non-obvious. Keep concise. |
| `schema` | yes | The schema wire object exactly as a client pushes it (`{"tables": {"<name>": {"fields": {...}, "indexes": [...], ...}}}` — the `SchemaDef` shape from `server/src/schema.rs`). Do NOT use golden-vector's flat `schema_table`/`schema_fields` shorthand; that is a legacy convention local to `golden-vector.json`. |
| `seed` | yes | Documents inserted (in array order, via the normal insert path) before the op. Either a plain doc object (legal only when `schema` declares exactly one table) or `{"table"?: "...", "doc": {...}, "$id"?: "<label>"}` (`table` may be omitted when the schema has exactly one table). Disambiguation: an entry is a wrapped entry iff it is an object with a `doc` key whose value is an object; any other object is a plain doc — so a table with a field literally named `doc` holding an object is inexpressible as a plain seed, and corpus tables never declare one. |
| `op` | yes | `{"query": <Query DSL>}`, `{"txn": {"steps": [<Step DSL>]}}`, or `{"migrate": {"directives": [<Directive>...], "dryRun": <bool>}}` — the query/txn wire shapes from `server/src/dsl.rs`; the `migrate` block is the admin `MigrateRequest` wire shape (`server/src/migrate.rs`, the same directives `wire-corpus.json`'s `migrate_requests` exercises). |
| `pushError` | no | `{"code": "<CODE>"}` — when present, the schema PUSH itself is expected to fail, and the push is the whole case: the runner attempts the push and asserts the failure's code (the same `{code}` object and code-only matching `expect.error` uses; never message text). A `pushError` case carries no `seed`, `op`, `then`, or `expect`. |
| `expect` | yes | The expected result (see below), or `{"error": {"code": "<CODE>"}}` for error cases. |
| `unordered` | no | `true` when `expect` (or `then.expect`) is an array of docs with no deterministic order; compare as a sorted set (see [Determinism rulings](#determinism-rulings)). |
| `normalize` | no | Keys projected out of both the actual and expected trees before comparison — the projection applies RECURSIVELY to every object in both trees (docs nested inside `paginate.docs`, step results, ...). Defaults to `["_id", "_creationTime", "_version"]` when absent; a present list REPLACES the default (txn cases add `"id"` for minted step-result ids). |
| `expect_next_cursor` | paginate cases | `true`/`false`: assert the `paginate` result carries (or omits) a `nextCursor`. The cursor value itself is generated and never compared. Required on every paginate case whose `expect` is a result (a paginate error case asserts the error instead and carries no `expect_next_cursor`). |
| `then` | no | `{"query": <Query DSL>, "expect": ..., "unordered"?, "normalize"?}` — a follow-up read executed after `op` succeeds and its `expect` has been checked. For write-then-read visibility cases (soft delete, defaults, upsert-patch). Inherits the case-level `normalize` unless it gives its own. `then` runs only after `op` SUCCEEDS; an error case (`expect` is `{"error": ...}`) must not carry `then` (runners do not execute it there). |
| `skip` | no | `{"ts" \| "rust" \| "python" \| "server" \| "swift": "reason"}` — a named runner may skip the case, loudly, until the gap is closed. Absent means every runner must execute the case. |

### `expect` shapes

The expected value is the serialized result of the operation, with `normalize`
keys projected out:

- **query op** — the serialized `QueryResult`:
  - `collect`/`take` → array of docs; `get`/`first`/`unique` → doc object or `null`
  - `paginate` → `{"docs": [...]}` (plus the case-level `expect_next_cursor`)
  - `count` → bare integer
  - `distinct` → array of scalar values (ascending; NULL index values are
    included — the server SQL is `SELECT DISTINCT to_jsonb(col) … ORDER BY v`,
    where a missing optional value projects to SQL NULL, sorts last under
    the ORDER BY default, and decodes to JSON `null`)
  - `aggregate` → bare scalar (`null` over an empty match set) or, with
    `groupBy`, an array of `{"key", "value"}` rows sorted by key ascending;
    rows missing the group field form one `key: null` group sorted last
    (Postgres `GROUP BY` includes the SQL NULL group under `NULLS LAST`), and
    a group whose aggregate input is entirely NULL aggregates to
    `value: null` (SQL aggregates ignore NULL rows, so a partially-present
    group aggregates the present values)
- **txn op** — array of per-step results: `insert` → `{"id", }`; `patch`,
  `replace`, `delete`, `undelete`, `expectVersion`, `expectAbsent` → `null`;
  `upsert` → `{"id", "inserted"}`; `patchByQuery` → `{"patched", "truncated"}`;
  `deleteByQuery` → `{"deleted", "truncated"}`
- **error case** — `{"error": {"code": "..."}}`. The code string is one of the
  `ErrorCode` wire names from `server/src/error.rs` (`BAD_REQUEST`,
  `NOT_FOUND`, `SCHEMA_VIOLATION`, `PRECONDITION_FAILED`, ...). Only the code is
  asserted — never message text.

## How a runner executes a case

1. Create a fresh database/instance, push `schema` through the normal
   schema-push path. If the case carries `pushError`, the push is EXPECTED to
   fail: assert its failure code and stop — the case has no seed, op, `then`,
   or `expect` (a push that succeeds fails the case loudly).
2. Insert each `seed` entry through the normal insert path — the `doc` member
   of a wrapped entry (into its `table`, or the single declared table), with
   any `$id` label stripped first and never sent — recording
   `label → minted id` when labeled.
3. Substitute placeholders (see below) throughout `op` — and `then.query` if
   present.
4. If `op.query.paginate.cursor` is the `"$prev"` sentinel: first execute the
   same query with the `cursor` field removed, take that page's `nextCursor`
   (fail loudly if there is none), then execute the query with it. `expect` and
   `expect_next_cursor` describe the SECOND page.
5. Execute `op`. If `expect` is an error object, assert the failure's `code`.
   Otherwise assert success and compare the result to `expect` after applying
   `normalize` projection, `unordered` (if set), and numeric-tolerant equality.
   A `migrate` op runs the package's real migrate path over `op.migrate`'s
   directives (`dryRun` honored); its result — `{applied, schema, directives}`
   (the derived schema plus per-directive reports; report `sampleChanges[].id`
   is minted, so those cases carry `"id"` in `normalize`) — compares like any
   op result, and the DERIVED schema replaces the case schema for `then`.
6. If `then` is present and `op` succeeded, execute `then.query` and compare to
   `then.expect` the same way.

Runners iterate every file in `semantics/` — adding a case is data-only. A case
a runner cannot express fails loudly (use `skip` with a reason, never a silent
drop). Runners must not advance time, tick schedulers, or run a TTL reaper
between seeding and the op: the corpus pins synchronous semantics only.

## Determinism rulings

These rules are binding on authors and runners alike; a case that cannot obey
them is not a corpus case.

1. **No generated values in `expect`.** Expected rows contain only values the
   fixture itself supplied (seed literals, wire inputs, or values derived from
   them deterministically). Anything minted at run time (`_id`,
   `_creationTime`, `_version`, step-result `id`, cursors) is either projected
   out via `normalize` or asserted structurally (`expect_next_cursor`).
2. **Order is asserted only when deterministic.** A multi-row `expect` produced
   by a query with an explicit `order` over an index is compared as a sequence.
   Sequence comparison is also licensed for the terminals' implicit ORDER BY —
   `distinct` (ascending) and grouped `aggregate` (`groupBy`, key-ascending) —
   whose order is deterministic by construction. Any other multi-row `expect`
   must carry `"unordered": true` and is compared
   as a multiset: sort both sides by each row's canonical JSON (compact
   serialization with object keys sorted recursively) and compare
   element-wise. This mirrors golden-vector's `expected_unordered`.
3. **Values stay in the DSL's typed universe.** `number` fields are JSON
   numbers; `int64` fields are decimal STRINGS end to end (wire form — a
   `{"type":"int64"}` field stores, binds, and returns `"42"`, never `42`);
   `bytes` are base64 strings; vectors are arrays of numbers. Within the JSON
   numbers, comparison is numeric-tolerant (`6 == 6.0`), mirroring
   golden-vector, so SQL `numeric` and client `f64` agree.
4. **Error cases assert `code` only** — messages are not part of the contract.
5. **Case isolation.** Each case gets its own schema and seed; never rely on
   another case's data, on wall-clock time, or on insert-order tiebreaks
   (btree scans tiebreak on `created_at`/`id`, which the `unordered` ruling
   keeps out of the comparison).

## Substitution placeholders

Placeholders exist because ids and cursors are minted at run time and cannot be
written literally. Runners implement exactly these three:

| Placeholder | Where | Expands to |
| --- | --- | --- |
| `"$id": "<label>"` | a `seed` entry | Nothing in-band: the runner strips the key before inserting and records `label → minted id`. |
| `{"$idRef": "<label>"}` | anywhere in `op` / `then.query` | The recorded id string for that seed label (e.g. the value for a `get`, a `patch`/`delete` step's `id`). |
| `"$prev"` | `op.query.paginate.cursor` | The `nextCursor` of the same query's first page (see step 4 of the runner algorithm). |

Placeholders never appear in `expect` — expectations use `normalize` instead.

## Adding a case

1. Copy the closest existing case and edit; keep one behavior per case.
2. Derive the expected value from the server (run it if in doubt), and cite the
   derivation in `$comment` when it is non-obvious.
3. Check the determinism rulings — especially `unordered` and `normalize`.
4. Name the file for the behavior (`<area>-<specifics>.json`), and make `name`
   match the filename.
5. `jq . <file>` must parse; the runner count assertion in each package's
   corpus test should be bumped only by adding files, never by skipping.

## Error-code parity

`error-codes.json` is a flat `{code, httpStatus}` table, one row per
`server/src/error.rs::ErrorCode` variant, in enum declaration order — not a
`semantics/` case (it has no schema/seed/op) and not part of
`wire-corpus.json` (whose `error_envelopes` section pins a handful of sample
envelopes for wire round-trip, not the full closed set with statuses).

**Authoring rule:** any change to `ErrorCode` (add, remove, rename a wire code,
or change its HTTP status) ships with an update to `error-codes.json` in the
same commit, and a matching update to every client's error-code type
(`ts-client/src/errors.ts`, `rust-client/src/error.rs`,
`python-client/src/par_rt_db/errors.py`,
`swift-client/Sources/ParRtDbClient/Errors.swift`). This mirrors the semantics
corpus's authoring rule above.

**Enforcement:**

- **Server** (`server/src/error.rs::tests::error_codes_match_wire_corpus`):
  regenerates the table from the enum through an exhaustive match — no
  catch-all arm — so adding a variant fails the crate to compile until the
  match (and `RtDbError::status`, itself already exhaustive) is updated; the
  test then diffs the regenerated table against the committed JSON.
- **Clients**: each of the four client corpus test files
  (`rust-client/tests/wire_corpus.rs`, `ts-client/tests/wire-corpus.test.ts`,
  `python-client/tests/test_wire_parity.py`,
  `swift-client/Tests/ParRtDbClientTests/WireCorpusTests.swift`) asserts every
  corpus code is known to that client's error-code type, and vice versa
  (Python's `StrEnum` and Swift's `CaseIterable` enumerate their own variants
  automatically; TS and Rust hand-maintain a parallel `ALL` list, so a
  forgotten update there is a test-time catch, not a compile-time one). Python
  additionally asserts its per-code HTTP status against `httpStatus`
  (`RtDbError.status_code`); TS and Rust don't model an HTTP status per code,
  so they check the code set only.
