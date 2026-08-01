# Schema Migration & Backfill

**Date:** 2026-07-31
**Status:** Design (pre-implementation)
**Scope:** Graduates the schema-migration policy past its additive-only MVP into a
general, declarative, admin-driven migration capability covering rename, type
coercion, removal, default backfill, and a scoped arbitrary-transform escape.

## Background & motivation

par-rt-db's schema-migration policy today is, by spec (line 99 of
`2026-07-21-par-rt-db-design.md`), the **MVP** policy:

> Additive changes (new tables, new optional fields, new indexes) apply
> automatically on push. Destructive/type-changing pushes are rejected with a
> clear error — handled manually if ever needed.

`ddl::detect_destructive_changes` enforces it: removed table/field/index,
changed field type (except a safe literal-union widening via
`schema::is_widening_of`), and changed index kind/field list are all rejected
as `BadRequest`. The server already backfills a *new* indexed column from
existing document jsonb via a straight cast (`ddl::backfill_expr`), inside the
push transaction — so it already transforms existing data into a new column.
What it refuses is **changing, renaming, or removing** what already exists.

This spec closes that gap. The four operations blocked today are: **rename**
(field/table — currently a silent remove+add = data loss), **type change /
coercion** on existing values, **removal** (field/table/index + its data), and
**set default / make required** (backfill a value into existing rows).

### Hard constraint

The architecture has **no embedded JS runtime and no per-app server code** (a
founding non-goal — `FEATURE_MATRIX.md` §3). Convex migrations are arbitrary JS
functions with `ctx.db`; par-rt-db cannot run those. Every migration here is
therefore **declarative and server-applied via generated SQL/DML**, or — for
the arbitrary long tail — a **scoped** admin-supplied SQL expression over a
single table's documents. There is no plan to add a runtime.

### Decision: Approach C (declarative directives + scoped eval-expr)

Three approaches were considered (see `FEATURE_MATRIX.md` history / the
brainstorm that produced this spec):

- **A — explicit declarative directives only.** Sound-by-construction closed
  set; arbitrary long tail left to the `psql`/HTTP escape hatch.
- **B — infer the migration from the schema diff.** Rejected: rename-vs-delete
  is fundamentally ambiguous under diff inference, and folding destructive
  work into `pushSchema` mixes the safe and dangerous paths.
- **C — directives *plus* a first-class scoped raw-SQL doc-rewrite op.**
  Chosen: gives the arbitrary-transform power of raw SQL, scoped so it cannot
  `DROP` a table or escape the database. The accepted footgun, bounded.

## Goals

1. Rename fields and tables without data loss.
2. Change a field's type over existing data via a sound, closed cast set.
3. Remove fields, tables, and indexes (and, for fields/tables, their data).
4. Backfill a default value into existing rows.
5. Apply arbitrary, server-side document transforms (computed fields, date
   reformatting, etc.) the closed cast set cannot express — scoped to one
   table's documents.
6. Every migration is **atomic** (all-or-nothing) and **publishes** through the
   existing subscription/op-feed/audit/webhook machinery, so live queries and
   operators see the new state — no silent staleness.

## Non-goals

- **Write-time enforcement** of required/default. `setDefault` is a one-time
  data backfill; the schema still does not encode defaults or presence
  constraints. "Make required" means "backfill a value into existing rows,"
  not "reject future writes missing the field."
- **Migration journal / undo log.** A migrate is a one-way atomic apply. Undo
  is another migrate, or `snapshot::import_database` from a pre-migrate export.
- **Auto-generating migrates from a schema diff** (approach B, rejected).
- **A WS `/sync` peer.** Migrate is an admin control-plane operation, like
  `pushSchema` and export/import — HTTP-only. Routing DDL through the reactive
  path would add risk for no benefit.

## Interaction model

**Transport & trust.** `POST /admin/db/{db}/migrate`, admin-only. Auth is the
existing `admin::require_admin` constant-time key check (admin key, or an
OAuth session on the `rtdb_auth.admins` allowlist) — unchanged, no new auth
mechanism. Per-`{db}` in the path, like storage. **No WS peer.**

**Single source of truth.** The request body is `{ directives: [Directive],
dryRun?: bool }` — **no caller-supplied schema**. The server loads the current
schema, validates each directive against it (and against the running state as
earlier directives apply, in order), applies them transactionally, and
**derives the resulting schema itself** from `old` + the directives, storing
that as the new `meta` row. The caller never passes a target schema, so the two
can never disagree.

`pushSchema` stays purely additive. Migrate is the only destructive verb, and
it is always a deliberate, separate call. A migrate that changes structure
subsumes the schema update (the derived schema is stored); a migrate that is
data-only (`setDefault` / `evalExpr`) leaves the schema untouched.

## Directive vocabulary

Tagged enum on the wire, `#[serde(tag = "type", rename_all = "camelCase")]` —
matching the rest of the protocol. Closed vocabulary; the server applies each
via generated SQL.

| `type` | Fields | Effect | Schema change? |
|---|---|---|---|
| `renameField` | `table`, `from`, `to` | `ALTER … RENAME COLUMN`; rewrite the jsonb key in every doc; fix index references | yes |
| `renameTable` | `from`, `to` | `ALTER TABLE RENAME`; fix `f_`/index physical-name references | yes |
| `changeType` | `table`, `field`, `to: FieldType`, `cast`, `default?` | coerce every doc value via `cast`; `ALTER` the `f_` column type; recompute | yes |
| `dropField` | `table`, `field` | drop the `f_` column; remove the jsonb key from every doc | yes |
| `dropTable` | `name` | drop the table and all its data | yes |
| `dropIndex` | `table`, `name` | drop the index only (data-safe — no document loss) | yes |
| `setDefault` | `table`, `field`, `value: Value` | `jsonb_set` `value` where the key is missing | no (data-only) |
| `evalExpr` | `table`, `set: string`, `expr: string`, `where?: string` | scoped raw-SQL doc rewrite (see below) | no (data-only) |

`cast` ∈ `{ "toString", "toNumber", "toInt64", "toBoolean" }`. Pure widening
changes (already accepted by `schema::is_widening_of`) remain a plain additive
`pushSchema` — `changeType` is specifically for **non-widening** coercions.

### `changeType` cast matrix & failure policy

| `cast` | Accepted old types | Failure mode |
|---|---|---|
| `toString` | number, boolean, int64, string | never fails |
| `toNumber` | string (parse), boolean (0/1), int64 (precision loss past 2⁵³ accepted) | fails on non-numeric string |
| `toInt64` | string (decimal digits), number (must be integer-valued, in range) | fails otherwise |
| `toBoolean` | string (`"true"`/`"false"`/`"1"`/`"0"`), number (0→false, else true) | fails otherwise |

The optional `default` chooses the failure policy:

- **absent** — a single un-coercible value fails the **whole migrate
  atomically**; the error names the offending row `id` and value. Nothing is
  committed.
- **present** — un-coercible rows take `default` instead, and the migrate
  proceeds. (Required for union-narrowing, where rows holding a dropped variant
  have no valid coerced value.)

### `evalExpr` — the scoped raw-SQL boundary

The C-option power op, scoped so it costs no real power:

```sql
UPDATE "db_{db}"."t_<table>"
   SET doc = jsonb_set(doc, '<set>', to_jsonb((<expr>)), true)
 [WHERE <where>]
-- then: recompute every indexed f_ column from the new doc; bump version
```

- `<expr>` and `<where>` are **admin-supplied SQL text** evaluated over the
  row's `doc` (and `id` / `created_at`). Full Postgres expression power: date
  parsing, string ops, math, jsonb ops, conditionals. This is the arbitrary
  transform capability.
- **Boundary (server-enforced before apply):** one table; mutates `doc` only;
  **no `FROM` clause / joins** (cannot read or touch other tables); **no DDL
  verbs** (cannot `DROP`/`TRUNCATE`/`ALTER`); scoped to the `db_{db}` schema.
  The server rejects a directive that parses outside this shape with
  `BadRequest`.
- The safety bound is **scoping, not parameterization**. The caller is an admin
  (already trusted via the admin key), so the expression text is the admin's
  own; the scope guarantee is that damage is confined to one table's documents.
  A determined admin can still write garbage into that one table — the accepted
  footgun of approach C, bounded to a single table. Mitigated operationally by
  dry-run-first (below) and by the existing pre-migrate `export-db`.

## Execution model

**Runs inside the committer's serialized turn.** The admin HTTP handler
enqueues `CommitterRequest::RunMigrate { db, directives, dry_run }`; the
committer executes the entire migrate in **one Postgres transaction, in its
serialized loop**, then calls `fan_out` and publishes at the same tap sites as
`handle_mutate`.

Rationale: `pushSchema` may run off-committer only because it never mutates the
`doc` jsonb and never needs to notify subscribers (adding a column does not
change query results). A migrate *does* rewrite `doc`, which changes query
results and must fire subscriptions, the op-feed/`/admin/stream`, the audit
log, and webhook outbox. Per the codebase invariant ("any future code path that
commits a document txn must publish at the tap sites, or the op-feed silently
misses those writes"), running migrate **inside** the committer's loop gets all
of those for free and preserves the single-writer invariant (no second
concurrent writer).

**Stall scope is one database.** `Committers` keys its channels by db name
(`committer.rs`: `HashMap<String, Sender>`, one lazily-spawned task per db). A
`RunMigrate` for `db_A` runs in `db_A`'s committer and serializes only `db_A`'s
writes. Writes to every other database on the instance are served by
independent committer tasks and are unaffected.

**Atomicity.** One transaction. Any directive failure, any cast failure, any
DDL error → full rollback; schema unchanged; zero rows touched. The derived
schema is stored only on commit.

**Concurrency.** Writes to the migrated database queue behind the migrate for
its duration (acceptable — even desirable — for a destructive admin op; you do
not want a write racing a `renameField`). Writes to other databases are
unaffected.

## Downstream correctness (the load-bearing invariants)

Because migrate runs in the committer's turn and rewrites `doc`, it must
satisfy every guarantee `handle_mutate`/`handle_scheduled` already satisfy:

1. **Subscription `fan_out`** — affected subscriptions re-run and push the new
   results, so reactive live queries reflect the migrate. Table-level
   invalidation (the over-approximating safe choice already used by
   `distinct`/`aggregate`/`search`/`vector`) is the baseline; structural
   directives that change a `collect`/`unique` projection or an indexed sort
   key also require it.
2. **Op-feed / `/admin/stream`** — publish one `DocOp` per rewritten document,
   so the operator feed and `WS /admin/stream` see the migrate.
3. **Audit log** — when `RTDB_AUDIT_LOG_ENABLED`, write one `rtdb.audit_log`
   row per `DocOp` (same shape: `ts_ms, db, table, op, doc_id, principal,
   source`).
4. **Webhook outbox** — when `RTDB_WEBHOOKS_ENABLED`, enqueue one
   `rtdb.webhook_deliveries` row per matching `DocOp`.

The migrate handler publishes at the **same tap sites** as
`handle_mutate`/`handle_scheduled`. Data-only directives (`setDefault`,
`evalExpr`, `changeType` coercion) clearly produce `DocOp`s. Structural
directives that rewrite or remove the `doc` jsonb — `renameField` (key rename),
`dropField` (key removal), `dropTable` (bulk delete) — produce `DocOp`s for the
affected rows. `renameTable` and `dropIndex` touch no document values directly
(only the table/index name or the index itself), so they publish a table-level
`fan_out` (subscriptions re-run) but no per-row `DocOp`.

## Pre-apply validation

Before the transaction opens, every directive is checked against the current
schema and rejected as `BadRequest` with nothing applied:

- Source field/table/index exists for `rename*`/`changeType`/`drop*`/`setDefault`/`evalExpr`.
- `rename*`/`changeType` target name is free and not produced by an earlier
  directive in the same request.
- `cast` is valid for the old→new type pair (per the matrix); `changeType`'s
  `to` is a valid `FieldType`.
- `evalExpr.set` is a valid field path; `expr`/`where` parse within the scoped
  shape (no `FROM`, no DDL verbs).
- No duplicate target names across directives; no directive targets a table an
  earlier directive dropped.

## Dry-run

The request carries `dryRun?: bool` (default `false`). When `true`, the server
executes the directives inside a transaction it **rolls back**, returning a
per-directive report without committing:

```
MigrateResult {
  applied: bool,            // false on dryRun
  schema:  SchemaDef,       // the derived resulting schema (returned either way)
  directives: [ DirectiveReport ]
}
DirectiveReport {
  type:          <op>,
  affectedRows:  i64,   // rows whose stored document changed (field-carriers), not every row
  castFailures:  [{ id, value }],   // changeType only; empty otherwise
  sampleChanges: [{ id, before, after }]   // capped (e.g. 10), data ops only
}
```

The dashboard migrate flow and `rtdb migrate` CLI **default to dry-run-first**:
preview the report, confirm, then apply. Strongly recommended for any
`changeType`/`evalExpr` over populated tables.

## Wire contract (four implementations, byte-identical)

`Migrate` request and `MigrateResult` response join the four-implementation wire
contract: `server/src/protocol.rs`, `ts-client/src/protocol.ts`,
`rust-client/src/wire.rs`, `python-client/src/par_rt_db/wire.py`. Serde tags and
field names match exactly (the casing is non-uniform and load-bearing, like the
rest of the protocol). The `Directive` enum embeds `FieldType` for
`changeType.to`, reusing the existing four-client `FieldType` representation.

## Client surface

Mirrors the existing `Mutation` / `FilterExpr` builder pattern, in all four
clients:

- A `Migration` builder: `.renameField()` / `.renameTable()` / `.changeType()`
  / `.dropField()` / `.dropTable()` / `.dropIndex()` / `.setDefault()` /
  `.evalExpr()`, plus `.dryRun()`.
- An admin execute method: `RtDbAdminClient.migrate(db, migration)` (TS),
  `admin.migrate(...)` (Rust), the equivalent on the Python admin client, and a
  `rtdb migrate` CLI subcommand.
- **Dashboard:** a guided migrate flow (dry-run → review report → confirm →
  apply) beside push-schema in the operator console.
- **In-memory test harnesses** (ts/rust/python): gain the structural and data
  directives so app-level flows are testable offline. `evalExpr` honestly
  throws "unsupported in-memory" — the same convention as the existing
  search/vector stubs (no SQL engine in the harness).

## Testing

- **`server/tests/migration_test.rs`** (module-per-binary convention): rename
  field/table round-trips; each `changeType` cast including atomic
  rollback-on-failure and `default` substitution; `dropField`/`dropTable`/
  `dropIndex`; `setDefault` on missing keys; `evalExpr` scoped rewrite **and**
  boundary rejections (`FROM`/join, DDL verb, cross-table → `BadRequest`);
  dry-run commits nothing and returns the report; derived-schema correctness
  after each op.
- **Invariant tests (load-bearing):** after a data migrate, (a) a live
  subscription sees the new values (`fan_out`), (b) the op-feed/`/admin/stream`
  publishes, (c) an audit row is written when `RTDB_AUDIT_LOG_ENABLED` — proving
  the committer routing fires all tap sites. Plus a concurrency test: a write
  to the **same** db queues behind a migrate; a write to a **different** db
  does not.
- **Cross-client byte-identical round-trip** tests (the existing
  `query_combinations` pattern) for the `Directive`/`MigrateResult` shapes.
- **Per-client** builder-shape and dry-run-report-decode tests.

## Docs to update (the doc-sync invariant)

- `docs/superpowers/specs/2026-07-21-par-rt-db-design.md` line 99 — graduate
  past "(MVP)" to reference this spec.
- `FEATURE_MATRIX.md` §1 — schema-migration row 🟡→✅, with a new gap-row note
  describing the directive set and the `evalExpr` scoped escape; client-mirror
  status noted.
- Client READMEs (`ts-client`, `rust-client`, `python-client`, `cli`) — a
  migrate section; dashboard README — the guided flow.
- `CLAUDE.md` architecture section — note `migrate` as a third per-db committer
  request arm alongside `handle_mutate`/`handle_scheduled`, and that it
  publishes at the same tap sites.

## Security note

`evalExpr` runs admin-supplied SQL text. It is admin-only (constant-time admin
key or admin-allowlist OAuth), scoped to one table's `doc` jsonb with no
cross-table/DDL verbs, and dry-run-first by default. It is deliberately not
parameterized — the admin is the trusted author of the expression. The scope is
the safety bound; pre-migrate `export-db` is the recovery path.
