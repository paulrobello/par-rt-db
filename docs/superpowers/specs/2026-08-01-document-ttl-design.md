# Document TTL / Auto-Expiry — Design

**Date:** 2026-08-01
**Status:** Draft (pending plan)
**Kanban:** `par-rt-db` — "Document TTL / auto-expiry" (`019fbe205cf57302af1f72f8c1e9ea8b`)
**Estimate:** M

## Motivation

par-rt-db has no document-level TTL today. Every `expires_at` in the server is for
sessions, tokens, mutation-dedup, webhooks, or scheduled txns — never a document.
Apps that need expiry (sessions, caches, rate-limit windows, ephemeral data, OTP
secrets) must roll their own via scheduled txns. This adds a first-class,
declarative document TTL: a table declares a field that holds each document's
absolute expiry; a per-db background reaper deletes expired rows through the
existing single-writer committer, so every TTL delete publishes through the same
four tap sites (subscription fan-out, op-feed, audit, webhook) as any other write.

Convex has no built-in document TTL, so this is a par-rt-db advantage (tracked in
`FEATURE_MATRIX.md`).

## Goals

- Declarative, per-document TTL expressed in the schema.
- Server-side reaper that deletes expired documents with full op-feed / audit /
  webhook / subscription coverage.
- Preserves every load-bearing invariant: single-writer committer, additive-only
  schema migrations, byte-identical client mirroring.

## Non-goals

- Hot-configurable sweep cadence (boot env only — TTL is not latency-critical).
- A clock/test-time seam for `now_ms()` (tests use a short real sweep interval;
  injecting a clock is a separate, repo-wide refactor).
- Per-row-auth predicate evaluation for TTL (TTL deletes are system-initiated and
  bypass per-row auth, exactly like scheduled jobs).
- Automatic re-stamping of TTL on patch/replace (the field is just a field; see
  Write Path).

## Expiry model

**Mongo-style field + optional default** (decided). A table declares:

```jsonc
{
  "sessions": {
    "fields": { "expiresAt": "number", "userId": "string" },
    "indexes": [{ "name": "by_expiresAt", "fields": ["expiresAt"] }],
    "ttl": { "field": "expiresAt", "defaultDurationMs": 86400000 }
  }
}
```

The TTL is the value of the declared `expiresAt` field — an absolute epoch-ms
timestamp. The app may set it per document; `defaultDurationMs` stamps it at
insert time when the client omits it. This is maximally flexible (per-document
expiry), idiomatic (matches Mongo TTL indexes and DynamoDB TTL attributes), and
reuses par-rt-db's existing typed-column index machinery for the reaper scan.

## Schema

Additive `ttl` on `TableDef`. Old schemas deserialize unchanged.

```rust
pub struct TtlDef {
    pub field: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_duration_ms: Option<i64>,
}

pub struct TableDef {
    pub fields: BTreeMap<String, FieldType>,
    #[serde(default)]
    pub indexes: Vec<IndexDef>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "ownerField")]
    pub owner_field: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "collaboratorsField")]
    pub collaborators_field: Option<String>,
    /// Additive: declare a field whose value is each document's absolute
    /// epoch-ms expiry. A per-db reaper deletes rows whose value is in the
    /// past. Omit entirely for tables that never expire.
    #[serde(default, skip_serializing_if = "Option::is_none", rename = "ttl")]
    pub ttl: Option<TtlDef>,
}
```

Wire casing: `ttl`, `field`, `defaultDurationMs`.

`TtlDef` is mirrored byte-for-byte in rust-client (`schema.rs`), ts-client
(`schema.ts`), and python-client (`schema.py`).

### Validation (`schema::validate`)

When `ttl` is `Some`:

1. `ttl.field` must name a declared field of numeric type — `Number` or `Int64`.
   (`Number` → `double precision`, `Int64` → `bigint`; both represent epoch-ms
   exactly for all realistic timestamps.)
2. There must exist a **declared** btree index on exactly `[ttl.field]`: an
   `IndexDef` with `search == false`, `vector.is_none()`, `unique == false`,
   `r#where.is_none()`, and `fields == [ttl.field]`. The server does **not**
   auto-create this index — it is explicit in the client schema (matches Mongo,
   and avoids inventing a server-managed implicit index that would tangle with
   `detect_destructive_changes` when `ttl` is later removed). A multi-field index
   does not satisfy the TTL requirement.
3. `default_duration_ms`, if `Some`, must be `> 0`.

Any violation is `BAD_REQUEST` at `push_schema`, before any DDL runs.

## Storage / DDL

`expiresAt` is a **normal declared field**, not a system column. Because the
validation above requires a btree index on it, it already receives a typed
`f_<field>` column (`double precision` or `bigint`) via the existing
`indexed_fields` + `indexed_column_type` path. No new system column, no
`merge_doc` change, no read-time merge — the value is transparently present in
the document.

`push_schema` (`ddl.rs`) additions when `ttl` is present:

- The `ttl.field` is already covered by `indexed_fields` (its index forces a
  column), so the `f_<field>` column is created with the table (new table) or
  added + backfilled from `doc` (existing table) by the existing code path.
- **Backfill on add** (decided: stamp from `createdAt + default`): if
  `default_duration_ms` is `Some`, in the same migration tx stamp existing rows
  lacking the field. Because the read path (`merge_doc`) returns the jsonb `doc`
  and never the typed column, the backfill must update **both** the typed column
  (for the reaper/index scan) and the `doc` (for reads), gated on the column
  being NULL so a caller-set value is preserved:
  ```sql
  UPDATE "{schema}"."{table}"
  SET "f_<field>" = created_at + $default,
      doc = doc || jsonb_build_object('<field>', created_at + $default)
  WHERE "f_<field>" IS NULL
  ```
  This matches insert-time stamping (Task 2 writes the `doc`; the column is
  derived). Rows whose `created_at + default` is already in the past get reaped
  on the next sweep (intentional — a retro-active TTL). If
  `default_duration_ms` is `None`, existing rows are untouched (only future
  inserts that omit the field are affected, and only if a default exists —
  otherwise the client must set the field on every write).

Removing `ttl` from a table is **not** a destructive change (it toggles expiry
behavior; the user-declared index on the field remains, governed by normal
destructive-change rules). The reaper simply stops considering the table.

## Write path

**TTL is the value of the `expiresAt` field — nothing more.**

- **Insert** (`txn.rs`, `Insert` step, which holds the `TableDef`): if
  `ttl.default_duration_ms` is `Some` and the document omits `ttl.field`, stamp
  `doc[field] = now_ms() + default_duration_ms` *before* column/bind extraction.
  The stamped value flows into the `f_<field>` column through the normal
  indexed-column bind path and is visible in the returned document.
- **Patch**: no special handling. Patching `expiresAt` updates the `f_<field>`
  column via existing indexed-column maintenance on the patch path (the same
  invariant every btree index relies on). Omitting the field leaves it unchanged.
- **Replace**: no special handling. A replace that omits `expiresAt` produces a
  document without it ⇒ the `f_<field>` column is NULL ⇒ the document stops
  expiring. This matches Mongo semantics. `defaultDurationMs` is never re-stamped
  after insert.

`defaultDurationMs` therefore only ever fills the field in at insert time when
the client omitted it. After that, `expiresAt` is an ordinary field.

## Reaper

The reaper is a **third per-db background task**, spawned in
`Committers::channel_for` alongside `scheduler::run_scheduler` and
`mutation_log::run_cleanup`. It is a hybrid of the two existing models: a dumb
periodic timer (like `run_cleanup`) that enqueues a fire-and-forget committer
request (like `run_scheduler`), because the actual delete must run inside the
committer's serialized turn to preserve the single-writer invariant and publish
at the tap sites.

### Lifecycle (`reaper::run_reaper`)

Mirrors `mutation_log::run_cleanup`:

```
run_reaper(pool, db, committer_tx, sweep_interval):
    tick = interval(sweep_interval); tick.tick().await  // skip immediate first tick
    loop {
        tokio::select! {
            _ = tick.tick() => {
                if database_exists(pool, db) == Ok(false) { return }  // db dropped
                if committer_tx.send(RunReaper).await.is_err() { return }  // committer gone
            }
            _ = committer_tx.closed() => { return }
        }
    }
```

- Self-terminates on database deletion (the `database_exists` check, same signal
  `run_scheduler`/`run_cleanup` use so `delete-db` retires the task cleanly
  instead of perpetual error spam).
- Self-terminates when the committer channel closes (its task died).
- Spawns with `tx.clone()` like the other two tasks; the next request to the db
  respawns all three.

### Committer arm

Add a fire-and-forget arm to `CommitterRequest`:

```rust
/// A TTL reaper sweep is due. Fire-and-forget like `RunScheduled`: the reaper
/// task does not wait for a reply. The committer runs the batch delete inside
/// its serialized turn and publishes through the four tap sites.
RunReaper,
```

Handled in `run_committer`'s `match` alongside `RunScheduled`; an error is logged
(`tracing::error!`), not propagated (there is no caller to surface to).

### `handle_reaper` (in the committer turn, full `CommitterCtx`)

1. Load the cached schema (`ctx.schemas.get`). For each table with `ttl`, run
   its delete as an **independent autocommit statement** on the pool (not wrapped
   in one multi-table tx) — a failure on one table is logged and does not abort
   the sweep for the others; the failed table retries next sweep:
   ```sql
   DELETE FROM "{schema}"."{table}" WHERE id IN (
       SELECT id FROM "{schema}"."{table}"
       WHERE "{f_field}" IS NOT NULL AND "{f_field}" < $1
       ORDER BY "{f_field}" LIMIT $2
   ) RETURNING id
   ```
   `$1 = now_ms()`, `$2 = ttl_batch` (config, default 5000). The subquery uses
   the required btree index on `f_field` for an efficient range + limit.
2. Collect the returned ids per table into a `WriteSet`:
   ```rust
   WriteSet {
       tables: {touched tables},
       docs: {(table, id) for each deleted doc},
       ops: vec![DocOp { table, id, kind: OpKind::Delete }, …],
       doc_values: empty,  // a delete captures no before/after ⇒ always re-run
   }
   ```
3. Fire the four tap sites with `owner = None`, `source = "ttl"`:
   - `subs.fan_out` — a delete captures no `doc_values`, so subscriptions
     table-level re-run (the sound over-approximation, already correct). The
     expired document is absent in the re-run, so subscribers see the removal.
   - `op_feed.publish` — connected clients / the op-feed see the delete.
   - `audit::write_audit_rows(…, source = "ttl")` — best-effort, warned on
     failure.
   - `webhook::enqueue_for_ops(…, source = "ttl")` — best-effort, warned on
     failure.
4. Increment `rtdb_ttl_expired_total{db,table}` by the deleted count.

A failed delete is logged, not surfaced. A row past expiry that fails this sweep
is retried on the next sweep (at-least-once). TTL deletes are system-initiated,
so they **bypass per-row `ownerField`/`collaboratorsField` authorization** —
identical to scheduled jobs (`owner = None`, `principal = null` in audit/webhook).
The db-level token/session/allowlist gate still applies to the transports; the
reaper is an internal task and is not subject to it.

### Bounded drainage

Each sweep deletes at most `ttl_batch` rows per TTL table. A backlog larger than
the batch drains over consecutive sweeps (one `RunReaper` per `sweep_interval`).
The default batch (5000) drains a 1M-row burst in a few minutes, which is
acceptable for best-effort expiry; both knobs are configurable. The reaper does
not pace faster than the configured interval (no reply-based "more remaining"
loop) — simplicity over burst latency.

## Config & metrics

Boot `Config` (not hot — TTL is not latency-critical):

| Env | Field | Default | Meaning |
|---|---|---|---|
| `RTDB_TTL_SWEEP_INTERVAL_SECS` | `ttl_sweep_interval_secs: u64` | 60 | Reaper cadence per db |
| `RTDB_TTL_BATCH` | `ttl_batch: i64` | 5000 | Max rows deleted per table per sweep |

Plumbed through `Committers`/`CommitterCtx` (interval → `run_reaper`, batch →
`handle_reaper`).

Metric: `rtdb_ttl_expired_total{db,table}` (counter), incremented in
`handle_reaper`; surfaced on the dashboard metrics page alongside the existing
subscription/skip counters.

## Clients

`ttl` is additive on `TableDef`; the field is declared normally so client type
inference is unchanged. Each SDK mirrors the server struct and the in-memory
semantics:

- **ts-client** (`schema.ts`): `ttl?: { field: string; defaultDurationMs?: number }`.
  In-memory harness (`in_memory.ts`): stamp default at insert when absent; add
  `tick(now?: number)` that removes docs where `doc[field] != null &&
  doc[field] < (now ?? Date.now())`.
- **rust-client** (`schema.rs`): `TtlDef { field, default_duration_ms }` with
  matching serde casing; in-memory `tick(now: i64)`.
- **python-client** (`schema.py`): `TtlDef` (pydantic, serializer drops `None`);
  in-memory `tick(now_ms: int | None = None)`.
- **CLI** (`cli/`): no change — `ttl` flows through the generic schema blob it
  already pushes.

Each client's `detect_destructive_changes` treats adding/removing `ttl` as
non-destructive (consistent with the server).

## Error handling

- Bad TTL config → `BAD_REQUEST` at `push_schema` / client schema validation,
  before any DDL.
- Reaper delete failure → logged (`tracing::error!`), retried next sweep. Never
  surfaced to a client (fire-and-forget).
- A TTL delete follows the exact tap-site contract of `handle_mutate` /
  `handle_scheduled` / `handle_migrate`, so the op-feed/audit/webhook "every
  durable write publishes here" guarantee extends to TTL.

## Testing

**Unit (server `schema.rs` + each client):** `validate` accepts `ttl` backed by
a proper single-field non-unique non-partial btree index on a numeric field;
rejects `ttl` with no qualifying index / wrong field / a multi-field index /
unique index / partial (`where`) index / search / vector index / non-numeric
field / undeclared field / `defaultDurationMs ≤ 0`.

**Integration (`server/tests/ttl_test.rs`, real dev Postgres):**

1. Insert with `defaultDurationMs`, omitting `expiresAt` → returned doc carries
   `expiresAt ≈ now + default` (±slack).
2. Insert with an explicit past `expiresAt`, sweep interval = 1s → poll the query
   until the doc is absent (assert within ~2s).
3. A subscription on the table receives an update reflecting the deletion.
4. With audit + webhooks enabled, the reaped row is recorded with `op = delete`,
   `source = "ttl"`, `principal = null`.
5. Backfill: push a table with rows, then add `ttl` + `defaultDurationMs` with
   rows whose `created_at` is old → next sweep reaps them.
6. Per-row auth: the reaper deletes a row owned by another user (bypass).
7. A non-TTL table on the same db is never touched by the reaper.

Clocks are real (`now_ms()`); tests use a short sweep interval + past expiry +
poll, consistent with how the rest of the suite handles time.

**Client in-memory (each SDK):** `tick()` expires docs; default stamped at
insert; patching `expiresAt` changes a doc's expiry; replacing without
`expiresAt` stops expiry.

## Docs to update

- `FEATURE_MATRIX.md` — new TTL row; note Convex has no built-in equivalent.
- `CLAUDE.md` — Architecture bullet (reaper is the third per-db task; tap-site
  `source = "ttl"`; single-writer preserved) and an Invariant entry (TTL deletes
  publish at the tap sites; reaper never writes outside the committer).
- This spec.
- Client READMEs that enumerate schema fields (ts / rust / python), if any.

## Kanban

Mark "Document TTL / auto-expiry" `done` only after `make checkall` passes and
all four clients + docs are updated.
