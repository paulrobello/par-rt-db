# ENH-013 — Schema Change History

**Status:** Implemented (2026-08-10) · **Date:** 2026-08-05 · **Card:** `[ENH-013] Schema change history`

## Problem

Schema pushes and migrate directives mutate the live schema in place with no
versioned record. The authoritative schema is a single `SchemaDef` JSON blob
stored at `key = 'schema'` in each database's per-db `meta` table
(`server/src/db.rs:311`, read by `load_schema` at `db.rs:474`). Two code paths
overwrite that row in place via `INSERT … ON CONFLICT (key) DO UPDATE SET value`
and discard the prior value:

- **Push** — `POST /admin/push-schema` → `ddl::push_schema`, whose persistence
  tail upserts `meta` (`server/src/ddl.rs:461`). This path does **not** run
  inside the committer (it is a direct admin-handler write) and fires no document
  tap sites.
- **Migrate** — `POST /admin/db/{db}/migrate` → `CommitterRequest::RunMigrate` →
  `handle_migrate`, whose tail upserts `meta` (`server/src/committer.rs:778`).
  This runs inside the per-db committer (single-writer) and fires the document
  tap sites.

There is no history, no versioning, and no snapshot of prior schemas anywhere in
the system (confirmed: a sweep for `schema_history` / `schema_version` /
`previous_schema` returns only an in-memory load used to reject destructive
pushes). Destructive migrations (drop field/table, change type) are therefore
not reversible from the console.

## Goals

1. Capture a full schema snapshot on every committed push and every applied
   (non-dry-run) migrate, plus on every restore.
2. Expose a history view in the operator dashboard: list, per-version snapshot,
   and a diff against the current schema.
3. Allow restoring a prior schema version — rebuilding the live schema **shape**
   to match a chosen snapshot — making destructive migrations reversible from
   the console.

## Non-goals

- **Data rewinding.** A snapshot is a schema-**shape** checkpoint, not a data
  checkpoint. Migrate data-transforms (rename field, change type) are not
  undone; restoring across them reconciles shape only (see Data-loss semantics).
- Time-travel reads of documents at a past schema version.
- Rust-client and Python-client admin mirrors (filed as follow-up backlog items;
  this change ships server + dashboard + ts-client).
- A server-side diff endpoint (v1 diffs client-side in the dashboard).

## Storage — per-db `schema_history` table (lazy, always-on)

```sql
CREATE TABLE IF NOT EXISTS "{schema}".schema_history (
    version     BIGSERIAL PRIMARY KEY,
    captured_at BIGINT NOT NULL,
    source      TEXT NOT NULL,   -- 'push' | 'migrate' | 'restore'
    principal   TEXT,            -- admin email/uid; NULL when system-initiated
    schema      JSONB NOT NULL   -- full SchemaDef snapshot
)
```

- **Per-db**, co-located with `meta` / `mutations` / `scheduled_txns` / `storage`
  in the database's own Postgres schema. Auto-cleanup on `delete-db` (`DROP
  SCHEMA`); naturally scoped; no orphaned rows (unlike the global `rtdb.audit_log`).
- **Created lazily**: the capture call runs `CREATE TABLE IF NOT EXISTS` before
  inserting, so pre-existing databases (created before this feature) self-heal
  without a boot migration.
- **Always-on, no boot config flag.** Schema history is low-volume (only on
  push/migrate/restore) and its value is being present when a revert is needed,
  so it defaults on — unlike `audit_log` / `webhooks`, which are off by default
  for volume / external-IO reasons. Always-on eliminates Config → Committers →
  CommitterCtx flag threading entirely. (An `RTDB_SCHEMA_HISTORY_ENABLED` opt-out
  can be added later if a deployment needs it.)
- **Soft retention cap:** prune to the last `MAX_SCHEMA_HISTORY_VERSIONS = 100`
  versions per database on each capture (a constant; no env var). Cheap insurance
  against a schema pushed in a loop. 100 is well above any realistic revert depth.

The latest row by `version` always equals the live schema (every mutation
captures the *new* current state; a restore captures outgoing-then-incoming).

## Capture — one shared function, two call sites

New module `server/src/schema_history.rs`:

```rust
/// Ensure the per-db history table exists, insert a snapshot row, and prune to
/// the retention cap. Best-effort contract identical to the audit tap: a failure
/// is warned, never propagated to the caller — the push/migrate has already
/// committed by the time this runs.
pub async fn capture(
    pool: &PgPool,
    db: &str,
    source: &str,                 // "push" | "migrate" | "restore"
    principal: Option<&str>,
    schema: &SchemaDef,
) -> Result<(), RtDbError>;

pub async fn list(pool: &PgPool, db: &str, limit: i64, offset: i64)
    -> Result<Vec<HistoryEntrySummary>, RtDbError>;   // no blobs

pub async fn get(pool: &PgPool, db: &str, version: i64)
    -> Result<Option<HistoryEntry>, RtDbError>;        // includes the schema blob
```

Capture is called at the two sites that overwrite the live schema:

- **Push** — in `admin/dbs.rs` `push_schema`, immediately after
  `ddl::push_schema` returns the applied `SchemaDef`. The handler already has the
  admin principal from `require_admin`, so it passes it through. Wrapped best-effort:
  `if let Err(e) = capture(...).await { tracing::warn!(...) }`.
- **Migrate** — in `committer.rs` `handle_migrate` (`committer.rs:736`), after the
  meta upsert commits and the cache is refreshed, alongside the existing tap-site
  block. `source = "migrate"`, `principal = None` (migrate carries no interactive
  principal — matches audit's `owner = None` for migrate).

Dry-run migrates do **not** capture (nothing is committed).

## Restore — new committer arm, destructive shape reconcile

`POST /admin/db/{db}/schema/restore` with body `{ version: i64, confirm: String }`.
The typed guard requires `confirm == db` (mirrors `delete-db`'s strongest guard).
Enqueues a new `CommitterRequest::RunRestoreSchema { target_version, reply }`,
following the established `RunMigrate` arm pattern (single-writer invariant
preserved).

`handle_restore_schema`:

1. `load_schema` the current schema; load the target snapshot from
   `schema_history` by `version` (scoped to this db — the table is per-db).
   404 if the version does not exist.
2. **Capture the outgoing (current) schema first** → history row, `source =
   "restore"`. This is the safety net: a restore is itself a versioned, undoable
   operation.
3. `pool.begin()` tx, then `ddl::reconcile_schema_destructive(&mut tx, &current,
   &target)`:
   - Tables in current but not target → `DROP TABLE`.
   - Tables in both → for each indexed-field column `f_<field>` and each index
     present now but absent in target → `DROP COLUMN` / `DROP INDEX`; for each
     field/index in target but not current → add (reuse `ddl.rs` additive
     primitives: `ALTER TABLE ADD COLUMN` + jsonb backfill, `CREATE INDEX`).
   - Tables in target but not current → `CREATE TABLE` (reuse `ddl.rs`).
   - Upssert `meta.schema = target` blob (same shape as the push/migrate tail).
   - Reuses the structural enumeration already computed by
     `detect_destructive_changes` (`schema_diff.rs` / `ddl.rs:219`) to identify
     the drop set, plus new minimal drop helpers.
4. `tx.commit()`, refresh the in-memory `SchemaCache`.
5. **Capture the incoming (target) schema** → history row, `source = "restore"`
   (restores the "latest row = current" invariant).
6. Re-evaluate subscriptions for the database (a dropped table invalidates its
   subs; handled by the existing `subs` re-run machinery).

`restore` does **not** write `audit_log` / `webhook` rows — those are per-`DocOp`
and a restore is DDL-only. `schema_history` is its own audit trail.

### Data-loss semantics

Documents are stored in the `doc` jsonb column; `f_<field>` columns are redundant
typed copies used only for indexing. Therefore:

- Removing an index column (`DROP COLUMN f_<field>`) **preserves document data**
  — the value remains in `doc` jsonb; the field is simply no longer indexed.
- The **only real data loss is `DROP TABLE`** for tables present now but absent in
  the target snapshot.
- Migrate data-transforms (rename field, change type) are **not** rewound. The
  snapshot records schema shape, not data; restoring across a rename yields an
  empty index column (the data lives under the renamed key in jsonb). The
  dashboard surfaces this clearly.

Net: restore is far less destructive than "destructive" implies — the dashboard
calls out specifically which **tables** will be dropped (the true data loss) and
notes that removed index columns merely un-index data.

## HTTP surface

| Method | Path | Returns |
|---|---|---|
| `GET` | `/admin/db/{db}/schema/history?limit=&offset=` | `{ entries: HistoryEntrySummary[] }` newest-first (no blobs); `limit` clamped `[1,1000]` default 100 |
| `GET` | `/admin/db/{db}/schema/history/{version}` | `HistoryEntry` (includes the full `SchemaDef`) |
| `POST` | `/admin/db/{db}/schema/restore` | `{ ok: true, restoredTo: version }` |

All admin-gated (`require_admin`). Diff is computed client-side in the dashboard.

## Clients

### ts-client (`ts-client/src/`)

- `protocol.ts`: wire types
  - `SchemaHistoryEntrySummary { version: number; capturedAt: number; source: "push" | "migrate" | "restore"; principal: string | null }`
  - `SchemaHistoryEntry { …summary; schema: SchemaDef }`
- `admin.ts`: `getSchemaHistory(db, opts?)`, `getSchemaVersion(db, version)`,
  `restoreSchema(db, version, confirm)`.

### dashboard (`dashboard/src/`)

- `lib/admin.tsx` `AdminClient`: the three methods above (raw `fetch`); types
  imported from `@par-rt-db/client` (consistent with `SchemaJson` import in
  `SchemaPage.tsx`).
- New page `pages/SchemaHistoryPage.tsx` + `SchemaHistoryPage.module.css`,
  routed at `dbs/:db/schema/history`:
  - Version list newest-first: version, `capturedAt` (formatted), source badge,
    principal.
  - Click a version → panel rendering the snapshot (reusing `SchemaPage`'s
    table/field/index rendering) plus a **client-side diff vs current**
    (tables/fields/indexes added & removed — a small local diff util).
  - "Restore to this version" button → confirm dialog requiring the db name →
    `POST restore` → refresh.
  - "History" link added to `DbPage` (alongside Schema / Migrate) and `SchemaPage`.

Rust-client and Python-client admin mirrors are **follow-up backlog items**, not
in this change.

## Invariants preserved

- **Single writer.** Restore runs as a committer arm; the destructive reconcile
  executes inside the committer's serialized turn. Push capture happens in the
  admin handler (push already writes `meta` outside the committer — unchanged).
- **SQL safety.** Identifiers double-quoted and length-capped via existing `ddl`
  helpers; no value interpolation. `fetch_optional` for the version lookup.
- **Best-effort capture.** A capture failure never fails a push/migrate/restore —
  the schema change has already committed.
- **Errors.** Failures use the `RtDbError` envelope; 500s carry a generic message.
- **Docs in sync.** Update `FEATURE_MATRIX.md` and the relevant README/docs when
  the feature lands.

## Testing

- **Server integration test** (new binary or folded into an existing one):
  - push captures one version (source `push`); a second push captures another;
    latest row == live schema.
  - applied migrate captures a version (source `migrate`); dry-run migrate does
    not.
  - restore writes two rows (outgoing + incoming, source `restore`), and the live
    schema afterward equals the target snapshot; restoring back to the outgoing
    version round-trips.
  - restore across a table present-now/absent-target drops the table (warned);
    restore that removes an index column preserves the doc jsonb.
  - per-db isolation (a second db's history is untouched); lazy table creation
    for a pre-existing db.
  - `GET history` pagination; `GET history/{version}` 404 for missing.
- **ts-client unit test** for the three new admin methods (against the in-memory
  harness or a mocked transport).
- **dashboard component test** for `SchemaHistoryPage` (list render, diff render,
  restore-confirm flow).
- **Gate:** `make checkall` (fmt-check + clippy `-D warnings` + typecheck + tests).
- **Doc fix bundled in:** correct the stale "two tap sites" doc comments in
  `audit.rs` and `webhook.rs` (the committer actually wires four: mutate,
  scheduled, migrate, reaper).

## Key file:line touch points

| Concern | Location |
|---|---|
| `meta` table DDL / `load_schema` | `server/src/db.rs:311`, `db.rs:474` |
| New module | `server/src/schema_history.rs` (mirror `audit.rs`) |
| Push capture | `server/src/admin/dbs.rs` (`push_schema` handler) |
| Migrate capture + restore arm | `server/src/committer.rs:736` (`handle_migrate`), new `handle_restore_schema`, `CommitterRequest::RunRestoreSchema` |
| Destructive reconcile | `server/src/ddl.rs` (new `reconcile_schema_destructive`, reuses `detect_destructive_changes` at `ddl.rs:219`) |
| New HTTP routes | `server/src/admin/` (route registration in `mod.rs`; handlers in `dbs.rs`) |
| ts-client wire + admin | `ts-client/src/protocol.ts`, `ts-client/src/admin.ts` |
| Dashboard client + page | `dashboard/src/lib/admin.tsx`, `dashboard/src/pages/SchemaHistoryPage.tsx`, `dashboard/src/App.tsx` (route) |

## Scope

Medium-large, matching the card. The riskiest piece is the destructive reconcile
(§Restore); the rest is mechanical mirroring of established patterns (audit tap,
`RunMigrate` arm, dashboard page).
