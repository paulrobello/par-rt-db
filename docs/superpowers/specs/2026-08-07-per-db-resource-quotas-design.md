# ENH-011 — Per-Database Resource Quotas

- **Date:** 2026-08-07
- **Status:** Approved (design); implementation pending
- **Source:** `ENHANCEMENTS.md` ENH-011
- **Approach:** Hard enforcement, global caps (Approach 1)

## Overview

One par-rt-db instance hosts many named databases, and today nothing stops one
database's tables, storage, or subscriptions from crowding out the rest. ENH-011
adds optional per-database resource quotas — `maxTablesPerDb`,
`maxStorageBytesPerDb`, `maxSubsPerDb` — enforced at create / push-schema /
migrate / mutate / subscribe, configurable at runtime via hot config. A `0` value
means **unlimited** (quota disabled), so the feature is off by default and an
operator opts in per resource by setting a non-zero cap.

Quota **values** are global: the same three caps apply to every database. This
matches the spec's "configurable via hot config" phrasing and fully achieves its
stated goal — no single db can crowd the others — with the smallest surface area.
Per-named-db overrides are explicitly out of scope (see Alternatives).

## Goals

- Prevent any one database from consuming unbounded tables, storage bytes, or
  concurrent subscriptions on a shared instance.
- Make caps runtime-mutable (no restart) through the existing hot-config path.
- Enforce **hard** — an over-cap request is rejected, not merely observed.
- Keep the feature opt-in and off by default; zero behavior change unless an
  operator sets a cap.

## Non-Goals

- Per-named-db limit overrides (global caps only). Deferred — see Alternatives.
- Usage *billing* or per-period averaging. This is a hard fairness gate, not a
  metering/billing system.
- Limiting the *number of databases* per instance (a separate, simpler cap;
  not requested here).
- Client-side pre-validation. The server is the source of truth; clients surface
  the rejection, they do not predict it.

## Background — current state

- **No quota concept exists.** There is no per-db metadata beyond
  `rtdb_auth.databases (name, created_at)` (`db.rs:60`) and the `<schema>.meta`
  schema JSON. `max_file_size` (upload) and `RTDB_MAX_AFFECTED_DOCS` (mutate step
  count) are the only resource caps, and neither is per-db.
- **Hot config** (`config.rs`): `HotConfig` is a single jsonb blob in one row
  `rtdb_config (id=1, hot)`, held on `AppState` as `Arc<ArcSwap<HotConfig>>`.
  Consumers read `state.runtime.hot.load().<field>`; `PATCH /admin/config`
  validates → persists → `store()`s live (no restart). Decode is **lenient** via
  `PersistedHotConfig` (every field `Option<T>` + `#[serde(default)]`) and
  `merge_onto` — this exists because a past prod incident silently dropped every
  operator PATCH when `idempotencyTtlMs` was added under strict decode. Any new
  field MUST be added to `HotConfig`, `PersistedHotConfig`, **and** `merge_onto`,
  or the incident recurs.
- **`HotConfigPatch`** is `deny_unknown_fields`, so every client mirror of
  `HotConfig`/`HotConfigPatch` must land in lockstep or a client PATCH 400s.
- **Storage size is already measurable live.** `db_stats` (`admin.rs:1369`)
  loops each table running `pg_total_relation_size`. There is **no maintained
  size counter** anywhere — every consumer re-queries.
- **Subscriptions are already sharded per-db.** `SubscriptionManager` holds
  `Mutex<HashMap<String /*db*/, Arc<Mutex<DbSubs>>>>` (`subs.rs:787`); a per-db
  count is a one-method addition following the sanctioned clone-shard-Arc-then-
  lock pattern (`subs.rs:920`).
- **Table count** is `SchemaDef.tables.len()` (`schema.rs:185`); both
  `ddl::push_schema` (after `schema.validate()`, `ddl.rs:213`) and migrate's
  `handle_migrate` see the proposed final schema before any DDL.

## Design

### A. Configuration model

Three new fields on `HotConfig` (`config.rs:442`):

```rust
pub struct HotConfig {
    // …existing 4 fields…
    pub max_tables_per_db: usize,       // RTDB_MAX_TABLES_PER_DB,         default 0
    pub max_storage_bytes_per_db: u64,  // RTDB_MAX_STORAGE_BYTES_PER_DB, default 0
    pub max_subs_per_db: usize,         // RTDB_MAX_SUBS_PER_DB,          default 0
}
```

- **`0 == unlimited`** for all three → feature off by default.
- `HotConfig::from_env` (`config.rs:455`) seeds the three `RTDB_*` defaults.
- `PersistedHotConfig` (`config.rs:571`) gets the three `Option<T>` mirrors with
  `#[serde(default)]`, and `merge_onto` (`config.rs:588`) takes persisted-when-
  present else env. **Skipping this replays the prod incident** (a persisted row
  missing the new field would silently revert to env on every boot, and a PATCH
  carrying the new field would round-trip-lossy).
- `HotConfigPatch` (`admin.rs:1651`) gets three optional fields; validation is
  "present → assign" (unsigned types already forbid negatives; **no hard
  ceiling** — unlike `max_file_size`, clamping a *limit the operator chose*
  would be wrong).
- `GET /admin/config` needs no change: `hot` is already serialized in full
  (`admin.rs:1634`); the three fields appear automatically and are not secrets
  (no redaction).
- **Env-drift gate** (`scripts/env-drift-check.sh`, a `checkall` dependency):
  add the three `RTDB_*` to **both** `.env.example` and `docker-compose.yml`'s
  `environment:` block, or `make checkall` fails.

### B. `maxTablesPerDb` — push-schema + migrate

A pure helper in `schema.rs`:

```rust
pub fn check_table_quota(schema: &SchemaDef, cap: usize) -> Result<(), RtDbError> {
    if cap > 0 && schema.tables.len() > cap {
        return Err(RtDbError::quota_exceeded(format!(
            "db has {} table(s), limit is {cap}", schema.tables.len()
        )));
    }
    Ok(())
}
```

Called from both schema-accepting entry points after the final `SchemaDef` is
known, each reading the live cap from its own scope:
- `admin::push_schema` handler → after `schema.validate()`, before
  `ddl::push_schema`; cap from `state.runtime.hot.load().max_tables_per_db`.
- `committer::handle_migrate` → after `plan_migration` produces the final
  schema, before `apply_schema_additive`; cap from `ctx.hot.load()`.

Count is `SchemaDef.tables.len()` — **user-declared tables only**. Framework
side-tables (`meta`, `mutations`, `scheduled_txns`, `schema_history`) never count.

### C. `maxSubsPerDb` — subscribe

In `committer::handle_subscribe` (`committer.rs:963`), before
`ctx.subs.register()` (`committer.rs:1015`). This sits inside the committer's
serialized per-db turn, so the count cannot race a concurrent subscribe on the
same db.

One new method on `SubscriptionManager` (`subs.rs`), following the sanctioned
shard-access pattern at `subs.rs:920`:

```rust
pub async fn count_for_db(&self, db: &str) -> usize {
    let shard = { /* clone shard Arc under the outer lock, then drop it */ };
    shard.lock().await.len()
}
```

Check: `cap > 0 && count >= cap` → `SubscribeErr { code: QuotaExceeded }`,
**connection stays open** (same shape as `RATE_LIMITED`). Subscribe is WS-only —
there is no HTTP subscribe path.

### D. `maxStorageBytesPerDb` — mutate + storage upload

#### Measurement: cached live query

A per-db usage cache on `AppState`:

```rust
// exact concurrent-map type settled in planning (DashMap if a dep, else
// RwLock<HashMap> matching the subs registry idiom)
db_usage: <ConcurrentMap<String /*db*/, StorageUsage>>

struct StorageUsage { bytes: u64, computed_at_ms: i64 }
```

One live query per schema (single statement, schema name `$1`-bound — never
interpolated):

```sql
SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0)::bigint
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = $1
  AND c.relkind = 'r'                              -- heap + indexes + TOAST
  AND c.relname NOT IN ('meta', 'mutations',
                        'scheduled_txns', 'schema_history');
```

This sums the **full on-disk footprint of all user tables** (document heaps +
their indexes + TOAST) **plus the `storage` blob table**, excluding fixed
per-db framework bookkeeping. `pg_total_relation_size` reads
`pg_class.relpages` from the catalog cache — sub-millisecond even for hundreds
of tables. **Maintenance note:** the exclusion list must be kept in sync if a
new framework side-table is added.

This deliberately supersedes the two-query form (relation size + separate
`SUM(size)` on `storage`): including the `storage` table in the relation sum is
more accurate than summing its logical `size` column (on-disk bytea with TOAST
compression ≠ declared byte length) and is a single statement.

#### Enforcement cadence

At the top of each **growing** write arm — `handle_mutate`, `handle_scheduled`,
`handle_migrate` (`committer.rs`). The TTL reaper is a shrinker and **skips**
enforcement (it only helps). `RTDB_QUOTA_CACHE_TTL_SECS` is a boot `Config`
value (default 60).

1. Read the cached `StorageUsage` for the db.
2. **Fresh** (`now − computed_at_ms < ttl`) → enforce against `bytes`.
3. **Stale or absent** → run the live query synchronously (sub-ms, at most once
   per TTL window per db — negligible cost inside the serialized turn), update
   the cache, enforce.
4. `bytes >= cap` → reject `QuotaExceeded` / HTTP **507**.
5. **Post-commit best-effort refresh** — spawn a recompute for that db
   (fire-and-forget, mirroring the audit/webhook tap pattern; never blocks the
   client) so the next write sees a near-current value.

The cache means a db can briefly overshoot its cap by the writes that land
within one TTL window (≈ a few MB at 60 s). This is acceptable for fair
multi-tenant use and far tighter than comparable systems (Supabase averages
across the billing period; Neon lags 15 min–1 h).

#### File blobs (`storage::put`)

`storage::put` bypasses the committer (`storage.rs:85`), so the upload handler
(`http_api.rs:411`) enforces separately: after the size limit is resolved and
before write, check `cached_usage_for(db) + blob_len > cap` → 507; after a
successful put, refresh the cache for that db.

#### Over-quota-at-enable

If a db is already over the cap when quotas are turned on (or a cap is lowered
below current usage), further **growing** writes fail; reads and same-size
updates still succeed. This is the Supabase/Neon "read-only past threshold"
behavior — existing data is never locked out, only growth is blocked.

#### Cache eviction

On `delete-db` (`POST /admin/delete-db`), drop the db's cache entry. (A stale
entry for a deleted db is harmless, but eviction keeps the map tidy.)

## Error model

One new `ErrorCode::QuotaExceeded` → wire string `"QUOTA_EXCEEDED"`,
`error.rs`'s 1:1 `status()` maps it to **507 Insufficient Storage** (the apt
status for the dominant storage case). A constructor
`RtDbError::quota_exceeded(msg)` mirrors `conflict`/`rate_limited`.

| Surface | Transport | Result | Connection |
|---|---|---|---|
| storage (mutate, upload) | HTTP | 507 `QUOTA_EXCEEDED` | n/a |
| tables (push-schema, migrate) | HTTP | 507 `QUOTA_EXCEEDED` | n/a |
| subs (subscribe) | WS | `SubscribeErr{code:"QUOTA_EXCEEDED"}` | stays open |

A storage-over mutate fails the txn as a normal `MutateErr` (existing txn-error
surface); the check runs at `handle_mutate` entry, so nothing partial is written.

> **Single-code decision:** 507 is exactly right for storage but mildly off for
> tables ("too many tables," not "no space"). The single `QUOTA_EXCEEDED` code is
> chosen for a simpler four-client mirror; the message disambiguates
> (`"db 'foo' has 25 tables, limit is 20"`). If the tables 507 reads wrong in
> review, split it to a second code `TableQuotaExceeded` → 422 — a cheap,
> localized change.

## Admin bypass

An admin (`is_admin` at the WS handshake, or admin bearer on
`/admin/db/{db}/mutate` + `/sync`) **skips all three checks**, reusing the same
flag that already bypasses `authorize` + per-row `ownerField`. Rationale: the
operator sets the caps and must not be locked out by their own limits during
restore/migrate. For a large restore, raise the cap first.

## Client mirror

No DSL or wire *frame* changes — enforcement is server-side. Two typed additions
per client (exact files from the hot-config audit):

- **`QUOTA_EXCEEDED` error code** in the error-code union:
  `ts-client/src/protocol.ts`, `rust-client/src/wire.rs`,
  `python-client/src/par_rt_db/wire.py`.
- **`HotConfig` + `HotConfigPatch`** gain the three fields:
  `ts-client/src/admin.ts:96/118` + `dashboard/src/lib/types.ts:118/226`,
  `rust-client/src/wire.rs:730/763`,
  `python-client/src/par_rt_db/http_client.py:178/207`.

`deny_unknown_fields` on the patch makes this strictly required — all four
clients land together or a client PATCH 400s.

## Dashboard + metrics

- `db_stats` response (`admin.rs:1369`) gains `storageQuotaBytes` +
  `storageUsedBytes` (and tables/subs usage+cap) so the dashboard renders a usage
  bar — "Storage: 142 MB / 500 MB".
- The hot-config panel gets the three new editable fields (types mirrored above).
- One new counter `rtdb_quota_rejections_total{db,kind="tables|storage|subs"}`,
  surfaced on the metrics page.

## Security

- **No untrusted input in SQL.** The schema name is `$1`-bound; physical names
  are validated/lowercased by `ddl::pg_schema` exactly as `db_stats` already
  does. No new interpolation.
- **Quotas do not weaken auth.** The checks run after the existing `authorize` /
  admin gate; they never replace it.
- **Admin bypass is deliberate and scoped** to the already-authenticated admin
  principal (see above).
- **No new secrets or tokens.** Config values are operator-set numeric limits,
  surfaced unredacted (not secrets).

## Testing

**Server:**
- *Config:* `HotConfig` round-trip + the persisted-row-missing-new-field
  regression — extend `config.rs` unit tests (`:654`–`718`) and
  `dashboard_test.rs:832` (`hot_config_round_trips_through_rtdb_config`). This is
  the test that prevents replaying the prod decode incident.
- *Tables:* push over cap → rejected; at-cap → ok; `cap=0` → unlimited; migrate
  over cap → rejected; admin bypass.
- *Subs:* open > cap subs → Nth rejected, connection stays open; admin bypass;
  `cap=0` unlimited.
- *Storage:* seed a db, low cap, exceed → 507; cache stale→live re-query; post-
  write refresh; `storage::put` exceed → 507; admin bypass; delete-db evicts the
  cache entry; over-quota-at-enable (already-over db blocks growth, reads fine).
- *Metrics:* rejection counter increments per `{kind}`.

**Clients:** `HotConfig` field round-trip in each client's suite; the new error
code round-trips through (de)serialize.

## Rollout / back-compat

- **Default off.** All three caps default to `0` (unlimited); zero behavior
  change unless an operator sets a non-zero value. No migration required for
  existing deployments — `rtdb_config`'s lenient decode absorbs the new fields.
- **Additive only.** No existing wire frame, DSL step, or error code changes.
- **`make checkall`** is the gate, including the env-drift check (both env files
  must carry the three new `RTDB_*` vars).

## Alternatives considered

- **Per-named-db overrides (global default + per-db column).** More flexible
  (prod-db capped higher than staging), but adds a `quotas jsonb` column on
  `rtdb_auth.databases` + new admin CRUD endpoints + client surface — beyond the
  spec's "configurable via hot config." Rejected for v1; deferred to a follow-up
  if a real need surfaces.
- **Soft (observe-only) enforcement.** Lowest risk, but does not *prevent* the
  noisy-neighbor problem the spec targets — it only reports it. Rejected.
- **Hard for tables+subs, soft for storage.** A pragmatic risk-minimizing split,
  but the storage cache is sound (sub-ms live query, tight TTL) and storage is
  the resource most worth gating. Rejected in favor of uniform hard enforcement.
- **Maintained byte counter** (increment/decrement at each write). Rejected:
  drifts via VACUUM page reuse, reindex, TOAST compression, crash mid-txn, and
  the `storage::put`/admin write paths that bypass the committer — and needs a
  periodic recompute to reconcile, at which point the counter earns nothing over
  the cached live query.

## Open questions

- **Tables → 507 vs 422** (see Error model). Default: single `QUOTA_EXCEEDED` →
  507. Confirm or split during implementation review.
