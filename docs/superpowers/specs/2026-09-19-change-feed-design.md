# Resumable per-db change feed (sequence-cursor polling) — design spec

Date: 2026-09-19 · Card: "Resumable per-db change feed (sequence-cursor polling)"
(board id `01a0bb207bd37b0399b2da4b4364d832`) · Status: implemented from this spec

## 1. Problem

Every realtime surface today requires a WebSocket: live query subscriptions, and
the admin op feed (`/admin/stream`). HTTP-only consumers — machines, agents,
cron jobs, constrained clients — have no sound way to stay current:

- The op feed is admin-only, carries no sequence number (reconnects replay ring
  duplicates, drops are indistinguishable from silence), and its 200-event ring
  evicts under load.
- A reconnect gap longer than the ring, or no WS at all, leaves a full re-query
  as the only option.

This spec adds a durable, resumable, per-database change feed over one-shot
HTTP: `GET /api/db/{db}/changes?since=<seq>` returns every committed document
op strictly after the cursor, in order, with post-image payloads, plus the
cursor to resume from. An expired cursor returns a typed error forcing an
explicit full resync — never silent data loss.

## 2. Goals / non-goals

**Goals**

1. Every committed document op in a db is observable exactly once per cursor
   position, in commit order, by an HTTP client holding a machine token.
2. Crash/lost-connection recovery is one request: replay from the last
   acknowledged cursor.
3. An impossible cursor (older than retention, or ahead of the log) is a typed
   error — `CURSOR_EXPIRED` — never an empty success.
4. Zero new writers: rows are stamped inside the already-serialized committer
   turn, atomically with the document writes they describe.

**Non-goals (v1)**

- No WebSocket surface (the card's consumer is HTTP-only clients).
- No user-principal (OAuth session) support: the route serves machine tokens
  only. Per-row auth filtering of post-images (ownerField / collaboratorsField
  / `authorize` predicate) is deferred — machine tokens bypass row auth on
  their allowed tables today, so v1 needs none of it; a user-principal filter
  is future work layered on the same route.
- No delta encoding (a `patch` op carries the merged post-image, not a diff).
- No cross-db feeds.

## 3. Storage — two per-db side tables

Created lazily per db (the `mutation_log::ensure_table` pattern — safe to call
at every use site; new dbs get them from `db::create_database`):

```sql
CREATE TABLE IF NOT EXISTS "<schema>".change_head (
    ok     boolean PRIMARY KEY DEFAULT true,   -- singleton row
    seq    bigint  NOT NULL DEFAULT 0,
    log_id text    NOT NULL DEFAULT ''         -- random hex minted on first ensure
);
CREATE TABLE IF NOT EXISTS "<schema>".changes (
    seq        bigint PRIMARY KEY,
    table_name text  NOT NULL,
    doc_id     text  NOT NULL,
    kind       text  NOT NULL,              -- insert|patch|replace|delete|upsert
    doc        jsonb,                       -- end-of-txn state; NULL = no visible end state
    ts         bigint NOT NULL              -- commit wall-clock (ms)
);
CREATE INDEX IF NOT EXISTS "<schema>".changes_table_seq
    ON "<schema>".changes (table_name, seq); -- the table=-filtered poll
```

Both live in the db's own pg schema (same isolation as `mutations`, the
scheduler tables, and document tables), so database deletion cleans them up for
free and per-db naming stays inside the existing `pg_schema` discipline.
`ensure_table` (called from `db::create_database` for new dbs, at committer
startup for dbs created before this feature, and from the read handler for dbs
whose committer never ran on this replica) is idempotent and **seeds the
singleton head row** — `INSERT (ok, seq, log_id) VALUES (true, 0, <random
hex>) ON CONFLICT DO NOTHING` — without which the counter UPDATE would match
nothing on an old db.

**Why a counter row, not `nextval()`:** a Postgres SEQUENCE is
non-transactional — a rolled-back transaction burns numbers, so retained seq
values would have gaps and the expired-cursor test could not be exact. The
counter row updates transactionally:

```sql
UPDATE "<schema>".change_head SET seq = seq + $1 WHERE ok RETURNING seq;
```

and rolls back with the write, so **retained seqs are contiguous, gap-free**.
Contention is impossible by construction: only one committer per db holds the
write turn (and under `RTDB_MULTI_INSTANCE` only the lease owner commits —
shadows forward through `Committers::submit`), so this UPDATE never races. The
`changes` INSERTs reuse the returned base and assign `base+1 … base+n` in
`WriteSet.ops` order.

The head row is never trimmed and `changes` never trims its newest row, so:

- `head` (`change_head.seq`) is readable even on an empty log — `0` means
  nothing was ever committed;
- an empty `changes` table ⟺ the db has no committed ops, which keeps the
  expired-cursor test exact (section 6).

`log_id` is a random hex string minted once at head-row creation and echoed in
every response. It detects the one cursor hole seqs cannot: `drop_database` is
`DROP SCHEMA … CASCADE`, so a db **recreated under the same name** restarts at
seq 0 and a consumer holding an old cursor could read the new history
believing it is the old one once writes pass its cursor. A `logId` that
changed says "this is not the log you were reading" — the consumer resyncs.
(Echo-only in v1: the response carries it; enforcing it is a param on the
client side, not a server error.)

## 4. Write-path stamping — the single choke point

A new module `server/src/change_log.rs` exposes one append helper called with
an OPEN sqlx transaction and the settled `WriteSet`:

```rust
pub(in crate) async fn append(
    tx: &mut PgConnection,      // the write transaction (deref of a Transaction)
    pg_schema_name: &str,
    write_set: &WriteSet,
) -> Result<(), RtDbError>
```

It advances `change_head` once and inserts **one `changes` row per written
`(table, id)`** (`write_set.docs`), not per touch: a transaction that touches
the same id several times (insert+delete, patch+patch, delete+re-insert)
produces ONE row — `kind` is the LAST touch's `OpKind`, and `doc` is the id's
END-OF-TXN state from `write_set.doc_values.after` (`Some(Some(map))` →
post-image; `Some(None)` or `None` → `doc = NULL`). This is the same net
collapse `doc_values` already performs (earliest `before`, latest `after`),
and it is what an incremental consumer applies. Consequences, documented:

- `doc = NULL` means "the id has no visible end state in this txn" — a delete
  (hard or soft), a txn-local insert+delete, or a payload-less backfill op.
  The consumer re-fetches the id: a 404 (or an invisible soft-deleted row)
  resolves the op as a deletion, matching read-path visibility exactly.
- A soft delete surfaces as `kind: "delete"`, `doc: null` — from the
  consumer's viewpoint identical to a hard delete, which is true: the row is
  invisible to every read it could issue.

`ts` is one `now_ms()` per append (the commit's wall clock). The rows land in
one multi-row `INSERT` (array params, not per-row round trips) — a txn may
carry up to `MAX_AFFECTED_ROWS_PER_TXN` (10 000) writes.

The two `execute_txn` paths already hold a transaction spanning the writes —
the append lands just before their existing `tx.commit()`. **The reaper and
merge arms do not** (they run per-statement autocommits on a pooled
connection today), so those arms gain a small transaction that wraps each
unit of write work together with its change rows:

- **TTL reaper, bulk path** — one transaction per table batch:
  `begin → DELETE … RETURNING id → append(change rows for the returned ids) →
  commit`. Failure rolls back the batch atomically; the rows stay expired and
  retry next sweep (unchanged at-least-once semantics).
- **TTL reaper, cascade path** — one transaction per expired row:
  `begin → delete_row_cascade(&mut *tx) → append → commit`. On a cascade
  failure the row's transaction rolls back and the sweep **stops for this
  tick** (log + break), so `visited` only ever describes committed work —
  this is a deliberate tightening of today's continue-past-failure loop,
  which relied on autocommit making partial work durable. The failed row (and
  the rest of the batch) retries next sweep.
- **Anon→real merge** — one transaction per ROW:
  `begin → apply_update(&mut *tx) → append → commit`, preserving the arm's
  existing per-row abort contract exactly (a 23505 unique abort skips only
  that row; rows before it stay committed and publish). The change row
  commits or rolls back with its own row's rewrite — never more, never less.

No `publish_taps` change: the op-feed/audit/webhook taps stay exactly where
they are (post-commit, best-effort). The change log is deliberately NOT a
fifth tap — it must be transactional with the write, which `publish_taps`
(post-commit, failure-tolerant by design) cannot offer. Failure to append
fails the enclosing transaction (the write rolls back with its change rows) —
the only semantics that keep "exactly once" true.

| Path | Where the append happens |
| --- | --- |
| `handle_mutate`, scheduled txns, workflow steps | inside `execute_txn`, before its `tx.commit()` |
| TTL reaper | in the new per-batch / per-row transaction, section above |
| Anon→real merge | in the new per-table transaction, section above |
| Migrate backfill (`handle_migrate`) | inside its existing `pool.begin()` transaction, before its commit (line ~92, on the directive collector's ops); the `WriteSet` carries no `doc_values`, so rows land with `doc = NULL` — the consumer re-fetches the id. Migrations may under-record touched ids at fine grain; operators resync consumers (`since=0`) after a migrate as a matter of practice (documented) |

**Snapshot import / clone-db / restore** — `POST /admin/import-db`
(`snapshot::import_database`) opens its own `pool.begin()` spanning every
inserted document; the import builds a `WriteSet` of `Insert` ops carrying
post-images and calls `append` on that same transaction before its commit, so
an import into a **live** db is observable like any write (the
`change_head` row lock serializes the import against the committer's counter
UPDATE, so seq order still equals commit order). `clone-db` and backup
restore ride the same import path into fresh databases — their stamps land in
a db no consumer has cursors into yet, so they are pure bookkeeping. All
three therefore produce change rows; none is excluded.

Documents written by `push_schema` backfill are DDL, not DocOps
(`docop_taps = false` today) — they produce no change rows, matching the op
feed.

## 5. Retention

The `changes` table is bounded by row count: the newest
`change_log_max_rows` rows are retained (default **100 000**), configured via
hot config (`HotConfig::change_log_max_rows`, env-seeded from
`RTDB_CHANGE_LOG_MAX_ROWS`, PATCHable through `PATCH /admin/config` like
`idempotencyTtlMs`; values `> 0` clamp to `>= 1000`, `0` keeps the default).
The retention floor one consumer sees is therefore
`change_log_max_rows` ops — at 100k ops a consumer polling within that budget
never expires; a consumer offline longer must resync, loudly.

Trimming rides the existing per-db cleanup task (the `mutation_log::run_cleanup`
loop spawned per db by the committer — same lifecycle: self-terminates when the
db dies): each sweep runs
`DELETE FROM "<schema>".changes WHERE seq <= (head - max_rows)` in addition to
the dedup sweep, off the write path entirely. The sweep's backoff feeds on
BOTH sweeps' deleted counts (`max(dedup_deleted, trim_deleted) == 0` backs off
to the idle cadence, any deletion resets to 60s) — the pre-existing backoff
ignores the change-log sweep, so a busy db without idempotency keys would
otherwise trim only every 5 minutes.

Mirrors the new config field needs (same checklist as `idempotencyTtlMs`):
server `HotConfig` + `PersistedHotConfig` (`#[serde(default)]` Option —
backward-safe) + `PATCH /admin/config` validation in `admin/settings.rs`, the
five admin clients' config types (the python admin model is
`extra="forbid"`, so an unmirrored field BREAKS `/admin/config` there), the
dashboard config editor, `RTDB_CHANGE_LOG_MAX_ROWS` in `.env.example` AND
`docker-compose.yml`'s environment block (`env-drift-check`), and
`FEATURE_MATRIX`/README mentions.

## 6. Read path

```
GET /api/db/{db}/changes?since=<int>&table=<name>&limit=<int>
Authorization: Bearer <machine token>
```

- **Auth**: the standard `authed()` prologue (bearer → `Principal` →
  `authorize`), then machine-token-only: a `Principal::User` gets
  `Forbidden` ("change feed requires a machine token"). Read-only machine
  tokens are fine (the feed is a read). Op filtering uses the SAME semantics
  as `auth::authorize_table` — reuse that helper per returned op row, so
  `tables: None` AND `tables: Some([])` both mean unrestricted, exactly as
  the write path treats them (a `Some([])`-scoped token that got an
  empty-but-200 feed would be a silent divergence from every other
  surface). HTTP rate limiting (`check_http_rate_limits`) applies like the
  storage `metadata_handler` (the pattern to copy — the new
  `db_schema_handler` predates this spec and omits it).
- **`since`** (default `0`): return ops with `seq > since`, oldest first.
- **`table`** (optional): filter to one table. `nextSeq`/`head` stay global —
  a filtered consumer advances by the same cursor as an unfiltered one.
- **`limit`** (default 500, clamped to `<= 1000`): page size.
- **Response** `200`:

```json
{
  "ops": [
    {"seq": 41, "table": "items", "docId": "d_01…", "kind": "patch",
     "ts": 1758300000000, "doc": { "…": "post-image" }}
  ],
  "nextSeq": 41,
  "head": 57
}
```

`nextSeq` = the last returned op's `seq` **only when the page is full**
(`ops.len() == limit`); otherwise the log is exhausted up to `head` (a
`table`-filtered page that comes back short means the same thing globally) and
`nextSeq = head`. This keeps the documented consumer loop
(`while nextSeq < head`) live for filtered and tail pages alike — `nextSeq =
since` on a short page would livock a filtered consumer on an empty page.
`head` = the log's current end. Re-reading the same `since` returns the same
page (idempotent).
`doc` is `null` for `delete` ops and migrate-backfill ops.

- **Expired / impossible cursors — `CURSOR_EXPIRED` (HTTP 410)**:
  - `since + 1 < min(seq)` while ops exist beyond `since`: the cursor predates
    retention. Exact because retained seqs are contiguous (section 3).
  - `since > head`: the cursor is ahead of the log (wrong db, or a restored
    snapshot rewound it). Erroring beats an empty success the consumer would
    wait on forever.

**Exactly-once semantics.** The cursor is a position, not a consumer offset:
re-reading the same `since` returns the same prefix (idempotent retry), and
every committed op has exactly one seq, so a consumer that advances
`since ← nextSeq` after acknowledging a page sees each op once. Ops are
delivered in seq order = commit order (single serialized committer).

## 7. Wire + mirrors

- **Error code**: `ErrorCode::CursorExpired` (`CURSOR_EXPIRED`, HTTP 410) in
  `server/src/error.rs`, mirrored in all five client error types and
  `wire-corpus/error-codes.json` (the ARC-017 authoring rule — same commit).
- **Types** `ChangeOp { seq, table, docId, kind, ts, doc }` +
  `ChangeFeedResponse { ops, nextSeq, head }` (camelCase serde, `doc`/`kind`
  conventions matching `OpKind`'s lowercase wire form) in
  `server/src/protocol.rs` and every client's wire module; encode/decode cases
  in `wire-corpus.json`.
- **Client methods**: `changes(since, table, limit)` on the five HTTP clients
  (ts `listChanges`, rust `changes`, python `changes` sync+async, swift
  `changes`, go `Changes`), returning the typed response. The route is
  machine-token-scoped, so the ws clients and dashboard (cookie sessions) are
  out of scope; the dashboard gains nothing in v1.
- **No protocol version bump**: additive HTTP route + additive error code
  (the ARC-013 guard protects against newer *clients*, not newer servers; an
  old client meeting `CURSOR_EXPIRED` degrades exactly as it does for
  `UNSUPPORTED_PROTOCOL` — an opaque error envelope).

## 8. Multi-instance

Only the lease owner runs the committer, so only the owner advances
`change_head` — no distributed counter. The tables live in the shared Postgres
schema, so a shadow replica serving the route reads the same committed rows;
failover is transparent (the counter is durable, not in-memory). No new NOTIFY
path: change-feed consumers poll; they do not push.

## 9. Testing

`server/tests/change_feed_test.rs` (module of `tests/main.rs`) — integration
against dev Postgres:

1. insert/patch/upsert/replace/delete each produce one row with the right
   kind + post-image (`delete` → `doc` null);
2. seqs are contiguous and ordered across successive commits, including a
   failed (rolled-back) transaction leaving no rows and no gap;
3. cursor pagination: `since`/`limit`/`table` filter + `nextSeq`/`head` math,
   re-reading a `since` returns the same page (idempotence);
4. expired cursor (trim below `since`) and ahead-of-log cursor → typed
   `CURSOR_EXPIRED`;
5. machine-token table allowlist filters ops; a User principal is Forbidden;
6. reaper + merge writes land in the feed;
7. trim keeps the newest N and the head row (seeded on old dbs by the read
   path's `ensure_table`);
8. filtered/short-page `nextSeq = head` (the livelock case) and a full page's
   `nextSeq = last seq`;
9. same-id multi-op transactions produce ONE net row (insert+delete →
   `doc = NULL`; delete+re-insert → end-state doc); a soft delete reads as
   `kind: "delete"`, `doc: null`;
10. migrate dry-run advances nothing; a committed migrate stamps inside its
    transaction before its commit;
11. snapshot import into a live db advances `head` and its docs appear in the
    feed; clone/restore stamps land in the fresh db;
12. reaper cascade and merge rows commit/rollback with their own transaction
    under a mid-batch failure (visited-set correctness for the cascade path);
13. cleanup backoff resets when the change-log sweep deletes (dedup idle);
14. `PATCH /admin/config` clamps `changeLogMaxRows` and old persisted configs
    (missing field) deserialize.

Clients: the wire-corpus runners cover the new shapes and the new error code
(all six runners); the rust client's live `http_integration` gains a
`changes()` round trip (env-gated like its siblings).

## 10. Docs

`README.md` (HTTP surface section), `FEATURE_MATRIX.md` (new row), and
`docs/ARCHITECTURE.md` (a paragraph under the committer section: the change
log is stamped in-transaction at the `execute_txn`/reaper/merge seams — it is
explicitly not a `publish_taps` tap).

## 11. Alternatives considered

- **Postgres SEQUENCE for seqs** — rejected: non-transactional `nextval` burns
  numbers on rollback, making the expired-cursor test inexact (gaps vs trimmed
  data indistinguishable).
- **A fifth `publish_taps` tap (post-commit append)** — rejected: a crash
  between commit and append silently drops feed rows (the exact "never silent
  data loss" violation the card forbids).
- **Reusing the audit log** — rejected: optional (flag-gated), admin-shaped,
  and not seq-addressed.
- **NOTIFY-based push to shadow replicas** — unneeded: all replicas share one
  Postgres; reads need no fan-out.
