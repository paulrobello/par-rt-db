# Safe mutation retry via idempotency keys — design

## Problem

par-rt-db mutations are explicitly at-most-once: `client.ts` never auto-retries
a mutation whose connection dropped before the ack arrived — it rejects the
promise with "connection closed before the mutation was acknowledged" and
leaves the decision to the app. If the app *does* retry, there is no way to
know whether the original attempt actually committed, so a naive retry can
double-apply. FEATURE_MATRIX #4 calls for a server-side dedup table keyed by
the existing `mutId` so a caller-driven retry becomes safe, without adopting
Convex's hidden auto-retry semantics (see FEATURE_MATRIX section 4: "opt-in
safe retry without giving this up").

## Finding: `mutId` already exists but is purely a reply-correlation id

`protocol.rs`'s `ClientMessage::Mutate { mut_id, txn }` and
`ServerMessage::MutateOk/MutateErr { mut_id, .. }` already carry a
client-generated id, but it is discarded after the WS handler tags the reply
with it (`ws.rs`) — it never reaches `Committers::mutate` or `execute_txn`.
The one-shot HTTP path (`/api/mutate`, `http_api.rs`'s `MutateRequest`) has no
id concept at all today. So the wire vocabulary for an idempotency key already
exists on WS; it just isn't plumbed through or persisted, and HTTP needs a new
optional field.

## Scope

Server: `db.rs` (new table at `create_database` time), a new
`server/src/mutation_log.rs`, `committer.rs` (`CommitterRequest::Mutate` gains
`mut_id: Option<String>`, `handle_mutate` checks/stores dedup), `ws.rs` (pass
the already-parsed `mut_id` through instead of only using it for the reply),
`http_api.rs` (new optional `mut_id` field on `MutateRequest`).

Client: `client.ts`'s `mutate()` and `http.ts`'s `mutate()` gain an optional
second parameter to let a caller supply a stable id for manual retry.
`protocol.ts`'s HTTP mutate request type gains the matching optional field.

Out of scope: no change to `client.ts`'s reject-on-close behavior, no
automatic retry loop anywhere in the SDK, no change to `react.tsx`'s
`useMutation`.

## Design

### Per-database dedup table

Each tenant database already gets a `"<schema>".meta(key, value)` table at
`create_database` time (`db.rs`). This adds a sibling:

```sql
CREATE TABLE "<schema>".mutations (
    mut_id text PRIMARY KEY,
    result jsonb NOT NULL,
    expires_at bigint NOT NULL
)
```

`create_database` creates it in the same transaction as `meta`, for every new
database going forward. For databases that already exist, `run_committer`
issues one `CREATE TABLE IF NOT EXISTS` the first time that db's committer
task starts (committer tasks are lazily spawned once per db behind a lock, so
this runs exactly once per db per server lifetime — no per-request overhead).

Rejected alternative: a single global `rtdb_auth.mutations` table keyed on
`(db_name, mut_id)`. It would avoid the per-db migration step, but couples
every tenant's mutation traffic into one shared table, breaking the per-db
schema isolation every other piece of state in this codebase follows.

Rejected alternative: an in-memory-only cache (`HashMap` inside each committer
task, swept by TTL). Much less code, but a server restart during a client's
retry window loses the cache, so a retry after a restart would double-apply —
defeating the point of a feature whose entire purpose is safe retry.

### `mutation_log.rs`

```rust
pub async fn check(pool: &PgPool, db: &str, mut_id: &str) -> Result<Option<Vec<Value>>, RtDbError>
pub async fn store(pool: &PgPool, db: &str, mut_id: &str, results: &[Value], ttl_ms: i64) -> Result<(), RtDbError>
```

`check` first deletes expired rows for this db (`DELETE FROM mutations WHERE
expires_at < $now`, a single indexed statement) then looks up `mut_id`. This
piggybacks cleanup on the same round trip instead of needing a background
sweep task — there is no periodic-task infrastructure in this codebase today
(scheduled transactions, FEATURE_MATRIX #9, are a separate, later backlog
item), and a dedicated sweep task would be new machinery for a problem this
lazy delete already solves at negligible cost (the table only ever holds
entries younger than the TTL for one db).

TTL is a `const DEDUP_TTL_MS: i64 = 5 * 60 * 1000` (5 minutes) in
`mutation_log.rs` — long enough to cover typical reconnect/backoff windows,
short enough to keep the table small.

### Committer wiring

`CommitterRequest::Mutate` gains `mut_id: Option<String>`. `handle_mutate`:

```rust
async fn handle_mutate(ctx: &CommitterCtx, mut_id: Option<&str>, txn: Transaction) -> Result<TxnOutcome, RtDbError> {
    if let Some(id) = mut_id {
        if let Some(results) = mutation_log::check(&ctx.pool, &ctx.db, id).await? {
            return Ok(TxnOutcome { results, write_set: BTreeSet::new() });
        }
    }
    let schema = ctx.schemas.get(&ctx.pool, &ctx.db).await?;
    let outcome = execute_txn(&ctx.pool, &ctx.db, &schema, &txn).await?;
    ctx.subs.fan_out(&ctx.pool, &ctx.db, &schema, &outcome.write_set).await;
    if let Some(id) = mut_id {
        mutation_log::store(&ctx.pool, &ctx.db, id, &outcome.results, DEDUP_TTL_MS).await?;
    }
    Ok(outcome)
}
```

A dedup hit returns an empty `write_set` so the caller's fan-out step (already
run once, on the original attempt) is correctly skipped — nothing changed on
this replay. `None` (the caller passed no id) behaves exactly as today: no
dedup check, no storage, unchanged at-most-once semantics. Because every
mutation for a db is already serialized through that db's single committer
task, two retries of the same `mut_id` can never race each other — there is
no window where both see a miss and both execute.

`Committers::mutate` gains a `mut_id: Option<String>` parameter threaded
through to `CommitterRequest::Mutate`. `ws.rs`'s `Mutate` arm passes its
already-parsed `mut_id` (`Some(mut_id.clone())`, since it also needs the
original for tagging the reply) instead of discarding it. `http_api.rs`'s
`mutate_handler` passes `body.mut_id` (a new field, default `None`).

### Wire changes

WS: no change — `mutId` is already required on `ClientMessage::Mutate`.

HTTP: `MutateRequest` gains `#[serde(default)] mut_id: Option<String>` —
additive and non-breaking for existing callers that omit it.

### Client changes

`client.ts`'s `mutate(txn: TransactionJson, opts?: { mutId?: string })`: when
`opts.mutId` is supplied, it replaces the internally auto-generated id instead
of being layered on top — the caller's id becomes the actual wire `mutId`,
so a manual retry (catch the rejection, call `mutate(txn, { mutId: sameId
})` again) reuses the exact key the server deduplicates on. When omitted,
behavior is identical to today (an internal `mut-${n}` counter id, never
exposed). No change to `dispatchMutate`, `flushOnAuth`, `handleClose`, or
`rejectPendingMutates` — retry remains something the caller does explicitly by
calling `mutate()` again, never something the SDK does on its own.

`http.ts`'s `mutate(txn, opts?: { mutId?: string })` forwards `opts.mutId` as
the new `mutId` field on the HTTP request body when present. Correction after
checking the actual code: `protocol.ts` has no separate HTTP request type to
update — `http.ts` builds the request body as an inline object literal, so
the new field is added there directly, with no `protocol.ts` change needed.

## Testing

- New `server/tests/mutation_dedup_test.rs`:
  - Same `mut_id` submitted twice through `Committers::mutate` produces one
    row (not two) and identical `results` both times.
  - Submitting with no `mut_id` (`None`) behaves exactly as today: two calls
    with the same txn body produce two rows (no accidental dedup).
  - An expired entry re-executes: call `mutation_log::store` directly with a
    tiny `ttl_ms` (e.g. `1`), sleep a few ms, then confirm `check` returns
    `None` and a subsequent `Committers::mutate` with that `mut_id` executes
    again (a second row is created).
- `client/tests/mutation.test.ts` / `client/tests/http.test.ts`: an explicit
  `opts.mutId` becomes the wire `mutId` (WS and HTTP respectively); omitting
  it preserves today's internal-counter behavior.

## Verification

`make checkall` from the repo root (fmt-check + clippy `-D warnings` +
typecheck + tests for both `server/` and `client/`) must be fully green.

## Out of scope

- Automatic client-side retry on reconnect — this feature makes retry *safe*,
  it does not make it *automatic*.
- Scheduled/background TTL sweep task (piggybacked on `check` instead).
- Any change to `react.tsx`'s `useMutation` hook.
- Idempotency for the admin snapshot import/export routes.
