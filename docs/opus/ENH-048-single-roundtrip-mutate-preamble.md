# ENH-048: One round trip for the mutate preamble

## Goal

Cut the fixed round trips that a mutate spends in its serialized committer turn before any step runs. The committer runs one turn at a time per database, so every round trip in the preamble is taken out of the database's write throughput.

## Current state

`handle_mutate` (`server/src/committer/arms/mutate.rs`) plus `execute_txn` (`server/src/txn.rs:1343`) issue these statements in order before the first step:

1. `mutation_log::check` (`mutation_log.rs:47`), only when an idempotency key is present. It runs on the pool, outside the transaction.
2. `db::is_read_only` (`db.rs:807`), which reads `rtdb_auth.databases.read_only`.
3. `schemas.get`. This is a cache hit on the hot path, so it costs no round trip.
4. `quotas.enforce`. This is a cheap stale read with no round trip when the cache is warm.
5. `pool.begin()`, which sends `BEGIN`.
6. `SET LOCAL statement_timeout = …` (`txn.rs:~1405`).

A keyed mutate therefore pays 4 round trips and an unkeyed one pays 3, each with its own pool acquire for the calls made outside the transaction.

**Prerequisite:** ARC-003 (AUDIT.md) moves the idempotency *store* into the write transaction. This plan moves the *check* there as well, so land ARC-003 first.

## Design

Run one statement inside the transaction, right after `BEGIN`:

```sql
SELECT set_config('statement_timeout', $1, true),
       COALESCE((SELECT read_only FROM rtdb_auth.databases WHERE name = $2), FALSE),
       (SELECT result FROM "{schema}".mutations WHERE mut_id = $3 AND expires_at > $4)
```

- `set_config(..., true)` has the same transaction scope as `SET LOCAL`.
- `$3` is NULL when there is no key, and the subselect then returns NULL.
- The results are applied in the current order:
  - A present cached result is an **idempotent replay**. Roll back and return the cached results, before the freeze check, exactly as today.
  - Otherwise, `read_only = true` returns a `READ_ONLY` error after rollback.
  - Otherwise, continue.

The keyed path goes from 4 round trips to 2 (`BEGIN` plus the combined statement), and the unkeyed path from 3 to 2. The freeze semantics stay "fresh per write", because the check is still a fresh read inside the turn.

## Implementation steps

1. **Add a preamble mode to `execute_txn`.** Add `preamble: MutatePreamble { idem_key: Option<&str>, check_freeze: bool }` to its options struct (introduced by ARC-003). The scheduled and workflow callers pass `check_freeze: false`, because system arms are exempt from the freeze today (see the comment in `mutate.rs`), and `idem_key: None`.
2. **Replace the `SET LOCAL` block** at `txn.rs:~1400-1409` with the combined query. Bind `STATEMENT_TIMEOUT_MS.to_string()` as `$1`: `set_config` takes text, and the value is still a constant, never user input. The schema identifier comes from `pg_schema(db)`, which is already validated. When `idem_key` is `None` and `check_freeze` is false (the system arms), keep a plain `set_config` so the query is not wasted.
3. **Return early.** Add a `TxnOutcome` variant or flag for replay, or reuse whatever ARC-003 added for a duplicate key, and add a `READ_ONLY` error path. Both must drop the transaction without committing.
4. **Simplify `handle_mutate`.** Delete the pre-transaction `mutation_log::check` and the `db::is_read_only` call. `mutation_log::check` and `db::is_read_only` stay public for their other callers (`grep -rn "is_read_only(" server/src`: the schedule and workflow enqueue helpers still use it).
5. **Keep the storage-quota enforce where it is.** It runs before any writes, has no round trip when warm, and changing it is out of scope.
6. **Tests.**
   - The existing read-only freeze tests (`grep -ln read_only server/tests`) must pass unchanged, and so must the idempotency replay tests.
   - Add one test: freeze a database, then replay an **already-committed** idempotency key. The cached result must be returned, not `READ_ONLY`. That preserves the documented order.
7. **Measure.** Run `make bench` (see `scripts/bench/`, with the baseline in `bench/baseline.json`) before and after on the dev machine. Record p50 and p99 mutate latency in the commit message.

## Files to touch

- `server/src/txn.rs`
- `server/src/committer/arms/{mutate,scheduled,workflow}.rs`
- `server/src/mutation_log.rs`, doc comment only
- `docs/ARCHITECTURE.md`, to describe the mutate turn

## Verification

- `cargo test --all-features --test main read_only`: the freeze semantics are unchanged.
- `cargo test --all-features --test main idempotency`: replay is unchanged, including the new frozen-replay ordering test.
- `cargo test --all-features --test main multi_instance_stage4_test::forwarded_mutate_is_deduped_by_a_server_minted_key`
- The mutate p50 in `make bench` is no worse than baseline, and the commit message records the before and after numbers.
- `make checkall` is green.

## Rollback

Revert the commit. There is no schema, wire, or configuration change.
