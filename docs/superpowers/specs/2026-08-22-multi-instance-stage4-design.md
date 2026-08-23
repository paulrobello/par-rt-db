# Multi-instance Stage 4 — cross-process rate limits + committer ownership

Status: approved 2026-08-22 (Paul Robello — A1 + B1) and implemented same
day; see "As built" notes at the end for the one simplification landed during
implementation. Kanban card 01a0225052467e90ae4e097a1cc4203e.
Completes ENH-022: lifts the last two blockers that keep `RTDB_MULTI_INSTANCE`
documented as reader-fan-out only.

## Problem

With `RTDB_MULTI_INSTANCE=true` (Stages 1–3: oauth state, op-feed, presence
coordinate via Postgres LISTEN/NOTIFY), two defects remain:

1. **Rate-limit counters are per-process** (`rate_limit.rs` — an in-memory
   fixed-window map). A fleet of N replicas each gets the full per-token /
   per-db RPM budget, so a configured ceiling of 600 RPM is really 600×N.
2. **Every replica lazily spawns its own committer for any database a client
   touches** (`Committers::channel_for`). Two replicas serving the same db
   each run a committer turn for it — the single-writer-per-database
   invariant that the whole design leans on (`docs/ARCHITECTURE.md`: reads
   run READ COMMITTED with no row locking; serialization is what makes
   correctness hold) holds only per-process, not across the fleet.

## Part A — committer ownership

### Options evaluated

**A1. Postgres session advisory lock as the ownership lease + per-transaction
advisory fence (recommended).**
The owner process holds `pg_advisory_lock(hash(db))` on one dedicated
long-lived connection. Ownership duties:

- *Acquisition*: `channel_for` first tries `pg_try_advisory_lock` on the
  ownership connection; a miss means another replica owns the db.
- *Fencing*: every committer transaction opens with
  `pg_try_advisory_xact_lock(hash(db))`. Session and xact advisory locks
  share one lock space, so this succeeds only while THIS process's session
  lock is still the held one. A zombie former owner (paused process,
  network-partitioned but alive, TCP not yet dead) fails the fence on its
  next txn, aborts cleanly, evicts its channel, and non-owner replicas are
  never raced by it. The fence rides the existing txn — no extra round trip.
- *Failover on kill -9*: the OS drops the owner's TCP session; Postgres
  releases the session advisory lock on backend death. A non-owner's next
  `pg_try_advisory_lock` acquires. Detection latency = TCP keepalive
  (`tcp_keepalives_idle/interface` on the ownership connection, tuned to
  ~10–15s — the lease TTL equivalent, enforced by the kernel, not by app
  clocks).
- *Non-owners*: a write/subscribe arriving at a non-owner for a db it does
  not own is forwarded to the owner over the Stage-3 notify channel
  (request carries a reply correlation id; the owner's committer executes
  and notifies the reply). Reads stay local (READ COMMITTED is safe
  fan-out). If no owner responds within a short deadline, the non-owner
  attempts acquisition itself (owner died between checks) — the acquire
  path is the failover path.

Pros: kill -9 failover for free (no app-level TTL, no clock skew); fencing
per-txn with zero added statements; no new tables. Cons: one dedicated
connection per owned db; forwarding adds one Postgres round trip per
cross-replica write.

**A2. Lease table + fencing tokens.**
`committer_leases(db, owner_instance, token, expires_at)`; renew every
TTL/3; every write checks `token >= lease.token`. Pros: no long-lived
connections; inspectable table. Cons: failover latency = TTL (kill -9 is
detected only at expiry); clock skew between replicas and Postgres;
application-level renewal loop per db; fencing requires threading the token
through every txn anyway. Strictly more machinery for a slower failover.

**A3. Static db→replica assignment (config).** No runtime protocol at all,
but defeats the point of an elastic fleet behind one tunnel and moves the
problem into deployment config.

### Recommendation
A1. The load-bearing property the card names — "the lease handoff must
preserve the single-writer invariant under kill -9" — is exactly what
session-death lock release + per-txn xact fencing gives, with no app clocks
in the correctness path.

## Part B — cross-process rate limits

### Options evaluated

**B1. Counters in Postgres, one UPSERT per check (recommended).**
`rate_counters(key, minute_bucket, count)`; a check is
`INSERT … ON CONFLICT (key, minute_bucket) DO UPDATE SET count = count + 1
RETURNING count`. Denied when the returned count exceeds the limit (the
increment still lands — the next-minute bucket resets naturally, matching
today's fixed-window semantics including the boundary-burst tolerance).
Costs one round trip per checked request, **only when a limit is
configured** — the default `0 = unlimited` path (today's default) stays
pure in-memory-free: no limit configured → no query at all. With a
`MAX_BUCKETS`-bounded cleanup sweep on the same lazy schedule the limiter
already has. Strict and exact: N replicas share one budget.

**B2. Local counters + LISTEN/NOTIFY delta sync + periodic reconcile.**
Cheaper per request, but eventual-consistent: two replicas can each admit
the last concurrent slot. Acceptable for flood control, not for a
contractual ceiling; also adds a notify per check (chattier than B1's one
row).

**B3. Hybrid (local pre-count, async DB flush).** Same weakness as B2 with
more parts.

### Recommendation
B1. Limits are opt-in; when an operator sets one they are buying a real
ceiling. One indexed UPSERT per checked request is well within budget at
the RPM scales a self-hosted instance runs, and it is the only option that
makes the ceiling true across processes.

## Gating and rollout

Both parts activate only under `RTDB_MULTI_INSTANCE=true`. A single-instance
deploy is unchanged: its ownership acquire is uncontended, its fences always
pass, and with no limits configured (default) the rate path never queries.

## Tests (the card's two-process criteria)

Two full `AppState`s (distinct `instance_id`s) against one Postgres,
`multi_instance=true`:

- **T1 single-writer + kill-failover**: both processes submit writes to one
  db; assert exactly one process's committer executes them (ownership) and
  both processes' writes COMMIT (forwarding). Then sever the owner's
  ownership connection; assert the survivor acquires within the keepalive
  window and subsequent writes still land exactly once.
- **T2 shared rate budget**: per-db limit N; drive N/2 checked requests
  through each process; assert request N+1 is DENIED regardless of which
  process handled it, and `retry_after_secs` is in range.

## As built (2026-08-22)

Part A landed with one simplification over the text above: **no per-txn
advisory fence, and no forwarding — yet.**

- *Fence superseded.* While implementing, the per-transaction
  `pg_try_advisory_xact_lock` fence proved both unnecessary and subtly wrong:
  session and xact advisory locks share one lock space, so an owner's OWN
  write transactions (on pool connections) would conflict with its OWN
  session lock (on the ownership connection) and self-deadlock. The correct
  and simpler mechanism: the ownership lease is a dedicated ONE-connection
  pool, and the owner's committer and its scheduler/reaper/dedup pollers all
  run ON that pool — the lease and every write share one backend, so another
  replica can never be mid-write (it cannot acquire the lease while the
  owner lives, and a dead owner's backend releases the lock on session
  death). Split-brain is impossible by construction; no fencing tokens.
- *Shadow committers.* A non-owner replica serves a SHADOW committer for the
  db: read arms (subscribe initial runs) work; every write arm replies
  CONFLICT naming the lease. A write submit on a shadow retires it and takes
  the lease if it can — that submit path IS the failover path.
- *Forwarding deferred (Stage 4c).* Non-owner writes reject with CONFLICT
  rather than forwarding over NOTIFY. The reply-shape protocol sketched
  above is unchanged and is filed as the follow-up stage; until it lands,
  writes must reach the owning replica (the takeover path keeps ownership
  following demand, so a load-balanced tunnel converges on one owner).
- *Residual, documented:* if Postgres discards the lease connection (protocol
  error) sqlx replaces it and the lock is silently lost; ownership then
  follows the next taker. Acceptable for Stage 4; revisit with a lease
  watchdog if a fleet ever runs hot enough to notice.

Tests: `server/tests/multi_instance_stage4_test.rs` — T1 single-writer +
failover-on-leaseholder-death (exactly-once asserted), T2 shared rate budget.
