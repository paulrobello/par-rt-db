# ENH-029 — Reusable two-replica test harness with failure injection

## Goal

Make multi-instance behavior as cheap to test as single-instance behavior. Today the only
multi-replica coverage is `server/tests/multi_instance_stage4_test.rs` (plus `presence_xreplica_test.rs`),
which builds two `AppState`s with a local `replica(...)` helper and drives them by hand. The audit's
two Critical findings (ARC-001 stale subscriptions on non-owners, ARC-002 the `pg_notify` payload
cap) were both invisible because no test could say "subscribe on B, write on A". A shared harness
with a subscription client and failure injection (kill owner, drop replies, delay listener) turns
that into a five-line test and gives ENH-022's follow-ups a place to land.

## Current state

- `server/tests/multi_instance_stage4_test.rs:21` `replica(...)` builds one `AppState` with
  `multi_instance: true` and a distinct `instance_id`; `shared_pool()` at `:36`;
  `mutate_until_landed` at `:59` polls for a forwarded write.
- `server/tests/common/mod.rs` has 15 `test_state_*` constructors, all `multi_instance: false`
  (`:89`), the `TestDb` RAII type, and WS helpers used by `subs_test.rs`/`ws_test.rs`.
- Forward/notify listeners are spawned inside `AppState::new` and cannot be paused from a test
  (ARC-009 proposes a `CancellationToken`; this harness benefits from it but does not require it).

## Design

`server/tests/common/cluster.rs` (module of `common`):

```rust
pub struct Cluster { pub a: Option<Replica>, pub b: Option<Replica>, pub db: TestDb }
pub struct Replica { pub state: AppState, pub instance_id: String, pub app: axum::Router, pub addr: SocketAddr }
impl Cluster {
    pub async fn two(schema: SchemaDef) -> Cluster;          // two replicas, one db, schema pushed, lease owned by A
    pub fn replica(&self, id: ReplicaId) -> &Replica;        // panics on a killed replica; addr() stays callable via SocketAddr stored outside the slot
    pub async fn owner(&self) -> ReplicaId;                   // whichever holds the lease now
    pub async fn ws(&self, id: ReplicaId) -> WsClient;        // authed /sync client (reuses subs_test helpers)
    pub async fn mutate_http(&self, id: ReplicaId, txn: Transaction) -> TxnOutcome;
    pub async fn kill(&mut self, id: ReplicaId);              // take() the slot, await axum shutdown, then drop Router
                                                             // AND every AppState clone so the lease-holding backend
                                                             // closes; ground this in however stage4's takeover test
                                                             // simulates owner death today and generalize THAT
    pub async fn wait_takeover(&self, id: ReplicaId);         // poll until `id` holds the lease (bounded)
}
pub struct Chaos<'a> { c: &'a mut Cluster }
impl Chaos<'_> {
    pub async fn drop_replies(&self, r: &Replica, on: bool); // test-only flag on Forwarder: swallow rtdb_write_replies
    pub async fn delay_listener(&self, r: &Replica, d: Duration);
}

`drop_replies`/`delay_listener` need a small `#[cfg(any(test, feature = "test-support"))]` hook in
`server/src/forward.rs` (an `Arc<AtomicBool>` / `Arc<Mutex<Duration>>` consulted in the listener
loop). Gate it behind a `test-support` cargo feature so production binaries carry no hook.

Each replica binds a real listener (`axum::serve` on `127.0.0.1:0`) so tests can drive HTTP and WS
exactly as a load balancer would, instead of calling handlers directly.

## Implementation

1. Add the `test-support` feature to `server/Cargo.toml` and the two chaos hooks in `forward.rs`.
2. Write `common/cluster.rs`; move `replica`, `shared_pool`, `insert_item`, `mutate_until_landed`
   from `multi_instance_stage4_test.rs` into it (keep the test file's own assertions).
3. Port `multi_instance_stage4_test.rs` and `presence_xreplica_test.rs` onto the harness (no new
   assertions, prove parity).
4. Add `server/tests/multi_instance_subs_test.rs`:
   - subscribe on B, mutate on A via HTTP, assert B's `queryUpdate` (ARC-001's regression test);
   - subscribe on B, schedule a job on A that writes, assert B pushes;
   - large forwarded mutate (20 KB doc) and large reply (ARC-002's regression test);
   - `drop_replies(A)` then mutate on B: assert exactly one row after takeover (ARC-003's
     server-minted idempotency key);
   - `kill(owner)` mid-stream: B's subscription keeps receiving after takeover.
5. `docs/ARCHITECTURE.md` multi-instance section (DOC-004) gets a "Testing" subsection pointing at
   the harness; `CONTRIBUTING.md` mentions `Cluster::two` as the way to write a multi-instance test.

Sequence after ARC-001/002/003 land (the harness's first tests are their regression tests) or land
the harness first with those tests marked `#[ignore]` until the fixes ship — either order works;
do not let the ignored tests linger past the fix.

## Files to touch

- `server/Cargo.toml` (feature), `server/src/forward.rs` (gated hooks), `server/src/lib.rs` (feature-gated re-export if needed)
- `server/tests/common/mod.rs`, `server/tests/common/cluster.rs` (new)
- `server/tests/multi_instance_stage4_test.rs`, `server/tests/presence_xreplica_test.rs`, `server/tests/multi_instance_subs_test.rs` (new)
- `Dockerfile` stub list if server declares `[[test]]` entries (it does not today; check)
- `docs/ARCHITECTURE.md`, `CONTRIBUTING.md`

## Verify

- `cargo test --manifest-path server/Cargo.toml --features test-support --test main multi_instance_stage4_test` and the equivalent `presence_xreplica_test` and `multi_instance_subs_test` module filters green.
- `cargo build --manifest-path server/Cargo.toml --release` and `strings target/release/rtdb-server | grep -c drop_replies` is 0 (hooks absent without the feature).
- Each of the five scenarios in step 4 exists as a named test and passes.
- `make checkall` green three times in a row (no new flake).

## Rollback

Delete `cluster.rs` and the feature; the ported tests fall back to their previous inline helpers
(keep the old helpers in git history; restoring is a revert).
