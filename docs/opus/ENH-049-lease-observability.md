# ENH-049: Observability for the multi-instance ownership lease

## Goal

Make write ownership visible at runtime in multi-instance mode, so an operator can alert on lease churn or a lost lease instead of discovering split-brain from corrupted data. ARC-001 existed undetected because ownership had no observable signal.

## Current state

- The lease is acquired in `server/src/committer/lease.rs` (`acquire_ownership_lease`). Replicas that don't own a database become shadows. The takeover happens in `takeover_submit` or the forward-timeout path (`server/src/committer/mod.rs`).
- None of this is counted.
- `/metrics` (Prometheus text, `server/src/metrics.rs:~862-900`, route `server/src/lib.rs:785`) is **aggregate-only**, with no per-db labels (`metrics.rs:~243`).
- `/admin/metrics` returns `MetricsSnapshot` (`metrics.rs:773`). That type is mirrored in all four admin clients, and the Python mirror is `extra="forbid"` (`python-client/src/par_rt_db/admin_models.py:228`). Any new field there must be mirrored in every client.
- **Prerequisite:** ARC-001 (AUDIT.md) introduces the `lease_lost` signal this plan counts.

## Design

Add Prometheus-only aggregate series, with no change to the admin JSON wire:

| Series | Type | Meaning |
|---|---|---|
| `rtdb_multi_instance` | gauge 0/1 | Whether multi-instance mode is on |
| `rtdb_leases_owned` | gauge | Number of databases whose lease this replica holds right now |
| `rtdb_lease_acquired_total` | counter | Successful `pg_try_advisory_lock` acquisitions, at first spawn or on takeover |
| `rtdb_lease_contended_total` | counter | Acquisitions that returned false, producing a shadow |
| `rtdb_lease_lost_total` | counter | Leases lost after being held (ARC-001's `after_connect` or per-turn check failed) |
| `rtdb_forward_timeouts_total` | counter | Forwards to an owner that got no answer and triggered takeover |

Also:

- Add `instance_id` as a label on the existing `rtdb_build_info` gauge, if that is compatible with its current label set. If it is not, add a new `rtdb_instance_info{instance_id="…"} 1`.
- Emit `tracing::warn!` with `db` and `instance_id` on lease lost and on takeover, and `info!` on acquisition. Per-database detail belongs in logs, not metric labels.

The admin JSON is deliberately untouched, so no client mirror is required. A later enhancement can add `GET /admin/leases` if operators want per-database ownership in the dashboard. That change would need all five clients.

## Implementation steps

1. `server/src/metrics.rs`:
   - Add `AtomicU64` counters and an `AtomicI64` gauge (`leases_owned`) to `Metrics`.
   - Add `record_lease_acquired`, `record_lease_contended`, `record_lease_lost`, `record_forward_timeout`, `lease_owned_inc` and `lease_owned_dec` methods, following `record_subs_missed_push`'s style.
   - Render them in the Prometheus renderer with `# HELP` and `# TYPE` lines.
   - **Do not** add them to `MetricsSnapshot`.
2. Call sites:
   - `lease.rs::acquire_ownership_lease` on success and on contention.
   - The ARC-001 lease-lost handler.
   - The forward-timeout takeover path in `committer/mod.rs`. Find it with `get_symbol_context` on `takeover_submit`, using `repository_id: "par-rt-db"`.
   - `lease_owned_inc` when a channel entry with `lease: Some` is inserted.
   - `lease_owned_dec` when the supervisor removes such an entry: on drain, idle reclaim, drop-db, or lease lost.
   - Audit every removal path of the `channels` map so the gauge can't drift. `grep -n "guard.remove\|channels.lock" server/src/committer/*.rs`.
3. Add `rtdb_multi_instance`, set from `config.multi_instance`.
4. Tests:
   - A metrics unit test that renders the new series.
   - In `server/tests/multi_instance_stage4_test.rs`, extend `ownership_lease_forwarding_and_failover_on_death`. Scrape `/metrics` on both replicas and assert:
     - the owner has `rtdb_leases_owned 1` and the shadow has `0`;
     - after failover, the survivor has `rtdb_lease_acquired_total` of at least 1 and `rtdb_leases_owned 1`.
5. Docs:
   - Add a "Multi-instance observability" subsection to `docs/ARCHITECTURE.md` with the table above and suggested alerts. The two alerts are `increase(rtdb_lease_lost_total[5m]) > 0` and a fleet-wide `sum(rtdb_leases_owned)` above the database count.
   - Add a README metrics-list entry if the README enumerates Prometheus series (`grep -n rtdb_mutations_total README.md`).

## Files to touch

- `server/src/metrics.rs`
- `server/src/committer/lease.rs`
- `server/src/committer/mod.rs`
- `server/src/committer/supervisor.rs`
- `server/tests/multi_instance_stage4_test.rs`
- `docs/ARCHITECTURE.md`
- `README.md` (metrics list, if present)

## Verification

- `curl -s localhost:8300/metrics | grep -E '^rtdb_(multi_instance|leases_owned|lease_acquired_total|lease_contended_total|lease_lost_total|forward_timeouts_total)'` lists all six series, run against a locally started server (see the `run-server-locally` recipe: dev-db on 55434 and `cargo run` on :8300).
- `cargo test --all-features --test main multi_instance_stage4_test` passes, with the new gauge and counter assertions.
- The `/admin/metrics` JSON is unchanged. The Python admin client parity tests pass (`uv run pytest` in `python-client/`).
- `make checkall` is green.

## Rollback

Revert the commit. The new series are additive and nothing reads them yet.
