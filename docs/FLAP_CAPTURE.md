# DB-stall flap capture procedure

How to capture (and read) one episode of the recurring server-flap class on
the prod docker host — re-arming the capture that card
`01a1dad76dd972fca7562695a3fcc4d5` ("RTDB server degradation episodes recur")
depends on. Written after the 2026-11-16 root-cause read, which reconstructed
three flaps (22:45, 00:30, 01:06 UTC) **after the fact** from two fingerprints
already sitting in existing logs. The next flap should be captured live; this
page is the checklist.

Everything below is **read-only** (log greps, curl, docker logs). No restarts,
no deploys, no SQL against prod — the episode drains by itself in 2-7 minutes
and a restart destroys the very evidence you are there to take.

## The class in one paragraph

Episodic host I/O degradation on the VPS stalls postgres. Checkpoint write
phases are chronically slow (2026-11-16 sample: 63 checkpoints, baseline
write=60-95s, spikes 269s **exactly at the 22:45 and 01:06 flap windows**);
during an episode the rtdb server's connection pool stalls silently — the
2026-11-16 windows had **zero WARN/ERROR** in server logs while every DB
consumer flapped. Episodes self-recover as the I/O stall drains.

## Signal 1 — postgres checkpoint write phases (the root-cause fingerprint)

Container logs carry one `checkpoint complete` line per checkpoint with the
write-phase duration:

```sh
# on the docker host (read-only)
docker logs <pg-container> --since 6h 2>&1 | grep "checkpoint complete" \
  | awk '{for(i=1;i<=NF;i++) if($i=="write=") print $1, $2, $(i+1)}'
```

Reading: a healthy baseline on this host is `write=60-95s` against the
checkpoint cadence; anything at or past the cadence itself (2026-11-16:
`write=269.2s`, `269.9s`, `269.4s`) is a spike, and a spike timestamp that
lands inside a consumer flap window is the correlation you are hunting. Note
the checkpoint cadence changed with the 2026-11 hardening (compose now pins
`checkpoint_timeout=15min` / `max_wal_size=4GB`) — judge spikes against the
cadence, not against "every ~5 min".

Pair with `docker logs <pg-container> --since 6h 2>&1 | grep "checkpoint starting"`
to see the trigger reason (`time` vs `wal`) per checkpoint: `wal`-triggered
checkpoints mean the size limit still binds (the tuning did not take or the
write volume outgrew it); `time`-triggered ones are the planned phases the
tuning intends.

## Signal 2 — server migration-notice bursts (the reconnect fingerprint)

The server's idempotent DDL (bootstrap/migration `... already exists,
skipping` notices surfaced by sqlx through `tracing`) does not fire on a
healthy steady state — a burst of them mid-uptime means the pool tore down
and re-established postgres connections. That makes each burst a usable
**postgres reconnect marker**, one per flap (2026-11-16: bursts at 06:54:51,
08:44:51, 09:05:39Z matching the 00:30-class flaps' recovery moments):

```sh
docker logs par-rt-db-server-1 --since 6h 2>&1 \
  | grep -iE "already exists|sqlx::postgres::notice" \
  | awk '{print $1, $2}' | sort | uniq -c
```

A clump of timestamps = reconnect burst = flap moment (times are UTC in the
logs).

## Signal 3 — the live pool signals (landed 2026-11-18)

Since the pool-watch increment, the silence during an episode is closed:

- **Server logs**: `pool watchdog: slow probe` / `pool watchdog: probe FAILED`
  WARNs (target `rtdb::pool`) with `wait_ms` + pool size/idle/open, one per
  probe second during a stall, plus `pool watchdog: stall flag set/cleared`
  on the flag transitions.
- **`/healthz`** (unauthenticated): `pool_probe_wait_us` (last probe,
  microseconds) and `degraded_reason: "pool_stalled"` — distinct from the
  committer watchdog's `"committer_queue_sustained_wait"`, so a DB-stall can
  be told apart from a committer wedge or transport failure. `status` still
  tracks only postgres reachability.

  ```sh
  curl -fsS http://127.0.0.1:8300/healthz | jq .
  ```

- **Scrape**: `rtdb_pool_probe_wait_us`, `rtdb_pool_probe_slow_total`,
  `rtdb_pool_probe_failures_total`, `rtdb_pool_stalled`,
  `rtdb_pool_stalled_transitions_total` (alongside the committer gauges).

## Reading the combination

| Server notice burst | Checkpoint spike | Class |
| --- | --- | --- |
| yes | yes | I/O-degradation stall, checkpoint-correlated (the 22:45 / 01:06 class) |
| yes | no | plain I/O stall (the 00:30 class) |
| no | no | not this class — look client-side (the 2026-11-14 finding: macdaddy load bursts wedging local clients) |

`degraded_reason: "pool_stalled"` + rising `rtdb_pool_probe_failures_total`
during the same window is the in-process confirmation that the pool (not the
committer, not cloudflared — cloudflared was exonerated 2026-11-16: silent
journal, zero restarts) is the stalling layer.

## Capture checklist (do this DURING the next flap)

1. Timestamp the start: `date -u` on the host.
2. `curl -fsS http://127.0.0.1:8300/healthz | jq .` — save `pool_probe_wait_us`,
   `degraded_reasons`, `committer_queue_*` every ~30s through the episode.
3. Run both greps above with `--since` covering the window; save full output.
4. Grab `docker logs par-rt-db-server-1 --since <window> 2>&1 | grep -E "WARN|ERROR|pool watchdog"`
   — with the pool-watch increment this should finally show the stall live;
   if it is STILL silent, that is a finding (the monitor did not fire) and
   belongs on the card.
5. Append everything to card `01a1dad76dd972fca7562695a3fcc4d5` notes with
   UTC timestamps. Host load at capture (`uptime`) is worth one line.

If checkpoint spikes persist **after** the compose tuning lands (spikes still
at/above the 15-min cadence), the IOPS ceiling binds before the checkpoint
pattern does — that is a host-plan decision (bigger disk tier / pin the
volume elsewhere), not more tuning knobs; record it and stop there.
