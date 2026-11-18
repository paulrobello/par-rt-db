//! Pool-stall watchdog (prod degradation hardening — the postgres-side class).
//!
//! The 2026-11-16 episode capture (card 01a1dad76dd9) root-caused the
//! recurring flap class: episodic host I/O degradation on the VPS stalls
//! postgres (checkpoint write phases chronically 60-95s with 269s spikes
//! EXACTLY at the 22:45 and 01:06 flap windows), and the server's connection
//! pool then stalls SILENTLY — the episode windows showed ZERO WARN/ERROR
//! lines while every request hung. This monitor closes that silence: it
//! probes the pool on a fixed cadence with a bounded acquire+`SELECT 1`
//! (the same probe `/healthz` makes on demand, but continuous), WARN-logs
//! every slow or failed probe in the sqlx slow-statement style, feeds the
//! last probe latency to the scrape (`rtdb_pool_probe_wait_us`) and
//! `/healthz` (`pool_probe_wait_us`), and flips a `pool_stalled` flag that
//! `/healthz` reports as `degraded_reason: "pool_stalled"` — distinct from
//! the committer watchdog's `committer_queue_sustained_wait`, so consumers
//! can attribute a flap to DB-stall instead of transport failure.
//!
//! Detection only, exactly like `watchdog.rs`: nothing sheds, sheds
//! connections, or restarts. The probe holds at most one pool slot for at
//! most PROBE_TIMEOUT, so the monitor itself cannot exhaust the pool.
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::PgPool;
use tracing::warn;

use crate::metrics::Metrics;

/// Probe cadence. 1s makes the monitor dense enough to catch the episode
/// class (stall windows ran 2-7 minutes) with per-second WARN fingerprints
/// in the container logs, at the cost of one trivial `SELECT 1`/s against
/// postgres — noise relative to the multi-tenant write load.
const PROBE_INTERVAL: Duration = Duration::from_secs(1);
/// Bound the whole probe (pool acquire + `SELECT 1`). Past this the probe is
/// a failure and the loop moves on; the monitor never queues behind the very
/// stall it is reporting. Must stay well under the pool's 10s acquire timeout
/// (`main.rs`) so a hung probe frees its slot long before real waiters do.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// WARN threshold, mirroring sqlx's own slow-statement WARN level (1s): the
/// 2026-11-15 capture saw exactly two 1.0-1.3s outliers in ~3h of healthy
/// traffic, so anything past 1s is episode-grade, not noise.
const SLOW_PROBE_WARN_US: u64 = 1_000_000;
/// Stall-scale threshold: matches `/healthz`'s own `POSTGRES_PROBE_TIMEOUT`
/// (2s) — past this, a healthz probe would already flip `status: degraded`,
/// so the pool flag flips in sync with what probers see. The 2026-11-16
/// evidence put the stall class at tens of seconds to minutes; 2s is far
/// below that and far above the sub-10ms healthy baseline.
const STALL_PROBE_WAIT_US: u64 = 2_000_000;
/// Consecutive bad probes required before the flag flips (3 at the 1s cadence
/// = 3s sustained): a single slow probe cannot trip it, mirroring the
/// committer watchdog's anti-burst reasoning at its own cadence.
const STALL_CONSECUTIVE_PROBES: u64 = 3;

/// How one probe round is classified (see the threshold consts above).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProbeOutcome {
    /// Completed within SLOW_PROBE_WARN_US.
    Ok,
    /// Completed between SLOW_PROBE_WARN_US and STALL_PROBE_WAIT_US:
    /// WARN-grade only, does not count toward the stall flag.
    Slow,
    /// Completed past STALL_PROBE_WAIT_US — WARN- and stall-grade: this
    /// probe counts toward the stall flag (a 1-2s probe warns but does not).
    Stall,
    /// Timed out past PROBE_TIMEOUT or errored (WARN, counted).
    Failed,
}

/// Pure classification so the threshold math is unit-testable without a
/// runtime. `ok` is the full probe result (acquire + `SELECT 1` returning 1).
pub fn classify(ok: bool, wait_us: u64) -> ProbeOutcome {
    if !ok {
        ProbeOutcome::Failed
    } else if wait_us >= STALL_PROBE_WAIT_US {
        ProbeOutcome::Stall
    } else if wait_us >= SLOW_PROBE_WARN_US {
        ProbeOutcome::Slow
    } else {
        ProbeOutcome::Ok
    }
}

/// Pure transition rule, mirroring `watchdog.rs`'s: returns the updated
/// consecutive-bad count and whether the stall flag should be set this tick.
pub fn next_state(consecutive_bad: u64, bad: bool) -> (u64, bool) {
    let consecutive_bad = if bad { consecutive_bad + 1 } else { 0 };
    (consecutive_bad, consecutive_bad >= STALL_CONSECUTIVE_PROBES)
}

/// Runs until the background-task token cancels it (see `AppState::new`).
pub async fn run(pool: PgPool, metrics: Arc<Metrics>) {
    let mut interval = tokio::time::interval(PROBE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut consecutive_bad: u64 = 0;
    let mut stalled = false;
    loop {
        interval.tick().await;
        let started = Instant::now();
        let ok = matches!(
            tokio::time::timeout(
                PROBE_TIMEOUT,
                sqlx::query_scalar::<_, i32>("SELECT 1").fetch_one(&pool)
            )
            .await,
            Ok(Ok(1))
        );
        let wait_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        let outcome = classify(ok, wait_us);
        metrics.record_pool_probe(
            wait_us,
            outcome == ProbeOutcome::Slow,
            outcome == ProbeOutcome::Failed,
        );
        match outcome {
            ProbeOutcome::Ok => {}
            ProbeOutcome::Slow => warn!(
                target: "rtdb::pool",
                wait_ms = wait_us / 1000,
                pool_size = pool.size(),
                pool_idle = pool.num_idle(),
                "pool watchdog: slow probe (acquire + SELECT 1 past 1s) — postgres I/O degrading"
            ),
            ProbeOutcome::Stall => warn!(
                target: "rtdb::pool",
                wait_ms = wait_us / 1000,
                pool_size = pool.size(),
                pool_idle = pool.num_idle(),
                "pool watchdog: stall-grade probe (acquire + SELECT 1 past 2s) — pool stalling behind postgres I/O"
            ),
            ProbeOutcome::Failed => warn!(
                target: "rtdb::pool",
                wait_ms = wait_us / 1000,
                pool_size = pool.size(),
                pool_idle = pool.num_idle(),
                "pool watchdog: probe FAILED (timeout or error) — pool stalled or postgres unreachable"
            ),
        }
        // Only stall-grade (>= 2s) or failed probes count toward the flag; a
        // run of 1-2s slow probes warns per probe but stays below the flag.
        let bad = matches!(outcome, ProbeOutcome::Stall | ProbeOutcome::Failed);
        let (next_bad, should_stall) = next_state(consecutive_bad, bad);
        consecutive_bad = next_bad;
        if should_stall != stalled {
            stalled = should_stall;
            metrics.set_pool_stalled(stalled);
            metrics.record_pool_stalled_transition();
            warn!(
                target: "rtdb::pool",
                wait_us,
                consecutive_bad,
                "pool watchdog: stall flag {}",
                if stalled { "set" } else { "cleared" }
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_matches_thresholds() {
        assert_eq!(classify(true, 0), ProbeOutcome::Ok);
        assert_eq!(classify(true, SLOW_PROBE_WARN_US - 1), ProbeOutcome::Ok);
        // 1s-2s: WARN-grade but not stall-grade — warns per probe, never
        // counts toward the flag.
        assert_eq!(classify(true, SLOW_PROBE_WARN_US), ProbeOutcome::Slow);
        assert_eq!(classify(true, STALL_PROBE_WAIT_US - 1), ProbeOutcome::Slow);
        // At/above 2s (the /healthz probe timeout): stall-grade.
        assert_eq!(classify(true, STALL_PROBE_WAIT_US), ProbeOutcome::Stall);
        // Failures are stall-grade regardless of how fast they failed — an
        // instantly refused connection is still an outage signal.
        assert_eq!(classify(false, 0), ProbeOutcome::Failed);
        assert_eq!(classify(false, u64::MAX), ProbeOutcome::Failed);
    }

    #[test]
    fn slow_but_not_stall_probes_warn_without_flipping_the_flag() {
        // A streak of 1-2s probes is WARN noise, not a stall: `bad` must stay
        // false so the transition rule never sees it.
        let mut consecutive = 0;
        for _ in 0..(STALL_CONSECUTIVE_PROBES * 10) {
            let outcome = classify(true, STALL_PROBE_WAIT_US - 1);
            assert_eq!(outcome, ProbeOutcome::Slow);
            let bad = matches!(outcome, ProbeOutcome::Stall | ProbeOutcome::Failed);
            assert!(!bad);
            let (c, s) = next_state(consecutive, bad);
            consecutive = c;
            assert!(!s);
        }
    }

    #[test]
    fn sustained_bad_probes_flip_stalled_only_after_consecutive() {
        let mut consecutive = 0;
        for _ in 0..(STALL_CONSECUTIVE_PROBES - 1) {
            let (c, s) = next_state(consecutive, true);
            consecutive = c;
            assert!(!s);
        }
        let (c, s) = next_state(consecutive, true);
        assert!(s);
        assert_eq!(c, STALL_CONSECUTIVE_PROBES);
    }

    #[test]
    fn single_good_probe_resets_and_clears() {
        // One good probe resets the streak…
        let (c, s) = next_state(STALL_CONSECUTIVE_PROBES - 1, false);
        assert_eq!(c, 0);
        assert!(!s);
        // …and one good probe clears a set flag (same rule as the committer
        // watchdog: recovery needs no hysteresis, the WARN transition is
        // logged either way).
        let (_, s) = next_state(STALL_CONSECUTIVE_PROBES, false);
        assert!(!s);
    }
}
