//! Committer write-path watchdog (prod degradation hardening).
//!
//! Samples the committer queue gauges on a fixed interval and flips a
//! degradation flag when queue-wait p99 stays at wedge scale for a sustained
//! window. Detection only: `/healthz` reports the flag as an additive
//! `degraded_reason` field while staying HTTP 200, so probers and the
//! container healthcheck stay green. Shedding / hard restart stays a separate
//! decision pending a confirmed wedge capture on prod — the 2026-11-14
//! episode windows left no server-side stall trace.
use std::sync::Arc;
use std::time::Duration;

use tracing::warn;

use crate::metrics::Metrics;

/// Sample cadence. Six consecutive high samples = 60s sustained before the
/// flag flips, so a burst of slow turns cannot trip it.
const SAMPLE_INTERVAL: Duration = Duration::from_secs(10);
/// Wedge-scale threshold: healthy prod p99 sits between ~32us and ~33ms; a
/// committer wedged in a hung turn drives waiter latency toward the 10s pool
/// acquire timeout. 5s is far above burst territory and far below that
/// ceiling.
const DEGRADE_QUEUE_WAIT_P99_US: u64 = 5_000_000;
/// Consecutive high samples required (six at the 10s cadence).
const DEGRADE_CONSECUTIVE_SAMPLES: u64 = 6;

/// Pure transition rule so the threshold math is unit-testable without a
/// runtime: returns the updated consecutive count and whether the flag
/// should be set this tick.
fn next_state(consecutive: u64, sample_high: bool) -> (u64, bool) {
    let consecutive = if sample_high { consecutive + 1 } else { 0 };
    (consecutive, consecutive >= DEGRADE_CONSECUTIVE_SAMPLES)
}

/// Runs until the background-task token cancels it (see `AppState::new`).
pub async fn run(metrics: Arc<Metrics>) {
    let mut interval = tokio::time::interval(SAMPLE_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut consecutive: u64 = 0;
    let mut degraded = false;
    loop {
        interval.tick().await;
        let sample = metrics.committer_watch_sample();
        let high = sample.queue_wait_p99_us >= DEGRADE_QUEUE_WAIT_P99_US;
        let (next_consecutive, should_degrade) = next_state(consecutive, high);
        consecutive = next_consecutive;
        if should_degrade != degraded {
            degraded = should_degrade;
            metrics.set_committer_degraded(degraded);
            metrics.record_committer_degraded_transition();
            warn!(
                queue_wait_p99_us = sample.queue_wait_p99_us,
                queue_depth = sample.queue_depth,
                "committer watchdog: degradation flag {}",
                if degraded { "set" } else { "cleared" }
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sustained_high_trips_only_after_consecutive_samples() {
        let mut consecutive = 0;
        for _ in 0..(DEGRADE_CONSECUTIVE_SAMPLES - 1) {
            let (c, d) = next_state(consecutive, true);
            consecutive = c;
            assert!(!d);
        }
        let (c, d) = next_state(consecutive, true);
        assert!(d);
        assert_eq!(c, DEGRADE_CONSECUTIVE_SAMPLES);
    }

    #[test]
    fn single_burst_resets() {
        let (c, d) = next_state(DEGRADE_CONSECUTIVE_SAMPLES - 1, false);
        assert_eq!(c, 0);
        assert!(!d);
    }
}
