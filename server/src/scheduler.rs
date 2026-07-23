//! Per-database scheduled/cron transaction store + timer. Jobs are *data*
//! (a declarative `Transaction` plus a `due_at`), not code — the scheduler
//! drains due rows through the single-writer committer, which executes them
//! via the normal `execute_txn` path. See
//! `docs/superpowers/specs/2026-07-23-scheduled-cron-transactions-design.md`.

use crate::error::RtDbError;

/// Computes the next fire time (UTC epoch ms) for a 5-field cron expression,
/// strictly after `now_ms`. Also validates the expression: a parse failure or
/// an expression with no future fire times is `BadRequest`.
pub fn next_fire(expr: &str, now_ms: i64) -> Result<i64, RtDbError> {
    use chrono::{DateTime, Utc};
    // `Cron::new` is infallible; `parse()` does the actual validation and
    // rejects malformed expressions. `croner` reads a 5-field expression
    // min-first (seconds default to 0), so `*/5 * * * *` means every 5
    // minutes, not every 5 seconds.
    let mut cron = croner::Cron::new(expr);
    cron.parse()
        .map_err(|_| RtDbError::bad_request("invalid cron expression"))?;
    let now = DateTime::<Utc>::from_timestamp_millis(now_ms)
        .ok_or_else(|| RtDbError::internal("invalid timestamp"))?;
    let next = cron
        .find_next_occurrence(&now, false)
        .map_err(|_| RtDbError::bad_request("cron expression has no future fire times"))?;
    Ok(next.timestamp_millis())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::now_ms;

    /// 2026-07-23T12:00:00Z = 1784808000000 ms (a Thursday). A fixed anchor so
    /// the minute/hour/day math is deterministic.
    const ANCHOR_MS: i64 = 1_784_808_000_000;

    #[test]
    fn every_5_minutes_not_seconds() {
        // `*/5 * * * *` must mean every 5 MINUTES (min-first), not every 5
        // seconds. The next fire after 12:00:00Z is 12:05:00Z = +300000 ms.
        let next = next_fire("*/5 * * * *", ANCHOR_MS).unwrap();
        assert_eq!(next - ANCHOR_MS, 300_000);
    }

    #[test]
    fn weekdays_at_9am_from_thursday() {
        // 2026-07-23 is a Thursday. `0 9 * * 1-5` next fires 2026-07-24 09:00Z.
        let next = next_fire("0 9 * * 1-5", ANCHOR_MS).unwrap();
        assert_eq!(next - ANCHOR_MS, 21 * 3600 * 1000); // +21h → Fri 09:00
    }

    #[test]
    fn rejects_garbage() {
        assert!(next_fire("not a cron", ANCHOR_MS).is_err());
    }

    #[test]
    fn next_is_strictly_after_now() {
        let next = next_fire("* * * * *", ANCHOR_MS).unwrap();
        assert!(next > ANCHOR_MS);
    }

    #[test]
    fn now_ms_is_available() {
        // Sanity: the helper imports compile against the real clock helper.
        let _ = now_ms();
    }
}
