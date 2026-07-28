//! In-memory fixed-window HTTP rate limiter. One noisy app on a multi-db
//! instance can otherwise starve the others by saturating the committer /
//! connection pool with one-shot HTTP requests. Boot-configurable RPM ceilings
//! (0 = unlimited, the default) cap requests per machine token and per database;
//! OAuth sessions, which carry no machine-token identity, are rate-limited per-db
//! only.
//!
//! HTTP-only for v1: the WebSocket handler keeps its existing per-connection
//! frame cap (`ws.rs`); message-level WS limiting is a documented follow-up.
//!
//! Algorithm: a fixed window per `RateKey` keyed by the wall-clock minute
//! (`secs_since_epoch / 60`). A burst at the minute boundary can momentarily
//! exceed the limit by up to one window's worth (the boundary case is
//! acceptable for v1 — predictable and cheap, no background reaping). The map
//! grows with the number of distinct (token_id, db) tuples ever seen; a future
//! enhancement can sweep stale buckets, but for a multi-tenant server with
//! bounded tokens/dbs this is not a leak in practice.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;

use crate::AppState;
use crate::auth::Principal;
use crate::error::RtDbError;

/// What the limiter is bucketing: a single machine token (per-token ceiling)
/// or every request against one database (per-db ceiling shared across all
/// principals). `String` is the token id or db name respectively.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum RateKey {
    Token(String),
    Db(String),
}

/// Outcome of a `RateLimiter::check`. `Denied` carries a `retry_after_secs`
/// hint for the `Retry-After` HTTP header: the seconds remaining in the current
/// minute bucket, so the client knows when one request's worth of budget frees
/// up. Always in `[1, 60]` since the bucket is minute-granular.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateDecision {
    Allowed,
    Denied { retry_after_secs: u32 },
}

#[derive(Debug, Default)]
pub struct RateLimiter {
    /// `(minute_bucket, count_so_far)` per key. Guarded by a tokio Mutex — the
    /// critical section is a HashMap get/insert with no `.await` inside, but
    /// tokio matches the rest of this codebase's locking convention.
    inner: Mutex<HashMap<RateKey, (u64, u32)>>,
}

impl RateLimiter {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Records one request against `key` and returns whether it was allowed
    /// under `limit_rpm`. `limit_rpm == 0` is the disabled case (always
    /// allowed, no map mutation) — the default, preserving today's behavior.
    pub async fn check(&self, key: RateKey, limit_rpm: u32) -> RateDecision {
        if limit_rpm == 0 {
            return RateDecision::Allowed;
        }
        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let current_bucket = now_secs / 60;
        let mut map = self.inner.lock().await;
        match map.get_mut(&key) {
            Some((bucket, count)) if *bucket == current_bucket => {
                if *count < limit_rpm {
                    *count += 1;
                    RateDecision::Allowed
                } else {
                    // Seconds remaining in the current minute window — the
                    // earliest the next slot opens. `now_secs % 60` is in
                    // `[0, 59]`, so the subtraction lands in `[1, 60]`.
                    let retry_after_secs = 60 - (now_secs % 60) as u32;
                    RateDecision::Denied { retry_after_secs }
                }
            }
            _ => {
                // Different minute bucket, or first time we've seen this key:
                // reset/open with count = 1 for this request.
                map.insert(key, (current_bucket, 1));
                RateDecision::Allowed
            }
        }
    }
}

/// Per-request HTTP rate-limit gate. Runs after `authorize` so an unauthorized
/// request never consumes rate-limit budget (a revoked token / wrong-db attempt
/// fails at `authorize` and never reaches this). Per-token applies only to
/// `Machine` principals — OAuth sessions have no machine-token identity and are
/// rate-limited per-db only, by design. Order is per-token then per-db: a
/// request that would blow either budget is denied with `RateLimited`.
pub async fn check_http_rate_limits(
    state: &AppState,
    principal: &Principal,
    db: &str,
) -> Result<(), RtDbError> {
    let token_limit = state.config.rate_limit_per_token_rpm;
    if token_limit > 0
        && let Principal::Machine { token_id, .. } = principal
        && let RateDecision::Denied { retry_after_secs } = state
            .rate_limiter
            .check(RateKey::Token(token_id.clone()), token_limit)
            .await
    {
        return Err(RtDbError::rate_limited(retry_after_secs));
    }

    let db_limit = state.config.rate_limit_per_db_rpm;
    if db_limit > 0
        && let RateDecision::Denied { retry_after_secs } = state
            .rate_limiter
            .check(RateKey::Db(db.to_string()), db_limit)
            .await
    {
        return Err(RtDbError::rate_limited(retry_after_secs));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn deny_secs(d: RateDecision) -> Option<u32> {
        match d {
            RateDecision::Allowed => None,
            RateDecision::Denied { retry_after_secs } => Some(retry_after_secs),
        }
    }

    #[tokio::test]
    async fn limit_zero_always_allows_without_mutating_state() {
        let limiter = RateLimiter::new();
        for _ in 0..10 {
            assert_eq!(
                limiter.check(RateKey::Token("t".into()), 0).await,
                RateDecision::Allowed
            );
        }
        // Map stays empty: disabled limits never insert.
        assert!(limiter.inner.lock().await.is_empty());
    }

    #[tokio::test]
    async fn under_limit_allows_then_denies_within_same_minute() {
        let limiter = RateLimiter::new();
        let key = RateKey::Db("d".into());
        for i in 1..=3 {
            assert_eq!(
                limiter.check(key.clone(), 3).await,
                RateDecision::Allowed,
                "request {i} under the limit should be allowed"
            );
        }
        // 4th request in the same minute is denied with a positive retry hint.
        match deny_secs(limiter.check(key.clone(), 3).await) {
            Some(secs) => assert!((1..=60).contains(&secs)),
            other => panic!("4th request should be denied, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn distinct_keys_have_independent_budgets() {
        let limiter = RateLimiter::new();
        // Token A exhausts its 1/min budget.
        assert_eq!(
            limiter.check(RateKey::Token("a".into()), 1).await,
            RateDecision::Allowed
        );
        assert!(deny_secs(limiter.check(RateKey::Token("a".into()), 1).await).is_some());
        // Token B still has its full budget — limits are per-key.
        assert_eq!(
            limiter.check(RateKey::Token("b".into()), 1).await,
            RateDecision::Allowed
        );
        // And a db-key budget is independent of both token budgets.
        assert_eq!(
            limiter.check(RateKey::Db("d".into()), 1).await,
            RateDecision::Allowed
        );
    }
}
