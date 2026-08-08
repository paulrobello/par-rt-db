//! In-memory fixed-window HTTP rate limiter. One noisy app on a multi-db
//! instance can otherwise starve the others by saturating the committer /
//! connection pool with one-shot HTTP requests. Boot-configurable RPM ceilings
//! (0 = unlimited, the default) cap requests per machine token and per database;
//! OAuth sessions, which carry no machine-token identity, are rate-limited per-db
//! only.
//!
//! Shared by HTTP (`check_http_rate_limits`) and the reactive WS handler: the
//! `Mutate` and `Subscribe` arms call `evaluate` after re-authorizing and, on a
//! denial, reply with a typed `RATE_LIMITED` error (`MutateErr`/`SubscribeErr`)
//! carrying `retryAfter` — the connection stays open. The WS handler's separate
//! per-connection frame cap (`ws.rs::ConnRateLimiter`, 200 msgs/10s) is a coarse
//! flood valve that closes the socket and is independent of this limiter.
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

/// What the limiter is bucketing: a single machine token (per-token ceiling),
/// every request against one database (per-db ceiling shared across all
/// principals), or an unauthenticated caller's client IP (per-IP ceiling on
/// the public storage serve route — SEC-004). `String` is the token id, db
/// name, or IP literal/textual key respectively.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub enum RateKey {
    Token(String),
    Db(String),
    Ip(String),
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
/// Runs the per-token then per-db fixed-window checks in order, returning the
/// first denial (with its `retry_after_secs` hint) or `Allowed`. The per-token
/// check applies only to `Principal::Machine` (OAuth sessions have no
/// machine-token identity and skip straight to per-db). Shared by the HTTP
/// gate (`check_http_rate_limits`) and the WS `Mutate`/`Subscribe` arms.
pub async fn evaluate(state: &AppState, principal: &Principal, db: &str) -> RateDecision {
    let token_limit = state.config.rate_limit_per_token_rpm;
    if token_limit > 0
        && let Principal::Machine { token_id, .. } = principal
        && let RateDecision::Denied { retry_after_secs } = state
            .rate_limiter
            .check(RateKey::Token(token_id.clone()), token_limit)
            .await
    {
        return RateDecision::Denied { retry_after_secs };
    }

    let db_limit = state.config.rate_limit_per_db_rpm;
    if db_limit > 0
        && let RateDecision::Denied { retry_after_secs } = state
            .rate_limiter
            .check(RateKey::Db(db.to_string()), db_limit)
            .await
    {
        return RateDecision::Denied { retry_after_secs };
    }

    RateDecision::Allowed
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
    match evaluate(state, principal, db).await {
        RateDecision::Denied { retry_after_secs } => Err(RtDbError::rate_limited(retry_after_secs)),
        RateDecision::Allowed => Ok(()),
    }
}

/// Per-IP gate for the unauthenticated public storage serve route
/// (`GET /storage/{id}`, SEC-004). The opaque blob id is not enumerable, but a
/// holder of one valid id can otherwise hammer the route without bound — the
/// on-the-fly image-transform path (`?w=&h=&...`) amplifies cost per request,
/// and an unauth route has no principal to key on, so the caller's IP is the
/// only available identity. `ip_key` is the textual client IP (already
/// canonicalized by the caller: X-Forwarded-For leftmost → ConnectInfo → a
/// shared `"unknown"` sentinel when neither is available). Disabled when
/// `Config::storage_rate_limit_per_ip_rpm == 0`, the default.
pub async fn check_storage_public_rate_limit(
    state: &AppState,
    ip_key: &str,
) -> Result<(), RtDbError> {
    let limit = state.config.storage_rate_limit_per_ip_rpm;
    if limit == 0 {
        return Ok(());
    }
    match state
        .rate_limiter
        .check(RateKey::Ip(ip_key.to_string()), limit)
        .await
    {
        RateDecision::Denied { retry_after_secs } => Err(RtDbError::rate_limited(retry_after_secs)),
        RateDecision::Allowed => Ok(()),
    }
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

    #[tokio::test]
    async fn ip_key_buckets_independently_of_token_and_db() {
        // SEC-004: per-IP budget is a separate axis from per-token / per-db so
        // an unauthenticated public-storage flood can be capped without
        // affecting authenticated traffic budgets on the same limiter.
        let limiter = RateLimiter::new();
        assert_eq!(
            limiter.check(RateKey::Ip("203.0.113.9".into()), 2).await,
            RateDecision::Allowed
        );
        assert_eq!(
            limiter.check(RateKey::Ip("203.0.113.9".into()), 2).await,
            RateDecision::Allowed
        );
        // Third hit same minute is denied; a different IP is unaffected, and
        // the token/db budgets remain untouched.
        assert!(deny_secs(limiter.check(RateKey::Ip("203.0.113.9".into()), 2).await).is_some());
        assert_eq!(
            limiter.check(RateKey::Ip("198.51.100.42".into()), 2).await,
            RateDecision::Allowed
        );
        assert_eq!(
            limiter.check(RateKey::Token("t".into()), 1).await,
            RateDecision::Allowed
        );
        assert_eq!(
            limiter.check(RateKey::Db("d".into()), 1).await,
            RateDecision::Allowed
        );
    }
}
