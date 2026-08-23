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
//! acceptable for v1 — predictable and cheap). The map is hard-bounded at
//! `MAX_BUCKETS` entries: on insert at the cap, expired buckets (older than the
//! current minute) are reclaimed first; if still at the cap, an arbitrary entry
//! is evicted so the new key can be tracked (SEC-112). This bounds memory in
//! the face of a varying-key flood, which the per-IP public route is exposed
//! to — `client_ip_key` (CF-Connecting-IP preferred, rightmost XFF fallback)
//! makes spoofing hard, but a real distributed flood can still surface many
//! distinct legitimate-looking keys.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::Mutex;

use crate::AppState;
use crate::auth::Principal;
use crate::error::RtDbError;

/// Hard cap on the number of distinct keys the limiter tracks at once. At
/// ~24 bytes per entry this is roughly 2.4 MB; large enough that legitimate
/// per-token / per-db / per-IP working sets never notice, small enough that a
/// varying-key flood can't grow memory unboundedly. Reached only under attack
/// or a misconfigured client spraying tokens; on insert past the cap the limter
/// first sweeps stale (last-minute) entries, then evicts an arbitrary entry.
pub const MAX_BUCKETS: usize = 100_000;

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
    /// ENH-022 Stage 4: when set (multi-instance only), checks go to
    /// `rtdb_auth.rate_counters` instead of the local map, so every replica
    /// shares one budget per key. One UPSERT per checked request, only when a
    /// limit is configured — `None` (single-instance, the default) never
    /// touches Postgres.
    pg: Option<sqlx::PgPool>,
}

impl RateLimiter {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Cross-process mode: counters live in Postgres (ENH-022 Stage 4, option
    /// B1 of docs/superpowers/specs/2026-08-22-multi-instance-stage4-design.md).
    pub fn new_pg(pool: sqlx::PgPool) -> Arc<Self> {
        Arc::new(Self {
            pg: Some(pool),
            ..Default::default()
        })
    }

    /// Records one request against `key` and returns whether it was allowed
    /// under `limit_rpm`. `limit_rpm == 0` is the disabled case (always
    /// allowed, no map mutation) — the default, preserving today's behavior.
    pub async fn check(&self, key: RateKey, limit_rpm: u32) -> RateDecision {
        if limit_rpm == 0 {
            return RateDecision::Allowed;
        }
        if let Some(pool) = &self.pg {
            return check_pg(pool, &key, limit_rpm).await;
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
                if map.len() >= MAX_BUCKETS {
                    // SEC-112: cap map growth. First reclaim expired buckets
                    // (older minute than current — they'd be overwritten on
                    // next hit anyway). If still at the cap, evict an arbitrary
                    // entry so this new key can be tracked — favor strict
                    // accounting over allowing an untracked request, since the
                    // only way to hit the cap is a varying-key flood and we
                    // want every active attacker key to remain rate-limited.
                    map.retain(|_, (bucket, _)| *bucket >= current_bucket);
                    if map.len() >= MAX_BUCKETS
                        && let Some(stale_key) = map.keys().next().cloned()
                    {
                        map.remove(&stale_key);
                    }
                }
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
/// ENH-022 Stage 4: periodic `rtdb_auth.rate_counters` cleanup — drops
/// minute buckets older than the previous minute, so the table stays bounded
/// at (active keys × 2) rows regardless of how long the instance runs.
/// Spawns only in multi-instance mode (`lib.rs`); errors log and retry on the
/// next tick rather than killing the task.
pub async fn run_counter_sweep(pool: sqlx::PgPool) {
    loop {
        let cutoff = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() / 60)
            .unwrap_or(0) as i64
            - 1;
        if let Err(err) =
            sqlx::query("DELETE FROM rtdb_auth.rate_counters WHERE minute_bucket < $1")
                .bind(cutoff)
                .execute(&pool)
                .await
        {
            tracing::warn!(error = %err, "rate counter sweep failed");
        }
        tokio::time::sleep(std::time::Duration::from_secs(60)).await;
    }
}

/// The Postgres-backed `check` (multi-instance): one atomic UPSERT per request
/// — the increment, the comparison, and the window all live in one row of
/// `rtdb_auth.rate_counters`, which is what makes the configured ceiling exact
/// across every replica sharing the Postgres. A denied request still lands its
/// increment (the decision — `count <= limit` — is identical either way; the
/// row resets with the next minute bucket). Fail-open on a counter error: a
/// limiter outage must not take writes down with it (the flood valve the
/// limiter is would become its own outage).
async fn check_pg(pool: &sqlx::PgPool, key: &RateKey, limit_rpm: u32) -> RateDecision {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let current_bucket = now_secs / 60;
    let (key_type, key_text) = match key {
        RateKey::Token(t) => ("token", t),
        RateKey::Db(d) => ("db", d),
        RateKey::Ip(ip) => ("ip", ip),
    };
    let count: i64 = match sqlx::query_scalar(
        "INSERT INTO rtdb_auth.rate_counters (key_type, key, minute_bucket, count) \
         VALUES ($1, $2, $3, 1) \
         ON CONFLICT (key_type, key, minute_bucket) \
         DO UPDATE SET count = rtdb_auth.rate_counters.count + 1 \
         RETURNING count",
    )
    .bind(key_type)
    .bind(key_text)
    .bind(current_bucket as i64)
    .fetch_one(pool)
    .await
    {
        Ok(n) => n,
        Err(err) => {
            tracing::warn!(error = %err, "rate counter upsert failed; allowing");
            return RateDecision::Allowed;
        }
    };
    if count <= i64::from(limit_rpm) {
        RateDecision::Allowed
    } else {
        let retry_after_secs = 60 - (now_secs % 60) as u32;
        RateDecision::Denied { retry_after_secs }
    }
}

pub async fn evaluate(state: &AppState, principal: &Principal, db: &str) -> RateDecision {
    let token_limit = state.config.rate_limit_per_token_rpm;
    if token_limit > 0
        && let Principal::Machine { token_id, .. } = principal
        && let RateDecision::Denied { retry_after_secs } = state
            .limits
            .rate_limiter
            .check(RateKey::Token(token_id.clone()), token_limit)
            .await
    {
        return RateDecision::Denied { retry_after_secs };
    }

    let db_limit = state.config.rate_limit_per_db_rpm;
    if db_limit > 0
        && let RateDecision::Denied { retry_after_secs } = state
            .limits
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
/// (`GET /storage/{id}`, SEC-004 / SEC-112). The opaque blob id is not
/// enumerable, but a holder of one valid id can otherwise hammer the route
/// without bound — the on-the-fly image-transform path (`?w=&h=&...`)
/// amplifies cost per request, and an unauth route has no principal to key on,
/// so the caller's IP is the only available identity. `ip_key` is the textual
/// client IP already canonicalized by `client_ip_key` (CF-Connecting-IP
/// preferred, rightmost XFF fallback, then the connection peer). Disabled when
/// `Config::storage_rate_limit_per_ip_rpm == 0` (the code default; the shipped
/// `docker-compose.yml` and `.env.example` set a non-zero default so the
/// mitigation is on out-of-the-box).
pub async fn check_storage_public_rate_limit(
    state: &AppState,
    ip_key: &str,
) -> Result<(), RtDbError> {
    let limit = state.config.storage_rate_limit_per_ip_rpm;
    if limit == 0 {
        return Ok(());
    }
    match state
        .limits
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

    // SEC-112: the map is hard-bounded. Inserting MAX_BUCKETS+1 distinct keys
    // must not grow beyond the cap. After the flood, the limiter must still
    // behave deterministically for a brand-new key (one outside the flood
    // range) — first hit allowed, second hit at limit=1 denied — proving the
    // cap-eviction path didn't leave the limiter in a degraded state.
    #[tokio::test]
    async fn sec112_map_is_hard_bounded_under_distinct_key_flood() {
        let limiter = RateLimiter::new();
        for i in 0..(MAX_BUCKETS + 50) {
            let _ = limiter.check(RateKey::Ip(format!("10.0.0.{i}")), 5).await;
        }
        let len = limiter.inner.lock().await.len();
        assert!(
            len <= MAX_BUCKETS,
            "map must stay at or under MAX_BUCKETS, got {len}"
        );
        // A key never seen before is still tracked correctly: first call opens
        // the bucket, second exceeds the 1/min limit.
        let fresh = RateKey::Ip("203.0.113.42".to_string());
        assert_eq!(
            limiter.check(fresh.clone(), 1).await,
            RateDecision::Allowed,
            "first hit on a brand-new key after the cap flood should still be allowed"
        );
        assert!(
            deny_secs(limiter.check(fresh, 1).await).is_some(),
            "second hit on the new key should be denied"
        );
    }
}
