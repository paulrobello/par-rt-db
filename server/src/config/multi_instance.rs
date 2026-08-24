//! Cross-instance op-feed fan-out knobs, nested under `Config::multi_instance`
//! (ENH-022 Stage 2 / ARC-012). Boot-only. When `enabled` is false (the
//! default), the committer never calls `pg_notify` and `AppState::new` never
//! spawns the LISTEN task — a single-instance deploy is byte-for-byte
//! unchanged. When true, each durable DocOp also emits one
//! `pg_notify('rtdb_ops', …)` at the committer's tap-site, and a per-process
//! LISTEN task mirrors peer notifications into the local op-feed ring.
//! `instance_id` tags payloads for self-dedupe; an explicit value is
//! recommended in a multi-replica deploy so a restart keeps the same id,
//! otherwise one is generated per boot.

use super::env_parsed;

#[derive(Clone, Debug)]
pub struct MultiInstanceConfig {
    /// RTDB_MULTI_INSTANCE (default false). Master switch for cross-instance
    /// op-feed via Postgres LISTEN/NOTIFY. Leave false for single-instance
    /// deploys (the default topology).
    pub enabled: bool,
    /// RTDB_INSTANCE_ID (default None = auto-generated). Stable replica id
    /// for NOTIFY self-dedupe. Set to a distinct value per replica in a
    /// multi-instance deploy; when unset, `AppState::new` generates a short
    /// hex id.
    pub instance_id: Option<String>,
    /// RTDB_FORWARD_TIMEOUT_MS (default 5000, clamped to >= 100). ENH-022
    /// Stage 4c: how long a non-owner replica waits for the lease owner to
    /// answer a forwarded write before attempting the takeover itself. The
    /// owner normally answers in milliseconds — this bounds the owner-dead
    /// failover latency. A reply arriving after the timeout is dropped (the
    /// write may still have committed; clients needing exactly-once retries
    /// should use idempotency keys).
    pub forward_timeout_ms: u64,
    /// RTDB_FORWARD_CONCURRENCY (default 64, clamped to >= 1). ARC-008: caps
    /// the number of forwarded-write executions `run_forward_listener` runs
    /// concurrently (one `tokio::spawn` per in-flight forwarded request).
    /// A request that arrives once the cap is saturated gets an immediate
    /// RATE_LIMITED reply instead of an unbounded task pile-up.
    pub forward_concurrency: usize,
}

impl Default for MultiInstanceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            instance_id: None,
            forward_timeout_ms: 5000,
            forward_concurrency: 64,
        }
    }
}

impl MultiInstanceConfig {
    pub(super) fn from_env() -> Result<Self, String> {
        let enabled = super::env_bool("RTDB_MULTI_INSTANCE", false);
        let instance_id = std::env::var("RTDB_INSTANCE_ID")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        // ENH-022 Stage 4c: forwarded-write reply deadline before the
        // non-owner attempts the lease takeover. Clamped to a floor of 100ms
        // so a typo cannot make every forward fall straight through to
        // takeover (which would ping-pong the lease under a load balancer).
        let forward_timeout_ms = env_parsed("RTDB_FORWARD_TIMEOUT_MS", 5_000u64)?.max(100);
        // ARC-008: bound `run_forward_listener`'s concurrent forwarded-write
        // executions so a burst of forwarded writes cannot spawn an unbounded
        // number of tasks against one owner's committer.
        let forward_concurrency = env_parsed("RTDB_FORWARD_CONCURRENCY", 64usize)?.max(1);
        Ok(Self {
            enabled,
            instance_id,
            forward_timeout_ms,
            forward_concurrency,
        })
    }
}
