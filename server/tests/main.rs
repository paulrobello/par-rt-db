//! The single server integration-test binary (ARC-010).
//!
//! Every file under `server/tests/` is a module of this one binary rather than
//! its own test target. 55 separate targets meant 55 links of the whole server
//! crate on every `cargo test`, 55 processes each opening its own Postgres pool
//! against the shared dev database — the source of the `oauth_test`
//! PoolTimedOut flake — and 55 clippy passes in the pre-commit hook.
//!
//! Adding a test file means adding one `mod` line here. `common` holds the
//! shared fixtures; submodules reach it as `crate::common::…`.

mod common;

mod admin_test;
mod anonymous_auth_test;
mod audit_test;
mod auth_test;
mod auto_increment_test;
mod cascade_test;
mod computed_test;
mod dashboard_test;
mod db_cleanup_test;
mod defaults_test;
mod golden_vector_test;
mod healthz_test;
mod hot_config_test;
mod http_api_test;
mod idle_reclaim_test;
mod image_transform_test;
mod merge_test;
mod migration_test;
mod multi_instance_stage4_test;
mod mutation_dedup_test;
mod notify_test;
mod oauth_ms_apple_test;
mod oauth_test;
mod per_row_auth_test;
mod presence_test;
mod presence_xreplica_test;
mod proptest_parity;
mod query_combinations;
mod query_introspect_test;
mod query_test;
mod quota_test;
mod rate_limit_test;
mod relative_filter_test;
mod schedule_step_test;
mod scheduled_test;
mod scheduler_lifecycle_test;
mod schema_evolution_test;
mod schema_history_test;
mod schema_validators_test;
mod search_test;
mod semantics_corpus_test;
mod sessions_test;
mod storage_signed_url_test;
mod storage_test;
mod sub_invalidation_test;
mod subs_test;
mod ttl_test;
mod txn_test;
mod updated_at_test;
mod vector_test;
mod webhook_test;
mod wire_corpus;
mod workflows_test;
mod ws_test;
