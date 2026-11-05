//! ARC-002: enforcement tests for `AppState::new`'s construction contract.
//!
//! `AppState` is the server's composition root, and its constructor is a long
//! sequence whose comments encode construction-order constraints. Which of
//! those constraints need a test, and which the compiler already enforces:
//!
//! * **Compiler-enforced (no test needed):** every "built before X so it can
//!   be threaded into X" ordering — `quotas`, `forwarder`, `instance_id` into
//!   `Committers::new`, `metrics` into `subs`/`Runtime`/`presence`/`image`,
//!   `background` before each `track`/`spawn`, and the pre-reads of config
//!   fields before `config` moves into the struct literal. Each is a move of
//!   a local into its consumer, so a reorder does not compile.
//! * **Runtime-enforced (tested here):** the resolved `instance_id` is
//!   non-empty and auto-generated ids are distinct across constructions
//!   (NOTIFY self-dedupe and the ownership lease key on it); the forwarder
//!   handle is threaded to the committers exactly when multi-instance mode is
//!   on; and dropping a `BackgroundGuard` cancels the token every spawned
//!   listener races against — the mechanism that keeps intentionally-leaked
//!   test `AppState`s from accumulating live listeners for the whole test
//!   binary.
//! * **Review-only (left prose):** that the Arc handles committers hold are
//!   the *same instances* AppState exposes (`Committers`' dependency fields
//!   are private; asserting identity would mean widening the API during
//!   QA-003's churn for little drift risk).

use std::sync::Arc;
use std::time::Duration;

use rtdb_server::AppState;

use crate::common::{connect_and_bootstrap, test_config, test_hot};

/// A multi-instance config with a unique per-process replica id (see
/// `unique_instance_id` in `common`): the NOTIFY channels are fleet-global,
/// so a hardcoded id collides with concurrent test processes.
fn multi_instance_config() -> rtdb_server::config::Config {
    let mut config = test_config();
    config.multi_instance.enabled = true;
    config.multi_instance.instance_id = Some(crate::common::unique_instance_id("appstate-test"));
    config
}

async fn build_state(config: rtdb_server::config::Config) -> Arc<AppState> {
    let pool = connect_and_bootstrap(&config.database_url)
        .await
        .expect("connect to test postgres + bootstrap rtdb_auth");
    AppState::new(pool, config, test_hot())
}

#[tokio::test]
async fn generated_instance_ids_are_nonempty_and_distinct() {
    let first = build_state(test_config()).await;
    let second = build_state(test_config()).await;
    assert!(
        !first.runtime.instance_id.trim().is_empty(),
        "auto-generated instance id must be non-empty (NOTIFY self-dedupe depends on it)"
    );
    assert_ne!(
        first.runtime.instance_id, second.runtime.instance_id,
        "two AppState constructions without an explicit id must not share an \
         identity — a shared id breaks op-feed/presence self-dedupe and the \
         forward-reply claim path across processes"
    );
}

#[tokio::test]
async fn explicit_instance_id_is_threaded_and_blank_falls_back() {
    let config = multi_instance_config();
    let explicit = config.multi_instance.instance_id.clone();
    let state = build_state(config).await;
    assert_eq!(state.runtime.instance_id, explicit.expect("explicit id"));

    // A whitespace-only explicit id is treated as unset: the constructor
    // filters it and generates one instead of threading an unusable identity.
    let mut blank = multi_instance_config();
    blank.multi_instance.instance_id = Some("   ".into());
    let state = build_state(blank).await;
    assert!(
        !state.runtime.instance_id.trim().is_empty(),
        "whitespace-only RTDB_INSTANCE_ID must fall back to a generated id"
    );
}

#[tokio::test]
async fn forwarder_is_threaded_to_committers_exactly_in_multi_instance_mode() {
    // Single-instance (the default): no forwarder — the forward path is
    // unreachable without a shadow replica, so construction omits it.
    let single = build_state(test_config()).await;
    assert!(
        single.realtime.committers.forwarder().is_none(),
        "single-instance committers must not carry a forwarder"
    );

    // Multi-instance: built before Committers::new and threaded in — this is
    // observable via the one public accessor the committers expose.
    let multi = build_state(multi_instance_config()).await;
    assert!(
        multi.realtime.committers.forwarder().is_some(),
        "multi-instance committers must hold the shared forwarder (shadow \
         writes submit through it)"
    );
}

#[tokio::test]
async fn dropping_background_guard_cancels_spawned_listeners() {
    let mut config = test_config();
    config.presence_enabled = true;
    let state = build_state(config).await;
    let _guard = crate::common::background_guard(&state);
    let token = state.background.token.clone();
    assert!(!token.is_cancelled(), "token live while the guard holds it");

    // A canary standing in for every spawned listener loop: each races
    // `token.cancelled()` (see BackgroundTasks::spawn), so the token firing
    // is the signal every listener exits on.
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let canary = tokio::spawn(async move {
        token.cancelled().await;
        let _ = tx.send(());
    });

    drop(_guard);
    assert!(
        state.background.token.is_cancelled(),
        "guard drop must cancel the token"
    );
    tokio::time::timeout(Duration::from_secs(1), rx)
        .await
        .expect("canary listener exits once the token is cancelled")
        .expect("canary sends before exiting");
    let _ = canary.await;
}
