//! Host-token and session liveness gating for subscription fan-out (QA-003
//! split): one bounded liveness check per unique credential per batch, dead
//! credentials drop their subscriptions, transient failures retain but
//! suppress this pass's pushes (fail-closed).

use std::collections::HashMap;

use futures::StreamExt;

use super::{DbSubs, FAN_OUT_CONCURRENCY, SubEntry};
use crate::error::RtDbError;

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum HostLive {
    Live,
    Dead,
    Transient,
}

fn host_token(entry: &SubEntry) -> Option<&str> {
    match entry.live_host_principal.as_ref()? {
        crate::auth::Principal::Machine { token_id, .. } => Some(token_id),
        _ => None,
    }
}

pub(super) async fn host_liveness<F, Fut>(
    entries: &DbSubs,
    _db: &str,
    mut check: F,
) -> HashMap<String, HostLive>
where
    F: FnMut(crate::auth::Principal) -> Fut,
    Fut: std::future::Future<Output = Result<(), RtDbError>>,
{
    let principals: HashMap<_, _> = entries
        .values()
        .filter_map(|entry| {
            Some((
                host_token(entry)?.to_owned(),
                entry.live_host_principal.clone()?,
            ))
        })
        .collect();
    futures::stream::iter(principals)
        .map(|(id, principal)| {
            let future = check(principal);
            async move {
                let live = match future.await {
                    Ok(()) => HostLive::Live,
                    Err(error)
                        if matches!(
                            error.code,
                            crate::error::ErrorCode::Unauthorized
                                | crate::error::ErrorCode::Forbidden
                        ) =>
                    {
                        HostLive::Dead
                    }
                    Err(_) => HostLive::Transient,
                };
                (id, live)
            }
        })
        .buffer_unordered(FAN_OUT_CONCURRENCY)
        .collect()
        .await
}

pub(super) fn apply_host_liveness(entries: &mut DbSubs, states: &HashMap<String, HostLive>) {
    // Definitive revocation/expiry is terminal. Database failures only suppress
    // this pass's pushes; retain subscriptions so a transient outage can recover.
    entries.retain(|_, entry| {
        host_token(entry).is_none_or(|id| states.get(id) != Some(&HostLive::Dead))
    });
}

pub(super) fn host_may_push(entry: &SubEntry, states: &HashMap<String, HostLive>) -> bool {
    if entry.principal_ctx.host_capability.is_none() {
        return true;
    }
    host_token(entry).is_some_and(|id| states.get(id) == Some(&HostLive::Live))
}

/// Fan-out session liveness: one bounded liveness check per UNIQUE session
/// hash per batch (never per subscription), so N subscriptions on one session
/// cost one query. `Dead` is terminal (revoked by any path or expired);
/// `Transient` suppresses nothing — a DB failure must not sever healthy
/// subscriptions, mirroring `apply_host_liveness`.
pub(super) async fn session_liveness<F, Fut>(
    entries: &DbSubs,
    mut check: F,
) -> HashMap<String, HostLive>
where
    F: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<(), RtDbError>>,
{
    let hashes: std::collections::HashSet<String> = entries
        .values()
        .filter_map(|entry| entry.principal_ctx.session_hash.clone())
        .collect();
    futures::stream::iter(hashes)
        .map(|hash| {
            let future = check(hash.clone());
            async move {
                let live = match future.await {
                    Ok(()) => HostLive::Live,
                    Err(error)
                        if matches!(
                            error.code,
                            crate::error::ErrorCode::Unauthorized
                                | crate::error::ErrorCode::Forbidden
                        ) =>
                    {
                        HostLive::Dead
                    }
                    Err(_) => HostLive::Transient,
                };
                (hash, live)
            }
        })
        .buffer_unordered(FAN_OUT_CONCURRENCY)
        .collect()
        .await
}

pub(super) fn apply_session_liveness(entries: &mut DbSubs, states: &HashMap<String, HostLive>) {
    entries.retain(|_, entry| {
        entry
            .principal_ctx
            .session_hash
            .as_ref()
            .is_none_or(|hash| states.get(hash) != Some(&HostLive::Dead))
    });
}

/// Publication gate mirroring `host_may_push`: a session-backed subscription
/// pushes only on a confirmed `Live` check this pass. A `Transient` lookup
/// failure SUPPRESSES this pass's pushes while retaining the subscription —
/// the same fail-closed posture as host tokens — which costs nothing extra
/// during a real Postgres outage (the re-run itself needs the database).
pub(super) fn session_may_push(entry: &SubEntry, states: &HashMap<String, HostLive>) -> bool {
    match entry.principal_ctx.session_hash.as_ref() {
        None => true,
        Some(hash) => states.get(hash) == Some(&HostLive::Live),
    }
}
