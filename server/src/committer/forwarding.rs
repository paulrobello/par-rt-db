//! Origin-side glue for Stage 4c forwarding: projecting a `CommitterRequest`
//! into its `ForwardWrite`, minting the idempotency key that makes a takeover
//! resubmission a replay (ARC-003), and delivering the owner's outcome back
//! into the original request's reply channel.

use super::*;

/// Stamp a server-minted idempotency key onto an unkeyed `Mutate` about to be
/// forwarded (ARC-003). Runs BEFORE `forward_write_of` so the forwarded
/// payload and the request the takeover path may resubmit carry the same key.
/// A client-supplied key is left alone; an empty one counts as absent
/// (`handle_mutate` filters empty keys, so it would dedup nothing).
pub(in crate::committer) fn mint_forward_idempotency_key(req: &mut CommitterRequest) {
    if let CommitterRequest::Mutate {
        idempotency_key, ..
    } = req
        && idempotency_key.as_deref().unwrap_or("").is_empty()
    {
        *idempotency_key = Some(uuid::Uuid::now_v7().simple().to_string());
    }
}

/// The forwardable projection of a write arm (Stage 4c): every reply-carrying
/// write arm has a `ForwardWrite` variant; fire-and-forget arms (scheduler,
/// reaper, workflow) return `None` — they originate only from an owner's own
/// pollers and keep the `run_committer` CONFLICT backstop.
pub(in crate::committer) fn forward_write_of(
    req: &CommitterRequest,
) -> Option<crate::forward::ForwardWrite> {
    match req {
        CommitterRequest::Mutate {
            idempotency_key,
            txn,
            principal_ctx,
            ..
        } => Some(crate::forward::ForwardWrite::Mutate {
            idempotency_key: idempotency_key.clone(),
            txn: txn.clone(),
            principal: principal_ctx.clone(),
        }),
        CommitterRequest::RunMigrate { request, .. } => {
            Some(crate::forward::ForwardWrite::Migrate {
                request: request.clone(),
            })
        }
        CommitterRequest::RunPushSchema { schema, .. } => {
            Some(crate::forward::ForwardWrite::PushSchema {
                schema: schema.clone(),
            })
        }
        CommitterRequest::RunMergeUsers {
            anon_id, real_id, ..
        } => Some(crate::forward::ForwardWrite::MergeUsers {
            anon_id: anon_id.clone(),
            real_id: real_id.clone(),
        }),
        CommitterRequest::RunRestoreSchema { target_version, .. } => {
            Some(crate::forward::ForwardWrite::RestoreSchema {
                target_version: *target_version,
            })
        }
        _ => None,
    }
}

/// Deliver the owner's error verbatim into whatever reply channel the
/// original request carries (the forward reached a live owner and the write
/// genuinely failed there — the client should see the owner's error, not a
/// generic one).
pub(in crate::committer) fn fail_forwarded_reply(req: CommitterRequest, err: RtDbError) {
    // One arm per type: the oneshot payloads differ, so the arms cannot share
    // an or-pattern.
    match req {
        CommitterRequest::Mutate { reply, .. } => {
            let _ = reply.send(Err(err));
        }
        CommitterRequest::RunMigrate { reply, .. } => {
            let _ = reply.send(Err(err));
        }
        CommitterRequest::RunPushSchema { reply, .. } => {
            let _ = reply.send(Err(err));
        }
        CommitterRequest::RunMergeUsers { reply, .. } => {
            let _ = reply.send(Err(err));
        }
        CommitterRequest::RunRestoreSchema { reply, .. } => {
            let _ = reply.send(Err(err));
        }
        _ => {}
    }
}

/// Decode the owner's serialized outcome into the arm's concrete type. A
/// decode failure means replica version skew on the result shape — surface
/// it as an internal error naming the decode, never as a silent success.
pub(in crate::committer) fn decode_or_internal<T: serde::de::DeserializeOwned>(
    value: serde_json::Value,
    reply: oneshot::Sender<Result<T, RtDbError>>,
) {
    match serde_json::from_value::<T>(value) {
        Ok(result) => {
            let _ = reply.send(Ok(result));
        }
        Err(err) => {
            let _ = reply.send(Err(RtDbError::internal(format!(
                "forwarded reply failed to decode: {err}"
            ))));
        }
    }
}
