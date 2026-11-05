use super::state::{
    ClientInner, Cmd, MutReply, Pending, PendingQueues, PresenceSnapshot, QueuedMutate,
    QueuedSchedule, QueuedWorkflow, SchedReply, ScheduleOutcome, Snapshot, SubMaps, WfReply,
    WorkflowOutcome,
};
use crate::error::{ErrorCode, RtDbError};
use crate::feed;
use crate::mutation::Transaction;
use crate::optimistic::{OptimisticProjection, project_optimistic_update};
use crate::wire::ServerMessage;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

// ── pure routing ─────────────────────────────────────────────────────────────

/// Route one inbound server message to its subscription / pending mutation /
/// pending schedule call. Pure with respect to the socket (no I/O) so it is
/// unit-testable without a server. Pong freshness is tracked by the session's
/// select arm, not here.
pub(super) fn apply_server_message(
    inner: &Arc<ClientInner>,
    msg: ServerMessage,
    queues: &mut PendingQueues,
) {
    match msg {
        ServerMessage::QueryUpdate { .. } | ServerMessage::SubscribeErr { .. } => {
            on_query_update(inner, msg);
        }
        ServerMessage::MutateOk { .. } | ServerMessage::MutateErr { .. } => {
            on_mutate_reply(inner, &mut queues.mutate.by_id, msg);
        }
        ServerMessage::ScheduleOk { .. }
        | ServerMessage::ScheduleErr { .. }
        | ServerMessage::ScheduleAck { .. }
        | ServerMessage::ListSchedulesOk { .. } => {
            on_schedule_reply(&mut queues.schedule.by_id, msg);
        }
        ServerMessage::StartWorkflowOk { .. }
        | ServerMessage::StartWorkflowErr { .. }
        | ServerMessage::WorkflowAck { .. }
        | ServerMessage::ListWorkflowsOk { .. } => {
            on_workflow_reply(&mut queues.workflow.by_id, msg);
        }
        ServerMessage::PresenceSnapshot { .. }
        | ServerMessage::PresenceErr { .. }
        | ServerMessage::PresenceDelta { .. } => {
            on_presence(inner, msg);
        }
        ServerMessage::FeedEvents { .. } | ServerMessage::FeedErr { .. } => {
            if let Some((feed_id, seq)) = feed::on_server_message(&inner.feeds, msg) {
                let _ = inner.cmd_tx.send(Cmd::FeedAck { feed_id, seq });
            }
        }
        // Pong is handled by the session loop; AuthOk/AuthErr arrive only at the
        // handshake, never mid-session.
        ServerMessage::Pong | ServerMessage::AuthOk { .. } | ServerMessage::AuthErr { .. } => {}
    }
}

/// Route a `queryUpdate`/`subscribeErr` frame to its subscription's watch
/// channel, reconciling or dropping the shape as appropriate.
fn on_query_update(inner: &Arc<ClientInner>, msg: ServerMessage) {
    match msg {
        ServerMessage::QueryUpdate { query_id, result } => {
            let maps = inner.subs.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(sub) = maps
                .by_id
                .get(&query_id)
                .and_then(|key| maps.by_key.get(key))
            {
                // Reconcile: the authoritative result supersedes any in-flight
                // overlay; record it as the new projection base.
                {
                    let mut opt = sub.optimistic.lock().unwrap_or_else(|p| p.into_inner());
                    opt.active = false;
                    opt.server_last = Some(result.clone());
                }
                let _ = sub.tx.send(Snapshot::Value(Box::new(result)));
            }
        }
        ServerMessage::SubscribeErr { query_id, error } => {
            let mut maps = inner.subs.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(key) = maps.by_id.remove(&query_id)
                && let Some(sub) = maps.by_key.remove(&key)
            {
                let _ = sub.tx.send(Snapshot::Error(error));
            }
        }
        _ => unreachable!("on_query_update called with a non-query message"),
    }
}

/// Route a `mutateOk`/`mutateErr` frame to its pending caller, syncing the
/// optimistic-overlay reverse index for both outcomes.
fn on_mutate_reply(
    inner: &Arc<ClientInner>,
    pending: &mut HashMap<String, MutReply>,
    msg: ServerMessage,
) {
    match msg {
        ServerMessage::MutateOk { mut_id, results } => {
            if let Some(reply) = pending.remove(&mut_id) {
                let _ = reply.send(Ok(results));
            }
            // No revert: the reconciling `QueryUpdate`(s) arrive and supersede
            // any overlay. Just drop the reverse index — the overlays are no
            // longer rollback-eligible.
            let mut maps = inner.subs.lock().unwrap_or_else(|p| p.into_inner());
            maps.overlays.remove(&mut_id);
        }
        ServerMessage::MutateErr { mut_id, error } => {
            if let Some(reply) = pending.remove(&mut_id) {
                let _ = reply.send(Err(error));
            }
            let mut maps = inner.subs.lock().unwrap_or_else(|p| p.into_inner());
            revert_overlays_for(&mut maps, &mut_id);
        }
        _ => unreachable!("on_mutate_reply called with a non-mutate message"),
    }
}

/// Route a `scheduleOk`/`scheduleErr`/`scheduleAck`/`listSchedulesOk` frame to
/// its pending caller.
fn on_schedule_reply(pending: &mut HashMap<String, SchedReply>, msg: ServerMessage) {
    match msg {
        ServerMessage::ScheduleOk { schedule_id, id } => {
            if let Some(reply) = pending.remove(&schedule_id) {
                let _ = reply.send(Ok(ScheduleOutcome::Id(id)));
            }
        }
        ServerMessage::ScheduleErr { schedule_id, error } => {
            if let Some(reply) = pending.remove(&schedule_id) {
                let _ = reply.send(Err(error));
            }
        }
        ServerMessage::ScheduleAck {
            schedule_id,
            ok,
            error,
        } => {
            if let Some(reply) = pending.remove(&schedule_id) {
                if ok {
                    let _ = reply.send(Ok(ScheduleOutcome::Ack(true)));
                } else if let Some(error) = error {
                    let _ = reply.send(Err(error));
                } else {
                    // Bare ok:false = no such pending job (unknown or already
                    // terminal): a no-op, not a failure (workflow-ack parity).
                    let _ = reply.send(Ok(ScheduleOutcome::Ack(false)));
                }
            }
        }
        ServerMessage::ListSchedulesOk {
            schedule_id,
            schedules,
        } => {
            if let Some(reply) = pending.remove(&schedule_id) {
                let _ = reply.send(Ok(ScheduleOutcome::List(schedules)));
            }
        }
        _ => unreachable!("on_schedule_reply called with a non-schedule message"),
    }
}

/// Route a `startWorkflowOk`/`startWorkflowErr`/`workflowAck`/
/// `listWorkflowsOk` frame to its pending caller.
fn on_workflow_reply(pending: &mut HashMap<String, WfReply>, msg: ServerMessage) {
    match msg {
        ServerMessage::StartWorkflowOk { workflow_id, info } => {
            if let Some(reply) = pending.remove(&workflow_id) {
                let _ = reply.send(Ok(WorkflowOutcome::Info(info)));
            }
        }
        ServerMessage::StartWorkflowErr { workflow_id, error } => {
            // Also carries a failed `listWorkflows`' correlation id — the
            // frame vocabulary has no distinct list-error frame, so both the
            // start and list families reject through this arm.
            if let Some(reply) = pending.remove(&workflow_id) {
                let _ = reply.send(Err(error));
            }
        }
        ServerMessage::WorkflowAck {
            workflow_id,
            ok,
            error,
        } => {
            if let Some(reply) = pending.remove(&workflow_id) {
                if ok {
                    let _ = reply.send(Ok(WorkflowOutcome::Ack(true)));
                } else if let Some(error) = error {
                    let _ = reply.send(Err(error));
                } else {
                    // Bare ok:false = unknown/terminal run: a no-op, not a
                    // failure (ts-client and http parity).
                    let _ = reply.send(Ok(WorkflowOutcome::Ack(false)));
                }
            }
        }
        ServerMessage::ListWorkflowsOk {
            workflow_id,
            workflows,
        } => {
            if let Some(reply) = pending.remove(&workflow_id) {
                let _ = reply.send(Ok(WorkflowOutcome::List(workflows)));
            }
        }
        _ => unreachable!("on_workflow_reply called with a non-workflow message"),
    }
}

/// Route a `presenceSnapshot`/`presenceDelta`/`presenceErr` frame to its
/// room's watch channel. Deltas are folded into the room's current member
/// list (the watch channel's own value is the baseline) under per-room
/// `seq`-gap discipline: a stale duplicate is dropped, a gap discards the
/// baseline and queues a `Cmd::PresenceResync` so the session re-joins and
/// the server answers with a fresh full snapshot.
fn on_presence(inner: &Arc<ClientInner>, msg: ServerMessage) {
    match msg {
        ServerMessage::PresenceSnapshot { room, members } => {
            // Per-room fan-out, mirroring how QueryUpdate routes to a per-id
            // handler through the subs map. Anyone holding a Presence handle for
            // this room observes the new member list via its watch receiver.
            let maps = inner.presence.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(state) = maps.by_room.get(&room) {
                // Snapshots carry no `seq`, so the delta cursor restarts
                // blank: the first delta after this may carry any `seq`.
                *state.last_seq.lock().unwrap_or_else(|p| p.into_inner()) = None;
                let _ = state.tx.send(PresenceSnapshot::Members(members));
            }
        }
        ServerMessage::PresenceDelta {
            room,
            seq,
            joined,
            left,
            state_changed,
        } => {
            let maps = inner.presence.lock().unwrap_or_else(|p| p.into_inner());
            // A delta for a room this client never joined has no baseline to
            // fold into — ignore it (the same rule as a snapshot's routing).
            let Some(state) = maps.by_room.get(&room) else {
                return;
            };
            let baseline = match state.tx.borrow().clone() {
                PresenceSnapshot::Members(members) => members,
                // Pending (no snapshot yet) or Error (dead join): nothing to
                // diff against; the server's next full snapshot will resync.
                _ => return,
            };
            let mut last = state.last_seq.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(prev) = *last {
                if seq <= prev {
                    // Stale duplicate or already-applied delta: idempotent
                    // no-op, never re-applied onto the baseline.
                    return;
                }
                if seq > prev + 1 {
                    // GAP: a delta was missed, so the baseline can no longer
                    // be trusted. Discard the cursor (and, with the next
                    // snapshot, the stale list) and re-join: the server
                    // resets the room's caught-up flag on every join, so the
                    // next flush delivers a fresh full snapshot. Never apply
                    // a delta onto a stale baseline.
                    *last = None;
                    drop(last);
                    drop(maps);
                    let _ = inner
                        .cmd_tx
                        .send(Cmd::PresenceResync { room: room.clone() });
                    return;
                }
            }
            // Contiguous delta: fold joined/left/stateChanged into the
            // baseline, then publish the folded FULL list — the public
            // `Presence` surface stays "the room's member list" regardless
            // of which frame shape the server used.
            let mut members = baseline;
            for id in &left {
                members.retain(|m| &m.connection_id != id);
            }
            for member in joined.into_iter().chain(state_changed) {
                match members
                    .iter_mut()
                    .find(|m| m.connection_id == member.connection_id)
                {
                    Some(existing) => *existing = member,
                    None => members.push(member),
                }
            }
            *last = Some(seq);
            let _ = state.tx.send(PresenceSnapshot::Members(members));
        }
        ServerMessage::PresenceErr { room, error } => {
            // The server rejected the join (e.g. presence not enabled). Surface
            // the error on the room's watch channel so a stale room doesn't keep
            // accumulating snapshots the caller can no longer act on, mirroring
            // the TS client which drops listeners on presenceErr.
            let maps = inner.presence.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(state) = maps.by_room.get(&room) {
                let _ = state.tx.send(PresenceSnapshot::Error(error));
            }
        }
        _ => unreachable!("on_presence called with a non-presence message"),
    }
}

/// Decrement a shape's refcount, removing it from both maps if it hit zero.
pub(super) fn maybe_unsubscribe(inner: &Arc<ClientInner>, query_id: &str) {
    let mut maps = inner.subs.lock().unwrap_or_else(|p| p.into_inner());
    let Some(key) = maps.by_id.get(query_id).cloned() else {
        return;
    };
    let Some(sub) = maps.by_key.get(&key) else {
        return;
    };
    // fetch_sub returns the PREVIOUS value; reaching 1 → this is the last handle.
    let was_last = sub.refcount.fetch_sub(1, Ordering::SeqCst) == 1;
    if was_last {
        maps.by_key.remove(&key);
        maps.by_id.remove(query_id);
    }
}

/// Mirror ts's `applyOptimistic`: for each live subscription whose `server_last`
/// base value is known, project `txn` onto it; for each non-`Skip` projection,
/// push the overlaid value through the watch channel immediately (before the
/// server round-trip) and record its `query_id` under `mut_id` in `overlays`
/// so a later rollback can find it. Caller-side (in `RtDbClient::mutate`) so
/// subscribers in other tasks see the overlay before the caller awaits.
pub(super) fn apply_optimistic(inner: &ClientInner, mut_id: &str, txn: &Transaction) {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let mut maps = inner.subs.lock().unwrap_or_else(|p| p.into_inner());
    let mut touched: Vec<String> = Vec::new();
    for sub in maps.by_key.values() {
        let mut opt = sub.optimistic.lock().unwrap_or_else(|p| p.into_inner());
        let Some(base) = opt.server_last.clone() else {
            continue;
        };
        match project_optimistic_update(&sub.query, &base, txn, now) {
            OptimisticProjection::Overlaid(v) => {
                opt.active = true;
                let _ = sub.tx.send(Snapshot::Value(Box::new(v)));
                touched.push(sub.query_id.clone());
            }
            OptimisticProjection::Skip => {}
        }
    }
    if !touched.is_empty() {
        maps.overlays
            .insert(mut_id.to_string(), touched.into_iter().collect());
    }
}

/// Revert one subscription's overlay: if one is active and a `server_last` base
/// exists, push the base back through the watch channel and clear `active`. No
/// op when no overlay is active (e.g. a `QueryUpdate` already reconciled).
fn revert_overlay(maps: &SubMaps, query_id: &str) {
    if let Some(sub) = maps
        .by_id
        .get(query_id)
        .and_then(|key| maps.by_key.get(key))
    {
        let mut opt = sub.optimistic.lock().unwrap_or_else(|p| p.into_inner());
        if opt.active
            && let Some(base) = opt.server_last.clone()
        {
            opt.active = false;
            let _ = sub.tx.send(Snapshot::Value(Box::new(base)));
        }
    }
}

/// Reverse-index revert: drop `mut_id`'s entry from `overlays` and revert every
/// subscription it had overlaid. Called from `MutateErr` and the reject paths.
fn revert_overlays_for(maps: &mut SubMaps, mut_id: &str) {
    if let Some(qids) = maps.overlays.remove(mut_id) {
        for qid in qids {
            revert_overlay(maps, &qid);
        }
    }
}

/// Reject every in-flight (sent, unacked) mutation. Queued (never-sent) mutations
/// are left intact so they survive the reconnect — see [`reject_all`]. Each
/// rejected mutation's overlays are reverted (a sent-but-unacked mutate never
/// gets a `MutateOk`/`MutateErr`, so the rollback must happen here).
pub(super) fn reject_inflight(
    inner: &Arc<ClientInner>,
    pending: &mut Pending<QueuedMutate, MutReply>,
    reason: &str,
) {
    let err = RtDbError::new(ErrorCode::Internal, reason);
    let mut maps = inner.subs.lock().unwrap_or_else(|p| p.into_inner());
    for (mut_id, reply) in pending.by_id.drain() {
        revert_overlays_for(&mut maps, &mut_id);
        let _ = reply.send(Err(err.clone()));
    }
}

/// Reject every mutation, in-flight and queued. Used on terminal teardown.
pub(super) fn reject_all(
    inner: &Arc<ClientInner>,
    pending: &mut Pending<QueuedMutate, MutReply>,
    reason: &str,
) {
    reject_inflight(inner, pending, reason);
    let err = RtDbError::new(ErrorCode::Internal, reason);
    // Queued mutates also had overlays applied (the apply hook runs in
    // `RtDbClient::mutate` before the driver even receives the command), so
    // revert them too.
    let mut maps = inner.subs.lock().unwrap_or_else(|p| p.into_inner());
    for q in pending.unsent.drain(..) {
        revert_overlays_for(&mut maps, &q.mut_id);
        let _ = q.reply.send(Err(err.clone()));
    }
}

/// Reject every in-flight (sent, unacked) schedule call. Queued (never-sent)
/// calls are left intact so they survive the reconnect — see
/// [`reject_all_schedules`].
pub(super) fn reject_inflight_schedules(
    pending: &mut Pending<QueuedSchedule, SchedReply>,
    reason: &str,
) {
    let err = RtDbError::new(ErrorCode::Internal, reason);
    for (_, reply) in pending.by_id.drain() {
        let _ = reply.send(Err(err.clone()));
    }
}

/// Reject every schedule call, in-flight and queued. Used on terminal teardown.
pub(super) fn reject_all_schedules(
    pending: &mut Pending<QueuedSchedule, SchedReply>,
    reason: &str,
) {
    reject_inflight_schedules(pending, reason);
    let err = RtDbError::new(ErrorCode::Internal, reason);
    for q in pending.unsent.drain(..) {
        let _ = q.reply.send(Err(err.clone()));
    }
}

/// Reject every in-flight (sent, unacked) workflow call. Queued (never-sent)
/// calls are left intact so they survive the reconnect — see
/// [`reject_all_workflows`].
pub(super) fn reject_inflight_workflows(
    pending: &mut Pending<QueuedWorkflow, WfReply>,
    reason: &str,
) {
    let err = RtDbError::new(ErrorCode::Internal, reason);
    for (_, reply) in pending.by_id.drain() {
        let _ = reply.send(Err(err.clone()));
    }
}

/// Reject every workflow call, in-flight and queued. Used on terminal teardown.
pub(super) fn reject_all_workflows(pending: &mut Pending<QueuedWorkflow, WfReply>, reason: &str) {
    reject_inflight_workflows(pending, reason);
    let err = RtDbError::new(ErrorCode::Internal, reason);
    for q in pending.unsent.drain(..) {
        let _ = q.reply.send(Err(err.clone()));
    }
}
