use super::driver::{backoff_delay, canonical_key, jitter_fraction, sync_url};
use super::server_msg::{apply_optimistic, apply_server_message, maybe_unsubscribe};
use super::state::{
    ClientInner, ClientStatus, Cmd, Config, Liveness, OptimisticState, PendingQueues, PresenceMaps,
    PresenceRoomState, PresenceSnapshot, ScheduleOutcome, Snapshot, SubMaps, SubState,
    WorkflowOutcome,
};
use crate::error::{ErrorCode, RtDbError};
use crate::feed::FeedMaps;
use crate::mutation::Transaction;
use crate::query::Query;
use crate::wire::{
    ClientMessage, ScheduleInfo, ScheduleKind, ScheduleStatus, ScheduleWhen, ServerMessage,
    WorkflowInfo, WorkflowSpec, WorkflowStatus,
};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;

use crate::query::TableQuery;
use crate::wire::WorkflowStepSpec;
use serde_json::json;

fn q(table: &str) -> Query {
    TableQuery::new(table).collect()
}

#[test]
fn sync_url_flips_scheme() {
    assert_eq!(sync_url("http://h:8000"), "ws://h:8000");
    assert_eq!(sync_url("https://h"), "wss://h");
    assert_eq!(sync_url("https://h/"), "wss://h");
    assert_eq!(sync_url("https://h//"), "wss://h");
    assert_eq!(sync_url("wss://h"), "wss://h");
    assert_eq!(sync_url("ws://h/sync"), "ws://h/sync");
}

#[test]
fn canonical_key_dedups_same_shape() {
    let a = canonical_key(&q("items"));
    let b = canonical_key(&q("items"));
    let c = canonical_key(&TableQuery::new("items").take(5));
    assert_eq!(a, b, "same shape must share a key");
    assert_ne!(a, c, "different shape must differ");
}

#[test]
fn backoff_is_bounded_and_jittered() {
    let base = Duration::from_millis(100);
    let max = Duration::from_secs(1);
    for attempt in 0..8 {
        let d = backoff_delay(base, max, attempt);
        assert!(d <= max, "attempt {attempt} exceeded ceiling: {d:?}");
        assert!(d >= base / 2, "attempt {attempt} below half-base: {d:?}");
    }
    let d0 = backoff_delay(base, max, 0);
    assert!(d0 >= base / 2 && d0 <= base);
}

#[test]
fn jitter_fraction_in_unit_range() {
    for _ in 0..256 {
        let f = jitter_fraction();
        assert!((0.0..1.0).contains(&f), "out of range: {f}");
    }
}

// ── message (de)serialization round-trips ───────────────────────────────

#[test]
fn auth_frame_shape() {
    let v = serde_json::to_value(ClientMessage::Auth {
        token: Some("t".into()),
        db: "d".into(),
        protocol_version: None,
    })
    .unwrap();
    assert_eq!(v, json!({"type":"auth","token":"t","db":"d"}));
}

#[test]
fn empty_mutate_frame_shape() {
    let v = serde_json::to_value(ClientMessage::Mutate {
        mut_id: "mut-1".into(),
        idempotency_key: Some("k".into()),
        txn: Transaction { steps: vec![] },
    })
    .unwrap();
    assert_eq!(
        v,
        json!({"type":"mutate","mutId":"mut-1","idempotencyKey":"k","txn":{"steps":[]}})
    );
}

#[test]
fn schedule_client_frame_shapes() {
    let s = serde_json::to_value(ClientMessage::Schedule {
        schedule_id: "sch-1".into(),
        when: ScheduleWhen::Cron {
            expr: "*/5 * * * *".into(),
            tz: None,
        },
        txn: Transaction { steps: vec![] },
        external: None,
    })
    .unwrap();
    assert_eq!(
        s,
        json!({
            "type":"schedule",
            "scheduleId":"sch-1",
            "when":{"type":"cron","expr":"*/5 * * * *"},
            "txn":{"steps":[]}
        })
    );
    // `external: true` (external-claim mode) rides on the same frame;
    // `None` above is omitted entirely, never serialized as `null`.
    let ext = serde_json::to_value(ClientMessage::Schedule {
        schedule_id: "sch-1".into(),
        when: ScheduleWhen::AfterMs { ms: 100 },
        txn: Transaction { steps: vec![] },
        external: Some(true),
    })
    .unwrap();
    assert_eq!(
        ext,
        json!({
            "type":"schedule",
            "scheduleId":"sch-1",
            "when":{"type":"afterMs","ms":100},
            "txn":{"steps":[]},
            "external":true
        })
    );
    assert_eq!(
        serde_json::to_value(ClientMessage::CancelSchedule {
            schedule_id: "sch-1".into(),
            id: "job-1".into(),
        })
        .unwrap(),
        json!({"type":"cancelSchedule","scheduleId":"sch-1","id":"job-1"})
    );
    assert_eq!(
        serde_json::to_value(ClientMessage::ListSchedules {
            schedule_id: "sch-1".into()
        })
        .unwrap(),
        json!({"type":"listSchedules","scheduleId":"sch-1"})
    );
}

#[test]
fn workflow_client_frame_shapes() {
    let spec = WorkflowSpec {
        name: "drip".into(),
        steps: vec![WorkflowStepSpec {
            txn: Some(Transaction { steps: vec![] }),
            await_signal: None,
            retry: None,
            sleep_before_ms: None,
        }],
    };
    let s = serde_json::to_value(ClientMessage::StartWorkflow {
        workflow_id: "wf-1".into(),
        spec,
    })
    .unwrap();
    assert_eq!(
        s,
        json!({
            "type":"startWorkflow",
            "workflowId":"wf-1",
            "spec":{"name":"drip","steps":[{"txn":{"steps":[]}}]}
        })
    );
    assert_eq!(
        serde_json::to_value(ClientMessage::CancelWorkflow {
            workflow_id: "wf-2".into(),
            id: "wf-9".into(),
        })
        .unwrap(),
        json!({"type":"cancelWorkflow","workflowId":"wf-2","id":"wf-9"})
    );
    // signalWorkflow: payload included when Some, omitted when None.
    assert_eq!(
        serde_json::to_value(ClientMessage::SignalWorkflow {
            workflow_id: "wf-4".into(),
            id: "wf-9".into(),
            name: "approve".into(),
            payload: Some(json!({"ok": true})),
        })
        .unwrap(),
        json!({"type":"signalWorkflow","workflowId":"wf-4","id":"wf-9",
               "name":"approve","payload":{"ok":true}})
    );
    assert_eq!(
        serde_json::to_value(ClientMessage::SignalWorkflow {
            workflow_id: "wf-4".into(),
            id: "wf-9".into(),
            name: "approve".into(),
            payload: None,
        })
        .unwrap(),
        json!({"type":"signalWorkflow","workflowId":"wf-4","id":"wf-9","name":"approve"})
    );
    assert_eq!(
        serde_json::to_value(ClientMessage::ListWorkflows {
            workflow_id: "wf-3".into(),
            status: None,
        })
        .unwrap(),
        json!({"type":"listWorkflows","workflowId":"wf-3"})
    );
    assert_eq!(
        serde_json::to_value(ClientMessage::ListWorkflows {
            workflow_id: "wf-3".into(),
            status: Some(WorkflowStatus::Failed),
        })
        .unwrap(),
        json!({"type":"listWorkflows","workflowId":"wf-3","status":"failed"})
    );
}

#[test]
fn server_messages_round_trip() {
    let cases = vec![
        json!({"type":"queryUpdate","queryId":"sub-1","result":[{"_id":"a"}]}),
        json!({"type":"mutateOk","mutId":"mut-1","results":[]}),
        json!({"type":"mutateErr","mutId":"mut-2","error":{"code":"NOT_FOUND","message":"x"}}),
        json!({"type":"subscribeErr","queryId":"sub-1","error":{"code":"BAD_REQUEST","message":"bad index"}}),
        json!({"type":"scheduleOk","scheduleId":"sch-1","id":"job-9"}),
        json!({"type":"scheduleErr","scheduleId":"sch-1","error":{"code":"BAD_REQUEST","message":"bad cron"}}),
        json!({"type":"scheduleAck","scheduleId":"sch-1","ok":true}),
        json!({"type":"scheduleAck","scheduleId":"sch-1","ok":false,"error":{"code":"NOT_FOUND","message":"missing job"}}),
        json!({"type":"listSchedulesOk","scheduleId":"sch-1","schedules":[]}),
        json!({"type":"startWorkflowOk","workflowId":"wf-1","info":{"id":"wf1","name":"drip","status":"pending","currentStep":0,"stepCount":2,"attempts":0,"sleepUntil":123,"createdAt":1,"updatedAt":2}}),
        json!({"type":"startWorkflowErr","workflowId":"wf-2","error":{"code":"BAD_REQUEST","message":"bad spec"}}),
        json!({"type":"workflowAck","workflowId":"wf-3","ok":true}),
        json!({"type":"workflowAck","workflowId":"wf-4","ok":false,"error":{"code":"NOT_FOUND","message":"no such run"}}),
        json!({"type":"listWorkflowsOk","workflowId":"wf-5","workflows":[]}),
        json!({"type":"pong"}),
    ];
    for raw in cases {
        let msg: ServerMessage = serde_json::from_value(raw.clone()).unwrap();
        let back = serde_json::to_value(&msg).unwrap();
        assert_eq!(back, raw);
    }
}

// ── subscription-state / routing logic (no live server) ─────────────────
//
// `apply_server_message` operates on the shared sub maps and a local pending
// map; exercise it directly to cover QueryUpdate, SubscribeErr, MutateOk/Err.

fn rig_with_sub() -> (Arc<ClientInner>, watch::Receiver<Snapshot>) {
    rig_with_sub_and_config(Config::default())
}

fn rig_with_sub_and_config(config: Config) -> (Arc<ClientInner>, watch::Receiver<Snapshot>) {
    let (cmd_tx, _cmd_rx) = mpsc::unbounded_channel::<Cmd>();
    let (status_tx, _status_rx) = watch::channel(ClientStatus::default());
    let inner = Arc::new(ClientInner {
        url: "ws://h".into(),
        db: "d".into(),
        config,
        get_token: Box::new(|| Box::pin(async { None })),
        cmd_tx,
        status_tx,
        subs: Mutex::new(SubMaps::default()),
        presence: Mutex::new(PresenceMaps::default()),
        feeds: Mutex::new(FeedMaps::default()),
        generation: Arc::new(AtomicU64::new(0)),
        closed: Arc::new(AtomicBool::new(false)),
        sub_counter: AtomicU64::new(1),
        feed_counter: AtomicU64::new(1),
        mut_counter: AtomicU64::new(1),
        sched_counter: AtomicU64::new(1),
        wf_counter: AtomicU64::new(1),
    });
    let query = q("items");
    let (tx, rx) = watch::channel(Snapshot::Pending);
    let sub = Arc::new(SubState {
        query_id: "sub-1".into(),
        query: query.clone(),
        tx,
        refcount: AtomicU64::new(1),
        optimistic: Mutex::new(OptimisticState::default()),
    });
    {
        let mut maps = inner.subs.lock().unwrap();
        maps.by_key.insert(canonical_key(&query), sub);
        maps.by_id.insert("sub-1".into(), canonical_key(&query));
    }
    (inner, rx)
}

/// Lock the maps and grab the lone seeded sub's `Arc`. Helper for tests that
/// need to seed `optimistic` state before delivering a server message.
fn only_sub(maps: &SubMaps) -> Arc<SubState> {
    maps.by_key
        .values()
        .next()
        .expect("rig seeds one sub")
        .clone()
}

#[test]
fn query_update_delivers_value() {
    let (inner, rx) = rig_with_sub();
    let mut queues = PendingQueues::default();
    apply_server_message(
        &inner,
        ServerMessage::QueryUpdate {
            query_id: "sub-1".into(),
            result: json!([{"_id":"a"}]),
        },
        &mut queues,
    );
    assert!(matches!(rx.borrow().clone(), Snapshot::Value(_)));
}

#[test]
fn subscribe_err_routes_error_and_removes() {
    let (inner, rx) = rig_with_sub();
    let mut queues = PendingQueues::default();
    apply_server_message(
        &inner,
        ServerMessage::SubscribeErr {
            query_id: "sub-1".into(),
            error: RtDbError::new(ErrorCode::BadRequest, "bad index"),
        },
        &mut queues,
    );
    assert!(matches!(rx.borrow().clone(), Snapshot::Error(_)));
    let maps = inner.subs.lock().unwrap();
    assert!(maps.by_id.is_empty() && maps.by_key.is_empty());
    assert!(maps.overlays.is_empty());
}

#[tokio::test]
async fn mutate_ok_and_err_resolve_pending() {
    let (inner, _) = rig_with_sub();
    let mut queues = PendingQueues::default();
    let (tx_ok, rx_ok) = oneshot::channel();
    let (tx_err, rx_err) = oneshot::channel();
    queues.mutate.by_id.insert("mut-1".into(), tx_ok);
    queues.mutate.by_id.insert("mut-2".into(), tx_err);

    apply_server_message(
        &inner,
        ServerMessage::MutateOk {
            mut_id: "mut-1".into(),
            results: vec![json!({"id":"a"})],
        },
        &mut queues,
    );
    apply_server_message(
        &inner,
        ServerMessage::MutateErr {
            mut_id: "mut-2".into(),
            error: RtDbError::new(ErrorCode::NotFound, "x"),
        },
        &mut queues,
    );
    let ok = rx_ok.await.unwrap().unwrap();
    assert_eq!(ok.len(), 1);
    let err = rx_err.await.unwrap().unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
    assert!(queues.mutate.by_id.is_empty());
}

// ── presence routing ───────────────────────────────────────────────────
//
// Mirror how queryUpdate/SubscribeErr route to a per-id handler, but for
// presence: a presenceSnapshot fans out to the room's watch sender, and a
// presenceErr sets the Error state (mirrors the TS client dropping listeners).

/// Helper: build an inner with one seeded presence room and return the receiver.
fn rig_with_presence(room: &str) -> (Arc<ClientInner>, watch::Receiver<PresenceSnapshot>) {
    let (inner, _) = rig_with_sub();
    let (tx, rx) = watch::channel(PresenceSnapshot::Pending);
    let state = Arc::new(PresenceRoomState {
        room: room.to_string(),
        state: Mutex::new(None),
        last_seq: Mutex::new(None),
        tx,
    });
    inner
        .presence
        .lock()
        .unwrap()
        .by_room
        .insert(room.to_string(), state);
    (inner, rx)
}

/// Same as [`rig_with_presence`] but keeps the command receiver, so a test
/// can observe the `Cmd::PresenceResync` the gap detector queues.
fn rig_with_presence_and_cmds(
    room: &str,
) -> (
    Arc<ClientInner>,
    watch::Receiver<PresenceSnapshot>,
    mpsc::UnboundedReceiver<Cmd>,
) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<Cmd>();
    let (status_tx, _status_rx) = watch::channel(ClientStatus::default());
    let inner = Arc::new(ClientInner {
        url: "ws://h".into(),
        db: "d".into(),
        config: Config::default(),
        get_token: Box::new(|| Box::pin(async { None })),
        cmd_tx,
        status_tx,
        subs: Mutex::new(SubMaps::default()),
        presence: Mutex::new(PresenceMaps::default()),
        feeds: Mutex::new(FeedMaps::default()),
        generation: Arc::new(AtomicU64::new(0)),
        closed: Arc::new(AtomicBool::new(false)),
        sub_counter: AtomicU64::new(1),
        feed_counter: AtomicU64::new(1),
        mut_counter: AtomicU64::new(1),
        sched_counter: AtomicU64::new(1),
        wf_counter: AtomicU64::new(1),
    });
    let (tx, rx) = watch::channel(PresenceSnapshot::Pending);
    let state = Arc::new(PresenceRoomState {
        room: room.to_string(),
        state: Mutex::new(None),
        last_seq: Mutex::new(None),
        tx,
    });
    inner
        .presence
        .lock()
        .unwrap()
        .by_room
        .insert(room.to_string(), state);
    (inner, rx, cmd_rx)
}

fn presence_delta(
    room: &str,
    seq: u64,
    joined: Vec<crate::wire::PresenceMember>,
    left: Vec<String>,
    state_changed: Vec<crate::wire::PresenceMember>,
) -> ServerMessage {
    ServerMessage::PresenceDelta {
        room: room.to_string(),
        seq,
        joined,
        left,
        state_changed,
    }
}

fn presence_member(cid: &str) -> crate::wire::PresenceMember {
    crate::wire::PresenceMember {
        connection_id: cid.to_string(),
        user: crate::wire::AuthedUser {
            kind: crate::wire::UserKind::User,
            email: None,
            name: None,
            github_login: None,
            github_id: None,
            user_id: None,
        },
        state: json!({}),
    }
}

#[test]
fn presence_snapshot_routes_to_room() {
    let (inner, rx) = rig_with_presence("doc:1");
    let mut queues = PendingQueues::default();
    apply_server_message(
        &inner,
        ServerMessage::PresenceSnapshot {
            room: "doc:1".into(),
            members: vec![presence_member("c1"), presence_member("c2")],
        },
        &mut queues,
    );
    match rx.borrow().clone() {
        PresenceSnapshot::Members(m) => {
            assert_eq!(m.len(), 2);
            assert_eq!(m[0].connection_id, "c1");
            assert_eq!(m[1].connection_id, "c2");
        }
        other => panic!("expected Members, got {other:?}"),
    }
}

#[test]
fn presence_snapshot_ignored_for_unknown_room() {
    let (inner, rx) = rig_with_presence("doc:1");
    let mut queues = PendingQueues::default();
    // A snapshot for a room we haven't joined should not affect our receiver.
    apply_server_message(
        &inner,
        ServerMessage::PresenceSnapshot {
            room: "doc:other".into(),
            members: vec![presence_member("c9")],
        },
        &mut queues,
    );
    assert!(matches!(rx.borrow().clone(), PresenceSnapshot::Pending));
}

#[test]
fn presence_err_routes_error_to_room() {
    let (inner, rx) = rig_with_presence("doc:1");
    let mut queues = PendingQueues::default();
    apply_server_message(
        &inner,
        ServerMessage::PresenceErr {
            room: "doc:1".into(),
            error: RtDbError::new(ErrorCode::Forbidden, "presence not enabled"),
        },
        &mut queues,
    );
    match rx.borrow().clone() {
        PresenceSnapshot::Error(e) => {
            assert_eq!(e.code, ErrorCode::Forbidden);
        }
        other => panic!("expected Error, got {other:?}"),
    }
}

#[test]
fn presence_delta_folds_into_the_member_list() {
    let (inner, rx) = rig_with_presence("doc:1");
    let mut queues = PendingQueues::default();
    // Baseline snapshot (any seq may follow it — snapshots carry none).
    apply_server_message(
        &inner,
        ServerMessage::PresenceSnapshot {
            room: "doc:1".into(),
            members: vec![presence_member("c1"), presence_member("c2")],
        },
        &mut queues,
    );
    // c1's state changes, c2 leaves, c3 joins — one contiguous delta.
    let mut c1 = presence_member("c1");
    c1.state = json!({"cursor": 7});
    apply_server_message(
        &inner,
        presence_delta(
            "doc:1",
            4,
            vec![presence_member("c3")],
            vec!["c2".into()],
            vec![c1],
        ),
        &mut queues,
    );
    match rx.borrow().clone() {
        PresenceSnapshot::Members(m) => {
            let ids: Vec<&str> = m.iter().map(|m| m.connection_id.as_str()).collect();
            assert_eq!(ids, vec!["c1", "c3"], "fold applied left/joined in order");
            assert_eq!(m[0].state, json!({"cursor": 7}), "stateChanged replaced");
        }
        other => panic!("expected folded Members, got {other:?}"),
    }
    // A following contiguous delta applies on the new cursor.
    apply_server_message(
        &inner,
        presence_delta("doc:1", 5, vec![presence_member("c4")], vec![], vec![]),
        &mut queues,
    );
    match rx.borrow().clone() {
        PresenceSnapshot::Members(m) => assert_eq!(m.len(), 3),
        other => panic!("expected Members, got {other:?}"),
    }
}

#[test]
fn presence_delta_gap_discards_baseline_and_requests_resync() {
    let (inner, rx, mut cmd_rx) = rig_with_presence_and_cmds("doc:1");
    let mut queues = PendingQueues::default();
    apply_server_message(
        &inner,
        ServerMessage::PresenceSnapshot {
            room: "doc:1".into(),
            members: vec![presence_member("c1")],
        },
        &mut queues,
    );
    apply_server_message(
        &inner,
        presence_delta("doc:1", 2, vec![presence_member("c2")], vec![], vec![]),
        &mut queues,
    );
    // seq jumps 2 → 9: a delta was missed. The stale delta must NOT be
    // applied, and the client must queue a resync (re-join).
    apply_server_message(
        &inner,
        presence_delta("doc:1", 9, vec![presence_member("c9")], vec![], vec![]),
        &mut queues,
    );
    match rx.borrow().clone() {
        PresenceSnapshot::Members(m) => {
            let ids: Vec<&str> = m.iter().map(|m| m.connection_id.as_str()).collect();
            assert_eq!(
                ids,
                vec!["c1", "c2"],
                "stale-baseline delta must not be applied"
            );
        }
        other => panic!("expected untouched Members, got {other:?}"),
    }
    match cmd_rx.try_recv().expect("resync cmd queued") {
        Cmd::PresenceResync { room } => assert_eq!(room, "doc:1"),
        _ => panic!("expected PresenceResync"),
    }
    // The re-join forces a fresh snapshot; the cursor restarts blank so
    // the next delta (any seq) applies on the recovered baseline.
    apply_server_message(
        &inner,
        ServerMessage::PresenceSnapshot {
            room: "doc:1".into(),
            members: vec![presence_member("c1"), presence_member("c2")],
        },
        &mut queues,
    );
    apply_server_message(
        &inner,
        presence_delta("doc:1", 12, vec![], vec!["c2".into()], vec![]),
        &mut queues,
    );
    match rx.borrow().clone() {
        PresenceSnapshot::Members(m) => {
            let ids: Vec<&str> = m.iter().map(|m| m.connection_id.as_str()).collect();
            assert_eq!(ids, vec!["c1"], "post-resync delta applied");
        }
        other => panic!("expected recovered Members, got {other:?}"),
    }
}

#[test]
fn presence_delta_stale_duplicate_is_ignored() {
    let (inner, rx, mut cmd_rx) = rig_with_presence_and_cmds("doc:1");
    let mut queues = PendingQueues::default();
    apply_server_message(
        &inner,
        ServerMessage::PresenceSnapshot {
            room: "doc:1".into(),
            members: vec![presence_member("c1")],
        },
        &mut queues,
    );
    apply_server_message(
        &inner,
        presence_delta("doc:1", 3, vec![presence_member("c2")], vec![], vec![]),
        &mut queues,
    );
    // A redelivered seq 3 (and an older 2) must change nothing.
    apply_server_message(
        &inner,
        presence_delta("doc:1", 3, vec![presence_member("c2")], vec![], vec![]),
        &mut queues,
    );
    apply_server_message(
        &inner,
        presence_delta("doc:1", 2, vec![presence_member("c0")], vec![], vec![]),
        &mut queues,
    );
    match rx.borrow().clone() {
        PresenceSnapshot::Members(m) => {
            let ids: Vec<&str> = m.iter().map(|m| m.connection_id.as_str()).collect();
            assert_eq!(ids, vec!["c1", "c2"], "duplicates never re-applied");
        }
        other => panic!("expected Members, got {other:?}"),
    }
    assert!(
        cmd_rx.try_recv().is_err(),
        "no resync should be queued for stale deltas"
    );
}

#[test]
fn presence_delta_without_a_baseline_is_ignored() {
    let (inner, rx) = rig_with_presence("doc:1");
    let mut queues = PendingQueues::default();
    // Pending room: nothing to diff against — no apply, no resync, no panic.
    apply_server_message(
        &inner,
        presence_delta("doc:1", 1, vec![presence_member("c1")], vec![], vec![]),
        &mut queues,
    );
    assert!(matches!(rx.borrow().clone(), PresenceSnapshot::Pending));
    // Unknown room: same — silently dropped.
    apply_server_message(
        &inner,
        presence_delta("doc:other", 1, vec![presence_member("c1")], vec![], vec![]),
        &mut queues,
    );
    assert!(matches!(rx.borrow().clone(), PresenceSnapshot::Pending));
}

// ── optimistic-update wiring ───────────────────────────────────────────
//
// With `optimistic_updates` off (the default) every preceding test is
// unchanged. The three tests below exercise the apply/reconcile/rollback
// hooks that light up only with the flag on.

/// Apply hook: an insert transaction overlays the projected insert on the
/// receiver immediately and records the reverse-index entry.
#[test]
fn apply_optimistic_pushes_overlay_when_enabled() {
    let (inner, rx) = rig_with_sub_and_config(Config {
        optimistic_updates: true,
        ..Config::default()
    });
    // Seed the projection base (normally set by the first QueryUpdate).
    {
        let maps = inner.subs.lock().unwrap();
        let sub = only_sub(&maps);
        let mut opt = sub.optimistic.lock().unwrap();
        opt.server_last = Some(json!([{"_id":"a","_creationTime":1,"_version":1,"n":1}]));
    }
    let txn = crate::mutation::Mutation::new()
        .insert("items", json!({ "n": 2 }))
        .build();
    apply_optimistic(&inner, "mut-7", &txn);

    match rx.borrow().clone() {
        Snapshot::Value(v) => {
            let arr = v.as_array().expect("overlaid is array");
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[1]["n"], 2);
            assert!(
                arr[1]["_id"]
                    .as_str()
                    .unwrap()
                    .starts_with("__optimistic__")
            );
        }
        other => panic!("expected overlaid Value, got {other:?}"),
    }
    let maps = inner.subs.lock().unwrap();
    let qids = maps
        .overlays
        .get("mut-7")
        .expect("overlay reverse-index entry recorded");
    assert_eq!(qids.len(), 1);
    let sub = only_sub(&maps);
    let opt = sub.optimistic.lock().unwrap();
    assert!(opt.active, "overlay marked active");
}

/// Reconcile hook: a `QueryUpdate` clears an active overlay, pushes the
/// authoritative value, and records it as the new projection base.
#[test]
fn query_update_reconcile_clears_active_and_records_base() {
    let (inner, rx) = rig_with_sub();
    // Seed an active overlay with a stale base.
    {
        let maps = inner.subs.lock().unwrap();
        let sub = only_sub(&maps);
        let mut opt = sub.optimistic.lock().unwrap();
        opt.active = true;
        opt.server_last = Some(json!([{"_id":"old"}]));
    }
    let mut queues = PendingQueues::default();
    apply_server_message(
        &inner,
        ServerMessage::QueryUpdate {
            query_id: "sub-1".into(),
            result: json!([{"_id":"a"},{"_id":"b"}]),
        },
        &mut queues,
    );
    match rx.borrow().clone() {
        Snapshot::Value(v) => {
            assert_eq!(v.as_array().unwrap().len(), 2);
        }
        other => panic!("expected reconciled Value, got {other:?}"),
    }
    let maps = inner.subs.lock().unwrap();
    let sub = only_sub(&maps);
    let opt = sub.optimistic.lock().unwrap();
    assert!(!opt.active, "reconcile cleared active");
    assert_eq!(
        opt.server_last,
        Some(json!([{"_id":"a"},{"_id":"b"}])),
        "server_last updated to authoritative base"
    );
}

/// Rollback hook: a `MutateErr` reverts every overlay indexed under its
/// `mut_id` back to `server_last` and drops the reverse-index entry.
#[tokio::test]
async fn mutate_err_rolls_back_overlay() {
    let (inner, rx) = rig_with_sub();
    let base = json!([{"_id":"a","_creationTime":1,"_version":1,"n":1}]);
    // Seed: an overlay is active, server_last holds the pre-overlay base,
    // and the overlay is reverse-indexed under mut-1.
    {
        let mut maps = inner.subs.lock().unwrap();
        let sub = only_sub(&maps);
        {
            let mut opt = sub.optimistic.lock().unwrap();
            opt.active = true;
            opt.server_last = Some(base.clone());
        }
        // Simulate the in-flight overlaid value the receiver currently sees.
        let _ = sub
            .tx
            .send(Snapshot::Value(Box::new(json!([{"_id":"a","n":2}]))));
        let mut set = HashSet::new();
        set.insert("sub-1".to_string());
        maps.overlays.insert("mut-1".into(), set);
    }
    match rx.borrow().clone() {
        Snapshot::Value(v) => assert_eq!(v[0]["n"], 2, "receiver shows overlaid value"),
        other => panic!("expected overlaid Value pre-rollback, got {other:?}"),
    }

    let mut queues = PendingQueues::default();
    apply_server_message(
        &inner,
        ServerMessage::MutateErr {
            mut_id: "mut-1".into(),
            error: RtDbError::new(ErrorCode::Internal, "boom"),
        },
        &mut queues,
    );

    match rx.borrow().clone() {
        Snapshot::Value(v) => assert_eq!(*v, base, "receiver reverted to base"),
        other => panic!("expected reverted base, got {other:?}"),
    }
    let maps = inner.subs.lock().unwrap();
    assert!(maps.overlays.is_empty(), "reverse-index entry dropped");
    let sub = only_sub(&maps);
    let opt = sub.optimistic.lock().unwrap();
    assert!(!opt.active, "overlay inactive after rollback");
    assert_eq!(
        opt.server_last,
        Some(base),
        "server_last preserved for next projection"
    );
}

/// `MutateOk` drops the reverse-index entry without reverting: the
/// reconciling `QueryUpdate`(s) will supersede any overlay.
#[tokio::test]
async fn mutate_ok_drops_overlay_without_revert() {
    let (inner, rx) = rig_with_sub();
    let overlaid = json!([{"_id":"a","n":2}]);
    {
        let mut maps = inner.subs.lock().unwrap();
        let sub = only_sub(&maps);
        {
            let mut opt = sub.optimistic.lock().unwrap();
            opt.active = true;
            opt.server_last = Some(json!([{"_id":"a","n":1}]));
        }
        let _ = sub.tx.send(Snapshot::Value(Box::new(overlaid.clone())));
        let mut set = HashSet::new();
        set.insert("sub-1".to_string());
        maps.overlays.insert("mut-1".into(), set);
    }

    let mut queues = PendingQueues::default();
    let (reply_tx, _reply_rx) = oneshot::channel();
    queues.mutate.by_id.insert("mut-1".into(), reply_tx);
    apply_server_message(
        &inner,
        ServerMessage::MutateOk {
            mut_id: "mut-1".into(),
            results: vec![json!({"id":"a"})],
        },
        &mut queues,
    );

    // Receiver is unchanged — the overlaid value stays until the QueryUpdate
    // arrives to reconcile.
    match rx.borrow().clone() {
        Snapshot::Value(v) => assert_eq!(*v, overlaid, "MutateOk does not revert"),
        other => panic!("expected overlaid value to persist, got {other:?}"),
    }
    let maps = inner.subs.lock().unwrap();
    assert!(
        maps.overlays.is_empty(),
        "reverse-index dropped on MutateOk"
    );
    let sub = only_sub(&maps);
    let opt = sub.optimistic.lock().unwrap();
    assert!(
        opt.active,
        "overlay still active until QueryUpdate reconciles"
    );
}

// Mirror of `mutate_ok_and_err_resolve_pending` for the schedule track:
// `scheduleOk`/`scheduleErr` resolve the pending reply; a `scheduleAck`
// with `ok:false` surfaces the server's error envelope, while a BARE
// `ok:false` (no envelope) is the no-op signal — resolves Ack(false).
#[tokio::test]
async fn schedule_replies_resolve_pending() {
    let (inner, _) = rig_with_sub();
    let mut queues = PendingQueues::default();
    let (tx_ok, rx_ok) = oneshot::channel();
    let (tx_err, rx_err) = oneshot::channel();
    let (tx_ack_ok, rx_ack_ok) = oneshot::channel();
    let (tx_ack_err, rx_ack_err) = oneshot::channel();
    let (tx_list, rx_list) = oneshot::channel();
    let (tx_ack_noop, rx_ack_noop) = oneshot::channel();
    queues.schedule.by_id.insert("sch-1".into(), tx_ok);
    queues.schedule.by_id.insert("sch-2".into(), tx_err);
    queues.schedule.by_id.insert("sch-3".into(), tx_ack_ok);
    queues.schedule.by_id.insert("sch-4".into(), tx_ack_err);
    queues.schedule.by_id.insert("sch-5".into(), tx_list);
    queues.schedule.by_id.insert("sch-6".into(), tx_ack_noop);

    apply_server_message(
        &inner,
        ServerMessage::ScheduleOk {
            schedule_id: "sch-1".into(),
            id: "job-9".into(),
        },
        &mut queues,
    );
    apply_server_message(
        &inner,
        ServerMessage::ScheduleErr {
            schedule_id: "sch-2".into(),
            error: RtDbError::new(ErrorCode::BadRequest, "bad cron"),
        },
        &mut queues,
    );
    apply_server_message(
        &inner,
        ServerMessage::ScheduleAck {
            schedule_id: "sch-3".into(),
            ok: true,
            error: None,
        },
        &mut queues,
    );
    apply_server_message(
        &inner,
        ServerMessage::ScheduleAck {
            schedule_id: "sch-4".into(),
            ok: false,
            error: Some(RtDbError::new(ErrorCode::NotFound, "missing job")),
        },
        &mut queues,
    );
    // Bare ok:false (no error envelope) = unknown/terminal job no-op:
    // resolves Ack(false), it must not reject (workflow-ack parity).
    apply_server_message(
        &inner,
        ServerMessage::ScheduleAck {
            schedule_id: "sch-6".into(),
            ok: false,
            error: None,
        },
        &mut queues,
    );
    apply_server_message(
        &inner,
        ServerMessage::ListSchedulesOk {
            schedule_id: "sch-5".into(),
            schedules: vec![ScheduleInfo {
                id: "job-1".into(),
                kind: ScheduleKind::Cron,
                due_at: 9000,
                cron: Some("*/5 * * * *".into()),
                tz: None,
                every_ms: None,
                status: ScheduleStatus::Pending,
                last_error: None,
                created_at: 1000,
                fired_count: 0,
                external: false,
                missed_count: 0,
                last_missed_at: None,
            }],
        },
        &mut queues,
    );

    match rx_ok.await.unwrap().unwrap() {
        ScheduleOutcome::Id(id) => assert_eq!(id, "job-9"),
        other => panic!("expected Id, got {other:?}"),
    }
    let err = rx_err.await.unwrap().unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(matches!(
        rx_ack_ok.await.unwrap().unwrap(),
        ScheduleOutcome::Ack(true)
    ));
    let ack_err = rx_ack_err.await.unwrap().unwrap_err();
    assert_eq!(ack_err.code, ErrorCode::NotFound);
    assert!(matches!(
        rx_ack_noop.await.unwrap().unwrap(),
        ScheduleOutcome::Ack(false)
    ));
    match rx_list.await.unwrap().unwrap() {
        ScheduleOutcome::List(list) => {
            assert_eq!(list.len(), 1);
            assert_eq!(list[0].id, "job-1");
        }
        other => panic!("expected List, got {other:?}"),
    }
    assert!(queues.schedule.by_id.is_empty());
}

// Mirror of `schedule_replies_resolve_pending` for the workflow track
// (FM-29): `startWorkflowOk`/`startWorkflowErr`/`workflowAck`/
// `listWorkflowsOk` resolve the pending reply. Note the error routing: the
// frame vocabulary has no listWorkflowsErr — a failed `listWorkflows`
// arrives as `startWorkflowErr` carrying the LIST call's correlation id
// (mirrors listSchedules → scheduleErr).
#[tokio::test]
async fn workflow_replies_resolve_pending() {
    let (inner, _) = rig_with_sub();
    let mut queues = PendingQueues::default();
    let (tx_ok, rx_ok) = oneshot::channel();
    let (tx_err, rx_err) = oneshot::channel();
    let (tx_ack_ok, rx_ack_ok) = oneshot::channel();
    let (tx_ack_err, rx_ack_err) = oneshot::channel();
    let (tx_list, rx_list) = oneshot::channel();
    let (tx_list_err, rx_list_err) = oneshot::channel();
    let (tx_ack_noop, rx_ack_noop) = oneshot::channel();
    queues.workflow.by_id.insert("wf-1".into(), tx_ok);
    queues.workflow.by_id.insert("wf-2".into(), tx_err);
    queues.workflow.by_id.insert("wf-3".into(), tx_ack_ok);
    queues.workflow.by_id.insert("wf-4".into(), tx_ack_err);
    queues.workflow.by_id.insert("wf-5".into(), tx_list);
    queues.workflow.by_id.insert("wf-6".into(), tx_list_err);
    queues.workflow.by_id.insert("wf-7".into(), tx_ack_noop);

    let info = WorkflowInfo {
        id: "wf1".into(),
        name: "drip".into(),
        status: WorkflowStatus::Pending,
        current_step: 0,
        step_count: 2,
        attempts: 0,
        sleep_until: None,
        last_error: None,
        waiting_for: None,
        waited_since: None,
        created_at: 1,
        updated_at: 2,
        started_at: None,
        finished_at: None,
    };
    apply_server_message(
        &inner,
        ServerMessage::StartWorkflowOk {
            workflow_id: "wf-1".into(),
            info: info.clone(),
        },
        &mut queues,
    );
    apply_server_message(
        &inner,
        ServerMessage::StartWorkflowErr {
            workflow_id: "wf-2".into(),
            error: RtDbError::new(ErrorCode::BadRequest, "bad spec"),
        },
        &mut queues,
    );
    apply_server_message(
        &inner,
        ServerMessage::WorkflowAck {
            workflow_id: "wf-3".into(),
            ok: true,
            error: None,
        },
        &mut queues,
    );
    apply_server_message(
        &inner,
        ServerMessage::WorkflowAck {
            workflow_id: "wf-4".into(),
            ok: false,
            error: Some(RtDbError::new(ErrorCode::NotFound, "no such run")),
        },
        &mut queues,
    );
    apply_server_message(
        &inner,
        ServerMessage::ListWorkflowsOk {
            workflow_id: "wf-5".into(),
            workflows: vec![info],
        },
        &mut queues,
    );
    // The binding ruling: a failed list arrives as startWorkflowErr with
    // the list call's correlation id.
    apply_server_message(
        &inner,
        ServerMessage::StartWorkflowErr {
            workflow_id: "wf-6".into(),
            error: RtDbError::new(ErrorCode::Internal, "list failed"),
        },
        &mut queues,
    );
    // Bare ok:false (no error envelope) = unknown/terminal run no-op:
    // resolves Ack(false), it must not reject.
    apply_server_message(
        &inner,
        ServerMessage::WorkflowAck {
            workflow_id: "wf-7".into(),
            ok: false,
            error: None,
        },
        &mut queues,
    );

    match rx_ok.await.unwrap().unwrap() {
        WorkflowOutcome::Info(info) => assert_eq!(info.id, "wf1"),
        other => panic!("expected Info, got {other:?}"),
    }
    let err = rx_err.await.unwrap().unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(matches!(
        rx_ack_ok.await.unwrap().unwrap(),
        WorkflowOutcome::Ack(true)
    ));
    assert!(matches!(
        rx_ack_noop.await.unwrap().unwrap(),
        WorkflowOutcome::Ack(false)
    ));
    let ack_err = rx_ack_err.await.unwrap().unwrap_err();
    assert_eq!(ack_err.code, ErrorCode::NotFound);
    match rx_list.await.unwrap().unwrap() {
        WorkflowOutcome::List(list) => {
            assert_eq!(list.len(), 1);
            assert_eq!(list[0].id, "wf1");
        }
        other => panic!("expected List, got {other:?}"),
    }
    let list_err = rx_list_err.await.unwrap().unwrap_err();
    assert_eq!(list_err.code, ErrorCode::Internal);
    assert!(queues.workflow.by_id.is_empty());
}

#[test]
fn maybe_unsubscribe_removes_at_zero() {
    let (inner, _) = rig_with_sub();
    // refcount starts at 1; one decrement → zero → removed.
    maybe_unsubscribe(&inner, "sub-1");
    let maps = inner.subs.lock().unwrap();
    assert!(maps.by_id.is_empty() && maps.by_key.is_empty());
}

#[test]
fn liveness_times_out_after_window_without_pong() {
    let mut l = Liveness::new(Duration::from_millis(10));
    assert!(!l.timed_out());
    l.last_pong = Instant::now() - Duration::from_millis(100);
    assert!(l.timed_out());
    l.note_pong();
    assert!(!l.timed_out());
}
