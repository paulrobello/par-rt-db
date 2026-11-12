//! Wire protocol — the source of truth for the client/server vocabulary.
//!
//! The WS client/server message envelopes (`ClientMessage`/`ServerMessage`),
//! their payload types, and the version constants are defined once in
//! `par_rt_db_core::envelope` (ARC-008) and re-exported here at their
//! historical paths. This module additionally defines the schedule-claim,
//! workflow-outcome, and change-feed HTTP shapes consumed by the WS handler
//! (`ws`) and the one-shot HTTP handler (`http_api`). The Rust client no
//! longer hand-mirrors the envelopes (it consumes the same core types); the
//! five hand-writing client SDKs (`ts-client`, `python-client`,
//! `swift-client`, `go-client`, `kotlin-client`) still mirror this vocabulary
//! verbatim: the serde tags and field casing are load-bearing and
//! deliberately non-uniform, so any change in core must be reflected in all
//! six implementations.

use crate::change_log::ChangeRow;
use crate::dsl::Transaction;
use crate::error::RtDbError;

/// `PROTOCOL_VERSION`/`PRESENCE_DELTA_MIN_VERSION` are now
/// `par_rt_db_core::envelope` constants (ARC-008), re-exported here at their
/// historical path so every existing `crate::protocol::{..}` call site keeps
/// resolving. Mirrored byte-for-byte in all six client SDKs' wire modules.
pub use par_rt_db_core::envelope::{PRESENCE_DELTA_MIN_VERSION, PROTOCOL_VERSION};

/// `ClientMessage` is now a `par_rt_db_core::envelope` type (ARC-008 — the
/// WS envelope vocabulary is single-sourced in core, the same pattern as the
/// ARC-004 txn vocabulary), re-exported here at its historical path so every
/// existing `crate::protocol::ClientMessage` call site keeps resolving.
pub use par_rt_db_core::envelope::ClientMessage;

/// Full WS server vocabulary — now `par_rt_db_core::envelope`'s generic
/// `ServerMessage<E, C>` (ARC-008), instantiated here with the server's error
/// envelope and change-feed op types; the variant fields keep their exact
/// serde shapes. Construction and pattern sites through
/// `crate::protocol::ServerMessage` keep working via the alias.
pub type ServerMessage = par_rt_db_core::envelope::ServerMessage<RtDbError, ChangeRow>;

/// `UserKind`, `AuthedUser`, and `PresenceMember` are now
/// `par_rt_db_core::envelope` types (ARC-008), re-exported here at their
/// historical paths so every existing `crate::protocol::{..}` call site keeps
/// resolving. Mirrored byte-for-byte in `rust-client/src/wire.rs` (single-
/// sourced) and the other five clients' wire modules (hand-mirrored).
pub use par_rt_db_core::envelope::{AuthedUser, PresenceMember, UserKind};

/// `ScheduleWhen`/`ScheduleKind` are now `par_rt_db_core::mutation` types
/// (ARC-004 follow-up — both were verified byte-identical against
/// `rust-client/src/wire.rs`, contradicting this card's original premise that
/// `ScheduleKind` was server-only), re-exported here at their historical path
/// so every existing `crate::protocol::{ScheduleWhen, ScheduleKind}` call site
/// keeps resolving. Mirrored byte-for-byte in `ts-client/src/protocol.ts`.
pub use par_rt_db_core::mutation::{ScheduleKind, ScheduleWhen};

/// `ScheduleStatus`/`ScheduleInfo` are now `par_rt_db_core::mutation` types
/// (ARC-004 follow-up — both were verified byte-identical against
/// `rust-client/src/wire.rs`), re-exported here at their historical path so
/// every existing `crate::protocol::{ScheduleStatus, ScheduleInfo}` call site
/// keeps resolving. `scheduler` re-exports `ScheduleInfo` for its `list`
/// return type. Mirrored byte-for-byte in `ts-client/src/protocol.ts`.
pub use par_rt_db_core::mutation::{ScheduleInfo, ScheduleStatus};

/// One externally-claimed job returned by `POST /api/schedule/claim`. The
/// `lease_generation` is the per-job monotonic fencing token: it increments on
/// every claim (including re-claims after lease expiry), and the worker's
/// complete/retry/fail calls are rejected `CONFLICT` once a newer generation
/// exists. `txn` is the full declarative transaction the worker executes
/// externally; `cron`/`everyMs` ride along for recurring jobs so a `complete`
/// can advance the job to its next due instant.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClaimedSchedule {
    /// The claimed job's id.
    pub id: String,
    /// One-shot, cron, or interval.
    pub kind: ScheduleKind,
    /// When the job came due, epoch ms.
    pub due_at: i64,
    /// The declarative transaction to execute externally.
    pub txn: Transaction,
    /// The cron expression, for cron jobs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    /// IANA timezone for cron evaluation, when the job declares one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tz: Option<String>,
    /// The fixed recurrence in ms, for interval jobs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub every_ms: Option<i64>,
    /// Per-job monotonic fencing token assigned atomically by the claim.
    pub lease_generation: i64,
    /// Lease expiry instant, epoch ms — past this the job is re-claimable by
    /// another worker (or another claim by the same worker).
    pub lease_deadline_ms: i64,
}

/// `StepRetry`/`AwaitSignalSpec`/`WorkflowStepSpec`/`WorkflowSpec`/
/// `WorkflowStatus` are now `par_rt_db_core::mutation` types (ARC-004
/// follow-up — `WorkflowStatus` was verified byte-identical against
/// `rust-client/src/wire.rs`, contradicting this card's original premise that
/// it was server-only), re-exported here at their historical path so every
/// existing `crate::protocol::{..}` call site keeps resolving.
pub use par_rt_db_core::mutation::{
    AwaitSignalSpec, StepRetry, WorkflowSpec, WorkflowStatus, WorkflowStepSpec,
};

/// Terminal record for one step: completed successfully, or exhausted its
/// retries (`status: failed`). Individual retried attempts are NOT recorded —
/// the `attempts` count on the entry (and on the row) carries them.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StepOutcome {
    pub step_index: u32,
    pub status: OutcomeStatus,
    pub attempts: u32,
    pub at: i64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum OutcomeStatus {
    Success,
    Failed,
}

/// One page of the durable per-db change feed (`GET /api/db/{db}/changes`).
/// `ops` carries rows strictly after the request's `since` cursor, oldest
/// first; `nextSeq` is the cursor to resume from — the last returned op's seq
/// ONLY on a full page, otherwise the log's end (a short page means the log
/// is exhausted, and `nextSeq = since` would livock a filtered consumer).
/// `head` is the log's current end and `logId` the log's identity, so a
/// consumer can detect a db dropped and recreated under the same name (seqs
/// restart at 0; the logId does not match). See the change-feed design spec.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ChangeFeedResponse {
    pub ops: Vec<crate::change_log::ChangeRow>,
    pub next_seq: i64,
    pub head: i64,
    pub log_id: String,
}

/// `WorkflowInfo` is now a `par_rt_db_core::envelope` type (ARC-008 — the
/// `StartWorkflowOk`/`ListWorkflowsOk` frame payload is part of the envelope
/// vocabulary), re-exported here at its historical path so every existing
/// `crate::protocol::WorkflowInfo` call site keeps resolving.
pub use par_rt_db_core::envelope::WorkflowInfo;

/// `GET .../{id}` shape: the info row plus the per-step outcome trail.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct WorkflowInfoFull {
    #[serde(flatten)]
    pub info: WorkflowInfo,
    pub step_outcomes: Vec<StepOutcome>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_query() -> crate::query::Query {
        serde_json::from_value(serde_json::json!({"table": "workItems"})).expect("query")
    }

    fn sample_txn() -> Transaction {
        Transaction { steps: vec![] }
    }

    // Client tags/fields exactly match the wire vocabulary consumed by
    // Task 9's WS handler and the TS client.
    #[test]
    fn client_message_wire_tags_and_fields() {
        assert_eq!(
            serde_json::to_value(ClientMessage::Auth {
                token: Some("t".to_string()),
                db: "d".to_string(),
                protocol_version: None,
            })
            .unwrap(),
            serde_json::json!({"type": "auth", "token": "t", "db": "d"})
        );
        // SEC-001 phase 2: a tokenless Auth (cookie-mode) serializes without the
        // `token` field and round-trips back to `None`.
        assert_eq!(
            serde_json::to_value(ClientMessage::Auth {
                token: None,
                db: "d".to_string(),
                protocol_version: None,
            })
            .unwrap(),
            serde_json::json!({"type": "auth", "db": "d"})
        );
        let parsed: ClientMessage =
            serde_json::from_value(serde_json::json!({"type": "auth", "db": "d"})).unwrap();
        match parsed {
            ClientMessage::Auth {
                token,
                db,
                protocol_version,
            } => {
                assert!(token.is_none());
                assert_eq!(db, "d");
                assert!(protocol_version.is_none());
            }
            other => panic!("unexpected variant: {other:?}"),
        }
        // ARC-013: `protocolVersion` round-trips when present.
        assert_eq!(
            serde_json::to_value(ClientMessage::Auth {
                token: Some("t".to_string()),
                db: "d".to_string(),
                protocol_version: Some(1),
            })
            .unwrap(),
            serde_json::json!({"type": "auth", "token": "t", "db": "d", "protocolVersion": 1})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Subscribe {
                query_id: "q1".to_string(),
                query: Box::new(sample_query())
            })
            .unwrap()["type"],
            serde_json::json!("subscribe")
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Unsubscribe {
                query_id: "q1".to_string()
            })
            .unwrap(),
            serde_json::json!({"type": "unsubscribe", "queryId": "q1"})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Mutate {
                mut_id: "m1".to_string(),
                idempotency_key: None,
                txn: sample_txn()
            })
            .unwrap(),
            serde_json::json!({"type": "mutate", "mutId": "m1", "txn": {"steps": []}})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Mutate {
                mut_id: "m1".to_string(),
                idempotency_key: Some("key1".to_string()),
                txn: sample_txn()
            })
            .unwrap(),
            serde_json::json!({
                "type": "mutate",
                "mutId": "m1",
                "idempotencyKey": "key1",
                "txn": {"steps": []}
            })
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::Ping).unwrap(),
            serde_json::json!({"type": "ping"})
        );
    }

    #[test]
    fn client_message_rejects_unknown_fields() {
        let raw = serde_json::json!({"type": "auth", "token": "t", "db": "d", "bogus": true});
        assert!(serde_json::from_value::<ClientMessage>(raw).is_err());
    }

    #[test]
    fn server_message_wire_tags_and_fields() {
        assert_eq!(
            serde_json::to_value(ServerMessage::AuthOk {
                user: AuthedUser {
                    kind: UserKind::User,
                    email: Some("a@b.com".to_string()),
                    name: None,
                    github_login: None,
                    github_id: None,
                    user_id: None,
                },
                protocol_version: None,
            })
            .unwrap()["type"],
            serde_json::json!("authOk")
        );
        // ARC-013: `protocolVersion` is omitted when the client didn't signal
        // support, present (as the server's `PROTOCOL_VERSION`) when it did.
        assert_eq!(
            serde_json::to_value(ServerMessage::AuthOk {
                user: AuthedUser {
                    kind: UserKind::User,
                    email: Some("a@b.com".to_string()),
                    name: None,
                    github_login: None,
                    github_id: None,
                    user_id: None,
                },
                protocol_version: None,
            })
            .unwrap()
            .get("protocolVersion"),
            None
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::AuthOk {
                user: AuthedUser {
                    kind: UserKind::User,
                    email: Some("a@b.com".to_string()),
                    name: None,
                    github_login: None,
                    github_id: None,
                    user_id: None,
                },
                protocol_version: Some(PROTOCOL_VERSION),
            })
            .unwrap()["protocolVersion"],
            serde_json::json!(PROTOCOL_VERSION)
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::QueryUpdate {
                query_id: "q1".to_string(),
                result: serde_json::json!([])
            })
            .unwrap(),
            serde_json::json!({"type": "queryUpdate", "queryId": "q1", "result": []})
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::MutateOk {
                mut_id: "m1".to_string(),
                results: vec![]
            })
            .unwrap(),
            serde_json::json!({"type": "mutateOk", "mutId": "m1", "results": []})
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::MutateErr {
                mut_id: "m1".to_string(),
                error: RtDbError::not_found("x")
            })
            .unwrap()["type"],
            serde_json::json!("mutateErr")
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::SubscribeErr {
                query_id: "q1".to_string(),
                error: RtDbError::bad_request("bad index")
            })
            .unwrap(),
            serde_json::json!({
                "type": "subscribeErr",
                "queryId": "q1",
                "error": {"code": "BAD_REQUEST", "message": "bad index"}
            })
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::Pong).unwrap(),
            serde_json::json!({"type": "pong"})
        );
    }

    #[test]
    fn schedule_when_wire_tags() {
        assert_eq!(
            serde_json::to_value(ScheduleWhen::AfterMs { ms: 5 }).unwrap(),
            serde_json::json!({"type": "afterMs", "ms": 5})
        );
        assert_eq!(
            serde_json::to_value(ScheduleWhen::RunAt { ms: 9 }).unwrap(),
            serde_json::json!({"type": "runAt", "ms": 9})
        );
        assert_eq!(
            serde_json::to_value(ScheduleWhen::Cron {
                expr: "*/5 * * * *".to_string(),
                tz: None,
            })
            .unwrap(),
            serde_json::json!({"type": "cron", "expr": "*/5 * * * *"})
        );
        assert_eq!(
            serde_json::to_value(ScheduleWhen::Interval { every_ms: 5_000 }).unwrap(),
            serde_json::json!({"type": "interval", "everyMs": 5000})
        );
    }

    #[test]
    fn schedule_message_wire_tags() {
        let s = serde_json::to_value(ClientMessage::Schedule {
            schedule_id: "s1".to_string(),
            when: ScheduleWhen::AfterMs { ms: 100 },
            txn: sample_txn(),
            external: None,
        })
        .unwrap();
        assert_eq!(s["type"], serde_json::json!("schedule"));
        assert_eq!(s["scheduleId"], serde_json::json!("s1"));
        assert_eq!(s["when"], serde_json::json!({"type": "afterMs", "ms": 100}));

        let ok = serde_json::to_value(ServerMessage::ScheduleOk {
            schedule_id: "s1".to_string(),
            id: "job-9".to_string(),
        })
        .unwrap();
        assert_eq!(
            ok,
            serde_json::json!({"type": "scheduleOk", "scheduleId": "s1", "id": "job-9"})
        );

        let ack = serde_json::to_value(ServerMessage::ScheduleAck {
            schedule_id: "s1".to_string(),
            ok: true,
            error: None,
        })
        .unwrap();
        assert_eq!(ack["type"], serde_json::json!("scheduleAck"));
        // `error` is skipped on the wire when `None`.
        assert!(ack.get("error").is_none());
    }

    #[test]
    fn client_message_round_trips_through_json() {
        let msg = ClientMessage::Subscribe {
            query_id: "q1".to_string(),
            query: Box::new(sample_query()),
        };
        let value = serde_json::to_value(&msg).unwrap();
        let restored: ClientMessage = serde_json::from_value(value).unwrap();
        assert!(matches!(restored, ClientMessage::Subscribe { query_id, .. } if query_id == "q1"));
    }

    #[test]
    fn presence_client_message_wire_tags() {
        // presence: optional state omitted when None
        assert_eq!(
            serde_json::to_value(ClientMessage::Presence {
                room: "doc:1".to_string(),
                state: None,
            })
            .unwrap(),
            serde_json::json!({"type": "presence", "room": "doc:1"})
        );
        // presence: state present when Some
        assert_eq!(
            serde_json::to_value(ClientMessage::Presence {
                room: "doc:1".to_string(),
                state: Some(serde_json::json!({"x": 3, "y": 4})),
            })
            .unwrap(),
            serde_json::json!({"type": "presence", "room": "doc:1", "state": {"x": 3, "y": 4}})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::PresenceState {
                room: "doc:1".to_string(),
                state: serde_json::json!({"typing": true}),
                ttl_ms: None,
            })
            .unwrap(),
            serde_json::json!({"type": "presenceState", "room": "doc:1", "state": {"typing": true}})
        );
        assert_eq!(
            serde_json::to_value(ClientMessage::LeavePresence {
                room: "doc:1".to_string(),
            })
            .unwrap(),
            serde_json::json!({"type": "leavePresence", "room": "doc:1"})
        );
    }

    #[test]
    fn presence_state_ttl_ms_wire_tag() {
        // ttlMs omitted when None (backward compatible — unchanged shape)
        assert_eq!(
            serde_json::to_value(ClientMessage::PresenceState {
                room: "doc:1".to_string(),
                state: serde_json::json!({"typing": true}),
                ttl_ms: None,
            })
            .unwrap(),
            serde_json::json!({"type": "presenceState", "room": "doc:1", "state": {"typing": true}})
        );
        // ttlMs present when Some
        assert_eq!(
            serde_json::to_value(ClientMessage::PresenceState {
                room: "doc:1".to_string(),
                state: serde_json::json!({"typing": true}),
                ttl_ms: Some(3000),
            })
            .unwrap(),
            serde_json::json!({"type": "presenceState", "room": "doc:1", "state": {"typing": true}, "ttlMs": 3000})
        );
        // and it deserializes back
        let parsed: ClientMessage = serde_json::from_str(
            r#"{"type":"presenceState","room":"doc:1","state":{},"ttlMs":500}"#,
        )
        .unwrap();
        match parsed {
            ClientMessage::PresenceState { ttl_ms, .. } => assert_eq!(ttl_ms, Some(500)),
            _ => panic!("expected PresenceState"),
        }
    }

    #[test]
    fn presence_server_message_wire_tags() {
        let member = PresenceMember {
            connection_id: "42".to_string(),
            user: AuthedUser {
                kind: UserKind::User,
                email: Some("a@b.com".to_string()),
                name: None,
                github_login: None,
                github_id: None,
                user_id: None,
            },
            state: serde_json::json!({"x": 1}),
        };
        assert_eq!(
            serde_json::to_value(ServerMessage::PresenceSnapshot {
                room: "doc:1".to_string(),
                members: vec![member.clone()],
            })
            .unwrap(),
            serde_json::json!({
                "type": "presenceSnapshot",
                "room": "doc:1",
                "members": [{
                    "connectionId": "42",
                    // AuthedUser has no `skip_serializing_if` on `name`, so
                    // `None` serializes as `null` (pre-existing wire shape
                    // mirrored across all four clients).
                    "user": {"kind": "user", "email": "a@b.com", "name": null},
                    "state": {"x": 1}
                }]
            })
        );
        assert_eq!(
            serde_json::to_value(ServerMessage::PresenceErr {
                room: "doc:1".to_string(),
                error: RtDbError::forbidden("presence not enabled"),
            })
            .unwrap()["type"],
            serde_json::json!("presenceErr")
        );
    }

    #[test]
    fn authed_user_user_id_wire_shape() {
        // Additive identity contract: a validated user principal exposes the
        // stable opaque `userId` (rtdb_auth.users.id); a machine token omits
        // the key entirely. Never the raw provider subject.
        let user = AuthedUser {
            kind: UserKind::User,
            email: Some("a@b.com".to_string()),
            name: None,
            github_login: None,
            github_id: None,
            user_id: Some("u_123".to_string()),
        };
        assert_eq!(
            serde_json::to_value(&user).unwrap(),
            serde_json::json!({
                "kind": "user",
                "email": "a@b.com",
                "name": null,
                "userId": "u_123"
            })
        );
        let machine = AuthedUser {
            kind: UserKind::Machine,
            email: None,
            name: None,
            github_login: None,
            github_id: None,
            user_id: None,
        };
        assert_eq!(
            serde_json::to_value(&machine).unwrap(),
            serde_json::json!({"kind": "machine", "email": null, "name": null})
        );
    }

    #[test]
    fn workflow_spec_wire_shape() {
        let spec = serde_json::from_value::<WorkflowSpec>(serde_json::json!({
            "name": "drip",
            "steps": [
                { "txn": { "steps": [ { "op": "insert", "table": "t", "doc": {} } ] } },
                { "txn": { "steps": [] },
                  "retry": { "maxAttempts": 5, "initialRetryMs": 500, "maxRetryMs": 2000 },
                  "sleepBeforeMs": 86400000 }
            ]
        }))
        .unwrap();
        assert_eq!(spec.steps.len(), 2);
        assert_eq!(spec.steps[1].sleep_before_ms, Some(86_400_000));
        let retry = spec.steps[1].retry.unwrap();
        assert_eq!(
            (
                retry.max_attempts,
                retry.initial_retry_ms,
                retry.max_retry_ms
            ),
            (5, 500, 2000)
        );
        // Omitted retry defaults on deserialize:
        assert!(
            serde_json::from_value::<StepRetry>(serde_json::json!({"maxAttempts": 2}))
                .unwrap()
                .initial_retry_ms
                == 1_000
        );
        // Round-trip: absent optionals are SKIPPED on serialize (corpus parity).
        let v = serde_json::to_value(&spec).unwrap();
        assert!(v["steps"][0].get("retry").is_none());
        assert!(v["steps"][0].get("sleepBeforeMs").is_none());
    }

    #[test]
    fn await_signal_step_wire_shape() {
        let spec = serde_json::from_value::<WorkflowSpec>(serde_json::json!({
            "name": "gate",
            "steps": [ { "awaitSignal": { "name": "approve", "timeoutMs": 3_600_000 } } ]
        }))
        .expect("parse awaitSignal spec");
        let step = &spec.steps[0];
        assert!(step.txn.is_none());
        let sig = step.await_signal.as_ref().expect("awaitSignal present");
        assert_eq!(sig.name, "approve");
        assert_eq!(sig.timeout_ms, Some(3_600_000));
        // Round-trip omits absent optionals:
        let v = serde_json::to_value(&spec).unwrap();
        assert!(v["steps"][0].get("txn").is_none());
        assert!(v["steps"][0].get("sleepBeforeMs").is_none());
        // deny_unknown_fields:
        assert!(
            serde_json::from_value::<WorkflowSpec>(serde_json::json!({
                "name": "g", "steps": [ { "awaitSignal": { "name": "a", "bogus": 1 } } ]
            }))
            .is_err()
        );
    }

    #[test]
    fn waiting_status_wire_is_snake_case() {
        assert_eq!(
            serde_json::to_value(WorkflowStatus::Waiting).unwrap(),
            serde_json::json!("waiting")
        );
        assert_eq!(
            "waiting".parse::<WorkflowStatus>().unwrap(),
            WorkflowStatus::Waiting
        );
    }

    #[test]
    fn workflow_info_wait_fields_omit_when_absent() {
        let info = serde_json::from_value::<WorkflowInfo>(serde_json::json!({
            "id": "w1", "name": "n", "status": "waiting", "currentStep": 1,
            "stepCount": 2, "attempts": 0, "createdAt": 1, "updatedAt": 2,
            "waitingFor": "approve", "waitedSince": 1234
        }))
        .expect("info");
        assert_eq!(info.waiting_for.as_deref(), Some("approve"));
        assert_eq!(info.waited_since, Some(1234));
        let v = serde_json::to_value(WorkflowInfo {
            waiting_for: None,
            waited_since: None,
            ..info
        })
        .unwrap();
        assert!(v.get("waitingFor").is_none() && v.get("waitedSince").is_none());
    }

    #[test]
    fn signal_workflow_frame_wire_shape() {
        let m = serde_json::to_value(ClientMessage::SignalWorkflow {
            workflow_id: "c1".into(),
            id: "w1".into(),
            name: "approve".into(),
            payload: Some(serde_json::json!({"ok": true})),
        })
        .unwrap();
        assert_eq!(m["type"], "signalWorkflow");
        assert_eq!(m["name"], "approve");
        assert_eq!(m["payload"]["ok"], true);
    }

    #[test]
    fn workflow_status_wire_is_snake_case() {
        assert_eq!(
            serde_json::to_value(WorkflowStatus::Pending).unwrap(),
            serde_json::json!("pending")
        );
        assert_eq!(
            "failed".parse::<WorkflowStatus>().unwrap(),
            WorkflowStatus::Failed
        );
        assert!("bogus".parse::<WorkflowStatus>().is_err());
    }

    #[test]
    fn workflow_info_wire_shape() {
        let info = serde_json::from_value::<WorkflowInfo>(serde_json::json!({
            "id": "wf1", "name": "drip", "status": "pending",
            "currentStep": 0, "stepCount": 3, "attempts": 0,
            "sleepUntil": 123, "createdAt": 1, "updatedAt": 2
        }))
        .unwrap();
        assert_eq!(info.step_count, 3);
        assert!(info.last_error.is_none());
        let v = serde_json::to_value(&info).unwrap();
        assert!(v.get("lastError").is_none() && v.get("finishedAt").is_none());
    }

    fn sample_workflow_spec() -> WorkflowSpec {
        serde_json::from_value(serde_json::json!({
            "name": "drip",
            "steps": [ { "txn": { "steps": [] } } ]
        }))
        .expect("sample workflow spec")
    }

    fn sample_workflow_info() -> WorkflowInfo {
        WorkflowInfo {
            id: "wf1".to_string(),
            name: "drip".to_string(),
            status: WorkflowStatus::Pending,
            current_step: 0,
            step_count: 2,
            attempts: 0,
            sleep_until: Some(123),
            last_error: None,
            waiting_for: None,
            waited_since: None,
            created_at: 1,
            updated_at: 2,
            started_at: None,
            finished_at: None,
        }
    }

    // FM-29 WS frame vocabulary: tags/fields exactly match the wire shapes the
    // WS handlers and the client SDKs speak (spec §Wire protocol, WS frames).
    #[test]
    fn workflow_frame_wire_shapes() {
        let m = serde_json::to_value(ClientMessage::StartWorkflow {
            workflow_id: "c1".to_string(),
            spec: sample_workflow_spec(),
        })
        .unwrap();
        assert_eq!(m["type"], serde_json::json!("startWorkflow"));
        assert_eq!(m["workflowId"], serde_json::json!("c1"));
        assert_eq!(m["spec"]["name"], serde_json::json!("drip"));

        assert_eq!(
            serde_json::to_value(ClientMessage::CancelWorkflow {
                workflow_id: "c2".to_string(),
                id: "wf9".to_string(),
            })
            .unwrap(),
            serde_json::json!({"type": "cancelWorkflow", "workflowId": "c2", "id": "wf9"})
        );

        // status omitted when None, snake_case string when Some, and the
        // filtered frame parses back.
        assert_eq!(
            serde_json::to_value(ClientMessage::ListWorkflows {
                workflow_id: "c3".to_string(),
                status: None,
            })
            .unwrap(),
            serde_json::json!({"type": "listWorkflows", "workflowId": "c3"})
        );
        let m = serde_json::to_value(ClientMessage::ListWorkflows {
            workflow_id: "c3".to_string(),
            status: Some(WorkflowStatus::Failed),
        })
        .unwrap();
        assert_eq!(m["status"], serde_json::json!("failed"));
        match serde_json::from_value::<ClientMessage>(serde_json::json!({
            "type": "listWorkflows", "workflowId": "c3", "status": "failed"
        }))
        .unwrap()
        {
            ClientMessage::ListWorkflows {
                workflow_id,
                status,
            } => {
                assert_eq!(workflow_id, "c3");
                assert_eq!(status, Some(WorkflowStatus::Failed));
            }
            other => panic!("unexpected variant: {other:?}"),
        }

        let m = serde_json::to_value(ServerMessage::StartWorkflowOk {
            workflow_id: "c1".to_string(),
            info: sample_workflow_info(),
        })
        .unwrap();
        assert_eq!(m["type"], serde_json::json!("startWorkflowOk"));
        assert_eq!(m["workflowId"], serde_json::json!("c1"));
        assert_eq!(m["info"]["id"], serde_json::json!("wf1"));

        let m = serde_json::to_value(ServerMessage::StartWorkflowErr {
            workflow_id: "c1".to_string(),
            error: RtDbError::bad_request("bad spec"),
        })
        .unwrap();
        assert_eq!(m["type"], serde_json::json!("startWorkflowErr"));
        assert_eq!(m["error"]["code"], serde_json::json!("BAD_REQUEST"));

        let m = serde_json::to_value(ServerMessage::WorkflowAck {
            workflow_id: "c1".to_string(),
            ok: true,
            error: None,
        })
        .unwrap();
        assert_eq!(m["type"], serde_json::json!("workflowAck"));
        // `error` is skipped on the wire when the ack is clean.
        assert!(m.get("error").is_none());
        let m = serde_json::to_value(ServerMessage::WorkflowAck {
            workflow_id: "c1".to_string(),
            ok: false,
            error: Some(RtDbError::not_found("no such run")),
        })
        .unwrap();
        assert_eq!(m["ok"], serde_json::json!(false));
        assert_eq!(m["error"]["code"], serde_json::json!("NOT_FOUND"));

        let m = serde_json::to_value(ServerMessage::ListWorkflowsOk {
            workflow_id: "c4".to_string(),
            workflows: vec![sample_workflow_info()],
        })
        .unwrap();
        assert_eq!(m["type"], serde_json::json!("listWorkflowsOk"));
        assert_eq!(m["workflows"][0]["id"], serde_json::json!("wf1"));
    }

    #[test]
    fn workflow_status_round_trips_all_variants() {
        let all = [
            (WorkflowStatus::Pending, "pending"),
            (WorkflowStatus::Running, "running"),
            (WorkflowStatus::Waiting, "waiting"),
            (WorkflowStatus::Success, "success"),
            (WorkflowStatus::Failed, "failed"),
            (WorkflowStatus::Cancelled, "cancelled"),
        ];
        for (variant, wire) in all {
            assert_eq!(
                serde_json::to_value(variant).unwrap(),
                serde_json::json!(wire)
            );
            assert_eq!(wire.parse::<WorkflowStatus>().unwrap(), variant);
            assert_eq!(variant.as_wire_str(), wire);
        }
    }

    #[test]
    fn workflow_info_full_flatten_round_trip() {
        let full = WorkflowInfoFull {
            info: sample_workflow_info(),
            step_outcomes: vec![StepOutcome {
                step_index: 0,
                status: OutcomeStatus::Success,
                attempts: 1,
                at: 99,
                error: None,
                signal: None,
            }],
        };
        let v = serde_json::to_value(&full).unwrap();
        // The flattened info keys land at the TOP level (no "info" wrapper),
        // alongside stepOutcomes.
        assert!(v.get("info").is_none());
        assert_eq!(v["id"], serde_json::json!("wf1"));
        assert_eq!(v["status"], serde_json::json!("pending"));
        assert_eq!(v["stepCount"], serde_json::json!(2));
        assert_eq!(v["stepOutcomes"][0]["stepIndex"], serde_json::json!(0));
        let restored: WorkflowInfoFull = serde_json::from_value(v).unwrap();
        assert_eq!(restored.info.id, "wf1");
        assert_eq!(restored.info.step_count, 2);
        assert_eq!(restored.step_outcomes.len(), 1);

        // deny_unknown_fields: a bogus key is rejected on the plain info
        // projection AND on the flattened full shape.
        let bad_info = serde_json::json!({
            "id": "wf1", "name": "drip", "status": "pending",
            "currentStep": 0, "stepCount": 1, "attempts": 0,
            "createdAt": 1, "updatedAt": 2, "bogus": true
        });
        assert!(serde_json::from_value::<WorkflowInfo>(bad_info).is_err());
        let bad_full = serde_json::json!({
            "id": "wf1", "name": "drip", "status": "pending",
            "currentStep": 0, "stepCount": 1, "attempts": 0,
            "createdAt": 1, "updatedAt": 2, "stepOutcomes": [], "bogus": true
        });
        assert!(serde_json::from_value::<WorkflowInfoFull>(bad_full).is_err());
    }

    // `maxAttempts` is the one required StepRetry field — omitting it is a
    // deserialize error, while initialRetryMs/maxRetryMs default (3.2 §retry).
    #[test]
    fn step_retry_requires_max_attempts() {
        assert!(
            serde_json::from_value::<StepRetry>(serde_json::json!({
                "initialRetryMs": 100, "maxRetryMs": 200
            }))
            .is_err(),
            "a retry object without maxAttempts must not deserialize"
        );
        let r: StepRetry = serde_json::from_value(serde_json::json!({ "maxAttempts": 4 })).unwrap();
        assert_eq!(
            (r.max_attempts, r.initial_retry_ms, r.max_retry_ms),
            (4, 1_000, 60_000)
        );
    }
}
