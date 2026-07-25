//! Cross-client wire-parity corpus test (ARC-008) — rust-client view.
//!
//! Loads the shared `wire-corpus/wire-corpus.json` at the repo root and
//! asserts every entry round-trips byte-identically through the rust-client's
//! wire types (the third implementation of the protocol; the server and TS /
//! Python clients have equivalent tests reading the same corpus). Drift here
//! means the rust-client drifted from the wire contract.
//!
//! The new ARC-004 enums (`UserKind`, `ScheduleKind`, `ScheduleStatus`) are
//! referenced through `par_rt_db_client::wire::` because they are not (yet)
//! re-exported from the crate root.

use par_rt_db_client::wire::{
    AuthedUser, ClientMessage, ScheduleInfo, ScheduleKind, ScheduleStatus, ScheduleWhen,
    ServerMessage, UserKind,
};
use serde_json::{Value, json};

fn load_corpus() -> Value {
    // include_str! resolves relative to this source file (rust-client/tests/),
    // so the test is independent of cargo test's runtime CWD (rust-client/).
    serde_json::from_str(include_str!("../../wire-corpus/wire-corpus.json"))
        .unwrap_or_else(|e| panic!("parse wire-corpus.json: {e}"))
}

fn section<'a>(corpus: &'a Value, name: &str) -> &'a Vec<Value> {
    corpus
        .get(name)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("corpus missing array section '{name}'"))
}

fn round_trip<T>(name: &str, idx: usize, input: &Value)
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    let parsed: T = serde_json::from_value(input.clone())
        .unwrap_or_else(|e| panic!("parse failure [{name} #{idx}]: {e}\n  input: {input}"));
    let dumped = serde_json::to_value(&parsed)
        .unwrap_or_else(|e| panic!("serialize failure [{name} #{idx}]: {e}\n  input: {input}"));
    assert_eq!(
        dumped, *input,
        "wire drift [{name} #{idx}]:\n  parsed-then-serialized: {dumped}\n  corpus input:         {input}"
    );
}

fn must_reject<T>(name: &str, idx: usize, input: &Value)
where
    T: serde::de::DeserializeOwned,
{
    let result = serde_json::from_value::<T>(input.clone());
    assert!(
        result.is_err(),
        "[{name} #{idx}] expected rejection but parsed successfully\n  input: {input}"
    );
}

#[test]
fn client_messages_round_trip() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "client_messages").iter().enumerate() {
        round_trip::<ClientMessage>("client_messages", i, entry);
    }
}

#[test]
fn server_messages_round_trip() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "server_messages").iter().enumerate() {
        round_trip::<ServerMessage>("server_messages", i, entry);
    }
}

#[test]
fn authed_users_round_trip() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "authed_users").iter().enumerate() {
        round_trip::<AuthedUser>("authed_users", i, entry);
    }
}

#[test]
fn schedule_whens_round_trip() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "schedule_whens").iter().enumerate() {
        round_trip::<ScheduleWhen>("schedule_whens", i, entry);
    }
}

#[test]
fn schedule_infos_round_trip() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "schedule_infos").iter().enumerate() {
        round_trip::<ScheduleInfo>("schedule_infos", i, entry);
    }
}

#[test]
fn rejects_unknown_fields() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "rejects_client_message_unknown_field")
        .iter()
        .enumerate()
    {
        must_reject::<ClientMessage>("rejects_client_message_unknown_field", i, entry);
    }
    for (i, entry) in section(&corpus, "rejects_schedule_when_unknown_field")
        .iter()
        .enumerate()
    {
        must_reject::<ScheduleWhen>("rejects_schedule_when_unknown_field", i, entry);
    }
}

#[test]
fn rejects_unknown_enum_values() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "rejects_authed_user_unknown_kind")
        .iter()
        .enumerate()
    {
        must_reject::<AuthedUser>("rejects_authed_user_unknown_kind", i, entry);
    }
    for (i, entry) in section(&corpus, "rejects_schedule_info_unknown_kind")
        .iter()
        .enumerate()
    {
        must_reject::<ScheduleInfo>("rejects_schedule_info_unknown_kind", i, entry);
    }
    for (i, entry) in section(&corpus, "rejects_schedule_info_unknown_status")
        .iter()
        .enumerate()
    {
        must_reject::<ScheduleInfo>("rejects_schedule_info_unknown_status", i, entry);
    }
}

#[test]
fn arc004_enums_round_trip_snake_case() {
    assert_eq!(serde_json::to_value(UserKind::User).unwrap(), json!("user"));
    assert_eq!(
        serde_json::to_value(UserKind::Machine).unwrap(),
        json!("machine")
    );
    assert_eq!(
        serde_json::to_value(ScheduleKind::Oneshot).unwrap(),
        json!("oneshot")
    );
    assert_eq!(
        serde_json::to_value(ScheduleKind::Cron).unwrap(),
        json!("cron")
    );
    assert_eq!(
        serde_json::to_value(ScheduleStatus::Pending).unwrap(),
        json!("pending")
    );
    assert_eq!(
        serde_json::to_value(ScheduleStatus::Running).unwrap(),
        json!("running")
    );
    assert_eq!(
        serde_json::to_value(ScheduleStatus::Paused).unwrap(),
        json!("paused")
    );
    assert_eq!(
        serde_json::to_value(ScheduleStatus::Error).unwrap(),
        json!("error")
    );
}
