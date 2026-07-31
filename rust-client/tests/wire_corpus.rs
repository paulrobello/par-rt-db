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

/// Admin migrate wire shapes — `Directive` list + `MigrateResult`. The migrate
/// types live under `wire::admin` (feature `admin`); the directive enum carries
/// the load-bearing `op` tag, camelCase, `where`/`from` aliases, and the cast
/// literals, so this is the drift net for the fourth implementation of that
/// contract. `MigrateRequestOwned` is the owned, deserialize-able counterpart of
/// the borrowed `MigrateRequest` (same wire shape).
#[cfg(feature = "admin")]
#[test]
fn migrate_requests_round_trip() {
    use par_rt_db_client::wire::admin::{Directive, MigrateRequestOwned};
    // `Directive` is a serde tagged enum; `MigrateRequestOwned` wraps
    // `Vec<Directive>`. Both round-trip through the corpus entry.
    for (i, entry) in section(&load_corpus(), "migrate_requests")
        .iter()
        .enumerate()
    {
        let parsed: MigrateRequestOwned =
            serde_json::from_value(entry.clone()).unwrap_or_else(|e| {
                panic!("parse failure [migrate_requests #{i}]: {e}\n  input: {entry}")
            });
        let dumped = serde_json::to_value(&parsed).unwrap_or_else(|e| {
            panic!("serialize failure [migrate_requests #{i}]: {e}\n  input: {entry}")
        });
        assert_eq!(
            dumped, *entry,
            "wire drift [migrate_requests #{i}]:\n  parsed-then-serialized: {dumped}\n  corpus input:         {entry}"
        );
        // Also round-trip each directive through the enum directly, catching a
        // variant-level drift even if the wrapper happened to mask it.
        for (j, d) in parsed.directives.iter().enumerate() {
            let dv = serde_json::to_value(d).unwrap_or_else(|e| {
                panic!("directive serialize failure [migrate_requests #{i}.{j}]: {e}")
            });
            let _: &Directive =
                &serde_json::from_value::<Directive>(dv.clone()).unwrap_or_else(|e| {
                    panic!("directive parse failure [migrate_requests #{i}.{j}]: {e}")
                });
        }
    }
}

#[cfg(feature = "admin")]
#[test]
fn migrate_results_round_trip() {
    use par_rt_db_client::wire::admin::MigrateResult;
    for (i, entry) in section(&load_corpus(), "migrate_results")
        .iter()
        .enumerate()
    {
        round_trip::<MigrateResult>("migrate_results", i, entry);
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
