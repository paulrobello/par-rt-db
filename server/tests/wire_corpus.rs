//! Cross-client wire-parity corpus test (ARC-008).
//!
//! Loads `wire-corpus/wire-corpus.json` (the shared canonical corpus at the
//! repo root) and asserts every entry round-trips byte-identically through the
//! server's wire types. Each entry is the raw JSON object as it appears on the
//! wire; we parse -> serialize -> deep-compare to the input. Drift here means
//! a wire-mirror invariant broke.
//!
//! The `rejects_*` sections assert the strict shapes (`deny_unknown_fields`
//! and the typed enums added in ARC-004) reject malformed payloads.
//!
//! This is the server's view; the TS, Rust, and Python clients each have an
//! equivalent test reading the same corpus.

use rtdb_server::protocol::{
    AuthedUser, ClientMessage, ScheduleInfo, ScheduleWhen, ServerMessage, UserKind,
};
use serde_json::{Value, json};

fn load_corpus() -> Value {
    // include_str! resolves relative to this source file (server/tests/), so
    // the test is independent of cargo test's runtime CWD (which is server/,
    // not server/tests/).
    serde_json::from_str(include_str!("../../wire-corpus/wire-corpus.json"))
        .unwrap_or_else(|e| panic!("parse wire-corpus.json: {e}"))
}

fn section<'a>(corpus: &'a Value, name: &str) -> &'a Vec<Value> {
    corpus
        .get(name)
        .and_then(Value::as_array)
        .unwrap_or_else(|| panic!("corpus missing array section '{name}'"))
}

/// Parse `input` as `T`, serialize the parsed value back to JSON, and assert
/// deep equality with `input`. Records the entry name in the panic message.
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

/// Asserts `input` does NOT parse as `T` (used for the `rejects_*` sections).
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

/// `ClientMessage` is `deny_unknown_fields`. So is `ScheduleWhen`. The corpus's
/// `rejects_*` sections assert a malformed payload is rejected.
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

/// The ARC-004 enums (`UserKind`, `ScheduleKind`, `ScheduleStatus`) must reject
/// any value outside the closed domain. A pre-ARC-004 `String` field silently
/// accepted these — the typing is the fix.
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

/// Spot-check the three new enums serialize to the exact snake_case bytes the
/// pre-ARC-004 `String` form produced. If this fails, the wire bytes changed
/// and already-deployed clients break.
#[test]
fn arc004_enums_serialize_byte_identical_to_prior_strings() {
    // AuthedUser.kind
    assert_eq!(serde_json::to_value(UserKind::User).unwrap(), json!("user"));
    assert_eq!(
        serde_json::to_value(UserKind::Machine).unwrap(),
        json!("machine")
    );

    // An AuthedUser with kind=User serializes kind as the bare string "user".
    let u = AuthedUser {
        kind: UserKind::User,
        email: None,
        name: None,
        github_login: None,
        github_id: None,
    };
    assert_eq!(serde_json::to_value(&u).unwrap()["kind"], json!("user"));

    // Round-trip an AuthedUser with the machine variant.
    let wire = json!({"kind": "machine", "email": null, "name": null});
    let parsed: AuthedUser = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(parsed.kind, UserKind::Machine);
    assert_eq!(serde_json::to_value(&parsed).unwrap(), wire);
}
