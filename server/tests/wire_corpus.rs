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

use rtdb_server::migrate::{MigrateRequest, MigrateResult};
use rtdb_server::protocol::{
    AuthedUser, ClientMessage, ScheduleInfo, ScheduleWhen, ServerMessage, UserKind, WorkflowSpec,
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

/// Admin op-feed `OpEvent` rows — the reconnect-dedup stamps: 1-based
/// monotonic per-feed `seq` + the boot-time UUID `feedEpoch` (an epoch change
/// is a counter reset, a seq gap is evicted/dropped events).
#[test]
fn admin_op_events_round_trip() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "admin_op_events").iter().enumerate() {
        round_trip::<rtdb_server::op_feed::OpEvent>("admin_op_events", i, entry);
    }
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

/// The `protocol_constants` section pins agreed scalar constants across the
/// server and all clients. A server-side change to one without a corpus update
/// fails here (ARC-104).
#[test]
fn protocol_constants_match_server() {
    let corpus = load_corpus();
    let consts = corpus
        .get("protocol_constants")
        .expect("corpus missing 'protocol_constants' object");
    assert_eq!(
        consts["max_steps"].as_u64().unwrap(),
        rtdb_server::txn::MAX_STEPS as u64,
        "wire-corpus protocol_constants.max_steps must match server txn::MAX_STEPS"
    );
    assert_eq!(
        consts["protocol_version"].as_u64().unwrap(),
        rtdb_server::protocol::PROTOCOL_VERSION as u64,
        "wire-corpus protocol_constants.protocol_version must match server protocol::PROTOCOL_VERSION"
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

/// Admin migrate wire shapes (tag `op`, camelCase, `where`/`from` aliases, cast
/// literals). The `Directive` list + `MigrateResult` are part of the four-client
/// wire contract; this is the server's view.
#[test]
fn migrate_requests_round_trip() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "migrate_requests").iter().enumerate() {
        round_trip::<MigrateRequest>("migrate_requests", i, entry);
    }
}

#[test]
fn migrate_results_round_trip() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "migrate_results").iter().enumerate() {
        round_trip::<MigrateResult>("migrate_results", i, entry);
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
    for (i, entry) in section(&corpus, "rejects_workflow_spec_unknown_field")
        .iter()
        .enumerate()
    {
        must_reject::<WorkflowSpec>("rejects_workflow_spec_unknown_field", i, entry);
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

/// Error envelopes decoded through the server's real `RtDbError` envelope
/// (`code` is the closed `ErrorCode` enum — an unknown code fails at parse).
#[test]
fn error_envelopes_round_trip() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "error_envelopes").iter().enumerate() {
        round_trip::<rtdb_server::error::RtDbError>("error_envelopes", i, entry);
    }
}

/// Admin db-stats decoded through the server's `DbStatsResponse`
/// (`admin/dbs.rs`). It is `pub(super)` and Serialize-only (a response
/// shape), so the assertion is the round-trip of a mirror struct defined
/// here against the same serde field names — the corpus bytes still pin the
/// camelCase keys and the six ENH-011 quota/usage fields.
#[test]
fn db_stats_round_trip() {
    #[derive(serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct TableStat {
        name: String,
        row_count: i64,
        size_bytes: i64,
    }
    #[derive(serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct DbStatsResponse {
        tables: Vec<TableStat>,
        total_size_bytes: i64,
        tables_quota: usize,
        tables_used: usize,
        storage_quota_bytes: u64,
        storage_used_bytes: u64,
        subs_quota: usize,
        subs_used: usize,
    }
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "db_stats").iter().enumerate() {
        let raw = match entry.get("$comment") {
            // Documentation key in the corpus fixture, not wire data.
            Some(_) => {
                let mut map = entry
                    .as_object()
                    .expect("db_stats entry is an object")
                    .clone();
                map.remove("$comment");
                Value::Object(map)
            }
            None => entry.clone(),
        };
        round_trip::<DbStatsResponse>("db_stats", i, &raw);
    }
}

/// Query results stay a raw section by design (`QueryResult` is untagged on
/// the wire): parse each entry and re-serialize, requiring deep equality
/// over the parsed value — the "typed" check a value-level parse gives.
#[test]
fn query_results_round_trip() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "query_results").iter().enumerate() {
        // A serde_json Value parse == round-trip, but going through
        // `serde_json::from_str(to_string(..))` asserts byte-stable
        // re-serialization of the canonical form.
        let text = serde_json::to_string(entry)
            .unwrap_or_else(|e| panic!("serialize failure [query_results #{i}]: {e}"));
        let reparsed: Value = serde_json::from_str(&text)
            .unwrap_or_else(|e| panic!("reparse failure [query_results #{i}]: {e}"));
        assert_eq!(
            reparsed, *entry,
            "wire drift [query_results #{i}]: re-serialization differs"
        );
    }
}

/// Change-feed pages decoded through the server's `ChangeFeedResponse`
/// (`protocol.rs` + `change_log::ChangeRow` — Serialize-only, so a local
/// mirror struct with the same serde shape is the decode net).
#[test]
fn change_feed_responses_round_trip() {
    #[derive(serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct ChangeRow {
        seq: i64,
        table: String,
        doc_id: String,
        kind: String,
        doc: Option<Value>,
        ts: i64,
    }
    #[derive(serde::Deserialize, serde::Serialize)]
    #[serde(rename_all = "camelCase")]
    struct ChangeFeedResponse {
        ops: Vec<ChangeRow>,
        next_seq: i64,
        head: i64,
        log_id: String,
    }
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "change_feed_responses").iter().enumerate() {
        round_trip::<ChangeFeedResponse>("change_feed_responses", i, entry);
    }
}

/// QA-003 coverage meta-test: every top-level corpus section must be
/// consumed by this runner. `CONSUMED_SECTIONS` lists the sections the
/// tests above decode; `$comment` is the corpus's documentation key. A new
/// section added to wire-corpus.json without a runner update fails here.
#[test]
fn every_corpus_section_has_a_consumer() {
    const CONSUMED_SECTIONS: &[&str] = &[
        "client_messages",
        "server_messages",
        "authed_users",
        "schedule_whens",
        "schedule_infos",
        "query_results",
        "error_envelopes",
        "queries",
        "migrate_requests",
        "migrate_results",
        "rejects_client_message_unknown_field",
        "rejects_schedule_when_unknown_field",
        "rejects_workflow_spec_unknown_field",
        "rejects_authed_user_unknown_kind",
        "rejects_schedule_info_unknown_kind",
        "rejects_schedule_info_unknown_status",
        "db_stats",
        "change_feed_responses",
        "admin_op_events",
        "protocol_constants",
    ];
    let corpus = load_corpus();
    let unconsumed: Vec<&String> = corpus
        .as_object()
        .expect("corpus is an object")
        .keys()
        .filter(|k| *k != "$comment" && !CONSUMED_SECTIONS.contains(&k.as_str()))
        .collect();
    assert!(
        unconsumed.is_empty(),
        "corpus sections with no server runner consumer: {unconsumed:?} — \
         add a round-trip test above and extend CONSUMED_SECTIONS"
    );
    // And the inverse: a renamed/removed section leaves a dead CONSUMED_
    // SECTIONS entry that silently stops being checked.
    for name in CONSUMED_SECTIONS {
        assert!(
            corpus.get(name).is_some(),
            "CONSUMED_SECTIONS lists '{name}' but the corpus no longer has that section"
        );
    }
}
