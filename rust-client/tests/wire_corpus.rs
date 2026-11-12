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

#![allow(clippy::unwrap_used, clippy::expect_used)]
use par_rt_db_client::ErrorEnvelope;
use par_rt_db_client::Query;
use par_rt_db_client::wire::{
    AuthedUser, ChangeFeedResponse, ClientMessage, ScheduleInfo, ScheduleKind, ScheduleStatus,
    ScheduleWhen, ServerMessage, UserKind, WorkflowSpec,
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
fn change_feed_responses_round_trip() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "change_feed_responses").iter().enumerate() {
        round_trip::<ChangeFeedResponse>("change_feed_responses", i, entry);
    }
}

/// Error envelopes decoded through the client's real `ErrorEnvelope` type
/// (`code` is the closed `ErrorCode` enum — an unknown code fails at parse).
#[test]
fn error_envelopes_round_trip() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "error_envelopes").iter().enumerate() {
        round_trip::<ErrorEnvelope>("error_envelopes", i, entry);
    }
}

/// Admin db-stats decoded through the client's `DbStats` (feature `admin`;
/// camelCase + the six ENH-011 quota/usage fields). `DbStats` is
/// decode-only (`Deserialize`, no `Serialize` — ARC-130 response-shaped), so
/// the assertion is a strict typed decode of every entry; the `$comment`
/// documentation key the corpus annotates entry #0 with is stripped first.
#[cfg(feature = "admin")]
#[test]
fn db_stats_round_trip() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "db_stats").iter().enumerate() {
        let raw = match entry.get("$comment") {
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
        let parsed: par_rt_db_client::wire::admin::DbStats = serde_json::from_value(raw.clone())
            .unwrap_or_else(|e| panic!("parse failure [db_stats #{i}]: {e}\n  input: {raw}"));
        assert_eq!(
            parsed.total_size_bytes,
            raw["totalSizeBytes"].as_i64().unwrap_or(-1),
            "db_stats #{i}: totalSizeBytes decode drifted"
        );
    }
}

/// Admin op-feed `OpEvent` rows — the reconnect-dedup stamps (`seq` +
/// `feedEpoch`), mirroring the server runner's section.
#[cfg(feature = "admin")]
#[test]
fn admin_op_events_round_trip() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "admin_op_events").iter().enumerate() {
        round_trip::<par_rt_db_client::wire::admin::OpEvent>("admin_op_events", i, entry);
    }
}

/// The corpus `queries` section — embedded `Query` wire shapes covering
/// filter/search/vectorSearch/paginate, including FM-31's operator-syntax
/// search text and `snippet: true` (the operator syntax is plain `query`
/// string bytes with no new wire fields; `snippet` must serialize back onto
/// the terminal). `Query` lives in the query module and is re-exported from
/// the crate root; it carries `deny_unknown_fields`, so this doubles as the
/// rejection net for unknown query keys.
#[test]
fn queries_round_trip() {
    let corpus = load_corpus();
    for (i, entry) in section(&corpus, "queries").iter().enumerate() {
        round_trip::<Query>("queries", i, entry);
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

/// ENH-010 subscription-inspector wire shapes. Self-contained (not in the
/// shared corpus — these admin-only response types landed with the inspector)
/// so the round-trip still asserts the exact camelCase keys the server emits.
#[cfg(feature = "admin")]
#[test]
fn subscriptions_response_round_trip() {
    use par_rt_db_client::wire::admin::{
        DbSubCounters, SubscriptionInfo, SubscriptionsPrincipal, SubscriptionsResponse,
    };
    let input = json!({
        "subscriptions": [
            {
                "db": "app",
                "table": "workItems",
                "terminal": "collect",
                "readSetClass": "indexed",
                "principal": {"userId": "u1", "email": "a@b.com"}
            },
            {
                "db": "app",
                "table": "users",
                "terminal": "get",
                "readSetClass": "point",
                "principal": null
            }
        ],
        "subsRerunsTotal": 12,
        "subsSkipsPointTotal": 3,
        "subsSkipsIndexedTotal": 5,
        "subsSkipsOrderedTotal": 1,
        "subsMissedPushesTotal": 0,
        "perDb": [
            {
                "db": "app",
                "reruns": 12,
                "skipsPoint": 3,
                "skipsIndexed": 5,
                "skipsOrdered": 1,
                "missed": 0,
                "skips": 9,
                "rerunRatio": 0.5714285714285714
            }
        ]
    });
    let parsed: SubscriptionsResponse =
        serde_json::from_value(input.clone()).expect("parse SubscriptionsResponse");
    let dumped = serde_json::to_value(&parsed).expect("serialize SubscriptionsResponse");
    assert_eq!(dumped, input, "wire drift in SubscriptionsResponse");
    assert!(parsed.subscriptions[1].principal.is_none());
    assert_eq!(parsed.per_db[0].skips_indexed, 5);
    assert_eq!(parsed.per_db[0].skips, 9);
    assert_eq!(parsed.per_db[0].rerun_ratio, 12.0 / 21.0);

    // Nested-type camelCase, including the `null` principal on the wire. These
    // types are `#[non_exhaustive]` (ARC-130) — external crates can't construct
    // them via struct literal, so the round-trip is JSON-in → JSON-out, which
    // is exactly the wire-shape assertion this test cares about.
    let p: SubscriptionsPrincipal =
        serde_json::from_value(json!({"userId": "u", "email": null})).unwrap();
    assert_eq!(
        serde_json::to_value(&p).unwrap(),
        json!({"userId": "u", "email": null})
    );
    let info: SubscriptionInfo = serde_json::from_value(json!({
        "db": "d", "table": "t", "terminal": "count",
        "readSetClass": "point", "principal": null
    }))
    .unwrap();
    assert_eq!(
        serde_json::to_value(&info).unwrap(),
        json!({"db":"d","table":"t","terminal":"count","readSetClass":"point","principal":null})
    );
    let c: DbSubCounters = serde_json::from_value(json!({
        "db": "d", "reruns": 1,
        "skipsPoint": 2, "skipsIndexed": 3, "skipsOrdered": 4, "missed": 5,
        "skips": 9, "rerunRatio": 0.1
    }))
    .unwrap();
    assert_eq!(
        serde_json::to_value(&c).unwrap(),
        json!({"db":"d","reruns":1,"skipsPoint":2,"skipsIndexed":3,"skipsOrdered":4,"missed":5,"skips":9,"rerunRatio":0.1})
    );
}

/// FM-27 anon→real merge report wire shape. Self-contained (not in the
/// shared corpus — these admin-only response types landed with the merge)
/// so the round-trip still asserts the exact camelCase keys the server emits.
#[cfg(feature = "admin")]
#[test]
fn merge_report_round_trip() {
    use par_rt_db_client::wire::admin::{MergeConflict, MergeDbResult, MergeReport};
    let input = json!({
        "dbs": {
            "kanban": {
                "tables": {"notes": 2, "cursors": 1},
                "conflicts": [{"table": "notes", "id": "n7"}]
            },
            "demo": {"tables": {}, "conflicts": []}
        },
        "storageRepointed": 4,
        "sessionsRepointed": 1,
        "anonDeleted": true
    });
    let parsed: MergeReport = serde_json::from_value(input.clone()).expect("parse MergeReport");
    let dumped = serde_json::to_value(&parsed).expect("serialize MergeReport");
    assert_eq!(dumped, input, "wire drift in MergeReport");
    assert_eq!(parsed.dbs["kanban"].tables["notes"], 2);
    assert!(parsed.anon_deleted);

    // Nested-type camelCase (`table`/`id` are single words, so this is a
    // stability check more than a casing one).
    let c: MergeConflict = serde_json::from_value(json!({"table": "notes", "id": "n7"})).unwrap();
    assert_eq!(
        serde_json::to_value(&c).unwrap(),
        json!({"table": "notes", "id": "n7"})
    );
    let d: MergeDbResult = serde_json::from_value(json!({"tables": {}, "conflicts": []})).unwrap();
    assert_eq!(
        serde_json::to_value(&d).unwrap(),
        json!({"tables": {}, "conflicts": []})
    );
}

/// Confirms the `perDbSubs` field added to `MetricsSnapshot` (ENH-010)
/// deserializes under its camelCase key and defaults to empty when an older
/// server omits it. `MetricsSnapshot` is `Deserialize`-only (server emits it;
/// client never sends it), so this is a one-way parse check.
#[cfg(feature = "admin")]
#[test]
fn metrics_snapshot_deserializes_per_db_subs() {
    use par_rt_db_client::wire::admin::MetricsSnapshot;
    let base = || {
        json!({
            "queriesTotal": 0, "mutationsTotal": 0, "uploadsTotal": 0,
            "wsConnections": 0, "activeSubscriptions": 0,
            "poolSize": 0, "poolIdle": 0, "uptimeSeconds": 0,
            "queryLatency": {"p50": 0, "p95": 0, "p99": 0},
            "mutateLatency": {"p50": 0, "p95": 0, "p99": 0},
            "subscribeLatency": {"p50": 0, "p95": 0, "p99": 0}
        })
    };
    let mut with_subs = base();
    with_subs["perDbSubs"] = json!([{
        "db": "app", "reruns": 1, "skipsPoint": 2,
        "skipsIndexed": 3, "skipsOrdered": 4, "missed": 0
    }]);
    let m: MetricsSnapshot = serde_json::from_value(with_subs).expect("parse with perDbSubs");
    assert_eq!(m.per_db_subs.len(), 1);
    assert_eq!(m.per_db_subs[0].db, "app");
    assert_eq!(m.per_db_subs[0].skips_ordered, 4);

    // Older server that omits the field → empty vec, not an error.
    let older: MetricsSnapshot = serde_json::from_value(base()).expect("parse without perDbSubs");
    assert!(older.per_db_subs.is_empty());
}

/// Confirms the presence/quota/workflow fields added to `MetricsSnapshot`
/// (ENH-015 / ENH-011 / FM-29) deserialize under their camelCase keys and
/// default (0 / empty) when an older server omits them — the same tolerance
/// contract as `metrics_snapshot_deserializes_per_db_subs` above.
#[cfg(feature = "admin")]
#[test]
fn metrics_snapshot_deserializes_presence_quota_workflows() {
    use par_rt_db_client::wire::admin::MetricsSnapshot;
    let base = || {
        json!({
            "queriesTotal": 0, "mutationsTotal": 0, "uploadsTotal": 0,
            "wsConnections": 0, "activeSubscriptions": 0,
            "poolSize": 0, "poolIdle": 0, "uptimeSeconds": 0,
            "queryLatency": {"p50": 0, "p95": 0, "p99": 0},
            "mutateLatency": {"p50": 0, "p95": 0, "p99": 0},
            "subscribeLatency": {"p50": 0, "p95": 0, "p99": 0}
        })
    };
    let mut full = base();
    full["presenceRooms"] = json!(2);
    full["presenceSessions"] = json!(5);
    full["quotaRejectionsTablesTotal"] = json!(1);
    full["quotaRejectionsStorageTotal"] = json!(2);
    full["quotaRejectionsSubsTotal"] = json!(3);
    full["adminAuthFailuresTotal"] = json!(7);
    full["workflowStepsSuccessTotal"] = json!(10);
    full["workflowStepsRetryTotal"] = json!(2);
    full["workflowStepsFailTotal"] = json!(1);
    full["perDbQuota"] = json!([{"db": "app", "tables": 1, "storage": 0, "subs": 0}]);
    full["perDbWorkflows"] = json!([{
        "db": "app", "pending": 1, "running": 1, "waiting": 0,
        "success": 4, "failed": 0, "cancelled": 0
    }]);
    let m: MetricsSnapshot = serde_json::from_value(full).expect("parse with new fields");
    assert_eq!(m.presence_rooms, 2);
    assert_eq!(m.presence_sessions, 5);
    assert_eq!(m.quota_rejections_tables_total, 1);
    assert_eq!(m.quota_rejections_storage_total, 2);
    assert_eq!(m.quota_rejections_subs_total, 3);
    assert_eq!(m.admin_auth_failures_total, 7);
    assert_eq!(m.workflow_steps_success_total, 10);
    assert_eq!(m.workflow_steps_retry_total, 2);
    assert_eq!(m.workflow_steps_fail_total, 1);
    assert_eq!(m.per_db_quota.len(), 1);
    assert_eq!(m.per_db_quota[0].db, "app");
    assert_eq!(m.per_db_quota[0].tables, 1);
    assert_eq!(m.per_db_workflows.len(), 1);
    assert_eq!(m.per_db_workflows[0].db, "app");
    assert_eq!(m.per_db_workflows[0].success, 4);

    // Older server that omits the fields → defaults, not an error.
    let older: MetricsSnapshot = serde_json::from_value(base()).expect("parse without new fields");
    assert_eq!(older.presence_rooms, 0);
    assert_eq!(older.presence_sessions, 0);
    assert_eq!(older.quota_rejections_tables_total, 0);
    assert_eq!(older.admin_auth_failures_total, 0);
    assert_eq!(older.workflow_steps_fail_total, 0);
    assert!(older.per_db_quota.is_empty());
    assert!(older.per_db_workflows.is_empty());
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
    for (i, entry) in section(&corpus, "rejects_workflow_spec_unknown_field")
        .iter()
        .enumerate()
    {
        must_reject::<WorkflowSpec>("rejects_workflow_spec_unknown_field", i, entry);
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

/// ARC-104: `MAX_STEPS` is part of the four-client wire contract. The corpus
/// records the canonical agreed value; assert the rust-client's const matches,
/// so the next server change fails a test here unless the corpus (and every
/// client) is updated too. Gated on `in_memory` (where `MAX_STEPS` lives).
#[cfg(feature = "in_memory")]
#[test]
fn protocol_constants_max_steps_matches_corpus() {
    let corpus = load_corpus();
    let max_steps = corpus
        .get("protocol_constants")
        .and_then(|v| v.get("max_steps"))
        .and_then(Value::as_u64)
        .expect("corpus missing protocol_constants.max_steps");
    assert_eq!(
        max_steps as usize,
        par_rt_db_client::in_memory::MAX_STEPS,
        "MAX_STEPS drifted from wire-corpus protocol_constants.max_steps"
    );
    let protocol_version = corpus
        .get("protocol_constants")
        .and_then(|v| v.get("protocol_version"))
        .and_then(Value::as_u64)
        .expect("corpus missing protocol_constants.protocol_version");
    assert_eq!(
        protocol_version,
        par_rt_db_client::wire::PROTOCOL_VERSION as u64,
        "PROTOCOL_VERSION drifted from wire-corpus protocol_constants.protocol_version"
    );
}

/// ARC-017: `wire-corpus/error-codes.json` is the canonical
/// `{code, httpStatus}` table generated from the server's `ErrorCode` enum
/// (`server/src/error.rs::tests::error_codes_match_wire_corpus`). This asserts
/// every code the corpus lists is known to `par_rt_db_client::ErrorCode` (round
/// trips through serde) and that the client's closed set carries no code the
/// corpus doesn't — a code added to the server without a matching client
/// variant would otherwise fail silently. The closed set comes from
/// `ErrorCode::ALL`, which is compile-checked complete against the corpus via
/// the exhaustive match in `error::tests::error_codes_corpus_matches_enum`
/// (ARC-001 follow-up), so adding a variant without a corpus row is now a
/// compile error, not a silent omission.
#[test]
fn error_codes_known_to_rust_client() {
    use par_rt_db_client::ErrorCode;

    let all = ErrorCode::ALL;

    let corpus: Value = serde_json::from_str(include_str!("../../wire-corpus/error-codes.json"))
        .unwrap_or_else(|e| panic!("parse error-codes.json: {e}"));
    let mut corpus_codes: Vec<String> = corpus["codes"]
        .as_array()
        .expect("error-codes.json missing 'codes' array")
        .iter()
        .map(|e| e["code"].as_str().expect("entry.code").to_string())
        .collect();
    corpus_codes.sort();

    let mut known_codes: Vec<String> = all
        .iter()
        .map(|c| {
            serde_json::to_value(c)
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    known_codes.sort();

    assert_eq!(
        corpus_codes, known_codes,
        "rust-client ErrorCode drifted from wire-corpus/error-codes.json"
    );
}

// ---------------------------------------------------------------------------
// ENH-052 family 1 — spec-table corpus: the FieldType → Postgres column-type
// mapping (wire-corpus/spec/index-column-types.json). The fixture pins this
// engine's `index_column_type` port of server `schema::indexed_column_type`
// for every FieldType variant; `columnType` carries the server's raw Postgres
// string, which the engine's `PgType` enum maps back to. A mapping change
// regenerates the fixture in the same commit
// (`make gen-spec-index-column-types`).
#[cfg(feature = "in_memory")]
mod spec_corpus {
    use par_rt_db_client::in_memory::{PgType, index_column_type};
    use par_rt_db_client::schema::{FieldType, OnDeleteAction};

    use super::{Value, json};

    fn load_rows() -> Vec<Value> {
        let doc: Value = serde_json::from_str(include_str!(
            "../../wire-corpus/spec/index-column-types.json"
        ))
        .unwrap_or_else(|e| panic!("parse index-column-types.json: {e}"));
        doc["rows"]
            .as_array()
            .expect("spec corpus missing rows array")
            .clone()
    }

    fn raw_pg(pg: PgType) -> &'static str {
        match pg {
            PgType::Text => "text",
            PgType::Number => "double precision",
            PgType::Boolean => "boolean",
            PgType::Int64 => "bigint",
        }
    }

    /// One representative per FieldType variant this engine's
    /// `index_column_type` distinguishes, in the wire spelling (the corpus
    /// README's own-variant coverage rule: a variant without a fixture row
    /// fails).
    fn own_variants() -> Vec<FieldType> {
        fn lit(v: Value) -> FieldType {
            FieldType::Literal { value: v }
        }
        vec![
            FieldType::String,
            FieldType::Number,
            FieldType::Boolean,
            FieldType::Int64,
            FieldType::Id {
                table: "t".into(),
                on_delete: None,
            },
            FieldType::Id {
                table: "t".into(),
                on_delete: Some(OnDeleteAction::Cascade),
            },
            lit(json!("x")),
            lit(json!(42)),
            FieldType::Optional {
                inner: Box::new(FieldType::String),
            },
            FieldType::Union {
                variants: vec![lit(json!("a")), lit(json!("b"))],
            },
            FieldType::Union {
                variants: Vec::new(),
            },
            FieldType::Union {
                variants: vec![lit(json!("a")), FieldType::Number],
            },
            FieldType::Null,
            FieldType::Array {
                element: Box::new(FieldType::String),
            },
            FieldType::Object {
                fields: [("a".to_string(), FieldType::String)].into_iter().collect(),
            },
            FieldType::Bytes,
            FieldType::Any,
            FieldType::Record {
                value: Box::new(FieldType::String),
            },
            FieldType::Vector { dimensions: 3 },
        ]
    }

    #[test]
    fn every_own_variant_has_a_fixture_row() {
        let rows = load_rows();
        for own in own_variants() {
            let wire = serde_json::to_value(&own).unwrap();
            assert!(
                rows.iter().any(|row| row["fieldType"] == wire),
                "no fixture row for {wire}"
            );
        }
    }

    #[test]
    fn every_fixture_row_matches_this_engine() {
        for (i, row) in load_rows().into_iter().enumerate() {
            let ty: FieldType = serde_json::from_value(row["fieldType"].clone())
                .unwrap_or_else(|e| panic!("row {i}: decode fieldType: {e}"));
            let resolved = index_column_type(&ty);
            if row["error"].as_bool().unwrap_or(false) {
                assert!(
                    resolved.is_err(),
                    "row {i}: expected an error for a non-indexable type"
                );
                continue;
            }
            let resolved = resolved.unwrap_or_else(|e| panic!("row {i}: unexpected error: {e}"));
            assert_eq!(
                raw_pg(resolved.pg),
                row["columnType"].as_str().expect("row columnType"),
                "row {i} columnType"
            );
            assert_eq!(
                resolved.nullable,
                row["nullable"].as_bool().expect("row nullable"),
                "row {i} nullable"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// ENH-052 family 2 — spec-table corpus: computed-field stamping
// (wire-corpus/spec/computed-stamping.json). The fixture pins this engine's
// `stamp_computed` port of server `txn::stamp_computed` +
// `value_expr::eval_value_expr`: each row's wire schema fragment (`fields` +
// `computed`) is decoded through this crate's own serde `TableDef`, stamped
// with this engine, and asserted per key — a JSON-null expected value means
// the key must be ABSENT after stamping. A semantics change regenerates the
// fixture in the same commit (`make gen-spec-computed-stamping`).
#[cfg(feature = "in_memory")]
mod computed_stamping_corpus {
    use par_rt_db_client::in_memory::stamp_computed;
    use par_rt_db_client::schema::TableDef;

    use super::{Value, json};

    fn load_rows() -> Vec<Value> {
        let doc: Value = serde_json::from_str(include_str!(
            "../../wire-corpus/spec/computed-stamping.json"
        ))
        .unwrap_or_else(|e| panic!("parse computed-stamping.json: {e}"));
        doc["rows"]
            .as_array()
            .expect("spec corpus missing rows array")
            .clone()
    }

    /// Per-key assertion: an expected JSON null means the stamped doc must NOT
    /// carry the key; anything else must deep-equal (serde `Value` equality is
    /// recursive and number-payload-sensitive — the fixture and this engine
    /// both spell whole floats `5.0`, so the spellings agree).
    fn assert_stamped(i: usize, stamped: &Value, expect: &Value) {
        for (name, expected) in expect["stampedValues"]
            .as_object()
            .expect("row expect.stampedValues")
        {
            match expected {
                Value::Null => assert!(
                    !stamped.get(name).is_some(),
                    "row {i} ({name}): key must be absent after stamping"
                ),
                expected => assert_eq!(
                    stamped
                        .get(name)
                        .unwrap_or_else(|| panic!("row {i}: missing computed key {name}")),
                    expected,
                    "row {i}: stamped value of {name}"
                ),
            }
        }
    }

    #[test]
    fn every_fixture_row_stamps_identically_to_the_server_contract() {
        for (i, row) in load_rows().into_iter().enumerate() {
            let table: TableDef = serde_json::from_value(json!({
                "fields": row["fields"],
                "computed": row["computed"],
            }))
            .unwrap_or_else(|e| panic!("row {i}: decode schema fragment: {e}"));
            let doc = row["inputDoc"].clone();
            let now = row["now"].as_i64().unwrap_or(0);
            let stamped = stamp_computed(&table, doc.as_object().cloned().unwrap_or_default(), now);
            if row["expect"]["error"].as_bool().unwrap_or(false) {
                let err = stamped.expect_err(&format!("row {i}: expected a stamp error"));
                assert_eq!(
                    err.code,
                    par_rt_db_client::error::ErrorCode::BadRequest,
                    "row {i}: error code"
                );
                continue;
            }
            let stamped =
                stamped.unwrap_or_else(|e| panic!("row {i}: unexpected stamp error: {e}"));
            assert_stamped(i, &Value::Object(stamped), &row["expect"]);
        }
    }
}
