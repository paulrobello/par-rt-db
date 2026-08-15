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

use par_rt_db_client::Query;
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
                "missed": 0
            }
        ]
    });
    let parsed: SubscriptionsResponse =
        serde_json::from_value(input.clone()).expect("parse SubscriptionsResponse");
    let dumped = serde_json::to_value(&parsed).expect("serialize SubscriptionsResponse");
    assert_eq!(dumped, input, "wire drift in SubscriptionsResponse");
    assert!(parsed.subscriptions[1].principal.is_none());
    assert_eq!(parsed.per_db[0].skips_indexed, 5);

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
        "skipsPoint": 2, "skipsIndexed": 3, "skipsOrdered": 4, "missed": 5
    }))
    .unwrap();
    assert_eq!(
        serde_json::to_value(&c).unwrap(),
        json!({"db":"d","reruns":1,"skipsPoint":2,"skipsIndexed":3,"skipsOrdered":4,"missed":5})
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
}
