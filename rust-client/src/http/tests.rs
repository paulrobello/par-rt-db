use super::url::encode_uri_component;
use std::time::Duration;

use super::*;
use crate::error::{ErrorCode, RtDbError};
use crate::mutation::Mutation;
use crate::mutation::Transaction;
use crate::query::TableQuery;
use crate::wire::UserKind;
use serde_json::{Value, json};
use wiremock::matchers::{body_bytes, body_partial_json, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn doc(id: &str) -> Value {
    json!({"_id": id, "name": format!("n-{id}")})
}

async fn setup() -> (MockServer, RtDbHttpClient) {
    let server = MockServer::start().await;
    let client = RtDbHttpClient::new(server.uri().as_str(), "t<uuid>", "machine-token");
    (server, client)
}

fn change_op(seq: i64) -> Value {
    json!({
        "seq": seq,
        "table": "notes",
        "docId": format!("d{seq}"),
        "kind": "insert",
        "doc": {"_id": format!("d{seq}")},
        "ts": 1_700_000_000_000i64 + seq,
    })
}

fn feed_page(seqs: &[i64], log: &str) -> Value {
    let last = seqs.last().copied().unwrap_or(0);
    json!({
        "ops": seqs.iter().map(|&s| change_op(s)).collect::<Vec<_>>(),
        "nextSeq": last,
        "head": last,
        "logId": log,
    })
}

fn stream_opts() -> ChangeFeedStreamOptions {
    ChangeFeedStreamOptions {
        poll_interval: Duration::from_millis(1),
        backoff_base: Duration::from_millis(1),
        backoff_max: Duration::from_millis(2),
    }
}

#[tokio::test]
async fn stream_changes_yields_ops_and_advances_the_cursor() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(query_param("since", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(feed_page(&[1, 2], "log-1")))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(query_param("since", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(feed_page(&[3], "log-1")))
        .up_to_n_times(1)
        .mount(&server)
        .await;

    let mut stream = client.stream_changes(0, None, None, stream_opts());
    let mut got = Vec::new();
    for _ in 0..3 {
        let op = tokio::time::timeout(Duration::from_millis(500), stream.next())
            .await
            .expect("stream stalled")
            .expect("stream ended early")
            .expect("stream errored");
        got.push(op.seq);
    }
    assert_eq!(got, vec![1, 2, 3]);
    assert_eq!(stream.cursor(), 3);
}

#[tokio::test]
async fn stream_changes_resumes_after_a_failed_poll() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(query_param("since", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(feed_page(&[1], "log-1")))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(500)
                .set_body_json(json!({"code": "INTERNAL", "message": "blip"})),
        )
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(query_param("since", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(feed_page(&[2], "log-1")))
        .mount(&server)
        .await;

    let mut stream = client.stream_changes(0, None, None, stream_opts());
    let first = tokio::time::timeout(Duration::from_millis(500), stream.next())
        .await
        .expect("stream stalled")
        .expect("stream ended early")
        .expect("stream errored");
    assert_eq!(first.seq, 1);
    let second = tokio::time::timeout(Duration::from_millis(500), stream.next())
        .await
        .expect("stream stalled")
        .expect("stream ended early")
        .expect("stream errored");
    assert_eq!(second.seq, 2);
    assert_eq!(stream.cursor(), 2);
    assert_eq!(server.received_requests().await.unwrap().len(), 3);
}

#[tokio::test]
async fn stream_changes_cursor_expired_is_terminal() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .respond_with(
            ResponseTemplate::new(410)
                .set_body_json(json!({"code": "CURSOR_EXPIRED", "message": "cursor too old"})),
        )
        .mount(&server)
        .await;

    let mut stream = client.stream_changes(0, None, None, stream_opts());
    let err = tokio::time::timeout(Duration::from_millis(500), stream.next())
        .await
        .expect("stream stalled")
        .expect("stream should surface the terminal error")
        .expect_err("expected CursorExpired");
    assert!(matches!(err.code, ErrorCode::CursorExpired));
    assert!(
        tokio::time::timeout(Duration::from_millis(500), stream.next())
            .await
            .expect("stream stalled")
            .is_none()
    );
}

#[tokio::test]
async fn stream_changes_log_id_change_is_terminal() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(query_param("since", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(feed_page(&[1], "log-a")))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(feed_page(&[2], "log-b")))
        .mount(&server)
        .await;

    let mut stream = client.stream_changes(0, None, None, stream_opts());
    let first = tokio::time::timeout(Duration::from_millis(500), stream.next())
        .await
        .expect("stream stalled")
        .expect("stream ended early")
        .expect("stream errored");
    assert_eq!(first.seq, 1);
    let err = tokio::time::timeout(Duration::from_millis(500), stream.next())
        .await
        .expect("stream stalled")
        .expect("stream should surface the terminal error")
        .expect_err("expected logId-change error");
    assert!(err.message.contains("logId changed"));
    assert!(
        tokio::time::timeout(Duration::from_millis(500), stream.next())
            .await
            .expect("stream stalled")
            .is_none()
    );
}

#[tokio::test]
async fn stream_changes_stop_ends_a_caught_up_stream() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_json(feed_page(&[], "log-1")))
        .mount(&server)
        .await;

    let mut stream = client.stream_changes(0, None, None, stream_opts());
    // Caught up: the poll loop is sleeping in poll_interval, not spinning.
    assert!(
        tokio::time::timeout(Duration::from_millis(20), stream.next())
            .await
            .is_err()
    );
    stream.stop();
    assert!(
        tokio::time::timeout(Duration::from_millis(500), stream.next())
            .await
            .expect("stop did not wake the poll wait")
            .is_none()
    );
}

#[tokio::test]
async fn run_collect_posts_query_and_parses_result() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/query"))
        .and(header("authorization", "Bearer machine-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [doc("a"), doc("b")]
        })))
        .mount(&server)
        .await;
    let q = TableQuery::new("items")
        .with_index("by_status", &[json!("active")])
        .take(2);
    let got: Vec<Value> = client.run(q).await.unwrap();
    assert_eq!(got.len(), 2);
}

#[tokio::test]
async fn run_count_parses_number() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": 5})))
        .mount(&server)
        .await;
    let n: i64 = client.run(TableQuery::new("items").count()).await.unwrap();
    assert_eq!(n, 5);
}

#[tokio::test]
async fn batch_query_posts_queries_and_returns_aligned_outcomes() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/query-batch"))
        .and(header("authorization", "Bearer machine-token"))
        .and(body_partial_json(json!({
            "db": "t<uuid>",
            "queries": [{"table": "items"}]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [
                {"ok": true, "result": [{"_id": "a"}]},
                {"ok": false, "error": {"code": "NOT_FOUND", "message": "no such table"}}
            ]
        })))
        .mount(&server)
        .await;
    let q = TableQuery::new("items").take(5);
    let got = client.batch_query(&[q]).await.expect("batch query");
    assert_eq!(got.len(), 2);
    assert!(got[0].ok);
    assert_eq!(
        got[0]
            .result
            .as_ref()
            .and_then(serde_json::Value::as_array)
            .map(Vec::len),
        Some(1)
    );
    assert!(got[0].error.is_none());
    assert!(!got[1].ok);
    let err = got[1].error.as_ref().expect("error envelope");
    assert_eq!(err.code, crate::error::ErrorCode::NotFound);
    assert_eq!(err.message, "no such table");
    assert!(got[1].result.is_none());
}

#[tokio::test]
async fn get_returns_optional_doc() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": doc("a")})))
        .mount(&server)
        .await;
    let some: Option<Value> = client.get("items", "a").await.unwrap();
    assert!(some.is_some());
}

#[tokio::test]
async fn mutate_posts_and_parses_results() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/mutate"))
        .and(header("authorization", "Bearer machine-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [{"id":"new1"}, null]
        })))
        .mount(&server)
        .await;
    let txn = Mutation::new()
        .insert("items", json!({"name":"x"}))
        .patch("items", "i1", json!({"y":1}))
        .build();
    let res = client.mutate(&txn, None).await.unwrap();
    assert_eq!(res.len(), 2);
    assert!(matches!(res[0], crate::mutation::StepResult::Insert { ref id } if id == "new1"));
}

#[tokio::test]
async fn mutate_sends_idempotency_key() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/mutate"))
        .and(wiremock::matchers::body_partial_json(
            json!({"idempotencyKey":"k1"}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"results":[]})))
        .mount(&server)
        .await;
    let txn = Mutation::new().delete("items", "i1").build();
    client.mutate(&txn, Some("k1")).await.unwrap();
}

#[tokio::test]
async fn schedule_posts_when_and_txn_and_returns_id() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/schedule"))
        .and(header("authorization", "Bearer machine-token"))
        .and(body_partial_json(json!({
            "db": "t<uuid>",
            "when": {"type": "afterMs", "ms": 5000},
            "txn": {"steps": []}
        })))
        // external=false must be OMITTED (the server serde-defaults it);
        // a wiremock matched-body check for the absent key rides on the
        // exact-body assertion in the external test below.
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "job-7"})))
        .mount(&server)
        .await;
    let txn = Mutation::new().build();
    let id = client
        .schedule(&txn, crate::wire::ScheduleWhen::AfterMs { ms: 5000 }, false)
        .await
        .unwrap();
    assert_eq!(id, "job-7");
}

#[tokio::test]
async fn schedule_external_posts_the_external_flag() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/schedule"))
        .and(body_partial_json(json!({
            "db": "t<uuid>",
            "when": {"type": "afterMs", "ms": 1000},
            "txn": {"steps": []},
            "external": true
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "job-8"})))
        .mount(&server)
        .await;
    let txn = Mutation::new().build();
    let id = client
        .schedule(&txn, crate::wire::ScheduleWhen::AfterMs { ms: 1000 }, true)
        .await
        .unwrap();
    assert_eq!(id, "job-8");
}

#[tokio::test]
async fn schedule_manage_ops_post_their_path_and_db_body() {
    let (server, client) = setup().await;
    for op in ["cancel", "pause", "resume"] {
        Mock::given(method("POST"))
            .and(path(format!("/api/schedule/job-1/{op}")))
            .and(body_partial_json(json!({"db": "t<uuid>"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
    }
    assert!(client.cancel_schedule("job-1").await.unwrap());
    assert!(client.pause_schedule("job-1").await.unwrap());
    assert!(client.resume_schedule("job-1").await.unwrap());
}

#[tokio::test]
async fn schedule_manage_ok_false_resolves_false_and_errors_reject() {
    let (server, client) = setup().await;
    // 200 {ok:false} = unknown/terminal id: the no-op signal, resolves
    // Ok(false) (same contract as the WS bare ack).
    Mock::given(method("POST"))
        .and(path("/api/schedule/job-9/cancel"))
        .and(body_partial_json(json!({"db": "t<uuid>"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": false})))
        .mount(&server)
        .await;
    // A real failure arrives as a non-2xx error envelope and still rejects.
    Mock::given(method("POST"))
        .and(path("/api/schedule/job-8/cancel"))
        .and(body_partial_json(json!({"db": "t<uuid>"})))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "code": "NOT_FOUND", "message": "no such job"
        })))
        .mount(&server)
        .await;
    assert!(!client.cancel_schedule("job-9").await.unwrap());
    let err = client.cancel_schedule("job-8").await.unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::NotFound);
}

#[test]
fn encode_uri_component_matches_browser() {
    // The encodeURIComponent unescaped set: A-Za-z0-9 - _ . ! ~ * ' ( )
    assert_eq!(encode_uri_component("aB3-_.!~*'()"), "aB3-_.!~*'()");
    // Reserved chars get uppercase-hex percent-encoding.
    assert_eq!(encode_uri_component("a/b"), "a%2Fb");
    assert_eq!(encode_uri_component("a b"), "a%20b");
    assert_eq!(encode_uri_component("a+b"), "a%2Bb");
    // Multi-byte UTF-8 percent-encodes each byte (é = C3 A9), matching the browser.
    assert_eq!(encode_uri_component("é"), "%C3%A9");
    // Real schedule ids (uuid v7) are untouched.
    assert_eq!(
        encode_uri_component("019f9111-acf2-7803-9074-fc50505582d4"),
        "019f9111-acf2-7803-9074-fc50505582d4"
    );
}

#[tokio::test]
async fn schedule_manage_encodes_the_id_path_segment() {
    let (server, client) = setup().await;
    // An id with a reserved path char (`/`) and a space must be
    // percent-encoded like encodeURIComponent, matching the TS client:
    // `a b/c` -> `a%20b%2Fc`.
    Mock::given(method("POST"))
        .and(path("/api/schedule/a%20b%2Fc/cancel"))
        .and(body_partial_json(json!({"db": "t<uuid>"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client.cancel_schedule("a b/c").await.unwrap();
}

#[tokio::test]
async fn list_schedules_returns_schedule_info_vec() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/schedules"))
        .and(header("authorization", "Bearer machine-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "schedules": [{
                "id": "job-1",
                "kind": "cron",
                "dueAt": 9000,
                "cron": "*/5 * * * *",
                "status": "pending",
                "createdAt": 1000,
                "firedCount": 0
            }]
        })))
        .mount(&server)
        .await;
    let list = client.list_schedules().await.unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, "job-1");
    assert_eq!(list[0].cron.as_deref(), Some("*/5 * * * *"));
}

#[tokio::test]
async fn claim_schedules_posts_claim_and_parses_jobs() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/schedule/claim"))
        .and(header("authorization", "Bearer machine-token"))
        .and(body_partial_json(json!({
            "db": "t<uuid>",
            "limit": 4,
            "leaseMs": 60_000
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobs": [{
                "id": "job-1",
                "kind": "cron",
                "dueAt": 9000,
                "txn": {"steps": [{"op": "insert", "table": "items", "doc": {"n": 1}}]},
                "cron": "*/5 * * * *",
                "leaseGeneration": 3,
                "leaseDeadlineMs": 1_234_567_890
            }]
        })))
        .mount(&server)
        .await;
    let jobs = client.claim_schedules(Some(4), Some(60_000)).await.unwrap();
    assert_eq!(jobs.len(), 1);
    let job = &jobs[0];
    assert_eq!(job.id, "job-1");
    assert_eq!(job.kind, crate::wire::ScheduleKind::Cron);
    assert_eq!(job.due_at, 9000);
    assert_eq!(job.cron.as_deref(), Some("*/5 * * * *"));
    assert_eq!(job.every_ms, None);
    assert_eq!(job.lease_generation, 3);
    assert_eq!(job.lease_deadline_ms, 1_234_567_890);
    // The txn decodes to a real Transaction, not raw JSON.
    assert_eq!(job.txn.steps.len(), 1);
}

#[tokio::test]
async fn claim_schedules_omits_unset_tunables_from_body() {
    let (server, client) = setup().await;
    // Exact-body match asserts the `None` limit/leaseMs keys are omitted
    // entirely — never serialized as `null` (matches the TS client, whose
    // `JSON.stringify` drops undefined).
    Mock::given(method("POST"))
        .and(path("/api/schedule/claim"))
        .and(body_bytes(
            serde_json::to_vec(&json!({"db": "t<uuid>"})).unwrap(),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"jobs": []})))
        .mount(&server)
        .await;
    let jobs = client.claim_schedules(None, None).await.unwrap();
    assert!(jobs.is_empty());
}

#[tokio::test]
async fn schedule_finalize_ops_post_paths_and_lease_body() {
    let (server, client) = setup().await;
    for op in ["complete", "retry", "fail"] {
        Mock::given(method("POST"))
            .and(path(format!("/api/schedule/job-1/{op}")))
            .and(body_partial_json(json!({"db": "t<uuid>", "lease": 7})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
            .mount(&server)
            .await;
    }
    client.complete_schedule("job-1", 7).await.unwrap();
    client.retry_schedule("job-1", 7, None, None).await.unwrap();
    client.fail_schedule("job-1", 7, "boom").await.unwrap();
}

#[tokio::test]
async fn schedule_finalize_retry_carries_delay_and_error() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/schedule/job-1/retry"))
        .and(body_partial_json(json!({
            "db": "t<uuid>", "lease": 7, "delayMs": 5000, "error": "boom"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client
        .retry_schedule("job-1", 7, Some(5000), Some("boom"))
        .await
        .unwrap();
}

#[tokio::test]
async fn schedule_finalize_omits_unset_retry_tunables() {
    let (server, client) = setup().await;
    // Exact-body match: `None` delayMs/error keys are omitted entirely.
    Mock::given(method("POST"))
        .and(path("/api/schedule/job-1/retry"))
        .and(body_bytes(
            serde_json::to_vec(&json!({
                "db": "t<uuid>", "lease": 7
            }))
            .unwrap(),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client.retry_schedule("job-1", 7, None, None).await.unwrap();
}

#[tokio::test]
async fn schedule_finalize_stale_lease_rejects_conflict() {
    let (server, client) = setup().await;
    // A fencing token that is no longer current arrives as a non-2xx
    // CONFLICT error envelope (see server `finalize_external`).
    Mock::given(method("POST"))
        .and(path("/api/schedule/job-1/complete"))
        .and(body_partial_json(json!({"db": "t<uuid>", "lease": 4})))
        .respond_with(ResponseTemplate::new(409).set_body_json(json!({
            "code": "CONFLICT", "message": "stale lease"
        })))
        .mount(&server)
        .await;
    let err = client.complete_schedule("job-1", 4).await.unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::Conflict);
}

#[tokio::test]
async fn start_workflow_posts_db_and_spec_and_returns_id() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/workflows"))
        .and(header("authorization", "Bearer machine-token"))
        .and(body_partial_json(json!({
            "db": "t<uuid>",
            "spec": {
                "name": "drip",
                "steps": [ { "txn": { "steps": [] } } ]
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "wf-7"})))
        .mount(&server)
        .await;
    let spec = crate::wire::WorkflowSpec {
        name: "drip".into(),
        steps: vec![crate::wire::WorkflowStepSpec {
            txn: Some(Mutation::new().build()),
            await_signal: None,
            retry: None,
            sleep_before_ms: None,
        }],
    };
    let id = client.start_workflow(&spec).await.unwrap();
    assert_eq!(id, "wf-7");
}

#[tokio::test]
async fn cancel_workflow_posts_db_body_and_returns_cancelled() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/workflows/wf-1/cancel"))
        .and(body_partial_json(json!({"db": "t<uuid>"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"cancelled": false})))
        .mount(&server)
        .await;
    // `false` (missing or already-terminal run) is a no-op, not an error.
    assert!(!client.cancel_workflow("wf-1").await.unwrap());
}

#[tokio::test]
async fn signal_workflow_posts_db_name_payload_and_returns_delivered() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/workflows/wf-1/signal"))
        .and(body_partial_json(json!({
            "db": "t<uuid>", "name": "approve", "payload": {"ok": true}
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"delivered": true})))
        .mount(&server)
        .await;
    // `payload: None` omits the key entirely — exact raw body, not a
    // partial match (same dual-mount pattern as `list_workflows`).
    Mock::given(method("POST"))
        .and(path("/api/workflows/wf-1/signal"))
        .and(body_bytes(
            serde_json::to_string(&json!({"db": "t<uuid>", "name": "approve"}))
                .unwrap()
                .into_bytes(),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"delivered": true})))
        .mount(&server)
        .await;
    assert!(
        client
            .signal_workflow("wf-1", "approve", Some(json!({"ok": true})))
            .await
            .unwrap()
    );
    assert!(
        client
            .signal_workflow("wf-1", "approve", None)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn list_workflows_posts_optional_status_filter() {
    let (server, client) = setup().await;
    // Some(status) serializes as a snake_case filter key in the body.
    Mock::given(method("POST"))
        .and(path("/api/workflows/list"))
        .and(header("authorization", "Bearer machine-token"))
        .and(body_partial_json(
            json!({"db": "t<uuid>", "status": "failed"}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"workflows": []})))
        .mount(&server)
        .await;
    // None omits the key entirely — exact raw body, not a partial match.
    Mock::given(method("POST"))
        .and(path("/api/workflows/list"))
        .and(body_bytes(
            serde_json::to_string(&json!({"db": "t<uuid>"}))
                .unwrap()
                .into_bytes(),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "workflows": [{
                "id": "wf1", "name": "drip", "status": "success",
                "currentStep": 2, "stepCount": 2, "attempts": 1,
                "createdAt": 1, "updatedAt": 9, "finishedAt": 9
            }]
        })))
        .mount(&server)
        .await;
    let failed = client
        .list_workflows(Some(crate::wire::WorkflowStatus::Failed))
        .await
        .unwrap();
    assert!(failed.is_empty());
    let all = client.list_workflows(None).await.unwrap();
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].id, "wf1");
    assert_eq!(all[0].status, crate::wire::WorkflowStatus::Success);
}

#[tokio::test]
async fn error_envelope_becomes_rtdb_error() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/query"))
        .respond_with(
            ResponseTemplate::new(409)
                .set_body_json(json!({"code":"PRECONDITION_FAILED","message":"version mismatch"})),
        )
        .mount(&server)
        .await;
    let err = client
        .run::<i64>(TableQuery::new("items").count())
        .await
        .unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::PreconditionFailed);
    assert_eq!(err.message, "version mismatch");
}

#[tokio::test]
async fn non_envelope_error_is_internal() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/query"))
        .respond_with(ResponseTemplate::new(500).set_body_string("gateway down"))
        .mount(&server)
        .await;
    let err = client
        .run::<i64>(TableQuery::new("items").count())
        .await
        .unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::Internal);
}

#[tokio::test]
async fn auth_me_returns_user() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/auth/me"))
        .and(header("authorization", "Bearer machine-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "user": {"kind":"user","email":"a@b.com","name":null}
        })))
        .mount(&server)
        .await;
    let user = client.auth_me().await.unwrap();
    assert_eq!(user.kind, UserKind::User);
    assert_eq!(user.email.as_deref(), Some("a@b.com"));
}

#[tokio::test]
async fn validate_session_token_returns_user_and_github_identity() {
    let (server, client) = setup().await;
    // The validated token is the argument, not the client's own token —
    // the mock asserts the request carries exactly the player's bearer.
    Mock::given(method("GET"))
        .and(path("/auth/validate"))
        .and(header("authorization", "Bearer player-session-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "user": {
                "kind": "user",
                "email": "player@example.com",
                "name": null,
                "githubLogin": "player",
                "githubId": 42
            }
        })))
        .mount(&server)
        .await;
    let user = client
        .validate_session_token("player-session-token")
        .await
        .unwrap();
    assert_eq!(user.kind, UserKind::User);
    assert_eq!(user.email.as_deref(), Some("player@example.com"));
    assert_eq!(user.github_login.as_deref(), Some("player"));
    assert_eq!(user.github_id, Some(42));
}

#[tokio::test]
async fn validate_session_token_surfaces_auth_error_for_invalid_token() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/auth/validate"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({
            "code": "UNAUTHORIZED",
            "message": "invalid token"
        })))
        .mount(&server)
        .await;
    let err = client
        .validate_session_token("not-a-real-token")
        .await
        .unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::Unauthorized);
}

#[tokio::test]
async fn validate_session_token_tolerates_response_without_github_fields() {
    let (server, client) = setup().await;
    // An older server omitting githubLogin/githubId must still parse,
    // defaulting both to None (backward-compatible additive fields).
    Mock::given(method("GET"))
        .and(path("/auth/validate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "user": {"kind": "machine", "email": null, "name": null}
        })))
        .mount(&server)
        .await;
    let user = client.validate_session_token("mach-tok").await.unwrap();
    assert_eq!(user.kind, UserKind::Machine);
    assert_eq!(user.github_login, None);
    assert_eq!(user.github_id, None);
}

// ---- host machine authorization (public /auth/enroll) -----------------

#[test]
fn enroll_request_serializes_only_the_grant_field() {
    // The server denies unknown fields on /auth/enroll — the request DTO
    // must serialize to exactly {grant} with no db/host/capability keys.
    let body = crate::wire::enrollment::EnrollRequest {
        grant: "rtdb_enroll_secret".to_string(),
    };
    assert_eq!(
        serde_json::to_value(&body).unwrap(),
        json!({"grant": "rtdb_enroll_secret"}),
    );
}

#[tokio::test]
async fn enroll_posts_grant_and_parses_redeemed_grant() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/auth/enroll"))
        .and(body_partial_json(json!({"grant": "rtdb_enroll_secret"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tokenId": "t1",
            "token": "mach-secret",
            "db": "app",
            "capability": {"type": "parMeshV1", "host": "macdaddy"},
        })))
        .mount(&server)
        .await;
    let redeemed = client.enroll("rtdb_enroll_secret").await.unwrap();
    assert_eq!(redeemed.token_id, "t1");
    assert_eq!(redeemed.token, "mach-secret");
    assert_eq!(redeemed.db, "app");
    assert_eq!(
        redeemed.capability,
        crate::wire::HostCapability::ParMeshV1 {
            host: "macdaddy".to_string()
        }
    );
}

#[tokio::test]
async fn enroll_surfaces_unauthorized_envelope() {
    // Invalid/expired/revoked/used grants all surface as the generic
    // 401 UNAUTHORIZED envelope, not a leaked reason.
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/auth/enroll"))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({
            "code": "UNAUTHORIZED",
            "message": "unauthorized"
        })))
        .mount(&server)
        .await;
    let err = client.enroll("stale-grant").await.unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::Unauthorized);
}

// `mutate_with_retry` reuses `retry_on_precondition`, so these tests focus on
// the read-modify-write composition: a conflict re-fetches and retries, and
// `max_attempts` bounds the loop. Rebuild folds the fetched `_version` into an
// `expectVersion` step, mirroring the common write-through pattern.
fn current_doc() -> Value {
    json!({"_id": "i1", "_version": 3, "n": 1})
}

// Fetches the doc's `_version` and guards a patch with `expectVersion`.
fn write_through(current: Option<Value>) -> Result<Transaction, RtDbError> {
    let version = current
        .as_ref()
        .and_then(|c| c.get("_version"))
        .and_then(Value::as_i64)
        .unwrap_or(0);
    Ok(Mutation::new()
        .expect_version("items", "i1", version)
        .patch("items", "i1", json!({"n": 2}))
        .build())
}

#[tokio::test]
async fn mutate_with_retry_succeeds_on_first_attempt() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": current_doc()})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/mutate"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"results":[{"id":"i1"}]})))
        .mount(&server)
        .await;

    let res = client
        .mutate_with_retry::<Value, _, _>(
            "items",
            "i1",
            DEFAULT_MUTATE_MAX_ATTEMPTS,
            |current| async move { write_through(current) },
        )
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
    assert!(matches!(res[0], crate::mutation::StepResult::Insert { ref id } if id == "i1"));
}

#[tokio::test]
async fn mutate_with_retry_retries_once_then_succeeds() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": current_doc()})))
        .mount(&server)
        .await;
    let mutate_calls = Arc::new(AtomicU32::new(0));
    let calls = mutate_calls.clone();
    Mock::given(method("POST"))
        .and(path("/api/mutate"))
        .respond_with(move |_: &wiremock::Request| {
            let n = calls.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                ResponseTemplate::new(409).set_body_json(json!({
                    "code": "PRECONDITION_FAILED",
                    "message": "version mismatch: expected 3, actual 4"
                }))
            } else {
                ResponseTemplate::new(200).set_body_json(json!({"results":[{"id":"i1"}]}))
            }
        })
        .mount(&server)
        .await;

    let res = client
        .mutate_with_retry::<Value, _, _>(
            "items",
            "i1",
            DEFAULT_MUTATE_MAX_ATTEMPTS,
            |current| async move { write_through(current) },
        )
        .await
        .unwrap();
    assert_eq!(res.len(), 1);
    // One initial attempt plus exactly one retry.
    assert_eq!(mutate_calls.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn mutate_with_retry_exhausts_attempts_and_surfaces_error() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/query"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": current_doc()})))
        .mount(&server)
        .await;
    let mutate_calls = Arc::new(AtomicU32::new(0));
    let calls = mutate_calls.clone();
    Mock::given(method("POST"))
        .and(path("/api/mutate"))
        .respond_with(move |_: &wiremock::Request| {
            calls.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(409).set_body_json(
                json!({"code": "PRECONDITION_FAILED", "message": "version mismatch"}),
            )
        })
        .mount(&server)
        .await;

    let err = client
        .mutate_with_retry::<Value, _, _>("items", "i1", 3, |current| async move {
            write_through(current)
        })
        .await
        .unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::PreconditionFailed);
    assert_eq!(err.message, "version mismatch");
    // `max_attempts = 3` → initial attempt plus 2 retries, then give up.
    assert_eq!(mutate_calls.load(Ordering::SeqCst), 3);
}

// `find_one_by_index` and `upsert_by_index` are thin wrappers over the
// query/mutate paths, so these tests assert both the on-the-wire request
// (the `first` terminal / the one-step upsert) and the parsed result.

#[tokio::test]
async fn find_one_by_index_hit_returns_doc() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/query"))
        .and(body_partial_json(json!({
            "query": {
                "table": "users",
                "index": "by_email",
                "eq": ["a@b.com"],
                "first": true
            }
        })))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"result": {"_id": "u1", "email": "a@b.com", "n": 1}})),
        )
        .mount(&server)
        .await;
    let got: Option<Value> = client
        .find_one_by_index("users", "by_email", "a@b.com")
        .await
        .unwrap();
    let doc = got.expect("expected a matching doc");
    assert_eq!(doc.get("email").and_then(Value::as_str), Some("a@b.com"));
}

#[tokio::test]
async fn find_one_by_index_miss_returns_none() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/query"))
        .and(body_partial_json(json!({
            "query": {
                "table": "users",
                "index": "by_email",
                "eq": ["none@x.com"],
                "first": true
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": null})))
        .mount(&server)
        .await;
    let got: Option<Value> = client
        .find_one_by_index("users", "by_email", "none@x.com")
        .await
        .unwrap();
    assert!(got.is_none());
}

#[tokio::test]
async fn upsert_by_index_inserts_when_no_match() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/mutate"))
        .and(body_partial_json(json!({
            "txn": {
                "steps": [{
                    "op": "upsert",
                    "table": "users",
                    "index": "by_email",
                    "eq": ["a@b.com"],
                    "insert": {"email": "a@b.com"},
                    "patch": {"n": 1}
                }]
            }
        })))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"results": [{"id": "new1", "inserted": true}]})),
        )
        .mount(&server)
        .await;
    let res = client
        .upsert_by_index(
            "users",
            "by_email",
            "a@b.com",
            json!({"email": "a@b.com"}),
            json!({"n": 1}),
        )
        .await
        .unwrap();
    assert!(matches!(
        res,
        crate::mutation::StepResult::Upsert { ref id, inserted: true } if id == "new1"
    ));
}

#[tokio::test]
async fn upsert_by_index_patches_when_match() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/mutate"))
        .and(body_partial_json(json!({
            "txn": {
                "steps": [{
                    "op": "upsert",
                    "table": "users",
                    "index": "by_email",
                    "eq": ["a@b.com"],
                    "insert": {"email": "a@b.com"},
                    "patch": {"n": 2}
                }]
            }
        })))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"results": [{"id": "u1", "inserted": false}]})),
        )
        .mount(&server)
        .await;
    let res = client
        .upsert_by_index(
            "users",
            "by_email",
            "a@b.com",
            json!({"email": "a@b.com"}),
            json!({"n": 2}),
        )
        .await
        .unwrap();
    assert!(matches!(
        res,
        crate::mutation::StepResult::Upsert { ref id, inserted: false } if id == "u1"
    ));
}

// Storage surface (`POST /api/storage/{db}` raw-body upload, `DELETE`, `GET
// .../metadata`, and the no-request `get_url`). `body_bytes` (a function, not
// a macro, in wiremock 0.6) matches the exact raw request bytes.
//
// Unlike the schedule routes (where `db` rides in the JSON body), the storage
// routes put `db` in the URL path. The `setup()` fixture uses `db = "t<uuid>"`
// as a recognizable placeholder, and reqwest parses the URL via the `url`
// crate — which percent-encodes `<` and `>` (members of the URL path encode
// set) — so the actual path the server receives is `/api/storage/t%3Cuuid%3E`.
// Production db names never contain `<>`, so the matchers use the encoded
// form to reflect what the client really sends.

#[tokio::test]
async fn upload_posts_raw_bytes_and_returns_metadata() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/storage/t%3Cuuid%3E"))
        .and(header("authorization", "Bearer machine-token"))
        .and(header("content-type", "image/png"))
        .and(body_bytes("raw-bytes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "f1", "sha256": "abc", "size": 9, "contentType": "image/png"
        })))
        .mount(&server)
        .await;
    let up = client
        .upload(b"raw-bytes", Some("image/png"))
        .await
        .unwrap();
    assert_eq!(up.id, "f1");
    assert_eq!(up.size, 9);
    assert_eq!(up.content_type.as_deref(), Some("image/png"));
}

// ENH-021 mirror: `upload_stream` posts the concatenated chunk bytes as the
// raw request body (same wire contract as `upload`) without buffering the
// whole file in memory. Two chunks exercise the multi-item path; wiremock
// concatenates them into the body the matcher sees, and the byte-for-byte
// match proves the streaming path lands on the identical route / headers /
// body the server expects.
#[tokio::test]
async fn upload_stream_posts_chunked_body_and_returns_metadata() {
    use bytes::Bytes;
    use futures_util::stream;
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/storage/t%3Cuuid%3E"))
        .and(header("authorization", "Bearer machine-token"))
        .and(header("content-type", "image/png"))
        .and(body_bytes("chunked-bytes"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "s1", "sha256": "def", "size": 13, "contentType": "image/png"
        })))
        .mount(&server)
        .await;
    let chunks: Vec<Result<Bytes, std::io::Error>> = vec![
        Ok(Bytes::from_static(b"chunked-")),
        Ok(Bytes::from_static(b"bytes")),
    ];
    let up = client
        .upload_stream(stream::iter(chunks), Some("image/png"))
        .await
        .unwrap();
    assert_eq!(up.id, "s1");
    assert_eq!(up.size, 13);
    assert_eq!(up.content_type.as_deref(), Some("image/png"));
}

#[tokio::test]
async fn delete_file_and_metadata_and_get_url() {
    let (server, client) = setup().await;
    Mock::given(method("DELETE"))
        .and(path("/api/storage/t%3Cuuid%3E/f1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/storage/t%3Cuuid%3E/f1/metadata"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "f1", "sha256": "abc", "size": 9, "creationTime": 5
        })))
        .mount(&server)
        .await;
    client.delete_file("f1").await.unwrap();
    let meta = client.get_file_metadata("f1").await.unwrap();
    assert_eq!(meta.size, 9);
    assert_eq!(meta.creation_time, 5);
    assert_eq!(meta.content_type, None);
    // `mod tests` is a child of the `http` module, so it can read the private
    // `url` field of `RtDbHttpClient`.
    assert_eq!(client.get_url("f1"), format!("{}/storage/f1", client.url));
}

// ARC-013 follow-up: the protocol-version header is attached by the shared
// `AuthedRequest::authed` seam, so it rides every route this client serves —
// not just query/mutate/batch, which were the only three carrying it when
// the header was hand-written per call site. One request per family
// (schedule, workflow, storage, auth) and one assertion over every request
// the server actually received.
#[tokio::test]
async fn protocol_header_rides_every_route_not_just_query_mutate_batch() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/api/schedule"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "job-1"})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/workflows"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "wf-1"})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/storage/t%3Cuuid%3E"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "f1", "sha256": "abc", "size": 3
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/storage/t%3Cuuid%3E/f1/metadata"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "f1", "sha256": "abc", "size": 3, "creationTime": 5
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/auth/me"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "user": {"kind": "machine", "subject": "s"}
        })))
        .mount(&server)
        .await;

    let txn = Mutation::new().build();
    client
        .schedule(&txn, crate::wire::ScheduleWhen::AfterMs { ms: 1 }, false)
        .await
        .unwrap();
    client
        .start_workflow(&crate::wire::WorkflowSpec {
            name: "w".into(),
            steps: vec![crate::wire::WorkflowStepSpec {
                txn: Some(Mutation::new().build()),
                await_signal: None,
                retry: None,
                sleep_before_ms: None,
            }],
        })
        .await
        .unwrap();
    client.upload(b"abc", None).await.unwrap();
    client.get_file_metadata("f1").await.unwrap();
    client.auth_me().await.unwrap();

    let expected = crate::wire::PROTOCOL_VERSION.to_string();
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 5);
    for req in requests {
        let sent = req
            .headers
            .get("x-rtdb-protocol")
            .and_then(|v| v.to_str().ok());
        assert_eq!(
            sent,
            Some(expected.as_str()),
            "{} {} sent no protocol header",
            req.method,
            req.url.path()
        );
    }
}

#[tokio::test]
async fn get_signed_url_builds_get_with_optional_ttl() {
    let (server, client) = setup().await;
    // With ttl — asserts the query param is sent.
    Mock::given(method("GET"))
        .and(path("/api/storage/t%3Cuuid%3E/f1/signed-url"))
        .and(query_param("ttlSeconds", "120"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "url": "http://x/storage/f1?exp=9&sig=ab", "expiresAt": 9
        })))
        .mount(&server)
        .await;
    let r = client.get_signed_url("f1", Some(120)).await.unwrap();
    assert_eq!(r.url, "http://x/storage/f1?exp=9&sig=ab");
    assert_eq!(r.expires_at, 9);

    // Without ttl — no query param matcher, still matches by path.
    Mock::given(method("GET"))
        .and(path("/api/storage/t%3Cuuid%3E/f1/signed-url"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "url": "u", "expiresAt": 7
        })))
        .mount(&server)
        .await;
    let r = client.get_signed_url("f1", None).await.unwrap();
    assert_eq!(r.expires_at, 7);
}

#[test]
fn transform_url_appends_query_params() {
    let client = RtDbHttpClient::new("https://rtdb.example", "db", "tok");
    let url = client.transform_url(
        "f1",
        &TransformOpts {
            w: Some(100),
            h: Some(50),
            fit: Some(Fit::Cover),
            q: Some(80),
            format: Some(OutFormat::Auto),
        },
    );
    assert_eq!(
        url,
        "https://rtdb.example/storage/f1?w=100&h=50&fit=cover&q=80"
    );
}

#[test]
fn transform_url_scale_down_and_jpeg() {
    let client = RtDbHttpClient::new("https://rtdb.example", "db", "tok");
    let url = client.transform_url(
        "f1",
        &TransformOpts {
            w: None,
            h: Some(200),
            fit: Some(Fit::ScaleDown),
            q: None,
            format: Some(OutFormat::Jpeg),
        },
    );
    assert_eq!(
        url,
        "https://rtdb.example/storage/f1?h=200&fit=scale-down&format=jpeg"
    );
}

#[test]
fn transform_url_png_omits_unset_params() {
    let client = RtDbHttpClient::new("https://rtdb.example", "db", "tok");
    let url = client.transform_url(
        "f1",
        &TransformOpts {
            w: None,
            h: None,
            fit: None,
            q: None,
            format: Some(OutFormat::Png),
        },
    );
    assert_eq!(url, "https://rtdb.example/storage/f1?format=png");
}
