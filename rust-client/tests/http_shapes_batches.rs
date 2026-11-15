//! HTTP-shape drift tests (QA-008 sweep) — the rust-client view of the
//! batch-transport vocabulary (`/api/query-batch`, `/api/mutate-batch`).
//! Loads the canonical fixtures from `wire-corpus/http-shapes.json` and
//! asserts each slot decodes into the rust-client's typed DTOs with exact
//! key-set parity. Drift here means the rust-client drifted from the wire
//! contract (the server-side parity test constructs the real structs against
//! the same fixture).

#![allow(clippy::unwrap_used, clippy::expect_used)]
use par_rt_db_client::mutation::Transaction;
use par_rt_db_client::wire::{BatchMutateOutcome, BatchMutateRequest, BatchQueryOutcome};

fn load_shapes() -> serde_json::Value {
    // include_str! resolves relative to this source file (rust-client/tests/),
    // so the test is independent of cargo test's runtime CWD (rust-client/).
    serde_json::from_str(include_str!("../../wire-corpus/http-shapes.json"))
        .unwrap_or_else(|e| panic!("parse http-shapes.json: {e}"))
}

/// The two query-batch slots decode into `BatchQueryOutcome` and re-serialize
/// byte-identically — the omit-when-None contract (an ok slot carries no
/// `error` key, an errored slot no `result` key) must survive the round trip.
#[test]
fn query_batch_slots_round_trip() {
    let shapes = load_shapes();
    for slot in shapes["queryBatchResponse"]["results"].as_array().unwrap() {
        let input = slot.clone();
        let parsed: BatchQueryOutcome = serde_json::from_value(input.clone())
            .unwrap_or_else(|e| panic!("parse query slot: {e}\n  input: {input}"));
        let dumped = serde_json::to_value(&parsed).expect("serialize BatchQueryOutcome");
        assert_eq!(dumped, input, "wire drift in BatchQueryOutcome");
    }
}

/// The two mutate-batch slots decode into `BatchMutateOutcome` and re-serialize
/// byte-identically; the fixture's ok slot carries positional `results`, the
/// failed slot only the error envelope.
#[test]
fn mutate_batch_slots_round_trip() {
    let shapes = load_shapes();
    for slot in shapes["mutateBatchResponse"]["results"].as_array().unwrap() {
        let input = slot.clone();
        let parsed: BatchMutateOutcome = serde_json::from_value(input.clone())
            .unwrap_or_else(|e| panic!("parse mutate slot: {e}\n  input: {input}"));
        let dumped = serde_json::to_value(&parsed).expect("serialize BatchMutateOutcome");
        assert_eq!(dumped, input, "wire drift in BatchMutateOutcome");
    }
}

/// `mutateBatchRequest` decodes entry-for-entry: each entry is the exact
/// `BatchMutateRequest` wire bytes (keyed entry carries `idempotencyKey`, the
/// bare entry omits it) and each txn re-serializes to the fixture bytes.
#[test]
fn mutate_batch_request_entries_round_trip() {
    let shapes = load_shapes();
    let entries = shapes["mutateBatchRequest"]["txns"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    let txn0: Transaction = serde_json::from_value(entries[0]["txn"].clone()).unwrap();
    let e0 = BatchMutateRequest {
        txn: &txn0,
        idempotency_key: Some("idem-1"),
    };
    assert_eq!(
        serde_json::to_value(&e0).unwrap(),
        entries[0],
        "entry-0 wire drift"
    );
    let txn1: Transaction = serde_json::from_value(entries[1]["txn"].clone()).unwrap();
    let e1 = BatchMutateRequest {
        txn: &txn1,
        idempotency_key: None,
    };
    assert_eq!(
        serde_json::to_value(&e1).unwrap(),
        entries[1],
        "entry-1 wire drift"
    );
}
