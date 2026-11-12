//! HTTP-shape drift tests (ARC-008) — the rust-client view of the bulk-delete
//! response vocabulary. Loads the canonical fixtures from
//! `wire-corpus/http-shapes.json` and asserts each bulk case decodes into the
//! rust-client's typed DTOs with the exact field values the wire contract
//! pins, plus the error-key presence contract (`error` absent on `running`,
//! present on `failed`). Drift here means the rust-client drifted from the
//! wire contract.

#![allow(clippy::unwrap_used, clippy::expect_used)]
use par_rt_db_client::wire::{BulkOpAccepted, BulkOpInfo};

fn load_shapes() -> serde_json::Value {
    // include_str! resolves relative to this source file (rust-client/tests/),
    // so the test is independent of cargo test's runtime CWD (rust-client/).
    serde_json::from_str(include_str!("../../wire-corpus/http-shapes.json"))
        .unwrap_or_else(|e| panic!("parse http-shapes.json: {e}"))
}

fn case(shapes: &serde_json::Value, name: &str) -> serde_json::Value {
    shapes
        .get(name)
        .unwrap_or_else(|| panic!("http-shapes.json missing case '{name}'"))
        .clone()
}

/// `bulkDeleteAccepted` decodes into `BulkOpAccepted` with the fixture's
/// exact field values.
#[test]
fn bulk_delete_accepted_decodes() {
    let input = case(&load_shapes(), "bulkDeleteAccepted");
    let parsed: BulkOpAccepted = serde_json::from_value(input.clone())
        .unwrap_or_else(|e| panic!("parse bulkDeleteAccepted: {e}\n  input: {input}"));
    assert_eq!(parsed.op_id, "bulk_01JENQ9Z7K3M2V8R5T4W6X8Y0A");
    assert_eq!(parsed.matched, 1500);
    assert_eq!(parsed.state, "running");
    // Round-trip: re-serialization is byte-identical to the fixture (no extra
    // keys, camelCase preserved).
    let dumped = serde_json::to_value(&parsed).expect("serialize BulkOpAccepted");
    assert_eq!(dumped, input, "wire drift in BulkOpAccepted");
}

/// `bulkOpInfoRunning` decodes into `BulkOpInfo` from the EXACT fixture bytes
/// — which carry no `error` key — proving the missing-key default works, and
/// re-serializing keeps `error` absent.
#[test]
fn bulk_op_info_running_decodes_without_error_key() {
    let input = case(&load_shapes(), "bulkOpInfoRunning");
    assert!(
        input.get("error").is_none(),
        "fixture drift: bulkOpInfoRunning must not carry an error key"
    );
    let raw = serde_json::to_string(&input).expect("stringify fixture");
    let parsed: BulkOpInfo = serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("parse bulkOpInfoRunning: {e}\n  input: {raw}"));
    assert_eq!(parsed.id, "bulk_01JENQ9Z7K3M2V8R5T4W6X8Y0A");
    assert_eq!(parsed.state, "running");
    assert_eq!(parsed.matched, 1500);
    assert_eq!(parsed.deleted, 500);
    assert!(parsed.error.is_none());
    // Round-trip: `error` must stay absent, not serialize as null.
    let dumped = serde_json::to_value(&parsed).expect("serialize BulkOpInfo");
    assert_eq!(dumped, input, "wire drift in BulkOpInfo (running)");
    assert!(
        dumped.get("error").is_none(),
        "error must not serialize as null"
    );
}

/// `bulkOpInfoFailed` decodes with `error` present, from the exact fixture
/// bytes.
#[test]
fn bulk_op_info_failed_decodes_with_error() {
    let input = case(&load_shapes(), "bulkOpInfoFailed");
    let raw = serde_json::to_string(&input).expect("stringify fixture");
    let parsed: BulkOpInfo = serde_json::from_str(&raw)
        .unwrap_or_else(|e| panic!("parse bulkOpInfoFailed: {e}\n  input: {raw}"));
    assert_eq!(parsed.id, "bulk_01JENQ9Z7K3M2V8R5T4W6X8Y0A");
    assert_eq!(parsed.state, "failed");
    assert_eq!(parsed.matched, 1500);
    assert_eq!(parsed.deleted, 500);
    assert_eq!(
        parsed.error.as_deref(),
        Some("chunk turn failed: schema changed mid-op")
    );
    // Round-trip: `error` re-serializes onto the wire.
    let dumped = serde_json::to_value(&parsed).expect("serialize BulkOpInfo");
    assert_eq!(dumped, input, "wire drift in BulkOpInfo (failed)");
}

/// Both bulk DTOs follow `ClaimedSchedule`'s strictness convention: lenient
/// decode — an unknown key neither fails nor survives a round-trip — and the
/// `BulkOpInfo::error` key may be entirely absent (a bare `{...}` without it
/// decodes, unlike a `deny_unknown_fields` shape's required-key contract).
#[test]
fn bulk_dto_decode_is_lenient() {
    let input = serde_json::json!({
        "opId": "bulk_x",
        "matched": 1,
        "state": "running",
        "unknownFutureKey": true
    });
    let parsed: BulkOpAccepted = serde_json::from_value(input).expect("unknown key must not fail");
    assert_eq!(parsed.op_id, "bulk_x");

    let input = serde_json::json!({
        "id": "bulk_x",
        "state": "succeeded",
        "matched": 3,
        "deleted": 3,
        "unknownFutureKey": true
    });
    let parsed: BulkOpInfo = serde_json::from_value(input).expect("unknown key must not fail");
    assert!(parsed.error.is_none());
}
