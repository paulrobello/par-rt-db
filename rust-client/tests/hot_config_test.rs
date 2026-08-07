//! Wire round-trip for the ENH-011 quota fields + the QUOTA_EXCEEDED error code.

use par_rt_db_client::{ErrorCode, HotConfig, HotConfigPatch};
use serde_json::json;

#[test]
fn hot_config_round_trips_quota_fields() {
    let hot: HotConfig = serde_json::from_value(json!({
        "allowedOrigins": ["https://example.com"],
        "sessionTtlDays": 30,
        "maxFileSize": 52428800,
        "idempotencyTtlMs": 300000,
        "maxTablesPerDb": 10,
        "maxStorageBytesPerDb": 1048576,
        "maxSubsPerDb": 50
    }))
    .expect("HotConfig with quota fields decodes");
    assert_eq!(hot.max_tables_per_db, 10);
    assert_eq!(hot.max_storage_bytes_per_db, 1048576);
    assert_eq!(hot.max_subs_per_db, 50);
}

#[test]
fn hot_config_patch_omits_unset_quota_fields() {
    let patch = HotConfigPatch {
        max_subs_per_db: Some(5),
        ..Default::default()
    };
    let v = serde_json::to_value(&patch).unwrap();
    assert_eq!(v["maxSubsPerDb"], 5);
    assert!(v.get("maxTablesPerDb").is_none());
    assert!(v.get("maxStorageBytesPerDb").is_none());
}

#[test]
fn quota_exceeded_error_code_round_trips() {
    assert_eq!(
        serde_json::from_str::<ErrorCode>("\"QUOTA_EXCEEDED\"").unwrap(),
        ErrorCode::QuotaExceeded
    );
    assert_eq!(
        serde_json::to_string(&ErrorCode::QuotaExceeded).unwrap(),
        "\"QUOTA_EXCEEDED\""
    );
}
