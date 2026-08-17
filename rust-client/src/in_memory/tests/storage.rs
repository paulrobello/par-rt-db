use super::*;

// ---- storage ----------------------------------------------------------
//
// The TS suite does not cover storage directly (the harness ships it as an
// honest stub); these exercise the surface so the wire shapes stay aligned
// with the live HTTP client (`crate::http::UploadResult` /
// `crate::http::FileMetadata`).

#[test]
fn upload_stores_bytes_and_returns_id_sha_size_and_content_type() {
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let bytes = b"hello world".to_vec();
    let result = c
        .upload(bytes.clone(), Some("text/plain".to_string()))
        .expect("upload ok");
    // Id is `f<base36>` — distinct in shape from a 32-hex-char doc id.
    assert!(result.id.starts_with('f'), "id shape: {}", result.id);
    assert_eq!(result.size, bytes.len() as i64);
    assert_eq!(result.content_type.as_deref(), Some("text/plain"));
    // SHA-256 of "hello world" is a known constant — verifies we computed
    // it correctly (not just non-empty).
    assert_eq!(
        result.sha256,
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    );
}

#[test]
fn upload_without_content_type_returns_none() {
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let result = c.upload(b"x".to_vec(), None).expect("upload ok");
    assert!(result.content_type.is_none());
}

#[test]
fn upload_mints_distinct_ids_for_distinct_uploads() {
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let a = c.upload(b"a".to_vec(), None).expect("upload ok");
    let b = c.upload(b"b".to_vec(), None).expect("upload ok");
    assert_ne!(a.id, b.id, "ids distinct");
}

#[test]
fn get_file_metadata_returns_size_and_creation_time() {
    // Mirrors the TS harness: getFileMetadata's sha256 is "" (only the
    // upload result carries the real digest).
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let up = c
        .upload(
            b"abc".to_vec(),
            Some("application/octet-stream".to_string()),
        )
        .expect("upload ok");
    let meta = c.get_file_metadata(&up.id).expect("metadata ok");
    assert_eq!(meta.id, up.id);
    assert_eq!(meta.size, 3);
    assert_eq!(meta.sha256, "");
    assert_eq!(
        meta.content_type.as_deref(),
        Some("application/octet-stream")
    );
    assert!(meta.creation_time > 0);
}

#[test]
fn get_file_metadata_unknown_id_is_not_found() {
    let c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let err = c.get_file_metadata("f99").unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
}

#[test]
fn delete_file_removes_the_blob_and_rejects_unknown_id_with_not_found() {
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let up = c.upload(b"x".to_vec(), None).expect("upload ok");
    c.delete_file(&up.id).expect("delete ok");
    // Second delete fails — NOT_FOUND (idempotent on the live server, but
    // the in-memory harness mirrors the TS surface which throws on miss).
    let err = c.delete_file(&up.id).unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
}

#[test]
fn get_url_returns_synthetic_memory_handle() {
    let c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    assert_eq!(c.get_url("f1"), "memory://f1");
}
