//! Pagination cursor codec. Server cursors are standard base64 of a JSON array
//! `[indexValues..., createdAt, id]`. The client normally passes cursors through
//! opaquely; these helpers exist for parity and tests.

use base64::Engine;
use serde_json::Value;

/// Base64-encode a cursor keyset `[indexValues..., createdAt, id]` into the
/// opaque server cursor format.
pub fn encode_cursor(values: &[Value]) -> Result<String, crate::error::RtDbError> {
    let json = serde_json::to_string(values)
        .map_err(|e| crate::error::RtDbError::internal(format!("cursor encode failed: {e}")))?;
    Ok(base64::engine::general_purpose::STANDARD.encode(json))
}

/// Decode an opaque cursor back into its keyset values. Errors on non-base64
/// or non-JSON input.
pub fn decode_cursor(s: &str) -> Result<Vec<Value>, crate::error::RtDbError> {
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(s)
        .map_err(|e| crate::error::RtDbError::internal(format!("invalid cursor base64: {e}")))?;
    let v: Vec<Value> = serde_json::from_slice(&bytes)
        .map_err(|e| crate::error::RtDbError::internal(format!("invalid cursor json: {e}")))?;
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use serde_json::json;

    #[test]
    fn round_trip() {
        let values = vec![
            json!("p1"),
            json!("backlog"),
            json!(1_700_000_000_000_i64),
            json!("id1"),
        ];
        let s = encode_cursor(&values).unwrap();
        // standard base64 (with padding) of the JSON array
        let raw = decode_cursor(&s).unwrap();
        assert_eq!(raw, values);
    }

    #[test]
    fn decode_rejects_garbage() {
        assert!(decode_cursor("!!!not-base64!!!").is_err());
    }

    #[test]
    fn decode_rejects_non_array() {
        // base64 of `"hello"` (a JSON string, not an array)
        let s = base64::engine::general_purpose::STANDARD.encode(b"\"hello\"");
        assert!(decode_cursor(&s).is_err());
    }
}
