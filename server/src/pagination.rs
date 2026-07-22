use crate::error::RtDbError;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use serde_json::Value;

/// Encode a cursor from an array of values: [index values..., created_at, id]
pub fn encode_cursor(values: &[Value]) -> Result<String, RtDbError> {
    let json = serde_json::to_string(values)
        .map_err(|e| RtDbError::internal(format!("failed to encode cursor: {e}")))?;
    Ok(BASE64.encode(json))
}

/// Decode a cursor into an array of values
pub fn decode_cursor(cursor: &str) -> Result<Vec<Value>, RtDbError> {
    let json = BASE64
        .decode(cursor)
        .map_err(|e| RtDbError::bad_request(format!("invalid cursor base64: {e}")))?;
    let json_str = std::str::from_utf8(&json)
        .map_err(|e| RtDbError::bad_request(format!("invalid cursor utf-8: {e}")))?;
    let values: Vec<Value> = serde_json::from_str(json_str)
        .map_err(|e| RtDbError::bad_request(format!("invalid cursor json: {e}")))?;
    Ok(values)
}
