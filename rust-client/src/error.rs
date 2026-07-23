//! Error envelope and retry helper. Mirrors the server's `{code, message}` wire shape.

use serde::{Deserialize, Serialize};
use std::future::Future;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    Unauthorized,
    Forbidden,
    NotFound,
    SchemaViolation,
    PreconditionFailed,
    BadRequest,
    Internal,
}

/// Raw `{code, message}` as it appears on the wire (HTTP body / WS error frame).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    pub code: ErrorCode,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
pub struct RtDbError {
    pub code: ErrorCode,
    pub message: String,
}

impl RtDbError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn from_envelope(env: ErrorEnvelope) -> Self {
        Self {
            code: env.code,
            message: env.message,
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }
}

/// Retries a read-modify-write closure only on `PRECONDITION_FAILED`.
/// `retries` is the number of retries after the first attempt.
pub async fn retry_on_precondition<F, Fut, T>(mut f: F, retries: u32) -> Result<T, RtDbError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, RtDbError>>,
{
    let mut left = retries;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) if e.code == ErrorCode::PreconditionFailed && left > 0 => {
                left -= 1;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_serializes_screaming_snake() {
        assert_eq!(
            serde_json::to_value(ErrorCode::PreconditionFailed).unwrap(),
            serde_json::json!("PRECONDITION_FAILED")
        );
        assert_eq!(
            serde_json::to_value(ErrorCode::SchemaViolation).unwrap(),
            serde_json::json!("SCHEMA_VIOLATION")
        );
    }

    #[test]
    fn error_code_round_trips_all_variants() {
        let all = [
            ErrorCode::Unauthorized,
            ErrorCode::Forbidden,
            ErrorCode::NotFound,
            ErrorCode::SchemaViolation,
            ErrorCode::PreconditionFailed,
            ErrorCode::BadRequest,
            ErrorCode::Internal,
        ];
        for c in all {
            let v = serde_json::to_value(c).unwrap();
            let back: ErrorCode = serde_json::from_value(v).unwrap();
            assert_eq!(c, back);
        }
    }

    #[test]
    fn rtdb_error_serializes_envelope() {
        let e = RtDbError::new(ErrorCode::NotFound, "missing doc");
        assert_eq!(
            serde_json::to_value(&e).unwrap(),
            serde_json::json!({"code":"NOT_FOUND","message":"missing doc"})
        );
    }

    #[test]
    fn rtdb_error_deserializes_envelope() {
        let e: RtDbError =
            serde_json::from_value(serde_json::json!({"code":"BAD_REQUEST","message":"bad"}))
                .unwrap();
        assert_eq!(e.code, ErrorCode::BadRequest);
        assert_eq!(e.message, "bad");
    }

    #[tokio::test]
    async fn retry_retries_only_on_precondition() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let attempts = AtomicU32::new(0);
        let f = || {
            let n = attempts.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 2 {
                    Err(RtDbError::new(ErrorCode::PreconditionFailed, "conflict"))
                } else {
                    Ok(7_i64)
                }
            }
        };
        let got: i64 = retry_on_precondition(f, 5).await.unwrap();
        assert_eq!(got, 7);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn retry_does_not_retry_other_errors() {
        use std::sync::atomic::{AtomicU32, Ordering};
        let attempts = AtomicU32::new(0);
        let f = || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async move { Err::<i64, _>(RtDbError::new(ErrorCode::NotFound, "x")) }
        };
        let err = retry_on_precondition::<_, _, i64>(f, 5).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::NotFound);
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }
}
