//! Error envelope and retry helper. Mirrors the server's `{code, message}` wire shape.

use serde::{Deserialize, Serialize};
use std::future::Future;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
/// Stable error codes mirroring the server's `error::ErrorCode` one-to-one
/// (serde `SCREAMING_SNAKE_CASE` on the wire).
pub enum ErrorCode {
    /// Missing or invalid credentials (HTTP 401).
    Unauthorized,
    /// Authenticated but denied — allowlist or per-row-rule rejection (HTTP 403).
    Forbidden,
    /// Target document does not exist (HTTP 404).
    NotFound,
    /// Document or schema violates the pushed schema.
    SchemaViolation,
    /// `expectVersion`/`expectAbsent` mismatch — the retryable write conflict (HTTP 409).
    PreconditionFailed,
    /// Malformed request or DSL shape.
    BadRequest,
    /// Server-side failure; carries a generic, non-leaking message (HTTP 500).
    Internal,
    /// Unique-index violation (mirrors server `error::ErrorCode::Conflict`,
    /// HTTP 409). Serialized as `"CONFLICT"` by the container `rename_all`.
    Conflict,
    /// Mirrors server `error::ErrorCode::RateLimited` (HTTP 429). Serialized
    /// `"RATE_LIMITED"`; the carrying envelope includes `retryAfter` when set.
    RateLimited,
    /// Mirrors server `error::ErrorCode::QuotaExceeded` (HTTP 507). Serialized
    /// `"QUOTA_EXCEEDED"`.
    QuotaExceeded,
}

/// Raw `{code, message, retryAfter?}` as it appears on the wire (HTTP body /
/// WS error frame). `retry_after` is present only on `RATE_LIMITED`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEnvelope {
    /// Stable error code.
    pub code: ErrorCode,
    /// Human-readable failure description.
    pub message: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "retryAfter"
    )]
    /// Seconds to wait, present only on `RATE_LIMITED`.
    pub retry_after: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, thiserror::Error)]
#[error("{message}")]
/// The client's error type: every failure surface is this `{code, message}`
/// envelope (the server's wire error adopted directly). Serializable so a
/// received wire error round-trips losslessly.
pub struct RtDbError {
    /// Stable error code.
    pub code: ErrorCode,
    /// Human-readable failure description.
    pub message: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "retryAfter"
    )]
    /// Seconds to wait, present only on `RATE_LIMITED`.
    pub retry_after: Option<u32>,
}

impl RtDbError {
    /// Build an error from a code and message.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            retry_after: None,
        }
    }

    /// Adopt a wire-received envelope as the error (no field changes).
    pub fn from_envelope(env: ErrorEnvelope) -> Self {
        Self {
            code: env.code,
            message: env.message,
            retry_after: env.retry_after,
        }
    }

    /// Shorthand for `ErrorCode::Internal`.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, message)
    }

    /// Rate-limit denial mirroring the server's `RtDbError::rate_limited`
    /// (`code: RATE_LIMITED`, `retryAfter: retry_after_secs`).
    pub fn rate_limited(retry_after_secs: u32) -> Self {
        Self {
            code: ErrorCode::RateLimited,
            message: "rate limit exceeded".to_string(),
            retry_after: Some(retry_after_secs),
        }
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
            ErrorCode::Conflict,
            ErrorCode::RateLimited,
            ErrorCode::QuotaExceeded,
        ];
        for c in all {
            let v = serde_json::to_value(c).unwrap();
            let back: ErrorCode = serde_json::from_value(v).unwrap();
            assert_eq!(c, back);
        }
    }

    #[test]
    fn conflict_serializes_as_conflict() {
        // Mirrors server `error::ErrorCode::Conflict` (HTTP 409): the container
        // `rename_all = "SCREAMING_SNAKE_CASE"` maps the `Conflict` variant to
        // the wire string `"CONFLICT"` — byte-for-byte with the server/TS/Python
        // clients, so a unique-index violation round-trips through all four.
        assert_eq!(
            serde_json::to_value(ErrorCode::Conflict).unwrap(),
            serde_json::json!("CONFLICT")
        );
        let back: ErrorCode = serde_json::from_value(serde_json::json!("CONFLICT")).unwrap();
        assert_eq!(back, ErrorCode::Conflict);
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

    #[test]
    fn rate_limited_round_trips_with_retry_after() {
        let err = RtDbError::rate_limited(42);
        let v = serde_json::to_value(&err).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"code":"RATE_LIMITED","message":"rate limit exceeded","retryAfter":42})
        );
        let back: RtDbError = serde_json::from_value(v).unwrap();
        assert_eq!(back.code, ErrorCode::RateLimited);
        assert_eq!(back.retry_after, Some(42));
    }

    #[test]
    fn non_rate_limited_error_omits_retry_after() {
        // Wire shape stays {code, message} for every non-rate error — the field
        // is skip-serialized when None, guarding a wire-shape regression.
        let v = serde_json::to_value(RtDbError::new(ErrorCode::BadRequest, "x")).unwrap();
        assert_eq!(v, serde_json::json!({"code":"BAD_REQUEST","message":"x"}));
    }
}
