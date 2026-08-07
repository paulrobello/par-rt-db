use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    Unauthorized,
    Forbidden,
    NotFound,
    SchemaViolation,
    PreconditionFailed,
    BadRequest,
    Internal,
    RateLimited,
    Conflict,
    QuotaExceeded,
}

/// Optional `Retry-After` hint, in seconds, attached to a `RateLimited` error.
/// Only set when `code == RateLimited`; absent on every other code (and absent
/// on the wire shape — `#[serde(skip_serializing_if)]` keeps existing error
/// envelopes byte-identical). Parsed back in via `#[serde(default)]` so older
/// clients/tests that produce an error envelope without this field still
/// deserialize.
#[derive(Debug, thiserror::Error, Clone, serde::Serialize, serde::Deserialize)]
#[error("{code:?}: {message}")]
pub struct RtDbError {
    pub code: ErrorCode,
    pub message: String,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        rename = "retryAfter"
    )]
    pub retry_after_secs: Option<u32>,
}

impl RtDbError {
    pub fn new(code: ErrorCode, msg: impl Into<String>) -> Self {
        Self {
            code,
            message: msg.into(),
            retry_after_secs: None,
        }
    }

    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::BadRequest, msg)
    }

    pub fn schema(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::SchemaViolation, msg)
    }

    pub fn unauthorized(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::Unauthorized, msg)
    }

    pub fn forbidden(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::Forbidden, msg)
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::NotFound, msg)
    }

    pub fn precondition(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::PreconditionFailed, msg)
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::Internal, msg)
    }

    /// Rate-limit denial. `retry_after_secs` is surfaced to the client via the
    /// HTTP `Retry-After` header (seconds) and on the wire body's `retryAfter`
    /// field — the rest of the error envelope is unchanged.
    pub fn rate_limited(retry_after_secs: u32) -> Self {
        Self {
            code: ErrorCode::RateLimited,
            message: "rate limit exceeded".to_string(),
            retry_after_secs: Some(retry_after_secs),
        }
    }

    /// A uniqueness / conflict violation (HTTP 409). Used for a Postgres
    /// `unique_violation` (SQLSTATE 23505) on a `UNIQUE` index, both at
    /// `CREATE UNIQUE INDEX` time and on a colliding write inside `execute_txn`.
    pub fn conflict(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::Conflict, msg)
    }

    /// A per-database resource quota was exceeded (HTTP 507). Used for
    /// table-count, storage-byte, and concurrent-subscription caps; the message
    /// identifies which. Wire code `QUOTA_EXCEEDED`.
    pub fn quota_exceeded(msg: impl Into<String>) -> Self {
        Self::new(ErrorCode::QuotaExceeded, msg)
    }

    pub fn status(&self) -> StatusCode {
        match self.code {
            ErrorCode::Unauthorized => StatusCode::UNAUTHORIZED,
            ErrorCode::Forbidden => StatusCode::FORBIDDEN,
            ErrorCode::NotFound => StatusCode::NOT_FOUND,
            ErrorCode::SchemaViolation => StatusCode::UNPROCESSABLE_ENTITY,
            ErrorCode::PreconditionFailed => StatusCode::CONFLICT,
            ErrorCode::BadRequest => StatusCode::BAD_REQUEST,
            ErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
            ErrorCode::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            ErrorCode::Conflict => StatusCode::CONFLICT,
            ErrorCode::QuotaExceeded => StatusCode::INSUFFICIENT_STORAGE,
        }
    }
}

impl From<sqlx::Error> for RtDbError {
    fn from(err: sqlx::Error) -> Self {
        if let Some(db) = err.as_database_error()
            && db.code().as_deref() == Some("23505")
        {
            let constraint = db
                .constraint()
                .map(|c| format!(" '{c}'"))
                .unwrap_or_default();
            return Self::conflict(format!("unique constraint{constraint} violated"));
        }
        tracing::error!(error = %err, "sqlx error");
        // For non-conflict errors, never leak Postgres error text
        // (relation/column names); the CONFLICT branch above intentionally
        // surfaces the constraint name as a schema identifier. The full
        // error is already logged above.
        Self::internal("internal error")
    }
}

impl IntoResponse for RtDbError {
    fn into_response(self) -> Response {
        let status = self.status();
        // `Retry-After` only on `RateLimited`; pull it out before `Json(self)`
        // moves the error so the header and body stay consistent.
        let retry_after = self.retry_after_secs;
        let mut resp = (status, Json(self)).into_response();
        if let Some(secs) = retry_after
            && let Ok(value) = HeaderValue::from_str(&secs.to_string())
        {
            resp.headers_mut().insert(header::RETRY_AFTER, value);
        }
        resp
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use serde_json::json;

    #[test]
    fn schema_error_serializes_to_wire_envelope() {
        let err = RtDbError::schema("x");
        assert_eq!(
            serde_json::to_value(&err).unwrap(),
            json!({"code": "SCHEMA_VIOLATION", "message": "x"})
        );
    }

    #[test]
    fn schema_error_round_trips() {
        let err = RtDbError::schema("x");
        let value = serde_json::to_value(&err).unwrap();
        let restored: RtDbError = serde_json::from_value(value).unwrap();
        assert_eq!(restored.code, err.code);
        assert_eq!(restored.message, err.message);
    }

    #[test]
    fn status_maps_each_code_to_expected_http_status() {
        assert_eq!(
            RtDbError::unauthorized("x").status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(RtDbError::forbidden("x").status(), StatusCode::FORBIDDEN);
        assert_eq!(RtDbError::not_found("x").status(), StatusCode::NOT_FOUND);
        assert_eq!(
            RtDbError::schema("x").status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(RtDbError::precondition("x").status(), StatusCode::CONFLICT);
        assert_eq!(
            RtDbError::bad_request("x").status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            RtDbError::internal("x").status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            RtDbError::rate_limited(30).status(),
            StatusCode::TOO_MANY_REQUESTS
        );
        assert_eq!(RtDbError::conflict("x").status(), StatusCode::CONFLICT);
    }

    #[test]
    fn rate_limited_carries_retry_after_and_round_trips() {
        let err = RtDbError::rate_limited(42);
        assert_eq!(err.code, ErrorCode::RateLimited);
        assert_eq!(err.retry_after_secs, Some(42));
        // Wire shape: code + message + retryAfter (camelCase, present when set).
        let v = serde_json::to_value(&err).unwrap();
        assert_eq!(
            v,
            json!({"code": "RATE_LIMITED", "message": "rate limit exceeded", "retryAfter": 42})
        );
        // Round-trip preserves the field.
        let restored: RtDbError = serde_json::from_value(v).unwrap();
        assert_eq!(restored.code, ErrorCode::RateLimited);
        assert_eq!(restored.retry_after_secs, Some(42));
    }

    #[test]
    fn non_rate_limited_errors_omit_retry_after_on_the_wire() {
        // Existing error envelope stays byte-identical for every other code:
        // the new field is skipped when None, so this test guards a wire-shape
        // regression on all pre-existing error responses.
        let v = serde_json::to_value(RtDbError::bad_request("x")).unwrap();
        assert_eq!(v, json!({"code": "BAD_REQUEST", "message": "x"}));
    }

    #[test]
    fn conflict_error_maps_to_http_409() {
        assert_eq!(RtDbError::conflict("dup").status(), StatusCode::CONFLICT);
    }

    #[test]
    fn conflict_error_serializes_to_wire_envelope() {
        let err = RtDbError::conflict("unique index 'i_t_by_email' violated");
        assert_eq!(
            serde_json::to_value(&err).unwrap(),
            json!({"code": "CONFLICT", "message": "unique index 'i_t_by_email' violated"})
        );
    }

    #[test]
    fn quota_exceeded_maps_to_507() {
        let err = RtDbError::quota_exceeded("db over cap");
        assert_eq!(err.code, ErrorCode::QuotaExceeded);
        assert_eq!(err.status(), StatusCode::INSUFFICIENT_STORAGE);
        let json = serde_json::to_value(&err).unwrap();
        assert_eq!(json["code"], "QUOTA_EXCEEDED");
        assert_eq!(json["message"], "db over cap");
    }
}
