use axum::Json;
use axum::http::StatusCode;
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
}

#[derive(Debug, thiserror::Error, Clone, serde::Serialize, serde::Deserialize)]
#[error("{code:?}: {message}")]
pub struct RtDbError {
    pub code: ErrorCode,
    pub message: String,
}

impl RtDbError {
    pub fn new(code: ErrorCode, msg: impl Into<String>) -> Self {
        Self {
            code,
            message: msg.into(),
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

    pub fn status(&self) -> StatusCode {
        match self.code {
            ErrorCode::Unauthorized => StatusCode::UNAUTHORIZED,
            ErrorCode::Forbidden => StatusCode::FORBIDDEN,
            ErrorCode::NotFound => StatusCode::NOT_FOUND,
            ErrorCode::SchemaViolation => StatusCode::UNPROCESSABLE_ENTITY,
            ErrorCode::PreconditionFailed => StatusCode::CONFLICT,
            ErrorCode::BadRequest => StatusCode::BAD_REQUEST,
            ErrorCode::Internal => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

impl From<sqlx::Error> for RtDbError {
    fn from(err: sqlx::Error) -> Self {
        tracing::error!(error = %err, "sqlx error");
        // Never leak Postgres error text (relation/column names, constraint
        // details) to clients; the full error is already logged above.
        Self::internal("internal error")
    }
}

impl IntoResponse for RtDbError {
    fn into_response(self) -> Response {
        let status = self.status();
        (status, Json(self)).into_response()
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
    }
}
