// go-client/errors/errors.go
package errors

// Mirrors server/src/error.rs::ErrorCode and the canonical {code,
// httpStatus} table in wire-corpus/error-codes.json. Twelve codes, closed
// domain; the fixture is regenerated from the server enum and this package
// must match it exactly (pinned by TestErrorCodesCorpus in wire/).

// ErrorCode is one of the twelve wire error codes.
type ErrorCode string

const (
	CodeUnauthorized        ErrorCode = "UNAUTHORIZED"
	CodeForbidden           ErrorCode = "FORBIDDEN"
	CodeNotFound            ErrorCode = "NOT_FOUND"
	CodeSchemaViolation     ErrorCode = "SCHEMA_VIOLATION"
	CodePreconditionFailed  ErrorCode = "PRECONDITION_FAILED"
	CodeBadRequest          ErrorCode = "BAD_REQUEST"
	CodeInternal            ErrorCode = "INTERNAL"
	CodeRateLimited         ErrorCode = "RATE_LIMITED"
	CodeConflict            ErrorCode = "CONFLICT"
	CodeQuotaExceeded       ErrorCode = "QUOTA_EXCEEDED"
	CodeUnsupportedProtocol ErrorCode = "UNSUPPORTED_PROTOCOL"
	CodeCursorExpired       ErrorCode = "CURSOR_EXPIRED"
	// CodeReadOnly is the per-database read-only freeze
	// (PATCH /admin/db/{db}/readonly): client-plane document writes are
	// rejected while frozen.
	CodeReadOnly ErrorCode = "READ_ONLY"
)

// httpStatus mirrors wire-corpus/error-codes.json's {code, httpStatus}
// pairs. Update in the same change as the fixture.
var httpStatus = map[ErrorCode]int{
	CodeUnauthorized:        401,
	CodeForbidden:           403,
	CodeNotFound:            404,
	CodeSchemaViolation:     422,
	CodePreconditionFailed:  409,
	CodeBadRequest:          400,
	CodeInternal:            500,
	CodeRateLimited:         429,
	CodeConflict:            409,
	CodeQuotaExceeded:       507,
	CodeUnsupportedProtocol: 400,
	CodeCursorExpired:       410,
	CodeReadOnly:            409,
}

// HTTPStatus maps a code to its canonical HTTP status.
func HTTPStatus(code ErrorCode) int {
	if s, ok := httpStatus[code]; ok {
		return s
	}
	return 500
}

// AllCodes is the closed set, in fixture order.
func AllCodes() []ErrorCode {
	return []ErrorCode{
		CodeUnauthorized,
		CodeForbidden,
		CodeNotFound,
		CodeSchemaViolation,
		CodePreconditionFailed,
		CodeBadRequest,
		CodeInternal,
		CodeRateLimited,
		CodeConflict,
		CodeQuotaExceeded,
		CodeUnsupportedProtocol,
		CodeCursorExpired,
		CodeReadOnly,
	}
}

// RtDbError is the wire error envelope as a Go error. Every client-facing
// failure decodes into one of these.
type RtDbError struct {
	Code       ErrorCode `json:"code"`
	Message    string    `json:"message"`
	RetryAfter *float64  `json:"retryAfter,omitempty"`
}

// Error implements the error interface.
func (e *RtDbError) Error() string {
	return string(e.Code) + ": " + e.Message
}

// New builds an RtDbError.
func New(code ErrorCode, message string) *RtDbError {
	return &RtDbError{Code: code, Message: message}
}

// IsCode reports whether err is an *RtDbError carrying exactly this code.
func IsCode(err error, code ErrorCode) bool {
	e, ok := err.(*RtDbError)
	return ok && e.Code == code
}
