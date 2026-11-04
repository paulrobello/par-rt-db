// go-client/errors/errors.go
package errors

// Mirrors server/src/error.rs::ErrorCode and the canonical {code,
// httpStatus} table in wire-corpus/error-codes.json. The enumerated table
// (const block, httpStatus map, AllCodes) is GENERATED from that fixture
// into error_codes_gen.go — never hand-edit it; run `make gen-error-codes`
// after the fixture changes. The table's agreement with the fixture is
// pinned by TestErrorCodesCorpus in wire/.

// ErrorCode is one of the wire error codes.
type ErrorCode string

// HTTPStatus maps a code to its canonical HTTP status.
func HTTPStatus(code ErrorCode) int {
	if s, ok := httpStatus[code]; ok {
		return s
	}
	return 500
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
