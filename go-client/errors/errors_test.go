// go-client/errors/errors_test.go
package errors_test

import (
	"encoding/json"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/errors"
)

func TestRtDbErrorDecode(t *testing.T) {
	var e errors.RtDbError
	if err := json.Unmarshal([]byte(`{"code":"RATE_LIMITED","message":"slow","retryAfter":2}`), &e); err != nil {
		t.Fatal(err)
	}
	if e.Code != errors.CodeRateLimited || e.Message != "slow" {
		t.Fatalf("decode: %+v", e)
	}
	if e.RetryAfter == nil || *e.RetryAfter != 2 {
		t.Fatalf("retryAfter: %v", e.RetryAfter)
	}
	if e.Error() != "RATE_LIMITED: slow" {
		t.Fatalf("Error(): %q", e.Error())
	}
}

func TestHTTPStatusTable(t *testing.T) {
	if got := errors.HTTPStatus(errors.CodeRateLimited); got != 429 {
		t.Fatalf("RATE_LIMITED status %d", got)
	}
	if got := errors.HTTPStatus(errors.CodeQuotaExceeded); got != 507 {
		t.Fatalf("QUOTA_EXCEEDED status %d", got)
	}
	if got := errors.HTTPStatus(errors.CodeCursorExpired); got != 410 {
		t.Fatalf("CURSOR_EXPIRED status %d", got)
	}
	codes := errors.AllCodes()
	if len(codes) != 12 {
		t.Fatalf("want 12 codes, got %d", len(codes))
	}
	seen := map[errors.ErrorCode]int{}
	for _, c := range codes {
		seen[c] = errors.HTTPStatus(c)
	}
	if len(seen) != 12 {
		t.Fatalf("duplicate constants in AllCodes: %v", codes)
	}
}
