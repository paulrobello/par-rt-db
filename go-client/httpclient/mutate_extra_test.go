// go-client/httpclient/mutate_extra_test.go
package httpclient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func contains(s, sub string) bool { return strings.Contains(s, sub) }

// MutateBatch rejects a key list whose length does not match txns before any
// request is made.
func TestMutateBatchKeyLengthMismatch(t *testing.T) {
	c := NewClient("http://localhost:1", "d1", "tk")
	_, err := c.MutateBatch(context.Background(),
		[]wire.Transaction{{}}, []string{"k1", "k2"})
	if err == nil || !contains(err.Error(), "does not match txns length") {
		t.Fatalf("want length-mismatch error, got %v", err)
	}
}

// MutateWithRetry succeeds on the first try when no precondition fails.
func TestMutateWithRetryImmediateSuccess(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.Write([]byte(`{"results":[{"id":"i1"}]}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	build := func() wire.Transaction {
		return wire.Transaction{Steps: []wire.Step{
			wire.StepInsert{Table: "items", Doc: wire.Object(nil)},
		}}
	}
	res, err := c.MutateWithRetry(context.Background(), build, 3)
	if err != nil || len(res) != 1 {
		t.Fatalf("%v %v", res, err)
	}
	if calls != 1 {
		t.Fatalf("calls %d, want 1", calls)
	}
}

// A PRECONDITION_FAILED failure is retried; a later success wins.
func TestMutateWithRetryRecoversFromPrecondition(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusConflict)
			w.Write([]byte(`{"code":"PRECONDITION_FAILED","message":"stale"}`))
			return
		}
		w.Write([]byte(`{"results":[{"id":"i1"}]}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	res, err := c.MutateWithRetry(context.Background(),
		func() wire.Transaction { return wire.Transaction{} }, 3)
	if err != nil || len(res) != 1 {
		t.Fatalf("%v %v", res, err)
	}
	if calls != 2 {
		t.Fatalf("calls %d, want 2", calls)
	}
}

// A non-precondition error aborts the retry loop immediately.
func TestMutateWithRetryNonPreconditionAborts(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"code":"NOT_FOUND","message":"no table"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	_, err := c.MutateWithRetry(context.Background(),
		func() wire.Transaction { return wire.Transaction{} }, 3)
	if !rtdberrors.IsCode(err, "NOT_FOUND") {
		t.Fatalf("error %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls %d, want 1 (no retry on other codes)", calls)
	}
}

// All attempts failing on PRECONDITION_FAILED return the last error.
func TestMutateWithRetryExhaustedReturnsLastError(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(`{"code":"PRECONDITION_FAILED","message":"stale"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	_, err := c.MutateWithRetry(context.Background(),
		func() wire.Transaction { return wire.Transaction{} }, 2)
	var re *rtdberrors.RtDbError
	if !errors.As(err, &re) || re.Code != rtdberrors.CodePreconditionFailed {
		t.Fatalf("error %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls %d, want 2", calls)
	}
}

// Zero attempts return the internal "no attempts made" error without any
// request.
func TestMutateWithRetryZeroAttempts(t *testing.T) {
	c := NewClient("http://localhost:1", "d1", "tk")
	_, err := c.MutateWithRetry(context.Background(),
		func() wire.Transaction { return wire.Transaction{} }, 0)
	var re *rtdberrors.RtDbError
	if !errors.As(err, &re) || re.Code != rtdberrors.CodeInternal {
		t.Fatalf("error %v", err)
	}
	if !contains(err.Error(), "no attempts made") {
		t.Fatalf("message %q", err.Error())
	}
}
