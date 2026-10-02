// go-client/httpclient/claim_test.go
package httpclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// The wire shape pinned against server/src/http_api.rs: the request carries
// external only when true; the claim request omits unset limit/leaseMs; the
// finalize routes carry lease (+ delayMs/error per op).
func TestScheduleExternalBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if !bytesHas(b, `"when":{"ms":1000,"type":"afterMs"}`) {
			t.Errorf("body %s", b)
		}
		if !bytesHas(b, `"external":true`) {
			t.Errorf("body missing external: %s", b)
		}
		w.Write([]byte(`{"id":"job-ext-1"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	id, err := c.Schedule(context.Background(), wire.WhenAfterMs{Ms: 1000}, wire.Transaction{}, WithExternal())
	if err != nil || id != "job-ext-1" {
		t.Fatalf("id=%q err=%v", id, err)
	}

	// No option: external must be omitted from the wire body.
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if bytesHas(b, "external") {
			t.Errorf("body must omit external: %s", b)
		}
		w.Write([]byte(`{"id":"job-2"}`))
	}))
	defer srv2.Close()
	c2 := NewClient(srv2.URL, "d1", "tk")
	if _, err := c2.Schedule(context.Background(), wire.WhenAfterMs{Ms: 1000}, wire.Transaction{}); err != nil {
		t.Fatal(err)
	}
}

func int64p(v int64) *int64 { return &v }

func TestClaimSchedulesDecode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/schedule/claim" {
			t.Errorf("path %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		if !bytesHas(b, `"db":"d1"`) || bytesHas(b, "limit") || bytesHas(b, "leaseMs") {
			t.Errorf("claim body %s", b)
		}
		w.Write([]byte(`{"jobs":[{"id":"j1","kind":"oneshot","dueAt":123,"txn":{"steps":[{"op":"insert","table":"items","doc":{"name":"x"}}]},"leaseGeneration":7,"leaseDeadlineMs":456}]}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	jobs, err := c.ClaimSchedules(context.Background(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 {
		t.Fatalf("jobs %+v", jobs)
	}
	j := jobs[0]
	if j.ID != "j1" || j.Kind != wire.ScheduleKindOneshot || j.DueAt != 123 ||
		j.LeaseGeneration != 7 || j.LeaseDeadlineMs != 456 {
		t.Fatalf("job %+v", j)
	}
	if len(j.Txn.Steps) != 1 {
		t.Fatalf("txn %+v", j.Txn)
	}
}

func TestClaimSchedulesRequestCarriesLimitAndLease(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if !bytesHas(b, `"limit":3`) || !bytesHas(b, `"leaseMs":60000`) {
			t.Errorf("claim body %s", b)
		}
		w.Write([]byte(`{"jobs":[]}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	jobs, err := c.ClaimSchedules(context.Background(), int64p(3), int64p(60000))
	if err != nil || len(jobs) != 0 {
		t.Fatalf("jobs %+v err=%v", jobs, err)
	}
}

func TestFinalizeScheduleBodies(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		switch r.URL.Path {
		case "/api/schedule/j1/complete":
			if !bytesHas(b, `"lease":7`) || bytesHas(b, "delayMs") || bytesHas(b, `"error"`) {
				t.Errorf("complete body %s", b)
			}
		case "/api/schedule/j1/retry":
			if !bytesHas(b, `"lease":7`) || !bytesHas(b, `"delayMs":5000`) || !bytesHas(b, `"error":"boom"`) {
				t.Errorf("retry body %s", b)
			}
		case "/api/schedule/j1/fail":
			if !bytesHas(b, `"lease":7`) || !bytesHas(b, `"error":"dead"`) || bytesHas(b, "delayMs") {
				t.Errorf("fail body %s", b)
			}
		default:
			t.Errorf("path %s", r.URL.Path)
		}
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	ctx := context.Background()
	if err := c.CompleteSchedule(ctx, "j1", 7); err != nil {
		t.Fatal(err)
	}
	if err := c.RetrySchedule(ctx, "j1", 7, int64p(5000), "boom"); err != nil {
		t.Fatal(err)
	}
	if err := c.FailSchedule(ctx, "j1", 7, "dead"); err != nil {
		t.Fatal(err)
	}
}

// leaseGeneration must stay int64 end to end: a float64 decode loses exact
// values past 2^53.
func TestClaimedScheduleLeaseGenerationStaysInt64(t *testing.T) {
	raw := `{"id":"j1","kind":"interval","dueAt":1,"txn":{"steps":[]},"everyMs":1000,"leaseGeneration":9223372036854775807,"leaseDeadlineMs":2}`
	var cs wire.ClaimedSchedule
	if err := json.Unmarshal([]byte(raw), &cs); err != nil {
		t.Fatal(err)
	}
	if cs.LeaseGeneration != 9223372036854775807 {
		t.Fatalf("leaseGeneration = %d", cs.LeaseGeneration)
	}
}
