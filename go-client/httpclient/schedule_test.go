// go-client/httpclient/schedule_test.go
package httpclient

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func bytesHas(b []byte, s string) bool { return bytes.Contains(b, []byte(s)) }

func TestScheduleCreateDecode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/schedule" {
			t.Errorf("path %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		if !bytesHas(b, `"when":{"ms":1000,"type":"afterMs"}`) {
			t.Errorf("body %s", b)
		}
		w.Write([]byte(`{"id":"job-1"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	id, err := c.Schedule(context.Background(), wire.WhenAfterMs{Ms: 1000}, wire.Transaction{})
	if err != nil || id != "job-1" {
		t.Fatalf("id=%q err=%v", id, err)
	}
}

func TestScheduleManageOps(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/schedule/job-9/cancel" && r.URL.Path != "/api/schedule/job-8/pause" {
			t.Errorf("path %s", r.URL.Path)
		}
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	ok, err := c.CancelSchedule(context.Background(), "job-9")
	if err != nil || !ok {
		t.Fatalf("cancel %v %v", ok, err)
	}
	ok, err = c.PauseSchedule(context.Background(), "job-8")
	if err != nil || !ok {
		t.Fatalf("pause %v %v", ok, err)
	}
}

func TestListSchedulesDecode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/schedules" {
			t.Errorf("path %s", r.URL.Path)
		}
		w.Write([]byte(`{"schedules":[{"id":"j1","kind":"oneshot","dueAt":123,"status":"pending","createdAt":1,"firedCount":0}]}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	jobs, err := c.ListSchedules(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].ID != "j1" || jobs[0].Kind != wire.ScheduleKindOneshot {
		t.Fatalf("jobs %+v", jobs)
	}
}

func TestWorkflowStartAndSignal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/workflows":
			w.Write([]byte(`{"id":"run-1"}`))
		case "/api/workflows/run-1/signal":
			b, _ := io.ReadAll(r.Body)
			if !bytesHas(b, `"name":"go"`) {
				t.Errorf("signal body %s", b)
			}
			w.Write([]byte(`{"ok":true}`))
		default:
			t.Errorf("path %s", r.URL.Path)
		}
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	id, err := c.StartWorkflow(context.Background(), wire.WorkflowSpec{Name: "drip", Steps: nil})
	if err != nil || id != "run-1" {
		t.Fatalf("start %q %v", id, err)
	}
	ok, err := c.SignalWorkflow(context.Background(), "run-1", "go", nil)
	if err != nil || !ok {
		t.Fatalf("signal %v %v", ok, err)
	}
}

func TestAuthMeDecode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/auth/me" {
			t.Errorf("path %s", r.URL.Path)
		}
		w.Write([]byte(`{"user":{"kind":"machine","email":null,"name":null}}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	u, err := c.AuthMe(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if u.Kind != wire.UserKindMachine || u.Name != nil {
		t.Fatalf("user %+v", u)
	}
}
