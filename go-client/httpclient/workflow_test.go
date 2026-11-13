// go-client/httpclient/workflow_test.go
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

// StartWorkflow posts {db, spec} and returns the run id.
func TestStartWorkflow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/workflows" {
			t.Errorf("path %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		if err := json.Unmarshal(b, &body); err != nil {
			t.Errorf("body %s", b)
		}
		if body["db"] != "d1" {
			t.Errorf("db %v", body["db"])
		}
		spec, _ := body["spec"].(map[string]any)
		if spec == nil || spec["name"] != "wf" {
			t.Errorf("spec %v", body["spec"])
		}
		w.Write([]byte(`{"id":"run1"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	id, err := c.StartWorkflow(context.Background(), wire.WorkflowSpec{Name: "wf", Steps: []wire.WorkflowStepSpec{
		{Txn: &wire.Transaction{}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if id != "run1" {
		t.Fatalf("id %q", id)
	}
}

// CancelWorkflow posts to the path-escaped run's cancel route.
func TestCancelWorkflow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The id goes out PathEscaped; the handler sees the decoded form.
		if got := r.URL.EscapedPath(); got != "/api/workflows/run%2Fx/cancel" {
			t.Errorf("path %s", got)
		}
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	ok, err := c.CancelWorkflow(context.Background(), "run/x")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatalf("ok %v", ok)
	}
}

// SignalWorkflow carries the name and the optional payload; a nil payload is
// omitted from the body.
func TestSignalWorkflow(t *testing.T) {
	var gotPayload bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/workflows/run1/signal" {
			t.Errorf("path %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(b, &body)
		if body["name"] != "go" {
			t.Errorf("name %v", body["name"])
		}
		_, gotPayload = body["payload"]
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")

	ok, err := c.SignalWorkflow(context.Background(), "run1", "go", wire.Object{"k": wire.String("v")})
	if err != nil || !ok {
		t.Fatalf("ok %v err %v", ok, err)
	}
	if !gotPayload {
		t.Fatal("payload must be present when non-nil")
	}

	ok, err = c.SignalWorkflow(context.Background(), "run1", "go", nil)
	if err != nil {
		t.Fatal(err)
	}
	if gotPayload {
		t.Fatal("payload must be omitted when nil")
	}
}

// ListWorkflows posts to /list and decodes the workflows array; status
// filtering is included only when non-nil.
func TestListWorkflows(t *testing.T) {
	var sawStatus bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/workflows/list" {
			t.Errorf("path %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		_ = json.Unmarshal(b, &body)
		_, sawStatus = body["status"]
		w.Write([]byte(`{"workflows":[{"id":"r1","name":"wf","status":"running","currentStep":0,"stepCount":2,"attempts":1,"createdAt":1,"updatedAt":2}]}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")

	runs, err := c.ListWorkflows(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 || runs[0].ID != "r1" || runs[0].Status != wire.WorkflowStatusRunning {
		t.Fatalf("runs %+v", runs)
	}
	if sawStatus {
		t.Fatal("nil status must be omitted from the body")
	}

	running := wire.WorkflowStatusRunning
	if _, err = c.ListWorkflows(context.Background(), &running); err != nil {
		t.Fatal(err)
	}
	if !sawStatus {
		t.Fatal("non-nil status must be sent")
	}
}
