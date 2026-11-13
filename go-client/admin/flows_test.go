// go-client/admin/flows_test.go
package admin

// Workflow runs, schedules, file storage, and the anonymous-access /
// read-only toggles (flows.go): route/method pinning through the httptest
// stub, query-building for the optional filters, and the error-envelope
// path.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/httpclient"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestListWorkflows(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts *WorkflowListOptions
		want map[string]string
	}{
		{"nil_opts", nil, map[string]string{}},
		{"status_and_limit", &WorkflowListOptions{Status: ptrWfStatus(wire.WorkflowStatusSuccess), Limit: ptrI64(3)},
			map[string]string{"status": "success", "limit": "3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newQueryCheckServer(t, "/admin/db/app/workflows", tc.want,
				`{"workflows":[{"id":"wf1","name":"drip","status":"success","currentStep":1,"stepCount":2,`+
					`"attempts":1,"createdAt":10,"updatedAt":20}]}`)
			client := NewAdminClient(srv.URL, testAdminKey)
			got, err := client.ListWorkflows(context.Background(), "app", tc.opts)
			if err != nil {
				t.Fatalf("ListWorkflows: %v", err)
			}
			if len(got) != 1 || got[0].ID != "wf1" || got[0].Status != wire.WorkflowStatusSuccess {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func ptrWfStatus(s wire.WorkflowStatus) *wire.WorkflowStatus { return &s }

func TestStartWorkflow(t *testing.T) {
	c := &capture{t: t, method: "POST", path: "/admin/db/app/workflows",
		status: 200, respBody: `{"id":"wf1"}`}
	client := stub(t, c.handler)
	id, err := client.StartWorkflow(context.Background(), "app",
		wire.WorkflowSpec{Name: "drip", Steps: []wire.WorkflowStepSpec{}})
	if err != nil {
		t.Fatalf("StartWorkflow: %v", err)
	}
	if id != "wf1" {
		t.Fatalf("got %q", id)
	}
	// Error envelope.
	c2 := &capture{t: t, method: "POST", path: "/admin/db/app/workflows",
		status: 404, respBody: `{"code":"NOT_FOUND","message":"no db"}`}
	client2 := stub(t, c2.handler)
	if _, err := client2.StartWorkflow(context.Background(), "app", wire.WorkflowSpec{}); !rtdberrors.IsCode(err, rtdberrors.CodeNotFound) {
		t.Fatalf("want NOT_FOUND, got %v", err)
	}
}

func TestCancelWorkflow(t *testing.T) {
	c := &capture{t: t, method: "POST", path: "/admin/db/app/workflows/wf1/cancel",
		status: 200, respBody: `{"ok":true}`}
	client := stub(t, c.handler)
	ok, err := client.CancelWorkflow(context.Background(), "app", "wf1")
	if err != nil || !ok {
		t.Fatalf("CancelWorkflow: %v %v", ok, err)
	}
}

// ok=false for an unknown run is a no-op result, not an error.
func TestDeleteWorkflowOKFalse(t *testing.T) {
	c := &capture{t: t, method: "DELETE", path: "/admin/db/app/workflows/wf1",
		status: 200, respBody: `{"ok":false}`}
	client := stub(t, c.handler)
	ok, err := client.DeleteWorkflow(context.Background(), "app", "wf1")
	if err != nil || ok {
		t.Fatalf("DeleteWorkflow: %v %v", ok, err)
	}
}

func TestListSchedules(t *testing.T) {
	c := &capture{t: t, method: "GET", path: "/admin/db/app/schedules", bearer: wantStr(testAdminKey),
		status: 200, respBody: `{"schedules":[{"id":"s1","kind":"oneshot","dueAt":99,"status":"pending",` +
			`"createdAt":1,"firedCount":0},{"id":"s2","kind":"cron","dueAt":100,"cron":"* * * * *","tz":"UTC",` +
			`"status":"running","createdAt":2,"firedCount":3,"external":true,"missedCount":1}]}`}
	client := stub(t, c.handler)
	got, err := client.ListSchedules(context.Background(), "app")
	if err != nil {
		t.Fatalf("ListSchedules: %v", err)
	}
	if len(got) != 2 || got[0].ID != "s1" || got[0].Kind != wire.ScheduleKindOneshot ||
		got[1].Cron == nil || !got[1].External || got[1].MissedCount != 1 {
		t.Fatalf("got %+v", got)
	}
}

func TestPauseResumeSchedule(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(c *AdminClient) (bool, error)
		path string
	}{
		{"pause", func(c *AdminClient) (bool, error) { return c.PauseSchedule(context.Background(), "app", "s1") }, "/admin/db/app/schedules/s1/pause"},
		{"resume", func(c *AdminClient) (bool, error) { return c.ResumeSchedule(context.Background(), "app", "s1") }, "/admin/db/app/schedules/s1/resume"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := &capture{t: t, method: "POST", path: tc.path,
				status: 200, respBody: `{"ok":true}`}
			client := stub(t, c.handler)
			ok, err := tc.call(client)
			if err != nil || !ok {
				t.Fatalf("%s: %v %v", tc.name, ok, err)
			}
		})
	}
}

func TestListFiles(t *testing.T) {
	c := &capture{t: t, method: "GET", path: "/admin/db/app/storage", bearer: wantStr(testAdminKey),
		status: 200, respBody: `{"files":[{"id":"f1","sha256":"aa","size":3,"contentType":"image/png","creationTime":7}]}`}
	client := stub(t, c.handler)
	got, err := client.ListFiles(context.Background(), "app")
	if err != nil {
		t.Fatalf("ListFiles: %v", err)
	}
	if len(got) != 1 || got[0].ID != "f1" || got[0].Size != 3 || got[0].ContentType == nil {
		t.Fatalf("got %+v", got)
	}
}

func TestDeleteFile(t *testing.T) {
	c := &capture{t: t, method: "DELETE", path: "/admin/db/app/storage/f1",
		status: 200, respBody: `{"ok":true}`}
	client := stub(t, c.handler)
	if err := client.DeleteFile(context.Background(), "app", "f1"); err != nil {
		t.Fatalf("DeleteFile: %v", err)
	}
	// Idempotent: a NOT_FOUND from a repeat delete still surfaces.
	c2 := &capture{t: t, method: "DELETE", path: "/admin/db/app/storage/f1",
		status: 404, respBody: `{"code":"NOT_FOUND","message":"gone"}`}
	client2 := stub(t, c2.handler)
	if err := client2.DeleteFile(context.Background(), "app", "f1"); !rtdberrors.IsCode(err, rtdberrors.CodeNotFound) {
		t.Fatalf("want NOT_FOUND, got %v", err)
	}
}

func TestGetSignedUrlAllTransforms(t *testing.T) {
	w, h, q := 512, 256, 80
	var gotQuery url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/storage/app/f1/signed-url" {
			t.Errorf("got %s %s", r.Method, r.URL.Path)
		}
		gotQuery = r.URL.Query()
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"url":"https://rtdb.example/f1?sig=a","expiresAt":5}`))
	}))
	defer srv.Close()
	client := NewAdminClient(srv.URL, testAdminKey)
	got, err := client.GetSignedUrl(context.Background(), "app", "f1", &httpclient.TransformOpts{
		W: &w, H: &h, Fit: httpclient.FitCover, Q: &q, Format: httpclient.FormatPNG,
	})
	if err != nil {
		t.Fatalf("GetSignedUrl: %v", err)
	}
	if got.URL != "https://rtdb.example/f1?sig=a" || got.ExpiresAt != 5 {
		t.Fatalf("got %+v", got)
	}
	for k, want := range map[string]string{"w": "512", "h": "256", "fit": "cover", "q": "80", "format": "png"} {
		if gotQuery.Get(k) != want {
			t.Errorf("query %s: got %q want %q", k, gotQuery.Get(k), want)
		}
	}
}

func TestGetSignedUrlAutoFormatOmitted(t *testing.T) {
	// format=auto is the server default and must not ride the query.
	var gotQuery url.Values
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query()
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"url":"u","expiresAt":1}`))
	}))
	defer srv.Close()
	client := NewAdminClient(srv.URL, testAdminKey)
	if _, err := client.GetSignedUrl(context.Background(), "app", "f1",
		&httpclient.TransformOpts{Format: "auto"}); err != nil {
		t.Fatalf("GetSignedUrl: %v", err)
	}
	if _, present := gotQuery["format"]; present {
		t.Fatalf("format must be omitted, got %v", gotQuery)
	}
}

func TestAnonymousAndReadOnlyToggles(t *testing.T) {
	anonBody := `{"enabled":true}`
	c := &capture{t: t, method: "GET", path: "/admin/db/app/anonymous-access", bearer: wantStr(testAdminKey),
		status: 200, respBody: `{"enabled":true}`}
	client := stub(t, c.handler)
	on, err := client.GetAnonymousAccess(context.Background(), "app")
	if err != nil || !on {
		t.Fatalf("GetAnonymousAccess: %v %v", on, err)
	}

	c2 := &capture{t: t, method: "PATCH", path: "/admin/db/app/anonymous-access", body: &anonBody,
		status: 200, respBody: `{"ok":true}`}
	client2 := stub(t, c2.handler)
	if err := client2.SetAnonymousAccess(context.Background(), "app", true); err != nil {
		t.Fatalf("SetAnonymousAccess: %v", err)
	}

	roBody := `{"readOnly":false}`
	c3 := &capture{t: t, method: "GET", path: "/admin/db/app/readonly", bearer: wantStr(testAdminKey),
		status: 200, respBody: `{"readOnly":false}`}
	client3 := stub(t, c3.handler)
	ro, err := client3.GetReadOnly(context.Background(), "app")
	if err != nil || ro {
		t.Fatalf("GetReadOnly: %v %v", ro, err)
	}

	c4 := &capture{t: t, method: "PATCH", path: "/admin/db/app/readonly", body: &roBody,
		status: 200, respBody: `{"ok":false}`}
	client4 := stub(t, c4.handler)
	if err := client4.SetReadOnly(context.Background(), "app", false); !rtdberrors.IsCode(err, rtdberrors.CodeInternal) {
		t.Fatalf("want INTERNAL on ok=false, got %v", err)
	}
}
