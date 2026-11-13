// go-client/admin/manage_test.go
package admin

// Backup, webhook, audit, and interactive-session management routes
// (manage.go): route/method/body pinning through the httptest stub, plus
// the {ok:false}-as-INTERNAL and error-envelope paths.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
)

func TestBackupNow(t *testing.T) {
	body := `{}`
	c := &capture{t: t, method: "POST", path: "/admin/backup", bearer: wantStr(testAdminKey), body: &body,
		status: 200, respBody: `{"ok":true}`}
	client := stub(t, c.handler)
	if err := client.BackupNow(context.Background()); err != nil {
		t.Fatalf("BackupNow: %v", err)
	}

	// ok=false surfaces as INTERNAL.
	c2 := &capture{t: t, method: "POST", path: "/admin/backup",
		status: 200, respBody: `{"ok":false}`}
	client2 := stub(t, c2.handler)
	if err := client2.BackupNow(context.Background()); !rtdberrors.IsCode(err, rtdberrors.CodeInternal) {
		t.Fatalf("want INTERNAL, got %v", err)
	}
}

func TestListBackups(t *testing.T) {
	c := &capture{t: t, method: "GET", path: "/admin/backups", bearer: wantStr(testAdminKey),
		status: 200, respBody: `{"running":false,"backups":[{"name":"a.dump","sizeBytes":10,"createdMs":5}]}`}
	client := stub(t, c.handler)
	got, err := client.ListBackups(context.Background())
	if err != nil {
		t.Fatalf("ListBackups: %v", err)
	}
	if got.Running || len(got.Backups) != 1 || got.Backups[0].Name != "a.dump" {
		t.Fatalf("got %+v", got)
	}
}

func TestDeleteBackup(t *testing.T) {
	c := &capture{t: t, method: "DELETE", path: "/admin/backups/x.dump", bearer: wantStr(testAdminKey),
		status: 200, respBody: `{"ok":true}`}
	client := stub(t, c.handler)
	if err := client.DeleteBackup(context.Background(), "x.dump"); err != nil {
		t.Fatalf("DeleteBackup: %v", err)
	}
}

func TestListWebhooks(t *testing.T) {
	c := &capture{t: t, method: "GET", path: "/admin/db/app/webhooks", bearer: wantStr(testAdminKey),
		status: 200, respBody: `{"webhooks":[{"id":1,"db":"app","table":null,"url":"http://x",` +
			`"events":["insert"],"createdAt":1,"enabled":true,"secret":null}]}`}
	client := stub(t, c.handler)
	got, err := client.ListWebhooks(context.Background(), "app")
	if err != nil {
		t.Fatalf("ListWebhooks: %v", err)
	}
	if len(got) != 1 || got[0].ID != 1 || got[0].URL != "http://x" || got[0].Table != nil {
		t.Fatalf("got %+v", got)
	}
}

func TestCreateWebhook(t *testing.T) {
	body := `{"url":"http://x","table":"t","events":["insert"],"enabled":true}`
	c := &capture{t: t, method: "POST", path: "/admin/db/app/webhooks", body: &body,
		status: 200, respBody: `{"id":5}`}
	client := stub(t, c.handler)
	id, err := client.CreateWebhook(context.Background(), "app", CreateWebhookOptions{
		URL: "http://x", Table: ptrStr("t"), Events: []string{"insert"}, Enabled: ptrBool(true),
	})
	if err != nil || id != 5 {
		t.Fatalf("CreateWebhook: id=%d err=%v", id, err)
	}
}

func TestEditWebhook(t *testing.T) {
	body := `{"url":"http://y","table":null,"events":["insert","update"],"rotateSecret":true}`
	c := &capture{t: t, method: "PUT", path: "/admin/db/app/webhooks/5", body: &body,
		status: 200, respBody: `{"id":5,"db":"app","table":null,"url":"http://y","events":["insert"],"createdAt":1,"enabled":true,"secret":"rotated"}`}
	client := stub(t, c.handler)
	got, err := client.EditWebhook(context.Background(), "app", 5, WebhookEditOptions{
		URL:          ptrStr("http://y"),
		Table:        TableNull,
		Events:       []string{"insert", "update"},
		RotateSecret: ptrBool(true),
	})
	if err != nil {
		t.Fatalf("EditWebhook: %v", err)
	}
	if got.URL != "http://y" || got.Secret == nil || !got.Enabled {
		t.Fatalf("got %+v", got)
	}
}

func TestDeleteWebhook(t *testing.T) {
	c := &capture{t: t, method: "DELETE", path: "/admin/db/app/webhooks/5",
		status: 200, respBody: `{"ok":true}`}
	client := stub(t, c.handler)
	if err := client.DeleteWebhook(context.Background(), "app", 5); err != nil {
		t.Fatalf("DeleteWebhook: %v", err)
	}
}

func TestListDeliveries(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts *ListDeliveriesOptions
		want map[string]string
	}{
		{"nil_opts", nil, map[string]string{}},
		{"all_filters", &ListDeliveriesOptions{Status: ptrStr("failed"), Limit: ptrI64(10), Offset: ptrI64(20)},
			map[string]string{"status": "failed", "limit": "10", "offset": "20"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newQueryCheckServer(t, "/admin/db/app/webhooks/7/deliveries", tc.want,
				`{"deliveries":[{"id":1,"attempts":1,"status":"failed","nextAttempt":2,"lastError":null,"payload":{"a":1}}]}`)
			client := NewAdminClient(srv.URL, testAdminKey)
			got, err := client.ListDeliveries(context.Background(), "app", 7, tc.opts)
			if err != nil {
				t.Fatalf("ListDeliveries: %v", err)
			}
			if len(got) != 1 || got[0].ID != 1 || got[0].LastError != nil {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func TestGetAudit(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts *AuditQuery
		want map[string]string
	}{
		{"db_only", nil, map[string]string{"db": "app"}},
		{"all_filters", &AuditQuery{
			Table: ptrStr("items"), Op: ptrStr("insert"), Principal: ptrStr("u"),
			Source: ptrStr("api"), Limit: ptrI64(5), Offset: ptrI64(10),
		}, map[string]string{"db": "app", "table": "items", "op": "insert", "principal": "u", "source": "api", "limit": "5", "offset": "10"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newQueryCheckServer(t, "/admin/audit", tc.want,
				`{"entries":[{"id":1,"tsMs":2,"db":"app","table":"items","op":"insert","docId":"d1","principal":null,"source":"api"}]}`)
			client := NewAdminClient(srv.URL, testAdminKey)
			got, err := client.GetAudit(context.Background(), "app", tc.opts)
			if err != nil {
				t.Fatalf("GetAudit: %v", err)
			}
			if len(got) != 1 || got[0].ID != 1 || got[0].DocID != "d1" {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func TestListSessions(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts *SessionListOptions
		want map[string]string
	}{
		{"nil_opts", nil, map[string]string{}},
		{"user_and_limit", &SessionListOptions{User: ptrStr("u1"), Limit: ptrI64(3)},
			map[string]string{"user": "u1", "limit": "3"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newQueryCheckServer(t, "/admin/sessions", tc.want,
				`{"sessions":[{"tokenHash":"h","userId":"u1","email":null,"name":null,"login":null,"anonymous":false,"createdAt":1,"expiresAt":2}]}`)
			client := NewAdminClient(srv.URL, testAdminKey)
			got, err := client.ListSessions(context.Background(), tc.opts)
			if err != nil {
				t.Fatalf("ListSessions: %v", err)
			}
			if len(got) != 1 || got[0].TokenHash != "h" || got[0].UserID != "u1" {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func TestRevokeSession(t *testing.T) {
	c := &capture{t: t, method: "DELETE", path: "/admin/sessions/abc123",
		status: 200, respBody: `{"ok":true}`}
	client := stub(t, c.handler)
	if err := client.RevokeSession(context.Background(), "abc123"); err != nil {
		t.Fatalf("RevokeSession: %v", err)
	}
}

func TestRevokeUserSessions(t *testing.T) {
	c := &capture{t: t, method: "DELETE", path: "/admin/sessions",
		status: 200, respBody: `{"ok":true,"revoked":2}`}
	client := stub(t, c.handler)
	got, err := client.RevokeUserSessions(context.Background(), "u1")
	if err != nil {
		t.Fatalf("RevokeUserSessions: %v", err)
	}
	if !got.OK || got.Revoked != 2 {
		t.Fatalf("got %+v", got)
	}
	// The error envelope surfaces with its code.
	c2 := &capture{t: t, method: "DELETE", path: "/admin/sessions",
		status: 401, respBody: `{"code":"UNAUTHORIZED","message":"no"}`}
	client2 := stub(t, c2.handler)
	if _, err := client2.RevokeUserSessions(context.Background(), "u1"); !rtdberrors.IsCode(err, rtdberrors.CodeUnauthorized) {
		t.Fatalf("want UNAUTHORIZED, got %v", err)
	}
}

func TestRevokeExpiredSessions(t *testing.T) {
	c := &capture{t: t, method: "DELETE", path: "/admin/sessions",
		status: 200, respBody: `{"ok":true,"revoked":0}`}
	client := stub(t, c.handler)
	got, err := client.RevokeExpiredSessions(context.Background())
	if err != nil {
		t.Fatalf("RevokeExpiredSessions: %v", err)
	}
	if !got.OK || got.Revoked != 0 {
		t.Fatalf("got %+v", got)
	}
}

func TestMergeUsersError(t *testing.T) {
	c := &capture{t: t, method: "POST", path: "/admin/merge-users",
		status: 400, respBody: `{"code":"BAD_REQUEST","message":"same user"}`}
	client := stub(t, c.handler)
	if _, err := client.MergeUsers(context.Background(), "u1", "u1"); !rtdberrors.IsCode(err, rtdberrors.CodeBadRequest) {
		t.Fatalf("want BAD_REQUEST, got %v", err)
	}
}

// newQueryCheckServer spins an httptest server asserting method, path, and
// that every wanted query key carries the expected value.
func newQueryCheckServer(t *testing.T, path string, wantQuery map[string]string, respBody string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodDelete {
			t.Errorf("method %s", r.Method)
		}
		if r.URL.Path != path {
			t.Errorf("got path %q want %q", r.URL.Path, path)
		}
		for k, v := range wantQuery {
			if got := r.URL.Query().Get(k); got != v {
				t.Errorf("query %s: got %q want %q", k, got, v)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(respBody))
	}))
	t.Cleanup(srv.Close)
	return srv
}
