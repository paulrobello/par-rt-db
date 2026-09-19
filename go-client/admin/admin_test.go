// go-client/admin/admin_test.go
package admin

// Mirror rust-client/src/admin/tests.rs's mock pattern: each test spins an
// httptest server that asserts the on-the-wire request (route, bearer, body)
// and replies with the server's exact response shape.

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

const testAdminKey = "test-admin-key"

func stub(t *testing.T, handler http.HandlerFunc) *AdminClient {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return NewAdminClient(srv.URL, testAdminKey)
}

// capture records the request and replies with a fixed status + JSON body.
type capture struct {
	t      *testing.T
	method string
	path   string
	// headers asserted when non-nil
	bearer   *string
	body     *string
	ctype    *string
	status   int
	respBody string
}

func (c *capture) handler(w http.ResponseWriter, r *http.Request) {
	if r.Method != c.method {
		c.t.Errorf("route %s: got method %s want %s", c.path, r.Method, c.method)
	}
	if r.URL.Path != c.path {
		c.t.Errorf("got path %q want %q", r.URL.Path, c.path)
	}
	if c.bearer != nil && r.Header.Get("Authorization") != "Bearer "+*c.bearer {
		c.t.Errorf("got Authorization %q want Bearer <admin key>", r.Header.Get("Authorization"))
	}
	if c.ctype != nil && r.Header.Get("Content-Type") != *c.ctype {
		c.t.Errorf("got Content-Type %q want %q", r.Header.Get("Content-Type"), *c.ctype)
	}
	if c.body != nil {
		b, _ := io.ReadAll(r.Body)
		if !bodyEqual(b, *c.body) {
			c.t.Errorf("got body %s want %s", b, *c.body)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(c.status)
	_, _ = w.Write([]byte(c.respBody))
}

// bodyEqual asserts structurally when both sides parse as JSON (Go marshals
// maps with sorted keys; rust emits declaration order — key order is not
// part of the wire contract), else as trimmed raw bytes.
func bodyEqual(got []byte, want string) bool {
	var g, w any
	gErr := json.Unmarshal(got, &g)
	wErr := json.Unmarshal([]byte(want), &w)
	if gErr == nil && wErr == nil {
		gb, gOK := json.Marshal(g)
		wb, wOK := json.Marshal(w)
		return gOK == nil && wOK == nil && bytes.Equal(gb, wb)
	}
	return strings.TrimSpace(string(got)) == strings.TrimSpace(want)
}

func wantStr(s string) *string { return &s }

func TestCreateDB(t *testing.T) {
	body := `{"name":"app"}`
	c := &capture{t: t, method: "POST", path: "/admin/create-db", bearer: wantStr(testAdminKey), body: &body,
		status: 200, respBody: `{"ok":true}`}
	client := stub(t, c.handler)
	if err := client.CreateDB(context.Background(), "app"); err != nil {
		t.Fatalf("CreateDB: %v", err)
	}
}

func TestDeleteDBConfirmGuard(t *testing.T) {
	body := `{"name":"app","confirm":"app"}`
	c := &capture{t: t, method: "POST", path: "/admin/delete-db", body: &body,
		status: 200, respBody: `{"ok":true}`}
	client := stub(t, c.handler)
	if err := client.DeleteDB(context.Background(), "app", "app"); err != nil {
		t.Fatalf("DeleteDB: %v", err)
	}
}

func TestMintTokenBodyAndDefaults(t *testing.T) {
	body := `{"db":"app","name":"ci"}`
	c := &capture{t: t, method: "POST", path: "/admin/mint-token", body: &body,
		status: 200, respBody: `{"tokenId":"t1","token":"sk_live_x"}`}
	client := stub(t, c.handler)
	tok, err := client.MintToken(context.Background(), "app", "ci")
	if err != nil {
		t.Fatalf("MintToken: %v", err)
	}
	if tok.TokenID != "t1" || tok.Token != "sk_live_x" {
		t.Fatalf("got %+v", tok)
	}
}

func TestMintTokenWithOptionsOmitsUnset(t *testing.T) {
	tables := []string{"projects", "items"}
	c := &capture{t: t, method: "POST", path: "/admin/mint-token",
		body:   wantStr(`{"db":"app","name":"ci","expiresAt":123,"readOnly":true,"tables":["projects","items"]}`),
		status: 200, respBody: `{"tokenId":"t2","token":"k"}`}
	client := stub(t, c.handler)
	tok, err := client.MintTokenWithOptions(context.Background(), "app", "ci", MintTokenOptions{
		ExpiresAt: ptrI64(123), ReadOnly: ptrBool(true), Tables: tables,
	})
	if err != nil {
		t.Fatalf("MintTokenWithOptions: %v", err)
	}
	if tok.TokenID != "t2" {
		t.Fatalf("got %q", tok.TokenID)
	}
}

func TestPushSchemaRouteBody(t *testing.T) {
	schema := wire.Object(map[string]wire.JSONValue{
		"tables": wire.Object(map[string]wire.JSONValue{}),
	})
	var got wire.JSONValue
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/admin/push-schema" {
			t.Errorf("got %s %s", r.Method, r.URL.Path)
		}
		var m map[string]json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&m)
		if string(m["db"]) != `"app"` {
			t.Errorf("got db %s", m["db"])
		}
		s, err := wire.UnmarshalJSON(m["schema"])
		if err != nil {
			t.Fatalf("decode schema: %v", err)
		}
		got = s
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	client := NewAdminClient(srv.URL, testAdminKey)
	if err := client.PushSchema(context.Background(), "app", schema); err != nil {
		t.Fatalf("PushSchema: %v", err)
	}
	if !jsonEqual(got, schema) {
		t.Fatalf("schema payload drifted: %v vs %v", got, schema)
	}
}

func jsonEqual(a, b wire.JSONValue) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return bytes.Equal(ab, bb)
}

func ptrI64(v int64) *int64   { return &v }
func ptrBool(v bool) *bool    { return &v }
func ptrStr(v string) *string { return &v }

func TestGetSchemaDecodes(t *testing.T) {
	c := &capture{t: t, method: "GET", path: "/admin/dbs/app/schema",
		status: 200, respBody: `{"tables":{"t":{"columns":{"a":{"type":"string"}}}}}`}
	client := stub(t, c.handler)
	s, err := client.GetSchema(context.Background(), "app")
	if err != nil {
		t.Fatalf("GetSchema: %v", err)
	}
	m, ok := s.(wire.Object)
	if !ok || len(m["tables"].(wire.Object)) != 1 {
		t.Fatalf("got %v", s)
	}
}

func TestDBStatsUnknownFieldRejected(t *testing.T) {
	c := &capture{t: t, method: "GET", path: "/admin/dbs/app/stats",
		status: 200, respBody: `{"tables":[],"totalSizeBytes":1,"bogus":2}`}
	client := stub(t, c.handler)
	if _, err := client.DBStats(context.Background(), "app"); err == nil {
		t.Fatal("want unknown-field rejection, got nil error")
	}
}

func TestAdminQueryIncludeDeleted(t *testing.T) {
	// nil includeDeleted omits the key entirely.
	c := &capture{t: t, method: "POST", path: "/admin/db/app/query",
		body:   wantStr(`{"query":{"table":"t","count":true},"includeDeleted":true}`),
		status: 200, respBody: `{"result":5}`}
	client := stub(t, c.handler)
	q := wire.Query{Table: "t", Count: true}
	res, err := client.AdminQuery(context.Background(), "app", q, ptrBool(true))
	if err != nil {
		t.Fatalf("AdminQuery: %v", err)
	}
	if n, ok := res.(wire.Number); !ok || string(n) != "5" {
		t.Fatalf("got %v", res)
	}
}

func TestAdminQueryOmitsIncludeDeleted(t *testing.T) {
	c := &capture{t: t, method: "POST", path: "/admin/db/app/query",
		body:   wantStr(`{"query":{"table":"t","count":true}}`),
		status: 200, respBody: `{"result":5}`}
	client := stub(t, c.handler)
	q := wire.Query{Table: "t", Count: true}
	if _, err := client.AdminQuery(context.Background(), "app", q, nil); err != nil {
		t.Fatalf("AdminQuery: %v", err)
	}
}

func TestEnvelopeErrorSurfaces(t *testing.T) {
	c := &capture{t: t, method: "POST", path: "/admin/create-db",
		body:   wantStr(`{"name":"app"}`),
		status: 403, respBody: `{"code":"FORBIDDEN","message":"no"}`}
	client := stub(t, c.handler)
	err := client.CreateDB(context.Background(), "app")
	if !rtdberrors.IsCode(err, rtdberrors.CodeForbidden) {
		t.Fatalf("want FORBIDDEN, got %v", err)
	}
}

func TestManageScheduleOKFalse(t *testing.T) {
	c := &capture{t: t, method: "POST", path: "/admin/db/app/schedules/s1/cancel",
		status: 200, respBody: `{"ok":false}`}
	client := stub(t, c.handler)
	ok, err := client.CancelSchedule(context.Background(), "app", "s1")
	if err != nil {
		t.Fatalf("CancelSchedule: %v", err)
	}
	if ok {
		t.Fatal("want ok=false")
	}
}

func TestExportDBRaw(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/admin/export-db" || r.URL.Query().Get("db") != "app" {
			t.Errorf("got %s %s?%s", r.Method, r.URL.Path, r.URL.RawQuery)
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte("line1\nline2\n"))
	}))
	defer srv.Close()
	client := NewAdminClient(srv.URL, testAdminKey)
	out, err := client.ExportDB(context.Background(), "app")
	if err != nil {
		t.Fatalf("ExportDB: %v", err)
	}
	if out != "line1\nline2\n" {
		t.Fatalf("got %q", out)
	}
}

func TestImportDBNDJSON(t *testing.T) {
	c := &capture{t: t, method: "POST", path: "/admin/import-db",
		ctype:  wantStr("application/x-ndjson"),
		body:   wantStr("line1\nline2\n"),
		status: 200, respBody: `{"ok":true}`}
	client := stub(t, c.handler)
	if err := client.ImportDB(context.Background(), "app", "line1\nline2\n"); err != nil {
		t.Fatalf("ImportDB: %v", err)
	}
}

func TestUploadFileRawBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/admin/db/app/storage" {
			t.Errorf("got %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Content-Type") != "image/png" {
			t.Errorf("got Content-Type %q", r.Header.Get("Content-Type"))
		}
		b, _ := io.ReadAll(r.Body)
		if !bytes.Equal(b, []byte{1, 2, 3}) {
			t.Errorf("got body %v", b)
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"id":"blob1"}`))
	}))
	defer srv.Close()
	client := NewAdminClient(srv.URL, testAdminKey)
	id, err := client.UploadFile(context.Background(), "app", []byte{1, 2, 3}, ptrStr("image/png"))
	if err != nil {
		t.Fatalf("UploadFile: %v", err)
	}
	if id != "blob1" {
		t.Fatalf("got %q", id)
	}
}

func TestDownloadBackupRawBytes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/admin/backups/x.dump" {
			t.Errorf("got %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(200)
		_, _ = w.Write([]byte{0, 1, 2, 255})
	}))
	defer srv.Close()
	client := NewAdminClient(srv.URL, testAdminKey)
	out, err := client.DownloadBackup(context.Background(), "x.dump")
	if err != nil {
		t.Fatalf("DownloadBackup: %v", err)
	}
	if !bytes.Equal(out, []byte{0, 1, 2, 255}) {
		t.Fatalf("got %v", out)
	}
}

func TestRestoreBackupSendsConfirmName(t *testing.T) {
	body := `{"name":"d.dump","confirm":"d.dump"}`
	c := &capture{t: t, method: "POST", path: "/admin/restore", body: &body,
		status: 200, respBody: `{"target":"rtdb_restored_1","instructions":"cut over"}`}
	client := stub(t, c.handler)
	res, err := client.RestoreBackup(context.Background(), "d.dump")
	if err != nil {
		t.Fatalf("RestoreBackup: %v", err)
	}
	if res.Target != "rtdb_restored_1" {
		t.Fatalf("got %+v", res)
	}
}

func TestMigrateSetDefaultDryRun(t *testing.T) {
	c := &capture{t: t, method: "POST", path: "/admin/db/app/migrate",
		body:   wantStr(`{"directives":[{"op":"setDefault","table":"t","field":"a","value":7}],"dryRun":true}`),
		status: 200, respBody: `{"applied":false,"schema":{"tables":{}},"directives":[{"op":"setDefault","affectedRows":0}]}`}
	client := stub(t, c.handler)
	res, err := client.MigrateSchema(context.Background(), "app", []Directive{
		DirectiveSetDefault{Table: "t", Field: "a", Value: wire.Number("7")},
	}, true)
	if err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	if res.Applied {
		t.Fatal("want applied=false")
	}
	if len(res.Directives) != 1 || res.Directives[0].Op != "setDefault" {
		t.Fatalf("got %+v", res.Directives)
	}
}

func TestMergeUsersDecodes(t *testing.T) {
	c := &capture{t: t, method: "POST", path: "/admin/merge-users",
		body:   wantStr(`{"anonUserId":"u1","realUserId":"u2","confirm":"u2"}`),
		status: 200, respBody: `{"dbs":{"app":{"tables":{"t":2},"conflicts":[]}},"storageRepointed":1,"sessionsRepointed":0,"anonDeleted":true}`}
	client := stub(t, c.handler)
	rep, err := client.MergeUsers(context.Background(), "u1", "u2")
	if err != nil {
		t.Fatalf("MergeUsers: %v", err)
	}
	app := rep.Dbs["app"]
	if app.Tables["t"] != 2 || !rep.AnonDeleted {
		t.Fatalf("got %+v", rep)
	}
}

func TestGetWorkflowDecodes(t *testing.T) {
	c := &capture{t: t, method: "GET", path: "/admin/db/app/workflows/wf1",
		status: 200, respBody: `{"id":"wf1","name":"drip","status":"success","currentStep":1,"stepCount":2,` +
			`"attempts":1,"createdAt":10,"updatedAt":20,"stepOutcomes":[{"stepIndex":0,"status":"success","attempts":1,"at":11}]}`}
	client := stub(t, c.handler)
	full, err := client.GetWorkflow(context.Background(), "app", "wf1")
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if full.ID != "wf1" || full.Status != wire.WorkflowStatusSuccess || len(full.StepOutcomes) != 1 {
		t.Fatalf("got %+v", full)
	}
}

func TestSignalWorkflowPayloadOmitted(t *testing.T) {
	c := &capture{t: t, method: "POST", path: "/admin/db/app/workflows/signal-test/signal",
		body:   wantStr(`{"name":"go"}`),
		status: 200, respBody: `{"ok":true}`}
	client := stub(t, c.handler)
	ok, err := client.SignalWorkflow(context.Background(), "app", "signal-test", "go", nil)
	if err != nil || !ok {
		t.Fatalf("SignalWorkflow: %v %v", ok, err)
	}
}
