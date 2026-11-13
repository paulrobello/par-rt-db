// go-client/admin/dbs_extra_test.go
package admin

// The dbs.go routes not pinned by admin_test.go: schema preview/history,
// allowlist, admins, tokens, and clone. Same stub patterns as the existing
// dbs tests (capture + query-checking servers).

import (
	"context"
	"testing"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestPreviewSchema(t *testing.T) {
	c := &capture{t: t, method: "POST", path: "/admin/db/app/schema/preview",
		status: 200, respBody: `{"added":[{"table":"t","columns":[{"name":"a","fieldType":"string"}],"indexes":[]}],"rejected":[]}`}
	client := stub(t, c.handler)
	got, err := client.PreviewSchema(context.Background(), "app",
		wire.Object(map[string]wire.JSONValue{"tables": wire.Object(map[string]wire.JSONValue{})}))
	if err != nil {
		t.Fatalf("PreviewSchema: %v", err)
	}
	if len(got.Added) != 1 || got.Added[0].Table != "t" || len(got.Added[0].Columns) != 1 {
		t.Fatalf("got %+v", got)
	}
	// Error envelope.
	c2 := &capture{t: t, method: "POST", path: "/admin/db/app/schema/preview",
		status: 400, respBody: `{"code":"BAD_REQUEST","message":"bad schema"}`}
	client2 := stub(t, c2.handler)
	if _, err := client2.PreviewSchema(context.Background(), "app", wire.Object{}); !rtdberrors.IsCode(err, rtdberrors.CodeBadRequest) {
		t.Fatalf("want BAD_REQUEST, got %v", err)
	}
}

func TestListDBs(t *testing.T) {
	c := &capture{t: t, method: "GET", path: "/admin/dbs", bearer: wantStr(testAdminKey),
		status: 200, respBody: `{"databases":["a","b"]}`}
	client := stub(t, c.handler)
	got, err := client.ListDBs(context.Background())
	if err != nil {
		t.Fatalf("ListDBs: %v", err)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("got %v", got)
	}
}

func TestRevokeToken(t *testing.T) {
	body := `{"tokenId":"t1"}`
	c := &capture{t: t, method: "POST", path: "/admin/revoke-token", body: &body,
		status: 200, respBody: `{"ok":true}`}
	client := stub(t, c.handler)
	if err := client.RevokeToken(context.Background(), "t1"); err != nil {
		t.Fatalf("RevokeToken: %v", err)
	}
}

func TestAllowlist(t *testing.T) {
	addBody := `{"db":"app","action":"add","email":"a@b.c"}`
	c := &capture{t: t, method: "POST", path: "/admin/allowlist", body: &addBody,
		status: 200, respBody: `{"ok":true}`}
	client := stub(t, c.handler)
	if err := client.AllowlistAdd(context.Background(), "app", "a@b.c"); err != nil {
		t.Fatalf("AllowlistAdd: %v", err)
	}

	rmBody := `{"db":"app","action":"remove","email":"a@b.c"}`
	c2 := &capture{t: t, method: "POST", path: "/admin/allowlist", body: &rmBody,
		status: 200, respBody: `{"ok":true}`}
	client2 := stub(t, c2.handler)
	if err := client2.AllowlistRemove(context.Background(), "app", "a@b.c"); err != nil {
		t.Fatalf("AllowlistRemove: %v", err)
	}

	srv := newQueryCheckServer(t, "/admin/allowlist", map[string]string{"db": "app"},
		`{"emails":["a@b.c","d@e.f"]}`)
	client3 := NewAdminClient(srv.URL, testAdminKey)
	got, err := client3.AllowlistList(context.Background(), "app")
	if err != nil {
		t.Fatalf("AllowlistList: %v", err)
	}
	if len(got) != 2 || got[0] != "a@b.c" {
		t.Fatalf("got %v", got)
	}
}

func TestAdminsListAddRemove(t *testing.T) {
	listC := &capture{t: t, method: "GET", path: "/admin/admins", bearer: wantStr(testAdminKey),
		status: 200, respBody: `{"admins":[{"email":"a@b.c","githubId":7},{"email":"d@e.f","githubId":null}]}`}
	client := stub(t, listC.handler)
	got, err := client.AdminsList(context.Background())
	if err != nil {
		t.Fatalf("AdminsList: %v", err)
	}
	if len(got) != 2 || got[0].GithubID == nil || got[1].GithubID != nil {
		t.Fatalf("got %+v", got)
	}

	// githubId nil must be omitted from the add body; set must be carried.
	addBody := `{"email":"a@b.c","githubId":7}`
	c := &capture{t: t, method: "POST", path: "/admin/admins", body: &addBody,
		status: 200, respBody: `{"ok":true}`}
	client2 := stub(t, c.handler)
	if err := client2.AdminsAdd(context.Background(), "a@b.c", ptrI64(7)); err != nil {
		t.Fatalf("AdminsAdd: %v", err)
	}
	noGH := `{"email":"d@e.f"}`
	c2 := &capture{t: t, method: "POST", path: "/admin/admins", body: &noGH,
		status: 200, respBody: `{"ok":true}`}
	client3 := stub(t, c2.handler)
	if err := client3.AdminsAdd(context.Background(), "d@e.f", nil); err != nil {
		t.Fatalf("AdminsAdd(nil githubId): %v", err)
	}

	rmBody := `{"email":"a@b.c"}`
	c3 := &capture{t: t, method: "DELETE", path: "/admin/admins", body: &rmBody,
		status: 200, respBody: `{"ok":true}`}
	client4 := stub(t, c3.handler)
	if err := client4.AdminsRemove(context.Background(), "a@b.c"); err != nil {
		t.Fatalf("AdminsRemove: %v", err)
	}
}

func TestSchemaHistoryPaging(t *testing.T) {
	for _, tc := range []struct {
		name   string
		limit  *int64
		offset *int64
		want   map[string]string
	}{
		{"nil_paging", nil, nil, map[string]string{}},
		{"limit_offset", ptrI64(10), ptrI64(20), map[string]string{"limit": "10", "offset": "20"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newQueryCheckServer(t, "/admin/db/app/schema/history", tc.want,
				`{"entries":[{"version":3,"capturedAt":42,"source":"push","principal":null}]}`)
			client := NewAdminClient(srv.URL, testAdminKey)
			got, err := client.SchemaHistory(context.Background(), "app", tc.limit, tc.offset)
			if err != nil {
				t.Fatalf("SchemaHistory: %v", err)
			}
			if len(got) != 1 || got[0].Version != 3 {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func TestSchemaHistoryGet(t *testing.T) {
	c := &capture{t: t, method: "GET", path: "/admin/db/app/schema/history/3", bearer: wantStr(testAdminKey),
		status: 200, respBody: `{"version":3,"capturedAt":42,"source":"push","principal":"p","schema":{"tables":{"t":{"columns":{"a":{"type":"string"}}}}}}`}
	client := stub(t, c.handler)
	got, err := client.SchemaHistoryGet(context.Background(), "app", 3)
	if err != nil {
		t.Fatalf("SchemaHistoryGet: %v", err)
	}
	obj, ok := got.Schema.(wire.Object)
	if !ok || got.Version != 3 || got.Principal == nil {
		t.Fatalf("got %+v schema %#v", got, got.Schema)
	}
	tables := obj["tables"].(wire.Object)
	if len(tables) != 1 {
		t.Fatalf("got schema tables %v", tables)
	}
}

func TestRestoreSchema(t *testing.T) {
	body := `{"version":3,"confirm":"app"}`
	c := &capture{t: t, method: "POST", path: "/admin/db/app/schema/restore", body: &body,
		status: 200, respBody: `{"restoredTo":3}`}
	client := stub(t, c.handler)
	got, err := client.RestoreSchema(context.Background(), "app", 3, "app")
	if err != nil || got != 3 {
		t.Fatalf("RestoreSchema: got=%d err=%v", got, err)
	}
}

func TestListTokens(t *testing.T) {
	c := &capture{t: t, method: "GET", path: "/admin/tokens", bearer: wantStr(testAdminKey),
		status: 200, respBody: `{"tokens":[{"id":"t1","name":"ci","createdAt":1,"revoked":false,` +
			`"expiresAt":null,"readOnly":false,"tables":["items"]}]}`}
	client := stub(t, c.handler)
	got, err := client.ListTokens(context.Background(), "app")
	if err != nil {
		t.Fatalf("ListTokens: %v", err)
	}
	if len(got) != 1 || got[0].ID != "t1" || got[0].HostCapability != nil ||
		len(got[0].Tables) != 1 || got[0].Tables[0] != "items" {
		t.Fatalf("got %+v", got)
	}
}

func TestCloneDB(t *testing.T) {
	c := &capture{t: t, method: "POST", path: "/admin/clone-db", bearer: wantStr(testAdminKey),
		status: 200, respBody: `{"ok":true}`}
	client := stub(t, c.handler)
	if err := client.CloneDB(context.Background(), "a", "b"); err != nil {
		t.Fatalf("CloneDB: %v", err)
	}
	// ok=false surfaces as INTERNAL (the expect_ok analog).
	c2 := &capture{t: t, method: "POST", path: "/admin/clone-db",
		status: 200, respBody: `{"ok":false}`}
	client2 := stub(t, c2.handler)
	if err := client2.CloneDB(context.Background(), "a", "b"); !rtdberrors.IsCode(err, rtdberrors.CodeInternal) {
		t.Fatalf("want INTERNAL, got %v", err)
	}
}

func TestImportDBAckDecodeError(t *testing.T) {
	// decodeImportAck's decode-error branch: a non-JSON ack body.
	c := &capture{t: t, method: "POST", path: "/admin/import-db",
		ctype:  wantStr("application/x-ndjson"),
		body:   wantStr("line1\n"),
		status: 200, respBody: `not-json`}
	client := stub(t, c.handler)
	if err := client.ImportDB(context.Background(), "app", "line1\n"); err == nil {
		t.Fatal("non-JSON ack must error")
	}
}
