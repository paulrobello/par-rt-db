// go-client/admin/ops_test.go
package admin

// Observability, config, and the admin owner-bypass query/mutate surfaces
// (ops.go): route/method pinning through the httptest stub, query-building
// for the optional filters, and the error-envelope path.

import (
	"context"
	"testing"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestMetrics(t *testing.T) {
	c := &capture{t: t, method: "GET", path: "/admin/metrics", bearer: wantStr(testAdminKey),
		status: 200, respBody: `{"queriesTotal":1,"mutationsTotal":2,"uptimeSeconds":3,` +
			`"queryLatency":{"p50":1,"p95":2,"p99":3},"perDbSubs":[],"presenceDetail":[],` +
			`"perDbQuota":[],"perDbWorkflows":[]}`}
	client := stub(t, c.handler)
	got, err := client.Metrics(context.Background())
	if err != nil {
		t.Fatalf("Metrics: %v", err)
	}
	if got.QueriesTotal != 1 || got.UptimeSeconds != 3 || got.QueryLatency.P95 != 2 {
		t.Fatalf("got %+v", got)
	}
}

func TestPresenceRooms(t *testing.T) {
	c := &capture{t: t, method: "GET", path: "/admin/presence", bearer: wantStr(testAdminKey),
		status: 200, respBody: `{"rooms":[{"room":"r","memberCount":1,"localMemberCount":1,"stateBytes":2,"oldestMemberAgeMs":3,"mergedWithPeers":false}]}`}
	client := stub(t, c.handler)
	got, err := client.PresenceRooms(context.Background())
	if err != nil {
		t.Fatalf("PresenceRooms: %v", err)
	}
	if len(got.Rooms) != 1 || got.Rooms[0].Room != "r" {
		t.Fatalf("got %+v", got)
	}
}

func TestListSubscriptions(t *testing.T) {
	for _, tc := range []struct {
		name string
		db   *string
		want map[string]string
	}{
		{"nil_db", nil, map[string]string{}},
		{"db_set", ptrStr("app"), map[string]string{"db": "app"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newQueryCheckServer(t, "/admin/subscriptions", tc.want,
				`{"subscriptions":[{"db":"app","table":"t","terminal":"select","readSetClass":"point","principal":null}],`+
					`"subsRerunsTotal":1,"subsSkipsPointTotal":0,"subsSkipsIndexedTotal":0,`+
					`"subsSkipsOrderedTotal":0,"subsMissedPushesTotal":0,"perDb":[]}`)
			client := NewAdminClient(srv.URL, testAdminKey)
			got, err := client.ListSubscriptions(context.Background(), tc.db)
			if err != nil {
				t.Fatalf("ListSubscriptions: %v", err)
			}
			if len(got.Subscriptions) != 1 || got.SubsRerunsTotal != 1 {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func TestGetConfig(t *testing.T) {
	c := &capture{t: t, method: "GET", path: "/admin/config", bearer: wantStr(testAdminKey),
		status: 200, respBody: configResponseFixture}
	client := stub(t, c.handler)
	got, err := client.GetConfig(context.Background())
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if got.Port != 8300 || got.Version != "1.0.0" {
		t.Fatalf("got %+v", got)
	}
}

// configResponseFixture is the full /admin/config body, shared by
// ops_test.go and any config-pinning test.
const configResponseFixture = `{"port":8300,"publicUrl":"http://x","githubBaseUrl":"","githubApiUrl":"",` +
	`"databaseUrlConfigured":true,"adminKeyConfigured":true,"githubConfigured":false,` +
	`"googleConfigured":false,"gitlabConfigured":false,"oidcConfigured":false,` +
	`"storageSignedUrls":{"mode":"databases","databases":["app"]},` +
	`"hot":{"allowedOrigins":[],"sessionTtlDays":30,"maxFileSize":1000,"idempotencyTtlMs":10,` +
	`"changeLogMaxRows":100000,"maxTablesPerDb":0,"maxStorageBytesPerDb":0,"maxSubsPerDb":0,` +
	`"perTokenRpm":1,"perDbRpm":2},"version":"1.0.0","gitCommit":"abc","admins":[]}`

func TestOpsRecent(t *testing.T) {
	for _, tc := range []struct {
		name  string
		table *string
		n     *int64
		want  map[string]string
	}{
		{"db_only", nil, nil, map[string]string{"db": "app"}},
		{"table_and_n", ptrStr("items"), ptrI64(5), map[string]string{"db": "app", "table": "items", "n": "5"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newQueryCheckServer(t, "/admin/ops/recent", tc.want,
				`{"ops":[{"db":"app","table":"items","docId":"d1","kind":"insert","ts":1,"owner":null,"seq":2,"feedEpoch":"e1"}]}`)
			client := NewAdminClient(srv.URL, testAdminKey)
			got, err := client.OpsRecent(context.Background(), "app", tc.table, tc.n)
			if err != nil {
				t.Fatalf("OpsRecent: %v", err)
			}
			if len(got) != 1 || got[0].DocID != "d1" || got[0].Seq != 2 {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func TestExplainQuery(t *testing.T) {
	c := &capture{t: t, method: "POST", path: "/admin/db/app/explain",
		status: 200, respBody: `{"sql":"SELECT * FROM t","params":["7"],"terminal":"select","warnings":[]}`}
	client := stub(t, c.handler)
	got, err := client.ExplainQuery(context.Background(), "app", wire.Query{Table: "t"})
	if err != nil {
		t.Fatalf("ExplainQuery: %v", err)
	}
	if got.SQL != "SELECT * FROM t" || len(got.Params) != 1 || got.Terminal != "select" {
		t.Fatalf("got %+v", got)
	}
}

func TestGetSlowQueries(t *testing.T) {
	for _, tc := range []struct {
		name  string
		db    *string
		limit *int64
		want  map[string]string
	}{
		{"nil_opts", nil, nil, map[string]string{}},
		{"db_and_limit", ptrStr("app"), ptrI64(5), map[string]string{"db": "app", "limit": "5"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newQueryCheckServer(t, "/admin/slow-queries", tc.want,
				`{"queries":[{"startedAtMs":1,"durationMs":900,"db":"app","table":"t","terminal":"select","sql":"s","params":[]}],"thresholdMs":500,"capacity":100}`)
			client := NewAdminClient(srv.URL, testAdminKey)
			got, err := client.GetSlowQueries(context.Background(), tc.db, tc.limit)
			if err != nil {
				t.Fatalf("GetSlowQueries: %v", err)
			}
			if len(got.Queries) != 1 || got.ThresholdMs != 500 || got.Capacity != 100 {
				t.Fatalf("got %+v", got)
			}
		})
	}
}

func TestAdminMutate(t *testing.T) {
	c := &capture{t: t, method: "POST", path: "/admin/db/app/mutate",
		status: 200, respBody: `{"results":[{"id":"d1"},{"patched":3}]}`}
	client := stub(t, c.handler)
	got, err := client.AdminMutate(context.Background(), "app", wire.Transaction{}, "idem-1")
	if err != nil {
		t.Fatalf("AdminMutate: %v", err)
	}
	if len(got) != 2 || got[0].ID == nil || *got[0].ID != "d1" || got[1].Patched == nil || *got[1].Patched != 3 {
		t.Fatalf("got %+v", got)
	}

	// The error envelope surfaces with its code.
	c2 := &capture{t: t, method: "POST", path: "/admin/db/app/mutate",
		status: 403, respBody: `{"code":"FORBIDDEN","message":"no"}`}
	client2 := stub(t, c2.handler)
	if _, err := client2.AdminMutate(context.Background(), "app", wire.Transaction{}, ""); !rtdberrors.IsCode(err, rtdberrors.CodeForbidden) {
		t.Fatalf("want FORBIDDEN, got %v", err)
	}
}
