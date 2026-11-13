// go-client/admin/decode_test.go
package admin

// Strict-decode coverage for decode.go's UnmarshalJSON family: each DTO's
// happy path decodes the server's exact wire shape, and every type rejects
// unknown fields (the wire family convention). The dynamic-blob decoders
// additionally reject a malformed inner blob.

import (
	"encoding/json"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// decodeHappyTable drives two passes: a strict decode of body (with check
// assertions on the decoded pointer), then the same body with an injected
// unknown field, which must error. Bodies are top-level objects ending in
// '}'.
var decodeHappyTable = []struct {
	name   string
	body   string
	target func() any
	check  func(t *testing.T, v any)
}{
	{
		name:   "MintedToken",
		body:   `{"tokenId":"t1","token":"sk"}`,
		target: func() any { return &MintedToken{} },
		check: func(t *testing.T, v any) {
			got := v.(*MintedToken)
			if got.TokenID != "t1" || got.Token != "sk" {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "AdminMember",
		body:   `{"email":"a@b.c","githubId":7}`,
		target: func() any { return &AdminMember{} },
		check: func(t *testing.T, v any) {
			got := v.(*AdminMember)
			if got.Email != "a@b.c" || got.GithubID == nil || *got.GithubID != 7 {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "TableStat",
		body:   `{"name":"t","rowCount":2,"sizeBytes":30}`,
		target: func() any { return &TableStat{} },
		check: func(t *testing.T, v any) {
			got := v.(*TableStat)
			if got.Name != "t" || got.RowCount != 2 || got.SizeBytes != 30 {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name: "DbStats",
		body: `{"tables":[{"name":"t","rowCount":2,"sizeBytes":30}],"totalSizeBytes":30,` +
			`"tablesQuota":10,"tablesUsed":1,"storageQuotaBytes":1000,"storageUsedBytes":30,` +
			`"subsQuota":100,"subsUsed":3,"creationId":"c1"}`,
		target: func() any { return &DbStats{} },
		check: func(t *testing.T, v any) {
			got := v.(*DbStats)
			if got.TotalSizeBytes != 30 || got.CreationID != "c1" || len(got.Tables) != 1 ||
				got.SubsUsed != 3 || got.StorageQuotaBytes != 1000 {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "SessionInfo",
		body:   `{"tokenHash":"h","userId":"u","email":null,"name":"n","login":"l","anonymous":false,"createdAt":1,"expiresAt":2}`,
		target: func() any { return &SessionInfo{} },
		check: func(t *testing.T, v any) {
			got := v.(*SessionInfo)
			if got.TokenHash != "h" || got.UserID != "u" || got.Email != nil || got.Name == nil || got.Anonymous {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "RevokeUserSessionsResponse",
		body:   `{"ok":true,"revoked":3}`,
		target: func() any { return &RevokeUserSessionsResponse{} },
		check: func(t *testing.T, v any) {
			got := v.(*RevokeUserSessionsResponse)
			if !got.OK || got.Revoked != 3 {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "MergeConflict",
		body:   `{"table":"t","id":"d1"}`,
		target: func() any { return &MergeConflict{} },
		check: func(t *testing.T, v any) {
			got := v.(*MergeConflict)
			if got.Table != "t" || got.ID != "d1" {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "MergeDbResult",
		body:   `{"tables":{"t":2},"conflicts":[{"table":"u","id":"d2"}]}`,
		target: func() any { return &MergeDbResult{} },
		check: func(t *testing.T, v any) {
			got := v.(*MergeDbResult)
			if got.Tables["t"] != 2 || len(got.Conflicts) != 1 || got.Conflicts[0].ID != "d2" {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "MergeReport",
		body:   `{"dbs":{"app":{"tables":{"t":2},"conflicts":[]}},"storageRepointed":1,"sessionsRepointed":0,"anonDeleted":true}`,
		target: func() any { return &MergeReport{} },
		check: func(t *testing.T, v any) {
			got := v.(*MergeReport)
			if got.Dbs["app"].Tables["t"] != 2 || got.StorageRepointed != 1 || !got.AnonDeleted {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "LatencyStats",
		body:   `{"p50":1,"p95":2,"p99":3}`,
		target: func() any { return &LatencyStats{} },
		check: func(t *testing.T, v any) {
			got := v.(*LatencyStats)
			if got.P50 != 1 || got.P95 != 2 || got.P99 != 3 {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "PresenceRoomInspect",
		body:   `{"room":"r","memberCount":2,"localMemberCount":1,"stateBytes":10,"oldestMemberAgeMs":5,"mergedWithPeers":true}`,
		target: func() any { return &PresenceRoomInspect{} },
		check: func(t *testing.T, v any) {
			got := v.(*PresenceRoomInspect)
			if got.Room != "r" || got.MemberCount != 2 || !got.MergedWithPeers {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "PresenceRoomsResponse",
		body:   `{"rooms":[{"room":"r","memberCount":1,"localMemberCount":1,"stateBytes":2,"oldestMemberAgeMs":3,"mergedWithPeers":false}]}`,
		target: func() any { return &PresenceRoomsResponse{} },
		check: func(t *testing.T, v any) {
			got := v.(*PresenceRoomsResponse)
			if len(got.Rooms) != 1 || got.Rooms[0].Room != "r" {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "SubscriptionsPrincipal",
		body:   `{"userId":"u","email":"e@x"}`,
		target: func() any { return &SubscriptionsPrincipal{} },
		check: func(t *testing.T, v any) {
			got := v.(*SubscriptionsPrincipal)
			if got.UserID == nil || *got.UserID != "u" || got.Email == nil {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "SubscriptionInfo",
		body:   `{"db":"app","table":"t","terminal":"select","readSetClass":"point","principal":null}`,
		target: func() any { return &SubscriptionInfo{} },
		check: func(t *testing.T, v any) {
			got := v.(*SubscriptionInfo)
			if got.DB != "app" || got.Table != "t" || got.ReadSetClass != "point" || got.Principal != nil {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "DbSubCounters",
		body:   `{"db":"app","reruns":1,"skipsPoint":2,"skipsIndexed":3,"skipsOrdered":4,"missed":5,"skips":6,"rerunRatio":0.5}`,
		target: func() any { return &DbSubCounters{} },
		check: func(t *testing.T, v any) {
			got := v.(*DbSubCounters)
			if got.DB != "app" || got.RerunRatio != 0.5 || got.Skips != 6 {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "DbQuotaCounters",
		body:   `{"db":"app","tables":1,"storage":2,"subs":3}`,
		target: func() any { return &DbQuotaCounters{} },
		check: func(t *testing.T, v any) {
			got := v.(*DbQuotaCounters)
			if got.DB != "app" || got.Storage != 2 {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "DbWorkflowStatusCounts",
		body:   `{"db":"app","pending":1,"running":2,"waiting":3,"success":4,"failed":5,"cancelled":6}`,
		target: func() any { return &DbWorkflowStatusCounts{} },
		check: func(t *testing.T, v any) {
			got := v.(*DbWorkflowStatusCounts)
			if got.Success != 4 || got.Cancelled != 6 {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name: "SubscriptionsResponse",
		body: `{"subscriptions":[{"db":"app","table":"t","terminal":"select","readSetClass":"point","principal":null}],` +
			`"subsRerunsTotal":1,"subsSkipsPointTotal":2,"subsSkipsIndexedTotal":3,` +
			`"subsSkipsOrderedTotal":4,"subsMissedPushesTotal":5,` +
			`"perDb":[{"db":"app","reruns":1,"skipsPoint":2,"skipsIndexed":3,"skipsOrdered":4,"missed":5,"skips":6,"rerunRatio":0.5}]}`,
		target: func() any { return &SubscriptionsResponse{} },
		check: func(t *testing.T, v any) {
			got := v.(*SubscriptionsResponse)
			if len(got.Subscriptions) != 1 || got.SubsMissedPushesTotal != 5 || len(got.PerDB) != 1 {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name: "HotConfig",
		body: `{"allowedOrigins":["https://x"],"sessionTtlDays":30,"maxFileSize":1000,"idempotencyTtlMs":10,` +
			`"changeLogMaxRows":100000,"maxTablesPerDb":0,"maxStorageBytesPerDb":0,"maxSubsPerDb":0,` +
			`"perTokenRpm":1200,"perDbRpm":9000}`,
		target: func() any { return &HotConfig{} },
		check: func(t *testing.T, v any) {
			got := v.(*HotConfig)
			if got.SessionTTLDays != 30 || got.PerTokenRPM != 1200 || len(got.AllowedOrigins) != 1 {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name: "ConfigResponse",
		body: `{"port":8300,"publicUrl":"http://x","githubBaseUrl":"","githubApiUrl":"",` +
			`"databaseUrlConfigured":true,"adminKeyConfigured":true,"githubConfigured":false,` +
			`"googleConfigured":false,"gitlabConfigured":false,"oidcConfigured":false,` +
			`"storageSignedUrls":{"mode":"databases","databases":["app"]},` +
			`"hot":{"allowedOrigins":[],"sessionTtlDays":30,"maxFileSize":1000,"idempotencyTtlMs":10,` +
			`"changeLogMaxRows":100000,"maxTablesPerDb":0,"maxStorageBytesPerDb":0,"maxSubsPerDb":0,` +
			`"perTokenRpm":1,"perDbRpm":2},"version":"1.0.0","gitCommit":"abc","admins":[{"email":"a@b.c","githubId":null}]}`,
		target: func() any { return &ConfigResponse{} },
		check: func(t *testing.T, v any) {
			got := v.(*ConfigResponse)
			if got.Port != 8300 || got.StorageSignedURLs.Mode != "databases" || got.Hot.PerDbRPM != 2 ||
				len(got.Admins) != 1 || got.Admins[0].GithubID != nil {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "SchemaHistorySummary",
		body:   `{"version":3,"capturedAt":42,"source":"push","principal":null}`,
		target: func() any { return &SchemaHistorySummary{} },
		check: func(t *testing.T, v any) {
			got := v.(*SchemaHistorySummary)
			if got.Version != 3 || got.CapturedAt != 42 || got.Source != "push" || got.Principal != nil {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "SchemaHistoryEntry",
		body:   `{"version":3,"capturedAt":42,"source":"push","principal":"p","schema":{"tables":{}}}`,
		target: func() any { return &SchemaHistoryEntry{} },
		check: func(t *testing.T, v any) {
			got := v.(*SchemaHistoryEntry)
			obj, ok := got.Schema.(wire.Object)
			if !ok || got.Version != 3 || got.Principal == nil || len(obj["tables"].(wire.Object)) != 0 {
				t.Fatalf("got %+v schema %#v", got, got.Schema)
			}
		},
	},
	{
		name:   "SchemaPreviewColumnAdd",
		body:   `{"name":"a","fieldType":"string"}`,
		target: func() any { return &SchemaPreviewColumnAdd{} },
		check: func(t *testing.T, v any) {
			got := v.(*SchemaPreviewColumnAdd)
			if got.Name != "a" || got.FieldType != "string" {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "SchemaPreviewIndexAdd",
		body:   `{"name":"ix","fields":["a","b"]}`,
		target: func() any { return &SchemaPreviewIndexAdd{} },
		check: func(t *testing.T, v any) {
			got := v.(*SchemaPreviewIndexAdd)
			if got.Name != "ix" || len(got.Fields) != 2 {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "SchemaPreviewTableAdd",
		body:   `{"table":"t","columns":[{"name":"a","fieldType":"string"}],"indexes":[{"name":"ix","fields":["a"]}]}`,
		target: func() any { return &SchemaPreviewTableAdd{} },
		check: func(t *testing.T, v any) {
			got := v.(*SchemaPreviewTableAdd)
			if got.Table != "t" || len(got.Columns) != 1 || len(got.Indexes) != 1 {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "SchemaPreviewRejection",
		body:   `{"table":"t","item":"idx_x","reason":"drop refused"}`,
		target: func() any { return &SchemaPreviewRejection{} },
		check: func(t *testing.T, v any) {
			got := v.(*SchemaPreviewRejection)
			if got.Table != "t" || got.Item != "idx_x" || got.Reason != "drop refused" {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "SchemaPreviewDiff",
		body:   `{"added":[{"table":"t","columns":[],"indexes":[]}],"rejected":[{"table":"u","item":"f","reason":"r"}]}`,
		target: func() any { return &SchemaPreviewDiff{} },
		check: func(t *testing.T, v any) {
			got := v.(*SchemaPreviewDiff)
			if len(got.Added) != 1 || len(got.Rejected) != 1 || got.Rejected[0].Table != "u" {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "DirectiveReport",
		body:   `{"op":"setDefault","affectedRows":3}`,
		target: func() any { return &DirectiveReport{} },
		check: func(t *testing.T, v any) {
			got := v.(*DirectiveReport)
			if got.Op != "setDefault" || got.AffectedRows != 3 || got.CastFailures != nil {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "CastFailure",
		body:   `{"id":"d1","value":"not-a-number"}`,
		target: func() any { return &CastFailure{} },
		check: func(t *testing.T, v any) {
			got := v.(*CastFailure)
			if got.ID != "d1" || got.Value != wire.String("not-a-number") {
				t.Fatalf("got %#v", got.Value)
			}
		},
	},
	{
		name:   "SampleChange",
		body:   `{"id":"d1","before":{"a":1},"after":{"a":2}}`,
		target: func() any { return &SampleChange{} },
		check: func(t *testing.T, v any) {
			got := v.(*SampleChange)
			before, ok1 := got.Before.(wire.Object)
			after, ok2 := got.After.(wire.Object)
			if !ok1 || !ok2 || before["a"] != wire.Number("1") || after["a"] != wire.Number("2") {
				t.Fatalf("got %#v %#v", got.Before, got.After)
			}
		},
	},
	{
		name:   "MigrateResult",
		body:   `{"applied":true,"schema":{"tables":{}},"directives":[{"op":"setDefault","affectedRows":1}]}`,
		target: func() any { return &MigrateResult{} },
		check: func(t *testing.T, v any) {
			got := v.(*MigrateResult)
			if !got.Applied || len(got.Directives) != 1 || got.Directives[0].Op != "setDefault" {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "BackupFile",
		body:   `{"name":"a.dump","sizeBytes":10,"createdMs":5}`,
		target: func() any { return &BackupFile{} },
		check: func(t *testing.T, v any) {
			got := v.(*BackupFile)
			if got.Name != "a.dump" || got.SizeBytes != 10 || got.CreatedMs != 5 {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "BackupsListResponse",
		body:   `{"running":false,"backups":[{"name":"a.dump","sizeBytes":10,"createdMs":5}]}`,
		target: func() any { return &BackupsListResponse{} },
		check: func(t *testing.T, v any) {
			got := v.(*BackupsListResponse)
			if got.Running || len(got.Backups) != 1 {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "RestoreResult",
		body:   `{"target":"rtdb_restored_1","instructions":"cut over"}`,
		target: func() any { return &RestoreResult{} },
		check: func(t *testing.T, v any) {
			got := v.(*RestoreResult)
			if got.Target != "rtdb_restored_1" || got.Instructions != "cut over" {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "ExplainResult",
		body:   `{"sql":"SELECT 1","params":["7"],"terminal":"select","warnings":[]}`,
		target: func() any { return &ExplainResult{} },
		check: func(t *testing.T, v any) {
			got := v.(*ExplainResult)
			if got.SQL != "SELECT 1" || len(got.Params) != 1 || got.Terminal != "select" {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "SlowQueryEntry",
		body:   `{"startedAtMs":1,"durationMs":900,"db":"app","table":"t","terminal":"select","sql":"SELECT 1","params":[]}`,
		target: func() any { return &SlowQueryEntry{} },
		check: func(t *testing.T, v any) {
			got := v.(*SlowQueryEntry)
			if got.DurationMs != 900 || got.DB != "app" || got.Terminal != "select" {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "SlowQueriesResponse",
		body:   `{"queries":[{"startedAtMs":1,"durationMs":900,"db":"app","table":"t","terminal":"select","sql":"s","params":[]}],"thresholdMs":500,"capacity":100}`,
		target: func() any { return &SlowQueriesResponse{} },
		check: func(t *testing.T, v any) {
			got := v.(*SlowQueriesResponse)
			if len(got.Queries) != 1 || got.ThresholdMs != 500 || got.Capacity != 100 {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "OpEvent",
		body:   `{"db":"app","table":"t","docId":"d1","kind":"patch","ts":1,"owner":"u","seq":2,"feedEpoch":"e1"}`,
		target: func() any { return &OpEvent{} },
		check: func(t *testing.T, v any) {
			got := v.(*OpEvent)
			if got.DocID != "d1" || got.Owner == nil || got.Seq != 2 || got.FeedEpoch != "e1" {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "AuditEntry",
		body:   `{"id":1,"tsMs":2,"db":"app","table":"t","op":"insert","docId":"d1","principal":"p","source":"api"}`,
		target: func() any { return &AuditEntry{} },
		check: func(t *testing.T, v any) {
			got := v.(*AuditEntry)
			if got.ID != 1 || got.Op == nil || *got.Op != "insert" || got.Principal == nil {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "Webhook",
		body:   `{"id":1,"db":"app","table":"t","url":"http://x","events":["insert"],"createdAt":1,"enabled":true,"secret":"s"}`,
		target: func() any { return &Webhook{} },
		check: func(t *testing.T, v any) {
			got := v.(*Webhook)
			if got.ID != 1 || got.Table == nil || got.Secret == nil || !got.Enabled {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name:   "WebhookDelivery",
		body:   `{"id":1,"attempts":1,"status":"failed","nextAttempt":2,"lastError":"boom","payload":{"a":1}}`,
		target: func() any { return &WebhookDelivery{} },
		check: func(t *testing.T, v any) {
			got := v.(*WebhookDelivery)
			obj, ok := got.Payload.(wire.Object)
			if !ok || got.ID != 1 || got.LastError == nil || obj["a"] != wire.Number("1") {
				t.Fatalf("got %+v payload %#v", got, got.Payload)
			}
		},
	},
	{
		name:   "EnrollmentGrant",
		body:   `{"grantId":"g1","grant":"s","db":"app","capability":{"type":"parMeshV1","host":"h"},"expiresAt":1}`,
		target: func() any { return &EnrollmentGrant{} },
		check: func(t *testing.T, v any) {
			got := v.(*EnrollmentGrant)
			if got.GrantID != "g1" || got.Capability.Host != "h" {
				t.Fatalf("got %+v", got)
			}
		},
	},
	{
		name: "MetricsSnapshot",
		body: `{"queriesTotal":1,"mutationsTotal":2,"uploadsTotal":3,"wsConnections":4,"activeSubscriptions":5,` +
			`"poolSize":6,"poolIdle":7,"uptimeSeconds":8,` +
			`"queryLatency":{"p50":1,"p95":2,"p99":3},"mutateLatency":{"p50":4,"p95":5,"p99":6},` +
			`"subscribeLatency":{"p50":7,"p95":8,"p99":9},` +
			`"subsRerunsTotal":1,"subsSkipsPointTotal":2,"subsSkipsIndexedTotal":3,"subsSkipsOrderedTotal":4,` +
			`"subsSkipVerificationsTotal":5,"subsMissedPushesTotal":6,` +
			`"perDbSubs":[{"db":"app","reruns":1,"skipsPoint":0,"skipsIndexed":0,"skipsOrdered":0,"missed":0,"skips":0,"rerunRatio":0}],` +
			`"presenceDetail":[{"room":"r","memberCount":1,"localMemberCount":1,"stateBytes":2,"oldestMemberAgeMs":3,"mergedWithPeers":false}],` +
			`"presenceRooms":1,"presenceSessions":2,` +
			`"quotaRejectionsTablesTotal":1,"quotaRejectionsStorageTotal":2,"quotaRejectionsSubsTotal":3,` +
			`"adminAuthFailuresTotal":4,` +
			`"perDbQuota":[{"db":"app","tables":1,"storage":2,"subs":3}],` +
			`"workflowStepsSuccessTotal":1,"workflowStepsRetryTotal":2,"workflowStepsFailTotal":3,` +
			`"perDbWorkflows":[{"db":"app","pending":1,"running":2,"waiting":3,"success":4,"failed":5,"cancelled":6}]}`,
		target: func() any { return &MetricsSnapshot{} },
		check: func(t *testing.T, v any) {
			got := v.(*MetricsSnapshot)
			if got.QueriesTotal != 1 || got.UptimeSeconds != 8 || got.QueryLatency.P99 != 3 ||
				len(got.PerDBSubs) != 1 || len(got.PerDBQuota) != 1 || len(got.PerDBWorkflows) != 1 ||
				len(got.PresenceDetail) != 1 || got.AdminAuthFailuresTotal != 4 {
				t.Fatalf("got %+v", got)
			}
		},
	},
}

// addExtra injects an unknown top-level field into a JSON object body —
// every strict DTO must reject it.
func addExtra(body string) string {
	return body[:len(body)-1] + `,"zzUnknown":1}`
}

func TestDecodeHappyPaths(t *testing.T) {
	for _, tc := range decodeHappyTable {
		t.Run(tc.name, func(t *testing.T) {
			dst := tc.target()
			if err := json.Unmarshal([]byte(tc.body), dst); err != nil {
				t.Fatalf("decode: %v", err)
			}
			tc.check(t, dst)
		})
	}
}

func TestDecodeRejectsUnknownFields(t *testing.T) {
	for _, tc := range decodeHappyTable {
		t.Run(tc.name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(addExtra(tc.body)), tc.target()); err == nil {
				t.Fatal("unknown field must be rejected")
			}
		})
	}
}

func TestDecodeRejectsWrongTypes(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		target func() any
	}{
		{"DbStats_tables_not_array", `{"tables":"x","totalSizeBytes":0,"tablesQuota":0,"tablesUsed":0,"storageQuotaBytes":0,"storageUsedBytes":0,"subsQuota":0,"subsUsed":0,"creationId":""}`, func() any { return &DbStats{} }},
		{"TokenInfo_revoked_not_bool", `{"id":"t","name":"n","createdAt":0,"revoked":"yes","expiresAt":null,"readOnly":false,"tables":null}`, func() any { return &TokenInfo{} }},
		{"HotConfig_sessionTtl_not_number", `{"allowedOrigins":[],"sessionTtlDays":"30","maxFileSize":0,"idempotencyTtlMs":0,"changeLogMaxRows":0,"maxTablesPerDb":0,"maxStorageBytesPerDb":0,"maxSubsPerDb":0,"perTokenRpm":0,"perDbRpm":0}`, func() any { return &HotConfig{} }},
		{"MetricsSnapshot_scalar_not_number", `{"queriesTotal":"many"}`, func() any { return &MetricsSnapshot{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(tc.body), tc.target()); err == nil {
				t.Fatal("wrong type must be rejected")
			}
		})
	}
}

// The five dynamic-blob decoders wrap a strict envelope around
// wire.UnmarshalJSON — a malformed inner blob must surface as an error, and
// the strict envelope still rejects unknown fields.
func TestDecodeBlobDecoders(t *testing.T) {
	for _, tc := range []struct {
		name   string
		body   string
		target func() any
	}{
		{"SchemaHistoryEntry_bad_schema", `{"version":1,"capturedAt":2,"source":"s","principal":null,"schema":{"x":}}`, func() any { return &SchemaHistoryEntry{} }},
		{"SchemaHistoryEntry_unknown_field", `{"version":1,"capturedAt":2,"source":"s","principal":null,"schema":{},"zz":1}`, func() any { return &SchemaHistoryEntry{} }},
		{"CastFailure_bad_value", `{"id":"d1","value":,}`, func() any { return &CastFailure{} }},
		{"CastFailure_unknown_field", `{"id":"d1","value":1,"zz":2}`, func() any { return &CastFailure{} }},
		{"SampleChange_bad_before", `{"id":"d1","before":{,"after":null}`, func() any { return &SampleChange{} }},
		{"SampleChange_bad_after", `{"id":"d1","before":null,"after":}}}nope`, func() any { return &SampleChange{} }},
		{"SampleChange_unknown_field", `{"id":"d1","before":null,"after":null,"zz":1}`, func() any { return &SampleChange{} }},
		{"MigrateResult_bad_schema", `{"applied":false,"schema":[`, func() any { return &MigrateResult{} }},
		{"MigrateResult_unknown_field", `{"applied":false,"schema":{},"directives":[],"zz":1}`, func() any { return &MigrateResult{} }},
		{"WebhookDelivery_bad_payload", `{"id":1,"attempts":0,"status":"pending","nextAttempt":1,"lastError":null,"payload":nope}`, func() any { return &WebhookDelivery{} }},
		{"WebhookDelivery_unknown_field", `{"id":1,"attempts":0,"status":"pending","nextAttempt":1,"lastError":null,"payload":null,"zz":1}`, func() any { return &WebhookDelivery{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := json.Unmarshal([]byte(tc.body), tc.target()); err == nil {
				t.Fatal("malformed blob / unknown field must be rejected")
			}
		})
	}
}
