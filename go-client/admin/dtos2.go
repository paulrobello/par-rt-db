// go-client/admin/dtos2.go
package admin

// Mirrors rust-client/src/wire/admin.rs — the observability, config, and
// schema-management DTOs.

import "github.com/paulrobello/par-rt-db/go-client/wire"

// LatencyStats is the p50/p95/p99 microsecond triple.
type LatencyStats struct {
	P50 int64 `json:"p50"`
	P95 int64 `json:"p95"`
	P99 int64 `json:"p99"`
}

// MetricsSnapshot is GET /admin/metrics — server counters and gauges. The
// subscription/quota/workflow counter groups default to zero when an older
// server omits them.
type MetricsSnapshot struct {
	QueriesTotal                int64                    `json:"queriesTotal"`
	MutationsTotal              int64                    `json:"mutationsTotal"`
	UploadsTotal                int64                    `json:"uploadsTotal"`
	WSConnections               int64                    `json:"wsConnections"`
	ActiveSubscriptions         int64                    `json:"activeSubscriptions"`
	PoolSize                    int64                    `json:"poolSize"`
	PoolIdle                    int64                    `json:"poolIdle"`
	UptimeSeconds               int64                    `json:"uptimeSeconds"`
	QueryLatency                LatencyStats             `json:"queryLatency"`
	MutateLatency               LatencyStats             `json:"mutateLatency"`
	SubscribeLatency            LatencyStats             `json:"subscribeLatency"`
	SubsRerunsTotal             int64                    `json:"subsRerunsTotal"`
	SubsSkipsPointTotal         int64                    `json:"subsSkipsPointTotal"`
	SubsSkipsIndexedTotal       int64                    `json:"subsSkipsIndexedTotal"`
	SubsSkipsOrderedTotal       int64                    `json:"subsSkipsOrderedTotal"`
	SubsSkipVerificationsTotal  int64                    `json:"subsSkipVerificationsTotal"`
	SubsMissedPushesTotal       int64                    `json:"subsMissedPushesTotal"`
	PerDBSubs                   []DbSubCounters          `json:"perDbSubs"`
	PresenceDetail              []PresenceRoomInspect    `json:"presenceDetail"`
	PresenceRooms               int64                    `json:"presenceRooms"`
	PresenceSessions            int64                    `json:"presenceSessions"`
	QuotaRejectionsTablesTotal  int64                    `json:"quotaRejectionsTablesTotal"`
	QuotaRejectionsStorageTotal int64                    `json:"quotaRejectionsStorageTotal"`
	QuotaRejectionsSubsTotal    int64                    `json:"quotaRejectionsSubsTotal"`
	AdminAuthFailuresTotal      int64                    `json:"adminAuthFailuresTotal"`
	PerDBQuota                  []DbQuotaCounters        `json:"perDbQuota"`
	WorkflowStepsSuccessTotal   int64                    `json:"workflowStepsSuccessTotal"`
	WorkflowStepsRetryTotal     int64                    `json:"workflowStepsRetryTotal"`
	WorkflowStepsFailTotal      int64                    `json:"workflowStepsFailTotal"`
	PerDBWorkflows              []DbWorkflowStatusCounts `json:"perDbWorkflows"`
}

// PresenceRoomInspect is one room's live footprint. In multi-instance mode
// MemberCount/StateBytes are merged with gossiped peer membership;
// LocalMemberCount is always this replica's own count and MergedWithPeers
// says whether the merge happened (eventually-consistent, best-effort —
// never an authoritative total).
type PresenceRoomInspect struct {
	Room              string `json:"room"`
	MemberCount       int64  `json:"memberCount"`
	LocalMemberCount  int64  `json:"localMemberCount"`
	StateBytes        int64  `json:"stateBytes"`
	OldestMemberAgeMs int64  `json:"oldestMemberAgeMs"`
	MergedWithPeers   bool   `json:"mergedWithPeers"`
}

// PresenceRoomsResponse is GET /admin/presence.
type PresenceRoomsResponse struct {
	Rooms []PresenceRoomInspect `json:"rooms"`
}

// SubscriptionsPrincipal is the subscriber identity; both fields null when
// the subscriber is a machine token or an admin bypass.
type SubscriptionsPrincipal struct {
	UserID *string `json:"userId"`
	Email  *string `json:"email"`
}

// SubscriptionInfo is one live subscription and its read-set class.
type SubscriptionInfo struct {
	DB           string                  `json:"db"`
	Table        string                  `json:"table"`
	Terminal     string                  `json:"terminal"`
	ReadSetClass string                  `json:"readSetClass"`
	Principal    *SubscriptionsPrincipal `json:"principal"`
}

// DbSubCounters is one database's subscription-invalidation counters.
type DbSubCounters struct {
	DB           string  `json:"db"`
	Reruns       int64   `json:"reruns"`
	SkipsPoint   int64   `json:"skipsPoint"`
	SkipsIndexed int64   `json:"skipsIndexed"`
	SkipsOrdered int64   `json:"skipsOrdered"`
	Missed       int64   `json:"missed"`
	Skips        int64   `json:"skips"`
	RerunRatio   float64 `json:"rerunRatio"`
}

// DbQuotaCounters is one database's quota-rejection counters.
type DbQuotaCounters struct {
	DB      string `json:"db"`
	Tables  int64  `json:"tables"`
	Storage int64  `json:"storage"`
	Subs    int64  `json:"subs"`
}

// DbWorkflowStatusCounts is one database's workflow-run counts by status.
type DbWorkflowStatusCounts struct {
	DB        string `json:"db"`
	Pending   int64  `json:"pending"`
	Running   int64  `json:"running"`
	Waiting   int64  `json:"waiting"`
	Success   int64  `json:"success"`
	Failed    int64  `json:"failed"`
	Cancelled int64  `json:"cancelled"`
}

// SubscriptionsResponse is GET /admin/subscriptions — the live subscription
// inspector with server-wide totals and the per-db breakdown.
type SubscriptionsResponse struct {
	Subscriptions         []SubscriptionInfo `json:"subscriptions"`
	SubsRerunsTotal       int64              `json:"subsRerunsTotal"`
	SubsSkipsPointTotal   int64              `json:"subsSkipsPointTotal"`
	SubsSkipsIndexedTotal int64              `json:"subsSkipsIndexedTotal"`
	SubsSkipsOrderedTotal int64              `json:"subsSkipsOrderedTotal"`
	SubsMissedPushesTotal int64              `json:"subsMissedPushesTotal"`
	PerDB                 []DbSubCounters    `json:"perDb"`
}

// HotConfig is the runtime-mutable config subset.
type HotConfig struct {
	AllowedOrigins       []string `json:"allowedOrigins"`
	SessionTTLDays       int64    `json:"sessionTtlDays"`
	MaxFileSize          int64    `json:"maxFileSize"`
	IdempotencyTTLMs     int64    `json:"idempotencyTtlMs"`
	ChangeLogMaxRows     int64    `json:"changeLogMaxRows"`
	MaxTablesPerDB       int64    `json:"maxTablesPerDb"`
	MaxStorageBytesPerDB int64    `json:"maxStorageBytesPerDb"`
	MaxSubsPerDB         int64    `json:"maxSubsPerDb"`
}

// ConfigResponse is GET /admin/config — redacted boot config + hot config +
// build identity + the admin allowlist.
type ConfigResponse struct {
	Port                  int64         `json:"port"`
	PublicURL             string        `json:"publicUrl"`
	GithubBaseURL         string        `json:"githubBaseUrl"`
	GithubAPIURL          string        `json:"githubApiUrl"`
	DatabaseURLConfigured bool          `json:"databaseUrlConfigured"`
	AdminKeyConfigured    bool          `json:"adminKeyConfigured"`
	GithubConfigured      bool          `json:"githubConfigured"`
	GoogleConfigured      bool          `json:"googleConfigured"`
	GitlabConfigured      bool          `json:"gitlabConfigured"`
	OidcConfigured        bool          `json:"oidcConfigured"`
	Hot                   HotConfig     `json:"hot"`
	Version               string        `json:"version"`
	GitCommit             string        `json:"gitCommit"`
	Admins                []AdminMember `json:"admins"`
}

// SchemaHistorySummary is one row of SchemaHistory (no schema blob).
type SchemaHistorySummary struct {
	Version    int64   `json:"version"`
	CapturedAt int64   `json:"capturedAt"`
	Source     string  `json:"source"`
	Principal  *string `json:"principal"`
}

// SchemaHistoryEntry is one full snapshot (with the schema blob).
type SchemaHistoryEntry struct {
	Version    int64          `json:"version"`
	CapturedAt int64          `json:"capturedAt"`
	Source     string         `json:"source"`
	Principal  *string        `json:"principal"`
	Schema     wire.JSONValue `json:"schema"`
}

// SchemaPreviewColumnAdd is one new column a push would add.
type SchemaPreviewColumnAdd struct {
	Name      string `json:"name"`
	FieldType string `json:"fieldType"`
}

// SchemaPreviewIndexAdd is one new index a push would add.
type SchemaPreviewIndexAdd struct {
	Name   string   `json:"name"`
	Fields []string `json:"fields"`
}

// SchemaPreviewTableAdd is one new table a push would add.
type SchemaPreviewTableAdd struct {
	Table   string                   `json:"table"`
	Columns []SchemaPreviewColumnAdd `json:"columns"`
	Indexes []SchemaPreviewIndexAdd  `json:"indexes"`
}

// SchemaPreviewRejection is one drop/type-change the push would refuse.
type SchemaPreviewRejection struct {
	Table  string `json:"table"`
	Item   string `json:"item"`
	Reason string `json:"reason"`
}

// SchemaPreviewDiff is PreviewSchema's result — what an additive-only push
// would add and what it would reject; purely advisory.
type SchemaPreviewDiff struct {
	Added    []SchemaPreviewTableAdd  `json:"added"`
	Rejected []SchemaPreviewRejection `json:"rejected"`
}

// MigrateResult is MigrateSchema's response; Schema is the post-migration
// derived schema, returned even on dry-run.
type MigrateResult struct {
	Applied    bool              `json:"applied"`
	Schema     wire.JSONValue    `json:"schema"`
	Directives []DirectiveReport `json:"directives"`
}

// DirectiveReport is one directive's outcome.
type DirectiveReport struct {
	Op            string         `json:"op"`
	AffectedRows  int64          `json:"affectedRows"`
	CastFailures  []CastFailure  `json:"castFailures,omitempty"`
	SampleChanges []SampleChange `json:"sampleChanges,omitempty"`
}

// CastFailure is one row that failed coercion.
type CastFailure struct {
	ID    string         `json:"id"`
	Value wire.JSONValue `json:"value"`
}

// SampleChange is one before/after sample.
type SampleChange struct {
	ID     string         `json:"id"`
	Before wire.JSONValue `json:"before"`
	After  wire.JSONValue `json:"after"`
}
