// go-client/admin/dtos.go
package admin

// Mirrors rust-client/src/wire/admin.rs — the /admin/* control-plane
// request/response DTOs, field-for-field (the casing is load-bearing).
// Response types decode strict (unknown fields rejected), matching the
// wire family's convention.

import (
	"encoding/json"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// --- request-side / option types (marshal-only) -------------------------

// MintTokenOptions carries the optional mint capabilities; every omitted
// field lets the server default apply (no expiry, read-write, all tables).
type MintTokenOptions struct {
	ExpiresAt *int64   `json:"expiresAt,omitempty"`
	ReadOnly  *bool    `json:"readOnly,omitempty"`
	Tables    []string `json:"tables,omitempty"`
}

// SessionListOptions filters ListSessions; both fields optional.
type SessionListOptions struct {
	User  *string `json:"-"`
	Limit *int64  `json:"-"`
}

// WorkflowListOptions filters ListWorkflows; both fields optional.
type WorkflowListOptions struct {
	Status *wire.WorkflowStatus `json:"-"`
	Limit  *int64               `json:"-"`
}

// ListDeliveriesOptions pages/filters ListDeliveries; all fields optional.
type ListDeliveriesOptions struct {
	Status *string `json:"-"`
	Limit  *int64  `json:"-"`
	Offset *int64  `json:"-"`
}

// AuditQuery filters GetAudit; every field optional, filters AND-combined
// server-side (an absent field matches all rows).
type AuditQuery struct {
	Table     *string `json:"-"`
	Op        *string `json:"-"`
	Principal *string `json:"-"`
	Source    *string `json:"-"`
	Limit     *int64  `json:"-"`
	Offset    *int64  `json:"-"`
}

// CreateWebhookOptions builds CreateWebhook's body; omitted fields take the
// server defaults (all-tables, ["*"] events, enabled).
type CreateWebhookOptions struct {
	URL     string   `json:"url"`
	Table   *string  `json:"table,omitempty"`
	Events  []string `json:"events,omitempty"`
	Enabled *bool    `json:"enabled,omitempty"`
}

// TableNull is WebhookEditOptions.Table's "clear to all-tables" value; nil
// omits the key (leaves the filter unchanged).
var TableNull = json.RawMessage("null")

// WebhookEditOptions builds EditWebhook's body; absent means "leave
// unchanged". Table is the tri-state filter: nil omits the key,
// TableNull clears to all-tables, a quoted string scopes the webhook.
type WebhookEditOptions struct {
	URL          *string         `json:"url,omitempty"`
	Table        json.RawMessage `json:"table,omitempty"`
	Events       []string        `json:"events,omitempty"`
	Enabled      *bool           `json:"enabled,omitempty"`
	RotateSecret *bool           `json:"rotateSecret,omitempty"`
}

// HotConfigPatch is the PATCH /admin/config body; every field optional,
// omitted fields left unchanged.
type HotConfigPatch struct {
	AllowedOrigins       []string `json:"allowedOrigins,omitempty"`
	SessionTTLDays       *int64   `json:"sessionTtlDays,omitempty"`
	MaxFileSize          *int64   `json:"maxFileSize,omitempty"`
	IdempotencyTTLMs     *int64   `json:"idempotencyTtlMs,omitempty"`
	ChangeLogMaxRows     *int64   `json:"changeLogMaxRows,omitempty"`
	MaxTablesPerDB       *int64   `json:"maxTablesPerDb,omitempty"`
	MaxStorageBytesPerDB *int64   `json:"maxStorageBytesPerDb,omitempty"`
	MaxSubsPerDB         *int64   `json:"maxSubsPerDb,omitempty"`
}

// --- response DTOs -------------------------------------------------------

// MintedToken is MintToken's response ({tokenId, token}).
type MintedToken struct {
	TokenID string `json:"tokenId"`
	Token   string `json:"token"`
}

// AdminMember is one row of AdminsList.
type AdminMember struct {
	Email    string `json:"email"`
	GithubID *int64 `json:"githubId"`
}

// TableStat is one row of DbStats.Tables.
type TableStat struct {
	Name      string `json:"name"`
	RowCount  int64  `json:"rowCount"`
	SizeBytes int64  `json:"sizeBytes"`
}

// DbStats is GET /admin/dbs/{db}/stats — per-table counts plus the six
// quota/usage fields (0 = unlimited).
type DbStats struct {
	Tables            []TableStat `json:"tables"`
	TotalSizeBytes    int64       `json:"totalSizeBytes"`
	TablesQuota       int64       `json:"tablesQuota"`
	TablesUsed        int64       `json:"tablesUsed"`
	StorageQuotaBytes int64       `json:"storageQuotaBytes"`
	StorageUsedBytes  int64       `json:"storageUsedBytes"`
	SubsQuota         int64       `json:"subsQuota"`
	SubsUsed          int64       `json:"subsUsed"`
}

// TokenInfo is one row of ListTokens.
type TokenInfo struct {
	ID        string   `json:"id"`
	Name      string   `json:"name"`
	CreatedAt int64    `json:"createdAt"`
	Revoked   bool     `json:"revoked"`
	ExpiresAt *int64   `json:"expiresAt"`
	ReadOnly  bool     `json:"readOnly"`
	Tables    []string `json:"tables"`
}

// SessionInfo is one active interactive session from ListSessions.
// TokenHash is a non-reversible sha256 digest (the revoke target).
type SessionInfo struct {
	TokenHash string  `json:"tokenHash"`
	UserID    string  `json:"userId"`
	Email     *string `json:"email"`
	Name      *string `json:"name"`
	Login     *string `json:"login"`
	Anonymous bool    `json:"anonymous"`
	CreatedAt int64   `json:"createdAt"`
	ExpiresAt int64   `json:"expiresAt"`
}

// RevokeUserSessionsResponse is the {ok, revoked} body of the session
// sweep routes.
type RevokeUserSessionsResponse struct {
	OK      bool  `json:"ok"`
	Revoked int64 `json:"revoked"`
}

// MergeConflict is one row skipped by the anon→real merge over a unique
// index collision.
type MergeConflict struct {
	Table string `json:"table"`
	ID    string `json:"id"`
}

// MergeDbResult is one database's merge outcome.
type MergeDbResult struct {
	Tables    map[string]int  `json:"tables"`
	Conflicts []MergeConflict `json:"conflicts"`
}

// MergeReport is the full-instance anon→real merge outcome.
type MergeReport struct {
	Dbs               map[string]MergeDbResult `json:"dbs"`
	StorageRepointed  int64                    `json:"storageRepointed"`
	SessionsRepointed int64                    `json:"sessionsRepointed"`
	AnonDeleted       bool                     `json:"anonDeleted"`
}

// BackupFile is one managed dump from ListBackups.
type BackupFile struct {
	Name      string `json:"name"`
	SizeBytes int64  `json:"sizeBytes"`
	CreatedMs int64  `json:"createdMs"`
}

// BackupsListResponse is GET /admin/backups.
type BackupsListResponse struct {
	Running bool         `json:"running"`
	Backups []BackupFile `json:"backups"`
}

// RestoreResult is POST /admin/restore's body.
type RestoreResult struct {
	Target       string `json:"target"`
	Instructions string `json:"instructions"`
}

// ExplainResult is POST /admin/db/{db}/explain — the compiled SQL plus the
// ordered binds, byte-identical to what execution runs.
type ExplainResult struct {
	SQL      string   `json:"sql"`
	Params   []string `json:"params"`
	Terminal string   `json:"terminal"`
	Warnings []string `json:"warnings"`
}

// SlowQueryEntry is one recorded slow query.
type SlowQueryEntry struct {
	StartedAtMs int64    `json:"startedAtMs"`
	DurationMs  int64    `json:"durationMs"`
	DB          string   `json:"db"`
	Table       string   `json:"table"`
	Terminal    string   `json:"terminal"`
	SQL         string   `json:"sql"`
	Params      []string `json:"params"`
}

// SlowQueriesResponse is GET /admin/slow-queries.
type SlowQueriesResponse struct {
	Queries     []SlowQueryEntry `json:"queries"`
	ThresholdMs int64            `json:"thresholdMs"`
	Capacity    int64            `json:"capacity"`
}

// OpEvent is one recent document-op event from OpsRecent.
type OpEvent struct {
	DB    string  `json:"db"`
	Table string  `json:"table"`
	DocID string  `json:"docId"`
	Kind  string  `json:"kind"`
	Ts    int64   `json:"ts"`
	Owner *string `json:"owner"`
	// Seq is the 1-based monotonic sequence within the feed instance: the
	// ring replays on every (re)connect, so dedup replays by tracking the
	// max Seq seen per FeedEpoch; a gap means evicted/dropped events.
	Seq uint64 `json:"seq"`
	// FeedEpoch is the feed's boot-time UUID identity — an epoch change
	// means the counter reset (server restart), not dropped events.
	FeedEpoch string `json:"feedEpoch"`
}

// AuditEntry is one durable-audit row from GetAudit.
type AuditEntry struct {
	ID        int64   `json:"id"`
	TsMs      int64   `json:"tsMs"`
	DB        string  `json:"db"`
	Table     string  `json:"table"`
	Op        *string `json:"op"`
	DocID     string  `json:"docId"`
	Principal *string `json:"principal"`
	Source    string  `json:"source"`
}

// Webhook is one registered webhook from ListWebhooks/EditWebhook.
// Table nil = all tables; Secret is the per-webhook HMAC signing key.
type Webhook struct {
	ID        int64    `json:"id"`
	DB        string   `json:"db"`
	Table     *string  `json:"table"`
	URL       string   `json:"url"`
	Events    []string `json:"events"`
	CreatedAt int64    `json:"createdAt"`
	Enabled   bool     `json:"enabled"`
	Secret    *string  `json:"secret"`
}

// WebhookDelivery is one delivery row from ListDeliveries.
type WebhookDelivery struct {
	ID          int64          `json:"id"`
	Attempts    int64          `json:"attempts"`
	Status      string         `json:"status"`
	NextAttempt int64          `json:"nextAttempt"`
	LastError   *string        `json:"lastError"`
	Payload     wire.JSONValue `json:"payload"`
}
