// go-client/admin/decode.go
package admin

// Strict-decode wrappers for the admin DTOs (the wire family convention:
// unknown fields rejected), plus side-struct decoders for the DTOs carrying
// dynamic wire.JSONValue blobs. Pure-scalar types use the methodless-alias
// form; the alias avoids MarshalJSON/UnmarshalJSON recursion.

import (
	"encoding/json"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// UnmarshalJSON rejects unknown fields.
func (t *MintedToken) UnmarshalJSON(b []byte) error {
	type alias MintedToken
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*t = MintedToken(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (m *AdminMember) UnmarshalJSON(b []byte) error {
	type alias AdminMember
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*m = AdminMember(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (t *TableStat) UnmarshalJSON(b []byte) error {
	type alias TableStat
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*t = TableStat(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (d *DbStats) UnmarshalJSON(b []byte) error {
	type alias DbStats
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*d = DbStats(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (t *TokenInfo) UnmarshalJSON(b []byte) error {
	type alias TokenInfo
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*t = TokenInfo(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (s *SessionInfo) UnmarshalJSON(b []byte) error {
	type alias SessionInfo
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*s = SessionInfo(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (r *RevokeUserSessionsResponse) UnmarshalJSON(b []byte) error {
	type alias RevokeUserSessionsResponse
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*r = RevokeUserSessionsResponse(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (m *MergeConflict) UnmarshalJSON(b []byte) error {
	type alias MergeConflict
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*m = MergeConflict(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (m *MergeDbResult) UnmarshalJSON(b []byte) error {
	type alias MergeDbResult
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*m = MergeDbResult(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (m *MergeReport) UnmarshalJSON(b []byte) error {
	type alias MergeReport
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*m = MergeReport(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (l *LatencyStats) UnmarshalJSON(b []byte) error {
	type alias LatencyStats
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*l = LatencyStats(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (p *PresenceRoomInspect) UnmarshalJSON(b []byte) error {
	type alias PresenceRoomInspect
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*p = PresenceRoomInspect(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (p *PresenceRoomsResponse) UnmarshalJSON(b []byte) error {
	type alias PresenceRoomsResponse
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*p = PresenceRoomsResponse(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (p *SubscriptionsPrincipal) UnmarshalJSON(b []byte) error {
	type alias SubscriptionsPrincipal
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*p = SubscriptionsPrincipal(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (s *SubscriptionInfo) UnmarshalJSON(b []byte) error {
	type alias SubscriptionInfo
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*s = SubscriptionInfo(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (d *DbSubCounters) UnmarshalJSON(b []byte) error {
	type alias DbSubCounters
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*d = DbSubCounters(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (d *DbQuotaCounters) UnmarshalJSON(b []byte) error {
	type alias DbQuotaCounters
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*d = DbQuotaCounters(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (d *DbWorkflowStatusCounts) UnmarshalJSON(b []byte) error {
	type alias DbWorkflowStatusCounts
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*d = DbWorkflowStatusCounts(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (s *SubscriptionsResponse) UnmarshalJSON(b []byte) error {
	type alias SubscriptionsResponse
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*s = SubscriptionsResponse(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (h *HotConfig) UnmarshalJSON(b []byte) error {
	type alias HotConfig
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*h = HotConfig(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (c *ConfigResponse) UnmarshalJSON(b []byte) error {
	type alias ConfigResponse
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*c = ConfigResponse(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (s *SchemaHistorySummary) UnmarshalJSON(b []byte) error {
	type alias SchemaHistorySummary
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*s = SchemaHistorySummary(v)
	return nil
}

// UnmarshalJSON decodes the dynamic schema blob (strict).
func (e *SchemaHistoryEntry) UnmarshalJSON(b []byte) error {
	r, err := wire.StrictUnmarshal[struct {
		Version    int64           `json:"version"`
		CapturedAt int64           `json:"capturedAt"`
		Source     string          `json:"source"`
		Principal  *string         `json:"principal"`
		Schema     json.RawMessage `json:"schema"`
	}](b)
	if err != nil {
		return err
	}
	schema, err := wire.UnmarshalJSON(r.Schema)
	if err != nil {
		return err
	}
	e.Version, e.CapturedAt, e.Source, e.Principal, e.Schema =
		r.Version, r.CapturedAt, r.Source, r.Principal, schema
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (c *SchemaPreviewColumnAdd) UnmarshalJSON(b []byte) error {
	type alias SchemaPreviewColumnAdd
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*c = SchemaPreviewColumnAdd(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (i *SchemaPreviewIndexAdd) UnmarshalJSON(b []byte) error {
	type alias SchemaPreviewIndexAdd
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*i = SchemaPreviewIndexAdd(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (t *SchemaPreviewTableAdd) UnmarshalJSON(b []byte) error {
	type alias SchemaPreviewTableAdd
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*t = SchemaPreviewTableAdd(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (r *SchemaPreviewRejection) UnmarshalJSON(b []byte) error {
	type alias SchemaPreviewRejection
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*r = SchemaPreviewRejection(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (d *SchemaPreviewDiff) UnmarshalJSON(b []byte) error {
	type alias SchemaPreviewDiff
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*d = SchemaPreviewDiff(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (d *DirectiveReport) UnmarshalJSON(b []byte) error {
	type alias DirectiveReport
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*d = DirectiveReport(v)
	return nil
}

// UnmarshalJSON decodes the dynamic value blob (strict).
func (c *CastFailure) UnmarshalJSON(b []byte) error {
	r, err := wire.StrictUnmarshal[struct {
		ID    string          `json:"id"`
		Value json.RawMessage `json:"value"`
	}](b)
	if err != nil {
		return err
	}
	val, err := wire.UnmarshalJSON(r.Value)
	if err != nil {
		return err
	}
	c.ID, c.Value = r.ID, val
	return nil
}

// UnmarshalJSON decodes the dynamic before/after blobs (strict).
func (s *SampleChange) UnmarshalJSON(b []byte) error {
	r, err := wire.StrictUnmarshal[struct {
		ID     string          `json:"id"`
		Before json.RawMessage `json:"before"`
		After  json.RawMessage `json:"after"`
	}](b)
	if err != nil {
		return err
	}
	before, err := wire.UnmarshalJSON(r.Before)
	if err != nil {
		return err
	}
	after, err := wire.UnmarshalJSON(r.After)
	if err != nil {
		return err
	}
	s.ID, s.Before, s.After = r.ID, before, after
	return nil
}

// UnmarshalJSON decodes the dynamic schema blob (strict).
func (m *MigrateResult) UnmarshalJSON(b []byte) error {
	r, err := wire.StrictUnmarshal[struct {
		Applied    bool              `json:"applied"`
		Schema     json.RawMessage   `json:"schema"`
		Directives []DirectiveReport `json:"directives"`
	}](b)
	if err != nil {
		return err
	}
	schema, err := wire.UnmarshalJSON(r.Schema)
	if err != nil {
		return err
	}
	m.Applied, m.Schema, m.Directives = r.Applied, schema, r.Directives
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (b *BackupFile) UnmarshalJSON(bts []byte) error {
	type alias BackupFile
	v, err := wire.StrictUnmarshal[alias](bts)
	if err != nil {
		return err
	}
	*b = BackupFile(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (b *BackupsListResponse) UnmarshalJSON(bts []byte) error {
	type alias BackupsListResponse
	v, err := wire.StrictUnmarshal[alias](bts)
	if err != nil {
		return err
	}
	*b = BackupsListResponse(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (r *RestoreResult) UnmarshalJSON(bts []byte) error {
	type alias RestoreResult
	v, err := wire.StrictUnmarshal[alias](bts)
	if err != nil {
		return err
	}
	*r = RestoreResult(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (e *ExplainResult) UnmarshalJSON(bts []byte) error {
	type alias ExplainResult
	v, err := wire.StrictUnmarshal[alias](bts)
	if err != nil {
		return err
	}
	*e = ExplainResult(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (q *SlowQueryEntry) UnmarshalJSON(bts []byte) error {
	type alias SlowQueryEntry
	v, err := wire.StrictUnmarshal[alias](bts)
	if err != nil {
		return err
	}
	*q = SlowQueryEntry(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (q *SlowQueriesResponse) UnmarshalJSON(bts []byte) error {
	type alias SlowQueriesResponse
	v, err := wire.StrictUnmarshal[alias](bts)
	if err != nil {
		return err
	}
	*q = SlowQueriesResponse(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (o *OpEvent) UnmarshalJSON(bts []byte) error {
	type alias OpEvent
	v, err := wire.StrictUnmarshal[alias](bts)
	if err != nil {
		return err
	}
	*o = OpEvent(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (a *AuditEntry) UnmarshalJSON(bts []byte) error {
	type alias AuditEntry
	v, err := wire.StrictUnmarshal[alias](bts)
	if err != nil {
		return err
	}
	*a = AuditEntry(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (w *Webhook) UnmarshalJSON(bts []byte) error {
	type alias Webhook
	v, err := wire.StrictUnmarshal[alias](bts)
	if err != nil {
		return err
	}
	*w = Webhook(v)
	return nil
}

// UnmarshalJSON decodes the dynamic payload blob (strict).
func (d *WebhookDelivery) UnmarshalJSON(bts []byte) error {
	r, err := wire.StrictUnmarshal[struct {
		ID          int64           `json:"id"`
		Attempts    int64           `json:"attempts"`
		Status      string          `json:"status"`
		NextAttempt int64           `json:"nextAttempt"`
		LastError   *string         `json:"lastError"`
		Payload     json.RawMessage `json:"payload"`
	}](bts)
	if err != nil {
		return err
	}
	payload, err := wire.UnmarshalJSON(r.Payload)
	if err != nil {
		return err
	}
	d.ID, d.Attempts, d.Status, d.NextAttempt, d.LastError, d.Payload =
		r.ID, r.Attempts, r.Status, r.NextAttempt, r.LastError, payload
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (m *MetricsSnapshot) UnmarshalJSON(bts []byte) error {
	type alias MetricsSnapshot
	v, err := wire.StrictUnmarshal[alias](bts)
	if err != nil {
		return err
	}
	*m = MetricsSnapshot(v)
	return nil
}
