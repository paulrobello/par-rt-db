// go-client/admin/ops.go
package admin

// Mirrors rust-client/src/admin/mod.rs — observability, config, and the
// admin owner-bypass query/mutate surfaces.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// Metrics: GET /admin/metrics → server-wide counters and gauges.
func (c *AdminClient) Metrics(ctx context.Context) (*MetricsSnapshot, error) {
	var out MetricsSnapshot
	if err := c.get(ctx, "/admin/metrics", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// PresenceRooms: GET /admin/presence → this replica's live room inspector.
func (c *AdminClient) PresenceRooms(ctx context.Context) (*PresenceRoomsResponse, error) {
	var out PresenceRoomsResponse
	if err := c.get(ctx, "/admin/presence", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListSubscriptions: GET /admin/subscriptions?db=<optional> → the live
// subscription inspector; nil db spans every database.
func (c *AdminClient) ListSubscriptions(ctx context.Context, db *string) (*SubscriptionsResponse, error) {
	q := url.Values{}
	if db != nil {
		q.Set("db", *db)
	}
	var out SubscriptionsResponse
	if err := c.get(ctx, "/admin/subscriptions", q, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetConfig: GET /admin/config → redacted config + build identity + admins.
func (c *AdminClient) GetConfig(ctx context.Context) (*ConfigResponse, error) {
	var out ConfigResponse
	if err := c.get(ctx, "/admin/config", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// PatchConfig: PATCH /admin/config with a partial hot-config body → the
// updated config.
func (c *AdminClient) PatchConfig(ctx context.Context, patch HotConfigPatch) (*ConfigResponse, error) {
	var out ConfigResponse
	if err := c.api.Call(ctx, methodPatch, "/admin/config", patch, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// OpsRecent: GET /admin/ops/recent?db=&table=<optional>&n=<optional> →
// recent document-op events, newest-first.
func (c *AdminClient) OpsRecent(ctx context.Context, db string, table *string, n *int64) ([]OpEvent, error) {
	q := url.Values{"db": {db}}
	if table != nil {
		q.Set("table", *table)
	}
	if n != nil {
		q.Set("n", fmt.Sprintf("%d", *n))
	}
	var resp struct {
		Ops []OpEvent `json:"ops"`
	}
	if err := c.get(ctx, "/admin/ops/recent", q, &resp); err != nil {
		return nil, err
	}
	return resp.Ops, nil
}

// AdminQuery: POST /admin/db/{db}/query {query, includeDeleted?} → the
// dynamic result. Owner-bypass: reads across every ownerField rule.
// includeDeleted true surfaces soft-deleted rows; nil omits the key so the
// server's live-rows-only default applies.
func (c *AdminClient) AdminQuery(ctx context.Context, db string, query wire.Query, includeDeleted *bool) (wire.JSONValue, error) {
	body := map[string]any{"query": query}
	if includeDeleted != nil {
		body["includeDeleted"] = *includeDeleted
	}
	var resp struct {
		Result json.RawMessage `json:"result"`
	}
	if err := c.api.Call(ctx, methodPost, fmt.Sprintf("/admin/db/%s/query", db), body, &resp); err != nil {
		return nil, err
	}
	return wire.UnmarshalJSON(resp.Result)
}

// ExplainQuery: POST /admin/db/{db}/explain {query} → the compiled SQL +
// binds, byte-identical to what execution runs.
func (c *AdminClient) ExplainQuery(ctx context.Context, db string, query wire.Query) (*ExplainResult, error) {
	var out ExplainResult
	if err := c.api.Call(ctx, methodPost, fmt.Sprintf("/admin/db/%s/explain", db),
		map[string]any{"query": query}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetSlowQueries: GET /admin/slow-queries?db=<optional>&limit=<optional> →
// the slow-query ring, newest-first.
func (c *AdminClient) GetSlowQueries(ctx context.Context, db *string, limit *int64) (*SlowQueriesResponse, error) {
	q := url.Values{}
	if db != nil {
		q.Set("db", *db)
	}
	if limit != nil {
		q.Set("limit", fmt.Sprintf("%d", *limit))
	}
	var out SlowQueriesResponse
	if err := c.get(ctx, "/admin/slow-queries", q, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// AdminMutate: POST /admin/db/{db}/mutate {txn, idempotencyKey?} → one
// StepResult per step. Owner-bypass writes. A non-empty idempotencyKey
// engages the server's idempotency cache.
func (c *AdminClient) AdminMutate(ctx context.Context, db string, txn wire.Transaction, idempotencyKey string) ([]wire.StepResult, error) {
	body := map[string]any{"txn": txn}
	if idempotencyKey != "" {
		body["idempotencyKey"] = idempotencyKey
	}
	var resp struct {
		Results []wire.StepResult `json:"results"`
	}
	if err := c.api.Call(ctx, methodPost, fmt.Sprintf("/admin/db/%s/mutate", db), body, &resp); err != nil {
		return nil, err
	}
	return resp.Results, nil
}
