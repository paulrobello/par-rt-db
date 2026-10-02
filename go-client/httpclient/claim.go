// go-client/httpclient/claim.go
package httpclient

// Mirrors ts-client/src/http.ts claimSchedules/completeSchedule/
// retrySchedule/failSchedule and the server handlers in
// server/src/http_api.rs (the claim route plus the three finalize routes).

import (
	"context"
	"net/url"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// ScheduleOption customizes a Schedule call.
type ScheduleOption func(*scheduleRequest)

type scheduleRequest struct {
	DB       string            `json:"db"`
	When     wire.ScheduleWhen `json:"when"`
	Txn      wire.Transaction  `json:"txn"`
	External *bool             `json:"external,omitempty"`
}

// WithExternal creates an external job: one the server never executes —
// an application worker claims it via ClaimSchedules and finalizes it with
// the returned LeaseGeneration fencing token.
func WithExternal() ScheduleOption {
	return func(r *scheduleRequest) { t := true; r.External = &t }
}

// Schedule creates a scheduled job; returns the minted job id. Pass
// WithExternal to create an external job instead.
// POST /api/schedule {db, when, txn[, external]} → {id}.
func (c *Client) Schedule(ctx context.Context, when wire.ScheduleWhen, txn wire.Transaction, opts ...ScheduleOption) (string, error) {
	req := scheduleRequest{DB: c.db, When: when, Txn: txn}
	for _, o := range opts {
		o(&req)
	}
	var resp struct {
		ID string `json:"id"`
	}
	if err := c.do(ctx, http_POST, "/api/schedule", &req, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

// ClaimSchedules atomically claims up to limit (nil = server default) due
// external jobs (POST /api/schedule/claim): rows marked external at create
// time that the server's internal scheduler never executes. Each claim
// assigns the job's per-job monotonic LeaseGeneration fencing token and
// stamps a leaseMs (nil = server default 5min, bounded 1s–24h) lease
// deadline; re-claims after expiry bump the token again, so a finalize from
// a stale holder rejects CONFLICT. The worker executes each claimed job's
// txn itself, then finalizes with CompleteSchedule/RetrySchedule/
// FailSchedule.
func (c *Client) ClaimSchedules(ctx context.Context, limit, leaseMs *int64) ([]wire.ClaimedSchedule, error) {
	body := map[string]any{"db": c.db}
	if limit != nil {
		body["limit"] = *limit
	}
	if leaseMs != nil {
		body["leaseMs"] = *leaseMs
	}
	var resp struct {
		Jobs []wire.ClaimedSchedule `json:"jobs"`
	}
	if err := c.do(ctx, http_POST, "/api/schedule/claim", body, &resp); err != nil {
		return nil, err
	}
	return resp.Jobs, nil
}

// CompleteSchedule completes an externally-claimed job
// (POST /api/schedule/{id}/complete) with the claim's lease fencing token:
// a one-shot is deleted (the same terminal as internal one-shot success);
// cron/interval advance to their next due instant. A stale token, or a row
// no longer running/external, rejects with CONFLICT.
func (c *Client) CompleteSchedule(ctx context.Context, id string, lease int64) error {
	body := map[string]any{"db": c.db, "lease": lease}
	path := "/api/schedule/" + url.PathEscape(id) + "/complete"
	return c.do(ctx, http_POST, path, body, nil)
}

// RetrySchedule retries an externally-claimed job
// (POST /api/schedule/{id}/retry): re-arms it at now + retryAfterMs
// (nil = server default 60s) and records errMsg as last_error. Stale
// lease → CONFLICT.
func (c *Client) RetrySchedule(ctx context.Context, id string, lease int64, retryAfterMs *int64, errMsg string) error {
	body := map[string]any{"db": c.db, "lease": lease}
	if retryAfterMs != nil {
		body["delayMs"] = *retryAfterMs
	}
	if errMsg != "" {
		body["error"] = errMsg
	}
	path := "/api/schedule/" + url.PathEscape(id) + "/retry"
	return c.do(ctx, http_POST, path, body, nil)
}

// FailSchedule fails an externally-claimed job terminally
// (POST /api/schedule/{id}/fail): the job moves to error status with errMsg
// recorded as last_error. Stale lease → CONFLICT.
func (c *Client) FailSchedule(ctx context.Context, id string, lease int64, errMsg string) error {
	body := map[string]any{"db": c.db, "lease": lease, "error": errMsg}
	path := "/api/schedule/" + url.PathEscape(id) + "/fail"
	return c.do(ctx, http_POST, path, body, nil)
}
