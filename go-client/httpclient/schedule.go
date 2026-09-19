// go-client/httpclient/schedule.go
package httpclient

// Mirrors rust-client/src/http.rs schedule section.

import (
	"context"
	"net/url"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// Schedule creates a scheduled job; returns the minted job id.
// POST /api/schedule {db, when, txn} → {id}.
func (c *Client) Schedule(ctx context.Context, when wire.ScheduleWhen, txn wire.Transaction) (string, error) {
	body := map[string]any{"db": c.db, "when": when, "txn": txn}
	var resp struct {
		ID string `json:"id"`
	}
	if err := c.do(ctx, http_POST, "/api/schedule", body, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

// manageSchedule POSTs {db} to /api/schedule/{id}/{op} → {ok}.
func (c *Client) manageSchedule(ctx context.Context, id, op string) (bool, error) {
	body := map[string]any{"db": c.db}
	var resp struct {
		OK bool `json:"ok"`
	}
	path := "/api/schedule/" + url.PathEscape(id) + "/" + op
	if err := c.do(ctx, http_POST, path, body, &resp); err != nil {
		return false, err
	}
	return resp.OK, nil
}

// CancelSchedule cancels a job (false = unknown/already-terminal no-op).
func (c *Client) CancelSchedule(ctx context.Context, id string) (bool, error) {
	return c.manageSchedule(ctx, id, "cancel")
}

// PauseSchedule pauses a recurring job.
func (c *Client) PauseSchedule(ctx context.Context, id string) (bool, error) {
	return c.manageSchedule(ctx, id, "pause")
}

// ResumeSchedule resumes a paused job.
func (c *Client) ResumeSchedule(ctx context.Context, id string) (bool, error) {
	return c.manageSchedule(ctx, id, "resume")
}

// ListSchedules returns the db's jobs. POST /api/schedules {db}.
func (c *Client) ListSchedules(ctx context.Context) ([]wire.ScheduleInfo, error) {
	body := map[string]any{"db": c.db}
	var resp struct {
		Schedules []wire.ScheduleInfo `json:"schedules"`
	}
	if err := c.do(ctx, http_POST, "/api/schedules", body, &resp); err != nil {
		return nil, err
	}
	return resp.Schedules, nil
}
