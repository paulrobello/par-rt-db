// go-client/httpclient/schedule.go
package httpclient

// Mirrors rust-client/src/http.rs schedule section. The create entry point
// (Schedule with its ScheduleOption variadics) lives in claim.go next to the
// external claim/finalize surface it shares the wire shape with.

import (
	"context"
	"net/url"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

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
