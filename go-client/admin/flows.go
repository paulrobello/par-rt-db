// go-client/admin/flows.go
package admin

// Mirrors rust-client/src/admin/mod.rs — workflow runs, schedules, file
// storage, and the anonymous-access toggle.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"github.com/paulrobello/par-rt-db/go-client/httpclient"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// ListWorkflows: GET /admin/db/{db}/workflows?status=&limit= → runs,
// newest-first. Nil opts = the server-default first page.
func (c *AdminClient) ListWorkflows(ctx context.Context, db string, opts *WorkflowListOptions) ([]wire.WorkflowInfo, error) {
	q := url.Values{}
	if opts != nil {
		if opts.Status != nil {
			q.Set("status", string(*opts.Status))
		}
		if opts.Limit != nil {
			q.Set("limit", fmt.Sprintf("%d", *opts.Limit))
		}
	}
	var resp struct {
		Workflows []wire.WorkflowInfo `json:"workflows"`
	}
	if err := c.get(ctx, fmt.Sprintf("/admin/db/%s/workflows", db), q, &resp); err != nil {
		return nil, err
	}
	return resp.Workflows, nil
}

// GetWorkflow: GET /admin/db/{db}/workflows/{id} → the full run row.
func (c *AdminClient) GetWorkflow(ctx context.Context, db, id string) (*wire.WorkflowInfoFull, error) {
	var out wire.WorkflowInfoFull
	if err := c.get(ctx, fmt.Sprintf("/admin/db/%s/workflows/%s", db, id), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// StartWorkflow: POST /admin/db/{db}/workflows with the bare WorkflowSpec
// body → the new run's id.
func (c *AdminClient) StartWorkflow(ctx context.Context, db string, spec wire.WorkflowSpec) (string, error) {
	var resp struct {
		ID string `json:"id"`
	}
	if err := c.api.Call(ctx, methodPost, fmt.Sprintf("/admin/db/%s/workflows", db), spec, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

// CancelWorkflow: POST /admin/db/{db}/workflows/{id}/cancel → ok=false for
// an unknown or already-terminal run (a no-op, not an error).
func (c *AdminClient) CancelWorkflow(ctx context.Context, db, id string) (bool, error) {
	return c.workflowOK(ctx, methodPost, fmt.Sprintf("/admin/db/%s/workflows/%s/cancel", db, id))
}

// SignalWorkflow: POST /admin/db/{db}/workflows/{id}/signal {name,
// payload?} → deliver a named signal to a waiting run. Payload is
// latest-wins; nil omits the key.
func (c *AdminClient) SignalWorkflow(ctx context.Context, db, id, name string, payload wire.JSONValue) (bool, error) {
	body := map[string]any{"name": name}
	if payload != nil {
		body["payload"] = payload
	}
	var resp struct {
		OK bool `json:"ok"`
	}
	if err := c.api.Call(ctx, methodPost, fmt.Sprintf("/admin/db/%s/workflows/%s/signal", db, id), body, &resp); err != nil {
		return false, err
	}
	return resp.OK, nil
}

// DeleteWorkflow: DELETE /admin/db/{db}/workflows/{id} → ok=false when
// already gone; the outcome trail does not survive.
func (c *AdminClient) DeleteWorkflow(ctx context.Context, db, id string) (bool, error) {
	return c.workflowOK(ctx, methodDelete, fmt.Sprintf("/admin/db/%s/workflows/%s", db, id))
}

// workflowOK decodes the {ok} body of the bodyless workflow ops.
func (c *AdminClient) workflowOK(ctx context.Context, method, path string) (bool, error) {
	var resp struct {
		OK bool `json:"ok"`
	}
	if err := c.api.Call(ctx, method, path, nil, &resp); err != nil {
		return false, err
	}
	return resp.OK, nil
}

// ListSchedules: GET /admin/db/{db}/schedules → every pending and
// in-flight scheduled job (the admin view spans all principals).
func (c *AdminClient) ListSchedules(ctx context.Context, db string) ([]wire.ScheduleInfo, error) {
	var resp struct {
		Schedules []wire.ScheduleInfo `json:"schedules"`
	}
	if err := c.get(ctx, fmt.Sprintf("/admin/db/%s/schedules", db), nil, &resp); err != nil {
		return nil, err
	}
	return resp.Schedules, nil
}

// CreateSchedule: POST /admin/db/{db}/schedules {when, txn} → the new
// job's id.
func (c *AdminClient) CreateSchedule(ctx context.Context, db string, when wire.ScheduleWhen, txn wire.Transaction) (string, error) {
	var resp struct {
		ID string `json:"id"`
	}
	body := map[string]any{"when": when, "txn": txn}
	if err := c.api.Call(ctx, methodPost, fmt.Sprintf("/admin/db/%s/schedules", db), body, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

// CancelSchedule: POST /admin/db/{db}/schedules/{id}/cancel → ok=false for
// an unknown or already-fired id.
func (c *AdminClient) CancelSchedule(ctx context.Context, db, id string) (bool, error) {
	return c.manageSchedule(ctx, db, id, "cancel")
}

// PauseSchedule: POST /admin/db/{db}/schedules/{id}/pause.
func (c *AdminClient) PauseSchedule(ctx context.Context, db, id string) (bool, error) {
	return c.manageSchedule(ctx, db, id, "pause")
}

// ResumeSchedule: POST /admin/db/{db}/schedules/{id}/resume.
func (c *AdminClient) ResumeSchedule(ctx context.Context, db, id string) (bool, error) {
	return c.manageSchedule(ctx, db, id, "resume")
}

// manageSchedule is the shared bodyless-POST op for cancel/pause/resume.
func (c *AdminClient) manageSchedule(ctx context.Context, db, id, op string) (bool, error) {
	var resp struct {
		OK bool `json:"ok"`
	}
	path := fmt.Sprintf("/admin/db/%s/schedules/%s/%s", db, id, op)
	if err := c.api.Call(ctx, methodPost, path, nil, &resp); err != nil {
		return false, err
	}
	return resp.OK, nil
}

// ListFiles: GET /admin/db/{db}/storage → every blob the database owns.
func (c *AdminClient) ListFiles(ctx context.Context, db string) ([]httpclient.FileMetadata, error) {
	var resp struct {
		Files []httpclient.FileMetadata `json:"files"`
	}
	if err := c.get(ctx, fmt.Sprintf("/admin/db/%s/storage", db), nil, &resp); err != nil {
		return nil, err
	}
	return resp.Files, nil
}

// UploadFile: POST /admin/db/{db}/storage with the RAW bytes as the body
// (not JSON) → the new blob's id. contentType nil stores the blob untyped.
func (c *AdminClient) UploadFile(ctx context.Context, db string, data []byte, contentType *string) (string, error) {
	ct := ""
	if contentType != nil {
		ct = *contentType
	}
	respData, err := c.raw(ctx, methodPost, fmt.Sprintf("/admin/db/%s/storage", db), ct, data)
	if err != nil {
		return "", err
	}
	var resp struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(respData, &resp); err != nil {
		return "", fmt.Errorf("admin: decode upload id: %w", err)
	}
	return resp.ID, nil
}

// DeleteFile: DELETE /admin/db/{db}/storage/{id} → {ok:true}; idempotent.
func (c *AdminClient) DeleteFile(ctx context.Context, db, id string) error {
	return c.ok(ctx, methodDelete, fmt.Sprintf("/admin/db/%s/storage/%s", db, id), nil)
}

// GetAnonymousAccess: GET /admin/db/{db}/anonymous-access → the per-db
// flag (the instance-wide boot gate applies on top).
func (c *AdminClient) GetAnonymousAccess(ctx context.Context, db string) (bool, error) {
	var resp struct {
		Enabled bool `json:"enabled"`
	}
	if err := c.get(ctx, fmt.Sprintf("/admin/db/%s/anonymous-access", db), nil, &resp); err != nil {
		return false, err
	}
	return resp.Enabled, nil
}

// SetAnonymousAccess: PATCH /admin/db/{db}/anonymous-access {enabled} →
// {ok:true}.
func (c *AdminClient) SetAnonymousAccess(ctx context.Context, db string, enabled bool) error {
	return c.ok(ctx, methodPatch, fmt.Sprintf("/admin/db/%s/anonymous-access", db),
		map[string]any{"enabled": enabled})
}
