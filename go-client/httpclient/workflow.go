// go-client/httpclient/workflow.go
package httpclient

// Mirrors rust-client/src/http.rs workflow section.

import (
	"context"
	"net/url"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// StartWorkflow submits a workflow spec; returns the run id.
// POST /api/workflows {db, spec} → {id}.
func (c *Client) StartWorkflow(ctx context.Context, spec wire.WorkflowSpec) (string, error) {
	body := map[string]any{"db": c.db, "spec": spec}
	var resp struct {
		ID string `json:"id"`
	}
	if err := c.do(ctx, http_POST, "/api/workflows", body, &resp); err != nil {
		return "", err
	}
	return resp.ID, nil
}

// CancelWorkflow cancels a run (false = unknown/already-terminal no-op).
// POST /api/workflows/{id}/cancel {db} → {ok}.
func (c *Client) CancelWorkflow(ctx context.Context, id string) (bool, error) {
	body := map[string]any{"db": c.db}
	var resp struct {
		OK bool `json:"ok"`
	}
	path := "/api/workflows/" + url.PathEscape(id) + "/cancel"
	if err := c.do(ctx, http_POST, path, body, &resp); err != nil {
		return false, err
	}
	return resp.OK, nil
}

// SignalWorkflow delivers a named signal to a waiting run; false = not
// delivered. POST /api/workflows/{id}/signal {db, name, payload?} → {ok}.
func (c *Client) SignalWorkflow(ctx context.Context, id, name string, payload wire.JSONValue) (bool, error) {
	body := map[string]any{"db": c.db, "name": name}
	if payload != nil {
		body["payload"] = payload
	}
	var resp struct {
		OK bool `json:"ok"`
	}
	path := "/api/workflows/" + url.PathEscape(id) + "/signal"
	if err := c.do(ctx, http_POST, path, body, &resp); err != nil {
		return false, err
	}
	return resp.OK, nil
}

// ListWorkflows lists runs, optionally filtered by status.
// POST /api/workflows/list {db, status?} → {workflows}.
func (c *Client) ListWorkflows(ctx context.Context, status *wire.WorkflowStatus) ([]wire.WorkflowInfo, error) {
	body := map[string]any{"db": c.db}
	if status != nil {
		body["status"] = status
	}
	var resp struct {
		Workflows []wire.WorkflowInfo `json:"workflows"`
	}
	if err := c.do(ctx, http_POST, "/api/workflows/list", body, &resp); err != nil {
		return nil, err
	}
	return resp.Workflows, nil
}
