// go-client/admin/manage.go
package admin

// Mirrors rust-client/src/admin/mod.rs — backups, webhooks, audit, and
// interactive-session management.

import (
	"context"
	"fmt"
	"net/url"
)

// BackupNow: POST /admin/backup (empty body) → {ok:true}. A second call
// while one runs → 409 CONFLICT.
func (c *AdminClient) BackupNow(ctx context.Context) error {
	return c.ok(ctx, methodPost, "/admin/backup", map[string]any{})
}

// ListBackups: GET /admin/backups → the in-progress flag + on-disk dumps.
func (c *AdminClient) ListBackups(ctx context.Context) (*BackupsListResponse, error) {
	var out BackupsListResponse
	if err := c.get(ctx, "/admin/backups", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DownloadBackup: GET /admin/backups/{name} → the raw dump bytes.
func (c *AdminClient) DownloadBackup(ctx context.Context, name string) ([]byte, error) {
	return c.raw(ctx, methodGet, fmt.Sprintf("/admin/backups/%s", name), "", nil)
}

// DeleteBackup: DELETE /admin/backups/{name} → 2xx, nothing to parse.
func (c *AdminClient) DeleteBackup(ctx context.Context, name string) error {
	return c.api.Call(ctx, methodDelete, fmt.Sprintf("/admin/backups/%s", name), nil, nil)
}

// RestoreBackup: POST /admin/restore {name, confirm} — the SDK sends
// confirm == name (the typed confirmation guard). Restores into a fresh
// rtdb_restored_<stamp> DB; the live DB is never touched.
func (c *AdminClient) RestoreBackup(ctx context.Context, name string) (*RestoreResult, error) {
	var out RestoreResult
	body := map[string]any{"name": name, "confirm": name}
	if err := c.api.Call(ctx, methodPost, "/admin/restore", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListWebhooks: GET /admin/db/{db}/webhooks → the registered webhooks.
func (c *AdminClient) ListWebhooks(ctx context.Context, db string) ([]Webhook, error) {
	var resp struct {
		Webhooks []Webhook `json:"webhooks"`
	}
	if err := c.get(ctx, fmt.Sprintf("/admin/db/%s/webhooks", db), nil, &resp); err != nil {
		return nil, err
	}
	return resp.Webhooks, nil
}

// CreateWebhook: POST /admin/db/{db}/webhooks → the new webhook's id.
func (c *AdminClient) CreateWebhook(ctx context.Context, db string, opts CreateWebhookOptions) (int64, error) {
	var resp struct {
		ID int64 `json:"id"`
	}
	if err := c.api.Call(ctx, methodPost, fmt.Sprintf("/admin/db/%s/webhooks", db), opts, &resp); err != nil {
		return 0, err
	}
	return resp.ID, nil
}

// EditWebhook: PUT /admin/db/{db}/webhooks/{id} → the updated webhook.
func (c *AdminClient) EditWebhook(ctx context.Context, db string, id int64, opts WebhookEditOptions) (*Webhook, error) {
	var out Webhook
	if err := c.api.Call(ctx, methodPut, fmt.Sprintf("/admin/db/%s/webhooks/%d", db, id), opts, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteWebhook: DELETE /admin/db/{db}/webhooks/{id} → {ok:true};
// cascades the webhook's pending deliveries.
func (c *AdminClient) DeleteWebhook(ctx context.Context, db string, id int64) error {
	return c.ok(ctx, methodDelete, fmt.Sprintf("/admin/db/%s/webhooks/%d", db, id), nil)
}

// ListDeliveries: GET /admin/db/{db}/webhooks/{id}/deliveries → the
// delivery outbox, newest next_attempt first. Nil opts = the server's
// default first page.
func (c *AdminClient) ListDeliveries(ctx context.Context, db string, id int64, opts *ListDeliveriesOptions) ([]WebhookDelivery, error) {
	q := url.Values{}
	if opts != nil {
		if opts.Status != nil {
			q.Set("status", *opts.Status)
		}
		if opts.Limit != nil {
			q.Set("limit", fmt.Sprintf("%d", *opts.Limit))
		}
		if opts.Offset != nil {
			q.Set("offset", fmt.Sprintf("%d", *opts.Offset))
		}
	}
	var resp struct {
		Deliveries []WebhookDelivery `json:"deliveries"`
	}
	if err := c.get(ctx, fmt.Sprintf("/admin/db/%s/webhooks/%d/deliveries", db, id), q, &resp); err != nil {
		return nil, err
	}
	return resp.Deliveries, nil
}

// GetAudit: GET /admin/audit → the durable audit rows, newest ts_ms first.
// db is always sent; the other filters are omitted when unset.
func (c *AdminClient) GetAudit(ctx context.Context, db string, opts *AuditQuery) ([]AuditEntry, error) {
	q := url.Values{"db": {db}}
	if opts != nil {
		if opts.Table != nil {
			q.Set("table", *opts.Table)
		}
		if opts.Op != nil {
			q.Set("op", *opts.Op)
		}
		if opts.Principal != nil {
			q.Set("principal", *opts.Principal)
		}
		if opts.Source != nil {
			q.Set("source", *opts.Source)
		}
		if opts.Limit != nil {
			q.Set("limit", fmt.Sprintf("%d", *opts.Limit))
		}
		if opts.Offset != nil {
			q.Set("offset", fmt.Sprintf("%d", *opts.Offset))
		}
	}
	var resp struct {
		Entries []AuditEntry `json:"entries"`
	}
	if err := c.get(ctx, "/admin/audit", q, &resp); err != nil {
		return nil, err
	}
	return resp.Entries, nil
}

// ListSessions: GET /admin/sessions?user=&limit= → active sessions,
// newest-first. Nil opts lists every session server-wide.
func (c *AdminClient) ListSessions(ctx context.Context, opts *SessionListOptions) ([]SessionInfo, error) {
	q := url.Values{}
	if opts != nil {
		if opts.User != nil {
			q.Set("user", *opts.User)
		}
		if opts.Limit != nil {
			q.Set("limit", fmt.Sprintf("%d", *opts.Limit))
		}
	}
	var resp struct {
		Sessions []SessionInfo `json:"sessions"`
	}
	if err := c.get(ctx, "/admin/sessions", q, &resp); err != nil {
		return nil, err
	}
	return resp.Sessions, nil
}

// RevokeSession: DELETE /admin/sessions/{tokenHash} → {ok:true}.
func (c *AdminClient) RevokeSession(ctx context.Context, tokenHash string) error {
	return c.ok(ctx, methodDelete, fmt.Sprintf("/admin/sessions/%s", tokenHash), nil)
}

// RevokeUserSessions: DELETE /admin/sessions?user={userId} → {ok, revoked}.
func (c *AdminClient) RevokeUserSessions(ctx context.Context, userID string) (*RevokeUserSessionsResponse, error) {
	var out RevokeUserSessionsResponse
	if err := c.api.Call(ctx, methodDelete,
		withQuery("/admin/sessions", url.Values{"user": {userID}}), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RevokeExpiredSessions: DELETE /admin/sessions?expired=true → {ok, revoked}.
func (c *AdminClient) RevokeExpiredSessions(ctx context.Context) (*RevokeUserSessionsResponse, error) {
	var out RevokeUserSessionsResponse
	if err := c.api.Call(ctx, methodDelete,
		withQuery("/admin/sessions", url.Values{"expired": {"true"}}), nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// MergeUsers: POST /admin/merge-users {anonUserId, realUserId, confirm} →
// MergeReport. confirm is sent as realUserId (the typed guard, same
// pattern as delete-db).
func (c *AdminClient) MergeUsers(ctx context.Context, anonUserID, realUserID string) (*MergeReport, error) {
	var out MergeReport
	body := map[string]any{
		"anonUserId": anonUserID,
		"realUserId": realUserID,
		"confirm":    realUserID,
	}
	if err := c.api.Call(ctx, methodPost, "/admin/merge-users", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
