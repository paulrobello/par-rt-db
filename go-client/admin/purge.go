package admin

import (
	"context"
	"fmt"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// PurgeOptions selects insertOnly rows and bounds each purge call. Nil Filter
// selects all rows. Nil Limit uses the server default (10,000); supplied limits
// are clamped by the server to [1, 10,000].
type PurgeOptions struct {
	Filter wire.FilterExpr `json:"filter,omitempty"`
	Limit  *int            `json:"limit,omitempty"`
}

// PurgeRequest is the two-phase admin purge body. Nil Confirm previews only.
type PurgeRequest struct {
	Table string `json:"table"`
	PurgeOptions
	Confirm *string `json:"confirm,omitempty"`
}

// PurgePreviewResponse describes the matching rows without deleting any.
type PurgePreviewResponse struct {
	Matching     int64  `json:"matching"`
	ConfirmToken string `json:"confirmToken"`
	Limit        int    `json:"limit"`
}

// PurgeResponse reports one confirmed purge batch.
type PurgeResponse struct {
	Deleted   uint64 `json:"deleted"`
	Truncated bool   `json:"truncated"`
}

// UnmarshalJSON rejects unknown fields.
func (p *PurgePreviewResponse) UnmarshalJSON(b []byte) error {
	type alias PurgePreviewResponse
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*p = PurgePreviewResponse(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (p *PurgeResponse) UnmarshalJSON(b []byte) error {
	type alias PurgeResponse
	v, err := wire.StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*p = PurgeResponse(v)
	return nil
}

// PurgePreview: POST /admin/db/{db}/purge without confirm counts insertOnly
// rows and returns a confirmation token. Nil opts uses the server defaults.
func (c *AdminClient) PurgePreview(ctx context.Context, db, table string, opts *PurgeOptions) (*PurgePreviewResponse, error) {
	body := PurgeRequest{Table: table}
	if opts != nil {
		body.PurgeOptions = *opts
	}
	var out PurgePreviewResponse
	if err := c.api.Call(ctx, methodPost, fmt.Sprintf("/admin/db/%s/purge", db), body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Purge: POST /admin/db/{db}/purge with a preview's confirmToken deletes up to
// the requested limit. Re-preview on a stale-token error. If Truncated is true,
// more rows match; deleting rows changes the count bound into the token, so a
// fresh preview may be required before the next batch. No retries are automatic.
func (c *AdminClient) Purge(ctx context.Context, db, table, confirm string, opts *PurgeOptions) (*PurgeResponse, error) {
	body := PurgeRequest{Table: table, Confirm: &confirm}
	if opts != nil {
		body.PurgeOptions = *opts
	}
	var out PurgeResponse
	if err := c.api.Call(ctx, methodPost, fmt.Sprintf("/admin/db/%s/purge", db), body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
