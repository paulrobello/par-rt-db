// go-client/httpclient/bulk.go
package httpclient

// Mirrors ts-client/src/http.ts bulkDelete/bulkStatus and the server
// handlers in server/src/http_api/bulk.rs (ARC-005 background bulk delete:
// POST /api/bulk/delete starts the op, GET /api/bulk/{id}?db= polls it).

import (
	"context"
	"net/url"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// BulkDelete starts a background bulk delete: the server freezes the ids
// matching filter (over-cap → BAD_REQUEST "narrow the filter"), then removes
// them chunk-by-chunk through the committer queue, so interactive writes
// interleave between chunk turns. Poll progress with BulkStatus using the
// returned opId.
func (c *Client) BulkDelete(ctx context.Context, db, table string, filter wire.FilterExpr) (wire.BulkOpAccepted, error) {
	req := struct {
		DB     string          `json:"db"`
		Table  string          `json:"table"`
		Filter wire.FilterExpr `json:"filter"`
	}{DB: db, Table: table, Filter: filter}
	var out wire.BulkOpAccepted
	if err := c.do(ctx, http_POST, "/api/bulk/delete", &req, &out); err != nil {
		return wire.BulkOpAccepted{}, err
	}
	return out, nil
}

// BulkStatus polls a bulk op started with BulkDelete
// (GET /api/bulk/{id}?db=). The DTO's error field is present only when the
// op state is "failed".
func (c *Client) BulkStatus(ctx context.Context, db, id string) (wire.BulkOpInfo, error) {
	q := url.Values{"db": {db}}
	path := "/api/bulk/" + url.PathEscape(id) + "?" + q.Encode()
	var out wire.BulkOpInfo
	if err := c.do(ctx, http_GET, path, nil, &out); err != nil {
		return wire.BulkOpInfo{}, err
	}
	return out, nil
}
