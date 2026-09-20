// go-client/httpclient/changes.go
package httpclient

// The resumable per-db change feed — GET /api/db/{db}/changes.

import (
	"context"
	"net/url"
	"strconv"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// Changes returns the committed ops strictly after since, oldest first.
// table = "" means all tables; limit <= 0 takes the server default (500,
// clamped to 1000). Advance with since = resp.NextSeq; a CURSOR_EXPIRED
// error means the cursor predates retention (or is ahead of the log) and
// the consumer must resync from 0.
func (c *Client) Changes(ctx context.Context, since int64, table string, limit int) (*wire.ChangeFeedResponse, error) {
	q := url.Values{"since": {strconv.FormatInt(since, 10)}}
	if table != "" {
		q.Set("table", table)
	}
	if limit > 0 {
		q.Set("limit", strconv.Itoa(limit))
	}
	path := "/api/db/" + url.PathEscape(c.db) + "/changes?" + q.Encode()
	var out wire.ChangeFeedResponse
	if err := c.do(ctx, http_GET, path, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
