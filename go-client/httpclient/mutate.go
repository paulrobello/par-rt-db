// go-client/httpclient/mutate.go
package httpclient

// Mirrors rust-client/src/http.rs mutate section — POST /api/mutate and
// the retryOnPrecondition analog.

import (
	"context"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

const http_POST = "POST"
const http_GET = "GET"
const http_DELETE = "DELETE"

// Mutate runs a transaction; one StepResult per step. A non-empty
// idempotencyKey engages the server's idempotency cache.
func (c *Client) Mutate(ctx context.Context, txn wire.Transaction, idempotencyKey string) ([]wire.StepResult, error) {
	body := map[string]any{"db": c.db, "txn": txn}
	if idempotencyKey != "" {
		body["idempotencyKey"] = idempotencyKey
	}
	var resp struct {
		Results []wire.StepResult `json:"results"`
	}
	if err := c.do(ctx, http_POST, "/api/mutate", body, &resp); err != nil {
		return nil, err
	}
	return resp.Results, nil
}

// MutateWithRetry rebuilds and re-runs the transaction on
// PRECONDITION_FAILED, up to attempts total tries (rust's
// mutate_with_retry analog).
func (c *Client) MutateWithRetry(ctx context.Context, build func() wire.Transaction, attempts int) ([]wire.StepResult, error) {
	var lastErr error
	for i := 0; i < attempts; i++ {
		results, err := c.Mutate(ctx, build(), "")
		if err == nil {
			return results, nil
		}
		if !rtdberrors.IsCode(err, rtdberrors.CodePreconditionFailed) {
			return nil, err
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = rtdberrors.New(rtdberrors.CodeInternal, "mutate_with_retry: no attempts made")
	}
	return nil, lastErr
}
