// go-client/httpclient/mutate.go
package httpclient

// Mirrors rust-client/src/http.rs mutate section — POST /api/mutate and
// the retryOnPrecondition analog.

import (
	"context"
	"fmt"

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

// MutateBatchSlot is one aligned outcome of MutateBatch: step results on
// success, or the standard error envelope on failure (never both). The
// server's slots are positionally aligned with the request's txns.
type MutateBatchSlot struct {
	OK      bool                `json:"ok"`
	Results []wire.StepResult   `json:"results,omitempty"`
	Error   *wire.ErrorEnvelope `json:"error,omitempty"`
}

// MutateBatch runs independent transactions through `/api/mutate-batch` in
// one round trip. Slots are aligned with txns; one failing entry does not
// roll back the others (the batch is deliberately not atomic — an atomic
// multi-step write is what the txn DSL is for). idempotencyKeys, when
// non-nil, must match txns in length; a non-empty key engages the server's
// per-entry idempotency cache.
func (c *Client) MutateBatch(ctx context.Context, txns []wire.Transaction, idempotencyKeys []string) ([]MutateBatchSlot, error) {
	if idempotencyKeys != nil && len(idempotencyKeys) != len(txns) {
		return nil, fmt.Errorf("httpclient: idempotencyKeys length %d does not match txns length %d",
			len(idempotencyKeys), len(txns))
	}
	entries := make([]map[string]any, len(txns))
	for i, txn := range txns {
		entry := map[string]any{"txn": txn}
		if idempotencyKeys != nil && idempotencyKeys[i] != "" {
			entry["idempotencyKey"] = idempotencyKeys[i]
		}
		entries[i] = entry
	}
	var resp struct {
		Results []MutateBatchSlot `json:"results"`
	}
	if err := c.do(ctx, http_POST, "/api/mutate-batch", map[string]any{"db": c.db, "txns": entries}, &resp); err != nil {
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
