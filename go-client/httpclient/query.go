// go-client/httpclient/query.go
package httpclient

// Mirrors rust-client/src/http.rs query section — POST /api/query and
// POST /api/query-batch.

import (
	"context"
	"encoding/json"
	"fmt"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// Query runs one query and decodes the result into T (use QueryRaw for
// dynamic values — Go generics cannot decode into wire.JSONValue).
func Query[T any](ctx context.Context, c *Client, q wire.Query) (T, error) {
	var zero T
	body := map[string]any{"db": c.db, "query": q}
	var resp struct {
		Result json.RawMessage `json:"result"`
	}
	if err := c.do(ctx, http_POST, "/api/query", body, &resp); err != nil {
		return zero, err
	}
	var out T
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		return zero, fmt.Errorf("httpclient: decode result into %T: %w", out, err)
	}
	return out, nil
}

// QueryRaw runs one query and returns the dynamic result tree.
func (c *Client) QueryRaw(ctx context.Context, q wire.Query) (wire.JSONValue, error) {
	body := map[string]any{"db": c.db, "query": q}
	var resp struct {
		Result json.RawMessage `json:"result"`
	}
	if err := c.do(ctx, http_POST, "/api/query", body, &resp); err != nil {
		return nil, err
	}
	return wire.UnmarshalJSON(resp.Result)
}

// Result is one aligned slot of a query batch.
type Result[T any] struct {
	OK    bool
	Value T
	Err   *rtdberrors.RtDbError
}

// QueryBatch runs several queries in one round trip; slots align with the
// input order and per-query errors land in their slot.
func QueryBatch[T any](ctx context.Context, c *Client, queries []wire.Query) ([]Result[T], error) {
	body := map[string]any{"db": c.db, "queries": queries}
	var resp struct {
		Results []struct {
			OK     bool                `json:"ok"`
			Result json.RawMessage     `json:"result,omitempty"`
			Error  *wire.ErrorEnvelope `json:"error,omitempty"`
		} `json:"results"`
	}
	if err := c.do(ctx, http_POST, "/api/query-batch", body, &resp); err != nil {
		return nil, err
	}
	out := make([]Result[T], len(resp.Results))
	for i, slot := range resp.Results {
		if slot.Error != nil {
			out[i] = Result[T]{OK: false, Err: &rtdberrors.RtDbError{
				Code: rtdberrors.ErrorCode(slot.Error.Code), Message: slot.Error.Message,
			}}
			continue
		}
		var v T
		if !slot.OK {
			out[i] = Result[T]{OK: false}
			continue
		}
		if err := json.Unmarshal(slot.Result, &v); err != nil {
			return nil, fmt.Errorf("httpclient: decode batch slot %d: %w", i, err)
		}
		out[i] = Result[T]{OK: true, Value: v}
	}
	return out, nil
}
