// go-client/httpclient/client.go
package httpclient

// Mirrors rust-client/src/http_common.rs — the one-shot HTTP seam. Every
// request carries Authorization: Bearer <token> and X-Rtdb-Protocol: 1;
// every non-2xx response decodes the {code,message,retryAfter} envelope
// into errors.RtDbError.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// Client talks to one database on one par-rt-db server over HTTP.
type Client struct {
	baseURL string
	db      string
	token   string
	http    *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient overrides the underlying net/http client (timeouts,
// transports).
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.http = hc }
}

// NewClient builds a client for base URL + database with a machine token.
func NewClient(baseURL, db, token string, opts ...Option) *Client {
	c := &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		db:      db,
		token:   token,
		http:    &http.Client{},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// DB returns the configured database name.
func (c *Client) DB() string { return c.db }

// do performs one request through the auth seam and decodes the response.
func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("httpclient: encode request: %w", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rdr)
	if err != nil {
		return fmt.Errorf("httpclient: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-Rtdb-Protocol", strconv.FormatUint(uint64(wire.PROTOCOL_VERSION), 10))
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("httpclient: transport: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("httpclient: read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		var env wire.ErrorEnvelope
		if json.Unmarshal(data, &env) == nil && env.Code != "" {
			return &rtdberrors.RtDbError{
				Code:       rtdberrors.ErrorCode(env.Code),
				Message:    env.Message,
				RetryAfter: env.RetryAfter,
			}
		}
		return &rtdberrors.RtDbError{
			Code:    rtdberrors.CodeInternal,
			Message: fmt.Sprintf("HTTP %d: %s", resp.StatusCode, truncate(string(data), 200)),
		}
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("httpclient: decode response: %w", err)
		}
	}
	return nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
