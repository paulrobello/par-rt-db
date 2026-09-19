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

// Call exposes the authed request seam (Bearer + X-Rtdb-Protocol + error
// envelope decode) for the admin client, whose routes carry the admin key
// as the bearer. Body/out follow do()'s conventions.
func (c *Client) Call(ctx context.Context, method, path string, body, out any) error {
	return c.do(ctx, method, path, body, out)
}

// RawCall is Call's raw-body seam: same auth headers and error-envelope
// decode, but the body goes on the wire verbatim (contentType overrides the
// JSON default; empty omits the header) and the 2xx response bytes are
// returned undecoded. Serves the admin client's raw routes (export/import,
// backup download, storage upload) and dynamic-decode bodies.
func (c *Client) RawCall(ctx context.Context, method, path, contentType string, body []byte) ([]byte, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, rdr)
	if err != nil {
		return nil, fmt.Errorf("httpclient: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("X-Rtdb-Protocol", strconv.FormatUint(uint64(wire.PROTOCOL_VERSION), 10))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("httpclient: transport: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("httpclient: read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, envelopeError(resp.StatusCode, data)
	}
	return data, nil
}

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
		return envelopeError(resp.StatusCode, data)
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			return fmt.Errorf("httpclient: decode response: %w", err)
		}
	}
	return nil
}

// envelopeError decodes a non-2xx body into *errors.RtDbError, falling
// back to a truncated-INTERNAL envelope for non-JSON bodies.
func envelopeError(status int, data []byte) error {
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
		Message: fmt.Sprintf("HTTP %d: %s", status, truncate(string(data), 200)),
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
