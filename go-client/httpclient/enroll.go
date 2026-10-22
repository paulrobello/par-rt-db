// go-client/httpclient/enroll.go
package httpclient

// Mirrors server/src/auth/enrollment.rs's public redemption route: POST
// /auth/enroll takes ONLY the grant secret — the server rejects any
// db/host/capability override attempt.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// Enroll redeems a one-time enrollment grant via the public, unauthenticated
// POST /auth/enroll, returning the minted machine token (shown once), its
// database, and the grant's host capability. Invalid, expired, revoked, or
// already-used grants surface as the standard RtDbError UNAUTHORIZED
// envelope.
//
// This is a dedicated unauthenticated request path, NOT doWithAuth(""):
// the route carries no Authorization header by design (the grant secret is
// the whole credential), while doWithAuth always sends whatever bearer its
// caller asked for — even an empty one — so an explicitly presented
// credential can never silently become "use ambient credentials"
// (audit F7).
func (c *Client) Enroll(ctx context.Context, grant string) (*wire.EnrollResult, error) {
	b, err := json.Marshal(map[string]any{"grant": grant})
	if err != nil {
		return nil, fmt.Errorf("httpclient: encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/auth/enroll", bytes.NewReader(b))
	if err != nil {
		return nil, fmt.Errorf("httpclient: build request: %w", err)
	}
	req.Header.Set("X-Rtdb-Protocol", strconv.FormatUint(uint64(wire.PROTOCOL_VERSION), 10))
	req.Header.Set("Content-Type", "application/json")
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
	var out wire.EnrollResult
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("httpclient: decode response: %w", err)
	}
	return &out, nil
}
