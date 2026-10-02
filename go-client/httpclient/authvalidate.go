// go-client/httpclient/authvalidate.go
package httpclient

// Mirrors rust-client/src/http.rs::validate_session_token and
// ts-client/src/http.ts::validateSessionToken.

import (
	"context"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// AuthValidate validates an arbitrary session/machine token via
// GET /auth/validate, returning the authed user. Unlike AuthMe (which
// validates this client's own bearer), this takes the token to validate as
// an argument and accepts both token kinds — for a trusted backend
// validating a player's credential. An invalid/expired token surfaces as
// the standard RtDbError auth envelope.
func (c *Client) AuthValidate(ctx context.Context, token string) (*wire.AuthedUser, error) {
	var resp struct {
		User wire.AuthedUser `json:"user"`
	}
	if err := c.doWithAuth(ctx, "GET", "/auth/validate", nil, &resp, token); err != nil {
		return nil, err
	}
	return &resp.User, nil
}
