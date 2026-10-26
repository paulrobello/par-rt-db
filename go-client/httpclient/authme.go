// go-client/httpclient/authme.go
package httpclient

// Mirrors rust-client/src/http.rs::auth_me.

import (
	"context"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// AuthMe resolves the principal this client's credential maps to via
// GET /auth/me → {user: AuthedUser}.
//
// Session-only: the credential must be a user-session bearer or the
// rtdb_session cookie. Machine tokens are rejected by the server (401
// "session required") and surface as the standard RtDbError envelope. To
// validate an arbitrary token (a trusted backend checking a player's
// credential), use AuthValidate.
func (c *Client) AuthMe(ctx context.Context) (*wire.AuthedUser, error) {
	var resp struct {
		User wire.AuthedUser `json:"user"`
	}
	if err := c.do(ctx, "GET", "/auth/me", nil, &resp); err != nil {
		return nil, err
	}
	return &resp.User, nil
}
