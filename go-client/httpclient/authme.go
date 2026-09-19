// go-client/httpclient/authme.go
package httpclient

// Mirrors rust-client/src/http.rs::auth_me.

import (
	"context"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// AuthMe resolves the caller's identity (machine token or session cookie).
// GET /auth/me → {user: AuthedUser}.
func (c *Client) AuthMe(ctx context.Context) (*wire.AuthedUser, error) {
	var resp struct {
		User wire.AuthedUser `json:"user"`
	}
	if err := c.do(ctx, "GET", "/auth/me", nil, &resp); err != nil {
		return nil, err
	}
	return &resp.User, nil
}
