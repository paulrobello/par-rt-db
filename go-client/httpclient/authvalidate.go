// go-client/httpclient/authvalidate.go
package httpclient

// Mirrors rust-client/src/http.rs::validate_session_token and
// ts-client/src/http.ts::validateSessionToken.

import (
	"context"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// AuthValidate validates an arbitrary session/machine token via
// GET /auth/validate, returning the authed user. Unlike AuthMe (which
// validates this client's own bearer), this takes the token to validate as
// an argument and accepts both token kinds — for a trusted backend
// validating a player's credential. A host-restricted machine token (one
// minted with a hostCapability) is the exception: the server denies it with
// 403 FORBIDDEN. An invalid/expired token surfaces as the standard RtDbError
// auth envelope.
//
// An EMPTY token is rejected locally (BAD_REQUEST, no HTTP request): Go's
// HTTP layer trims trailing OWS, so `Bearer ` would arrive at the server as
// `Bearer`, fail the server's `"Bearer "` prefix check, and fall through to
// ambient rtdb_session cookie resolution — validating an unintended session
// (audit F7). The caller asked to validate an explicit credential; there is
// no honest wire form for an empty one, so it fails before any request.
func (c *Client) AuthValidate(ctx context.Context, token string) (*wire.AuthedUser, error) {
	if token == "" {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"auth validate requires a non-empty token")
	}
	var resp struct {
		User wire.AuthedUser `json:"user"`
	}
	if err := c.doWithAuth(ctx, "GET", "/auth/validate", nil, &resp, token); err != nil {
		return nil, err
	}
	return &resp.User, nil
}
