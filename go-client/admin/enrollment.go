// go-client/admin/enrollment.go
package admin

// Mirrors server/src/admin/tokens.rs's enrollment-grant surface: typed
// create/revoke methods on the admin client. The grant secret is returned
// once at creation and never persisted in plaintext server-side; redemption
// goes through httpclient's Enroll (public /auth/enroll).

import (
	"context"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// EnrollmentGrant is POST /admin/enrollment-grants's response: the one-time
// plaintext grant (shown once) plus the capability it fixes.
type EnrollmentGrant struct {
	GrantID    string              `json:"grantId"`
	Grant      string              `json:"grant"`
	DB         string              `json:"db"`
	Capability wire.HostCapability `json:"capability"`
	ExpiresAt  int64               `json:"expiresAt"`
}

// CreateEnrollmentGrant: POST /admin/enrollment-grants → EnrollmentGrant.
// expiresAt is required (ms epoch); tokenExpiresAt is optional — nil omits
// the key and takes the server default. The grant's plaintext is shown only
// in this response.
func (c *AdminClient) CreateEnrollmentGrant(ctx context.Context, db, name string,
	capability wire.HostCapability, expiresAt int64, tokenExpiresAt *int64,
) (*EnrollmentGrant, error) {
	body := struct {
		DB             string              `json:"db"`
		Name           string              `json:"name"`
		Capability     wire.HostCapability `json:"capability"`
		ExpiresAt      int64               `json:"expiresAt"`
		TokenExpiresAt *int64              `json:"tokenExpiresAt,omitempty"`
	}{DB: db, Name: name, Capability: capability, ExpiresAt: expiresAt, TokenExpiresAt: tokenExpiresAt}
	var out EnrollmentGrant
	if err := c.api.Call(ctx, methodPost, "/admin/enrollment-grants", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// RevokeEnrollmentGrant: POST /admin/revoke-enrollment-grant {grantId} →
// {ok:true}. NotFound for an unknown grant id.
func (c *AdminClient) RevokeEnrollmentGrant(ctx context.Context, grantID string) error {
	return c.ok(ctx, methodPost, "/admin/revoke-enrollment-grant", map[string]any{"grantId": grantID})
}
