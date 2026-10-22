// go-client/admin/enrollment_test.go
package admin

// Host-machine-authorization mirror tests: the enrollment-grant admin
// surface, the mint request's optional hostCapability, and the token row's
// absent-optional hostCapability (skip_serializing_if semantics — the field
// is ABSENT, not null, for legacy tokens).

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestCreateEnrollmentGrantBodyAndDecode(t *testing.T) {
	body := `{"db":"app","name":"macdaddy-enroll","capability":{"type":"parMeshV1","host":"macdaddy"},` +
		`"expiresAt":1761100000000,"tokenExpiresAt":1761100900000}`
	c := &capture{t: t, method: "POST", path: "/admin/enrollment-grants", body: &body,
		bearer: wantStr(testAdminKey),
		status: 200,
		respBody: `{"grantId":"g1","grant":"otc-secret","db":"app",` +
			`"capability":{"type":"parMeshV1","host":"macdaddy"},"expiresAt":1761100000000}`}
	client := stub(t, c.handler)
	grant, err := client.CreateEnrollmentGrant(context.Background(), "app", "macdaddy-enroll",
		wire.HostCapability{Type: wire.HostCapabilityTypeParMeshV1, Host: "macdaddy"},
		1761100000000, ptrI64(1761100900000))
	if err != nil {
		t.Fatalf("CreateEnrollmentGrant: %v", err)
	}
	if grant.GrantID != "g1" || grant.Grant != "otc-secret" || grant.DB != "app" ||
		grant.Capability.Host != "macdaddy" || grant.ExpiresAt != 1761100000000 {
		t.Fatalf("got %+v", grant)
	}
}

func TestCreateEnrollmentGrantOmitsTokenExpiresAt(t *testing.T) {
	body := `{"db":"app","name":"h","capability":{"type":"parMeshV1","host":"macdaddy"},"expiresAt":1761100000000}`
	c := &capture{t: t, method: "POST", path: "/admin/enrollment-grants", body: &body,
		status: 200, respBody: `{"grantId":"g2","grant":"s","db":"app",` +
			`"capability":{"type":"parMeshV1","host":"macdaddy"},"expiresAt":1761100000000}`}
	client := stub(t, c.handler)
	if _, err := client.CreateEnrollmentGrant(context.Background(), "app", "h",
		wire.HostCapability{Type: wire.HostCapabilityTypeParMeshV1, Host: "macdaddy"},
		1761100000000, nil); err != nil {
		t.Fatalf("CreateEnrollmentGrant: %v", err)
	}
}

func TestRevokeEnrollmentGrant(t *testing.T) {
	body := `{"grantId":"g1"}`
	c := &capture{t: t, method: "POST", path: "/admin/revoke-enrollment-grant", body: &body,
		bearer: wantStr(testAdminKey), status: 200, respBody: `{"ok":true}`}
	client := stub(t, c.handler)
	if err := client.RevokeEnrollmentGrant(context.Background(), "g1"); err != nil {
		t.Fatalf("RevokeEnrollmentGrant: %v", err)
	}
}

func TestMintTokenWithHostCapability(t *testing.T) {
	body := `{"db":"app","name":"h","hostCapability":{"type":"parMeshV1","host":"macdaddy"}}`
	c := &capture{t: t, method: "POST", path: "/admin/mint-token", body: &body,
		status: 200, respBody: `{"tokenId":"t3","token":"k"}`}
	client := stub(t, c.handler)
	tok, err := client.MintTokenWithOptions(context.Background(), "app", "h", MintTokenOptions{
		HostCapability: &wire.HostCapability{Type: wire.HostCapabilityTypeParMeshV1, Host: "macdaddy"},
	})
	if err != nil {
		t.Fatalf("MintTokenWithOptions: %v", err)
	}
	if tok.TokenID != "t3" {
		t.Fatalf("got %q", tok.TokenID)
	}
}

// Regression proof: a mint request without a capability marshals WITHOUT the
// hostCapability key — the legacy wire form, byte-compatible.
func TestLegacyMintOmitsHostCapability(t *testing.T) {
	body := `{"db":"app","name":"ci","expiresAt":123}`
	c := &capture{t: t, method: "POST", path: "/admin/mint-token", body: &body,
		status: 200, respBody: `{"tokenId":"t4","token":"k"}`}
	client := stub(t, c.handler)
	if _, err := client.MintTokenWithOptions(context.Background(), "app", "ci", MintTokenOptions{
		ExpiresAt: ptrI64(123),
	}); err != nil {
		t.Fatalf("MintTokenWithOptions: %v", err)
	}
}

// Token rows: hostCapability is absent-optional server-side
// (skip_serializing_if) — legacy rows decode with a nil capability and newer
// rows decode with it set; unknown fields stay rejected either way.
func TestTokenInfoHostCapabilityAbsentOptional(t *testing.T) {
	var row TokenInfo
	legacy := `{"id":"t1","name":"ci","createdAt":1,"revoked":false,` +
		`"expiresAt":null,"readOnly":false,"tables":null}`
	if err := json.Unmarshal([]byte(legacy), &row); err != nil {
		t.Fatalf("legacy row: %v", err)
	}
	if row.HostCapability != nil {
		t.Fatalf("legacy row must have nil capability, got %+v", row.HostCapability)
	}
	modern := `{"id":"t2","name":"h","createdAt":1,"revoked":false,` +
		`"expiresAt":null,"readOnly":false,"tables":null,` +
		`"hostCapability":{"type":"parMeshV1","host":"macdaddy"}}`
	if err := json.Unmarshal([]byte(modern), &row); err != nil {
		t.Fatalf("modern row: %v", err)
	}
	if row.HostCapability == nil || row.HostCapability.Host != "macdaddy" {
		t.Fatalf("got %+v", row.HostCapability)
	}
	bogus := `{"id":"t","name":"n","createdAt":0,"revoked":false,` +
		`"expiresAt":null,"readOnly":false,"tables":null,"bogus":1}`
	if err := json.Unmarshal([]byte(bogus), &row); err == nil {
		t.Fatal("unknown field on token row must be rejected")
	}
}
