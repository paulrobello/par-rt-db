// go-client/httpclient/enroll_test.go
package httpclient

// POST /auth/enroll is public and unauthenticated: the grant secret is the
// whole request, no bearer is sent, and the response carries the minted
// machine token plus the grant's host capability.

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestEnrollSendsGrantOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/auth/enroll" {
			t.Errorf("path %s", r.URL.Path)
		}
		// Public route: no bearer may be sent.
		if got := r.Header.Get("Authorization"); got != "" {
			t.Errorf("authorization %q, want none", got)
		}
		b, _ := io.ReadAll(r.Body)
		if got, want := string(b), `{"grant":"otc-secret"}`; got != want {
			t.Errorf("body %s want %s", got, want)
		}
		w.Write([]byte(`{"tokenId":"t1","token":"sk_live_x","db":"app",` +
			`"capability":{"type":"parMeshV1","host":"macdaddy"}}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	out, err := c.Enroll(context.Background(), "otc-secret")
	if err != nil {
		t.Fatal(err)
	}
	if out.TokenID != "t1" || out.Token != "sk_live_x" || out.DB != "app" ||
		out.Capability.Host != "macdaddy" {
		t.Fatalf("got %+v", out)
	}
	if out.Capability.Type != wire.HostCapabilityTypeParMeshV1 {
		t.Fatalf("capability type %q", out.Capability.Type)
	}
}

func TestEnrollSurfacesErrorEnvelope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"code":"UNAUTHORIZED","message":"invalid or expired grant"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	// The generic 401 envelope — not a leaked reason — must reach the caller
	// as the UNAUTHORIZED code, not a collapsed INTERNAL fallback.
	if _, err := c.Enroll(context.Background(), "bad-grant"); err == nil ||
		!rtdberrors.IsCode(err, rtdberrors.CodeUnauthorized) {
		t.Fatalf("want UNAUTHORIZED envelope, got %v", err)
	}
}
