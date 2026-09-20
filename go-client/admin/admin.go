// go-client/admin/admin.go
package admin

// Mirrors rust-client/src/admin/mod.rs — the /admin/* control-plane client.
// The bearer is the instance admin key; the auth seam is the shared
// httpclient.Client (constructed with an empty db, since admin routes carry
// the database in the path or body). Dynamic path segments are injected
// raw, matching rust format! paths.

import (
	"context"
	"net/url"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/httpclient"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// AdminClient drives one par-rt-db instance's /admin/* control plane.
// Every call sends the admin key as the bearer.
type AdminClient struct {
	api *httpclient.Client
	// baseURL/adminKey back the /admin/stream websocket seam (stream.go);
	// the HTTP seam carries the same values inside httpclient.Client.
	baseURL  string
	adminKey string
}

// NewAdminClient builds a client against baseURL authenticated with the
// instance admin key. httpclient options (e.g. WithHTTPClient) flow through.
func NewAdminClient(baseURL, adminKey string, opts ...httpclient.Option) *AdminClient {
	return &AdminClient{
		api:      httpclient.NewClient(baseURL, "", adminKey, opts...),
		baseURL:  baseURL,
		adminKey: adminKey,
	}
}

// Method literals (httpclient's are unexported).
const (
	methodGet    = "GET"
	methodPost   = "POST"
	methodPut    = "PUT"
	methodPatch  = "PATCH"
	methodDelete = "DELETE"
)

// ok decodes the {ok:true} ack; ok=false is an INTERNAL error, like rust's
// expect_ok.
func (c *AdminClient) ok(ctx context.Context, method, path string, body any) error {
	var resp struct {
		OK bool `json:"ok"`
	}
	if err := c.api.Call(ctx, method, path, body, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return rtdberrors.New(rtdberrors.CodeInternal, "admin request returned ok=false")
	}
	return nil
}

// get issues a GET with optional query and decodes the JSON body into out.
func (c *AdminClient) get(ctx context.Context, path string, query url.Values, out any) error {
	return c.api.Call(ctx, methodGet, withQuery(path, query), nil, out)
}

// jsonValue GETs and decodes a dynamic JSON body (GetSchema).
func (c *AdminClient) jsonValue(ctx context.Context, path string, query url.Values) (wire.JSONValue, error) {
	data, err := c.api.RawCall(ctx, methodGet, withQuery(path, query), "", nil)
	if err != nil {
		return nil, err
	}
	return wire.UnmarshalJSON(data)
}

// raw sends a verbatim body / returns the undecoded 2xx body
// (export/import/download/upload).
func (c *AdminClient) raw(ctx context.Context, method, path, contentType string, body []byte) ([]byte, error) {
	return c.api.RawCall(ctx, method, path, contentType, body)
}

// withQuery appends ?k=v&… when q is non-empty.
func withQuery(path string, q url.Values) string {
	if len(q) == 0 {
		return path
	}
	return path + "?" + q.Encode()
}
