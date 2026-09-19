// go-client/httpclient/storage.go
package httpclient

// Mirrors rust-client/src/http.rs storage section. Uploads are RAW bodies
// (the plan's "multipart" sketch was wrong — the server takes the bytes
// directly with an optional Content-Type header). The public serve base is
// /storage/{id}; admin/managed routes live under /api/storage/{db}.

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

// UploadResult mirrors rust http.rs::UploadResult.
type UploadResult struct {
	ID          string  `json:"id"`
	SHA256      string  `json:"sha256"`
	Size        int64   `json:"size"`
	ContentType *string `json:"contentType,omitempty"`
}

// FileMetadata mirrors rust http.rs::FileMetadata.
type FileMetadata struct {
	ID           string  `json:"id"`
	SHA256       string  `json:"sha256"`
	Size         int64   `json:"size"`
	ContentType  *string `json:"contentType,omitempty"`
	CreationTime int64   `json:"creationTime"`
}

// SignedUrl mirrors rust http.rs::SignedUrl.
type SignedUrl struct {
	URL       string `json:"url"`
	ExpiresAt int64  `json:"expiresAt"`
}

// Fit modes for TransformURL.
type Fit string

// Fit modes, mirroring rust's Fit.
const (
	FitCover   Fit = "cover"
	FitContain Fit = "contain"
	FitInside  Fit = "inside"
	FitOutside Fit = "outside"
)

// OutFormat for TransformURL.
type OutFormat string

// Output formats, mirroring rust's OutFormat.
const (
	FormatJPEG OutFormat = "jpeg"
	FormatPNG  OutFormat = "png"
	FormatWebP OutFormat = "webp"
	FormatAVIF OutFormat = "avif"
)

// TransformOpts mirrors rust http.rs::TransformOpts — only non-nil fields
// appear in the query.
type TransformOpts struct {
	W      *int
	H      *int
	Fit    Fit
	Q      *int
	Format OutFormat
}

// Upload posts raw bytes; content type is stored. Returns server metadata.
func (c *Client) Upload(ctx context.Context, contentType string, r io.Reader) (*UploadResult, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("httpclient: read upload: %w", err)
	}
	var out UploadResult
	if err := c.doRaw(ctx, http_POST, "/api/storage/"+c.db, contentType, data, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// UploadBytes is Upload for in-memory bytes.
func (c *Client) UploadBytes(ctx context.Context, contentType string, data []byte) (*UploadResult, error) {
	var out UploadResult
	if err := c.doRaw(ctx, http_POST, "/api/storage/"+c.db, contentType, data, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Download streams the file's bytes from the public serve route. Non-2xx
// responses are surfaced as *errors.RtDbError, never as a body the caller
// could mistake for the payload.
func (c *Client) Download(ctx context.Context, id string) (io.ReadCloser, error) {
	resp, err := c.doRawResponse(ctx, http_GET, "/storage/"+id, "", nil)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("httpclient: read error body: %w", err)
		}
		return nil, envelopeError(resp.StatusCode, data)
	}
	return resp.Body, nil
}

// DeleteFile removes the file (idempotent). DELETE /api/storage/{db}/{id}.
func (c *Client) DeleteFile(ctx context.Context, id string) error {
	var resp struct {
		OK bool `json:"ok"`
	}
	if err := c.do(ctx, http_DELETE, "/api/storage/"+c.db+"/"+id, nil, &resp); err != nil {
		return err
	}
	if !resp.OK {
		return fmt.Errorf("httpclient: delete file returned ok=false")
	}
	return nil
}

// GetFileMetadata fetches stored metadata. GET .../metadata.
func (c *Client) GetFileMetadata(ctx context.Context, id string) (*FileMetadata, error) {
	var out FileMetadata
	if err := c.do(ctx, http_GET, "/api/storage/"+c.db+"/"+id+"/metadata", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetSignedURL mints a time-limited signed URL. GET .../signed-url.
func (c *Client) GetSignedURL(ctx context.Context, id string, ttlSeconds *int) (*SignedUrl, error) {
	path := "/api/storage/" + c.db + "/" + id + "/signed-url"
	if ttlSeconds != nil {
		path += "?ttlSeconds=" + strconv.Itoa(*ttlSeconds)
	}
	var out SignedUrl
	if err := c.do(ctx, http_GET, path, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetURL returns the public serve URL.
func (c *Client) GetURL(id string) string {
	return c.baseURL + "/storage/" + id
}

// TransformURL returns the public serve URL with transform query params.
func (c *Client) TransformURL(id string, opts TransformOpts) string {
	parts := []string{}
	if opts.W != nil {
		parts = append(parts, "w="+strconv.Itoa(*opts.W))
	}
	if opts.H != nil {
		parts = append(parts, "h="+strconv.Itoa(*opts.H))
	}
	if opts.Fit != "" {
		parts = append(parts, "fit="+string(opts.Fit))
	}
	if opts.Q != nil {
		parts = append(parts, "q="+strconv.Itoa(*opts.Q))
	}
	if opts.Format != "" {
		parts = append(parts, "format="+string(opts.Format))
	}
	url := c.GetURL(id)
	if len(parts) == 0 {
		return url
	}
	return url + "?" + strings.Join(parts, "&")
}

// doRaw posts/gets a raw body (uploads) through the same auth seam.
func (c *Client) doRaw(ctx context.Context, method, path, contentType string, body []byte, out any) error {
	resp, err := c.doRawResponse(ctx, method, path, contentType, body)
	if err != nil {
		return err
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

func (c *Client) doRawResponse(ctx context.Context, method, path, contentType string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("httpclient: build request: %w", err)
	}
	if body != nil {
		req.Body = io.NopCloser(bytes.NewReader(body))
		req.ContentLength = int64(len(body))
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
	return resp, nil
}
