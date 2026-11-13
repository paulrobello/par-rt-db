// go-client/httpclient/client_extra_test.go
package httpclient

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// The base URL drops a trailing slash exactly once; DB() reports the
// configured database.
func TestNewClientTrimsBaseURL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/query" {
			t.Errorf("path %s", r.URL.Path)
		}
		w.Write([]byte(`{"result":[]}`))
	}))
	defer srv.Close()

	c := NewClient(srv.URL+"/", "d1", "tk")
	if c.DB() != "d1" {
		t.Fatalf("db %q", c.DB())
	}
	if _, err := c.QueryRaw(context.Background(), wire.Query{Table: "items"}); err != nil {
		t.Fatalf("request against trimmed base failed: %v", err)
	}
}

// WithHTTPClient swaps the underlying net/http client (timeouts,
// transports).
func TestWithHTTPClientOption(t *testing.T) {
	hc := &http.Client{Timeout: 5 * time.Second}
	c := NewClient("http://localhost:1", "d1", "tk", WithHTTPClient(hc))
	if _, err := c.QueryRaw(context.Background(), wire.Query{Table: "items"}); err == nil {
		t.Fatal("want transport error against closed port, got nil")
	}
}

// Call exposes the authed JSON seam: headers set, out decoded.
func TestCallSeamDecodesOut(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tk" {
			t.Errorf("auth %q", r.Header.Get("Authorization"))
		}
		w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	var out struct {
		OK bool `json:"ok"`
	}
	if err := c.Call(context.Background(), "POST", "/api/whatever", nil, &out); err != nil {
		t.Fatal(err)
	}
	if !out.OK {
		t.Fatalf("out %+v", out)
	}
}

// A body that fails JSON encoding surfaces as an encode error, not a
// transport failure.
func TestCallEncodeError(t *testing.T) {
	c := NewClient("http://localhost:1", "d1", "tk")
	body := map[string]any{"bad": make(chan int)}
	err := c.Call(context.Background(), "POST", "/api/whatever", body, nil)
	if err == nil || !strings.Contains(err.Error(), "encode request") {
		t.Fatalf("want encode error, got %v", err)
	}
}

// RawCall sends the body verbatim with the given content type and returns
// the undecoded 2xx bytes.
func TestRawCallRoundTrip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Content-Type") != "text/csv" {
			t.Errorf("content type %q", r.Header.Get("Content-Type"))
		}
		if r.Header.Get("Authorization") != "Bearer tk" {
			t.Errorf("auth %q", r.Header.Get("Authorization"))
		}
		b, _ := io.ReadAll(r.Body)
		w.Write(b)
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	got, err := c.RawCall(context.Background(), "POST", "/api/raw", "text/csv", []byte("a,b\n1,2"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "a,b\n1,2" {
		t.Fatalf("raw body %q", got)
	}
}

// An empty contentType omits the Content-Type header.
func TestRawCallEmptyContentTypeOmitsHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if ct := r.Header.Get("Content-Type"); ct != "" {
			t.Errorf("content type %q, want empty", ct)
		}
		w.Write([]byte("ok"))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	if _, err := c.RawCall(context.Background(), "GET", "/api/raw", "", nil); err != nil {
		t.Fatal(err)
	}
}

// Non-2xx with a JSON envelope decodes into RtDbError.
func TestRawCallErrorEnvelope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"code":"BAD_REQUEST","message":"no"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	_, err := c.RawCall(context.Background(), "POST", "/api/raw", "", nil)
	if err == nil {
		t.Fatal("want error")
	}
	if !rtdberrors.IsCode(err, "BAD_REQUEST") {
		t.Fatalf("error %v", err)
	}
}

// Non-2xx with a non-JSON body falls back to the truncated-INTERNAL
// envelope.
func TestErrorEnvelopeNonJSONFallback(t *testing.T) {
	long := strings.Repeat("x", 300)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		w.Write([]byte(long))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	_, err := c.QueryRaw(context.Background(), wire.Query{Table: "items"})
	if err == nil {
		t.Fatal("want error")
	}
	var re *rtdberrors.RtDbError
	if !errors.As(err, &re) {
		t.Fatalf("error type %T", err)
	}
	if re.Code != rtdberrors.CodeInternal {
		t.Fatalf("code %q", re.Code)
	}
	if !strings.HasPrefix(re.Message, "HTTP 502: ") {
		t.Fatalf("message %q", re.Message)
	}
	if !strings.HasSuffix(re.Message, "…") || strings.Count(re.Message, "x") != 200 {
		t.Fatalf("message not truncated: %q", re.Message)
	}
}

// A short non-JSON body passes through untruncated.
func TestTruncateShortBodyUnchanged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("boom"))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	_, err := c.QueryRaw(context.Background(), wire.Query{Table: "items"})
	var re *rtdberrors.RtDbError
	if !errors.As(err, &re) || re.Message != "HTTP 500: boom" {
		t.Fatalf("message %q", err)
	}
}

// RawStream returns the open 2xx body for incremental reads.
func TestRawStreamSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("chunk1"))
		w.Write([]byte("chunk2"))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	body, err := c.RawStream(context.Background(), "POST", "/api/stream", "application/octet-stream", strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	defer body.Close()
	got, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "chunk1chunk2" {
		t.Fatalf("stream %q", got)
	}
}

// RawStream drains an error status and decodes the envelope.
func TestRawStreamErrorPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"code":"NOT_FOUND","message":"gone"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	_, err := c.RawStream(context.Background(), "POST", "/api/stream", "", nil)
	if !rtdberrors.IsCode(err, "NOT_FOUND") {
		t.Fatalf("error %v", err)
	}
}

// A dead transport surfaces as a transport error on every seam.
func TestTransportErrorOnEverySeam(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	ctx := context.Background()

	for name, fn := range map[string]func() error{
		"call": func() error { return c.Call(ctx, "POST", "/api/x", nil, nil) },
		"rawcall": func() error {
			_, err := c.RawCall(ctx, "POST", "/api/x", "", nil)
			return err
		},
		"rawstream": func() error {
			_, err := c.RawStream(ctx, "POST", "/api/x", "", nil)
			return err
		},
		"do": func() error {
			_, err := c.QueryRaw(ctx, wire.Query{Table: "t"})
			return err
		},
	} {
		err := fn()
		if err == nil || !strings.Contains(err.Error(), "transport") {
			t.Fatalf("%s: want transport error, got %v", name, err)
		}
	}
}
