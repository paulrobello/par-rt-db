// go-client/httpclient/storage_test.go
package httpclient

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
)

func TestUploadRawBodyAndHeader(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/storage/d1" {
			t.Errorf("path %s", r.URL.Path)
		}
		if got := r.Header.Get("Content-Type"); got != "image/png" {
			t.Errorf("content type %q", got)
		}
		b, _ := io.ReadAll(r.Body)
		if string(b) != "hello-bytes" {
			t.Errorf("body %q", string(b))
		}
		w.Write([]byte(`{"id":"f1","sha256":"abc","size":11}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	res, err := c.UploadBytes(context.Background(), "image/png", []byte("hello-bytes"))
	if err != nil {
		t.Fatal(err)
	}
	if res.ID != "f1" || res.Size != 11 {
		t.Fatalf("upload %+v", res)
	}
}

func TestTransformURLParams(t *testing.T) {
	c := NewClient("http://x", "d1", "tk")
	w := 100
	h := 200
	got := c.TransformURL("f1", TransformOpts{W: &w, H: &h, Fit: FitCover, Format: FormatWebP})
	want := "http://x/storage/f1?w=100&h=200&fit=cover&format=webp"
	if got != want {
		t.Fatalf("transform url %s", got)
	}
	if c.GetURL("f1") != "http://x/storage/f1" {
		t.Fatalf("get url %s", c.GetURL("f1"))
	}
}

func TestDownloadAndDelete(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/storage/f1":
			w.Write([]byte("payload"))
		case r.Method == http.MethodDelete && r.URL.Path == "/api/storage/d1/f1":
			w.Write([]byte(`{"ok":true}`))
		default:
			t.Errorf("%s %s", r.Method, r.URL.Path)
		}
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	rc, err := c.Download(context.Background(), "f1")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(rc)
	rc.Close()
	if string(data) != "payload" {
		t.Fatalf("download %q", string(data))
	}
	if err := c.DeleteFile(context.Background(), "f1"); err != nil {
		t.Fatal(err)
	}
}

func TestListFiles(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/storage/d1" {
			t.Errorf("%s %s", r.Method, r.URL.Path)
		}
		if r.URL.RawQuery != "" {
			t.Errorf("unexpected query %q", r.URL.RawQuery)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tk" {
			t.Errorf("auth %q", got)
		}
		w.Write([]byte(`{"files":[{"id":"f2","sha256":"def","size":5,"contentType":"text/plain","creationTime":1700000002000},{"id":"f1","sha256":"abc","size":11,"creationTime":1700000001000}]}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	files, err := c.ListFiles(context.Background(), 0, -1)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatalf("files %d", len(files))
	}
	if files[0].ID != "f2" || files[0].ContentType == nil || *files[0].ContentType != "text/plain" {
		t.Fatalf("files[0] %+v", files[0])
	}
	if files[1].ContentType != nil {
		t.Fatalf("files[1] contentType %v", files[1].ContentType)
	}
}

func TestListFilesPaging(t *testing.T) {
	cases := []struct {
		name   string
		limit  int
		offset int
		want   url.Values
	}{
		{"both set", 25, 50, url.Values{"limit": {"25"}, "offset": {"50"}}},
		{"limit only", 10, -1, url.Values{"limit": {"10"}}},
		{"offset only", 0, 100, url.Values{"offset": {"100"}}},
		{"neither", 0, -1, url.Values{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if got := r.URL.Query(); !reflect.DeepEqual(got, tc.want) {
					t.Errorf("query %v, want %v", got, tc.want)
				}
				w.Write([]byte(`{"files":[]}`))
			}))
			defer srv.Close()
			c := NewClient(srv.URL, "d1", "tk")
			files, err := c.ListFiles(context.Background(), tc.limit, tc.offset)
			if err != nil {
				t.Fatal(err)
			}
			if len(files) != 0 {
				t.Fatalf("files %d", len(files))
			}
		})
	}
}

func TestDownloadErrorSurfacesAsRtDbError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"code":"NOT_FOUND","message":"gone"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	_, err := c.Download(context.Background(), "missing")
	if err == nil {
		t.Fatal("404 download must error")
	}
	if err.Error() != "NOT_FOUND: gone" {
		t.Fatalf("error %q", err.Error())
	}
}
