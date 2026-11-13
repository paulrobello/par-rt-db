// go-client/httpclient/bulk_test.go
package httpclient

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// BulkDelete posts {db, table, filter} and decodes the accepted op.
func TestBulkDeleteRequestShape(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/bulk/delete" {
			t.Errorf("path %s", r.URL.Path)
		}
		b, _ := io.ReadAll(r.Body)
		var body map[string]any
		if err := json.Unmarshal(b, &body); err != nil {
			t.Errorf("body %s", b)
		}
		if body["db"] != "d1" || body["table"] != "items" {
			t.Errorf("body %v", body)
		}
		filter, _ := body["filter"].(map[string]any)
		if filter == nil || filter["op"] != "eq" || filter["field"] != "done" {
			t.Errorf("filter %v", body["filter"])
		}
		w.Write([]byte(`{"opId":"op1","matched":42,"state":"running"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	accepted, err := c.BulkDelete(context.Background(), "d1", "items",
		wire.FilterEq{Field: "done", Value: wire.Bool(true)})
	if err != nil {
		t.Fatal(err)
	}
	if accepted.OpID != "op1" || accepted.Matched != 42 || accepted.State != "running" {
		t.Fatalf("accepted %+v", accepted)
	}
}

// BulkStatus polls GET /api/bulk/{id}?db= with the id path-escaped.
func TestBulkStatusRequestShape(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/bulk/op/1" {
			t.Errorf("path %s", r.URL.Path)
		}
		if r.URL.Query().Get("db") != "d1" {
			t.Errorf("db %q", r.URL.Query().Get("db"))
		}
		w.Write([]byte(`{"id":"op/1","state":"running","matched":42,"deleted":10}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	info, err := c.BulkStatus(context.Background(), "d1", "op/1")
	if err != nil {
		t.Fatal(err)
	}
	if info.ID != "op/1" || info.State != "running" || info.Matched != 42 || info.Deleted != 10 {
		t.Fatalf("info %+v", info)
	}
}

// BulkStatus surfaces the error field when the op state is failed.
func TestBulkStatusFailedState(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"id":"op1","state":"failed","matched":3,"deleted":0,"error":"over cap"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	info, err := c.BulkStatus(context.Background(), "d1", "op1")
	if err != nil {
		t.Fatal(err)
	}
	if info.State != "failed" || info.Error == nil || *info.Error != "over cap" {
		t.Fatalf("info %+v", info)
	}
}

// BulkDelete maps a server error envelope (e.g. over-cap BAD_REQUEST) onto
// the standard error.
func TestBulkDeleteErrorEnvelope(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"code":"BAD_REQUEST","message":"narrow the filter"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	_, err := c.BulkDelete(context.Background(), "d1", "items",
		wire.FilterEq{Field: "done", Value: wire.Bool(true)})
	if err == nil || err.Error() != "BAD_REQUEST: narrow the filter" {
		t.Fatalf("error %v", err)
	}
}
