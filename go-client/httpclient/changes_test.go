// go-client/httpclient/changes_test.go
package httpclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestChangesDecode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/db/d1/changes" {
			t.Errorf("%s %s", r.Method, r.URL.Path)
		}
		q := r.URL.Query()
		if q.Get("since") != "40" || q.Get("table") != "items" || q.Get("limit") != "2" {
			t.Errorf("query %s", r.URL.RawQuery)
		}
		if r.Header.Get("Authorization") != "Bearer tk" {
			t.Errorf("auth %q", r.Header.Get("Authorization"))
		}
		w.Write([]byte(`{"ops":[` +
			`{"seq":41,"table":"items","docId":"d41","kind":"patch","doc":{"title":"x"},"ts":1758300000000},` +
			`{"seq":42,"table":"items","docId":"d42","kind":"delete","doc":null,"ts":1758300000001}],` +
			`"nextSeq":42,"head":57,"logId":"0a1b2c3d4e5f6071"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	resp, err := c.Changes(context.Background(), 40, "items", 2)
	if err != nil {
		t.Fatal(err)
	}
	if resp.NextSeq != 42 || resp.Head != 57 || resp.LogID != "0a1b2c3d4e5f6071" || len(resp.Ops) != 2 {
		t.Fatalf("resp %+v", resp)
	}
	if resp.Ops[0].Kind != wire.ChangeKindPatch || resp.Ops[0].DocID != "d41" {
		t.Fatalf("op0 %+v", resp.Ops[0])
	}
	if doc, ok := resp.Ops[0].Doc.(wire.Object); !ok || doc["title"] != wire.String("x") {
		t.Fatalf("op0 doc %#v", resp.Ops[0].Doc)
	}
	if _, ok := resp.Ops[1].Doc.(wire.Null); !ok || resp.Ops[1].Kind != wire.ChangeKindDelete {
		t.Fatalf("op1 %+v", resp.Ops[1])
	}
}

func TestChangesOmitsOptionalParamsAndCursorExpired(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "since=0" {
			t.Errorf("query %s", r.URL.RawQuery)
		}
		w.WriteHeader(http.StatusGone)
		w.Write([]byte(`{"code":"CURSOR_EXPIRED","message":"cursor predates retention"}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	_, err := c.Changes(context.Background(), 0, "", 0)
	if !rtdberrors.IsCode(err, rtdberrors.CodeCursorExpired) {
		t.Fatalf("want CURSOR_EXPIRED, got %v", err)
	}
}
