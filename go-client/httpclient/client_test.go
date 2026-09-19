// go-client/httpclient/client_test.go
package httpclient

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestMutateSendsExactBodyAndHeaders(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/mutate" {
			t.Errorf("path %s", r.URL.Path)
		}
		if got := r.Header.Get("X-Rtdb-Protocol"); got != "1" {
			t.Errorf("proto header %q", got)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer tk" {
			t.Errorf("auth %q", got)
		}
		b, _ := io.ReadAll(r.Body)
		if !bytes.Contains(b, []byte(`"db":"d1"`)) || bytes.Contains(b, []byte(`"idempotencyKey"`)) {
			t.Errorf("body %s", b)
		}
		w.Write([]byte(`{"results":[{"id":"i1"},null]}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	res, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{
		wire.StepInsert{Table: "items", Doc: wire.Object(nil)},
	}}, "")
	if err != nil || len(res) != 2 {
		t.Fatalf("%v %v", res, err)
	}
}

func TestMutateWithIdempotencyKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if !bytes.Contains(b, []byte(`"idempotencyKey":"abc"`)) {
			t.Errorf("body %s", b)
		}
		w.Write([]byte(`{"results":[]}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	if _, err := c.Mutate(context.Background(), wire.Transaction{}, "abc"); err != nil {
		t.Fatal(err)
	}
}

func TestErrorEnvelopeDecode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		w.Write([]byte(`{"code":"RATE_LIMITED","message":"slow","retryAfter":2}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	_, err := c.QueryRaw(context.Background(), wire.Query{Table: "items"})
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "RATE_LIMITED: slow" {
		t.Fatalf("error %q", err.Error())
	}
}

func TestQueryTypedDecode(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/query" {
			t.Errorf("path %s", r.URL.Path)
		}
		w.Write([]byte(`{"result":[{"n":1},{"n":2}]}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	type row struct {
		N int `json:"n"`
	}
	rows, err := Query[[]row](context.Background(), c, wire.Query{Table: "items"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[1].N != 2 {
		t.Fatalf("rows %v", rows)
	}
}

func TestQueryBatchMixed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/query-batch" {
			t.Errorf("path %s", r.URL.Path)
		}
		w.Write([]byte(`{"results":[{"ok":true,"result":1},{"ok":false,"error":{"code":"BAD_REQUEST","message":"no"}}]}`))
	}))
	defer srv.Close()
	c := NewClient(srv.URL, "d1", "tk")
	slots, err := QueryBatch[int](context.Background(), c, []wire.Query{{Table: "a"}, {Table: "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(slots) != 2 || !slots[0].OK || slots[0].Value != 1 || slots[1].OK || slots[1].Err == nil {
		t.Fatalf("slots %+v", slots)
	}
}
