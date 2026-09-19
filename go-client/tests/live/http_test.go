//go:build live

// Live HTTP-surface tests: typed query round-trip, mutate idempotency
// replay, storage upload/download/delete, schedule create/cancel.
package live

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/paulrobello/par-rt-db/go-client/dsl"
	"github.com/paulrobello/par-rt-db/go-client/httpclient"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

type item struct {
	Name string  `json:"name"`
	N    *string `json:"n"`
	ID   string  `json:"_id"`
}

func newHTTP(t *testing.T, e *liveEnv) *httpclient.Client {
	t.Helper()
	return httpclient.NewClient(e.httpURL, e.db, e.token)
}

func TestLiveQueryRoundTripTyped(t *testing.T) {
	e := envOrSkip(t)
	e.setup()
	c := newHTTP(t, e)
	ctx := context.Background()
	_, err := c.Mutate(ctx, wire.Transaction{Steps: []wire.Step{wire.StepInsert{
		Table: "items", Doc: wire.Object{"name": wire.String("alpha"), "n": wire.String("7")},
	}}}, "")
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	q := dsl.NewTableQuery("items").WithIndex("by_n", wire.String("7"))
	items, err := httpclient.Query[[]item](ctx, c, q.Build())
	if err != nil {
		t.Fatalf("typed query: %v", err)
	}
	if len(items) != 1 || items[0].Name != "alpha" || items[0].N == nil || *items[0].N != "7" {
		t.Fatalf("typed round-trip: %+v", items)
	}
}

func TestLiveMutateIdempotencyReplay(t *testing.T) {
	e := envOrSkip(t)
	e.setup()
	c := newHTTP(t, e)
	ctx := context.Background()
	txn := wire.Transaction{Steps: []wire.Step{wire.StepInsert{
		Table: "items", Doc: wire.Object{"name": wire.String("once"), "n": wire.String("1")},
	}}}
	first, err := c.Mutate(ctx, txn, "live-idem-1")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := c.Mutate(ctx, txn, "live-idem-1")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if first[0].ID == nil || second[0].ID == nil || *first[0].ID != *second[0].ID {
		t.Fatalf("replay diverged: %v vs %v", first[0], second[0])
	}
}

func TestLiveStorageUploadDownloadDelete(t *testing.T) {
	e := envOrSkip(t)
	e.setup()
	c := newHTTP(t, e)
	ctx := context.Background()
	payload := []byte("live storage payload")
	up, err := c.UploadBytes(ctx, "text/plain", payload)
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	rc, err := c.Download(ctx, up.ID)
	if err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := io.ReadAll(rc)
	rc.Close()
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("round-trip: %q", got)
	}
	if err := c.DeleteFile(ctx, up.ID); err != nil {
		t.Fatalf("delete: %v", err)
	}
}

func TestLiveScheduleCreateCancel(t *testing.T) {
	e := envOrSkip(t)
	e.setup()
	c := newHTTP(t, e)
	ctx := context.Background()
	id, err := c.Schedule(ctx, wire.WhenAfterMs{Ms: 3_600_000}, wire.Transaction{Steps: []wire.Step{
		wire.StepInsert{Table: "items", Doc: wire.Object{"name": wire.String("later"), "n": wire.String("1")}},
	}})
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	if id == "" {
		t.Fatal("empty schedule id")
	}
	cancelled, err := e.admin.CancelSchedule(ctx, e.db, id)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if !cancelled {
		t.Fatal("fresh schedule must cancel")
	}
}

// time is used by ws_test; keep the import honest here if unused.
var _ = time.Second
