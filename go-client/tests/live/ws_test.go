//go:build live

// Live WebSocket tests — the port of rust tests/ws_integration.rs:33-88:
// subscribe, first snapshot, WS-mutate insert, the live queryUpdate
// reflecting it (polled with a 10s deadline), then unsubscribe.
package live

import (
	"context"
	"testing"
	"time"

	"github.com/paulrobello/par-rt-db/go-client/wire"
	"github.com/paulrobello/par-rt-db/go-client/wsclient"
)

func newWS(t *testing.T, e *liveEnv) *wsclient.Client {
	t.Helper()
	c := wsclient.NewClient(e.wsURL, e.db, func(context.Context) (string, error) { return e.token, nil })
	if err := c.Connect(context.Background()); err != nil {
		t.Fatalf("ws connect: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestLiveWSSubscribeMutateUpdate(t *testing.T) {
	e := envOrSkip(t)
	e.setup()
	c := newWS(t, e)
	ctx := context.Background()

	sub, err := c.Subscribe(ctx, wire.Query{Table: "items"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Close()

	// First snapshot arrives right after subscribe (may be empty).
	select {
	case <-sub.Updates():
	case <-time.After(10 * time.Second):
		t.Fatal("no first snapshot")
	}

	// Insert over the SAME ws connection; the live query must reflect it.
	_, err = c.Mutate(ctx, wire.Transaction{Steps: []wire.Step{wire.StepInsert{
		Table: "items", Doc: wire.Object{"name": wire.String("ws-live"), "n": wire.String("42")},
	}}}, "")
	if err != nil {
		t.Fatalf("ws mutate: %v", err)
	}

	deadline := time.After(10 * time.Second)
	for {
		select {
		case snap := <-sub.Updates():
			if arr, ok := snap.Value.(wire.Array); ok && len(arr) > 0 {
				doc, ok := arr[0].(wire.Object)
				if !ok {
					t.Fatalf("snapshot doc shape: %v", arr[0])
				}
				if name, ok := doc["name"].(wire.String); ok && string(name) == "ws-live" {
					return // the live update reflected the insert
				}
			}
		case <-deadline:
			t.Fatal("queryUpdate reflecting the insert never arrived")
		}
	}
}

func TestLiveWSUnsubscribeStopsUpdates(t *testing.T) {
	e := envOrSkip(t)
	e.setup()
	c := newWS(t, e)
	ctx := context.Background()

	sub, err := c.Subscribe(ctx, wire.Query{Table: "items"})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	select {
	case <-sub.Updates():
	case <-time.After(10 * time.Second):
		t.Fatal("no first snapshot")
	}
	sub.Close()
	time.Sleep(200 * time.Millisecond)

	_, err = c.Mutate(ctx, wire.Transaction{Steps: []wire.Step{wire.StepInsert{
		Table: "items", Doc: wire.Object{"name": wire.String("after-unsub"), "n": wire.String("1")},
	}}}, "")
	if err != nil {
		t.Fatalf("mutate after unsub: %v", err)
	}
	select {
	case snap, ok := <-sub.Updates():
		if !ok {
			// Unsubscribe closed the updates channel: clean.
			return
		}
		if snap.Value != nil {
			t.Fatalf("update arrived after unsubscribe: %v", snap.Value)
		}
	case <-time.After(1500 * time.Millisecond):
		// No update within the window: unsubscribed cleanly.
	}
}
