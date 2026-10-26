package wsclient

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func feedOp(seq int64) wire.ChangeOp {
	return wire.ChangeOp{Seq: seq, Table: "t", DocID: "d", Kind: wire.ChangeKindInsert, Doc: wire.Null{}, Ts: 1}
}

func peerSend(t *testing.T, conn *websocket.Conn, msg wire.ServerMessage) {
	t.Helper()
	b, err := json.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(context.Background(), websocket.MessageText, b); err != nil {
		t.Fatal(err)
	}
}

// nextFrame returns the next captured peer frame containing needle.
func nextFrame(t *testing.T, fp *fakePeer, needle string) map[string]any {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case data := <-fp.frames:
			if strings.Contains(string(data), needle) {
				var m map[string]any
				if err := json.Unmarshal(data, &m); err != nil {
					t.Fatal(err)
				}
				return m
			}
		case <-deadline:
			t.Fatalf("no frame containing %s", needle)
		}
	}
}

func nextBatch(t *testing.T, f *Feed) FeedBatch {
	t.Helper()
	select {
	case b, ok := <-f.Batches():
		if !ok {
			t.Fatalf("feed closed: %v", f.Err())
		}
		return b
	case <-time.After(2 * time.Second):
		t.Fatal("no batch")
		return FeedBatch{}
	}
}

func feedClient(t *testing.T, fp *fakePeer) *Client {
	t.Helper()
	c := NewClient(wsURL(fp), "d1", func(ctx context.Context) (string, error) { return "tk", nil },
		WithBackoff(10*time.Millisecond, 50*time.Millisecond))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	_ = fp.waitAuth(t)
	return c
}

func TestFeedSendsTablesAndOmitsSinceWhenAbsent(t *testing.T) {
	fp := newFakePeer(t)
	c := feedClient(t, fp)
	f, err := c.WatchChanges(context.Background(), FeedOptions{Tables: []string{"tasks", "projects"}})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	m := nextFrame(t, fp, `"type":"feedSubscribe"`)
	if _, has := m["since"]; has {
		t.Fatalf("since must be omitted: %v", m)
	}
	if _, has := m["feedEpoch"]; has {
		t.Fatalf("feedEpoch must be omitted: %v", m)
	}
	tables, _ := m["tables"].([]any)
	if len(tables) != 2 || tables[0] != "tasks" || tables[1] != "projects" {
		t.Fatalf("tables: %v", m["tables"])
	}
	if m["feedId"] == "" {
		t.Fatal("feedId missing")
	}
}

func TestFeedSinceZeroIsSent(t *testing.T) {
	fp := newFakePeer(t)
	c := feedClient(t, fp)
	zero := int64(0)
	f, err := c.WatchChanges(context.Background(), FeedOptions{Since: &zero})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	m := nextFrame(t, fp, `"type":"feedSubscribe"`)
	if v, has := m["since"]; !has || v != float64(0) {
		t.Fatalf("since 0 must be sent: %v", m)
	}
	if _, has := m["tables"]; has {
		t.Fatalf("empty tables must be omitted: %v", m)
	}
}

func TestFeedResumesWithLastSeqDedupesAndAcks(t *testing.T) {
	fp := newFakePeer(t)
	c := feedClient(t, fp)
	f, err := c.WatchChanges(context.Background(), FeedOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sub := nextFrame(t, fp, `"type":"feedSubscribe"`)
	id := sub["feedId"].(string)
	conn1 := <-fp.conns

	// Confirming frame (empty ops, head 40) yields no batch but sets the cursor.
	peerSend(t, conn1, wire.ServerFeedEvents{FeedID: id, FeedEpoch: "E1", Ops: nil, Head: 40})
	peerSend(t, conn1, wire.ServerFeedEvents{FeedID: id, FeedEpoch: "E1", Ops: []wire.ChangeOp{feedOp(41), feedOp(42)}, Head: 42})
	b := nextBatch(t, f)
	if len(b.Ops) != 2 || b.Ops[0].Seq != 41 || b.Epoch != "E1" {
		t.Fatalf("batch: %+v", b)
	}
	ack := nextFrame(t, fp, `"type":"feedAck"`)
	if ack["seq"] != float64(42) || ack["feedId"] != id {
		t.Fatalf("ack: %v", ack)
	}

	// Drop the connection; the client must resubscribe with since=42, epoch.
	_ = conn1.Close(websocket.StatusInternalError, "boom")
	_ = fp.waitAuth(t)
	re := nextFrame(t, fp, `"type":"feedSubscribe"`)
	if re["feedId"] != id || re["since"] != float64(42) || re["feedEpoch"] != "E1" {
		t.Fatalf("resubscribe: %v", re)
	}

	// Replayed 41,42 are dropped; 43 is delivered; an all-replayed frame
	// delivers nothing.
	conn2 := <-fp.conns
	peerSend(t, conn2, wire.ServerFeedEvents{FeedID: id, FeedEpoch: "E1", Ops: []wire.ChangeOp{feedOp(41), feedOp(42)}, Head: 42})
	peerSend(t, conn2, wire.ServerFeedEvents{FeedID: id, FeedEpoch: "E1", Ops: []wire.ChangeOp{feedOp(42), feedOp(43)}, Head: 43})
	b = nextBatch(t, f)
	if len(b.Ops) != 1 || b.Ops[0].Seq != 43 {
		t.Fatalf("dedupe failed: %+v", b)
	}
	ack = nextFrame(t, fp, `"type":"feedAck"`)
	if ack["seq"] != float64(43) {
		t.Fatalf("ack: %v", ack)
	}
}

func TestFeedCursorExpiredSurfacedNotRetried(t *testing.T) {
	fp := newFakePeer(t)
	c := feedClient(t, fp)
	f, err := c.WatchChanges(context.Background(), FeedOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sub := nextFrame(t, fp, `"type":"feedSubscribe"`)
	id := sub["feedId"].(string)
	conn1 := <-fp.conns
	peerSend(t, conn1, wire.ServerFeedEvents{FeedID: id, FeedEpoch: "E1", Ops: []wire.ChangeOp{feedOp(1)}, Head: 1})
	_ = nextBatch(t, f)
	peerSend(t, conn1, wire.ServerFeedErr{FeedID: id, Error: wire.ErrorEnvelope{Code: "CURSOR_EXPIRED", Message: "gone"}})

	select {
	case _, ok := <-f.Batches():
		if ok {
			t.Fatal("unexpected batch")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel not closed")
	}
	var rerr *rtdberrors.RtDbError
	if !errors.As(f.Err(), &rerr) || rerr.Code != rtdberrors.CodeCursorExpired {
		t.Fatalf("err: %v", f.Err())
	}

	// Reconnect must NOT resubscribe the dead feed.
	_ = conn1.Close(websocket.StatusInternalError, "boom")
	_ = fp.waitAuth(t)
	deadline := time.After(300 * time.Millisecond)
	for {
		select {
		case data := <-fp.frames:
			if strings.Contains(string(data), `"type":"feedSubscribe"`) && strings.Count(string(data), id) > 0 {
				// the first subscribe was already consumed above; any more is a retry
				t.Fatalf("feed retried after terminal error: %s", data)
			}
		case <-deadline:
			return
		}
	}
}

func TestFeedCloseSendsUnsubscribe(t *testing.T) {
	fp := newFakePeer(t)
	c := feedClient(t, fp)
	ctx, cancel := context.WithCancel(context.Background())
	f, err := c.WatchChanges(ctx, FeedOptions{})
	if err != nil {
		t.Fatal(err)
	}
	sub := nextFrame(t, fp, `"type":"feedSubscribe"`)
	cancel()
	un := nextFrame(t, fp, `"type":"feedUnsubscribe"`)
	if un["feedId"] != sub["feedId"] {
		t.Fatalf("unsubscribe: %v", un)
	}
	select {
	case _, ok := <-f.Batches():
		if ok {
			t.Fatal("unexpected batch")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("channel not closed")
	}
	if f.Err() != nil {
		t.Fatalf("clean close should have nil Err, got %v", f.Err())
	}
}
