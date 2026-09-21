// go-client/admin/stream_test.go
package wsclient

// Tests for the /admin/stream mirror (stream.go), against a mock stream:
// an httptest server that upgrades with coder/websocket and pushes frames.
// Covers the rust/ts semantics: unknown-kind frames are skipped, transport
// loss reconnects on backoff, a rejected handshake and a mid-stream 4401
// close are terminal without retry.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
)

// testStreamKey mirrors the admin package's test key literal.
const testStreamKey = "test-admin-key"

// shortBackoff shrinks the reconnect backoff for the test's duration.
func shortBackoff(t *testing.T) {
	t.Helper()
	oldBase, oldMax := streamBackoffBase, streamBackoffMax
	streamBackoffBase = 2 * time.Millisecond
	streamBackoffMax = 4 * time.Millisecond
	t.Cleanup(func() { streamBackoffBase, streamBackoffMax = oldBase, oldMax })
}

// opFrame builds a wire-shaped op frame.
func opFrame(docID string) string {
	b, _ := json.Marshal(map[string]any{
		"kind": "op",
		"event": map[string]any{
			"db": "d", "table": "items", "docId": docID, "kind": "insert", "ts": 1,
		},
	})
	return string(b)
}

// streamStub upgrades every request and hands each connection to onConn
// with its 1-based hit count; hits counts upgrades (auth failures included).
func streamStub(t *testing.T, onConn func(conn *websocket.Conn, hits int32)) (*AdminStreamClient, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		onConn(conn, n)
	}))
	t.Cleanup(srv.Close)
	return NewAdminStreamClient(srv.URL, testStreamKey), &hits
}

// writeText pushes one text frame with a 1s budget.
func writeText(conn *websocket.Conn, s string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = conn.Write(ctx, websocket.MessageText, []byte(s))
}

// recvEvent reads one event or fails the test after 2s.
func recvEvent(t *testing.T, ch <-chan AdminStreamEvent) AdminStreamEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for a stream event")
		return AdminStreamEvent{}
	}
}

// expectClosed drains the channel until it closes (2s budget).
func expectClosed(t *testing.T, ch <-chan AdminStreamEvent) {
	t.Helper()
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatal("channel did not close")
		}
	}
}

// skip + reconnect: op, unknown kind, gauges from connection 1 (dropped
// abruptly), then connection 2 replays. The unknown frame must never
// surface, and the stream must continue after the drop.
func TestStreamAdminSkipsUnknownKindsAndReconnects(t *testing.T) {
	shortBackoff(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, hits := streamStub(t, func(conn *websocket.Conn, hits int32) {
		if hits == 1 {
			writeText(conn, opFrame("doc-1"))
			writeText(conn, `{"kind":"futureKind","x":1}`)
			writeText(conn, `{"kind":"gauges","gauges":{}}`)
			_ = conn.CloseNow() // abrupt transport drop, not a clean close
			return
		}
		writeText(conn, opFrame("doc-2"))
		<-ctx.Done() // hold the connection until the test ends
	})
	ch, err := client.StreamAdmin(ctx, nil, nil)
	if err != nil {
		t.Fatalf("StreamAdmin: %v", err)
	}

	ev := recvEvent(t, ch)
	if ev.Frame == nil || ev.Frame.Event == nil || ev.Frame.Event.DocID != "doc-1" {
		t.Fatalf("expected doc-1 op frame, got %+v", ev)
	}
	ev = recvEvent(t, ch)
	if ev.Frame == nil || ev.Frame.Gauges == nil {
		t.Fatalf("expected gauges frame (unknown kind skipped), got %+v", ev)
	}
	ev = recvEvent(t, ch)
	if ev.Frame == nil || ev.Frame.Event == nil || ev.Frame.Event.DocID != "doc-2" {
		t.Fatalf("expected doc-2 op frame after reconnect, got %+v", ev)
	}
	if n := hits.Load(); n != 2 {
		t.Fatalf("expected exactly 2 connections, got %d", n)
	}

	cancel()
	expectClosed(t, ch)
}

// rejected handshake: 401 on the upgrade returns an UNAUTHORIZED error from
// the call itself, with no retry (one connection attempt only).
func TestStreamAdminRejectedHandshake(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, "denied", http.StatusUnauthorized)
	}))
	t.Cleanup(srv.Close)
	client := NewAdminStreamClient(srv.URL, testStreamKey)

	ch, err := client.StreamAdmin(context.Background(), nil, nil)
	if err == nil {
		t.Fatal("expected the rejected handshake to fail the call")
	}
	if ch != nil {
		t.Fatal("expected no channel on a rejected handshake")
	}
	var rtdbErr *rtdberrors.RtDbError
	if !errors.As(err, &rtdbErr) || rtdbErr.Code != rtdberrors.CodeUnauthorized {
		t.Fatalf("expected UNAUTHORIZED, got %v", err)
	}
	if n := hits.Load(); n != 1 {
		t.Fatalf("expected no retry after a rejected handshake, got %d attempts", n)
	}
}

// mid-stream 4401 close: the op frame before the close still delivers, then
// the UNAUTHORIZED terminal error ends the stream with no reconnect.
func TestStreamAdminCredentialRevokedMidStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, hits := streamStub(t, func(conn *websocket.Conn, hits int32) {
		writeText(conn, opFrame("doc-1"))
		_ = conn.Close(4401, "credential revoked")
	})
	ch, err := client.StreamAdmin(ctx, nil, nil)
	if err != nil {
		t.Fatalf("StreamAdmin: %v", err)
	}

	ev := recvEvent(t, ch)
	if ev.Frame == nil || ev.Frame.Event == nil || ev.Frame.Event.DocID != "doc-1" {
		t.Fatalf("expected doc-1 op frame, got %+v", ev)
	}
	ev = recvEvent(t, ch)
	if ev.Err == nil {
		t.Fatalf("expected terminal error after 4401, got %+v", ev)
	}
	var rtdbErr *rtdberrors.RtDbError
	if !errors.As(ev.Err, &rtdbErr) || rtdbErr.Code != rtdberrors.CodeUnauthorized {
		t.Fatalf("expected UNAUTHORIZED terminal, got %v", ev.Err)
	}
	expectClosed(t, ch)
	if n := hits.Load(); n != 1 {
		t.Fatalf("expected no reconnect after 4401, got %d connections", n)
	}
}
