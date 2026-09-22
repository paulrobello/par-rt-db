// go-client/wsclient/client_test.go
package wsclient

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// fakePeer is a scripted in-process /sync server. serveAuth reads the
// first frame, hands it to the test, and replies authOk — Connect blocks
// on the reply, so the read/reply must happen in a goroutine.
type fakePeer struct {
	srv    *httptest.Server
	conns  chan *websocket.Conn
	auths  chan []byte // each connection's auth frame, in order
	frames chan []byte // every received frame, in order (buffered)
}

func newFakePeer(t *testing.T) *fakePeer {
	t.Helper()
	fp := &fakePeer{
		conns:  make(chan *websocket.Conn, 8),
		auths:  make(chan []byte, 8),
		frames: make(chan []byte, 32),
	}
	fp.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		fp.conns <- conn
		// Drain frames for the life of the connection so close handshakes
		// echo promptly.
		go func() {
			for {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, data, err := conn.Read(ctx)
				cancel()
				if err != nil {
					return
				}
				select {
				case fp.frames <- data:
				default:
				}
				if strings.Contains(string(data), `"type":"auth"`) {
					fp.auths <- data
					ok, _ := json.Marshal(wire.ServerAuthOk{User: wire.AuthedUser{Kind: wire.UserKindMachine}})
					_ = conn.Write(context.Background(), websocket.MessageText, ok)
				}
			}
		}()
	}))
	t.Cleanup(fp.srv.Close)
	return fp
}

func wsURL(fp *fakePeer) string {
	return "ws" + strings.TrimPrefix(fp.srv.URL, "http")
}

// waitAuth takes the next auth frame (blocking with a deadline).
func (fp *fakePeer) waitAuth(t *testing.T) []byte {
	t.Helper()
	select {
	case f := <-fp.auths:
		return f
	case <-time.After(2 * time.Second):
		t.Fatal("peer: no auth frame arrived")
		return nil
	}
}

func TestConnectHandshake(t *testing.T) {
	fp := newFakePeer(t)
	c := NewClient(wsURL(fp), "d1", func(ctx context.Context) (string, error) { return "tk", nil })
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	frame := fp.waitAuth(t)
	if !strings.Contains(string(frame), `"type":"auth"`) {
		t.Fatalf("first frame not auth: %s", frame)
	}
	if !strings.Contains(string(frame), `"db":"d1"`) || !strings.Contains(string(frame), `"protocolVersion":3`) {
		t.Fatalf("auth frame missing db/protocolVersion: %s", frame)
	}
}

func TestSubscribeDedupesByCanonicalQuery(t *testing.T) {
	fp := newFakePeer(t)
	c := NewClient(wsURL(fp), "d1", func(ctx context.Context) (string, error) { return "tk", nil })
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = fp.waitAuth(t)
	q := wire.Query{Table: "items"}
	sub1, err := c.Subscribe(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer sub1.Close()
	sub2, err := c.Subscribe(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	if sub1 != sub2 {
		t.Fatal("identical queries must share one subscription")
	}
	// only ONE subscribe frame at the peer (watch a full second, then count)
	deadline := time.After(1 * time.Second)
	count := 0
	watching := true
	for watching {
		select {
		case data := <-fp.frames:
			if strings.Contains(string(data), `"type":"subscribe"`) {
				count++
			}
		case <-deadline:
			watching = false
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly one subscribe frame, saw %d", count)
	}
	sub2.Close()
	// one handle still held: no unsubscribe frame yet
	select {
	case data := <-fp.frames:
		if strings.Contains(string(data), `"type":"unsubscribe"`) {
			t.Fatal("unsubscribe must wait for the last handle")
		}
	case <-time.After(100 * time.Millisecond):
	}
}

func TestReconnectReplaysAuth(t *testing.T) {
	fp := newFakePeer(t)
	c := NewClient(wsURL(fp), "d1", func(ctx context.Context) (string, error) { return "tk", nil },
		WithBackoff(10*time.Millisecond, 50*time.Millisecond))
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	first := fp.waitAuth(t)
	if !strings.Contains(string(first), `"type":"auth"`) {
		t.Fatalf("first auth missing: %s", first)
	}
	// The peer's per-connection goroutine is now blocked reading; force the
	// socket closed by closing the accepted conn via the conns channel.
	conn1 := <-fp.conns
	_ = conn1.Close(websocket.StatusInternalError, "boom")
	second := fp.waitAuth(t)
	if !strings.Contains(string(second), `"type":"auth"`) {
		t.Fatalf("no re-auth on the new connection: %s", second)
	}
}

func TestSubscribeSendsFrameAndDeliversUpdate(t *testing.T) {
	fp := newFakePeer(t)
	c := NewClient(wsURL(fp), "d1", func(ctx context.Context) (string, error) { return "tk", nil })
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = fp.waitAuth(t)
	conn := <-fp.conns

	sub, err := c.Subscribe(context.Background(), wire.Query{Table: "items"})
	if err != nil {
		t.Fatal(err)
	}
	// Peer: take the subscribe frame from the capture channel, echo a
	// queryUpdate with its queryId.
	var subFrame struct {
		QueryID string `json:"queryId"`
	}
	deadline := time.After(2 * time.Second)
	for subFrame.QueryID == "" {
		select {
		case data := <-fp.frames:
			json.Unmarshal(data, &subFrame)
		case <-deadline:
			t.Fatal("no subscribe frame at the peer")
		}
	}
	upd, _ := json.Marshal(wire.ServerQueryUpdate{
		QueryID: subFrame.QueryID,
		Result:  wire.Object(map[string]wire.JSONValue{"ping": wire.String("pong")}),
	})
	if err := conn.Write(context.Background(), websocket.MessageText, upd); err != nil {
		t.Fatalf("peer write: %v", err)
	}

	defer sub.Close()
	for {
		select {
		case snap := <-sub.Updates():
			if snap.Kind == SnapshotValue {
				obj, ok := snap.Value.(wire.Object)
				if !ok || obj["ping"] != wire.JSONValue(wire.String("pong")) {
					t.Fatalf("update value %v", snap.Value)
				}
				return
			}
			if snap.Kind == SnapshotError {
				t.Fatalf("error snapshot: %v", snap.Err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("no value snapshot")
		}
	}
}
