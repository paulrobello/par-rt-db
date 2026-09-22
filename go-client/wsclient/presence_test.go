// go-client/wsclient/presence_test.go
package wsclient

// Drives the presenceDelta fold + seq-gap resync over the real read pump
// against the fakePeer scripted server: snapshot replaces the baseline,
// contiguous deltas fold, stale duplicates and seq gaps are dropped (a gap
// re-sends the room's join frame so the server's fresh snapshot resyncs).

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func presenceMember(id, state string) wire.PresenceMember {
	return wire.PresenceMember{
		ConnectionID: id,
		User:         wire.AuthedUser{Kind: wire.UserKindMachine},
		State:        wire.Object{"s": wire.String(state)},
	}
}

func memberStates(t *testing.T, members []wire.PresenceMember) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, m := range members {
		b, err := json.Marshal(m.State)
		if err != nil {
			t.Fatal(err)
		}
		out[m.ConnectionID] = string(b)
	}
	return out
}

// waitForState polls until pred sees the room's current folded members.
func waitForState(t *testing.T, mu *sync.Mutex, got map[string][]wire.PresenceMember, room string, pred func(map[string]string) bool) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		states := memberStates(t, got[room])
		mu.Unlock()
		if pred(states) {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for room %q state, last: %v", room, states)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestPresenceDeltaFoldAndGapResync(t *testing.T) {
	fp := newFakePeer(t)
	c := NewClient(wsURL(fp), "d1", func(ctx context.Context) (string, error) { return "tk", nil })
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = fp.waitAuth(t)

	var mu sync.Mutex
	got := map[string][]wire.PresenceMember{}
	c.OnPresence(func(room string, members []wire.PresenceMember) {
		mu.Lock()
		got[room] = members
		mu.Unlock()
	})

	// Join (records the state for gap resync and resets the baseline); let
	// the join frame land in the peer's drain before counting frames.
	if err := c.JoinPresence(context.Background(), "r", wire.Object{"k": wire.String("v")}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	peer := <-fp.conns
	send := func(msg wire.ServerMessage) {
		t.Helper()
		b, err := json.Marshal(msg)
		if err != nil {
			t.Fatal(err)
		}
		if err := peer.Write(context.Background(), websocket.MessageText, b); err != nil {
			t.Fatal(err)
		}
	}

	// 1. Snapshot replaces the member list and resets the seq baseline.
	send(wire.ServerPresenceSnapshot{Room: "r", Members: []wire.PresenceMember{presenceMember("a", "1")}})
	waitForState(t, &mu, got, "r", func(s map[string]string) bool { return len(s) == 1 && s["a"] == `{"s":"1"}` })

	// 2. First delta after a snapshot: any seq accepted (snapshot carries
	// none), joined member folded in.
	send(wire.ServerPresenceDelta{Room: "r", Seq: 5, Joined: []wire.PresenceMember{presenceMember("b", "2")}})
	waitForState(t, &mu, got, "r", func(s map[string]string) bool { return len(s) == 2 && s["b"] == `{"s":"2"}` })

	// 3. Contiguous delta: state change replaces in place.
	send(wire.ServerPresenceDelta{Room: "r", Seq: 6, StateChanged: []wire.PresenceMember{presenceMember("a", "9")}})
	waitForState(t, &mu, got, "r", func(s map[string]string) bool { return s["a"] == `{"s":"9"}` && len(s) == 2 })

	// 4. Stale duplicate (seq 4 <= 6) must be ignored, and 5. a seq gap
	// (9 > 6+1) must NOT apply — instead the room's join frame is re-sent
	// so the server's fresh snapshot resyncs the baseline.
	send(wire.ServerPresenceDelta{Room: "r", Seq: 4, Joined: []wire.PresenceMember{presenceMember("z", "z")}})
	send(wire.ServerPresenceDelta{Room: "r", Seq: 9, Joined: []wire.PresenceMember{presenceMember("q", "q")}})

	// The re-join is the SECOND presence frame at the peer (the first was
	// the test's own JoinPresence above).
	presenceFrames := 0
	deadline := time.After(2 * time.Second)
	for presenceFrames < 2 {
		select {
		case data := <-fp.frames:
			if strings.Contains(string(data), `"type":"presence"`) && strings.Contains(string(data), `"room":"r"`) {
				presenceFrames++
			}
		case <-deadline:
			t.Fatalf("gap did not trigger a re-join; saw %d presence frames", presenceFrames)
		}
	}
	// Give a wrongly-applied gap delta time to (incorrectly) surface.
	time.Sleep(100 * time.Millisecond)
	waitForState(t, &mu, got, "r", func(s map[string]string) bool {
		return len(s) == 2 && s["a"] == `{"s":"9"}` && s["b"] == `{"s":"2"}`
	})

	// 6. Recovery snapshot: replaces and resets the baseline again.
	send(wire.ServerPresenceSnapshot{Room: "r", Members: []wire.PresenceMember{presenceMember("a", "1")}})
	waitForState(t, &mu, got, "r", func(s map[string]string) bool { return len(s) == 1 && s["a"] == `{"s":"1"}` })

	// 7. A delta right after the snapshot is accepted at any seq: left
	// removes by connection id.
	send(wire.ServerPresenceDelta{Room: "r", Seq: 3, Left: []string{"a"}})
	waitForState(t, &mu, got, "r", func(s map[string]string) bool { return len(s) == 0 })
}

// TestPresenceDeltaUnknownRoomIgnored: a delta for a room the client never
// saw a snapshot for must neither deliver nor panic.
func TestPresenceDeltaUnknownRoomIgnored(t *testing.T) {
	fp := newFakePeer(t)
	c := NewClient(wsURL(fp), "d1", func(ctx context.Context) (string, error) { return "tk", nil })
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	_ = fp.waitAuth(t)

	var mu sync.Mutex
	delivered := 0
	c.OnPresence(func(room string, members []wire.PresenceMember) {
		mu.Lock()
		delivered++
		mu.Unlock()
	})
	peer := <-fp.conns
	b, err := json.Marshal(wire.ServerPresenceDelta{Room: "ghost", Seq: 7, Joined: []wire.PresenceMember{presenceMember("x", "x")}})
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.Write(context.Background(), websocket.MessageText, b); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if delivered != 0 {
		t.Fatalf("unknown-room delta must not deliver, got %d callbacks", delivered)
	}
}
