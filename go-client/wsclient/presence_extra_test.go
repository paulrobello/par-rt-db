// go-client/wsclient/presence_extra_test.go
package wsclient

// Covers the WS reply-correlated surfaces in presence.go (mutate/schedule/
// workflow request-response over the fakePeer) plus the presence room
// lifecycle edges the main test does not hit: leave drops fold state,
// re-join resets the delta baseline, unrouted replies are ignored.

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// frameResponder drains the peer's captured frames and answers each with
// whatever classify returns (nil = no reply, e.g. for the auth handshake).
func frameResponder(fp *fakePeer, peer *websocket.Conn, classify func(raw []byte) wire.ServerMessage) {
	go func() {
		deadline := time.After(5 * time.Second)
		for {
			select {
			case data := <-fp.frames:
				msg := classify(data)
				if msg == nil {
					continue
				}
				b, err := json.Marshal(msg)
				if err != nil {
					return
				}
				_ = peer.Write(context.Background(), websocket.MessageText, b)
			case <-deadline:
				return
			}
		}
	}()
}

// frameType pulls the "type" tag out of a raw client frame.
func frameType(raw []byte) string {
	var head struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(raw, &head)
	return head.Type
}

// corrID pulls a correlation id field from a raw client frame.
func corrID(raw []byte, key string) string {
	var head map[string]any
	_ = json.Unmarshal(raw, &head)
	if v, ok := head[key].(string); ok {
		return v
	}
	return ""
}

// newConnectedClient builds a client wired to fp with the auth handshake
// complete and the accepted peer connection handed back.
func newConnectedClient(t *testing.T, fp *fakePeer) (*Client, *websocket.Conn) {
	t.Helper()
	c := NewClient(wsURL(fp), "d1", func(ctx context.Context) (string, error) { return "tk", nil })
	if err := c.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	_ = fp.waitAuth(t)
	return c, <-fp.conns
}

func txnDelete(table, id string) wire.Transaction {
	return wire.Transaction{Steps: []wire.Step{wire.StepDelete{Table: table, ID: id}}}
}

func TestMutateOverWSOk(t *testing.T) {
	fp := newFakePeer(t)
	c, peer := newConnectedClient(t, fp)
	frameResponder(fp, peer, func(raw []byte) wire.ServerMessage {
		if frameType(raw) != "mutate" {
			return nil
		}
		return wire.ServerMutateOk{
			MutID: corrID(raw, "mutId"),
			Results: []wire.JSONValue{
				wire.Object{"id": wire.String("doc1")},
				wire.Object{"patched": wire.Number("3")},
			},
		}
	})
	results, err := c.Mutate(context.Background(), txnDelete("items", "a"), "idem-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].ID == nil || *results[0].ID != "doc1" {
		t.Fatalf("results %+v", results)
	}
	if results[1].Patched == nil || *results[1].Patched != 3 {
		t.Fatalf("patched result %+v", results[1])
	}
}

func TestMutateOverWSErrAndTimeout(t *testing.T) {
	fp := newFakePeer(t)
	c, peer := newConnectedClient(t, fp)
	var silent bool
	frameResponder(fp, peer, func(raw []byte) wire.ServerMessage {
		if frameType(raw) != "mutate" {
			return nil
		}
		if silent {
			return nil
		}
		return wire.ServerMutateErr{
			MutID: corrID(raw, "mutId"),
			Error: wire.ErrorEnvelope{Code: "BAD_REQUEST", Message: "nope"},
		}
	})
	_, err := c.Mutate(context.Background(), txnDelete("items", "a"), "")
	var rtErr *rtdberrors.RtDbError
	if !errors.As(err, &rtErr) || rtErr.Code != rtdberrors.CodeBadRequest || rtErr.Message != "nope" {
		t.Fatalf("expected RtDbError BAD_REQUEST, got %v", err)
	}

	// A reply of a foreign type never routes to the pending mutate, so the
	// await times out and Mutate surfaces ctx.Err().
	silent = true
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	_, err = c.Mutate(ctx, txnDelete("items", "a"), "")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func TestMutateNotConnectedDropsReply(t *testing.T) {
	fp := newFakePeer(t)
	c, _ := newConnectedClient(t, fp)
	// Closing the transport makes sendFrame fail; the pending reply entry
	// must be dropped, not leaked.
	_ = c.Close()
	if _, err := c.Mutate(context.Background(), txnDelete("items", "a"), ""); err == nil {
		t.Fatal("expected send error on a closed client")
	}
	c.subsMu.Lock()
	leaked := len(c.replies)
	c.subsMu.Unlock()
	if leaked != 0 {
		t.Fatalf("reply entry leaked: %d", leaked)
	}
}

func TestScheduleOverWS(t *testing.T) {
	fp := newFakePeer(t)
	c, peer := newConnectedClient(t, fp)
	var mu sync.Mutex
	var rawSchedules []string
	frameResponder(fp, peer, func(raw []byte) wire.ServerMessage {
		if frameType(raw) != "schedule" {
			return nil
		}
		mu.Lock()
		rawSchedules = append(rawSchedules, string(raw))
		mu.Unlock()
		return wire.ServerScheduleOk{ScheduleID: corrID(raw, "scheduleId"), ID: "job1"}
	})
	txn := wire.Transaction{Steps: []wire.Step{
		wire.StepInsert{Table: "items", Doc: wire.Object{"n": wire.Number("1")}},
	}}
	id, err := c.Schedule(context.Background(), wire.WhenAfterMs{Ms: 500}, txn, false)
	if err != nil || id != "job1" {
		t.Fatalf("id %q err %v", id, err)
	}
	id, err = c.Schedule(context.Background(), wire.WhenAfterMs{Ms: 500}, txn, true)
	if err != nil || id != "job1" {
		t.Fatalf("external schedule id %q err %v", id, err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(rawSchedules) != 2 || !strings.Contains(rawSchedules[1], `"external":true`) {
		t.Fatalf("external flag not sent: %v", rawSchedules)
	}
}

func TestScheduleManageOverWS(t *testing.T) {
	// Each op answers scheduleAck (ok, then with an error envelope) or a
	// scheduleErr; drive all three ops through all three reply shapes.
	cases := []struct {
		name string
		op   func(*Client, context.Context, string) (bool, error)
	}{
		{"cancel", func(c *Client, ctx context.Context, id string) (bool, error) { return c.CancelSchedule(ctx, id) }},
		{"pause", func(c *Client, ctx context.Context, id string) (bool, error) { return c.PauseSchedule(ctx, id) }},
		{"resume", func(c *Client, ctx context.Context, id string) (bool, error) { return c.ResumeSchedule(ctx, id) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fp := newFakePeer(t)
			c, peer := newConnectedClient(t, fp)
			mode := 0
			frameResponder(fp, peer, func(raw []byte) wire.ServerMessage {
				ft := frameType(raw)
				if ft != "cancelSchedule" && ft != "pauseSchedule" && ft != "resumeSchedule" {
					return nil
				}
				sid := corrID(raw, "scheduleId")
				switch mode {
				case 0:
					mode = 1
					return wire.ServerScheduleAck{ScheduleID: sid, OK: true}
				case 1:
					mode = 2
					return wire.ServerScheduleAck{ScheduleID: sid, OK: false, Error: &wire.ErrorEnvelope{Code: "NOT_FOUND", Message: "gone"}}
				default:
					return wire.ServerScheduleErr{ScheduleID: sid, Error: wire.ErrorEnvelope{Code: "CONFLICT", Message: "bad state"}}
				}
			})
			ok, err := tc.op(c, context.Background(), "job1")
			if !ok || err != nil {
				t.Fatalf("ack ok: %v %v", ok, err)
			}
			ok, err = tc.op(c, context.Background(), "job1")
			var rtErr *rtdberrors.RtDbError
			if ok || !errors.As(err, &rtErr) || rtErr.Code != rtdberrors.CodeNotFound {
				t.Fatalf("ack error: %v %v", ok, err)
			}
			ok, err = tc.op(c, context.Background(), "job1")
			if ok || !errors.As(err, &rtErr) || rtErr.Code != rtdberrors.CodeConflict {
				t.Fatalf("scheduleErr: %v %v", ok, err)
			}
		})
	}
}

func TestScheduleNotConnectedDropsReply(t *testing.T) {
	fp := newFakePeer(t)
	c, _ := newConnectedClient(t, fp)
	_ = c.Close()
	if _, err := c.Schedule(context.Background(), wire.WhenAfterMs{Ms: 1}, wire.Transaction{}, false); err == nil {
		t.Fatal("expected send error on a closed client")
	}
	c.subsMu.Lock()
	leaked := len(c.replies)
	c.subsMu.Unlock()
	if leaked != 0 {
		t.Fatalf("reply entry leaked: %d", leaked)
	}
}

func TestListSchedulesOverWS(t *testing.T) {
	fp := newFakePeer(t)
	c, peer := newConnectedClient(t, fp)
	frameResponder(fp, peer, func(raw []byte) wire.ServerMessage {
		if frameType(raw) != "listSchedules" {
			return nil
		}
		every := int64(1000)
		return wire.ServerListSchedulesOk{
			ScheduleID: corrID(raw, "scheduleId"),
			Schedules: []wire.ScheduleInfo{{
				ID: "job1", Kind: wire.ScheduleKindInterval, DueAt: 42,
				Status: wire.ScheduleStatusPending, EveryMs: &every,
			}},
		}
	})
	schedules, err := c.ListSchedules(context.Background())
	if err != nil || len(schedules) != 1 || schedules[0].ID != "job1" {
		t.Fatalf("schedules %v err %v", schedules, err)
	}
}

func TestWorkflowSurfacesOverWS(t *testing.T) {
	fp := newFakePeer(t)
	c, peer := newConnectedClient(t, fp)
	wfInfo := func() wire.WorkflowInfo {
		return wire.WorkflowInfo{ID: "run1", Name: "etl", Status: wire.WorkflowStatusPending, CreatedAt: 1, UpdatedAt: 2}
	}
	mode := 0
	frameResponder(fp, peer, func(raw []byte) wire.ServerMessage {
		switch frameType(raw) {
		case "startWorkflow":
			wid := corrID(raw, "workflowId")
			if mode == 1 {
				return wire.ServerStartWorkflowErr{WorkflowID: wid, Error: wire.ErrorEnvelope{Code: "QUOTA_EXCEEDED", Message: "too many"}}
			}
			mode = 1
			return wire.ServerStartWorkflowOk{WorkflowID: wid, Info: wfInfo()}
		case "cancelWorkflow":
			return wire.ServerWorkflowAck{WorkflowID: corrID(raw, "workflowId"), OK: true}
		case "signalWorkflow":
			return wire.ServerWorkflowAck{WorkflowID: corrID(raw, "workflowId"), OK: false, Error: &wire.ErrorEnvelope{Code: "NOT_FOUND", Message: "no run"}}
		case "listWorkflows":
			return wire.ServerListWorkflowsOk{WorkflowID: corrID(raw, "workflowId"), Workflows: []wire.WorkflowInfo{wfInfo()}}
		default:
			return nil
		}
	})
	spec := wire.WorkflowSpec{Name: "etl", Steps: []wire.WorkflowStepSpec{{
		Txn: &wire.Transaction{Steps: []wire.Step{wire.StepDelete{Table: "items", ID: "a"}}},
	}}}
	id, err := c.StartWorkflow(context.Background(), spec)
	if err != nil || id != "run1" {
		t.Fatalf("start id %q err %v", id, err)
	}
	if _, err = c.StartWorkflow(context.Background(), spec); err == nil {
		t.Fatal("expected startWorkflowErr")
	}
	ok, err := c.CancelWorkflow(context.Background(), "run1")
	if !ok || err != nil {
		t.Fatalf("cancel %v %v", ok, err)
	}
	ok, err = c.SignalWorkflow(context.Background(), "run1", "go", wire.Object{"k": wire.String("v")})
	var rtErr *rtdberrors.RtDbError
	if ok || !errors.As(err, &rtErr) || rtErr.Code != rtdberrors.CodeNotFound {
		t.Fatalf("signal %v %v", ok, err)
	}
	runs, err := c.ListWorkflows(context.Background(), nil)
	if err != nil || len(runs) != 1 || runs[0].ID != "run1" {
		t.Fatalf("list %v err %v", runs, err)
	}
	status := wire.WorkflowStatusPending
	if runs, err = c.ListWorkflows(context.Background(), &status); err != nil || len(runs) != 1 {
		t.Fatalf("list with status %v err %v", runs, err)
	}
}

func TestListWorkflowsUnexpectedReply(t *testing.T) {
	fp := newFakePeer(t)
	c, peer := newConnectedClient(t, fp)
	// Answer listWorkflows with a scheduleOk: the correlation id matches so
	// routeReply delivers it, but the type is not what the caller awaited.
	frameResponder(fp, peer, func(raw []byte) wire.ServerMessage {
		if frameType(raw) != "listWorkflows" {
			return nil
		}
		return wire.ServerScheduleOk{ScheduleID: corrID(raw, "workflowId"), ID: "bogus"}
	})
	if _, err := c.ListWorkflows(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "unexpected reply to listWorkflows") {
		t.Fatalf("expected unexpected-reply error, got %v", err)
	}
}

func TestWorkflowNotConnectedDropsReply(t *testing.T) {
	fp := newFakePeer(t)
	c, _ := newConnectedClient(t, fp)
	_ = c.Close()
	if _, err := c.StartWorkflow(context.Background(), wire.WorkflowSpec{Name: "x"}); err == nil {
		t.Fatal("expected send error on a closed client")
	}
	if _, err := c.CancelWorkflow(context.Background(), "r"); err == nil {
		t.Fatal("expected send error on a closed client")
	}
	if _, err := c.SignalWorkflow(context.Background(), "r", "go", wire.Object{}); err == nil {
		t.Fatal("expected send error on a closed client")
	}
	if _, err := c.ListWorkflows(context.Background(), nil); err == nil {
		t.Fatal("expected send error on a closed client")
	}
	if _, err := c.ListSchedules(context.Background()); err == nil {
		t.Fatal("expected send error on a closed client")
	}
	c.subsMu.Lock()
	leaked := len(c.replies)
	c.subsMu.Unlock()
	if leaked != 0 {
		t.Fatalf("reply entry leaked: %d", leaked)
	}
}

func TestLeavePresenceDropsFoldState(t *testing.T) {
	fp := newFakePeer(t)
	c, peer := newConnectedClient(t, fp)
	var mu sync.Mutex
	delivered := 0
	c.OnPresence(func(room string, members []wire.PresenceMember) {
		mu.Lock()
		delivered++
		mu.Unlock()
	})
	if err := c.JoinPresence(context.Background(), "r", wire.Object{"k": wire.String("v")}); err != nil {
		t.Fatal(err)
	}
	snap, _ := json.Marshal(wire.ServerPresenceSnapshot{Room: "r", Members: []wire.PresenceMember{presenceMember("a", "1")}})
	if err := peer.Write(context.Background(), websocket.MessageText, snap); err != nil {
		t.Fatal(err)
	}
	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		d := delivered
		mu.Unlock()
		if d == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("snapshot not delivered after join")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := c.LeavePresence(context.Background(), "r"); err != nil {
		t.Fatal(err)
	}
	// The leave frame must reach the peer...
	sawLeave := false
	deadline = time.After(2 * time.Second)
	for !sawLeave {
		select {
		case data := <-fp.frames:
			if frameType(data) == "leavePresence" && strings.Contains(string(data), `"room":"r"`) {
				sawLeave = true
			}
		case <-deadline:
			t.Fatal("leavePresence frame never reached the peer")
		}
	}
	// ...and a delta for the departed room must be dropped: the fold entry
	// was deleted, so foldPresenceDelta finds no baseline.
	delta, _ := json.Marshal(wire.ServerPresenceDelta{Room: "r", Seq: 2, Joined: []wire.PresenceMember{presenceMember("b", "2")}})
	if err := peer.Write(context.Background(), websocket.MessageText, delta); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if delivered != 1 {
		t.Fatalf("post-leave delta must not deliver, got %d callbacks", delivered)
	}
}

func TestRejoinResetsDeltaBaseline(t *testing.T) {
	fp := newFakePeer(t)
	c, peer := newConnectedClient(t, fp)
	var mu sync.Mutex
	delivered := 0
	c.OnPresence(func(room string, members []wire.PresenceMember) {
		mu.Lock()
		delivered++
		mu.Unlock()
	})
	if err := c.JoinPresence(context.Background(), "r", wire.Object{"k": wire.String("v")}); err != nil {
		t.Fatal(err)
	}
	sendDelta := func(seq uint64) {
		t.Helper()
		b, _ := json.Marshal(wire.ServerPresenceDelta{Room: "r", Seq: seq, Joined: []wire.PresenceMember{presenceMember("m", "s")}})
		if err := peer.Write(context.Background(), websocket.MessageText, b); err != nil {
			t.Fatal(err)
		}
	}
	waitDelivered := func(want int) {
		t.Helper()
		deadline := time.After(2 * time.Second)
		for {
			mu.Lock()
			d := delivered
			mu.Unlock()
			if d == want {
				return
			}
			select {
			case <-deadline:
				t.Fatalf("delivered %d, want %d", d, want)
			case <-time.After(5 * time.Millisecond):
			}
		}
	}
	sendDelta(5) // first delta after the join: accepted at any seq
	waitDelivered(1)
	// Re-join resets the baseline (recordJoin), so a LOWER seq is accepted
	// again instead of being treated as a stale duplicate.
	if err := c.JoinPresence(context.Background(), "r", wire.Object{"k": wire.String("v2")}); err != nil {
		t.Fatal(err)
	}
	sendDelta(3)
	waitDelivered(2)
}

func TestRouteReplyIgnoresUnroutedMessages(t *testing.T) {
	fp := newFakePeer(t)
	c, _ := newConnectedClient(t, fp)
	// Not a reply-correlated type: must return without touching the map.
	c.routeReply(wire.ServerQueryUpdate{QueryID: "q1"})
	c.subsMu.Lock()
	leaked := len(c.replies)
	c.subsMu.Unlock()
	if leaked != 0 {
		t.Fatalf("replies map touched: %d", leaked)
	}
	// An entry exists: routeReply must find it, delete it, and deliver.
	p := c.expectReply("m1")
	c.routeReply(wire.ServerMutateOk{MutID: "m1"})
	select {
	case msg := <-p.ch:
		if _, ok := msg.(wire.ServerMutateOk); !ok {
			t.Fatalf("routed message %T", msg)
		}
	default:
		t.Fatal("reply not delivered to the pending waiter")
	}
	c.subsMu.Lock()
	leaked = len(c.replies)
	c.subsMu.Unlock()
	if leaked != 0 {
		t.Fatalf("reply entry not deleted: %d", leaked)
	}
}

func TestOptStr(t *testing.T) {
	if optStr("") != nil {
		t.Fatal("empty string must map to nil")
	}
	if got := optStr("x"); got == nil || *got != "x" {
		t.Fatal("non-empty string must map to a pointer carrying it")
	}
}
