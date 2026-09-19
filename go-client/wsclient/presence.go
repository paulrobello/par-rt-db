// go-client/wsclient/presence.go
package wsclient

// Presence + WS-frame mutate/schedule/workflow surfaces (mirrors rust
// ws.rs). Replies correlate by id; presence snapshots multiplex to a
// registered room callback.

import (
	"context"
	"encoding/json"
	"fmt"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// envelopeToRtDb converts a wire error envelope into the shared error type.
func envelopeToRtDb(e wire.ErrorEnvelope) *rtdberrors.RtDbError {
	return &rtdberrors.RtDbError{
		Code:       rtdberrors.ErrorCode(e.Code),
		Message:    e.Message,
		RetryAfter: e.RetryAfter,
	}
}

// Mutate runs a transaction over WS; correlates mutateOk/mutateErr by mutId.
func (c *Client) Mutate(ctx context.Context, txn wire.Transaction, idempotencyKey string) ([]wire.StepResult, error) {
	mutID := newID()
	frameBody := wire.ClientMutate{MutID: mutID, IdempotencyKey: optStr(idempotencyKey), Txn: txn}
	frame, err := json.Marshal(frameBody)
	if err != nil {
		return nil, err
	}
	wait := c.expectReply(mutID)
	if err := c.sendFrame(ctx, frame); err != nil {
		c.dropReply(mutID)
		return nil, err
	}
	msg := wait.await(ctx)
	switch m := msg.(type) {
	case wire.ServerMutateOk:
		results := make([]wire.StepResult, len(m.Results))
		for i, raw := range m.Results {
			b, err := json.Marshal(raw)
			if err != nil {
				return nil, fmt.Errorf("wsclient: re-encode result %d: %w", i, err)
			}
			if err := json.Unmarshal(b, &results[i]); err != nil {
				return nil, fmt.Errorf("wsclient: decode result %d: %w", i, err)
			}
		}
		return results, nil
	case wire.ServerMutateErr:
		return nil, envelopeToRtDb(m.Error)
	default:
		return nil, ctx.Err()
	}
}

// Schedule creates a job over WS; returns the minted schedule id.
func (c *Client) Schedule(ctx context.Context, when wire.ScheduleWhen, txn wire.Transaction, external bool) (string, error) {
	corrID := newID()
	body := wire.ClientSchedule{ScheduleID: corrID, When: when, Txn: txn}
	if external {
		t := true
		body.External = &t
	}
	frame, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	wait := c.expectReply(corrID)
	if err := c.sendFrame(ctx, frame); err != nil {
		c.dropReply(corrID)
		return "", err
	}
	msg := wait.await(ctx)
	switch m := msg.(type) {
	case wire.ServerScheduleOk:
		return m.ID, nil
	case wire.ServerScheduleErr:
		return "", envelopeToRtDb(m.Error)
	default:
		return "", ctx.Err()
	}
}

// ListSchedules lists jobs over WS (I4: the request side was missing).
func (c *Client) ListSchedules(ctx context.Context) ([]wire.ScheduleInfo, error) {
	corrID := newID()
	frame, err := json.Marshal(wire.ClientListSchedules{ScheduleID: corrID})
	if err != nil {
		return nil, err
	}
	wait := c.expectReply(corrID)
	if err := c.sendFrame(ctx, frame); err != nil {
		c.dropReply(corrID)
		return nil, err
	}
	msg := wait.await(ctx)
	switch m := msg.(type) {
	case wire.ServerListSchedulesOk:
		return m.Schedules, nil
	default:
		return nil, ctx.Err()
	}
}

// CancelSchedule cancels over WS; false = no-op.
func (c *Client) CancelSchedule(ctx context.Context, id string) (bool, error) {
	return c.scheduleManage(ctx, "cancelSchedule", id)
}

// PauseSchedule pauses over WS.
func (c *Client) PauseSchedule(ctx context.Context, id string) (bool, error) {
	return c.scheduleManage(ctx, "pauseSchedule", id)
}

// ResumeSchedule resumes over WS.
func (c *Client) ResumeSchedule(ctx context.Context, id string) (bool, error) {
	return c.scheduleManage(ctx, "resumeSchedule", id)
}

func (c *Client) scheduleManage(ctx context.Context, op, id string) (bool, error) {
	corrID := newID()
	var frame []byte
	var err error
	switch op {
	case "cancelSchedule":
		frame, err = json.Marshal(wire.ClientCancelSchedule{ScheduleID: corrID, ID: id})
	case "pauseSchedule":
		frame, err = json.Marshal(wire.ClientPauseSchedule{ScheduleID: corrID, ID: id})
	default:
		frame, err = json.Marshal(wire.ClientResumeSchedule{ScheduleID: corrID, ID: id})
	}
	if err != nil {
		return false, err
	}
	wait := c.expectReply(corrID)
	if err := c.sendFrame(ctx, frame); err != nil {
		c.dropReply(corrID)
		return false, err
	}
	msg := wait.await(ctx)
	switch m := msg.(type) {
	case wire.ServerScheduleAck:
		if m.Error != nil {
			return false, envelopeToRtDb(*m.Error)
		}
		return m.OK, nil
	case wire.ServerScheduleErr:
		return false, envelopeToRtDb(m.Error)
	default:
		return false, ctx.Err()
	}
}

// StartWorkflow starts a run over WS; returns the run id from the ok's
// WorkflowInfo.
func (c *Client) StartWorkflow(ctx context.Context, spec wire.WorkflowSpec) (string, error) {
	corrID := newID()
	frame, err := json.Marshal(wire.ClientStartWorkflow{WorkflowID: corrID, Spec: spec})
	if err != nil {
		return "", err
	}
	wait := c.expectReply(corrID)
	if err := c.sendFrame(ctx, frame); err != nil {
		c.dropReply(corrID)
		return "", err
	}
	msg := wait.await(ctx)
	switch m := msg.(type) {
	case wire.ServerStartWorkflowOk:
		return m.Info.ID, nil
	case wire.ServerStartWorkflowErr:
		return "", envelopeToRtDb(m.Error)
	default:
		return "", ctx.Err()
	}
}

// CancelWorkflow cancels a run over WS.
func (c *Client) CancelWorkflow(ctx context.Context, id string) (bool, error) {
	corrID := newID()
	frame, err := json.Marshal(wire.ClientCancelWorkflow{WorkflowID: corrID, ID: id})
	if err != nil {
		return false, err
	}
	wait := c.expectReply(corrID)
	if err := c.sendFrame(ctx, frame); err != nil {
		c.dropReply(corrID)
		return false, err
	}
	msg := wait.await(ctx)
	switch m := msg.(type) {
	case wire.ServerWorkflowAck:
		if m.Error != nil {
			return false, envelopeToRtDb(*m.Error)
		}
		return m.OK, nil
	default:
		return false, ctx.Err()
	}
}

// SignalWorkflow delivers a signal over WS (reply reuses WorkflowAck).
func (c *Client) SignalWorkflow(ctx context.Context, id, name string, payload wire.JSONValue) (bool, error) {
	corrID := newID()
	frame, err := json.Marshal(wire.ClientSignalWorkflow{WorkflowID: corrID, ID: id, Name: name, Payload: payload})
	if err != nil {
		return false, err
	}
	wait := c.expectReply(corrID)
	if err := c.sendFrame(ctx, frame); err != nil {
		c.dropReply(corrID)
		return false, err
	}
	msg := wait.await(ctx)
	switch m := msg.(type) {
	case wire.ServerWorkflowAck:
		if m.Error != nil {
			return false, envelopeToRtDb(*m.Error)
		}
		return m.OK, nil
	default:
		return false, ctx.Err()
	}
}

// ListWorkflows lists runs over WS.
func (c *Client) ListWorkflows(ctx context.Context, status *wire.WorkflowStatus) ([]wire.WorkflowInfo, error) {
	corrID := newID()
	frame, err := json.Marshal(wire.ClientListWorkflows{WorkflowID: corrID, Status: status})
	if err != nil {
		return nil, err
	}
	wait := c.expectReply(corrID)
	if err := c.sendFrame(ctx, frame); err != nil {
		c.dropReply(corrID)
		return nil, err
	}
	msg := wait.await(ctx)
	switch m := msg.(type) {
	case wire.ServerListWorkflowsOk:
		return m.Workflows, nil
	default:
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("wsclient: unexpected reply to listWorkflows")
	}
}

// --- reply correlation ---

type pendingReply struct {
	ch chan wire.ServerMessage
}

func (c *Client) expectReply(corrID string) *pendingReply {
	p := &pendingReply{ch: make(chan wire.ServerMessage, 1)}
	c.subsMu.Lock()
	if c.replies == nil {
		c.replies = map[string]*pendingReply{}
	}
	c.replies[corrID] = p
	c.subsMu.Unlock()
	return p
}

func (p *pendingReply) await(ctx context.Context) wire.ServerMessage {
	select {
	case m := <-p.ch:
		return m
	case <-ctx.Done():
		return nil
	}
}

// cancel removes a pending reply entry; every caller invokes it on the
// failure/timeout paths so stale entries cannot leak (I5).
func (c *Client) dropReply(corrID string) {
	c.subsMu.Lock()
	delete(c.replies, corrID)
	c.subsMu.Unlock()
}

func (c *Client) routeReply(msg wire.ServerMessage) {
	var corrID string
	switch m := msg.(type) {
	case wire.ServerMutateOk:
		corrID = m.MutID
	case wire.ServerMutateErr:
		corrID = m.MutID
	case wire.ServerScheduleOk:
		corrID = m.ScheduleID
	case wire.ServerScheduleErr:
		corrID = m.ScheduleID
	case wire.ServerScheduleAck:
		corrID = m.ScheduleID
	case wire.ServerListSchedulesOk:
		corrID = m.ScheduleID
	case wire.ServerStartWorkflowOk:
		corrID = m.WorkflowID
	case wire.ServerStartWorkflowErr:
		corrID = m.WorkflowID
	case wire.ServerWorkflowAck:
		corrID = m.WorkflowID
	case wire.ServerListWorkflowsOk:
		corrID = m.WorkflowID
	default:
		return
	}
	c.subsMu.Lock()
	p := c.replies[corrID]
	delete(c.replies, corrID)
	c.subsMu.Unlock()
	if p != nil {
		select {
		case p.ch <- msg:
		default:
		}
	}
}

// --- presence ---

// JoinPresence joins a room with a state blob; presenceSnapshot frames
// arrive for every member change.
func (c *Client) JoinPresence(ctx context.Context, room string, state wire.JSONValue) error {
	frame, err := json.Marshal(wire.ClientPresence{Room: room, State: state})
	if err != nil {
		return err
	}
	return c.sendFrame(ctx, frame)
}

// LeavePresence leaves a room.
func (c *Client) LeavePresence(ctx context.Context, room string) error {
	frame, err := json.Marshal(wire.ClientLeavePresence{Room: room})
	if err != nil {
		return err
	}
	return c.sendFrame(ctx, frame)
}

// OnPresence registers a callback for presenceSnapshot frames.
func (c *Client) OnPresence(fn func(room string, members []wire.PresenceMember)) {
	c.setServerObserver(func(msg wire.ServerMessage) {
		if m, ok := msg.(wire.ServerPresenceSnapshot); ok {
			fn(m.Room, m.Members)
		}
	})
}

// optStr returns nil for empty strings (omitted fields).
func optStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
