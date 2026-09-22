// go-client/wire/envelope.go
package wire

// Mirrors server/src/protocol.rs — the WS ClientMessage/ServerMessage
// vocabularies. Tag "type", camelCase variants and fields, deny-unknown.
// The plan's variant inventory predates the server in several places; the
// server file wins (ledger R11): mutId is required; schedule/workflow
// frames carry correlation ids (scheduleId/workflowId); error frames nest
// an envelope (error: {code, message}); StartWorkflowOk carries a full
// WorkflowInfo; PresenceSnapshot carries members.
//
// Variants with only scalar/struct fields decode with plain struct tags
// (strict via DecodeTagged). Variants carrying JSONValue fields get raw-
// conversion UnmarshalJSON methods.

import (
	"encoding/json"
	"fmt"
)

// PROTOCOL_VERSION mirrors server/src/protocol.rs::PROTOCOL_VERSION.
// ARC-013 (wire-v2 bundle): 1 -> 2 for multi-op aggregates, composite
// groupBy, the mutate-batch endpoint, and cron timezone support — all
// additive/optional wire changes, so a v1 server still parses this client's
// existing traffic. 2 -> 3 (2026-09-22) for the presenceDelta frame: the
// server emits deltas only to connections that authenticated with
// protocolVersion >= 3 and full presenceSnapshot frames to everything else,
// so the gate — not lockstep SDK upgrades — keeps older clients working.
const PROTOCOL_VERSION uint32 = 3

// ---------------------------------------------------------------------------
// ClientMessage — mirrors server/src/protocol.rs::ClientMessage.

// ClientMessage is the full WS client vocabulary.
type ClientMessage interface {
	isClientMessage()
}

// Mirrors server/src/protocol.rs::ClientMessage::Auth — token optional
// (cookie-mode dashboards); protocolVersion optional (ARC-013).
type ClientAuth struct {
	Token           *string `json:"token,omitempty"`
	DB              string  `json:"db"`
	ProtocolVersion *uint32 `json:"protocolVersion,omitempty"`
}

func (ClientAuth) isClientMessage() {}

func (v ClientAuth) MarshalJSON() ([]byte, error) {
	type alias ClientAuth
	return MarshalTagged("type", "auth", alias(v))
}

// Mirrors server/src/protocol.rs::ClientMessage::Subscribe
type ClientSubscribe struct {
	QueryID string `json:"queryId"`
	Query   Query  `json:"query"`
}

func (ClientSubscribe) isClientMessage() {}

func (v ClientSubscribe) MarshalJSON() ([]byte, error) {
	type alias ClientSubscribe
	return MarshalTagged("type", "subscribe", alias(v))
}

// Mirrors server/src/protocol.rs::ClientMessage::Unsubscribe
type ClientUnsubscribe struct {
	QueryID string `json:"queryId"`
}

func (ClientUnsubscribe) isClientMessage() {}

func (v ClientUnsubscribe) MarshalJSON() ([]byte, error) {
	type alias ClientUnsubscribe
	return MarshalTagged("type", "unsubscribe", alias(v))
}

// Mirrors server/src/protocol.rs::ClientMessage::Mutate — mutId required.
type ClientMutate struct {
	MutID          string      `json:"mutId"`
	IdempotencyKey *string     `json:"idempotencyKey,omitempty"`
	Txn            Transaction `json:"txn"`
}

func (ClientMutate) isClientMessage() {}

func (v ClientMutate) MarshalJSON() ([]byte, error) {
	type alias ClientMutate
	return MarshalTagged("type", "mutate", alias(v))
}

func (v *ClientMutate) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		MutID          string          `json:"mutId"`
		IdempotencyKey *string         `json:"idempotencyKey,omitempty"`
		Txn            json.RawMessage `json:"txn"`
	}](b)
	if err != nil {
		return err
	}
	var txn Transaction
	if err := json.Unmarshal(r.Txn, &txn); err != nil {
		return err
	}
	v.MutID, v.IdempotencyKey, v.Txn = r.MutID, r.IdempotencyKey, txn
	return nil
}

// Mirrors server/src/protocol.rs::ClientMessage::Schedule — scheduleId is
// the caller's correlation id; external opts into the claim surface.
type ClientSchedule struct {
	ScheduleID string       `json:"scheduleId"`
	When       ScheduleWhen `json:"when"`
	Txn        Transaction  `json:"txn"`
	External   *bool        `json:"external,omitempty"`
}

func (ClientSchedule) isClientMessage() {}

func (v ClientSchedule) MarshalJSON() ([]byte, error) {
	type alias ClientSchedule
	return MarshalTagged("type", "schedule", alias(v))
}

func (v *ClientSchedule) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		ScheduleID string          `json:"scheduleId"`
		When       json.RawMessage `json:"when"`
		Txn        json.RawMessage `json:"txn"`
		External   *bool           `json:"external,omitempty"`
	}](b)
	if err != nil {
		return err
	}
	when, err := UnmarshalScheduleWhen(r.When)
	if err != nil {
		return err
	}
	var txn Transaction
	if err := json.Unmarshal(r.Txn, &txn); err != nil {
		return err
	}
	v.ScheduleID, v.When, v.Txn, v.External = r.ScheduleID, when, txn, r.External
	return nil
}

// Mirrors server/src/protocol.rs::ClientMessage::CancelSchedule
type ClientCancelSchedule struct {
	ScheduleID string `json:"scheduleId"`
	ID         string `json:"id"`
}

func (ClientCancelSchedule) isClientMessage() {}

func (v ClientCancelSchedule) MarshalJSON() ([]byte, error) {
	type alias ClientCancelSchedule
	return MarshalTagged("type", "cancelSchedule", alias(v))
}

// Mirrors server/src/protocol.rs::ClientMessage::PauseSchedule
type ClientPauseSchedule struct {
	ScheduleID string `json:"scheduleId"`
	ID         string `json:"id"`
}

func (ClientPauseSchedule) isClientMessage() {}

func (v ClientPauseSchedule) MarshalJSON() ([]byte, error) {
	type alias ClientPauseSchedule
	return MarshalTagged("type", "pauseSchedule", alias(v))
}

// Mirrors server/src/protocol.rs::ClientMessage::ResumeSchedule
type ClientResumeSchedule struct {
	ScheduleID string `json:"scheduleId"`
	ID         string `json:"id"`
}

func (ClientResumeSchedule) isClientMessage() {}

func (v ClientResumeSchedule) MarshalJSON() ([]byte, error) {
	type alias ClientResumeSchedule
	return MarshalTagged("type", "resumeSchedule", alias(v))
}

// Mirrors server/src/protocol.rs::ClientMessage::ListSchedules
type ClientListSchedules struct {
	ScheduleID string `json:"scheduleId"`
}

func (ClientListSchedules) isClientMessage() {}

func (v ClientListSchedules) MarshalJSON() ([]byte, error) {
	type alias ClientListSchedules
	return MarshalTagged("type", "listSchedules", alias(v))
}

// Mirrors server/src/protocol.rs::ClientMessage::StartWorkflow
type ClientStartWorkflow struct {
	WorkflowID string       `json:"workflowId"`
	Spec       WorkflowSpec `json:"spec"`
}

func (ClientStartWorkflow) isClientMessage() {}

func (v ClientStartWorkflow) MarshalJSON() ([]byte, error) {
	type alias ClientStartWorkflow
	return MarshalTagged("type", "startWorkflow", alias(v))
}

// Mirrors server/src/protocol.rs::ClientMessage::CancelWorkflow
type ClientCancelWorkflow struct {
	WorkflowID string `json:"workflowId"`
	ID         string `json:"id"`
}

func (ClientCancelWorkflow) isClientMessage() {}

func (v ClientCancelWorkflow) MarshalJSON() ([]byte, error) {
	type alias ClientCancelWorkflow
	return MarshalTagged("type", "cancelWorkflow", alias(v))
}

// Mirrors server/src/protocol.rs::ClientMessage::SignalWorkflow — the
// reply reuses ServerMessage::WorkflowAck.
type ClientSignalWorkflow struct {
	WorkflowID string    `json:"workflowId"`
	ID         string    `json:"id"`
	Name       string    `json:"name"`
	Payload    JSONValue `json:"payload,omitempty"`
}

func (ClientSignalWorkflow) isClientMessage() {}

func (v ClientSignalWorkflow) MarshalJSON() ([]byte, error) {
	type alias ClientSignalWorkflow
	return MarshalTagged("type", "signalWorkflow", alias(v))
}

func (v *ClientSignalWorkflow) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		WorkflowID string          `json:"workflowId"`
		ID         string          `json:"id"`
		Name       string          `json:"name"`
		Payload    json.RawMessage `json:"payload,omitempty"`
	}](b)
	if err != nil {
		return err
	}
	v.WorkflowID, v.ID, v.Name = r.WorkflowID, r.ID, r.Name
	if len(r.Payload) > 0 {
		p, err := UnmarshalJSON(r.Payload)
		if err != nil {
			return err
		}
		v.Payload = p
	}
	return nil
}

// Mirrors server/src/protocol.rs::ClientMessage::ListWorkflows — status is
// the optional WorkflowStatus filter (snake_case values).
type ClientListWorkflows struct {
	WorkflowID string          `json:"workflowId"`
	Status     *WorkflowStatus `json:"status,omitempty"`
}

func (ClientListWorkflows) isClientMessage() {}

func (v ClientListWorkflows) MarshalJSON() ([]byte, error) {
	type alias ClientListWorkflows
	return MarshalTagged("type", "listWorkflows", alias(v))
}

// Mirrors server/src/protocol.rs::ClientMessage::Presence
type ClientPresence struct {
	Room  string    `json:"room"`
	State JSONValue `json:"state,omitempty"`
}

func (ClientPresence) isClientMessage() {}

func (v ClientPresence) MarshalJSON() ([]byte, error) {
	type alias ClientPresence
	return MarshalTagged("type", "presence", alias(v))
}

func (v *ClientPresence) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Room  string          `json:"room"`
		State json.RawMessage `json:"state,omitempty"`
	}](b)
	if err != nil {
		return err
	}
	v.Room = r.Room
	if len(r.State) > 0 {
		s, err := UnmarshalJSON(r.State)
		if err != nil {
			return err
		}
		v.State = s
	}
	return nil
}

// Mirrors server/src/protocol.rs::ClientMessage::PresenceState — state
// required; ttlMs optional.
type ClientPresenceState struct {
	Room  string    `json:"room"`
	State JSONValue `json:"state"`
	TtlMs *uint64   `json:"ttlMs,omitempty"`
}

func (ClientPresenceState) isClientMessage() {}

func (v ClientPresenceState) MarshalJSON() ([]byte, error) {
	type alias ClientPresenceState
	return MarshalTagged("type", "presenceState", alias(v))
}

func (v *ClientPresenceState) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Room  string          `json:"room"`
		State json.RawMessage `json:"state"`
		TtlMs *uint64         `json:"ttlMs,omitempty"`
	}](b)
	if err != nil {
		return err
	}
	v.Room, v.TtlMs = r.Room, r.TtlMs
	s, err := UnmarshalJSON(r.State)
	if err != nil {
		return err
	}
	v.State = s
	return nil
}

// Mirrors server/src/protocol.rs::ClientMessage::LeavePresence
type ClientLeavePresence struct {
	Room string `json:"room"`
}

func (ClientLeavePresence) isClientMessage() {}

func (v ClientLeavePresence) MarshalJSON() ([]byte, error) {
	type alias ClientLeavePresence
	return MarshalTagged("type", "leavePresence", alias(v))
}

// Mirrors server/src/protocol.rs::ClientMessage::Ping — unit variant.
type ClientPing struct{}

func (ClientPing) isClientMessage() {}

func (v ClientPing) MarshalJSON() ([]byte, error) {
	return MarshalTagged("type", "ping", struct{}{})
}

func (v *ClientPing) UnmarshalJSON(b []byte) error {
	_, err := StrictUnmarshal[struct{}](b)
	return err
}

// UnmarshalClientMessage routes a WS frame to its client variant.
func UnmarshalClientMessage(data []byte) (ClientMessage, error) {
	tag, err := PeekTag(data, "type")
	if err != nil {
		return nil, err
	}
	switch tag {
	case "auth":
		return DecodeTagged[ClientAuth](data, "type")
	case "subscribe":
		return DecodeTagged[ClientSubscribe](data, "type")
	case "unsubscribe":
		return DecodeTagged[ClientUnsubscribe](data, "type")
	case "mutate":
		return DecodeTagged[ClientMutate](data, "type")
	case "schedule":
		return DecodeTagged[ClientSchedule](data, "type")
	case "cancelSchedule":
		return DecodeTagged[ClientCancelSchedule](data, "type")
	case "pauseSchedule":
		return DecodeTagged[ClientPauseSchedule](data, "type")
	case "resumeSchedule":
		return DecodeTagged[ClientResumeSchedule](data, "type")
	case "listSchedules":
		return DecodeTagged[ClientListSchedules](data, "type")
	case "startWorkflow":
		return DecodeTagged[ClientStartWorkflow](data, "type")
	case "cancelWorkflow":
		return DecodeTagged[ClientCancelWorkflow](data, "type")
	case "signalWorkflow":
		return DecodeTagged[ClientSignalWorkflow](data, "type")
	case "listWorkflows":
		return DecodeTagged[ClientListWorkflows](data, "type")
	case "presence":
		return DecodeTagged[ClientPresence](data, "type")
	case "presenceState":
		return DecodeTagged[ClientPresenceState](data, "type")
	case "leavePresence":
		return DecodeTagged[ClientLeavePresence](data, "type")
	case "ping":
		return DecodeTagged[ClientPing](data, "type")
	default:
		return nil, fmt.Errorf("wire: unknown client tag %q", tag)
	}
}

// ---------------------------------------------------------------------------
// ServerMessage — mirrors server/src/protocol.rs::ServerMessage.

// ServerMessage is the full WS server vocabulary.
type ServerMessage interface {
	isServerMessage()
}

// Mirrors server/src/protocol.rs::ServerMessage::AuthOk — protocolVersion
// echoed only when the client sent one.
type ServerAuthOk struct {
	User            AuthedUser `json:"user"`
	ProtocolVersion *uint32    `json:"protocolVersion,omitempty"`
}

func (ServerAuthOk) isServerMessage() {}

func (v ServerAuthOk) MarshalJSON() ([]byte, error) {
	type alias ServerAuthOk
	return MarshalTagged("type", "authOk", alias(v))
}

// Mirrors server/src/protocol.rs::ServerMessage::AuthErr — error nests the
// envelope.
type ServerAuthErr struct {
	Error ErrorEnvelope `json:"error"`
}

func (ServerAuthErr) isServerMessage() {}

func (v ServerAuthErr) MarshalJSON() ([]byte, error) {
	type alias ServerAuthErr
	return MarshalTagged("type", "authErr", alias(v))
}

// Mirrors server/src/protocol.rs::ServerMessage::QueryUpdate
type ServerQueryUpdate struct {
	QueryID string    `json:"queryId"`
	Result  JSONValue `json:"result"`
}

func (ServerQueryUpdate) isServerMessage() {}

func (v ServerQueryUpdate) MarshalJSON() ([]byte, error) {
	type alias ServerQueryUpdate
	return MarshalTagged("type", "queryUpdate", alias(v))
}

func (v *ServerQueryUpdate) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		QueryID string          `json:"queryId"`
		Result  json.RawMessage `json:"result"`
	}](b)
	if err != nil {
		return err
	}
	v.QueryID = r.QueryID
	res, err := UnmarshalJSON(r.Result)
	if err != nil {
		return err
	}
	v.Result = res
	return nil
}

// Mirrors server/src/protocol.rs::ServerMessage::MutateOk
type ServerMutateOk struct {
	MutID   string      `json:"mutId"`
	Results []JSONValue `json:"results"`
}

func (ServerMutateOk) isServerMessage() {}

func (v ServerMutateOk) MarshalJSON() ([]byte, error) {
	type alias ServerMutateOk
	return MarshalTagged("type", "mutateOk", alias(v))
}

func (v *ServerMutateOk) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		MutID   string            `json:"mutId"`
		Results []json.RawMessage `json:"results"`
	}](b)
	if err != nil {
		return err
	}
	v.MutID = r.MutID
	results := make([]JSONValue, len(r.Results))
	for i := range r.Results {
		res, err := UnmarshalJSON(r.Results[i])
		if err != nil {
			return err
		}
		results[i] = res
	}
	v.Results = results
	return nil
}

// Mirrors server/src/protocol.rs::ServerMessage::MutateErr
type ServerMutateErr struct {
	MutID string        `json:"mutId"`
	Error ErrorEnvelope `json:"error"`
}

func (ServerMutateErr) isServerMessage() {}

func (v ServerMutateErr) MarshalJSON() ([]byte, error) {
	type alias ServerMutateErr
	return MarshalTagged("type", "mutateErr", alias(v))
}

// Mirrors server/src/protocol.rs::ServerMessage::SubscribeErr
type ServerSubscribeErr struct {
	QueryID string        `json:"queryId"`
	Error   ErrorEnvelope `json:"error"`
}

func (ServerSubscribeErr) isServerMessage() {}

func (v ServerSubscribeErr) MarshalJSON() ([]byte, error) {
	type alias ServerSubscribeErr
	return MarshalTagged("type", "subscribeErr", alias(v))
}

// Mirrors server/src/protocol.rs::ServerMessage::ScheduleOk — id is the
// job id the server minted.
type ServerScheduleOk struct {
	ScheduleID string `json:"scheduleId"`
	ID         string `json:"id"`
}

func (ServerScheduleOk) isServerMessage() {}

func (v ServerScheduleOk) MarshalJSON() ([]byte, error) {
	type alias ServerScheduleOk
	return MarshalTagged("type", "scheduleOk", alias(v))
}

// Mirrors server/src/protocol.rs::ServerMessage::ScheduleErr
type ServerScheduleErr struct {
	ScheduleID string        `json:"scheduleId"`
	Error      ErrorEnvelope `json:"error"`
}

func (ServerScheduleErr) isServerMessage() {}

func (v ServerScheduleErr) MarshalJSON() ([]byte, error) {
	type alias ServerScheduleErr
	return MarshalTagged("type", "scheduleErr", alias(v))
}

// Mirrors server/src/protocol.rs::ServerMessage::ScheduleAck — error
// omitted when ok.
type ServerScheduleAck struct {
	ScheduleID string         `json:"scheduleId"`
	OK         bool           `json:"ok"`
	Error      *ErrorEnvelope `json:"error,omitempty"`
}

func (ServerScheduleAck) isServerMessage() {}

func (v ServerScheduleAck) MarshalJSON() ([]byte, error) {
	type alias ServerScheduleAck
	return MarshalTagged("type", "scheduleAck", alias(v))
}

// Mirrors server/src/protocol.rs::ServerMessage::ListSchedulesOk
type ServerListSchedulesOk struct {
	ScheduleID string         `json:"scheduleId"`
	Schedules  []ScheduleInfo `json:"schedules"`
}

func (ServerListSchedulesOk) isServerMessage() {}

func (v ServerListSchedulesOk) MarshalJSON() ([]byte, error) {
	type alias ServerListSchedulesOk
	return MarshalTagged("type", "listSchedulesOk", alias(v))
}

// Mirrors server/src/protocol.rs::ServerMessage::StartWorkflowOk — info is
// the full WorkflowInfo projection.
type ServerStartWorkflowOk struct {
	WorkflowID string       `json:"workflowId"`
	Info       WorkflowInfo `json:"info"`
}

func (ServerStartWorkflowOk) isServerMessage() {}

func (v ServerStartWorkflowOk) MarshalJSON() ([]byte, error) {
	type alias ServerStartWorkflowOk
	return MarshalTagged("type", "startWorkflowOk", alias(v))
}

// Mirrors server/src/protocol.rs::ServerMessage::StartWorkflowErr
type ServerStartWorkflowErr struct {
	WorkflowID string        `json:"workflowId"`
	Error      ErrorEnvelope `json:"error"`
}

func (ServerStartWorkflowErr) isServerMessage() {}

func (v ServerStartWorkflowErr) MarshalJSON() ([]byte, error) {
	type alias ServerStartWorkflowErr
	return MarshalTagged("type", "startWorkflowErr", alias(v))
}

// Mirrors server/src/protocol.rs::ServerMessage::WorkflowAck — error
// omitted when ok.
type ServerWorkflowAck struct {
	WorkflowID string         `json:"workflowId"`
	OK         bool           `json:"ok"`
	Error      *ErrorEnvelope `json:"error,omitempty"`
}

func (ServerWorkflowAck) isServerMessage() {}

func (v ServerWorkflowAck) MarshalJSON() ([]byte, error) {
	type alias ServerWorkflowAck
	return MarshalTagged("type", "workflowAck", alias(v))
}

// Mirrors server/src/protocol.rs::ServerMessage::ListWorkflowsOk
type ServerListWorkflowsOk struct {
	WorkflowID string         `json:"workflowId"`
	Workflows  []WorkflowInfo `json:"workflows"`
}

func (ServerListWorkflowsOk) isServerMessage() {}

func (v ServerListWorkflowsOk) MarshalJSON() ([]byte, error) {
	type alias ServerListWorkflowsOk
	return MarshalTagged("type", "listWorkflowsOk", alias(v))
}

// Mirrors server/src/protocol.rs::ServerMessage::PresenceSnapshot
type ServerPresenceSnapshot struct {
	Room    string           `json:"room"`
	Members []PresenceMember `json:"members"`
}

func (ServerPresenceSnapshot) isServerMessage() {}

func (v ServerPresenceSnapshot) MarshalJSON() ([]byte, error) {
	type alias ServerPresenceSnapshot
	return MarshalTagged("type", "presenceSnapshot", alias(v))
}

// Mirrors server/src/protocol.rs::ServerMessage::PresenceDelta — the
// incremental broadcast sent INSTEAD of a snapshot to a connection that
// authenticated with protocolVersion >= 3 once the server knows it has seen
// the room. Seq is a per-room monotonic counter (snapshots carry none), so
// a receiver detects a missed delta by a seq gap and resyncs by re-joining.
// The three buckets are omitted when empty, matching the server's
// skip_serializing_if — the all-empty shape is
// {"room":R,"seq":N,"type":"presenceDelta"}.
type ServerPresenceDelta struct {
	Room         string           `json:"room"`
	Seq          uint64           `json:"seq"`
	Joined       []PresenceMember `json:"joined,omitempty"`
	Left         []string         `json:"left,omitempty"`
	StateChanged []PresenceMember `json:"stateChanged,omitempty"`
}

func (ServerPresenceDelta) isServerMessage() {}

func (v ServerPresenceDelta) MarshalJSON() ([]byte, error) {
	type alias ServerPresenceDelta
	return MarshalTagged("type", "presenceDelta", alias(v))
}

// Mirrors server/src/protocol.rs::ServerMessage::PresenceErr
type ServerPresenceErr struct {
	Room  string        `json:"room"`
	Error ErrorEnvelope `json:"error"`
}

func (ServerPresenceErr) isServerMessage() {}

func (v ServerPresenceErr) MarshalJSON() ([]byte, error) {
	type alias ServerPresenceErr
	return MarshalTagged("type", "presenceErr", alias(v))
}

// Mirrors server/src/protocol.rs::ServerMessage::Pong — unit variant.
type ServerPong struct{}

func (ServerPong) isServerMessage() {}

func (v ServerPong) MarshalJSON() ([]byte, error) {
	return MarshalTagged("type", "pong", struct{}{})
}

func (v *ServerPong) UnmarshalJSON(b []byte) error {
	_, err := StrictUnmarshal[struct{}](b)
	return err
}

// UnmarshalServerMessage routes a WS frame to its server variant.
func UnmarshalServerMessage(data []byte) (ServerMessage, error) {
	tag, err := PeekTag(data, "type")
	if err != nil {
		return nil, err
	}
	switch tag {
	case "authOk":
		return DecodeTagged[ServerAuthOk](data, "type")
	case "authErr":
		return DecodeTagged[ServerAuthErr](data, "type")
	case "queryUpdate":
		return DecodeTagged[ServerQueryUpdate](data, "type")
	case "mutateOk":
		return DecodeTagged[ServerMutateOk](data, "type")
	case "mutateErr":
		return DecodeTagged[ServerMutateErr](data, "type")
	case "subscribeErr":
		return DecodeTagged[ServerSubscribeErr](data, "type")
	case "scheduleOk":
		return DecodeTagged[ServerScheduleOk](data, "type")
	case "scheduleErr":
		return DecodeTagged[ServerScheduleErr](data, "type")
	case "scheduleAck":
		return DecodeTagged[ServerScheduleAck](data, "type")
	case "listSchedulesOk":
		return DecodeTagged[ServerListSchedulesOk](data, "type")
	case "startWorkflowOk":
		return DecodeTagged[ServerStartWorkflowOk](data, "type")
	case "startWorkflowErr":
		return DecodeTagged[ServerStartWorkflowErr](data, "type")
	case "workflowAck":
		return DecodeTagged[ServerWorkflowAck](data, "type")
	case "listWorkflowsOk":
		return DecodeTagged[ServerListWorkflowsOk](data, "type")
	case "presenceSnapshot":
		return DecodeTagged[ServerPresenceSnapshot](data, "type")
	case "presenceDelta":
		return DecodeTagged[ServerPresenceDelta](data, "type")
	case "presenceErr":
		return DecodeTagged[ServerPresenceErr](data, "type")
	case "pong":
		return DecodeTagged[ServerPong](data, "type")
	default:
		return nil, fmt.Errorf("wire: unknown server tag %q", tag)
	}
}
