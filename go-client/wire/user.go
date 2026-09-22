// go-client/wire/user.go
package wire

// Mirrors server/src/protocol.rs support types: the auth identity, the
// error envelope, and the workflow/schedule projection shapes.

import "encoding/json"

// Mirrors server/src/protocol.rs::UserKind — snake_case wire values.
type UserKind string

const (
	UserKindUser    UserKind = "user"
	UserKindMachine UserKind = "machine"
)

// Mirrors server/src/protocol.rs::AuthedUser. email and name serialize as
// JSON null when absent (no skip_serializing_if); githubLogin/githubId are
// omitted entirely — same struct, two behaviors.
type AuthedUser struct {
	Kind        UserKind `json:"kind"`
	Email       *string  `json:"email"`
	Name        *string  `json:"name"`
	GithubLogin *string  `json:"githubLogin,omitempty"`
	GithubID    *int64   `json:"githubId,omitempty"`
}

// Mirrors server/src/protocol.rs::RtDbError — the wire error envelope
// (SCREAMING_SNAKE_CASE codes; retryAfter in seconds on RATE_LIMITED).
type ErrorEnvelope struct {
	Code       string   `json:"code"`
	Message    string   `json:"message"`
	RetryAfter *float64 `json:"retryAfter,omitempty"`
}

// Mirrors server/src/protocol.rs::PresenceMember
type PresenceMember struct {
	ConnectionID string     `json:"connectionId"`
	User         AuthedUser `json:"user"`
	State        JSONValue  `json:"state"`
}

// UnmarshalJSON decodes the member's dynamic state blob.
func (p *PresenceMember) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		ConnectionID string          `json:"connectionId"`
		User         AuthedUser      `json:"user"`
		State        json.RawMessage `json:"state"`
	}](b)
	if err != nil {
		return err
	}
	p.ConnectionID, p.User = r.ConnectionID, r.User
	s, err := UnmarshalJSON(r.State)
	if err != nil {
		return err
	}
	p.State = s
	return nil
}

// Mirrors core/src/mutation.rs::WorkflowStatus — snake_case values.
type WorkflowStatus string

const (
	WorkflowStatusPending   WorkflowStatus = "pending"
	WorkflowStatusRunning   WorkflowStatus = "running"
	WorkflowStatusWaiting   WorkflowStatus = "waiting"
	WorkflowStatusSuccess   WorkflowStatus = "success"
	WorkflowStatusFailed    WorkflowStatus = "failed"
	WorkflowStatusCancelled WorkflowStatus = "cancelled"
)

// Mirrors protocol.rs::OutcomeStatus — lowercase values.
type OutcomeStatus string

const (
	OutcomeSuccess OutcomeStatus = "success"
	OutcomeFailed  OutcomeStatus = "failed"
)

// Mirrors server/src/protocol.rs::StepOutcome
type StepOutcome struct {
	StepIndex int           `json:"stepIndex"`
	Status    OutcomeStatus `json:"status"`
	Attempts  int           `json:"attempts"`
	At        int64         `json:"at"`
	Error     *string       `json:"error,omitempty"`
	Signal    JSONValue     `json:"signal,omitempty"`
}

// UnmarshalJSON converts the dynamic signal blob.
func (s *StepOutcome) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		StepIndex int             `json:"stepIndex"`
		Status    OutcomeStatus   `json:"status"`
		Attempts  int             `json:"attempts"`
		At        int64           `json:"at"`
		Error     *string         `json:"error,omitempty"`
		Signal    json.RawMessage `json:"signal,omitempty"`
	}](b)
	if err != nil {
		return err
	}
	s.StepIndex, s.Status, s.Attempts, s.At, s.Error = r.StepIndex, r.Status, r.Attempts, r.At, r.Error
	if len(r.Signal) > 0 {
		v, err := UnmarshalJSON(r.Signal)
		if err != nil {
			return err
		}
		s.Signal = v
	}
	return nil
}

// Mirrors server/src/protocol.rs::WorkflowInfo — the list/get projection
// of one workflow run.
type WorkflowInfo struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Status      WorkflowStatus `json:"status"`
	CurrentStep int            `json:"currentStep"`
	StepCount   int            `json:"stepCount"`
	Attempts    int            `json:"attempts"`
	SleepUntil  *int64         `json:"sleepUntil,omitempty"`
	LastError   *string        `json:"lastError,omitempty"`
	WaitingFor  *string        `json:"waitingFor,omitempty"`
	WaitedSince *int64         `json:"waitedSince,omitempty"`
	CreatedAt   int64          `json:"createdAt"`
	UpdatedAt   int64          `json:"updatedAt"`
	StartedAt   *int64         `json:"startedAt,omitempty"`
	FinishedAt  *int64         `json:"finishedAt,omitempty"`
}

// Mirrors server/src/protocol.rs::WorkflowInfoFull — the run info
// projection flattened with the per-step outcome trail.
type WorkflowInfoFull struct {
	WorkflowInfo
	StepOutcomes []StepOutcome `json:"stepOutcomes"`
}

// UnmarshalJSON rejects unknown fields at both levels: the info keys strict-
// decode through WorkflowInfo's own UnmarshalJSON (the embedded strict
// Unmarshaler defeats DisallowUnknownFields flattening, so the alias form
// cannot see stepOutcomes as a declared field), and stepOutcomes decodes
// through each element's signal-converting UnmarshalJSON.
func (w *WorkflowInfoFull) UnmarshalJSON(b []byte) error {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil {
		return err
	}
	infoKeys := make(map[string]json.RawMessage, len(m))
	for k, v := range m {
		if k != "stepOutcomes" {
			infoKeys[k] = v
		}
	}
	var info WorkflowInfo
	if len(infoKeys) > 0 {
		rest, err := json.Marshal(infoKeys)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(rest, &info); err != nil {
			return err
		}
	}
	var outcomes []StepOutcome
	if raw, ok := m["stepOutcomes"]; ok {
		if err := json.Unmarshal(raw, &outcomes); err != nil {
			return err
		}
	}
	w.WorkflowInfo, w.StepOutcomes = info, outcomes
	return nil
}

// Mirrors core/src/mutation.rs::ScheduleInfo — a scheduled job's public
// view (listSchedules). cron/tz/everyMs/lastError omitted when absent;
// external omitted when false.
type ScheduleInfo struct {
	ID         string         `json:"id"`
	Kind       ScheduleKind   `json:"kind"`
	DueAt      int64          `json:"dueAt"`
	Cron       *string        `json:"cron,omitempty"`
	Tz         *string        `json:"tz,omitempty"`
	EveryMs    *int64         `json:"everyMs,omitempty"`
	Status     ScheduleStatus `json:"status"`
	LastError  *string        `json:"lastError,omitempty"`
	CreatedAt  int64          `json:"createdAt"`
	FiredCount int64          `json:"firedCount"`
	External   bool           `json:"external,omitempty"`
	// MissedCount is the cumulative count of recurring-job windows that
	// elapsed before a fire (e.g. the process was down across one or more
	// fire times). Recurring jobs skip missed windows by design — this is
	// observability, not a policy change. Omitted on the wire when 0.
	MissedCount int64 `json:"missedCount,omitempty"`
	// LastMissedAt is the epoch ms of the last fire at which a missed
	// window was detected, present only when MissedCount is nonzero.
	LastMissedAt *int64 `json:"lastMissedAt,omitempty"`
}

// Mirrors core/src/mutation.rs::ScheduleKind — snake_case values.
type ScheduleKind string

const (
	ScheduleKindOneshot  ScheduleKind = "oneshot"
	ScheduleKindCron     ScheduleKind = "cron"
	ScheduleKindInterval ScheduleKind = "interval"
)

// Mirrors core/src/mutation.rs::ScheduleStatus — snake_case values.
type ScheduleStatus string

const (
	ScheduleStatusPending ScheduleStatus = "pending"
	ScheduleStatusRunning ScheduleStatus = "running"
	ScheduleStatusPaused  ScheduleStatus = "paused"
	ScheduleStatusError   ScheduleStatus = "error"
)

// --- strict-decode wrappers for scalar-only wire structs (fix round 1:
// unknown-field rejection on every wire type). ---

// UnmarshalJSON rejects unknown fields.
func (u *AuthedUser) UnmarshalJSON(b []byte) error {
	type alias AuthedUser
	v, err := StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	// ARC-009 narrowing: kind is a closed enum — an unknown kind is a decode
	// error exactly like serde's `deny_unknown_fields` enum repr.
	switch v.Kind {
	case UserKindUser, UserKindMachine:
	default:
		return errUnknownKind
	}
	*u = AuthedUser(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (e *ErrorEnvelope) UnmarshalJSON(b []byte) error {
	type alias ErrorEnvelope
	v, err := StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*e = ErrorEnvelope(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (w *WorkflowInfo) UnmarshalJSON(b []byte) error {
	type alias WorkflowInfo
	v, err := StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*w = WorkflowInfo(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (s *ScheduleInfo) UnmarshalJSON(b []byte) error {
	type alias ScheduleInfo
	v, err := StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	switch v.Kind {
	case ScheduleKindOneshot, ScheduleKindCron, ScheduleKindInterval:
	default:
		return errUnknownKind
	}
	switch v.Status {
	case ScheduleStatusPending, ScheduleStatusRunning, ScheduleStatusPaused, ScheduleStatusError:
	default:
		return errUnknownStatus
	}
	*s = ScheduleInfo(v)
	return nil
}

// Enum-value decode errors (the corpus's rejects_schedule_info_unknown_*
// and rejects_authed_user_unknown_kind fixtures pin these).
var (
	errUnknownKind   = &json.UnmarshalTypeError{Value: "unknown enum kind"}
	errUnknownStatus = &json.UnmarshalTypeError{Value: "unknown enum status"}
)
