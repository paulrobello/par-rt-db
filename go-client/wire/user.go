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

// Mirrors core/src/mutation.rs::ScheduleInfo — a scheduled job's public
// view (listSchedules). cron/everyMs/lastError omitted when absent;
// external omitted when false.
type ScheduleInfo struct {
	ID         string         `json:"id"`
	Kind       ScheduleKind   `json:"kind"`
	DueAt      int64          `json:"dueAt"`
	Cron       *string        `json:"cron,omitempty"`
	EveryMs    *int64         `json:"everyMs,omitempty"`
	Status     ScheduleStatus `json:"status"`
	LastError  *string        `json:"lastError,omitempty"`
	CreatedAt  int64          `json:"createdAt"`
	FiredCount int64          `json:"firedCount"`
	External   bool           `json:"external,omitempty"`
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
