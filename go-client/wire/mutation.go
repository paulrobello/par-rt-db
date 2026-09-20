// go-client/wire/mutation.go
package wire

// Mirrors core/src/mutation.rs — Transaction, Step (15 variants; the plan
// listed 14 but the server's AdjustCounter wins — ledger R10a),
// ScheduleWhen, and the untagged StepResult decode shape. Tag key "op"
// for Step (camelCase), "type" for ScheduleWhen. Strict decode; variants
// marshal through methodless aliases (see filter.go).

import (
	"encoding/json"
	"fmt"
)

// Mirrors core/src/mutation.rs::Transaction — ordered steps, atomic.
// Step is an interface, so decode goes through raw conversion.
type Transaction struct {
	Steps []Step `json:"steps"`
}

// UnmarshalJSON decodes each step through the UnmarshalStep dispatcher.
func (t *Transaction) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Steps []json.RawMessage `json:"steps"`
	}](b)
	if err != nil {
		return err
	}
	steps := make([]Step, len(r.Steps))
	for i := range r.Steps {
		s, err := UnmarshalStep(r.Steps[i])
		if err != nil {
			return err
		}
		steps[i] = s
	}
	t.Steps = steps
	return nil
}

// Step is one write/control step. Variants: StepInsert/StepPatch/
// StepAdjustCounter/StepReplace/StepDelete/StepExpectVersion/
// StepExpectAbsent/StepUpsert/StepPatchByQuery/StepDeleteByQuery/
// StepSchedule/StepCancelSchedule/StepStartWorkflow/StepCancelWorkflow/
// StepUndelete.
type Step interface {
	isStep()
}

// Mirrors core/src/mutation.rs::Step::Insert
type StepInsert struct {
	Table string    `json:"table"`
	Doc   JSONValue `json:"doc"`
}

func (StepInsert) isStep() {}

func (v StepInsert) MarshalJSON() ([]byte, error) {
	type alias StepInsert
	return MarshalTagged("op", "insert", alias(v))
}

func (v *StepInsert) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Table string          `json:"table"`
		Doc   json.RawMessage `json:"doc"`
	}](b)
	if err != nil {
		return err
	}
	doc, err := UnmarshalJSON(r.Doc)
	if err != nil {
		return err
	}
	v.Table, v.Doc = r.Table, doc
	return nil
}

// Mirrors core/src/mutation.rs::Step::Patch
type StepPatch struct {
	Table  string    `json:"table"`
	ID     string    `json:"id"`
	Fields JSONValue `json:"fields"`
}

func (StepPatch) isStep() {}

func (v StepPatch) MarshalJSON() ([]byte, error) {
	type alias StepPatch
	return MarshalTagged("op", "patch", alias(v))
}

func (v *StepPatch) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Table  string          `json:"table"`
		ID     string          `json:"id"`
		Fields json.RawMessage `json:"fields"`
	}](b)
	if err != nil {
		return err
	}
	fields, err := UnmarshalJSON(r.Fields)
	if err != nil {
		return err
	}
	v.Table, v.ID, v.Fields = r.Table, r.ID, fields
	return nil
}

// Mirrors core/src/mutation.rs::Step::AdjustCounter — the plan's 14-variant
// inventory predates this step; the server file wins.
type StepAdjustCounter struct {
	Table    string    `json:"table"`
	ID       string    `json:"id"`
	Field    string    `json:"field"`
	Delta    int64     `json:"delta"`
	Min      *int64    `json:"min,omitempty"`
	Max      *int64    `json:"max,omitempty"`
	Expected JSONValue `json:"expected,omitempty"`
}

func (StepAdjustCounter) isStep() {}

func (v StepAdjustCounter) MarshalJSON() ([]byte, error) {
	type alias StepAdjustCounter
	return MarshalTagged("op", "adjustCounter", alias(v))
}

func (v *StepAdjustCounter) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Table    string          `json:"table"`
		ID       string          `json:"id"`
		Field    string          `json:"field"`
		Delta    int64           `json:"delta"`
		Min      *int64          `json:"min,omitempty"`
		Max      *int64          `json:"max,omitempty"`
		Expected json.RawMessage `json:"expected,omitempty"`
	}](b)
	if err != nil {
		return err
	}
	v.Table, v.ID, v.Field, v.Delta, v.Min, v.Max = r.Table, r.ID, r.Field, r.Delta, r.Min, r.Max
	if len(r.Expected) > 0 {
		e, err := UnmarshalJSON(r.Expected)
		if err != nil {
			return err
		}
		v.Expected = e
	}
	return nil
}

// Mirrors core/src/mutation.rs::Step::Replace
type StepReplace struct {
	Table string    `json:"table"`
	ID    string    `json:"id"`
	Doc   JSONValue `json:"doc"`
}

func (StepReplace) isStep() {}

func (v StepReplace) MarshalJSON() ([]byte, error) {
	type alias StepReplace
	return MarshalTagged("op", "replace", alias(v))
}

func (v *StepReplace) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Table string          `json:"table"`
		ID    string          `json:"id"`
		Doc   json.RawMessage `json:"doc"`
	}](b)
	if err != nil {
		return err
	}
	doc, err := UnmarshalJSON(r.Doc)
	if err != nil {
		return err
	}
	v.Table, v.ID, v.Doc = r.Table, r.ID, doc
	return nil
}

// Mirrors core/src/mutation.rs::Step::Delete
type StepDelete struct {
	Table string `json:"table"`
	ID    string `json:"id"`
}

func (StepDelete) isStep() {}

func (v StepDelete) MarshalJSON() ([]byte, error) {
	type alias StepDelete
	return MarshalTagged("op", "delete", alias(v))
}

func (v *StepDelete) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Table string `json:"table"`
		ID    string `json:"id"`
	}](b)
	if err != nil {
		return err
	}
	v.Table, v.ID = r.Table, r.ID
	return nil
}

// Mirrors core/src/mutation.rs::Step::Undelete — soft-deleted rows only.
type StepUndelete struct {
	Table string `json:"table"`
	ID    string `json:"id"`
}

func (StepUndelete) isStep() {}

func (v StepUndelete) MarshalJSON() ([]byte, error) {
	type alias StepUndelete
	return MarshalTagged("op", "undelete", alias(v))
}

func (v *StepUndelete) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Table string `json:"table"`
		ID    string `json:"id"`
	}](b)
	if err != nil {
		return err
	}
	v.Table, v.ID = r.Table, r.ID
	return nil
}

// Mirrors core/src/mutation.rs::Step::ExpectVersion — version is a JSON
// number (serde i64), not the quoted-string index-value convention.
type StepExpectVersion struct {
	Table   string `json:"table"`
	ID      string `json:"id"`
	Version int64  `json:"version"`
}

func (StepExpectVersion) isStep() {}

func (v StepExpectVersion) MarshalJSON() ([]byte, error) {
	type alias StepExpectVersion
	return MarshalTagged("op", "expectVersion", alias(v))
}

func (v *StepExpectVersion) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Table   string `json:"table"`
		ID      string `json:"id"`
		Version int64  `json:"version"`
	}](b)
	if err != nil {
		return err
	}
	v.Table, v.ID, v.Version = r.Table, r.ID, r.Version
	return nil
}

// Mirrors core/src/mutation.rs::Step::ExpectAbsent
type StepExpectAbsent struct {
	Table string      `json:"table"`
	Index string      `json:"index"`
	Eq    []JSONValue `json:"eq"`
}

func (StepExpectAbsent) isStep() {}

func (v StepExpectAbsent) MarshalJSON() ([]byte, error) {
	type alias StepExpectAbsent
	return MarshalTagged("op", "expectAbsent", alias(v))
}

func (v *StepExpectAbsent) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Table string            `json:"table"`
		Index string            `json:"index"`
		Eq    []json.RawMessage `json:"eq"`
	}](b)
	if err != nil {
		return err
	}
	eq := make([]JSONValue, len(r.Eq))
	for i := range r.Eq {
		v, err := UnmarshalJSON(r.Eq[i])
		if err != nil {
			return err
		}
		eq[i] = v
	}
	v.Table, v.Index, v.Eq = r.Table, r.Index, eq
	return nil
}

// Mirrors core/src/mutation.rs::Step::Upsert
type StepUpsert struct {
	Table  string      `json:"table"`
	Index  string      `json:"index"`
	Eq     []JSONValue `json:"eq"`
	Insert JSONValue   `json:"insert"`
	Patch  JSONValue   `json:"patch"`
}

func (StepUpsert) isStep() {}

func (v StepUpsert) MarshalJSON() ([]byte, error) {
	type alias StepUpsert
	return MarshalTagged("op", "upsert", alias(v))
}

func (v *StepUpsert) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Table  string            `json:"table"`
		Index  string            `json:"index"`
		Eq     []json.RawMessage `json:"eq"`
		Insert json.RawMessage   `json:"insert"`
		Patch  json.RawMessage   `json:"patch"`
	}](b)
	if err != nil {
		return err
	}
	eq := make([]JSONValue, len(r.Eq))
	for i := range r.Eq {
		ev, err := UnmarshalJSON(r.Eq[i])
		if err != nil {
			return err
		}
		eq[i] = ev
	}
	ins, err := UnmarshalJSON(r.Insert)
	if err != nil {
		return err
	}
	patch, err := UnmarshalJSON(r.Patch)
	if err != nil {
		return err
	}
	v.Table, v.Index, v.Eq, v.Insert, v.Patch = r.Table, r.Index, eq, ins, patch
	return nil
}

// Mirrors core/src/mutation.rs::Step::PatchByQuery
type StepPatchByQuery struct {
	Table  string     `json:"table"`
	Filter FilterExpr `json:"filter"`
	Patch  JSONValue  `json:"patch"`
	Limit  *int       `json:"limit,omitempty"`
}

func (StepPatchByQuery) isStep() {}

func (v StepPatchByQuery) MarshalJSON() ([]byte, error) {
	type alias StepPatchByQuery
	return MarshalTagged("op", "patchByQuery", alias(v))
}

func (v *StepPatchByQuery) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Table  string          `json:"table"`
		Filter json.RawMessage `json:"filter"`
		Patch  json.RawMessage `json:"patch"`
		Limit  *int            `json:"limit,omitempty"`
	}](b)
	if err != nil {
		return err
	}
	f, err := UnmarshalFilterExpr(r.Filter)
	if err != nil {
		return err
	}
	patch, err := UnmarshalJSON(r.Patch)
	if err != nil {
		return err
	}
	v.Table, v.Filter, v.Patch, v.Limit = r.Table, f, patch, r.Limit
	return nil
}

// Mirrors core/src/mutation.rs::Step::DeleteByQuery
type StepDeleteByQuery struct {
	Table  string     `json:"table"`
	Filter FilterExpr `json:"filter"`
	Limit  *int       `json:"limit,omitempty"`
}

func (StepDeleteByQuery) isStep() {}

func (v StepDeleteByQuery) MarshalJSON() ([]byte, error) {
	type alias StepDeleteByQuery
	return MarshalTagged("op", "deleteByQuery", alias(v))
}

func (v *StepDeleteByQuery) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Table  string          `json:"table"`
		Filter json.RawMessage `json:"filter"`
		Limit  *int            `json:"limit,omitempty"`
	}](b)
	if err != nil {
		return err
	}
	f, err := UnmarshalFilterExpr(r.Filter)
	if err != nil {
		return err
	}
	v.Table, v.Filter, v.Limit = r.Table, f, r.Limit
	return nil
}

// Mirrors core/src/mutation.rs::Step::Schedule — the nested txn and `when`
// decode through their own dispatchers; external opts out of internal
// scheduling (external claim surface).
type StepSchedule struct {
	When     ScheduleWhen `json:"when"`
	Txn      Transaction  `json:"txn"`
	External *bool        `json:"external,omitempty"`
}

func (StepSchedule) isStep() {}

func (v StepSchedule) MarshalJSON() ([]byte, error) {
	type alias StepSchedule
	return MarshalTagged("op", "schedule", alias(v))
}

func (v *StepSchedule) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		When     json.RawMessage `json:"when"`
		Txn      json.RawMessage `json:"txn"`
		External *bool           `json:"external,omitempty"`
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
	v.When, v.Txn, v.External = when, txn, r.External
	return nil
}

// Mirrors core/src/mutation.rs::Step::CancelSchedule
type StepCancelSchedule struct {
	ID string `json:"id"`
}

func (StepCancelSchedule) isStep() {}

func (v StepCancelSchedule) MarshalJSON() ([]byte, error) {
	type alias StepCancelSchedule
	return MarshalTagged("op", "cancelSchedule", alias(v))
}

func (v *StepCancelSchedule) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		ID string `json:"id"`
	}](b)
	if err != nil {
		return err
	}
	v.ID = r.ID
	return nil
}

// Mirrors core/src/mutation.rs::Step::StartWorkflow
type StepStartWorkflow struct {
	Spec WorkflowSpec `json:"spec"`
}

func (StepStartWorkflow) isStep() {}

func (v StepStartWorkflow) MarshalJSON() ([]byte, error) {
	type alias StepStartWorkflow
	return MarshalTagged("op", "startWorkflow", alias(v))
}

func (v *StepStartWorkflow) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Spec json.RawMessage `json:"spec"`
	}](b)
	if err != nil {
		return err
	}
	var spec WorkflowSpec
	if err := json.Unmarshal(r.Spec, &spec); err != nil {
		return err
	}
	v.Spec = spec
	return nil
}

// Mirrors core/src/mutation.rs::Step::CancelWorkflow
type StepCancelWorkflow struct {
	ID string `json:"id"`
}

func (StepCancelWorkflow) isStep() {}

func (v StepCancelWorkflow) MarshalJSON() ([]byte, error) {
	type alias StepCancelWorkflow
	return MarshalTagged("op", "cancelWorkflow", alias(v))
}

func (v *StepCancelWorkflow) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		ID string `json:"id"`
	}](b)
	if err != nil {
		return err
	}
	v.ID = r.ID
	return nil
}

// Mirrors core/src/mutation.rs::WorkflowSpec — a submitted workflow
// definition, stored verbatim per run.
type WorkflowSpec struct {
	Name  string             `json:"name"`
	Steps []WorkflowStepSpec `json:"steps"`
}

// Mirrors core/src/mutation.rs::WorkflowStepSpec — txn or awaitSignal
// (exactly one, server-validated), plus policy.
type WorkflowStepSpec struct {
	Txn           *Transaction     `json:"txn,omitempty"`
	AwaitSignal   *AwaitSignalSpec `json:"awaitSignal,omitempty"`
	Retry         *StepRetry       `json:"retry,omitempty"`
	SleepBeforeMs *uint64          `json:"sleepBeforeMs,omitempty"`
}

// Mirrors core/src/mutation.rs::StepRetry — maxAttempts counts TOTAL
// attempts. initialRetryMs/maxRetryMs always serialize (server uses serde
// defaults, not skip_serializing_if).
type StepRetry struct {
	MaxAttempts    int    `json:"maxAttempts"`
	InitialRetryMs uint64 `json:"initialRetryMs"`
	MaxRetryMs     uint64 `json:"maxRetryMs"`
}

// Mirrors core/src/mutation.rs::AwaitSignalSpec
type AwaitSignalSpec struct {
	Name      string  `json:"name"`
	TimeoutMs *uint64 `json:"timeoutMs,omitempty"`
}

// Mirrors core/src/mutation.rs::ScheduleWhen — tag "type", camelCase
// variants; When* Go names avoid generic-name collisions (ledger R10b).
type ScheduleWhen interface {
	isScheduleWhen()
}

// Mirrors core/src/mutation.rs::ScheduleWhen::AfterMs
type WhenAfterMs struct {
	Ms int64 `json:"ms"`
}

func (WhenAfterMs) isScheduleWhen() {}

func (v WhenAfterMs) MarshalJSON() ([]byte, error) {
	type alias WhenAfterMs
	return MarshalTagged("type", "afterMs", alias(v))
}

func (v *WhenAfterMs) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Ms int64 `json:"ms"`
	}](b)
	if err != nil {
		return err
	}
	v.Ms = r.Ms
	return nil
}

// Mirrors core/src/mutation.rs::ScheduleWhen::RunAt
type WhenRunAt struct {
	Ms int64 `json:"ms"`
}

func (WhenRunAt) isScheduleWhen() {}

func (v WhenRunAt) MarshalJSON() ([]byte, error) {
	type alias WhenRunAt
	return MarshalTagged("type", "runAt", alias(v))
}

func (v *WhenRunAt) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Ms int64 `json:"ms"`
	}](b)
	if err != nil {
		return err
	}
	v.Ms = r.Ms
	return nil
}

// Mirrors core/src/mutation.rs::ScheduleWhen::Cron — Tz is the optional IANA
// timezone used to evaluate the cron's local wall clock, omitted when absent.
type WhenCron struct {
	Expr string  `json:"expr"`
	Tz   *string `json:"tz,omitempty"`
}

func (WhenCron) isScheduleWhen() {}

func (v WhenCron) MarshalJSON() ([]byte, error) {
	type alias WhenCron
	return MarshalTagged("type", "cron", alias(v))
}

func (v *WhenCron) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		Expr string  `json:"expr"`
		Tz   *string `json:"tz,omitempty"`
	}](b)
	if err != nil {
		return err
	}
	v.Expr = r.Expr
	v.Tz = r.Tz
	return nil
}

// Mirrors core/src/mutation.rs::ScheduleWhen::Interval — everyMs is the
// camelCase rename; ms stays bare in afterMs/runAt.
type WhenInterval struct {
	EveryMs int64 `json:"everyMs"`
}

func (WhenInterval) isScheduleWhen() {}

func (v WhenInterval) MarshalJSON() ([]byte, error) {
	type alias WhenInterval
	return MarshalTagged("type", "interval", alias(v))
}

func (v *WhenInterval) UnmarshalJSON(b []byte) error {
	r, err := StrictUnmarshal[struct {
		EveryMs int64 `json:"everyMs"`
	}](b)
	if err != nil {
		return err
	}
	v.EveryMs = r.EveryMs
	return nil
}

// UnmarshalScheduleWhen routes a tagged when-object to its variant.
func UnmarshalScheduleWhen(data []byte) (ScheduleWhen, error) {
	tag, err := PeekTag(data, "type")
	if err != nil {
		return nil, err
	}
	switch tag {
	case "afterMs":
		return DecodeTagged[WhenAfterMs](data, "type")
	case "runAt":
		return DecodeTagged[WhenRunAt](data, "type")
	case "cron":
		return DecodeTagged[WhenCron](data, "type")
	case "interval":
		return DecodeTagged[WhenInterval](data, "type")
	default:
		return nil, fmt.Errorf("wire: unknown when tag %q", tag)
	}
}

// UnmarshalStep routes a tagged step to its variant across all 15 ops.
func UnmarshalStep(data []byte) (Step, error) {
	tag, err := PeekTag(data, "op")
	if err != nil {
		return nil, err
	}
	switch tag {
	case "insert":
		return DecodeTagged[StepInsert](data, "op")
	case "patch":
		return DecodeTagged[StepPatch](data, "op")
	case "adjustCounter":
		return DecodeTagged[StepAdjustCounter](data, "op")
	case "replace":
		return DecodeTagged[StepReplace](data, "op")
	case "delete":
		return DecodeTagged[StepDelete](data, "op")
	case "undelete":
		return DecodeTagged[StepUndelete](data, "op")
	case "expectVersion":
		return DecodeTagged[StepExpectVersion](data, "op")
	case "expectAbsent":
		return DecodeTagged[StepExpectAbsent](data, "op")
	case "upsert":
		return DecodeTagged[StepUpsert](data, "op")
	case "patchByQuery":
		return DecodeTagged[StepPatchByQuery](data, "op")
	case "deleteByQuery":
		return DecodeTagged[StepDeleteByQuery](data, "op")
	case "schedule":
		return DecodeTagged[StepSchedule](data, "op")
	case "cancelSchedule":
		return DecodeTagged[StepCancelSchedule](data, "op")
	case "startWorkflow":
		return DecodeTagged[StepStartWorkflow](data, "op")
	case "cancelWorkflow":
		return DecodeTagged[StepCancelWorkflow](data, "op")
	default:
		return nil, fmt.Errorf("wire: unknown step op %q", tag)
	}
}

// Mirrors rust-client/src/mutation.rs::StepResult — the untagged
// decode-by-shape of one step's result. adjustCounter's result is null
// (it executes as a patch), so no dedicated arm exists, matching rust.
type StepResult struct {
	ID         *string `json:"id,omitempty"`
	Inserted   *bool   `json:"inserted,omitempty"`
	Patched    *int64  `json:"patched,omitempty"`
	Truncated  *bool   `json:"truncated,omitempty"`
	Deleted    *int64  `json:"deleted,omitempty"`
	Cancelled  *bool   `json:"cancelled,omitempty"`
	ScheduleID *string `json:"scheduleId,omitempty"`
	WorkflowID *string `json:"workflowId,omitempty"`
}

// MarshalJSON emits JSON null for a null step result (patch/delete/
// expect*/undelete steps carry no payload; the server's untagged
// StepResult::Null serializes as null, never {}).
func (r StepResult) MarshalJSON() ([]byte, error) {
	if r.ID == nil && r.Inserted == nil && r.Patched == nil && r.Truncated == nil &&
		r.Deleted == nil && r.Cancelled == nil && r.ScheduleID == nil && r.WorkflowID == nil {
		return []byte("null"), nil
	}
	type alias StepResult
	return json.Marshal(alias(r))
}

// --- strict-decode wrappers for scalar-only wire structs (fix round 1:
// unknown-field rejection on every wire type). ---

// UnmarshalJSON rejects unknown fields.
func (w *WorkflowSpec) UnmarshalJSON(b []byte) error {
	type alias WorkflowSpec
	v, err := StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*w = WorkflowSpec(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (w *WorkflowStepSpec) UnmarshalJSON(b []byte) error {
	type alias WorkflowStepSpec
	v, err := StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*w = WorkflowStepSpec(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (s *StepRetry) UnmarshalJSON(b []byte) error {
	type alias StepRetry
	v, err := StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*s = StepRetry(v)
	return nil
}

// UnmarshalJSON rejects unknown fields.
func (a *AwaitSignalSpec) UnmarshalJSON(b []byte) error {
	type alias AwaitSignalSpec
	v, err := StrictUnmarshal[alias](b)
	if err != nil {
		return err
	}
	*a = AwaitSignalSpec(v)
	return nil
}
