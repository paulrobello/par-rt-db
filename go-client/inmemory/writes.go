// The transactional write path — the Go port of rust in_memory/mod.rs's
// mutate/execute_transaction/execute_step and the per-step helpers (server
// txn.rs semantics: atomic rollback via doc-store snapshot, subscription
// fan-out by table, and the step/row caps).
package inmemory

import (
	"sort"
	"strconv"

	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

const (
	// maxSteps is the flat per-txn step ceiling (recursive count against the
	// nesting bomb).
	maxSteps = 1024
	// maxAffectedRowsPerTxn is the SEC-104 worst-case row budget.
	maxAffectedRowsPerTxn = 10_000
	// maxCascadeRows bounds one initiating delete's cascade.
	maxCascadeRows = 10_000
	// maxByQueryRows caps one by-query step's match set.
	maxByQueryRows = 1000
	// maxByQueryStepsPerTxn bounds by-query steps per txn.
	maxByQueryStepsPerTxn = 16
	// cronStepMs is the in-memory cron re-fire interval (real 5-field cron
	// parsing is server-side).
	cronStepMs = 60_000
	// maxEveryMs bounds an interval job's recurrence (one year).
	maxEveryMs = 365 * 24 * 60 * 60 * 1000
)

// ApplyTxn executes a transaction atomically and returns one StepResult per
// step, in order. A mutID that has been seen before short-circuits with the
// cached results. The Go mirror of rust mutate + execute_transaction.
func ApplyTxn(s *Store, txn wire.Transaction, mutID string) ([]wire.StepResult, error) {
	s.mu.Lock()
	if mutID != "" {
		if cached, ok := s.idempotency[mutID]; ok {
			s.mu.Unlock()
			return cached, nil
		}
	}
	results, err := executeTransaction(s, txn)
	if err == nil && mutID != "" {
		s.idempotency[mutID] = results
	}
	// Subscriber callbacks flush OUTSIDE the lock (rust fires outside the
	// borrow): a re-entering callback would self-deadlock the mutex.
	fires := s.takePendingFires()
	s.mu.Unlock()
	for _, f := range fires {
		f.callback(f.value)
	}
	if err != nil {
		return nil, err
	}
	return results, nil
}

func countSteps(txn wire.Transaction) int {
	n := 0
	for _, step := range txn.Steps {
		switch t := step.(type) {
		case wire.StepSchedule:
			n += 1 + countSteps(t.Txn)
		default:
			n++
		}
	}
	return n
}

// worstCaseAffected over-approximates the documents txn could touch; the
// budget check must never under-approximate (server SEC-104).
func worstCaseAffected(txn wire.Transaction) int {
	total := 0
	for _, step := range txn.Steps {
		switch t := step.(type) {
		case wire.StepPatchByQuery:
			limit := maxByQueryRows
			if t.Limit != nil && *t.Limit >= 0 && *t.Limit < limit {
				limit = *t.Limit
			}
			total += limit
		case wire.StepDeleteByQuery:
			limit := maxByQueryRows
			if t.Limit != nil && *t.Limit >= 0 && *t.Limit < limit {
				limit = *t.Limit
			}
			total += limit
		case wire.StepSchedule, wire.StepCancelSchedule,
			wire.StepStartWorkflow, wire.StepCancelWorkflow:
			// Control-flow steps touch no documents.
		default:
			total++
		}
	}
	return total
}

func executeTransaction(s *Store, txn wire.Transaction) ([]wire.StepResult, error) {
	if countSteps(txn) > maxSteps {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"transaction exceeds maximum of "+formatI64(maxSteps)+" steps")
	}
	byQuerySteps := 0
	for _, step := range txn.Steps {
		switch step.(type) {
		case wire.StepPatchByQuery, wire.StepDeleteByQuery:
			byQuerySteps++
		}
	}
	if byQuerySteps > maxByQueryStepsPerTxn {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"transaction has "+formatI64(int64(byQuerySteps))+
				" by-query steps, exceeding the limit of "+formatI64(maxByQueryStepsPerTxn))
	}
	worst := worstCaseAffected(txn)
	if worst > maxAffectedRowsPerTxn {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"transaction could affect up to "+formatI64(int64(worst))+
				" documents, exceeding the limit of "+formatI64(maxAffectedRowsPerTxn))
	}
	snapshot := snapshotDocs(s)
	schedulesSnapshot := cloneSchedules(s.scheduledJobs)
	results := make([]wire.StepResult, 0, len(txn.Steps))
	writeSet := map[string]bool{}
	for _, step := range txn.Steps {
		result, written, err := executeStep(s, step)
		if err != nil {
			// Atomicity: any step's error rolls back everything applied.
			s.docs = snapshot
			s.scheduledJobs = schedulesSnapshot
			return nil, err
		}
		results = append(results, result)
		for _, t := range written {
			writeSet[t] = true
		}
	}
	notifySubs(s, writeSet)
	return results, nil
}

func snapshotDocs(s *Store) map[rowKey]*StoredRow {
	out := make(map[rowKey]*StoredRow, len(s.docs))
	for k, row := range s.docs {
		cp := *row
		cp.Doc = cloneObject(row.Doc)
		out[k] = &cp
	}
	return out
}

func cloneSchedules(jobs []*ScheduledJob) []*ScheduledJob {
	if jobs == nil {
		return nil
	}
	out := make([]*ScheduledJob, len(jobs))
	for i, j := range jobs {
		cp := *j
		out[i] = &cp
	}
	return out
}

// notifySubs re-runs each live subscriber whose table is in the write-set
// and QUEUES its callback iff the canonical result changed — the caller
// flushes via takePendingFires after releasing s.mu (rust fires outside the
// borrow; a re-entering callback would deadlock the non-reentrant mutex).
// Query errors are suppressed (a failing subscriber query must not abort the
// write).
func notifySubs(s *Store, writeSet map[string]bool) {
	var live []*storeSubscription
	for _, sub := range s.subscribers {
		if !sub.alive.get() {
			continue
		}
		live = append(live, sub)
		if !writeSet[sub.Table] {
			continue
		}
		next, err := evalQueryLocked(s, sub.Query)
		if err != nil {
			continue
		}
		nextCanon := diffCanonical(next, &sub.Query)
		if sub.hasLast && sub.last == nextCanon {
			continue
		}
		sub.last = nextCanon
		sub.hasLast = true
		s.pendingFires = append(s.pendingFires, notifyFire{callback: sub.Callback, value: next})
	}
	s.subscribers = live
}

// diffCanonical is the push-decision form: the plain canonical for an
// unprojected query; for a projected query the volatile _version is stripped
// from every doc before comparing.
func diffCanonical(result wire.JSONValue, q *wire.Query) string {
	if q.Fields == nil {
		return canonical(result)
	}
	stripped := cloneValue(result)
	mapResultDocs(q, stripped, func(doc wire.Object) {
		delete(doc, "_version")
	})
	return canonical(stripped)
}

// executeStep runs one step and reports every table it wrote (a cascading
// delete can write several) — the notify fan-out is table-keyed.
func executeStep(s *Store, step wire.Step) (wire.StepResult, []string, error) {
	switch t := step.(type) {
	case wire.StepInsert:
		tableDef, err := requireTable(s, t.Table)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		doc, err := wireObject(t.Doc)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		id, err := doInsert(s, t.Table, tableDef, doc)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		return wire.StepResult{ID: &id}, []string{t.Table}, nil
	case wire.StepPatch:
		tableDef, err := requireTable(s, t.Table)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		fields, err := wireObject(t.Fields)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		if err := doPatch(s, tableDef, t.Table, t.ID, fields); err != nil {
			return wire.StepResult{}, nil, err
		}
		return wire.StepResult{}, []string{t.Table}, nil
	case wire.StepAdjustCounter:
		return executeAdjustCounter(s, t)
	case wire.StepReplace:
		tableDef, err := requireTable(s, t.Table)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		doc, err := wireObject(t.Doc)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		if err := doReplace(s, tableDef, t.Table, t.ID, doc); err != nil {
			return wire.StepResult{}, nil, err
		}
		return wire.StepResult{}, []string{t.Table}, nil
	case wire.StepDelete:
		tableDef, err := requireTable(s, t.Table)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		var touched []string
		if tableDef.SoftDelete {
			if err := doSoftDelete(s, t.Table, t.ID); err != nil {
				return wire.StepResult{}, nil, err
			}
			touched = append(touched, t.Table)
		} else {
			visited := map[rowKey]bool{}
			cascadeRows := 0
			if err := deleteRowCascade(s, t.Table, t.ID, visited, &cascadeRows, false, &touched); err != nil {
				return wire.StepResult{}, nil, err
			}
		}
		return wire.StepResult{}, touched, nil
	case wire.StepUndelete:
		tableDef, err := requireTable(s, t.Table)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		if err := doUndelete(s, tableDef, t.Table, t.ID); err != nil {
			return wire.StepResult{}, nil, err
		}
		return wire.StepResult{}, []string{t.Table}, nil
	case wire.StepExpectVersion:
		if _, err := requireTable(s, t.Table); err != nil {
			return wire.StepResult{}, nil, err
		}
		if err := doExpectVersion(s, t.Table, t.ID, t.Version); err != nil {
			return wire.StepResult{}, nil, err
		}
		return wire.StepResult{}, nil, nil
	case wire.StepExpectAbsent:
		tableDef, err := requireTable(s, t.Table)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		rows, err := eqLookup(s, tableDef, t.Table, t.Index, t.Eq)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		if len(rows) > 0 {
			return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodePreconditionFailed,
				"index '"+t.Index+"' already has a matching document")
		}
		return wire.StepResult{}, nil, nil
	case wire.StepUpsert:
		return executeUpsert(s, t)
	case wire.StepPatchByQuery:
		tableDef, err := requireTable(s, t.Table)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		if t.Limit != nil && *t.Limit < 0 {
			return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeBadRequest,
				"limit must be >= 0")
		}
		patch, err := wireObject(t.Patch)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		patched, truncated, err := patchByQuery(s, tableDef, t.Table, t.Filter, patch, t.Limit)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		return wire.StepResult{Patched: i64Ptr(int64(patched)), Truncated: boolPtr(truncated)}, []string{t.Table}, nil
	case wire.StepDeleteByQuery:
		tableDef, err := requireTable(s, t.Table)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		if t.Limit != nil && *t.Limit < 0 {
			return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeBadRequest,
				"limit must be >= 0")
		}
		deleted, truncated, touched, err := deleteByQuery(s, tableDef, t.Table, t.Filter, t.Limit)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		return wire.StepResult{Deleted: i64Ptr(int64(deleted)), Truncated: boolPtr(truncated)}, touched, nil
	case wire.StepSchedule:
		id, err := scheduleJob(s, t.Txn, t.When, t.External != nil && *t.External)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		return wire.StepResult{ScheduleID: &id}, nil, nil
	case wire.StepCancelSchedule:
		cancelled := cancelScheduleLocked(s, t.ID) == nil
		return wire.StepResult{Cancelled: boolPtr(cancelled)}, nil, nil
	case wire.StepStartWorkflow, wire.StepCancelWorkflow:
		// FM-29: the workflow engine is server-pinned and not modeled here.
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeInternal,
			"workflow steps are not supported by the in-memory harness")
	default:
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeInternal, "unknown step kind")
	}
}

// wireObject narrows a JSONValue to an object, BAD_REQUEST otherwise.
func wireObject(v wire.JSONValue) (wire.Object, error) {
	obj, ok := v.(wire.Object)
	if !ok {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest, "expected a JSON object")
	}
	return obj, nil
}

func i64Ptr(v int64) *int64       { return &v }
func boolPtr(v bool) *bool        { return &v }
func stringPtr2(s string) *string { return &s }

// stampAutoIncrement stamps the table's autoIncrementField with the next
// counter value (lazy sequence positioned past the stored max; the counter
// is monotonic — a rolled-back txn does not reclaim its number).
func stampAutoIncrement(s *Store, tableName string, tableDef *TableDef, doc wire.Object) wire.Object {
	if tableDef.AutoIncrementField == nil {
		return doc
	}
	field := *tableDef.AutoIncrementField
	next, ok := s.autoIncrementCounters[tableName]
	if ok {
		next++
		s.autoIncrementCounters[tableName] = next
	} else {
		max := int64(0)
		for key, row := range s.docs {
			if key.Table != tableName {
				continue
			}
			if str, ok := row.Doc[field].(wire.String); ok {
				if v, ok := parseI64(string(str)); ok && v > max {
					max = v
				}
			}
		}
		next = max + 1
		s.autoIncrementCounters[tableName] = next
	}
	out := doc
	if out == nil {
		out = wire.Object{}
	}
	out[field] = wire.String(formatI64(next))
	return out
}

// doInsert inserts a new doc: the stamp chain runs ttl → updatedAt →
// defaults → autoIncrement → computed, then validate, strip, unique-check,
// mint id.
func doInsert(s *Store, tableName string, tableDef *TableDef, doc wire.Object) (string, error) {
	now := s.now()
	stamped := stampTTLDefault(tableDef, doc, now)
	stamped = stampUpdatedAt(tableDef, stamped, now)
	stamped = applyDefaults(tableDef, stamped)
	stamped = stampAutoIncrement(s, tableName, tableDef, stamped)
	stamped, err := stampComputed(tableDef, stamped, now)
	if err != nil {
		return "", err
	}
	if err := validateDoc(tableDef, stamped); err != nil {
		return "", err
	}
	stored := stripUnsetOptionals(tableDef, stamped)
	if err := checkUniqueIndexes(s, tableDef, tableName, stored, ""); err != nil {
		return "", err
	}
	id := s.newIDLocked()
	s.docs[rowKey{Table: tableName, ID: id}] = &StoredRow{
		ID:        id,
		Doc:       stored,
		Version:   1,
		CreatedAt: s.now(),
	}
	return id, nil
}

// doPatch patches an existing doc, bumping _version; a soft-deleted row is
// absent to the lookup.
func doPatch(s *Store, tableDef *TableDef, tableName, id string, fields wire.Object) error {
	row, ok := s.docs[rowKey{Table: tableName, ID: id}]
	if !ok || row.DeletedAt != nil {
		return rtdberrors.New(rtdberrors.CodeNotFound, "document '"+id+"' not found")
	}
	now := s.now()
	stampedFields := stampUpdatedAt(tableDef, fields, now)
	merged, err := applyPatch(tableDef, row.Doc, stampedFields, now)
	if err != nil {
		return err
	}
	return doUpdate(s, tableDef, tableName, id, merged)
}

// doReplace replaces a doc whole: defaults but no ttl stamp, updatedAt
// stamp, autoIncrement preserve-or-reject, computed re-derivation, unique
// check, version bump.
func doReplace(s *Store, tableDef *TableDef, tableName, id string, doc wire.Object) error {
	key := rowKey{Table: tableName, ID: id}
	row, ok := s.docs[key]
	if !ok || row.DeletedAt != nil {
		return rtdberrors.New(rtdberrors.CodeNotFound, "document '"+id+"' not found")
	}
	prevDoc := row.Doc
	stamped := applyDefaults(tableDef, doc)
	stamped = stampUpdatedAt(tableDef, stamped, s.now())
	if tableDef.AutoIncrementField != nil {
		auto := *tableDef.AutoIncrementField
		if prev, had := prevDoc[auto]; had {
			value, present := stamped[auto]
			switch {
			case !present || isNullValue(value):
				stamped[auto] = prev
			case !jsonEq(value, prev):
				return rtdberrors.New(rtdberrors.CodeBadRequest,
					"autoIncrementField '"+auto+"' cannot be changed")
			}
		}
	}
	for name := range tableDef.Computed {
		delete(stamped, name)
	}
	stamped, err := stampComputed(tableDef, stamped, s.now())
	if err != nil {
		return err
	}
	if err := validateDoc(tableDef, stamped); err != nil {
		return err
	}
	stored := stripUnsetOptionals(tableDef, stamped)
	if err := checkUniqueIndexes(s, tableDef, tableName, stored, id); err != nil {
		return err
	}
	row.Doc = stored
	row.Version++
	return nil
}

// doUpdate is the shared write-back: unique-check then write + version bump.
func doUpdate(s *Store, tableDef *TableDef, tableName, id string, merged wire.Object) error {
	if err := checkUniqueIndexes(s, tableDef, tableName, merged, id); err != nil {
		return err
	}
	if row, ok := s.docs[rowKey{Table: tableName, ID: id}]; ok {
		row.Doc = merged
		row.Version++
	}
	return nil
}

// doSoftDelete stamps the row soft-deleted (deleted_at + version bump); an
// absent or already-stamped row is NOT_FOUND.
func doSoftDelete(s *Store, tableName, id string) error {
	row, ok := s.docs[rowKey{Table: tableName, ID: id}]
	if !ok || row.DeletedAt != nil {
		return rtdberrors.New(rtdberrors.CodeNotFound, "document '"+id+"' not found")
	}
	now := s.now()
	row.DeletedAt = &now
	row.Version++
	return nil
}

// doUndelete restores a soft-deleted row: idempotent on a live row,
// NOT_FOUND when absent, BAD_REQUEST without the softDelete declaration, and
// the unique indexes re-apply on restore.
func doUndelete(s *Store, tableDef *TableDef, tableName, id string) error {
	if !tableDef.SoftDelete {
		return rtdberrors.New(rtdberrors.CodeBadRequest,
			"table '"+tableName+"' does not declare softDelete")
	}
	row, ok := s.docs[rowKey{Table: tableName, ID: id}]
	if !ok {
		return rtdberrors.New(rtdberrors.CodeNotFound, "document '"+id+"' not found")
	}
	if row.DeletedAt == nil {
		return nil
	}
	if err := checkUniqueIndexes(s, tableDef, tableName, row.Doc, id); err != nil {
		return err
	}
	row.DeletedAt = nil
	row.Version++
	return nil
}

// executeAdjustCounter runs the counter-adjust step: safe-integer bounds,
// number-or-optional-number field, not computed/autoIncrement/updatedAt,
// expected-field preconditions, then patch the new value through doPatch.
func executeAdjustCounter(s *Store, t wire.StepAdjustCounter) (wire.StepResult, []string, error) {
	tableDef, err := requireTable(s, t.Table)
	if err != nil {
		return wire.StepResult{}, nil, err
	}
	const maxSafe = int64(9_007_199_254_740_991)
	if absI64(t.Delta) > uint64(maxSafe) ||
		(t.Min != nil && absI64(*t.Min) > uint64(maxSafe)) ||
		(t.Max != nil && absI64(*t.Max) > uint64(maxSafe)) ||
		(t.Min != nil && t.Max != nil && *t.Min > *t.Max) {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"counter delta and bounds must be safe integers with min <= max")
	}
	fty, declared := tableDef.Fields[t.Field]
	if !declared {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeSchemaViolation,
			"unknown field '"+t.Field+"'")
	}
	if _, computed := tableDef.Computed[t.Field]; computed {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"computed field '"+t.Field+"' cannot be adjusted")
	}
	if tableDef.AutoIncrementField != nil && *tableDef.AutoIncrementField == t.Field {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"autoIncrementField '"+t.Field+"' cannot be changed")
	}
	if tableDef.UpdatedAtField != nil && *tableDef.UpdatedAtField == t.Field {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"updatedAtField '"+t.Field+"' cannot be adjusted")
	}
	numeric := fty.Kind == "number" ||
		(fty.Kind == "optional" && fty.Inner != nil && fty.Inner.Kind == "number")
	if !numeric {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeSchemaViolation,
			"counter field '"+t.Field+"' must be number or optional(number)")
	}
	row, ok := s.docs[rowKey{Table: t.Table, ID: t.ID}]
	if !ok || row.DeletedAt != nil {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeNotFound,
			"document '"+t.ID+"' not found")
	}
	if expected, ok := t.Expected.(wire.Object); ok {
		for fieldName, wanted := range expected {
			if _, declared := tableDef.Fields[fieldName]; !declared {
				return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeSchemaViolation,
					"unknown expected field '"+fieldName+"'")
			}
			actual, present := row.Doc[fieldName]
			if !present || !expectedJSONEq(actual, wanted) {
				return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodePreconditionFailed,
					"expected field '"+fieldName+"' did not match")
			}
		}
	}
	inner := fty
	if inner.Kind == "optional" && inner.Inner != nil {
		inner = *inner.Inner
	}
	if inner.Kind != "number" {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"counter field '"+t.Field+"' must contain a safe integer")
	}
	currentValue, present := row.Doc[t.Field]
	if !present {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"counter field '"+t.Field+"' must contain a safe integer")
	}
	num, ok := currentValue.(wire.Number)
	if !ok {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"counter field '"+t.Field+"' must contain a safe integer")
	}
	f := jsonNumberF64(num)
	if isInfOrNaN(f) || f != float64(int64(f)) || f < -float64(maxSafe) || f > float64(maxSafe) {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"counter field '"+t.Field+"' must contain a safe integer")
	}
	current := int64(f)
	next := current + t.Delta
	if next < 0 && current > 0 && t.Delta < 0 && next > current {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"counter result is outside the safe integer range")
	}
	if absI64(next) > uint64(maxSafe) {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"counter result is outside the safe integer range")
	}
	if (t.Min != nil && next < *t.Min) || (t.Max != nil && next > *t.Max) {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodePreconditionFailed,
			"counter result is outside the configured bounds")
	}
	fields := wire.Object{t.Field: wire.Number(formatI64(next))}
	if err := doPatch(s, tableDef, t.Table, t.ID, fields); err != nil {
		return wire.StepResult{}, nil, err
	}
	return wire.StepResult{}, []string{t.Table}, nil
}

func absI64(v int64) uint64 {
	if v < 0 {
		return uint64(-(v + 1)) + 1
	}
	return uint64(v)
}

// expectedJSONEq is the counter-expectation equality: integer-valued numbers
// compare exactly as integers across spellings; other numbers as doubles.
func expectedJSONEq(left, right wire.JSONValue) bool {
	ln, lok := left.(wire.Number)
	rn, rok := right.(wire.Number)
	if lok && rok {
		li, lokInt := parseI64(string(ln))
		ri, rokInt := parseI64(string(rn))
		if lokInt && rokInt {
			return li == ri
		}
		lf, lokF := parseIntValued(string(ln))
		rf, rokF := parseIntValued(string(rn))
		if lokInt == rokInt {
			if lokInt {
				return false
			}
			if lokF && rokF {
				return lf == rf
			}
		}
		return jsonNumberF64(ln) == jsonNumberF64(rn)
	}
	return jsonEq(left, right)
}

func parseIntValued(s string) (float64, bool) {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// executeUpsert runs the insert-or-patch keyed by a full-arity index eq
// match; the update branch restamps updatedAtField into the patch.
func executeUpsert(s *Store, t wire.StepUpsert) (wire.StepResult, []string, error) {
	tableDef, err := requireTable(s, t.Table)
	if err != nil {
		return wire.StepResult{}, nil, err
	}
	rows, err := eqLookup(s, tableDef, t.Table, t.Index, t.Eq)
	if err != nil {
		return wire.StepResult{}, nil, err
	}
	if len(rows) > 1 {
		return wire.StepResult{}, nil, rtdberrors.New(rtdberrors.CodePreconditionFailed,
			"upsert matched multiple documents")
	}
	if len(rows) == 1 {
		row := rows[0]
		patch, err := wireObject(t.Patch)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		now := s.now()
		patch = stampUpdatedAt(tableDef, patch, now)
		merged, err := applyPatch(tableDef, row.Doc, patch, now)
		if err != nil {
			return wire.StepResult{}, nil, err
		}
		if err := doUpdate(s, tableDef, t.Table, row.ID, merged); err != nil {
			return wire.StepResult{}, nil, err
		}
		id := row.ID
		ins := false
		return wire.StepResult{ID: &id, Inserted: &ins}, []string{t.Table}, nil
	}
	insert, err := wireObject(t.Insert)
	if err != nil {
		return wire.StepResult{}, nil, err
	}
	id, err := doInsert(s, t.Table, tableDef, insert)
	if err != nil {
		return wire.StepResult{}, nil, err
	}
	ins := true
	return wire.StepResult{ID: &id, Inserted: &ins}, []string{t.Table}, nil
}

// deleteRowCascade deletes one row expanding the app-level onDelete rules:
// children first (restrict conflicts, cascade recurses, setNull patches),
// parent last; softDelete tables stamp and stop unless forceHard; visited
// guards cycles and cascadeRows is the shared budget.
func deleteRowCascade(s *Store, tableName, id string, visited map[rowKey]bool, cascadeRows *int, forceHard bool, touched *[]string) error {
	tableDef, err := requireTable(s, tableName)
	if err != nil {
		return err
	}
	key := rowKey{Table: tableName, ID: id}
	if visited[key] {
		return nil
	}
	visited[key] = true
	if *cascadeRows >= maxCascadeRows {
		return rtdberrors.New(rtdberrors.CodeConflict,
			"onDelete cascade exceeds the limit of "+formatI64(maxCascadeRows)+" rows")
	}
	*cascadeRows++
	if tableDef.SoftDelete && !forceHard {
		if err := doSoftDelete(s, tableName, id); err != nil {
			return err
		}
		*touched = append(*touched, tableName)
		return nil
	}
	if s.schema == nil {
		return rtdberrors.New(rtdberrors.CodeInternal, "schema not pushed")
	}
	type childRef struct {
		table string
		def   *TableDef
		field string
		query func(FieldType, string) (OnDeleteAction, bool)
	}
	// Deterministic order: sort child table names (the schema map is Go
	// unordered; rust walks the BTreeMap).
	var childTables []string
	for name := range s.schema.Tables {
		childTables = append(childTables, name)
	}
	sort.Strings(childTables)
	var plan []struct {
		childTable string
		childDef   *TableDef
		fieldName  string
		action     OnDeleteAction
	}
	for _, childTable := range childTables {
		childDef := s.schema.Tables[childTable]
		for fieldName, fieldTy := range childDef.Fields {
			if action, ok := onDeleteRef(fieldTy, tableName); ok {
				plan = append(plan, struct {
					childTable string
					childDef   *TableDef
					fieldName  string
					action     OnDeleteAction
				}{childTable, childDef, fieldName, action})
			}
		}
	}
	for _, p := range plan {
		switch p.action {
		case OnDeleteRestrict:
			hits := visibleChildIDs(s, p.childTable, p.fieldName, id, true)
			if len(hits) > 0 {
				return rtdberrors.New(rtdberrors.CodeConflict,
					"cannot delete '"+tableName+"': '"+p.childTable+"."+p.fieldName+
						"' is referenced by document '"+hits[0]+"'")
			}
		case OnDeleteCascade:
			for _, childID := range visibleChildIDs(s, p.childTable, p.fieldName, id, false) {
				if err := deleteRowCascade(s, p.childTable, childID, visited, cascadeRows, forceHard, touched); err != nil {
					return err
				}
			}
		case OnDeleteSetNull:
			for _, childID := range visibleChildIDs(s, p.childTable, p.fieldName, id, false) {
				if *cascadeRows >= maxCascadeRows {
					return rtdberrors.New(rtdberrors.CodeConflict,
						"onDelete cascade exceeds the limit of "+formatI64(maxCascadeRows)+" rows")
				}
				*cascadeRows++
				fields := wire.Object{p.fieldName: wire.Null{}}
				if err := doPatch(s, p.childDef, p.childTable, childID, fields); err != nil {
					return err
				}
				*touched = append(*touched, p.childTable)
			}
		}
	}
	if err := doDeleteHard(s, tableName, id); err != nil {
		return err
	}
	*touched = append(*touched, tableName)
	return nil
}

func doDeleteHard(s *Store, tableName, id string) error {
	key := rowKey{Table: tableName, ID: id}
	if _, ok := s.docs[key]; !ok {
		return rtdberrors.New(rtdberrors.CodeNotFound, "document '"+id+"' not found")
	}
	delete(s.docs, key)
	return nil
}

// visibleChildIDs lists live child rows referencing parentID; limitOne
// probes a single hit, otherwise the fetch is capped at the cascade budget
// plus one.
func visibleChildIDs(s *Store, childTable, field, parentID string, limitOne bool) []string {
	cap := maxCascadeRows + 1
	if limitOne {
		cap = 1
	}
	var ids []string
	// Deterministic order: ids ascending (the server's scans order by id).
	var keys []rowKey
	for key, row := range s.docs {
		if key.Table != childTable || row.DeletedAt != nil {
			continue
		}
		if str, ok := row.Doc[field].(wire.String); ok && string(str) == parentID {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(a, b int) bool { return keys[a].ID < keys[b].ID })
	if len(keys) > cap {
		keys = keys[:cap]
	}
	for _, k := range keys {
		ids = append(ids, k.ID)
	}
	return ids
}

// scanIDsByFilter scans for matching live rows ordered by (created_at, id) —
// the server's by-query order — returning at most limit ids plus the
// truncation flag. The by-query filter context is the one that admits
// olderThan (clock read once per step).
func scanIDsByFilter(s *Store, tableDef *TableDef, tableName string, filter wire.FilterExpr, limitOpt *int) ([]string, bool, error) {
	if err := validateByQueryFilter(filter, tableDef); err != nil {
		return nil, false, err
	}
	now := s.now()
	limit := maxByQueryRows
	if limitOpt != nil && *limitOpt < limit {
		limit = *limitOpt
	}
	type rowStamp struct {
		createdAt int64
		id        string
	}
	var matching []rowStamp
	for key, row := range s.docs {
		if key.Table != tableName || row.DeletedAt != nil {
			continue
		}
		if matchesFilterAt(filter, row.Doc, tableDef.Fields, now) {
			matching = append(matching, rowStamp{createdAt: row.CreatedAt, id: row.ID})
		}
	}
	sort.SliceStable(matching, func(a, b int) bool {
		if matching[a].createdAt != matching[b].createdAt {
			return matching[a].createdAt < matching[b].createdAt
		}
		return matching[a].id < matching[b].id
	})
	truncated := len(matching) > limit
	if truncated {
		matching = matching[:limit]
	}
	ids := make([]string, 0, len(matching))
	for _, m := range matching {
		ids = append(ids, m.id)
	}
	return ids, truncated, nil
}

func patchByQuery(s *Store, tableDef *TableDef, tableName string, filter wire.FilterExpr, patch wire.Object, limitOpt *int) (int, bool, error) {
	ids, truncated, err := scanIDsByFilter(s, tableDef, tableName, filter, limitOpt)
	if err != nil {
		return 0, false, err
	}
	for _, id := range ids {
		if err := doPatch(s, tableDef, tableName, id, patch); err != nil {
			return 0, false, err
		}
	}
	return len(ids), truncated, nil
}

func deleteByQuery(s *Store, tableDef *TableDef, tableName string, filter wire.FilterExpr, limitOpt *int) (int, bool, []string, error) {
	ids, truncated, err := scanIDsByFilter(s, tableDef, tableName, filter, limitOpt)
	if err != nil {
		return 0, false, nil, err
	}
	deleted := len(ids)
	var touched []string
	visited := map[rowKey]bool{}
	cascadeRows := 0
	for _, id := range ids {
		if err := deleteRowCascade(s, tableName, id, visited, &cascadeRows, false, &touched); err != nil {
			return 0, false, nil, err
		}
	}
	return deleted, truncated, touched, nil
}

// doExpectVersion asserts a doc's current _version (a soft-deleted row is
// absent — NOT_FOUND).
func doExpectVersion(s *Store, tableName, id string, expected int64) error {
	row, ok := s.docs[rowKey{Table: tableName, ID: id}]
	if !ok || row.DeletedAt != nil {
		return rtdberrors.New(rtdberrors.CodeNotFound, "document '"+id+"' not found")
	}
	if row.Version != expected {
		return rtdberrors.New(rtdberrors.CodePreconditionFailed,
			"version mismatch: expected "+formatI64(expected)+", actual "+formatI64(row.Version))
	}
	return nil
}

// checkUniqueIndexes enforces unique indexes on a candidate write: no other
// live row satisfying the partial predicate may share the candidate's key
// values; NULL/absent key fields disable the constraint (Postgres NULLs are
// distinct); soft-deleted rows are excluded.
func checkUniqueIndexes(s *Store, tableDef *TableDef, tableName string, candidateDoc wire.Object, excludeID string) error {
	for i := range tableDef.Indexes {
		index := &tableDef.Indexes[i]
		if !index.Unique {
			continue
		}
		if index.WhereClause != nil && !matchesFilter(index.WhereClause, candidateDoc, tableDef.Fields) {
			continue
		}
		candidateKey := collectIndexKey(index.Fields, candidateDoc)
		if candidateKey == nil {
			continue
		}
		for key, row := range s.docs {
			if key.Table != tableName {
				continue
			}
			if excludeID != "" && row.ID == excludeID {
				continue
			}
			if row.DeletedAt != nil {
				continue
			}
			if index.WhereClause != nil && !matchesFilter(index.WhereClause, row.Doc, tableDef.Fields) {
				continue
			}
			rowKeyVals := collectIndexKey(index.Fields, row.Doc)
			if rowKeyVals == nil {
				continue
			}
			equal := true
			for j := range candidateKey {
				if !jsonEq(candidateKey[j], rowKeyVals[j]) {
					equal = false
					break
				}
			}
			if equal {
				return rtdberrors.New(rtdberrors.CodeConflict,
					"unique index '"+index.Name+"' violated")
			}
		}
	}
	return nil
}

// collectIndexKey gathers the index-key values positionally; nil when ANY
// indexed field is absent or null (no collision).
func collectIndexKey(fields []string, doc wire.Object) []wire.JSONValue {
	key := make([]wire.JSONValue, 0, len(fields))
	for _, field := range fields {
		v, present := doc[field]
		if !present || isNullValue(v) {
			return nil
		}
		key = append(key, v)
	}
	return key
}

// eqLookup is the full-arity index eq lookup shared by expectAbsent and
// upsert: soft-deleted rows absent, null index fields never match.
func eqLookup(s *Store, tableDef *TableDef, tableName, indexName string, eq []wire.JSONValue) ([]*StoredRow, error) {
	index, err := requireIndex(tableDef, indexName)
	if err != nil {
		return nil, err
	}
	if len(eq) != len(index.Fields) {
		return nil, rtdberrors.New(rtdberrors.CodeBadRequest,
			"index '"+indexName+"' expects "+formatI64(int64(len(index.Fields)))+
				" eq value(s), got "+formatI64(int64(len(eq))))
	}
	typed := make([]wire.JSONValue, len(eq))
	for i, value := range eq {
		tv, err := coerceIndexValue(tableDef, index.Fields[i], value)
		if err != nil {
			return nil, err
		}
		typed[i] = tv
	}
	var matches []*StoredRow
	for key, row := range s.docs {
		if key.Table != tableName || row.DeletedAt != nil {
			continue
		}
		allMatch := true
		for i, field := range index.Fields {
			v, present := row.Doc[field]
			if !present || isNullValue(v) || !jsonEq(v, typed[i]) {
				allMatch = false
				break
			}
		}
		if allMatch {
			cp := *row
			matches = append(matches, &cp)
		}
	}
	return matches, nil
}

// scheduleJob enqueues a scheduled txn: interval validates its everyMs, the
// kind derives from the when variant, and dueAt follows the engine's
// due-time math (cron re-arms on the fixed step).
func scheduleJob(s *Store, txn wire.Transaction, when wire.ScheduleWhen, external bool) (string, error) {
	var everyMs *int64
	switch w := when.(type) {
	case wire.WhenInterval:
		if w.EveryMs <= 0 {
			return "", rtdberrors.New(rtdberrors.CodeBadRequest, "everyMs must be positive")
		}
		if w.EveryMs > maxEveryMs {
			return "", rtdberrors.New(rtdberrors.CodeBadRequest,
				"everyMs must be at most "+formatI64(maxEveryMs))
		}
		v := w.EveryMs
		everyMs = &v
	}
	id := s.newIDLocked()
	now := s.now()
	kind := ScheduleKindOneShot
	var cron *string
	switch w := when.(type) {
	case wire.WhenCron:
		kind = ScheduleKindCron
		expr := w.Expr
		cron = &expr
	case wire.WhenInterval:
		kind = ScheduleKindInterval
	}
	dueAt := dueAtFor(when, now)
	s.scheduledJobs = append(s.scheduledJobs, &ScheduledJob{
		ID:        id,
		Kind:      kind,
		Txn:       txn,
		DueAt:     dueAt,
		Cron:      cron,
		EveryMs:   everyMs,
		Status:    ScheduleStatusPending,
		CreatedAt: now,
		External:  external,
	})
	return id, nil
}

func dueAtFor(when wire.ScheduleWhen, now int64) int64 {
	switch w := when.(type) {
	case wire.WhenAfterMs:
		return now + w.Ms
	case wire.WhenRunAt:
		return w.Ms
	case wire.WhenCron:
		return now + cronStepMs
	case wire.WhenInterval:
		return now + w.EveryMs
	default:
		return now
	}
}

// cancelScheduleLocked removes the job; NOT_FOUND when the id is missing.
func cancelScheduleLocked(s *Store, id string) error {
	for i, j := range s.scheduledJobs {
		if j.ID == id {
			s.scheduledJobs = append(s.scheduledJobs[:i], s.scheduledJobs[i+1:]...)
			return nil
		}
	}
	return rtdberrors.New(rtdberrors.CodeNotFound, "schedule '"+id+"' not found")
}
