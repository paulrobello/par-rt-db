// The scheduler clock and TTL reaper — the Go port of rust in_memory/mod.rs's
// tick/reap_ttl. Tick is the test-only deterministic knob; the corpus runner
// never ticks.
package inmemory

import (
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// ScheduleStatusError marks a job whose last fire failed.
const ScheduleStatusError ScheduleStatus = "error"

// Tick fires due non-paused jobs (external jobs are never internally
// executed) by applying their txn through the same atomic path as ApplyTxn,
// then reaps expired TTL docs. Returns the number of docs reaped.
func (s *Store) Tick(now int64) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := 0
	for i < len(s.scheduledJobs) {
		job := s.scheduledJobs[i]
		if job.External || job.Status == ScheduleStatusPaused || job.DueAt > now {
			i++
			continue
		}
		txn := job.Txn
		jobID := job.ID
		kind := job.Kind
		everyMs := job.EveryMs
		if _, err := executeTransaction(s, txn); err != nil {
			if j := s.findJob(jobID); j != nil {
				j.Status = ScheduleStatusError
				msg := err.Error()
				if re, ok := err.(*rtdberrors.RtDbError); ok {
					msg = re.Message
				}
				j.LastError = &msg
				if kind == ScheduleKindCron {
					j.DueAt = now + cronStepMs
				}
				if kind == ScheduleKindInterval && everyMs != nil {
					j.DueAt = now + *everyMs
				}
			}
			i++
			continue
		}
		if j := s.findJob(jobID); j != nil {
			j.FiredCount++
			switch kind {
			case ScheduleKindOneShot:
				s.removeJob(jobID)
				continue // don't bump i; the next job shifted into this index
			case ScheduleKindCron:
				j.DueAt = now + cronStepMs
				j.Status = ScheduleStatusPending
			case ScheduleKindInterval:
				if everyMs != nil {
					// Re-arm from each actual fire time: windows missed
					// during the fire's latency are skipped, not backfilled.
					j.DueAt = now + *everyMs
					j.Status = ScheduleStatusPending
				} else {
					j.Status = ScheduleStatusError
					msg := "interval job missing everyMs"
					j.LastError = &msg
				}
			}
		}
		i++
	}
	return s.reapTTL(now)
}

func (s *Store) findJob(id string) *ScheduledJob {
	for _, j := range s.scheduledJobs {
		if j.ID == id {
			return j
		}
	}
	return nil
}

func (s *Store) removeJob(id string) {
	out := s.scheduledJobs[:0]
	for _, j := range s.scheduledJobs {
		if j.ID != id {
			out = append(out, j)
		}
	}
	s.scheduledJobs = out
}

// reapTTL removes docs whose TTL field is a number strictly less than now,
// for every table declaring a ttl. A table with onDelete children reaps
// through the cascade with forceHard (TTL expiry is a real delete even on a
// softDelete table); a per-row failure skips that row (at-least-once).
func (s *Store) reapTTL(now int64) int {
	if s.schema == nil {
		return 0
	}
	var toRemove []rowKey
	cascadeTables := map[string]bool{}
	for tableName, tableDef := range s.tables {
		if tableDef.TTL == nil {
			continue
		}
		if hasOnDeleteChildren(s.schema, tableName) {
			cascadeTables[tableName] = true
		}
		ttlField := tableDef.TTL.Field
		for key, row := range s.docs {
			if key.Table != tableName {
				continue
			}
			if n, ok := row.Doc[ttlField].(wire.Number); ok {
				if jsonNumberF64(n) < float64(now) {
					toRemove = append(toRemove, key)
				}
			}
		}
	}
	byTable := map[string][]string{}
	for _, key := range toRemove {
		byTable[key.Table] = append(byTable[key.Table], key.ID)
	}
	removed := 0
	touched := map[string]bool{}
	for tableName, ids := range byTable {
		if cascadeTables[tableName] {
			visited := map[rowKey]bool{}
			for _, id := range ids {
				cascadeRows := 0
				var stepTouched []string
				if err := deleteRowCascade(s, tableName, id, visited, &cascadeRows, true, &stepTouched); err == nil {
					removed++
					for _, t := range stepTouched {
						touched[t] = true
					}
				}
			}
			continue
		}
		for _, id := range ids {
			key := rowKey{Table: tableName, ID: id}
			if _, ok := s.docs[key]; ok {
				delete(s.docs, key)
				removed++
				touched[tableName] = true
			}
		}
	}
	if len(touched) > 0 {
		notifySubs(s, touched)
	}
	return removed
}
