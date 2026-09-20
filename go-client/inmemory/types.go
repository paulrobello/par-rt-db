// Engine data shapes shared by the store and the later task surfaces:
// scheduled jobs, reactive subscriptions, and stored blobs. The struct
// shapes mirror rust in_memory/mod.rs; the behaviors (tick, subscribe,
// upload) land with the scheduler/subscription/storage tasks.
package inmemory

import (
	"sync"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// ScheduleKind is one-shot, cron, or interval.
type ScheduleKind string

const (
	ScheduleKindOneShot  ScheduleKind = "one_shot"
	ScheduleKindCron     ScheduleKind = "cron"
	ScheduleKindInterval ScheduleKind = "interval"
)

// ScheduleStatus is pending / paused / running / done / cancelled.
type ScheduleStatus string

const (
	ScheduleStatusPending   ScheduleStatus = "pending"
	ScheduleStatusPaused    ScheduleStatus = "paused"
	ScheduleStatusRunning   ScheduleStatus = "running"
	ScheduleStatusDone      ScheduleStatus = "done"
	ScheduleStatusCancelled ScheduleStatus = "cancelled"
)

// ScheduledJob is a stored scheduled job. Tick fires due non-paused jobs by
// applying the txn through the same atomic path as Mutate.
type ScheduledJob struct {
	ID         string
	Kind       ScheduleKind
	Txn        wire.Transaction
	DueAt      int64
	Cron       *string
	Tz         *string
	EveryMs    *int64
	Status     ScheduleStatus
	CreatedAt  int64
	FiredCount int64
	LastError  *string
	// External jobs are never internally executed; claimed by application
	// workers with a fencing token.
	External bool
}

// storeSubscription is the inner state of one reactive subscription: alive is
// cleared by unsubscribe, last holds the canonicalized previous result so
// only real changes re-fire.
type storeSubscription struct {
	Query    wire.Query
	Table    string
	alive    *syncFlag
	Callback func(wire.JSONValue)
	last     string
	hasLast  bool
}

// syncFlag is a tiny atomic bool with a mutex (avoids importing sync/atomic
// semantics into the harness).
type syncFlag struct {
	mu sync.Mutex
	v  bool
}

func (f *syncFlag) get() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.v
}

func (f *syncFlag) set(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.v = v
}

// swap sets the flag and reports the PREVIOUS value (a close-once guard).
func (f *syncFlag) swap(v bool) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	prev := f.v
	f.v = v
	return prev
}

// StoredBlob is the storage stub's per-id record.
type StoredBlob struct {
	Bytes       []byte
	ContentType *string
	CreatedAt   int64
	SHA256      string
}
