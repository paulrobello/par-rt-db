// The in-memory engine's state: document store, schema, id minting, and the
// schema-push entry point. Mirrors rust in_memory/mod.rs's InMemoryRtDbClient
// state (the per-table denormalized map, auto-increment counters, id
// counter) with Go maps.
package inmemory

import (
	"fmt"
	"sync"
	"time"

	"github.com/paulrobello/par-rt-db/go-client/wire"
)

// StoredRow is the stored row: the user doc plus its identity/history, kept
// separate so the system fields (_id/_creationTime/_version) are merged in
// only at read time — exactly as the server stores doc jsonb alongside
// id/created_at/version columns.
type StoredRow struct {
	ID        string
	Doc       wire.Object
	Version   int64
	CreatedAt int64
	// DeletedAt marks a soft-deleted row (invisible to every read and write
	// lookup, restorable via the undelete step); nil = live. Only a
	// softDelete table ever stamps.
	DeletedAt *int64
}

// rowKey addresses one document: (table, id).
type rowKey struct {
	Table string
	ID    string
}

// Store is the engine state. The zero value is not usable; construct with
// NewStore. All exported methods are safe for concurrent use (the harness is
// typically driven by a single test goroutine, but the mutex keeps the
// subscription callbacks and Ticker honest).
type Store struct {
	mu sync.Mutex

	now    func() int64
	random func() float64

	schema *SchemaDef
	tables map[string]*TableDef
	docs   map[rowKey]*StoredRow
	// autoIncrementCounters holds the LAST value handed out per table
	// (absent = never; the first stamp lazily initializes from the stored
	// max). Persists across additive schema pushes.
	autoIncrementCounters map[string]int64
	idCounter             uint64
	// mutID -> cached results; the idempotency short-circuit.
	idempotency map[string][]wire.StepResult
	// readOnly is the per-database read-only freeze (server
	// PATCH /admin/db/{db}/readonly). While set, ApplyTxn fails with
	// READ_ONLY after the idempotency-replay lookup; queries are unaffected.
	readOnly bool
	// scheduledJobs are the in-memory scheduled txns; tick drains due
	// non-paused entries.
	scheduledJobs []*ScheduledJob
	// subscribers re-run their query on writes to affected tables.
	subscribers []*storeSubscription
	// storage is the file-upload stub: per-id blobs.
	storage map[string]*StoredBlob
	// pendingFires buffers subscriber callbacks queued while the lock was
	// held; ApplyTxn/Tick flush them AFTER unlocking (rust fires outside
	// the borrow — a callback re-entering the store must not deadlock).
	pendingFires []notifyFire
}

// notifyFire is one deferred subscriber callback.
type notifyFire struct {
	callback func(wire.JSONValue)
	value    wire.JSONValue
}

// takePendingFires drains the deferred callback queue (caller flushes after
// releasing s.mu).
func (s *Store) takePendingFires() []notifyFire {
	fires := s.pendingFires
	s.pendingFires = nil
	return fires
}

// NewStore constructs the engine with the system clock and a constant 0.5
// random source. Tests needing deterministic ids/timestamps should inject
// both.
func NewStore() *Store {
	return &Store{
		now:                   defaultNow,
		random:                func() float64 { return 0.5 },
		tables:                map[string]*TableDef{},
		docs:                  map[rowKey]*StoredRow{},
		autoIncrementCounters: map[string]int64{},
		idempotency:           map[string][]wire.StepResult{},
		storage:               map[string]*StoredBlob{},
	}
}

// WithClock injects the epoch-millis clock for deterministic _creationTime
// and id minting.
func (s *Store) WithClock(f func() int64) *Store {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = f
	return s
}

// WithRandom injects the [0,1) RNG for deterministic id minting.
func (s *Store) WithRandom(f func() float64) *Store {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.random = f
	return s
}

// Now returns the engine clock (epoch millis).
func (s *Store) Now() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.now()
}

func defaultNow() int64 {
	return timeNowMillis()
}

// newIDLocked mints a uuidv7-shaped id: 12 hex chars of the low 48 bits of
// the epoch-millis timestamp, a constant 7 version nibble, then 19 random
// hex chars — 32 chars total. Caller holds s.mu.
func (s *Store) newIDLocked() string {
	ts := uint64(s.now()) & 0xFFFF_FFFF_FFFF
	rand := s.randomHexLocked(19)
	return fmt.Sprintf("%012x7%s", ts, rand)
}

// randomHexLocked draws count lowercase hex chars from the injected RNG.
// Caller holds s.mu.
func (s *Store) randomHexLocked(count int) string {
	out := make([]byte, count)
	for i := range out {
		digit := int(s.random()*16.0) & 0xF
		if digit < 10 {
			out[i] = byte('0' + digit)
		} else {
			out[i] = byte('a' + digit - 10)
		}
	}
	return string(out)
}

// mergeDoc layers the system fields (_id/_creationTime/_version) onto a
// stored row's doc — a fresh object every call.
func mergeDoc(row *StoredRow) wire.Object {
	out := cloneObject(row.Doc)
	out["_id"] = wire.String(row.ID)
	out["_creationTime"] = wire.Number(formatI64(row.CreatedAt))
	out["_version"] = wire.Number(formatI64(row.Version))
	return out
}

func formatI64(v int64) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	u := uint64(v)
	if neg {
		u = uint64(-v)
	}
	var buf [20]byte
	pos := len(buf)
	for u > 0 {
		pos--
		buf[pos] = byte('0' + u%10)
		u /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

func timeNowMillis() int64 {
	return time.Now().UnixMilli()
}

// PushSchema installs schema as the sole database schema, merging additively
// on subsequent pushes: existing docs and idempotency entries are preserved
// and the denormalized table map is repopulated. Destructive changes — a
// removed/changed table, field, or index — are rejected with the server's
// messages. Validation runs first, so an invalid TTL or non-indexable index
// field fails SCHEMA_VIOLATION exactly as the live server 422s.
// SetReadOnly toggles the per-database read-only freeze (server
// PATCH /admin/db/{db}/readonly). While set, ApplyTxn fails with READ_ONLY
// after the idempotency-replay lookup; queries are unaffected.
func (s *Store) SetReadOnly(readOnly bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.readOnly = readOnly
}

func (s *Store) PushSchema(schema wire.JSONValue) error {
	parsed, err := parseSchema(schema)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := validateSchema(parsed); err != nil {
		return err
	}
	if err := validateOnDelete(parsed); err != nil {
		return err
	}
	if s.schema != nil {
		if err := detectDestructiveChanges(s.schema, parsed); err != nil {
			return err
		}
	}
	s.schema = parsed
	for name, def := range parsed.Tables {
		s.tables[name] = def
	}
	return nil
}

// SchemaSnapshot returns the currently-installed typed schema (nil before
// the first PushSchema).
func (s *Store) SchemaSnapshot() *SchemaDef {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.schema
}

// Get returns the merged doc (system fields included) for (table, id), nil
// when absent or soft-deleted.
func (s *Store) Get(table, id string) wire.Object {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.docs[rowKey{table, id}]
	if !ok || row.DeletedAt != nil {
		return nil
	}
	return mergeDoc(row)
}

// CollectAll returns every merged doc in table (soft-deleted rows excluded),
// in unspecified order. Test/debug helper.
func (s *Store) CollectAll(table string) []wire.Object {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []wire.Object
	for key, row := range s.docs {
		if key.Table != table || row.DeletedAt != nil {
			continue
		}
		out = append(out, mergeDoc(row))
	}
	return out
}

func formatFloatText(f float64) string {
	return jsNumberString(f)
}
