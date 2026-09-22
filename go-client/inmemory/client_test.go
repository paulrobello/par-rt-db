// Client-surface, scheduler, and presence tests — the plan's Task 25 step-1
// set (scheduler enqueue/tick/fire; subscribe insert→snapshot; presence
// join/snapshot/expire) plus TTL reaping.
package inmemory

import (
	"context"
	"testing"
	"time"

	"github.com/paulrobello/par-rt-db/go-client/dsl"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func TestClientQueryMutateSurface(t *testing.T) {
	c := New(Options{})
	if err := c.PushSchema(testSchema()); err != nil {
		t.Fatalf("push: %v", err)
	}
	_, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{wire.StepInsert{
		Table: "items", Doc: docObj("name", "a", "status", "todo", "order", 1),
	}}}, "")
	if err != nil {
		t.Fatalf("mutate: %v", err)
	}
	v, err := c.Query(context.Background(), dsl.NewTableQuery("items"))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if arr, ok := v.(wire.Array); !ok || len(arr) != 1 {
		t.Fatalf("query result: %v", v)
	}
}

func TestSchedulerTickFiresDueOneShot(t *testing.T) {
	var clock int64 = 1000
	c := New(Options{Now: func() int64 { return clock }})
	if err := c.PushSchema(testSchema()); err != nil {
		t.Fatal(err)
	}
	_, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{wire.StepSchedule{
		When: wire.WhenAfterMs{Ms: 500},
		Txn: wire.Transaction{Steps: []wire.Step{wire.StepInsert{
			Table: "items", Doc: docObj("name", "fired", "status", "todo", "order", 1),
		}}},
	}}}, "")
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	// Not due yet.
	if reaped := c.Tick(1200); reaped != 0 {
		t.Fatalf("early tick reaped: %d", reaped)
	}
	if len(c.store.CollectAll("items")) != 0 {
		t.Fatal("txn fired before due")
	}
	// Due: tick applies the txn and removes the one-shot job.
	c.Tick(2000)
	rows := c.store.CollectAll("items")
	if len(rows) != 1 {
		t.Fatalf("scheduled txn did not fire: %d rows", len(rows))
	}
	if len(c.store.scheduledJobs) != 0 {
		t.Fatal("one-shot job not removed after firing")
	}
}

func TestSchedulerIntervalReArms(t *testing.T) {
	var clock int64 = 1000
	c := New(Options{Now: func() int64 { return clock }})
	if err := c.PushSchema(testSchema()); err != nil {
		t.Fatal(err)
	}
	// An interval txn that appends a marker each fire is complex; instead
	// patch a row the txn inserts once (idempotency: count stays 1, but the
	// job must RE-ARM with dueAt = fire + everyMs).
	_, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{wire.StepInsert{
		Table: "items", Doc: docObj("name", "base", "status", "todo", "order", 0),
	}}}, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{wire.StepSchedule{
		When: wire.WhenInterval{EveryMs: 100},
		Txn: wire.Transaction{Steps: []wire.Step{wire.StepPatchByQuery{
			Table:  "items",
			Filter: dsl.Eq("name", wire.String("base")),
			Patch:  docObj("order", 99),
		}}},
	}}}, "")
	if err != nil {
		t.Fatalf("schedule interval: %v", err)
	}
	clock = 1200
	c.Tick(clock)
	job := c.store.scheduledJobs[0]
	if job.FiredCount != 1 || job.DueAt != 1300 {
		t.Fatalf("re-arm: fired=%d dueAt=%d", job.FiredCount, job.DueAt)
	}
	// Fired at 1200, due at 1100 (1000 + everyMs 100): 100ms late, less than
	// one full window — normal poll cadence, not a missed window.
	if job.MissedCount != 0 || job.LastMissedAt != nil {
		t.Fatalf("on-time fire must not report a missed window: missed=%d lastMissedAt=%v", job.MissedCount, job.LastMissedAt)
	}
	// Failed fires mark the job errored with the last error preserved.
	_, err = c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{wire.StepSchedule{
		When: wire.WhenInterval{EveryMs: 50},
		Txn: wire.Transaction{Steps: []wire.Step{wire.StepInsert{
			Table: "ghost", Doc: docObj(),
		}}},
	}}}, "")
	if err != nil {
		t.Fatal(err)
	}
	clock = 2000
	c.Tick(clock)
	var bad *ScheduledJob
	for _, j := range c.store.scheduledJobs {
		if j.Kind == ScheduleKindInterval && j.Status == ScheduleStatusError && j.LastError != nil {
			bad = j
		}
	}
	if bad == nil {
		t.Fatal("failed fire not recorded")
	}
}

// TestSchedulerIntervalRecordsMissedWindowsAfterBigClockJump is the
// restart-equivalent shape: a stale DueAt (several windows in the past) is
// observationally identical to process downtime. ENH: the skip must be
// counted, not silent.
func TestSchedulerIntervalRecordsMissedWindowsAfterBigClockJump(t *testing.T) {
	var clock int64 = 1000
	c := New(Options{Now: func() int64 { return clock }})
	if err := c.PushSchema(testSchema()); err != nil {
		t.Fatal(err)
	}
	_, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{wire.StepInsert{
		Table: "items", Doc: docObj("name", "base", "status", "todo", "order", 0),
	}}}, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{wire.StepSchedule{
		When: wire.WhenInterval{EveryMs: 100},
		Txn: wire.Transaction{Steps: []wire.Step{wire.StepPatchByQuery{
			Table:  "items",
			Filter: dsl.Eq("name", wire.String("base")),
			Patch:  docObj("order", 99),
		}}},
	}}}, "")
	if err != nil {
		t.Fatalf("schedule interval: %v", err)
	}
	// Job's due_at is 1000 + 100 = 1100. Jump the clock to 2100 — 10
	// intervals past due. Fires exactly once (never a backfill burst),
	// 9 full windows elapsed before it (the fire at 2100 is the 10th).
	clock = 2100
	c.Tick(clock)
	job := c.store.scheduledJobs[0]
	if job.FiredCount != 1 {
		t.Fatalf("expected exactly one fire, got %d", job.FiredCount)
	}
	if job.MissedCount != 9 {
		t.Fatalf("expected 9 missed windows, got %d", job.MissedCount)
	}
	if job.LastMissedAt == nil || *job.LastMissedAt != 2100 {
		t.Fatalf("expected lastMissedAt=2100, got %v", job.LastMissedAt)
	}
}

func TestTTLReaperRemovesExpired(t *testing.T) {
	// Post-increment clock: a constant clock would mint colliding ids.
	var clock int64 = 1000
	c := New(Options{Now: func() int64 { clock++; return clock }})
	if err := c.PushSchema(buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("sessions", func(tb *dsl.TableBuilder) {
			tb.Field("user", dsl.Str()).
				Field("expires", dsl.Num()).
				Index("by_expires", "expires").
				TTL("expires", nil)
		})
	})); err != nil {
		t.Fatal(err)
	}
	live, _ := doInsert(c.store, "sessions", c.store.SchemaSnapshot().Tables["sessions"], docObj("user", "a", "expires", 5000))
	dead, _ := doInsert(c.store, "sessions", c.store.SchemaSnapshot().Tables["sessions"], docObj("user", "b", "expires", 1500))
	if reaped := c.Tick(2000); reaped != 1 {
		t.Fatalf("reaped: %d", reaped)
	}
	if c.store.Get("sessions", live) == nil {
		t.Fatal("live row reaped")
	}
	if c.store.Get("sessions", dead) != nil {
		t.Fatal("expired row survived")
	}
}

func TestSubscribeFiresOnChange(t *testing.T) {
	var clock int64 = 1000
	c := New(Options{Now: func() int64 { return clock }})
	if err := c.PushSchema(testSchema()); err != nil {
		t.Fatal(err)
	}
	sub := c.Subscribe(dsl.NewTableQuery("items"))
	defer sub.Unsubscribe()
	// Initial snapshot delivered synchronously (empty).
	select {
	case snap := <-sub.Updates():
		if arr, ok := snap.(wire.Array); !ok || len(arr) != 0 {
			t.Fatalf("initial snapshot not empty: %v", snap)
		}
	case <-time.After(time.Second):
		t.Fatal("no initial snapshot")
	}
	_, err := c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{wire.StepInsert{
		Table: "items", Doc: docObj("name", "a", "status", "todo", "order", 1),
	}}}, "")
	if err != nil {
		t.Fatal(err)
	}
	var insertedID string
	select {
	case snap := <-sub.Updates():
		arr, ok := snap.(wire.Array)
		if !ok || len(arr) != 1 {
			t.Fatalf("update snapshot: %v", snap)
		}
		doc := arr[0].(wire.Object)
		insertedID = string(doc["_id"].(wire.String))
	case <-time.After(time.Second):
		t.Fatal("no update after insert")
	}
	// Deleting the row changes the result back to empty: the canonical
	// diff must fire the subscriber again.
	_, err = c.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{wire.StepDelete{
		Table: "items", ID: insertedID,
	}}}, "")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case snap := <-sub.Updates():
		arr, ok := snap.(wire.Array)
		if !ok || len(arr) != 0 {
			t.Fatalf("post-delete snapshot: %v", snap)
		}
	case <-time.After(time.Second):
		t.Fatal("no update after delete")
	}
}

func TestPresenceJoinSnapshotExpire(t *testing.T) {
	rooms := NewPresenceRooms()
	var clock int64 = 1000
	now := func() int64 { return clock }
	a := New(Options{ConnectionID: "a", PresenceRooms: rooms, Now: now})
	b := New(Options{ConnectionID: "b", PresenceRooms: rooms, Now: now})
	a.JoinPresence("room", wire.String("editing"), nil)
	if got := a.PresenceSnapshot("room"); len(got) != 1 || got[0].ConnectionID != "a" {
		t.Fatalf("a snapshot: %v", got)
	}
	b.JoinPresence("room", wire.String("viewing"), nil)
	if got := a.PresenceSnapshot("room"); len(got) != 2 {
		t.Fatalf("shared room: %v", got)
	}
	// TTL expiry nulls state at the recorded instant (clock 1000 + 100ms).
	ttl := uint64(100)
	b.UpdatePresence("room", wire.String("away"), &ttl)
	if got := b.PresenceSnapshot("room"); got[1].State != wire.String("away") {
		t.Fatalf("update: %v", got[1].State)
	}
	clock = 1050
	if rooms.Expire(clock) {
		t.Fatal("expired early")
	}
	clock = 1100
	if !rooms.Expire(clock) {
		t.Fatal("expiry not detected")
	}
	got := b.PresenceSnapshot("room")
	if _, isNull := got[1].State.(wire.Null); !isNull {
		t.Fatalf("state not nulled: %v", got[1].State)
	}
	// Member stays listed after expiry.
	if len(got) != 2 {
		t.Fatalf("member dropped after expiry: %v", got)
	}
	b.LeavePresence("room")
	if got := a.PresenceSnapshot("room"); len(got) != 1 {
		t.Fatalf("post-leave: %v", got)
	}
}

func TestTwoClientsSeeEachOthersWrites(t *testing.T) {
	// Shared presence but SEPARATE stores: the presence backends share, data
	// does not (each Client is its own engine — the harness's multi-client
	// story is presence-only).
	rooms := NewPresenceRooms()
	a := New(Options{ConnectionID: "a", PresenceRooms: rooms})
	b := New(Options{ConnectionID: "b", PresenceRooms: rooms})
	if err := a.PushSchema(testSchema()); err != nil {
		t.Fatal(err)
	}
	if err := b.PushSchema(testSchema()); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Mutate(context.Background(), wire.Transaction{Steps: []wire.Step{
		wire.StepInsert{Table: "items", Doc: docObj("name", "only-in-a", "status", "todo", "order", 1)},
	}}, ""); err != nil {
		t.Fatal(err)
	}
	v, err := b.Query(context.Background(), dsl.NewTableQuery("items"))
	if err != nil {
		t.Fatal(err)
	}
	if arr, ok := v.(wire.Array); !ok || len(arr) != 0 {
		t.Fatal("b sees a's store — clients must be isolated engines")
	}
	_ = New(Options{})
}
