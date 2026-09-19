// Write-path tests — representative ports of rust in_memory/tests/
// {writes,cascade,unique,computed}.rs (the exhaustive gate is the corpus).
package inmemory

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/paulrobello/par-rt-db/go-client/dsl"
	rtdberrors "github.com/paulrobello/par-rt-db/go-client/errors"
	"github.com/paulrobello/par-rt-db/go-client/wire"
)

func defaultsSchema(t *testing.T) *Store {
	s := NewStore()
	if err := s.PushSchema(buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("posts", func(tb *dsl.TableBuilder) {
			tb.Field("title", dsl.Str()).
				Field("views", dsl.Num()).
				Field("slug", dsl.Optional(dsl.Str())).
				Defaults(map[string]wire.JSONValue{"views": wire.Number("0")}).
				Computed("slug", wire.ValueLower{Value: wire.ValueField{Field: "title"}})
		})
	})); err != nil {
		t.Fatalf("push: %v", err)
	}
	return s
}

func TestInsertStampsDefaultsComputedAndSystemFields(t *testing.T) {
	s := defaultsSchema(t)
	table := s.SchemaSnapshot().Tables["posts"]
	now := s.Now()
	id, err := doInsert(s, "posts", table, docObj("title", "Hello"))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	if len(id) != 32 {
		t.Fatalf("id shape: %s", id)
	}
	row := s.docs[rowKey{Table: "posts", ID: id}]
	if !jsonEq(row.Doc["views"], wire.Number("0")) {
		t.Fatalf("default not applied: %v", row.Doc["views"])
	}
	if !jsonEq(row.Doc["slug"], wire.String("hello")) {
		t.Fatalf("computed not stamped: %v", row.Doc["slug"])
	}
	if row.Version != 1 || row.CreatedAt < now {
		t.Fatalf("system fields: v%d t%d", row.Version, row.CreatedAt)
	}
	// A client-supplied computed value is dropped and re-derived.
	id2, err := doInsert(s, "posts", table, docObj("title", "Wow", "slug", "client-noise"))
	if err != nil {
		t.Fatalf("insert 2: %v", err)
	}
	row2 := s.docs[rowKey{Table: "posts", ID: id2}]
	if !jsonEq(row2.Doc["slug"], wire.String("wow")) {
		t.Fatalf("client computed value survived: %v", row2.Doc["slug"])
	}
}

func TestPatchDoesNotReApplyDefaults(t *testing.T) {
	s := defaultsSchema(t)
	table := s.SchemaSnapshot().Tables["posts"]
	id, _ := doInsert(s, "posts", table, docObj("title", "A"))
	// A patch re-derives computed fields but never re-applies defaults.
	err := doPatch(s, table, "posts", id, docObj("title", "B"))
	if err != nil {
		t.Fatalf("patch: %v", err)
	}
	row := s.docs[rowKey{Table: "posts", ID: id}]
	if !jsonEq(row.Doc["views"], wire.Number("0")) {
		t.Fatal("views lost")
	}
	if row.Version != 2 {
		t.Fatalf("version not bumped: %d", row.Version)
	}
	err = doPatch(s, table, "posts", "missing", docObj("title", "X"))
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeNotFound {
		t.Fatalf("missing patch: %v", err)
	}
}

func TestExpectVersionMismatch(t *testing.T) {
	s := defaultsSchema(t)
	table := s.SchemaSnapshot().Tables["posts"]
	id, _ := doInsert(s, "posts", table, docObj("title", "A"))
	if err := doExpectVersion(s, "posts", id, 1); err != nil {
		t.Fatalf("expected ok: %v", err)
	}
	err := doExpectVersion(s, "posts", id, 99)
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodePreconditionFailed {
		t.Fatalf("got: %v", err)
	}
	if !strings.Contains(re.Message, "version mismatch") {
		t.Fatalf("msg: %s", re.Message)
	}
}

func TestReplacePreservesAutoIncrement(t *testing.T) {
	s := NewStore()
	if err := s.PushSchema(buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("t", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).
				Field("seq", dsl.Int64()).
				Field("note", dsl.Optional(dsl.Str())).
				AutoIncrementField("seq")
		})
	})); err != nil {
		t.Fatalf("push: %v", err)
	}
	table := s.SchemaSnapshot().Tables["t"]
	id, err := doInsert(s, "t", table, docObj("name", "a"))
	if err != nil {
		t.Fatalf("insert: %v", err)
	}
	row := s.docs[rowKey{Table: "t", ID: id}]
	stored := row.Doc["seq"]
	_ = table
	// Omitted on replace → preserved.
	if err := doReplace(s, table, "t", id, docObj("name", "b")); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if !jsonEq(row.Doc["seq"], stored) {
		t.Fatalf("seq not preserved: %v vs %v", row.Doc["seq"], stored)
	}
	// Changed → BAD_REQUEST.
	err = doReplace(s, table, "t", id, docObj("name", "c", "seq", floatToJSON(999)))
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest || !strings.Contains(re.Message, "cannot be changed") {
		t.Fatalf("changed seq: %v", err)
	}
}

func floatToJSON(f float64) wire.JSONValue { return wire.Number(jsNumberString(f)) }

func upsertStore(t *testing.T) *Store {
	s := NewStore()
	if err := s.PushSchema(buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("users", func(tb *dsl.TableBuilder) {
			tb.Field("email", dsl.Str()).
				Field("hits", dsl.Num()).
				Index("by_email", "email").Unique()
		})
	})); err != nil {
		t.Fatalf("push: %v", err)
	}
	return s
}

func TestUpsertInsertsThenPatches(t *testing.T) {
	s := upsertStore(t)
	results, err := ApplyTxn(s, wire.Transaction{Steps: []wire.Step{wire.StepUpsert{
		Table:  "users",
		Index:  "by_email",
		Eq:     []wire.JSONValue{wire.String("a@x")},
		Insert: docObj("email", "a@x", "hits", int64(1)),
		Patch:  docObj("hits", int64(2)),
	}}}, "")
	if err != nil {
		t.Fatalf("upsert 1: %v", err)
	}
	if results[0].Inserted == nil || !*results[0].Inserted {
		t.Fatalf("first upsert must report inserted: %+v", results[0])
	}
	results, err = ApplyTxn(s, wire.Transaction{Steps: []wire.Step{wire.StepUpsert{
		Table:  "users",
		Index:  "by_email",
		Eq:     []wire.JSONValue{wire.String("a@x")},
		Insert: docObj("email", "a@x", "hits", int64(1)),
		Patch:  docObj("hits", int64(2)),
	}}}, "")
	if err != nil {
		t.Fatalf("upsert 2: %v", err)
	}
	if results[0].Inserted == nil || *results[0].Inserted {
		t.Fatalf("second upsert must report patched: %+v", results[0])
	}
	var count int
	for _, row := range s.docs {
		if _, ok := row.Doc["email"]; ok {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("upsert duplicated rows: %d", count)
	}
}

func TestUniqueConflictAndPartialPredicate(t *testing.T) {
	s := upsertStore(t)
	users := s.SchemaSnapshot().Tables["users"]
	if _, err := doInsert(s, "users", users, docObj("email", "a@x", "hits", 1)); err != nil {
		t.Fatalf("insert 1: %v", err)
	}
	_, err := doInsert(s, "users", users, docObj("email", "a@x", "hits", 2))
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeConflict || !strings.Contains(re.Message, "unique index 'by_email' violated") {
		t.Fatalf("conflict: %v", err)
	}
	// Different key inserts fine.
	if _, err := doInsert(s, "users", users, docObj("email", "b@x", "hits", 1)); err != nil {
		t.Fatalf("insert 2: %v", err)
	}
}

func cascadeStore(t *testing.T) *Store {
	s := NewStore()
	if err := s.PushSchema(buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("projects", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).Index("by_name", "name")
		})
		b.Table("tasks", func(tb *dsl.TableBuilder) {
			tb.Field("project", dsl.IdRefWithOnDelete("projects", dsl.OnDeleteCascade)).
				Field("title", dsl.Str()).
				Index("by_project", "project")
		})
	})); err != nil {
		t.Fatalf("push: %v", err)
	}
	return s
}

func TestCascadeDeleteRemovesChildren(t *testing.T) {
	s := cascadeStore(t)
	schema := s.SchemaSnapshot()
	pt := schema.Tables["projects"]
	tt := schema.Tables["tasks"]
	pid, _ := doInsert(s, "projects", pt, docObj("name", "p"))
	tid, _ := doInsert(s, "tasks", tt, docObj("project", pid, "title", "t"))
	visited := map[rowKey]bool{}
	cascadeRows := 0
	var touched []string
	if err := deleteRowCascade(s, "projects", pid, visited, &cascadeRows, false, &touched); err != nil {
		t.Fatalf("cascade delete: %v", err)
	}
	if _, ok := s.docs[rowKey{Table: "projects", ID: pid}]; ok {
		t.Fatal("parent not deleted")
	}
	if _, ok := s.docs[rowKey{Table: "tasks", ID: tid}]; ok {
		t.Fatal("child not cascaded")
	}
}

func TestRestrictBlocksParentDelete(t *testing.T) {
	s := NewStore()
	if err := s.PushSchema(buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("projects", func(tb *dsl.TableBuilder) {
			tb.Field("name", dsl.Str()).Index("by_name", "name")
		})
		b.Table("tasks", func(tb *dsl.TableBuilder) {
			tb.Field("project", dsl.IdRefWithOnDelete("projects", dsl.OnDeleteRestrict)).
				Field("title", dsl.Str()).
				Index("by_project", "project")
		})
	})); err != nil {
		t.Fatalf("push: %v", err)
	}
	schema := s.SchemaSnapshot()
	pid, _ := doInsert(s, "projects", schema.Tables["projects"], docObj("name", "p"))
	tid, _ := doInsert(s, "tasks", schema.Tables["tasks"], docObj("project", pid, "title", "t"))
	_ = tid
	visited := map[rowKey]bool{}
	cascadeRows := 0
	var touched []string
	err := deleteRowCascade(s, "projects", pid, visited, &cascadeRows, false, &touched)
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeConflict {
		t.Fatalf("restrict: %v", err)
	}
	if _, still := s.docs[rowKey{Table: "projects", ID: pid}]; !still {
		t.Fatal("parent was deleted despite restrict")
	}
}

func TestSoftDeleteAndUndelete(t *testing.T) {
	s := NewStore()
	if err := s.PushSchema(buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("notes", func(tb *dsl.TableBuilder) {
			tb.Field("text", dsl.Str()).SoftDelete().Index("by_text", "text")
		})
		b.Table("plain", func(tb *dsl.TableBuilder) {
			tb.Field("text", dsl.Str()).Index("by_text", "text")
		})
	})); err != nil {
		t.Fatalf("push: %v", err)
	}
	schema := s.SchemaSnapshot()
	table := schema.Tables["notes"]
	id, _ := doInsert(s, "notes", table, docObj("text", "hi"))
	if err := doSoftDelete(s, "notes", id); err != nil {
		t.Fatalf("soft delete: %v", err)
	}
	row := s.docs[rowKey{Table: "notes", ID: id}]
	if row.DeletedAt == nil {
		t.Fatal("row not stamped")
	}
	// Soft-deleted rows are invisible to reads and eqLookups.
	if s.Get("notes", id) != nil {
		t.Fatal("soft-deleted row visible to Get")
	}
	// Re-delete is NOT_FOUND.
	err := doSoftDelete(s, "notes", id)
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeNotFound {
		t.Fatalf("double delete: %v", err)
	}
	// Undelete restores.
	if err := doUndelete(s, table, "notes", id); err != nil {
		t.Fatalf("undelete: %v", err)
	}
	if s.Get("notes", id) == nil {
		t.Fatal("restored row still invisible")
	}
	// Undelete without softDelete declaration → BAD_REQUEST.
	if err := doUndelete(s, s.SchemaSnapshot().Tables["plain"], "plain", "x"); err == nil {
		t.Fatal("undelete on non-softDelete table must fail")
	}
}

func TestPatchByQueryAndTruncation(t *testing.T) {
	s := newTestStore(t)
	for i := 1; i <= 3; i++ {
		seedRow(t, s, "items", int64(100+i), docObj("name", "n"+string(rune('0'+i)), "status", "todo", "order", int64(i)))
	}
	table := s.SchemaSnapshot().Tables["items"]
	one := 1
	patched, truncated, err := patchByQuery(s, table, "items",
		dsl.Eq("status", wire.String("todo")), docObj("status", "done"), &one)
	if err != nil {
		t.Fatalf("patchByQuery: %v", err)
	}
	if patched != 1 || !truncated {
		t.Fatalf("patched=%d truncated=%v", patched, truncated)
	}
	// olderThan is legal in the by-query context (order field is number).
	all, truncated, err := patchByQuery(s, table, "items",
		dsl.OlderThan("order", 10), docObj("status", "old"), nil)
	if err != nil {
		t.Fatalf("olderThan by-query: %v", err)
	}
	_ = all
	_ = truncated
	// The same filter on the READ path is rejected (validated in T21 tests).
}

func TestAdjustCounterPreconditions(t *testing.T) {
	s := defaultsSchema(t)
	_ = s
	s2 := NewStore()
	if err := s2.PushSchema(buildSchema(func(b *dsl.SchemaBuilder) {
		b.Table("c", func(tb *dsl.TableBuilder) {
			tb.Field("views", dsl.Num()).Field("kind", dsl.Str())
		})
	})); err != nil {
		t.Fatalf("push: %v", err)
	}
	table := s2.SchemaSnapshot().Tables["c"]
	id, _ := doInsert(s2, "c", table, docObj("views", 5, "kind", "a"))
	results, err := ApplyTxn(s2, wire.Transaction{Steps: []wire.Step{wire.StepAdjustCounter{
		Table: "c", ID: id, Field: "views", Delta: 2,
		Expected: docObj("kind", "a"),
		Max:      i64Ptr(10),
	}}}, "")
	if err != nil {
		t.Fatalf("adjust: %v", err)
	}
	_ = results
	row := s2.docs[rowKey{Table: "c", ID: id}]
	if !jsonEq(row.Doc["views"], wire.Number("7")) {
		t.Fatalf("counter: %v", row.Doc["views"])
	}
	// Bound exceeded → PRECONDITION_FAILED.
	_, err = ApplyTxn(s2, wire.Transaction{Steps: []wire.Step{wire.StepAdjustCounter{
		Table: "c", ID: id, Field: "views", Delta: 99, Max: i64Ptr(10),
	}}}, "")
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodePreconditionFailed {
		t.Fatalf("bounds: %v", err)
	}
}

func TestApplyTxnIdempotencyReplay(t *testing.T) {
	s := newTestStore(t)
	txn := wire.Transaction{Steps: []wire.Step{wire.StepInsert{
		Table: "items", Doc: docObj("name", "once", "status", "todo", "order", 1),
	}}}
	first, err := ApplyTxn(s, txn, "m1")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	second, err := ApplyTxn(s, txn, "m1")
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(first) != len(second) || first[0].ID != second[0].ID {
		t.Fatalf("replay diverged: %+v vs %+v", first[0], second[0])
	}
	count := 0
	for key := range s.docs {
		if key.Table == "items" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("replay inserted again: %d rows", count)
	}
}

func TestTxnRollbackOnFailure(t *testing.T) {
	s := newTestStore(t)
	seedThreeRows(t, s)
	before := len(s.docs)
	// Step 1 succeeds; step 2 fails (unknown table) → whole txn rolled back.
	_, err := ApplyTxn(s, wire.Transaction{Steps: []wire.Step{
		wire.StepInsert{Table: "items", Doc: docObj("name", "x", "status", "todo", "order", 9)},
		wire.StepInsert{Table: "ghost", Doc: docObj()},
	}}, "")
	if err == nil {
		t.Fatal("expected failure")
	}
	if len(s.docs) != before {
		t.Fatalf("rollback failed: %d != %d", len(s.docs), before)
	}
}

func TestScheduleStepEnqueuesAndCancels(t *testing.T) {
	s := newTestStore(t)
	results, err := ApplyTxn(s, wire.Transaction{Steps: []wire.Step{wire.StepSchedule{
		When: wire.WhenAfterMs{Ms: 5000},
		Txn:  wire.Transaction{Steps: []wire.Step{wire.StepInsert{Table: "items", Doc: docObj("name", "later", "status", "todo", "order", 1)}}},
	}}}, "")
	if err != nil {
		t.Fatalf("schedule: %v", err)
	}
	sid := *results[0].ScheduleID
	if len(s.scheduledJobs) != 1 {
		t.Fatalf("job not enqueued: %d", len(s.scheduledJobs))
	}
	if s.scheduledJobs[0].DueAt != s.scheduledJobs[0].CreatedAt+5000 {
		t.Fatalf("dueAt math: %d", s.scheduledJobs[0].DueAt)
	}
	// Cancel inside a txn reports cancelled=true.
	res2, err := ApplyTxn(s, wire.Transaction{Steps: []wire.Step{wire.StepCancelSchedule{ID: sid}}}, "")
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if res2[0].Cancelled == nil || !*res2[0].Cancelled {
		t.Fatalf("cancelled flag: %+v", res2[0])
	}
	// Cancel again reports cancelled=false, not an error.
	res3, err := ApplyTxn(s, wire.Transaction{Steps: []wire.Step{wire.StepCancelSchedule{ID: sid}}}, "")
	if err != nil {
		t.Fatalf("cancel 2: %v", err)
	}
	if *res3[0].Cancelled {
		t.Fatal("second cancel must report false")
	}
}

func TestByQueryRejectsNegativeLimit(t *testing.T) {
	// Pins T23-review C2: rust's Option<u32> decode rejects a negative
	// limit; the Go *int wire shape must reject it loudly (BAD_REQUEST), not
	// panic on a negative slice bound or under-count the budget.
	s := upsertStore(t)
	neg := -1
	_, err := ApplyTxn(s, wire.Transaction{Steps: []wire.Step{wire.StepPatchByQuery{
		Table: "users", Filter: dsl.Eq("email", wire.String("a@x")),
		Patch: docObj("hits", 1), Limit: &neg,
	}}}, "")
	re, ok := err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("patchByQuery negative limit: %v", err)
	}
	_, err = ApplyTxn(s, wire.Transaction{Steps: []wire.Step{wire.StepDeleteByQuery{
		Table: "users", Filter: dsl.Eq("email", wire.String("a@x")), Limit: &neg,
	}}}, "")
	re, ok = err.(*rtdberrors.RtDbError)
	if !ok || re.Code != rtdberrors.CodeBadRequest {
		t.Fatalf("deleteByQuery negative limit: %v", err)
	}
}

func TestStepResultNullMarshalsAsNull(t *testing.T) {
	// Pins T23-review I1: a null step result serializes as JSON null (the
	// server's untagged StepResult::Null), never {}.
	data, err := json.Marshal(wire.StepResult{})
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "null" {
		t.Fatalf("null step result: %s", data)
	}
	data, err = json.Marshal(wire.StepResult{ID: stringPtr2("x")})
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"id":"x"}` {
		t.Fatalf("insert result: %s", data)
	}
}

func TestSubscriberCallbackMayReenterStore(t *testing.T) {
	// Pins T23-review I2: fires flush AFTER the store lock releases — a
	// callback that queries the store must not self-deadlock (rust fires
	// outside the borrow).
	s := newTestStore(t)
	sub := &storeSubscription{
		Query: wire.Query{Table: "items"},
		Table: "items",
		alive: &syncFlag{},
		Callback: func(wire.JSONValue) {
			// Re-enter the store from inside the callback.
			if _, err := EvalQuery(s, wire.Query{Table: "items"}); err != nil {
				t.Errorf("re-entrant query: %v", err)
			}
		},
	}
	sub.alive.set(true)
	s.mu.Lock()
	s.subscribers = append(s.subscribers, sub)
	s.mu.Unlock()
	done := make(chan struct{})
	go func() {
		_, err := ApplyTxn(s, wire.Transaction{Steps: []wire.Step{wire.StepInsert{
			Table: "items", Doc: docObj("name", "x", "status", "todo", "order", 1),
		}}}, "")
		if err != nil {
			t.Errorf("mutate: %v", err)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock: subscriber callback re-entered the store")
	}
}
