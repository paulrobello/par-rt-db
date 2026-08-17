use super::*;

// ---- schedules --------------------------------------------------------
//
// Ports `describe("InMemoryRtDbClient — schedules")`
// (`ts-client/tests/in_memory.test.ts:432-537`). The harness mirrors the
// server semantics: one-shot catches up if past due (fires once even when
// `due_at < now`); cron steps by `CRON_STEP_MS` and skips missed windows.

/// The TS `insertTxn` shared by every schedules test (`:433`).
fn insert_todo_txn() -> Transaction {
    Mutation::new()
        .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
        .build()
}

/// Fixed-clock harness so schedule due-times are stable under `tick`
/// (mirrors TS `newClockClient` `:33-38`). Returns the client and a setter
/// for the clock.
fn new_clock_client() -> (InMemoryRtDbClient, Arc<Mutex<i64>>) {
    let cell: Arc<Mutex<i64>> = Arc::new(Mutex::new(1_700_000_000_000_i64));
    let cell_for_closure = cell.clone();
    let mut client = InMemoryRtDbClient::new(
        InMemoryRtDbClientOptions::default()
            .now(move || *cell_for_closure.lock().expect("not poisoned"))
            .random(|| 0.0),
    );
    client.push_schema(&test_schema()).unwrap();
    (client, cell)
}

#[tokio::test]
async fn schedule_and_tick_fires_a_due_oneshot_and_write_is_visible() {
    // Ports TS "schedule + tick fires a due one-shot and the write is
    // visible via query".
    let (mut c, clock) = new_clock_client();
    let id = c
        .schedule(insert_todo_txn(), ScheduleWhen::AfterMs { ms: 1000 })
        .expect("schedule ok");
    assert!(is_hex_id(&json!(id)), "id is 32 hex chars: {id}");

    *clock.lock().expect("not poisoned") += 2000; // past the due time
    c.tick(None);

    let docs = c
        .run::<Vec<Value>>(&TableQuery::new("items").collect())
        .expect("collect ok");
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["name"], json!("a"));
    // A fired one-shot is removed from the registry.
    let remaining = c.list_schedules();
    assert!(
        remaining.iter().all(|s| s.id != id),
        "fired oneshot removed"
    );
}

#[tokio::test]
async fn tick_does_not_fire_a_not_yet_due_oneshot() {
    let (mut c, clock) = new_clock_client();
    c.schedule(insert_todo_txn(), ScheduleWhen::AfterMs { ms: 5000 })
        .expect("schedule ok");

    *clock.lock().expect("not poisoned") += 1000; // before the due time
    c.tick(None);

    let docs = c
        .run::<Vec<Value>>(&TableQuery::new("items").collect())
        .expect("collect ok");
    assert!(docs.is_empty(), "not yet due — no fire");
}

#[tokio::test]
async fn tick_does_not_fire_a_paused_job() {
    // Ports TS "a paused scheduled job does not fire on tick".
    let (mut c, clock) = new_clock_client();
    let id = c
        .schedule(insert_todo_txn(), ScheduleWhen::AfterMs { ms: 1000 })
        .expect("schedule ok");
    c.pause_schedule(&id).expect("pause ok");

    *clock.lock().expect("not poisoned") += 2000; // due, but paused
    c.tick(None);

    let docs = c
        .run::<Vec<Value>>(&TableQuery::new("items").collect())
        .expect("collect ok");
    assert!(docs.is_empty(), "paused — no fire");
    let info = c
        .list_schedules()
        .into_iter()
        .find(|s| s.id == id)
        .expect("paused job still listed");
    assert_eq!(info.status.as_wire_str(), "paused");
}

#[tokio::test]
async fn cancel_schedule_removes_the_job() {
    // Ports TS "cancelSchedule removes the job so it does not fire on tick".
    let (mut c, clock) = new_clock_client();
    let id = c
        .schedule(insert_todo_txn(), ScheduleWhen::AfterMs { ms: 1000 })
        .expect("schedule ok");
    c.cancel_schedule(&id).expect("cancel ok");
    assert!(
        c.list_schedules().iter().all(|s| s.id != id),
        "cancelled id no longer listed"
    );

    *clock.lock().expect("not poisoned") += 2000;
    c.tick(None);

    let docs = c
        .run::<Vec<Value>>(&TableQuery::new("items").collect())
        .expect("collect ok");
    assert!(docs.is_empty(), "cancelled — no fire");
}

#[tokio::test]
async fn pause_then_resume_lets_the_job_fire_on_a_later_tick() {
    // Ports TS "pause then resume lets the job fire on a later tick".
    let (mut c, clock) = new_clock_client();
    let id = c
        .schedule(insert_todo_txn(), ScheduleWhen::AfterMs { ms: 1000 })
        .expect("schedule ok");
    c.pause_schedule(&id).expect("pause ok");
    *clock.lock().expect("not poisoned") += 2000;
    c.tick(None);
    assert_eq!(
        c.run::<Vec<Value>>(&TableQuery::new("items").collect())
            .expect("collect")
            .len(),
        0,
        "still paused at the first tick"
    );

    c.resume_schedule(&id).expect("resume ok");
    let info = c
        .list_schedules()
        .into_iter()
        .find(|s| s.id == id)
        .expect("resumed job listed");
    assert_eq!(info.status.as_wire_str(), "pending");

    c.tick(None);
    let docs = c
        .run::<Vec<Value>>(&TableQuery::new("items").collect())
        .expect("collect");
    assert_eq!(docs.len(), 1, "fired after resume");
}

#[tokio::test]
async fn list_schedules_returns_server_aligned_info() {
    // Ports TS "listSchedules returns schedule info with server-aligned
    // status/kind names".
    let (mut c, _clock) = new_clock_client();
    let id = c
        .schedule(
            insert_todo_txn(),
            ScheduleWhen::Cron {
                expr: "* * * * *".to_string(),
            },
        )
        .expect("schedule ok");

    let list = c.list_schedules();
    assert_eq!(list.len(), 1);
    let info = &list[0];
    assert_eq!(info.id, id);
    assert_eq!(info.kind.as_wire_str(), "cron");
    assert_eq!(info.status.as_wire_str(), "pending");
    assert_eq!(info.cron.as_deref(), Some("* * * * *"));
    assert_eq!(info.fired_count, 0);
    // dueAt / createdAt are present (numbers).
    let _ = info.due_at;
    let _ = info.created_at;
}

#[tokio::test]
async fn cancel_pause_resume_on_unknown_id_returns_not_found() {
    // Ports TS "cancel/pause/resume on an unknown id reject with
    // NOT_FOUND".
    let (mut c, _clock) = new_clock_client();
    let err = c.cancel_schedule("nope").unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
    let err = c.pause_schedule("nope").unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
    let err = c.resume_schedule("nope").unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
}

#[tokio::test]
async fn tick_cron_re_arms_and_fires_again_on_a_later_tick() {
    // The TS suite does not cover cron re-arm directly, but the brief calls
    // it out: cron steps by `CRON_STEP_MS` and fires again on a later tick.
    // Skipping missed windows is verified separately.
    let (mut c, clock) = new_clock_client();
    // The cron's initial due_at is `now + CRON_STEP_MS` (per `dueAtFor`),
    // so a tick at the schedule-time `now` does nothing. Advance one step
    // before the first fire.
    c.schedule(
        insert_todo_txn(),
        ScheduleWhen::Cron {
            expr: "* * * * *".to_string(),
        },
    )
    .expect("schedule ok");

    // First fire: advance one CRON_STEP_MS.
    *clock.lock().expect("not poisoned") += CRON_STEP_MS;
    c.tick(None);
    assert_eq!(
        c.run::<Vec<Value>>(&TableQuery::new("items").collect())
            .expect("collect")
            .len(),
        1,
        "cron fired once"
    );
    // Immediately re-ticking without advancing the clock does nothing —
    // the next due_at is now + CRON_STEP_MS.
    c.tick(None);
    assert_eq!(
        c.list_schedules().len(),
        1,
        "cron still registered (not removed after fire)"
    );
    let fired_count = c.list_schedules()[0].fired_count;
    assert_eq!(fired_count, 1, "fired_count tracks successful fires");

    // Advance the clock one CRON_STEP_MS — the cron should fire again.
    *clock.lock().expect("not poisoned") += CRON_STEP_MS;
    c.tick(None);
    assert_eq!(
        c.run::<Vec<Value>>(&TableQuery::new("items").collect())
            .expect("collect")
            .len(),
        2,
        "cron fired a second time after re-arm"
    );
    let fired_count = c.list_schedules()[0].fired_count;
    assert_eq!(fired_count, 2);
}

#[tokio::test]
async fn tick_cron_skips_missed_windows_does_not_backfill() {
    // Brief: cron skips missed windows — no N-fires for N missed windows.
    // Advance the clock many CRON_STEP_MS beyond the due_at; the cron fires
    // exactly once and re-arms one step ahead of `now`.
    let (mut c, _clock) = new_clock_client();
    c.schedule(
        insert_todo_txn(),
        ScheduleWhen::Cron {
            expr: "* * * * *".to_string(),
        },
    )
    .expect("schedule ok");

    // Jump 10 × CRON_STEP_MS past the due time and tick once.
    let big_jump = CRON_STEP_MS * 10;
    c.tick(Some(1_700_000_000_000_i64 + big_jump));

    let docs = c
        .run::<Vec<Value>>(&TableQuery::new("items").collect())
        .expect("collect");
    assert_eq!(docs.len(), 1, "missed windows are not backfilled");
    let info = &c.list_schedules()[0];
    assert_eq!(info.fired_count, 1, "fired exactly once");
    // Re-armed to `now + CRON_STEP_MS` (not `due_at + N × CRON_STEP_MS`).
    assert_eq!(info.due_at, 1_700_000_000_000_i64 + big_jump + CRON_STEP_MS);
}

#[tokio::test]
async fn tick_oneshot_in_the_past_fires_immediately_catch_up() {
    // Brief: one-shot catches up if past due — a `RunAt` in the past fires
    // once even when `due_at < now`.
    let (mut c, _clock) = new_clock_client();
    c.schedule(
        insert_todo_txn(),
        ScheduleWhen::RunAt {
            ms: 1_600_000_000_000, // 100B ms before the clock's starting value
        },
    )
    .expect("schedule ok");
    c.tick(None);
    let docs = c
        .run::<Vec<Value>>(&TableQuery::new("items").collect())
        .expect("collect");
    assert_eq!(docs.len(), 1, "past-due oneshot catches up");
    assert!(c.list_schedules().is_empty(), "oneshot removed after fire");
}

#[tokio::test]
async fn tick_oneshot_with_failing_txn_marks_error_and_keeps_it() {
    // A failing txn records `last_error` and flips status to `Error`. The
    // TS source keeps a failed oneshot in the registry (only crons re-arm).
    let (mut c, _clock) = new_clock_client();
    let id = c
        .schedule(
            // Reference an unknown table to force a NOT_FOUND.
            Mutation::new().insert("missing", json!({"x": 1})).build(),
            ScheduleWhen::AfterMs { ms: 0 },
        )
        .expect("schedule ok");
    c.tick(None);
    let info = c
        .list_schedules()
        .into_iter()
        .find(|s| s.id == id)
        .expect("failed oneshot kept in registry");
    assert_eq!(info.status.as_wire_str(), "error");
    assert!(
        info.last_error.is_some(),
        "last_error recorded: {:?}",
        info.last_error
    );
}

#[tokio::test]
async fn failed_txn_rolls_back_schedule_step_enqueue() {
    // FM-28 rollback: the schedule step's enqueue joins the atomicity
    // snapshot — a later step's error must not leave a phantom job that
    // tick() would fire (mirrors the server's single sqlx transaction
    // around the insert).
    let (mut c, clock) = new_clock_client();
    let txn = Mutation::new()
        .schedule(ScheduleWhen::AfterMs { ms: 1000 }, insert_todo_txn())
        .delete("items", "nonexistent") // NOT_FOUND -> rollback the enqueue
        .build();
    let err = c.mutate(&txn, None).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
    assert!(c.list_schedules().is_empty(), "enqueue rolled back");
    // Past the would-be due time: nothing fires.
    *clock.lock().expect("not poisoned") += 2000;
    c.tick(None);
    let docs = c
        .run::<Vec<Value>>(&TableQuery::new("items").collect())
        .expect("collect ok");
    assert!(docs.is_empty(), "no phantom job fired");
}

#[tokio::test]
async fn failed_txn_rolls_back_cancel_schedule_step() {
    // Same snapshot covers a cancel step's removal: a pre-existing job
    // survives a txn that cancelled it and then failed.
    let (mut c, clock) = new_clock_client();
    let id = c
        .schedule(insert_todo_txn(), ScheduleWhen::AfterMs { ms: 1000 })
        .expect("schedule ok");
    let txn = Mutation::new()
        .cancel_schedule(id.clone())
        .delete("items", "nonexistent") // NOT_FOUND -> rollback the cancel
        .build();
    let err = c.mutate(&txn, None).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
    let jobs = c.list_schedules();
    assert_eq!(jobs.len(), 1, "job survived the failed txn");
    assert_eq!(jobs[0].id, id);
    // The surviving job still fires on its original schedule.
    *clock.lock().expect("not poisoned") += 2000;
    c.tick(None);
    let docs = c
        .run::<Vec<Value>>(&TableQuery::new("items").collect())
        .expect("collect ok");
    assert_eq!(docs.len(), 1, "surviving job fired");
}
