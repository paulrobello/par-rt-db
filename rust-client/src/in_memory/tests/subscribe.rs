use super::*;

// ---- subscribe --------------------------------------------------------
//
// Ports `describe("InMemoryRtDbClient — subscribe")`
// (`ts-client/tests/in_memory.test.ts:229-248`). The harness re-runs each
// subscriber's query on a successful txn that touched its table, and fires
// its callback iff the canonicalized result changed. The initial value is
// delivered synchronously inside `subscribe`.

/// Mirror of the TS `subscribe` test: a `count()` over `by_status=todo`
/// starts at 0, goes to 1 on a todo insert, and stays at 1 on a done
/// insert (different table-write, but same table — done doesn't change the
/// todo count). Unsubscribing stops further updates.
#[tokio::test]
async fn subscribe_delivers_initial_value_and_recomputes_only_on_change() {
    let mut c = new_client();
    let updates: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
    let updates_clone = updates.clone();
    let _unsub = c.subscribe(
        TableQuery::new("items")
            .with_index("by_status", &[json!("todo")])
            .count(),
        move |v| {
            if let Some(n) = v.as_i64() {
                updates_clone.lock().expect("not poisoned").push(n);
            }
        },
    );
    assert_eq!(
        updates.lock().expect("not poisoned").as_slice(),
        &[0],
        "initial value delivered synchronously"
    );

    c.mutate(
        &Mutation::new()
            .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
            .build(),
        None,
    )
    .await
    .expect("insert todo");
    assert_eq!(
        updates.lock().expect("not poisoned").as_slice(),
        &[0, 1],
        "todo insert bumped the count"
    );

    // A write to a different status doesn't change the todo count, so the
    // callback is not invoked.
    c.mutate(
        &Mutation::new()
            .insert("items", json!({"name": "b", "status": "done", "order": 2}))
            .build(),
        None,
    )
    .await
    .expect("insert done");
    assert_eq!(
        updates.lock().expect("not poisoned").as_slice(),
        &[0, 1],
        "done insert did not change the todo count"
    );
}

#[tokio::test]
async fn subscribe_unsubscribe_stops_further_updates() {
    let mut c = new_client();
    let updates: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
    let updates_clone = updates.clone();
    let unsub = c.subscribe(
        TableQuery::new("items")
            .with_index("by_status", &[json!("todo")])
            .count(),
        move |v| {
            if let Some(n) = v.as_i64() {
                updates_clone.lock().expect("not poisoned").push(n);
            }
        },
    );
    assert_eq!(updates.lock().expect("not poisoned").as_slice(), &[0]);

    // Explicit unsubscribe (the Drop path is exercised by the next test).
    unsub.unsubscribe();

    c.mutate(
        &Mutation::new()
            .insert("items", json!({"name": "c", "status": "todo", "order": 3}))
            .build(),
        None,
    )
    .await
    .expect("insert todo");
    assert_eq!(
        updates.lock().expect("not poisoned").as_slice(),
        &[0],
        "no further updates after unsubscribe"
    );
}

#[tokio::test]
async fn subscribe_dropping_handle_unsubscribes() {
    // The RAII guard path: dropping the handle clears the listener.
    let mut c = new_client();
    let updates: Arc<Mutex<Vec<i64>>> = Arc::new(Mutex::new(Vec::new()));
    let updates_clone = updates.clone();
    {
        let _unsub = c.subscribe(
            TableQuery::new("items")
                .with_index("by_status", &[json!("todo")])
                .count(),
            move |v| {
                if let Some(n) = v.as_i64() {
                    updates_clone.lock().expect("not poisoned").push(n);
                }
            },
        );
        assert_eq!(updates.lock().expect("not poisoned").as_slice(), &[0]);
    }
    c.mutate(
        &Mutation::new()
            .insert("items", json!({"name": "d", "status": "todo", "order": 4}))
            .build(),
        None,
    )
    .await
    .expect("insert todo");
    assert_eq!(
        updates.lock().expect("not poisoned").as_slice(),
        &[0],
        "drop(unsub) cleared the listener"
    );
}

/// A projected subscription ignores `_version`-only changes (a port of server
/// `diff_canonical`): the diff strips `_version` before comparing, so a patch
/// to a NON-projected field re-runs the query but pushes nothing, while a
/// patch to a projected field pushes. The pushed payload still carries
/// `_version` and only the projected fields.
#[tokio::test]
async fn subscribe_projected_ignores_non_projected_field_writes() {
    let mut c = new_client();
    let results = c
        .mutate(
            &Mutation::new()
                .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
                .build(),
            None,
        )
        .await
        .expect("insert");
    let StepResult::Insert { id } = &results[0] else {
        panic!("expected Insert step result");
    };

    let updates: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let updates_clone = updates.clone();
    let _unsub = c.subscribe(
        TableQuery::new("items")
            .with_index("by_status", &[json!("todo")])
            .fields(&["name"])
            .collect(),
        move |v| updates_clone.lock().expect("not poisoned").push(v),
    );
    // Initial delivery: one doc, only the projected field + system fields.
    {
        let guard = updates.lock().expect("not poisoned");
        assert_eq!(guard.len(), 1, "initial value delivered synchronously");
        let doc = &guard[0].as_array().expect("docs")[0];
        assert_eq!(doc["name"], json!("a"));
        assert!(doc["_version"].is_number(), "payload carries _version");
        assert!(
            doc.get("status").is_none() && doc.get("order").is_none(),
            "non-projected fields dropped from the payload"
        );
    }

    // Patch a NON-projected field: the write bumps `_version` (and re-runs
    // the sub), but the diff strips `_version` — nothing is pushed.
    c.mutate(
        &Mutation::new()
            .patch("items", id, json!({"order": 99}))
            .build(),
        None,
    )
    .await
    .expect("patch order");
    assert_eq!(
        updates.lock().expect("not poisoned").len(),
        1,
        "non-projected write did not re-emit"
    );

    // Patch the projected field: the canonical changes — push, still without
    // the non-projected fields.
    c.mutate(
        &Mutation::new()
            .patch("items", id, json!({"name": "b"}))
            .build(),
        None,
    )
    .await
    .expect("patch name");
    {
        let guard = updates.lock().expect("not poisoned");
        assert_eq!(guard.len(), 2, "projected-field write re-emitted");
        let doc = &guard[1].as_array().expect("docs")[0];
        assert_eq!(doc["name"], json!("b"));
        assert!(
            doc["_version"].is_number(),
            "payload still carries _version"
        );
        assert!(doc.get("order").is_none());
    }
}
