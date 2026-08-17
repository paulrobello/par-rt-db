use super::*;

#[tokio::test]
async fn insert_merges_system_fields_at_read_time() {
    let mut c = new_client();
    let txn = Mutation::new()
        .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
        .build();
    let results = c.mutate(&txn, None).await.expect("mutate ok");
    assert_eq!(results.len(), 1);
    let id = match &results[0] {
        StepResult::Insert { id } => id.clone(),
        other => panic!("expected Insert, got {other:?}"),
    };
    assert!(is_hex_id(&json!(id)), "id should be 32 hex chars: {id}");

    let doc = c.get("items", &id).expect("doc present");
    // System fields merged at read time:
    assert_eq!(doc["_id"], json!(id));
    assert_eq!(doc["_version"], 1);
    assert!(doc["_creationTime"].is_number(), "creationTime is a number");
    // User fields preserved:
    assert_eq!(doc["name"], "a");
    assert_eq!(doc["status"], "todo");
    assert_eq!(doc["order"], 1);
}

#[tokio::test]
async fn insert_strips_optional_field_set_to_null() {
    // Mirrors TS "strips an optional field set to null on insert".
    let mut c = new_client();
    let txn = Mutation::new()
        .insert(
            "items",
            json!({"name": "a", "status": "todo", "order": 1, "note": null}),
        )
        .build();
    let results = c.mutate(&txn, None).await.expect("mutate ok");
    let id = match &results[0] {
        StepResult::Insert { id } => id.clone(),
        _ => unreachable!(),
    };
    let doc = c.get("items", &id).expect("doc present");
    // `note: null` was stripped on insert — the server's single representation
    // of an unset Optional<String> is "key absent", never "key present with null".
    assert!(
        doc.get("note").is_none(),
        "optional-null should be stripped, got: {doc}"
    );
}

#[tokio::test]
async fn insert_rejects_missing_required_field() {
    // Mirrors TS "rejects an insert missing a required field".
    let mut c = new_client();
    let txn = Mutation::new()
        .insert("items", json!({"status": "todo", "order": 1})) // missing required "name"
        .build();
    let err = c.mutate(&txn, None).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(err.message.contains("name"), "got: {}", err.message);
}

// ---- mutate: upsert by index --------------------------------------

#[tokio::test]
async fn upsert_inserts_on_no_match_and_patches_on_match() {
    // Mirrors TS "inserts on no match (inserted: true) and patches on match".
    let mut c = new_client();
    let upsert = |patch_order: i64| {
        Mutation::new()
            .upsert(
                "items",
                "by_name",
                &[json!("a")],
                json!({"name": "a", "status": "todo", "order": 1}),
                json!({"order": patch_order}),
            )
            .build()
    };

    let r1 = c.mutate(&upsert(2), None).await.expect("first upsert ok");
    let (id, inserted) = match &r1[0] {
        StepResult::Upsert { id, inserted } => (id.clone(), *inserted),
        other => panic!("expected Upsert, got {other:?}"),
    };
    assert!(inserted, "first upsert should insert");
    assert!(is_hex_id(&json!(id)));

    let r2 = c.mutate(&upsert(3), None).await.expect("second upsert ok");
    match &r2[0] {
        StepResult::Upsert {
            id: id2,
            inserted: false,
        } => {
            assert_eq!(id2, &id, "second upsert patched the same doc");
        }
        other => panic!("expected Upsert inserted=false, got {other:?}"),
    }

    let doc = c.get("items", &id).expect("doc present");
    assert_eq!(doc["order"], 3, "patch applied");
    assert_eq!(doc["_version"], 2, "patch bumped version");
}

#[tokio::test]
async fn upsert_patch_visible_in_later_index_lookup() {
    // Mirrors TS "patches a matched doc onto an index field and reflects it
    // in a later query" — now via the real query DSL (Task 3), not the
    // internal `eq_lookup` helper. The patched `order` value is observable
    // through a `unique()` query on `by_name`.
    let mut c = new_client();
    let upsert = |patch_order: i64| {
        Mutation::new()
            .upsert(
                "items",
                "by_name",
                &[json!("a")],
                json!({"name": "a", "status": "todo", "order": 1}),
                json!({"order": patch_order}),
            )
            .build()
    };
    c.mutate(&upsert(2), None).await.unwrap();
    let r2 = c.mutate(&upsert(3), None).await.unwrap();
    let id = match &r2[0] {
        StepResult::Upsert { id, .. } => id.clone(),
        _ => unreachable!(),
    };

    let matched: Value = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_name", &[json!("a")])
                .unique(),
        )
        .expect("unique query ok");
    assert_eq!(matched["_id"], json!(id), "matched the patched doc");
    assert_eq!(matched["order"], 3, "patch value visible through the DSL");
}

#[tokio::test]
async fn upsert_rejects_multiple_matches() {
    // The brief calls out the multi-match rejection explicitly. Seed two
    // docs with the same indexed value, then upsert by that index.
    let mut c = new_client();
    c.mutate(
        &Mutation::new()
            .insert(
                "items",
                json!({"name": "dup", "status": "todo", "order": 1}),
            )
            .build(),
        None,
    )
    .await
    .unwrap();
    c.mutate(
        &Mutation::new()
            .insert(
                "items",
                json!({"name": "dup", "status": "todo", "order": 2}),
            )
            .build(),
        None,
    )
    .await
    .unwrap();

    let txn = Mutation::new()
        .upsert(
            "items",
            "by_name",
            &[json!("dup")],
            json!({"name": "dup", "status": "todo", "order": 1}),
            json!({"order": 9}),
        )
        .build();
    let err = c.mutate(&txn, None).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::PreconditionFailed);
    assert!(err.message.contains("multiple"), "got: {}", err.message);
}

// ---- mutate: transactions ----------------------------------------

#[tokio::test]
async fn txn_runs_multi_steps_and_returns_one_result_per_step() {
    // Mirrors TS "runs a multi-step txn and returns one result per step".
    let mut c = new_client();
    let txn = Mutation::new()
        .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
        .insert("items", json!({"name": "b", "status": "todo", "order": 2}))
        .build();
    let results = c.mutate(&txn, None).await.expect("mutate ok");
    assert_eq!(results.len(), 2, "one result per step");
    for r in &results {
        match r {
            StepResult::Insert { id } => assert!(is_hex_id(&json!(id.clone()))),
            other => panic!("expected Insert, got {other:?}"),
        }
    }
    let docs = c.collect_all("items");
    assert_eq!(docs.len(), 2, "both inserts landed");
}

#[tokio::test]
async fn txn_patch_inside_txn_bumps_version() {
    // Mirrors TS "patches a doc inside a txn and bumps its version".
    let mut c = new_client();
    let r = c
        .mutate(
            &Mutation::new()
                .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
                .build(),
            None,
        )
        .await
        .unwrap();
    let id = match &r[0] {
        StepResult::Insert { id } => id.clone(),
        _ => unreachable!(),
    };

    // patch then expectVersion=2 (the patch bumps to 2 inside the same txn).
    let patch_txn = Mutation::new()
        .patch("items", &id, json!({"order": 9}))
        .expect_version("items", &id, 2)
        .build();
    c.mutate(&patch_txn, None).await.expect("patch txn ok");

    let doc = c.get("items", &id).expect("doc present");
    assert_eq!(doc["order"], 9);
    assert_eq!(doc["_version"], 2);
}

#[tokio::test]
async fn txn_rolls_back_on_later_step_failure() {
    // Mirrors TS "rolls back the whole txn when a later step fails".
    let mut c = new_client();
    let r = c
        .mutate(
            &Mutation::new()
                .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
                .build(),
            None,
        )
        .await
        .unwrap();
    let id = match &r[0] {
        StepResult::Insert { id } => id.clone(),
        _ => unreachable!(),
    };

    let bad_txn = Mutation::new()
        .insert("items", json!({"name": "b", "status": "todo", "order": 2}))
        .expect_version("items", &id, 999) // mismatch → aborts the whole txn
        .build();
    let err = c.mutate(&bad_txn, None).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::PreconditionFailed);

    // Atomicity: the second insert was rolled back; only the original "a"
    // remains.
    let docs = c.collect_all("items");
    assert_eq!(docs.len(), 1, "rollback removed the second insert");
    assert_eq!(docs[0]["name"], "a");
}

#[tokio::test]
async fn txn_rejects_more_than_max_steps() {
    // MAX_STEPS guard (mirror `executeTransaction` :546-548).
    let mut c = new_client();
    let mut m = Mutation::new();
    for _ in 0..(MAX_STEPS + 1) {
        m = m.insert("items", json!({"name": "x", "status": "todo", "order": 1}));
    }
    let txn = m.build();
    let err = c.mutate(&txn, None).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("maximum"), "got: {}", err.message);
}

#[tokio::test]
async fn txn_rejects_nested_tree_exceeding_recursive_step_budget() {
    // FM-28: flat length is 1 (just the schedule step); the recursive count
    // is 1 + 1025 = 1026 > MAX_STEPS — a nested tree can't smuggle past the
    // flat cap. Rejected pre-execution, so no doc lands and no job enqueues.
    let mut c = new_client();
    let mut nested = Mutation::new();
    for i in 0..=MAX_STEPS {
        nested = nested.insert(
            "items",
            json!({"name": format!("n{i}"), "status": "todo", "order": i}),
        );
    }
    let txn = Mutation::new()
        .schedule(ScheduleWhen::AfterMs { ms: 1000 }, nested.build())
        .build();
    let err = c.mutate(&txn, None).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("maximum"), "got: {}", err.message);
    assert!(c.collect_all("items").is_empty(), "no doc applied");
    assert!(c.list_schedules().is_empty(), "no job enqueued");
}

#[tokio::test]
async fn txn_accepts_more_than_256_steps() {
    // ARC-104: the server raised MAX_STEPS 256 -> 1024; the in-memory engine
    // must accept a 300-step txn (previously over the stale 256 cap).
    let mut c = new_client();
    let mut m = Mutation::new();
    for i in 0..300 {
        m = m.insert(
            "items",
            json!({"name": format!("n{i}"), "status": "todo", "order": i}),
        );
    }
    let txn = m.build();
    let results = c.mutate(&txn, None).await.expect("300-step txn accepted");
    assert_eq!(results.len(), 300);
}

#[tokio::test]
async fn mut_id_caches_results_and_short_circuits() {
    // Brief: port the TS `mutId` idempotency-key semantics (mutate :40-47).
    let mut c = new_client();
    let txn = Mutation::new()
        .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
        .build();

    let r1 = c.mutate(&txn, Some("m1")).await.expect("first ok");
    let r2 = c.mutate(&txn, Some("m1")).await.expect("cached ok");
    assert_eq!(r1.len(), 1);
    assert_eq!(r2.len(), 1);
    // The cached result is byte-identical to the first call — same id.
    let id1 = match &r1[0] {
        StepResult::Insert { id } => id.clone(),
        _ => unreachable!(),
    };
    let id2 = match &r2[0] {
        StepResult::Insert { id } => id.clone(),
        _ => unreachable!(),
    };
    assert_eq!(id1, id2, "cached mut_id returned the same id");
    // The cache short-circuits execution, so only one doc was actually
    // stored — the second `mutate` did not run the txn again.
    assert_eq!(c.collect_all("items").len(), 1);
}

// ---- mutate: patchByQuery / deleteByQuery -----------------------

#[tokio::test]
async fn patch_by_query_patches_every_match_and_reports_count() {
    let mut c = new_client();
    seed_query_rows(&mut c).await; // three "todo" rows (orders 3,1,2)
    let results = c
        .mutate(
            &Mutation::new()
                .patch_by_query(
                    "items",
                    FilterExpr::Eq {
                        field: "status".into(),
                        value: json!("todo"),
                    },
                    json!({"status": "done"}),
                    None,
                )
                .build(),
            None,
        )
        .await
        .expect("patchByQuery ok");
    assert_eq!(results.len(), 1);
    match &results[0] {
        StepResult::PatchByQuery { patched, truncated } => {
            assert_eq!(*patched, 3);
            assert!(!*truncated);
        }
        other => panic!("expected PatchByQuery, got {other:?}"),
    }
    // Every matching row was patched; no "todo" remains.
    let docs = c.collect_all("items");
    assert_eq!(docs.len(), 3);
    assert!(docs.iter().all(|d| d["status"] == "done"));
}

#[tokio::test]
async fn delete_by_query_removes_matches_and_reports_count() {
    let mut c = new_client();
    seed_query_rows(&mut c).await; // three "todo" rows
    let results = c
        .mutate(
            &Mutation::new()
                .delete_by_query(
                    "items",
                    FilterExpr::Eq {
                        field: "status".into(),
                        value: json!("todo"),
                    },
                    None,
                )
                .build(),
            None,
        )
        .await
        .expect("deleteByQuery ok");
    assert_eq!(results.len(), 1);
    match &results[0] {
        StepResult::DeleteByQuery { deleted, truncated } => {
            assert_eq!(*deleted, 3);
            assert!(!*truncated);
        }
        other => panic!("expected DeleteByQuery, got {other:?}"),
    }
    assert!(c.collect_all("items").is_empty());
}

#[tokio::test]
async fn patch_by_query_truncates_at_limit() {
    let mut c = new_client();
    seed_query_rows(&mut c).await; // three "todo" rows
    // limit below the match set: patches exactly `limit` and reports
    // truncated.
    let results = c
        .mutate(
            &Mutation::new()
                .patch_by_query(
                    "items",
                    FilterExpr::Eq {
                        field: "status".into(),
                        value: json!("todo"),
                    },
                    json!({"status": "done"}),
                    Some(2),
                )
                .build(),
            None,
        )
        .await
        .expect("patchByQuery ok");
    match &results[0] {
        StepResult::PatchByQuery { patched, truncated } => {
            assert_eq!(*patched, 2);
            assert!(*truncated, "match set (3) exceeded limit (2)");
        }
        other => panic!("expected PatchByQuery, got {other:?}"),
    }
    // Two patched, one still "todo".
    let docs = c.collect_all("items");
    let done = docs.iter().filter(|d| d["status"] == "done").count();
    let todo = docs.iter().filter(|d| d["status"] == "todo").count();
    assert_eq!(done, 2);
    assert_eq!(todo, 1);
}

#[tokio::test]
async fn patch_by_query_zero_matches_reports_zero_not_truncated() {
    let mut c = new_client();
    seed_query_rows(&mut c).await;
    let results = c
        .mutate(
            &Mutation::new()
                .patch_by_query(
                    "items",
                    FilterExpr::Eq {
                        field: "status".into(),
                        value: json!("missing"),
                    },
                    json!({"status": "done"}),
                    None,
                )
                .build(),
            None,
        )
        .await
        .expect("patchByQuery ok");
    match &results[0] {
        StepResult::PatchByQuery { patched, truncated } => {
            assert_eq!(*patched, 0);
            assert!(!*truncated);
        }
        other => panic!("expected PatchByQuery, got {other:?}"),
    }
    // Nothing changed.
    assert_eq!(c.collect_all("items").len(), 3);
}

#[tokio::test]
async fn sec104_rejects_over_budget_by_query_step_count() {
    // Mirrors server `sec104_rejects_over_budget_by_query_step_count`. A
    // txn with MAX_BY_QUERY_STEPS_PER_TXN+1 patchByQuery steps is rejected
    // at the top of execute_transaction, before any step applies. The
    // original AUDIT finding was 1024 by-query steps (~1M-row single-writer
    // stall); the 16-step cap rejects it pre-execution.
    let mut c = new_client();
    c.mutate(
        &Mutation::new()
            .insert(
                "items",
                json!({"name": "seed", "status": "todo", "order": 0}),
            )
            .build(),
        None,
    )
    .await
    .unwrap();
    let mut m = Mutation::new();
    for i in 0..=(MAX_BY_QUERY_STEPS_PER_TXN as i32) {
        m = m.patch_by_query(
            "items",
            FilterExpr::Eq {
                field: "status".into(),
                value: json!("todo"),
            },
            json!({"order": i}),
            None,
        );
    }
    let err = c.mutate(&m.build(), None).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("by-query steps"),
        "got: {}",
        err.message
    );
    // Pre-execution rejection commits nothing.
    let docs = c.collect_all("items");
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["order"], 0);
}

#[tokio::test]
async fn sec104_rejects_over_budget_aggregate_affected() {
    // Mirrors server `sec104_rejects_over_budget_aggregate_affected`. A
    // txn with few by-query steps (under the step cap) but each at the
    // default 1000-row limit can still exceed MAX_AFFECTED_ROWS_PER_TXN;
    // reject it before any step applies.
    let over_steps = (MAX_AFFECTED_ROWS_PER_TXN / 1000) + 1;
    assert!(over_steps <= MAX_BY_QUERY_STEPS_PER_TXN);
    let mut c = new_client();
    c.mutate(
        &Mutation::new()
            .insert(
                "items",
                json!({"name": "seed", "status": "todo", "order": 0}),
            )
            .build(),
        None,
    )
    .await
    .unwrap();
    let mut m = Mutation::new();
    for _ in 0..over_steps {
        m = m.delete_by_query(
            "items",
            FilterExpr::Eq {
                field: "status".into(),
                value: json!("todo"),
            },
            None,
        );
    }
    let err = c.mutate(&m.build(), None).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("affect up to"), "got: {}", err.message);
    // Pre-execution rejection commits nothing.
    assert_eq!(c.collect_all("items").len(), 1);
}
