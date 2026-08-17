use super::*;

// ---- FM-33: cascade delete + soft delete ---------------------------
//
// Mirrors `server/tests/cascade_test.rs` against the harness: the app-level
// `onDelete` rules (cascade/restrict/setNull with a cycle guard and the
// MAX_CASCADE_ROWS budget), soft-delete stamps (invisible to every read/write
// lookup, excluded from unique indexes), `undelete`, and the TTL reaper's
// force-hard cascade. Gaps with no harness equivalent (per-row ownerField
// auth, includeDeleted admin read) stay on the server suite.

/// A client with `schema` pushed and a deterministic clock/RNG — the same
/// recipe as [`new_client`], parameterized for the FM-33 fixtures.
fn fm33_client(schema: &SchemaDef) -> InMemoryRtDbClient {
    let counter = Arc::new(Mutex::new(1_700_000_000_000_i64));
    let mut client = InMemoryRtDbClient::new(
        InMemoryRtDbClientOptions::default()
            .now(move || {
                let mut g = counter.lock().expect("counter not poisoned");
                let v = *g;
                *g += 1;
                v
            })
            .random(|| 0.0),
    );
    client.push_schema(schema).unwrap();
    client
}

/// Insert one doc and return its id.
async fn fm33_put(c: &mut InMemoryRtDbClient, table: &str, doc: Value) -> String {
    let results = c
        .mutate(&Mutation::new().insert(table, doc).build(), None)
        .await
        .expect("insert ok");
    match &results[0] {
        StepResult::Insert { id } => id.clone(),
        other => panic!("expected Insert, got {other:?}"),
    }
}

/// The `children.parentId` type for the FK fixture: an id referencing
/// `parents` with `onDelete: action`, wrapped in one `optional` when the
/// action is `setNull` (the only action that must be able to hold null).
/// Mirrors `id_field` in `server/tests/cascade_test.rs`.
fn fk_id_field(action: OnDeleteAction) -> FieldType {
    let id = FieldType::id("parents").on_delete(action);
    if matches!(action, OnDeleteAction::SetNull) {
        FieldType::optional(id)
    } else {
        id
    }
}

/// parents + children with `children.parentId` declaring `onDelete: action`.
/// Mirrors `fk_schema` (`server/tests/cascade_test.rs:44`).
fn fk_schema(action: OnDeleteAction) -> SchemaDef {
    Schema::builder()
        .table("parents", Table::new().field("title", FieldType::String))
        .table(
            "children",
            Table::new()
                .field("note", FieldType::String)
                .field("parentId", fk_id_field(action))
                .index("by_parent", &["parentId"]),
        )
        .build()
}

/// The v1 shape the additive-push test upgrades from: same FK, no onDelete.
/// Mirrors `fk_schema_without_on_delete`.
fn fk_schema_without_on_delete() -> SchemaDef {
    Schema::builder()
        .table("parents", Table::new().field("title", FieldType::String))
        .table(
            "children",
            Table::new()
                .field("note", FieldType::String)
                .field("parentId", FieldType::id("parents"))
                .index("by_parent", &["parentId"]),
        )
        .build()
}

/// Self-referencing `nodes` (optional parentId → nodes, cascade) for the
/// cycle-termination test. Mirrors `self_ref_schema`.
fn self_ref_schema() -> SchemaDef {
    Schema::builder()
        .table(
            "nodes",
            Table::new()
                .field("name", FieldType::String)
                .field(
                    "parentId",
                    FieldType::optional(FieldType::id("nodes").on_delete(OnDeleteAction::Cascade)),
                )
                .index("by_parent", &["parentId"]),
        )
        .build()
}

/// parents + children(`onDelete: action`) where `children` declares
/// `softDelete`, plus grandchildren referencing children (cascade) — proves
/// the cascade stops at a stamped row. Mirrors `soft_child_cascade_schema`.
fn soft_child_schema(action: OnDeleteAction) -> SchemaDef {
    Schema::builder()
        .table("parents", Table::new().field("title", FieldType::String))
        .table(
            "children",
            Table::new()
                .field("note", FieldType::String)
                .field("parentId", fk_id_field(action))
                .index("by_parent", &["parentId"])
                .soft_delete(),
        )
        .table(
            "grandchildren",
            Table::new().field("note", FieldType::String).field(
                "childId",
                FieldType::id("children").on_delete(OnDeleteAction::Cascade),
            ),
        )
        .build()
}

/// A soft-delete tasks table with a unique index plus a hard `plain` table.
/// Mirrors `soft_tasks_schema` (unique `by_name` is the partial-index seam).
fn soft_tasks_schema() -> SchemaDef {
    Schema::builder()
        .table(
            "tasks",
            Table::new()
                .field("name", FieldType::String)
                .field("done", FieldType::Boolean)
                .index("by_name", &["name"])
                .unique()
                .index("by_done", &["done"])
                .soft_delete(),
        )
        .table("plain", Table::new().field("note", FieldType::String))
        .build()
}

/// The `soft_tasks_schema` shape with the softDelete flag togglable — the
/// flag-add re-push test upgrades from `false` to `true`.
fn soft_tasks_schema_flag(soft: bool) -> SchemaDef {
    let mut t = Table::new()
        .field("name", FieldType::String)
        .index("by_name", &["name"])
        .unique();
    if soft {
        t = t.soft_delete();
    }
    Schema::builder()
        .table("tasks", t)
        .table("plain", Table::new().field("note", FieldType::String))
        .build()
}

/// sessions (ttl on expiresAt + softDelete) + children (cascade) for the
/// reaper test. Mirrors `reaper_cascade_schema`.
fn reaper_schema() -> SchemaDef {
    Schema::builder()
        .table(
            "sessions",
            Table::new()
                .field("kind", FieldType::String)
                .field("expiresAt", FieldType::Number)
                .ttl("expiresAt", None)
                .soft_delete(),
        )
        .table(
            "children",
            Table::new().field("note", FieldType::String).field(
                "sessionId",
                FieldType::id("sessions").on_delete(OnDeleteAction::Cascade),
            ),
        )
        .build()
}

#[tokio::test]
async fn cascade_delete_removes_children() {
    let mut c = fm33_client(&fk_schema(OnDeleteAction::Cascade));
    let pid = fm33_put(&mut c, "parents", json!({"title": "p"})).await;
    let c1 = fm33_put(&mut c, "children", json!({"note": "a", "parentId": pid})).await;
    let _c2 = fm33_put(&mut c, "children", json!({"note": "b", "parentId": pid})).await;
    c.mutate(&Mutation::new().delete("parents", &pid).build(), None)
        .await
        .expect("cascade delete ok");
    assert!(c.get("parents", &pid).is_none());
    assert!(c.get("children", &c1).is_none());
    assert!(c.collect_all("children").is_empty());
}

#[tokio::test]
async fn restrict_delete_conflicts_and_rolls_back_the_txn() {
    let mut c = fm33_client(&fk_schema(OnDeleteAction::Restrict));
    let p1 = fm33_put(&mut c, "parents", json!({"title": "free"})).await;
    let p2 = fm33_put(&mut c, "parents", json!({"title": "held"})).await;
    let ch = fm33_put(&mut c, "children", json!({"note": "a", "parentId": p2})).await;

    // Two-step txn: the first delete succeeds, the second restricts — the
    // whole txn must roll back atomically.
    let err = c
        .mutate(
            &Mutation::new()
                .delete("parents", &p1)
                .delete("parents", &p2)
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err.code, ErrorCode::Conflict),
        "got: {:?} ({err})",
        err.code
    );
    assert_eq!(
        err.message,
        format!("cannot delete 'parents': 'children.parentId' is referenced by document '{ch}'")
    );
    // Rolled back: both parents survive.
    assert!(c.get("parents", &p1).is_some());
    assert!(c.get("parents", &p2).is_some());

    // Deleting the child unblocks the parent delete.
    c.mutate(&Mutation::new().delete("children", &ch).build(), None)
        .await
        .expect("child delete ok");
    c.mutate(&Mutation::new().delete("parents", &p2).build(), None)
        .await
        .expect("parent delete ok after child removed");
    assert!(c.get("parents", &p2).is_none());
}

#[tokio::test]
async fn set_null_removes_the_reference_key_and_bumps_version() {
    let mut c = fm33_client(&fk_schema(OnDeleteAction::SetNull));
    let pid = fm33_put(&mut c, "parents", json!({"title": "p"})).await;
    let ch = fm33_put(&mut c, "children", json!({"note": "a", "parentId": pid})).await;

    c.mutate(&Mutation::new().delete("parents", &pid).build(), None)
        .await
        .expect("setNull delete ok");
    assert!(c.get("parents", &pid).is_none());
    // The child survives with the reference key REMOVED (patch-null unset
    // semantics — never a present-but-null key) and `_version` bumped.
    let doc = c.get("children", &ch).expect("child survives setNull");
    assert!(doc.get("parentId").is_none(), "got: {doc}");
    assert_eq!(doc["_version"], 2);
}

#[tokio::test]
async fn cascade_self_reference_cycle_terminates() {
    let mut c = fm33_client(&self_ref_schema());
    let a = fm33_put(&mut c, "nodes", json!({"name": "a"})).await;
    let b = fm33_put(&mut c, "nodes", json!({"name": "b", "parentId": a})).await;
    let d = fm33_put(&mut c, "nodes", json!({"name": "d", "parentId": b})).await;
    // Close the cycle: a → b → d → a. The visited guard must terminate it.
    c.mutate(
        &Mutation::new()
            .patch("nodes", &a, json!({"parentId": d}))
            .build(),
        None,
    )
    .await
    .expect("cycle patch ok");

    c.mutate(&Mutation::new().delete("nodes", &a).build(), None)
        .await
        .expect("cycle delete terminates");
    assert!(c.collect_all("nodes").is_empty(), "every node removed");
}

#[tokio::test]
async fn soft_delete_child_is_stamped_and_cascade_stops_there() {
    let mut c = fm33_client(&soft_child_schema(OnDeleteAction::Cascade));
    let pid = fm33_put(&mut c, "parents", json!({"title": "p"})).await;
    let ch = fm33_put(&mut c, "children", json!({"note": "a", "parentId": pid})).await;
    let g = fm33_put(&mut c, "grandchildren", json!({"note": "g", "childId": ch})).await;

    c.mutate(&Mutation::new().delete("parents", &pid).build(), None)
        .await
        .expect("cascade into soft child ok");
    // The parent is hard-deleted; the soft child is stamped (not removed) and
    // the recursion STOPS — the grandchild referencing it is untouched.
    assert!(c.get("parents", &pid).is_none());
    assert!(c.get("children", &ch).is_none());
    let row = c
        .docs
        .get(&("children".to_string(), ch.clone()))
        .expect("soft child still stored");
    assert!(row.deleted_at.is_some());
    assert_eq!(row.version, 2);
    assert!(c.get("grandchildren", &g).is_some());
}

#[tokio::test]
async fn soft_deleted_child_is_invisible_to_cascade() {
    let mut c = fm33_client(&soft_child_schema(OnDeleteAction::Cascade));
    let pid = fm33_put(&mut c, "parents", json!({"title": "p"})).await;
    let gone = fm33_put(&mut c, "children", json!({"note": "gone", "parentId": pid})).await;
    let live = fm33_put(&mut c, "children", json!({"note": "live", "parentId": pid})).await;
    // Soft-delete one child first (never a cascade trigger).
    c.mutate(&Mutation::new().delete("children", &gone).build(), None)
        .await
        .expect("soft delete child ok");

    // The parent's cascade sees only the live child — encountering the
    // stamped row would be a NotFound abort, so success proves invisibility.
    c.mutate(&Mutation::new().delete("parents", &pid).build(), None)
        .await
        .expect("cascade skips stamped child");
    let stamped_first = c
        .docs
        .get(&("children".to_string(), gone.clone()))
        .expect("first child stored");
    assert_eq!(stamped_first.version, 2, "stamped exactly once");
    let stamped_second = c
        .docs
        .get(&("children".to_string(), live.clone()))
        .expect("second child stored");
    assert!(stamped_second.deleted_at.is_some());
    assert!(c.get("parents", &pid).is_none());
}

#[tokio::test]
async fn soft_deleted_child_is_invisible_to_restrict() {
    let mut c = fm33_client(&soft_child_schema(OnDeleteAction::Restrict));
    let pid = fm33_put(&mut c, "parents", json!({"title": "p"})).await;
    let ch = fm33_put(&mut c, "children", json!({"note": "a", "parentId": pid})).await;
    c.mutate(&Mutation::new().delete("children", &ch).build(), None)
        .await
        .expect("soft delete child ok");

    // No LIVE child references the parent — restrict does not block.
    c.mutate(&Mutation::new().delete("parents", &pid).build(), None)
        .await
        .expect("restrict ignores stamped child");
    assert!(c.get("parents", &pid).is_none());
}

#[tokio::test]
async fn delete_by_query_cascades_each_matched_row() {
    let mut c = fm33_client(&fk_schema(OnDeleteAction::Cascade));
    let p1 = fm33_put(&mut c, "parents", json!({"title": "del1"})).await;
    let p2 = fm33_put(&mut c, "parents", json!({"title": "del2"})).await;
    let p3 = fm33_put(&mut c, "parents", json!({"title": "keep"})).await;
    let _c1 = fm33_put(&mut c, "children", json!({"note": "a", "parentId": p1})).await;
    let _c2 = fm33_put(&mut c, "children", json!({"note": "b", "parentId": p2})).await;
    let c3 = fm33_put(&mut c, "children", json!({"note": "c", "parentId": p3})).await;

    let results = c
        .mutate(
            &Mutation::new()
                .delete_by_query(
                    "parents",
                    FilterExpr::In {
                        field: "title".into(),
                        values: vec![json!("del1"), json!("del2")],
                    },
                    None,
                )
                .build(),
            None,
        )
        .await
        .expect("deleteByQuery ok");
    match &results[0] {
        StepResult::DeleteByQuery { deleted, truncated } => {
            assert_eq!(*deleted, 2);
            assert!(!truncated);
        }
        other => panic!("expected DeleteByQuery, got {other:?}"),
    }
    assert!(c.get("parents", &p3).is_some());
    let remaining = c.collect_all("parents");
    assert_eq!(remaining.len(), 1);
    // Each matched parent's cascade removed its child; the keeper's survived.
    assert!(c.get("children", &c3).is_some());
    assert_eq!(c.collect_all("children").len(), 1);
}

#[tokio::test]
async fn delete_by_query_skips_rows_already_cascaded() {
    // Self-referencing cascade + deleteByQuery matching both rows: the root's
    // cascade removes the dependent, and the shared visited set skips it when
    // the loop reaches it — no NotFound abort.
    let mut c = fm33_client(&self_ref_schema());
    let a1 = fm33_put(&mut c, "nodes", json!({"name": "a1"})).await;
    let _a2 = fm33_put(&mut c, "nodes", json!({"name": "a2", "parentId": a1})).await;

    let results = c
        .mutate(
            &Mutation::new()
                .delete_by_query(
                    "nodes",
                    FilterExpr::In {
                        field: "name".into(),
                        values: vec![json!("a1"), json!("a2")],
                    },
                    None,
                )
                .build(),
            None,
        )
        .await
        .expect("deleteByQuery skips already-cascaded rows");
    match &results[0] {
        StepResult::DeleteByQuery { deleted, .. } => assert_eq!(*deleted, 2),
        other => panic!("expected DeleteByQuery, got {other:?}"),
    }
    assert!(c.collect_all("nodes").is_empty());
}

#[tokio::test]
async fn delete_by_query_on_soft_table_stamps_rows() {
    let mut c = fm33_client(&soft_tasks_schema());
    let t1 = fm33_put(&mut c, "tasks", json!({"name": "one", "done": false})).await;
    let t2 = fm33_put(&mut c, "tasks", json!({"name": "two", "done": false})).await;
    let t3 = fm33_put(&mut c, "tasks", json!({"name": "three", "done": true})).await;

    let results = c
        .mutate(
            &Mutation::new()
                .delete_by_query(
                    "tasks",
                    FilterExpr::Eq {
                        field: "done".into(),
                        value: json!(false),
                    },
                    None,
                )
                .build(),
            None,
        )
        .await
        .expect("deleteByQuery on soft table ok");
    match &results[0] {
        StepResult::DeleteByQuery { deleted, .. } => assert_eq!(*deleted, 2),
        other => panic!("expected DeleteByQuery, got {other:?}"),
    }
    for id in [&t1, &t2] {
        let row = c.docs.get(&("tasks".to_string(), id.clone())).unwrap();
        assert!(row.deleted_at.is_some(), "stamped, not removed");
    }
    assert!(c.get("tasks", &t3).is_some());
    let count = c
        .run_query(&TableQuery::new("tasks").count())
        .expect("count ok");
    assert_eq!(count, json!(1));
}

#[tokio::test]
async fn soft_delete_stamps_version_and_hides_from_reads() {
    let mut c = fm33_client(&soft_tasks_schema());
    let t1 = fm33_put(&mut c, "tasks", json!({"name": "one", "done": false})).await;
    assert_eq!(c.get("tasks", &t1).unwrap()["_version"], 1);

    c.mutate(&Mutation::new().delete("tasks", &t1).build(), None)
        .await
        .expect("soft delete ok");
    // Invisible to every read terminal...
    assert!(c.get("tasks", &t1).is_none());
    assert!(c.collect_all("tasks").is_empty());
    assert_eq!(
        c.run_query(&TableQuery::new("tasks").count()).unwrap(),
        json!(0)
    );
    // ...but still stored, stamped, with `_version` bumped (a stale client
    // copy fails OCC against the stamped row).
    let row = c.docs.get(&("tasks".to_string(), t1.clone())).unwrap();
    assert!(row.deleted_at.is_some());
    assert_eq!(row.version, 2);

    // Double delete: the stamped row is as absent as a removed one.
    let err = c
        .mutate(&Mutation::new().delete("tasks", &t1).build(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err.code, ErrorCode::NotFound),
        "got: {:?} ({err})",
        err.code
    );
    assert_eq!(err.message, format!("document '{t1}' not found"));
}

#[tokio::test]
async fn soft_deleted_row_is_excluded_from_unique_index_and_undelete_conflicts() {
    let mut c = fm33_client(&soft_tasks_schema());
    let t1 = fm33_put(&mut c, "tasks", json!({"name": "dup", "done": false})).await;
    c.mutate(&Mutation::new().delete("tasks", &t1).build(), None)
        .await
        .expect("soft delete ok");

    // The stamped row is outside the partial unique index — the key is
    // re-insertable while soft-deleted.
    let t2 = fm33_put(&mut c, "tasks", json!({"name": "dup", "done": false})).await;
    assert_ne!(t1, t2);

    // Restoring re-enters the unique index, which t2 now holds → Conflict.
    let err = c
        .mutate(&Mutation::new().undelete("tasks", &t1).build(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err.code, ErrorCode::Conflict),
        "got: {:?} ({err})",
        err.code
    );
    assert!(err.message.contains("by_name"), "got: {err}");

    // Remove the holder and the restore goes through.
    c.mutate(&Mutation::new().delete("tasks", &t2).build(), None)
        .await
        .expect("soft delete holder ok");
    c.mutate(&Mutation::new().undelete("tasks", &t1).build(), None)
        .await
        .expect("undelete ok after holder removed");
    let doc = c.get("tasks", &t1).expect("restored");
    assert_eq!(doc["name"], "dup");
    assert_eq!(doc["_version"], 3, "insert(1) + stamp(2) + restore(3)");
}

#[tokio::test]
async fn undelete_is_idempotent_absent_not_found_and_requires_soft_delete() {
    let mut c = fm33_client(&soft_tasks_schema());
    let t1 = fm33_put(&mut c, "tasks", json!({"name": "one", "done": false})).await;
    c.mutate(&Mutation::new().delete("tasks", &t1).build(), None)
        .await
        .expect("soft delete ok");

    // Absent id → NotFound.
    let err = c
        .mutate(&Mutation::new().undelete("tasks", "missing").build(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err.code, ErrorCode::NotFound),
        "got: {:?} ({err})",
        err.code
    );
    assert_eq!(err.message, "document 'missing' not found");

    // Restore → Ok; a second undelete of the now-live row is idempotent.
    c.mutate(&Mutation::new().undelete("tasks", &t1).build(), None)
        .await
        .expect("undelete ok");
    let v = c.get("tasks", &t1).unwrap()["_version"].clone();
    c.mutate(&Mutation::new().undelete("tasks", &t1).build(), None)
        .await
        .expect("undelete of a live row is idempotent");
    assert_eq!(c.get("tasks", &t1).unwrap()["_version"], v);

    // A table that does not declare softDelete → BadRequest.
    let p1 = fm33_put(&mut c, "plain", json!({"note": "x"})).await;
    let err = c
        .mutate(&Mutation::new().undelete("plain", &p1).build(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err.code, ErrorCode::BadRequest),
        "got: {:?} ({err})",
        err.code
    );
    assert_eq!(
        err.message,
        "table 'plain' does not declare softDelete".to_string()
    );
}

#[tokio::test]
async fn upsert_over_soft_deleted_key_inserts_fresh() {
    let mut c = fm33_client(&soft_tasks_schema());
    let t1 = fm33_put(&mut c, "tasks", json!({"name": "u", "done": false})).await;
    c.mutate(&Mutation::new().delete("tasks", &t1).build(), None)
        .await
        .expect("soft delete ok");

    // The stamped row is invisible to the upsert's index lookup → the insert
    // arm wins: a fresh row, two physical rows carrying the same key.
    let results = c
        .mutate(
            &Mutation::new()
                .upsert(
                    "tasks",
                    "by_name",
                    &[json!("u")],
                    json!({"name": "u", "done": true}),
                    json!({"done": false}),
                )
                .build(),
            None,
        )
        .await
        .expect("upsert over soft-deleted key ok");
    match &results[0] {
        StepResult::Upsert { id, inserted } => {
            assert!(inserted, "insert arm wins over a stamped row");
            assert_ne!(id, &t1);
        }
        other => panic!("expected Upsert, got {other:?}"),
    }
    let physical = c.docs.keys().filter(|(t, _)| t == "tasks").count();
    assert_eq!(physical, 2);
    let live = c.collect_all("tasks");
    assert_eq!(live.len(), 1);
    assert_eq!(live[0]["done"], json!(true));
}

#[tokio::test]
async fn expect_absent_treats_soft_deleted_as_absent() {
    let mut c = fm33_client(&soft_tasks_schema());
    let t1 = fm33_put(&mut c, "tasks", json!({"name": "ea", "done": false})).await;
    c.mutate(&Mutation::new().delete("tasks", &t1).build(), None)
        .await
        .expect("soft delete ok");

    // Stamped row absent → expectAbsent passes and the key is re-insertable.
    c.mutate(
        &Mutation::new()
            .expect_absent("tasks", "by_name", &[json!("ea")])
            .insert("tasks", json!({"name": "ea", "done": true}))
            .build(),
        None,
    )
    .await
    .expect("expectAbsent over a stamped row passes");

    // A LIVE holder blocks it.
    let err = c
        .mutate(
            &Mutation::new()
                .expect_absent("tasks", "by_name", &[json!("ea")])
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err.code, ErrorCode::PreconditionFailed),
        "got: {:?} ({err})",
        err.code
    );
    assert_eq!(
        err.message,
        "index 'by_name' already has a matching document".to_string()
    );
}

#[tokio::test]
async fn expect_version_patch_and_replace_treat_soft_deleted_as_absent() {
    let mut c = fm33_client(&soft_tasks_schema());
    let t1 = fm33_put(&mut c, "tasks", json!({"name": "one", "done": false})).await;
    c.mutate(&Mutation::new().delete("tasks", &t1).build(), None)
        .await
        .expect("soft delete ok");

    // Every per-id write/observe path sees NotFound, not the stamped row —
    // even with the version that row now carries.
    for txn in [
        Mutation::new().expect_version("tasks", &t1, 2).build(),
        Mutation::new()
            .patch("tasks", &t1, json!({"done": true}))
            .build(),
        Mutation::new()
            .replace("tasks", &t1, json!({"name": "one", "done": true}))
            .build(),
    ] {
        let err = c.mutate(&txn, None).await.unwrap_err();
        assert!(
            matches!(err.code, ErrorCode::NotFound),
            "got: {:?} ({err})",
            err.code
        );
        assert_eq!(err.message, format!("document '{t1}' not found"));
    }
}

#[tokio::test]
async fn adding_or_changing_on_delete_is_an_additive_push() {
    // v1 has no onDelete; v2 adds cascade; v3 changes it to restrict — all
    // additive (onDelete alters runtime delete behavior only), and the newly
    // pushed action governs the next delete.
    let mut c = fm33_client(&fk_schema_without_on_delete());
    let pid = fm33_put(&mut c, "parents", json!({"title": "p"})).await;
    let _ch = fm33_put(&mut c, "children", json!({"note": "a", "parentId": pid})).await;

    c.push_schema(&fk_schema(OnDeleteAction::Cascade))
        .expect("adding onDelete is additive");
    c.push_schema(&fk_schema(OnDeleteAction::Restrict))
        .expect("changing onDelete is additive");

    let err = c
        .mutate(&Mutation::new().delete("parents", &pid).build(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err.code, ErrorCode::Conflict),
        "restrict now governs: {:?} ({err})",
        err.code
    );
    assert!(err.message.contains("is referenced by document"));
}

#[tokio::test]
async fn adding_soft_delete_flag_is_an_additive_push() {
    // v1 (hard): the unique index blocks a second "dup". v2 adds softDelete —
    // a flag-only change is additive — and the stamped row leaves the unique
    // index, so the key becomes re-insertable.
    let mut c = fm33_client(&soft_tasks_schema_flag(false));
    let t1 = fm33_put(&mut c, "tasks", json!({"name": "dup"})).await;
    let err = c
        .mutate(
            &Mutation::new()
                .insert("tasks", json!({"name": "dup"}))
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err.code, ErrorCode::Conflict),
        "got: {:?} ({err})",
        err.code
    );

    c.push_schema(&soft_tasks_schema_flag(true))
        .expect("softDelete flag-add is additive");
    c.mutate(&Mutation::new().delete("tasks", &t1).build(), None)
        .await
        .expect("now a soft delete");
    let _t2 = fm33_put(&mut c, "tasks", json!({"name": "dup"})).await;
    assert_eq!(
        c.docs.keys().filter(|(t, _)| t == "tasks").count(),
        2,
        "two physical rows share the unique key while one is stamped"
    );
}

#[tokio::test]
async fn reaper_hard_deletes_soft_delete_table_and_cascades_children() {
    let mut c = fm33_client(&reaper_schema());
    let s1 = fm33_put(&mut c, "sessions", json!({"kind": "a", "expiresAt": 5})).await;
    let _ch1 = fm33_put(&mut c, "children", json!({"note": "a", "sessionId": s1})).await;
    // A second session that is ALREADY soft-deleted when it expires — the
    // reaper is force-hard and must collect it physically.
    let s2 = fm33_put(&mut c, "sessions", json!({"kind": "b", "expiresAt": 5})).await;
    c.mutate(&Mutation::new().delete("sessions", &s2).build(), None)
        .await
        .expect("soft delete s2 ok");

    let removed = c.tick(Some(1_000));
    assert_eq!(removed, 2, "both expired sessions reaped");
    assert!(
        c.docs.is_empty(),
        "sessions hard-deleted (softDelete overridden), children cascaded"
    );
}

#[tokio::test]
async fn cascade_over_budget_conflicts_and_rolls_back() {
    let mut c = fm33_client(&fk_schema(OnDeleteAction::Cascade));
    let pid = fm33_put(&mut c, "parents", json!({"title": "p"})).await;
    // Bulk-seed MAX_CASCADE_ROWS + 1 children directly (the harness analogue
    // of the server test's generate_series seed): parent + 10001 children
    // exceeds the 10000-row cascade budget.
    for i in 0..=MAX_CASCADE_ROWS {
        let id = format!("bulk{i:05}");
        c.docs.insert(
            ("children".to_string(), id.clone()),
            StoredRow {
                id,
                doc: json!({"note": "bulk", "parentId": pid}),
                version: 1,
                created_at: 1,
                deleted_at: None,
            },
        );
    }

    let err = c
        .mutate(&Mutation::new().delete("parents", &pid).build(), None)
        .await
        .unwrap_err();
    assert!(
        matches!(err.code, ErrorCode::Conflict),
        "got: {:?} ({err})",
        err.code
    );
    assert_eq!(
        err.message,
        format!("onDelete cascade exceeds the limit of {MAX_CASCADE_ROWS} rows")
    );
    // Atomic rollback: the parent and every child survive.
    assert!(c.get("parents", &pid).is_some());
    assert_eq!(
        c.docs.keys().filter(|(t, _)| t == "children").count(),
        MAX_CASCADE_ROWS + 1
    );
}

#[tokio::test]
async fn cascade_delete_fires_child_table_subscriptions() {
    let mut c = fm33_client(&fk_schema(OnDeleteAction::Cascade));
    let pid = fm33_put(&mut c, "parents", json!({"title": "p"})).await;
    let _ch = fm33_put(&mut c, "children", json!({"note": "a", "parentId": pid})).await;

    let seen: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_clone = seen.clone();
    let _h = c.subscribe(TableQuery::new("children").collect(), move |v| {
        seen_clone.lock().unwrap().push(v);
    });
    assert_eq!(seen.lock().unwrap().len(), 1, "initial value delivered");

    // The cascade writes `children` in the same step that deletes `parents`
    // — the write-set must carry every touched table so the subscriber fires.
    c.mutate(&Mutation::new().delete("parents", &pid).build(), None)
        .await
        .expect("cascade delete ok");
    let got = seen.lock().unwrap();
    assert_eq!(got.len(), 2, "cascade fired the child-table subscriber");
    assert_eq!(got.last().unwrap(), &json!([]));
}
