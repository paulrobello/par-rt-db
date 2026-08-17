use super::*;

// ---- unique / partial-unique index enforcement -------------------------
//
// Mirrors the TS `checkUniqueIndexes` suite: a `unique` index rejects a
// colliding insert/patch/replace/upsert with `Conflict`; a partial unique
// index (`where` predicate) constrains only rows matching the predicate;
// uniqueness is on declared `fields` only (never `id`/`created_at`), and a
// NULL/absent key field disables the constraint for that row (Postgres
// UNIQUE treats NULLs as distinct). Rollback reuses the snapshot/restore
// path shared with the `PreconditionFailed` checks.

fn unique_users_schema() -> SchemaDef {
    // `users(email, org, archived)` with a unique `by_email` btree index.
    Schema::builder()
        .table(
            "users",
            Table::new()
                .field("email", FieldType::String)
                .field("org", FieldType::String)
                .field("archived", FieldType::optional(FieldType::Boolean))
                .index("by_email", &["email"])
                .unique(),
        )
        .build()
}

/// A client whose injected clock advances one millisecond per call, so each
/// `new_id()` (timestamp-prefixed) mints a distinct id even for back-to-back
/// inserts in the same txn. The default options have a constant clock, which
/// collapses same-txn inserts to identical ids (HashMap self-collision).
fn unique_client() -> InMemoryRtDbClient {
    let counter = Arc::new(Mutex::new(1_700_000_000_000_i64));
    InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default().now(move || {
        let mut g = counter.lock().expect("counter not poisoned");
        let v = *g;
        *g += 1;
        v
    }))
}

fn partial_users_schema() -> SchemaDef {
    // A partial unique index: constrains `email` only for rows where
    // `archived != true` (i.e. active rows).
    Schema::builder()
        .table(
            "users",
            Table::new()
                .field("email", FieldType::String)
                .field("org", FieldType::String)
                .field("archived", FieldType::optional(FieldType::Boolean))
                .index("by_email_active", &["email"])
                .unique()
                .where_clause(FilterExpr::Neq {
                    field: "archived".into(),
                    value: json!(true),
                }),
        )
        .build()
}

/// Collect the table's stored docs as a JSON array (a bare `collect` query).
fn collect_table(c: &InMemoryRtDbClient, table: &str) -> Vec<Value> {
    let r = c
        .run_query(&Query {
            table: table.into(),
            ..Default::default()
        })
        .unwrap();
    r.as_array().expect("collect returns an array").clone()
}

#[tokio::test]
async fn unique_index_rejects_duplicate_insert_with_conflict() {
    let mut c = unique_client();
    c.push_schema(&unique_users_schema()).unwrap();
    c.mutate(
        &Mutation::new()
            .insert("users", json!({"email": "a@b.com", "org": "x"}))
            .build(),
        None,
    )
    .await
    .unwrap();
    // A second insert with the same `email` violates `by_email`.
    let err = c
        .mutate(
            &Mutation::new()
                .insert("users", json!({"email": "a@b.com", "org": "y"}))
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::Conflict);
    assert!(
        err.message.contains("unique index 'by_email' violated"),
        "got: {err}"
    );
    // The whole txn rolled back: only the first row remains.
    assert_eq!(
        collect_table(&c, "users").len(),
        1,
        "conflicting insert rolled back"
    );
}

#[tokio::test]
async fn unique_index_allows_distinct_keys() {
    let mut c = unique_client();
    c.push_schema(&unique_users_schema()).unwrap();
    c.mutate(
        &Mutation::new()
            .insert("users", json!({"email": "a@b.com", "org": "x"}))
            .insert("users", json!({"email": "c@d.com", "org": "y"}))
            .build(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(collect_table(&c, "users").len(), 2);
}

#[tokio::test]
async fn unique_index_rejects_collision_via_patch_with_conflict() {
    // Patching an existing row's `email` to a value already taken by another
    // row must Conflict (the candidate row is self-excluded by `exclude_id`).
    let mut c = unique_client();
    c.push_schema(&unique_users_schema()).unwrap();
    let res = c
        .mutate(
            &Mutation::new()
                .insert("users", json!({"email": "a@b.com", "org": "x"}))
                .insert("users", json!({"email": "c@d.com", "org": "y"}))
                .build(),
            None,
        )
        .await
        .unwrap();
    let second_id = match &res[1] {
        StepResult::Insert { id } => id.clone(),
        other => panic!("expected an insert step result, got {other:?}"),
    };
    // Patch the second row's email to collide with the first → Conflict.
    let err = c
        .mutate(
            &Mutation::new()
                .patch("users", &second_id, json!({"email": "a@b.com"}))
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::Conflict);
    // Patching to its OWN email (or any non-colliding value) is allowed —
    // the row is excluded from its own uniqueness check.
    c.mutate(
        &Mutation::new()
            .patch("users", &second_id, json!({"email": "c@d.com", "org": "z"}))
            .build(),
        None,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn unique_index_rejects_collision_via_replace_with_conflict() {
    let mut c = unique_client();
    c.push_schema(&unique_users_schema()).unwrap();
    let res = c
        .mutate(
            &Mutation::new()
                .insert("users", json!({"email": "a@b.com", "org": "x"}))
                .insert("users", json!({"email": "c@d.com", "org": "y"}))
                .build(),
            None,
        )
        .await
        .unwrap();
    let second_id = match &res[1] {
        StepResult::Insert { id } => id.clone(),
        other => panic!("expected an insert step result, got {other:?}"),
    };
    let err = c
        .mutate(
            &Mutation::new()
                .replace("users", &second_id, json!({"email": "a@b.com", "org": "y"}))
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::Conflict);
}

#[tokio::test]
async fn partial_unique_index_allows_predicate_excluded_duplicate() {
    // Predicate `archived != true`: a row with `archived: true` is excluded
    // from the constraint, so two archived rows may share an email.
    let mut c = unique_client();
    c.push_schema(&partial_users_schema()).unwrap();
    c.mutate(
        &Mutation::new()
            .insert(
                "users",
                json!({"email": "dup@b.com", "org": "x", "archived": true}),
            )
            .insert(
                "users",
                json!({"email": "dup@b.com", "org": "y", "archived": true}),
            )
            .build(),
        None,
    )
    .await
    .unwrap();
    assert_eq!(
        collect_table(&c, "users").len(),
        2,
        "archived dupes are unconstrained"
    );
}

#[tokio::test]
async fn partial_unique_index_rejects_predicate_matching_duplicate() {
    // Two active rows (archived explicitly false ⇒ `archived != true` holds)
    // sharing an email must Conflict. (A doc with `archived` absent evaluates
    // the predicate false — SQL NULL exclusion — and is unconstrained, so the
    // rows must carry `archived: false` to land inside the partial index.)
    let mut c = unique_client();
    c.push_schema(&partial_users_schema()).unwrap();
    c.mutate(
        &Mutation::new()
            .insert(
                "users",
                json!({"email": "dup@b.com", "org": "x", "archived": false}),
            )
            .build(),
        None,
    )
    .await
    .unwrap();
    let err = c
        .mutate(
            &Mutation::new()
                .insert(
                    "users",
                    json!({"email": "dup@b.com", "org": "y", "archived": false}),
                )
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::Conflict);
    assert!(
        err.message
            .contains("unique index 'by_email_active' violated"),
        "got: {err}"
    );
}
