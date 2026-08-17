use super::*;

// ---- schema push ---------------------------------------------------

#[test]
fn push_schema_stores_the_schema() {
    // Mirrors the TS "schema push" suite: after pushSchema, the schema is
    // installed and the table is known (the TS suite verifies this by
    // running `query().collect()` and getting `[]`; here we verify the
    // schema snapshot directly because query/collect land in task 3).
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let schema = test_schema();
    c.push_schema(&schema).unwrap();
    let stored = c.to_schema_json().expect("schema installed");
    assert!(stored.tables.contains_key("items"));
    assert!(c.tables.contains_key("items"));
}

#[test]
fn push_schema_rejects_a_destructive_second_push() {
    // Server parity (ddl.rs::detect_destructive_changes): a second push
    // missing a previously-declared table is rejected with BadRequest and
    // the exact "removed table '<name>'" message; nothing is mutated.
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    c.push_schema(&test_schema()).unwrap();
    let only_other = Schema::builder()
        .table("solo", Table::new().field("x", FieldType::Number))
        .build();
    let err = c.push_schema(&only_other).unwrap_err();
    assert!(
        matches!(err.code, ErrorCode::BadRequest),
        "got: {:?}",
        err.code
    );
    assert!(err.message.contains("removed table 'items'"), "got: {err}");
    // The rejected push left the prior schema in place.
    let stored = c.to_schema_json().expect("schema still installed");
    assert!(stored.tables.contains_key("items"));
    assert!(c.tables.contains_key("items"));
    assert!(!stored.tables.contains_key("solo"));
}

#[tokio::test]
async fn push_schema_additively_preserves_docs() {
    // An additive second push (new optional field + new table) preserves
    // previously-inserted docs and the prior idempotency cache.
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    c.push_schema(&test_schema()).unwrap();
    c.mutate(
        &Mutation::new()
            .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
            .build(),
        Some("m1"),
    )
    .await
    .unwrap();
    // Add a new optional field on `items` and an entirely new `users` table.
    let additive = Schema::builder()
        .table(
            "items",
            Table::new()
                .field("name", FieldType::String)
                .field("status", FieldType::String)
                .field("order", FieldType::Number)
                .field("note", FieldType::optional(FieldType::String))
                .field("priority", FieldType::optional(FieldType::Number))
                .index("by_name", &["name"])
                .index("by_status", &["status"])
                .index("by_status_and_order", &["status", "order"])
                .search_index("by_content", &["name"], None),
        )
        .table("users", Table::new().field("email", FieldType::String))
        .build();
    c.push_schema(&additive).unwrap();
    // The new field/table are folded in…
    let stored = c.to_schema_json().expect("schema installed");
    assert!(stored.tables.contains_key("users"));
    assert!(stored.tables["items"].fields.contains_key("priority"));
    // …and the pre-existing row is still queryable.
    let r = c
        .run_query(&Query {
            table: "items".into(),
            ..Default::default()
        })
        .unwrap();
    let docs = r.as_array().expect("collect returns an array");
    assert_eq!(docs.len(), 1, "pre-existing row survived the additive push");
    assert_eq!(docs[0]["name"], json!("a"));
    // Idempotency cache is preserved across the additive push.
    c.mutate(
        &Mutation::new()
            .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
            .build(),
        Some("m1"),
    )
    .await
    .expect("idempotency cache hit short-circuits with the cached results");
}

#[test]
fn push_schema_allows_widening_a_literal_union() {
    // Server parity (schema::is_widening_of): a second push that widens a
    // finite literal-union field — adding a variant — is additive and
    // accepted, mirroring the live server's `pushSchema` behavior.
    let union_field =
        || FieldType::union([FieldType::literal("backlog"), FieldType::literal("done")]);
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let first = Schema::builder()
        .table(
            "items",
            Table::new()
                .field("title", FieldType::String)
                .field("status", union_field()),
        )
        .build();
    c.push_schema(&first).unwrap();
    // Widen {backlog, done} -> {backlog, done, archived}.
    let widened = Schema::builder()
        .table(
            "items",
            Table::new().field("title", FieldType::String).field(
                "status",
                FieldType::union([
                    FieldType::literal("backlog"),
                    FieldType::literal("done"),
                    FieldType::literal("archived"),
                ]),
            ),
        )
        .build();
    c.push_schema(&widened).expect("widening push succeeds");
    // The widened field type is folded into the stored schema.
    let stored = c.to_schema_json().expect("schema installed");
    let status = stored.tables["items"]
        .fields
        .get("status")
        .expect("status present");
    match status {
        FieldType::Union { variants } => assert_eq!(variants.len(), 3),
        other => panic!("expected Union, got {other:?}"),
    }
}

#[test]
fn push_schema_rejects_narrowing_a_literal_union() {
    // Server parity: a second push that narrows a literal-union field —
    // dropping a variant some rows may hold — is destructive and rejected
    // with BadRequest and the "changed type of field" message.
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let first = Schema::builder()
        .table(
            "items",
            Table::new().field("title", FieldType::String).field(
                "status",
                FieldType::union([
                    FieldType::literal("backlog"),
                    FieldType::literal("done"),
                    FieldType::literal("archived"),
                ]),
            ),
        )
        .build();
    c.push_schema(&first).unwrap();
    // Narrow {backlog, done, archived} -> {backlog, done}.
    let narrowed = Schema::builder()
        .table(
            "items",
            Table::new().field("title", FieldType::String).field(
                "status",
                FieldType::union([FieldType::literal("backlog"), FieldType::literal("done")]),
            ),
        )
        .build();
    let err = c.push_schema(&narrowed).unwrap_err();
    assert!(
        matches!(err.code, ErrorCode::BadRequest),
        "got: {:?}",
        err.code
    );
    assert!(
        err.message.contains("changed type of field 'items.status'"),
        "got: {err}"
    );
    // The rejected push left the prior (3-variant) schema in place.
    let stored = c.to_schema_json().expect("schema still installed");
    match stored.tables["items"]
        .fields
        .get("status")
        .expect("status present")
    {
        FieldType::Union { variants } => assert_eq!(variants.len(), 3),
        other => panic!("expected Union, got {other:?}"),
    }
}
