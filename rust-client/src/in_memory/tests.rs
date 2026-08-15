use super::*;
use crate::mutation::Mutation;
use crate::query::{Paginate, Paginated, SearchOpts, TableQuery, VectorSearchOpts};
use crate::schema::{Schema, Table};
use crate::wire::{AggregateOp, AggregateSpec, FilterExpr, SearchMode};
use serde_json::json;
use std::sync::{Arc, Mutex};

/// The test schema mirrored from `ts-client/tests/in_memory.test.ts:10-20`.
fn test_schema() -> SchemaDef {
    Schema::builder()
        .table(
            "items",
            Table::new()
                .field("name", FieldType::String)
                .field("status", FieldType::String)
                .field("order", FieldType::Number)
                .field("note", FieldType::optional(FieldType::String))
                .index("by_name", &["name"])
                .index("by_status", &["status"])
                .index("by_status_and_order", &["status", "order"])
                .search_index("by_content", &["name"], None),
        )
        .build()
}

fn items_table(schema: &SchemaDef) -> &TableDef {
    schema.tables.get("items").expect("items table present")
}

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

// ---- migrate -------------------------------------------------------
//
// The harness `migrate_schema` ports the server's `plan_migration` (schema
// fold) + `apply_migration` (data effects). Structural directives update
// the installed schema; data directives rewrite the in-memory doc map;
// `evalExpr` is unsupported (no SQL engine).

#[cfg(feature = "admin")]
async fn migrate_schema_with_rows() -> InMemoryRtDbClient {
    // Schema: items { name: string, status: string, order: number }, two rows.
    // Inject an incrementing clock + constant RNG (the `new_client` pattern)
    // so the two server-minted ids differ — the default RNG/now collide.
    let counter = Arc::new(Mutex::new(1_700_000_000_000_i64));
    let mut c = InMemoryRtDbClient::new(
        InMemoryRtDbClientOptions::default()
            .now(move || {
                let mut g = counter.lock().expect("counter not poisoned");
                let v = *g;
                *g += 1;
                v
            })
            .random(|| 0.0),
    );
    c.push_schema(&test_schema()).unwrap();
    c.mutate(
        &Mutation::new()
            .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
            .insert("items", json!({"name": "b", "status": "done", "order": 2}))
            .build(),
        Some("m1"),
    )
    .await
    .unwrap();
    c
}

#[cfg(feature = "admin")]
#[tokio::test]
async fn migrate_rename_field_moves_doc_key() {
    let mut c = migrate_schema_with_rows().await;
    let directives = vec![crate::wire::admin::Directive::RenameField {
        table: "items".into(),
        from: "name".into(),
        to: "title".into(),
    }];
    let result = c.migrate_schema(&directives, false).unwrap();
    assert!(result.applied);
    assert_eq!(result.directives.len(), 1);
    assert_eq!(result.directives[0].op, "renameField");
    assert_eq!(result.directives[0].affected_rows, 2);
    // The folded schema carries the renamed field.
    assert!(result.schema.tables["items"].fields.contains_key("title"));
    assert!(!result.schema.tables["items"].fields.contains_key("name"));
    // And the stored docs were rewritten to match.
    let docs = c.collect_all("items");
    assert_eq!(docs.len(), 2);
    assert!(
        docs.iter()
            .all(|d| d.get("title").is_some() && d.get("name").is_none())
    );
}

#[cfg(feature = "admin")]
#[tokio::test]
async fn migrate_drop_table_clears_rows() {
    let mut c = migrate_schema_with_rows().await;
    let directives = vec![crate::wire::admin::Directive::DropTable {
        name: "items".into(),
    }];
    let result = c.migrate_schema(&directives, false).unwrap();
    assert_eq!(result.directives[0].op, "dropTable");
    assert_eq!(result.directives[0].affected_rows, 2);
    assert!(result.schema.tables.is_empty());
    assert!(c.collect_all("items").is_empty());
}

#[cfg(feature = "admin")]
#[tokio::test]
async fn migrate_change_type_without_default_rolls_back() {
    let mut c = migrate_schema_with_rows().await;
    // String -> Int64 via ToInt64. "1"/"2" parse; the server coerces per row.
    let directives = vec![crate::wire::admin::Directive::ChangeType {
        table: "items".into(),
        field: "name".into(),
        to: FieldType::Int64,
        cast: crate::wire::admin::Cast::ToInt64,
        default: None,
    }];
    // All rows have non-numeric `name` values → coercion fails with no default.
    let err = c.migrate_schema(&directives, false).unwrap_err();
    assert!(matches!(err.code, ErrorCode::BadRequest));
    // Rollback: schema and docs unchanged.
    let stored = c.to_schema_json().unwrap();
    assert_eq!(
        stored.tables["items"].fields.get("name"),
        Some(&FieldType::String)
    );
    assert_eq!(c.collect_all("items").len(), 2);
}

#[cfg(feature = "admin")]
#[tokio::test]
async fn migrate_set_default_populates_missing_field() {
    let mut c = migrate_schema_with_rows().await;
    let directives = vec![crate::wire::admin::Directive::SetDefault {
        table: "items".into(),
        field: "note".into(),
        value: json!("untagged"),
    }];
    let result = c.migrate_schema(&directives, false).unwrap();
    assert_eq!(result.directives[0].op, "setDefault");
    assert_eq!(result.directives[0].affected_rows, 2);
    let docs = c.collect_all("items");
    assert!(
        docs.iter()
            .all(|d| d.get("note") == Some(&json!("untagged")))
    );
}

#[cfg(feature = "admin")]
#[tokio::test]
async fn migrate_dry_run_leaves_state_unchanged() {
    let mut c = migrate_schema_with_rows().await;
    let directives = vec![crate::wire::admin::Directive::DropTable {
        name: "items".into(),
    }];
    let result = c.migrate_schema(&directives, true).unwrap();
    assert!(!result.applied);
    // Preview reports the dropped table, but nothing was committed.
    assert!(result.schema.tables.is_empty());
    assert!(c.to_schema_json().unwrap().tables.contains_key("items"));
    assert_eq!(c.collect_all("items").len(), 2);
}

#[cfg(feature = "admin")]
#[tokio::test]
async fn migrate_eval_expr_unsupported() {
    let mut c = migrate_schema_with_rows().await;
    let directives = vec![crate::wire::admin::Directive::EvalExpr {
        table: "items".into(),
        set: "upper".into(),
        expr: crate::wire::admin::ExprSource::Legacy("upper(doc->>'name')".into()),
        where_clause: None,
    }];
    let err = c.migrate_schema(&directives, false).unwrap_err();
    assert!(matches!(err.code, ErrorCode::BadRequest));
    assert!(err.message.contains("evalExpr unsupported in-memory"));
}

#[cfg(feature = "admin")]
#[tokio::test]
async fn migrate_failed_directive_is_atomic() {
    let mut c = migrate_schema_with_rows().await;
    // renameField succeeds (folds into planned + docs), then DropTable on a
    // missing table fails. The earlier rename must roll back.
    let directives = vec![
        crate::wire::admin::Directive::RenameField {
            table: "items".into(),
            from: "name".into(),
            to: "title".into(),
        },
        crate::wire::admin::Directive::DropTable {
            name: "nope".into(),
        },
    ];
    let err = c.migrate_schema(&directives, false).unwrap_err();
    assert!(matches!(err.code, ErrorCode::BadRequest));
    // Schema untouched: `name` still present, `title` absent.
    let stored = c.to_schema_json().unwrap();
    assert!(stored.tables["items"].fields.contains_key("name"));
    assert!(!stored.tables["items"].fields.contains_key("title"));
    // Docs untouched: `name` key still present on every row.
    assert!(
        c.collect_all("items")
            .iter()
            .all(|d| d.get("name").is_some())
    );
}

#[cfg(feature = "admin")]
#[tokio::test]
async fn migrate_drop_field_affected_rows_counts_only_carriers() {
    // dropField reports `affected_rows` as only the rows whose `doc`
    // actually changed — rows that carried the field — not every row in the
    // table (server parity). Build a table where most rows LACK the
    // optional `note` field, drop it, and assert the count is the CARRIER
    // count.
    let mut c = migrate_schema_with_rows().await;
    // Third row that DOES carry the optional `note` field (the fixture's
    // two rows omit it).
    c.mutate(
        &Mutation::new()
            .insert(
                "items",
                json!({"name": "c", "status": "todo", "order": 3, "note": "tagged"}),
            )
            .build(),
        Some("m-note"),
    )
    .await
    .unwrap();
    let before = c.collect_all("items");
    assert_eq!(before.len(), 3);
    assert_eq!(
        before.iter().filter(|d| d.get("note").is_some()).count(),
        1,
        "precondition: exactly one row carries `note`"
    );

    let directives = vec![crate::wire::admin::Directive::DropField {
        table: "items".into(),
        field: "note".into(),
    }];
    let result = c.migrate_schema(&directives, false).unwrap();
    assert!(result.applied);
    assert_eq!(result.directives[0].op, "dropField");
    // Counts only the single row that carried the field, not all 3 rows.
    assert_eq!(result.directives[0].affected_rows, 1);

    // The field is nonetheless removed from the row that carried it, and
    // the derived schema no longer declares it.
    let after = c.collect_all("items");
    assert_eq!(after.len(), 3);
    assert!(after.iter().all(|d| d.get("note").is_none()));
    assert!(!result.schema.tables["items"].fields.contains_key("note"));
}

// ---- validate_doc --------------------------------------------------

#[test]
fn validate_doc_rejects_unknown_field() {
    let schema = test_schema();
    let bad = json!({"name": "a", "status": "todo", "order": 1, "bogus": 9});
    let err = validate_doc(items_table(&schema), &bad).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(err.message.contains("bogus"), "got: {}", err.message);
}

#[test]
fn validate_doc_rejects_reserved_field() {
    let schema = test_schema();
    let bad = json!({"name": "a", "status": "todo", "order": 1, "_id": "x"});
    let err = validate_doc(items_table(&schema), &bad).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(err.message.contains("_id"), "got: {}", err.message);
}

#[test]
fn validate_doc_rejects_wrong_field_type() {
    // The "invalid field type on a doc is rejected" case from the brief.
    let schema = test_schema();
    let bad = json!({"name": 42, "status": "todo", "order": 1});
    let err = validate_doc(items_table(&schema), &bad).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(err.message.contains("name"), "got: {}", err.message);
}

#[test]
fn validate_doc_rejects_missing_required_field() {
    let schema = test_schema();
    let bad = json!({"name": "a", "order": 1}); // missing required "status"
    let err = validate_doc(items_table(&schema), &bad).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(err.message.contains("status"), "got: {}", err.message);
}

#[test]
fn validate_doc_accepts_a_valid_doc_with_optional_absent() {
    let schema = test_schema();
    let good = json!({"name": "a", "status": "todo", "order": 1});
    validate_doc(items_table(&schema), &good).expect("valid doc");
}

#[test]
fn validate_doc_accepts_an_optional_field_set_to_null() {
    // `note` is `Optional<String>`; null is accepted at the doc level
    // because Optional accepts null. `strip_unset_optionals` is what
    // converts it to "absent" for storage.
    let schema = test_schema();
    let good = json!({"name": "a", "status": "todo", "order": 1, "note": null});
    validate_doc(items_table(&schema), &good).expect("valid doc");
}

// ---- strip_unset_optionals ----------------------------------------

#[test]
fn strip_unset_optionals_drops_null_optional_string() {
    // `note: Optional<String>` set to null → key is stripped (the inner
    // String doesn't accept null, so this is "unset").
    let schema = test_schema();
    let doc = json!({"name": "a", "status": "todo", "order": 1, "note": null});
    let stripped = strip_unset_optionals(items_table(&schema), &doc);
    assert_eq!(stripped, json!({"name": "a", "status": "todo", "order": 1}));
}

#[test]
fn strip_unset_optionals_keeps_null_for_optional_that_accepts_null() {
    // `Optional<Null>` does accept null as its inner value, so the key is
    // preserved.
    let schema = Schema::builder()
        .table(
            "t",
            Table::new().field("x", FieldType::optional(FieldType::Null)),
        )
        .build();
    let table = schema.tables.get("t").expect("table present");
    let doc = json!({"x": null});
    let stripped = strip_unset_optionals(table, &doc);
    assert_eq!(stripped, json!({"x": null}));
}

// ---- id/format helpers --------------------------------------------

#[test]
fn is_hex_id_checks_32_lowercase_hex_chars() {
    assert!(is_hex_id(&json!("0123456789abcdef0123456789abcdef")));
    assert!(!is_hex_id(&json!("0123456789ABCDEF0123456789ABCDEF"))); // uppercase
    assert!(!is_hex_id(&json!("0123456789abcdef"))); // too short
    assert!(!is_hex_id(&json!(42)));
    assert!(!is_hex_id(&json!(null)));
}

#[test]
fn is_int64_string_accepts_i64_range_only() {
    assert!(is_int64_string(&json!("0")));
    assert!(is_int64_string(&json!("-1")));
    assert!(is_int64_string(&json!("9223372036854775807"))); // i64::MAX
    assert!(is_int64_string(&json!("-9223372036854775808"))); // i64::MIN
    // Out of i64 range:
    assert!(!is_int64_string(&json!("9223372036854775808")));
    assert!(!is_int64_string(&json!("-9223372036854775809")));
    // Bad shape:
    assert!(!is_int64_string(&json!("1.5")));
    assert!(!is_int64_string(&json!("-")));
    assert!(!is_int64_string(&json!("")));
    assert!(!is_int64_string(&json!(42)));
}

#[test]
fn is_base64_string_matches_the_ts_regex() {
    assert!(is_base64_string(&json!("")));
    assert!(is_base64_string(&json!("ABCD")));
    assert!(is_base64_string(&json!("ABC=")));
    assert!(is_base64_string(&json!("AB==")));
    assert!(is_base64_string(&json!("YWJjZA=="))); // "abcd"
    // Length not a multiple of 4:
    assert!(!is_base64_string(&json!("ABC")));
    // Too much padding:
    assert!(!is_base64_string(&json!("A===")));
    // Bad body char:
    assert!(!is_base64_string(&json!("ABC!")));
    assert!(!is_base64_string(&json!(42)));
}

#[test]
fn validate_value_handles_each_field_type_variant() {
    // A sanity sweep over the variants; full per-variant coverage lives in
    // the schema tests. Here we just confirm routing works.
    assert!(validate_value(&FieldType::String, &json!("hi")));
    assert!(!validate_value(&FieldType::String, &json!(2)));
    assert!(validate_value(&FieldType::Number, &json!(2.5)));
    assert!(validate_value(&FieldType::Boolean, &json!(true)));
    assert!(validate_value(&FieldType::Null, &json!(null)));
    assert!(validate_value(&FieldType::Any, &json!(null)));
    assert!(validate_value(
        &FieldType::Id { table: "x".into() },
        &json!("0123456789abcdef0123456789abcdef")
    ));
    assert!(validate_value(
        &FieldType::Literal { value: json!("a") },
        &json!("a")
    ));
    assert!(validate_value(
        &FieldType::Optional {
            inner: Box::new(FieldType::String)
        },
        &json!(null)
    ));
    assert!(validate_value(
        &FieldType::Union {
            variants: vec![FieldType::String, FieldType::Number]
        },
        &json!(2)
    ));
    assert!(validate_value(
        &FieldType::Array {
            element: Box::new(FieldType::Number)
        },
        &json!([1, 2, 3])
    ));
    assert!(validate_value(&FieldType::Int64, &json!("42")));
    assert!(validate_value(&FieldType::Bytes, &json!("YWJjZA==")));
    assert!(validate_value(
        &FieldType::Vector { dimensions: 3 },
        &json!([1.0, 2.0, 3.0])
    ));
}

#[test]
fn canonical_is_key_order_independent() {
    // serde_json's default BTreeMap-backed Map serializes with sorted keys,
    // so canonical(a) == canonical(b) even when the source maps had
    // different insertion order.
    let a = json!({"b": 1, "a": 2});
    let b = json!({"a": 2, "b": 1});
    assert_eq!(canonical(&a), canonical(&b));
}

// ---- mutate: insert + read ---------------------------------------

/// Deterministic clock + RNG so ids, `_creationTime`, and `_version` are
/// stable. Mirrors TS `newClient` (`ts-client/tests/in_memory.test.ts:25-30`):
/// post-incrementing epoch-millis clock + a constant `0` RNG.
fn new_client() -> InMemoryRtDbClient {
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
    client.push_schema(&test_schema()).unwrap();
    client
}

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

// ---- mutate: step helpers ----------------------------------------

#[test]
fn apply_patch_merges_fields_and_re_validates_whole_doc() {
    let schema = test_schema();
    let table = items_table(&schema);
    let doc = json!({"name": "a", "status": "todo", "order": 1});
    let fields = json!({"order": 9}).as_object().unwrap().clone();
    let merged = apply_patch(table, &doc, &fields).expect("patch ok");
    assert_eq!(merged["order"], 9);
    assert_eq!(merged["name"], "a", "non-patched fields preserved");
}

#[test]
fn apply_patch_null_on_optional_inner_that_rejects_null_deletes_key() {
    // `note: Optional<String>` + null → key is removed (mirrors
    // strip_unset_optionals' single-representation rule).
    let schema = test_schema();
    let table = items_table(&schema);
    let doc = json!({"name": "a", "status": "todo", "order": 1, "note": "hi"});
    let fields = json!({"note": null}).as_object().unwrap().clone();
    let merged = apply_patch(table, &doc, &fields).expect("patch ok");
    assert!(merged.get("note").is_none(), "note key stripped: {merged}");
}

#[test]
fn apply_patch_rejects_unknown_field() {
    let schema = test_schema();
    let table = items_table(&schema);
    let doc = json!({"name": "a", "status": "todo", "order": 1});
    let fields = json!({"bogus": 1}).as_object().unwrap().clone();
    let err = apply_patch(table, &doc, &fields).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(err.message.contains("bogus"));
}

#[test]
fn index_column_type_maps_each_indexable_field_and_rejects_others() {
    // Indexable shapes:
    assert_eq!(
        index_column_type(&FieldType::String).unwrap().pg,
        PgType::Text
    );
    assert_eq!(
        index_column_type(&FieldType::Number).unwrap().pg,
        PgType::Number
    );
    assert_eq!(
        index_column_type(&FieldType::Boolean).unwrap().pg,
        PgType::Boolean
    );
    assert_eq!(
        index_column_type(&FieldType::Int64).unwrap().pg,
        PgType::Int64
    );
    assert_eq!(
        index_column_type(&FieldType::id("t")).unwrap().pg,
        PgType::Text
    );
    assert_eq!(
        index_column_type(&FieldType::literal("a")).unwrap().pg,
        PgType::Text
    );
    assert_eq!(
        index_column_type(&FieldType::optional(FieldType::Number))
            .unwrap()
            .pg,
        PgType::Number
    );
    // Optional wraps and reports nullable=true.
    let it = index_column_type(&FieldType::optional(FieldType::Number)).unwrap();
    assert!(it.nullable);
    // Non-indexable shapes:
    let err = index_column_type(&FieldType::Array {
        element: Box::new(FieldType::Number),
    })
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    let err = index_column_type(&FieldType::literal(7)).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
}

#[test]
fn coerce_index_value_type_checks_against_index_column() {
    let schema = test_schema();
    let table = items_table(&schema);
    // `name` is String → text column. Number is rejected.
    coerce_index_value(table, "name", &json!("a")).expect("string ok");
    let err = coerce_index_value(table, "name", &json!(7)).unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    // `order` is Number → number column. String is rejected.
    coerce_index_value(table, "order", &json!(7)).expect("number ok");
    let err = coerce_index_value(table, "order", &json!("7")).unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    // Unknown field is INTERNAL (schema-declared index references a missing
    // field — a server-side programming error, not a client one).
    let err = coerce_index_value(table, "bogus", &json!(7)).unwrap_err();
    assert_eq!(err.code, ErrorCode::Internal);
}

#[test]
fn compare_index_values_orders_nulls_last_and_compares_each_domain() {
    use std::cmp::Ordering;
    // Numbers:
    assert_eq!(
        compare_index_values(&json!(1), &json!(2), PgType::Number),
        Ordering::Less
    );
    assert_eq!(
        compare_index_values(&json!(2), &json!(2), PgType::Number),
        Ordering::Equal
    );
    // Strings (lexicographic):
    assert_eq!(
        compare_index_values(&json!("a"), &json!("b"), PgType::Text),
        Ordering::Less
    );
    // Booleans (false < true):
    assert_eq!(
        compare_index_values(&json!(false), &json!(true), PgType::Boolean),
        Ordering::Less
    );
    // Int64 decimal strings compare numerically, not lexicographically:
    assert_eq!(
        compare_index_values(&json!("3"), &json!("20"), PgType::Int64),
        Ordering::Less
    );
    assert_eq!(
        compare_index_values(&json!("100"), &json!("20"), PgType::Int64),
        Ordering::Greater
    );
    assert_eq!(
        compare_index_values(&json!("-1"), &json!("0"), PgType::Int64),
        Ordering::Less
    );
    // Nulls sort last under asc — `null > anything`. The `pg` domain is
    // irrelevant once either side is null.
    assert_eq!(
        compare_index_values(&json!(null), &json!(1), PgType::Number),
        Ordering::Greater
    );
    assert_eq!(
        compare_index_values(&json!(1), &json!(null), PgType::Number),
        Ordering::Less
    );
    assert_eq!(
        compare_index_values(&json!(null), &json!(null), PgType::Number),
        Ordering::Equal
    );
}

#[test]
fn merge_doc_layers_system_fields_over_user_doc() {
    let row = StoredRow {
        id: "0018beacc10070000000000000000000".to_string(),
        doc: json!({"name": "a", "status": "todo", "order": 1}),
        version: 7,
        created_at: 1_700_000_000_000,
    };
    let merged = merge_doc(&row);
    assert_eq!(merged["_id"], json!("0018beacc10070000000000000000000"));
    assert_eq!(merged["_version"], 7);
    assert_eq!(merged["_creationTime"], 1_700_000_000_000_i64);
    // User fields preserved.
    assert_eq!(merged["name"], "a");
    assert_eq!(merged["order"], 1);
}

// ---- query: get / collect ----------------------------------------

/// Mirrors TS `seed` (`ts-client/tests/in_memory.test.ts:134-142`): insert
/// three rows in `order` = 3, 1, 2 so an ascending sort differs from
/// insertion order (catches a fall-back-to-insertion-order bug).
async fn seed_query_rows(c: &mut InMemoryRtDbClient) {
    for order in [3_i64, 1, 2] {
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": format!("n{order}"), "status": "todo", "order": order}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn query_collect_returns_empty_for_empty_table() {
    // Mirrors TS "collects [] from an empty table after pushSchema".
    let c = new_client();
    let docs = c
        .run::<Vec<Value>>(&TableQuery::new("items").collect())
        .expect("collect ok");
    assert!(docs.is_empty());
}

#[tokio::test]
async fn query_get_returns_merged_doc() {
    // Mirrors TS "inserts a doc and merges system fields at read time"
    // (the read is now via the DSL `get` terminal, not the bare helper).
    let mut c = new_client();
    let r = c
        .mutate(
            &Mutation::new()
                .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
                .build(),
            None,
        )
        .await
        .expect("insert ok");
    let id = match &r[0] {
        StepResult::Insert { id } => id.clone(),
        other => panic!("expected Insert, got {other:?}"),
    };

    let doc = c
        .run::<Value>(&TableQuery::get("items", &id))
        .expect("get ok");
    assert_eq!(doc["_id"], json!(id));
    assert_eq!(doc["name"], "a");
    assert_eq!(doc["status"], "todo");
    assert_eq!(doc["order"], 1);
    assert_eq!(doc["_version"], 1);
    assert!(doc["_creationTime"].is_number());
}

#[tokio::test]
async fn query_get_returns_null_for_missing_id() {
    // Mirrors TS "point-reads a missing id as null". The server returns
    // JSON null for a missing point read (TS :916), not an error.
    let c = new_client();
    let v = c
        .run::<Value>(&TableQuery::get(
            "items",
            "0123456789abcdef0123456789abcdef",
        ))
        .expect("get resolves");
    assert!(v.is_null(), "missing get returns Value::Null, got: {v}");
}

#[tokio::test]
async fn query_get_rejects_combinations() {
    // Ports the `get`-exclusivity guard at TS :895-914. `get` plus any
    // narrowing clause is BAD_REQUEST.
    let c = new_client();
    let q = Query {
        table: "items".into(),
        get: Some("x".into()),
        index: Some("by_name".into()),
        ..Default::default()
    };
    let err = c.run_query(&q).unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("get cannot be combined"),
        "got: {}",
        err.message
    );
}

// ---- query: index eq + order + take ------------------------------

#[tokio::test]
async fn query_eq_prefix_with_order_asc_sorts_by_remaining_field() {
    // Mirrors TS "filters by an eq index prefix and orders by the remaining
    // index field" — the asc branch.
    let mut c = new_client();
    seed_query_rows(&mut c).await;

    let asc = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .order(Order::Asc)
                .collect(),
        )
        .expect("asc ok");
    let orders: Vec<i64> = asc
        .iter()
        .map(|d| d["order"].as_i64().unwrap_or_default())
        .collect();
    assert_eq!(orders, vec![1, 2, 3], "asc order");
}

#[tokio::test]
async fn query_eq_prefix_with_order_desc_and_take_n() {
    // Mirrors TS "filters by an eq index prefix and orders by the remaining
    // index field" — the desc+take(2) branch.
    let mut c = new_client();
    seed_query_rows(&mut c).await;

    let desc = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .order(Order::Desc)
                .take(2),
        )
        .expect("desc+take ok");
    let orders: Vec<i64> = desc
        .iter()
        .map(|d| d["order"].as_i64().unwrap_or_default())
        .collect();
    assert_eq!(orders, vec![3, 2], "desc order, take 2");
}

#[tokio::test]
async fn query_eq_on_single_field_index_returns_matching_rows() {
    // The brief calls out single-field eq match explicitly; `by_name` is
    // single-field. Two rows share `name="dup"`, the third doesn't.
    let mut c = new_client();
    for order in [1_i64, 2, 3] {
        let name = if order <= 2 { "dup" } else { "uniq" };
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": name, "status": "todo", "order": order}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
    }
    let docs = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .with_index("by_name", &[json!("dup")])
                .collect(),
        )
        .expect("eq ok");
    assert_eq!(docs.len(), 2, "both dup rows match");
    for d in &docs {
        assert_eq!(d["name"], "dup");
    }
}

// ---- query: range bounds ----------------------------------------

#[tokio::test]
async fn query_range_filters_by_index_field() {
    // gt / lt / gte / lte over the remaining index field. `by_status_and_order`
    // has `status` then `order`; the eq prefix pins status, the range
    // narrows order. Seed order values [3,1,2] and assert each bound.
    let mut c = new_client();
    seed_query_rows(&mut c).await;

    let collect_range = |gt: Option<i64>, gte: Option<i64>, lt: Option<i64>, lte: Option<i64>| {
        let mut q = TableQuery::new("items").with_index("by_status_and_order", &[json!("todo")]);
        if let Some(v) = gt {
            q = q.gt(v);
        }
        if let Some(v) = gte {
            q = q.gte(v);
        }
        if let Some(v) = lt {
            q = q.lt(v);
        }
        if let Some(v) = lte {
            q = q.lte(v);
        }
        c.run::<Vec<Value>>(&q.order(Order::Asc).collect())
            .expect("range ok")
    };

    let orders = |docs: Vec<Value>| -> Vec<i64> {
        docs.iter()
            .map(|d| d["order"].as_i64().unwrap_or_default())
            .collect()
    };

    // gt=1 → {2,3}; gte=2 → {2,3}; lt=3 → {1,2}; lte=2 → {1,2}.
    assert_eq!(orders(collect_range(Some(1), None, None, None)), vec![2, 3]);
    assert_eq!(orders(collect_range(None, Some(2), None, None)), vec![2, 3]);
    assert_eq!(orders(collect_range(None, None, Some(3), None)), vec![1, 2]);
    assert_eq!(orders(collect_range(None, None, None, Some(2))), vec![1, 2]);
}

// ---- query: int64 index (numeric ordering + range) ----------------

/// Schema for int64-indexable coverage: a single `by_ts` index over an
/// `Int64` field, plus a string payload to identify rows in assertions.
fn int64_test_schema() -> SchemaDef {
    Schema::builder()
        .table(
            "events",
            Table::new()
                .field("ts", FieldType::Int64)
                .field("kind", FieldType::String)
                .index("by_ts", &["ts"]),
        )
        .build()
}

/// Client seeded with [`int64_test_schema`] and a deterministic incrementing
/// clock so each insert gets a distinct `_id` (the default constant-RNG id
/// collides within a single millisecond, which would make successive inserts
/// overwrite each other).
fn int64_client() -> InMemoryRtDbClient {
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
    client.push_schema(&int64_test_schema()).unwrap();
    client
}

#[tokio::test]
async fn int64_index_orders_and_ranges_numerically() {
    // Int64 indexes store decimal strings, but the index order has to be
    // numeric (3 < 20 < 100), not lexicographic (100 < 20 < 3). Seeds the
    // rows out of numeric order to catch a lexicographic regression on
    // both the sort path and the range-bound path.
    let mut c = int64_client();
    for (ts, kind) in [("100", "a"), ("20", "b"), ("3", "c")] {
        c.mutate(
            &Mutation::new()
                .insert("events", json!({ "ts": ts, "kind": kind }))
                .build(),
            None,
        )
        .await
        .unwrap();
    }

    let kinds = |docs: Vec<Value>| -> Vec<String> {
        docs.iter()
            .map(|d| d["kind"].as_str().unwrap_or_default().to_string())
            .collect()
    };

    // Ascending numeric sort over the by_ts index → 3, 20, 100.
    let asc = c
        .run::<Vec<Value>>(
            &TableQuery::new("events")
                .with_index("by_ts", &[])
                .order(Order::Asc)
                .collect(),
        )
        .expect("asc ok");
    assert_eq!(
        kinds(asc),
        vec!["c".to_string(), "b".to_string(), "a".to_string()],
        "int64 index should sort numerically (3, 20, 100)"
    );

    // Range on the int64 field: gte=20 keeps {20, 100}, asc → [b, a].
    let ranged = c
        .run::<Vec<Value>>(
            &TableQuery::new("events")
                .with_index("by_ts", &[])
                .gte(json!("20"))
                .order(Order::Asc)
                .collect(),
        )
        .expect("range ok");
    assert_eq!(
        kinds(ranged),
        vec!["b".to_string(), "a".to_string()],
        "int64 range bound should compare numerically (gte=20 keeps 20, 100)"
    );
}

// ---- query: terminals -------------------------------------------

#[tokio::test]
async fn query_count_returns_number_of_matching_rows() {
    // Mirrors TS "counts matching rows over an eq prefix".
    let mut c = new_client();
    seed_query_rows(&mut c).await;
    let n = c
        .run::<i64>(
            &TableQuery::new("items")
                .with_index("by_status", &[json!("todo")])
                .count(),
        )
        .expect("count ok");
    assert_eq!(n, 3);
}

#[tokio::test]
async fn query_unique_returns_doc_when_exactly_one_match() {
    let mut c = new_client();
    c.mutate(
        &Mutation::new()
            .insert(
                "items",
                json!({"name": "only", "status": "todo", "order": 1}),
            )
            .build(),
        None,
    )
    .await
    .unwrap();
    let doc = c
        .run::<Value>(
            &TableQuery::new("items")
                .with_index("by_name", &[json!("only")])
                .unique(),
        )
        .expect("unique ok");
    assert_eq!(doc["name"], "only");
}

#[tokio::test]
async fn query_unique_throws_precondition_failed_when_multiple_match() {
    // Mirrors TS "unique throws PRECONDITION_FAILED when more than one doc
    // matches".
    let mut c = new_client();
    for order in [1_i64, 2] {
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": "dup", "status": "todo", "order": order}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
    }
    let err = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_name", &[json!("dup")])
                .unique(),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::PreconditionFailed);
}

#[tokio::test]
async fn query_unique_returns_null_when_zero_match() {
    // TS :1143 — `unique` with zero matches returns null (no precondition
    // to fail; only a multi-match is an error).
    let c = new_client();
    let v = c
        .run::<Value>(
            &TableQuery::new("items")
                .with_index("by_name", &[json!("ghost")])
                .unique(),
        )
        .expect("unique resolves");
    assert!(v.is_null(), "zero-match unique returns null, got: {v}");
}

#[tokio::test]
async fn query_first_returns_first_or_null() {
    // Mirrors TS `first` terminal: the first row of the filtered+sorted
    // set, or null when empty.
    let mut c = new_client();
    // Empty table: first = null.
    let v = c
        .run::<Value>(
            &TableQuery::new("items")
                .with_index("by_status", &[json!("todo")])
                .first(),
        )
        .expect("first on empty");
    assert!(v.is_null(), "first on empty table is null");

    seed_query_rows(&mut c).await;
    // With rows sorted ascending, first is order=1.
    let first = c
        .run::<Value>(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .order(Order::Asc)
                .first(),
        )
        .expect("first ok");
    assert_eq!(first["order"], 1, "first asc is order=1");
}

#[tokio::test]
async fn query_take_caps_results_at_n() {
    let mut c = new_client();
    seed_query_rows(&mut c).await;
    let docs = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .with_index("by_status", &[json!("todo")])
                .order(Order::Asc)
                .take(2),
        )
        .expect("take ok");
    assert_eq!(docs.len(), 2, "take(2) on 3 rows caps at 2");
}

// ---- query: validation rejections -------------------------------

#[tokio::test]
async fn query_rejects_eq_without_index() {
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            eq: vec![json!("x")],
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("eq requires an index"), "got: {err}");
}

#[tokio::test]
async fn query_rejects_range_without_index() {
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            gt: Some(json!(1)),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("range bound requires an index"),
        "got: {err}"
    );
}

#[tokio::test]
async fn query_rejects_range_without_remaining_field_after_eq() {
    // `by_name` has one field — a full-arity eq leaves no field for a
    // range bound.
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            index: Some("by_name".into()),
            eq: vec![json!("a")],
            gt: Some(json!("z")),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("remaining index field after eq"),
        "got: {err}"
    );
}

#[tokio::test]
async fn query_rejects_eq_arity_above_index_field_count() {
    // `by_name` is single-field; two eq values is over-arity.
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            index: Some("by_name".into()),
            eq: vec![json!("a"), json!("b")],
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("expects at most"), "got: {err}");
}

#[tokio::test]
async fn query_rejects_gt_and_gte_together() {
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            index: Some("by_status_and_order".into()),
            eq: vec![json!("todo")],
            gt: Some(json!(1)),
            gte: Some(json!(1)),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("gt and gte"), "got: {err}");
}

#[tokio::test]
async fn query_rejects_lt_and_lte_together() {
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            index: Some("by_status_and_order".into()),
            eq: vec![json!("todo")],
            lt: Some(json!(1)),
            lte: Some(json!(1)),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("lt and lte"), "got: {err}");
}

#[tokio::test]
async fn query_rejects_take_over_max_take() {
    // MAX_TAKE guard (TS :963-965).
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            take: Some((MAX_TAKE as u32) + 1),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("maximum"), "got: {err}");
}

#[tokio::test]
async fn query_accepts_take_at_max_take() {
    // `take == MAX_TAKE` is the boundary — accepted.
    let c = new_client();
    let docs = c
        .run::<Vec<Value>>(&Query {
            table: "items".into(),
            take: Some(MAX_TAKE as u32),
            ..Default::default()
        })
        .expect("take=MAX_TAKE ok");
    assert!(docs.is_empty(), "empty table → empty page");
}

/// One assertion per conflicting-terminal guard at TS :919-939. Each case
/// is BAD_REQUEST; the needle distinguishes which guard fired.
#[tokio::test]
async fn query_rejects_conflicting_terminals() {
    let c = new_client();
    let base_index_query =
        |unique: bool, first: bool, count: bool, order: bool, take: Option<u32>| Query {
            table: "items".into(),
            index: Some("by_status".into()),
            eq: vec![json!("todo")],
            unique,
            first,
            count,
            order: order.then_some(Order::Asc),
            take,
            ..Default::default()
        };

    let cases: &[(Query, &str)] = &[
        // unique + take
        (
            base_index_query(true, false, false, false, Some(1)),
            "unique cannot be combined with take",
        ),
        // unique + order
        (
            base_index_query(true, false, false, true, None),
            "unique cannot be combined with take, order",
        ),
        // first + unique
        (
            base_index_query(true, true, false, false, None),
            "first cannot be combined with unique",
        ),
        // first + take
        (
            base_index_query(false, true, false, false, Some(1)),
            "first cannot be combined with take",
        ),
        // count + unique
        (
            base_index_query(true, false, true, false, None),
            "count cannot be combined with unique",
        ),
        // count + take
        (
            base_index_query(false, false, true, false, Some(1)),
            "count cannot be combined with take",
        ),
        // count + first
        (
            base_index_query(false, true, true, false, None),
            "count cannot be combined with first",
        ),
        // count + order
        (
            base_index_query(false, false, true, true, None),
            "count cannot be combined with order",
        ),
    ];
    for (q, needle) in cases {
        let err = c.run_query(q).unwrap_err();
        assert_eq!(
            err.code,
            ErrorCode::BadRequest,
            "case '{needle}': got {err:?}"
        );
        assert!(
            err.message.contains(needle),
            "case '{needle}' missing needle: got {}",
            err.message
        );
    }
}

// ---- query: distinct + aggregate terminals ---------------------
//
// Ports distinct/aggregate coverage from `ts-client/src/in_memory.ts`
// (`executeQuery` :1355-1462) and the server's `execute_query` arms. Both
// are standalone terminals over the index field immediately after the eq
// prefix; they compose only with index/eq/range/filter.

/// Seeds `items` with duplicated `order` values {3,1,2,1,2} (all "todo") so
/// distinct dedupe and asc sort are both observable.
async fn seed_dup_orders(c: &mut InMemoryRtDbClient) {
    for order in [3_i64, 1, 2, 1, 2] {
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": format!("n{order}"), "status": "todo", "order": order}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
    }
}

/// Seeds `items` with two statuses so a `groupBy` over
/// `by_status_and_order` has multiple groups: todo {1,2}, done {3,4}.
async fn seed_group_rows(c: &mut InMemoryRtDbClient) {
    for (status, order) in [("todo", 1_i64), ("todo", 2), ("done", 3), ("done", 4)] {
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": "n", "status": status, "order": order}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
    }
}

/// Seeds `items` with `status` values {charlie, alpha, bravo} so non-numeric
/// MIN/MAX pick lexicographic extremes.
async fn seed_status_rows(c: &mut InMemoryRtDbClient) {
    for (i, status) in ["charlie", "alpha", "bravo"].iter().enumerate() {
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": "n", "status": status, "order": i as i64}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn distinct_returns_unique_index_field_values_sorted_asc() {
    let mut c = new_client();
    seed_query_rows(&mut c).await; // orders 3, 1, 2 — all "todo"
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .distinct(),
        )
        .expect("distinct ok");
    assert_eq!(v, json!([1, 2, 3]));
}

#[tokio::test]
async fn distinct_dedupes_repeated_values() {
    let mut c = new_client();
    seed_dup_orders(&mut c).await; // orders 3,1,2,1,2
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .distinct(),
        )
        .expect("distinct ok");
    assert_eq!(v, json!([1, 2, 3]));
}

#[tokio::test]
async fn distinct_composes_with_range_bound() {
    let mut c = new_client();
    seed_query_rows(&mut c).await; // orders 3, 1, 2
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .gte(2)
                .distinct(),
        )
        .expect("distinct+range ok");
    assert_eq!(v, json!([2, 3]));
}

#[tokio::test]
async fn distinct_empty_matching_set_returns_empty_array() {
    let mut c = new_client();
    seed_query_rows(&mut c).await;
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("missing")])
                .distinct(),
        )
        .expect("distinct ok");
    assert_eq!(v, json!([]));
}

#[tokio::test]
async fn distinct_requires_an_index_field_beyond_eq_prefix() {
    let c = new_client();
    // eq prefix [todo, 1] consumes both index fields of by_status_and_order.
    let err = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo"), json!(1)])
                .distinct(),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("distinct requires an index field beyond the eq prefix"),
        "got: {}",
        err.message
    );
}

#[tokio::test]
async fn distinct_rejects_conflicting_terminals() {
    // Ownership mirrors the server's check order (query.rs :676-706): get,
    // unique, first, count are validated before distinct, so distinct+
    // {get,unique,first,count} surfaces *that* terminal's message; distinct
    // owns only take/order/aggregate.
    let c = new_client();
    let base = || Query {
        table: "items".into(),
        index: Some("by_status_and_order".into()),
        eq: vec![json!("todo")],
        ..Default::default()
    };
    let cases: &[(Query, &str)] = &[
        (
            Query {
                distinct: true,
                take: Some(1),
                ..base()
            },
            "distinct cannot be combined with take",
        ),
        (
            Query {
                distinct: true,
                order: Some(Order::Asc),
                ..base()
            },
            "distinct cannot be combined with order",
        ),
        (
            Query {
                distinct: true,
                aggregate: Some(AggregateSpec {
                    op: AggregateOp::Sum,
                    group_by: false,
                }),
                ..base()
            },
            "distinct cannot be combined with aggregate",
        ),
        (
            Query {
                distinct: true,
                unique: true,
                ..base()
            },
            "unique cannot be combined with take, order, distinct, or aggregate",
        ),
        (
            Query {
                distinct: true,
                first: true,
                ..base()
            },
            "first cannot be combined with distinct",
        ),
        (
            Query {
                distinct: true,
                count: true,
                ..base()
            },
            "count cannot be combined with distinct",
        ),
        (
            Query {
                distinct: true,
                get: Some("x".into()),
                ..base()
            },
            "get cannot be combined with",
        ),
    ];
    for (q, needle) in cases {
        let err = c.run_query(q).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest, "case '{needle}': {err:?}");
        assert!(
            err.message.contains(needle),
            "case '{needle}': got {}",
            err.message
        );
    }
}

#[tokio::test]
async fn aggregate_sum_avg_min_max_over_numeric_field() {
    let mut c = new_client();
    seed_query_rows(&mut c).await; // orders 3, 1, 2

    let sum = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .aggregate(AggregateOp::Sum, false),
        )
        .expect("sum");
    assert_eq!(sum.as_f64(), Some(6.0));
    let avg = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .aggregate(AggregateOp::Avg, false),
        )
        .expect("avg");
    assert_eq!(avg.as_f64(), Some(2.0));
    let min = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .aggregate(AggregateOp::Min, false),
        )
        .expect("min");
    assert_eq!(min.as_f64(), Some(1.0));
    let max = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .aggregate(AggregateOp::Max, false),
        )
        .expect("max");
    assert_eq!(max.as_f64(), Some(3.0));
}

#[tokio::test]
async fn aggregate_empty_matching_set_returns_null() {
    let mut c = new_client();
    seed_query_rows(&mut c).await;
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("missing")])
                .aggregate(AggregateOp::Sum, false),
        )
        .expect("aggregate ok");
    assert!(v.is_null(), "empty aggregate is null, got: {v}");
}

#[tokio::test]
async fn aggregate_sum_requires_a_numeric_field() {
    let mut c = new_client();
    seed_status_rows(&mut c).await;
    // by_status [status], empty eq → agg field is `status` (a string).
    let err = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status", &[])
                .aggregate(AggregateOp::Sum, false),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("aggregate op sum requires a numeric index field"),
        "got: {}",
        err.message
    );
}

#[tokio::test]
async fn aggregate_min_max_over_string_field_are_lexicographic() {
    let mut c = new_client();
    seed_status_rows(&mut c).await; // statuses charlie, alpha, bravo
    let min = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status", &[])
                .aggregate(AggregateOp::Min, false),
        )
        .expect("min");
    assert_eq!(min.as_str(), Some("alpha"));
    let max = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status", &[])
                .aggregate(AggregateOp::Max, false),
        )
        .expect("max");
    assert_eq!(max.as_str(), Some("charlie"));
}

#[tokio::test]
async fn aggregate_group_by_groups_and_aggregates() {
    let mut c = new_client();
    seed_group_rows(&mut c).await; // todo{1,2}, done{3,4}
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[])
                .aggregate(AggregateOp::Sum, true),
        )
        .expect("groupBy ok");
    let arr = v.as_array().expect("array of {key,value}");
    assert_eq!(arr.len(), 2);
    // Groups are ordered by key ascending: "done" < "todo".
    assert_eq!(arr[0]["key"].as_str(), Some("done"));
    assert_eq!(arr[0]["value"].as_f64(), Some(7.0));
    assert_eq!(arr[1]["key"].as_str(), Some("todo"));
    assert_eq!(arr[1]["value"].as_f64(), Some(3.0));
}

#[tokio::test]
async fn aggregate_count_scalar_returns_matching_row_count() {
    let mut c = new_client();
    seed_query_rows(&mut c).await; // three "todo" rows
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .aggregate(AggregateOp::Count, false),
        )
        .expect("count ok");
    assert_eq!(v.as_i64(), Some(3));
}

#[tokio::test]
async fn aggregate_count_scalar_empty_matching_set_is_zero() {
    let mut c = new_client();
    seed_query_rows(&mut c).await;
    // count over zero rows is 0 (never null, unlike sum/avg/min/max).
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("missing")])
                .aggregate(AggregateOp::Count, false),
        )
        .expect("count ok");
    assert_eq!(v.as_i64(), Some(0));
}

#[tokio::test]
async fn aggregate_count_grouped_returns_group_sizes() {
    let mut c = new_client();
    seed_group_rows(&mut c).await; // todo{1,2}, done{3,4}
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[])
                .aggregate(AggregateOp::Count, true),
        )
        .expect("groupBy count ok");
    let arr = v.as_array().expect("array of {key,value}");
    assert_eq!(arr.len(), 2);
    // Ordered by key ascending: "done" < "todo".
    assert_eq!(arr[0]["key"].as_str(), Some("done"));
    assert_eq!(arr[0]["value"].as_i64(), Some(2));
    assert_eq!(arr[1]["key"].as_str(), Some("todo"));
    assert_eq!(arr[1]["value"].as_i64(), Some(2));
}

#[tokio::test]
async fn aggregate_count_consumes_no_aggregate_field() {
    // count needs no field beyond the eq prefix: by_status [status] with an
    // empty eq prefix would error for sum/avg ("requires an index field
    // beyond the eq prefix") but succeeds for count.
    let mut c = new_client();
    seed_status_rows(&mut c).await; // three rows
    let v = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status", &[])
                .aggregate(AggregateOp::Count, false),
        )
        .expect("count needs no agg field");
    assert_eq!(v.as_i64(), Some(3));
}

#[tokio::test]
async fn aggregate_group_by_requires_two_index_fields_beyond_prefix() {
    let c = new_client();
    // by_status [status], empty eq → only one field beyond the prefix.
    let err = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status", &[])
                .aggregate(AggregateOp::Sum, true),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("aggregate groupBy requires two index fields beyond the eq prefix"),
        "got: {}",
        err.message
    );
}

#[tokio::test]
async fn aggregate_requires_an_index_field_beyond_eq_prefix() {
    let c = new_client();
    // eq prefix [todo, 1] consumes both fields of by_status_and_order.
    let err = c
        .run_query(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo"), json!(1)])
                .aggregate(AggregateOp::Min, false),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message
            .contains("aggregate requires an index field beyond the eq prefix"),
        "got: {}",
        err.message
    );
}

#[tokio::test]
async fn aggregate_rejects_conflicting_terminals() {
    let c = new_client();
    let base = || Query {
        table: "items".into(),
        index: Some("by_status_and_order".into()),
        eq: vec![json!("todo")],
        ..Default::default()
    };
    let sum = || AggregateSpec {
        op: AggregateOp::Sum,
        group_by: false,
    };
    let cases: &[(Query, &str)] = &[
        (
            Query {
                aggregate: Some(sum()),
                take: Some(1),
                ..base()
            },
            "aggregate cannot be combined with take",
        ),
        (
            Query {
                aggregate: Some(sum()),
                order: Some(Order::Asc),
                ..base()
            },
            "aggregate cannot be combined with order",
        ),
        (
            Query {
                aggregate: Some(sum()),
                unique: true,
                ..base()
            },
            "unique cannot be combined with take, order, distinct, or aggregate",
        ),
        (
            Query {
                aggregate: Some(sum()),
                first: true,
                ..base()
            },
            "first cannot be combined with aggregate",
        ),
        (
            Query {
                aggregate: Some(sum()),
                count: true,
                ..base()
            },
            "count cannot be combined with aggregate",
        ),
        (
            Query {
                aggregate: Some(sum()),
                distinct: true,
                ..base()
            },
            "distinct cannot be combined with aggregate",
        ),
        (
            Query {
                aggregate: Some(sum()),
                get: Some("x".into()),
                ..base()
            },
            "get cannot be combined with",
        ),
    ];
    for (q, needle) in cases {
        let err = c.run_query(q).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest, "case '{needle}': {err:?}");
        assert!(
            err.message.contains(needle),
            "case '{needle}': got {}",
            err.message
        );
    }
}

// ---- query: paginate (cursor keyset) -----------------------------
//
// Direct port of `describe("InMemoryRtDbClient — paginate (cursor keyset)")`
// (`ts-client/tests/in_memory.test.ts:250-431`). The deterministic clock +
// RNG make `_creationTime` and `_id` rise with insertion order, so an
// ascending sort yields insertion order and a descending sort reverses it.

/// Mirrors TS `seedItems` (`ts-client/tests/in_memory.test.ts:254-269`):
/// insert `count` items with `order` = 1..count and `status` cycling
/// through `statuses`. Returns the inserted ids in insertion order.
async fn seed_items(c: &mut InMemoryRtDbClient, count: i64, statuses: &[&str]) -> Vec<String> {
    let mut ids = Vec::new();
    for i in 1..=count {
        let txn = Mutation::new()
            .insert(
                "items",
                json!({
                    "name": format!("n{i}"),
                    "status": statuses[((i - 1) as usize) % statuses.len()],
                    "order": i,
                }),
            )
            .build();
        let results = c.mutate(&txn, None).await.expect("insert ok");
        match &results[0] {
            StepResult::Insert { id } => ids.push(id.clone()),
            other => panic!("expected Insert, got {other:?}"),
        }
    }
    ids
}

/// Walks the full cursor chain until `next_cursor` is absent — ports TS
/// `walkPages` (`ts-client/tests/in_memory.test.ts:272-295`). Returns the
/// observed page sizes, the per-page cursors (final one `None`), and all
/// docs concatenated in page order.
async fn walk_pages<F>(
    c: &InMemoryRtDbClient,
    build: F,
) -> (Vec<usize>, Vec<Option<String>>, Vec<Value>)
where
    F: Fn(Option<&str>) -> Query,
{
    let mut page_sizes = Vec::new();
    let mut cursors = Vec::new();
    let mut docs = Vec::new();
    let mut cursor: Option<String> = None;
    for _ in 0..1000 {
        let page: Paginated<Value> = c.run(&build(cursor.as_deref())).expect("paginate ok");
        page_sizes.push(page.docs.len());
        cursors.push(page.next_cursor.clone());
        docs.extend(page.docs);
        if page.next_cursor.is_none() {
            return (page_sizes, cursors, docs);
        }
        cursor = page.next_cursor;
    }
    panic!("pagination did not terminate");
}

#[tokio::test]
async fn paginate_returns_empty_page_with_no_cursor_on_empty_table() {
    // Ports TS "returns an empty page with no nextCursor on an empty table".
    let c = new_client();
    let page: Paginated<Value> = c
        .run(&TableQuery::new("items").paginate(None, 3))
        .expect("paginate ok");
    assert!(page.docs.is_empty());
    assert!(page.next_cursor.is_none());
}

#[tokio::test]
async fn paginate_walks_all_pages_terminating_on_short_last_page() {
    // Ports TS "walks all pages in order, terminating on a short last page".
    let mut c = new_client();
    seed_items(&mut c, 7, &["todo"]).await;
    let (page_sizes, cursors, docs) =
        walk_pages(&c, |cursor| TableQuery::new("items").paginate(cursor, 3)).await;
    // Page sizes 3, 3, 1; the walk must equal a plain collect() with no
    // skips or duplicates.
    assert_eq!(page_sizes, vec![3, 3, 1]);
    assert!(cursors[..cursors.len() - 1].iter().all(|x| x.is_some()));
    assert!(cursors.last().is_some_and(|x| x.is_none()));

    let collected: Vec<Value> = c
        .run::<Vec<Value>>(&TableQuery::new("items").collect())
        .expect("collect ok");
    let walked_ids: Vec<&str> = docs
        .iter()
        .map(|d| d["_id"].as_str().expect("id string"))
        .collect();
    let collected_ids: Vec<&str> = collected
        .iter()
        .map(|d| d["_id"].as_str().expect("id string"))
        .collect();
    assert_eq!(walked_ids, collected_ids);
    let mut unique = walked_ids.to_vec();
    unique.sort();
    unique.dedup();
    assert_eq!(unique.len(), walked_ids.len(), "no duplicates across pages");
}

#[tokio::test]
async fn paginate_terminates_on_full_last_page_when_count_is_exact_multiple() {
    // Ports TS "terminates on a full last page when the count is an exact
    // multiple": the final page is full but `nextCursor` is None.
    let mut c = new_client();
    seed_items(&mut c, 6, &["todo"]).await;
    let (page_sizes, cursors, _docs) =
        walk_pages(&c, |cursor| TableQuery::new("items").paginate(cursor, 3)).await;
    assert_eq!(page_sizes, vec![3, 3]);
    assert!(cursors[0].is_some());
    assert!(cursors[1].is_none());
}

#[tokio::test]
async fn paginate_within_eq_prefixed_index_in_index_order() {
    // Ports TS "paginates within an eq-prefixed multi-field index in index
    // order": status cycles todo/done/todo ⇒ todos are orders 1,3,4,6,7,9.
    let mut c = new_client();
    seed_items(&mut c, 9, &["todo", "done", "todo"]).await;
    let (page_sizes, _cursors, docs) = walk_pages(&c, |cursor| {
        TableQuery::new("items")
            .with_index("by_status_and_order", &[json!("todo")])
            .paginate(cursor, 4)
    })
    .await;
    assert_eq!(page_sizes, vec![4, 2]);
    let orders: Vec<i64> = docs
        .iter()
        .map(|d| d["order"].as_i64().expect("order number"))
        .collect();
    assert_eq!(orders, vec![1, 3, 4, 6, 7, 9]);
    assert!(docs.iter().all(|d| d["status"] == json!("todo")));
}

#[tokio::test]
async fn paginate_descending_pages_in_reverse_index_order() {
    // Ports TS "walks descending pages in reverse index order": same seed
    // as the asc case, but order=desc ⇒ 9,7,6,4,3,1.
    let mut c = new_client();
    seed_items(&mut c, 9, &["todo", "done", "todo"]).await;
    let (page_sizes, _cursors, docs) = walk_pages(&c, |cursor| {
        TableQuery::new("items")
            .with_index("by_status_and_order", &[json!("todo")])
            .order(Order::Desc)
            .paginate(cursor, 4)
    })
    .await;
    assert_eq!(page_sizes, vec![4, 2]);
    let orders: Vec<i64> = docs
        .iter()
        .map(|d| d["order"].as_i64().expect("order number"))
        .collect();
    assert_eq!(orders, vec![9, 7, 6, 4, 3, 1]);
}

#[tokio::test]
async fn paginate_cursor_round_trips_and_resumes_chain() {
    // Ports TS "emits cursors decodable by the live client; resume
    // continues the chain": the cursor decodes to the last row's
    // [order, _creationTime, _id] tuple — cursors are interchangeable.
    let mut c = new_client();
    seed_items(&mut c, 5, &["todo"]).await; // todo orders 1..5
    let first: Paginated<Value> = c
        .run(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .paginate(None, 2),
        )
        .expect("first page");
    let orders: Vec<i64> = first
        .docs
        .iter()
        .map(|d| d["order"].as_i64().expect("order number"))
        .collect();
    assert_eq!(orders, vec![1, 2]);
    let next_cursor = first.next_cursor.expect("expected a nextCursor");

    // Cursor decodes to [order, _creationTime, _id] of the page's last row.
    let decoded = crate::cursor::decode_cursor(&next_cursor).expect("cursor decodes");
    let last = &first.docs[1];
    assert_eq!(decoded.len(), 3);
    assert_eq!(decoded[0], last["order"]);
    assert_eq!(decoded[1], last["_creationTime"]);
    assert_eq!(decoded[2], last["_id"]);

    let second: Paginated<Value> = c
        .run(
            &TableQuery::new("items")
                .with_index("by_status_and_order", &[json!("todo")])
                .paginate(Some(&next_cursor), 2),
        )
        .expect("second page");
    let orders: Vec<i64> = second
        .docs
        .iter()
        .map(|d| d["order"].as_i64().expect("order number"))
        .collect();
    assert_eq!(orders, vec![3, 4]);
}

#[tokio::test]
async fn paginate_rejects_malformed_cursor_as_bad_request() {
    // Ports TS "rejects a malformed (non-base64) cursor with BAD_REQUEST,
    // not INTERNAL" — the codec returns INTERNAL; the harness rewraps it.
    let mut c = new_client();
    seed_items(&mut c, 3, &["todo"]).await;
    let err = c
        .run_query(&Query {
            table: "items".into(),
            paginate: Some(Paginate {
                cursor: Some("not-valid-base64!!!".into()),
                num_items: 3,
            }),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
}

#[tokio::test]
async fn paginate_rejects_cursor_with_mismatched_arity() {
    // Ports TS "rejects a cursor whose arity mismatches the sort columns":
    // no-index query sorts over 2 columns (createdAt, id); 3 values
    // mismatch.
    let mut c = new_client();
    seed_items(&mut c, 3, &["todo"]).await;
    let bad = crate::cursor::encode_cursor(&[json!(1), json!(2), json!(3)]).expect("encode");
    let err = c
        .run_query(&Query {
            table: "items".into(),
            paginate: Some(Paginate {
                cursor: Some(bad),
                num_items: 3,
            }),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("sorts over 2 column(s)"), "got: {err}");
}

#[tokio::test]
async fn paginate_rejects_cursor_whose_created_at_is_not_a_number() {
    // Ports TS "rejects a cursor whose created_at value is not a number":
    // no-index cursor = [createdAt, id]; a non-numeric createdAt fails
    // type-check.
    let mut c = new_client();
    seed_items(&mut c, 3, &["todo"]).await;
    let bad = crate::cursor::encode_cursor(&[
        json!("not-a-number"),
        json!("0123456789abcdef0123456789abcdef"),
    ])
    .expect("encode");
    let err = c
        .run_query(&Query {
            table: "items".into(),
            paginate: Some(Paginate {
                cursor: Some(bad),
                num_items: 3,
            }),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("created_at must be a number"),
        "got: {err}"
    );
}

#[tokio::test]
async fn paginate_rejects_combination_with_take_count_unique_or_first() {
    // Ports TS "rejects paginate combined with take or count" and extends
    // to unique/first — the validation cascade Task 3 collapsed is now
    // restored (TS :940-955).
    let mut c = new_client();
    seed_items(&mut c, 3, &["todo"]).await;
    for (needle, q) in [
        (
            "take",
            Query {
                table: "items".into(),
                paginate: Some(Paginate {
                    cursor: None,
                    num_items: 3,
                }),
                take: Some(3),
                ..Default::default()
            },
        ),
        (
            "count",
            Query {
                table: "items".into(),
                paginate: Some(Paginate {
                    cursor: None,
                    num_items: 3,
                }),
                count: true,
                ..Default::default()
            },
        ),
        (
            "unique",
            Query {
                table: "items".into(),
                paginate: Some(Paginate {
                    cursor: None,
                    num_items: 3,
                }),
                unique: true,
                ..Default::default()
            },
        ),
        (
            "first",
            Query {
                table: "items".into(),
                paginate: Some(Paginate {
                    cursor: None,
                    num_items: 3,
                }),
                first: true,
                ..Default::default()
            },
        ),
    ] {
        let err = c.run_query(&q).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest, "case '{needle}'");
        assert!(
            err.message.contains(needle),
            "case '{needle}' missing needle: got {}",
            err.message
        );
    }
}

#[tokio::test]
async fn query_search_returns_empty_array_stub() {
    // No in-memory ts_rank — the cascade agrees with the server by
    // returning [] for a valid `search`, while still rejecting conflicting
    // combinations.
    let c = new_client();
    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search("by_content", "hello", ())
                .take(5),
        )
        .expect("search stub");
    assert!(v.is_empty(), "search stub returns []");
}

#[tokio::test]
async fn query_search_rejects_conflicting_terminals() {
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            search: Some(crate::wire::SearchQuery {
                index: "by_content".into(),
                query: "hello".into(),
                filter: None,
                mode: None,
                snippet: None,
            }),
            index: Some("by_name".into()),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("search cannot be combined"),
        "got: {err}"
    );
}

#[tokio::test]
async fn query_search_with_filter_returns_empty_after_narrowing() {
    // ts_rank is unavailable in-memory, so the search stub stays empty; the
    // carried `filter` is still validated and run through `matches_filter`
    // on the (empty) result set, exercising the narrowing path.
    let c = new_client();
    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "hello",
                    SearchOpts {
                        filter: Some(FilterExpr::Eq {
                            field: "status".into(),
                            value: "done".into(),
                        }),
                        mode: None,
                        snippet: None,
                    },
                )
                .take(5),
        )
        .expect("search with filter narrows cleanly");
    assert!(v.is_empty(), "search stub still returns [] after narrowing");
}

#[tokio::test]
async fn query_search_with_unknown_filter_field_is_bad_request() {
    // The search filter runs through `validate_filter` against the table's
    // declared fields, so an unknown field surfaces as BadRequest before
    // the (stub) result is returned.
    let c = new_client();
    let err = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "hello",
                    SearchOpts {
                        filter: Some(FilterExpr::Eq {
                            field: "nonexistent".into(),
                            value: "x".into(),
                        }),
                        mode: None,
                        snippet: None,
                    },
                )
                .take(5),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("nonexistent"), "got: {err}");
}

/// Inserts `(name, status)` rows into `items` in order — the deterministic
/// `new_client()` clock makes later inserts newer (`_creationTime` asc, ids
/// lexicographically asc), which the trgm tie-break tests rely on.
async fn seed_search_items(c: &mut InMemoryRtDbClient, rows: &[(&str, &str)]) {
    for (i, (name, status)) in rows.iter().enumerate() {
        c.mutate(
            &Mutation::new()
                .insert("items", json!({"name": name, "status": status, "order": i}))
                .build(),
            None,
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn query_search_trgm_substring_match_ranks_and_takes() {
    // trgm matches the whole query as a case-insensitive SUBSTRING of an
    // indexed field — "conv" hits "convex"/"Convex"/"convexity appendix",
    // infixes server-side plainto_tsquery stemming cannot match. Ranking is
    // the pinned cross-harness approximation: query.len()/field.len() per
    // containing field, max per doc (shorter field = more similar), then
    // created_at desc, then id desc; `take` truncates after ranking.
    let mut c = new_client();
    seed_search_items(
        &mut c,
        &[
            ("unrelated", "todo"),
            ("convexity appendix", "todo"),
            ("convex", "todo"),
            ("Convex", "todo"),
        ],
    )
    .await;
    let names = |v: &[Value]| -> Vec<String> {
        v.iter()
            .filter_map(|d| d["name"].as_str().map(String::from))
            .collect()
    };

    // Untruncated (take above the match count): all three containing docs,
    // ranked — "Convex"/"convex" tie at 4/6 and the LATER insert wins the
    // created_at tie-break; "convexity appendix" (4/18) ranks last.
    // "unrelated" never matched.
    let all = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "conv",
                    SearchOpts {
                        filter: None,
                        mode: Some(SearchMode::Trgm),
                        snippet: None,
                    },
                )
                .take(10),
        )
        .expect("trgm search without take");
    assert_eq!(
        names(&all),
        ["Convex", "convex", "convexity appendix"].map(String::from)
    );

    // take(2) truncates the ranked list (drops the lowest-similarity doc).
    let capped = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "conv",
                    SearchOpts {
                        filter: None,
                        mode: Some(SearchMode::Trgm),
                        snippet: None,
                    },
                )
                .take(2),
        )
        .expect("trgm search with take");
    assert_eq!(names(&capped), ["Convex", "convex"].map(String::from));
}

#[tokio::test]
async fn query_search_trgm_is_case_insensitive_and_index_scoped() {
    // Containment is lowercased on both sides; only the search index's
    // declared fields (here just `name`) are matched — `status` containing
    // the query never hits.
    let mut c = new_client();
    seed_search_items(&mut c, &[("Shiny Widget", "todo")]).await;
    for query in ["widget", "SHINY", "sHiNy"] {
        let v = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .search(
                        "by_content",
                        query,
                        SearchOpts {
                            filter: None,
                            mode: Some(SearchMode::Trgm),
                            snippet: None,
                        },
                    )
                    .take(5),
            )
            .expect("trgm case-insensitive search");
        assert_eq!(v.len(), 1, "query '{query}' should match");
        assert_eq!(v[0]["name"], json!("Shiny Widget"));
    }
    let none = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "tod", // substring of status="todo", not of name
                    SearchOpts {
                        filter: None,
                        mode: Some(SearchMode::Trgm),
                        snippet: None,
                    },
                )
                .take(5),
        )
        .expect("trgm index-scoped search");
    assert!(none.is_empty(), "non-indexed fields must not match");
}

#[tokio::test]
async fn query_search_trgm_requires_the_whole_query_as_substring() {
    // trgm matches the query as ONE contiguous substring; the tsquery
    // approximation matches per-token. "con vex" over "convex" therefore
    // diverges: both tokens are substrings (tsquery-mode hit) but the phrase
    // is not (trgm miss).
    let mut c = new_client();
    seed_search_items(&mut c, &[("convex", "todo")]).await;
    let tsquery = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search("by_content", "con vex", ())
                .take(5),
        )
        .expect("default-mode search");
    assert_eq!(
        tsquery.len(),
        1,
        "token-AND approximation matches per-token"
    );
    let trgm = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "con vex",
                    SearchOpts {
                        filter: None,
                        mode: Some(SearchMode::Trgm),
                        snippet: None,
                    },
                )
                .take(5),
        )
        .expect("trgm search");
    assert!(trgm.is_empty(), "trgm requires the contiguous phrase");
}

#[tokio::test]
async fn query_search_trgm_composes_with_filter() {
    // The carried FilterExpr narrows BEFORE ranking, so filter + take compose
    // exactly as in tsquery mode.
    let mut c = new_client();
    seed_search_items(
        &mut c,
        &[
            ("convex", "done"),
            ("convexity appendix", "done"),
            ("convex ruler", "todo"),
        ],
    )
    .await;
    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "conv",
                    SearchOpts {
                        filter: Some(FilterExpr::Eq {
                            field: "status".into(),
                            value: "done".into(),
                        }),
                        mode: Some(SearchMode::Trgm),
                        snippet: None,
                    },
                )
                .take(1),
        )
        .expect("trgm search with filter");
    // "convex ruler" matches the substring but is filtered out; among the
    // done docs "convex" (4/6) outranks "convexity appendix" (4/18) and
    // take(1) keeps only it.
    assert_eq!(v.len(), 1);
    assert_eq!(v[0]["name"], json!("convex"));
}

#[tokio::test]
async fn query_search_explicit_tsquery_mode_equals_omitted() {
    // Explicit SearchMode::Tsquery routes through the same default path —
    // results identical to mode omitted (both run against the same unchanged
    // store, so array order is comparable).
    let mut c = new_client();
    seed_search_items(&mut c, &[("alpha beta", "todo"), ("gamma", "todo")]).await;
    let omitted = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search("by_content", "alpha beta", ())
                .take(5),
        )
        .expect("search with mode omitted");
    let explicit = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "alpha beta",
                    SearchOpts {
                        filter: None,
                        mode: Some(SearchMode::Tsquery),
                        snippet: None,
                    },
                )
                .take(5),
        )
        .expect("search with explicit tsquery");
    assert_eq!(omitted.len(), 1, "token-AND matches the containing doc");
    assert_eq!(omitted, explicit, "explicit tsquery == default");
}

#[tokio::test]
async fn query_search_rejects_empty_query_in_both_modes() {
    // Empty (or whitespace-only) query text is BadRequest before the mode
    // branch — mirrors server `compile_search` and the ts/python harnesses.
    let mut c = new_client();
    seed_search_items(&mut c, &[("convex", "todo")]).await;
    for mode in [None, Some(SearchMode::Tsquery), Some(SearchMode::Trgm)] {
        for query in ["", "   "] {
            let err = c
                .run::<Vec<Value>>(
                    &TableQuery::new("items")
                        .search(
                            "by_content",
                            query,
                            SearchOpts {
                                filter: None,
                                mode,
                                snippet: None,
                            },
                        )
                        .take(5),
                )
                .unwrap_err();
            assert_eq!(err.code, ErrorCode::BadRequest, "mode {mode:?}");
            assert_eq!(err.message, "search query text must not be empty");
        }
    }
}

#[tokio::test]
async fn query_search_requires_a_search_index_in_both_modes() {
    // The index check lives in the shared prologue (server `compile_search`
    // runs it before the mode branch), so a btree index name is rejected for
    // tsquery (the default) too — not just trgm.
    let mut c = new_client();
    seed_search_items(&mut c, &[("convex", "todo")]).await;
    for mode in [None, Some(SearchMode::Tsquery), Some(SearchMode::Trgm)] {
        let err = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .search(
                        "by_name",
                        "convex",
                        SearchOpts {
                            filter: None,
                            mode,
                            snippet: None,
                        },
                    )
                    .take(5),
            )
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest, "mode {mode:?}");
        assert_eq!(err.message, "search index 'by_name' not found");
    }
}

#[tokio::test]
async fn query_search_phrase_requires_adjacent_words() {
    // A quoted phrase requires the words ADJACENT (FM-31): only the doc where
    // "database notes" appears contiguously matches; the doc carrying the
    // same words apart does not. Unquoted, the same words stay ANDed — so
    // both docs match — pinning plain-query equivalence with the pre-FM-31
    // token-AND behavior through the websearch upgrade (mirrors the server's
    // `phrase_query_requires_adjacent_words`).
    let mut c = new_client();
    seed_search_items(
        &mut c,
        &[
            ("the database notes are great", "todo"),
            ("notes about the database", "todo"),
        ],
    )
    .await;
    let names = |v: &[Value]| -> Vec<String> {
        v.iter()
            .filter_map(|d| d["name"].as_str().map(String::from))
            .collect()
    };

    let phrase = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search("by_content", "\"database notes\"", ())
                .take(5),
        )
        .expect("phrase search");
    assert_eq!(
        names(&phrase),
        ["the database notes are great".to_string()],
        "only the adjacent doc matches a quoted phrase"
    );

    let plain = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search("by_content", "database notes", ())
                .take(5),
        )
        .expect("plain AND search");
    let plain_names = names(&plain);
    assert_eq!(plain_names.len(), 2, "unquoted terms stay ANDed");
    assert!(plain_names.contains(&"the database notes are great".to_string()));
    assert!(plain_names.contains(&"notes about the database".to_string()));
}

#[tokio::test]
async fn query_search_or_operator_unions_alternatives() {
    // The bare word `or` unions alternatives (FM-31): a doc with either term
    // matches; an unrelated doc does not (mirrors the server's
    // `or_operator_unions_alternatives`).
    let mut c = new_client();
    seed_search_items(
        &mut c,
        &[
            ("alpha only", "todo"),
            ("beta only", "todo"),
            ("gamma", "todo"),
        ],
    )
    .await;
    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search("by_content", "alpha or beta", ())
                .take(5),
        )
        .expect("or search");
    let names: Vec<&str> = v.iter().filter_map(|d| d["name"].as_str()).collect();
    assert_eq!(names.len(), 2);
    assert!(names.contains(&"alpha only"));
    assert!(names.contains(&"beta only"));
    assert!(!names.contains(&"gamma"));
}

#[tokio::test]
async fn query_search_minus_operator_excludes_term() {
    // `-term` excludes docs carrying the negated word while keeping the
    // positive one (FM-31; mirrors the server's `minus_operator_excludes_term`).
    let mut c = new_client();
    seed_search_items(
        &mut c,
        &[("database intro", "todo"), ("database cooking", "todo")],
    )
    .await;
    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search("by_content", "database -cooking", ())
                .take(5),
        )
        .expect("minus search");
    let names: Vec<&str> = v.iter().filter_map(|d| d["name"].as_str()).collect();
    assert_eq!(names, ["database intro"]);
}

#[tokio::test]
async fn query_search_snippet_marks_matched_terms() {
    // snippet: true attaches a `_searchSnippet` to every hit — a ≤35-word
    // excerpt with the matched word wrapped in <mark> (FM-31). Omitted or
    // explicitly false, no snippet field appears (mirrors the server's
    // `snippet_returns_highlighted_fragment` /
    // `snippet_false_behaves_like_omitted`).
    let mut c = new_client();
    seed_search_items(&mut c, &[("the database notes are great", "todo")]).await;

    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "database",
                    SearchOpts {
                        filter: None,
                        mode: None,
                        snippet: Some(true),
                    },
                )
                .take(5),
        )
        .expect("snippet search");
    assert_eq!(v.len(), 1);
    let snippet = v[0]["_searchSnippet"].as_str().expect("snippet string");
    assert!(
        snippet.contains("<mark>database</mark>"),
        "no highlighted term in {snippet}"
    );
    assert!(
        snippet.split_whitespace().count() <= 35,
        "snippet exceeds the word bound: {snippet}"
    );

    // Omitted snippet: no field.
    let plain = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search("by_content", "database", ())
                .take(5),
        )
        .expect("plain search");
    assert_eq!(plain.len(), 1);
    assert!(
        plain[0].get("_searchSnippet").is_none(),
        "snippet field present without snippet: true"
    );

    // Explicit `Some(false)` behaves exactly like omission.
    let off = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "database",
                    SearchOpts {
                        filter: None,
                        mode: None,
                        snippet: Some(false),
                    },
                )
                .take(5),
        )
        .expect("snippet-false search");
    assert_eq!(off.len(), 1);
    assert!(off[0].get("_searchSnippet").is_none());
}

#[tokio::test]
async fn query_search_snippet_highlights_phrase_queries() {
    // The snippet render highlights the PHRASE words too — like the server's
    // ts_headline, each matched word carries its own <mark>, adjacent for a
    // phrase hit (mirrors `snippet_highlights_phrase_queries`).
    let mut c = new_client();
    seed_search_items(&mut c, &[("the database notes are great", "todo")]).await;
    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "\"database notes\"",
                    SearchOpts {
                        filter: None,
                        mode: None,
                        snippet: Some(true),
                    },
                )
                .take(5),
        )
        .expect("phrase snippet search");
    assert_eq!(v.len(), 1);
    let snippet = v[0]["_searchSnippet"].as_str().expect("snippet string");
    assert!(
        snippet.contains("<mark>database</mark> <mark>notes</mark>"),
        "phrase words not contiguously highlighted in {snippet}"
    );
}

#[tokio::test]
async fn query_search_snippet_rejected_with_trgm_mode() {
    // snippet + trgm is rejected up front — trgm matches substrings, so
    // there is no tsquery tree to highlight (mirrors the server's
    // `snippet_rejected_with_trgm_mode`).
    let c = new_client();
    let err = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .search(
                    "by_content",
                    "conv",
                    SearchOpts {
                        filter: None,
                        mode: Some(SearchMode::Trgm),
                        snippet: Some(true),
                    },
                )
                .take(5),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("tsquery mode"), "got: {err}");
}

#[tokio::test]
async fn query_vector_search_returns_empty_array_stub() {
    // The TS harness rejects `vectorSearch` combined with any other
    // terminal (including `take`) — unlike `search`, vectorSearch carries
    // its own `limit`. So the bare-stub path is exercised without a
    // trailing terminal.
    let c = new_client();
    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .vector_search("by_embedding", vec![1.0, 0.0, 0.0], 5, ())
                .build(),
        )
        .expect("vector stub");
    assert!(v.is_empty(), "vector stub returns []");
}

#[tokio::test]
async fn query_vector_search_rejects_conflicting_terminals() {
    let c = new_client();
    let err = c
        .run_query(&Query {
            table: "items".into(),
            vector_search: Some(crate::wire::VectorSearchQuery {
                index: "by_embedding".into(),
                vector: vec![1.0],
                limit: 5,
                filter: None,
            }),
            index: Some("by_name".into()),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("vectorSearch cannot be combined"),
        "got: {err}"
    );
}

#[tokio::test]
async fn query_vector_search_with_filter_returns_empty_after_narrowing() {
    // No in-memory vector ranking, so the vector stub stays empty; the
    // carried `filter` (a `FilterExpr`) is still validated and run through
    // `matches_filter` on the (empty) candidate set, exercising the same
    // narrowing path as the `search` terminal.
    let c = new_client();
    let v = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .vector_search(
                    "by_embedding",
                    vec![1.0, 0.0, 0.0],
                    5,
                    VectorSearchOpts {
                        filter: Some(FilterExpr::Eq {
                            field: "status".into(),
                            value: "done".into(),
                        }),
                    },
                )
                .build(),
        )
        .expect("vector search with filter narrows cleanly");
    assert!(v.is_empty(), "vector stub still returns [] after narrowing");
}

#[tokio::test]
async fn query_vector_search_with_unknown_filter_field_is_bad_request() {
    // The vector-search filter runs through `validate_filter` against the
    // table's declared fields, so an unknown field surfaces as BadRequest
    // before the (stub) result is returned.
    let c = new_client();
    let err = c
        .run::<Vec<Value>>(
            &TableQuery::new("items")
                .vector_search(
                    "by_embedding",
                    vec![1.0, 0.0, 0.0],
                    5,
                    VectorSearchOpts {
                        filter: Some(FilterExpr::Eq {
                            field: "nonexistent".into(),
                            value: "x".into(),
                        }),
                    },
                )
                .build(),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("nonexistent"), "got: {err}");
}

// ---- filter: eval_filter_expr + validate_filter ----------------
//
// Direct unit tests for the filter evaluator + validator, ported verbatim
// from `describe("evalFilterExpr + validateFilter")`
// (`ts-client/tests/in_memory.test.ts:539-653`). These are the cases item C
// fixed in the TS source — E must not regress them.

/// The field set used by the unit tests below — mirrors the TS
/// `new Set(["name", "age", "active", "score", "tags"])`.
fn filter_unit_fields() -> BTreeSet<String> {
    ["name", "age", "active", "score", "tags"]
        .iter()
        .map(|s| s.to_string())
        .collect()
}

#[test]
fn eval_filter_eq_neq_on_strings_compare_the_doc_field_text() {
    let fields = filter_unit_fields();
    validate_filter(
        &FilterExpr::Eq {
            field: "name".into(),
            value: json!("ada"),
        },
        &fields,
    )
    .expect("valid");
    assert!(eval_filter_expr(
        &FilterExpr::Eq {
            field: "name".into(),
            value: json!("ada"),
        },
        &json!({"name": "ada"}),
    ));
    assert!(!eval_filter_expr(
        &FilterExpr::Eq {
            field: "name".into(),
            value: json!("ada"),
        },
        &json!({"name": "bob"}),
    ));
    assert!(eval_filter_expr(
        &FilterExpr::Neq {
            field: "name".into(),
            value: json!("ada"),
        },
        &json!({"name": "bob"}),
    ));
}

#[test]
fn eval_filter_number_domain_compares_numerically() {
    // gt/gte/lt/lte over a numeric doc field.
    assert!(eval_filter_expr(
        &FilterExpr::Gt {
            field: "age".into(),
            value: json!(30),
        },
        &json!({"age": 42}),
    ));
    assert!(!eval_filter_expr(
        &FilterExpr::Gt {
            field: "age".into(),
            value: json!(50),
        },
        &json!({"age": 42}),
    ));
    assert!(eval_filter_expr(
        &FilterExpr::Lte {
            field: "age".into(),
            value: json!(42),
        },
        &json!({"age": 42}),
    ));
}

#[test]
fn eval_filter_string_ordering_is_lexicographic() {
    assert!(eval_filter_expr(
        &FilterExpr::Lt {
            field: "name".into(),
            value: json!("b"),
        },
        &json!({"name": "ada"}),
    ));
    assert!(eval_filter_expr(
        &FilterExpr::Gte {
            field: "name".into(),
            value: json!("a"),
        },
        &json!({"name": "ada"}),
    ));
}

#[test]
fn eval_filter_boolean_domain_compares_booleans() {
    assert!(eval_filter_expr(
        &FilterExpr::Eq {
            field: "active".into(),
            value: json!(true),
        },
        &json!({"active": true}),
    ));
    assert!(!eval_filter_expr(
        &FilterExpr::Eq {
            field: "active".into(),
            value: json!(true),
        },
        &json!({"active": false}),
    ));
}

#[test]
fn eval_filter_number_value_matches_a_numeric_string_field() {
    // float8 cast: doc field is the string "5", filter value is the number
    // 5 → match. Mirrors Postgres `(doc->>'field')::float8 = 5`.
    assert!(eval_filter_expr(
        &FilterExpr::Eq {
            field: "score".into(),
            value: json!(5),
        },
        &json!({"score": "5"}),
    ));
}

#[test]
fn eval_filter_null_or_absent_doc_field_never_matches() {
    // SQL NULL exclusion: null/absent never matches any op (even neq).
    assert!(!eval_filter_expr(
        &FilterExpr::Eq {
            field: "name".into(),
            value: json!("ada"),
        },
        &json!({"name": null}),
    ));
    assert!(!eval_filter_expr(
        &FilterExpr::Eq {
            field: "name".into(),
            value: json!("ada"),
        },
        &json!({}),
    ));
    assert!(!eval_filter_expr(
        &FilterExpr::Neq {
            field: "name".into(),
            value: json!("ada"),
        },
        &json!({}),
    ));
}

#[test]
fn eval_filter_and_or_nest_recursively() {
    let expr = FilterExpr::And {
        exprs: vec![
            FilterExpr::Gte {
                field: "age".into(),
                value: json!(30),
            },
            FilterExpr::Or {
                exprs: vec![
                    FilterExpr::Eq {
                        field: "name".into(),
                        value: json!("ada"),
                    },
                    FilterExpr::Eq {
                        field: "name".into(),
                        value: json!("bob"),
                    },
                ],
            },
        ],
    };
    assert!(eval_filter_expr(&expr, &json!({"age": 42, "name": "ada"})));
    assert!(!eval_filter_expr(&expr, &json!({"age": 42, "name": "zed"})));
    assert!(!eval_filter_expr(&expr, &json!({"age": 10, "name": "ada"})));
}

#[test]
fn eval_filter_in_matches_membership() {
    assert!(eval_filter_expr(
        &FilterExpr::In {
            field: "name".into(),
            values: vec![json!("ada"), json!("bob")],
        },
        &json!({"name": "bob"}),
    ));
    assert!(!eval_filter_expr(
        &FilterExpr::In {
            field: "name".into(),
            values: vec![json!("ada"), json!("bob")],
        },
        &json!({"name": "zed"}),
    ));
}

#[test]
fn validate_filter_rejects_an_unknown_field() {
    let fields = filter_unit_fields();
    let err = validate_filter(
        &FilterExpr::Eq {
            field: "missing".into(),
            value: json!("x"),
        },
        &fields,
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("unknown field"), "got: {err}");
}

#[test]
fn validate_filter_rejects_empty_and_or_and_empty_in() {
    let fields = filter_unit_fields();
    let err = validate_filter(&FilterExpr::And { exprs: vec![] }, &fields).unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("at least one expr"), "got: {err}");

    let err = validate_filter(&FilterExpr::Or { exprs: vec![] }, &fields).unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("at least one expr"), "got: {err}");

    let err = validate_filter(
        &FilterExpr::In {
            field: "name".into(),
            values: vec![],
        },
        &fields,
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("at least one value"), "got: {err}");
}

#[test]
fn validate_filter_rejects_a_non_string_number_boolean_value() {
    let fields = filter_unit_fields();
    let err = validate_filter(
        &FilterExpr::Eq {
            field: "name".into(),
            value: Value::Null,
        },
        &fields,
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("string, number, or boolean"),
        "got: {err}"
    );

    let err = validate_filter(
        &FilterExpr::Eq {
            field: "tags".into(),
            value: json!(["a"]),
        },
        &fields,
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("string, number, or boolean"),
        "got: {err}"
    );
}

#[test]
fn validate_filter_accepts_a_well_formed_nested_filter() {
    let fields = filter_unit_fields();
    validate_filter(
        &FilterExpr::And {
            exprs: vec![
                FilterExpr::Eq {
                    field: "name".into(),
                    value: json!("ada"),
                },
                FilterExpr::In {
                    field: "age".into(),
                    values: vec![json!(1), json!(2)],
                },
            ],
        },
        &fields,
    )
    .expect("well-formed nested filter");
}

#[test]
fn validate_filter_rejects_mixed_type_in_values() {
    let fields = filter_unit_fields();
    let err = validate_filter(
        &FilterExpr::In {
            field: "age".into(),
            values: vec![json!(5), json!("ada")],
        },
        &fields,
    )
    .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(err.message.contains("same type"), "got: {err}");
}

#[test]
fn validate_filter_accepts_same_type_in_values() {
    let fields = filter_unit_fields();
    validate_filter(
        &FilterExpr::In {
            field: "age".into(),
            values: vec![json!(5), json!(6), json!(7)],
        },
        &fields,
    )
    .expect("same-type in values");
}

// ---- query: filter end-to-end ----------------------------------
//
// Ports `describe("InMemoryRtDbClient filter")`
// (`ts-client/tests/in_memory.test.ts:655-756`) — exercises the typed
// `TableQuery.filter(...)` builder end-to-end through `run_query`, the
// same surface live app code uses.

/// Self-contained `users` schema so this block doesn't perturb the shared
/// `items` harness above. Mirrors the TS `usersSchema`.
fn users_schema() -> SchemaDef {
    Schema::builder()
        .table(
            "users",
            Table::new()
                .field("name", FieldType::String)
                .field("age", FieldType::Number)
                .field("active", FieldType::Boolean)
                .index("by_name", &["name"]),
        )
        .build()
}

fn new_users_client() -> InMemoryRtDbClient {
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
    client.push_schema(&users_schema()).unwrap();
    client
}

async fn seed_users(c: &mut InMemoryRtDbClient) {
    for (name, age, active) in [("ada", 42_i64, true), ("bob", 17, false), ("cy", 65, true)] {
        c.mutate(
            &Mutation::new()
                .insert("users", json!({"name": name, "age": age, "active": active}))
                .build(),
            None,
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn query_filter_reduces_the_result_set_to_matching_docs() {
    let mut c = new_users_client();
    seed_users(&mut c).await;
    let docs = c
        .run::<Vec<Value>>(
            &TableQuery::new("users")
                .filter(FilterExpr::Gt {
                    field: "age".into(),
                    value: json!(20),
                })
                .collect(),
        )
        .expect("filter query ok");
    let mut names: Vec<String> = docs
        .iter()
        .map(|d| d["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["ada".to_string(), "cy".to_string()]);
}

#[tokio::test]
async fn query_filter_composes_with_an_index_eq_prefix_and_take() {
    let mut c = new_users_client();
    seed_users(&mut c).await;
    let docs = c
        .run::<Vec<Value>>(
            &TableQuery::new("users")
                .with_index("by_name", &[json!("ada")])
                .filter(FilterExpr::Eq {
                    field: "active".into(),
                    value: json!(true),
                })
                .take(10),
        )
        .expect("filter+index ok");
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["name"], json!("ada"));
}

#[tokio::test]
async fn query_and_or_in_filter_evaluates_correctly_end_to_end() {
    let mut c = new_users_client();
    seed_users(&mut c).await;

    let docs = c
        .run::<Vec<Value>>(
            &TableQuery::new("users")
                .filter(FilterExpr::Or {
                    exprs: vec![
                        FilterExpr::Lt {
                            field: "age".into(),
                            value: json!(18),
                        },
                        FilterExpr::Gte {
                            field: "age".into(),
                            value: json!(65),
                        },
                    ],
                })
                .collect(),
        )
        .expect("or filter ok");
    let mut names: Vec<String> = docs
        .iter()
        .map(|d| d["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["bob".to_string(), "cy".to_string()]);

    let in_docs = c
        .run::<Vec<Value>>(
            &TableQuery::new("users")
                .filter(FilterExpr::In {
                    field: "name".into(),
                    values: vec![json!("ada"), json!("cy")],
                })
                .collect(),
        )
        .expect("in filter ok");
    let mut names: Vec<String> = in_docs
        .iter()
        .map(|d| d["name"].as_str().unwrap().to_string())
        .collect();
    names.sort();
    assert_eq!(names, vec!["ada".to_string(), "cy".to_string()]);
}

#[tokio::test]
async fn query_filter_unknown_field_throws_bad_request() {
    let mut c = new_users_client();
    seed_users(&mut c).await;
    let err = c
        .run_query(
            &TableQuery::new("users")
                .filter(FilterExpr::Eq {
                    field: "nope".into(),
                    value: json!("x"),
                })
                .collect(),
        )
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
}

#[tokio::test]
async fn query_filter_combined_with_get_is_rejected() {
    // Mirrors the server: `get` is exclusive of `filter` (and everything
    // else); the get-exclusivity guard fires before filter validation.
    let mut c = new_users_client();
    let r = c
        .mutate(
            &Mutation::new()
                .insert("users", json!({"name": "ada", "age": 42, "active": true}))
                .build(),
            None,
        )
        .await
        .unwrap();
    let id = match &r[0] {
        StepResult::Insert { id } => id.clone(),
        _ => unreachable!(),
    };
    let err = c
        .run_query(&Query {
            table: "users".into(),
            get: Some(id),
            filter: Some(FilterExpr::Eq {
                field: "age".into(),
                value: json!(42),
            }),
            ..Default::default()
        })
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
}

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

// ---- storage ----------------------------------------------------------
//
// The TS suite does not cover storage directly (the harness ships it as an
// honest stub); these exercise the surface so the wire shapes stay aligned
// with the live HTTP client (`crate::http::UploadResult` /
// `crate::http::FileMetadata`).

#[test]
fn upload_stores_bytes_and_returns_id_sha_size_and_content_type() {
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let bytes = b"hello world".to_vec();
    let result = c
        .upload(bytes.clone(), Some("text/plain".to_string()))
        .expect("upload ok");
    // Id is `f<base36>` — distinct in shape from a 32-hex-char doc id.
    assert!(result.id.starts_with('f'), "id shape: {}", result.id);
    assert_eq!(result.size, bytes.len() as i64);
    assert_eq!(result.content_type.as_deref(), Some("text/plain"));
    // SHA-256 of "hello world" is a known constant — verifies we computed
    // it correctly (not just non-empty).
    assert_eq!(
        result.sha256,
        "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
    );
}

#[test]
fn upload_without_content_type_returns_none() {
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let result = c.upload(b"x".to_vec(), None).expect("upload ok");
    assert!(result.content_type.is_none());
}

#[test]
fn upload_mints_distinct_ids_for_distinct_uploads() {
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let a = c.upload(b"a".to_vec(), None).expect("upload ok");
    let b = c.upload(b"b".to_vec(), None).expect("upload ok");
    assert_ne!(a.id, b.id, "ids distinct");
}

#[test]
fn get_file_metadata_returns_size_and_creation_time() {
    // Mirrors the TS harness: getFileMetadata's sha256 is "" (only the
    // upload result carries the real digest).
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let up = c
        .upload(
            b"abc".to_vec(),
            Some("application/octet-stream".to_string()),
        )
        .expect("upload ok");
    let meta = c.get_file_metadata(&up.id).expect("metadata ok");
    assert_eq!(meta.id, up.id);
    assert_eq!(meta.size, 3);
    assert_eq!(meta.sha256, "");
    assert_eq!(
        meta.content_type.as_deref(),
        Some("application/octet-stream")
    );
    assert!(meta.creation_time > 0);
}

#[test]
fn get_file_metadata_unknown_id_is_not_found() {
    let c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let err = c.get_file_metadata("f99").unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
}

#[test]
fn delete_file_removes_the_blob_and_rejects_unknown_id_with_not_found() {
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let up = c.upload(b"x".to_vec(), None).expect("upload ok");
    c.delete_file(&up.id).expect("delete ok");
    // Second delete fails — NOT_FOUND (idempotent on the live server, but
    // the in-memory harness mirrors the TS surface which throws on miss).
    let err = c.delete_file(&up.id).unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
}

#[test]
fn get_url_returns_synthetic_memory_handle() {
    let c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    assert_eq!(c.get_url("f1"), "memory://f1");
}

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

// ---- presence ----------------------------------------------------------
//
// Ports the presence surface of `ts-client/src/in_memory.ts:1217-1285`.
// A private PresenceRooms sees only self; a shared backing lets two clients
// see each other's joins/updates/leaves — approximating the server's
// per-connection registry for tests.

fn new_presence_client(conn: &str, rooms: Arc<Mutex<PresenceRooms>>) -> InMemoryRtDbClient {
    InMemoryRtDbClient::new(
        InMemoryRtDbClientOptions::default()
            .connection_id(conn)
            .presence_user(AuthedUser {
                kind: crate::wire::UserKind::User,
                email: Some(format!("{conn}@x.com")),
                name: None,
                github_login: None,
                github_id: None,
            })
            .presence_rooms(rooms),
    )
}

#[tokio::test]
async fn presence_join_fires_initial_snapshot_with_self() {
    // Brief: join a room; callback fires immediately with a one-member
    // snapshot (the joining connection itself).
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default().connection_id("c1"));
    let snaps: Arc<Mutex<Vec<Vec<PresenceMember>>>> = Arc::new(Mutex::new(Vec::new()));
    let snaps_clone = snaps.clone();
    let _h = c.presence("doc:1", Some(json!({"cursor": 5})), move |members| {
        snaps_clone.lock().unwrap().push(members);
    });
    let got = snaps.lock().unwrap();
    assert_eq!(got.len(), 1, "initial snapshot delivered on join");
    assert_eq!(got[0].len(), 1);
    assert_eq!(got[0][0].connection_id, "c1");
    assert_eq!(got[0][0].state, json!({"cursor": 5}));
}

#[tokio::test]
async fn presence_update_broadcasts_new_state() {
    // Brief: update_presence fans out a fresh snapshot with the new state.
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default().connection_id("c1"));
    let snaps: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let snaps_clone = snaps.clone();
    let _h = c.presence("room", None, move |members| {
        if let Some(m) = members.first() {
            snaps_clone.lock().unwrap().push(m.state.clone());
        }
    });
    c.update_presence("room", json!({"typing": true}), None);
    let got = snaps.lock().unwrap();
    assert_eq!(got.len(), 2, "initial + update");
    assert_eq!(got[1], json!({"typing": true}));
}

#[tokio::test]
async fn presence_update_noop_for_unjoined_room() {
    // Brief: update_presence on a room we haven't joined does nothing.
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let snaps: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let snaps_clone = snaps.clone();
    let _h = c.presence("room", None, move |members| {
        snaps_clone.lock().unwrap().push(members.len());
    });
    // Update a different room — no fan-out for "room".
    c.update_presence("other", json!({}), None);
    assert_eq!(snaps.lock().unwrap().len(), 1, "no new snapshot");
}

#[tokio::test]
async fn presence_leave_removes_member_and_drops_listeners() {
    // Brief: leave_presence removes the member and fans out; further updates
    // to the room from a peer do not invoke the (now-dropped) callback.
    let rooms = Arc::new(Mutex::new(PresenceRooms::default()));
    let mut c1 = new_presence_client("c1", rooms.clone());
    let mut c2 = new_presence_client("c2", rooms.clone());

    let c1_snaps: Arc<Mutex<Vec<usize>>> = Arc::new(Mutex::new(Vec::new()));
    let c1_snaps_clone = c1_snaps.clone();
    let h1 = c1.presence("room", None, move |members| {
        c1_snaps_clone.lock().unwrap().push(members.len());
    });

    // c2 joins → c1 sees 2 members.
    let _h2 = c2.presence("room", None, |_| {});
    assert_eq!(*c1_snaps.lock().unwrap(), [1, 2]);

    // c1 leaves → its listener is dropped; the fan-out goes to remaining
    // listeners only. h1 is now inert.
    c1.leave_presence("room");
    drop(h1);

    // c2 updates — c1's callback must not fire (listener dropped).
    c2.update_presence("room", json!({"x": 1}), None);
    assert_eq!(
        *c1_snaps.lock().unwrap(),
        [1, 2],
        "no further fire after leave"
    );
}

#[tokio::test]
async fn presence_two_clients_on_shared_rooms_see_each_other() {
    // Brief: two clients sharing a PresenceRooms instance see each other's
    // joins and leaves — approximating the server's per-db registry.
    let rooms = Arc::new(Mutex::new(PresenceRooms::default()));
    let mut c1 = new_presence_client("c1", rooms.clone());
    let mut c2 = new_presence_client("c2", rooms.clone());

    let c1_snaps: Arc<Mutex<Vec<Vec<String>>>> = Arc::new(Mutex::new(Vec::new()));
    let c1_snaps_clone = c1_snaps.clone();
    let _h1 = c1.presence("room", None, move |members| {
        let ids: Vec<String> = members.into_iter().map(|m| m.connection_id).collect();
        c1_snaps_clone.lock().unwrap().push(ids);
    });

    // c2 joins → c1 sees [c1, c2].
    let _h2 = c2.presence("room", None, |_| {});
    {
        let got = c1_snaps.lock().unwrap();
        assert_eq!(got.len(), 2, "initial self + c2 join");
        assert_eq!(got[1], ["c1", "c2"]);
    }

    // c2 leaves → c1 sees [c1] again.
    c2.leave_presence("room");
    {
        let got = c1_snaps.lock().unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[2], ["c1"]);
    }
}

// ---- presence ttl (ENH-015) ------------------------------------------
//
// Mirrors `PresenceRooms.expire` + `update(..., ttlMs, now)` in
// `ts-client/src/in_memory.ts`: a refresh with a ttl schedules an expiry
// sweep that nulls this member's `state` to Value::Null at `now + ttl`
// (the member stays listed); a refresh with no ttl clears any pending
// expiry. Mirrors the live server's `expire_once`.
//
// These tests drive `PresenceRooms` directly with controlled `now` values
// (the harness's `update`/`expire` take `now` explicitly) so the expiry
// math is deterministic without relying on the client's injected clock.
// The client-surface helper is covered separately below.

fn presence_member(conn: &str, state: Value) -> PresenceMember {
    PresenceMember {
        connection_id: conn.to_string(),
        user: AuthedUser {
            kind: crate::wire::UserKind::User,
            email: Some(format!("{conn}@x.com")),
            name: None,
            github_login: None,
            github_id: None,
        },
        state,
    }
}

#[tokio::test]
async fn presence_ttl_expires_state_to_null_member_stays() {
    // Brief: c1 and c2 share a PresenceRooms. c1 updates with ttl_ms = 1000
    // at t = 5000. At t = 5999 nothing has expired. At t = 6000+ the sweep
    // nulls c1's state, c2 observes the null, c1 is still a member.
    let mut rooms = PresenceRooms::default();

    let c2_states: Arc<Mutex<Vec<(Value, String)>>> = Arc::new(Mutex::new(Vec::new()));
    let c2_states_clone = c2_states.clone();
    let _h2 = rooms.subscribe("room", move |members| {
        if let Some(c1) = members.iter().find(|m| m.connection_id == "c1") {
            c2_states_clone
                .lock()
                .unwrap()
                .push((c1.state.clone(), c1.connection_id.clone()));
        }
    });

    // c1 joins, then refreshes with a ttl at t = 5000.
    rooms.join("room", presence_member("c1", Value::Null));
    rooms.update("room", "c1", json!({"typing": true}), Some(1000), 5000);
    {
        let got = c2_states.lock().unwrap();
        // Two observations of c1's state so far: c1 join (null), c1 update
        // (typing). (c2 has no presence entry — it only subscribes.)
        assert_eq!(got.len(), 2);
        assert_eq!(got[1].0, json!({"typing": true}));
    }

    // Before expiry: no change, expire returns false.
    assert!(!rooms.expire(5999));
    {
        let got = c2_states.lock().unwrap();
        assert_eq!(got.len(), 2, "no fire before expiry");
    }

    // At/after expiry: state → null, member stays, expire returns true.
    assert!(rooms.expire(6000));
    {
        let got = c2_states.lock().unwrap();
        assert_eq!(got.len(), 3, "one fire on expiry");
        assert_eq!(got[2].0, Value::Null, "state cleared to null");
        assert_eq!(got[2].1, "c1", "member stays in the room");
    }
    let snap = rooms.snapshot("room");
    assert_eq!(snap.len(), 1, "member stays listed after expiry");
    assert_eq!(snap[0].state, Value::Null);

    // Idempotent: a second sweep at the same instant is a no-op.
    assert!(!rooms.expire(6000));
    {
        let got = c2_states.lock().unwrap();
        assert_eq!(got.len(), 3, "no further fire");
    }
}

#[tokio::test]
async fn presence_ttl_refresh_without_ttl_clears_expiry() {
    // Brief: a refresh with ttl_ms = None clears any pending expiry — the
    // state persists past the original expiry instant.
    let mut rooms = PresenceRooms::default();
    rooms.join("room", presence_member("c1", Value::Null));
    rooms.update("room", "c1", json!({"typing": true}), Some(1000), 5000);
    rooms.update("room", "c1", json!({"typing": false}), None, 5500);
    // Past the original expiry instant — no expiry, state persists.
    assert!(!rooms.expire(10_000));
    let snap = rooms.snapshot("room");
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].state, json!({"typing": false}));
}

#[tokio::test]
async fn presence_ttl_leave_clears_expiry_entry() {
    // Brief: leaving clears the expiry entry, so a re-join with the same
    // connectionId does not inherit a stale ttl.
    let mut rooms = PresenceRooms::default();
    rooms.join("room", presence_member("c1", Value::Null));
    rooms.update("room", "c1", json!({"typing": true}), Some(1000), 5000);
    rooms.leave("room", "c1");
    // After leave, the expiry map should be empty (no fire, no panic).
    assert!(!rooms.expire(10_000));
    // And re-join with the same connId does not carry the old ttl.
    rooms.join("room", presence_member("c1", json!({"fresh": true})));
    assert!(!rooms.expire(10_000));
    let snap = rooms.snapshot("room");
    assert_eq!(snap.len(), 1);
    assert_eq!(snap[0].state, json!({"fresh": true}));
}

#[tokio::test]
async fn presence_ttl_client_expire_presence_helper() {
    // Brief: the client's `expire_presence(now)` helper drives the same
    // sweep through the client's injected clock, mirroring `tick` for the
    // document reaper. Two clients on shared rooms; one updates with a
    // short ttl; the other observes the null at expiry.
    let t: Arc<Mutex<i64>> = Arc::new(Mutex::new(0));
    let t_clone = t.clone();
    let rooms = Arc::new(Mutex::new(PresenceRooms::default()));
    let make = |conn: &'static str| {
        let t = t_clone.clone();
        let rooms = rooms.clone();
        InMemoryRtDbClient::new(
            InMemoryRtDbClientOptions::default()
                .connection_id(conn)
                .now(move || *t.lock().unwrap())
                .presence_user(AuthedUser {
                    kind: crate::wire::UserKind::User,
                    email: Some(format!("{conn}@x.com")),
                    name: None,
                    github_login: None,
                    github_id: None,
                })
                .presence_rooms(rooms),
        )
    };
    let mut c1 = make("c1");
    let mut c2 = make("c2");

    let c2_states: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
    let c2_states_clone = c2_states.clone();
    let _h2 = c2.presence("room", None, move |members| {
        if let Some(c1) = members.iter().find(|m| m.connection_id == "c1") {
            c2_states_clone.lock().unwrap().push(c1.state.clone());
        }
    });

    let _h1 = c1.presence("room", None, |_| {});

    // Advance the clock to t = 5000 and refresh c1 with a 1000ms ttl.
    *t.lock().unwrap() = 5000;
    c1.update_presence("room", json!({"typing": true}), Some(1000));

    // Before expiry: helper returns false, no new observation.
    assert!(!c2.expire_presence(Some(5999)));
    {
        let got = c2_states.lock().unwrap();
        assert!(got.len() >= 2);
        assert_eq!(got.last().unwrap(), &json!({"typing": true}));
    }

    // After expiry: helper returns true, c2 observes the null.
    assert!(c2.expire_presence(Some(6000)));
    {
        let got = c2_states.lock().unwrap();
        assert_eq!(got.last().unwrap(), &Value::Null);
    }
}
