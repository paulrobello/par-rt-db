use super::*;

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
