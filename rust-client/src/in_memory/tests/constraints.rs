//! Declarative per-field write constraints (the server's
//! `TableDef::constraints`): `.constraints()` builder wire shape plus
//! in-memory push-time and write-path enforcement. Port of
//! `ts-client/tests/field-constraints.test.ts`. Cross-engine agreement is
//! pinned by the `constraint-*` / `error-constraint-*` semantics-corpus
//! cases; these unit tests pin the DSL builder surface.
use super::*;
use crate::schema::FieldConstraints;

/// An all-empty constraint set to spread from (`FieldConstraints` derives no
/// `Default`, and core is out of this mirror's scope).
fn no_constraints() -> FieldConstraints {
    FieldConstraints {
        min: None,
        max: None,
        min_length: None,
        max_length: None,
        pattern: None,
    }
}

/// The ts fixture's constraint set: bounds on `score` (number) and `counter`
/// (int64), length + pattern on `name` (string).
fn tasks_schema() -> SchemaDef {
    Schema::builder()
        .table(
            "tasks",
            Table::new()
                .field("name", FieldType::String)
                .field("score", FieldType::Number)
                .field("counter", FieldType::Int64)
                .field("note", FieldType::optional(FieldType::String))
                .index("by_name", &["name"])
                .constraints(&[
                    (
                        "score",
                        FieldConstraints {
                            min: Some(serde_json::Number::from(1)),
                            max: Some(serde_json::Number::from(10)),
                            ..no_constraints()
                        },
                    ),
                    (
                        "counter",
                        FieldConstraints {
                            min: Some(serde_json::Number::from(1)),
                            max: Some(serde_json::Number::from(100)),
                            ..no_constraints()
                        },
                    ),
                    (
                        "name",
                        FieldConstraints {
                            min_length: Some(2),
                            max_length: Some(5),
                            pattern: Some("^[a-z]+$".into()),
                            ..no_constraints()
                        },
                    ),
                ]),
        )
        .build()
}

fn tasks_doc(name: &str, score: f64, counter: &str) -> Value {
    json!({"name": name, "score": score, "counter": counter})
}

async fn insert_ok(c: &mut InMemoryRtDbClient, doc: Value) -> String {
    let inserted = c
        .mutate(&Mutation::new().insert("tasks", doc).build(), None)
        .await
        .expect("insert should pass constraints");
    match &inserted[0] {
        StepResult::Insert { id } => id.clone(),
        other => panic!("expected insert, got {other:?}"),
    }
}

// ---- DSL builder wire shape ---------------------------------------

#[test]
fn constraints_round_trip_on_the_wire_and_are_omitted_when_absent() {
    let json = tasks_schema();
    let table = json.tables.get("tasks").expect("tasks table present");
    let got = table.constraints.get("name").expect("name constraints");
    assert_eq!(got.min_length, Some(2));
    assert_eq!(got.max_length, Some(5));
    assert_eq!(got.pattern.as_deref(), Some("^[a-z]+$"));
    let score = table.constraints.get("score").expect("score constraints");
    assert_eq!(score.min.as_ref().map(|n| n.as_f64()), Some(Some(1.0)));
    assert_eq!(score.max.as_ref().map(|n| n.as_f64()), Some(Some(10.0)));

    // Wire form carries camelCase keys, omitted when the table declares none.
    let plain = Schema::builder()
        .table("solo", Table::new().field("name", FieldType::String))
        .build();
    let solo = plain.tables.get("solo").expect("solo table present");
    assert!(solo.constraints.is_empty());
    let wire = serde_json::to_value(&plain).expect("serialize");
    assert!(
        wire.pointer("/tables/solo/constraints").is_none(),
        "empty constraints must be omitted on the wire"
    );
    let wire = serde_json::to_value(tasks_schema()).expect("serialize");
    assert_eq!(
        wire.pointer("/tables/tasks/constraints/name/minLength"),
        Some(&json!(2))
    );
}

// ---- push-time validation -----------------------------------------

#[test]
fn push_rejects_a_misapplied_constraint_naming_the_field() {
    let schema = Schema::builder()
        .table(
            "tasks",
            Table::new()
                .field("name", FieldType::String)
                .index("by_name", &["name"])
                .constraints(&[(
                    "name",
                    FieldConstraints {
                        min: Some(serde_json::Number::from(1)),
                        ..no_constraints()
                    },
                )]),
        )
        .build();
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let err = c.push_schema(&schema).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(err.message.contains("'name'"), "got: {err}");
    assert!(err.message.contains("number/int64"), "got: {err}");
    // The rejected push left no schema installed.
    assert!(c.to_schema_json().is_none());
}

#[test]
fn push_rejects_an_invalid_regex_naming_the_field() {
    let schema = Schema::builder()
        .table(
            "tasks",
            Table::new()
                .field("name", FieldType::String)
                .index("by_name", &["name"])
                .constraints(&[(
                    "name",
                    FieldConstraints {
                        pattern: Some("([a-z".into()),
                        ..no_constraints()
                    },
                )]),
        )
        .build();
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let err = c.push_schema(&schema).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(err.message.contains("invalid pattern"), "got: {err}");
}

// ---- write-path enforcement ---------------------------------------

#[tokio::test]
async fn accepts_inclusive_boundary_values() {
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    c.push_schema(&tasks_schema()).unwrap();
    insert_ok(&mut c, tasks_doc("aa", 1.0, "1")).await;
    insert_ok(&mut c, tasks_doc("abcde", 10.0, "100")).await;
}

#[tokio::test]
async fn fails_an_insert_below_min_with_schema_violation_and_rolls_back() {
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    c.push_schema(&tasks_schema()).unwrap();
    let err = c
        .mutate(
            &Mutation::new()
                .insert("tasks", tasks_doc("good", 5.0, "1"))
                .insert("tasks", tasks_doc("bad", 0.5, "1"))
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert_eq!(
        c.collect_all("tasks").len(),
        0,
        "the txn must roll back whole"
    );
}

#[tokio::test]
async fn enforces_length_and_pattern_on_strings() {
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    c.push_schema(&tasks_schema()).unwrap();
    let err = c
        .mutate(
            &Mutation::new()
                .insert("tasks", tasks_doc("a", 5.0, "1"))
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert!(err.message.contains("'minLength' constraint"), "got: {err}");
    let err = c
        .mutate(
            &Mutation::new()
                .insert("tasks", tasks_doc("abcdef", 5.0, "1"))
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert!(err.message.contains("'maxLength' constraint"), "got: {err}");
    let err = c
        .mutate(
            &Mutation::new()
                .insert("tasks", tasks_doc("ABC", 5.0, "1"))
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert!(err.message.contains("'pattern' constraint"), "got: {err}");
}

#[tokio::test]
async fn enforces_constraints_through_patch_replace_upsert_and_patch_by_query() {
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    c.push_schema(&tasks_schema()).unwrap();
    let id = insert_ok(&mut c, tasks_doc("ok", 5.0, "1")).await;

    let err = c
        .mutate(
            &Mutation::new()
                .patch("tasks", &id, json!({"score": 50.0}))
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(err.message.contains("'max' constraint"), "got: {err}");

    let err = c
        .mutate(
            &Mutation::new()
                .replace("tasks", &id, tasks_doc("ab", 99.0, "2"))
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);

    let err = c
        .mutate(
            &Mutation::new()
                .upsert(
                    "tasks",
                    "by_name",
                    &[json!("ok")],
                    tasks_doc("ok", 5.0, "1"),
                    json!({"counter": "101"}),
                )
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert!(err.message.contains("'max' constraint"), "got: {err}");

    let err = c
        .mutate(
            &Mutation::new()
                .patch_by_query(
                    "tasks",
                    FilterExpr::Eq {
                        field: "name".into(),
                        value: json!("ok"),
                    },
                    json!({"score": 99.0}),
                    None,
                )
                .build(),
            None,
        )
        .await
        .unwrap_err();
    assert!(err.message.contains("'max' constraint"), "got: {err}");

    // Nothing survived: the stored doc is untouched at its original version.
    let rows = c.collect_all("tasks");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["score"], 5.0);
    assert_eq!(rows[0]["counter"], "1");
    assert_eq!(rows[0]["_version"], 1);
}

#[tokio::test]
async fn never_violates_on_null_or_absent_values() {
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    c.push_schema(&tasks_schema()).unwrap();
    insert_ok(
        &mut c,
        json!({"name": "ok", "score": 5.0, "counter": "1", "note": null}),
    )
    .await;
}

// ---- int64 bound edge ---------------------------------------------

#[test]
fn push_rejects_a_non_integer_bound_on_an_int64_field() {
    let schema = Schema::builder()
        .table(
            "tasks",
            Table::new()
                .field("counter", FieldType::Int64)
                .index("by_counter", &["counter"])
                .constraints(&[(
                    "counter",
                    FieldConstraints {
                        min: Some(serde_json::Number::from_f64(1.5).expect("1.5")),
                        ..no_constraints()
                    },
                )]),
        )
        .build();
    let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let err = c.push_schema(&schema).unwrap_err();
    assert_eq!(err.code, ErrorCode::SchemaViolation);
    assert!(
        err.message.contains("must be an integer on an int64 field"),
        "got: {err}"
    );
}
