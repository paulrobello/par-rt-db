use std::collections::BTreeMap;

use crate::query::FilterExpr;
use crate::schema::*;
use crate::value_expr::ValueExpr;

fn simple_table() -> TableDef {
    TableDef {
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields: BTreeMap::from([("name".to_string(), FieldType::String)]),
        indexes: vec![],
        owner_field: None,
        collaborators_field: None,
        ttl: None,
        updated_at_field: None,
        auto_increment_field: None,
        authorize: None,

        soft_delete: false,
    }
}

// ---- computed fields (ENH-028) ----

fn field(name: &str) -> ValueExpr {
    ValueExpr::Field {
        field: name.to_string(),
    }
}

fn literal(value: serde_json::Value) -> ValueExpr {
    ValueExpr::Literal { value }
}

/// Base table with a field of every kind the computed rules distinguish.
/// Computed targets (`fullName`, `slug`, `total`, `label`, `rankText`) are
/// declared but the `computed` map starts empty — each test installs only
/// the entries it exercises.
fn computed_table() -> TableDef {
    TableDef {
        defaults: BTreeMap::new(),
        fields: BTreeMap::from([
            ("first".to_string(), FieldType::String),
            ("last".to_string(), FieldType::String),
            (
                "nickname".to_string(),
                FieldType::Optional {
                    inner: Box::new(FieldType::String),
                },
            ),
            ("score".to_string(), FieldType::Number),
            ("rank".to_string(), FieldType::Int64),
            ("active".to_string(), FieldType::Boolean),
            (
                "status".to_string(),
                FieldType::Union {
                    variants: vec![
                        FieldType::Literal {
                            value: serde_json::json!("admin"),
                        },
                        FieldType::Literal {
                            value: serde_json::json!("user"),
                        },
                    ],
                },
            ),
            ("owner".to_string(), FieldType::String),
            (
                "collaborators".to_string(),
                FieldType::Array {
                    element: Box::new(FieldType::String),
                },
            ),
            ("fullName".to_string(), FieldType::String),
            (
                "slug".to_string(),
                FieldType::Optional {
                    inner: Box::new(FieldType::String),
                },
            ),
            ("total".to_string(), FieldType::Number),
            (
                "label".to_string(),
                FieldType::Union {
                    variants: vec![
                        FieldType::Literal {
                            value: serde_json::json!("staff"),
                        },
                        FieldType::Literal {
                            value: serde_json::json!("guest"),
                        },
                    ],
                },
            ),
            ("rankText".to_string(), FieldType::Int64),
        ]),
        indexes: vec![],
        owner_field: None,
        collaborators_field: None,
        ttl: None,
        updated_at_field: None,
        auto_increment_field: None,
        authorize: None,
        computed: BTreeMap::new(),

        soft_delete: false,
    }
}

fn computed_schema(table: TableDef) -> SchemaDef {
    SchemaDef {
        tables: BTreeMap::from([("users".to_string(), table)]),
    }
}

fn one_computed(name: &str, expr: ValueExpr) -> BTreeMap<String, ValueExpr> {
    BTreeMap::from([(name.to_string(), expr)])
}

// (a) a computed key must be a declared field.
#[test]
fn computed_key_must_be_declared() {
    let mut table = computed_table();
    table.computed = one_computed(
        "bogus",
        ValueExpr::Concat {
            parts: vec![field("first"), field("last")],
        },
    );
    let err = validate_computed(&computed_schema(table)).unwrap_err();
    assert!(
        err.message.contains("not a declared field"),
        "{}",
        err.message
    );
    assert!(err.message.contains("users.bogus"), "{}", err.message);
}

// The rule also rides `SchemaDef::validate` (the push/migrate chokepoint).
#[test]
fn computed_validation_rides_schema_validate() {
    let mut table = computed_table();
    table.computed = one_computed("bogus", field("first"));
    assert!(computed_schema(table).validate().is_err());
}

// (b) every referenced field must be declared.
#[test]
fn computed_reference_must_be_declared() {
    let mut table = computed_table();
    table.computed = one_computed(
        "fullName",
        ValueExpr::Concat {
            parts: vec![field("first"), field("bogus")],
        },
    );
    let err = validate_computed(&computed_schema(table)).unwrap_err();
    assert!(
        err.message.contains("references undeclared field 'bogus'"),
        "{}",
        err.message
    );
}

// (c) computed fields may not reference each other.
#[test]
fn computed_reference_must_not_be_computed() {
    let mut table = computed_table();
    table.computed = BTreeMap::from([
        (
            "fullName".to_string(),
            ValueExpr::Concat {
                parts: vec![field("first"), field("last")],
            },
        ),
        (
            "slug".to_string(),
            ValueExpr::Lower {
                value: Box::new(field("fullName")),
            },
        ),
    ]);
    let err = validate_computed(&computed_schema(table)).unwrap_err();
    assert!(
        err.message.contains("references computed field 'fullName'"),
        "{}",
        err.message
    );
}

// The authorize predicate may not reference a computed field: it runs
// before computed stamping on the insert paths, so it would evaluate
// forgeable client input.
#[test]
fn authorize_must_not_reference_computed_field() {
    let mut table = computed_table();
    table.computed = one_computed(
        "fullName",
        ValueExpr::Concat {
            parts: vec![field("first"), field("last")],
        },
    );
    table.authorize = Some(FilterExpr::Eq {
        field: "fullName".into(),
        value: serde_json::json!("x"),
    });
    let err = validate_computed(&computed_schema(table)).unwrap_err();
    assert!(
        err.message
            .contains("authorize predicates may not reference computed fields"),
        "{}",
        err.message
    );
    assert!(err.message.contains("users.fullName"), "{}", err.message);
    // a predicate over a plain (non-computed) field stays legal
    let mut table = computed_table();
    table.computed = one_computed(
        "fullName",
        ValueExpr::Concat {
            parts: vec![field("first"), field("last")],
        },
    );
    table.authorize = Some(FilterExpr::Eq {
        field: "status".into(),
        value: serde_json::json!("admin"),
    });
    assert!(validate_computed(&computed_schema(table)).is_ok());
}

// (d) principal markers are rejected inside Case.when filters.
#[test]
fn computed_case_when_rejects_principal_markers() {
    let mut table = computed_table();
    table.computed = one_computed(
        "label",
        ValueExpr::Case {
            whens: vec![crate::value_expr::CaseWhen {
                when: FilterExpr::Eq {
                    field: "status".into(),
                    value: serde_json::json!({"$user": true}),
                },
                then: literal(serde_json::json!("staff")),
            }],
            otherwise: Box::new(literal(serde_json::json!("guest"))),
        },
    );
    let err = validate_computed(&computed_schema(table.clone())).unwrap_err();
    assert!(err.message.contains("principal markers"), "{}", err.message);
    // The email marker is rejected the same way.
    table.computed = one_computed(
        "label",
        ValueExpr::Case {
            whens: vec![crate::value_expr::CaseWhen {
                when: FilterExpr::Eq {
                    field: "status".into(),
                    value: serde_json::json!({"$email": true}),
                },
                then: literal(serde_json::json!("staff")),
            }],
            otherwise: Box::new(literal(serde_json::json!("guest"))),
        },
    );
    assert!(validate_computed(&computed_schema(table)).is_err());
}

// (e) static-kind rejects: Concat into number, arithmetic into int64,
// Lower into boolean.
#[test]
fn computed_rejects_concat_into_number_field() {
    let mut table = computed_table();
    table.computed = one_computed(
        "total",
        ValueExpr::Concat {
            parts: vec![field("first")],
        },
    );
    let err = validate_computed(&computed_schema(table)).unwrap_err();
    assert!(err.message.contains("produces a string"), "{}", err.message);
}

#[test]
fn computed_rejects_arithmetic_into_int64_field() {
    let mut table = computed_table();
    table.computed = one_computed(
        "rank",
        ValueExpr::Add {
            left: Box::new(field("score")),
            right: Box::new(literal(serde_json::json!(1))),
        },
    );
    let err = validate_computed(&computed_schema(table)).unwrap_err();
    assert!(err.message.contains("produces a number"), "{}", err.message);
}

#[test]
fn computed_rejects_lower_into_boolean_field() {
    let mut table = computed_table();
    table.computed = one_computed(
        "active",
        ValueExpr::Lower {
            value: Box::new(field("first")),
        },
    );
    let err = validate_computed(&computed_schema(table)).unwrap_err();
    assert!(err.message.contains("produces a string"), "{}", err.message);
}

// (f) the canonical shapes all pass — one per-shape test each below.

#[test]
fn computed_accepts_concat_on_string() {
    let mut table = computed_table();
    table.computed = one_computed(
        "fullName",
        ValueExpr::Concat {
            parts: vec![field("first"), field("last")],
        },
    );
    assert!(validate_computed(&computed_schema(table)).is_ok());
}

#[test]
fn computed_accepts_lower_trim_on_optional_string() {
    let mut table = computed_table();
    table.computed = one_computed(
        "slug",
        ValueExpr::Lower {
            value: Box::new(ValueExpr::Trim {
                value: Box::new(field("nickname")),
            }),
        },
    );
    assert!(validate_computed(&computed_schema(table)).is_ok());
}

#[test]
fn computed_accepts_arithmetic_on_number() {
    let mut table = computed_table();
    table.computed = one_computed(
        "total",
        ValueExpr::Add {
            left: Box::new(field("score")),
            right: Box::new(field("score")),
        },
    );
    assert!(validate_computed(&computed_schema(table)).is_ok());
}

#[test]
fn computed_accepts_case_on_union() {
    let mut table = computed_table();
    table.computed = one_computed(
        "label",
        ValueExpr::Case {
            whens: vec![crate::value_expr::CaseWhen {
                when: FilterExpr::Eq {
                    field: "status".into(),
                    value: serde_json::json!("admin"),
                },
                then: literal(serde_json::json!("staff")),
            }],
            otherwise: Box::new(literal(serde_json::json!("guest"))),
        },
    );
    assert!(validate_computed(&computed_schema(table)).is_ok());
}

#[test]
fn computed_accepts_now_on_number() {
    let mut table = computed_table();
    table.computed = one_computed("total", ValueExpr::Now);
    assert!(validate_computed(&computed_schema(table)).is_ok());
}

// Int64's wire form is a decimal string, so a String-kind expression is
// the accepted shape (the plan's Int64 note) while Number-kind is rejected.
#[test]
fn computed_accepts_cast_to_string_into_int64() {
    let mut table = computed_table();
    table.computed = one_computed(
        "rankText",
        ValueExpr::Cast {
            value: Box::new(field("score")),
            to: crate::value_expr::Cast::ToString,
        },
    );
    assert!(validate_computed(&computed_schema(table)).is_ok());
}

// (g) computed keys must not collide with the stamped declaration fields.
#[test]
fn computed_rejects_owner_field_conflict() {
    let mut table = computed_table();
    table.owner_field = Some("owner".to_string());
    table.computed = one_computed("owner", field("first"));
    let err = validate_computed(&computed_schema(table)).unwrap_err();
    assert!(err.message.contains("ownerField"), "{}", err.message);
}

#[test]
fn computed_rejects_collaborators_field_conflict() {
    let mut table = computed_table();
    table.collaborators_field = Some("collaborators".to_string());
    table.computed = one_computed("collaborators", field("first"));
    let err = validate_computed(&computed_schema(table)).unwrap_err();
    assert!(
        err.message.contains("collaboratorsField"),
        "{}",
        err.message
    );
}

#[test]
fn computed_rejects_auto_increment_field_conflict() {
    let mut table = computed_table();
    table.auto_increment_field = Some("rank".to_string());
    table.computed = one_computed(
        "rank",
        ValueExpr::Cast {
            value: Box::new(field("score")),
            to: crate::value_expr::Cast::ToString,
        },
    );
    let err = validate_computed(&computed_schema(table)).unwrap_err();
    assert!(
        err.message.contains("autoIncrementField"),
        "{}",
        err.message
    );
}

// Additive wire: `computed` is omitted when empty and absent JSON still
// deserializes.
#[test]
fn computed_wire_is_additive() {
    let table = simple_table();
    let json = serde_json::to_value(&table).unwrap();
    assert!(json.get("computed").is_none());
    let back: TableDef = serde_json::from_value(json).unwrap();
    assert!(back.computed.is_empty());
    // pre-ENH-028 schema JSON (no computed key) deserializes unchanged
    let legacy: TableDef = serde_json::from_value(serde_json::json!({
        "fields": {"name": {"type": "string"}}
    }))
    .unwrap();
    assert!(legacy.computed.is_empty());
}

// A computed entry round-trips through the wire with its expr intact.
#[test]
fn computed_wire_round_trips_expression() {
    let mut table = simple_table();
    let expr = ValueExpr::Concat {
        parts: vec![field("name"), literal(serde_json::json!("!"))],
    };
    table.computed = one_computed("name", expr.clone());
    let json = serde_json::to_value(&table).unwrap();
    assert_eq!(json["computed"]["name"]["op"], "concat");
    let back: TableDef = serde_json::from_value(json).unwrap();
    assert_eq!(back.computed.get("name"), Some(&expr));
}
