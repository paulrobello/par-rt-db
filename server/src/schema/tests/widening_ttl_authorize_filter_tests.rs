use std::collections::BTreeMap;

use crate::query::FilterExpr;
use crate::schema::*;

fn union_of(vals: &[&str]) -> FieldType {
    FieldType::Union {
        variants: vals
            .iter()
            .map(|v| FieldType::Literal {
                value: serde_json::Value::String((*v).to_string()),
            })
            .collect(),
    }
}

fn lit(s: &str) -> FieldType {
    FieldType::Literal {
        value: serde_json::Value::String(s.to_string()),
    }
}

#[test]
fn is_widening_of_allows_adding_a_union_variant() {
    let old = union_of(&["low", "medium", "high"]);
    let new = union_of(&["low", "medium", "high", "critical"]);
    assert!(is_widening_of(&old, &new));
}

#[test]
fn is_widening_of_rejects_narrowing_a_union() {
    let old = union_of(&["low", "medium", "high", "critical"]);
    let new = union_of(&["low", "medium", "high"]);
    assert!(!is_widening_of(&old, &new));
}

#[test]
fn is_widening_of_allows_literal_to_union() {
    let old = FieldType::Literal {
        value: serde_json::Value::String("a".to_string()),
    };
    let new = union_of(&["a", "b"]);
    assert!(is_widening_of(&old, &new));
}

#[test]
fn is_widening_of_rejects_scalar_swap_and_nonliteral_types() {
    assert!(!is_widening_of(&FieldType::Number, &FieldType::String));
    assert!(!is_widening_of(&FieldType::String, &union_of(&["a"])));
    // A union mixing a Literal with a non-literal variant is an open type.
    let mixed = FieldType::Union {
        variants: vec![
            FieldType::Literal {
                value: serde_json::Value::String("a".to_string()),
            },
            FieldType::String,
        ],
    };
    assert!(!is_widening_of(&mixed, &union_of(&["a", "b"])));
    assert!(!is_widening_of(&union_of(&["a", "b"]), &mixed));
}

#[test]
fn is_widening_of_rejects_replacing_a_single_literal() {
    // Lit("a") -> Lit("b"): "a" is not in {"b"} -> narrowing, rejected.
    assert!(!is_widening_of(&lit("a"), &lit("b")));
}

#[test]
fn is_widening_of_rejects_collapsing_union_to_literal() {
    // Union{a,b} -> Lit(a): "b" is dropped -> rejected.
    assert!(!is_widening_of(&union_of(&["a", "b"]), &lit("a")));
}

#[test]
fn is_widening_of_rejects_empty_union() {
    // An empty union accepts no values; refuse it outright rather than
    // returning a vacuous superset `true`.
    let empty = FieldType::Union { variants: vec![] };
    assert!(!is_widening_of(&empty, &union_of(&["a", "b"])));
    assert!(!is_widening_of(&union_of(&["a", "b"]), &empty));
}

fn object_of(pairs: &[(&str, FieldType)]) -> FieldType {
    FieldType::Object {
        fields: pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect(),
    }
}

fn optional_of(inner: FieldType) -> FieldType {
    FieldType::Optional {
        inner: Box::new(inner),
    }
}

fn array_of(element: FieldType) -> FieldType {
    FieldType::Array {
        element: Box::new(element),
    }
}

/// The `array<object>` element shape this widening rule exists for: a versions
/// array whose elements carry a required `tag` and (after the widening) an
/// optional `size` — the par-artifacts consumer shape.
fn versions_element(with_size: bool) -> FieldType {
    let mut pairs = vec![("tag", FieldType::String)];
    if with_size {
        pairs.push(("size", optional_of(FieldType::Number)));
    }
    object_of(&pairs)
}

#[test]
fn is_widening_of_accepts_adding_optional_field_inside_array_object_elements() {
    let old = array_of(versions_element(false));
    let new = array_of(versions_element(true));
    assert!(is_widening_of(&old, &new));
    // Identical shapes stay trivially accepted (unchanged, not widening).
    assert!(!is_type_change_rejected(&old, &new));
}

#[test]
fn is_widening_of_accepts_adding_optional_field_to_directly_nested_object() {
    // The helper naturally covers a directly-nested `object` field — same
    // safety argument, no extra code.
    let old = object_of(&[("tag", FieldType::String)]);
    let new = object_of(&[
        ("tag", FieldType::String),
        ("size", optional_of(FieldType::Number)),
    ]);
    assert!(is_widening_of(&old, &new));
    let wrapped_old = object_of(&[("versions", old.clone())]);
    let wrapped_new = object_of(&[("versions", new.clone())]);
    assert!(is_widening_of(&wrapped_old, &wrapped_new));
}

#[test]
fn is_widening_of_accepts_empty_old_object_shape() {
    // object{} -> object{size: optional<number>}: every stored {} validates.
    let old = object_of(&[]);
    let new = object_of(&[("size", optional_of(FieldType::Number))]);
    assert!(is_widening_of(&old, &new));
}

#[test]
fn is_widening_of_rejects_removing_an_element_object_field() {
    let old = array_of(versions_element(true));
    let new = array_of(versions_element(false));
    assert!(!is_widening_of(&old, &new));
    assert!(is_type_change_rejected(&old, &new));
}

#[test]
fn is_widening_of_rejects_retyping_an_element_object_field() {
    let old = array_of(object_of(&[("size", optional_of(FieldType::Number))]));
    let new = array_of(object_of(&[("size", optional_of(FieldType::String))]));
    assert!(!is_widening_of(&old, &new));
}

#[test]
fn is_widening_of_rejects_adding_required_field_inside_array_object_elements() {
    let old = array_of(versions_element(false));
    let new = array_of(object_of(&[
        ("tag", FieldType::String),
        ("size", FieldType::Number),
    ]));
    assert!(!is_widening_of(&old, &new));
}

#[test]
fn is_widening_of_rejects_narrowing_union_inside_element_object() {
    let old = array_of(object_of(&[("state", union_of(&["a", "b", "c"]))]));
    let new = array_of(object_of(&[("state", union_of(&["a", "b"]))]));
    assert!(!is_widening_of(&old, &new));
}

#[test]
fn is_widening_of_rejects_changing_array_element_object_to_scalar() {
    let old = array_of(FieldType::String);
    let new = array_of(versions_element(true));
    assert!(!is_widening_of(&old, &new));
}

#[test]
fn is_widening_of_rejects_optional_to_required_element_field() {
    // Existing field rewritten from optional to required is a retyping of a
    // stored shape: old rows lacking it would now fail validation.
    let old = array_of(object_of(&[
        ("tag", FieldType::String),
        ("size", optional_of(FieldType::Number)),
    ]));
    let new = array_of(object_of(&[
        ("tag", FieldType::String),
        ("size", FieldType::Number),
    ]));
    assert!(!is_widening_of(&old, &new));
}

#[test]
fn is_widening_of_rejects_scalar_array_element_to_object_swap() {
    // A plain scalar -> object swap is a retyping, not a widening.
    let old = array_of(FieldType::String);
    let new = array_of(versions_element(true));
    assert!(!is_widening_of(&old, &new));
}

fn table_with_ttl(ttl: Option<TtlDef>) -> TableDef {
    let mut fields = BTreeMap::new();
    fields.insert("expiresAt".to_string(), FieldType::Number);
    TableDef {
        constraints: std::collections::BTreeMap::new(),
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields,
        indexes: vec![IndexDef {
            name: "by_expiresAt".to_string(),
            fields: vec!["expiresAt".to_string()],
            unique: false,
            search: false,
            vector: None,
            r#where: None,
            language: None,
            trgm: false,
        }],
        owner_field: None,
        collaborators_field: None,
        ttl,
        updated_at_field: None,
        auto_increment_field: None,
        authorize: None,

        soft_delete: false,
    }
}

#[test]
fn ttl_accepts_numeric_field_with_single_btree_index() {
    let mut schema = SchemaDef::default();
    schema.tables.insert(
        "t".to_string(),
        table_with_ttl(Some(TtlDef {
            field: "expiresAt".to_string(),
            default_duration_ms: Some(86_400_000),
        })),
    );
    assert!(schema.validate().is_ok());
}

#[test]
fn ttl_rejects_missing_index() {
    let mut table = table_with_ttl(Some(TtlDef {
        field: "expiresAt".to_string(),
        default_duration_ms: None,
    }));
    table.indexes.clear();
    let mut schema = SchemaDef::default();
    schema.tables.insert("t".to_string(), table);
    let err = schema.validate().unwrap_err();
    assert!(
        err.message
            .contains("requires a single-field, non-unique, non-partial btree index"),
        "{}",
        err.message
    );
}

#[test]
fn ttl_rejects_non_numeric_field() {
    let mut table = table_with_ttl(Some(TtlDef {
        field: "name".to_string(),
        default_duration_ms: None,
    }));
    table.fields.insert("name".to_string(), FieldType::String);
    let mut schema = SchemaDef::default();
    schema.tables.insert("t".to_string(), table);
    assert!(schema.validate().is_err());
}

#[test]
fn ttl_rejects_unique_or_partial_or_multifield_index() {
    for bad in [
        IndexDef {
            name: "x".to_string(),
            fields: vec!["expiresAt".to_string()],
            unique: true,
            search: false,
            vector: None,
            r#where: None,
            language: None,
            trgm: false,
        },
        IndexDef {
            name: "x".to_string(),
            fields: vec!["expiresAt".to_string()],
            unique: false,
            search: false,
            vector: None,
            r#where: Some(FilterExpr::Gt {
                field: "expiresAt".to_string(),
                value: serde_json::json!(0),
            }),
            language: None,
            trgm: false,
        },
        IndexDef {
            name: "x".to_string(),
            fields: vec!["expiresAt".to_string(), "expiresAt".to_string()],
            unique: false,
            search: false,
            vector: None,
            r#where: None,
            language: None,
            trgm: false,
        },
    ] {
        let mut table = table_with_ttl(Some(TtlDef {
            field: "expiresAt".to_string(),
            default_duration_ms: None,
        }));
        table.indexes = vec![bad];
        let mut schema = SchemaDef::default();
        schema.tables.insert("t".to_string(), table);
        assert!(
            schema.validate().is_err(),
            "should reject this index variant"
        );
    }
}

#[test]
fn ttl_rejects_non_positive_default_duration() {
    let mut schema = SchemaDef::default();
    schema.tables.insert(
        "t".to_string(),
        table_with_ttl(Some(TtlDef {
            field: "expiresAt".to_string(),
            default_duration_ms: Some(0),
        })),
    );
    assert!(schema.validate().is_err());
}

// ---- TableDef.authorize: per-row predicate declaration (Model C) ----

/// Helper: a TableDef with the named string fields plus `editors`
/// (array-of-strings) and `count` (number), so every authorize-validation
/// branch has a field of the right shape to target.
fn table_with_string_fields(field_names: &[&str]) -> TableDef {
    let mut fields = BTreeMap::new();
    for f in field_names {
        fields.insert((*f).to_string(), FieldType::String);
    }
    fields.insert(
        "editors".to_string(),
        FieldType::Array {
            element: Box::new(FieldType::String),
        },
    );
    fields.insert("count".to_string(), FieldType::Number);
    TableDef {
        constraints: std::collections::BTreeMap::new(),
        defaults: std::collections::BTreeMap::new(),
        computed: std::collections::BTreeMap::new(),
        fields,
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

#[test]
fn authorize_validates_fields_and_markers() {
    let mut t = table_with_string_fields(&["owner", "visibility"]);
    // valid: public OR owned (principal marker allowed in authorize)
    t.authorize = Some(FilterExpr::Or {
        exprs: vec![
            FilterExpr::Eq {
                field: "owner".into(),
                value: serde_json::json!({"$user": true}),
            },
            FilterExpr::Eq {
                field: "visibility".into(),
                value: serde_json::json!("public"),
            },
        ],
    });
    assert!(t.validate_structure("posts").is_ok());

    // invalid: unknown field
    let mut bad = t.clone();
    bad.authorize = Some(FilterExpr::Eq {
        field: "nope".into(),
        value: serde_json::json!(1),
    });
    assert!(bad.validate_structure("posts").is_err());

    // invalid: Contains on a non-array field
    let mut bad2 = t.clone();
    bad2.authorize = Some(FilterExpr::Contains {
        field: "visibility".into(),
        value: serde_json::json!("x"),
    });
    assert!(bad2.validate_structure("posts").is_err());

    // valid: Contains on an array-of-strings field
    let mut good = t.clone();
    good.authorize = Some(FilterExpr::Contains {
        field: "editors".into(),
        value: serde_json::json!({"$user": true}),
    });
    assert!(good.validate_structure("posts").is_ok());

    // valid: And/Not recurse; principal markers resolve in any leaf
    let mut nested = t.clone();
    nested.authorize = Some(FilterExpr::And {
        exprs: vec![
            FilterExpr::Not {
                expr: Box::new(FilterExpr::Exists {
                    field: "count".into(),
                }),
            },
            FilterExpr::Eq {
                field: "owner".into(),
                value: serde_json::json!({"$email": true}),
            },
        ],
    });
    assert!(nested.validate_structure("posts").is_ok());

    // invalid: comparison against a non-scalar (array) field — not type-compatible
    let mut bad3 = t.clone();
    bad3.authorize = Some(FilterExpr::Gt {
        field: "editors".into(),
        value: serde_json::json!(1),
    });
    assert!(bad3.validate_structure("posts").is_err());
}

#[test]
fn authorize_round_trips_and_absent_is_unchanged() {
    // present: wire key `authorize` survives a round trip. (Server TableDef
    // always serializes `indexes`, so the expected JSON includes it.)
    let json = serde_json::json!({
        "fields": {
            "owner": {"type":"string"},
            "visibility": {"type":"string"}
        },
        "indexes": [],
        "authorize": {"op":"or","exprs":[
            {"op":"eq","field":"owner","value":{"$user":true}},
            {"op":"eq","field":"visibility","value":"public"}
        ]}
    });
    let td: TableDef = serde_json::from_value(json.clone()).unwrap();
    assert!(td.authorize.is_some());
    assert_eq!(serde_json::to_value(&td).unwrap(), json);

    // validates as part of a schema
    let mut tables = BTreeMap::new();
    tables.insert("posts".to_string(), td);
    SchemaDef { tables }.validate().unwrap();

    // absent authorize is omitted from the wire and deserializes as None
    let none_json = r#"{"fields":{"title":{"type":"string"}}}"#;
    let td2: TableDef = serde_json::from_str(none_json).unwrap();
    assert!(td2.authorize.is_none());
    assert!(
        !serde_json::to_string(&td2).unwrap().contains("authorize"),
        "authorize must be omitted on the wire when unset"
    );
}

#[test]
fn validate_filter_expr_fields_rejects_principal_markers_when_disallowed() {
    // The walker is reused by Task 6 to reject principal markers in client
    // .filter() queries. Here we lock the flag's behavior directly.
    let table = table_with_string_fields(&["owner", "visibility"]);
    let with_marker = FilterExpr::Eq {
        field: "owner".into(),
        value: serde_json::json!({"$user": true}),
    };
    assert!(validate_filter_expr_fields(&with_marker, &table, true, false).is_ok());
    assert!(validate_filter_expr_fields(&with_marker, &table, false, false).is_err());
    let email_marker = FilterExpr::Eq {
        field: "owner".into(),
        value: serde_json::json!({"$email": true}),
    };
    assert!(validate_filter_expr_fields(&email_marker, &table, true, false).is_ok());
    assert!(validate_filter_expr_fields(&email_marker, &table, false, false).is_err());
    // a marker nested under And is still rejected
    let nested = FilterExpr::And {
        exprs: vec![
            FilterExpr::Eq {
                field: "visibility".into(),
                value: serde_json::json!("public"),
            },
            FilterExpr::Contains {
                field: "editors".into(),
                value: serde_json::json!({"$user": true}),
            },
        ],
    };
    assert!(validate_filter_expr_fields(&nested, &table, false, false).is_err());
    // a non-marker value passes regardless of the flag
    let plain = FilterExpr::Eq {
        field: "visibility".into(),
        value: serde_json::json!("public"),
    };
    assert!(validate_filter_expr_fields(&plain, &table, true, false).is_ok());
    assert!(validate_filter_expr_fields(&plain, &table, false, false).is_ok());
}

#[test]
fn sec007_filter_depth_and_in_length_are_capped() {
    let table = table_with_string_fields(&["visibility"]);
    let leaf = || FilterExpr::Eq {
        field: "visibility".into(),
        value: serde_json::json!("public"),
    };
    // Nest `not` to exactly the cap, then one level past it.
    let nest = |levels: usize| {
        let mut expr = leaf();
        for _ in 1..levels {
            expr = FilterExpr::Not {
                expr: Box::new(expr),
            };
        }
        expr
    };
    assert!(
        validate_filter_expr_fields(&nest(MAX_FILTER_DEPTH), &table, false, false).is_ok(),
        "a filter exactly at the depth cap must be accepted"
    );
    let err = validate_filter_expr_fields(&nest(MAX_FILTER_DEPTH + 1), &table, false, false)
        .expect_err("one level past the cap must be rejected");
    assert!(err.to_string().contains("nesting"), "got {err}");

    // `in` list length.
    let values = |n: usize| -> Vec<serde_json::Value> {
        (0..n).map(|i| serde_json::json!(format!("v{i}"))).collect()
    };
    let at_cap = FilterExpr::In {
        field: "visibility".into(),
        values: values(MAX_IN_VALUES),
    };
    assert!(validate_filter_expr_fields(&at_cap, &table, false, false).is_ok());
    let over = FilterExpr::In {
        field: "visibility".into(),
        values: values(MAX_IN_VALUES + 1),
    };
    let err = validate_filter_expr_fields(&over, &table, false, false)
        .expect_err("an over-long in list must be rejected");
    assert!(err.to_string().contains("at most"), "got {err}");
}
