use super::backfill::coerce_value;
use super::plan::cast_valid_for;
use super::*;
use crate::schema::{FieldType, SchemaDef, TableDef};

#[test]
fn directive_round_trip() {
    let req = MigrateRequest {
        directives: vec![
            Directive::RenameField {
                table: "users".into(),
                from: "name".into(),
                to: "fullName".into(),
            },
            Directive::ChangeType {
                table: "users".into(),
                field: "age".into(),
                to: FieldType::String,
                cast: Cast::ToString,
                default: None,
            },
            Directive::EvalExpr {
                table: "users".into(),
                set: "upper".into(),
                expr: ExprSource::Legacy("upper(doc->>'fullName')".into()),
                where_clause: Some(CondSource::Legacy("doc ? 'fullName'".into())),
            },
        ],
        dry_run: true,
    };
    let json = serde_json::to_value(&req).unwrap();
    // tag is "op", camelCase keys, `where` alias.
    assert_eq!(json["directives"][0]["op"], "renameField");
    assert_eq!(json["directives"][1]["op"], "changeType");
    assert_eq!(json["directives"][1]["cast"], "toString");
    assert_eq!(json["directives"][2]["where"], "doc ? 'fullName'");
    assert_eq!(json["dryRun"], true);
    let back: MigrateRequest = serde_json::from_value(json).unwrap();
    assert!(back.dry_run);
    assert_eq!(back.directives.len(), 3);
}

#[test]
fn directive_report_default_supports_spread() {
    // Later tasks construct DirectiveReport with `..Default::default()`.
    let report = DirectiveReport {
        op: "renameField".into(),
        affected_rows: 3,
        ..Default::default()
    };
    assert_eq!(report.op, "renameField");
    assert_eq!(report.affected_rows, 3);
    assert!(report.cast_failures.is_empty());
    assert!(report.sample_changes.is_empty());
}

// ENH-028: directive folding must not be able to invalidate a computed
// entry — a changeType that retypes a computed field so the expression's
// static kind no longer fits fails the plan before any DB work.
#[test]
fn plan_migration_revalidates_computed_after_change_type() {
    let mut old = one_table_schema();
    let users = old.tables.get_mut("users").expect("users table");
    users.fields.insert("count".into(), FieldType::Number);
    users.computed.insert(
        "age".into(),
        ValueExpr::Add {
            left: Box::new(ValueExpr::Field {
                field: "count".into(),
            }),
            right: Box::new(ValueExpr::Literal {
                value: serde_json::json!(1),
            }),
        },
    );
    let d = vec![Directive::ChangeType {
        table: "users".into(),
        field: "age".into(),
        to: FieldType::String,
        cast: Cast::ToString,
        default: None,
    }];
    let err = plan_migration(&old, &d).unwrap_err();
    assert!(err.message.contains("produces a number"), "{}", err.message);
    // Without the computed entry the same fold still plans fine.
    let clean = one_table_schema();
    assert!(plan_migration(&clean, &d).is_ok());
}

use std::collections::BTreeMap;

fn one_table_schema() -> SchemaDef {
    let mut fields = BTreeMap::new();
    fields.insert("name".into(), FieldType::String);
    fields.insert("age".into(), FieldType::Number);
    let mut tables = BTreeMap::new();
    tables.insert(
        "users".into(),
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

            ..Default::default()
        },
    );
    SchemaDef { tables }
}

#[test]
fn plan_rename_field_derives_schema() {
    let old = one_table_schema();
    let d = vec![Directive::RenameField {
        table: "users".into(),
        from: "name".into(),
        to: "fullName".into(),
    }];
    let got = plan_migration(&old, &d).unwrap();
    assert!(got.tables["users"].fields.contains_key("fullName"));
    assert!(!got.tables["users"].fields.contains_key("name"));
}

#[test]
fn plan_rename_field_follows_auto_increment_declaration() {
    let mut old = one_table_schema();
    old.tables
        .get_mut("users")
        .expect("users")
        .fields
        .insert("ticketNum".into(), FieldType::Int64);
    old.tables
        .get_mut("users")
        .expect("users")
        .auto_increment_field = Some("ticketNum".into());
    let d = vec![Directive::RenameField {
        table: "users".into(),
        from: "ticketNum".into(),
        to: "number".into(),
    }];
    let got = plan_migration(&old, &d).unwrap();
    assert_eq!(
        got.tables["users"].auto_increment_field,
        Some("number".to_string()),
        "the declaration follows the renamed field"
    );
}

#[test]
fn plan_rejects_missing_source_field() {
    let old = one_table_schema();
    let d = vec![Directive::RenameField {
        table: "users".into(),
        from: "nope".into(),
        to: "x".into(),
    }];
    assert!(plan_migration(&old, &d).is_err());
}

#[test]
fn plan_rejects_taken_rename_target() {
    let old = one_table_schema();
    // renaming `name` -> `age` collides with the existing `age`
    let d = vec![Directive::RenameField {
        table: "users".into(),
        from: "name".into(),
        to: "age".into(),
    }];
    assert!(plan_migration(&old, &d).is_err());
}

#[test]
fn cast_matrix_locked() {
    // Lock `cast_valid_for`'s matrix: one (ideally several) accepted and one
    // rejected source type per cast. Mirrors the spec's coercion table.
    for (cast, old) in [
        (Cast::ToString, FieldType::String),
        (Cast::ToString, FieldType::Number),
        (Cast::ToString, FieldType::Boolean),
        (Cast::ToString, FieldType::Int64),
        (Cast::ToNumber, FieldType::String),
        (Cast::ToNumber, FieldType::Boolean),
        (Cast::ToNumber, FieldType::Int64),
        (Cast::ToInt64, FieldType::String),
        (Cast::ToInt64, FieldType::Number),
        (Cast::ToBoolean, FieldType::String),
        (Cast::ToBoolean, FieldType::Number),
    ] {
        assert!(cast_valid_for(cast, &old), "{cast:?} should accept {old:?}");
    }
    // Rejected source types: Object has no sound coercion for any cast, and
    // a couple of near-miss scalar pairs that fall outside the matrix
    // (ToInt64 rejects Boolean; ToBoolean rejects Int64).
    let object = FieldType::Object {
        fields: BTreeMap::new(),
    };
    for (cast, old) in [
        (Cast::ToString, object.clone()),
        (Cast::ToNumber, object.clone()),
        (Cast::ToInt64, object.clone()),
        (Cast::ToBoolean, object.clone()),
        (Cast::ToInt64, FieldType::Boolean),
        (Cast::ToBoolean, FieldType::Int64),
    ] {
        assert!(
            !cast_valid_for(cast, &old),
            "{cast:?} should reject {old:?}"
        );
    }
}

#[test]
fn plan_rejects_cast_with_no_accepted_source() {
    // ToString on an Object field has no valid coercion → rejected end-to-end.
    let mut fields = BTreeMap::new();
    fields.insert(
        "meta".into(),
        FieldType::Object {
            fields: BTreeMap::new(),
        },
    );
    let mut tables = BTreeMap::new();
    tables.insert(
        "things".into(),
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

            ..Default::default()
        },
    );
    let schema = SchemaDef { tables };
    let d = vec![Directive::ChangeType {
        table: "things".into(),
        field: "meta".into(),
        to: FieldType::String,
        cast: Cast::ToString,
        default: None,
    }];
    assert!(plan_migration(&schema, &d).is_err());
}

// SEC-107: `plan_migration` intentionally does NOT inspect `expr`/`where` SQL
// text. The prior `has_sql_violation` denylist was unsound (bypassed by
// whitespace variants around ` FROM ` and by `SELECT` without `FROM`) and
// was removed. Containment is now structural: `admin_migrate` admits an
// evalExpr directive only under the root admin_key, never under a delegated
// (OAuth-allowlist) admin — covered by the HTTP-layer tests in
// migration_test.rs (`sec107_*`). This test pins the planner half: a
// FROM-clause `expr` is ACCEPTED here (the gate is the control, not the
// planner), so the next reader does not reintroduce a SQL denylist.
#[test]
fn plan_does_not_inspect_evalexpr_sql_text() {
    let old = one_table_schema();
    let d = vec![Directive::EvalExpr {
        table: "users".into(),
        set: "name".into(),
        expr: ExprSource::Legacy("x FROM other".into()),
        where_clause: None,
    }];
    assert!(
        plan_migration(&old, &d).is_ok(),
        "planner no longer SQL-gates evalExpr; the admin gate does"
    );
}

#[test]
fn plan_drop_table_removes_it() {
    let old = one_table_schema();
    let d = vec![Directive::DropTable {
        name: "users".into(),
    }];
    let got = plan_migration(&old, &d).unwrap();
    assert!(got.tables.is_empty());
}

/// Table with an index, ownerField, and collaboratorsField so the rename/drop
/// fixup arms have something to rewrite. `by_owner` indexes `ownerId` so a
/// rename of `ownerId` must follow it in `index.fields`.
fn schema_with_auth_and_index() -> SchemaDef {
    let mut fields = BTreeMap::new();
    fields.insert("email".into(), FieldType::String);
    fields.insert("ownerId".into(), FieldType::String);
    fields.insert(
        "collabs".into(),
        FieldType::Array {
            element: Box::new(FieldType::String),
        },
    );
    let mut tables = BTreeMap::new();
    tables.insert(
        "users".into(),
        TableDef {
            constraints: std::collections::BTreeMap::new(),
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields,
            indexes: vec![crate::schema::IndexDef {
                name: "by_owner".into(),
                fields: vec!["ownerId".into()],
                search: false,
                vector: None,
                unique: false,
                r#where: None,
                language: None,
                trgm: false,
            }],
            owner_field: Some("ownerId".into()),
            collaborators_field: Some("collabs".into()),
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,

            ..Default::default()
        },
    );
    SchemaDef { tables }
}

// plan fixup: renameField rewrites index.fields + ownerField +
// collaboratorsField that referenced the old name.
#[test]
fn plan_rename_field_fixes_index_owner_and_collab_refs() {
    let old = schema_with_auth_and_index();
    let d = vec![
        Directive::RenameField {
            table: "users".into(),
            from: "ownerId".into(),
            to: "uid".into(),
        },
        Directive::RenameField {
            table: "users".into(),
            from: "collabs".into(),
            to: "members".into(),
        },
    ];
    let got = plan_migration(&old, &d).unwrap();
    let t = &got.tables["users"];
    // index reference followed the rename.
    assert_eq!(t.indexes[0].fields, vec!["uid".to_string()]);
    // ownerField + collaboratorsField followed their respective renames.
    assert_eq!(t.owner_field.as_deref(), Some("uid"));
    assert_eq!(t.collaborators_field.as_deref(), Some("members"));
    // old field names are gone.
    assert!(!t.fields.contains_key("ownerId"));
    assert!(!t.fields.contains_key("collabs"));
}

// plan fixup: dropField prunes index.fields and clears ownerField /
// collaboratorsField that named the dropped field.
#[test]
fn plan_drop_field_clears_index_owner_and_collab_refs() {
    let old = schema_with_auth_and_index();
    let d = vec![
        Directive::DropField {
            table: "users".into(),
            field: "ownerId".into(),
        },
        Directive::DropField {
            table: "users".into(),
            field: "collabs".into(),
        },
    ];
    let got = plan_migration(&old, &d).unwrap();
    let t = &got.tables["users"];
    assert!(
        t.indexes[0].fields.is_empty(),
        "index pruned of dropped field"
    );
    assert!(t.owner_field.is_none(), "ownerField cleared");
    assert!(
        t.collaborators_field.is_none(),
        "collaboratorsField cleared"
    );
}

// `authorize` carries across a field rename: every `field` reference inside
// the predicate follows the rename (mirrors ownerField/collaboratorsField).
#[test]
fn plan_rename_field_carries_authorize_predicate() {
    use crate::query::FilterExpr;
    let mut old = schema_with_auth_and_index();
    // visibility OR owner==caller — references renamed field `ownerId`.
    old.tables
        .get_mut("users")
        .unwrap()
        .fields
        .insert("visibility".into(), FieldType::String);
    old.tables.get_mut("users").unwrap().authorize = Some(FilterExpr::Or {
        exprs: vec![
            FilterExpr::Eq {
                field: "ownerId".into(),
                value: serde_json::json!({"$user": true}),
            },
            FilterExpr::Eq {
                field: "visibility".into(),
                value: serde_json::json!("public"),
            },
        ],
    });
    let d = vec![Directive::RenameField {
        table: "users".into(),
        from: "ownerId".into(),
        to: "uid".into(),
    }];
    let got = plan_migration(&old, &d).unwrap();
    let t = &got.tables["users"];
    // The predicate now references `uid`, not `ownerId`, and the unrelated
    // `visibility` leaf is untouched.
    let expr = t.authorize.as_ref().expect("authorize preserved");
    match expr {
        FilterExpr::Or { exprs } => match (&exprs[0], &exprs[1]) {
            (FilterExpr::Eq { field: f0, .. }, FilterExpr::Eq { field: f1, .. }) => {
                assert_eq!(f0, "uid");
                assert_eq!(f1, "visibility");
            }
            other => panic!("expected two Eq leaves, got {other:?}"),
        },
        other => panic!("expected Or, got {other:?}"),
    }
}

// Dropping a field the `authorize` predicate still references is rejected:
// silently clearing the predicate would widen access (load-bearing for auth).
#[test]
fn plan_drop_field_rejects_when_authorize_references_it() {
    use crate::error::ErrorCode;
    use crate::query::FilterExpr;
    let mut old = schema_with_auth_and_index();
    old.tables
        .get_mut("users")
        .unwrap()
        .fields
        .insert("visibility".into(), FieldType::String);
    old.tables.get_mut("users").unwrap().authorize = Some(FilterExpr::Eq {
        field: "visibility".into(),
        value: serde_json::json!("public"),
    });
    let d = vec![Directive::DropField {
        table: "users".into(),
        field: "visibility".into(),
    }];
    let err = plan_migration(&old, &d).unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert!(
        err.message.contains("authorize"),
        "should explain the authorize conflict: {}",
        err.message
    );
    // Dropping an unrelated field is unaffected.
    let d2 = vec![Directive::DropField {
        table: "users".into(),
        field: "email".into(),
    }];
    assert!(plan_migration(&old, &d2).is_ok());
}

// ENH-028: renameField carries the computed map — expression `Field`
// references (and `Case.whens` predicate fields) follow the rename, and an
// entry KEYED on the renamed field moves to the new name (the `defaults`
// discipline; leaving it keyed on the old name would fail
// `validate_computed`'s declared-field rule on the derived schema).
#[test]
fn plan_rename_field_rewrites_computed_refs_and_key() {
    use crate::query::FilterExpr;
    let mut old = one_table_schema();
    let users = old.tables.get_mut("users").expect("users table");
    users.fields.insert("first".into(), FieldType::String);
    users.fields.insert("nick".into(), FieldType::String);
    users.computed.insert(
        "name".into(),
        ValueExpr::Concat {
            parts: vec![
                ValueExpr::Field {
                    field: "age".into(),
                },
                ValueExpr::Case {
                    whens: vec![CaseWhen {
                        when: FilterExpr::Exists {
                            field: "first".into(),
                        },
                        then: ValueExpr::Field {
                            field: "first".into(),
                        },
                    }],
                    otherwise: Box::new(ValueExpr::Literal {
                        value: serde_json::json!(""),
                    }),
                },
            ],
        },
    );
    users.computed.insert(
        "nick".into(),
        ValueExpr::Field {
            field: "age".into(),
        },
    );
    let d = vec![
        Directive::RenameField {
            table: "users".into(),
            from: "first".into(),
            to: "givenName".into(),
        },
        Directive::RenameField {
            table: "users".into(),
            from: "nick".into(),
            to: "handle".into(),
        },
    ];
    let got = plan_migration(&old, &d).unwrap();
    let t = &got.tables["users"];
    // `name`'s expr reads the renamed field — in the Case `then` and the
    // Case `when` predicate alike (the `Field(age)` part is untouched).
    let name_expr = t.computed.get("name").expect("name entry preserved");
    let expected = ValueExpr::Concat {
        parts: vec![
            ValueExpr::Field {
                field: "age".into(),
            },
            ValueExpr::Case {
                whens: vec![CaseWhen {
                    when: FilterExpr::Exists {
                        field: "givenName".into(),
                    },
                    then: ValueExpr::Field {
                        field: "givenName".into(),
                    },
                }],
                otherwise: Box::new(ValueExpr::Literal {
                    value: serde_json::json!(""),
                }),
            },
        ],
    };
    assert_eq!(name_expr, &expected);
    // The entry keyed on the renamed field moved; the old key is gone.
    assert!(t.computed.contains_key("handle"));
    assert!(!t.computed.contains_key("nick"));
}

// ENH-028: dropping a field a computed expression reads is rejected,
// naming the computed field; dropping the field an entry is keyed on
// removes the entry with it (the applier removes the stored key too).
#[test]
fn plan_drop_field_rejects_computed_reference_and_drops_keyed_entry() {
    let mut old = one_table_schema();
    let users = old.tables.get_mut("users").expect("users table");
    users.computed.insert(
        "age".into(),
        ValueExpr::Field {
            field: "name".into(),
        },
    );
    // Referenced field: rejected, naming the computed field.
    let d = vec![Directive::DropField {
        table: "users".into(),
        field: "name".into(),
    }];
    let err = plan_migration(&old, &d).unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::BadRequest);
    assert!(
        err.message.contains("computed field 'users.age'"),
        "should name the computed field: {}",
        err.message
    );
    // The keyed field: the entry goes with it, plan succeeds.
    let d2 = vec![Directive::DropField {
        table: "users".into(),
        field: "age".into(),
    }];
    let got = plan_migration(&old, &d2).unwrap();
    assert!(
        got.tables["users"].computed.is_empty(),
        "entry keyed on the dropped field is removed"
    );
}

// evalExpr `set` is interpolated into the jsonb_set key literal, so a stray
// quote (the classic injection shape) must be rejected as BAD_REQUEST at plan
// time — not reach the scoped-raw-SQL applier. Locks `is_valid_identifier`.
#[test]
fn plan_rejects_evalexpr_set_with_stray_quote() {
    let old = one_table_schema();
    let d = vec![Directive::EvalExpr {
        table: "users".into(),
        set: "name' WHERE 1=1".into(),
        expr: ExprSource::Legacy("'x'".into()),
        where_clause: None,
    }];
    let err = plan_migration(&old, &d).unwrap_err();
    assert_eq!(err.code, crate::error::ErrorCode::BadRequest);
    assert!(
        err.message.contains("valid field name"),
        "should explain the set-name rejection: {}",
        err.message
    );
}

// renameTable rewrites a self-referential `Id { table: <old self> }` inside
// the renamed table itself — the renamed table is removed from the map before
// the cross-table fixup loop, so without the self-ref pass it would dangle.
#[test]
fn plan_rename_table_rewrites_self_referential_id() {
    let mut fields = BTreeMap::new();
    fields.insert("name".into(), FieldType::String);
    fields.insert(
        "parent".into(),
        FieldType::Id {
            table: "node".into(),
            on_delete: None,
        },
    );
    let mut tables = BTreeMap::new();
    tables.insert(
        "node".into(),
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

            ..Default::default()
        },
    );
    let old = SchemaDef { tables };
    let d = vec![Directive::RenameTable {
        from: "node".into(),
        to: "nodes".into(),
    }];
    let got = plan_migration(&old, &d).unwrap();
    assert!(got.tables.contains_key("nodes"));
    assert_eq!(
        got.tables["nodes"].fields["parent"],
        FieldType::Id {
            table: "nodes".into(),
            on_delete: None,
        },
        "self-referential Id must follow the rename"
    );
}

// renameTable rewrites Id references in OTHER tables (companion to the
// self-ref case above); locks the pre-existing cross-table behavior.
#[test]
fn plan_rename_table_rewrites_cross_table_id_refs() {
    let mut user_fields = BTreeMap::new();
    user_fields.insert("name".into(), FieldType::String);
    let mut account_fields = BTreeMap::new();
    account_fields.insert(
        "owner".into(),
        FieldType::Id {
            table: "user".into(),
            on_delete: None,
        },
    );
    let mut tables = BTreeMap::new();
    tables.insert(
        "user".into(),
        TableDef {
            constraints: std::collections::BTreeMap::new(),
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: user_fields,
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,

            ..Default::default()
        },
    );
    tables.insert(
        "account".into(),
        TableDef {
            constraints: std::collections::BTreeMap::new(),
            defaults: std::collections::BTreeMap::new(),
            computed: std::collections::BTreeMap::new(),
            fields: account_fields,
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            updated_at_field: None,
            auto_increment_field: None,
            authorize: None,

            soft_delete: false,

            ..Default::default()
        },
    );
    let old = SchemaDef { tables };
    let d = vec![Directive::RenameTable {
        from: "user".into(),
        to: "users".into(),
    }];
    let got = plan_migration(&old, &d).unwrap();
    assert_eq!(
        got.tables["account"].fields["owner"],
        FieldType::Id {
            table: "users".into(),
            on_delete: None,
        },
        "cross-table Id must follow the rename"
    );
}

#[test]
fn coerce_value_emits_target_forms() {
    use serde_json::json;
    // ToNumber emits a JSON number (f64). NaN/inf strings parse as f64 but
    // are rejected by the `is_finite()` guard rather than collapsing to
    // Value::Null via serde_json's f64→Number path.
    assert_eq!(
        coerce_value(Cast::ToNumber, &json!("42")),
        Some(json!(42.0))
    );
    assert_eq!(coerce_value(Cast::ToNumber, &json!(true)), Some(json!(1.0)));
    assert_eq!(
        coerce_value(Cast::ToNumber, &json!(false)),
        Some(json!(0.0))
    );
    assert!(coerce_value(Cast::ToNumber, &json!("NaN")).is_none());
    assert!(coerce_value(Cast::ToNumber, &json!("inf")).is_none());
    assert!(coerce_value(Cast::ToNumber, &json!("not-a-number")).is_none());
    // ToInt64 emits a decimal-string JSON value (int64 wire convention —
    // see schema::is_valid_int64 and FEATURE_MATRIX.md #13).
    assert_eq!(
        coerce_value(Cast::ToInt64, &json!("123")),
        Some(json!("123"))
    );
    assert_eq!(coerce_value(Cast::ToInt64, &json!(456)), Some(json!("456")));
    // A float-valued JSON number with a fractional part does not fit i64.
    assert!(coerce_value(Cast::ToInt64, &json!(456.5)).is_none());
    assert!(coerce_value(Cast::ToInt64, &json!("not-an-int")).is_none());
    // ToString / ToBoolean round out the matrix.
    assert_eq!(coerce_value(Cast::ToString, &json!(42)), Some(json!("42")));
    assert_eq!(
        coerce_value(Cast::ToString, &json!(true)),
        Some(json!("true"))
    );
    assert_eq!(
        coerce_value(Cast::ToBoolean, &json!("true")),
        Some(json!(true))
    );
    assert_eq!(
        coerce_value(Cast::ToBoolean, &json!("0")),
        Some(json!(false))
    );
    assert_eq!(
        coerce_value(Cast::ToBoolean, &json!(0.0)),
        Some(json!(false))
    );
    assert_eq!(coerce_value(Cast::ToBoolean, &json!(3)), Some(json!(true)));
    assert!(coerce_value(Cast::ToBoolean, &json!("maybe")).is_none());
    // Objects/arrays/null have no coercion under any cast.
    assert!(coerce_value(Cast::ToString, &json!({"a":1})).is_none());
    assert!(coerce_value(Cast::ToNumber, &json!([1])).is_none());
    assert!(coerce_value(Cast::ToInt64, &json!(null)).is_none());
}
