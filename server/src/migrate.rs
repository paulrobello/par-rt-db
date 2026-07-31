//! Declarative schema migration: an ordered list of directives the server
//! applies transactionally to transform a database's schema and documents.
//! See docs/superpowers/specs/2026-07-31-schema-migration-backfill-design.md.
use crate::error::RtDbError;
use crate::schema::{FieldType, SchemaDef, TableDef};

/// One migration step. Wire shape mirrors `txn::Step`: `tag = "op"`,
/// camelCase, `deny_unknown_fields`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "op", rename_all = "camelCase", deny_unknown_fields)]
pub enum Directive {
    RenameField {
        table: String,
        from: String,
        to: String,
    },
    RenameTable {
        from: String,
        to: String,
    },
    ChangeType {
        table: String,
        field: String,
        to: FieldType,
        cast: Cast,
        #[serde(default)]
        default: Option<serde_json::Value>,
    },
    DropField {
        table: String,
        field: String,
    },
    DropTable {
        name: String,
    },
    DropIndex {
        table: String,
        name: String,
    },
    SetDefault {
        table: String,
        field: String,
        value: serde_json::Value,
    },
    EvalExpr {
        table: String,
        set: String,
        expr: String,
        #[serde(default, rename = "where")]
        where_clause: Option<String>,
    },
}

/// Closed set of sound coercions for `Directive::ChangeType`.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum Cast {
    ToString,
    ToNumber,
    ToInt64,
    ToBoolean,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrateRequest {
    pub directives: Vec<Directive>,
    #[serde(default)]
    pub dry_run: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MigrateResult {
    pub applied: bool,
    pub schema: crate::schema::SchemaDef,
    pub directives: Vec<DirectiveReport>,
}

/// Per-directive outcome. `Default` is derived so later tasks can build it
/// incrementally with `..Default::default()`; the `op` and `affected_rows`
/// fields are always set explicitly.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DirectiveReport {
    pub op: String,
    pub affected_rows: i64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cast_failures: Vec<CastFailure>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sample_changes: Vec<SampleChange>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CastFailure {
    pub id: String,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SampleChange {
    pub id: String,
    pub before: serde_json::Value,
    pub after: serde_json::Value,
}

/// Validates `directives` against `old`, folding each into a working copy of
/// the schema in order, and returns the derived resulting `SchemaDef`. Pure
/// (no DB). Rejects: missing source table/field/index; a rename/changeType
/// target that already exists or is produced by an earlier directive; a cast
/// invalid for the old→new type pair; an out-of-scope `evalExpr` (contains a
/// `FROM`/JOIN or a DDL verb keyword, or targets a missing table).
pub fn plan_migration(old: &SchemaDef, directives: &[Directive]) -> Result<SchemaDef, RtDbError> {
    let mut schema = old.clone();
    for d in directives {
        validate_one(&mut schema, d)?;
    }
    Ok(schema)
}

fn validate_one(schema: &mut SchemaDef, d: &Directive) -> Result<(), RtDbError> {
    match d {
        Directive::RenameField { table, from, to } => {
            let t = table_mut(schema, table)?;
            if t.fields.contains_key(to) {
                return Err(RtDbError::bad_request(format!(
                    "rename target '{table}.{to}' already exists"
                )));
            }
            let ft = t.fields.remove(from).ok_or_else(|| {
                RtDbError::bad_request(format!("renamed field '{table}.{from}' does not exist"))
            })?;
            t.fields.insert(to.clone(), ft);
            // fix index references that used `from`
            for ix in t.indexes.iter_mut() {
                for f in ix.fields.iter_mut() {
                    if f == from {
                        *f = to.clone();
                    }
                }
            }
            if t.owner_field.as_deref() == Some(from.as_str()) {
                t.owner_field = Some(to.clone());
            }
            if t.collaborators_field.as_deref() == Some(from.as_str()) {
                t.collaborators_field = Some(to.clone());
            }
        }
        Directive::RenameTable { from, to } => {
            if schema.tables.contains_key(to) {
                return Err(RtDbError::bad_request(format!(
                    "rename target table '{to}' already exists"
                )));
            }
            let def = schema.tables.remove(from).ok_or_else(|| {
                RtDbError::bad_request(format!("renamed table '{from}' does not exist"))
            })?;
            // Id references to `from` in other tables follow the rename.
            for t in schema.tables.values_mut() {
                for ft in t.fields.values_mut() {
                    if let FieldType::Id { table } = ft
                        && table == from
                    {
                        *table = to.clone();
                    }
                }
            }
            schema.tables.insert(to.clone(), def);
        }
        Directive::ChangeType {
            table,
            field,
            to: new_ty,
            cast,
            ..
        } => {
            let t = table_mut(schema, table)?;
            let old_ty = t.fields.get(field).ok_or_else(|| {
                RtDbError::bad_request(format!("changed field '{table}.{field}' does not exist"))
            })?;
            if !cast_valid_for(*cast, old_ty) {
                return Err(RtDbError::bad_request(format!(
                    "cast {cast:?} is not valid for {table}.{field}"
                )));
            }
            t.fields.insert(field.clone(), new_ty.clone());
        }
        Directive::DropField { table, field } => {
            let t = table_mut(schema, table)?;
            if t.fields.remove(field).is_none() {
                return Err(RtDbError::bad_request(format!(
                    "dropped field '{table}.{field}' does not exist"
                )));
            }
            for ix in t.indexes.iter_mut() {
                ix.fields.retain(|f| f != field);
            }
            if t.owner_field.as_deref() == Some(field.as_str()) {
                t.owner_field = None;
            }
            if t.collaborators_field.as_deref() == Some(field.as_str()) {
                t.collaborators_field = None;
            }
        }
        Directive::DropTable { name } => {
            if schema.tables.remove(name).is_none() {
                return Err(RtDbError::bad_request(format!(
                    "dropped table '{name}' does not exist"
                )));
            }
        }
        Directive::DropIndex { table, name } => {
            let t = table_mut(schema, table)?;
            if !t.indexes.iter().any(|ix| &ix.name == name) {
                return Err(RtDbError::bad_request(format!(
                    "dropped index '{table}.{name}' does not exist"
                )));
            }
            t.indexes.retain(|ix| &ix.name != name);
        }
        Directive::SetDefault { table, field, .. } => {
            let t = table_mut(schema, table)?;
            if !t.fields.contains_key(field) {
                return Err(RtDbError::bad_request(format!(
                    "setDefault target '{table}.{field}' does not exist"
                )));
            }
            // data-only; schema unchanged
        }
        Directive::EvalExpr {
            table,
            set,
            expr,
            where_clause,
        } => {
            let _ = table_mut(schema, table)?; // table must exist
            // `set` is a field path; the field need not exist (evalExpr may populate a
            // new key the caller adds via a later additive push), but the table must.
            if has_sql_violation(expr) || where_clause.as_deref().is_some_and(has_sql_violation) {
                return Err(RtDbError::bad_request(format!(
                    "evalExpr for '{table}.{set}' is out of scope (no FROM/joins or DDL verbs)"
                )));
            }
        }
    }
    Ok(())
}

fn table_mut<'a>(schema: &'a mut SchemaDef, table: &str) -> Result<&'a mut TableDef, RtDbError> {
    schema
        .tables
        .get_mut(table)
        .ok_or_else(|| RtDbError::bad_request(format!("table '{table}' does not exist")))
}

/// True if `cast` can coerce from `old`. Mirrors the matrix in the spec.
fn cast_valid_for(cast: Cast, old: &FieldType) -> bool {
    use FieldType::*;
    matches!(
        (cast, old),
        (Cast::ToString, String | Number | Boolean | Int64)
            | (Cast::ToNumber, String | Boolean | Int64)
            | (Cast::ToInt64, String | Number)
            | (Cast::ToBoolean, String | Number)
    )
}

/// Rejects the scoped-raw-SQL boundary violations: a `FROM`/`JOIN` (cross-table)
/// or any DDL verb. The admin is trusted; this is blast-radius scoping.
fn has_sql_violation(sql: &str) -> bool {
    let upper = sql.to_ascii_uppercase();
    const FORBIDDEN: &[&str] = &[
        " FROM ",
        " JOIN ",
        " INTO ",
        "UPDATE ",
        "DELETE ",
        "INSERT ",
        "DROP ",
        "ALTER ",
        "TRUNCATE ",
        "CREATE ",
        "GRANT ",
        "REVOKE ",
    ];
    FORBIDDEN.iter().any(|kw| upper.contains(kw))
}

#[cfg(test)]
mod tests {
    use super::*;

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
                    expr: "upper(doc->>'fullName')".into(),
                    where_clause: Some("doc ? 'fullName'".into()),
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

    use std::collections::BTreeMap;

    fn one_table_schema() -> SchemaDef {
        let mut fields = BTreeMap::new();
        fields.insert("name".into(), FieldType::String);
        fields.insert("age".into(), FieldType::Number);
        let mut tables = BTreeMap::new();
        tables.insert(
            "users".into(),
            TableDef {
                fields,
                indexes: vec![],
                owner_field: None,
                collaborators_field: None,
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
                fields,
                indexes: vec![],
                owner_field: None,
                collaborators_field: None,
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

    #[test]
    fn plan_rejects_evalexpr_with_from_clause() {
        let old = one_table_schema();
        let d = vec![Directive::EvalExpr {
            table: "users".into(),
            set: "name".into(),
            expr: "x FROM other".into(),
            where_clause: None,
        }];
        assert!(plan_migration(&old, &d).is_err());
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
}
