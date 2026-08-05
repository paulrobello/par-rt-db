//! Pure advisory diff of a pending `SchemaDef` against the currently-applied
//! schema. Used by the `/admin/db/{db}/schema/preview` endpoint to show the
//! operator exactly what an additive-only push will ADD and what it would have
//! to drop or change (and therefore will be rejected by `ddl::push_schema`).
//!
//! This module performs NO I/O and applies nothing — `ddl::push_schema` remains
//! the authoritative gate. The diff is best-effort advisory UI: it tries to be
//! accurate about what the DDL layer will accept, but the final word belongs to
//! `push_schema` itself.

use serde::Serialize;

use crate::schema::{FieldType, SchemaDef};

/// Result of comparing a pending schema against the currently-applied one.
/// `added` lists every new table/column/index the push will create; `rejected`
/// lists every drop or type-change the additive-only DDL layer will refuse.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SchemaDiff {
    pub added: Vec<TableAdd>,
    pub rejected: Vec<Rejection>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableAdd {
    pub table: String,
    pub columns: Vec<ColumnAdd>,
    pub indexes: Vec<IndexAdd>,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnAdd {
    pub name: String,
    /// Human-readable field type, mirroring the dashboard's `formatFieldType`
    /// (e.g. `string`, `id<projects>`, `string?`, `string | number`).
    pub field_type: String,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexAdd {
    pub name: String,
    pub fields: Vec<String>,
}

/// A single drop or type-change the additive-only push will refuse. `item` is
/// the bare column/index name; `reason` is a self-describing sentence naming it.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Rejection {
    pub table: String,
    pub item: String,
    pub reason: String,
}

/// Compare `pending` against `current` (the currently-applied schema, or `None`
/// for a fresh database that has never had a schema pushed). Pure and
/// allocation-only — no pool, no async, no side effects.
///
/// Rules (mirroring `ddl::push_schema`'s additive-only contract):
/// - A table in `pending` not in `current` → every column and index under it is
///   `added`.
/// - A table in both: each pending-only column is `added`; each current-only
///   column is `rejected` (cannot drop); a column present in both with a
///   different `FieldType` is `rejected` (cannot change type). The same three
///   rules apply to indexes, matched by name. (`FieldType` and `IndexDef` both
///   derive `PartialEq`, so `!=` is a structural equality.)
/// - Tables present in `current` but absent from `pending` are out of scope:
///   `push_schema` iterates the pending schema's tables and ignores others, so
///   a missing table here is not a drop the DDL layer would reject.
pub fn diff(current: Option<&SchemaDef>, pending: &SchemaDef) -> SchemaDiff {
    let mut added: Vec<TableAdd> = Vec::new();
    let mut rejected: Vec<Rejection> = Vec::new();

    for (table_name, pending_table) in &pending.tables {
        let Some(current_schema) = current else {
            added.push(fresh_table_add(table_name, pending_table));
            continue;
        };
        let Some(current_table) = current_schema.tables.get(table_name) else {
            added.push(fresh_table_add(table_name, pending_table));
            continue;
        };

        let mut columns: Vec<ColumnAdd> = Vec::new();
        let mut indexes: Vec<IndexAdd> = Vec::new();

        // Columns: pending-only → added; current-only → rejected (drop);
        // both-but-different-type → rejected (change).
        for (fname, ftype) in &pending_table.fields {
            match current_table.fields.get(fname) {
                None => columns.push(ColumnAdd {
                    name: fname.clone(),
                    field_type: field_type_display(ftype),
                }),
                Some(existing) if existing != ftype => {
                    rejected.push(Rejection {
                        table: table_name.clone(),
                        item: fname.clone(),
                        reason: format!(
                            "column '{fname}' type cannot be changed (pushes are additive): {} \u{2192} {}",
                            field_type_display(existing),
                            field_type_display(ftype),
                        ),
                    });
                }
                _ => {}
            }
        }
        for fname in current_table.fields.keys() {
            if !pending_table.fields.contains_key(fname) {
                rejected.push(Rejection {
                    table: table_name.clone(),
                    item: fname.clone(),
                    reason: format!("column '{fname}' cannot be dropped (pushes are additive)",),
                });
            }
        }

        // Indexes (matched by name): pending-only → added; current-only →
        // rejected (drop); both-but-different-definition → rejected (change).
        for pending_idx in &pending_table.indexes {
            match current_table
                .indexes
                .iter()
                .find(|c| c.name == pending_idx.name)
            {
                None => indexes.push(IndexAdd {
                    name: pending_idx.name.clone(),
                    fields: pending_idx.fields.clone(),
                }),
                Some(existing) if existing != pending_idx => {
                    rejected.push(Rejection {
                        table: table_name.clone(),
                        item: pending_idx.name.clone(),
                        reason: format!(
                            "index '{}' definition cannot be changed (pushes are additive)",
                            pending_idx.name,
                        ),
                    });
                }
                _ => {}
            }
        }
        for current_idx in &current_table.indexes {
            if !pending_table
                .indexes
                .iter()
                .any(|i| i.name == current_idx.name)
            {
                rejected.push(Rejection {
                    table: table_name.clone(),
                    item: current_idx.name.clone(),
                    reason: format!(
                        "index '{}' cannot be dropped (pushes are additive)",
                        current_idx.name,
                    ),
                });
            }
        }

        if !columns.is_empty() || !indexes.is_empty() {
            added.push(TableAdd {
                table: table_name.clone(),
                columns,
                indexes,
            });
        }
    }

    SchemaDiff { added, rejected }
}

/// Whole-table add: every column and index is new.
fn fresh_table_add(table_name: &str, pending_table: &crate::schema::TableDef) -> TableAdd {
    TableAdd {
        table: table_name.to_string(),
        columns: pending_table
            .fields
            .iter()
            .map(|(name, ty)| ColumnAdd {
                name: name.clone(),
                field_type: field_type_display(ty),
            })
            .collect(),
        indexes: pending_table
            .indexes
            .iter()
            .map(|idx| IndexAdd {
                name: idx.name.clone(),
                fields: idx.fields.clone(),
            })
            .collect(),
    }
}

/// Compact human-readable rendering of a `FieldType`, mirroring the dashboard's
/// `formatFieldType` helper (`string`, `id<projects>`, `string?`,
/// `string | number`, `vector(1536)`, …). Kept in sync so the preview panel and
/// the schema viewer describe types the same way.
fn field_type_display(ty: &FieldType) -> String {
    match ty {
        FieldType::String => "string".into(),
        FieldType::Number => "number".into(),
        FieldType::Boolean => "boolean".into(),
        FieldType::Null => "null".into(),
        FieldType::Int64 => "int64".into(),
        FieldType::Bytes => "bytes".into(),
        FieldType::Any => "any".into(),
        FieldType::Id { table } => format!("id<{table}>"),
        FieldType::Literal { value } => format!("literal({value})"),
        FieldType::Optional { inner } => format!("{}?", field_type_display(inner)),
        FieldType::Union { variants } => variants
            .iter()
            .map(field_type_display)
            .collect::<Vec<_>>()
            .join(" | "),
        FieldType::Array { element } => format!("{}[]", field_type_display(element)),
        FieldType::Object { .. } => "object".into(),
        FieldType::Record { value } => format!("record<{}>", field_type_display(value)),
        FieldType::Vector { dimensions } => format!("vector({dimensions})"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{IndexDef, SchemaDef, TableDef};

    fn table(fields: &[(&str, FieldType)], indexes: &[IndexDef]) -> TableDef {
        TableDef {
            fields: fields
                .iter()
                .map(|(n, t)| (n.to_string(), t.clone()))
                .collect(),
            indexes: indexes.to_vec(),
            owner_field: None,
            collaborators_field: None,
            ttl: None,
            authorize: None,
        }
    }

    fn idx(name: &str, fields: &[&str]) -> IndexDef {
        IndexDef {
            name: name.to_string(),
            fields: fields.iter().map(|s| s.to_string()).collect(),
            search: false,
            vector: None,
            unique: false,
            r#where: None,
            language: None,
        }
    }

    fn schema(tables: &[(&str, TableDef)]) -> SchemaDef {
        SchemaDef {
            tables: tables
                .iter()
                .map(|(n, t)| (n.to_string(), t.clone()))
                .collect(),
        }
    }

    // Fresh db (current = None): every table/column/index is added, nothing
    // rejected.
    #[test]
    fn fresh_db_adds_everything() {
        let pending = schema(&[(
            "items",
            table(
                &[("name", FieldType::String), ("count", FieldType::Number)],
                &[idx("by_name", &["name"])],
            ),
        )]);
        let diff = diff(None, &pending);
        assert!(diff.rejected.is_empty());
        assert_eq!(diff.added.len(), 1);
        let t = &diff.added[0];
        assert_eq!(t.table, "items");
        assert_eq!(t.columns.len(), 2);
        // BTreeMap iterates sorted: count before name.
        assert_eq!(t.columns[0].name, "count");
        assert_eq!(t.columns[0].field_type, "number");
        assert_eq!(t.columns[1].name, "name");
        assert_eq!(t.columns[1].field_type, "string");
        assert_eq!(t.indexes.len(), 1);
        assert_eq!(t.indexes[0].name, "by_name");
        assert_eq!(t.indexes[0].fields, vec!["name".to_string()]);
    }

    // Added column + index on an existing table appear in `added`; existing
    // columns/indexes carried over unchanged produce no rejection.
    #[test]
    fn added_column_and_index_listed() {
        let current = schema(&[("items", table(&[("name", FieldType::String)], &[]))]);
        let pending = schema(&[(
            "items",
            table(
                &[("name", FieldType::String), ("count", FieldType::Number)],
                &[idx("by_count", &["count"])],
            ),
        )]);
        let diff = diff(Some(&current), &pending);
        assert!(diff.rejected.is_empty());
        assert_eq!(diff.added.len(), 1);
        let t = &diff.added[0];
        assert_eq!(t.columns.len(), 1);
        assert_eq!(t.columns[0].name, "count");
        assert_eq!(t.indexes.len(), 1);
        assert_eq!(t.indexes[0].name, "by_count");
    }

    // Dropping a column is rejected.
    #[test]
    fn dropped_column_rejected() {
        let current = schema(&[("items", table(&[("name", FieldType::String)], &[]))]);
        let pending = schema(&[("items", table(&[], &[]))]);
        let diff = diff(Some(&current), &pending);
        assert!(diff.added.is_empty());
        assert_eq!(diff.rejected.len(), 1);
        let r = &diff.rejected[0];
        assert_eq!(r.table, "items");
        assert_eq!(r.item, "name");
        assert!(r.reason.contains("cannot be dropped"));
    }

    // Changing a column's type is rejected; both sides named in the reason.
    #[test]
    fn type_change_rejected() {
        let current = schema(&[("items", table(&[("name", FieldType::String)], &[]))]);
        let pending = schema(&[("items", table(&[("name", FieldType::Number)], &[]))]);
        let diff = diff(Some(&current), &pending);
        assert!(diff.added.is_empty());
        assert_eq!(diff.rejected.len(), 1);
        let r = &diff.rejected[0];
        assert_eq!(r.item, "name");
        assert!(r.reason.contains("cannot be changed"));
        assert!(r.reason.contains("string"));
        assert!(r.reason.contains("number"));
    }

    // Dropping an index is rejected.
    #[test]
    fn dropped_index_rejected() {
        let current = schema(&[(
            "items",
            table(&[("name", FieldType::String)], &[idx("by_name", &["name"])]),
        )]);
        let pending = schema(&[("items", table(&[("name", FieldType::String)], &[]))]);
        let diff = diff(Some(&current), &pending);
        assert!(diff.added.is_empty());
        assert_eq!(diff.rejected.len(), 1);
        assert_eq!(diff.rejected[0].item, "by_name");
        assert!(diff.rejected[0].reason.contains("cannot be dropped"));
    }

    // Changing an index's field list (same name) is rejected.
    #[test]
    fn changed_index_rejected() {
        let current = schema(&[(
            "items",
            table(
                &[("a", FieldType::String), ("b", FieldType::String)],
                &[idx("by_ab", &["a"])],
            ),
        )]);
        let pending = schema(&[(
            "items",
            table(
                &[("a", FieldType::String), ("b", FieldType::String)],
                &[idx("by_ab", &["a", "b"])],
            ),
        )]);
        let diff = diff(Some(&current), &pending);
        assert!(diff.added.is_empty());
        assert_eq!(diff.rejected.len(), 1);
        assert_eq!(diff.rejected[0].item, "by_ab");
        assert!(diff.rejected[0].reason.contains("cannot be changed"));
    }

    // Identical schemas produce an empty diff (no additions, no rejections).
    #[test]
    fn identical_schema_is_empty_diff() {
        let current = schema(&[(
            "items",
            table(&[("name", FieldType::String)], &[idx("by_name", &["name"])]),
        )]);
        let diff = diff(Some(&current), &current);
        assert!(diff.added.is_empty());
        assert!(diff.rejected.is_empty());
    }

    // A pending table absent from `current` is a fresh-table add even when
    // other tables already exist.
    #[test]
    fn new_table_is_fully_added_alongside_existing_tables() {
        let current = schema(&[("old", table(&[("name", FieldType::String)], &[]))]);
        let pending = schema(&[
            ("old", table(&[("name", FieldType::String)], &[])),
            (
                "new",
                table(
                    &[("title", FieldType::String)],
                    &[idx("by_title", &["title"])],
                ),
            ),
        ]);
        let diff = diff(Some(&current), &pending);
        assert!(diff.rejected.is_empty());
        assert_eq!(diff.added.len(), 1);
        assert_eq!(diff.added[0].table, "new");
        assert_eq!(diff.added[0].columns.len(), 1);
        assert_eq!(diff.added[0].indexes.len(), 1);
    }

    // The `field_type_display` form covers the recursive/nested cases the
    // preview panel renders (optional, union, array, id, vector).
    #[test]
    fn field_type_display_covers_common_shapes() {
        assert_eq!(field_type_display(&FieldType::String), "string");
        assert_eq!(
            field_type_display(&FieldType::Id {
                table: "users".into()
            }),
            "id<users>",
        );
        assert_eq!(
            field_type_display(&FieldType::Optional {
                inner: Box::new(FieldType::String)
            }),
            "string?",
        );
        assert_eq!(
            field_type_display(&FieldType::Array {
                element: Box::new(FieldType::String)
            }),
            "string[]",
        );
        let union = FieldType::Union {
            variants: vec![
                FieldType::Literal {
                    value: serde_json::json!("a"),
                },
                FieldType::Literal {
                    value: serde_json::json!("b"),
                },
            ],
        };
        assert_eq!(
            field_type_display(&union),
            "literal(\"a\") | literal(\"b\")"
        );
        assert_eq!(
            field_type_display(&FieldType::Vector { dimensions: 1536 }),
            "vector(1536)",
        );
    }
}
