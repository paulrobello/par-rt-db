//! Anon→real account merge (FM-27): pure derivation of principal-bearing
//! fields from a table def, the doc rewrite, and the cross-database
//! orchestration. See docs/superpowers/specs/2026-08-14-anon-merge-design.md.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::RtDbError;
use crate::query::FilterExpr;
use crate::schema::{FieldType, TableDef};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FieldKind {
    /// A scalar string field: rewrite is a whole-value swap.
    Scalar,
    /// An array-of-strings field: rewrite is element swap + dedupe.
    Array,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrincipalField {
    pub field: String,
    pub kind: FieldKind,
}

/// Per-db outcome of `RunMergeUsers`: restamped-doc counts per table and the
/// rows skipped because the restamp would violate a unique index.
#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeDbResult {
    pub tables: BTreeMap<String, usize>,
    pub conflicts: Vec<MergeConflict>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeConflict {
    pub table: String,
    pub id: String,
}

/// Whether values of this declared type are string-comparable (so a scalar
/// swap is sound). `Optional` unwraps; a `Union` qualifies only if every
/// variant does; a string-valued `Literal` qualifies.
fn string_compatible(ty: &FieldType) -> bool {
    match ty {
        FieldType::String | FieldType::Id { .. } => true,
        FieldType::Optional { inner } | FieldType::Array { element: inner } => {
            string_compatible(inner)
        }
        FieldType::Union { variants } => variants.iter().all(string_compatible),
        FieldType::Literal { value } => value.is_string(),
        _ => false,
    }
}

/// The rewrite kind a declared field supports: `Scalar` for string-compatible
/// types, `Array` for array-of-strings, `None` when neither (skip with a
/// warning — over-approximate to skipping, never fail the merge).
fn rewrite_kind(ty: &FieldType) -> Option<FieldKind> {
    match ty {
        FieldType::Array { element } if string_compatible(element) => Some(FieldKind::Array),
        FieldType::Optional { inner } => rewrite_kind(inner),
        ty if string_compatible(ty) => Some(FieldKind::Scalar),
        _ => None,
    }
}

/// `true` for the exact principal marker `{"$user": true}` anywhere in a
/// value — including nested inside an `In` array. Mirrors
/// `txn.rs::user_eq_fields`' marker test, but broader: the merge walker must
/// find EVERY field referencing the anon uid, not only stampable Eq leaves.
fn mentions_user_marker(v: &serde_json::Value) -> bool {
    if let serde_json::Value::Object(map) = v
        && map.len() == 1
    {
        return map.get("$user").and_then(|x| x.as_bool()) == Some(true);
    }
    v.as_array()
        .is_some_and(|arr| arr.iter().any(mentions_user_marker))
}

/// Collects every field that can carry a user principal for this table:
/// `ownerField`, `collaboratorsField`, and every field of the `authorize`
/// predicate whose comparison value mentions the `$user` marker (the walk
/// descends `And`/`Or`/`Not` and checks every value-bearing variant — a new
/// `FilterExpr` variant is a compile-visible change site here). The rewrite
/// kind comes from the field's declared type, so a field arriving from two
/// sources (ownerField AND authorize) dedupes consistently.
pub(crate) fn principal_bearing_fields(table: &TableDef) -> Vec<PrincipalField> {
    let mut out: Vec<PrincipalField> = Vec::new();
    let push = |name: &str, out: &mut Vec<PrincipalField>| match table.fields.get(name) {
        Some(ty) => match rewrite_kind(ty) {
            Some(kind) => {
                if !out.iter().any(|f| f.field == name) {
                    out.push(PrincipalField {
                        field: name.to_string(),
                        kind,
                    });
                }
            }
            None => tracing::warn!(
                field = name,
                "merge: principal-bearing field is not string or array-of-strings; skipping"
            ),
        },
        None => tracing::warn!(
            field = name,
            "merge: principal-bearing field not declared; skipping"
        ),
    };

    if let Some(owner) = &table.owner_field {
        push(owner, &mut out);
    }
    if let Some(collab) = &table.collaborators_field {
        push(collab, &mut out);
    }
    if let Some(authorize) = &table.authorize {
        fn walk_authorize(
            expr: &FilterExpr,
            out: &mut Vec<PrincipalField>,
            push: &impl Fn(&str, &mut Vec<PrincipalField>),
        ) {
            match expr {
                FilterExpr::Eq { field, value }
                | FilterExpr::Neq { field, value }
                | FilterExpr::Gt { field, value }
                | FilterExpr::Gte { field, value }
                | FilterExpr::Lt { field, value }
                | FilterExpr::Lte { field, value }
                | FilterExpr::Contains { field, value } => {
                    if mentions_user_marker(value) {
                        push(field, out);
                    }
                }
                FilterExpr::In { field, values } => {
                    if values.iter().any(mentions_user_marker) {
                        push(field, out);
                    }
                }
                FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
                    for e in exprs {
                        walk_authorize(e, out, push);
                    }
                }
                FilterExpr::Not { expr } => walk_authorize(expr, out, push),
                FilterExpr::Exists { .. } => {}
            }
        }
        walk_authorize(authorize, &mut out, &push);
    }
    out
}

/// Rewrites occurrences of `anon` to `real` in `doc` for exactly the given
/// principal-bearing fields. Scalar: whole-value swap when the value equals
/// `anon`. Array: drop `anon` elements, append `real` unless already present.
/// Returns whether anything changed. Never touches other values.
pub(crate) fn rewrite_doc(
    doc: &mut serde_json::Map<String, serde_json::Value>,
    fields: &[PrincipalField],
    anon: &str,
    real: &str,
) -> bool {
    let mut changed = false;
    for pf in fields {
        let Some(value) = doc.get_mut(&pf.field) else {
            continue;
        };
        match pf.kind {
            FieldKind::Scalar => {
                if value.as_str() == Some(anon) {
                    *value = serde_json::Value::String(real.to_string());
                    changed = true;
                }
            }
            FieldKind::Array => {
                let Some(arr) = value.as_array_mut() else {
                    continue;
                };
                let had_anon = arr.iter().any(|v| v.as_str() == Some(anon));
                if !had_anon {
                    continue;
                }
                arr.retain(|v| v.as_str() != Some(anon));
                if !arr.iter().any(|v| v.as_str() == Some(real)) {
                    arr.push(serde_json::Value::String(real.to_string()));
                }
                changed = true;
            }
        }
    }
    changed
}

/// Full-instance merge outcome across every database plus the auth/storage
/// steps. Returned by `POST /admin/merge-users` and logged by the OAuth
/// callback hook (Tasks 4–5).
#[derive(Debug, Clone, Default, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MergeReport {
    pub dbs: BTreeMap<String, MergeDbResult>,
    pub storage_repointed: u64,
    pub sessions_repointed: u64,
    pub anon_deleted: bool,
}

/// Anon→real merge, crash-safe by ordering (spec §"Merge order"):
/// 1. document re-stamps per db, each inside that db's committer turn;
/// 2. storage blob owner swap per db (direct SQL — storage bypasses the
///    committer by design);
/// 3. session re-point (`UPDATE ... SET user_id`, NOT delete — an open WS or
///    stored SDK token promotes to the real principal on its next op);
/// 4. guarded anon-row delete (`AND anonymous = TRUE` makes re-runs inert).
///
/// Any interruption is recovered by signing in again: every step is
/// idempotent and `/begin` re-records the binding while the anon row exists.
pub async fn merge_users(
    state: &Arc<crate::AppState>,
    anon_id: &str,
    real_id: &str,
) -> Result<MergeReport, RtDbError> {
    // Self-merge refusal: the admin path takes caller-supplied ids, and an
    // anon==real merge would re-stamp docs to the target then delete the
    // target row itself.
    if anon_id == real_id {
        return Err(RtDbError::bad_request(
            "anon and real user ids must differ; refusing self-merge",
        ));
    }

    // Guard: the source row must exist and be anonymous. This refuses admin
    // mistakes (real→real). A missing row is a completed merge (or none ever
    // started), not an error — the OAuth callback hook must be able to re-fire
    // after a crash between the guarded delete and its reply.
    let row: Option<(bool,)> =
        sqlx::query_as("SELECT anonymous FROM rtdb_auth.users WHERE id = $1")
            .bind(anon_id)
            .fetch_optional(&state.pool)
            .await?;
    match row {
        Some((true,)) => {}
        Some((false,)) => {
            return Err(RtDbError::bad_request(
                "source user is not anonymous; refusing merge",
            ));
        }
        None => {
            tracing::info!(anon_id, "merge: anonymous user not found; nothing to do");
            return Ok(MergeReport::default());
        }
    }

    let mut report = MergeReport::default();
    for db in crate::db::list_databases(&state.pool).await? {
        match state
            .realtime
            .committers
            .merge_users(&db, anon_id, real_id)
            .await
        {
            Ok(res) => {
                report.dbs.insert(db.clone(), res);
            }
            // A registered db with no pushed schema (`NotFound` from the
            // committer arm's schema load) has no doc restamp to run — but it
            // can still hold storage blobs (uploads need no schema), so fall
            // through to the owner swap below.
            Err(err) if err.code == crate::error::ErrorCode::NotFound => {}
            Err(err) => match crate::db::database_exists(&state.pool, &db).await {
                // Db still there: a real merge failure — propagate.
                Ok(true) => return Err(err),
                // Db gone mid-flight: not a merge failure; fall through to
                // the storage swap (42P01-tolerated on the dropped schema).
                Ok(false) => tracing::warn!(
                    db = %db,
                    error = %err,
                    "merge: doc restamp failed on a db that no longer exists; skipping"
                ),
                // The existence check itself failed: over-approximate to
                // "db exists" so the original error propagates instead of
                // being dropped right before the irreversible later steps.
                Err(check) => {
                    tracing::warn!(db = %db, error = %check, "merge: db existence check failed");
                    return Err(err);
                }
            },
        }

        // Storage owner swap. The table is lazy-created; a db with no uploads
        // yet has no relation — treat undefined_table (42P01) as zero rows.
        let schema_name = crate::ddl::pg_schema(&db);
        let swapped = sqlx::query(&format!(
            "UPDATE \"{schema_name}\".\"storage\" SET \"owner_id\" = $1 WHERE \"owner_id\" = $2"
        ))
        .bind(real_id)
        .bind(anon_id)
        .execute(&state.pool)
        .await;
        match swapped {
            Ok(res) => report.storage_repointed += res.rows_affected(),
            Err(err) if is_undefined_table(&err) => {}
            Err(err) => match crate::db::database_exists(&state.pool, &db).await {
                Ok(true) => return Err(err.into()),
                Ok(false) => tracing::warn!(
                    db = %db,
                    error = %err,
                    "merge: storage owner swap failed on a db that no longer exists; skipping"
                ),
                // Same over-approximation as the doc-restamp branch: never
                // drop a real error with the anon delete still ahead.
                Err(check) => {
                    tracing::warn!(db = %db, error = %err, "merge: db existence check failed");
                    return Err(check);
                }
            },
        }
    }

    let repointed = sqlx::query("UPDATE rtdb_auth.sessions SET user_id = $1 WHERE user_id = $2")
        .bind(real_id)
        .bind(anon_id)
        .execute(&state.pool)
        .await?;
    report.sessions_repointed = repointed.rows_affected();

    let deleted = sqlx::query("DELETE FROM rtdb_auth.users WHERE id = $1 AND anonymous = TRUE")
        .bind(anon_id)
        .execute(&state.pool)
        .await?;
    report.anon_deleted = deleted.rows_affected() == 1;

    Ok(report)
}

/// Postgres `undefined_table` (42P01): the per-db storage relation does not
/// exist yet (lazy-created on first upload).
fn is_undefined_table(err: &sqlx::Error) -> bool {
    err.as_database_error()
        .is_some_and(|d| d.code().is_some_and(|c| c == "42P01"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn table(fields: &[(&str, FieldType)]) -> TableDef {
        let mut map = BTreeMap::new();
        for (name, ty) in fields {
            map.insert((*name).to_string(), ty.clone());
        }
        TableDef {
            defaults: std::collections::BTreeMap::new(),
            fields: map,
            indexes: vec![],
            owner_field: None,
            collaborators_field: None,
            authorize: None,

            soft_delete: false,
            ttl: None,
            updated_at_field: None,
        }
    }

    fn user_marker() -> serde_json::Value {
        json!({ "$user": true })
    }

    #[test]
    fn derives_owner_and_collaborators_fields() {
        let mut def = table(&[
            ("owner", FieldType::String),
            (
                "editors",
                FieldType::Array {
                    element: Box::new(FieldType::String),
                },
            ),
        ]);
        def.owner_field = Some("owner".into());
        def.collaborators_field = Some("editors".into());
        let fields = principal_bearing_fields(&def);
        assert_eq!(fields.len(), 2);
        assert!(fields.contains(&PrincipalField {
            field: "owner".into(),
            kind: FieldKind::Scalar
        }));
        assert!(fields.contains(&PrincipalField {
            field: "editors".into(),
            kind: FieldKind::Array
        }));
    }

    #[test]
    fn walks_authorize_across_all_variants_including_not_and_in() {
        let mut def = table(&[
            ("uid", FieldType::String),
            (
                "members",
                FieldType::Array {
                    element: Box::new(FieldType::String),
                },
            ),
            ("count", FieldType::Number),
        ]);
        def.authorize = Some(FilterExpr::And {
            exprs: vec![
                FilterExpr::Or {
                    exprs: vec![
                        FilterExpr::Eq {
                            field: "uid".into(),
                            value: user_marker(),
                        },
                        FilterExpr::Contains {
                            field: "members".into(),
                            value: user_marker(),
                        },
                    ],
                },
                FilterExpr::Not {
                    expr: Box::new(FilterExpr::In {
                        field: "uid".into(),
                        values: vec![json!("x"), user_marker()],
                    }),
                },
                FilterExpr::Neq {
                    field: "uid".into(),
                    value: user_marker(),
                },
            ],
        });
        // "uid" arrives from Eq/In/Neq — deduped; "members" from Contains.
        let fields = principal_bearing_fields(&def);
        assert_eq!(fields.len(), 2);
        assert!(
            fields
                .iter()
                .any(|f| f.field == "uid" && f.kind == FieldKind::Scalar)
        );
        assert!(
            fields
                .iter()
                .any(|f| f.field == "members" && f.kind == FieldKind::Array)
        );
    }

    #[test]
    fn skips_non_string_and_non_array_of_string_fields() {
        let mut def = table(&[
            ("count", FieldType::Number),
            (
                "flags",
                FieldType::Array {
                    element: Box::new(FieldType::Number),
                },
            ),
            ("uid", FieldType::String),
        ]);
        def.authorize = Some(FilterExpr::Eq {
            field: "count".into(),
            value: user_marker(),
        });
        def.owner_field = Some("flags".into()); // declared wrongly; must be skipped
        def.collaborators_field = Some("uid".into()); // declared wrongly; scalar field
        let fields = principal_bearing_fields(&def);
        // "flags" (array of number) skipped; "uid" as collaboratorsField on a
        // scalar string field degrades to Scalar (a scalar swap is the only
        // sound rewrite); "count" skipped.
        assert!(
            fields
                .iter()
                .any(|f| f.field == "uid" && f.kind == FieldKind::Scalar)
        );
        assert!(!fields.iter().any(|f| f.field == "count"));
        assert!(!fields.iter().any(|f| f.field == "flags"));
    }

    #[test]
    fn rewrite_swaps_scalar_and_array_elements_only_for_anon() {
        let fields = vec![
            PrincipalField {
                field: "owner".into(),
                kind: FieldKind::Scalar,
            },
            PrincipalField {
                field: "editors".into(),
                kind: FieldKind::Array,
            },
        ];
        let mut doc = serde_json::Map::new();
        doc.insert("owner".into(), json!("user_anon"));
        doc.insert("editors".into(), json!(["user_other", "user_anon"]));
        doc.insert("title".into(), json!("user_anon")); // not principal-bearing: untouched
        let changed = rewrite_doc(&mut doc, &fields, "user_anon", "user_real");
        assert!(changed);
        assert_eq!(doc["owner"], json!("user_real"));
        assert_eq!(doc["editors"], json!(["user_other", "user_real"]));
        assert_eq!(doc["title"], json!("user_anon"));
    }

    #[test]
    fn rewrite_dedupes_real_already_present_and_reports_no_change() {
        let fields = vec![PrincipalField {
            field: "editors".into(),
            kind: FieldKind::Array,
        }];
        let mut doc = serde_json::Map::new();
        doc.insert("editors".into(), json!(["user_real", "user_anon"]));
        assert!(rewrite_doc(&mut doc, &fields, "user_anon", "user_real"));
        assert_eq!(doc["editors"], json!(["user_real"]));

        let mut untouched = serde_json::Map::new();
        untouched.insert("editors".into(), json!(["user_other"]));
        assert!(!rewrite_doc(
            &mut untouched,
            &fields,
            "user_anon",
            "user_real"
        ));
    }
}
