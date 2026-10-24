//! Server-side lookup expansion — the id/id-array join on the read DSL.
//!
//! Validation runs in the compile cascade (`compile_query`), so the error
//! mapping fires identically for HTTP one-shots, the WS initial push, /explain,
//! and every subscription re-run. Expansion runs inside `execute_query`
//! immediately before the projection seam, so joined keys are part of the
//! canonical a subscription diffs against: a child write that changes joined
//! output changes the canonical and pushes; a re-run where nothing visible
//! changed diffs equal and pushes nothing.
//!
//! Authorization on joined rows (D1 — omit, don't error): the batched fetch
//! appends the exact predicates the read path applies to the parent — the
//! `ownerField`/`collaboratorsField` fragment (`row_auth_predicate_body`) and
//! the schema `authorize` fragment (`authorize_predicate_body`), principal
//! markers resolved from the same `PrincipalCtx` — plus soft-delete visibility,
//! so an invisible row is simply never fetched (null / dropped from the array),
//! never an error that would freeze a live query.

use std::collections::{HashMap, HashSet};

use sqlx::PgPool;

use super::row_auth::{authorize_predicate_body, row_auth_enforced_uid, row_auth_predicate_body};
use super::terminals::merge_doc;
use crate::auth::PrincipalCtx;
use crate::dsl::{EqBind, Query, QueryResult};
use crate::error::RtDbError;
use crate::schema::{FieldType, SchemaDef, SchemaDefExt, TableDef};

/// At most 4 lookups per query; a fifth is `BadRequest` (resource-cap style).
const MAX_LOOKUPS: usize = 4;

/// Hard max of a `LookupSpec.cap` value; above is `BadRequest` at compile time
/// (D3 — the caller made a static mistake; clamping would hide it).
const MAX_LOOKUP_CAP: u32 = 1000;

/// Default per-parent joined-row cap for an id-ARRAY lookup (D2 — truncate).
const DEFAULT_LOOKUP_CAP: u32 = 100;

/// Fixed ceiling on total joined rows materialized per query execution; on
/// exceed, truncation proceeds in parent-result order (deterministic). Worst
/// case ~2x `MAX_TAKE`, consistent with `MAX_TAKE`'s per-query memory-ceiling
/// role (`super::MAX_TAKE`).
const MAX_JOINED_ROWS: usize = 8192;

/// Compile-time lookup validation, invoked from `compile_query`. Returns the
/// lookup aliases so `validate_projection` can admit them into a `fields` list.
///
/// Error mapping (per the design spec's table): unknown lookup `table` →
/// `NOT_FOUND`; `field` not declared, or not `Id`/`Optional<Id>`/`Array<Id>` →
/// `BAD_REQUEST`; declared `Id.table` ≠ lookup `table` → `BAD_REQUEST`; `as`
/// collides with a declared field, a `_`-prefixed name, or another lookup's
/// alias → `BAD_REQUEST`; `cap` > 1000 → `BAD_REQUEST`; > 4 lookups →
/// `BAD_REQUEST`. The doc-less terminals are rejected by the
/// `lookups-excludes-*` combination rules, not here.
pub(crate) fn validate_lookups(
    schema: &SchemaDef,
    table_def: &TableDef,
    q: &Query,
) -> Result<Vec<String>, RtDbError> {
    if q.lookups.is_empty() {
        return Ok(Vec::new());
    }
    if q.lookups.len() > MAX_LOOKUPS {
        return Err(RtDbError::bad_request(format!(
            "at most {MAX_LOOKUPS} lookups per query"
        )));
    }
    let mut aliases: Vec<String> = Vec::new();
    for lk in &q.lookups {
        let child = schema.table(&lk.table)?;
        let _ = child;
        let ty = table_def.fields.get(&lk.field).ok_or_else(|| {
            RtDbError::bad_request(format!(
                "lookup field '{}' is not declared on table '{}'",
                lk.field, q.table
            ))
        })?;
        let declared_child_table = match ty {
            FieldType::Id { table, .. } => Some(table),
            FieldType::Optional { inner } => match inner.as_ref() {
                FieldType::Id { table, .. } => Some(table),
                _ => None,
            },
            FieldType::Array { element } => match element.as_ref() {
                FieldType::Id { table, .. } => Some(table),
                _ => None,
            },
            _ => None,
        };
        let Some(declared) = declared_child_table else {
            return Err(RtDbError::bad_request(format!(
                "lookup field '{}' must be declared Id, Optional<Id>, or Array<Id>",
                lk.field
            )));
        };
        if declared != &lk.table {
            return Err(RtDbError::bad_request(format!(
                "lookup field '{}' declares Id table '{}' but the lookup names '{}'",
                lk.field, declared, lk.table
            )));
        }
        if lk.r#as.starts_with('_') {
            return Err(RtDbError::bad_request(format!(
                "lookup alias '{}' must not start with '_'",
                lk.r#as
            )));
        }
        if table_def.fields.contains_key(&lk.r#as) {
            return Err(RtDbError::bad_request(format!(
                "lookup alias '{}' collides with a declared field",
                lk.r#as
            )));
        }
        if aliases.iter().any(|a| a == &lk.r#as) {
            return Err(RtDbError::bad_request(format!(
                "lookup alias '{}' collides with another lookup alias",
                lk.r#as
            )));
        }
        if let Some(cap) = lk.cap
            && cap > MAX_LOOKUP_CAP
        {
            return Err(RtDbError::bad_request(format!(
                "lookup cap exceeds maximum of {MAX_LOOKUP_CAP}"
            )));
        }
        aliases.push(lk.r#as.clone());
    }
    Ok(aliases)
}

/// Whether the declared field type makes the lookup array-form (`Array<Id>`)
/// rather than single-form (`Id` / `Optional<Id>`). Validation has already
/// pinned the field to one of the three accepted shapes.
fn is_array_lookup(ty: &FieldType) -> bool {
    matches!(ty, FieldType::Array { .. })
}

/// Execute-time expansion. Mutates the doc-bearing `QueryResult` variants in
/// place, attaching each lookup's joined rows under the `as` key. Doc-less
/// terminals never reach here (the `lookups-excludes-*` combination rules
/// reject them at compile time). Parent docs are visited in result order and
/// lookups in declared order, so the `MAX_JOINED_ROWS` ceiling truncates
/// deterministically.
pub(crate) async fn expand_lookups(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    result: &mut QueryResult,
    q: &Query,
    ctx: &PrincipalCtx,
    include_deleted: bool,
) -> Result<(), RtDbError> {
    if q.lookups.is_empty() {
        return Ok(());
    }
    let owner = ctx.user_id.as_deref();
    let mut budget = MAX_JOINED_ROWS;

    for lk in &q.lookups {
        let child_def = schema.table(&lk.table)?;
        let parent_ty = schema
            .tables
            .get(&q.table)
            .and_then(|td| td.fields.get(&lk.field))
            .ok_or_else(|| {
                RtDbError::internal(format!(
                    "lookup field '{}' vanished from the schema between compile and execute",
                    lk.field
                ))
            })?;
        let array_form = is_array_lookup(parent_ty);

        // Collect the distinct ids across the result set, first-seen order.
        let mut ids: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for doc in docs_of(result) {
            collect_field_ids(doc.get(&lk.field), &mut ids, &mut seen);
        }

        let fetched: HashMap<String, serde_json::Value> = if ids.is_empty() {
            HashMap::new()
        } else {
            fetch_joined_rows(
                &FetchCtx {
                    pool,
                    db,
                    ctx,
                    owner,
                    include_deleted,
                },
                child_def,
                &lk.table,
                &ids,
            )
            .await?
        };

        let cap = lk.cap.unwrap_or(DEFAULT_LOOKUP_CAP);
        for doc in docs_of_mut(result) {
            match doc.get(&lk.field) {
                Some(serde_json::Value::String(id)) => {
                    let joined = if budget > 0 {
                        fetched.get(id).cloned()
                    } else {
                        None
                    };
                    if joined.is_some() {
                        budget -= 1;
                    }
                    attach(
                        doc,
                        lk.r#as.clone(),
                        joined.unwrap_or(serde_json::Value::Null),
                    );
                }
                Some(serde_json::Value::Array(arr)) => {
                    let mut out: Vec<serde_json::Value> = Vec::new();
                    for id in arr {
                        if out.len() >= cap as usize || budget == 0 {
                            break;
                        }
                        if let Some(id) = id.as_str()
                            && let Some(joined) = fetched.get(id)
                        {
                            out.push(joined.clone());
                            budget -= 1;
                        }
                    }
                    attach(doc, lk.r#as.clone(), serde_json::Value::Array(out));
                }
                _ => {
                    // Absent/null field on the parent: single form reads as
                    // null; array form reads as an empty array (no ids).
                    let leaf = if array_form {
                        serde_json::Value::Array(Vec::new())
                    } else {
                        serde_json::Value::Null
                    };
                    attach(doc, lk.r#as.clone(), leaf);
                }
            }
        }
    }
    Ok(())
}

/// Pushes every id string in `value` (a bare id string or an id array) that is
/// not already in `seen` onto `ids`.
fn collect_field_ids(
    value: Option<&serde_json::Value>,
    ids: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    match value {
        Some(serde_json::Value::String(id)) => {
            if seen.insert(id.clone()) {
                ids.push(id.clone());
            }
        }
        Some(serde_json::Value::Array(arr)) => {
            for id in arr {
                if let Some(id) = id.as_str() {
                    let owned = id.to_string();
                    if seen.insert(owned.clone()) {
                        ids.push(owned);
                    }
                }
            }
        }
        _ => {}
    }
}

/// The shared execution context one batched lookup fetch needs, grouped to
/// keep `fetch_joined_rows` under the clippy arity cap.
struct FetchCtx<'a> {
    pool: &'a PgPool,
    db: &'a str,
    ctx: &'a PrincipalCtx,
    owner: Option<&'a str>,
    include_deleted: bool,
}

/// One batched fetch for one lookup: the child table's rows for `ids` with the
/// parent read path's exact authorization appended — owner/collaborator
/// (`row_auth_predicate_body`), the schema `authorize` predicate
/// (`authorize_predicate_body`), soft-delete visibility, and the host-policy
/// visibility fragment — every value `$n`-bound, never interpolated.
async fn fetch_joined_rows(
    fx: &FetchCtx<'_>,
    child_def: &TableDef,
    child_table: &str,
    ids: &[String],
) -> Result<HashMap<String, serde_json::Value>, RtDbError> {
    let db = fx.db;
    let ctx = fx.ctx;
    let owner = fx.owner;
    let include_deleted = fx.include_deleted;
    let pg_schema_name = crate::ddl::pg_schema(db);
    let table_ident = crate::ddl::pg_table(child_table);
    let mut sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = ANY($1)"
    );
    let mut binds: Vec<EqBind> = Vec::new();

    let mut conditions: Vec<String> = Vec::new();
    // Owner/collaborator predicate, same single-bind shape the parent scan uses.
    let uid = row_auth_enforced_uid(
        child_def.owner_field.as_deref(),
        child_def.collaborators_field.as_deref(),
        owner,
    );
    if let Some(uid) = uid {
        conditions.push(row_auth_predicate_body(
            child_def.owner_field.as_deref(),
            child_def.collaborators_field.as_deref(),
            2,
        ));
        binds.push(EqBind::Text(uid.to_string()));
    }
    // `authorize` predicate — principal markers resolved against the same ctx,
    // compiled with the same `$n`-bind discipline as the parent's fragment.
    // NOTE the parent's convention: `start_pos` is an OFFSET over the shared
    // bind accumulator (first placeholder = start_pos + binds.len()), so it
    // stays 2 (after the ids' `$1`) for every fragment below no matter how
    // many binds accumulate.
    if let Some(frag) = authorize_predicate_body(child_def, ctx, 2, &mut binds)? {
        conditions.push(frag);
    }
    // FM-33 soft delete: a stamped child row is invisible, exactly as to the
    // parent terminals. Bindless literal; does not shift placeholder numbering.
    if child_def.soft_delete && !include_deleted {
        conditions.push("\"deleted_at\" IS NULL".to_string());
    }
    // Host-policy visibility, the last fragment the parent scan appends.
    if let Some(frag) = crate::auth::host_policy::sql_visibility(child_table, ctx, 2, &mut binds)? {
        conditions.push(frag);
    }
    for cond in &conditions {
        sql.push_str(" AND ");
        sql.push_str(cond);
    }

    let mut query =
        sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(sqlx::AssertSqlSafe(sql));
    query = query.bind(ids.to_vec());
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    let rows = query.fetch_all(fx.pool).await?;
    let mut map = HashMap::with_capacity(rows.len());
    for (id, doc, created_at, version) in rows {
        let merged = merge_doc(id.clone(), doc, created_at, version)?;
        map.insert(id, merged);
    }
    Ok(map)
}

/// Inserts `key`/`value` into `doc` when it is a JSON object (result docs
/// always are; a non-object stored doc is not this pass's problem).
fn attach(doc: &mut serde_json::Value, key: String, value: serde_json::Value) {
    if let serde_json::Value::Object(map) = doc {
        map.insert(key, value);
    }
}

fn docs_of(result: &QueryResult) -> Vec<&serde_json::Value> {
    match result {
        QueryResult::Doc(Some(doc)) => vec![doc],
        QueryResult::Docs(docs) => docs.iter().collect(),
        QueryResult::Paginated(page) => page.docs.iter().collect(),
        _ => Vec::new(),
    }
}

fn docs_of_mut(result: &mut QueryResult) -> Vec<&mut serde_json::Value> {
    match result {
        QueryResult::Doc(Some(doc)) => vec![doc],
        QueryResult::Docs(docs) => docs.iter_mut().collect(),
        QueryResult::Paginated(page) => page.docs.iter_mut().collect(),
        _ => Vec::new(),
    }
}
