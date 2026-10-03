//! Write-path document stamping: owner, authorize, TTL default, updatedAt, auto-increment, declared defaults, and computed fields (ARC-008 split; pure move from the former single-file `txn.rs`).

use sqlx::PgConnection;

use crate::auth::PrincipalCtx;
use crate::ddl::pg_sequence;
use crate::dsl::{FilterExpr, filter_matches};
use crate::error::RtDbError;
use crate::schema::{FieldType, TableDef};
use crate::value_expr::eval_value_expr;

// ARC-202: the wire types this module used to define live in `dsl.rs` now;
// re-exported so every `crate::txn::` path (and the integration tests'
// `rtdb_server::txn::` paths) keep resolving unchanged.
/// Forces `doc[owner_field] = owner` for owner-gated tables when the caller is
/// a user, overwriting any client-supplied value. Bypass callers and
/// non-owner tables leave `doc` unchanged.
pub(crate) fn stamp_owner(
    table_def: &TableDef,
    mut doc: serde_json::Map<String, serde_json::Value>,
    owner: Option<&str>,
) -> serde_json::Map<String, serde_json::Value> {
    if let (Some(field), Some(uid)) = (&table_def.owner_field, owner) {
        doc.insert(field.clone(), serde_json::Value::String(uid.to_string()));
    }
    doc
}

/// Walks `expr` collecting the `field` of every `Eq { field, value: {"$user": true} }`
/// leaf reachable through `And`/`Or`. `Not` is intentionally NOT descended: a
/// negated equality (`Not(Eq{owner,$user})`) is a prohibition, not a stampable
/// ownership, and stamping it would invert the predicate's meaning. `Contains`,
/// `Exists`, `In`, the comparison operators, `Neq`, `{"$email":true}`, and any
/// non-marker `Eq` value contribute nothing — only an exact `Eq{field,$user}`
/// asserts "this field IS the caller" and is therefore stampable. De-dups
/// preserving first occurrence. Empty when `expr` has no stampable leaf, in
/// which case `stamp_authorize` is a no-op and the inserted doc must satisfy
/// the predicate from client values alone (else `verify_authorize_doc`
/// rejects with `Forbidden`).
fn user_eq_fields(expr: &FilterExpr) -> Vec<String> {
    /// `true` only for the exact principal marker `{"$user": true}` — the sole
    /// value form that makes an `Eq` leaf stampable. Mirrors `resolve_value`'s
    /// marker test in `query.rs` so the two paths agree on what `$user` is.
    fn is_user_marker(v: &serde_json::Value) -> bool {
        if let serde_json::Value::Object(map) = v
            && map.len() == 1
        {
            return map.get("$user").and_then(|x| x.as_bool()) == Some(true);
        }
        false
    }
    fn walk(expr: &FilterExpr, out: &mut Vec<String>) {
        match expr {
            FilterExpr::Eq { field, value } if is_user_marker(value) => {
                if !out.iter().any(|f| f == field) {
                    out.push(field.clone());
                }
            }
            FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
                for e in exprs {
                    walk(e, out);
                }
            }
            // `Not` is NOT descended — see the doc comment. Every other leaf
            // variant is non-stampable by construction.
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(expr, &mut out);
    out
}

/// For each `Eq { field, value: {"$user": true} }` leaf reachable through
/// `And`/`Or` in `table.authorize`, force `doc[field] = ctx.user_id`,
/// overwriting any client value — unforgeable, exactly like `stamp_owner`.
/// `Not`, `Contains`, `Exists`, and non-`$user` leaves are not stampable; a
/// table without `authorize` or a bypass caller (`user_id = None`) is a no-op.
/// Call AFTER `stamp_owner`; then call `verify_authorize_doc` to reject
/// predicates the stamp could not satisfy (no `$user` leaf, or an unsatisfied
/// `And`/literal branch).
pub(crate) fn stamp_authorize(
    table_def: &TableDef,
    mut doc: serde_json::Map<String, serde_json::Value>,
    ctx: &PrincipalCtx,
) -> serde_json::Map<String, serde_json::Value> {
    if let (Some(expr), Some(uid)) = (&table_def.authorize, &ctx.user_id) {
        for field in user_eq_fields(expr) {
            doc.insert(field, serde_json::Value::String(uid.clone()));
        }
    }
    doc
}

/// Post-write verification for every write path on an `authorize`-gated table:
/// a user caller must leave the doc satisfying the predicate. The stamp
/// satisfies every `Eq{field,$user}` leaf, but a predicate with no stampable
/// leaf (e.g. `Eq{visibility,"public"}` or `Contains{editors,$user}`) stamps
/// nothing — the client must satisfy it from supplied values, else `Forbidden`.
/// On Patch/Replace/Upsert-update the stamp re-stamps `$user` leaves (parity
/// with `stamp_owner`), and this verify catches the residual cases (e.g. patching
/// a no-`$user`-arm `Eq{visibility,"public"}` to `"private"`). Bypass callers
/// (`user_id = None`) and tables without `authorize` are no-ops. Runs inside the
/// serialized txn so a `Forbidden` rolls back the whole transaction (same
/// atomicity guarantee as the patch/replace/delete pre-check).
pub(crate) fn verify_authorize_doc(
    table_def: &TableDef,
    doc: &serde_json::Map<String, serde_json::Value>,
    ctx: &PrincipalCtx,
) -> Result<(), RtDbError> {
    if let Some(authorize) = &table_def.authorize
        && ctx.user_id.is_some()
        && !filter_matches(&serde_json::Value::Object(doc.clone()), authorize, ctx)
    {
        return Err(RtDbError::forbidden(
            "write conflicts with the table's authorize predicate",
        ));
    }
    Ok(())
}

/// Stamps the TTL field at insert time when the table declares a
/// `default_duration_ms` and the document omits the field. Both insert paths
/// stamp — the `Insert` step and upsert's insert branch (a doc born via
/// upsert is born at insert time; the engines' shared insert paths agree).
/// After this, the TTL field is ordinary (patch/replace manipulate it
/// normally). See `docs/superpowers/specs/2026-08-01-document-ttl-design.md`.
pub(crate) fn stamp_ttl_default(
    table_def: &TableDef,
    mut doc: serde_json::Map<String, serde_json::Value>,
    now: i64,
) -> serde_json::Map<String, serde_json::Value> {
    if let Some(ttl) = &table_def.ttl
        && let Some(duration) = ttl.default_duration_ms
        && !doc.contains_key(&ttl.field)
    {
        doc.insert(ttl.field.clone(), serde_json::Value::from(now + duration));
    }
    doc
}

/// Stamps the table's `updatedAtField` (FM-36) with the current epoch-ms,
/// overwriting any client-supplied value — the same authority model as
/// `stamp_owner`. Runs on every version-bumping write path: insert, patch,
/// replace, upsert (both branches), patchByQuery, and cascade setNull. The
/// value matches the field's wire convention (`validate_value` /
/// `scalar_bind`): a JSON number on `number`, a decimal string on `int64`.
/// Snapshot replay (`insert_snapshot_row`) is not a step path and preserves
/// the stored value verbatim — import never re-stamps.
pub(crate) fn stamp_updated_at(
    table_def: &TableDef,
    mut doc: serde_json::Map<String, serde_json::Value>,
    now: i64,
) -> serde_json::Map<String, serde_json::Value> {
    if let Some(field) = &table_def.updated_at_field {
        let value = match table_def.fields.get(field) {
            Some(FieldType::Int64) => serde_json::Value::String(now.to_string()),
            _ => serde_json::Value::from(now),
        };
        doc.insert(field.clone(), value);
    }
    doc
}

/// Stamps the table's `autoIncrementField` with the next value of the table's
/// Postgres sequence, overwriting any client-supplied value — the same
/// authority model as `stamp_updated_at`. Runs on the two insert paths only
/// (`Insert` and upsert's insert branch); after insert the field is
/// immutable (`apply_patch` / `do_replace` reject changes). The value is a
/// decimal string, matching the int64 wire convention (`validate_value` /
/// `scalar_bind`). `nextval` is non-transactional: a rolled-back txn still
/// consumes its number, so sequences are monotonic but not gap-free.
/// Snapshot replay (`insert_snapshot_row`) is not a step path and preserves
/// the stored value verbatim — import never re-stamps.
pub(crate) async fn stamp_auto_increment(
    conn: &mut PgConnection,
    pg_schema_name: &str,
    table_def: &TableDef,
    table_name: &str,
    mut doc: serde_json::Map<String, serde_json::Value>,
) -> Result<serde_json::Map<String, serde_json::Value>, RtDbError> {
    if let Some(field) = &table_def.auto_increment_field {
        let seq_ident = pg_sequence(table_name);
        // `nextval` takes the sequence name as a regclass — a string literal
        // of the double-quoted ident (a bare quoted identifier would be read
        // as a table reference).
        let next: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT nextval('\"{pg_schema_name}\".\"{seq_ident}\"'::regclass)"
        )))
        .fetch_one(&mut *conn)
        .await?;
        doc.insert(field.clone(), serde_json::Value::String(next.to_string()));
    }
    Ok(doc)
}

/// Applies the table's push-time-validated `defaults` (FM-32) to a NEW
/// document: every key the doc omits is stamped from the schema. Runs after
/// `stamp_ttl_default` (a ttl default on the same field wins) and before the
/// owner/authorize stamps (server-stamped principal values win). Callers are
/// exactly the new-document paths — insert, replace, upsert-insert; `patch`
/// (and upsert-update / patchByQuery) never re-apply, so clearing an optional
/// field stays cleared.
pub(crate) fn apply_defaults(
    table_def: &TableDef,
    mut doc: serde_json::Map<String, serde_json::Value>,
) -> serde_json::Map<String, serde_json::Value> {
    for (field, value) in &table_def.defaults {
        if !doc.contains_key(field) {
            doc.insert(field.clone(), value.clone());
        }
    }
    doc
}

/// Stamps the table's computed fields (ENH-028): every `computed` entry is
/// re-evaluated against the final doc and stored — a null result REMOVES the
/// key (an unset optional field is an absent key, `strip_unset_optionals`'
/// shape convention) and a non-null result overwrites whatever is there (the
/// ownerField authority model: client-supplied values never survive). An
/// evaluation error fails the whole write as `bad_request`, naming the field.
/// Runs last in the stamp chain — after ttl default, updatedAt, defaults,
/// owner/authorize, and autoIncrement, so expressions see final inputs
/// (including a freshly stamped updatedAt) — and before `validate_doc` at
/// every site: `apply_patch` (patch, upsert's update branch, patchByQuery,
/// and cascade setNull via `do_patch`), `do_insert` (insert + upsert's insert
/// branch), and `do_replace`. `handle_merge_users` (committer.rs) bypasses
/// the `do_*` functions and calls this directly on the rewritten doc, so a
/// computed expr over a principal-bearing field sees the rewritten uid. A
/// `Case` predicate would need a principal, but push validation rejects
/// principal markers inside computed expressions, so the bypass ctx is
/// semantically irrelevant. Snapshot replay (`insert_snapshot_row`) is not a
/// step path and preserves stored values verbatim — import never re-stamps.
pub(crate) fn stamp_computed(
    table_def: &TableDef,
    mut doc: serde_json::Map<String, serde_json::Value>,
    now: i64,
) -> Result<serde_json::Map<String, serde_json::Value>, RtDbError> {
    for (name, expr) in &table_def.computed {
        let value = eval_value_expr(expr, &doc, now, &PrincipalCtx::bypass()).map_err(|e| {
            RtDbError::bad_request(format!("computed field '{name}': {}", e.message))
        })?;
        if value.is_null() {
            doc.remove(name);
        } else {
            doc.insert(name.clone(), value);
        }
    }
    Ok(doc)
}
