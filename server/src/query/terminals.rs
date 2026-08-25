//! Terminal compilers + the compile/execute routing for the btree-window
//! terminals (`get`/`count`/`distinct`/`aggregate`/`paginate`/`collect`/
//! `unique`/`first`) — ARC-203 mechanical split of the former `query.rs`.
//! SQL text is built here per terminal; typed binds are `EqBind`s so the
//! execute tails share one bind loop. Keep the compile path and the execute
//! path rejecting the same shapes for the same reasons (see `compile_query`).

use sqlx::PgPool;

use super::collect_filter_warnings;
use super::filter::compile_filter;
use super::row_auth::{
    authorize_predicate_body, owner_filter, row_auth_enforced_uid, row_auth_predicate_body,
};
use super::search::{
    CompileSearchCtx, SearchCtx, compile_hybrid_search, compile_search, compile_vector_search,
};
use super::{MAX_TAKE, check_query_combinations, validate_projection};
use crate::auth::{PrincipalCtx, authorize_table};
use crate::db::validate_db_name;
use crate::ddl::{pg_col, pg_schema, pg_table};
use crate::dsl::{
    AggregateGroup, AggregateOp, AggregateSpec, EqBind, Order, Paginate, PaginatedResult, Query,
    QueryResult, eq_bind_for, eq_binds, filter_matches, row_visible_to,
};
use crate::error::RtDbError;
use crate::pagination::{decode_cursor, encode_cursor};
use crate::schema::{FieldType, IndexDef, SchemaDef, SchemaDefExt, TableDef, TableDefExt};

/// Result docs = stored doc merged with {"_id", "_creationTime", "_version"}.
/// get: point SELECT, null if missing. unique: error PreconditionFailed "unique query matched
/// multiple documents" if >1 row, null if 0. eq len may be a PREFIX of index fields (0..=all),
/// each typed like Task 5. Sort: unbound index fields in index order, then created_at, then id —
/// all in `order` direction. No index => eq must be empty, sort by (created_at, id).
/// `gt`/`gte`/`lt`/`lte` add an optional inequality bound on the single index field immediately
/// after the `eq` prefix (`index.fields[eq.len()]`): at most one of `gt`/`gte` and at most one of
/// `lt`/`lte` may be set, both may be set together for a bounded range, and the bound value is
/// typed via the same `eq_binds`/`eq_bind_for` conversion `txn.rs` uses for `eq`. A range bound
/// requires an index and a remaining (unconsumed by `eq`) index field -> BadRequest otherwise.
/// `first` is sugar over `take(1)`: applies the same eq/range/order filters with LIMIT 1 and
/// returns `Doc(Some)` (or `Doc(None)` if nothing matched) instead of `Docs`; mutually exclusive
/// with `take` and `unique`.
/// `count` is a terminal that runs `SELECT COUNT(*)` over the same eq/range WHERE clause as every
/// other terminal (no index required, same as collect), skipping ORDER BY/LIMIT entirely and
/// returning `Count(n)` uncapped by `MAX_TAKE`; mutually exclusive with `get`, `take`, `unique`,
/// `first`, and `order` (a count has no rows to order).
/// `distinct` is a terminal that runs `SELECT DISTINCT` over the index field immediately after the
/// `eq` prefix (`index.fields[eq.len()]`) using the same eq/range WHERE clause every other terminal
/// builds, returning `Distinct(values)` — a JSON array of those scalar values, ordered ascending
/// for deterministic output and capped by `MAX_TAKE`. Useful for autocomplete/facet UIs. Requires
/// both an `index` and an index field beyond the eq prefix → BadRequest otherwise. Mutually
/// exclusive with every other terminal except `eq`/range bounds/`filter` (which narrow the
/// matching set the distinct values are drawn from).
/// `aggregate` is a terminal that runs `SUM`/`AVG`/`MIN`/`MAX` over the index field immediately
/// after the `eq` prefix using the same eq/range WHERE clause, returning `Aggregate(value)` — a
/// bare scalar (null if no rows match). With `groupBy: true`, it groups by `index.fields[eq.len()]`
/// and aggregates `index.fields[eq.len()+1]`, returning `AggregateGroups([{key,value},…])`
/// ordered by group key and capped by `MAX_TAKE`. `sum`/`avg` require a numeric aggregate field
/// (only `number` is numeric among indexable types) → BadRequest otherwise. Requires an index AND
/// a field beyond the eq prefix (TWO fields beyond for `groupBy`) → BadRequest otherwise. Mutually
/// exclusive with every other terminal except `eq`/range bounds/`filter` (which narrow the
/// matching set); `take` is also rejected — group count is capped internally by `MAX_TAKE`.
/// Unknown table -> NotFound; unknown index / eq too long / get+query mix / unique+take /
/// first+take / first+unique / count+take / count+unique / count+first / count+order -> BadRequest.
/// `take: 0` is valid and returns an empty `Docs([])`, not an error.
/// `unique` without an `index` scans the whole table (LIMIT 2) and applies the same 0/1/>1 rule.
/// Compile a `Query` into a [`CompiledQuery`] — the SQL string and ordered
/// typed binds the server will execute — plus a list of compile-time warnings
/// (currently: filter on a declared-but-unindexed field). This is the single
/// compile chokepoint for the read path: `execute_query` calls it then binds +
/// fetches the result, and `POST /admin/db/{db}/explain` calls it to show the
/// operator exactly the SQL the server would run (the "no second compiler that
/// drifts" guarantee). Pure — takes the same args as `execute_query` MINUS the
/// `pool`, performs no I/O, and is not async.
///
/// Mirrors `execute_query`'s terminal routing exactly: validate_db_name +
/// authorize_table + table_def resolve → early returns for get/search/vector/
/// hybrid (their compile fns) → compile_query_window → terminal-specific compile
/// (count/distinct/aggregate/paginate/collect). The cascade-order invariants
/// (peer rejection, take cap, range-bound mutual exclusion) are re-checked here
/// so a request that would fail at execute time fails at compile time too — the
/// compile path and the execute path reject the same shapes for the same
/// reasons.
pub fn compile_query(
    db: &str,
    schema: &SchemaDef,
    q: &Query,
    ctx: &PrincipalCtx,
    include_deleted: bool,
) -> Result<(CompiledQuery, Vec<String>), RtDbError> {
    let warnings = collect_filter_warnings(schema, q);
    validate_db_name(db)?;
    authorize_table(ctx, &q.table)?;
    let owner = ctx.user_id.as_deref();
    let table_def = schema.table(&q.table)?;
    // Projection validation runs before every early return so all terminals
    // (including `get`) reject unknown field names at compile time — the same
    // shapes fail for the same reasons on the /explain path.
    if let Some(fields) = &q.fields {
        validate_projection(table_def, fields)?;
    }
    let owner_field = table_def.owner_field.as_deref();
    let collaborators_field = table_def.collaborators_field.as_deref();
    let sctx = CompileSearchCtx {
        db,
        table_def,
        table_name: q.table.as_str(),
        owner_field,
        collaborators_field,
        ctx,
        include_deleted,
    };
    let _ = owner;

    // ENH-028 phase 2: one table-driven check replaces the old per-terminal
    // peer-rejection cascade (see `check_query_combinations` in `mod.rs`).
    // `gt`+`gte` and `lt`+`lte` mutual exclusion are rules in the same table
    // (`gt-excludes-gte`/`lt-excludes-lte`), so those no longer need a
    // separate inline check.
    check_query_combinations(q)?;
    if let Some(take) = q.take
        && take > MAX_TAKE
    {
        return Err(RtDbError::bad_request(format!(
            "take exceeds maximum of {MAX_TAKE}"
        )));
    }

    if let Some(id) = &q.get {
        return Ok((compile_point_read(&sctx, id)?, warnings));
    }

    if let Some(vs) = &q.vector_search {
        return Ok((compile_vector_search(&sctx, vs)?, warnings));
    }
    if let Some(hs) = &q.hybrid_search {
        return Ok((compile_hybrid_search(&sctx, hs)?, warnings));
    }
    if let Some(search) = &q.search {
        return Ok((compile_search(&sctx, search, q.take)?, warnings));
    }

    let w = compile_query_window(
        table_def,
        q,
        ctx,
        owner,
        owner_field,
        collaborators_field,
        include_deleted,
    )?;

    if q.count {
        return Ok((compile_count_terminal(w, db, &q.table)?, warnings));
    }
    if q.distinct {
        return Ok((compile_distinct_terminal(w, db, &q.table)?, warnings));
    }
    if let Some(agg) = &q.aggregate {
        return Ok((
            compile_aggregate_terminal(w, table_def, agg, db, &q.table)?,
            warnings,
        ));
    }

    let mut sort_cols: Vec<String> = match w.index_def {
        Some(idx) => idx.fields[w.eq_len..]
            .iter()
            .map(|field_name| format!("\"{}\"", pg_col(field_name)))
            .collect(),
        None => Vec::new(),
    };
    sort_cols.push("\"created_at\"".to_string());
    sort_cols.push("\"id\"".to_string());

    let dir = match q.order {
        Some(Order::Desc) => "DESC",
        _ => "ASC",
    };
    let order_by = sort_cols
        .iter()
        .map(|col| format!("{col} {dir}"))
        .collect::<Vec<_>>()
        .join(", ");

    if let Some(paginate) = &q.paginate {
        let sort = SortSpec {
            sort_cols,
            dir,
            order_by: &order_by,
        };
        let (cq, _ctx) = compile_paginate_terminal(w, table_def, paginate, sort, db, &q.table)?;
        return Ok((cq, warnings));
    }

    Ok((
        compile_collect_terminal(w, q.unique, q.first, q.take, &order_by, db, &q.table)?,
        warnings,
    ))
}

/// A compiled read query: the SQL string (with `$n` placeholders) plus the
/// ordered typed binds to fill them, plus the wire terminal name for telemetry.
/// Produced by [`compile_query`] (and the per-terminal `compile_*_terminal`
/// helpers it dispatches to) and consumed by the matching `execute_*_terminal`
/// helper, which binds `binds` into a sqlx query in order and fetches. The
/// `/explain` admin route also consumes it — the SQL it shows the operator is
/// exactly the string this struct carries, never a second compiler's output.
///
/// `binds` is `Vec<EqBind>` rather than a broader enum because every value a
/// read path binds — eq/range binds, client filter binds, owner/collab uids,
/// `authorize` predicate binds, LIMIT/cursor values, and the search/vector
/// family's text queries, query vectors (as `[a,b,c]` text), and RRF `k` —
/// fits one of the four `EqBind` variants. Encoding them all uniformly is what
/// lets the execute tail be one shared bind loop per terminal.
#[derive(Debug, Clone)]
pub struct CompiledQuery {
    pub sql: String,
    pub binds: Vec<EqBind>,
    pub terminal: &'static str,
}

/// The compiled index window shared by every non-early-return terminal in
/// `execute_query`. Built once by `compile_query_window` and consumed by-value
/// on exactly one terminal's return path. The field set is precisely what the
/// inline terminals read — no speculative fields.
pub(crate) struct QueryWindow<'a> {
    pub(crate) index_def: Option<&'a IndexDef>,
    pub(crate) binds: Vec<EqBind>,
    pub(crate) eq_len: usize,
    pub(crate) range_binds: Vec<EqBind>,
    pub(crate) where_conditions: Vec<String>,
    pub(crate) filter_binds: Vec<EqBind>,
    pub(crate) limit_placeholder: usize,
}

/// Compiles the eq/range binds, the client `filter`, and the owner/collaborator/
/// `authorize` predicates into a `QueryWindow`. Verbatim lift of the block that
/// previously lived inline in `execute_query`; the locals it left behind
/// (`index_def`, `binds`, `eq_len`, `range_binds`, `where_conditions`,
/// `filter_binds`, `limit_placeholder`) became the returned struct's fields.
pub(crate) fn compile_query_window<'a>(
    table_def: &'a TableDef,
    q: &Query,
    ctx: &PrincipalCtx,
    owner: Option<&str>,
    owner_field: Option<&str>,
    collaborators_field: Option<&str>,
    include_deleted: bool,
) -> Result<QueryWindow<'a>, RtDbError> {
    let index_def: Option<&IndexDef> = match &q.index {
        Some(name) => Some(table_def.index(name)?),
        None => {
            if !q.eq.is_empty() {
                return Err(RtDbError::bad_request("eq requires an index"));
            }
            None
        }
    };

    let binds = match index_def {
        Some(idx) => eq_binds(table_def, idx, &q.eq)?,
        None => Vec::new(),
    };
    let eq_len = binds.len();

    let has_range_bound = q.gt.is_some() || q.gte.is_some() || q.lt.is_some() || q.lte.is_some();
    let range_field_name: Option<&str> = if has_range_bound {
        let idx =
            index_def.ok_or_else(|| RtDbError::bad_request("range bound requires an index"))?;
        if eq_len >= idx.fields.len() {
            return Err(RtDbError::bad_request(
                "range bound requires a remaining index field after eq",
            ));
        }
        Some(idx.fields[eq_len].as_str())
    } else {
        None
    };

    let mut range_where: Vec<String> = Vec::new();
    let mut range_binds: Vec<EqBind> = Vec::new();
    if let Some(field_name) = range_field_name {
        let field_type = table_def.fields.get(field_name).ok_or_else(|| {
            RtDbError::internal(format!("index references unknown field '{field_name}'"))
        })?;
        let col = pg_col(field_name);
        if let Some(v) = &q.gt {
            range_where.push(format!("\"{col}\" > ${}", eq_len + range_binds.len() + 1));
            range_binds.push(eq_bind_for(field_type, v)?);
        } else if let Some(v) = &q.gte {
            range_where.push(format!("\"{col}\" >= ${}", eq_len + range_binds.len() + 1));
            range_binds.push(eq_bind_for(field_type, v)?);
        }
        if let Some(v) = &q.lt {
            range_where.push(format!("\"{col}\" < ${}", eq_len + range_binds.len() + 1));
            range_binds.push(eq_bind_for(field_type, v)?);
        } else if let Some(v) = &q.lte {
            range_where.push(format!("\"{col}\" <= ${}", eq_len + range_binds.len() + 1));
            range_binds.push(eq_bind_for(field_type, v)?);
        }
    }
    let mut where_conditions: Vec<String> = match index_def {
        Some(idx) => idx.fields[..eq_len]
            .iter()
            .enumerate()
            .map(|(i, field_name)| format!("\"{}\" = ${}", pg_col(field_name), i + 1))
            .collect(),
        None => Vec::new(),
    };
    where_conditions.extend(range_where);

    // `filter` is an additional WHERE predicate composed after the eq/range
    // conditions. It binds after the eq+range binds, so the LIMIT and cursor
    // placeholder offsets below account for `filter_binds.len()`. When the
    // caller is an authenticated user and the table declares an `ownerField`
    // (and no `collaboratorsField`), `owner_filter` wraps the client filter
    // with a server-side equality predicate so the user sees only their own
    // rows; bypass callers (`None`) and tables without an `ownerField` get the
    // original filter back unchanged. When `collaboratorsField` is also
    // declared, the owner OR collaborator predicate is appended below as a
    // separate where_condition (FilterExpr has no jsonb-array membership leaf).
    let owner_only = owner_field.is_some() && collaborators_field.is_none();
    let effective_filter = if owner_only {
        owner_filter(q.filter.as_ref(), owner_field, owner)
    } else {
        q.filter.as_ref().cloned()
    };
    // Absolute 1-based position of the first client-filter bind; shared by the
    // client filter, the owner/collab uid bind, and the `authorize` predicate
    // fragment below. Each appends to `filter_binds`, so `start_pos +
    // binds.len()` (via `compile_filter_node` / `push_filter_bind`) keeps every
    // placeholder correctly numbered.
    let filter_start = eq_len + range_binds.len() + 1;
    let mut filter_binds: Vec<EqBind> = match &effective_filter {
        Some(filter) => {
            let (fragment, binds) = compile_filter(filter, table_def, filter_start, false)?;
            where_conditions.push(fragment);
            binds
        }
        None => Vec::new(),
    };
    // Owner OR collaborator predicate appended after the client filter. The
    // schema-validated identifiers are interpolated; the uid is `$n`-bound once
    // and reused on both sides of the OR. Same single-bind shape as owner-only.
    let row_auth_uid = row_auth_enforced_uid(owner_field, collaborators_field, owner);
    if let Some(uid) = row_auth_uid {
        let ph = filter_start + filter_binds.len();
        where_conditions.push(row_auth_predicate_body(
            owner_field,
            collaborators_field,
            ph,
        ));
        filter_binds.push(EqBind::Text(uid.to_string()));
    }
    // `authorize` predicate (model C): when the table declares `authorize` and
    // the caller is a user, AND the resolved predicate into the scan. Composes
    // with owner/collab above (both must pass) — a table may declare both.
    // Bypass callers (`user_id = None`) and tables without `authorize` get no
    // fragment. Shares `filter_binds` so placeholders stay correctly numbered.
    if let Some(frag) = authorize_predicate_body(table_def, ctx, filter_start, &mut filter_binds)? {
        where_conditions.push(frag);
    }
    // FM-33 soft delete: a soft-delete table hides its stamped rows from every
    // read terminal unless the caller passed `include_deleted` (admin docs
    // pass-through only). The literal is bindless, so appending it here does
    // not shift `filter_start`-based placeholder numbering.
    if table_def.soft_delete && !include_deleted {
        where_conditions.push("\"deleted_at\" IS NULL".to_string());
    }
    let limit_placeholder = filter_start + filter_binds.len();

    Ok(QueryWindow {
        index_def,
        binds,
        eq_len,
        range_binds,
        where_conditions,
        filter_binds,
        limit_placeholder,
    })
}

/// `count` terminal SQL compilation: `SELECT COUNT(*)` over the compiled
/// window's WHERE clause. The compile half of the former inline `if q.count`
/// block — SQL and bind-order are byte-for-byte identical to the pre-refactor
/// cascade; only the sqlx bind+fetch tail lives in `execute_count_terminal`.
fn compile_count_terminal(
    w: QueryWindow<'_>,
    db: &str,
    table: &str,
) -> Result<CompiledQuery, RtDbError> {
    let QueryWindow {
        binds,
        range_binds,
        where_conditions,
        filter_binds,
        ..
    } = w;
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table);
    let mut sql = format!("SELECT COUNT(*) FROM \"{pg_schema_name}\".\"{table_ident}\"");
    if !where_conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_conditions.join(" AND "));
    }
    let mut all = Vec::with_capacity(binds.len() + range_binds.len() + filter_binds.len());
    all.extend(binds);
    all.extend(range_binds);
    all.extend(filter_binds);
    Ok(CompiledQuery {
        sql,
        binds: all,
        terminal: "count",
    })
}

/// `count` terminal execute tail: bind `CompiledQuery.binds` into a scalar
/// `i64` query in order and fetch the count.
pub(crate) async fn execute_count_terminal(
    cq: CompiledQuery,
    pool: &PgPool,
) -> Result<QueryResult, RtDbError> {
    let CompiledQuery { sql, binds, .. } = cq;
    let mut query = sqlx::query_scalar::<_, i64>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    let count = query.fetch_one(pool).await?;
    Ok(QueryResult::Count(count))
}

/// Distinct terminal SQL compilation: `SELECT DISTINCT` of the index field
/// immediately after the eq prefix over the same eq/range WHERE clause every
/// other terminal builds. The combination cascade already rejected every other
/// terminal; `distinct` composes only with `index`/`eq`/range bounds. The
/// preconditions below reject the no-index and no-remaining-field cases with
/// the same BadRequest shape as a missing-index `eq` bind. Capped by `MAX_TAKE`
/// for parity with `collect` (a distinct set bounded by the matching row count).
/// Compile half of the former inline `if q.distinct { … }` block — SQL and
/// bind-order byte-for-byte identical to the pre-refactor cascade.
fn compile_distinct_terminal(
    w: QueryWindow<'_>,
    db: &str,
    table: &str,
) -> Result<CompiledQuery, RtDbError> {
    let QueryWindow {
        index_def,
        binds,
        eq_len,
        range_binds,
        where_conditions,
        filter_binds,
        limit_placeholder,
    } = w;
    let idx = index_def.ok_or_else(|| {
        RtDbError::bad_request("distinct requires an index field beyond the eq prefix")
    })?;
    if eq_len >= idx.fields.len() {
        return Err(RtDbError::bad_request(
            "distinct requires an index field beyond the eq prefix",
        ));
    }
    let field_name = idx.fields[eq_len].as_str();
    // The field's existence is guaranteed by the schema's index definition
    // (validated at schema push), so no extra lookup is needed here.
    let col = pg_col(field_name);
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table);
    // Project the column to jsonb so a single `serde_json::Value` decoder
    // handles text/number/boolean columns uniformly. The physical column
    // name and schema/table identifiers are schema-validated and double-
    // quoted; only the LIMIT is `$n`-bound.
    let mut sql = format!(
        "SELECT DISTINCT to_jsonb(\"{col}\") AS v FROM \"{pg_schema_name}\".\"{table_ident}\""
    );
    if !where_conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_conditions.join(" AND "));
    }
    sql.push_str(&format!(" ORDER BY v LIMIT ${limit_placeholder}"));
    let mut all = Vec::with_capacity(binds.len() + range_binds.len() + filter_binds.len() + 1);
    all.extend(binds);
    all.extend(range_binds);
    all.extend(filter_binds);
    all.push(EqBind::I64(i64::from(MAX_TAKE)));
    Ok(CompiledQuery {
        sql,
        binds: all,
        terminal: "distinct",
    })
}

/// Distinct terminal execute tail: bind + fetch, project rows to `Value`s.
pub(crate) async fn execute_distinct_terminal(
    cq: CompiledQuery,
    pool: &PgPool,
) -> Result<QueryResult, RtDbError> {
    let CompiledQuery { sql, binds, .. } = cq;
    // Rows missing an optional indexed field project to a SQL NULL cell
    // (`to_jsonb(NULL)` is SQL NULL), which sorts last under the ORDER BY
    // default — but sqlx cannot decode a NULL cell into `serde_json::Value`,
    // so decode as `Option` and surface those rows as JSON null.
    let mut query = sqlx::query_as::<_, (Option<serde_json::Value>,)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    let rows = query.fetch_all(pool).await?;
    let values: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(v,)| v.unwrap_or(serde_json::Value::Null))
        .collect();
    Ok(QueryResult::Distinct(values))
}

/// Aggregate terminal SQL compilation: runs `<OP>("<col>")` (SUM/AVG/MIN/MAX)
/// over the same eq/range WHERE clause every other terminal builds, returning
/// one scalar (`Aggregate(value)`). With `group_by: true`, it groups by the
/// index field after the eq prefix and aggregates the one after that, returning
/// `AggregateGroups([{key,value},…])`. The combination cascade already rejected
/// every other terminal; `aggregate` composes only with `index`/`eq`/range
/// bounds/`filter`. The preconditions below reject the no-index,
/// no-remaining-field, and (for sum/avg) non-numeric-field cases. Group count
/// is capped by `MAX_TAKE` for parity with `collect`. Compile half of the
/// former inline `if let Some(agg) = &q.aggregate { … }` block — SQL and
/// bind-order byte-for-byte identical to the pre-refactor cascade.
fn compile_aggregate_terminal(
    w: QueryWindow<'_>,
    table_def: &TableDef,
    agg: &AggregateSpec,
    db: &str,
    table: &str,
) -> Result<CompiledQuery, RtDbError> {
    let QueryWindow {
        index_def,
        binds,
        eq_len,
        range_binds,
        where_conditions,
        filter_binds,
        limit_placeholder,
    } = w;
    // Resolve the group column (groupBy) and the aggregate field. `count`
    // aggregates rows, not a field — it consumes no aggregate index field (a
    // scalar `count` needs no index at all; a grouped `count` needs one index
    // field beyond the eq prefix to group by). Every other op needs an aggregate
    // field: the one after the eq prefix (plain), or the one after the group
    // field (groupBy). The groupcol for groupBy is the same field `distinct` uses.
    let group_col: Option<String> = if agg.group_by {
        let idx = index_def.ok_or_else(|| {
            RtDbError::bad_request("aggregate groupBy requires an index field beyond the eq prefix")
        })?;
        let group_field = idx.fields.get(eq_len).ok_or_else(|| {
            RtDbError::bad_request("aggregate groupBy requires an index field beyond the eq prefix")
        })?;
        Some(pg_col(group_field))
    } else {
        None
    };
    let agg_field_name: Option<&str> = if !agg.op.needs_field() {
        None
    } else {
        let idx = index_def.ok_or_else(|| {
            RtDbError::bad_request("aggregate requires an index field beyond the eq prefix")
        })?;
        let agg_field = if agg.group_by {
            idx.fields.get(eq_len + 1).ok_or_else(|| {
                RtDbError::bad_request(
                    "aggregate groupBy requires two index fields beyond the eq prefix",
                )
            })?
        } else {
            idx.fields.get(eq_len).ok_or_else(|| {
                RtDbError::bad_request("aggregate requires an index field beyond the eq prefix")
            })?
        };
        Some(agg_field.as_str())
    };
    // Validate the aggregate field's schema type and sum/avg's numeric
    // requirement. count/min/max skip the numeric check; count has no field.
    if let Some(name) = agg_field_name {
        let agg_field_type = table_def.fields.get(name).ok_or_else(|| {
            RtDbError::internal(format!("index references unknown field '{name}'"))
        })?;
        if matches!(agg.op, AggregateOp::Sum | AggregateOp::Avg)
            && !is_numeric_index_field(agg_field_type)
        {
            return Err(RtDbError::bad_request(format!(
                "aggregate op {} requires a numeric index field",
                agg.op.sql_fn().to_lowercase()
            )));
        }
    }
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table);
    let op_sql = agg.op.sql_fn();
    // The aggregate expression: COUNT(*) for `count` (no column), else
    // OP("agg_col") over the resolved aggregate field.
    let agg_expr = match agg_field_name {
        Some(name) => {
            let agg_col = pg_col(name);
            format!("{op_sql}(\"{agg_col}\")")
        }
        None => "COUNT(*)".to_string(),
    };
    // Project via `to_jsonb` so a single `serde_json::Value` decoder handles
    // text/number/boolean columns uniformly, exactly like `distinct`. A
    // scalar SUM/AVG/MIN/MAX over zero matching rows yields one row with
    // SQL NULL → `serde_json::Value::Null`; COUNT(*) over zero rows yields 0.
    let filter = FilterBinds {
        binds,
        range_binds,
        where_conditions,
        filter_binds,
    };
    if let Some(group_col) = group_col {
        return compile_aggregate_grouped(
            group_col,
            agg_expr,
            filter,
            limit_placeholder,
            &pg_schema_name,
            &table_ident,
        );
    }
    compile_aggregate_scalar(agg_expr, filter, &pg_schema_name, &table_ident)
}

/// The eq/range/filter binds and their WHERE clause, carved out of
/// `QueryWindow` (dropping `index_def`/`eq_len`, which the aggregate
/// terminals resolve before compiling). Shared by `compile_aggregate_grouped`
/// and `compile_aggregate_scalar` so each stays under clippy's
/// argument-count threshold.
struct FilterBinds {
    binds: Vec<EqBind>,
    range_binds: Vec<EqBind>,
    where_conditions: Vec<String>,
    filter_binds: Vec<EqBind>,
}

/// Grouped aggregate SQL compilation:
/// `SELECT to_jsonb(group_col), COALESCE(to_jsonb(OP(agg_col)), 'null'::jsonb) … GROUP BY … ORDER BY k LIMIT $`.
/// The value COALESCE mirrors the scalar branch: a group whose aggregate
/// input is entirely NULL aggregates to SQL NULL, which must surface as JSON
/// null, not fail the decoder. The key is NOT COALESCEd — jsonb `'null'`
/// sorts before every other scalar, so COALESCEing it would flip the NULLS
/// LAST order `ORDER BY k` gives the SQL NULL group.
fn compile_aggregate_grouped(
    group_col: String,
    agg_expr: String,
    filter: FilterBinds,
    limit_placeholder: usize,
    pg_schema_name: &str,
    table_ident: &str,
) -> Result<CompiledQuery, RtDbError> {
    let FilterBinds {
        binds,
        range_binds,
        where_conditions,
        filter_binds,
    } = filter;
    let mut sql = format!(
        "SELECT to_jsonb(\"{group_col}\") AS k, COALESCE(to_jsonb({agg_expr}), 'null'::jsonb) AS v FROM \"{pg_schema_name}\".\"{table_ident}\""
    );
    if !where_conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_conditions.join(" AND "));
    }
    sql.push_str(&format!(
        " GROUP BY \"{group_col}\" ORDER BY k LIMIT ${limit_placeholder}"
    ));
    let mut all = Vec::with_capacity(binds.len() + range_binds.len() + filter_binds.len() + 1);
    all.extend(binds);
    all.extend(range_binds);
    all.extend(filter_binds);
    all.push(EqBind::I64(i64::from(MAX_TAKE)));
    Ok(CompiledQuery {
        sql,
        binds: all,
        terminal: "aggregate",
    })
}

/// Scalar (ungrouped) aggregate SQL compilation:
/// `SELECT COALESCE(to_jsonb(OP(agg_col)), 'null'::jsonb) …`.
/// Verbatim lift of the former scalar branch's SQL builder.
fn compile_aggregate_scalar(
    agg_expr: String,
    filter: FilterBinds,
    pg_schema_name: &str,
    table_ident: &str,
) -> Result<CompiledQuery, RtDbError> {
    let FilterBinds {
        binds,
        range_binds,
        where_conditions,
        filter_binds,
    } = filter;
    let mut sql = format!(
        "SELECT COALESCE(to_jsonb({agg_expr}), 'null'::jsonb) AS v FROM \"{pg_schema_name}\".\"{table_ident}\""
    );
    if !where_conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_conditions.join(" AND "));
    }
    let mut all = Vec::with_capacity(binds.len() + range_binds.len() + filter_binds.len());
    all.extend(binds);
    all.extend(range_binds);
    all.extend(filter_binds);
    Ok(CompiledQuery {
        sql,
        binds: all,
        terminal: "aggregate",
    })
}

/// Aggregate terminal execute tail: branch on the `terminal` tag, bind, and
/// fetch. The group vs scalar shape difference is encoded by which `query_as`
/// decoder is used; `terminal` disambiguates which to call.
pub(crate) async fn execute_aggregate_terminal(
    cq: CompiledQuery,
    pool: &PgPool,
) -> Result<QueryResult, RtDbError> {
    let CompiledQuery {
        sql,
        binds,
        terminal,
    } = cq;
    // The grouped path is tagged `aggregate` and emits a 2-column row shape
    // (k, v); the scalar path is also tagged `aggregate` but emits 1 column.
    // The compiled SQL itself encodes the shape (its SELECT list), so dispatch
    // on the SQL's projection rather than the tag. Both paths carry the same
    // tag because the wire terminal is the same; the executor reads the SQL.
    if sql.contains("GROUP BY") {
        // Rows missing the group field form one SQL NULL group, which sorts
        // last under `ORDER BY k`'s NULLS LAST default — but sqlx cannot
        // decode a NULL cell into `serde_json::Value`, so decode the key as
        // `Option` and surface that group's key as JSON null (the value is
        // COALESCEd in the SQL; see compile_aggregate_grouped).
        let mut query = sqlx::query_as::<_, (Option<serde_json::Value>, serde_json::Value)>(&sql);
        for bind in binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        let rows = query.fetch_all(pool).await?;
        let groups: Vec<AggregateGroup> = rows
            .into_iter()
            .map(|(k, v)| AggregateGroup {
                key: k.unwrap_or(serde_json::Value::Null),
                value: v,
            })
            .collect();
        Ok(QueryResult::AggregateGroups(groups))
    } else {
        let _ = terminal; // shape encoded by SQL; tag unused on this path
        let mut query = sqlx::query_as::<_, (serde_json::Value,)>(&sql);
        for bind in binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        let (v,) = query.fetch_one(pool).await?;
        Ok(QueryResult::Aggregate(v))
    }
}

/// The sort spec for a paginated scan, computed once by the caller from the
/// query window's index fields: `sort_cols` (the unbound index fields +
/// `created_at` + `id`), `dir` (`ASC`/`DESC`), and `order_by` (the joined
/// `"col" DIR, …` clause). Bundled so `compile_paginate_terminal` stays under
/// clippy's argument-count threshold.
struct SortSpec<'a> {
    sort_cols: Vec<String>,
    dir: &'a str,
    order_by: &'a str,
}

/// Paginate terminal SQL compilation: keyset-paginated scan over the compiled
/// window's WHERE clause, using `sort` (see [`SortSpec`]). Fetches one extra
/// row to detect a next page; the cursor encodes the last row's sort-column
/// values. Compile half of the former inline `if let Some(paginate) =
/// &q.paginate { … }` block — SQL and bind-order byte-for-byte identical to
/// the pre-refactor cascade. The `index_def` + `eq_len` are returned alongside
/// the [`CompiledQuery`] so the executor can build the next-page cursor from
/// the last row's projected fields.
fn compile_paginate_terminal<'a>(
    w: QueryWindow<'a>,
    table_def: &TableDef,
    paginate: &Paginate,
    sort: SortSpec<'_>,
    db: &str,
    table: &str,
) -> Result<(CompiledQuery, PaginateExecCtx<'a>), RtDbError> {
    let SortSpec {
        sort_cols,
        dir,
        order_by,
    } = sort;
    let QueryWindow {
        index_def,
        binds,
        eq_len,
        range_binds,
        mut where_conditions,
        filter_binds,
        ..
    } = w;
    let num_items = paginate.num_items.min(MAX_TAKE);

    // Sort-column types parallel `sort_cols`, for cursor bind typing.
    let sort_col_types: Vec<SortCol> = {
        let mut v: Vec<SortCol> = match index_def {
            Some(idx) => idx.fields[eq_len..]
                .iter()
                .map(|fname| {
                    let ft = table_def.fields.get(fname).ok_or_else(|| {
                        RtDbError::internal(format!("index references unknown field '{fname}'"))
                    })?;
                    Ok(SortCol::IndexField(ft))
                })
                .collect::<Result<Vec<_>, RtDbError>>()?,
            None => Vec::new(),
        };
        v.push(SortCol::CreatedAt);
        v.push(SortCol::Id);
        v
    };

    // Decode the cursor (if any) and append the keyset resume predicate to
    // the eq/range WHERE already built for this query.
    let cursor_start = eq_len + range_binds.len() + filter_binds.len() + 1;
    let cursor_binds: Vec<EqBind> = if let Some(cursor) = &paginate.cursor {
        let cursor_values = decode_cursor(cursor)?;
        if cursor_values.len() != sort_cols.len() {
            return Err(RtDbError::bad_request(format!(
                "cursor has {} value(s) but this query sorts over {} column(s)",
                cursor_values.len(),
                sort_cols.len()
            )));
        }
        let (clause, binds) = build_cursor_conditions(
            &cursor_values,
            &sort_cols,
            &sort_col_types,
            dir,
            cursor_start,
        )?;
        where_conditions.push(clause);
        binds
    } else {
        Vec::new()
    };

    let limit_placeholder = cursor_start + cursor_binds.len();
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table);
    let mut sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\""
    );
    if !where_conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY ");
    sql.push_str(order_by);
    sql.push_str(&format!(" LIMIT ${limit_placeholder}"));

    let mut all = Vec::with_capacity(
        binds.len() + range_binds.len() + filter_binds.len() + cursor_binds.len() + 1,
    );
    all.extend(binds);
    all.extend(range_binds);
    all.extend(filter_binds);
    all.extend(cursor_binds);
    // Fetch one extra row so a next page can be detected without a second
    // round-trip; the extra is discarded after the has-next check.
    all.push(EqBind::I64(i64::from(num_items) + 1));

    let cq = CompiledQuery {
        sql,
        binds: all,
        terminal: "paginate",
    };
    Ok((
        cq,
        PaginateExecCtx {
            index_def,
            eq_len,
            num_items,
        },
    ))
}

/// Postgres-fetch context the paginate executor needs to build the next-page
/// cursor after the rows come back. Pure metadata — no SQL of its own.
pub(crate) struct PaginateExecCtx<'a> {
    pub(crate) index_def: Option<&'a IndexDef>,
    pub(crate) eq_len: usize,
    pub(crate) num_items: u32,
}

/// Paginate terminal execute tail: bind + fetch, then build the next-page
/// cursor from the last row's sort-column values.
pub(crate) async fn execute_paginate_terminal(
    cq: CompiledQuery,
    ctx: PaginateExecCtx<'_>,
    pool: &PgPool,
) -> Result<QueryResult, RtDbError> {
    let CompiledQuery { sql, binds, .. } = cq;
    let PaginateExecCtx {
        index_def,
        eq_len,
        num_items,
    } = ctx;
    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    let mut rows = query.fetch_all(pool).await?;

    let has_next = rows.len() > num_items as usize;
    if has_next {
        rows.pop();
    }

    // The next cursor is built from the last row of the page (after the
    // extra is discarded); absent when the page is empty or last.
    let next_cursor = if has_next && let Some((last_id, last_doc, last_created_at, _)) = rows.last()
    {
        let mut cursor_values: Vec<serde_json::Value> = Vec::new();
        if let Some(idx) = index_def {
            for fname in &idx.fields[eq_len..] {
                let val = last_doc.get(fname).ok_or_else(|| {
                    RtDbError::internal(format!("stored doc is missing indexed field '{fname}'"))
                })?;
                cursor_values.push(val.clone());
            }
        }
        cursor_values.push(serde_json::json!(*last_created_at));
        cursor_values.push(serde_json::Value::String(last_id.clone()));
        Some(encode_cursor(&cursor_values)?)
    } else {
        None
    };

    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Paginated(PaginatedResult {
        docs,
        next_cursor,
    }))
}

/// `unique`/`first`/`collect` fall-through terminal SQL compilation: scans the
/// compiled window's WHERE clause ordered by `order_by`, applies a `limit`
/// derived from `unique` (2), `first` (1), or `take` (defaulting to `MAX_TAKE`),
/// and shapes the result (`Doc`/`Doc(None)` for unique/first, `Docs` for
/// collect). Compile half of the former inline fall-through block — SQL and
/// bind-order byte-for-byte identical to the pre-refactor cascade.
fn compile_collect_terminal(
    w: QueryWindow<'_>,
    unique: bool,
    first: bool,
    take: Option<u32>,
    order_by: &str,
    db: &str,
    table: &str,
) -> Result<CompiledQuery, RtDbError> {
    let QueryWindow {
        binds,
        range_binds,
        where_conditions,
        filter_binds,
        limit_placeholder,
        ..
    } = w;
    let limit: u32 = if unique {
        2
    } else if first {
        1
    } else {
        take.unwrap_or(MAX_TAKE)
    };

    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table);
    let mut sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\""
    );
    if !where_conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY ");
    sql.push_str(order_by);
    sql.push_str(&format!(" LIMIT ${limit_placeholder}"));

    let mut all = Vec::with_capacity(binds.len() + range_binds.len() + filter_binds.len() + 1);
    all.extend(binds);
    all.extend(range_binds);
    all.extend(filter_binds);
    all.push(EqBind::I64(i64::from(limit)));
    // The wire terminal name distinguishes the three shapes (unique/first/
    // collect) for the executor's result-shaping switch and for /explain.
    let terminal: &'static str = if unique {
        "unique"
    } else if first {
        "first"
    } else {
        "collect"
    };
    Ok(CompiledQuery {
        sql,
        binds: all,
        terminal,
    })
}

/// `unique`/`first`/`collect` terminal execute tail: bind + fetch, then shape
/// per the wire `terminal` tag (`unique`/`first` → `Doc`, `collect` → `Docs`).
pub(crate) async fn execute_collect_terminal(
    cq: CompiledQuery,
    pool: &PgPool,
) -> Result<QueryResult, RtDbError> {
    let CompiledQuery {
        sql,
        binds,
        terminal,
    } = cq;
    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    let mut rows = query.fetch_all(pool).await?;

    if terminal == "unique" {
        if rows.len() > 1 {
            return Err(RtDbError::precondition(
                "unique query matched multiple documents",
            ));
        }
        return match rows.pop() {
            Some((id, doc, created_at, version)) => Ok(QueryResult::Doc(Some(merge_doc(
                id, doc, created_at, version,
            )?))),
            None => Ok(QueryResult::Doc(None)),
        };
    }

    if terminal == "first" {
        return match rows.pop() {
            Some((id, doc, created_at, version)) => Ok(QueryResult::Doc(Some(merge_doc(
                id, doc, created_at, version,
            )?))),
            None => Ok(QueryResult::Doc(None)),
        };
    }

    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Docs(docs))
}

/// A sort column's nature, used to type cursor binds. The sort order is always
/// the unbound index fields (those after the `eq` prefix) followed by
/// `created_at` then `id`.
enum SortCol<'a> {
    /// An unbound indexed user field — typed via its declared `FieldType`, so
    /// cursor binds reuse the same `eq_bind_for` path as `eq` prefixes.
    IndexField(&'a FieldType),
    /// `created_at` column — stored as `bigint`. The cursor value is bound as
    /// float8 and cast to bigint in the SQL (`$n::bigint`) so the comparison
    /// is integer-vs-integer regardless of the float8 wire type.
    CreatedAt,
    /// `id` column — stored as `text`.
    Id,
}

impl SortCol<'_> {
    /// Returns the SQL placeholder (with cast when needed) and the typed bind
    /// for one cursor value at 1-based position `pos`.
    fn cursor_bind(
        &self,
        value: &serde_json::Value,
        pos: usize,
    ) -> Result<(String, EqBind), RtDbError> {
        match self {
            SortCol::IndexField(ft) => Ok((format!("${pos}"), eq_bind_for(ft, value)?)),
            SortCol::CreatedAt => {
                let n = value.as_f64().ok_or_else(|| {
                    RtDbError::bad_request("cursor value for created_at must be a number")
                })?;
                Ok((format!("${pos}::bigint"), EqBind::Num(n)))
            }
            SortCol::Id => {
                let s = value.as_str().ok_or_else(|| {
                    RtDbError::bad_request("cursor value for id must be a string")
                })?;
                Ok((format!("${pos}"), EqBind::Text(s.to_string())))
            }
        }
    }
}

/// Builds the keyset-pagination resume predicate for a cursor over `sort_cols`
/// in direction `dir` ("ASC"/"DESC"). The cursor stores one value per sort
/// column, in order; the predicate is the standard row-value comparison
/// expanded to OR-of-AND:
///
/// ```text
/// (c0 OP v0)
///   OR (c0 = v0 AND c1 OP v1)
///   OR ...
///   OR (c0 = v0 AND ... AND cN-1 = vN-1 AND cN OP vN)
/// ```
///
/// where OP is `>` (ASC) or `<` (DESC). Because `id` is always the final sort
/// column and is globally unique, this fully determines a stable order — no
/// row is skipped or duplicated across pages. `next_bind_idx` is the 1-based
/// position of the first new bind. Returns the fully parenthesized predicate
/// and the binds in placeholder order.
fn build_cursor_conditions(
    cursor_values: &[serde_json::Value],
    sort_cols: &[String],
    sort_col_types: &[SortCol<'_>],
    dir: &str,
    next_bind_idx: usize,
) -> Result<(String, Vec<EqBind>), RtDbError> {
    let op = if dir == "DESC" { "<" } else { ">" };
    let mut binds: Vec<EqBind> = Vec::new();
    let mut branches: Vec<String> = Vec::new();
    for i in 0..sort_cols.len() {
        let mut conjuncts: Vec<String> = Vec::new();
        for j in 0..=i {
            let pos = next_bind_idx + binds.len();
            let (placeholder, bind) = sort_col_types[j].cursor_bind(&cursor_values[j], pos)?;
            let cmp = if j < i { "=" } else { op };
            conjuncts.push(format!("{} {cmp} {placeholder}", sort_cols[j]));
            binds.push(bind);
        }
        branches.push(conjuncts.join(" AND "));
    }
    Ok((format!("({})", branches.join(" OR ")), binds))
}

/// `get` (point read) terminal SQL compilation. Compile half of the former
/// inline `point_read` body — SQL byte-for-byte identical to the pre-refactor
/// cascade. The id bind is encoded as `EqBind::Text` so the executor uses the
/// same bind loop every other terminal does. Per-row owner/collaborator and
/// `authorize` filtering happen post-fetch in the executor (silent filter —
/// Convex-like), so neither appears in the SQL.
fn compile_point_read(sctx: &CompileSearchCtx<'_>, id: &str) -> Result<CompiledQuery, RtDbError> {
    let db = sctx.db;
    let table_name = sctx.table_name;
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table_name);
    // FM-33: a soft-delete table's point read hides stamped rows the same way
    // the scan terminals do — a deleted doc reads as absent unless the admin
    // `includeDeleted` pass-through is set. Bindless literal appended to `$1`.
    let soft_filter = if sctx.table_def.soft_delete && !sctx.include_deleted {
        " AND \"deleted_at\" IS NULL"
    } else {
        ""
    };
    let sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1{soft_filter}"
    );
    Ok(CompiledQuery {
        sql,
        binds: vec![EqBind::Text(id.to_string())],
        terminal: "get",
    })
}

pub(crate) async fn point_read(
    sctx: &SearchCtx<'_>,
    cq: CompiledQuery,
    owner: Option<&str>,
) -> Result<QueryResult, RtDbError> {
    let pool = sctx.pool;
    let table_def = sctx.table_def;
    let owner_field = sctx.owner_field;
    let collaborators_field = sctx.collaborators_field;
    let ctx = sctx.ctx;
    let CompiledQuery { sql, binds, .. } = cq;
    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    let row: Option<(String, serde_json::Value, i64, i64)> = query.fetch_optional(pool).await?;

    match row {
        Some((id, doc, created_at, version)) => {
            // Per-row: a user may only point-read a doc they own OR appear in
            // the collaborators array of. Silent filter (Convex-like) — docs
            // neither owned-by nor shared-with the caller read as absent.
            if let Some(uid) = row_auth_enforced_uid(owner_field, collaborators_field, owner)
                && !row_visible_to(&doc, owner_field, collaborators_field, uid)
            {
                return Ok(QueryResult::Doc(None));
            }
            // `authorize` predicate gate (user-only; bypass callers and
            // owner/collab-only tables are unaffected). Composes with the
            // owner/collab check above — both must pass when a table declares
            // both. Silent on a false predicate (Doc(None)), matching the
            // Convex-style point-read filtering above.
            if let Some(expr) = &table_def.authorize
                && ctx.user_id.is_some()
                && !filter_matches(&doc, expr, ctx)
            {
                return Ok(QueryResult::Doc(None));
            }
            Ok(QueryResult::Doc(Some(merge_doc(
                id, doc, created_at, version,
            )?)))
        }
        None => Ok(QueryResult::Doc(None)),
    }
}

/// Whether an indexed field's declared type is numeric (the only numeric
/// Whether an indexed field's declared type is numeric enough for `sum`/`avg`.
/// `Number` and `Int64` both qualify (`Optional<…>` unwraps one layer). Note:
/// `SUM(bigint)`/`AVG(bigint)` return Postgres `numeric`, which serializes as a
/// JSON number (f64) — precision is lost past 2^53; accepted trade-off.
fn is_numeric_index_field(ty: &FieldType) -> bool {
    match ty {
        FieldType::Number | FieldType::Int64 => true,
        FieldType::Optional { inner } => is_numeric_index_field(inner),
        _ => false,
    }
}

/// Merges a stored doc with its system fields. Result docs never collide on
/// these keys: `validate_doc` rejects any `"_"`-prefixed field at write time.
pub(crate) fn merge_doc(
    id: String,
    doc: serde_json::Value,
    created_at: i64,
    version: i64,
) -> Result<serde_json::Value, RtDbError> {
    let mut map = match doc {
        serde_json::Value::Object(map) => map,
        _ => return Err(RtDbError::internal("stored doc is not a JSON object")),
    };
    map.insert("_id".to_string(), serde_json::Value::String(id));
    map.insert("_creationTime".to_string(), serde_json::json!(created_at));
    map.insert("_version".to_string(), serde_json::json!(version));
    Ok(serde_json::Value::Object(map))
}
