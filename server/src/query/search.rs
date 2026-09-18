//! The ranked search family — `search` (tsvector FTS + trgm), `vectorSearch`,
//! `hybridSearch` (RRF fusion) — their compile/execute halves, and the shared
//! borrow-only ctx structs those terminals (and `point_read`) thread through.
//! ARC-203 mechanical split of the former `query.rs`.

use sqlx::PgPool;

use super::MAX_TAKE;
use super::filter::compile_filter;
use super::row_auth::{authorize_predicate_body, row_auth_enforced_uid, row_auth_predicate_body};
use super::terminals::{CompiledQuery, SortCol, build_cursor_conditions, merge_doc};
use crate::auth::PrincipalCtx;
use crate::ddl::{pg_col, pg_schema, pg_search_col, pg_table, pg_vector_col};
use crate::dsl::{
    EqBind, HybridSearchQuery, Paginate, PaginatedResult, QueryResult, SearchMode, SearchQuery,
    VectorSearchQuery,
};
use crate::error::RtDbError;
use crate::pagination::{decode_cursor, encode_cursor};
use crate::schema::TableDef;

/// Hard cap on `vectorSearch` `limit`.
/// SEC-007: hard ceiling on the raw `search.query` text. Rejected before
/// compilation so an oversized string never reaches `websearch_to_tsquery` or
/// the `%…%` ILIKE pattern builder.
pub const MAX_SEARCH_QUERY_BYTES: usize = 4096;

const VECTOR_SEARCH_MAX_LIMIT: u32 = 256;

/// Server-fixed `ts_headline` options for `snippet: true` search results
/// (FM-31). The client opts in with a boolean and can supply none of these —
/// word bounds and highlight delimiters are server-owned. `<mark>` renders
/// directly in HTML/React; MaxWords/MinWords are the PostgreSQL defaults made
/// explicit so the bound is visible and owned here.
const SNIPPET_HEADLINE_OPTS: &str = "StartSel=<mark>, StopSel=</mark>, MaxWords=35, MinWords=15";

// ============ Validation cascade infrastructure (QA-002) ============
//
// The validation cascade in `execute_query` is a sequence of "if terminal X is
// set AND peer Y is set → BadRequest" guards. Each terminal historically carried
// its own hand-written if-chain, so adding a terminal meant adding 5–10 clauses
// across server + TS — exactly the drift that produced QA-001 (the TS `get`
// guard omitted `filter`/`search`/`vectorSearch`).
//
// The infrastructure below lets each terminal DECLARE its incompatible peers;
// `execute_query` consults the active terminal's list once. Adding a terminal is
// now a one-line addition to the relevant const table, and the cross-client
// combination-matrix test (`server/tests/query_combinations.rs` and its TS
// mirror `ts-client/tests/query_combinations.test.ts`) catches any drift.
//
// Behavior-preserving: same peers checked, same messages emitted, same
// first-match-wins ordering as the pre-refactor inline cascade.

/// `websearch_to_tsquery` SQL fragment honoring an optional search-index
/// `language` (a `schema::validate_structure`-checked regconfig name
/// interpolated as a literal). The query text stays a `$ph` bind, so user
/// input can never inject tsquery syntax or escape the regconfig literal;
/// `websearch_to_tsquery` additionally honors quoted phrases, the bare word
/// `or`, and `-term` negation in the bound text (FM-31) — a pure superset of
/// the former `plainto_tsquery` for plain terms. The tsvector column and the
/// tsquery must share a regconfig for `@@` to match correctly, so every search
/// path builds both from the same index `language`.
fn websearch_tsquery_sql(language: Option<&str>, ph: usize) -> String {
    match language {
        Some(lang) => format!("websearch_to_tsquery('{lang}'::regconfig, ${ph})"),
        None => format!("websearch_to_tsquery(${ph})"),
    }
}

/// Full-text search terminal: matches a search index's generated tsvector
/// against `websearch_to_tsquery(<query text>)` and ranks by `ts_rank` descending,
/// with `(created_at, id)` tie-breakers. Composes with `take` (defaulting to
/// `MAX_TAKE`); the caller has already rejected every other terminal. The query
/// text is bound once via `$1` and reused in the `ORDER BY ts_rank`, so user
/// text can never inject tsquery syntax. Unknown index / empty query →
/// `BadRequest`, never a 500.
/// Shared, borrow-only context for the read-path terminals that don't use the
/// btree `QueryWindow` — `point_read` and the `search`/`vectorSearch`/
/// `hybridSearch` family. Those four terminals share the same caller-resolved
/// inputs (pool, the resolved table definition, and the per-row-auth fields
/// derived from it, plus the principal), so `SearchCtx`
/// bundles them the way `QueryWindow` bundles the index-window inputs and
/// `StepCtx` (`txn.rs`) bundles the write-path inputs. Borrow-only: every
/// field is a shared reference (these terminals read, never mutate), so it
/// threads through `&SearchCtx` cleanly. QA-105.
pub(crate) struct SearchCtx<'a> {
    pub(crate) pool: &'a PgPool,
    pub(crate) table_def: &'a TableDef,
    pub(crate) owner_field: Option<&'a str>,
    pub(crate) collaborators_field: Option<&'a str>,
    pub(crate) ctx: &'a PrincipalCtx,
}

/// Compile-only view of [`SearchCtx`] — every field EXCEPT `pool`. The compile
/// fns are pure (no I/O), so they take this smaller context; the execute tails
/// take the pool directly. The fields here are the same names/types as
/// `SearchCtx`'s, so the compile bodies read identically to the pre-refactor
/// inline bodies.
pub(crate) struct CompileSearchCtx<'a> {
    pub(crate) db: &'a str,
    pub(crate) table_def: &'a TableDef,
    pub(crate) table_name: &'a str,
    pub(crate) owner_field: Option<&'a str>,
    pub(crate) collaborators_field: Option<&'a str>,
    pub(crate) ctx: &'a PrincipalCtx,
    /// FM-33: when `true` (admin `includeDeleted` pass-through), the
    /// soft-delete literal is NOT composed — soft-deleted rows surface.
    pub(crate) include_deleted: bool,
}

// ============ ENH-030: cursor pagination over the ranked terminals ==========
//
// `search`/`vectorSearch`/`hybridSearch` each rank rows by a scalar key —
// `ts_rank`/`similarity`, the index's metric distance, and the RRF fused score
// respectively. `paginate` composes with all three as a peer clause exactly
// the way `take` already composes with `search`: it does not change WHICH
// terminal runs (`cq.terminal` stays the terminal's own name), only how many
// rows come back and whether the result is a `Paginated` envelope.
//
// The resume predicate is the same keyset OR-of-AND `paginate` uses on the
// btree path, over `[<ranking key>, created_at, id]` — the two tie-breakers
// make the order total, which is what guarantees no row is skipped or
// duplicated across pages. `id` is globally unique, so three columns always
// suffice; every ranked terminal therefore mints a three-value cursor.

/// The ranked terminals' cursor sort columns: the ranking key, then the
/// `created_at`/`id` tie-breakers. Kept as one list so the length check, the
/// bind typing, and the minted cursor cannot drift apart.
const RANKED_SORT_COL_COUNT: usize = 3;

/// Decodes a ranked terminal's cursor into its keyset resume predicate.
///
/// `rank_sql` is the SQL for the ranking key AS THE CONSUMING STAGE SEES IT —
/// the raw ranking expression for `search` (whose keyset sits in the same
/// SELECT that computes it) or the CTE's output column for `vectorSearch` /
/// `hybridSearch`. `rank_dir` is `ASC` for a distance (nearest first) and
/// `DESC` for a relevance score; the tie-breakers are always `DESC`, matching
/// every ranked terminal's existing ORDER BY.
///
/// Returns `("", [])` when the paginate block carries no cursor (the first
/// page), so callers can splice the fragment in unconditionally.
fn ranked_cursor_clause(
    paginate: &Paginate,
    rank_sql: &str,
    rank_dir: &str,
    next_bind_idx: usize,
) -> Result<(String, Vec<EqBind>), RtDbError> {
    let Some(cursor) = &paginate.cursor else {
        return Ok((String::new(), Vec::new()));
    };
    let cursor_values = decode_cursor(cursor)?;
    if cursor_values.len() != RANKED_SORT_COL_COUNT {
        return Err(RtDbError::bad_request(format!(
            "cursor has {} value(s) but this query sorts over {RANKED_SORT_COL_COUNT} column(s)",
            cursor_values.len(),
        )));
    }
    let sort_cols = [
        rank_sql.to_string(),
        "\"created_at\"".to_string(),
        "\"id\"".to_string(),
    ];
    let sort_col_types = [SortCol::Rank, SortCol::CreatedAt, SortCol::Id];
    let dirs = [rank_dir, "DESC", "DESC"];
    build_cursor_conditions(
        &cursor_values,
        &sort_cols,
        &sort_col_types,
        &dirs,
        next_bind_idx,
    )
}

/// Full-text search terminal SQL compilation. Compile half of the former
/// inline `execute_search` body. Bind order: `$1` is the search query text;
/// in `trgm` mode `$2` is the server-built `'%…%'` ILIKE pattern (so
/// downstream placeholders shift by one); the optional client `filter`
/// compiles next, then the per-row owner/collaborator and `authorize`
/// predicates, then `LIMIT`. The search text/pattern and the limit are
/// encoded as `EqBind::Text` / `EqBind::I64` respectively so the executor
/// uses the same bind loop every other terminal does. `snippet: true`
/// (tsquery mode only) appends one trailing `ts_headline` column to the
/// SELECT, reusing `$1` — bind order and count are unchanged.
pub(crate) fn compile_search(
    sctx: &CompileSearchCtx<'_>,
    search: &SearchQuery,
    take: Option<u32>,
    paginate: Option<&Paginate>,
) -> Result<CompiledQuery, RtDbError> {
    let db = sctx.db;
    let table_def = sctx.table_def;
    let table_name = sctx.table_name;
    let owner_field = sctx.owner_field;
    let collaborators_field = sctx.collaborators_field;
    let ctx = sctx.ctx;
    if search.query.trim().is_empty() {
        return Err(RtDbError::bad_request(
            "search query text must not be empty",
        ));
    }
    // SEC-007: bound the text handed to `websearch_to_tsquery`/`ILIKE` before
    // any compilation work happens.
    if search.query.len() > MAX_SEARCH_QUERY_BYTES {
        return Err(RtDbError::bad_request(format!(
            "search query text must be at most {MAX_SEARCH_QUERY_BYTES} bytes"
        )));
    }
    let owner = ctx.user_id.as_deref();
    let index_def = table_def
        .indexes
        .iter()
        .find(|index| index.name == search.index && index.search)
        .ok_or_else(|| {
            RtDbError::bad_request(format!("search index '{}' not found", search.index))
        })?;
    let sv_col = pg_search_col(&index_def.name);
    let tsq = websearch_tsquery_sql(index_def.language.as_deref(), 1);
    let limit = take.unwrap_or(MAX_TAKE);
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table_name);
    let mode = search.mode.unwrap_or_default();
    // `snippet` needs a tsquery tree to highlight; trgm mode matches raw
    // substrings, so the combination is rejected rather than silently
    // ignored.
    let snippet = search.snippet.unwrap_or(false);
    if snippet && mode == SearchMode::Trgm {
        return Err(RtDbError::bad_request(
            "snippet is only supported in tsquery mode",
        ));
    }
    // The headline renders from the same `websearch_to_tsquery($1)` the WHERE
    // matched, so a snippet highlights exactly why the doc is a hit — phrases
    // included. Source text is the index's fields in declared order
    // (`concat_ws` skips NULL columns; a doc that matched the tsvector
    // necessarily carries text in at least one of them). Options are the
    // `SNIPPET_HEADLINE_OPTS` server constant, never client-supplied.
    let snippet_col = if snippet {
        let src = index_def
            .fields
            .iter()
            .map(|field_name| format!("\"{}\"", pg_col(field_name)))
            .collect::<Vec<_>>()
            .join(", ");
        let cfg = match index_def.language.as_deref() {
            Some(lang) => format!("'{lang}'::regconfig, "),
            None => String::new(),
        };
        format!(", ts_headline({cfg}concat_ws(' ', {src}), {tsq}, '{SNIPPET_HEADLINE_OPTS}')")
    } else {
        String::new()
    };

    // `$1` is the search query text in both modes (the tsquery in `tsquery`
    // mode, the similarity argument in `trgm` mode). The optional client
    // `filter` compiles next, then the per-row owner/collaborator and
    // `authorize` predicates, then `LIMIT` — the same compose order
    // `compile_scan_where` uses for reads. One shared accumulator keeps
    // `compile_filter_node`'s `start_pos + binds.len()` placeholders correctly
    // numbered. Schema-validated identifiers are interpolated; every value is
    // `$n`-bound. With no filter and an owner-only table this emits the
    // single-predicate form byte-identical to the pre-filter SQL.
    let mut binds: Vec<EqBind> = Vec::new();
    let mut extra = String::new();
    let start = match mode {
        SearchMode::Tsquery => 2usize,
        SearchMode::Trgm => 3,
    };
    if let Some(filter) = &search.filter {
        let (fragment, filter_binds) = compile_filter(filter, table_def, start, false)?;
        binds.extend(filter_binds);
        extra.push_str(" AND (");
        extra.push_str(&fragment);
        extra.push(')');
    }
    let enforced_uid = row_auth_enforced_uid(owner_field, collaborators_field, owner);
    if let Some(uid) = enforced_uid {
        let ph = start + binds.len();
        extra.push_str(" AND ");
        extra.push_str(&row_auth_predicate_body(
            owner_field,
            collaborators_field,
            ph,
        ));
        binds.push(EqBind::Text(uid.to_string()));
    }
    if let Some(frag) = authorize_predicate_body(table_def, ctx, start, &mut binds)? {
        extra.push_str(" AND ");
        extra.push_str(&frag);
    }
    // FM-33: hide soft-deleted rows from ranked search unless the admin
    // `includeDeleted` pass-through is set. Bindless literal — `limit_ph`
    // below is unaffected.
    if table_def.soft_delete && !sctx.include_deleted {
        extra.push_str(" AND \"deleted_at\" IS NULL");
    }
    // The match predicate and the ranking expression, derived ONCE per mode and
    // reused by the WHERE, the ORDER BY, the paginated SELECT's extra ranking
    // column, and the keyset resume predicate. Deriving them together is what
    // keeps `trgm` mode's keyset paging on `GREATEST(similarity(...))` rather
    // than silently on `ts_rank` — a mismatch there would page by the wrong
    // key with no compile error.
    //
    // Trgm (FM-30): substring match over the index's text `f_` columns. `$1`
    // (raw query text) feeds `similarity`; `$2` is the server-built
    // `'%' || query || '%'` ILIKE pattern, bound — never interpolated. A result
    // row ILIKE-matched some field, so at least one `similarity` argument is
    // non-NULL and GREATEST is well-defined. The `created_at`/`id` tiebreaks
    // keep ordering deterministic like the tsquery arm.
    let (match_clause, rank_expr) = match mode {
        SearchMode::Tsquery => (
            format!("\"{sv_col}\" @@ {tsq}"),
            format!("ts_rank(\"{sv_col}\", {tsq})"),
        ),
        SearchMode::Trgm => {
            let ilike = index_def
                .fields
                .iter()
                .map(|field_name| format!("\"{}\" ILIKE $2", pg_col(field_name)))
                .collect::<Vec<_>>()
                .join(" OR ");
            let sim = index_def
                .fields
                .iter()
                .map(|field_name| format!("similarity(\"{}\", $1)", pg_col(field_name)))
                .collect::<Vec<_>>()
                .join(", ");
            (format!("({ilike})"), format!("GREATEST({sim})"))
        }
    };

    // ENH-030: with `paginate`, the keyset predicate binds after every
    // filter/auth bind and before the LIMIT, the LIMIT becomes the page size
    // plus one probe row, and the ranking key is selected as an extra column so
    // the executor can mint the next cursor from the page's last row. Without
    // it, SQL and bind order stay byte-identical to the pre-ENH-030 form.
    let cursor_start = start + binds.len();
    let (cursor_predicate, cursor_binds) = match paginate {
        Some(p) => ranked_cursor_clause(p, &rank_expr, "DESC", cursor_start)?,
        None => (String::new(), Vec::new()),
    };
    let cursor_clause = if cursor_predicate.is_empty() {
        String::new()
    } else {
        format!(" AND {cursor_predicate}")
    };
    let rank_col = if paginate.is_some() {
        // `ts_rank` is `real`; the cast makes the minted cursor value a lossless
        // float8 promotion of exactly what the ORDER BY compared, so the resume
        // boundary is exact rather than approximate.
        format!(", {rank_expr}::float8")
    } else {
        String::new()
    };
    let limit_ph = cursor_start + cursor_binds.len();
    let limit_value = match paginate {
        // One extra row so a next page is detectable without a second
        // round-trip; the executor discards it after the has-next check.
        Some(p) => i64::from(p.num_items.min(MAX_TAKE)) + 1,
        None => i64::from(limit),
    };
    let sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\"{snippet_col}{rank_col} FROM \"{pg_schema_name}\".\"{table_ident}\" \
         WHERE {match_clause}{extra}{cursor_clause} \
         ORDER BY {rank_expr} DESC, \"created_at\" DESC, \"id\" DESC \
         LIMIT ${limit_ph}"
    );
    // The leading bind(s) are the search text (and, in trgm mode, its
    // `%…%` ILIKE pattern); then the filter/auth binds, then the cursor's
    // keyset values, and the trailing bind is the LIMIT. All are folded into
    // the same Vec<EqBind> the executor drains in order.
    let mut all = Vec::with_capacity(start - 1 + binds.len() + cursor_binds.len() + 1);
    all.push(EqBind::Text(search.query.clone()));
    if mode == SearchMode::Trgm {
        all.push(EqBind::Text(format!("%{}%", search.query)));
    }
    all.extend(binds);
    all.extend(cursor_binds);
    all.push(EqBind::I64(limit_value));
    Ok(CompiledQuery {
        sql,
        binds: all,
        terminal: "search",
    })
}

/// Execute tail for the `search` terminal. Two row shapes, selected by the
/// caller-re-derived `snippet` flag: the 4-column default, or a 5-column
/// form whose trailing `ts_headline` render becomes each doc's
/// `_searchSnippet` field (inserted after `merge_doc` — write-time
/// validation rejects `_`-prefixed keys, so the additive field never
/// collides with stored data).
pub(crate) async fn execute_search(
    cq: CompiledQuery,
    pool: &PgPool,
    snippet: bool,
) -> Result<QueryResult, RtDbError> {
    let CompiledQuery { sql, binds, .. } = cq;
    if snippet {
        let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64, String)>(&sql);
        for bind in binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        let rows = query.fetch_all(pool).await?;
        let docs = rows
            .into_iter()
            .map(|(id, doc, created_at, version, snippet_text)| {
                let mut merged = merge_doc(id, doc, created_at, version)?;
                merged["_searchSnippet"] = serde_json::Value::String(snippet_text);
                Ok::<_, RtDbError>(merged)
            })
            .collect::<Result<Vec<_>, _>>()?;
        return Ok(QueryResult::Docs(docs));
    }
    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
            EqBind::I64(v) => query.bind(v),
        };
    }
    let rows = query.fetch_all(pool).await?;
    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Docs(docs))
}

/// Vector-similarity terminal SQL compilation: ranks rows by the index's
/// declared metric distance (`<=>`/`<->`/`<#>` for cosine/l2/ip) between the
/// index's `v_<index>` column and the query vector, ascending, limited to
/// `limit`. Optional `filter` eq-binds over the index's declared `filterFields`.
/// Unknown index / length mismatch / unknown filter key / out-of-range limit
/// → `BadRequest`. Bind order: filter eq-binds occupy `$1..$k`, then (when the
/// table is owner-gated and the caller is a user) the owner id occupies
/// `$(k+1)`, then the query vector (`$n::vector`), then `limit`. Compile half
/// of the former inline `execute_vector_search` body — SQL and bind-order
/// byte-for-byte identical to the pre-refactor cascade; the query vector (in
/// its `[a,b,c]` text form) and the limit are encoded as `EqBind::Text` and
/// `EqBind::I64` so the executor uses the same bind loop every other terminal
/// does.
pub(crate) fn compile_vector_search(
    sctx: &CompileSearchCtx<'_>,
    vs: &VectorSearchQuery,
    paginate: Option<&Paginate>,
) -> Result<CompiledQuery, RtDbError> {
    let db = sctx.db;
    let table_def = sctx.table_def;
    let table_name = sctx.table_name;
    let owner_field = sctx.owner_field;
    let collaborators_field = sctx.collaborators_field;
    let ctx = sctx.ctx;
    let index_def = table_def
        .indexes
        .iter()
        .find(|index| index.name == vs.index && index.vector.is_some())
        .ok_or_else(|| RtDbError::bad_request(format!("vector index '{}' not found", vs.index)))?;
    // Unreachable given the find predicate above, but keep the error path
    // explicit instead of panicking on a future predicate change.
    let vec_spec = index_def
        .vector
        .as_ref()
        .ok_or_else(|| RtDbError::internal("matched vector index has no vector spec"))?;

    if vs.vector.len() != vec_spec.dimensions as usize {
        return Err(RtDbError::bad_request(format!(
            "vectorSearch vector length {} != index '{}' dimensions {}",
            vs.vector.len(),
            vs.index,
            vec_spec.dimensions
        )));
    }
    // serde_json can't carry NaN/Infinity, but a Rust-constructed query can —
    // reject before binding so pgvector never sees a non-finite value (which
    // would surface as a 500 instead of a clean BadRequest).
    if !vs.vector.iter().all(|v| v.is_finite()) {
        return Err(RtDbError::bad_request(
            "vectorSearch query vector must contain only finite numbers",
        ));
    }
    if !(1..=VECTOR_SEARCH_MAX_LIMIT).contains(&vs.limit) {
        return Err(RtDbError::bad_request(format!(
            "vectorSearch limit must be 1..={VECTOR_SEARCH_MAX_LIMIT}"
        )));
    }

    let v_col = pg_vector_col(&index_def.name);
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table_name);

    // pgvector accepts the text form `[a,b,c]` for a `::vector`-cast bind.
    let qvec_text = format!(
        "[{}]",
        vs.vector
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );

    // Bind numbering: the optional client `filter` (full `FilterExpr`, reusing
    // `compile_filter`) compiles first at `$1`, then the per-row owner/
    // collaborator uid and the `authorize` predicate, then the query vector
    // (cast to `vector`), then `limit` — the same compose order `execute_search`
    // uses. One shared accumulator keeps `compile_filter_node`'s
    // `start_pos + binds.len()` placeholders correctly numbered. The WHERE always
    // excludes rows whose vector column is NULL, so undimensioned rows never
    // surface as bogus nearest-neighbors. Filtering is no longer restricted to
    // declared `filterFields` (any field works — typed column when indexed, jsonb
    // extraction otherwise); declared filterFields still create indexed columns.
    let owner = ctx.user_id.as_deref();
    let enforced_uid = row_auth_enforced_uid(owner_field, collaborators_field, owner);
    let mut binds: Vec<EqBind> = Vec::new();
    let mut extra = String::new();
    let start = 1usize;
    if let Some(filter) = &vs.filter {
        let (fragment, filter_binds) = compile_filter(filter, table_def, start, false)?;
        binds.extend(filter_binds);
        extra.push_str(" AND (");
        extra.push_str(&fragment);
        extra.push(')');
    }
    if let Some(uid) = enforced_uid {
        let ph = start + binds.len();
        extra.push_str(" AND ");
        extra.push_str(&row_auth_predicate_body(
            owner_field,
            collaborators_field,
            ph,
        ));
        binds.push(EqBind::Text(uid.to_string()));
    }
    if let Some(frag) = authorize_predicate_body(table_def, ctx, start, &mut binds)? {
        extra.push_str(" AND ");
        extra.push_str(&frag);
    }
    // FM-33: hide soft-deleted rows from vector ranking unless the admin
    // `includeDeleted` pass-through is set. Bindless literal — `qvec_ph`/`
    // `limit_ph` below are unaffected.
    if table_def.soft_delete && !sctx.include_deleted {
        extra.push_str(" AND \"deleted_at\" IS NULL");
    }
    let qvec_ph = start + binds.len();
    let limit_ph = qvec_ph + 1;

    let dist_op = vec_spec.metric.distance_op();
    let where_clause = format!("\"{v_col}\" IS NOT NULL{extra}");
    let Some(paginate) = paginate else {
        let sql = format!(
            "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" \
             WHERE {where_clause} \
             ORDER BY \"{v_col}\" {dist_op} ${qvec_ph}::vector \
             LIMIT ${limit_ph}"
        );
        let mut all = Vec::with_capacity(binds.len() + 2);
        all.extend(binds);
        all.push(EqBind::Text(qvec_text));
        all.push(EqBind::I64(i64::from(vs.limit)));
        return Ok(CompiledQuery {
            sql,
            binds: all,
            terminal: "vectorSearch",
        });
    };

    // ENH-030, paginated form. `limit` keeps its meaning — the size of the
    // ranked candidate POOL — and `numItems` slices that pool into pages, so
    // paging a `vectorSearch` delivers the same top-`limit` neighbors the
    // unpaginated call returns, just in instalments.
    //
    // The pool is a CTE rather than a predicate on the same SELECT for two
    // reasons. First, a keyset predicate on the distance in the SELECT that
    // computes it would sit beside the ANN `ORDER BY`, where it risks
    // defeating the HNSW index (`ddl.rs` creates one per vector index); a
    // materialized `LIMIT`ed CTE keeps the ANN top-K intact and pages over the
    // (at most `limit`) rows it produced. Second, `dist` becomes a real column
    // of `cand`, so the outer WHERE can reference it — a SELECT-list alias is
    // not visible to a WHERE at the same level.
    //
    // The inner stage keeps EVERY filter/owner/authorize/soft-delete predicate
    // and the bare `ORDER BY <distance>` it has today, so the pool a paginated
    // scan walks is exactly the row set (and order) the unpaginated terminal
    // would return. Only the total order needed to resume — the `created_at`/
    // `id` tie-breakers — is added, on the outer stage, where it sorts at most
    // `limit` already-materialized rows and cannot reach the index.
    let cursor_start = limit_ph + 1;
    let (cursor_predicate, cursor_binds) =
        ranked_cursor_clause(paginate, "dist", "ASC", cursor_start)?;
    let cursor_clause = if cursor_predicate.is_empty() {
        String::new()
    } else {
        format!(" WHERE {cursor_predicate}")
    };
    let page_limit_ph = cursor_start + cursor_binds.len();
    let sql = format!(
        "WITH cand AS ( \
           SELECT \"id\", \"doc\", \"created_at\", \"version\", \
                  (\"{v_col}\" {dist_op} ${qvec_ph}::vector) AS dist \
           FROM \"{pg_schema_name}\".\"{table_ident}\" \
           WHERE {where_clause} \
           ORDER BY \"{v_col}\" {dist_op} ${qvec_ph}::vector \
           LIMIT ${limit_ph} \
         ) \
         SELECT \"id\", \"doc\", \"created_at\", \"version\", dist FROM cand{cursor_clause} \
         ORDER BY dist ASC, \"created_at\" DESC, \"id\" DESC \
         LIMIT ${page_limit_ph}"
    );
    let mut all = Vec::with_capacity(binds.len() + cursor_binds.len() + 3);
    all.extend(binds);
    all.push(EqBind::Text(qvec_text));
    all.push(EqBind::I64(i64::from(vs.limit)));
    all.extend(cursor_binds);
    // One extra row so a next page is detectable without a second round-trip.
    all.push(EqBind::I64(i64::from(paginate.num_items.min(MAX_TAKE)) + 1));
    Ok(CompiledQuery {
        sql,
        binds: all,
        terminal: "vectorSearch",
    })
}

pub(crate) async fn execute_vector_search(
    cq: CompiledQuery,
    pool: &PgPool,
) -> Result<QueryResult, RtDbError> {
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
    let rows = query.fetch_all(pool).await?;
    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Docs(docs))
}

/// Hybrid search terminal SQL compilation: fuses full-text (`ts_rank`) and
/// vector ranking (the resolved vector index's metric operator) over the same
/// table via RRF. The table must declare BOTH a search index (tsvector) AND a
/// vector index; if either is missing → `BadRequest`. The candidate set is the
/// UNION of rows matching `websearch_to_tsquery($text)` and rows with a non-null
/// vector; both rankings are computed in a single statement with window
/// functions and fused as `1/(k + r_text) + 1/(k + r_vec)` (default `k = 60`).
/// Bind order: the text query (`$1`, referenced in WHERE/ts_rank), the owner id
/// (`$2`) when owner-enforced, the query vector (`$n::vector`), the RRF constant
/// `k`, then `limit`. Column names come from `pg_search_col`/`pg_vector_col`
/// over the resolved indexes (auto-selected when not named); all identifiers
/// are schema-validated and double-quoted; every value is `$n`-bound. Compile
/// half of the former inline `execute_hybrid_search` body — SQL and bind-order
/// byte-for-byte identical to the pre-refactor cascade; the text query, query
/// vector (as `[a,b,c]` text), RRF `k`, and limit are all folded into the
/// single `binds: Vec<EqBind>` the executor drains in order.
pub(crate) fn compile_hybrid_search(
    sctx: &CompileSearchCtx<'_>,
    hs: &HybridSearchQuery,
    paginate: Option<&Paginate>,
) -> Result<CompiledQuery, RtDbError> {
    let db = sctx.db;
    let table_def = sctx.table_def;
    let table_name = sctx.table_name;
    let owner_field = sctx.owner_field;
    let collaborators_field = sctx.collaborators_field;
    let ctx = sctx.ctx;
    if hs.query.trim().is_empty() {
        return Err(RtDbError::bad_request(
            "hybrid search query text must not be empty",
        ));
    }

    // Resolve the search index (named or first search index on the table).
    let search_index = match &hs.search_index {
        Some(name) => table_def
            .indexes
            .iter()
            .find(|index| index.name == name.as_str() && index.search)
            .ok_or_else(|| RtDbError::bad_request(format!("search index '{}' not found", name)))?,
        None => table_def
            .indexes
            .iter()
            .find(|index| index.search)
            .ok_or_else(|| {
                RtDbError::bad_request(
                    "hybrid search requires both a search index and a vector index on the table",
                )
            })?,
    };
    // Resolve the vector index (named or first vector index on the table).
    let vector_index = match &hs.vector_index {
        Some(name) => table_def
            .indexes
            .iter()
            .find(|index| index.name == name.as_str() && index.vector.is_some())
            .ok_or_else(|| RtDbError::bad_request(format!("vector index '{}' not found", name)))?,
        None => table_def
            .indexes
            .iter()
            .find(|index| index.vector.is_some())
            .ok_or_else(|| {
                RtDbError::bad_request(
                    "hybrid search requires both a search index and a vector index on the table",
                )
            })?,
    };
    let vec_spec = vector_index
        .vector
        .as_ref()
        .ok_or_else(|| RtDbError::internal("matched vector index has no vector spec"))?;

    // Validate the query vector against the index dimensions and finiteness
    // (mirrors `execute_vector_search` so pgvector never sees a bad vector).
    if hs.vector.len() != vec_spec.dimensions as usize {
        return Err(RtDbError::bad_request(format!(
            "hybridSearch vector length {} != vector index '{}' dimensions {}",
            hs.vector.len(),
            vector_index.name,
            vec_spec.dimensions
        )));
    }
    if !hs.vector.iter().all(|v| v.is_finite()) {
        return Err(RtDbError::bad_request(
            "hybridSearch query vector must contain only finite numbers",
        ));
    }
    if hs.limit > MAX_TAKE {
        return Err(RtDbError::bad_request(format!(
            "hybridSearch limit exceeds maximum of {MAX_TAKE}"
        )));
    }

    let sv_col = pg_search_col(&search_index.name);
    let v_col = pg_vector_col(&vector_index.name);
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table_name);
    // RRF constant (default 60). `k + r` never divides by zero because
    // ROW_NUMBER starts at 1; k=0 is therefore safe but unusual.
    let k = i64::from(hs.k.unwrap_or(60));

    // pgvector accepts the text form `[a,b,c]` for a `::vector`-cast bind.
    let qvec_text = format!(
        "[{}]",
        hs.vector
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>()
            .join(",")
    );

    // Bind numbering: text query ($1, referenced in WHERE and ts_rank), the
    // per-row auth uid ($2) when owner/collaborators-enforced, the `authorize`
    // predicate's binds (when declared + user caller), then the query vector
    // (cast to `vector`), the RRF constant `k`, and `limit`. The per-row auth
    // predicate interpolates only the schema-validated `ownerField` /
    // `collaboratorsField` into jsonb string-literal positions; the uid is
    // `$n`-bound once. Owner uid and `authorize` binds share one accumulator
    // numbered from `auth_start` so `compile_filter_node`'s `start_pos +
    // binds.len()` rule keeps placeholders correct.
    let owner = ctx.user_id.as_deref();
    let enforced_uid = row_auth_enforced_uid(owner_field, collaborators_field, owner);
    let text_ph = 1usize;
    let tsq = websearch_tsquery_sql(search_index.language.as_deref(), text_ph);
    let mut auth_binds: Vec<EqBind> = Vec::new();
    let mut auth_clause = String::new();
    let auth_start = 2usize;
    if let Some(uid) = enforced_uid {
        let ph = auth_start + auth_binds.len();
        auth_clause.push_str(" AND ");
        auth_clause.push_str(&row_auth_predicate_body(
            owner_field,
            collaborators_field,
            ph,
        ));
        auth_binds.push(EqBind::Text(uid.to_string()));
    }
    if let Some(frag) = authorize_predicate_body(table_def, ctx, auth_start, &mut auth_binds)? {
        auth_clause.push_str(" AND ");
        auth_clause.push_str(&frag);
    }
    // FM-33: hide soft-deleted rows from hybrid ranking unless the admin
    // `includeDeleted` pass-through is set. Bindless literal — `qvec_ph`/`
    // `k_ph`/`limit_ph` below are unaffected.
    if table_def.soft_delete && !sctx.include_deleted {
        auth_clause.push_str(" AND \"deleted_at\" IS NULL");
    }
    let qvec_ph = auth_start + auth_binds.len();
    let k_ph = qvec_ph + 1;
    let limit_ph = k_ph + 1;

    // RRF over the union of text matches and vector-bearing rows. Rows that
    // don't match the text get ts_rank = 0 (ranked last on the text axis); rows
    // with a null vector get dist NULL (ranked last on the vector axis via NULLS
    // LAST). The final ORDER BY tie-breakers (created_at, id) keep output
    // deterministic when RRF scores collide.
    let dist_op = vec_spec.metric.distance_op();
    // The `matched` + `ranked` prefix is identical in both forms; only what
    // consumes `ranked` differs.
    let ranked_ctes = format!(
        "WITH matched AS ( \
           SELECT \"id\", \"doc\", \"created_at\", \"version\", \
                  ts_rank(\"{sv_col}\", {tsq}) AS trank, \
                  (\"{v_col}\" {dist_op} ${qvec_ph}::vector) AS dist \
           FROM \"{pg_schema_name}\".\"{table_ident}\" \
           WHERE (\"{sv_col}\" @@ {tsq} OR \"{v_col}\" IS NOT NULL){auth_clause} \
         ), ranked AS ( \
           SELECT \"id\", \"doc\", \"created_at\", \"version\", \
                  ROW_NUMBER() OVER (ORDER BY trank DESC, \"created_at\" DESC, \"id\" DESC) AS r_text, \
                  ROW_NUMBER() OVER (ORDER BY dist ASC NULLS LAST, \"created_at\" DESC, \"id\" DESC) AS r_vec \
           FROM matched \
         )"
    );
    let Some(paginate) = paginate else {
        let sql = format!(
            "{ranked_ctes} \
             SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM ranked \
             ORDER BY (1.0/(${k_ph} + r_text) + 1.0/(${k_ph} + r_vec)) DESC, \"created_at\" DESC, \"id\" DESC \
             LIMIT ${limit_ph}"
        );
        let mut all = Vec::with_capacity(1 + auth_binds.len() + 3);
        all.push(EqBind::Text(hs.query.clone()));
        all.extend(auth_binds);
        all.push(EqBind::Text(qvec_text));
        all.push(EqBind::I64(k));
        all.push(EqBind::I64(i64::from(hs.limit)));
        return Ok(CompiledQuery {
            sql,
            binds: all,
            terminal: "hybridSearch",
        });
    };

    // ENH-030, paginated form. The unpaginated query computes the RRF score
    // inline in its final ORDER BY, where a WHERE at the same level cannot see
    // it (a SELECT-list computation is not in scope for its own WHERE). Two
    // more CTE stages fix that without a second round-trip: `scored` turns the
    // fusion into a real column, and `topk` applies `limit` — which keeps its
    // unpaginated meaning as the size of the ranked candidate POOL, with
    // `numItems` slicing that pool into pages. The keyset predicate then sits
    // on the outer SELECT over `topk`, where `score` is an ordinary column.
    //
    // `::float8` makes `score` a double rather than the `numeric` the `1.0`
    // literals would otherwise produce, so the value the executor mints into
    // the cursor is bit-identical to the one the resume predicate compares.
    let cursor_start = limit_ph + 1;
    let (cursor_predicate, cursor_binds) =
        ranked_cursor_clause(paginate, "score", "DESC", cursor_start)?;
    let cursor_clause = if cursor_predicate.is_empty() {
        String::new()
    } else {
        format!(" WHERE {cursor_predicate}")
    };
    let page_limit_ph = cursor_start + cursor_binds.len();
    let sql = format!(
        "{ranked_ctes}, scored AS ( \
           SELECT \"id\", \"doc\", \"created_at\", \"version\", \
                  (1.0/(${k_ph} + r_text) + 1.0/(${k_ph} + r_vec))::float8 AS score \
           FROM ranked \
         ), topk AS ( \
           SELECT \"id\", \"doc\", \"created_at\", \"version\", score FROM scored \
           ORDER BY score DESC, \"created_at\" DESC, \"id\" DESC \
           LIMIT ${limit_ph} \
         ) \
         SELECT \"id\", \"doc\", \"created_at\", \"version\", score FROM topk{cursor_clause} \
         ORDER BY score DESC, \"created_at\" DESC, \"id\" DESC \
         LIMIT ${page_limit_ph}"
    );
    let mut all = Vec::with_capacity(1 + auth_binds.len() + cursor_binds.len() + 4);
    all.push(EqBind::Text(hs.query.clone()));
    all.extend(auth_binds);
    all.push(EqBind::Text(qvec_text));
    all.push(EqBind::I64(k));
    all.push(EqBind::I64(i64::from(hs.limit)));
    all.extend(cursor_binds);
    // One extra row so a next page is detectable without a second round-trip.
    all.push(EqBind::I64(i64::from(paginate.num_items.min(MAX_TAKE)) + 1));
    Ok(CompiledQuery {
        sql,
        binds: all,
        terminal: "hybridSearch",
    })
}

pub(crate) async fn execute_hybrid_search(
    cq: CompiledQuery,
    pool: &PgPool,
) -> Result<QueryResult, RtDbError> {
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
    let rows = query.fetch_all(pool).await?;
    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Docs(docs))
}

/// Execute tail shared by `search`/`vectorSearch`/`hybridSearch` when the
/// query carries a `paginate` block (ENH-030). Every one of the three compiles
/// to the same paginated row shape — the terminal's four document columns, the
/// `search` snippet when opted in, and the ranking key as the trailing column
/// — so one tail serves all three, the same way `execute_collect_terminal`
/// serves `unique`/`first`/`collect`.
///
/// `num_items` is the requested page size already capped to `MAX_TAKE`; the
/// compiled `LIMIT` asked for one more than that, so a full-plus-one fetch is
/// the has-next signal. The next cursor is minted from the page's LAST row
/// after the probe row is discarded — the ranking key is the value the ORDER BY
/// compared, so resuming lands exactly one row past it.
pub(crate) async fn execute_ranked_paginated(
    cq: CompiledQuery,
    pool: &PgPool,
    num_items: u32,
    snippet: bool,
) -> Result<QueryResult, RtDbError> {
    let CompiledQuery { sql, binds, .. } = cq;
    // `(id, doc, created_at, version, snippet?, rank)` — the snippet column is
    // present only for `search` with `snippet: true`, so the two row shapes are
    // fetched separately and normalized to one tuple.
    type RankedRow = (String, serde_json::Value, i64, i64, Option<String>, f64);
    let mut rows: Vec<RankedRow> = if snippet {
        let mut query =
            sqlx::query_as::<_, (String, serde_json::Value, i64, i64, String, f64)>(&sql);
        for bind in binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        query
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|(id, doc, created_at, version, snip, rank)| {
                (id, doc, created_at, version, Some(snip), rank)
            })
            .collect()
    } else {
        let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64, f64)>(&sql);
        for bind in binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
                EqBind::I64(v) => query.bind(v),
            };
        }
        query
            .fetch_all(pool)
            .await?
            .into_iter()
            .map(|(id, doc, created_at, version, rank)| (id, doc, created_at, version, None, rank))
            .collect()
    };

    let has_next = rows.len() > num_items as usize;
    if has_next {
        rows.pop();
    }
    let next_cursor =
        if has_next && let Some((last_id, _, last_created_at, _, _, last_rank)) = rows.last() {
            Some(encode_cursor(&[
                serde_json::json!(last_rank),
                serde_json::json!(*last_created_at),
                serde_json::Value::String(last_id.clone()),
            ])?)
        } else {
            None
        };

    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version, snip, _)| {
            let mut merged = merge_doc(id, doc, created_at, version)?;
            // Write-time validation rejects `_`-prefixed keys, so the additive
            // field never collides with stored data (same seam as
            // `execute_search`'s non-paginated snippet path).
            if let Some(text) = snip {
                merged["_searchSnippet"] = serde_json::Value::String(text);
            }
            Ok::<_, RtDbError>(merged)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Paginated(PaginatedResult {
        docs,
        next_cursor,
    }))
}
