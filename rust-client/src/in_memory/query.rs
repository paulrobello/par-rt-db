//! Query engine (`run_query`) + index/cursor/aggregate helpers.
//!
//! Extracted from `in_memory.rs` (QA-108). Pure file movement - behavior
//! unchanged; `run_query` keeps `pub` visibility via a second
//! `impl InMemoryRtDbClient` block. `collect_index_key` and `require_index`
//! widen from private to `pub(super)` so the store (in `mod.rs`) can call them.

use super::*;

impl InMemoryRtDbClient {
    /// One-shot query — ports `executeQuery` (`ts-client/src/in_memory.ts:889-1151`).
    /// Returns the terminal result as a [`Value`]:
    /// - `get(id)` / `first` → merged doc, or [`Value::Null`] when absent.
    /// - `unique` → merged doc, or `PRECONDITION_FAILED` when more than one row
    ///   matches (and [`Value::Null`] when zero match).
    /// - `count` → number of matching rows.
    /// - `take` / `collect` → array of merged docs.
    /// - `search` → websearch-operator matched docs narrowed by an optional
    ///   `filter` (no in-memory ts_rank; result order is unspecified,
    ///   compared as a set) — quoted phrases, `or` unions, and `-term`
    ///   exclusion approximated (FM-31), optionally carrying a
    ///   `_searchSnippet` highlight per hit (`snippet: true`) — or, under
    ///   `mode: "trgm"` (FM-30), case-insensitive substring matches ranked by
    ///   approximate similarity, tie-broken and capped by `take`.
    /// - `vector_search` → all docs narrowed by an optional `filter`, capped at
    ///   `limit` (no in-memory distance model; same over-approximation as the
    ///   ts/python clients).
    ///
    /// The harness is in-process — no `{result}` wire envelope; callers either
    /// match on the [`Value`] directly or use [`run`](Self::run) for typed
    /// deserialization.
    ///
    /// `filter` is structurally validated against the table's declared fields
    /// once up front (via [`validate_filter`], mirroring the server's
    /// compile-then-execute order), then evaluated per row via
    /// [`eval_filter_expr`]. `paginate` returns the wire `Paginated<T>` shape
    /// (`{docs, nextCursor?}`) via keyset-cursor paging over the sorted set;
    /// its combination guards reject `count`/`unique`/`first`/`take`.
    pub fn run_query(&self, q: &Query) -> Result<Value, RtDbError> {
        let table_def = self.require_table(&q.table)?.clone();
        let eq = &q.eq;
        let has_range = q.gt.is_some() || q.gte.is_some() || q.lt.is_some() || q.lte.is_some();

        // `get` terminal — exclusive of every other clause.
        if let Some(id) = &q.get {
            return self.execute_get_terminal(q, id, eq, has_range);
        }

        check_query_combinations(q)?;

        // `vectorSearch` terminal — cascade mirror of server `execute_query`.
        // In-memory replica approximation: there is no pgvector distance model
        // client-side, so every table doc is a candidate; the carried `filter`
        // (the db-side `FilterExpr` DSL) narrows the set, capped at `limit`.
        // This is the same over-approximation the ts/python clients use; result
        // order is unspecified (no ranking) so callers compare as a set. QA-103:
        // the previous stub returned `[]` unconditionally, which diverged from
        // the server (and the other clients) on every non-empty match.
        if let Some(vector) = &q.vector_search {
            return self.execute_vector_search_terminal(q, vector, &table_def, eq, has_range);
        }

        // `hybridSearch` terminal — cascade mirror of server `execute_query`.
        // Standalone terminal like `vectorSearch`/`search`: rejects every other
        // peer (HYBRID_SEARCH_PEERS = all except self). In-memory replica
        // approximation: no pgvector distance + ts_rank blend client-side, so
        // every table doc is a candidate, capped at `limit`. Same
        // over-approximation the ts/python clients use; result order is
        // unspecified (no ranking) so callers compare as a set.
        if let Some(hybrid) = &q.hybrid_search {
            return self.execute_hybrid_search_terminal(q, hybrid, eq, has_range);
        }

        // `search` terminal — cascade mirror of server `execute_query`.
        // In-memory replica approximation: there is no ts_rank model
        // client-side, so the default (tsquery) mode mirrors the ts-client's
        // token-AND matching against the search index's text fields (a doc
        // matches when every whitespace token of the query appears,
        // case-insensitively, as a substring of at least one indexed field's
        // string value); the carried `filter` then narrows the candidate set.
        // Result order is unspecified (no ranking) so callers compare as a
        // set. QA-103: the previous stub returned `[]` unconditionally, which
        // diverged from the server (and the other clients) on every non-empty
        // match. `mode: "trgm"` (FM-30) instead substring-matches and ranks
        // by approximate similarity — see `execute_search_terminal`.
        if let Some(search) = &q.search {
            return self.execute_search_terminal(q, search, &table_def, eq, has_range);
        }

        let plan = prepare_scan(q, &table_def, eq, has_range)?;
        let mut filtered = self.fetch_filtered_rows(q, &plan, &table_def.fields);

        // `count` short-circuits before the sort (the count is the cardinality
        // of the filtered set, regardless of ordering).
        if q.count {
            return self.execute_count_terminal(&filtered);
        }

        // `distinct` terminal: unique values of the index field immediately
        // after the eq prefix over the matching set, sorted ascending, capped by
        // MAX_TAKE. Ports ts `executeQuery` :1355-1382 and the server's distinct
        // arm. Null index values are skipped (mirror `WHERE "<col>" IS NOT NULL`).
        if q.distinct {
            return self.execute_distinct_terminal(
                &table_def,
                plan.index_def.as_ref(),
                &plan.typed_eq,
                &filtered,
            );
        }

        // `aggregate` terminal: <OP> over the index field after the eq prefix
        // (groupBy: group by that field, aggregate the next). Ports ts
        // `executeQuery` :1391-1462 and the server's aggregate arm. Null agg
        // values are skipped (SQL SUM/AVG/MIN/MAX ignore NULL); an empty scalar
        // set yields null, an empty group yields a null `value`. Group count is
        // capped by MAX_TAKE.
        if let Some(agg) = &q.aggregate {
            return self.execute_aggregate_terminal(
                agg,
                &table_def,
                plan.index_def.as_ref(),
                &plan.typed_eq,
                &filtered,
            );
        }

        // Sort keys: unbound index fields (after the eq prefix), then
        // `_creationTime`, then `_id`. The unique `id` tiebreaker means the
        // order is total — no row is ambiguous relative to another.
        let dir = q.order.unwrap_or(Order::Asc);
        sort_filtered_rows(&mut filtered, &table_def, &plan, dir);

        // `paginate` terminal: keyset-cursor paging over the sorted set. Ports
        // TS `executeQuery` :1135-1137 → `paginateResult` (`:1164-1202`). The
        // sort columns mirror the sort above (unbound index fields after the
        // eq prefix, then `_creationTime`, then `_id`); the cursor encodes one
        // value per column.
        if let Some(pag) = &q.paginate {
            return execute_paginate_terminal(pag, &table_def, &filtered, &plan, dir);
        }

        self.execute_collect_terminal(q, filtered)
    }

    /// Row fetch + filter (eq prefix → range → filter hook) over
    /// `self.docs`, per `plan`. FM-33: a soft-deleted row is absent to every
    /// read terminal (the server composes the same live-only predicate into
    /// every scan WHERE). Mirrors the ts/python engines' `fetchFilteredRows`
    /// / `_fetch_filtered_rows`.
    fn fetch_filtered_rows(
        &self,
        q: &Query,
        plan: &ScanPlan,
        fields: &BTreeMap<String, FieldType>,
    ) -> Vec<StoredRow> {
        // Row fetch + filter (eq prefix → range → filter hook).
        let mut filtered: Vec<StoredRow> = Vec::new();
        for ((t, _id), row) in &self.docs {
            if t != &q.table {
                continue;
            }
            // FM-33: a soft-deleted row is absent to every read terminal
            // (the server composes the same live-only predicate into every
            // scan WHERE).
            if row.deleted_at.is_some() {
                continue;
            }
            if let Some(idx) = &plan.index_def {
                let mut ok = true;
                for (i, tv) in plan.typed_eq.iter().enumerate() {
                    match row.doc.get(&idx.fields[i]) {
                        Some(v) if !v.is_null() && v == tv => {}
                        _ => {
                            ok = false;
                            break;
                        }
                    }
                }
                if !ok {
                    continue;
                }
            }
            if let Some(field) = plan.range_field.as_deref() {
                let v = match row.doc.get(field) {
                    Some(v) if !v.is_null() => v,
                    _ => continue,
                };
                if let Some(bound) = &plan.gt
                    && compare_index_values(v, bound, plan.range_field_pg)
                        != std::cmp::Ordering::Greater
                {
                    continue;
                }
                if let Some(bound) = &plan.gte
                    && compare_index_values(v, bound, plan.range_field_pg)
                        == std::cmp::Ordering::Less
                {
                    continue;
                }
                if let Some(bound) = &plan.lt
                    && compare_index_values(v, bound, plan.range_field_pg)
                        != std::cmp::Ordering::Less
                {
                    continue;
                }
                if let Some(bound) = &plan.lte
                    && compare_index_values(v, bound, plan.range_field_pg)
                        == std::cmp::Ordering::Greater
                {
                    continue;
                }
            }
            if let Some(expr) = &q.filter
                && !matches_filter(expr, &row.doc, fields)
            {
                continue;
            }
            filtered.push(row.clone());
        }
        filtered
    }

    /// `get` terminal — exclusive of every other clause.
    fn execute_get_terminal(
        &self,
        q: &Query,
        id: &str,
        eq: &[Value],
        has_range: bool,
    ) -> Result<Value, RtDbError> {
        if q.index.is_some()
            || !eq.is_empty()
            || has_range
            || q.order.is_some()
            || q.take.is_some()
            || q.unique
            || q.first
            || q.count
            || q.distinct
            || q.aggregate.is_some()
            || q.paginate.is_some()
            || q.filter.is_some()
            || q.search.is_some()
            || q.vector_search.is_some()
            || q.hybrid_search.is_some()
        {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "get cannot be combined with index, eq, range bounds, order, take, \
                 unique, first, count, distinct, aggregate, paginate, filter, search, \
                 vector search, or hybrid search",
            ));
        }
        // The DSL `get` terminal reuses the point-read primitive so the
        // system-field merge path is shared with the Task 2 helper.
        Ok(self.get(&q.table, id).unwrap_or(Value::Null))
    }

    /// `vectorSearch` terminal — cascade mirror of server `execute_query`.
    /// In-memory replica approximation: there is no pgvector distance model
    /// client-side, so every table doc is a candidate; the carried `filter`
    /// (the db-side `FilterExpr` DSL) narrows the set, capped at `limit`.
    /// This is the same over-approximation the ts/python clients use; result
    /// order is unspecified (no ranking) so callers compare as a set. QA-103:
    /// the previous stub returned `[]` unconditionally, which diverged from
    /// the server (and the other clients) on every non-empty match.
    fn execute_vector_search_terminal(
        &self,
        q: &Query,
        vector: &crate::wire::VectorSearchQuery,
        table_def: &TableDef,
        eq: &[Value],
        has_range: bool,
    ) -> Result<Value, RtDbError> {
        if q.index.is_some()
            || !eq.is_empty()
            || has_range
            || q.order.is_some()
            || q.unique
            || q.first
            || q.count
            || q.distinct
            || q.aggregate.is_some()
            || q.paginate.is_some()
            || q.filter.is_some()
            || q.search.is_some()
            || q.hybrid_search.is_some()
            || q.take.is_some()
        {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "vectorSearch cannot be combined with any other terminal",
            ));
        }
        if let Some(filter) = &vector.filter {
            let fields: BTreeSet<String> = table_def.fields.keys().cloned().collect();
            validate_filter(filter, &fields)?;
        }
        let mut rows: Vec<Value> = self.collect_all(&q.table);
        if let Some(filter) = &vector.filter {
            rows.retain(|d| matches_filter(filter, d, &table_def.fields));
        }
        rows.truncate(vector.limit as usize);
        Ok(Value::Array(rows))
    }

    /// `hybridSearch` terminal — cascade mirror of server `execute_query`.
    /// Standalone terminal like `vectorSearch`/`search`: rejects every other
    /// peer (HYBRID_SEARCH_PEERS = all except self). In-memory replica
    /// approximation: no pgvector distance + ts_rank blend client-side, so
    /// every table doc is a candidate, capped at `limit`. Same
    /// over-approximation the ts/python clients use; result order is
    /// unspecified (no ranking) so callers compare as a set.
    fn execute_hybrid_search_terminal(
        &self,
        q: &Query,
        hybrid: &crate::wire::HybridSearchQuery,
        eq: &[Value],
        has_range: bool,
    ) -> Result<Value, RtDbError> {
        if q.index.is_some()
            || !eq.is_empty()
            || has_range
            || q.order.is_some()
            || q.unique
            || q.first
            || q.count
            || q.distinct
            || q.aggregate.is_some()
            || q.paginate.is_some()
            || q.filter.is_some()
            || q.search.is_some()
            || q.vector_search.is_some()
            || q.take.is_some()
        {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "hybridSearch cannot be combined with any other terminal",
            ));
        }
        let mut rows: Vec<Value> = self.collect_all(&q.table);
        rows.truncate(hybrid.limit as usize);
        Ok(Value::Array(rows))
    }

    /// `search` terminal — cascade mirror of server `execute_query`.
    /// In-memory replica approximation, by mode:
    /// - tsquery (the default, and explicit `SearchMode::Tsquery`): there is
    ///   no ts_rank model client-side, so matching approximates the server's
    ///   `websearch_to_tsquery` (FM-31) with plain string ops — quoted
    ///   phrases require their words adjacently, the bare word `or` unions
    ///   adjacent operands, `-term`/`-"phrase"` excludes, and every other
    ///   term must appear (case-insensitively, as a substring of at least
    ///   one indexed field's string value — the pre-FM-31 token-AND
    ///   approximation, so operator-free queries keep their meaning).
    ///   QA-103: the previous stub returned `[]` unconditionally, which
    ///   diverged from the server (and the other clients) on every non-empty
    ///   match.
    /// - trgm (FM-30): case-insensitive substring match (the WHOLE query as
    ///   a contiguous substring of an indexed field's string value), ranked
    ///   by the approximate similarity the three harnesses pin for
    ///   cross-client agreement: query.len()/field.len() per containing
    ///   field, max per doc (a shorter containing field is more similar),
    ///   then created_at desc and id desc tiebreaks. `take` (already capped
    ///   to MAX_TAKE in `run_query`) limits the ranked result.
    ///
    /// `snippet: true` (tsquery mode only — rejected with `trgm`, mirroring
    /// server `compile_search`) attaches a `_searchSnippet` to every hit: a
    /// ≤35-word excerpt over the index's fields with matched words wrapped in
    /// `<mark>…</mark>` (the harness's `ts_headline` approximation). In both
    /// modes the carried `filter` narrows the candidate set.
    fn execute_search_terminal(
        &self,
        q: &Query,
        search: &crate::wire::SearchQuery,
        table_def: &TableDef,
        eq: &[Value],
        has_range: bool,
    ) -> Result<Value, RtDbError> {
        if q.index.is_some()
            || !eq.is_empty()
            || has_range
            || q.order.is_some()
            || q.unique
            || q.first
            || q.count
            || q.distinct
            || q.aggregate.is_some()
            || q.paginate.is_some()
            || q.filter.is_some()
            || q.vector_search.is_some()
            || q.hybrid_search.is_some()
        {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "search cannot be combined with index, eq, range bounds, order, \
                 unique, first, count, distinct, aggregate, paginate, filter, \
                 vector search, or hybrid search",
            ));
        }
        // Shared prologue (guard → empty query → search index → filter), run
        // before the mode branch so both modes reject identically — mirrors
        // server `compile_search` and the ts/python harnesses.
        if search.query.trim().is_empty() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "search query text must not be empty",
            ));
        }
        let search_not_found = || {
            RtDbError::new(
                ErrorCode::BadRequest,
                format!("search index '{}' not found", search.index),
            )
        };
        let index_def = table_def
            .indexes
            .as_ref()
            .and_then(|indexes| indexes.iter().find(|i| i.name == search.index && i.search))
            .ok_or_else(search_not_found)?;
        let index_fields: Vec<String> = index_def.fields.clone();
        // `snippet` needs a tsquery tree to highlight; trgm mode matches raw
        // substrings, so the combination is rejected rather than silently
        // ignored. Mirrors server `compile_search`, which runs this check
        // after index resolution but before filter compilation — so this
        // error wins over a filter-validation error, like on the server.
        let snippet = search.snippet.unwrap_or(false);
        if snippet && search.mode == Some(crate::wire::SearchMode::Trgm) {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "snippet is only supported in tsquery mode",
            ));
        }
        if let Some(filter) = &search.filter {
            let fields: BTreeSet<String> = table_def.fields.keys().cloned().collect();
            validate_filter(filter, &fields)?;
        }
        let mut rows: Vec<Value> = self.collect_all(&q.table);
        if search.mode == Some(crate::wire::SearchMode::Trgm) {
            if let Some(filter) = &search.filter {
                rows.retain(|d| matches_filter(filter, d, &table_def.fields));
            }
            let needle = search.query.to_lowercase();
            let query_len = search.query.len();
            let mut scored: Vec<(f64, i64, String, Value)> = Vec::new();
            for doc in rows {
                let mut best: Option<f64> = None;
                for field in &index_fields {
                    if let Some(text) = doc.get(field).and_then(Value::as_str)
                        && text.to_lowercase().contains(&needle)
                    {
                        // Empty fields cannot contain a non-empty query;
                        // the 0.0 guard only pins the empty-query-on-empty
                        // field case to a finite score instead of NaN.
                        let score = if text.is_empty() {
                            0.0
                        } else {
                            query_len as f64 / text.len() as f64
                        };
                        best = Some(best.map_or(score, |b: f64| b.max(score)));
                    }
                }
                if let Some(score) = best {
                    let created_at = doc
                        .get("_creationTime")
                        .and_then(Value::as_i64)
                        .unwrap_or(i64::MIN);
                    let id = doc
                        .get("_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string();
                    scored.push((score, created_at, id, doc));
                }
            }
            scored.sort_by(|a, b| {
                b.0.total_cmp(&a.0)
                    .then_with(|| b.1.cmp(&a.1))
                    .then_with(|| b.2.cmp(&a.2))
            });
            let limit = q.take.map(|t| t as usize).unwrap_or(MAX_TAKE);
            scored.truncate(limit);
            return Ok(Value::Array(
                scored.into_iter().map(|(_, _, _, doc)| doc).collect(),
            ));
        }
        // tsquery (default) mode — websearch operator approximation (FM-31).
        // A doc matches when no excluded operand hits and every or-group has
        // at least one operand hitting some indexed field; plain terms keep
        // the pre-FM-31 token-AND meaning (each term a case-insensitive
        // substring of some indexed field).
        let parsed = parse_websearch_query(&search.query);
        rows.retain(|doc| {
            let texts = search_field_texts(doc, &index_fields);
            !parsed
                .excluded
                .iter()
                .any(|op| search_operand_matches(op, &texts))
                && parsed
                    .groups
                    .iter()
                    .all(|g| g.iter().any(|op| search_operand_matches(op, &texts)))
        });
        if let Some(filter) = &search.filter {
            rows.retain(|d| matches_filter(filter, d, &table_def.fields));
        }
        if snippet {
            attach_search_snippets(&mut rows, &index_fields, &parsed);
        }
        Ok(Value::Array(rows))
    }

    /// `count` terminal — short-circuits before the sort (the count is the
    /// cardinality of the filtered set, regardless of ordering).
    fn execute_count_terminal(&self, filtered: &[StoredRow]) -> Result<Value, RtDbError> {
        Ok(Value::Number(serde_json::Number::from(
            filtered.len() as i64
        )))
    }

    /// `distinct` terminal: unique values of the index field immediately after
    /// the eq prefix over the matching set, sorted ascending, capped by
    /// MAX_TAKE. Ports ts `executeQuery` :1355-1382 and the server's distinct
    /// arm. Null index values are skipped (mirror `WHERE "<col>" IS NOT NULL`).
    fn execute_distinct_terminal(
        &self,
        table_def: &TableDef,
        index_def: Option<&IndexDef>,
        typed_eq: &[Value],
        filtered: &[StoredRow],
    ) -> Result<Value, RtDbError> {
        let idx = index_def.ok_or_else(|| {
            RtDbError::new(
                ErrorCode::BadRequest,
                "distinct requires an index field beyond the eq prefix",
            )
        })?;
        if typed_eq.len() >= idx.fields.len() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "distinct requires an index field beyond the eq prefix",
            ));
        }
        let field = idx.fields[typed_eq.len()].as_str();
        let field_pg = table_def
            .fields
            .get(field)
            .and_then(|ty| index_column_type(ty).ok())
            .map(|it| it.pg)
            .unwrap_or(PgType::Text);
        let mut seen: BTreeSet<String> = BTreeSet::new();
        let mut values: Vec<Value> = Vec::new();
        for row in filtered {
            let Some(v) = row.doc.get(field) else {
                continue;
            };
            if v.is_null() {
                continue;
            }
            // Canonical JSON key so equal scalars dedupe.
            if seen.insert(v.to_string()) {
                values.push(v.clone());
            }
        }
        values.sort_by(|a, b| compare_index_values(a, b, field_pg));
        let out: Vec<Value> = values.into_iter().take(MAX_TAKE).collect();
        Ok(Value::Array(out))
    }

    /// `aggregate` terminal: <OP> over the index field after the eq prefix
    /// (groupBy: group by that field, aggregate the next). Ports ts
    /// `executeQuery` :1391-1462 and the server's aggregate arm. Null agg
    /// values are skipped (SQL SUM/AVG/MIN/MAX ignore NULL); an empty scalar
    /// set yields null, an empty group yields a null `value`. Group count is
    /// capped by MAX_TAKE.
    fn execute_aggregate_terminal(
        &self,
        agg: &crate::wire::AggregateSpec,
        table_def: &TableDef,
        index_def: Option<&IndexDef>,
        typed_eq: &[Value],
        filtered: &[StoredRow],
    ) -> Result<Value, RtDbError> {
        let idx = index_def.ok_or_else(|| {
            RtDbError::new(
                ErrorCode::BadRequest,
                "aggregate requires an index field beyond the eq prefix",
            )
        })?;
        let eq_len = typed_eq.len();
        // `count` aggregates matching rows and consumes no aggregate field
        // (mirrors `server/src/query.rs::AggregateOp::needs_field`). Scalar
        // count = number of matching rows (0 if none, never null); grouped
        // count = the size of each group.
        if matches!(agg.op, AggregateOp::Count) {
            if agg.group_by {
                let group_field = idx.fields.get(eq_len).ok_or_else(|| {
                    RtDbError::new(
                        ErrorCode::BadRequest,
                        "aggregate groupBy requires an index field beyond the eq prefix",
                    )
                })?;
                let group_field_pg = table_def
                    .fields
                    .get(group_field.as_str())
                    .and_then(|ty| index_column_type(ty).ok())
                    .map(|it| it.pg)
                    .unwrap_or(PgType::Text);
                let mut groups: Vec<(Value, u64)> = Vec::new();
                let mut group_index: HashMap<String, usize> = HashMap::new();
                for row in filtered {
                    let Some(k) = row.doc.get(group_field.as_str()) else {
                        continue;
                    };
                    if k.is_null() {
                        continue;
                    }
                    let key = k.to_string();
                    let i = match group_index.get(&key).copied() {
                        Some(i) => i,
                        None => {
                            let i = groups.len();
                            group_index.insert(key, i);
                            groups.push((k.clone(), 0));
                            i
                        }
                    };
                    groups[i].1 += 1;
                }
                let mut out: Vec<Value> = groups
                    .into_iter()
                    .map(|(k, count)| {
                        let mut obj = Map::new();
                        obj.insert("key".to_string(), k);
                        obj.insert(
                            "value".to_string(),
                            Value::Number(serde_json::Number::from(count)),
                        );
                        Value::Object(obj)
                    })
                    .collect();
                out.sort_by(|a, b| compare_index_values(&a["key"], &b["key"], group_field_pg));
                let out: Vec<Value> = out.into_iter().take(MAX_TAKE).collect();
                return Ok(Value::Array(out));
            }
            // Scalar count: number of matching rows (0 if none, never null).
            return Ok(Value::Number(serde_json::Number::from(
                filtered.len() as i64
            )));
        }
        let (group_field, agg_field) = if agg.group_by {
            if eq_len + 1 >= idx.fields.len() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "aggregate groupBy requires two index fields beyond the eq prefix",
                ));
            }
            (
                Some(idx.fields[eq_len].as_str()),
                idx.fields[eq_len + 1].as_str(),
            )
        } else {
            if eq_len >= idx.fields.len() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "aggregate requires an index field beyond the eq prefix",
                ));
            }
            (None, idx.fields[eq_len].as_str())
        };
        let agg_field_pg = table_def
            .fields
            .get(agg_field)
            .and_then(|ty| index_column_type(ty).ok())
            .map(|it| it.pg)
            .unwrap_or(PgType::Text);
        let op_name = match agg.op {
            AggregateOp::Sum => "sum",
            AggregateOp::Avg => "avg",
            AggregateOp::Min => "min",
            AggregateOp::Max => "max",
            // Count returns early above; this arm is unreachable but keeps
            // the match exhaustive as the enum grows.
            AggregateOp::Count => "count",
        };
        if matches!(agg.op, AggregateOp::Sum | AggregateOp::Avg)
            && !matches!(agg_field_pg, PgType::Number | PgType::Int64)
        {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!("aggregate op {op_name} requires a numeric index field"),
            ));
        }
        if let Some(group_field) = group_field {
            let group_field_pg = table_def
                .fields
                .get(group_field)
                .and_then(|ty| index_column_type(ty).ok())
                .map(|it| it.pg)
                .unwrap_or(PgType::Text);
            // Group rows by `group_field` (skip null keys), preserving
            // first-seen order; sort by key ascending after, for parity with
            // the server's `ORDER BY k`.
            let mut groups: Vec<(Value, Vec<Value>)> = Vec::new();
            let mut group_index: HashMap<String, usize> = HashMap::new();
            for row in filtered {
                let Some(k) = row.doc.get(group_field) else {
                    continue;
                };
                if k.is_null() {
                    continue;
                }
                let key = k.to_string();
                let i = match group_index.get(&key).copied() {
                    Some(i) => i,
                    None => {
                        let i = groups.len();
                        group_index.insert(key, i);
                        groups.push((k.clone(), Vec::new()));
                        i
                    }
                };
                if let Some(v) = row.doc.get(agg_field)
                    && !v.is_null()
                {
                    groups[i].1.push(v.clone());
                }
            }
            let mut out: Vec<Value> = groups
                .into_iter()
                .map(|(k, vs)| {
                    let value = if vs.is_empty() {
                        Value::Null
                    } else {
                        apply_aggregate(agg.op, &vs, agg_field_pg)
                    };
                    let mut obj = Map::new();
                    obj.insert("key".to_string(), k);
                    obj.insert("value".to_string(), value);
                    Value::Object(obj)
                })
                .collect();
            out.sort_by(|a, b| compare_index_values(&a["key"], &b["key"], group_field_pg));
            let out: Vec<Value> = out.into_iter().take(MAX_TAKE).collect();
            return Ok(Value::Array(out));
        }
        // Scalar aggregate.
        let values: Vec<Value> = filtered
            .iter()
            .filter_map(|row| row.doc.get(agg_field))
            .filter(|v| !v.is_null())
            .cloned()
            .collect();
        if values.is_empty() {
            return Ok(Value::Null);
        }
        Ok(apply_aggregate(agg.op, &values, agg_field_pg))
    }

    /// `collect` tail of `run_query` — the fallthrough after every standalone
    /// terminal: `unique` (PreconditionFailed on >1 match), `first`, or
    /// `take`-bounded `collect`. Mirrors the server/ts/python
    /// `executeCollect` terminal.
    fn execute_collect_terminal(
        &self,
        q: &Query,
        filtered: Vec<StoredRow>,
    ) -> Result<Value, RtDbError> {
        if q.unique {
            if filtered.len() > 1 {
                return Err(RtDbError::new(
                    ErrorCode::PreconditionFailed,
                    "unique query matched multiple documents",
                ));
            }
            return Ok(filtered.first().map(merge_doc).unwrap_or(Value::Null));
        }
        if q.first {
            return Ok(filtered.first().map(merge_doc).unwrap_or(Value::Null));
        }

        let limit = q.take.map(|t| t as usize).unwrap_or(MAX_TAKE);
        let out: Vec<Value> = filtered
            .into_iter()
            .take(limit)
            .map(|row| merge_doc(&row))
            .collect();
        Ok(Value::Array(out))
    }
}

/// Conflicting-terminal guards, in the server's validation order: each
/// terminal rejects the peers it cannot compose with, then the range-bound
/// and take-cap checks apply to every remaining shape. Mirrors the ts/python
/// engines' `checkQueryCombinations` / `_check_query_combinations`.
fn check_query_combinations(q: &Query) -> Result<(), RtDbError> {
    // Conflicting-terminal guards (ports :919-939).
    if q.unique && (q.take.is_some() || q.order.is_some() || q.distinct || q.aggregate.is_some()) {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            "unique cannot be combined with take, order, distinct, or aggregate",
        ));
    }
    if q.first && q.unique {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            "first cannot be combined with unique",
        ));
    }
    if q.first && q.take.is_some() {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            "first cannot be combined with take",
        ));
    }
    if q.first && q.distinct {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            "first cannot be combined with distinct",
        ));
    }
    if q.first && q.aggregate.is_some() {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            "first cannot be combined with aggregate",
        ));
    }
    if q.count && q.unique {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            "count cannot be combined with unique",
        ));
    }
    if q.count && q.take.is_some() {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            "count cannot be combined with take",
        ));
    }
    if q.count && q.first {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            "count cannot be combined with first",
        ));
    }
    if q.count && q.order.is_some() {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            "count cannot be combined with order",
        ));
    }
    if q.count && q.distinct {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            "count cannot be combined with distinct",
        ));
    }
    if q.count && q.aggregate.is_some() {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            "count cannot be combined with aggregate",
        ));
    }
    // Paginate combination guards (ports `:940-955`): paginate is one-shot
    // paging, so it cannot also narrow to count/unique/first/take. (`get`
    // is rejected above; `order`, index, eq, and range bounds are allowed.)
    if q.paginate.is_some() {
        if q.count {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "paginate cannot be combined with count",
            ));
        }
        if q.unique {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "paginate cannot be combined with unique",
            ));
        }
        if q.first {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "paginate cannot be combined with first",
            ));
        }
        if q.take.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "paginate cannot be combined with take",
            ));
        }
    }
    if q.gt.is_some() && q.gte.is_some() {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            "gt and gte cannot both be set",
        ));
    }
    if q.lt.is_some() && q.lte.is_some() {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            "lt and lte cannot both be set",
        ));
    }
    if q.take.is_some_and(|t| t as usize > MAX_TAKE) {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            format!("take exceeds maximum of {MAX_TAKE}"),
        ));
    }

    // `distinct`/`aggregate` are standalone terminals (like `count`): they
    // compose only with index/eq/range/filter. `get`/`unique`/`first`/`count`
    // rejected their own combinations above (validated first, matching the
    // server's check order in query.rs), so these blocks only reject the
    // remaining peers each terminal owns — mirroring the server's
    // DISTINCT/AGGREGATE_INCOMPATIBLES tables.
    if q.distinct {
        if q.take.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "distinct cannot be combined with take",
            ));
        }
        if q.order.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "distinct cannot be combined with order",
            ));
        }
        if q.aggregate.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "distinct cannot be combined with aggregate",
            ));
        }
        if q.paginate.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "distinct cannot be combined with paginate",
            ));
        }
        if q.search.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "distinct cannot be combined with search",
            ));
        }
        if q.vector_search.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "distinct cannot be combined with vector search",
            ));
        }
        if q.hybrid_search.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "distinct cannot be combined with hybrid search",
            ));
        }
    }
    if q.aggregate.is_some() {
        if q.take.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "aggregate cannot be combined with take",
            ));
        }
        if q.order.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "aggregate cannot be combined with order",
            ));
        }
        if q.paginate.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "aggregate cannot be combined with paginate",
            ));
        }
        if q.search.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "aggregate cannot be combined with search",
            ));
        }
        if q.vector_search.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "aggregate cannot be combined with vector search",
            ));
        }
        if q.hybrid_search.is_some() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "aggregate cannot be combined with hybrid search",
            ));
        }
    }
    Ok(())
}

/// Everything the row scan needs besides the query itself: the resolved
/// index, the type-checked eq prefix, and the coerced range bounds. Produced
/// once by `prepare_scan`, consumed by `fetch_filtered_rows` /
/// `sort_filtered_rows` / `execute_paginate_terminal`. Mirrors the ts/python
/// engines' `ScanPlan` / `_ScanPlan`.
struct ScanPlan {
    index_def: Option<IndexDef>,
    typed_eq: Vec<Value>,
    range_field: Option<String>,
    range_field_pg: PgType,
    gt: Option<Value>,
    gte: Option<Value>,
    lt: Option<Value>,
    lte: Option<Value>,
}

/// Index resolution, eq-prefix binding, range-bound coercion, and one-time
/// filter validation — everything the row scan needs before touching a row.
/// Mirrors the ts/python engines' `prepareScan` / `_prepare_scan`.
fn prepare_scan(
    q: &Query,
    table_def: &TableDef,
    eq: &[Value],
    has_range: bool,
) -> Result<ScanPlan, RtDbError> {
    // Resolve index — required for `eq` and for any range bound.
    let index_def: Option<IndexDef> = match &q.index {
        Some(name) => Some(require_index(table_def, name)?.clone()),
        None if !eq.is_empty() => {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "eq requires an index",
            ));
        }
        _ => None,
    };

    // eq-arity check (server `eq_binds` length guard at :1033-1038).
    if let Some(idx) = &index_def
        && eq.len() > idx.fields.len()
    {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            format!(
                "index '{}' expects at most {} eq value(s), got {}",
                idx.name,
                idx.fields.len(),
                eq.len()
            ),
        ));
    }

    // Type-check each eq prefix bind positionally.
    let typed_eq: Vec<Value> = match &index_def {
        Some(idx) => {
            let mut out = Vec::with_capacity(eq.len());
            for (i, value) in eq.iter().enumerate() {
                out.push(coerce_index_value(table_def, &idx.fields[i], value)?);
            }
            out
        }
        None => Vec::new(),
    };

    // Range bounds apply to the next index field after the eq prefix.
    let range_field: Option<String> = if has_range {
        let idx = index_def.as_ref().ok_or_else(|| {
            RtDbError::new(ErrorCode::BadRequest, "range bound requires an index")
        })?;
        if eq.len() >= idx.fields.len() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "range bound requires a remaining index field after eq",
            ));
        }
        Some(idx.fields[eq.len()].clone())
    } else {
        None
    };
    // The range field's storage type selects the comparison domain for the
    // bound checks below (int64 sorts numerically). `coerce_index_value`
    // already validated indexability when binding each bound, so the lookup
    // is guaranteed to succeed; the `Text` fallback is purely defensive.
    let range_field_pg: PgType = match range_field.as_deref() {
        Some(f) => table_def
            .fields
            .get(f)
            .and_then(|ty| index_column_type(ty).ok())
            .map(|it| it.pg)
            .unwrap_or(PgType::Text),
        None => PgType::Text,
    };
    let gt = match (&q.gt, range_field.as_deref()) {
        (Some(v), Some(f)) => Some(coerce_index_value(table_def, f, v)?),
        _ => None,
    };
    let gte = match (&q.gte, range_field.as_deref()) {
        (Some(v), Some(f)) => Some(coerce_index_value(table_def, f, v)?),
        _ => None,
    };
    let lt = match (&q.lt, range_field.as_deref()) {
        (Some(v), Some(f)) => Some(coerce_index_value(table_def, f, v)?),
        _ => None,
    };
    let lte = match (&q.lte, range_field.as_deref()) {
        (Some(v), Some(f)) => Some(coerce_index_value(table_def, f, v)?),
        _ => None,
    };

    // Compile the filter against the table's declared fields once up front,
    // mirroring the server's compile-then-execute order. Surfaces the
    // BAD_REQUEST cases (unknown field, empty and/or/in, mixed-type `in`
    // values, wrong value-kind) before any row is touched.
    if let Some(filter) = &q.filter {
        let fields: BTreeSet<String> = table_def.fields.keys().cloned().collect();
        validate_filter(filter, &fields)?;
    }
    Ok(ScanPlan {
        index_def,
        typed_eq,
        range_field,
        range_field_pg,
        gt,
        gte,
        lt,
        lte,
    })
}

/// Sorts the filtered set in place by the shared sort columns (unbound index
/// fields after the eq prefix, then `_creationTime`, then `_id`) in direction
/// `dir`. The unique `id` tiebreaker means the order is total — no row is
/// ambiguous relative to another. Mirrors the ts/python engines'
/// `sortFilteredRows` / `_sort_filtered_rows`.
fn sort_filtered_rows(
    filtered: &mut [StoredRow],
    table_def: &TableDef,
    plan: &ScanPlan,
    dir: Order,
) {
    // Per-sort-column storage types — the comparator needs the domain to
    // pick numeric vs lexicographic ordering (int64 indexes store decimal
    // strings, which would otherwise sort lexicographically). The eq prefix
    // and range field have already been validated as indexable by
    // `coerce_index_value`; any remaining index field is schema-declared
    // indexable, so the lookup is total — the `Text` fallback is defensive.
    let sort_field_pgs: Vec<PgType> = match &plan.index_def {
        Some(idx) => idx.fields[plan.typed_eq.len()..]
            .iter()
            .map(|f| {
                table_def
                    .fields
                    .get(f)
                    .and_then(|ty| index_column_type(ty).ok())
                    .map(|it| it.pg)
                    .unwrap_or(PgType::Text)
            })
            .collect(),
        None => Vec::new(),
    };
    filtered.sort_by(|a, b| {
        if let Some(idx) = &plan.index_def {
            for (i, field) in idx.fields[plan.typed_eq.len()..].iter().enumerate() {
                let av = a.doc.get(field).unwrap_or(&Value::Null);
                let bv = b.doc.get(field).unwrap_or(&Value::Null);
                let cmp = compare_index_values(av, bv, sort_field_pgs[i]);
                if cmp != std::cmp::Ordering::Equal {
                    return dir_order(cmp, dir);
                }
            }
        }
        let cmp = a.created_at.cmp(&b.created_at);
        if cmp != std::cmp::Ordering::Equal {
            return dir_order(cmp, dir);
        }
        dir_order(a.id.cmp(&b.id), dir)
    });
}

/// `paginate` terminal: keyset-cursor paging over the already-filtered,
/// already-sorted set. The sort columns mirror the producing sort (unbound
/// index fields after the eq prefix, then `_creationTime`, then `_id`); the
/// cursor encodes one value per column. Mirrors the ts/python engines'
/// `executePaginateTerminal` / `_execute_paginate_terminal`.
fn execute_paginate_terminal(
    pag: &crate::query::Paginate,
    table_def: &TableDef,
    filtered: &[StoredRow],
    plan: &ScanPlan,
    dir: Order,
) -> Result<Value, RtDbError> {
    let mut sort_cols: Vec<SortCol> = Vec::new();
    if let Some(idx) = &plan.index_def {
        for field in idx.fields[plan.typed_eq.len()..].iter() {
            sort_cols.push(SortCol::Index(field.clone()));
        }
    }
    sort_cols.push(SortCol::CreatedAt);
    sort_cols.push(SortCol::Id);
    // Mirror the sort caller's per-column storage types so keyset
    // resume agrees with the ordering that produced `filtered`.
    let col_types: Vec<PgType> = sort_cols
        .iter()
        .map(|c| match c {
            SortCol::Index(field) => table_def
                .fields
                .get(field)
                .and_then(|ty| index_column_type(ty).ok())
                .map(|it| it.pg)
                .unwrap_or(PgType::Text),
            SortCol::CreatedAt => PgType::Number,
            SortCol::Id => PgType::Text,
        })
        .collect();
    paginate_result(pag, table_def, filtered, &sort_cols, &col_types, dir)
}

/// One operand of a parsed websearch query (the FM-31 harness approximation
/// of the server's `websearch_to_tsquery`): a bare word — matched as a
/// case-insensitive substring, like the pre-FM-31 token-AND approximation —
/// or a quoted phrase, whose words must appear adjacently (matched as the
/// space-joined phrase being a substring of a field, which pins adjacency
/// without a stemming model).
#[derive(Debug, Clone, PartialEq)]
enum SearchOperand {
    Term(String),
    Phrase(Vec<String>),
}

/// Parsed approximation of the server's `websearch_to_tsquery` (FM-31).
/// `groups` are ANDed; within a group — a maximal run of operands separated
/// by the bare word `or` — any operand matching suffices. `excluded`
/// collects the `-term` / `-"phrase"` operands a matching doc must NOT
/// contain. Plain terms stay ANDed exactly as before the server's FM-31
/// upgrade (a pure superset); stemming is over-approximated as substring
/// containment, as the harness always has.
#[derive(Debug, Clone, Default)]
struct WebsearchQuery {
    groups: Vec<Vec<SearchOperand>>,
    excluded: Vec<SearchOperand>,
}

/// Parses web-search operator syntax: `"exact phrase"` → adjacent-words
/// phrase, the bare word `or` (outside quotes) unions adjacent operands,
/// `-term` / `-"phrase"` negates, everything else is a plain ANDed term.
/// Degenerate input (a lone `-`, an empty phrase, a trailing unterminated
/// quote) is dropped or read to end-of-string — over-approximating where the
/// server's lexing is subtler than plain string ops can be.
fn parse_websearch_query(text: &str) -> WebsearchQuery {
    let mut out = WebsearchQuery::default();
    let mut current: Vec<SearchOperand> = Vec::new();
    let mut or_pending = false;
    let mut chars = text.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
            continue;
        }
        // `-` at a token start negates the word or quoted phrase that
        // follows; a `-` standing alone (or before whitespace/end) is not an
        // operator and is skipped rather than read as an empty term.
        let negated = c == '-';
        if negated {
            chars.next();
            if !chars.peek().is_some_and(|n| !n.is_whitespace()) {
                continue;
            }
        }
        let operand = if chars.peek() == Some(&'"') {
            chars.next();
            let mut phrase = String::new();
            for ch in chars.by_ref() {
                if ch == '"' {
                    break;
                }
                phrase.push(ch);
            }
            let words: Vec<String> = phrase
                .split_whitespace()
                .map(|w| w.to_lowercase())
                .collect();
            (!words.is_empty()).then_some(SearchOperand::Phrase(words))
        } else {
            let mut word = String::new();
            while let Some(&ch) = chars.peek() {
                if ch.is_whitespace() || ch == '"' {
                    break;
                }
                word.push(ch);
                chars.next();
            }
            (!word.is_empty()).then_some(SearchOperand::Term(word.to_lowercase()))
        };
        let Some(operand) = operand else {
            continue;
        };
        // The bare word `or` is the union operator, not an operand; a
        // leading `or` (nothing to its left) is ignored.
        if !negated && matches!(&operand, SearchOperand::Term(t) if t == "or") {
            or_pending = !current.is_empty();
            continue;
        }
        if negated {
            out.excluded.push(operand);
            continue;
        }
        if or_pending {
            current.push(operand);
            or_pending = false;
        } else {
            if !current.is_empty() {
                out.groups.push(std::mem::take(&mut current));
            }
            current.push(operand);
        }
    }
    if !current.is_empty() {
        out.groups.push(current);
    }
    out
}

/// True when `op` matches any of `field_texts` (all pre-lowercased): a Term
/// is a substring of some field; a Phrase is its space-joined words
/// appearing contiguously in some field.
fn search_operand_matches(op: &SearchOperand, field_texts: &[String]) -> bool {
    match op {
        SearchOperand::Term(t) => field_texts.iter().any(|f| f.contains(t.as_str())),
        SearchOperand::Phrase(words) => {
            let joined = words.join(" ");
            field_texts.iter().any(|f| f.contains(joined.as_str()))
        }
    }
}

/// Lowercased string values of the index's fields on `doc` (missing and
/// non-string fields contribute nothing) — the searchable text per doc.
fn search_field_texts(doc: &Value, index_fields: &[String]) -> Vec<String> {
    index_fields
        .iter()
        .filter_map(|f| doc.get(f).and_then(Value::as_str))
        .map(|s| s.to_lowercase())
        .collect()
}

/// Attaches the harness's `_searchSnippet` approximation (FM-31) to every
/// hit: a plain excerpt of at most 35 words over the index's fields (joined
/// like the server's `concat_ws(' ', …)` headline source), with every word
/// containing a matched term wrapped in `<mark>…</mark>` (the server's
/// ts_headline StartSel/StopSel; MaxWords=35). The excerpt window is centered
/// on the first matched word so it shows why the doc is a hit.
fn attach_search_snippets(rows: &mut [Value], index_fields: &[String], parsed: &WebsearchQuery) {
    let terms: Vec<String> = parsed
        .groups
        .iter()
        .flatten()
        .flat_map(|op| match op {
            SearchOperand::Term(t) => vec![t.clone()],
            SearchOperand::Phrase(words) => words.clone(),
        })
        .collect();
    for doc in rows.iter_mut() {
        let text = index_fields
            .iter()
            .filter_map(|f| doc.get(f).and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(" ");
        let marked = |w: &str| {
            let lw = w.to_lowercase();
            terms.iter().any(|t| lw.contains(t.as_str()))
        };
        let words: Vec<&str> = text.split_whitespace().collect();
        let first = words.iter().position(|w| marked(w)).unwrap_or(0);
        let start = first.saturating_sub(15);
        let end = (start + 35).min(words.len());
        let snippet = words[start..end]
            .iter()
            .map(|w| {
                if marked(w) {
                    format!("<mark>{w}</mark>")
                } else {
                    (*w).to_string()
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        if let Some(obj) = doc.as_object_mut() {
            obj.insert("_searchSnippet".to_string(), Value::String(snippet));
        }
    }
}

/// Lowercase camelCase type tag for a [`FieldType`] — used in error messages
/// (mirrors `typeTag` in `ts-client/src/in_memory.ts:267-269` and the serde tag
/// on [`FieldType`]).
pub fn type_tag(ty: &FieldType) -> &'static str {
    match ty {
        FieldType::String => "string",
        FieldType::Number => "number",
        FieldType::Boolean => "boolean",
        FieldType::Null => "null",
        FieldType::Id { .. } => "id",
        FieldType::Literal { .. } => "literal",
        FieldType::Optional { .. } => "optional",
        FieldType::Union { .. } => "union",
        FieldType::Array { .. } => "array",
        FieldType::Object { .. } => "object",
        FieldType::Int64 => "int64",
        FieldType::Bytes => "bytes",
        FieldType::Any => "any",
        FieldType::Record { .. } => "record",
        FieldType::Vector { .. } => "vector",
    }
}

/// Indexed-column storage type, mirroring server `indexed_column_type` and the
/// TS `IndexedType` (`ts-client/src/in_memory.ts:43-49`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgType {
    /// String-backed indexed values.
    Text,
    /// `f64`-backed indexed values.
    Number,
    /// Boolean indexed values.
    Boolean,
    /// 64-bit integer indexed values (wire-encoded as strings).
    Int64,
}

/// Shape returned by [`index_column_type`]: the storage type plus whether the
/// source field was wrapped in `Optional` (so callers can let null sort).
#[derive(Debug, Clone, Copy)]
pub struct IndexedType {
    /// The storage type used for comparison/cursor encoding.
    pub pg: PgType,
    /// Whether the source field was `Optional` (nulls sort first).
    pub nullable: bool,
}

/// Indexable column type — a port of server `schema::indexed_column_type` and
/// the TS `indexColumnType` (`ts-client/src/in_memory.ts:271-298`). Returns
/// SCHEMA_VIOLATION for non-indexable types.
pub fn index_column_type(ty: &FieldType) -> Result<IndexedType, RtDbError> {
    let pg = match ty {
        FieldType::String | FieldType::Id { .. } => PgType::Text,
        FieldType::Number => PgType::Number,
        FieldType::Boolean => PgType::Boolean,
        FieldType::Int64 => PgType::Int64,
        FieldType::Literal {
            value: Value::String(_),
        } => PgType::Text,
        FieldType::Literal { .. } => {
            return Err(RtDbError::new(
                ErrorCode::SchemaViolation,
                format!("field type '{}' is not indexable", type_tag(ty)),
            ));
        }
        FieldType::Union { variants } => {
            if variants.iter().all(|v| {
                matches!(
                    v,
                    FieldType::Literal {
                        value: Value::String(_)
                    }
                )
            }) {
                PgType::Text
            } else {
                return Err(RtDbError::new(
                    ErrorCode::SchemaViolation,
                    format!("field type '{}' is not indexable", type_tag(ty)),
                ));
            }
        }
        FieldType::Optional { inner } => {
            let inner_ty = index_column_type(inner)?;
            return Ok(IndexedType {
                pg: inner_ty.pg,
                nullable: true,
            });
        }
        _ => {
            return Err(RtDbError::new(
                ErrorCode::SchemaViolation,
                format!("field type '{}' is not indexable", type_tag(ty)),
            ));
        }
    };
    Ok(IndexedType {
        pg,
        nullable: false,
    })
}

/// Type-checks an eq/range bind value, mirroring server `eq_bind_for` and the
/// TS `coerceIndexValue` (`ts-client/src/in_memory.ts:301-324`). Returns the
/// value unchanged on success.
pub fn coerce_index_value(
    table: &TableDef,
    field_name: &str,
    value: &Value,
) -> Result<Value, RtDbError> {
    let field_ty = table.fields.get(field_name).ok_or_else(|| {
        RtDbError::new(
            ErrorCode::Internal,
            format!("index references unknown field '{field_name}'"),
        )
    })?;
    let indexed = index_column_type(field_ty)?;
    match indexed.pg {
        PgType::Text => {
            if !value.is_string() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "eq value must be a string",
                ));
            }
        }
        PgType::Number => {
            if !value.is_number() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "eq value must be a number",
                ));
            }
        }
        PgType::Boolean => {
            if !value.is_boolean() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "eq value must be a boolean",
                ));
            }
        }
        PgType::Int64 => {
            // Int64 fields are stored as decimal strings; eq stays structural
            // equality on the string, so the value is returned unchanged. We
            // only validate that it parses as `i64` (mirrors `is_int64_string`).
            match value.as_str().and_then(|s| s.parse::<i64>().ok()) {
                Some(_) => {}
                None => {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        "eq value must be an int64 string",
                    ));
                }
            }
        }
    }
    Ok(value.clone())
}

/// Null-sorting comparison for one index sort key. Mirrors `compareIndexValues`
/// (`ts-client/src/in_memory.ts:329-350`): numbers compare numerically, strings
/// lexicographically, booleans as `false < true`; nulls sort last (asc) / first
/// (desc, via the caller flipping the result). Mixed types fall back to
/// [`Ordering::Equal`](std::cmp::Ordering) — indexed columns are single-type by schema, so this is
/// unreachable in practice.
///
/// `pg` selects the comparison domain. `PgType::Int64` parses the decimal
/// string to `i64` so int64 index values sort/range numerically (3 < 20 < 100)
/// rather than lexicographically (100 < 20 < 3); the on-the-wire representation
/// stays a string, so eq remains structural equality on the `Value`.
pub fn compare_index_values(a: &Value, b: &Value, pg: PgType) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let a_null = a.is_null();
    let b_null = b.is_null();
    if a_null && b_null {
        return Ordering::Equal;
    }
    if a_null {
        return Ordering::Greater;
    }
    if b_null {
        return Ordering::Less;
    }
    if pg == PgType::Int64 {
        let an = a
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(i64::MIN);
        let bn = b
            .as_str()
            .and_then(|s| s.parse::<i64>().ok())
            .unwrap_or(i64::MIN);
        return an.cmp(&bn);
    }
    match (a, b) {
        (Value::Number(an), Value::Number(bn)) => {
            let av = an.as_f64().unwrap_or(f64::NAN);
            let bv = bn.as_f64().unwrap_or(f64::NAN);
            av.partial_cmp(&bv).unwrap_or(Ordering::Equal)
        }
        (Value::String(as_), Value::String(bs_)) => as_.cmp(bs_),
        (Value::Bool(ab), Value::Bool(bb)) => ab.cmp(bb),
        _ => Ordering::Equal,
    }
}

/// Applies one aggregate op over a non-empty slice of values, mirroring the
/// server's SQL semantics and ts `applyAggregate` (`in_memory.ts:432-449`).
/// SUM/AVG reduce numerically (`int64` values are decimal strings → parsed);
/// MIN/MAX pick the smallest/largest per [`compare_index_values`], so a string
/// field's extremes match Postgres lexicographic ordering. Only called on
/// non-empty input — the caller maps an empty set to JSON null.
pub fn apply_aggregate(op: AggregateOp, values: &[Value], pg: PgType) -> Value {
    match op {
        AggregateOp::Sum | AggregateOp::Avg => {
            let sum: f64 = values.iter().filter_map(|v| numeric_value(v, pg)).sum();
            let result = if matches!(op, AggregateOp::Avg) {
                sum / values.len() as f64
            } else {
                sum
            };
            serde_json::Number::from_f64(result)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        AggregateOp::Min | AggregateOp::Max => {
            let want_less = matches!(op, AggregateOp::Min);
            let mut best = &values[0];
            for v in &values[1..] {
                let cmp = compare_index_values(v, best, pg);
                if (want_less && cmp == std::cmp::Ordering::Less)
                    || (!want_less && cmp == std::cmp::Ordering::Greater)
                {
                    best = v;
                }
            }
            best.clone()
        }
        // Count counts rows and is handled by an early return in the aggregate
        // path (it consumes no field); this arm is for exhaustiveness when the
        // helper is called directly — it returns the count of provided values.
        AggregateOp::Count => Value::Number(serde_json::Number::from(values.len() as i64)),
    }
}

/// Parses an index value to `f64` for SUM/AVG. `Number` columns are JSON
/// numbers; `int64` columns are decimal strings on the wire and in this harness.
fn numeric_value(v: &Value, pg: PgType) -> Option<f64> {
    match pg {
        PgType::Int64 => v.as_str().and_then(|s| s.parse::<f64>().ok()),
        _ => v.as_f64(),
    }
}

/// Merges a stored row with its system fields — a port of server `merge_doc`
/// and the TS `mergeDoc` (`ts-client/src/in_memory.ts:1154-1156`). The stored
/// `doc` is the user-written payload; system fields (`_id`/`_creationTime`/
/// `_version`) are layered on top at read time so they always reflect the
/// current `StoredRow` identity/history.
pub fn merge_doc(row: &StoredRow) -> Value {
    let mut out = match row.doc.as_object() {
        Some(m) => m.clone(),
        None => Map::new(),
    };
    out.insert("_id".to_string(), Value::String(row.id.clone()));
    out.insert(
        "_creationTime".to_string(),
        Value::Number(serde_json::Number::from(row.created_at)),
    );
    out.insert(
        "_version".to_string(),
        Value::Number(serde_json::Number::from(row.version)),
    );
    Value::Object(out)
}

/// Collect a unique-index collision key from `doc` over the declared `fields`.
/// Returns `Some([&Value; n])` positionally, or `None` if ANY indexed field is
/// absent or null in `doc` — mirroring Postgres UNIQUE, which treats NULLs as
/// distinct (a row with a NULL key column never collides). Used by
/// [`InMemoryRtDbClient::check_unique_indexes`]; the returned key lives only as
/// long as `doc` (the caller compares positionally against another key built
/// from a doc of equal or longer lifetime).
pub(super) fn collect_index_key<'a>(fields: &[String], doc: &'a Value) -> Option<Vec<&'a Value>> {
    let mut key = Vec::with_capacity(fields.len());
    for field in fields {
        match doc.get(field) {
            Some(v) if !v.is_null() => key.push(v),
            _ => return None,
        }
    }
    Some(key)
}

/// Flip an [`std::cmp::Ordering`] by the query's sort direction: identity for
/// `Asc`, reversed for `Desc`. Used by the sort comparator in
/// [`InMemoryRtDbClient::run_query`] so the same comparison serves either
/// direction. Inline in the TS source (`dir === "desc" ? -cmp : cmp`).
fn dir_order(o: std::cmp::Ordering, dir: Order) -> std::cmp::Ordering {
    match dir {
        Order::Asc => o,
        Order::Desc => o.reverse(),
    }
}

// ---------------------------------------------------------------------------
// Cursor-keyset pagination — a port of TS `paginateResult` and its helpers
// (`ts-client/src/in_memory.ts:1164-1290`). The cursor stores one value per
// sort column (unbound index fields, then `_creationTime`, then `_id`); the
// resume predicate is the standard OR-of-AND row-value comparison, so paging
// is stable — the unique `id` tiebreaker means no row is skipped or duplicated
// across pages.
// ---------------------------------------------------------------------------

/// A sort column for keyset pagination — either an indexed field or one of the
/// two synthetic tiebreakers. Mirrors the TS `sortKeys` sentinel strings
/// `__createdAt` / `__id` (`ts-client/src/in_memory.ts:1119-1120`) without
/// risking a collision with a real field name.
enum SortCol {
    Index(String),
    CreatedAt,
    Id,
}

/// Cursor keyset pagination. `sorted` is already filtered (eq/range) and
/// sorted over `sort_cols` in direction `dir`. Returns a `Value` shaped as
/// `{docs, nextCursor?}` — the wire `Paginated<T>` (camelCase field names match
/// [`crate::query::Paginated`] and the TS `PaginatedResultJson`).
///
/// Fetch one past the page size so a next page is detectable without a second
/// pass; the extra is discarded after the has-next check (server `LIMIT n+1`).
/// The next cursor is built from the page's last row; absent when the page is
/// empty or this was the final page.
fn paginate_result(
    paginate: &crate::query::Paginate,
    table_def: &TableDef,
    sorted: &[StoredRow],
    sort_cols: &[SortCol],
    col_types: &[PgType],
    dir: Order,
) -> Result<Value, RtDbError> {
    let num_items = std::cmp::min(paginate.num_items as usize, MAX_TAKE);

    // Decode + structurally validate the cursor (BAD_REQUEST on any failure —
    // the codec returns INTERNAL, so rewrap to match the live client's surface
    // and the TS `decodePaginateCursor` rethrow at `:1206-1217`).
    let cursor_values: Option<Vec<Value>> = match &paginate.cursor {
        None => None,
        Some(cursor) => {
            let decoded = crate::cursor::decode_cursor(cursor).map_err(|e| {
                RtDbError::new(
                    ErrorCode::BadRequest,
                    format!("invalid cursor: {}", e.message),
                )
            })?;
            if decoded.len() != sort_cols.len() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    format!(
                        "cursor has {} value(s) but this query sorts over {} column(s)",
                        decoded.len(),
                        sort_cols.len()
                    ),
                ));
            }
            validate_cursor_values(&decoded, sort_cols, table_def)?;
            Some(decoded)
        }
    };

    // Apply the keyset resume predicate (strictly-after in the sort direction).
    let rows: Vec<&StoredRow> = match &cursor_values {
        Some(cv) => sorted
            .iter()
            .filter(|row| is_after_cursor(row, cv, sort_cols, col_types, dir))
            .collect(),
        None => sorted.iter().collect(),
    };

    let has_next = rows.len() > num_items;
    let page: Vec<&StoredRow> = rows.into_iter().take(num_items).collect();
    let docs: Vec<Value> = page.iter().map(|row| merge_doc(row)).collect();

    let next_cursor = match (has_next, page.last()) {
        (true, Some(last)) => {
            let keyset: Vec<Value> = sort_cols.iter().map(|c| sort_value(last, c)).collect();
            Some(crate::cursor::encode_cursor(&keyset)?)
        }
        _ => None,
    };

    let mut out = Map::new();
    out.insert("docs".to_string(), Value::Array(docs));
    if let Some(nc) = next_cursor {
        out.insert("nextCursor".to_string(), Value::String(nc));
    }
    Ok(Value::Object(out))
}

/// Type-checks decoded cursor values positionally against the sort columns —
/// a port of TS `validateCursorValues`
/// (`ts-client/src/in_memory.ts:1223-1244`). Index columns use
/// [`coerce_index_value`] (null is a legitimate optional-field value, so only
/// present values are type-checked); the final two columns are always
/// `_creationTime` (number) and `_id` (string).
fn validate_cursor_values(
    cursor_values: &[Value],
    sort_cols: &[SortCol],
    table_def: &TableDef,
) -> Result<(), RtDbError> {
    for (i, col) in sort_cols.iter().enumerate() {
        let value = &cursor_values[i];
        match col {
            SortCol::Index(field) => {
                if !value.is_null() {
                    coerce_index_value(table_def, field, value)?;
                }
            }
            SortCol::CreatedAt => {
                if !value.is_number() {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        "cursor value for created_at must be a number",
                    ));
                }
            }
            SortCol::Id => {
                if !value.is_string() {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        "cursor value for id must be a string",
                    ));
                }
            }
        }
    }
    Ok(())
}

/// The keyset resume predicate: true when `row` sorts strictly after the cursor
/// row. This is the lexicographic "greater than" expanded to OR-of-AND —
///
///   (c0 OP v0) OR (c0 = v0 AND c1 OP v1) OR … —
///
/// where OP is `>` (asc) / `<` (desc). Evaluated with the same null-sorts-last
/// comparator as the sort, so it agrees with the ordering that produced
/// `sorted`. Ports `isAfterCursor` (`ts-client/src/in_memory.ts:1253-1276`).
///
/// `col_types` is the per-column storage type parallel to `sort_cols`, used to
/// select the comparison domain (int64 needs numeric parsing).
fn is_after_cursor(
    row: &StoredRow,
    cursor_values: &[Value],
    sort_cols: &[SortCol],
    col_types: &[PgType],
    dir: Order,
) -> bool {
    for i in 0..sort_cols.len() {
        let mut prefix_equal = true;
        for j in 0..i {
            let row_v = sort_value(row, &sort_cols[j]);
            if compare_index_values(&row_v, &cursor_values[j], col_types[j])
                != std::cmp::Ordering::Equal
            {
                prefix_equal = false;
                break;
            }
        }
        if !prefix_equal {
            continue;
        }
        let row_v = sort_value(row, &sort_cols[i]);
        let cmp = compare_index_values(&row_v, &cursor_values[i], col_types[i]);
        let ahead = match dir {
            Order::Asc => cmp == std::cmp::Ordering::Greater,
            Order::Desc => cmp == std::cmp::Ordering::Less,
        };
        if ahead {
            return true;
        }
    }
    false
}

/// Sort value for a column, normalizing an absent optional index field to
/// null. Ports TS `sortValue` (`ts-client/src/in_memory.ts:1281-1290`).
fn sort_value(row: &StoredRow, col: &SortCol) -> Value {
    match col {
        SortCol::CreatedAt => Value::Number(serde_json::Number::from(row.created_at)),
        SortCol::Id => Value::String(row.id.clone()),
        SortCol::Index(field) => row.doc.get(field).cloned().unwrap_or(Value::Null),
    }
}

/// Looks up an index by name (BAD_REQUEST if absent). Free function so it's
/// callable without `&self`. Ports `requireIndex`
/// (`ts-client/src/in_memory.ts:1328-1334`).
pub(super) fn require_index<'a>(
    table_def: &'a TableDef,
    name: &str,
) -> Result<&'a IndexDef, RtDbError> {
    let indexes = table_def.indexes.as_ref().ok_or_else(|| {
        RtDbError::new(ErrorCode::BadRequest, format!("index '{name}' not found"))
    })?;
    indexes
        .iter()
        .find(|i| i.name == name)
        .ok_or_else(|| RtDbError::new(ErrorCode::BadRequest, format!("index '{name}' not found")))
}
