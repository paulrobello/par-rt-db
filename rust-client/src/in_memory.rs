//! In-memory par-rt-db client for unit tests. No network, no Postgres; mirrors
//! server DSL/step-result/system-field semantics. Ports
//! `ts-client/src/in_memory.ts`.
//!
//! The server (`server/src/{txn,query,schema,protocol}.rs`) is the source of
//! truth for the declarative DSL, step-result shapes, system fields, and query
//! semantics; this client mirrors them so app code can exercise query/txn/schema
//! behavior with no network and no live Postgres. It exposes the same data
//! surface as the live clients — `push_schema`, `query` (one-shot, like
//! [`crate::RtDbHttpClient`]), `mutate`/transactions (like
//! [`crate::RtDbClient`]), and `subscribe` (reactive `query_update`s) — so a
//! test can swap it in behind a shared interface.
//!
//! Parity is deliberately scoped to the documented core (schema push, insert /
//! patch / replace / delete / expect_version / expect_absent / upsert, point
//! reads, index eq + range queries with order/take/unique/first/count, and
//! reactive subscriptions). Gaps are marked with `TODO` and return an `INTERNAL`
//! error rather than silently misbehaving.
//!
//! This module currently houses the scaffold (Task 1: struct + options +
//! `push_schema` + the validation/id/format helpers), the mutate executor
//! (Task 2: insert/patch/replace/delete/expectVersion/expectAbsent/upsert with
//! idempotency-key caching, MAX_STEPS guard, and atomic rollback), and the
//! query executor (Task 3: `run_query` — index-eq + range filtering, sort over
//! unbound index fields with `_creationTime`/`_id` tiebreakers, and the
//! `get`/`first`/`unique`/`count`/`take`/`collect` terminals), and the
//! `FilterExpr` evaluator (Task 4: `validate_filter` + `eval_filter_expr`,
//! ported from the C-corrected TS logic). `paginate`/`search`/`vector_search`
//! stub out. Subsequent tasks fill in subscriptions, scheduling, and storage.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::de::DeserializeOwned;
use serde_json::{Map, Value};

use crate::error::{ErrorCode, RtDbError};
use crate::mutation::{Step, StepResult, Transaction};
use crate::query::{Order, Query};
use crate::schema::{FieldType, IndexDef, SchemaDef, TableDef};
use crate::wire::FilterExpr;

/// Maximum number of steps in a single transaction (mirrors the server cap).
pub const MAX_STEPS: usize = 256;
/// Maximum rows returned from a single `take`/`collect` (mirrors the server cap).
pub const MAX_TAKE: usize = 4096;
/// Approximate cron re-fire interval for the in-memory stub. Real 5-field cron
/// parsing is deferred to the server; the harness only needs crons to re-arm.
pub const CRON_STEP_MS: i64 = 60_000;

/// A stored row: the user doc plus its identity/history, kept separate so the
/// system fields (`_id`/`_creationTime`/`_version`) are merged in only at read
/// time — exactly as the server stores `doc` jsonb alongside `id`/`created_at`/
/// `version` columns.
#[derive(Debug, Clone)]
pub struct StoredRow {
    pub id: String,
    pub doc: Value,
    pub version: i64,
    pub created_at: i64,
}

/// Injectable clock and RNG for deterministic id minting and `_creationTime`.
///
/// Mirrors `InMemoryRtDbClientOptions` in `ts-client/src/in_memory.ts:91-96`.
/// Both `now` and `random` are optional; `InMemoryRtDbClient::new` supplies
/// defaults (system clock for `now`, a constant `0.5` for `random` — tests that
/// need determinism should always inject both).
#[derive(Default)]
pub struct InMemoryRtDbClientOptions {
    now: Option<Arc<dyn Fn() -> i64 + Send + Sync>>,
    random: Option<Arc<dyn Fn() -> f64 + Send + Sync>>,
}

impl InMemoryRtDbClientOptions {
    /// Inject a clock (epoch millis) for deterministic `_creationTime` and id
    /// minting.
    pub fn now(mut self, f: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.now = Some(Arc::new(f));
        self
    }
    /// Inject an RNG in `[0, 1)` for deterministic id minting.
    pub fn random(mut self, f: impl Fn() -> f64 + Send + Sync + 'static) -> Self {
        self.random = Some(Arc::new(f));
        self
    }
}

/// In-memory par-rt-db client for unit tests. See the
/// [module docs](crate::in_memory) for the parity scope and deferred gaps.
pub struct InMemoryRtDbClient {
    now: Arc<dyn Fn() -> i64 + Send + Sync>,
    random: Arc<dyn Fn() -> f64 + Send + Sync>,
    schema: Option<SchemaDef>,
    /// Per-table schema defs, keyed by table name. Separate from `schema` so
    /// Task 2+'s hot paths (validate-on-write, table lookups) don't re-walk the
    /// whole schema.
    tables: HashMap<String, TableDef>,
    /// Document store keyed by `(table_name, id)` — flat representation of the
    /// TS `Map<string, Map<string, StoredRow>>`.
    docs: HashMap<(String, String), StoredRow>,
    #[expect(dead_code, reason = "consumed by task 6 (storage id minting)")]
    id_counter: u64,
    /// `mut_id` → cached results. `push_schema` clears this on every push
    /// (matching TS); `mutate` reads/writes it for its idempotency short-circuit.
    idempotency: HashMap<String, Vec<StepResult>>,
    #[expect(dead_code, reason = "consumed by task 4 (scheduling)")]
    schedules: Vec<Value>,
    #[expect(dead_code, reason = "consumed by task 5 (subscriptions)")]
    subscribers: Vec<Value>,
    #[expect(dead_code, reason = "consumed by task 6 (storage)")]
    storage: HashMap<String, Value>,
}

impl InMemoryRtDbClient {
    /// Construct a new harness. `options.now` and `options.random` default to
    /// the system clock and a constant `0.5` respectively; tests that need
    /// deterministic ids/timestamps should always inject both.
    pub fn new(options: InMemoryRtDbClientOptions) -> Self {
        Self {
            now: options.now.unwrap_or_else(|| {
                Arc::new(|| {
                    SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_millis() as i64)
                        .unwrap_or(0)
                })
            }),
            random: options.random.unwrap_or_else(|| Arc::new(|| 0.5)),
            schema: None,
            tables: HashMap::new(),
            docs: HashMap::new(),
            id_counter: 0,
            idempotency: HashMap::new(),
            schedules: Vec::new(),
            subscribers: Vec::new(),
            storage: HashMap::new(),
        }
    }

    /// Installs `schema` as this client's sole in-memory database schema. Clears
    /// any previously-stored documents so each push starts from a clean slate.
    /// (The live server is additive-only; full additive evolution is deferred.)
    ///
    /// Ports `pushSchema` in `ts-client/src/in_memory.ts:512-519`. The Rust
    /// signature takes the typed [`SchemaDef`] directly (no `toSchemaJson`
    /// conversion needed since the builder already produces the wire shape).
    pub fn push_schema(&mut self, schema: &SchemaDef) {
        self.schema = Some(schema.clone());
        self.tables.clear();
        self.docs.clear();
        self.idempotency.clear();
        for (name, def) in &schema.tables {
            self.tables.insert(name.clone(), def.clone());
        }
    }

    /// Snapshot of the currently-installed schema (or `None` before
    /// `push_schema`). Returns a clone so callers can freely inspect/mutate.
    pub fn to_schema_json(&self) -> Option<SchemaDef> {
        self.schema.clone()
    }

    /// Minimal point read — returns the merged doc (system fields included) for
    /// `(table, id)`, or `None` if absent. Mirrors the server's `get(id)` read
    /// semantics. The full query DSL (`withIndex`, `order`, `take`, `filter`, …)
    /// lands in Task 3; tests that need a quick read use this until then.
    pub fn get(&self, table: &str, id: &str) -> Option<Value> {
        self.docs
            .get(&(table.to_string(), id.to_string()))
            .map(merge_doc)
    }

    /// Test/debug helper — every merged doc in `table`, in unspecified order.
    /// Not part of the query DSL; Task 3 replaces callers with proper queries.
    pub fn collect_all(&self, table: &str) -> Vec<Value> {
        self.docs
            .iter()
            .filter(|((t, _), _)| t == table)
            .map(|(_, row)| merge_doc(row))
            .collect()
    }

    /// One-shot query — ports `executeQuery` (`ts-client/src/in_memory.ts:889-1151`).
    /// Returns the terminal result as a [`Value`]:
    /// - `get(id)` / `first` → merged doc, or [`Value::Null`] when absent.
    /// - `unique` → merged doc, or `PRECONDITION_FAILED` when more than one row
    ///   matches (and [`Value::Null`] when zero match).
    /// - `count` → number of matching rows.
    /// - `take` / `collect` → array of merged docs.
    /// - `search` / `vector_search` → empty array (no in-memory ranking; the
    ///   guards still reject conflicting combinations so the cascade agrees with
    ///   the server).
    ///
    /// The harness is in-process — no `{result}` wire envelope; callers either
    /// match on the [`Value`] directly or use [`run`](Self::run) for typed
    /// deserialization.
    ///
    /// `filter` is structurally validated against the table's declared fields
    /// once up front (via [`validate_filter`], mirroring the server's
    /// compile-then-execute order), then evaluated per row via
    /// [`eval_filter_expr`]. `paginate` is Task 5 and returns an `INTERNAL`
    /// error.
    pub fn run_query(&self, q: &Query) -> Result<Value, RtDbError> {
        let table_def = self.require_table(&q.table)?.clone();
        let eq = &q.eq;
        let has_range = q.gt.is_some() || q.gte.is_some() || q.lt.is_some() || q.lte.is_some();

        // `get` terminal — exclusive of every other clause.
        if let Some(id) = &q.get {
            if q.index.is_some()
                || !eq.is_empty()
                || has_range
                || q.order.is_some()
                || q.take.is_some()
                || q.unique
                || q.first
                || q.count
                || q.paginate.is_some()
                || q.filter.is_some()
                || q.search.is_some()
                || q.vector_search.is_some()
            {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "get cannot be combined with index, eq, range bounds, order, take, \
                     unique, first, count, paginate, filter, search, or vector search",
                ));
            }
            // The DSL `get` terminal reuses the point-read primitive so the
            // system-field merge path is shared with the Task 2 helper.
            return Ok(self.get(&q.table, id).unwrap_or(Value::Null));
        }

        // Conflicting-terminal guards (ports :919-939).
        if q.unique && (q.take.is_some() || q.order.is_some()) {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                "unique cannot be combined with take or order",
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
        // Paginate is Task 5 — bail with a clear TODO so a caller knows the
        // path isn't silent. The paginate-specific combination guards
        // (count+paginate, take+paginate, …) are skipped: any paginate use
        // returns the same TODO error regardless of accompanying clauses.
        if q.paginate.is_some() {
            // TODO(task 5): port the keyset-cursor paginate branch.
            return Err(RtDbError::new(
                ErrorCode::Internal,
                "paginate is not implemented in the in-memory harness (task 5)",
            ));
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

        // `vectorSearch` terminal — cascade mirror of server `execute_query`.
        // No in-memory ranking; return an empty array so the cascade agrees
        // with the server without silently misranking by falling through to
        // the collect path.
        if q.vector_search.is_some() {
            if q.index.is_some()
                || !eq.is_empty()
                || has_range
                || q.order.is_some()
                || q.unique
                || q.first
                || q.count
                || q.filter.is_some()
                || q.search.is_some()
                || q.take.is_some()
            {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "vectorSearch cannot be combined with any other terminal",
                ));
            }
            return Ok(Value::Array(Vec::new()));
        }

        // `search` terminal — same reasoning as `vectorSearch`: no in-memory
        // ts_rank, but the guard exists so invalid combinations fail here
        // instead of silently returning an unranked result.
        if q.search.is_some() {
            if q.index.is_some()
                || !eq.is_empty()
                || has_range
                || q.order.is_some()
                || q.unique
                || q.first
                || q.count
                || q.filter.is_some()
                || q.vector_search.is_some()
            {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "search cannot be combined with index, eq, range bounds, order, \
                     unique, first, count, filter, or vector search",
                ));
            }
            return Ok(Value::Array(Vec::new()));
        }

        // Resolve index — required for `eq` and for any range bound.
        let index_def: Option<IndexDef> = match &q.index {
            Some(name) => Some(require_index(&table_def, name)?.clone()),
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
                    out.push(coerce_index_value(&table_def, &idx.fields[i], value)?);
                }
                out
            }
            None => Vec::new(),
        };

        // Range bounds apply to the next index field after the eq prefix.
        let range_field: Option<&str> = if has_range {
            let idx = index_def.as_ref().ok_or_else(|| {
                RtDbError::new(ErrorCode::BadRequest, "range bound requires an index")
            })?;
            if eq.len() >= idx.fields.len() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "range bound requires a remaining index field after eq",
                ));
            }
            Some(idx.fields[eq.len()].as_str())
        } else {
            None
        };
        let gt = match (&q.gt, range_field) {
            (Some(v), Some(f)) => Some(coerce_index_value(&table_def, f, v)?),
            _ => None,
        };
        let gte = match (&q.gte, range_field) {
            (Some(v), Some(f)) => Some(coerce_index_value(&table_def, f, v)?),
            _ => None,
        };
        let lt = match (&q.lt, range_field) {
            (Some(v), Some(f)) => Some(coerce_index_value(&table_def, f, v)?),
            _ => None,
        };
        let lte = match (&q.lte, range_field) {
            (Some(v), Some(f)) => Some(coerce_index_value(&table_def, f, v)?),
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

        // Row fetch + filter (eq prefix → range → filter hook).
        let mut filtered: Vec<StoredRow> = Vec::new();
        for ((t, _id), row) in &self.docs {
            if t != &q.table {
                continue;
            }
            if let Some(idx) = &index_def {
                let mut ok = true;
                for (i, tv) in typed_eq.iter().enumerate() {
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
            if let Some(field) = range_field {
                let v = match row.doc.get(field) {
                    Some(v) if !v.is_null() => v,
                    _ => continue,
                };
                if let Some(bound) = &gt
                    && compare_index_values(v, bound) != std::cmp::Ordering::Greater
                {
                    continue;
                }
                if let Some(bound) = &gte
                    && compare_index_values(v, bound) == std::cmp::Ordering::Less
                {
                    continue;
                }
                if let Some(bound) = &lt
                    && compare_index_values(v, bound) != std::cmp::Ordering::Less
                {
                    continue;
                }
                if let Some(bound) = &lte
                    && compare_index_values(v, bound) == std::cmp::Ordering::Greater
                {
                    continue;
                }
            }
            if let Some(expr) = &q.filter
                && !matches_filter(expr, &row.doc)
            {
                continue;
            }
            filtered.push(row.clone());
        }

        // `count` short-circuits before the sort (the count is the cardinality
        // of the filtered set, regardless of ordering).
        if q.count {
            return Ok(Value::Number(serde_json::Number::from(
                filtered.len() as i64
            )));
        }

        // Sort keys: unbound index fields (after the eq prefix), then
        // `_creationTime`, then `_id`. The unique `id` tiebreaker means the
        // order is total — no row is ambiguous relative to another.
        let dir = q.order.unwrap_or(Order::Asc);
        filtered.sort_by(|a, b| {
            if let Some(idx) = &index_def {
                for field in &idx.fields[typed_eq.len()..] {
                    let av = a.doc.get(field).unwrap_or(&Value::Null);
                    let bv = b.doc.get(field).unwrap_or(&Value::Null);
                    let cmp = compare_index_values(av, bv);
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

    /// Typed wrapper around [`run_query`](Self::run_query) that deserializes
    /// the result into `T` via [`crate::query::parse_result`]. Pick `T` to
    /// match the terminal: `Vec<T>` for `take`/`collect`, `Option<T>` for
    /// `get`/`first`/`unique`, `i64` for `count`, `Paginated<T>` for
    /// `paginate` (once Task 5 lands).
    pub fn run<T: DeserializeOwned>(&self, q: &Query) -> Result<T, RtDbError> {
        let value = self.run_query(q)?;
        crate::query::parse_result(value)
    }

    /// Executes a transaction and returns one [`StepResult`] per step, in order.
    /// Same shape (and `mut_id` idempotency-key semantics) as the live clients.
    ///
    /// Ports `mutate` in `ts-client/src/in_memory.ts:528-540`: a `mut_id` that
    /// has been seen before short-circuits with the cached results; otherwise
    /// the txn runs through [`execute_transaction`](Self::execute_transaction)
    /// and, on success, the results are cached under `mut_id` for next time.
    pub async fn mutate(
        &mut self,
        txn: &Transaction,
        mut_id: Option<&str>,
    ) -> Result<Vec<StepResult>, RtDbError> {
        if let Some(mid) = mut_id
            && let Some(cached) = self.idempotency.get(mid)
        {
            return Ok(cached.clone());
        }
        let results = self.execute_transaction(txn)?;
        if let Some(mid) = mut_id {
            self.idempotency.insert(mid.to_string(), results.clone());
        }
        Ok(results)
    }

    /// Synchronous atomic core shared by [`mutate`](Self::mutate) and (in Task
    /// 4) the scheduler's `tick`: enforces the [`MAX_STEPS`] cap, snapshots the
    /// docs store, applies every step (rolling back the whole txn on any error).
    /// Subscription fan-out happens in Task 6; this is where the write-set
    /// notification will hook in.
    fn execute_transaction(&mut self, txn: &Transaction) -> Result<Vec<StepResult>, RtDbError> {
        if txn.steps.len() > MAX_STEPS {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!("transaction exceeds maximum of {MAX_STEPS} steps"),
            ));
        }
        let snapshot = self.snapshot_docs();
        let mut results = Vec::with_capacity(txn.steps.len());
        for step in &txn.steps {
            match self.execute_step(step) {
                Ok((result, _written_table)) => results.push(result),
                Err(error) => {
                    // Atomicity: any step's error rolls back everything already
                    // applied, mirroring the server's single-transaction semantics.
                    self.restore_docs(snapshot);
                    return Err(error);
                }
            }
        }
        Ok(results)
    }

    /// Per-step executor — ports `executeStep` (`ts-client/src/in_memory.ts:747-805`).
    /// Each step validates against the live schema, mutates `self.docs` (or, for
    /// `Expect*`, just observes), and returns the [`StepResult`] plus the table
    /// that was written (so the Task 5 notify path can fan out by table).
    fn execute_step(&mut self, step: &Step) -> Result<(StepResult, Option<String>), RtDbError> {
        match step {
            Step::Insert { table, doc } => {
                let table_def = self.require_table(table)?.clone();
                let id = self.do_insert(table, &table_def, doc)?;
                Ok((StepResult::Insert { id }, Some(table.clone())))
            }
            Step::Patch { table, id, fields } => {
                let table_def = self.require_table(table)?.clone();
                self.do_patch(&table_def, table, id, fields)?;
                Ok((StepResult::Null, Some(table.clone())))
            }
            Step::Replace { table, id, doc } => {
                let table_def = self.require_table(table)?.clone();
                self.do_replace(&table_def, table, id, doc)?;
                Ok((StepResult::Null, Some(table.clone())))
            }
            Step::Delete { table, id } => {
                self.require_table(table)?;
                self.do_delete(table, id)?;
                Ok((StepResult::Null, Some(table.clone())))
            }
            Step::ExpectVersion { table, id, version } => {
                self.require_table(table)?;
                self.do_expect_version(table, id, *version)?;
                Ok((StepResult::Null, None))
            }
            Step::ExpectAbsent { table, index, eq } => {
                let table_def = self.require_table(table)?.clone();
                let rows = self.eq_lookup(&table_def, table, index, eq)?;
                if !rows.is_empty() {
                    return Err(RtDbError::new(
                        ErrorCode::PreconditionFailed,
                        format!("index '{index}' already has a matching document"),
                    ));
                }
                Ok((StepResult::Null, None))
            }
            Step::Upsert {
                table,
                index,
                eq,
                insert,
                patch,
            } => {
                let table_def = self.require_table(table)?.clone();
                let rows = self.eq_lookup(&table_def, table, index, eq)?;
                if rows.len() > 1 {
                    return Err(RtDbError::new(
                        ErrorCode::PreconditionFailed,
                        "upsert matched multiple documents",
                    ));
                }
                if let Some(row) = rows.into_iter().next() {
                    let merged = apply_patch(&table_def, &row.doc, patch)?;
                    self.do_update(table, &row.id, merged);
                    Ok((
                        StepResult::Upsert {
                            id: row.id.clone(),
                            inserted: false,
                        },
                        Some(table.clone()),
                    ))
                } else {
                    let id = self.do_insert(table, &table_def, insert)?;
                    Ok((
                        StepResult::Upsert { id, inserted: true },
                        Some(table.clone()),
                    ))
                }
            }
        }
    }

    /// Inserts a new doc, minting the id and stamping `_creationTime` /
    /// `_version = 1`. Ports `doInsert` (`ts-client/src/in_memory.ts:807-813`).
    fn do_insert(
        &mut self,
        table_name: &str,
        table_def: &TableDef,
        doc: &Map<String, Value>,
    ) -> Result<String, RtDbError> {
        let doc_value = Value::Object(doc.clone());
        validate_doc(table_def, &doc_value)?;
        let stored = strip_unset_optionals(table_def, &doc_value);
        let id = self.new_id();
        self.docs.insert(
            (table_name.to_string(), id.clone()),
            StoredRow {
                id: id.clone(),
                doc: stored,
                version: 1,
                created_at: (self.now)(),
            },
        );
        Ok(id)
    }

    /// Patches an existing doc with `fields`, bumping `_version`. Ports
    /// `doPatch` (`ts-client/src/in_memory.ts:815-824`) — apply then update.
    fn do_patch(
        &mut self,
        table_def: &TableDef,
        table_name: &str,
        id: &str,
        fields: &Map<String, Value>,
    ) -> Result<(), RtDbError> {
        let key = (table_name.to_string(), id.to_string());
        let row = self.docs.get(&key).cloned().ok_or_else(|| {
            RtDbError::new(ErrorCode::NotFound, format!("document '{id}' not found"))
        })?;
        let merged = apply_patch(table_def, &row.doc, fields)?;
        self.do_update(table_name, id, merged);
        Ok(())
    }

    /// Replaces an existing doc whole, bumping `_version`. Ports `doReplace`
    /// (`ts-client/src/in_memory.ts:826-836`).
    fn do_replace(
        &mut self,
        table_def: &TableDef,
        table_name: &str,
        id: &str,
        doc: &Map<String, Value>,
    ) -> Result<(), RtDbError> {
        let key = (table_name.to_string(), id.to_string());
        let row = self.docs.get_mut(&key).ok_or_else(|| {
            RtDbError::new(ErrorCode::NotFound, format!("document '{id}' not found"))
        })?;
        let doc_value = Value::Object(doc.clone());
        validate_doc(table_def, &doc_value)?;
        row.doc = strip_unset_optionals(table_def, &doc_value);
        row.version += 1;
        Ok(())
    }

    /// Deletes a doc by id. Ports `doDelete` (`ts-client/src/in_memory.ts:838-842`).
    fn do_delete(&mut self, table_name: &str, id: &str) -> Result<(), RtDbError> {
        let key = (table_name.to_string(), id.to_string());
        self.docs.remove(&key).ok_or_else(|| {
            RtDbError::new(ErrorCode::NotFound, format!("document '{id}' not found"))
        })?;
        Ok(())
    }

    /// Asserts a doc's current `_version` matches `expected`. Ports
    /// `doExpectVersion` (`ts-client/src/in_memory.ts:844-852`).
    fn do_expect_version(
        &self,
        table_name: &str,
        id: &str,
        expected: i64,
    ) -> Result<(), RtDbError> {
        let key = (table_name.to_string(), id.to_string());
        let row = self.docs.get(&key).ok_or_else(|| {
            RtDbError::new(ErrorCode::NotFound, format!("document '{id}' not found"))
        })?;
        if row.version != expected {
            return Err(RtDbError::new(
                ErrorCode::PreconditionFailed,
                format!(
                    "version mismatch: expected {expected}, actual {}",
                    row.version
                ),
            ));
        }
        Ok(())
    }

    /// Shared write-back helper for patch/replace/upsert-patch: writes the
    /// merged doc and bumps `_version`. Ports `doUpdate`
    /// (`ts-client/src/in_memory.ts:856-860`).
    fn do_update(&mut self, table_name: &str, id: &str, merged: Value) {
        let key = (table_name.to_string(), id.to_string());
        if let Some(row) = self.docs.get_mut(&key) {
            row.doc = merged;
            row.version += 1;
        }
    }

    /// Full-arity index eq lookup — ports `eqLookup`
    /// (`ts-client/src/in_memory.ts:864-885`), shared by `expectAbsent` and
    /// `upsert`. Returns every stored row whose indexed fields equal `eq`
    /// positionally (null/absent index fields never match, mirroring SQL NULL
    /// exclusion).
    fn eq_lookup(
        &self,
        table_def: &TableDef,
        table_name: &str,
        index_name: &str,
        eq: &[Value],
    ) -> Result<Vec<StoredRow>, RtDbError> {
        let index = require_index(table_def, index_name)?;
        if eq.len() != index.fields.len() {
            return Err(RtDbError::new(
                ErrorCode::BadRequest,
                format!(
                    "index '{}' expects {} eq value(s), got {}",
                    index_name,
                    index.fields.len(),
                    eq.len()
                ),
            ));
        }
        let typed: Vec<Value> = index
            .fields
            .iter()
            .zip(eq.iter())
            .map(|(field, value)| coerce_index_value(table_def, field, value))
            .collect::<Result<_, _>>()?;
        let mut matches = Vec::new();
        for ((t, _id), row) in &self.docs {
            if t != table_name {
                continue;
            }
            let all_match =
                index
                    .fields
                    .iter()
                    .zip(typed.iter())
                    .all(|(field, tv)| match row.doc.get(field) {
                        Some(v) => !v.is_null() && v == tv,
                        None => false,
                    });
            if all_match {
                matches.push(row.clone());
            }
        }
        Ok(matches)
    }

    /// Looks up a table def by name (NOT_FOUND if the schema has no such table).
    /// Ports `requireTable` (`ts-client/src/in_memory.ts:1320-1326`).
    fn require_table(&self, name: &str) -> Result<&TableDef, RtDbError> {
        self.tables
            .get(name)
            .ok_or_else(|| RtDbError::new(ErrorCode::NotFound, format!("table '{name}' not found")))
    }

    /// Snapshots the docs store for atomic rollback. Ports `snapshotTables`
    /// (`ts-client/src/in_memory.ts:1368-1383`).
    fn snapshot_docs(&self) -> HashMap<(String, String), StoredRow> {
        self.docs.clone()
    }

    /// Restores a previously-taken snapshot, discarding any partial writes.
    /// Ports `restoreTables` (`ts-client/src/in_memory.ts:1385-1390`).
    fn restore_docs(&mut self, snapshot: HashMap<(String, String), StoredRow>) {
        self.docs = snapshot;
    }

    /// UUIDv7-shaped id (timestamp-prefixed for sort stability), 32 hex chars.
    /// Ports `newId` (`ts-client/src/in_memory.ts:1354-1358`): low 48 bits of
    /// the epoch-millis timestamp (12 hex chars, the TS `.slice(-12)` of
    /// `toString(16)`), a constant `7` version nibble, then 19 random hex chars.
    fn new_id(&self) -> String {
        let ts = (self.now)() as u64 & 0xFFFF_FFFF_FFFF;
        let rand = self.random_hex(19);
        format!("{ts:012x}7{rand}")
    }

    /// `count` lowercase hex chars drawn from the injected RNG. Ports
    /// `randomHex` (`ts-client/src/in_memory.ts:1360-1366`).
    fn random_hex(&self, count: usize) -> String {
        let mut out = String::with_capacity(count);
        for _ in 0..count {
            // `random` is documented as `[0, 1)`; the `& 0xF` is a defensive
            // guard against a stray `1.0` overflowing the digit range.
            let digit = ((self.random)() * 16.0).floor() as u32 & 0xF;
            out.push(char::from_digit(digit, 16).unwrap_or('0'));
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Free helpers — ports of the module-private functions in
// `ts-client/src/in_memory.ts`. Kept `pub` so task tests can exercise them
// directly (the TS source exports them via the module surface too).
// ---------------------------------------------------------------------------

/// Deep clone of a JSON doc. Docs are pure JSON — safe to round-trip — so
/// cloning is just [`Value::clone`]. Named to mirror the TS helper.
pub fn clone_value(value: &Value) -> Value {
    value.clone()
}

/// Canonical string form for change detection, independent of key order.
/// `serde_json` with default features uses a `BTreeMap`-backed `Map`, so
/// [`Value`] already serializes with sorted keys — `to_string` is canonical.
/// If `preserve_order` is ever enabled on the `serde_json` dep, replace this
/// with a key-sorting canonicalizer (same caveat as `optimistic.rs`).
pub fn canonical(value: &Value) -> String {
    value.to_string()
}

/// `true` iff `value` is a 32-char lowercase hex string (an `_id`). Mirrors
/// the TS `/^[0-9a-f]+$/` (lowercase only).
pub fn is_hex_id(value: &Value) -> bool {
    match value.as_str() {
        Some(s) if s.len() == 32 => s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)),
        _ => false,
    }
}

/// `true` iff `value` is a syntactically-valid integer string within `i64`
/// range (the wire form of an `int64` field). Mirrors the BigInt range check in
/// the TS source.
pub fn is_int64_string(value: &Value) -> bool {
    let s = match value.as_str() {
        Some(s) => s,
        None => return false,
    };
    // Strict `^-?\d+$`: an optional leading '-' then one or more ASCII digits.
    let digits = s.strip_prefix('-').unwrap_or(s);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return false;
    }
    // i64 parse handles both sign and range.
    s.parse::<i64>().is_ok()
}

/// `true` iff `value` is a base64-shaped string: length a multiple of 4, body in
/// `[A-Za-z0-9+/]`, at most two trailing `=`. Mirrors the TS regex
/// `/^[A-Za-z0-9+/]*={0,2}$/`.
pub fn is_base64_string(value: &Value) -> bool {
    let s = match value.as_str() {
        Some(s) => s,
        None => return false,
    };
    if s.len() % 4 != 0 {
        return false;
    }
    let bytes = s.as_bytes();
    let eq_count = bytes.iter().rev().take_while(|&&b| b == b'=').count();
    eq_count <= 2
        && bytes[..bytes.len() - eq_count]
            .iter()
            .all(|&b| b.is_ascii_alphanumeric() || b == b'+' || b == b'/')
}

/// `true` iff `value` is a non-null, non-array JSON object. In `serde_json` the
/// only object kind is `Value::Object`, so this is `value.is_object()`.
pub fn is_plain_object(value: &Value) -> bool {
    value.is_object()
}

/// Recursive value validator — a port of server `schema::validate_value` and
/// the TS `validateValue` at `ts-client/src/in_memory.ts:150-198`. Switches on
/// the [`FieldType`] variant.
pub fn validate_value(ty: &FieldType, value: &Value) -> bool {
    match ty {
        FieldType::String => value.is_string(),
        FieldType::Number => value.is_number(),
        FieldType::Boolean => value.is_boolean(),
        FieldType::Null => value.is_null(),
        FieldType::Id { .. } => is_hex_id(value),
        FieldType::Literal { value: lit } => value == lit,
        FieldType::Optional { inner } => value.is_null() || validate_value(inner, value),
        FieldType::Union { variants } => variants.iter().any(|v| validate_value(v, value)),
        FieldType::Array { element } => value
            .as_array()
            .is_some_and(|arr| arr.iter().all(|item| validate_value(element, item))),
        FieldType::Object { fields } => {
            let map = match value.as_object() {
                Some(m) => m,
                None => return false,
            };
            // Reject unknown keys.
            for key in map.keys() {
                if !fields.contains_key(key) {
                    return false;
                }
            }
            // Declared fields: present-and-valid, or absent-and-optional.
            for (field, field_ty) in fields {
                match map.get(field) {
                    Some(v) => {
                        if !validate_value(field_ty, v) {
                            return false;
                        }
                    }
                    None if !matches!(field_ty, FieldType::Optional { .. }) => return false,
                    None => {}
                }
            }
            true
        }
        FieldType::Int64 => is_int64_string(value),
        FieldType::Bytes => is_base64_string(value),
        FieldType::Any => true,
        FieldType::Record { value: value_ty } => value
            .as_object()
            .is_some_and(|m| m.values().all(|v| validate_value(value_ty, v))),
        FieldType::Vector { dimensions } => {
            let arr = match value.as_array() {
                Some(a) => a,
                None => return false,
            };
            arr.len() == (*dimensions as usize)
                && arr
                    .iter()
                    .all(|v| v.as_f64().is_some_and(|f| f.is_finite()))
        }
    }
}

/// Full-document validator — a port of server `schema::validate_doc` and the TS
/// `validateDoc` at `ts-client/src/in_memory.ts:200-219`. Returns the first
/// violation as an [`RtDbError`] with code `SCHEMA_VIOLATION`.
///
/// Reserved (`_`-prefixed) and unknown fields are rejected, every declared
/// field is either present-and-valid or absent-and-optional.
pub fn validate_doc(table: &TableDef, doc: &Value) -> Result<(), RtDbError> {
    let map = doc.as_object();
    let map = match map {
        Some(m) => m,
        None => {
            return Err(RtDbError::new(
                ErrorCode::SchemaViolation,
                "document must be a JSON object",
            ));
        }
    };
    for key in map.keys() {
        if key.starts_with('_') {
            return Err(RtDbError::new(
                ErrorCode::SchemaViolation,
                format!("field '{key}' is reserved"),
            ));
        }
        if !table.fields.contains_key(key) {
            return Err(RtDbError::new(
                ErrorCode::SchemaViolation,
                format!("unknown field '{key}'"),
            ));
        }
    }
    for (field, field_ty) in &table.fields {
        match map.get(field) {
            Some(v) => {
                if !validate_value(field_ty, v) {
                    return Err(RtDbError::new(
                        ErrorCode::SchemaViolation,
                        format!("field '{field}' has an invalid value"),
                    ));
                }
            }
            None if !matches!(field_ty, FieldType::Optional { .. }) => {
                return Err(RtDbError::new(
                    ErrorCode::SchemaViolation,
                    format!("field '{field}' is required"),
                ));
            }
            None => {}
        }
    }
    Ok(())
}

/// Removes keys whose value is `null` for an `Optional` field whose inner type
/// does not itself accept `null` — a port of server `strip_unset_optionals` and
/// the TS helper at `ts-client/src/in_memory.ts:225-240`. An
/// inserted/patched-then-nulled optional lands as "key absent", matching the
/// server's single representation of an unset optional.
pub fn strip_unset_optionals(table: &TableDef, doc: &Value) -> Value {
    let map = match doc.as_object() {
        Some(m) => m,
        None => return doc.clone(),
    };
    let mut out = Map::new();
    for (key, value) in map {
        if value.is_null()
            && let Some(FieldType::Optional { inner }) = table.fields.get(key)
            && !validate_value(inner, value)
        {
            continue;
        }
        out.insert(key.clone(), value.clone());
    }
    Value::Object(out)
}

/// Applies a patch's `fields` onto `doc` — a port of server `txn::apply_patch`
/// and the TS `applyPatch` (`ts-client/src/in_memory.ts:243-265`). A `null`
/// onto an `Optional` field whose inner type doesn't itself accept `null`
/// deletes the key (mirroring `strip_unset_optionals`'s single representation
/// of an unset optional); the merged doc is then re-validated whole.
pub fn apply_patch(
    table: &TableDef,
    doc: &Value,
    fields: &Map<String, Value>,
) -> Result<Value, RtDbError> {
    let mut merged = match doc.as_object() {
        Some(m) => m.clone(),
        None => Map::new(),
    };
    for (field, value) in fields {
        let field_ty = match table.fields.get(field) {
            Some(t) => t,
            None => {
                return Err(RtDbError::new(
                    ErrorCode::SchemaViolation,
                    format!("unknown field '{field}'"),
                ));
            }
        };
        // null on an Optional<String> (or any Optional whose inner rejects null)
        // deletes the key — the server's strip_unset_optionals semantics.
        let strip = if let FieldType::Optional { inner } = field_ty {
            value.is_null() && !validate_value(inner, value)
        } else {
            false
        };
        if strip {
            merged.remove(field);
            continue;
        }
        if !validate_value(field_ty, value) {
            return Err(RtDbError::new(
                ErrorCode::SchemaViolation,
                format!("field '{field}' has an invalid value"),
            ));
        }
        merged.insert(field.clone(), value.clone());
    }
    let merged_value = Value::Object(merged);
    validate_doc(table, &merged_value)?;
    Ok(merged_value)
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
    Text,
    Number,
    Boolean,
}

/// Shape returned by [`index_column_type`]: the storage type plus whether the
/// source field was wrapped in `Optional` (so callers can let null sort).
#[derive(Debug, Clone, Copy)]
pub struct IndexedType {
    pub pg: PgType,
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
    }
    Ok(value.clone())
}

/// Null-sorting comparison for one index sort key. Mirrors `compareIndexValues`
/// (`ts-client/src/in_memory.ts:329-350`): numbers compare numerically, strings
/// lexicographically, booleans as `false < true`; nulls sort last (asc) / first
/// (desc, via the caller flipping the result). Mixed types fall back to
/// [`Ordering::Equal`] — indexed columns are single-type by schema, so this is
/// unreachable in practice.
pub fn compare_index_values(a: &Value, b: &Value) -> std::cmp::Ordering {
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
// Filter evaluation — a port of `validateFilter`/`evalFilterExpr` and the leaf
// helpers in `ts-client/src/in_memory.ts:361-488`. The server compiles a
// `FilterExpr` once against the table's declared fields
// (`query::compile_filter`), then evaluates the compiled predicate per row
// (`query::jsonb_lhs_and_bind`). This harness mirrors that two-phase split:
// [`validate_filter`] runs once in `run_query` before the row loop,
// [`eval_filter_expr`] runs per row inside [`matches_filter`].
// ---------------------------------------------------------------------------

/// The six leaf comparison operators, mirroring `FilterLeafOp` in the TS
/// source. Used as the dispatch key for [`compare_leaf`]/[`compare_values`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterOp {
    Eq,
    Neq,
    Gt,
    Gte,
    Lt,
    Lte,
}

/// Value-kind domain that picks the comparison semantics for a leaf, mirroring
/// `inValueKind`'s three variants. Post-[`check_leaf_value`] the
/// `Boolean` fallthrough is unreachable — every value is one of the three.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ValueKind {
    String,
    Number,
    Boolean,
}

/// Structural validation of a [`FilterExpr`] against a table's declared fields,
/// mirroring server `query::compile_filter` and the TS `validateFilter`
/// (`ts-client/src/in_memory.ts:361-386`). Returns `BAD_REQUEST` for: an empty
/// `and`/`or`, an empty `in`, an unknown field, a non-string/number/boolean
/// leaf value, or mixed-type `in` values. Call once before evaluating per row.
pub fn validate_filter(expr: &FilterExpr, fields: &BTreeSet<String>) -> Result<(), RtDbError> {
    match expr {
        FilterExpr::And { exprs } => {
            if exprs.is_empty() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "and filter requires at least one expr",
                ));
            }
            for e in exprs {
                validate_filter(e, fields)?;
            }
            Ok(())
        }
        FilterExpr::Or { exprs } => {
            if exprs.is_empty() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "or filter requires at least one expr",
                ));
            }
            for e in exprs {
                validate_filter(e, fields)?;
            }
            Ok(())
        }
        FilterExpr::In { field, values } => {
            if values.is_empty() {
                return Err(RtDbError::new(
                    ErrorCode::BadRequest,
                    "in filter requires at least one value",
                ));
            }
            for v in values {
                check_leaf_value(field, v, fields)?;
            }
            let first_kind = in_value_kind(&values[0]);
            for v in &values[1..] {
                if in_value_kind(v) != first_kind {
                    return Err(RtDbError::new(
                        ErrorCode::BadRequest,
                        "in filter values must all be the same type",
                    ));
                }
            }
            Ok(())
        }
        FilterExpr::Eq { field, value }
        | FilterExpr::Neq { field, value }
        | FilterExpr::Gt { field, value }
        | FilterExpr::Gte { field, value }
        | FilterExpr::Lt { field, value }
        | FilterExpr::Lte { field, value } => check_leaf_value(field, value, fields),
    }
}

/// `BAD_REQUEST` if `field` is not in the table's declared fields or `value`
/// is not a string/number/boolean. Mirrors `checkLeafValue`
/// (`ts-client/src/in_memory.ts:388-395`).
fn check_leaf_value(
    field: &str,
    value: &Value,
    fields: &BTreeSet<String>,
) -> Result<(), RtDbError> {
    if !fields.contains(field) {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            format!("filter references unknown field '{field}'"),
        ));
    }
    if !matches!(value, Value::String(_) | Value::Number(_) | Value::Bool(_)) {
        return Err(RtDbError::new(
            ErrorCode::BadRequest,
            "filter value must be a string, number, or boolean",
        ));
    }
    Ok(())
}

/// Value-kind domain for an `in` value, mirroring `inValueKind`
/// (`ts-client/src/in_memory.ts:397-401`).
fn in_value_kind(value: &Value) -> ValueKind {
    match value {
        Value::String(_) => ValueKind::String,
        Value::Number(_) => ValueKind::Number,
        _ => ValueKind::Boolean,
    }
}

/// Evaluate a [`FilterExpr`] predicate against a stored doc, mirroring server
/// `query::jsonb_lhs_and_bind` and the TS `evalFilterExpr`
/// (`ts-client/src/in_memory.ts:410-421`): the filter value's kind picks the
/// comparison domain — string compares the doc field's `->>` text, number
/// compares it as `float8`, boolean as `boolean`. A null/absent field never
/// matches (SQL NULL exclusion). Assumes [`validate_filter`] already passed.
pub fn eval_filter_expr(expr: &FilterExpr, doc: &Value) -> bool {
    match expr {
        FilterExpr::And { exprs } => exprs.iter().all(|e| eval_filter_expr(e, doc)),
        FilterExpr::Or { exprs } => exprs.iter().any(|e| eval_filter_expr(e, doc)),
        FilterExpr::In { field, values } => values
            .iter()
            .any(|v| compare_leaf(FilterOp::Eq, field, v, doc)),
        FilterExpr::Eq { field, value } => compare_leaf(FilterOp::Eq, field, value, doc),
        FilterExpr::Neq { field, value } => compare_leaf(FilterOp::Neq, field, value, doc),
        FilterExpr::Gt { field, value } => compare_leaf(FilterOp::Gt, field, value, doc),
        FilterExpr::Gte { field, value } => compare_leaf(FilterOp::Gte, field, value, doc),
        FilterExpr::Lt { field, value } => compare_leaf(FilterOp::Lt, field, value, doc),
        FilterExpr::Lte { field, value } => compare_leaf(FilterOp::Lte, field, value, doc),
    }
}

/// Per-leaf comparison, mirroring `compareLeaf`
/// (`ts-client/src/in_memory.ts:423-444`). `doc[field]` null/absent → `false`
/// (SQL NULL exclusion); the filter value's kind picks the comparison domain.
fn compare_leaf(op: FilterOp, field: &str, filter_value: &Value, doc: &Value) -> bool {
    let doc_val = match doc.get(field) {
        Some(v) if !v.is_null() => v,
        _ => return false,
    };
    match filter_value {
        Value::String(s) => {
            let lhs = doc_to_text(doc_val);
            compare_values(op, &lhs, s)
        }
        Value::Number(_) => match doc_to_number(doc_val) {
            Some(lhs) => match filter_value.as_f64() {
                Some(rhs) => compare_values(op, &lhs, &rhs),
                None => false,
            },
            None => false,
        },
        Value::Bool(b) => match doc_val {
            Value::Bool(db) => compare_values(op, db, b),
            _ => false,
        },
        // Unreachable post-validate (`check_leaf_value` rejects non-string/
        // number/boolean values); defensively treat as no-match.
        _ => false,
    }
}

/// Mirrors Postgres `doc->>'field'`: the JSON text of the value. Ports
/// `docToText` (`ts-client/src/in_memory.ts:447-452`) — string→as-is,
/// number→`JSON.stringify(n)`, boolean→"true"/"false", else JSON text.
fn doc_to_text(doc_val: &Value) -> String {
    match doc_val {
        Value::String(s) => s.clone(),
        Value::Number(n) => Value::Number(n.clone()).to_string(),
        Value::Bool(b) => b.to_string(),
        other => other.to_string(),
    }
}

/// Mirrors Postgres `(doc->>'field')::float8`: a finite number, or a parsed
/// numeric string. Ports `docToNumber` (`ts-client/src/in_memory.ts:455-462`).
fn doc_to_number(doc_val: &Value) -> Option<f64> {
    match doc_val {
        Value::Number(n) => n.as_f64().filter(|f| f.is_finite()),
        Value::String(s) => {
            let trimmed = s.trim();
            if trimmed.is_empty() {
                return None;
            }
            trimmed.parse::<f64>().ok().filter(|f| f.is_finite())
        }
        _ => None,
    }
}

/// Op dispatch over a same-typed pair (string/number/boolean — the filter
/// value's kind fixes the domain, so the operands never mix). Ports
/// `compareValues` (`ts-client/src/in_memory.ts:464-483`).
fn compare_values<T: PartialEq + PartialOrd>(op: FilterOp, lhs: &T, rhs: &T) -> bool {
    match op {
        FilterOp::Eq => lhs == rhs,
        FilterOp::Neq => lhs != rhs,
        FilterOp::Gt => lhs > rhs,
        FilterOp::Gte => lhs >= rhs,
        FilterOp::Lt => lhs < rhs,
        FilterOp::Lte => lhs <= rhs,
    }
}

/// Filter hook for [`InMemoryRtDbClient::run_query`]. Delegates to
/// [`eval_filter_expr`]; validation runs once in `run_query` before the row
/// loop, so by the time this runs the filter is structurally sound.
fn matches_filter(expr: &FilterExpr, doc: &Value) -> bool {
    eval_filter_expr(expr, doc)
}

/// Looks up an index by name (BAD_REQUEST if absent). Free function so it's
/// callable without `&self`. Ports `requireIndex`
/// (`ts-client/src/in_memory.ts:1328-1334`).
fn require_index<'a>(table_def: &'a TableDef, name: &str) -> Result<&'a IndexDef, RtDbError> {
    let indexes = table_def.indexes.as_ref().ok_or_else(|| {
        RtDbError::new(ErrorCode::BadRequest, format!("index '{name}' not found"))
    })?;
    indexes
        .iter()
        .find(|i| i.name == name)
        .ok_or_else(|| RtDbError::new(ErrorCode::BadRequest, format!("index '{name}' not found")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutation::Mutation;
    use crate::query::TableQuery;
    use crate::schema::{Schema, Table};
    use crate::wire::FilterExpr;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    /// The test schema mirrored from `ts-client/tests/in_memory.test.ts:10-20`.
    fn test_schema() -> SchemaDef {
        Schema::builder()
            .table(
                "items",
                Table::new()
                    .field("name", FieldType::String)
                    .field("status", FieldType::String)
                    .field("order", FieldType::Number)
                    .field("note", FieldType::optional(FieldType::String))
                    .index("by_name", &["name"])
                    .index("by_status", &["status"])
                    .index("by_status_and_order", &["status", "order"]),
            )
            .build()
    }

    fn items_table(schema: &SchemaDef) -> &TableDef {
        schema.tables.get("items").expect("items table present")
    }

    // ---- schema push ---------------------------------------------------

    #[test]
    fn push_schema_stores_the_schema() {
        // Mirrors the TS "schema push" suite: after pushSchema, the schema is
        // installed and the table is known (the TS suite verifies this by
        // running `query().collect()` and getting `[]`; here we verify the
        // schema snapshot directly because query/collect land in task 3).
        let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        let schema = test_schema();
        c.push_schema(&schema);
        let stored = c.to_schema_json().expect("schema installed");
        assert!(stored.tables.contains_key("items"));
        assert!(c.tables.contains_key("items"));
    }

    #[test]
    fn push_schema_replaces_the_previous_schema() {
        // The TS harness replaces (not additive-merges) on each push and clears
        // stored docs/idempotency so each push starts from a clean slate. (The
        // live server is additive-only; that evolution is deferred here.)
        let mut c = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
        c.push_schema(&test_schema());
        let only_other = Schema::builder()
            .table("solo", Table::new().field("x", FieldType::Number))
            .build();
        c.push_schema(&only_other);
        let stored = c.to_schema_json().expect("schema installed");
        assert!(stored.tables.contains_key("solo"));
        assert!(!stored.tables.contains_key("items"));
        assert!(!c.tables.contains_key("items"));
    }

    // ---- validate_doc --------------------------------------------------

    #[test]
    fn validate_doc_rejects_unknown_field() {
        let schema = test_schema();
        let bad = json!({"name": "a", "status": "todo", "order": 1, "bogus": 9});
        let err = validate_doc(items_table(&schema), &bad).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(err.message.contains("bogus"), "got: {}", err.message);
    }

    #[test]
    fn validate_doc_rejects_reserved_field() {
        let schema = test_schema();
        let bad = json!({"name": "a", "status": "todo", "order": 1, "_id": "x"});
        let err = validate_doc(items_table(&schema), &bad).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(err.message.contains("_id"), "got: {}", err.message);
    }

    #[test]
    fn validate_doc_rejects_wrong_field_type() {
        // The "invalid field type on a doc is rejected" case from the brief.
        let schema = test_schema();
        let bad = json!({"name": 42, "status": "todo", "order": 1});
        let err = validate_doc(items_table(&schema), &bad).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(err.message.contains("name"), "got: {}", err.message);
    }

    #[test]
    fn validate_doc_rejects_missing_required_field() {
        let schema = test_schema();
        let bad = json!({"name": "a", "order": 1}); // missing required "status"
        let err = validate_doc(items_table(&schema), &bad).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(err.message.contains("status"), "got: {}", err.message);
    }

    #[test]
    fn validate_doc_accepts_a_valid_doc_with_optional_absent() {
        let schema = test_schema();
        let good = json!({"name": "a", "status": "todo", "order": 1});
        validate_doc(items_table(&schema), &good).expect("valid doc");
    }

    #[test]
    fn validate_doc_accepts_an_optional_field_set_to_null() {
        // `note` is `Optional<String>`; null is accepted at the doc level
        // because Optional accepts null. `strip_unset_optionals` is what
        // converts it to "absent" for storage.
        let schema = test_schema();
        let good = json!({"name": "a", "status": "todo", "order": 1, "note": null});
        validate_doc(items_table(&schema), &good).expect("valid doc");
    }

    // ---- strip_unset_optionals ----------------------------------------

    #[test]
    fn strip_unset_optionals_drops_null_optional_string() {
        // `note: Optional<String>` set to null → key is stripped (the inner
        // String doesn't accept null, so this is "unset").
        let schema = test_schema();
        let doc = json!({"name": "a", "status": "todo", "order": 1, "note": null});
        let stripped = strip_unset_optionals(items_table(&schema), &doc);
        assert_eq!(stripped, json!({"name": "a", "status": "todo", "order": 1}));
    }

    #[test]
    fn strip_unset_optionals_keeps_null_for_optional_that_accepts_null() {
        // `Optional<Null>` does accept null as its inner value, so the key is
        // preserved.
        let schema = Schema::builder()
            .table(
                "t",
                Table::new().field("x", FieldType::optional(FieldType::Null)),
            )
            .build();
        let table = schema.tables.get("t").expect("table present");
        let doc = json!({"x": null});
        let stripped = strip_unset_optionals(table, &doc);
        assert_eq!(stripped, json!({"x": null}));
    }

    // ---- id/format helpers --------------------------------------------

    #[test]
    fn is_hex_id_checks_32_lowercase_hex_chars() {
        assert!(is_hex_id(&json!("0123456789abcdef0123456789abcdef")));
        assert!(!is_hex_id(&json!("0123456789ABCDEF0123456789ABCDEF"))); // uppercase
        assert!(!is_hex_id(&json!("0123456789abcdef"))); // too short
        assert!(!is_hex_id(&json!(42)));
        assert!(!is_hex_id(&json!(null)));
    }

    #[test]
    fn is_int64_string_accepts_i64_range_only() {
        assert!(is_int64_string(&json!("0")));
        assert!(is_int64_string(&json!("-1")));
        assert!(is_int64_string(&json!("9223372036854775807"))); // i64::MAX
        assert!(is_int64_string(&json!("-9223372036854775808"))); // i64::MIN
        // Out of i64 range:
        assert!(!is_int64_string(&json!("9223372036854775808")));
        assert!(!is_int64_string(&json!("-9223372036854775809")));
        // Bad shape:
        assert!(!is_int64_string(&json!("1.5")));
        assert!(!is_int64_string(&json!("-")));
        assert!(!is_int64_string(&json!("")));
        assert!(!is_int64_string(&json!(42)));
    }

    #[test]
    fn is_base64_string_matches_the_ts_regex() {
        assert!(is_base64_string(&json!("")));
        assert!(is_base64_string(&json!("ABCD")));
        assert!(is_base64_string(&json!("ABC=")));
        assert!(is_base64_string(&json!("AB==")));
        assert!(is_base64_string(&json!("YWJjZA=="))); // "abcd"
        // Length not a multiple of 4:
        assert!(!is_base64_string(&json!("ABC")));
        // Too much padding:
        assert!(!is_base64_string(&json!("A===")));
        // Bad body char:
        assert!(!is_base64_string(&json!("ABC!")));
        assert!(!is_base64_string(&json!(42)));
    }

    #[test]
    fn validate_value_handles_each_field_type_variant() {
        // A sanity sweep over the variants; full per-variant coverage lives in
        // the schema tests. Here we just confirm routing works.
        assert!(validate_value(&FieldType::String, &json!("hi")));
        assert!(!validate_value(&FieldType::String, &json!(2)));
        assert!(validate_value(&FieldType::Number, &json!(2.5)));
        assert!(validate_value(&FieldType::Boolean, &json!(true)));
        assert!(validate_value(&FieldType::Null, &json!(null)));
        assert!(validate_value(&FieldType::Any, &json!(null)));
        assert!(validate_value(
            &FieldType::Id { table: "x".into() },
            &json!("0123456789abcdef0123456789abcdef")
        ));
        assert!(validate_value(
            &FieldType::Literal { value: json!("a") },
            &json!("a")
        ));
        assert!(validate_value(
            &FieldType::Optional {
                inner: Box::new(FieldType::String)
            },
            &json!(null)
        ));
        assert!(validate_value(
            &FieldType::Union {
                variants: vec![FieldType::String, FieldType::Number]
            },
            &json!(2)
        ));
        assert!(validate_value(
            &FieldType::Array {
                element: Box::new(FieldType::Number)
            },
            &json!([1, 2, 3])
        ));
        assert!(validate_value(&FieldType::Int64, &json!("42")));
        assert!(validate_value(&FieldType::Bytes, &json!("YWJjZA==")));
        assert!(validate_value(
            &FieldType::Vector { dimensions: 3 },
            &json!([1.0, 2.0, 3.0])
        ));
    }

    #[test]
    fn canonical_is_key_order_independent() {
        // serde_json's default BTreeMap-backed Map serializes with sorted keys,
        // so canonical(a) == canonical(b) even when the source maps had
        // different insertion order.
        let a = json!({"b": 1, "a": 2});
        let b = json!({"a": 2, "b": 1});
        assert_eq!(canonical(&a), canonical(&b));
    }

    // ---- mutate: insert + read ---------------------------------------

    /// Deterministic clock + RNG so ids, `_creationTime`, and `_version` are
    /// stable. Mirrors TS `newClient` (`ts-client/tests/in_memory.test.ts:25-30`):
    /// post-incrementing epoch-millis clock + a constant `0` RNG.
    fn new_client() -> InMemoryRtDbClient {
        let counter = Arc::new(Mutex::new(1_700_000_000_000_i64));
        let mut client = InMemoryRtDbClient::new(
            InMemoryRtDbClientOptions::default()
                .now(move || {
                    let mut g = counter.lock().expect("counter not poisoned");
                    let v = *g;
                    *g += 1;
                    v
                })
                .random(|| 0.0),
        );
        client.push_schema(&test_schema());
        client
    }

    #[tokio::test]
    async fn insert_merges_system_fields_at_read_time() {
        let mut c = new_client();
        let txn = Mutation::new()
            .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
            .build();
        let results = c.mutate(&txn, None).await.expect("mutate ok");
        assert_eq!(results.len(), 1);
        let id = match &results[0] {
            StepResult::Insert { id } => id.clone(),
            other => panic!("expected Insert, got {other:?}"),
        };
        assert!(is_hex_id(&json!(id)), "id should be 32 hex chars: {id}");

        let doc = c.get("items", &id).expect("doc present");
        // System fields merged at read time:
        assert_eq!(doc["_id"], json!(id));
        assert_eq!(doc["_version"], 1);
        assert!(doc["_creationTime"].is_number(), "creationTime is a number");
        // User fields preserved:
        assert_eq!(doc["name"], "a");
        assert_eq!(doc["status"], "todo");
        assert_eq!(doc["order"], 1);
    }

    #[tokio::test]
    async fn insert_strips_optional_field_set_to_null() {
        // Mirrors TS "strips an optional field set to null on insert".
        let mut c = new_client();
        let txn = Mutation::new()
            .insert(
                "items",
                json!({"name": "a", "status": "todo", "order": 1, "note": null}),
            )
            .build();
        let results = c.mutate(&txn, None).await.expect("mutate ok");
        let id = match &results[0] {
            StepResult::Insert { id } => id.clone(),
            _ => unreachable!(),
        };
        let doc = c.get("items", &id).expect("doc present");
        // `note: null` was stripped on insert — the server's single representation
        // of an unset Optional<String> is "key absent", never "key present with null".
        assert!(
            doc.get("note").is_none(),
            "optional-null should be stripped, got: {doc}"
        );
    }

    #[tokio::test]
    async fn insert_rejects_missing_required_field() {
        // Mirrors TS "rejects an insert missing a required field".
        let mut c = new_client();
        let txn = Mutation::new()
            .insert("items", json!({"status": "todo", "order": 1})) // missing required "name"
            .build();
        let err = c.mutate(&txn, None).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(err.message.contains("name"), "got: {}", err.message);
    }

    // ---- mutate: upsert by index --------------------------------------

    #[tokio::test]
    async fn upsert_inserts_on_no_match_and_patches_on_match() {
        // Mirrors TS "inserts on no match (inserted: true) and patches on match".
        let mut c = new_client();
        let upsert = |patch_order: i64| {
            Mutation::new()
                .upsert(
                    "items",
                    "by_name",
                    &[json!("a")],
                    json!({"name": "a", "status": "todo", "order": 1}),
                    json!({"order": patch_order}),
                )
                .build()
        };

        let r1 = c.mutate(&upsert(2), None).await.expect("first upsert ok");
        let (id, inserted) = match &r1[0] {
            StepResult::Upsert { id, inserted } => (id.clone(), *inserted),
            other => panic!("expected Upsert, got {other:?}"),
        };
        assert!(inserted, "first upsert should insert");
        assert!(is_hex_id(&json!(id)));

        let r2 = c.mutate(&upsert(3), None).await.expect("second upsert ok");
        match &r2[0] {
            StepResult::Upsert {
                id: id2,
                inserted: false,
            } => {
                assert_eq!(id2, &id, "second upsert patched the same doc");
            }
            other => panic!("expected Upsert inserted=false, got {other:?}"),
        }

        let doc = c.get("items", &id).expect("doc present");
        assert_eq!(doc["order"], 3, "patch applied");
        assert_eq!(doc["_version"], 2, "patch bumped version");
    }

    #[tokio::test]
    async fn upsert_patch_visible_in_later_index_lookup() {
        // Mirrors TS "patches a matched doc onto an index field and reflects it
        // in a later query" — now via the real query DSL (Task 3), not the
        // internal `eq_lookup` helper. The patched `order` value is observable
        // through a `unique()` query on `by_name`.
        let mut c = new_client();
        let upsert = |patch_order: i64| {
            Mutation::new()
                .upsert(
                    "items",
                    "by_name",
                    &[json!("a")],
                    json!({"name": "a", "status": "todo", "order": 1}),
                    json!({"order": patch_order}),
                )
                .build()
        };
        c.mutate(&upsert(2), None).await.unwrap();
        let r2 = c.mutate(&upsert(3), None).await.unwrap();
        let id = match &r2[0] {
            StepResult::Upsert { id, .. } => id.clone(),
            _ => unreachable!(),
        };

        let matched: Value = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_name", &[json!("a")])
                    .unique(),
            )
            .expect("unique query ok");
        assert_eq!(matched["_id"], json!(id), "matched the patched doc");
        assert_eq!(matched["order"], 3, "patch value visible through the DSL");
    }

    #[tokio::test]
    async fn upsert_rejects_multiple_matches() {
        // The brief calls out the multi-match rejection explicitly. Seed two
        // docs with the same indexed value, then upsert by that index.
        let mut c = new_client();
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": "dup", "status": "todo", "order": 1}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": "dup", "status": "todo", "order": 2}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();

        let txn = Mutation::new()
            .upsert(
                "items",
                "by_name",
                &[json!("dup")],
                json!({"name": "dup", "status": "todo", "order": 1}),
                json!({"order": 9}),
            )
            .build();
        let err = c.mutate(&txn, None).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::PreconditionFailed);
        assert!(err.message.contains("multiple"), "got: {}", err.message);
    }

    // ---- mutate: transactions ----------------------------------------

    #[tokio::test]
    async fn txn_runs_multi_steps_and_returns_one_result_per_step() {
        // Mirrors TS "runs a multi-step txn and returns one result per step".
        let mut c = new_client();
        let txn = Mutation::new()
            .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
            .insert("items", json!({"name": "b", "status": "todo", "order": 2}))
            .build();
        let results = c.mutate(&txn, None).await.expect("mutate ok");
        assert_eq!(results.len(), 2, "one result per step");
        for r in &results {
            match r {
                StepResult::Insert { id } => assert!(is_hex_id(&json!(id.clone()))),
                other => panic!("expected Insert, got {other:?}"),
            }
        }
        let docs = c.collect_all("items");
        assert_eq!(docs.len(), 2, "both inserts landed");
    }

    #[tokio::test]
    async fn txn_patch_inside_txn_bumps_version() {
        // Mirrors TS "patches a doc inside a txn and bumps its version".
        let mut c = new_client();
        let r = c
            .mutate(
                &Mutation::new()
                    .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
                    .build(),
                None,
            )
            .await
            .unwrap();
        let id = match &r[0] {
            StepResult::Insert { id } => id.clone(),
            _ => unreachable!(),
        };

        // patch then expectVersion=2 (the patch bumps to 2 inside the same txn).
        let patch_txn = Mutation::new()
            .patch("items", &id, json!({"order": 9}))
            .expect_version("items", &id, 2)
            .build();
        c.mutate(&patch_txn, None).await.expect("patch txn ok");

        let doc = c.get("items", &id).expect("doc present");
        assert_eq!(doc["order"], 9);
        assert_eq!(doc["_version"], 2);
    }

    #[tokio::test]
    async fn txn_rolls_back_on_later_step_failure() {
        // Mirrors TS "rolls back the whole txn when a later step fails".
        let mut c = new_client();
        let r = c
            .mutate(
                &Mutation::new()
                    .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
                    .build(),
                None,
            )
            .await
            .unwrap();
        let id = match &r[0] {
            StepResult::Insert { id } => id.clone(),
            _ => unreachable!(),
        };

        let bad_txn = Mutation::new()
            .insert("items", json!({"name": "b", "status": "todo", "order": 2}))
            .expect_version("items", &id, 999) // mismatch → aborts the whole txn
            .build();
        let err = c.mutate(&bad_txn, None).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::PreconditionFailed);

        // Atomicity: the second insert was rolled back; only the original "a"
        // remains.
        let docs = c.collect_all("items");
        assert_eq!(docs.len(), 1, "rollback removed the second insert");
        assert_eq!(docs[0]["name"], "a");
    }

    #[tokio::test]
    async fn txn_rejects_more_than_max_steps() {
        // MAX_STEPS guard (mirror `executeTransaction` :546-548).
        let mut c = new_client();
        let mut m = Mutation::new();
        for _ in 0..(MAX_STEPS + 1) {
            m = m.insert("items", json!({"name": "x", "status": "todo", "order": 1}));
        }
        let txn = m.build();
        let err = c.mutate(&txn, None).await.unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("maximum"), "got: {}", err.message);
    }

    #[tokio::test]
    async fn mut_id_caches_results_and_short_circuits() {
        // Brief: port the TS `mutId` idempotency-key semantics (mutate :40-47).
        let mut c = new_client();
        let txn = Mutation::new()
            .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
            .build();

        let r1 = c.mutate(&txn, Some("m1")).await.expect("first ok");
        let r2 = c.mutate(&txn, Some("m1")).await.expect("cached ok");
        assert_eq!(r1.len(), 1);
        assert_eq!(r2.len(), 1);
        // The cached result is byte-identical to the first call — same id.
        let id1 = match &r1[0] {
            StepResult::Insert { id } => id.clone(),
            _ => unreachable!(),
        };
        let id2 = match &r2[0] {
            StepResult::Insert { id } => id.clone(),
            _ => unreachable!(),
        };
        assert_eq!(id1, id2, "cached mut_id returned the same id");
        // The cache short-circuits execution, so only one doc was actually
        // stored — the second `mutate` did not run the txn again.
        assert_eq!(c.collect_all("items").len(), 1);
    }

    // ---- mutate: step helpers ----------------------------------------

    #[test]
    fn apply_patch_merges_fields_and_re_validates_whole_doc() {
        let schema = test_schema();
        let table = items_table(&schema);
        let doc = json!({"name": "a", "status": "todo", "order": 1});
        let fields = json!({"order": 9}).as_object().unwrap().clone();
        let merged = apply_patch(table, &doc, &fields).expect("patch ok");
        assert_eq!(merged["order"], 9);
        assert_eq!(merged["name"], "a", "non-patched fields preserved");
    }

    #[test]
    fn apply_patch_null_on_optional_inner_that_rejects_null_deletes_key() {
        // `note: Optional<String>` + null → key is removed (mirrors
        // strip_unset_optionals' single-representation rule).
        let schema = test_schema();
        let table = items_table(&schema);
        let doc = json!({"name": "a", "status": "todo", "order": 1, "note": "hi"});
        let fields = json!({"note": null}).as_object().unwrap().clone();
        let merged = apply_patch(table, &doc, &fields).expect("patch ok");
        assert!(merged.get("note").is_none(), "note key stripped: {merged}");
    }

    #[test]
    fn apply_patch_rejects_unknown_field() {
        let schema = test_schema();
        let table = items_table(&schema);
        let doc = json!({"name": "a", "status": "todo", "order": 1});
        let fields = json!({"bogus": 1}).as_object().unwrap().clone();
        let err = apply_patch(table, &doc, &fields).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        assert!(err.message.contains("bogus"));
    }

    #[test]
    fn index_column_type_maps_each_indexable_field_and_rejects_others() {
        // Indexable shapes:
        assert_eq!(
            index_column_type(&FieldType::String).unwrap().pg,
            PgType::Text
        );
        assert_eq!(
            index_column_type(&FieldType::Number).unwrap().pg,
            PgType::Number
        );
        assert_eq!(
            index_column_type(&FieldType::Boolean).unwrap().pg,
            PgType::Boolean
        );
        assert_eq!(
            index_column_type(&FieldType::id("t")).unwrap().pg,
            PgType::Text
        );
        assert_eq!(
            index_column_type(&FieldType::literal("a")).unwrap().pg,
            PgType::Text
        );
        assert_eq!(
            index_column_type(&FieldType::optional(FieldType::Number))
                .unwrap()
                .pg,
            PgType::Number
        );
        // Optional wraps and reports nullable=true.
        let it = index_column_type(&FieldType::optional(FieldType::Number)).unwrap();
        assert!(it.nullable);
        // Non-indexable shapes:
        let err = index_column_type(&FieldType::Array {
            element: Box::new(FieldType::Number),
        })
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
        let err = index_column_type(&FieldType::literal(7)).unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaViolation);
    }

    #[test]
    fn coerce_index_value_type_checks_against_index_column() {
        let schema = test_schema();
        let table = items_table(&schema);
        // `name` is String → text column. Number is rejected.
        coerce_index_value(table, "name", &json!("a")).expect("string ok");
        let err = coerce_index_value(table, "name", &json!(7)).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        // `order` is Number → number column. String is rejected.
        coerce_index_value(table, "order", &json!(7)).expect("number ok");
        let err = coerce_index_value(table, "order", &json!("7")).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        // Unknown field is INTERNAL (schema-declared index references a missing
        // field — a server-side programming error, not a client one).
        let err = coerce_index_value(table, "bogus", &json!(7)).unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
    }

    #[test]
    fn compare_index_values_orders_nulls_last_and_compares_each_domain() {
        use std::cmp::Ordering;
        // Numbers:
        assert_eq!(compare_index_values(&json!(1), &json!(2)), Ordering::Less);
        assert_eq!(compare_index_values(&json!(2), &json!(2)), Ordering::Equal);
        // Strings (lexicographic):
        assert_eq!(
            compare_index_values(&json!("a"), &json!("b")),
            Ordering::Less
        );
        // Booleans (false < true):
        assert_eq!(
            compare_index_values(&json!(false), &json!(true)),
            Ordering::Less
        );
        // Nulls sort last under asc — `null > anything`.
        assert_eq!(
            compare_index_values(&json!(null), &json!(1)),
            Ordering::Greater
        );
        assert_eq!(
            compare_index_values(&json!(1), &json!(null)),
            Ordering::Less
        );
        assert_eq!(
            compare_index_values(&json!(null), &json!(null)),
            Ordering::Equal
        );
    }

    #[test]
    fn merge_doc_layers_system_fields_over_user_doc() {
        let row = StoredRow {
            id: "0018beacc10070000000000000000000".to_string(),
            doc: json!({"name": "a", "status": "todo", "order": 1}),
            version: 7,
            created_at: 1_700_000_000_000,
        };
        let merged = merge_doc(&row);
        assert_eq!(merged["_id"], json!("0018beacc10070000000000000000000"));
        assert_eq!(merged["_version"], 7);
        assert_eq!(merged["_creationTime"], 1_700_000_000_000_i64);
        // User fields preserved.
        assert_eq!(merged["name"], "a");
        assert_eq!(merged["order"], 1);
    }

    // ---- query: get / collect ----------------------------------------

    /// Mirrors TS `seed` (`ts-client/tests/in_memory.test.ts:134-142`): insert
    /// three rows in `order` = 3, 1, 2 so an ascending sort differs from
    /// insertion order (catches a fall-back-to-insertion-order bug).
    async fn seed_query_rows(c: &mut InMemoryRtDbClient) {
        for order in [3_i64, 1, 2] {
            c.mutate(
                &Mutation::new()
                    .insert(
                        "items",
                        json!({"name": format!("n{order}"), "status": "todo", "order": order}),
                    )
                    .build(),
                None,
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn query_collect_returns_empty_for_empty_table() {
        // Mirrors TS "collects [] from an empty table after pushSchema".
        let c = new_client();
        let docs = c
            .run::<Vec<Value>>(&TableQuery::new("items").collect())
            .expect("collect ok");
        assert!(docs.is_empty());
    }

    #[tokio::test]
    async fn query_get_returns_merged_doc() {
        // Mirrors TS "inserts a doc and merges system fields at read time"
        // (the read is now via the DSL `get` terminal, not the bare helper).
        let mut c = new_client();
        let r = c
            .mutate(
                &Mutation::new()
                    .insert("items", json!({"name": "a", "status": "todo", "order": 1}))
                    .build(),
                None,
            )
            .await
            .expect("insert ok");
        let id = match &r[0] {
            StepResult::Insert { id } => id.clone(),
            other => panic!("expected Insert, got {other:?}"),
        };

        let doc = c
            .run::<Value>(&TableQuery::get("items", &id))
            .expect("get ok");
        assert_eq!(doc["_id"], json!(id));
        assert_eq!(doc["name"], "a");
        assert_eq!(doc["status"], "todo");
        assert_eq!(doc["order"], 1);
        assert_eq!(doc["_version"], 1);
        assert!(doc["_creationTime"].is_number());
    }

    #[tokio::test]
    async fn query_get_returns_null_for_missing_id() {
        // Mirrors TS "point-reads a missing id as null". The server returns
        // JSON null for a missing point read (TS :916), not an error.
        let c = new_client();
        let v = c
            .run::<Value>(&TableQuery::get(
                "items",
                "0123456789abcdef0123456789abcdef",
            ))
            .expect("get resolves");
        assert!(v.is_null(), "missing get returns Value::Null, got: {v}");
    }

    #[tokio::test]
    async fn query_get_rejects_combinations() {
        // Ports the `get`-exclusivity guard at TS :895-914. `get` plus any
        // narrowing clause is BAD_REQUEST.
        let c = new_client();
        let q = Query {
            table: "items".into(),
            get: Some("x".into()),
            index: Some("by_name".into()),
            ..Default::default()
        };
        let err = c.run_query(&q).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("get cannot be combined"),
            "got: {}",
            err.message
        );
    }

    // ---- query: index eq + order + take ------------------------------

    #[tokio::test]
    async fn query_eq_prefix_with_order_asc_sorts_by_remaining_field() {
        // Mirrors TS "filters by an eq index prefix and orders by the remaining
        // index field" — the asc branch.
        let mut c = new_client();
        seed_query_rows(&mut c).await;

        let asc = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo")])
                    .order(Order::Asc)
                    .collect(),
            )
            .expect("asc ok");
        let orders: Vec<i64> = asc
            .iter()
            .map(|d| d["order"].as_i64().unwrap_or_default())
            .collect();
        assert_eq!(orders, vec![1, 2, 3], "asc order");
    }

    #[tokio::test]
    async fn query_eq_prefix_with_order_desc_and_take_n() {
        // Mirrors TS "filters by an eq index prefix and orders by the remaining
        // index field" — the desc+take(2) branch.
        let mut c = new_client();
        seed_query_rows(&mut c).await;

        let desc = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo")])
                    .order(Order::Desc)
                    .take(2),
            )
            .expect("desc+take ok");
        let orders: Vec<i64> = desc
            .iter()
            .map(|d| d["order"].as_i64().unwrap_or_default())
            .collect();
        assert_eq!(orders, vec![3, 2], "desc order, take 2");
    }

    #[tokio::test]
    async fn query_eq_on_single_field_index_returns_matching_rows() {
        // The brief calls out single-field eq match explicitly; `by_name` is
        // single-field. Two rows share `name="dup"`, the third doesn't.
        let mut c = new_client();
        for order in [1_i64, 2, 3] {
            let name = if order <= 2 { "dup" } else { "uniq" };
            c.mutate(
                &Mutation::new()
                    .insert(
                        "items",
                        json!({"name": name, "status": "todo", "order": order}),
                    )
                    .build(),
                None,
            )
            .await
            .unwrap();
        }
        let docs = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .with_index("by_name", &[json!("dup")])
                    .collect(),
            )
            .expect("eq ok");
        assert_eq!(docs.len(), 2, "both dup rows match");
        for d in &docs {
            assert_eq!(d["name"], "dup");
        }
    }

    // ---- query: range bounds ----------------------------------------

    #[tokio::test]
    async fn query_range_filters_by_index_field() {
        // gt / lt / gte / lte over the remaining index field. `by_status_and_order`
        // has `status` then `order`; the eq prefix pins status, the range
        // narrows order. Seed order values [3,1,2] and assert each bound.
        let mut c = new_client();
        seed_query_rows(&mut c).await;

        let collect_range =
            |gt: Option<i64>, gte: Option<i64>, lt: Option<i64>, lte: Option<i64>| {
                let mut q =
                    TableQuery::new("items").with_index("by_status_and_order", &[json!("todo")]);
                if let Some(v) = gt {
                    q = q.gt(v);
                }
                if let Some(v) = gte {
                    q = q.gte(v);
                }
                if let Some(v) = lt {
                    q = q.lt(v);
                }
                if let Some(v) = lte {
                    q = q.lte(v);
                }
                c.run::<Vec<Value>>(&q.order(Order::Asc).collect())
                    .expect("range ok")
            };

        let orders = |docs: Vec<Value>| -> Vec<i64> {
            docs.iter()
                .map(|d| d["order"].as_i64().unwrap_or_default())
                .collect()
        };

        // gt=1 → {2,3}; gte=2 → {2,3}; lt=3 → {1,2}; lte=2 → {1,2}.
        assert_eq!(orders(collect_range(Some(1), None, None, None)), vec![2, 3]);
        assert_eq!(orders(collect_range(None, Some(2), None, None)), vec![2, 3]);
        assert_eq!(orders(collect_range(None, None, Some(3), None)), vec![1, 2]);
        assert_eq!(orders(collect_range(None, None, None, Some(2))), vec![1, 2]);
    }

    // ---- query: terminals -------------------------------------------

    #[tokio::test]
    async fn query_count_returns_number_of_matching_rows() {
        // Mirrors TS "counts matching rows over an eq prefix".
        let mut c = new_client();
        seed_query_rows(&mut c).await;
        let n = c
            .run::<i64>(
                &TableQuery::new("items")
                    .with_index("by_status", &[json!("todo")])
                    .count(),
            )
            .expect("count ok");
        assert_eq!(n, 3);
    }

    #[tokio::test]
    async fn query_unique_returns_doc_when_exactly_one_match() {
        let mut c = new_client();
        c.mutate(
            &Mutation::new()
                .insert(
                    "items",
                    json!({"name": "only", "status": "todo", "order": 1}),
                )
                .build(),
            None,
        )
        .await
        .unwrap();
        let doc = c
            .run::<Value>(
                &TableQuery::new("items")
                    .with_index("by_name", &[json!("only")])
                    .unique(),
            )
            .expect("unique ok");
        assert_eq!(doc["name"], "only");
    }

    #[tokio::test]
    async fn query_unique_throws_precondition_failed_when_multiple_match() {
        // Mirrors TS "unique throws PRECONDITION_FAILED when more than one doc
        // matches".
        let mut c = new_client();
        for order in [1_i64, 2] {
            c.mutate(
                &Mutation::new()
                    .insert(
                        "items",
                        json!({"name": "dup", "status": "todo", "order": order}),
                    )
                    .build(),
                None,
            )
            .await
            .unwrap();
        }
        let err = c
            .run_query(
                &TableQuery::new("items")
                    .with_index("by_name", &[json!("dup")])
                    .unique(),
            )
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::PreconditionFailed);
    }

    #[tokio::test]
    async fn query_unique_returns_null_when_zero_match() {
        // TS :1143 — `unique` with zero matches returns null (no precondition
        // to fail; only a multi-match is an error).
        let c = new_client();
        let v = c
            .run::<Value>(
                &TableQuery::new("items")
                    .with_index("by_name", &[json!("ghost")])
                    .unique(),
            )
            .expect("unique resolves");
        assert!(v.is_null(), "zero-match unique returns null, got: {v}");
    }

    #[tokio::test]
    async fn query_first_returns_first_or_null() {
        // Mirrors TS `first` terminal: the first row of the filtered+sorted
        // set, or null when empty.
        let mut c = new_client();
        // Empty table: first = null.
        let v = c
            .run::<Value>(
                &TableQuery::new("items")
                    .with_index("by_status", &[json!("todo")])
                    .first(),
            )
            .expect("first on empty");
        assert!(v.is_null(), "first on empty table is null");

        seed_query_rows(&mut c).await;
        // With rows sorted ascending, first is order=1.
        let first = c
            .run::<Value>(
                &TableQuery::new("items")
                    .with_index("by_status_and_order", &[json!("todo")])
                    .order(Order::Asc)
                    .first(),
            )
            .expect("first ok");
        assert_eq!(first["order"], 1, "first asc is order=1");
    }

    #[tokio::test]
    async fn query_take_caps_results_at_n() {
        let mut c = new_client();
        seed_query_rows(&mut c).await;
        let docs = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .with_index("by_status", &[json!("todo")])
                    .order(Order::Asc)
                    .take(2),
            )
            .expect("take ok");
        assert_eq!(docs.len(), 2, "take(2) on 3 rows caps at 2");
    }

    // ---- query: validation rejections -------------------------------

    #[tokio::test]
    async fn query_rejects_eq_without_index() {
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                eq: vec![json!("x")],
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("eq requires an index"), "got: {err}");
    }

    #[tokio::test]
    async fn query_rejects_range_without_index() {
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                gt: Some(json!(1)),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("range bound requires an index"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn query_rejects_range_without_remaining_field_after_eq() {
        // `by_name` has one field — a full-arity eq leaves no field for a
        // range bound.
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                index: Some("by_name".into()),
                eq: vec![json!("a")],
                gt: Some(json!("z")),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("remaining index field after eq"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn query_rejects_eq_arity_above_index_field_count() {
        // `by_name` is single-field; two eq values is over-arity.
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                index: Some("by_name".into()),
                eq: vec![json!("a"), json!("b")],
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("expects at most"), "got: {err}");
    }

    #[tokio::test]
    async fn query_rejects_gt_and_gte_together() {
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                index: Some("by_status_and_order".into()),
                eq: vec![json!("todo")],
                gt: Some(json!(1)),
                gte: Some(json!(1)),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("gt and gte"), "got: {err}");
    }

    #[tokio::test]
    async fn query_rejects_lt_and_lte_together() {
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                index: Some("by_status_and_order".into()),
                eq: vec![json!("todo")],
                lt: Some(json!(1)),
                lte: Some(json!(1)),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("lt and lte"), "got: {err}");
    }

    #[tokio::test]
    async fn query_rejects_take_over_max_take() {
        // MAX_TAKE guard (TS :963-965).
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                take: Some((MAX_TAKE as u32) + 1),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("maximum"), "got: {err}");
    }

    #[tokio::test]
    async fn query_accepts_take_at_max_take() {
        // `take == MAX_TAKE` is the boundary — accepted.
        let c = new_client();
        let docs = c
            .run::<Vec<Value>>(&Query {
                table: "items".into(),
                take: Some(MAX_TAKE as u32),
                ..Default::default()
            })
            .expect("take=MAX_TAKE ok");
        assert!(docs.is_empty(), "empty table → empty page");
    }

    /// One assertion per conflicting-terminal guard at TS :919-939. Each case
    /// is BAD_REQUEST; the needle distinguishes which guard fired.
    #[tokio::test]
    async fn query_rejects_conflicting_terminals() {
        let c = new_client();
        let base_index_query =
            |unique: bool, first: bool, count: bool, order: bool, take: Option<u32>| Query {
                table: "items".into(),
                index: Some("by_status".into()),
                eq: vec![json!("todo")],
                unique,
                first,
                count,
                order: order.then_some(Order::Asc),
                take,
                ..Default::default()
            };

        let cases: &[(Query, &str)] = &[
            // unique + take
            (
                base_index_query(true, false, false, false, Some(1)),
                "unique cannot be combined with take",
            ),
            // unique + order
            (
                base_index_query(true, false, false, true, None),
                "unique cannot be combined with take or order",
            ),
            // first + unique
            (
                base_index_query(true, true, false, false, None),
                "first cannot be combined with unique",
            ),
            // first + take
            (
                base_index_query(false, true, false, false, Some(1)),
                "first cannot be combined with take",
            ),
            // count + unique
            (
                base_index_query(true, false, true, false, None),
                "count cannot be combined with unique",
            ),
            // count + take
            (
                base_index_query(false, false, true, false, Some(1)),
                "count cannot be combined with take",
            ),
            // count + first
            (
                base_index_query(false, true, true, false, None),
                "count cannot be combined with first",
            ),
            // count + order
            (
                base_index_query(false, false, true, true, None),
                "count cannot be combined with order",
            ),
        ];
        for (q, needle) in cases {
            let err = c.run_query(q).unwrap_err();
            assert_eq!(
                err.code,
                ErrorCode::BadRequest,
                "case '{needle}': got {err:?}"
            );
            assert!(
                err.message.contains(needle),
                "case '{needle}' missing needle: got {}",
                err.message
            );
        }
    }

    // ---- query: paginate / search / vector stubs --------------------

    #[tokio::test]
    async fn query_paginate_returns_internal_error_task_5() {
        // Task 5 will port the keyset-cursor paginate branch; until then the
        // TODO returns INTERNAL so the path can't silently misbehave.
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                paginate: Some(crate::query::Paginate {
                    cursor: None,
                    num_items: 10,
                }),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(err.message.contains("task 5"), "got: {err}");
    }

    #[tokio::test]
    async fn query_search_returns_empty_array_stub() {
        // No in-memory ts_rank — the cascade agrees with the server by
        // returning [] for a valid `search`, while still rejecting conflicting
        // combinations.
        let c = new_client();
        let v = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .search("by_content", "hello")
                    .take(5),
            )
            .expect("search stub");
        assert!(v.is_empty(), "search stub returns []");
    }

    #[tokio::test]
    async fn query_search_rejects_conflicting_terminals() {
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                search: Some(crate::wire::SearchQuery {
                    index: "by_content".into(),
                    query: "hello".into(),
                }),
                index: Some("by_name".into()),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("search cannot be combined"),
            "got: {err}"
        );
    }

    #[tokio::test]
    async fn query_vector_search_returns_empty_array_stub() {
        // The TS harness rejects `vectorSearch` combined with any other
        // terminal (including `take`) — unlike `search`, vectorSearch carries
        // its own `limit`. So the bare-stub path is exercised without a
        // trailing terminal.
        let c = new_client();
        let v = c
            .run::<Vec<Value>>(
                &TableQuery::new("items")
                    .vector_search("by_embedding", vec![1.0, 0.0, 0.0], 5, BTreeMap::new())
                    .build(),
            )
            .expect("vector stub");
        assert!(v.is_empty(), "vector stub returns []");
    }

    #[tokio::test]
    async fn query_vector_search_rejects_conflicting_terminals() {
        let c = new_client();
        let err = c
            .run_query(&Query {
                table: "items".into(),
                vector_search: Some(crate::wire::VectorSearchQuery {
                    index: "by_embedding".into(),
                    vector: vec![1.0],
                    limit: 5,
                    filter: BTreeMap::new(),
                }),
                index: Some("by_name".into()),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("vectorSearch cannot be combined"),
            "got: {err}"
        );
    }

    // ---- filter: eval_filter_expr + validate_filter ----------------
    //
    // Direct unit tests for the filter evaluator + validator, ported verbatim
    // from `describe("evalFilterExpr + validateFilter")`
    // (`ts-client/tests/in_memory.test.ts:539-653`). These are the cases item C
    // fixed in the TS source — E must not regress them.

    /// The field set used by the unit tests below — mirrors the TS
    /// `new Set(["name", "age", "active", "score", "tags"])`.
    fn filter_unit_fields() -> BTreeSet<String> {
        ["name", "age", "active", "score", "tags"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    }

    #[test]
    fn eval_filter_eq_neq_on_strings_compare_the_doc_field_text() {
        let fields = filter_unit_fields();
        validate_filter(
            &FilterExpr::Eq {
                field: "name".into(),
                value: json!("ada"),
            },
            &fields,
        )
        .expect("valid");
        assert!(eval_filter_expr(
            &FilterExpr::Eq {
                field: "name".into(),
                value: json!("ada"),
            },
            &json!({"name": "ada"}),
        ));
        assert!(!eval_filter_expr(
            &FilterExpr::Eq {
                field: "name".into(),
                value: json!("ada"),
            },
            &json!({"name": "bob"}),
        ));
        assert!(eval_filter_expr(
            &FilterExpr::Neq {
                field: "name".into(),
                value: json!("ada"),
            },
            &json!({"name": "bob"}),
        ));
    }

    #[test]
    fn eval_filter_number_domain_compares_numerically() {
        // gt/gte/lt/lte over a numeric doc field.
        assert!(eval_filter_expr(
            &FilterExpr::Gt {
                field: "age".into(),
                value: json!(30),
            },
            &json!({"age": 42}),
        ));
        assert!(!eval_filter_expr(
            &FilterExpr::Gt {
                field: "age".into(),
                value: json!(50),
            },
            &json!({"age": 42}),
        ));
        assert!(eval_filter_expr(
            &FilterExpr::Lte {
                field: "age".into(),
                value: json!(42),
            },
            &json!({"age": 42}),
        ));
    }

    #[test]
    fn eval_filter_string_ordering_is_lexicographic() {
        assert!(eval_filter_expr(
            &FilterExpr::Lt {
                field: "name".into(),
                value: json!("b"),
            },
            &json!({"name": "ada"}),
        ));
        assert!(eval_filter_expr(
            &FilterExpr::Gte {
                field: "name".into(),
                value: json!("a"),
            },
            &json!({"name": "ada"}),
        ));
    }

    #[test]
    fn eval_filter_boolean_domain_compares_booleans() {
        assert!(eval_filter_expr(
            &FilterExpr::Eq {
                field: "active".into(),
                value: json!(true),
            },
            &json!({"active": true}),
        ));
        assert!(!eval_filter_expr(
            &FilterExpr::Eq {
                field: "active".into(),
                value: json!(true),
            },
            &json!({"active": false}),
        ));
    }

    #[test]
    fn eval_filter_number_value_matches_a_numeric_string_field() {
        // float8 cast: doc field is the string "5", filter value is the number
        // 5 → match. Mirrors Postgres `(doc->>'field')::float8 = 5`.
        assert!(eval_filter_expr(
            &FilterExpr::Eq {
                field: "score".into(),
                value: json!(5),
            },
            &json!({"score": "5"}),
        ));
    }

    #[test]
    fn eval_filter_null_or_absent_doc_field_never_matches() {
        // SQL NULL exclusion: null/absent never matches any op (even neq).
        assert!(!eval_filter_expr(
            &FilterExpr::Eq {
                field: "name".into(),
                value: json!("ada"),
            },
            &json!({"name": null}),
        ));
        assert!(!eval_filter_expr(
            &FilterExpr::Eq {
                field: "name".into(),
                value: json!("ada"),
            },
            &json!({}),
        ));
        assert!(!eval_filter_expr(
            &FilterExpr::Neq {
                field: "name".into(),
                value: json!("ada"),
            },
            &json!({}),
        ));
    }

    #[test]
    fn eval_filter_and_or_nest_recursively() {
        let expr = FilterExpr::And {
            exprs: vec![
                FilterExpr::Gte {
                    field: "age".into(),
                    value: json!(30),
                },
                FilterExpr::Or {
                    exprs: vec![
                        FilterExpr::Eq {
                            field: "name".into(),
                            value: json!("ada"),
                        },
                        FilterExpr::Eq {
                            field: "name".into(),
                            value: json!("bob"),
                        },
                    ],
                },
            ],
        };
        assert!(eval_filter_expr(&expr, &json!({"age": 42, "name": "ada"})));
        assert!(!eval_filter_expr(&expr, &json!({"age": 42, "name": "zed"})));
        assert!(!eval_filter_expr(&expr, &json!({"age": 10, "name": "ada"})));
    }

    #[test]
    fn eval_filter_in_matches_membership() {
        assert!(eval_filter_expr(
            &FilterExpr::In {
                field: "name".into(),
                values: vec![json!("ada"), json!("bob")],
            },
            &json!({"name": "bob"}),
        ));
        assert!(!eval_filter_expr(
            &FilterExpr::In {
                field: "name".into(),
                values: vec![json!("ada"), json!("bob")],
            },
            &json!({"name": "zed"}),
        ));
    }

    #[test]
    fn validate_filter_rejects_an_unknown_field() {
        let fields = filter_unit_fields();
        let err = validate_filter(
            &FilterExpr::Eq {
                field: "missing".into(),
                value: json!("x"),
            },
            &fields,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("unknown field"), "got: {err}");
    }

    #[test]
    fn validate_filter_rejects_empty_and_or_and_empty_in() {
        let fields = filter_unit_fields();
        let err = validate_filter(&FilterExpr::And { exprs: vec![] }, &fields).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("at least one expr"), "got: {err}");

        let err = validate_filter(&FilterExpr::Or { exprs: vec![] }, &fields).unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("at least one expr"), "got: {err}");

        let err = validate_filter(
            &FilterExpr::In {
                field: "name".into(),
                values: vec![],
            },
            &fields,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("at least one value"), "got: {err}");
    }

    #[test]
    fn validate_filter_rejects_a_non_string_number_boolean_value() {
        let fields = filter_unit_fields();
        let err = validate_filter(
            &FilterExpr::Eq {
                field: "name".into(),
                value: Value::Null,
            },
            &fields,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("string, number, or boolean"),
            "got: {err}"
        );

        let err = validate_filter(
            &FilterExpr::Eq {
                field: "tags".into(),
                value: json!(["a"]),
            },
            &fields,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(
            err.message.contains("string, number, or boolean"),
            "got: {err}"
        );
    }

    #[test]
    fn validate_filter_accepts_a_well_formed_nested_filter() {
        let fields = filter_unit_fields();
        validate_filter(
            &FilterExpr::And {
                exprs: vec![
                    FilterExpr::Eq {
                        field: "name".into(),
                        value: json!("ada"),
                    },
                    FilterExpr::In {
                        field: "age".into(),
                        values: vec![json!(1), json!(2)],
                    },
                ],
            },
            &fields,
        )
        .expect("well-formed nested filter");
    }

    #[test]
    fn validate_filter_rejects_mixed_type_in_values() {
        let fields = filter_unit_fields();
        let err = validate_filter(
            &FilterExpr::In {
                field: "age".into(),
                values: vec![json!(5), json!("ada")],
            },
            &fields,
        )
        .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
        assert!(err.message.contains("same type"), "got: {err}");
    }

    #[test]
    fn validate_filter_accepts_same_type_in_values() {
        let fields = filter_unit_fields();
        validate_filter(
            &FilterExpr::In {
                field: "age".into(),
                values: vec![json!(5), json!(6), json!(7)],
            },
            &fields,
        )
        .expect("same-type in values");
    }

    // ---- query: filter end-to-end ----------------------------------
    //
    // Ports `describe("InMemoryRtDbClient filter")`
    // (`ts-client/tests/in_memory.test.ts:655-756`) — exercises the typed
    // `TableQuery.filter(...)` builder end-to-end through `run_query`, the
    // same surface live app code uses.

    /// Self-contained `users` schema so this block doesn't perturb the shared
    /// `items` harness above. Mirrors the TS `usersSchema`.
    fn users_schema() -> SchemaDef {
        Schema::builder()
            .table(
                "users",
                Table::new()
                    .field("name", FieldType::String)
                    .field("age", FieldType::Number)
                    .field("active", FieldType::Boolean)
                    .index("by_name", &["name"]),
            )
            .build()
    }

    fn new_users_client() -> InMemoryRtDbClient {
        let counter = Arc::new(Mutex::new(1_700_000_000_000_i64));
        let mut client = InMemoryRtDbClient::new(
            InMemoryRtDbClientOptions::default()
                .now(move || {
                    let mut g = counter.lock().expect("counter not poisoned");
                    let v = *g;
                    *g += 1;
                    v
                })
                .random(|| 0.0),
        );
        client.push_schema(&users_schema());
        client
    }

    async fn seed_users(c: &mut InMemoryRtDbClient) {
        for (name, age, active) in [("ada", 42_i64, true), ("bob", 17, false), ("cy", 65, true)] {
            c.mutate(
                &Mutation::new()
                    .insert("users", json!({"name": name, "age": age, "active": active}))
                    .build(),
                None,
            )
            .await
            .unwrap();
        }
    }

    #[tokio::test]
    async fn query_filter_reduces_the_result_set_to_matching_docs() {
        let mut c = new_users_client();
        seed_users(&mut c).await;
        let docs = c
            .run::<Vec<Value>>(
                &TableQuery::new("users")
                    .filter(FilterExpr::Gt {
                        field: "age".into(),
                        value: json!(20),
                    })
                    .collect(),
            )
            .expect("filter query ok");
        let mut names: Vec<String> = docs
            .iter()
            .map(|d| d["name"].as_str().unwrap().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["ada".to_string(), "cy".to_string()]);
    }

    #[tokio::test]
    async fn query_filter_composes_with_an_index_eq_prefix_and_take() {
        let mut c = new_users_client();
        seed_users(&mut c).await;
        let docs = c
            .run::<Vec<Value>>(
                &TableQuery::new("users")
                    .with_index("by_name", &[json!("ada")])
                    .filter(FilterExpr::Eq {
                        field: "active".into(),
                        value: json!(true),
                    })
                    .take(10),
            )
            .expect("filter+index ok");
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0]["name"], json!("ada"));
    }

    #[tokio::test]
    async fn query_and_or_in_filter_evaluates_correctly_end_to_end() {
        let mut c = new_users_client();
        seed_users(&mut c).await;

        let docs = c
            .run::<Vec<Value>>(
                &TableQuery::new("users")
                    .filter(FilterExpr::Or {
                        exprs: vec![
                            FilterExpr::Lt {
                                field: "age".into(),
                                value: json!(18),
                            },
                            FilterExpr::Gte {
                                field: "age".into(),
                                value: json!(65),
                            },
                        ],
                    })
                    .collect(),
            )
            .expect("or filter ok");
        let mut names: Vec<String> = docs
            .iter()
            .map(|d| d["name"].as_str().unwrap().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["bob".to_string(), "cy".to_string()]);

        let in_docs = c
            .run::<Vec<Value>>(
                &TableQuery::new("users")
                    .filter(FilterExpr::In {
                        field: "name".into(),
                        values: vec![json!("ada"), json!("cy")],
                    })
                    .collect(),
            )
            .expect("in filter ok");
        let mut names: Vec<String> = in_docs
            .iter()
            .map(|d| d["name"].as_str().unwrap().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["ada".to_string(), "cy".to_string()]);
    }

    #[tokio::test]
    async fn query_filter_unknown_field_throws_bad_request() {
        let mut c = new_users_client();
        seed_users(&mut c).await;
        let err = c
            .run_query(
                &TableQuery::new("users")
                    .filter(FilterExpr::Eq {
                        field: "nope".into(),
                        value: json!("x"),
                    })
                    .collect(),
            )
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
    }

    #[tokio::test]
    async fn query_filter_combined_with_get_is_rejected() {
        // Mirrors the server: `get` is exclusive of `filter` (and everything
        // else); the get-exclusivity guard fires before filter validation.
        let mut c = new_users_client();
        let r = c
            .mutate(
                &Mutation::new()
                    .insert("users", json!({"name": "ada", "age": 42, "active": true}))
                    .build(),
                None,
            )
            .await
            .unwrap();
        let id = match &r[0] {
            StepResult::Insert { id } => id.clone(),
            _ => unreachable!(),
        };
        let err = c
            .run_query(&Query {
                table: "users".into(),
                get: Some(id),
                filter: Some(FilterExpr::Eq {
                    field: "age".into(),
                    value: json!(42),
                }),
                ..Default::default()
            })
            .unwrap_err();
        assert_eq!(err.code, ErrorCode::BadRequest);
    }
}
