//! ENH-032 extends and supersedes the original ENH-027 query-only property
//! testing plan with transaction and migration parity properties.
//!
//! Randomly generated schemas/documents/queries are executed against BOTH the
//! real server (Postgres) and the rust-client in-memory engine, asserting
//! identical results — the generative complement to the fixed
//! `wire-corpus/semantics/` corpus. A divergence found here is exactly the
//! mirror-drift class the audit identified as the repo's top risk, caught with
//! zero per-case authoring cost.
//!
//! Case budget: `PROPTEST_CASES` (env) with a default of 64, so the default
//! `make checkall` run stays fast; CI/nightly can raise the count via the env
//! var — the standard proptest mechanism (see `case_count`).
//!
//! Comparison contract (copies ENH-023's normalization, generalized for
//! generated data per the corpus README's determinism rulings):
//! - System fields `_id`/`_creationTime`/`_version` are projected out of both
//!   sides recursively (the corpus `normalize` default).
//! - `count` terminals compare as bare integers.
//! - A collect with a bound index has a deterministic user-visible order up to
//!   the system tiebreak: both engines sort by (unbound index fields after the
//!   eq prefix, created_at, id). Rows that tie on the unbound index fields are
//!   ordered by the SYSTEM tiebreak, which is engine-specific (server ids are
//!   random uuid v7s; engine ids come from a deterministic clock) — corpus
//!   ruling 2 keeps such ties out of hand-authored cases, but a generator hits
//!   them constantly, so those rows compare as a multiset within their
//!   equal-key run (ruling 2's multiset rule applied to the nondeterministic
//!   suffix only). With no index bound (or a full-arity eq prefix) the sort is
//!   purely the system tiebreak, so the whole result compares as a multiset.
//! - Floats compare numeric-tolerantly (`6 == 6.0`; both sides round-trip the
//!   same JSON representation); int64 values are decimal strings end to end
//!   (ruling 3).
//!
//! Generator scope (v1): scalar index-typable fields only — string, float
//! (`number`), int64, boolean, plus `optional<scalar>` (null/missing cells) and
//! one non-indexable `array<string>` (to exercise `contains`). The plan's
//! "timestamp" is covered by `number`: the schema vocabulary has no timestamp
//! type, and epoch-ms doubles are the repo's convention. Vector/FTS/bytes/id/
//! object/union fields are v2+. Terminals: collect and count only (no take /
//! paginate / aggregates / first / unique / distinct / search). Soft-delete
//! and TTL are off.
//!
//! Divergence protocol (see CONTRIBUTING.md): when proptest finds a real
//! server-vs-engine divergence, fix the engine (the server is the source of
//! truth), commit the `proptest-regressions/` seed file proptest writes, AND
//! add the minimized case to `wire-corpus/semantics/` so all three client
//! engines inherit it. Where the two engines legitimately differ on an edge
//! the spec leaves open, the generator is narrowed and the reason documented
//! here — the one such narrowing in v1:
//! - String ORDER BY parity relies on the cluster's `C` collation (byte
//!   order; see deploy/README.md "Collation") matching Rust's byte-wise `str`
//!   ordering. A linguistic collation would reorder accented/case-mixed text.
//!   Unicode strings are still generated: this dev cluster and the deployment
//!   both pin `--lc-collate=C`.
//!
//! Like every server integration test, the dev Postgres must be up
//! (`make dev-db-up`).

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};

use crate::common::{admin_post, spawn_app, test_state, wrap_test_db};
use par_rt_db_client::in_memory::InMemoryRtDbClientOptions;
use par_rt_db_client::schema::SchemaDef as ClientSchemaDef;
use par_rt_db_client::{
    InMemoryRtDbClient, Query as ClientQuery, Transaction as ClientTransaction,
};
use proptest::collection;
use proptest::prelude::*;
use proptest::strategy::BoxedStrategy;
use proptest::test_runner::{Config, FileFailurePersistence, TestCaseError, TestRunner};
use rtdb_server::auth::PrincipalCtx;
use rtdb_server::db;
use rtdb_server::ddl;
use rtdb_server::query::{Query as ServerQuery, execute_query};
use rtdb_server::schema::SchemaDef as ServerSchemaDef;
use rtdb_server::txn::{Transaction as ServerTransaction, execute_txn};
use serde_json::{Map, Value};

/// Default case count when `PROPTEST_CASES` is unset (the env var wins — the
/// standard proptest knob; see the module docs).
const DEFAULT_CASES: u32 = 64;

fn case_count() -> u32 {
    std::env::var("PROPTEST_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_CASES)
}

/// Deterministic engine clock: a monotonically increasing counter (the same
/// harness pattern as `rust-client/tests/semantics_corpus.rs`), so each insert
/// mints a distinct `_id`/`_creationTime` and the engine's system tiebreak is
/// insert order.
static CLOCK: AtomicI64 = AtomicI64::new(1_700_000_000_000);

// ============ Generator vocabulary ============

/// Scalar index-typable field types (the `indexed_column_type` set minus the
/// id/literal/union text aliases, which v1 does not generate).
#[derive(Clone, Copy, Debug, PartialEq)]
enum Scalar {
    Str,
    Num,
    I64,
    Bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    Scalar(Scalar),
    /// `optional<T>`: doc cells may be present, explicit null (stripped on
    /// insert by both engines), or missing.
    Opt(Scalar),
    /// Non-indexable `array<string>` — exercises `contains`/`exists` only.
    ArrayStr,
}

impl Kind {
    fn scalar_inner(self) -> Option<Scalar> {
        match self {
            Kind::Scalar(s) | Kind::Opt(s) => Some(s),
            Kind::ArrayStr => None,
        }
    }
}

fn scalar_kind() -> impl Strategy<Value = Scalar> {
    prop_oneof![
        4 => Just(Scalar::Str),
        3 => Just(Scalar::Num),
        3 => Just(Scalar::I64),
        2 => Just(Scalar::Bool),
    ]
}

fn kind() -> impl Strategy<Value = Kind> {
    prop_oneof![
        6 => scalar_kind().prop_map(Kind::Scalar),
        3 => scalar_kind().prop_map(Kind::Opt),
        2 => Just(Kind::ArrayStr),
    ]
}

/// Small value pools with heavy overlap between the doc and filter pools, so
/// matches (not just misses) are generated; boundary values ride along.
fn string_value() -> BoxedStrategy<String> {
    prop_oneof![
        4 => Just(String::from("a")),
        4 => Just(String::from("b")),
        3 => Just(String::from("c")),
        2 => Just(String::new()),
        2 => Just(String::from("ab")),
        1 => Just(String::from("日")),
        1 => Just(String::from("é")),
        1 => Just(String::from("🦀")),
        3 => collection::vec((b'a'..=b'z').prop_map(char::from), 0..=4)
            .prop_map(|cs| cs.into_iter().collect::<String>()),
    ]
    .boxed()
}

/// Finite, modest floats — JSON-representable, round-tripping through both
/// jsonb (doc) and double precision (typed column) unchanged. `-0.0` is
/// deliberately excluded: jsonb numeric normalizes it to `0`, which would
/// split equal-sort-key runs across the two representations.
fn num_value() -> impl Strategy<Value = f64> {
    prop_oneof![
        6 => (-4i64..=4).prop_map(|i| i as f64),
        2 => Just(0.5),
        2 => Just(-2.5),
        1 => Just(100.25),
        1 => Just(1_000_000.5),
    ]
}

/// int64 pool mixing digit-count-variant values (9/15/42/1000 — lexicographic
/// vs numeric ordering diverges on exactly these) with the i64 boundaries.
fn i64_value() -> impl Strategy<Value = i64> {
    prop_oneof![
        4 => 0i64..=15,
        2 => Just(-15),
        2 => Just(42),
        2 => Just(-1),
        1 => Just(i64::MAX),
        1 => Just(i64::MIN),
        1 => Just(1_000_000_000_000),
        2 => -1000i64..=1000,
    ]
}

fn number_value(f: f64) -> Value {
    Value::Number(serde_json::Number::from_f64(f).expect("generator emits finite f64"))
}

/// A doc cell for one scalar kind.
fn scalar_doc_value(s: Scalar) -> BoxedStrategy<Value> {
    match s {
        Scalar::Str => string_value().prop_map(Value::String).boxed(),
        Scalar::Num => num_value().prop_map(number_value).boxed(),
        Scalar::I64 => i64_value()
            .prop_map(|i| Value::String(i.to_string()))
            .boxed(),
        Scalar::Bool => any::<bool>().prop_map(Value::Bool).boxed(),
    }
}

/// One doc cell for a declared field kind (position-known, so the strategy is
/// exact — no cross-type coercion). `None` = field omitted from the doc.
fn doc_cell(k: Kind) -> BoxedStrategy<Option<Value>> {
    match k {
        Kind::Scalar(s) => scalar_doc_value(s).prop_map(Some).boxed(),
        Kind::Opt(s) => prop_oneof![
            6 => scalar_doc_value(s).prop_map(Some),
            2 => Just(Some(Value::Null)),
            2 => Just(None),
        ]
        .boxed(),
        Kind::ArrayStr => collection::vec(string_value(), 0..=3)
            .prop_map(|v| {
                Some(Value::Array(
                    v.into_iter().map(Value::String).collect::<Vec<_>>(),
                ))
            })
            .boxed(),
    }
}

/// A whole doc as a per-field cell vector, composed by recursion over the
/// field list (runtime-length tuples are not otherwise expressible).
fn doc_cells(fields: &[(String, Kind)]) -> BoxedStrategy<Vec<Option<Value>>> {
    if fields.is_empty() {
        return Just(Vec::new()).boxed();
    }
    let head = fields[0].1;
    let tail = fields[1..].to_vec();
    doc_cell(head)
        .prop_flat_map(move |head_cell| {
            doc_cells(&tail).prop_map(move |mut rest| {
                rest.insert(0, head_cell.clone());
                rest
            })
        })
        .boxed()
}

#[derive(Clone, Debug)]
struct TableCase {
    name: String,
    fields: Vec<(String, Kind)>,
    /// (index name, field positions into `fields`).
    indexes: Vec<(String, Vec<usize>)>,
    docs: Vec<Map<String, Value>>,
}

fn field_name(i: usize) -> String {
    format!("f_{}", char::from(b'a' + i as u8))
}

/// The table's index list from the indexed flags: one single-field index per
/// flagged scalar/optional field, plus one compound index over the first two
/// flagged fields when there are at least two (drives eq-prefix + range +
/// multi-column ordering). Array fields are never indexable, so flags on them
/// are ignored; at least one index is always forced (the plan wants an indexed
/// subset, and order-by needs an index).
fn derive_indexes(fields: &[(String, Kind)], flags: &[bool]) -> Vec<(String, Vec<usize>)> {
    let mut flagged: Vec<usize> = fields
        .iter()
        .enumerate()
        .filter(|(i, (_, k))| flags.get(*i).copied().unwrap_or(false) && k.scalar_inner().is_some())
        .map(|(i, _)| i)
        .collect();
    if flagged.is_empty() {
        flagged = fields
            .iter()
            .position(|(_, k)| k.scalar_inner().is_some())
            .into_iter()
            .collect();
    }
    let mut indexes: Vec<(String, Vec<usize>)> = flagged
        .iter()
        .map(|&i| (format!("by_{}", fields[i].0), vec![i]))
        .collect();
    if flagged.len() >= 2 {
        let name = format!("by_{}_{}", fields[flagged[0]].0, fields[flagged[1]].0);
        indexes.push((name, vec![flagged[0], flagged[1]]));
    }
    indexes
}

fn table_strategy() -> BoxedStrategy<TableCase> {
    (2usize..=6)
        .prop_flat_map(|n| {
            (
                collection::vec(kind(), n),
                collection::vec(any::<bool>(), n),
            )
        })
        .prop_flat_map(|(mut kinds, flags)| {
            // Every table needs at least one indexable (scalar) field for the
            // forced index below — otherwise an all-array table has no legal
            // index and the index-selection range would be empty.
            if kinds[0] == Kind::ArrayStr {
                kinds[0] = Kind::Scalar(Scalar::Str);
            }
            let fields: Vec<(String, Kind)> = kinds
                .iter()
                .enumerate()
                .map(|(i, k)| (field_name(i), *k))
                .collect();
            let names: Vec<String> = fields.iter().map(|(n, _)| n.clone()).collect();
            let doc = doc_cells(&fields).prop_map(move |cells| {
                cells
                    .into_iter()
                    .zip(names.iter().cloned())
                    .filter_map(|(cell, name)| cell.map(|v| (name, v)))
                    .collect::<Map<String, Value>>()
            });
            (
                Just(fields.clone()),
                Just(flags.clone()),
                collection::vec(doc, 5..=50),
            )
        })
        .prop_map(|(fields, flags, docs)| {
            let indexes = derive_indexes(&fields, &flags);
            TableCase {
                name: String::new(), // assigned positionally at case assembly
                fields,
                indexes,
                docs,
            }
        })
        .boxed()
}

// ============ Query generation ============

/// A filter/query bind value for a field of kind `k`, in the WIRE form the
/// server accepts for that position: eq/range binds and filter values on
/// indexed fields go through `eq_bind_for` (int64 binds are decimal STRINGS),
/// while filter values on non-indexed declared fields go through the jsonb
/// path (int64 compares as a JSON NUMBER — see the server's
/// `validate_jsonb_comparison_value`). `contains` values on `array<string>`
/// are strings — the one kind where the SQL and in-memory evaluators agree.
fn wire_value(k: Kind, indexed: bool) -> BoxedStrategy<Value> {
    let Some(s) = k.scalar_inner() else {
        return string_value().prop_map(Value::String).boxed();
    };
    match s {
        Scalar::Str => string_value().prop_map(Value::String).boxed(),
        Scalar::Bool => any::<bool>().prop_map(Value::Bool).boxed(),
        Scalar::Num => num_value().prop_map(number_value).boxed(),
        Scalar::I64 => {
            if indexed {
                i64_value()
                    .prop_map(|i| Value::String(i.to_string()))
                    .boxed()
            } else {
                i64_value().prop_map(|i| number_value(i as f64)).boxed()
            }
        }
    }
}

/// One field as the filter generator sees it.
#[derive(Clone, Debug)]
struct FieldRef {
    name: String,
    kind: Kind,
    indexed: bool,
}

fn table_field_refs(table: &TableCase) -> Vec<FieldRef> {
    table
        .fields
        .iter()
        .enumerate()
        .map(|(pos, (name, kind))| FieldRef {
            name: name.clone(),
            kind: *kind,
            indexed: table.indexes.iter().any(|(_, fs)| fs.contains(&pos)),
        })
        .collect()
}

fn filter_cmp_leaf(fr: &FieldRef) -> BoxedStrategy<Value> {
    let op = prop_oneof![
        2 => Just("eq"),
        2 => Just("neq"),
        2 => Just("gt"),
        2 => Just("gte"),
        2 => Just("lt"),
        2 => Just("lte"),
    ];
    let fr = fr.clone();
    (op, wire_value(fr.kind, fr.indexed))
        .prop_map(
            move |(op, value)| serde_json::json!({"op": op, "field": fr.name, "value": value}),
        )
        .boxed()
}

fn filter_in_leaf(fr: &FieldRef) -> BoxedStrategy<Value> {
    let fr = fr.clone();
    collection::vec(wire_value(fr.kind, fr.indexed), 1..=3)
        .prop_map(move |values| serde_json::json!({"op": "in", "field": fr.name, "values": values}))
        .boxed()
}

fn filter_leaf(fields: Arc<Vec<FieldRef>>) -> BoxedStrategy<Value> {
    let n = fields.len();
    (0..n)
        .prop_flat_map(move |i| {
            let fr = fields[i].clone();
            let exists = Just(serde_json::json!({"op": "exists", "field": fr.name}));
            if fr.kind == Kind::ArrayStr {
                prop_oneof![
                    3 => string_value().prop_map(move |value| serde_json::json!({
                        "op": "contains", "field": fr.name, "value": value
                    })),
                    2 => exists,
                ]
                .boxed()
            } else {
                prop_oneof![
                    7 => filter_cmp_leaf(&fr),
                    2 => filter_in_leaf(&fr),
                    1 => exists,
                ]
                .boxed()
            }
        })
        .boxed()
}

/// Filter trees to depth 3 over the DSL's actual operator set
/// (eq/neq/gt/gte/lt/lte/in/and/or/not/contains/exists — enumerated from
/// `FilterExpr` in `server/src/dsl.rs`, not assumed).
fn filter_tree(fields: Arc<Vec<FieldRef>>, depth: u8) -> BoxedStrategy<Value> {
    if depth == 0 {
        return filter_leaf(fields).boxed();
    }
    let sub = filter_tree(fields.clone(), depth - 1);
    let not_sub = sub.clone();
    let and_sub = sub.clone();
    let or_sub = sub;
    let leaf = filter_leaf(fields);
    prop_oneof![
        6 => leaf,
        2 => not_sub.prop_map(|expr| serde_json::json!({"op": "not", "expr": expr})),
        3 => collection::vec(and_sub, 1..=3)
            .prop_map(|exprs| serde_json::json!({"op": "and", "exprs": exprs})),
        3 => collection::vec(or_sub, 1..=3)
            .prop_map(|exprs| serde_json::json!({"op": "or", "exprs": exprs})),
    ]
    .boxed()
}

fn optional_filter(fields: Arc<Vec<FieldRef>>) -> BoxedStrategy<Option<Value>> {
    prop_oneof![
        3 => Just(None),
        7 => filter_tree(fields, 3).prop_map(Some),
    ]
    .boxed()
}

#[derive(Clone, Debug)]
struct QueryCase {
    table: usize,
    json: Value,
    /// `Some((eq_len, sort_field_names))` when the query binds an index:
    /// sort fields are the index fields after the eq prefix — the
    /// user-deterministic sort columns for the run-grouped ordered compare.
    /// `None` = no index bound → the result compares as a multiset.
    sort: Option<(usize, Vec<String>)>,
}

/// A runtime-length tuple of typed bind values (an eq prefix), composed by
/// recursion over the field kinds — same shape as `doc_cells`.
fn value_vec(kinds: &[Kind]) -> BoxedStrategy<Vec<Value>> {
    if kinds.is_empty() {
        return Just(Vec::new()).boxed();
    }
    let head = kinds[0];
    let tail = kinds[1..].to_vec();
    wire_value(head, true)
        .prop_flat_map(move |head_value| {
            value_vec(&tail).prop_map(move |mut rest| {
                rest.insert(0, head_value.clone());
                rest
            })
        })
        .boxed()
}

/// One optional range bound on the index field after the eq prefix:
/// `None`, or `Some((inclusive, value))` — the caller renders lower bounds as
/// gt/gte and upper bounds as lt/lte. `None` for the kind when the eq prefix
/// consumed the whole index (the DSL rejects a range then).
fn bound_strategy(kind: Option<Kind>) -> BoxedStrategy<Option<(bool, Value)>> {
    let Some(kind) = kind else {
        return Just(None).boxed();
    };
    prop_oneof![
        3 => Just(None),
        2 => wire_value(kind, true).prop_map(|v| Some((false, v))),
        2 => wire_value(kind, true).prop_map(|v| Some((true, v))),
    ]
    .boxed()
}

#[allow(clippy::too_many_arguments)]
fn query_json(
    table: &TableCase,
    index: Option<&str>,
    eq: &[Value],
    lower: &Option<(bool, Value)>,
    upper: &Option<(bool, Value)>,
    order: Option<&str>,
    filter: Option<Value>,
    count: bool,
) -> Value {
    let mut m = Map::new();
    m.insert("table".into(), Value::String(table.name.clone()));
    if let Some(index) = index {
        m.insert("index".into(), Value::String(index.to_string()));
        if !eq.is_empty() {
            m.insert("eq".into(), Value::Array(eq.to_vec()));
        }
    }
    if let Some((inclusive, v)) = lower {
        m.insert(if *inclusive { "gte" } else { "gt" }.into(), v.clone());
    }
    if let Some((inclusive, v)) = upper {
        m.insert(if *inclusive { "lte" } else { "lt" }.into(), v.clone());
    }
    if let Some(o) = order {
        m.insert("order".into(), Value::String(o.to_string()));
    }
    if let Some(f) = filter {
        m.insert("filter".into(), f);
    }
    if count {
        m.insert("count".into(), Value::Bool(true));
    }
    Value::Object(m)
}

/// An index-bound query: eq prefix (0..=index arity), optional range bounds on
/// the field after the prefix, optional order (never with `count` — the DSL
/// rejects that combination), optional filter, collect or count terminal.
fn indexed_query(
    tbl_idx: usize,
    table: Arc<TableCase>,
    idx_count: usize,
) -> BoxedStrategy<QueryCase> {
    (0..idx_count)
        .prop_flat_map(move |j| {
            let table = table.clone();
            let (idx_name, idx_fields) = table.indexes[j].clone();
            let arity = idx_fields.len();
            (0..=arity)
                .prop_flat_map(move |eq_len| {
                    let table = table.clone();
                    let idx_name = idx_name.clone();
                    let idx_fields = idx_fields.clone();
                    let eq_kinds: Vec<Kind> =
                        (0..eq_len).map(|p| table.fields[idx_fields[p]].1).collect();
                    let eq = value_vec(&eq_kinds);
                    let range_kind = (eq_len < arity).then(|| table.fields[idx_fields[eq_len]].1);
                    let lower = bound_strategy(range_kind);
                    let upper = bound_strategy(range_kind);
                    let filter = optional_filter(Arc::new(table_field_refs(&table)));
                    let count = prop_oneof![3 => Just(false), 1 => Just(true)];
                    let order = prop_oneof![
                        2 => Just(None),
                        2 => Just(Some("asc")),
                        2 => Just(Some("desc")),
                    ];
                    (eq, lower, upper, filter, count, order).prop_map(
                        move |(eq, lower, upper, filter, count, order)| {
                            let order = if count { None } else { order };
                            let sort_fields: Vec<String> = idx_fields[eq.len()..]
                                .iter()
                                .map(|p| table.fields[*p].0.clone())
                                .collect();
                            QueryCase {
                                table: tbl_idx,
                                sort: Some((eq.len(), sort_fields)),
                                json: query_json(
                                    &table,
                                    Some(&idx_name),
                                    &eq,
                                    &lower,
                                    &upper,
                                    order,
                                    filter,
                                    count,
                                ),
                            }
                        },
                    )
                })
                .boxed()
        })
        .boxed()
}

/// A no-index query: the DSL rejects eq/range/order without an index, so this
/// is an optional filter plus collect or count — full multiset territory.
fn plain_query(tbl_idx: usize, table: Arc<TableCase>) -> BoxedStrategy<QueryCase> {
    let filter = optional_filter(Arc::new(table_field_refs(&table)));
    let count = prop_oneof![2 => Just(false), 1 => Just(true)];
    (filter, count)
        .prop_map(move |(filter, count)| QueryCase {
            table: tbl_idx,
            sort: None,
            json: query_json(&table, None, &[], &None, &None, None, filter, count),
        })
        .boxed()
}

fn query_for(tbl_idx: usize, table: Arc<TableCase>) -> BoxedStrategy<QueryCase> {
    let idx_count = table.indexes.len();
    prop_oneof![3 => Just(true), 1 => Just(false)]
        .prop_flat_map(move |use_index| {
            if use_index {
                indexed_query(tbl_idx, table.clone(), idx_count)
            } else {
                plain_query(tbl_idx, table.clone())
            }
        })
        .boxed()
}

#[derive(Clone, Debug)]
struct Case {
    tables: Vec<TableCase>,
    queries: Vec<QueryCase>,
}

impl Case {
    fn schema_json(&self) -> Value {
        let mut tables = Map::new();
        for t in &self.tables {
            let mut fields = Map::new();
            for (name, kind) in &t.fields {
                fields.insert(name.clone(), kind_json(*kind));
            }
            let indexes: Vec<Value> = t
                .indexes
                .iter()
                .map(|(n, fs)| {
                    serde_json::json!({
                        "name": n,
                        "fields": fs.iter().map(|p| t.fields[*p].0.clone()).collect::<Vec<_>>()
                    })
                })
                .collect();
            tables.insert(
                t.name.clone(),
                serde_json::json!({"fields": fields, "indexes": indexes}),
            );
        }
        serde_json::json!({"tables": tables})
    }

    /// One seed txn carrying every table's inserts (well under `MAX_STEPS` =
    /// 1024: ≤ 3 tables × 50 docs). Steps serialize in the wire's tagged form
    /// (`{"op": "insert", ...}` — `Step` in `server/src/dsl.rs`).
    fn seed_txn_json(&self) -> Value {
        let steps: Vec<Value> = self
            .tables
            .iter()
            .flat_map(|t| {
                t.docs
                    .iter()
                    .map(|doc| serde_json::json!({"op": "insert", "table": t.name, "doc": doc}))
            })
            .collect();
        serde_json::json!({"steps": steps})
    }
}

fn kind_json(k: Kind) -> Value {
    fn scalar_json(s: Scalar) -> Value {
        match s {
            Scalar::Str => serde_json::json!({"type": "string"}),
            Scalar::Num => serde_json::json!({"type": "number"}),
            Scalar::I64 => serde_json::json!({"type": "int64"}),
            Scalar::Bool => serde_json::json!({"type": "boolean"}),
        }
    }
    match k {
        Kind::Scalar(s) => scalar_json(s),
        Kind::Opt(s) => serde_json::json!({"type": "optional", "inner": scalar_json(s)}),
        Kind::ArrayStr => serde_json::json!({"type": "array", "element": {"type": "string"}}),
    }
}

fn case_strategy() -> BoxedStrategy<Case> {
    (1usize..=3)
        .prop_flat_map(|n| collection::vec(table_strategy(), n))
        .prop_flat_map(|mut tables| {
            for (i, t) in tables.iter_mut().enumerate() {
                t.name = format!("t{i}");
            }
            let tables: Vec<Arc<TableCase>> = tables.into_iter().map(Arc::new).collect();
            let n = tables.len();
            let qtables = Arc::new(tables);
            let just_tables = qtables.clone();
            let queries = collection::vec(
                (0..n).prop_flat_map(move |ti| query_for(ti, qtables[ti].clone())),
                1..=3,
            );
            (Just(just_tables), queries)
        })
        .prop_map(|(tables, queries)| Case {
            tables: tables.iter().map(|t| (**t).clone()).collect(),
            queries,
        })
        .boxed()
}

// ============ Oracle loop ============

async fn run_case(pool: &sqlx::PgPool, case: &Case) -> Result<(), String> {
    // `t<32-hex>` matches the `fresh_db` naming convention deliberately: the
    // RAII worker drops databases asynchronously while the run proceeds, but
    // the handful still queued when the process exits leak — and
    // `make dev-db-clean` (scripts/dev-db-clean.sql) only sweeps
    // `^db_t[0-9a-f]{32}$` schemas and their registry rows.
    let db_name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(pool, &db_name)
        .await
        .map_err(|e| format!("create database: {e:?}"))?;
    let _guard = wrap_test_db(db_name.clone());

    let schema_json = case.schema_json();
    let seed_json = case.seed_txn_json();

    // Server side: push schema, seed via the normal txn path, then queries.
    let schema: ServerSchemaDef = serde_json::from_value(schema_json.clone())
        .map_err(|e| format!("server schema parse (generator bug): {e}"))?;
    ddl::push_schema(pool, &db_name, schema.clone())
        .await
        .map_err(|e| format!("push_schema (generator bug?): {e:?}"))?;
    let seed: ServerTransaction = serde_json::from_value(seed_json.clone())
        .map_err(|e| format!("server seed parse (generator bug): {e}"))?;
    execute_txn(pool, &db_name, &schema, &seed, &PrincipalCtx::bypass())
        .await
        .map_err(|e| format!("server seed txn (generator bug?): {e:?}"))?;

    // Engine side: the same schema + seed through the public surface.
    let mut client = InMemoryRtDbClient::new(
        InMemoryRtDbClientOptions::default()
            .now(|| CLOCK.fetch_add(1, AtomicOrdering::SeqCst))
            .random(|| 0.0),
    );
    let client_schema: ClientSchemaDef = serde_json::from_value(schema_json)
        .map_err(|e| format!("client schema parse (wire drift): {e}"))?;
    client
        .push_schema(&client_schema)
        .map_err(|e| format!("client push_schema (wire drift): {e:?}"))?;
    let client_seed: ClientTransaction = serde_json::from_value(seed_json)
        .map_err(|e| format!("client seed parse (wire drift): {e}"))?;
    client
        .mutate(&client_seed, None)
        .await
        .map_err(|e| format!("client seed mutate (engine bug): {e:?}"))?;

    for q in &case.queries {
        let server_query: ServerQuery = serde_json::from_value(q.json.clone())
            .map_err(|e| format!("server query parse (generator bug): {e} — {}", q.json))?;
        let client_query: ClientQuery = serde_json::from_value(q.json.clone())
            .map_err(|e| format!("client query parse (wire drift): {e} — {}", q.json))?;

        let server_result = execute_query(
            pool,
            &db_name,
            &schema,
            &server_query,
            &PrincipalCtx::bypass(),
            false,
        )
        .await
        .map_err(|e| {
            format!(
                "server query error (generator bug?): {:?} {} — {}",
                e.code, e.message, q.json
            )
        })?;
        let server_value =
            serde_json::to_value(&server_result).map_err(|e| format!("serialize result: {e}"))?;

        let engine_value = client.run_query(&client_query).map_err(|e| {
            format!(
                "engine query error: {:?} {} — {}",
                e.code, e.message, q.json
            )
        })?;

        compare_results(q, &server_value, &engine_value)
            .map_err(|detail| format!("table {}: {detail}\nquery: {}", q.table, q.json))?;
    }
    Ok(())
}

// ============ Comparison (ENH-023 normalization) ============

/// System fields minted at run time, projected out of both sides recursively.
const NORMALIZE: [&str; 3] = ["_id", "_creationTime", "_version"];

fn project_recursive(node: &mut Value, keys: &[&str]) {
    match node {
        Value::Object(m) => {
            for k in keys {
                m.remove(*k);
            }
            for v in m.values_mut() {
                project_recursive(v, keys);
            }
        }
        Value::Array(a) => {
            for v in a.iter_mut() {
                project_recursive(v, keys);
            }
        }
        _ => {}
    }
}

/// Canonical JSON for multiset sorting (serde_json's map is a BTreeMap, so
/// compact serialization already sorts keys recursively).
fn canonical(v: &Value) -> String {
    serde_json::to_string(v).expect("canonical serialize")
}

/// Numeric-tolerant equality (`6 == 6.0`) — the corpus comparison contract.
fn json_eq_numeric(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(xf), Some(yf)) => (xf - yf).abs() < f64::EPSILON || xf == yf,
            _ => x == y,
        },
        (Value::Null, Value::Null) => true,
        (Value::Array(x), Value::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| json_eq_numeric(a, b))
        }
        (Value::Object(x), Value::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).is_some_and(|yv| json_eq_numeric(v, yv)))
        }
        _ => a == b,
    }
}

fn multiset_equal(a: &[Value], b: &[Value]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut as_: Vec<&Value> = a.iter().collect();
    let mut bs: Vec<&Value> = b.iter().collect();
    as_.sort_by_key(|v| canonical(v));
    bs.sort_by_key(|v| canonical(v));
    as_.iter()
        .zip(bs.iter())
        .all(|(x, y)| json_eq_numeric(x, y))
}

/// The sort key of one normalized row under the query's deterministic sort
/// columns: the doc values of the index fields after the eq prefix (absent
/// field → null — both engines order nulls identically, so keys stay
/// comparable).
fn row_sort_key(row: &Value, sort_fields: &[String]) -> Vec<Value> {
    sort_fields
        .iter()
        .map(|f| row.get(f).cloned().unwrap_or(Value::Null))
        .collect()
}

fn keys_equal(a: &[Value], b: &[Value]) -> bool {
    a.len() == b.len() && a.iter().zip(b).all(|(x, y)| json_eq_numeric(x, y))
}

/// Ordered-with-run-grouping compare: split each side into maximal runs of
/// rows sharing an equal sort key. The run SEQUENCE (keys and lengths) is
/// fully deterministic; membership WITHIN a run is ordered by the system
/// tiebreak, which is engine-specific, so each run compares as a multiset.
fn ordered_runs_equal(
    server: &[Value],
    engine: &[Value],
    sort_fields: &[String],
) -> Result<(), String> {
    if server.len() != engine.len() {
        return Err(format!(
            "row count mismatch (ordered): server {} vs engine {}",
            server.len(),
            engine.len()
        ));
    }
    let mut i = 0usize;
    let mut j = 0usize;
    while i < server.len() {
        let key = row_sort_key(&server[i], sort_fields);
        let mut si = i + 1;
        while si < server.len() && keys_equal(&row_sort_key(&server[si], sort_fields), &key) {
            si += 1;
        }
        if j >= engine.len() || !keys_equal(&row_sort_key(&engine[j], sort_fields), &key) {
            return Err(format!(
                "run-boundary mismatch at row {i}: server sort key {key:?} vs engine sort key {:?}",
                engine.get(j).map(|r| row_sort_key(r, sort_fields))
            ));
        }
        let mut ej = j + 1;
        while ej < engine.len() && keys_equal(&row_sort_key(&engine[ej], sort_fields), &key) {
            ej += 1;
        }
        let (srun, erun) = (&server[i..si], &engine[j..ej]);
        if srun.len() != erun.len() {
            return Err(format!(
                "run length mismatch for key {key:?}: server {} vs engine {}",
                srun.len(),
                erun.len()
            ));
        }
        if !multiset_equal(srun, erun) {
            return Err(format!(
                "rows differ within an equal-sort-key run (key {key:?}) — server {srun:?} vs engine {erun:?}"
            ));
        }
        i = si;
        j = ej;
    }
    Ok(())
}

fn compare_results(q: &QueryCase, server: &Value, engine: &Value) -> Result<(), String> {
    // count terminal: a bare integer on both sides.
    if q.json.get("count").and_then(Value::as_bool) == Some(true) {
        return match (server.as_i64(), engine.as_i64()) {
            (Some(s), Some(e)) if s == e => Ok(()),
            _ => Err(format!(
                "count mismatch: server {server} vs engine {engine}"
            )),
        };
    }
    let Value::Array(sraw) = server else {
        return Err(format!(
            "result shape mismatch (server): {server} vs engine {engine}"
        ));
    };
    let Value::Array(eraw) = engine else {
        return Err(format!(
            "result shape mismatch (engine): {server} vs engine {engine}"
        ));
    };
    let mut srows = sraw.clone();
    for row in srows.iter_mut() {
        project_recursive(row, &NORMALIZE);
    }
    let mut erows = eraw.clone();
    for row in erows.iter_mut() {
        project_recursive(row, &NORMALIZE);
    }

    let empty_sort = q.sort.as_ref().is_some_and(|(_, f)| f.is_empty());
    match (&q.sort, empty_sort) {
        (None, _) | (Some(_), true) => {
            // No index bound, or the eq prefix consumed the whole index: every
            // row ties on the user columns — pure system ordering — so the
            // result compares as a multiset.
            if multiset_equal(&srows, &erows) {
                Ok(())
            } else {
                Err(format!(
                    "multiset mismatch: server {srows:?} vs engine {erows:?}"
                ))
            }
        }
        (Some((_, sort_fields)), false) => ordered_runs_equal(&srows, &erows, sort_fields),
    }
}

#[test]
fn query_dsl_server_vs_in_memory_parity() {
    let rt = tokio::runtime::Runtime::new().expect("build tokio runtime");
    let pool = rt.block_on(async {
        let state = test_state().await;
        state.pool.clone()
    });

    let mut runner = TestRunner::new(Config {
        // A manually-driven TestRunner has no source-file context, so point
        // failure persistence at a fixed file (cargo runs test binaries with
        // cwd = the package root, i.e. `server/`). Committed per the plan so
        // found counterexamples re-run forever.
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "proptest-regressions/proptest_parity.txt",
        ))),
        ..Config::with_cases(case_count())
    });
    let strategy = case_strategy();
    let result = runner.run(&strategy, |case| {
        rt.block_on(run_case(&pool, &case))
            .map_err(TestCaseError::fail)
    });
    result.expect("query DSL parity property failed (counterexample above)");
}
// ============ ENH-032: Transaction generator + oracle ============
//
// Step variants enumerate every supported non-workflow mutation, including
// scheduling and soft-delete restoration. Workflow steps remain narrowed
// because the in-memory harness intentionally reports Internal for them.

#[derive(Clone, Debug)]
struct TxnCase {
    tables: Vec<TableCase>,
    steps: Vec<Value>,
}

fn id_pick() -> impl Strategy<Value = &'static str> {
    prop_oneof![8 => Just("seed-id"), 2 => Just("missing-id")]
}
fn schedule_id_pick() -> impl Strategy<Value = &'static str> {
    prop_oneof![8 => Just("schedule-id"), 2 => Just("missing-schedule-id")]
}
fn version_pick() -> impl Strategy<Value = i64> {
    prop_oneof![8 => Just(1i64), 2 => Just(999i64)]
}
fn typed_patch_fields(table: &TableCase) -> BoxedStrategy<Map<String, Value>> {
    let (name, kind) = table.fields[0].clone();
    let values = match kind {
        Kind::Scalar(s) | Kind::Opt(s) => scalar_doc_value(s),
        Kind::ArrayStr => collection::vec(string_value(), 0..=3)
            .prop_map(|v| Value::Array(v.into_iter().map(Value::String).collect()))
            .boxed(),
    };
    values
        .prop_map(move |v| {
            let mut fields = Map::new();
            fields.insert(name.clone(), v);
            fields
        })
        .boxed()
}
fn seed_index_eq(table: &TableCase) -> Option<(String, Vec<Value>)> {
    let (name, positions) = table.indexes.first().cloned()?;
    let doc = table.docs.iter().find(|doc| {
        positions.iter().all(|p| {
            table
                .fields
                .get(*p)
                .and_then(|(n, _)| doc.get(n))
                .is_some_and(|v| !v.is_null())
        })
    })?;
    let eq = positions
        .into_iter()
        .map(|p| {
            table
                .fields
                .get(p)
                .and_then(|(n, _)| doc.get(n).cloned())
                .ok_or(())
        })
        .collect::<Result<Vec<_>, _>>()
        .ok()?;
    Some((name, eq))
}
fn index_eq_strategy(table: Arc<TableCase>) -> BoxedStrategy<(String, Vec<Value>)> {
    let kinds = table
        .indexes
        .first()
        .map(|(_, ps)| {
            ps.iter()
                .filter_map(|p| table.fields.get(*p).map(|(_, k)| *k))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let generated_name = table
        .indexes
        .first()
        .map(|(n, _)| n.clone())
        .unwrap_or_else(|| "by_f_a".into());
    let generated = value_vec(&kinds).prop_map(move |v| (generated_name.clone(), v));
    match seed_index_eq(&table) {
        Some(present) => prop_oneof![8 => Just(present), 2 => generated].boxed(),
        None => generated.boxed(),
    }
}
fn gen_step(table: Arc<TableCase>) -> BoxedStrategy<Value> {
    let t = table.name.clone();
    let doc = table.docs.first().cloned().unwrap_or_default();
    let fields = Arc::new(table_field_refs(&table));
    let pbq = {
        let t = t.clone();
        (filter_tree(fields.clone(),2), typed_patch_fields(&table)).prop_map(move |(f,p)| serde_json::json!({"op":"patchByQuery","table":t,"filter":f,"patch":p,"limit":1})).boxed()
    };
    let dbq = {
        let t = t.clone();
        filter_tree(fields, 2)
            .prop_map(
                move |f| serde_json::json!({"op":"deleteByQuery","table":t,"filter":f,"limit":1}),
            )
            .boxed()
    };
    let idx = index_eq_strategy(table.clone());
    prop_oneof![
        2 => Just(serde_json::json!({"op":"insert","table":t,"doc":doc})),
        2 => { let t=t.clone(); (id_pick(),typed_patch_fields(&table)).prop_map(move |(id,p)| serde_json::json!({"op":"patch","table":t,"id":id,"fields":p})).boxed() },
        2 => { let t=t.clone(); let d=doc.clone(); id_pick().prop_map(move |id| serde_json::json!({"op":"replace","table":t,"id":id,"doc":d})).boxed() },
        2 => { let t=t.clone(); id_pick().prop_map(move |id| serde_json::json!({"op":"delete","table":t,"id":id})).boxed() },
        2 => { let t=t.clone(); (id_pick(),version_pick()).prop_map(move |(id,v)| serde_json::json!({"op":"expectVersion","table":t,"id":id,"version":v})).boxed() },
        2 => { let t=t.clone(); idx.clone().prop_map(move |(i,e)| serde_json::json!({"op":"expectAbsent","table":t,"index":i,"eq":e})).boxed() },
        2 => { let t=t.clone(); let d=doc.clone(); (idx,typed_patch_fields(&table)).prop_map(move |((i,e),p)| serde_json::json!({"op":"upsert","table":t,"index":i,"eq":e,"insert":d,"patch":p})).boxed() },
        2 => pbq,
        2 => dbq,
        2 => { let t=t.clone(); id_pick().prop_map(move |id| serde_json::json!({"op":"undelete","table":t,"id":id})).boxed() },
        2 => { let t=t.clone(); let d=doc.clone(); Just(serde_json::json!({"op":"schedule","when":{"type":"afterMs","ms":60000},"txn":{"steps":[{"op":"insert","table":t,"doc":d}]}})).boxed() },
        2 => schedule_id_pick().prop_map(|id| serde_json::json!({"op":"cancelSchedule","id":id})).boxed(),
    ].boxed()
}
fn txn_case_strategy() -> BoxedStrategy<TxnCase> {
    table_strategy()
        .prop_flat_map(|mut t| {
            t.name = "t0".into();
            let t = Arc::new(t);
            collection::vec(gen_step(t.clone()), 1..=8).prop_map(move |steps| TxnCase {
                tables: vec![(*t).clone()],
                steps,
            })
        })
        .boxed()
}
fn overflow_txn_strategy() -> BoxedStrategy<TxnCase> {
    table_strategy().prop_flat_map(|mut t| { t.name="t0".into(); let fields=Arc::new(table_field_refs(&t)); filter_tree(fields,2).prop_map(move |f| TxnCase{tables:vec![t.clone()],steps:(0..17).map(|_| serde_json::json!({"op":"patchByQuery","table":"t0","filter":f,"patch":{},"limit":1})).collect()}) }).boxed()
}

fn txn_schema_json(case: &TxnCase) -> Value {
    let mut schema = Case {
        tables: case.tables.clone(),
        queries: Vec::new(),
    }
    .schema_json();
    let table = &mut schema["tables"]["t0"];
    table["softDelete"] = Value::Bool(true);
    if let Some(source) = case.tables[0].fields.iter().find_map(|(name, kind)| {
        kind.scalar_inner()
            .filter(|s| *s == Scalar::Str)
            .map(|_| name.clone())
    }) {
        table["fields"]["computed_label"] = serde_json::json!({"type":"string"});
        table["computed"] = serde_json::json!({"computed_label":{"op":"concat","parts":[
            {"op":"field","field":source},{"op":"literal","value":""}
        ]}});
    }
    schema
}

fn txn_seed_json(case: &TxnCase) -> Value {
    Case {
        tables: case.tables.clone(),
        queries: Vec::new(),
    }
    .seed_txn_json()
}
fn normalize_txn_result(mut value: Value) -> Value {
    project_recursive(&mut value, &NORMALIZE);
    if let Value::Array(rows) = &mut value {
        for row in rows {
            if let Value::Object(obj) = row {
                for key in ["id", "scheduleId", "workflowId"] {
                    if obj.contains_key(key) {
                        obj.insert(key.to_string(), Value::String("<id>".into()));
                    }
                }
            }
        }
    }
    value
}
fn materialize_seed_id(mut value: Value, seed_id: &str, schedule_id: &str) -> Value {
    match &mut value {
        Value::String(s) if s == "seed-id" => *s = seed_id.to_string(),
        Value::String(s) if s == "schedule-id" => *s = schedule_id.to_string(),
        Value::Array(items) => {
            for item in items {
                *item = materialize_seed_id(item.take(), seed_id, schedule_id);
            }
        }
        Value::Object(obj) => {
            for item in obj.values_mut() {
                *item = materialize_seed_id(item.take(), seed_id, schedule_id);
            }
        }
        _ => {}
    }
    value
}

async fn run_txn_case(state: &rtdb_server::AppState, case: &TxnCase) -> Result<(), String> {
    let db_name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &db_name)
        .await
        .map_err(|e| format!("create db: {e:?}"))?;
    let _guard = wrap_test_db(db_name.clone());
    let schema_json = txn_schema_json(case);
    let schema: ServerSchemaDef =
        serde_json::from_value(schema_json.clone()).map_err(|e| e.to_string())?;
    ddl::push_schema(&state.pool, &db_name, schema.clone())
        .await
        .map_err(|e| format!("schema: {e:?}"))?;
    let seed: ServerTransaction =
        serde_json::from_value(txn_seed_json(case)).map_err(|e| e.to_string())?;
    let server_seed = state
        .realtime
        .committers
        .mutate(&db_name, None, seed, PrincipalCtx::bypass())
        .await
        .map_err(|e| format!("seed: {e:?}"))?;
    let server_id = server_seed
        .results
        .first()
        .and_then(|v| v.get("id"))
        .and_then(Value::as_str)
        .ok_or("seed did not return id")?;
    let mut client = InMemoryRtDbClient::new(
        InMemoryRtDbClientOptions::default()
            .now(|| CLOCK.fetch_add(1, AtomicOrdering::SeqCst))
            .random(|| 0.0),
    );
    let client_schema: ClientSchemaDef =
        serde_json::from_value(schema_json.clone()).map_err(|e| e.to_string())?;
    client
        .push_schema(&client_schema)
        .map_err(|e| format!("client schema: {e:?}"))?;
    let client_seed: ClientTransaction =
        serde_json::from_value(txn_seed_json(case)).map_err(|e| e.to_string())?;
    let client_seed_result = client
        .mutate(&client_seed, None)
        .await
        .map_err(|e| format!("client seed: {e:?}"))?;
    let engine_id = serde_json::to_value(&client_seed_result)
        .ok()
        .and_then(|v| {
            v.as_array().and_then(|a| {
                a.first()
                    .and_then(|v| v.get("id").and_then(Value::as_str).map(str::to_owned))
            })
        })
        .ok_or("engine seed did not return id")?;
    let schedule_json = serde_json::json!({"steps":[{"op":"schedule","when":{"type":"afterMs","ms":60000},"txn":{"steps":[]}}]});
    let schedule_server: ServerTransaction =
        serde_json::from_value(schedule_json.clone()).map_err(|e| e.to_string())?;
    let schedule_server = state
        .realtime
        .committers
        .mutate(&db_name, None, schedule_server, PrincipalCtx::bypass())
        .await
        .map_err(|e| format!("schedule preamble: {e:?}"))?;
    let schedule_id = schedule_server
        .results
        .first()
        .and_then(|v| v.get("scheduleId"))
        .and_then(Value::as_str)
        .ok_or("schedule preamble missing id")?;
    let schedule_client: ClientTransaction =
        serde_json::from_value(schedule_json).map_err(|e| e.to_string())?;
    let schedule_result = client
        .mutate(&schedule_client, None)
        .await
        .map_err(|e| format!("engine schedule preamble: {e:?}"))?;
    let engine_schedule_id = serde_json::to_value(schedule_result)
        .ok()
        .and_then(|v| {
            v.as_array().and_then(|a| {
                a.first().and_then(|v| {
                    v.get("scheduleId")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
            })
        })
        .ok_or("engine schedule preamble missing id")?;
    let server_txn_json = materialize_seed_id(
        serde_json::json!({"steps": case.steps}),
        server_id,
        schedule_id,
    );
    let engine_txn_json = materialize_seed_id(
        serde_json::json!({"steps": case.steps}),
        &engine_id,
        &engine_schedule_id,
    );
    let server_txn: ServerTransaction =
        serde_json::from_value(server_txn_json).map_err(|e| e.to_string())?;
    let server = state
        .realtime
        .committers
        .mutate(&db_name, None, server_txn, PrincipalCtx::bypass())
        .await;
    let client_txn: ClientTransaction =
        serde_json::from_value(engine_txn_json).map_err(|e| e.to_string())?;
    let engine = client.mutate(&client_txn, None).await;
    match (server, engine) {
        (Ok(s), Ok(e)) => {
            let sv = normalize_txn_result(Value::Array(s.results));
            let ev = normalize_txn_result(serde_json::to_value(e).map_err(|e| e.to_string())?);
            if sv != ev {
                return Err(format!("txn result mismatch: server={sv} engine={ev}"));
            }
        }
        (Err(s), Err(e)) if format!("{:?}", s.code) == format!("{:?}", e.code) => {}
        (s, e) => return Err(format!("txn error mismatch: server={s:?} engine={e:?}")),
    }
    for table in &case.tables {
        let q: ServerQuery = serde_json::from_value(serde_json::json!({"table": table.name}))
            .map_err(|e| e.to_string())?;
        let rows = execute_query(
            &state.pool,
            &db_name,
            &schema,
            &q,
            &PrincipalCtx::bypass(),
            false,
        )
        .await
        .map_err(|e| format!("collect: {e:?}"))?;
        let mut sv = serde_json::to_value(rows).map_err(|e| e.to_string())?;
        let mut ev = Value::Array(client.collect_all(&table.name));
        project_recursive(&mut sv, &NORMALIZE);
        project_recursive(&mut ev, &NORMALIZE);
        let empty = Vec::new();
        if !multiset_equal(
            sv.as_array().unwrap_or(&empty),
            ev.as_array().unwrap_or(&empty),
        ) {
            return Err(format!("post-state mismatch: server={sv} engine={ev}"));
        }
    }
    Ok(())
}

async fn run_overflow_case(state: &rtdb_server::AppState, case: &TxnCase) -> Result<(), String> {
    let db_name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &db_name)
        .await
        .map_err(|e| format!("create db: {e:?}"))?;
    let _guard = wrap_test_db(db_name.clone());
    let schema_json = txn_schema_json(case);
    let schema: ServerSchemaDef =
        serde_json::from_value(schema_json.clone()).map_err(|e| e.to_string())?;
    ddl::push_schema(&state.pool, &db_name, schema)
        .await
        .map_err(|e| format!("schema: {e:?}"))?;
    let txn_json = serde_json::json!({"steps": case.steps});
    let server_txn: ServerTransaction =
        serde_json::from_value(txn_json.clone()).map_err(|e| e.to_string())?;
    let server = state
        .realtime
        .committers
        .mutate(&db_name, None, server_txn, PrincipalCtx::bypass())
        .await;
    let mut client = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    let client_schema: ClientSchemaDef =
        serde_json::from_value(schema_json).map_err(|e| e.to_string())?;
    client
        .push_schema(&client_schema)
        .map_err(|e| format!("client schema: {e:?}"))?;
    let client_txn: ClientTransaction =
        serde_json::from_value(txn_json).map_err(|e| e.to_string())?;
    let engine = client.mutate(&client_txn, None).await;
    match (server, engine) {
        (Err(s), Err(e))
            if serde_json::to_value(s.code).ok() == serde_json::to_value(e.code).ok()
                && serde_json::to_value(s.code).ok()
                    == Some(Value::String("BAD_REQUEST".into())) =>
        {
            Ok(())
        }
        (s, e) => Err(format!("overflow mismatch: server={s:?} engine={e:?}")),
    }
}

#[test]
fn txn_dsl_server_vs_in_memory_parity() {
    let rt = tokio::runtime::Runtime::new().expect("build tokio runtime");
    let state = rt.block_on(async { test_state().await });

    let mut runner = TestRunner::new(Config {
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "proptest-regressions/proptest_parity_txn.txt",
        ))),
        ..Config::with_cases(case_count())
    });
    let strategy = txn_case_strategy();
    let result = runner.run(&strategy, |case| {
        match rt.block_on(run_txn_case(&state, &case)) {
            Ok(()) => Ok(()),
            Err(error) => {
                let path = rt.block_on(export_txn_counterexample(
                    &state.pool,
                    &counterexample_dir(),
                    "txn-dsl-server-vs-in-memory-parity",
                    &case,
                ));
                eprintln!("counterexample envelope: {}", path.display());
                Err(TestCaseError::fail(error))
            }
        }
    });
    result.expect("txn DSL parity property failed (counterexample above)");
}

#[test]
fn txn_cap_overflow_is_bad_request_on_both() {
    let rt = tokio::runtime::Runtime::new().expect("build tokio runtime");
    let state = rt.block_on(async { test_state().await });

    let mut runner = TestRunner::new(Config {
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "proptest-regressions/proptest_parity_txn_overflow.txt",
        ))),
        ..Config::with_cases(case_count())
    });
    let strategy = overflow_txn_strategy();
    let result = runner.run(&strategy, |case| {
        rt.block_on(run_overflow_case(&state, &case))
            .map_err(TestCaseError::fail)
    });
    result.expect("txn cap-overflow property failed (counterexample above)");
}
// ============ ENH-032: Migration generator + oracle ============

#[derive(Clone, Debug)]
struct MigrateCase {
    directives: Vec<Value>,
    count_query: bool,
    query_kind: u8,
}

fn migrate_case_strategy() -> BoxedStrategy<MigrateCase> {
    // Unique 1–4 ops in canonical order so every subset is valid by construction.
    // Collect vs count is an independent bit — not derived from which ops fired.
    (
        collection::vec(any::<u8>(), 1..=4),
        any::<bool>(),
        any::<u8>(),
    )
        .prop_map(|(choices, count_query, query_kind)| {
            let mut seen = std::collections::BTreeSet::new();
            for c in choices {
                seen.insert(c % 9);
            }
            let mut directives = Vec::new();
            for c in seen {
                directives.push(match c {
                    0 => serde_json::json!({"op":"renameField","table":"docs","from":"nick","to":"alias"}),
                    1 => serde_json::json!({"op":"renameField","table":"docs","from":"owner","to":"account"}),
                    2 => serde_json::json!({"op":"renameField","table":"docs","from":"editors","to":"collaborators"}),
                    3 => serde_json::json!({"op":"renameField","table":"docs","from":"ticket","to":"number"}),
                    4 => serde_json::json!({"op":"renameField","table":"docs","from":"expires","to":"expiresAt"}),
                    5 => serde_json::json!({"op":"renameField","table":"docs","from":"score","to":"rating"}),
                    6 => serde_json::json!({"op":"changeType","table":"docs","field":"spare","to":{"type":"string"},"cast":"toString"}),
                    7 => serde_json::json!({"op":"dropIndex","table":"docs","name":"by_nick"}),
                    _ => serde_json::json!({"op":"dropField","table":"docs","field":"spare"}),
                });
            }
            MigrateCase {
                directives,
                count_query,
                query_kind: query_kind % 3,
            }
        })
        .boxed()
}

fn migration_schema_json() -> Value {
    serde_json::json!({"tables":{"docs":{"fields":{
        "owner":{"type":"string"},"editors":{"type":"array","element":{"type":"string"}},
        "ticket":{"type":"int64"},"expires":{"type":"number"},
        "nick":{"type":"string"},"score":{"type":"number"},"spare":{"type":"number"},"label":{"type":"string"}
    },"indexes":[{"name":"by_nick","fields":["nick"]},{"name":"by_ticket","fields":["ticket"]},{"name":"by_expires","fields":["expires"]}],
    "ownerField":"owner","collaboratorsField":"editors","autoIncrementField":"ticket",
    "ttl":{"field":"expires"},"authorize":{"op":"eq","field":"owner","value":{"$user":true}},
    "defaults":{"score":0},"computed":{"label":{"op":"concat","parts":[{"op":"literal","value":"n:"},{"op":"field","field":"nick"}]}},"softDelete":true}}})
}

fn additive_migration_schema_json() -> Value {
    let mut schema = migration_schema_json();
    schema["tables"]["docs"]["fields"]["added"] = serde_json::json!({"type":"string"});
    schema["tables"]["docs"]["defaults"]["added"] = serde_json::json!("new");
    schema["tables"]["docs"]["indexes"]
        .as_array_mut()
        .expect("migration indexes")
        .push(serde_json::json!({"name":"by_added","fields":["added"]}));
    schema
}

fn migrate_case_query_json(case: &MigrateCase) -> Value {
    let owner_field = if case.directives.iter().any(|d| d["from"] == "owner") {
        "account"
    } else {
        "owner"
    };
    let mut query = match case.query_kind {
        0 => {
            serde_json::json!({"table":"docs","filter":{"op":"eq","field":owner_field,"value":"u"}})
        }
        1 => serde_json::json!({"table":"docs","index":"by_added","eq":["new"]}),
        _ => serde_json::json!({"table":"docs"}),
    };
    if case.count_query {
        query["count"] = Value::Bool(true);
    }
    query
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum MigratePhase {
    Preview,
    Apply,
    Other,
}

impl MigratePhase {
    fn label(self) -> &'static str {
        match self {
            Self::Preview => "dry-run preview",
            Self::Apply => "apply",
            Self::Other => "setup/verification",
        }
    }
}

#[derive(Clone, Debug)]
struct MigrateCaseFailure {
    phase: MigratePhase,
    message: String,
}

impl MigrateCaseFailure {
    fn preview(message: impl Into<String>) -> Self {
        Self {
            phase: MigratePhase::Preview,
            message: message.into(),
        }
    }

    fn apply(message: impl Into<String>) -> Self {
        Self {
            phase: MigratePhase::Apply,
            message: message.into(),
        }
    }

    fn other(message: impl Into<String>) -> Self {
        Self {
            phase: MigratePhase::Other,
            message: message.into(),
        }
    }
}

impl From<String> for MigrateCaseFailure {
    fn from(message: String) -> Self {
        Self::other(message)
    }
}

impl From<&str> for MigrateCaseFailure {
    fn from(message: &str) -> Self {
        Self::other(message)
    }
}

async fn run_migration_case(
    state: &Arc<rtdb_server::AppState>,
    addr: std::net::SocketAddr,
    case: &MigrateCase,
) -> Result<(), MigrateCaseFailure> {
    let db_name = format!("t{}", uuid::Uuid::now_v7().simple());
    db::create_database(&state.pool, &db_name)
        .await
        .map_err(|e| format!("create: {e:?}"))?;
    let _guard = wrap_test_db(db_name.clone());
    let schema_json = migration_schema_json();
    let base_schema: ServerSchemaDef =
        serde_json::from_value(schema_json.clone()).map_err(|e| e.to_string())?;
    ddl::push_schema(&state.pool, &db_name, base_schema.clone())
        .await
        .map_err(|e| format!("schema: {e:?}"))?;
    let additive_json = additive_migration_schema_json();
    let additive: ServerSchemaDef =
        serde_json::from_value(additive_json.clone()).map_err(|e| e.to_string())?;
    ddl::push_schema(&state.pool, &db_name, additive.clone())
        .await
        .map_err(|e| format!("additive push: {e:?}"))?;
    let schema = additive;
    let seed = ServerTransaction { steps: vec![rtdb_server::txn::Step::Insert { table: "docs".into(), doc: serde_json::from_value(serde_json::json!({"owner":"u","editors":["v"],"nick":"Ada","score":4.0,"spare":1.0,"expires":4102444800000.0})).unwrap() }]};
    state
        .realtime
        .committers
        .mutate(&db_name, None, seed, PrincipalCtx::bypass())
        .await
        .map_err(|e| format!("seed: {e:?}"))?;
    let directives: Vec<rtdb_server::migrate::Directive> = case
        .directives
        .iter()
        .cloned()
        .map(|v| serde_json::from_value(v).map_err(|e| e.to_string()))
        .collect::<Result<_, _>>()?;
    let request_json = serde_json::json!({
        "directives": case.directives.clone(),
        "dryRun": true,
    });
    let preview_response =
        admin_post(addr, &format!("/admin/db/{db_name}/migrate"), request_json).await;
    if !preview_response.status().is_success() {
        return Err(MigrateCaseFailure::preview(format!(
            "dry-run HTTP status: {}",
            preview_response.status()
        )));
    }
    let preview: rtdb_server::migrate::MigrateResult = preview_response
        .json()
        .await
        .map_err(|e| format!("dry-run JSON: {e}"))?;
    if preview.applied {
        return Err(MigrateCaseFailure::preview("dry-run unexpectedly applied"));
    }
    let apply_response = admin_post(
        addr,
        &format!("/admin/db/{db_name}/migrate"),
        serde_json::json!({"directives": case.directives.clone(), "dryRun": false}),
    )
    .await;
    if !apply_response.status().is_success() {
        return Err(MigrateCaseFailure::apply(format!(
            "apply HTTP status: {}",
            apply_response.status()
        )));
    }
    let applied: rtdb_server::migrate::MigrateResult = apply_response
        .json()
        .await
        .map_err(|e| format!("apply JSON: {e}"))?;
    let mut client = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
    if !applied.applied {
        return Err(MigrateCaseFailure::apply(
            "server apply unexpectedly returned applied=false",
        ));
    }
    let base_client_schema: ClientSchemaDef =
        serde_json::from_value(schema_json.clone()).map_err(|e| e.to_string())?;
    client
        .push_schema(&base_client_schema)
        .map_err(|e| format!("client base schema: {e:?}"))?;
    let client_schema: ClientSchemaDef =
        serde_json::from_value(additive_json).map_err(|e| e.to_string())?;
    client
        .push_schema(&client_schema)
        .map_err(|e| format!("client additive schema: {e:?}"))?;
    let server_additive_json = serde_json::to_value(&schema).map_err(|e| e.to_string())?;
    let engine_additive_json =
        serde_json::to_value(client.to_schema_json().ok_or("client schema missing")?)
            .map_err(|e| e.to_string())?;
    if server_additive_json != engine_additive_json {
        return Err(MigrateCaseFailure::other(format!(
            "post-additive schema mismatch: {server_additive_json} != {engine_additive_json}"
        )));
    }
    let seed: ClientTransaction = serde_json::from_value(serde_json::json!({"steps":[{"op":"insert","table":"docs","doc":{"owner":"u","editors":["v"],"nick":"Ada","score":4.0,"spare":1.0,"expires":4102444800000.0}}]})).unwrap();
    client
        .mutate(&seed, None)
        .await
        .map_err(|e| format!("client seed: {e:?}"))?;
    let client_directives: Vec<par_rt_db_client::Directive> =
        serde_json::from_value(serde_json::to_value(&directives).unwrap())
            .map_err(|e| e.to_string())?;
    let dry = client
        .migrate_schema(&client_directives, true)
        .map_err(|e| format!("client dry-run: {e:?}"))?;
    let live = client
        .migrate_schema(&client_directives, false)
        .map_err(|e| format!("client apply: {e:?}"))?;
    if dry.applied || !live.applied {
        return Err(MigrateCaseFailure::other(
            "engine dry-run/apply flags mismatch",
        ));
    }
    let server_json = serde_json::to_value(&applied.schema).map_err(|e| e.to_string())?;
    let engine_json = serde_json::to_value(&live.schema).map_err(|e| e.to_string())?;
    if server_json != engine_json {
        return Err(MigrateCaseFailure::other(format!(
            "schema mismatch: {server_json} != {engine_json}"
        )));
    }
    for directive in &case.directives {
        if directive["op"] == "renameField" {
            let from = directive["from"].as_str().unwrap();
            if !server_json.is_object() || server_json.to_string().contains(&format!("\"{from}\""))
            {
                return Err(MigrateCaseFailure::other(format!(
                    "QA-002: renamed field '{from}' remains in schema JSON"
                )));
            }
        }
    }
    let query_json = migrate_case_query_json(case);
    let q: ServerQuery = serde_json::from_value(query_json.clone()).unwrap();
    let rows = execute_query(
        &state.pool,
        &db_name,
        &applied.schema,
        &q,
        &PrincipalCtx::bypass(),
        false,
    )
    .await
    .map_err(|e| format!("query: {e:?}"))?;
    let mut s = serde_json::to_value(rows).unwrap();
    let cq: ClientQuery = serde_json::from_value(query_json).unwrap();
    let mut e = client
        .run_query(&cq)
        .map_err(|e| format!("engine query: {e:?}"))?;
    project_recursive(&mut s, &NORMALIZE);
    project_recursive(&mut e, &NORMALIZE);
    if case.count_query {
        if s != e {
            return Err(MigrateCaseFailure::other(format!(
                "post-apply generated count mismatch: {s} != {e}"
            )));
        }
        return Ok(());
    }
    if !multiset_equal(
        s.as_array().unwrap_or(&vec![]),
        e.as_array().unwrap_or(&vec![]),
    ) {
        return Err(MigrateCaseFailure::other(format!(
            "post-apply query mismatch: {s} != {e}"
        )));
    }
    Ok(())
}

#[test]
fn migrate_dsl_server_vs_in_memory_parity() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let state = rt.block_on(async { test_state().await });
    let addr = rt.block_on(spawn_app(state.clone()));
    let mut runner = TestRunner::new(Config {
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "proptest-regressions/proptest_parity_migrate.txt",
        ))),
        ..Config::with_cases(case_count())
    });
    runner
        .run(&migrate_case_strategy(), |case| {
            match rt.block_on(run_migration_case(&state, addr, &case)) {
                Ok(()) => Ok(()),
                Err(failure) => {
                    let path = rt.block_on(export_migrate_counterexample(
                        &state.pool,
                        &counterexample_dir(),
                        "migrate-dsl-server-vs-in-memory-parity",
                        &case,
                        &failure,
                    ));
                    eprintln!("counterexample envelope: {}", path.display());
                    Err(TestCaseError::fail(failure.message))
                }
            }
        })
        .expect("migration parity");
}
fn destructive_schema_strategy() -> BoxedStrategy<Value> {
    prop_oneof![
        Just({
            let mut v = migration_schema_json();
            v["tables"]["docs"]["fields"]
                .as_object_mut()
                .unwrap()
                .remove("score");
            v["tables"]["docs"]["defaults"]
                .as_object_mut()
                .unwrap()
                .remove("score");
            v
        }),
        Just({
            let mut v = migration_schema_json();
            v["tables"]["docs"]["fields"]["score"] = serde_json::json!({"type":"string"});
            v["tables"]["docs"]["defaults"]
                .as_object_mut()
                .unwrap()
                .remove("score");
            v
        }),
        Just({
            let mut v = migration_schema_json();
            v["tables"]["docs"]["indexes"]
                .as_array_mut()
                .unwrap()
                .retain(|ix| ix["name"] != "by_nick");
            v
        }),
    ]
    .boxed()
}

#[test]
fn migrate_destructive_change_detector_rejects_on_both() {
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let state = rt.block_on(async { test_state().await });
    let mut runner = TestRunner::new(Config {
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "proptest-regressions/proptest_parity_migrate_destructive.txt",
        ))),
        ..Config::with_cases(case_count())
    });
    runner
        .run(&destructive_schema_strategy(), |new_json| {
            let state = &state;
            rt.block_on(async {
                let db_name = format!("t{}", uuid::Uuid::now_v7().simple());
                db::create_database(&state.pool, &db_name)
                    .await
                    .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
                let _guard = wrap_test_db(db_name.clone());
                let old: ServerSchemaDef = serde_json::from_value(migration_schema_json())
                    .map_err(|e| TestCaseError::fail(e.to_string()))?;
                ddl::push_schema(&state.pool, &db_name, old.clone())
                    .await
                    .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
                let new: ServerSchemaDef = serde_json::from_value(new_json.clone())
                    .map_err(|e| TestCaseError::fail(e.to_string()))?;
                if ddl::push_schema(&state.pool, &db_name, new.clone())
                    .await
                    .is_ok()
                {
                    return Err(TestCaseError::fail("server accepted destructive schema"));
                }
                let mut client = InMemoryRtDbClient::new(InMemoryRtDbClientOptions::default());
                let old_client: ClientSchemaDef =
                    serde_json::from_value(serde_json::to_value(old).unwrap()).unwrap();
                client
                    .push_schema(&old_client)
                    .map_err(|e| TestCaseError::fail(format!("{e:?}")))?;
                let new_client: ClientSchemaDef =
                    serde_json::from_value(serde_json::to_value(new).unwrap()).unwrap();
                if client.push_schema(&new_client).is_ok() {
                    return Err(TestCaseError::fail("engine accepted destructive schema"));
                }
                Ok(())
            })
        })
        .expect("destructive detector parity");
}

fn is_preamble_schedule_cancel(step: &Value) -> bool {
    step["op"].as_str() == Some("cancelSchedule") && step["id"].as_str() == Some("schedule-id")
}
// ============ ENH-032: semantics-corpus counterexample export ============

fn rewrite_counterexample_refs(mut value: Value) -> Value {
    match &mut value {
        Value::String(s) if s == "seed-id" => return serde_json::json!({"$idRef":"seed"}),
        Value::Array(items) => {
            for item in items {
                *item = rewrite_counterexample_refs(item.take());
            }
        }
        Value::Object(obj) => {
            for item in obj.values_mut() {
                *item = rewrite_counterexample_refs(item.take());
            }
        }
        _ => {}
    }
    value
}

fn txn_case_to_semantics_envelope(case: &TxnCase, name: &str) -> Value {
    let mut seed = Vec::new();
    for (table_index, table) in case.tables.iter().enumerate() {
        for (doc_index, doc) in table.docs.iter().enumerate() {
            let mut entry = serde_json::json!({"table": table.name, "doc": doc});
            if table_index == 0 && doc_index == 0 {
                entry["$id"] = Value::String("seed".into());
            }
            seed.push(entry);
        }
    }
    let omitted = case
        .steps
        .iter()
        .filter(|step| is_preamble_schedule_cancel(step))
        .count();
    let steps = case
        .steps
        .iter()
        .filter(|step| !is_preamble_schedule_cancel(step))
        .cloned()
        .collect::<Vec<_>>();
    let mut comment = "Generated shrunk transaction counterexample; expect and post-state query are captured from the server at export time. seed-id operands use $idRef against the first seeded document; missing ids remain literal probes.".to_string();
    if omitted != 0 {
        comment.push_str(&format!(
            " Omitted {omitted} preamble cancelSchedule step(s) with id schedule-id: the property harness mints their schedule in a preamble, while corpus $idRef labels come only from seeded documents; literal missing-schedule-id cancelSchedule probes are retained."
        ));
    }
    serde_json::json!({
        "name": name,
        "$comment": comment,
        "schema": txn_schema_json(case),
        "seed": seed,
        "op": {"txn": {"steps": rewrite_counterexample_refs(Value::Array(steps))}},
        "expect": Value::Null,
        "normalize": ["_id","_creationTime","_version","id","scheduleId"],
        "then": {"query": {"table": case.tables[0].name}, "unordered": true, "expect": Value::Null}
    })
}

fn migrate_case_to_semantics_envelope(
    case: &MigrateCase,
    name: &str,
    failure: &MigrateCaseFailure,
) -> Value {
    let dry_run = failure.phase == MigratePhase::Preview;
    let message: String = failure.message.chars().take(240).collect();
    let mut comment = format!(
        "Generated shrunk migration counterexample; property failed in {} phase: {}. expect contains server-derived results for this dryRun={} operation.",
        failure.phase.label(),
        message,
        dry_run
    );
    if dry_run {
        comment.push_str(
            " then omitted: dry-run rolls back, so post-migration queries are not expressible against the rolled-back database.",
        );
    }
    let mut envelope = serde_json::json!({
        "name": name,
        "$comment": comment,
        "schema": additive_migration_schema_json(),
        "seed": [{"table":"docs","doc":{"owner":"u","editors":["v"],"nick":"Ada","score":4.0,"spare":1.0,"expires":4102444800000.0}}],
        "op": {"migrate":{"directives":case.directives,"dryRun":dry_run}},
        "expect": Value::Null,
        "normalize": ["_id","_creationTime","_version","id"],
    });
    if !dry_run {
        envelope["then"] = serde_json::json!({
            "query": migrate_case_query_json(case),
            "unordered": !case.count_query,
            "expect": Value::Null
        });
    }
    envelope
}

fn write_counterexample_to(
    dir: &std::path::Path,
    name: &str,
    envelope: &Value,
) -> std::path::PathBuf {
    std::fs::create_dir_all(dir).expect("create counterexample directory");
    let path = dir.join(format!("{name}.json"));
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(envelope).expect("serialize counterexample"),
    )
    .expect("write counterexample");
    path
}

fn counterexample_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../target/proptest-counterexamples")
}

async fn export_txn_counterexample(
    pool: &sqlx::PgPool,
    dir: &std::path::Path,
    name: &str,
    case: &TxnCase,
) -> std::path::PathBuf {
    let mut envelope = txn_case_to_semantics_envelope(case, name);
    let captured = crate::semantics_corpus_test::capture_case_expect(pool, name, &envelope).await;
    envelope["expect"] = captured.op;
    if let Some(expect) = captured.then {
        envelope["then"]["expect"] = expect;
    } else {
        envelope.as_object_mut().unwrap().remove("then");
    }
    write_counterexample_to(dir, name, &envelope)
}

async fn export_migrate_counterexample(
    pool: &sqlx::PgPool,
    dir: &std::path::Path,
    name: &str,
    case: &MigrateCase,
    failure: &MigrateCaseFailure,
) -> std::path::PathBuf {
    let mut envelope = migrate_case_to_semantics_envelope(case, name, failure);
    let captured = crate::semantics_corpus_test::capture_case_expect(pool, name, &envelope).await;
    envelope["expect"] = captured.op;
    if let Some(expect) = captured.then {
        envelope["then"]["expect"] = expect;
    } else {
        envelope.as_object_mut().unwrap().remove("then");
    }
    write_counterexample_to(dir, name, &envelope)
}

#[test]
fn txn_counterexample_envelope_replays_through_corpus_runner() {
    let case = TxnCase {
        tables: vec![TableCase {
            name: "t0".into(),
            fields: vec![("f_a".into(), Kind::Scalar(Scalar::Str))],
            indexes: vec![("by_f_a".into(), vec![0])],
            docs: vec![
                serde_json::json!({"f_a":"seed-key"})
                    .as_object()
                    .unwrap()
                    .clone(),
                serde_json::json!({"f_a":"other"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ],
        }],
        steps: vec![
            serde_json::json!({"op":"patch","table":"t0","id":"seed-id","fields":{"f_a":"patched"}}),
            serde_json::json!({"op":"expectVersion","table":"t0","id":"seed-id","version":2}),
            serde_json::json!({"op":"upsert","table":"t0","index":"by_f_a","eq":["other"],"insert":{"f_a":"other"},"patch":{"f_a":"upserted"}}),
            serde_json::json!({"op":"patchByQuery","table":"t0","filter":{"op":"eq","field":"f_a","value":"patched"},"patch":{"f_a":"z"},"limit":1}),
        ],
    };
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let state = rt.block_on(async { test_state().await });
    let path = rt.block_on(export_txn_counterexample(
        &state.pool,
        &counterexample_dir(),
        "txn-counterexample-replay-check",
        &case,
    ));
    let raw = std::fs::read_to_string(&path).expect("read exported envelope");
    let envelope: Value = serde_json::from_str(&raw).expect("parse exported envelope");
    assert_eq!(envelope["name"], "txn-counterexample-replay-check");
    assert_eq!(envelope["seed"][0]["$id"], "seed");
    assert_eq!(
        envelope["op"]["txn"]["steps"][0]["id"],
        serde_json::json!({"$idRef":"seed"})
    );
    assert!(envelope["expect"].is_array());
    assert!(envelope["then"]["expect"].is_array());
    rt.block_on(crate::semantics_corpus_test::run_case(
        &state.pool,
        "txn-counterexample-replay-check",
        &envelope,
    ));
}

#[test]
fn txn_counterexample_retains_missing_schedule_cancel() {
    let case = TxnCase {
        tables: vec![TableCase {
            name: "t0".into(),
            fields: vec![("f_a".into(), Kind::Scalar(Scalar::Str))],
            indexes: vec![],
            docs: vec![
                serde_json::json!({"f_a":"seed"})
                    .as_object()
                    .unwrap()
                    .clone(),
            ],
        }],
        steps: vec![
            serde_json::json!({"op":"cancelSchedule","id":"missing-schedule-id"}),
            serde_json::json!({"op":"cancelSchedule","id":"schedule-id"}),
        ],
    };
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let state = rt.block_on(async { test_state().await });
    let path = rt.block_on(export_txn_counterexample(
        &state.pool,
        &counterexample_dir(),
        "txn-counterexample-schedule-labels",
        &case,
    ));
    let raw = std::fs::read_to_string(&path).expect("read exported envelope");
    let envelope: Value = serde_json::from_str(&raw).expect("parse exported envelope");
    let steps = envelope["op"]["txn"]["steps"]
        .as_array()
        .expect("txn steps");
    assert_eq!(steps.len(), 1);
    assert_eq!(steps[0]["id"], "missing-schedule-id");
    assert!(envelope["expect"].is_array());
    assert!(envelope["then"]["expect"].is_array());
    rt.block_on(crate::semantics_corpus_test::run_case(
        &state.pool,
        "txn-counterexample-schedule-labels",
        &envelope,
    ));
}

#[test]
fn migrate_counterexample_envelope_replays_through_corpus_runner() {
    let case = MigrateCase {
        directives: vec![
            serde_json::json!({"op":"renameField","table":"docs","from":"nick","to":"alias"}),
            serde_json::json!({"op":"changeType","table":"docs","field":"spare","to":{"type":"string"},"cast":"toString"}),
        ],
        count_query: false,
        query_kind: 0,
    };
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let state = rt.block_on(async { test_state().await });
    let failure = MigrateCaseFailure::apply("roundtrip apply phase");
    let path = rt.block_on(export_migrate_counterexample(
        &state.pool,
        &counterexample_dir(),
        "migrate-counterexample-replay-check",
        &case,
        &failure,
    ));
    let raw = std::fs::read_to_string(&path).expect("read exported envelope");
    let envelope: Value = serde_json::from_str(&raw).expect("parse exported envelope");
    assert_eq!(envelope["name"], "migrate-counterexample-replay-check");
    assert_eq!(envelope["expect"]["applied"], true);
    let derived = envelope["expect"]["schema"].to_string();
    assert!(derived.contains("\"alias\"") && !derived.contains("\"nick\""));
    assert_eq!(
        envelope["expect"]["directives"].as_array().unwrap().len(),
        2
    );
    assert!(envelope["then"]["expect"].is_array());
    rt.block_on(crate::semantics_corpus_test::run_case(
        &state.pool,
        "migrate-counterexample-replay-check",
        &envelope,
    ));
    let preview_failure = MigrateCaseFailure::preview("roundtrip preview phase");
    let preview_path = rt.block_on(export_migrate_counterexample(
        &state.pool,
        &counterexample_dir(),
        "migrate-counterexample-preview-check",
        &case,
        &preview_failure,
    ));
    let preview_raw = std::fs::read_to_string(&preview_path).expect("read preview envelope");
    let preview_envelope: Value =
        serde_json::from_str(&preview_raw).expect("parse preview envelope");
    assert_eq!(preview_envelope["op"]["migrate"]["dryRun"], true);
    assert_eq!(preview_envelope["expect"]["applied"], false);
    rt.block_on(crate::semantics_corpus_test::run_case(
        &state.pool,
        "migrate-counterexample-preview-check",
        &preview_envelope,
    ));
}

#[test]
fn migrate_counterexample_preview_rollback_replays() {
    let case = MigrateCase {
        directives: vec![
            serde_json::json!({"op":"renameField","table":"docs","from":"owner","to":"account"}),
        ],
        count_query: false,
        query_kind: 1,
    };
    let rt = tokio::runtime::Runtime::new().expect("runtime");
    let state = rt.block_on(async { test_state().await });
    let failure = MigrateCaseFailure::preview("preview rollback roundtrip");
    let path = rt.block_on(export_migrate_counterexample(
        &state.pool,
        &counterexample_dir(),
        "migrate-counterexample-preview-rollback",
        &case,
        &failure,
    ));
    let raw = std::fs::read_to_string(&path).expect("read exported envelope");
    let envelope: Value = serde_json::from_str(&raw).expect("parse exported envelope");
    assert_eq!(envelope["op"]["migrate"]["dryRun"], true);
    assert_eq!(envelope["expect"]["applied"], false);
    assert!(envelope.get("then").is_none());
    rt.block_on(crate::semantics_corpus_test::run_case(
        &state.pool,
        "migrate-counterexample-preview-rollback",
        &envelope,
    ));
}
