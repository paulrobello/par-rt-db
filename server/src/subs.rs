use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use sqlx::PgPool;
use tokio::sync::Mutex;
use tokio::sync::mpsc::UnboundedSender;

use crate::metrics::{Metrics, SkipClass};
use crate::protocol::ServerMessage;
use crate::query::{Order, Query, QueryResult, canonical, execute_query};
use crate::schema::{FieldType, IndexDef, SchemaDef, TableDef};
use crate::txn::{DocValues, EqBind, WriteSet, eq_bind_for, eq_binds};

pub type ConnId = u64;

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

/// Allocates a fresh, process-unique connection id.
pub fn next_conn_id() -> ConnId {
    NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed)
}

/// What a subscription's result depends on, used to skip needless re-runs.
/// Derived once from the (immutable) `Query` + the table def at registration.
#[derive(Debug, Clone)]
enum ReadSet {
    /// A `get(id)` point read: the result is exactly this one document, so a
    /// write to any other document cannot change it.
    Point { id: String },
    /// A `count`, `collect`, or `unique` query filtered on a btree index's
    /// eq-prefix (and an optional range bound on the next index field). A write
    /// to a document provably outside the window cannot change the result, so
    /// `fan_out` can skip the re-run. See `IndexedRead` for the soundness model.
    Indexed(IndexedRead),
    /// A `take(N)` / `first` / `paginate` query: an ORDERED, truncated window.
    /// Window membership alone can't decide these (a doc can enter or leave the
    /// truncated window without its eq-prefix changing), so `OrderedRead` also
    /// tracks the sort key of the last result's final document — the boundary —
    /// and skips only writes that provably rank beyond it.
    Ordered(OrderedRead),
    /// Every other shape (distinct / aggregate / search / vector / hybrid):
    /// the result depends on the VALUES of the matching set or on a ranking
    /// function, neither of which membership or boundary reasoning can bound,
    /// so re-run on any write to the table (today's behavior).
    Table,
}

/// The eq-prefix (+ optional range bound) predicate shared by `IndexedRead`
/// and `OrderedRead`: which documents a query's `index`/`eq`/range bounds can
/// match, ignoring ordering, truncation, `filter`, and per-row ownership.
///
/// `eq` carries the index field name, its declared `FieldType` (so a written
/// doc's field value is typed identically to the DB's stored column), and the
/// typed bind the query pins it to. `range` is an optional inequality bound
/// (`gt`/`gte`/`lt`/`lte`) on the index field immediately after the eq-prefix
/// (`index.fields[eq.len()]`); present only when the eq-prefix is a strict
/// prefix of the index (a full-arity eq has no remaining field to range on).
///
/// An empty `eq` with no `range` matches everything — the whole table. That is
/// useless for `Indexed` (no skip to be had, so `try_indexed` rejects it) but
/// perfectly usable for `Ordered`, whose skipping comes from the boundary.
#[derive(Debug, Clone)]
struct Window {
    eq: Vec<(String, FieldType, EqBind)>,
    range: Option<RangeBound>,
}

/// A `count` / `collect` / `unique` subscription's window. `fan_out` re-runs
/// only when a written document may have crossed the window boundary.
///
/// `content_bearing` is true for `collect` and `unique` (return doc bodies — a
/// member's content change matters) and false for `count` (pure membership).
#[derive(Debug, Clone)]
struct IndexedRead {
    window: Window,
    content_bearing: bool,
}

#[derive(Debug, Clone)]
struct RangeBound {
    field: String,
    field_type: FieldType,
    /// (value, inclusive): a `gte` bound is `(v, true)`, a `gt` bound is `(v, false)`.
    lower: Option<(EqBind, bool)>,
    /// (value, inclusive): a `lte` bound is `(v, true)`, an `lt` bound is `(v, false)`.
    upper: Option<(EqBind, bool)>,
}

impl Window {
    /// Whether `doc` falls inside this subscription's eq-prefix + range window.
    /// Pure and total: never panics. Returns `false` (outside) on a typing
    /// failure, missing field, or comparison doubt.
    ///
    /// NOTE: `false` here means fan_out will SKIP this doc (`indexed_affects`
    /// returns false), so a wrong `false` is UNDER-approximation — a missed
    /// push. The comparison path therefore requires every `EqBind` variant to
    /// have a real `cmp_binds` arm (see that function's doc); the eq-prefix
    /// path's typing fallbacks are themselves under-approximate and only safe
    /// insofar as they fire on truly untellable cases (e.g. a doc whose field
    /// has the wrong JSON type — a schema violation that should already have
    /// been rejected at write time).
    fn contains(&self, doc: &serde_json::Map<String, serde_json::Value>) -> bool {
        // Eq prefix: AND of equalities. One miss ⇒ outside.
        for (field, ty, want) in &self.eq {
            let doc_val = doc.get(field).unwrap_or(&serde_json::Value::Null);
            let Ok(have) = eq_bind_for(ty, doc_val) else {
                return false;
            };
            if &have != want {
                return false;
            }
        }
        // Optional range bound on the field after the eq-prefix.
        if let Some(r) = &self.range {
            let doc_val = doc.get(&r.field).unwrap_or(&serde_json::Value::Null);
            let Ok(have) = eq_bind_for(&r.field_type, doc_val) else {
                return false;
            };
            if let Some((bound, inclusive)) = &r.lower
                && !satisfies_lower(&have, bound, *inclusive)
            {
                return false;
            }
            if let Some((bound, inclusive)) = &r.upper
                && !satisfies_upper(&have, bound, *inclusive)
            {
                return false;
            }
        }
        true
    }
}

/// Three-way comparison of two typed binds.
///
/// **Every `EqBind` variant MUST be comparable here**, and the structure of
/// this function is what enforces it: the OUTER match is exhaustive over
/// `EqBind`, so adding a variant fails to compile here rather than silently
/// falling into a catch-all. Do not collapse it back into a
/// `match (a, b) { .., _ => None }` — that shape compiles fine with a new
/// variant and is UNSOUND (see below). Only the inner mismatch arms are
/// catch-alls, and a mismatch cannot occur in practice: both binds derive from
/// the same `FieldType`.
///
/// Why a missing arm is a correctness bug and not a conservative fallback:
/// `None` propagates through `satisfies_lower`/`satisfies_upper` as `false`,
/// which makes `Window::contains` return `false`, which makes `indexed_affects`
/// return `false` for created/updated docs, which makes `fan_out` SKIP the
/// re-run. That is UNDER-approximation — a missed push — and violates the
/// committer's load-bearing "never under-approximate" invariant. (In the
/// `Ordered` path a `None` is handled the other way, resolving to "re-run", so
/// only the window path is exposed; keeping every variant comparable removes
/// the hazard from both.)
///
/// `Num` uses `partial_cmp` so a (theoretically impossible from JSON) NaN is
/// treated as unorderable rather than panicking.
///
/// COLLATION: the `Text` arm compares byte-wise (Rust `String::cmp`), which
/// matches Postgres only under a `C` collation — the collation par-rt-db's
/// clusters initialize with and prod was migrated to (`deploy/README.md`,
/// "Collation"). Under a linguistic collation (`en_US.utf8`) Postgres would
/// order some text differently, and a range bound or sort boundary on a text
/// field could then judge an inside-the-window document to be outside — an
/// under-approximation. Only ORDER comparisons are affected; equality is
/// byte-wise under any deterministic collation, so eq-prefix matching is safe
/// either way.
fn cmp_binds(a: &EqBind, b: &EqBind) -> Option<std::cmp::Ordering> {
    // Exhaustive on `a` by construction — see the doc comment above.
    match a {
        EqBind::Text(x) => match b {
            EqBind::Text(y) => Some(x.cmp(y)),
            _ => None,
        },
        EqBind::Num(x) => match b {
            EqBind::Num(y) => x.partial_cmp(y),
            _ => None,
        },
        EqBind::Bool(x) => match b {
            EqBind::Bool(y) => Some(x.cmp(y)),
            _ => None,
        },
        EqBind::I64(x) => match b {
            EqBind::I64(y) => Some(x.cmp(y)),
            _ => None,
        },
    }
}

/// Whether `have >= bound` (inclusive) or `have > bound` (exclusive), per
/// `inclusive`. Comparison doubt ⇒ `false` (judged outside the bound — every `EqBind` variant must be orderable in `cmp_binds`, else a missed arm would under-approximate here).
fn satisfies_lower(have: &EqBind, bound: &EqBind, inclusive: bool) -> bool {
    match cmp_binds(have, bound) {
        Some(std::cmp::Ordering::Greater) => true,
        Some(std::cmp::Ordering::Equal) => inclusive,
        Some(std::cmp::Ordering::Less) | None => false,
    }
}

/// Whether `have <= bound` (inclusive) or `have < bound` (exclusive), per
/// `inclusive`. Comparison doubt ⇒ `false` (judged outside the bound — every `EqBind` variant must be orderable in `cmp_binds`, else a missed arm would under-approximate here).
fn satisfies_upper(have: &EqBind, bound: &EqBind, inclusive: bool) -> bool {
    match cmp_binds(have, bound) {
        Some(std::cmp::Ordering::Less) => true,
        Some(std::cmp::Ordering::Equal) => inclusive,
        Some(std::cmp::Ordering::Greater) | None => false,
    }
}

/// Whether a written doc's before/after affects an `Indexed` subscription, per
/// the spec's per-doc decision:
/// - deleted (after None) ⇒ `true` (values gone, must re-run).
/// - created (before None, after Some) ⇒ `in_window(after)` (entered iff now in).
/// - updated (both Some) ⇒ `content_bearing ? (in_window(before) || in_window(after))`
///   `(collect/unique — a member's body change matters)` else
///   `(in_window(before) != in_window(after))` `(count — only membership flips matter)`.
///
/// Owner filtering and `filter` are intentionally NOT consulted here: they can
/// only narrow the real result, so ignoring them over-approximates (re-runs a
/// matching-but-filtered-out or not-visible doc), never under-approximates.
fn indexed_affects(indexed: &IndexedRead, values: &DocValues) -> bool {
    match (&values.before, &values.after) {
        // after None ⇒ deleted this txn. Always re-run.
        (_, None) => true,
        // Created this txn: entered iff the new state is in window.
        (None, Some(after)) => indexed.window.contains(after),
        // Updated: both states known.
        (Some(before), Some(after)) => {
            if indexed.content_bearing {
                // collect / unique: a body change to a current/past member matters.
                indexed.window.contains(before) || indexed.window.contains(after)
            } else {
                // count: only a membership flip (in↔out) matters.
                indexed.window.contains(before) != indexed.window.contains(after)
            }
        }
    }
}

// =====================================================================
// Ordered top-N reads (v3): take(N) / first / paginate
// =====================================================================

/// A `take(N)` / `first` / `paginate` subscription's ordered window: the
/// eq/range `window`, the sort order the server applies inside it, and the
/// sort key of the last computed result's FINAL document — the boundary.
///
/// The result is the first N documents of `window` in sort order (plus
/// `filter` / per-row ownership, which only narrow it further). So a written
/// document can change the result only if it is inside `window` AND ranks at
/// or before the boundary, in either its before- or after-state:
///
/// - a document ranking beyond the boundary cannot be in the top N, because
///   the N documents that are all rank strictly before it;
/// - it cannot displace one either — displacement requires a member to leave,
///   and that member's own write is itself at-or-before the boundary, so the
///   re-run is already triggered by that write.
///
/// `boundary: None` means the last result was NOT full (fewer than N docs, or
/// a page with no next page), so the window is effectively unbounded: any
/// in-window write can change the result, and the decision degenerates to
/// plain window membership (exactly what `collect` does).
///
/// The boundary is refreshed from every successful re-run in `fan_out`, so it
/// always describes the most recently COMPUTED result. A skip means the result
/// cannot have changed since that computation, which keeps it valid.
#[derive(Debug, Clone)]
struct OrderedRead {
    window: Window,
    /// The index fields after the eq-prefix (`index.fields[eq.len()..]`), in
    /// sort order, with their declared types. Empty when the query declares no
    /// index (or pins every index field), leaving `created_at` as the sole
    /// comparable sort component — see `SortKey`.
    sort_fields: Vec<(String, FieldType)>,
    /// `order: "desc"` — applies uniformly to every sort column, matching
    /// `execute_query`'s single `dir` for the whole `ORDER BY`.
    desc: bool,
    terminal: OrderedTerminal,
    boundary: Option<SortKey>,
}

/// Which truncating terminal produced the result, i.e. how to tell whether a
/// result was FULL (a further document could exist beyond its last row).
#[derive(Debug, Clone)]
enum OrderedTerminal {
    /// `take(N)`: full iff the result holds exactly N docs.
    Take(u32),
    /// `first`: full iff a doc was returned at all (N = 1).
    First,
    /// `paginate`: full iff a next cursor was issued. A page holding exactly
    /// `numItems` docs but NO next cursor is deliberately treated as not full:
    /// an insert beyond its last row would flip `hasNext` on and mint a cursor,
    /// which changes the result even though the docs are untouched.
    Paginate,
}

/// A document's position in a subscription's sort order: the index fields
/// after the eq-prefix, then `created_at`. Mirrors `execute_query`'s
/// `ORDER BY <index fields...>, created_at, id`.
///
/// `id` — the DB's final tie-breaker — is deliberately NOT carried: Postgres
/// orders it by the database collation while Rust would compare it bytewise,
/// so a tie on every other component is reported as doubt (`compare_keys`
/// returns `None`) and the caller re-runs. Over-approximating the handful of
/// documents that tie exactly with the boundary costs nothing and removes the
/// only collation-sensitive comparison from the skip decision.
#[derive(Debug, Clone, PartialEq)]
struct SortKey {
    fields: Vec<EqBind>,
    created_at: i64,
}

/// Lexicographic comparison of two sort keys, in ASCENDING terms (the caller
/// applies the query's direction). `None` = "cannot tell", which every caller
/// resolves as "re-run".
///
/// `None` is returned when the keys have different arity (a schema evolved
/// under a live subscription), when `cmp_binds` cannot compare a component, or
/// when the keys are equal on every carried component (the DB would break that
/// tie on `id` under its own collation — see `SortKey`).
fn compare_keys(a: &SortKey, b: &SortKey) -> Option<std::cmp::Ordering> {
    if a.fields.len() != b.fields.len() {
        return None;
    }
    for (x, y) in a.fields.iter().zip(b.fields.iter()) {
        match cmp_binds(x, y)? {
            std::cmp::Ordering::Equal => continue,
            ord => return Some(ord),
        }
    }
    match a.created_at.cmp(&b.created_at) {
        std::cmp::Ordering::Equal => None,
        ord => Some(ord),
    }
}

/// Whether `key` ranks at or before `boundary` in the subscription's order —
/// i.e. whether a document with that key could be inside a top-N window whose
/// last row has `boundary`. `None` = cannot tell.
///
/// Ascending: at-or-before means `key <= boundary`. Descending: the DB lists
/// larger keys first, so at-or-before means `key >= boundary`.
fn ranks_at_or_before(key: &SortKey, boundary: &SortKey, desc: bool) -> Option<bool> {
    let ord = compare_keys(key, boundary)?;
    Some(if desc {
        ord != std::cmp::Ordering::Less
    } else {
        ord != std::cmp::Ordering::Greater
    })
}

impl OrderedRead {
    /// The sort key of a written document's captured state. `None` when the
    /// document cannot be ranked — `created_at` was not captured (a `Delete`),
    /// or a sort field is missing / null / wrongly typed (Postgres would order
    /// SQL NULLs by its own NULLS FIRST/LAST rule, which this does not model).
    /// Callers treat `None` as "re-run".
    fn sort_key_of_written(
        &self,
        doc: &serde_json::Map<String, serde_json::Value>,
        created_at: Option<i64>,
    ) -> Option<SortKey> {
        let created_at = created_at?;
        let mut fields = Vec::with_capacity(self.sort_fields.len());
        for (name, ty) in &self.sort_fields {
            let value = doc.get(name).unwrap_or(&serde_json::Value::Null);
            fields.push(eq_bind_for(ty, value).ok()?);
        }
        Some(SortKey { fields, created_at })
    }

    /// The sort key of a document as it appears in a `QueryResult` — a merged
    /// doc, so `created_at` is read from the `_creationTime` system field that
    /// `merge_doc` stamps on. `None` ⇒ the caller leaves the boundary unset
    /// (unbounded window), which only over-approximates.
    fn sort_key_of_result_doc(&self, doc: &serde_json::Value) -> Option<SortKey> {
        let obj = doc.as_object()?;
        let created_at = obj.get("_creationTime")?.as_i64()?;
        let mut fields = Vec::with_capacity(self.sort_fields.len());
        for (name, ty) in &self.sort_fields {
            let value = obj.get(name).unwrap_or(&serde_json::Value::Null);
            fields.push(eq_bind_for(ty, value).ok()?);
        }
        Some(SortKey { fields, created_at })
    }

    /// The boundary implied by a computed result: the sort key of its last
    /// document when the result was FULL, `None` otherwise (an unfull result
    /// bounds nothing — anything matching can still enter it).
    ///
    /// A result shape that doesn't match the terminal (impossible in practice)
    /// also yields `None`, the over-approximating answer.
    fn boundary_from_result(&self, result: &QueryResult) -> Option<SortKey> {
        match (&self.terminal, result) {
            (OrderedTerminal::Take(n), QueryResult::Docs(docs)) => {
                if docs.len() as u32 == *n {
                    self.sort_key_of_result_doc(docs.last()?)
                } else {
                    None
                }
            }
            (OrderedTerminal::First, QueryResult::Doc(Some(doc))) => {
                self.sort_key_of_result_doc(doc)
            }
            (OrderedTerminal::Paginate, QueryResult::Paginated(page)) => {
                if page.next_cursor.is_some() {
                    self.sort_key_of_result_doc(page.docs.last()?)
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Whether a document in this state could be inside the ordered window:
    /// inside the eq/range window AND ranking at or before the boundary.
    /// Any doubt about the ranking resolves to `true` (re-run).
    fn state_affects(
        &self,
        doc: &serde_json::Map<String, serde_json::Value>,
        created_at: Option<i64>,
    ) -> bool {
        if !self.window.contains(doc) {
            return false;
        }
        let Some(boundary) = &self.boundary else {
            // Unbounded: the last result was not full, so any matching doc is
            // in it (or can enter it) — plain membership decides.
            return true;
        };
        let Some(key) = self.sort_key_of_written(doc, created_at) else {
            return true;
        };
        ranks_at_or_before(&key, boundary, self.desc).unwrap_or(true)
    }
}

/// Whether a written doc's before/after affects an `Ordered` subscription.
///
/// - deleted (after None) ⇒ `true`: `Delete` captures no values, so the doc
///   can be neither window-checked nor ranked.
/// - otherwise ⇒ affects iff EITHER state could be inside the ordered window.
///   Both states matter for the same reason `collect` is content-bearing: a
///   `take` result carries doc bodies, so a member's body change is a result
///   change, and a doc leaving the window changes the result even though its
///   after-state is outside it.
///
/// Owner filtering and `filter` are intentionally NOT consulted, exactly as in
/// `indexed_affects` — they only narrow the real result, so ignoring them
/// over-approximates. (They also only push the boundary further out, never
/// closer, so a doc ranking beyond the boundary stays out of the result.)
fn ordered_affects(ordered: &OrderedRead, values: &DocValues) -> bool {
    match (&values.before, &values.after) {
        (_, None) => true,
        (None, Some(after)) => ordered.state_affects(after, values.created_at),
        (Some(before), Some(after)) => {
            ordered.state_affects(before, values.created_at)
                || ordered.state_affects(after, values.created_at)
        }
    }
}

/// Builds the eq-prefix + range `Window` a query's `index` / `eq` / range
/// bounds describe, along with the eq arity and the resolved index def.
///
/// A query with no `index` yields an empty window (matches everything), arity
/// 0, and no index def — correct for `Ordered`, and rejected by `try_indexed`
/// as offering no skip. Returns `None` when the index doesn't resolve or an eq
/// value fails to type, so the caller falls back to a coarser read set.
///
/// A range bound whose VALUE fails to type is dropped rather than failing the
/// whole derivation: a window missing a bound is wider than the query's, which
/// over-approximates (more docs judged "inside" ⇒ more re-runs), never less.
fn try_window<'a>(
    query: &Query,
    table_def: &'a TableDef,
) -> Option<(Window, usize, Option<&'a IndexDef>)> {
    let Some(index_name) = query.index.as_ref() else {
        return Some((
            Window {
                eq: Vec::new(),
                range: None,
            },
            0,
            None,
        ));
    };
    let index_def = table_def.index(index_name).ok()?;

    // Type the eq-prefix via the shared typer (`eq_binds`); a type mismatch
    // (e.g. a string value against a Number field) ⇒ no window.
    let binds = eq_binds(table_def, index_def, &query.eq).ok()?;
    let eq_len = binds.len();

    // Zip the typed binds with their (field name, FieldType) from the table.
    let mut eq: Vec<(String, FieldType, EqBind)> = Vec::with_capacity(eq_len);
    for (field_name, bind) in index_def.fields[..eq_len].iter().zip(binds) {
        let field_type = table_def.fields.get(field_name)?.clone();
        eq.push((field_name.clone(), field_type, bind));
    }

    // Optional range bound on `index.fields[eq_len]` — present only when the
    // eq-prefix is a strict prefix of the index. A full-arity eq has no
    // remaining field to range on, which is fine (`range = None`).
    let range = if eq_len < index_def.fields.len() {
        let range_field = &index_def.fields[eq_len];
        let field_type = table_def.fields.get(range_field)?.clone();
        let lower = query
            .gte
            .as_ref()
            .and_then(|v| eq_bind_for(&field_type, v).ok())
            .map(|b| (b, true))
            .or_else(|| {
                query
                    .gt
                    .as_ref()
                    .and_then(|v| eq_bind_for(&field_type, v).ok())
                    .map(|b| (b, false))
            });
        let upper = query
            .lte
            .as_ref()
            .and_then(|v| eq_bind_for(&field_type, v).ok())
            .map(|b| (b, true))
            .or_else(|| {
                query
                    .lt
                    .as_ref()
                    .and_then(|v| eq_bind_for(&field_type, v).ok())
                    .map(|b| (b, false))
            });
        (lower.is_some() || upper.is_some()).then_some(RangeBound {
            field: range_field.clone(),
            field_type,
            lower,
            upper,
        })
    } else {
        None
    };

    Some((Window { eq, range }, eq_len, Some(index_def)))
}

impl ReadSet {
    fn from_query(query: &Query, table_def: &TableDef) -> Self {
        if let Some(id) = &query.get {
            return ReadSet::Point { id: id.clone() };
        }
        if let Some(indexed) = Self::try_indexed(query, table_def) {
            return ReadSet::Indexed(indexed);
        }
        match Self::try_ordered(query, table_def) {
            Some(ordered) => ReadSet::Ordered(ordered),
            None => ReadSet::Table,
        }
    }

    /// Derive an `Indexed` window from the query + table def. Returns `None`
    /// (⇒ try `Ordered`, else fall back to `Table`) when ANY of:
    /// - the terminal is not one of `count` / `collect` (no terminal) / `unique`;
    /// - a truncating/value-sensitive/ranking terminal is set
    ///   (`take`/`first`/`paginate`/`distinct`/`aggregate`/`search`/`vector`/`hybrid`);
    /// - no `index` is declared, or it has no eq bind AND no range bound
    ///   (the window would be the whole table → no skip benefit);
    /// - the index or any eq/range value fails to type (defensive: any doubt
    ///   ⇒ `Table`, which can only over-approximate).
    ///
    /// `from_query` never panics.
    fn try_indexed(query: &Query, table_def: &TableDef) -> Option<IndexedRead> {
        // "No terminal" (collect) = none of the terminals below is set. `take`
        // is checked here too: a `take`-less collect is the eligible shape.
        let is_collect = !query.unique
            && !query.count
            && !query.first
            && !query.distinct
            && query.aggregate.is_none()
            && query.paginate.is_none()
            && query.search.is_none()
            && query.vector_search.is_none()
            && query.hybrid_search.is_none()
            && query.take.is_none();
        let eligible_terminal = query.count || query.unique || is_collect;
        if !eligible_terminal {
            return None;
        }

        // Need an index with at least one eq bind OR a range bound — otherwise
        // the window is the whole table and there is no skip to be had.
        query.index.as_ref()?;
        let (window, _, _) = try_window(query, table_def)?;
        if window.eq.is_empty() && window.range.is_none() {
            return None;
        }

        Some(IndexedRead {
            window,
            // collect / unique return doc bodies (a member's content change
            // matters); count returns only a cardinality.
            content_bearing: query.unique || is_collect,
        })
    }

    /// Derive an `Ordered` (top-N boundary) read from a `take` / `first` /
    /// `paginate` query. Returns `None` (⇒ `Table`) when ANY of:
    /// - the terminal is not one of `take` / `first` / `paginate`, or a
    ///   value-sensitive / ranking terminal is also set (`count` / `unique` /
    ///   `distinct` / `aggregate` / `search` / `vector` / `hybrid`) — those
    ///   combinations are rejected by `execute_query`'s cascade anyway, so
    ///   checking them here is purely defensive;
    /// - the declared index, an eq value, or a sort field fails to resolve or
    ///   type (any doubt ⇒ `Table`, which can only over-approximate).
    ///
    /// Unlike `try_indexed` this does NOT require an index or an eq bind: a
    /// bare `take(N)` over a whole table is still bounded by its sort order,
    /// and `created_at, id` is always the tail of that order.
    fn try_ordered(query: &Query, table_def: &TableDef) -> Option<OrderedRead> {
        if query.count
            || query.unique
            || query.distinct
            || query.aggregate.is_some()
            || query.search.is_some()
            || query.vector_search.is_some()
            || query.hybrid_search.is_some()
        {
            return None;
        }
        let terminal = match (&query.paginate, query.first, query.take) {
            (Some(_), false, None) => OrderedTerminal::Paginate,
            (None, true, None) => OrderedTerminal::First,
            (None, false, Some(n)) => OrderedTerminal::Take(n),
            // No truncating terminal (plain collect), or a mutually-exclusive
            // combination the cascade rejects.
            _ => return None,
        };

        let (window, eq_len, index_def) = try_window(query, table_def)?;

        // The sort columns after the eq-prefix, mirroring `execute_query`'s
        // `sort_cols` (index fields beyond the eq-prefix, then created_at, id).
        let sort_fields = match index_def {
            Some(index_def) => {
                let mut fields = Vec::with_capacity(index_def.fields.len() - eq_len);
                for name in &index_def.fields[eq_len..] {
                    let field_type = table_def.fields.get(name)?.clone();
                    fields.push((name.clone(), field_type));
                }
                fields
            }
            None => Vec::new(),
        };

        Some(OrderedRead {
            window,
            sort_fields,
            desc: matches!(query.order, Some(Order::Desc)),
            terminal,
            // Seeded from the subscription's initial result by `register`, and
            // refreshed from every re-run; `None` until then (unbounded).
            boundary: None,
        })
    }
}

/// What `fan_out` should do with one subscription whose table was written.
enum Decision {
    Rerun,
    /// Skip: `class`'s read set proved every written document irrelevant.
    Skip(SkipClass),
}

/// Decide whether a subscription on `table` must re-run for this transaction.
/// The caller has already established that `table` was written.
///
/// Over-approximation is always safe (a needless re-run is diff-suppressed);
/// under-approximation is a missed realtime update, so every uncertain case
/// answers `Rerun`.
fn decide(read_set: &ReadSet, table: &str, write_set: &WriteSet) -> Decision {
    let (class, irrelevant) = match read_set {
        // Today's coarse behavior: any write to the table re-runs.
        ReadSet::Table => return Decision::Rerun,
        // A `get(id)` point read depends only on its one document, so a write
        // that didn't touch it cannot change the result.
        ReadSet::Point { id } => (
            SkipClass::Point,
            !write_set.docs.contains(&(table.to_string(), id.clone())),
        ),
        // count / collect / unique: re-run only if some written doc crossed
        // the eq-prefix/range window boundary. Sound only because every
        // `EqBind` variant is comparable in `cmp_binds` — `Window::contains`
        // returns false on comparison doubt, which would UNDER-approximate.
        ReadSet::Indexed(indexed) => (
            SkipClass::Indexed,
            !any_written_affects(write_set, table, |values| indexed_affects(indexed, values)),
        ),
        // take / first / paginate: re-run only if some written doc is inside
        // the window AND ranks at or before the last result's boundary.
        ReadSet::Ordered(ordered) => (
            SkipClass::Ordered,
            !any_written_affects(write_set, table, |values| ordered_affects(ordered, values)),
        ),
    };
    if irrelevant {
        Decision::Skip(class)
    } else {
        Decision::Rerun
    }
}

/// Whether any document written to `table` satisfies `affects`. A written
/// `(table, id)` with no `doc_values` entry (shouldn't happen — every `touch`
/// site also captures) counts as affecting, so a capture gap can never turn
/// into a missed push.
fn any_written_affects(
    write_set: &WriteSet,
    table: &str,
    mut affects: impl FnMut(&DocValues) -> bool,
) -> bool {
    for (doc_table, doc_id) in &write_set.docs {
        if doc_table != table {
            continue;
        }
        match write_set
            .doc_values
            .get(&(doc_table.clone(), doc_id.clone()))
        {
            None => return true,
            Some(values) => {
                if affects(values) {
                    return true;
                }
            }
        }
    }
    false
}

struct SubEntry {
    query: Query,
    tx: UnboundedSender<ServerMessage>,
    last: String,
    read_set: ReadSet,
    /// The subscriber's per-row auth identity, captured at subscribe time.
    /// `None` = bypass (machine tokens / scheduled jobs); `Some(user_id)` =
    /// re-run this subscription's query filtered to that user's rows.
    owner: Option<String>,
}

/// One database's subscriptions, keyed by `(connection, queryId)`.
type DbSubs = HashMap<(ConnId, String), SubEntry>;

/// Registered live-query subscriptions, sharded per database. Each database's
/// subscriptions live behind its own `Arc<Mutex<DbSubs>>`, so a per-db
/// committer's `fan_out` (a Postgres re-run per affected subscription) holds
/// ONLY its own shard lock — it does not collide with other databases' writes,
/// subscribes, or teardowns. The outer `Mutex<HashMap<..>>` is acquired only
/// long enough to look up or insert the target shard's `Arc` (an in-memory
/// op with no I/O), then dropped before any per-shard lock is taken.
///
/// `register` and `fan_out` are called only from the per-db committer task
/// (see `committer.rs`), which serializes them against every mutation;
/// `remove`/`remove_conn` may be called from anywhere (e.g. connection
/// teardown).
///
/// Lock discipline: the outer lock is NEVER held while waiting on an inner
/// shard lock — every path drops outer before taking inner, so there is no
/// lock-ordering cycle. Empty shards are RETAINED (lazy) rather than evicted
/// under a second lock acquire: evicting a now-empty shard would race a
/// concurrent `register` that already cloned this same Arc (before the
/// eviction) and would orphan its subscription — `fan_out` would no longer
/// find the shard. Shard count is bounded by the number of databases (each
/// persistent), and an empty shard is a single empty HashMap, so retaining
/// them costs nothing measurable.
pub struct SubscriptionManager {
    subs: Mutex<HashMap<String, Arc<Mutex<DbSubs>>>>,
    /// Where `fan_out` records skip/re-run effectiveness and verification
    /// outcomes. `None` = don't record (the bare `new()` used by tests that
    /// assert nothing about instrumentation).
    metrics: Option<Arc<Metrics>>,
    /// Verify 1 skip in every N (0 = off). See `Config::subs_verify_skip_every`.
    verify_skip_every: u64,
    /// Monotonic skip counter driving the deterministic 1-in-N sampler. Only
    /// read when `verify_skip_every > 0`.
    skip_seq: AtomicU64,
}

impl SubscriptionManager {
    /// Uninstrumented manager: no metrics recording, no skip verification.
    pub fn new() -> Arc<Self> {
        Self::with_instrumentation(None, 0)
    }

    /// Manager wired to the process metrics and the skip-verification sampler
    /// (`AppState::new` passes `Config::subs_verify_skip_every`).
    pub fn with_instrumentation(
        metrics: Option<Arc<Metrics>>,
        verify_skip_every: u64,
    ) -> Arc<Self> {
        Arc::new(Self {
            subs: Mutex::new(HashMap::new()),
            metrics,
            verify_skip_every,
            skip_seq: AtomicU64::new(0),
        })
    }

    /// Whether this skip should be shadow-verified. Deterministic every-Nth
    /// sampling rather than random, so a test can pin the rate and get exact
    /// counts. Counts every skip so the stride is stable regardless of which
    /// read-set class produced it.
    fn sample_skip_verification(&self) -> bool {
        let every = self.verify_skip_every;
        if every == 0 {
            return false;
        }
        let seq = self.skip_seq.fetch_add(1, Ordering::Relaxed);
        seq.is_multiple_of(every)
    }

    /// Get-or-insert the shard for `db` (used by `register`). The outer lock
    /// is held only across the entry API; the returned `Arc` clones out.
    async fn shard_insert(&self, db: &str) -> Arc<Mutex<DbSubs>> {
        let mut guard = self.subs.lock().await;
        guard
            .entry(db.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(HashMap::new())))
            .clone()
    }

    /// Clone the shard `Arc` for `db` if it exists (used by `remove`,
    /// `remove_conn`, `fan_out`). The outer lock is held only across the get.
    async fn shard_get(&self, db: &str) -> Option<Arc<Mutex<DbSubs>>> {
        let guard = self.subs.lock().await;
        guard.get(db).cloned()
    }

    pub async fn remove(&self, db: &str, conn: ConnId, query_id: &str) {
        let Some(shard) = self.shard_get(db).await else {
            return;
        };
        let mut db_subs = shard.lock().await;
        db_subs.remove(&(conn, query_id.to_string()));
        // Empty shard is retained (lazy) — see the struct doc. Evicting here
        // would orphan a concurrently-registered subscription.
    }

    pub async fn remove_conn(&self, db: &str, conn: ConnId) {
        let Some(shard) = self.shard_get(db).await else {
            return;
        };
        let mut db_subs = shard.lock().await;
        db_subs.retain(|(c, _), _| *c != conn);
        // Empty shard is retained (lazy) — see the struct doc.
    }

    /// Drops every subscription for `db` and removes its shard, used by
    /// `delete-db` to evict a deleted database's live-query state. Live
    /// `/sync` connections to `db` will see errors on their next op (the
    /// schema is gone), which is acceptable for a deleted database. Unlike
    /// `remove`/`remove_conn`, this drops the shard Arc entirely — fine here
    /// because no future `register` will target a deleted db (the next
    /// `submit` for it 404s at `database_exists` before reaching the committer).
    pub async fn drop_db(&self, db: &str) {
        let mut guard = self.subs.lock().await;
        guard.remove(db);
    }

    /// Total active subscriptions across all databases (a dashboard gauge).
    /// Approximate by design — each shard is locked individually after the
    /// outer map is released, so a subscribe/unsubscribe racing this call can
    /// shift the count by one. That is acceptable for a metrics gauge and
    /// avoids holding the outer lock while waiting on every shard's inner
    /// lock (which would re-introduce the global serialization ARC-001
    /// removed).
    pub async fn count(&self) -> usize {
        let shards: Vec<Arc<Mutex<DbSubs>>> = {
            let guard = self.subs.lock().await;
            guard.values().cloned().collect()
        };
        let mut total = 0;
        for shard in shards {
            total += shard.lock().await.len();
        }
        total
    }

    /// Registers a subscription that has already sent its initial
    /// `QueryUpdate` with `last` as the canonical form of that initial result.
    /// Called only by the committer task, immediately after the initial send,
    /// so no fan-out between execute and register can be missed. `owner` is
    /// the subscriber's per-row auth identity (see `SubEntry::owner`).
    /// `table_def` resolves the query's index/field types so an `Indexed` or
    /// `Ordered` ReadSet can be derived for fine-grained invalidation; a `get`
    /// query ignores it. The def is borrowed only for derivation — the stored
    /// `ReadSet` owns its binds, so the subscription survives a later schema
    /// evolution (an `Indexed` referencing a since-changed field biases to
    /// re-run via `Window::contains`'s "any doubt ⇒ outside" rule).
    /// `initial` is the result just pushed, from which an `Ordered` read seeds
    /// its top-N boundary; every other shape ignores it.
    // Each arg is independently required by the committer's register path;
    // bundling them into a context struct would add indirection without
    // reducing coupling (same call as `ws::handle_text_frame`).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn register(
        &self,
        db: &str,
        conn: ConnId,
        query_id: String,
        query: Query,
        tx: UnboundedSender<ServerMessage>,
        last: String,
        owner: Option<String>,
        table_def: &TableDef,
        initial: &QueryResult,
    ) {
        let mut read_set = ReadSet::from_query(&query, table_def);
        if let ReadSet::Ordered(ordered) = &mut read_set {
            ordered.boundary = ordered.boundary_from_result(initial);
        }
        let shard = self.shard_insert(db).await;
        let mut db_subs = shard.lock().await;
        db_subs.insert(
            (conn, query_id),
            SubEntry {
                query,
                tx,
                last,
                read_set,
                owner,
            },
        );
    }

    /// Re-runs every subscription on `db` whose query table is in
    /// `write_set`, pushing a `QueryUpdate` only when the canonical result
    /// changed. A subscriber whose re-run errors (e.g. an evolved schema) is
    /// logged and skipped, never fails the caller. Send errors (receiver
    /// dropped) are ignored; connection teardown is expected to call
    /// `remove_conn` separately.
    ///
    /// Records skip/re-run effectiveness on `self.metrics`, and — for 1 skip in
    /// every `verify_skip_every` — SHADOW-VERIFIES the skip: the query runs
    /// anyway and its result is compared against the last pushed one. A
    /// divergence means the read set under-approximated (a realtime update
    /// would have been dropped), so it is logged at ERROR, counted, and the
    /// corrected result IS pushed — verification repairs as well as reports.
    /// Only read-set skips are verified; the `write_set.tables` fast path above
    /// it is trivially sound (a query reads exactly one table) and verifying it
    /// would cost a round-trip per subscription per write.
    pub(crate) async fn fan_out(
        &self,
        pool: &PgPool,
        db: &str,
        schema: &SchemaDef,
        write_set: &WriteSet,
    ) {
        // Clone the shard Arc out under the outer lock, then drop the outer
        // guard before the per-subscription re-runs. This is the heart of
        // ARC-001: the re-runs (each a Postgres round-trip) hold only this
        // db's shard lock, never the global map lock, so a slow `fan_out` on
        // db A cannot stall writes/subscribes/teardowns on db B.
        let Some(shard) = self.shard_get(db).await else {
            return;
        };
        let mut db_subs = shard.lock().await;

        for ((_, query_id), entry) in db_subs.iter_mut() {
            if !write_set.tables.contains(&entry.query.table) {
                continue;
            }
            // `Some(class)` once this iteration is a shadow verification of a
            // skip rather than a real re-run: the decision said "skip", and the
            // sampler picked it for checking. The class rides along so a
            // divergence names which read set got it wrong.
            let verifying = match decide(&entry.read_set, &entry.query.table, write_set) {
                Decision::Rerun => {
                    if let Some(metrics) = &self.metrics {
                        metrics.record_subs_rerun();
                    }
                    None
                }
                Decision::Skip(class) => {
                    if let Some(metrics) = &self.metrics {
                        metrics.record_subs_skip(class);
                    }
                    if !self.sample_skip_verification() {
                        continue;
                    }
                    if let Some(metrics) = &self.metrics {
                        metrics.record_subs_skip_verification();
                    }
                    Some(class)
                }
            };

            let result =
                match execute_query(pool, db, schema, &entry.query, entry.owner.as_deref()).await {
                    Ok(result) => result,
                    Err(err) => {
                        tracing::warn!(
                            error = %err,
                            db,
                            query_id,
                            "subscription re-run failed; skipping"
                        );
                        continue;
                    }
                };

            // Refresh an `Ordered` read's top-N boundary from the result just
            // computed, BEFORE the canonical diff: the boundary describes the
            // most recent COMPUTED result (not the last pushed one), which is
            // exactly the baseline the next skip decision reasons against. A
            // diff-suppressed re-run recomputes the same boundary, and a
            // re-run that errored above left the previous one in place —
            // still correct, since the previous result is then still current.
            if let ReadSet::Ordered(ordered) = &mut entry.read_set {
                ordered.boundary = ordered.boundary_from_result(&result);
            }

            let canon = canonical(&result);
            if canon == entry.last {
                // Unchanged. For a verification pass this is the expected
                // outcome — the skip was correct, and it cost one round-trip to
                // prove it.
                continue;
            }

            if let Some(class) = verifying {
                // The skip decision was WRONG: we had decided this write could
                // not change the result, and it did. That is the one failure
                // mode invalidation must never have, and it is silent without
                // this check — so shout, count it, and fall through to push the
                // corrected result so the subscriber is repaired rather than
                // left stale.
                if let Some(metrics) = &self.metrics {
                    metrics.record_subs_missed_push();
                }
                tracing::error!(
                    db,
                    query_id,
                    class = ?class,
                    table = %entry.query.table,
                    read_set = ?entry.read_set,
                    "MISSED PUSH: subscription invalidation skipped a re-run whose result had \
                     changed — this is an invalidation soundness bug. Pushing the corrected \
                     result. Report the class + read_set + query shape."
                );
            }

            let value = match serde_json::to_value(&result) {
                Ok(value) => value,
                Err(err) => {
                    tracing::warn!(
                        error = %err,
                        db,
                        query_id,
                        "failed to serialize query result; skipping"
                    );
                    continue;
                }
            };

            entry.last = canon;
            let _ = entry.tx.send(ServerMessage::QueryUpdate {
                query_id: query_id.clone(),
                result: value,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{IndexDef, VectorIndexSpec};
    use std::collections::BTreeMap;

    fn q(value: serde_json::Value) -> Query {
        serde_json::from_value(value).expect("parse query")
    }

    /// A table with string/number/boolean indexable fields and a couple of
    /// multi-field indexes — enough to exercise eq-prefix + range derivation
    /// and `in_window` typing across all three indexable scalar types.
    fn test_table_def() -> TableDef {
        TableDef {
            fields: BTreeMap::from([
                ("status".to_string(), FieldType::String),
                ("order".to_string(), FieldType::Number),
                ("flag".to_string(), FieldType::Boolean),
                ("label".to_string(), FieldType::String),
            ]),
            indexes: vec![
                IndexDef {
                    name: "by_status".to_string(),
                    fields: vec!["status".to_string()],
                    search: false,
                    vector: None,
                    unique: false,
                    r#where: None,
                },
                IndexDef {
                    name: "by_status_order".to_string(),
                    fields: vec!["status".to_string(), "order".to_string()],
                    search: false,
                    vector: None,
                    unique: false,
                    r#where: None,
                },
                IndexDef {
                    name: "by_flag".to_string(),
                    fields: vec!["flag".to_string()],
                    search: false,
                    vector: None,
                    unique: false,
                    r#where: None,
                },
            ],
            owner_field: None,
            collaborators_field: None,
        }
    }

    // Suppress the unused warning for VectorIndexSpec: it's part of IndexDef's
    // construction shape (search/vector defaults) but the test table is btree-only.
    #[allow(dead_code)]
    fn _unused_vector_spec() -> VectorIndexSpec {
        VectorIndexSpec {
            dimensions: 1,
            filter_fields: vec![],
        }
    }

    #[test]
    fn get_query_is_a_point_read() {
        let query = q(serde_json::json!({ "table": "t", "get": "abc" }));
        assert!(matches!(
            ReadSet::from_query(&query, &test_table_def()),
            ReadSet::Point { id } if id == "abc"
        ));
    }

    #[test]
    fn value_sensitive_queries_are_table_level() {
        let td = test_table_def();
        // Value-sensitive / ranking terminals stay Table even with an index +
        // eq: their result depends on the VALUES of the matching set, which
        // neither window membership nor a sort boundary can bound.
        let cases = [
            serde_json::json!({ "table": "t", "index": "by_status", "eq": ["x"], "distinct": true }),
            serde_json::json!({ "table": "t", "index": "by_status_order", "eq": ["x"], "aggregate": { "op": "sum" } }),
            // Untruncated collect / count with no eq and no range ⇒ the window
            // is the whole table and there is no boundary ⇒ no skip benefit.
            serde_json::json!({ "table": "t" }),
            serde_json::json!({ "table": "t", "count": true }),
            serde_json::json!({ "table": "t", "index": "by_status", "count": true }),
        ];
        for case in cases {
            let query = q(case);
            assert!(
                matches!(ReadSet::from_query(&query, &td), ReadSet::Table),
                "expected Table-level for {:?}",
                query
            );
        }
    }

    #[test]
    fn count_collect_unique_on_eq_prefix_derive_indexed() {
        let td = test_table_def();
        // count → Indexed, content_bearing=false
        let query = q(
            serde_json::json!({ "table": "t", "index": "by_status", "eq": ["backlog"], "count": true }),
        );
        match ReadSet::from_query(&query, &td) {
            ReadSet::Indexed(idx) => {
                assert_eq!(idx.window.eq.len(), 1);
                assert_eq!(idx.window.eq[0].0, "status");
                assert!(matches!(idx.window.eq[0].1, FieldType::String));
                assert!(matches!(idx.window.eq[0].2, EqBind::Text(ref s) if s == "backlog"));
                assert!(!idx.content_bearing);
                assert!(idx.window.range.is_none());
            }
            other => panic!("count+eq should be Indexed, got {other:?}"),
        }

        // collect (no terminal) → Indexed, content_bearing=true
        let query = q(serde_json::json!({ "table": "t", "index": "by_status", "eq": ["backlog"] }));
        match ReadSet::from_query(&query, &td) {
            ReadSet::Indexed(idx) => assert!(idx.content_bearing),
            other => panic!("collect+eq should be Indexed, got {other:?}"),
        }

        // unique → Indexed, content_bearing=true
        let query = q(
            serde_json::json!({ "table": "t", "index": "by_status", "eq": ["backlog"], "unique": true }),
        );
        match ReadSet::from_query(&query, &td) {
            ReadSet::Indexed(idx) => assert!(idx.content_bearing),
            other => panic!("unique+eq should be Indexed, got {other:?}"),
        }
    }

    #[test]
    fn range_bound_is_captured_when_present() {
        let td = test_table_def();
        // collect with eq=[status] + gte on order (the field after the eq prefix).
        let query = q(
            serde_json::json!({ "table": "t", "index": "by_status_order", "eq": ["backlog"], "gte": 10 }),
        );
        match ReadSet::from_query(&query, &td) {
            ReadSet::Indexed(idx) => {
                assert_eq!(idx.window.eq.len(), 1);
                let r = idx.window.range.expect("range bound present");
                assert_eq!(r.field, "order");
                assert!(matches!(r.field_type, FieldType::Number));
                // gte ⇒ lower inclusive
                let (lower, inc) = r.lower.expect("lower bound");
                assert!(matches!(lower, EqBind::Num(n) if n == 10.0));
                assert!(inc);
                assert!(r.upper.is_none());
            }
            other => panic!("collect+eq+range should be Indexed, got {other:?}"),
        }

        // lt ⇒ upper exclusive
        let query = q(
            serde_json::json!({ "table": "t", "index": "by_status_order", "eq": ["backlog"], "lt": 100 }),
        );
        match ReadSet::from_query(&query, &td) {
            ReadSet::Indexed(idx) => {
                let r = idx.window.range.expect("range");
                let (upper, inc) = r.upper.expect("upper");
                assert!(matches!(upper, EqBind::Num(n) if n == 100.0));
                assert!(!inc);
                assert!(r.lower.is_none());
            }
            other => panic!("got {other:?}"),
        }
    }

    #[test]
    fn range_only_no_eq_derives_indexed() {
        let td = test_table_def();
        // eq=[] + a range bound on index.fields[0]. Eligible per the spec
        // ("eq non-empty OR a range bound present").
        let query = q(serde_json::json!({ "table": "t", "index": "by_status_order", "gte": "m" }));
        match ReadSet::from_query(&query, &td) {
            ReadSet::Indexed(idx) => {
                assert!(idx.window.eq.is_empty());
                let r = idx.window.range.expect("range");
                assert_eq!(r.field, "status");
            }
            other => panic!("range-only should be Indexed, got {other:?}"),
        }
    }

    #[test]
    fn wrong_typed_eq_value_falls_back_to_table() {
        let td = test_table_def();
        // eq value "not-a-number" against a Number field — typing fails ⇒ Table.
        let query = q(
            serde_json::json!({ "table": "t", "index": "by_status_order", "eq": ["backlog", "oops"] }),
        );
        assert!(matches!(ReadSet::from_query(&query, &td), ReadSet::Table));
    }

    // ---- Window::contains unit cases (spec test #8: null/wrong-typed/missing ⇒ false) ----

    /// A window pinning `status` to one value, with no range bound.
    fn status_window(status: &str) -> Window {
        Window {
            eq: vec![(
                "status".to_string(),
                FieldType::String,
                EqBind::Text(status.to_string()),
            )],
            range: None,
        }
    }

    fn indexed_status_eq(status: &str, content_bearing: bool) -> IndexedRead {
        IndexedRead {
            window: status_window(status),
            content_bearing,
        }
    }

    #[test]
    fn in_window_matching_eq_returns_true() {
        let idx = indexed_status_eq("backlog", true);
        let doc = serde_json::json!({ "status": "backlog" })
            .as_object()
            .unwrap()
            .clone();
        assert!(idx.window.contains(&doc));
    }

    #[test]
    fn in_window_non_matching_eq_returns_false() {
        let idx = indexed_status_eq("backlog", true);
        let doc = serde_json::json!({ "status": "done" })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.window.contains(&doc));
    }

    #[test]
    fn in_window_missing_field_returns_false() {
        let idx = indexed_status_eq("backlog", true);
        let doc = serde_json::json!({ "other": "x" })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.window.contains(&doc));
    }

    #[test]
    fn in_window_null_field_returns_false() {
        let idx = indexed_status_eq("backlog", true);
        let doc = serde_json::json!({ "status": null })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.window.contains(&doc));
    }

    #[test]
    fn in_window_wrong_typed_field_returns_false() {
        let idx = indexed_status_eq("backlog", true);
        // status declared String; doc carries a number ⇒ typing fails ⇒ outside.
        let doc = serde_json::json!({ "status": 5 })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.window.contains(&doc));
    }

    #[test]
    fn in_window_range_satisfied_and_not() {
        let idx = Window {
            eq: vec![(
                "status".to_string(),
                FieldType::String,
                EqBind::Text("backlog".to_string()),
            )],
            range: Some(RangeBound {
                field: "order".to_string(),
                field_type: FieldType::Number,
                lower: Some((EqBind::Num(10.0), true)), // gte 10
                upper: Some((EqBind::Num(20.0), false)), // lt 20
            }),
        };
        // In range.
        let doc = serde_json::json!({ "status": "backlog", "order": 15 })
            .as_object()
            .unwrap()
            .clone();
        assert!(idx.contains(&doc));
        // Below lower bound.
        let doc = serde_json::json!({ "status": "backlog", "order": 5 })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.contains(&doc));
        // At lower bound (inclusive) — in.
        let doc = serde_json::json!({ "status": "backlog", "order": 10 })
            .as_object()
            .unwrap()
            .clone();
        assert!(idx.contains(&doc));
        // At upper bound (exclusive) — out.
        let doc = serde_json::json!({ "status": "backlog", "order": 20 })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.contains(&doc));
        // Above upper bound.
        let doc = serde_json::json!({ "status": "backlog", "order": 25 })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.contains(&doc));
    }

    #[test]
    fn in_window_range_null_order_field_returns_false() {
        let idx = Window {
            eq: vec![(
                "status".to_string(),
                FieldType::String,
                EqBind::Text("backlog".to_string()),
            )],
            range: Some(RangeBound {
                field: "order".to_string(),
                field_type: FieldType::Number,
                lower: Some((EqBind::Num(10.0), true)),
                upper: None,
            }),
        };
        // order absent ⇒ doc.get returns Null ⇒ typing fails ⇒ outside.
        let doc = serde_json::json!({ "status": "backlog" })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.contains(&doc));
    }

    #[test]
    fn in_window_boolean_eq_matches() {
        let idx = Window {
            eq: vec![("flag".to_string(), FieldType::Boolean, EqBind::Bool(true))],
            range: None,
        };
        let doc = serde_json::json!({ "flag": true })
            .as_object()
            .unwrap()
            .clone();
        assert!(idx.contains(&doc));
        let doc = serde_json::json!({ "flag": false })
            .as_object()
            .unwrap()
            .clone();
        assert!(!idx.contains(&doc));
    }

    // ---- indexed_affects per-doc decision (spec's fan_out table) ----

    fn doc_with_status(status: &str) -> serde_json::Map<String, serde_json::Value> {
        serde_json::json!({ "status": status })
            .as_object()
            .unwrap()
            .clone()
    }

    #[test]
    fn affects_deleted_is_always_true() {
        let idx = indexed_status_eq("backlog", false);
        let values = DocValues {
            before: Some(doc_with_status("backlog")),
            after: None,
            created_at: None,
        };
        assert!(indexed_affects(&idx, &values));
        // Even a delete of a doc that was never in window still re-runs
        // (values gone ⇒ conservative re-run).
        let values = DocValues {
            before: Some(doc_with_status("done")),
            after: None,
            created_at: None,
        };
        assert!(indexed_affects(&idx, &values));
    }

    #[test]
    fn affects_created_in_window_true_outside_false() {
        let idx = indexed_status_eq("backlog", false);
        let in_win = DocValues {
            before: None,
            after: Some(doc_with_status("backlog")),
            created_at: None,
        };
        assert!(indexed_affects(&idx, &in_win));
        let out_win = DocValues {
            before: None,
            after: Some(doc_with_status("done")),
            created_at: None,
        };
        assert!(!indexed_affects(&idx, &out_win));
    }

    #[test]
    fn affects_count_only_membership_flip() {
        let idx = indexed_status_eq("backlog", false); // count
        // stayed outside — no affect
        let stayed_out = DocValues {
            before: Some(doc_with_status("done")),
            after: Some(doc_with_status("done")),
            created_at: None,
        };
        assert!(!indexed_affects(&idx, &stayed_out));
        // body-only change to a member (eq unchanged) — count unaffected
        let member_body_change = DocValues {
            before: Some(doc_with_status("backlog")),
            after: Some(doc_with_status("backlog")),
            created_at: None,
        };
        assert!(!indexed_affects(&idx, &member_body_change));
        // entered (done → backlog) — count increased
        let entered = DocValues {
            before: Some(doc_with_status("done")),
            after: Some(doc_with_status("backlog")),
            created_at: None,
        };
        assert!(indexed_affects(&idx, &entered));
        // left (backlog → done) — count decreased (regression guard for `before`)
        let left = DocValues {
            before: Some(doc_with_status("backlog")),
            after: Some(doc_with_status("done")),
            created_at: None,
        };
        assert!(indexed_affects(&idx, &left));
    }

    #[test]
    fn affects_collect_is_content_bearing() {
        let idx = indexed_status_eq("backlog", true); // collect
        // body-only change to a member — collect re-runs (content matters)
        let member_body_change = DocValues {
            before: Some(doc_with_status("backlog")),
            after: Some(doc_with_status("backlog")),
            created_at: None,
        };
        assert!(indexed_affects(&idx, &member_body_change));
        // stayed outside — no affect
        let stayed_out = DocValues {
            before: Some(doc_with_status("done")),
            after: Some(doc_with_status("done")),
            created_at: None,
        };
        assert!(!indexed_affects(&idx, &stayed_out));
    }

    // =================================================================
    // v3: ordered top-N boundary tracking (take / first / paginate)
    // =================================================================

    // ---- derivation ----

    #[test]
    fn take_first_paginate_derive_ordered() {
        let td = test_table_def();

        // take + index with a strict-prefix eq ⇒ the remaining index field is
        // the leading sort column.
        let query = q(
            serde_json::json!({ "table": "t", "index": "by_status_order", "eq": ["backlog"], "take": 5 }),
        );
        match ReadSet::from_query(&query, &td) {
            ReadSet::Ordered(o) => {
                assert!(matches!(o.terminal, OrderedTerminal::Take(5)));
                assert_eq!(o.window.eq.len(), 1);
                assert_eq!(o.sort_fields.len(), 1);
                assert_eq!(o.sort_fields[0].0, "order");
                assert!(!o.desc);
                assert!(o.boundary.is_none(), "unbounded until register seeds it");
            }
            other => panic!("take+eq should be Ordered, got {other:?}"),
        }

        // first ⇒ Ordered with a First terminal.
        let query = q(
            serde_json::json!({ "table": "t", "index": "by_status", "eq": ["x"], "first": true }),
        );
        assert!(matches!(
            ReadSet::from_query(&query, &td),
            ReadSet::Ordered(o) if matches!(o.terminal, OrderedTerminal::First)
        ));

        // paginate ⇒ Ordered with a Paginate terminal.
        let query = q(
            serde_json::json!({ "table": "t", "index": "by_status", "eq": ["x"], "paginate": { "numItems": 10 } }),
        );
        assert!(matches!(
            ReadSet::from_query(&query, &td),
            ReadSet::Ordered(o) if matches!(o.terminal, OrderedTerminal::Paginate)
        ));

        // desc order is carried.
        let query = q(serde_json::json!({ "table": "t", "take": 3, "order": "desc" }));
        assert!(matches!(
            ReadSet::from_query(&query, &td),
            ReadSet::Ordered(o) if o.desc
        ));
    }

    #[test]
    fn bare_take_without_index_is_ordered_on_created_at() {
        // No index at all: the window matches everything and `created_at` is
        // the only sort component — still bounded by the top-N boundary.
        let query = q(serde_json::json!({ "table": "t", "take": 5 }));
        match ReadSet::from_query(&query, &test_table_def()) {
            ReadSet::Ordered(o) => {
                assert!(o.window.eq.is_empty());
                assert!(o.window.range.is_none());
                assert!(o.sort_fields.is_empty());
            }
            other => panic!("bare take should be Ordered, got {other:?}"),
        }
    }

    #[test]
    fn ordered_falls_back_to_table_on_unresolvable_index() {
        let td = test_table_def();
        // Unknown index ⇒ no window can be derived ⇒ Table (over-approximate).
        let query = q(serde_json::json!({ "table": "t", "index": "nope", "take": 5 }));
        assert!(matches!(ReadSet::from_query(&query, &td), ReadSet::Table));
        // Wrongly-typed eq value ⇒ Table for the ordered path too.
        let query = q(
            serde_json::json!({ "table": "t", "index": "by_status_order", "eq": ["backlog", "oops"], "take": 5 }),
        );
        assert!(matches!(ReadSet::from_query(&query, &td), ReadSet::Table));
    }

    // ---- fixtures ----

    /// A `take(n)` read over `status = "backlog"` sorted by `order`.
    fn ordered_take(n: u32, desc: bool, boundary: Option<SortKey>) -> OrderedRead {
        OrderedRead {
            window: status_window("backlog"),
            sort_fields: vec![("order".to_string(), FieldType::Number)],
            desc,
            terminal: OrderedTerminal::Take(n),
            boundary,
        }
    }

    fn key(order: f64, created_at: i64) -> SortKey {
        SortKey {
            fields: vec![EqBind::Num(order)],
            created_at,
        }
    }

    fn doc_at(status: &str, order: f64) -> serde_json::Map<String, serde_json::Value> {
        serde_json::json!({ "status": status, "order": order })
            .as_object()
            .expect("object")
            .clone()
    }

    /// A written doc that existed before the txn and still does (an update).
    fn updated(
        before: serde_json::Map<String, serde_json::Value>,
        after: serde_json::Map<String, serde_json::Value>,
        created_at: i64,
    ) -> DocValues {
        DocValues {
            before: Some(before),
            after: Some(after),
            created_at: Some(created_at),
        }
    }

    fn created(after: serde_json::Map<String, serde_json::Value>, created_at: i64) -> DocValues {
        DocValues {
            before: None,
            after: Some(after),
            created_at: Some(created_at),
        }
    }

    // ---- compare_keys / ranks_at_or_before ----

    #[test]
    fn compare_keys_orders_by_field_then_created_at() {
        use std::cmp::Ordering;
        assert_eq!(
            compare_keys(&key(1.0, 100), &key(2.0, 100)),
            Some(Ordering::Less)
        );
        assert_eq!(
            compare_keys(&key(2.0, 100), &key(1.0, 999)),
            Some(Ordering::Greater),
            "the leading sort field outranks created_at"
        );
        assert_eq!(
            compare_keys(&key(1.0, 100), &key(1.0, 200)),
            Some(Ordering::Less),
            "created_at breaks a tie on the sort field"
        );
    }

    #[test]
    fn compare_keys_reports_doubt_rather_than_guessing() {
        // Exact tie on every carried component: the DB would break it on `id`
        // under its own collation, which this deliberately does not model.
        assert_eq!(compare_keys(&key(1.0, 100), &key(1.0, 100)), None);
        // Arity mismatch (a schema evolved under a live subscription).
        let short = SortKey {
            fields: vec![],
            created_at: 100,
        };
        assert_eq!(compare_keys(&key(1.0, 100), &short), None);
        // Variant mismatch: no `cmp_binds` arm.
        let texty = SortKey {
            fields: vec![EqBind::Text("x".into())],
            created_at: 100,
        };
        assert_eq!(compare_keys(&key(1.0, 100), &texty), None);
    }

    #[test]
    fn ranks_at_or_before_follows_the_sort_direction() {
        let boundary = key(10.0, 500);
        // Ascending: smaller keys come first, so they are at-or-before.
        assert_eq!(
            ranks_at_or_before(&key(5.0, 500), &boundary, false),
            Some(true)
        );
        assert_eq!(
            ranks_at_or_before(&key(20.0, 500), &boundary, false),
            Some(false)
        );
        // Descending: larger keys come first.
        assert_eq!(
            ranks_at_or_before(&key(20.0, 500), &boundary, true),
            Some(true)
        );
        assert_eq!(
            ranks_at_or_before(&key(5.0, 500), &boundary, true),
            Some(false)
        );
        // Doubt propagates.
        assert_eq!(ranks_at_or_before(&boundary, &boundary, false), None);
    }

    // ---- boundary extraction ----

    fn result_doc(order: f64, created_at: i64) -> serde_json::Value {
        serde_json::json!({
            "status": "backlog",
            "order": order,
            "_id": "abc",
            "_creationTime": created_at,
            "_version": 1
        })
    }

    #[test]
    fn boundary_set_only_when_the_result_was_full() {
        let ordered = ordered_take(2, false, None);
        // Exactly N docs ⇒ bounded by the last one.
        let full = QueryResult::Docs(vec![result_doc(1.0, 100), result_doc(2.0, 200)]);
        assert_eq!(
            ordered.boundary_from_result(&full),
            Some(key(2.0, 200)),
            "a full take is bounded by its Nth doc"
        );
        // Fewer than N ⇒ unbounded: anything matching can still enter.
        let partial = QueryResult::Docs(vec![result_doc(1.0, 100)]);
        assert_eq!(ordered.boundary_from_result(&partial), None);
        assert_eq!(
            ordered.boundary_from_result(&QueryResult::Docs(vec![])),
            None
        );
    }

    #[test]
    fn boundary_for_first_and_paginate_terminals() {
        let first = OrderedRead {
            terminal: OrderedTerminal::First,
            ..ordered_take(1, false, None)
        };
        assert_eq!(
            first.boundary_from_result(&QueryResult::Doc(Some(result_doc(3.0, 300)))),
            Some(key(3.0, 300))
        );
        assert_eq!(first.boundary_from_result(&QueryResult::Doc(None)), None);

        let paginate = OrderedRead {
            terminal: OrderedTerminal::Paginate,
            ..ordered_take(1, false, None)
        };
        // A next cursor means a further doc exists ⇒ the page's last doc bounds it.
        let has_next = QueryResult::Paginated(crate::query::PaginatedResult {
            docs: vec![result_doc(1.0, 100), result_doc(2.0, 200)],
            next_cursor: Some("cursor".to_string()),
        });
        assert_eq!(
            paginate.boundary_from_result(&has_next),
            Some(key(2.0, 200))
        );
        // No next cursor: an insert beyond the last row would flip hasNext on
        // and mint a cursor, so the page must NOT be treated as bounded.
        let last_page = QueryResult::Paginated(crate::query::PaginatedResult {
            docs: vec![result_doc(1.0, 100), result_doc(2.0, 200)],
            next_cursor: None,
        });
        assert_eq!(paginate.boundary_from_result(&last_page), None);
    }

    #[test]
    fn boundary_unset_when_a_result_doc_cannot_be_ranked() {
        let ordered = ordered_take(1, false, None);
        // Missing `_creationTime` / missing sort field ⇒ no boundary (the
        // over-approximating answer), never a panic.
        let no_time =
            QueryResult::Docs(vec![serde_json::json!({ "status": "backlog", "order": 1 })]);
        assert_eq!(ordered.boundary_from_result(&no_time), None);
        let no_sort_field = QueryResult::Docs(vec![serde_json::json!({
            "status": "backlog", "_id": "a", "_creationTime": 1, "_version": 1
        })]);
        assert_eq!(ordered.boundary_from_result(&no_sort_field), None);
    }

    // ---- ordered_affects: the skip decision ----

    #[test]
    fn ordered_unbounded_degenerates_to_window_membership() {
        let ordered = ordered_take(5, false, None);
        // In-window insert affects; out-of-window insert does not.
        assert!(ordered_affects(
            &ordered,
            &created(doc_at("backlog", 999.0), 999)
        ));
        assert!(!ordered_affects(
            &ordered,
            &created(doc_at("done", 1.0), 100)
        ));
    }

    #[test]
    fn ordered_skips_writes_beyond_the_boundary() {
        // Top-2 of `order` ascending; boundary = (order 20, created 200).
        let ordered = ordered_take(2, false, Some(key(20.0, 200)));

        // A doc ranking after the boundary cannot be in the top 2, in any of
        // its states ⇒ skip.
        assert!(!ordered_affects(
            &ordered,
            &created(doc_at("backlog", 50.0), 500)
        ));
        assert!(!ordered_affects(
            &ordered,
            &updated(doc_at("backlog", 50.0), doc_at("backlog", 60.0), 500)
        ));
        // Out of the window entirely ⇒ skip regardless of rank.
        assert!(!ordered_affects(
            &ordered,
            &updated(doc_at("done", 1.0), doc_at("done", 2.0), 100)
        ));
    }

    #[test]
    fn ordered_reruns_for_writes_at_or_before_the_boundary() {
        let ordered = ordered_take(2, false, Some(key(20.0, 200)));

        // Inserted ahead of the boundary ⇒ enters the top 2.
        assert!(ordered_affects(
            &ordered,
            &created(doc_at("backlog", 5.0), 50)
        ));
        // A member's body/rank change ⇒ the result carries doc bodies.
        assert!(ordered_affects(
            &ordered,
            &updated(doc_at("backlog", 10.0), doc_at("backlog", 12.0), 100)
        ));
        // Moving OUT of the window from inside the top 2 ⇒ the before-state
        // was a member (regression guard for dropping `before`).
        assert!(ordered_affects(
            &ordered,
            &updated(doc_at("backlog", 10.0), doc_at("done", 10.0), 100)
        ));
        // Moving INTO the window from beyond the boundary, landing ahead of it.
        assert!(ordered_affects(
            &ordered,
            &updated(doc_at("done", 5.0), doc_at("backlog", 5.0), 50)
        ));
        // Exactly at the boundary ⇒ a tie compare_keys reports as doubt ⇒ re-run.
        assert!(ordered_affects(
            &ordered,
            &updated(doc_at("backlog", 20.0), doc_at("backlog", 20.0), 200)
        ));
    }

    #[test]
    fn ordered_desc_inverts_the_boundary_test() {
        // Newest-first feed: boundary = (order 20, created 200).
        let ordered = ordered_take(2, true, Some(key(20.0, 200)));
        // Larger sorts first ⇒ a larger key is INSIDE the window.
        assert!(ordered_affects(
            &ordered,
            &created(doc_at("backlog", 50.0), 500)
        ));
        // Smaller sorts later ⇒ beyond the boundary ⇒ skip.
        assert!(!ordered_affects(
            &ordered,
            &created(doc_at("backlog", 5.0), 50)
        ));
    }

    // ---- skip-verification sampler ----

    #[test]
    fn sampler_is_off_by_default_and_touches_no_state() {
        let mgr = SubscriptionManager::new();
        for _ in 0..10 {
            assert!(!mgr.sample_skip_verification());
        }
        // Disabled means the counter is never even incremented, so a busy
        // instance pays nothing for the knob being present.
        assert_eq!(mgr.skip_seq.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn sampler_every_1_verifies_every_skip() {
        let mgr = SubscriptionManager::with_instrumentation(None, 1);
        for _ in 0..5 {
            assert!(mgr.sample_skip_verification());
        }
    }

    #[test]
    fn sampler_every_n_verifies_one_in_n_deterministically() {
        let mgr = SubscriptionManager::with_instrumentation(None, 3);
        // Deterministic 1-in-3 starting with the first skip, so a test can pin
        // the rate and assert exact counts.
        let picks: Vec<bool> = (0..7).map(|_| mgr.sample_skip_verification()).collect();
        assert_eq!(
            picks,
            vec![true, false, false, true, false, false, true],
            "expected every 3rd skip to be verified"
        );
    }

    #[test]
    fn ordered_reruns_when_a_doc_cannot_be_ranked() {
        let ordered = ordered_take(2, false, Some(key(20.0, 200)));

        // Delete: no captured values at all ⇒ always re-run.
        let deleted = DocValues {
            before: None,
            after: None,
            created_at: None,
        };
        assert!(ordered_affects(&ordered, &deleted));

        // In-window but `created_at` was not captured ⇒ unrankable ⇒ re-run.
        let no_created_at = DocValues {
            before: None,
            after: Some(doc_at("backlog", 50.0)),
            created_at: None,
        };
        assert!(ordered_affects(&ordered, &no_created_at));

        // In-window but the sort field is missing / null / wrongly typed ⇒
        // Postgres would order the SQL NULL by its own rule ⇒ re-run.
        for bad in [
            serde_json::json!({ "status": "backlog" }),
            serde_json::json!({ "status": "backlog", "order": null }),
            serde_json::json!({ "status": "backlog", "order": "not-a-number" }),
        ] {
            let values = created(bad.as_object().expect("object").clone(), 500);
            assert!(
                ordered_affects(&ordered, &values),
                "unrankable doc must re-run: {bad}"
            );
        }
    }
}
