//! Read-set classification (QA-003 split): what a subscription's result
//! depends on, derived once from the (immutable) `Query` + table def at
//! registration. Any doubt over-approximates to re-run. Two safety nets guard
//! the skip logic: an exhaustive `cmp_binds` match (a new `EqBind` variant is
//! a compile error, not a silent under-approximation) and the
//! `RTDB_SUBS_VERIFY_SKIP_EVERY` shadow-verify probe. `distinct`/`search`/
//! `vector`/`hybrid` stay table-level by design — their results depend on
//! member values (beyond the eq/range window) or a ranking function.
//! `aggregate` (`sum`/`avg`/`min`/`max`/`count`) shares `count`/`collect`'s
//! `ReadSet::Indexed` window: a write provably outside the eq-prefix/range
//! window cannot move an aggregate scoped to that window.

use crate::query::{Order, Query, QueryResult};
use crate::schema::{FieldType, IndexDef, TableDef, TableDefExt};
use crate::txn::{DocValues, EqBind, eq_bind_for, eq_binds};

/// What a subscription's result depends on, used to skip needless re-runs.
/// Derived once from the (immutable) `Query` + the table def at registration.
#[derive(Debug, Clone)]
pub(super) enum ReadSet {
    /// A `get(id)` point read: the result is exactly this one document, so a
    /// write to any other document cannot change it.
    Point { id: String },
    /// A `count`, `collect`, `unique`, or `aggregate` query filtered on a
    /// btree index's eq-prefix (and an optional range bound on the next index
    /// field). A write to a document provably outside the window cannot
    /// change the result, so `fan_out` can skip the re-run. See `IndexedRead`
    /// for the soundness model. `aggregate` reuses the `content_bearing: true`
    /// (in-window value change matters) path `collect`/`unique` already use —
    /// its SUM/AVG/MIN/MAX/COUNT is a function of the values of documents
    /// INSIDE the window, so any write that cannot cross the window boundary
    /// cannot move it, exactly like a `collect`'s member set.
    Indexed(IndexedRead),
    /// A `take(N)` / `first` / `paginate` query: an ORDERED, truncated window.
    /// Window membership alone can't decide these (a doc can enter or leave the
    /// truncated window without its eq-prefix changing), so `OrderedRead` also
    /// tracks the sort key of the last result's final document — the boundary —
    /// and skips only writes that provably rank beyond it.
    Ordered(OrderedRead),
    /// Every other shape (distinct / search / vector / hybrid), plus an
    /// `aggregate` whose window derivation failed (no index, no eq/range
    /// bound, an unresolvable index, or a mistyped eq/range value): the
    /// result depends on the VALUES of the matching set or on a ranking
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
pub(super) struct Window {
    pub(super) eq: Vec<(String, FieldType, EqBind)>,
    pub(super) range: Option<RangeBound>,
}

/// A `count` / `collect` / `unique` / `aggregate` subscription's window.
/// `fan_out` re-runs only when a written document may have crossed the window
/// boundary.
///
/// `content_bearing` is true for `collect`, `unique`, and `aggregate` (the
/// result is a function of a member's field VALUES — a body change inside the
/// window can change it) and false for `count` (pure membership: only a
/// window-crossing changes the cardinality).
#[derive(Debug, Clone)]
pub(super) struct IndexedRead {
    pub(super) window: Window,
    pub(super) content_bearing: bool,
}

#[derive(Debug, Clone)]
pub(super) struct RangeBound {
    pub(super) field: String,
    pub(super) field_type: FieldType,
    /// (value, inclusive): a `gte` bound is `(v, true)`, a `gt` bound is `(v, false)`.
    pub(super) lower: Option<(EqBind, bool)>,
    /// (value, inclusive): a `lte` bound is `(v, true)`, an `lt` bound is `(v, false)`.
    pub(super) upper: Option<(EqBind, bool)>,
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
    pub(super) fn contains(&self, doc: &serde_json::Map<String, serde_json::Value>) -> bool {
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
pub(super) fn cmp_binds(a: &EqBind, b: &EqBind) -> Option<std::cmp::Ordering> {
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
pub(super) fn indexed_affects(indexed: &IndexedRead, values: &DocValues) -> bool {
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
pub(super) struct OrderedRead {
    pub(super) window: Window,
    /// The index fields after the eq-prefix (`index.fields[eq.len()..]`), in
    /// sort order, with their declared types. Empty when the query declares no
    /// index (or pins every index field), leaving `created_at` as the sole
    /// comparable sort component — see `SortKey`.
    pub(super) sort_fields: Vec<(String, FieldType)>,
    /// `order: "desc"` — applies uniformly to every sort column, matching
    /// `execute_query`'s single `dir` for the whole `ORDER BY`.
    pub(super) desc: bool,
    pub(super) terminal: OrderedTerminal,
    pub(super) boundary: Option<SortKey>,
}

/// Which truncating terminal produced the result, i.e. how to tell whether a
/// result was FULL (a further document could exist beyond its last row).
#[derive(Debug, Clone)]
pub(super) enum OrderedTerminal {
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
pub(super) struct SortKey {
    pub(super) fields: Vec<EqBind>,
    pub(super) created_at: i64,
}

/// Lexicographic comparison of two sort keys, in ASCENDING terms (the caller
/// applies the query's direction). `None` = "cannot tell", which every caller
/// resolves as "re-run".
///
/// `None` is returned when the keys have different arity (a schema evolved
/// under a live subscription), when `cmp_binds` cannot compare a component, or
/// when the keys are equal on every carried component (the DB would break that
/// tie on `id` under its own collation — see `SortKey`).
pub(super) fn compare_keys(a: &SortKey, b: &SortKey) -> Option<std::cmp::Ordering> {
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
pub(super) fn ranks_at_or_before(key: &SortKey, boundary: &SortKey, desc: bool) -> Option<bool> {
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
    pub(super) fn boundary_from_result(&self, result: &QueryResult) -> Option<SortKey> {
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
pub(super) fn ordered_affects(ordered: &OrderedRead, values: &DocValues) -> bool {
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
    pub(super) fn from_query(query: &Query, table_def: &TableDef) -> Self {
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

    /// The wire label for this read-set class (`point` / `indexed` / `ordered` /
    /// `table`), surfaced per-subscription by the inspector endpoint.
    pub(super) fn class_name(&self) -> &'static str {
        match self {
            ReadSet::Point { .. } => "point",
            ReadSet::Indexed(_) => "indexed",
            ReadSet::Ordered(_) => "ordered",
            ReadSet::Table => "table",
        }
    }

    /// Derive an `Indexed` window from the query + table def. Returns `None`
    /// (⇒ try `Ordered`, else fall back to `Table`) when ANY of:
    /// - the terminal is not one of `count` / `collect` (no terminal) /
    ///   `unique` / `aggregate`;
    /// - a truncating/value-sensitive/ranking terminal is set
    ///   (`take`/`first`/`paginate`/`distinct`/`search`/`vector`/`hybrid`);
    /// - no `index` is declared, or it has no eq bind AND no range bound
    ///   (the window would be the whole table → no skip benefit);
    /// - the index or any eq/range value fails to type (defensive: any doubt
    ///   ⇒ `Table`, which can only over-approximate).
    ///
    /// `aggregate` (any op, any `groupBy` shape) is eligible on the same
    /// footing as `collect`: the query's `WHERE` clause is the eq/range window
    /// regardless of grouping, so a write a written doc provably outside that
    /// window cannot enter ANY group and cannot move the aggregate — grouping
    /// only changes how the matching set is bucketed, not which docs are in
    /// it. This mirrors `count`/`collect`'s precedent (ARC-013 spec matrix
    /// row 21) and is the reason the window derivation needs no groupBy-aware
    /// branch here.
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
        let is_aggregate = query.aggregate.is_some();
        let eligible_terminal = query.count || query.unique || is_aggregate || is_collect;
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
            // collect / unique / aggregate return or depend on doc field
            // values (a member's content change matters); count returns only
            // a cardinality.
            content_bearing: query.unique || is_aggregate || is_collect,
        })
    }

    /// Derive an `Ordered` (top-N boundary) read from a `take` / `first` /
    /// `paginate` query. Returns `None` (⇒ `Table`) when ANY of:
    /// - the terminal is not one of `take` / `first` / `paginate`, or a
    ///   value-sensitive / ranking terminal is also set (`count` / `unique` /
    ///   `distinct` / `aggregate` / `search` / `vector` / `hybrid`). Most of
    ///   those combinations are rejected by `execute_query`'s cascade anyway,
    ///   but the three RANKING ones are load-bearing, not defensive, since
    ///   ENH-030: `paginate` now legally composes with `search` /
    ///   `vectorSearch` / `hybridSearch`. Such a page is ordered by a
    ///   relevance/distance/fusion score, not by the index tail this boundary
    ///   logic models, so it must fall back to `Table` (always re-run) rather
    ///   than be skipped against a boundary that does not describe it;
    /// - the declared index, an eq value, or a sort field fails to resolve or
    ///   type (any doubt ⇒ `Table`, which can only over-approximate).
    ///
    /// Unlike `try_indexed` this does NOT require an index or an eq bind: a
    /// bare `take(N)` over a whole table is still bounded by its sort order,
    /// and `created_at, id` is always the tail of that order.
    pub(super) fn try_ordered(query: &Query, table_def: &TableDef) -> Option<OrderedRead> {
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
