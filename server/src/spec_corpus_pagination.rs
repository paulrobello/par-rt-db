//! ENH-052 family 4 — the btree keyset-pagination spec-table corpus
//! (`wire-corpus/spec/pagination.json`), a sibling of `spec_corpus.rs`
//! (families 1-3). The server crate stays the single source: the expected
//! page of every row is computed by the reference reduction of the SQL that
//! `query/terminals.rs::compile_paginate_terminal` emits, and the runner
//! additions in the six clients assert their own `paginateResult` port
//! against it.
//!
//! `server/src/bin/gen-spec-pagination.rs` writes the fixture;
//! `make gen-spec-pagination-check` (in `checkall`) diffs the committed
//! fixture against a fresh generation.
//!
//! Cursor values are carried RAW in the fixture; each runner encodes them
//! through its own codec before the query, so cursor bytes stay
//! engine-neutral. The tiebreaker positions use engine-neutral extremes
//! (created_at 0 = before every engine-minted stamp, 4e12 = after; id "" /
//! "zzzz" = the lexicographic extremes of engine-minted ids) so the expected
//! page never depends on an engine's private clock or id minting.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use crate::schema::FieldType;

/// The sort direction of one row (the wire `Order` spelling).
enum PagOrder {
    Asc,
    Desc,
}

impl PagOrder {
    fn wire(&self) -> &'static str {
        match self {
            PagOrder::Asc => "asc",
            PagOrder::Desc => "desc",
        }
    }

    fn is_asc(&self) -> bool {
        matches!(self, PagOrder::Asc)
    }
}

/// Engine-neutral tiebreaker extremes for fixture cursors. Every engine's
/// `now()` stamps docs at ~1.7e12 ms, so 0 is strictly before and 4e12
/// strictly after every doc's `created_at`; `""` and `"zzzz"` are the
/// lexicographic extremes of the id strings engines mint.
const CREATED_AT_BEFORE_ALL: i64 = 0;
const CREATED_AT_PAST_END: i64 = 4_000_000_000_000;
const ID_MIN: &str = "";
const ID_MAX: &str = "zzzz";

/// The representative doc under a fixture cursor's tiebreaker tail: an id
/// string strictly between the `""` and `"zzzz"` extremes, so prefix-equal
/// and past-end cursors place every doc identically in every engine.
const REF_DOC_ID: &str = "m";

/// One column position inside a decoded cursor.
#[derive(Clone, Copy)]
enum Col {
    /// An unbound index field (position in the index's field list).
    Index(usize),
    /// The created_at tiebreaker.
    CreatedAt,
    /// The id tiebreaker.
    Id,
}

struct PaginateSpecRow {
    id: &'static str,
    fields: BTreeMap<String, FieldType>,
    index_name: &'static str,
    index_fields: Vec<&'static str>,
    docs: Vec<serde_json::Map<String, Value>>,
    eq: Vec<Value>,
    order: PagOrder,
    num_items: u32,
    cursor: Option<Vec<Value>>,
}

/// `Optional` over `inner` — the spelling for rows whose `v` carries nulls.
fn pag_opt(inner: FieldType) -> FieldType {
    FieldType::Optional {
        inner: Box::new(inner),
    }
}

fn pag_fields(entries: &[(&str, FieldType)]) -> BTreeMap<String, FieldType> {
    entries
        .iter()
        .map(|(name, ty)| (name.to_string(), ty.clone()))
        .collect()
}

fn pag_doc(entries: &[(&str, Value)]) -> serde_json::Map<String, Value> {
    entries
        .iter()
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect()
}

/// A two-field-index row: `s` is the eq-pinned prefix, `v` the keyset column.
fn pag_row(
    id: &'static str,
    vty: FieldType,
    docs: &[(&str, Value)],
    eq: &str,
    order: PagOrder,
    num_items: u32,
    cursor: Option<Vec<Value>>,
) -> PaginateSpecRow {
    PaginateSpecRow {
        id,
        fields: pag_fields(&[("s", FieldType::String), ("v", vty)]),
        index_name: "by_s_v",
        index_fields: vec!["s", "v"],
        // A JSON-null `v` pair spells an ABSENT key: the engines normalize an
        // explicit null on an optional field to a missing key on insert, and
        // absent equals null in the keyset ordering.
        docs: docs
            .iter()
            .map(|(s, v)| {
                if v.is_null() {
                    pag_doc(&[("s", json!(s))])
                } else {
                    pag_doc(&[("s", json!(s)), ("v", v.clone())])
                }
            })
            .collect(),
        eq: vec![json!(eq)],
        order,
        num_items,
        cursor,
    }
}

/// The multi-column rows' doc set over index [s, v, w]: four docs at one eq
/// value covering every (v, w) combination, so both unbound user columns
/// participate in the sort and the resume predicate.
fn pag_docs_swv(docs: &[(i64, &str)]) -> Vec<serde_json::Map<String, Value>> {
    docs.iter()
        .map(|(v, w)| pag_doc(&[("s", json!("todo")), ("v", json!(v)), ("w", json!(w))]))
        .collect()
}

/// The multi-column rows' wire field map.
fn pag_fields_swv() -> BTreeMap<String, FieldType> {
    pag_fields(&[
        ("s", FieldType::String),
        ("v", FieldType::Number),
        ("w", FieldType::String),
    ])
}

/// A cursor-kind rejection row: full-arity cursor with one tiebreaker
/// position mis-typed (`created_at_value` not a number, or `id_value` not a
/// string), or — on an int64 column — a JSON-number index value.
fn cursor_kind_row(
    id: &'static str,
    vty: FieldType,
    cursor_index_value: Value,
    created_at_value: Value,
    id_value: Value,
) -> PaginateSpecRow {
    PaginateSpecRow {
        id,
        fields: pag_fields(&[("s", FieldType::String), ("v", vty.clone())]),
        index_name: "by_s_v",
        index_fields: vec!["s", "v"],
        // int64 doc values take the decimal-string wire form.
        docs: vec![
            pag_doc(&[("s", json!("todo")), ("v", doc_v_value(&vty, 1))]),
            pag_doc(&[("s", json!("todo")), ("v", doc_v_value(&vty, 2))]),
        ],
        eq: vec![json!("todo")],
        order: PagOrder::Asc,
        num_items: 2,
        cursor: Some(vec![cursor_index_value, created_at_value, id_value]),
    }
}

/// A fixture doc value for the declared keyset type: int64 fields carry the
/// decimal-string wire form.
fn doc_v_value(ty: &FieldType, v: i64) -> Value {
    if pag_col_int64_ty(ty) {
        json!(v.to_string())
    } else {
        json!(v)
    }
}

/// Whether `ty` projects through the int64 decimal-string convention.
fn pag_col_int64_ty(ty: &FieldType) -> bool {
    match ty {
        FieldType::Int64 => true,
        FieldType::Optional { inner } => pag_col_int64_ty(inner),
        _ => false,
    }
}

/// A created_at/id cursor tail strictly before every doc (`[prefix, 0, ""]`)
/// or strictly past every doc's tiebreaker tail (`[prefix, 4e12, "zzzz"]`),
/// for one index-field prefix.
fn cursor_at(prefix: &[Value], past_end: bool) -> Vec<Value> {
    let created_at = if past_end {
        CREATED_AT_PAST_END
    } else {
        CREATED_AT_BEFORE_ALL
    };
    let doc_id = if past_end { ID_MAX } else { ID_MIN };
    let mut out = prefix.to_vec();
    out.push(json!(created_at));
    out.push(json!(doc_id));
    out
}

// ---------------------------------------------------------------------------
// The representative rows
// ---------------------------------------------------------------------------

/// Every wire-visible paginate rule the keyset walk distinguishes. Order is
/// the committed fixture's row order.
#[allow(clippy::vec_init_then_push)] // the function IS a rows-vector builder
fn paginate_spec_rows() -> Vec<PaginateSpecRow> {
    let mut rows: Vec<PaginateSpecRow> = Vec::new();

    // -- First page, asc: sorted by the keyset; a next cursor is minted while
    //    rows remain ----------------------------------------------------------
    rows.push(pag_row(
        "first-page-asc",
        FieldType::Number,
        &[
            ("todo", json!(4)),
            ("todo", json!(2)),
            ("todo", json!(5)),
            ("todo", json!(1)),
            ("todo", json!(3)),
        ],
        "todo",
        PagOrder::Asc,
        2,
        None,
    ));

    // -- Resume walks: the OR-of-AND predicate's prefix-equal branch (the
    //    boundary row's own tiebreaker tail re-serves it) and the strict
    //    past-end branch (the boundary group is skipped entirely) --------------
    rows.push(pag_row(
        "cursor-prefix-equal-resumes-by-tiebreakers",
        FieldType::Number,
        &[
            ("todo", json!(4)),
            ("todo", json!(2)),
            ("todo", json!(5)),
            ("todo", json!(1)),
            ("todo", json!(3)),
        ],
        "todo",
        PagOrder::Asc,
        2,
        Some(cursor_at(&[json!(2)], false)),
    ));
    rows.push(pag_row(
        "cursor-past-end-skips-boundary-group",
        FieldType::Number,
        &[
            ("todo", json!(4)),
            ("todo", json!(2)),
            ("todo", json!(5)),
            ("todo", json!(1)),
            ("todo", json!(3)),
        ],
        "todo",
        PagOrder::Asc,
        2,
        Some(cursor_at(&[json!(2)], true)),
    ));

    // -- Asc: nulls sort LAST (Postgres `ORDER BY v` default), and an asc
    //    cursor at the last non-null value yields exactly the null tail ------
    rows.push(pag_row(
        "asc-nulls-last",
        pag_opt(FieldType::Number),
        &[
            ("todo", json!(2)),
            ("todo", Value::Null),
            ("todo", json!(1)),
            ("todo", Value::Null),
            ("todo", json!(3)),
        ],
        "todo",
        PagOrder::Asc,
        4,
        None,
    ));
    rows.push(pag_row(
        "asc-cursor-yields-null-tail",
        pag_opt(FieldType::Number),
        &[
            ("todo", json!(2)),
            ("todo", Value::Null),
            ("todo", json!(1)),
            ("todo", Value::Null),
            ("todo", json!(3)),
        ],
        "todo",
        PagOrder::Asc,
        4,
        Some(cursor_at(&[json!(3)], true)),
    ));

    // -- Desc: nulls sort FIRST (Postgres `ORDER BY v DESC` default), the
    //    page leads with the null rows, ties within the equal prefix order
    //    created_at DESC, and a desc cursor at the boundary value resumes
    //    strictly below it ------------------------------------------------------
    rows.push(pag_row(
        "desc-nulls-first-and-tiebreakers",
        pag_opt(FieldType::Number),
        &[
            ("todo", json!(2)),
            ("todo", Value::Null),
            ("todo", json!(1)),
            ("todo", Value::Null),
            ("todo", json!(3)),
        ],
        "todo",
        PagOrder::Desc,
        4,
        None,
    ));
    rows.push(pag_row(
        "desc-cursor-resumes-strictly-below",
        FieldType::Number,
        &[
            ("todo", json!(1)),
            ("todo", json!(4)),
            ("todo", json!(3)),
            ("todo", json!(2)),
        ],
        "todo",
        PagOrder::Desc,
        2,
        Some(cursor_at(&[json!(3)], false)),
    ));

    // -- Exhausted / boundary pages: a cursor past the last row pages empty
    //    with no next cursor, an exact-fit page carries no cursor, and
    //    numItems 0 pages empty WITHOUT a cursor (the server truncates the
    //    LIMIT n+1 fetch before minting, so an empty page never mints) --------
    rows.push(pag_row(
        "exhausted-cursor-pages-empty",
        FieldType::Number,
        &[("todo", json!(1)), ("todo", json!(2)), ("todo", json!(3))],
        "todo",
        PagOrder::Asc,
        2,
        Some(cursor_at(&[json!(3)], true)),
    ));
    rows.push(pag_row(
        "exact-fit-page-no-cursor",
        FieldType::Number,
        &[("todo", json!(1)), ("todo", json!(2)), ("todo", json!(3))],
        "todo",
        PagOrder::Asc,
        3,
        None,
    ));
    rows.push(pag_row(
        "num-items-zero-empty-page",
        FieldType::Number,
        &[("todo", json!(1)), ("todo", json!(2))],
        "todo",
        PagOrder::Asc,
        0,
        None,
    ));

    // -- int64: decimal strings sort NUMERICALLY (BigInt parse), not
    //    lexicographically — ascending, plus a mid-walk cursor whose boundary
    //    sits between two values no lexicographic compare could place --------
    rows.push(pag_row(
        "int64-sorts-numerically-asc",
        FieldType::Int64,
        &[
            ("todo", json!("9007199254740992")),
            ("todo", json!("10000000000000000")),
            ("todo", json!("2")),
        ],
        "todo",
        PagOrder::Asc,
        3,
        None,
    ));
    rows.push(pag_row(
        "int64-cursor-mid-numeric",
        FieldType::Int64,
        &[
            ("todo", json!("9007199254740992")),
            ("todo", json!("10000000000000000")),
            ("todo", json!("2")),
        ],
        "todo",
        PagOrder::Asc,
        3,
        Some(cursor_at(&[json!("9500000000000000")], true)),
    ));

    // -- Booleans order false < true; the two `true` docs tie on the prefix
    //    and resolve by insertion order (the created_at tiebreaker) -----------
    rows.push(pag_row(
        "boolean-false-before-true",
        FieldType::Boolean,
        &[
            ("todo", json!(true)),
            ("todo", json!(false)),
            ("todo", json!(true)),
        ],
        "todo",
        PagOrder::Asc,
        2,
        None,
    ));

    // -- Multi-column keysets: index [s, v, w] with a one-value eq prefix
    //    spans [v, w, created_at, id]; first page + both walk directions -----
    let multi_docs = pag_docs_swv(&[(1, "b"), (1, "a"), (2, "b"), (2, "a")]);
    rows.push(PaginateSpecRow {
        id: "multi-column-first-page-asc",
        fields: pag_fields_swv(),
        index_name: "by_s_v_w",
        index_fields: vec!["s", "v", "w"],
        docs: multi_docs.clone(),
        eq: vec![json!("todo")],
        order: PagOrder::Asc,
        num_items: 2,
        cursor: None,
    });
    rows.push(PaginateSpecRow {
        id: "multi-column-cursor-prefix-equal",
        fields: pag_fields_swv(),
        index_name: "by_s_v_w",
        index_fields: vec!["s", "v", "w"],
        docs: multi_docs.clone(),
        eq: vec![json!("todo")],
        order: PagOrder::Asc,
        num_items: 3,
        cursor: Some(cursor_at(&[json!(1), json!("a")], false)),
    });
    rows.push(PaginateSpecRow {
        id: "multi-column-cursor-past-end-desc",
        fields: pag_fields_swv(),
        index_name: "by_s_v_w",
        index_fields: vec!["s", "v", "w"],
        docs: multi_docs,
        eq: vec![json!("todo")],
        order: PagOrder::Desc,
        num_items: 2,
        cursor: Some(cursor_at(&[json!(2), json!("a")], true)),
    });

    // -- A longer eq prefix shrinks the keyset: eq on [s, v] leaves
    //    [w, created_at, id], so the cursor carries three values --------------
    rows.push(PaginateSpecRow {
        id: "eq-prefix-shrinks-keyset",
        fields: pag_fields_swv(),
        index_name: "by_s_v_w",
        index_fields: vec!["s", "v", "w"],
        docs: pag_docs_swv(&[(1, "b"), (1, "a")]),
        eq: vec![json!("todo"), json!(1)],
        order: PagOrder::Asc,
        num_items: 1,
        cursor: Some(cursor_at(&[json!("a")], false)),
    });

    // -- Cursor rejections: wrong arity, wrong created_at kind, wrong id kind,
    //    and a JSON-number value on an int64 field (the typed bind wants the
    //    decimal string) --------------------------------------------------------
    rows.push(pag_row(
        "cursor-arity-mismatch-rejected",
        FieldType::Number,
        &[("todo", json!(1)), ("todo", json!(2))],
        "todo",
        PagOrder::Asc,
        2,
        Some(vec![json!(1)]),
    ));
    rows.push(cursor_kind_row(
        "cursor-created-at-kind-rejected",
        FieldType::Number,
        json!(1),
        json!("not-a-number"),
        json!(""),
    ));
    rows.push(cursor_kind_row(
        "cursor-id-kind-rejected",
        FieldType::Number,
        json!(1),
        json!(0),
        json!(42),
    ));
    rows.push(cursor_kind_row(
        "int64-cursor-number-value-rejected",
        FieldType::Int64,
        json!(9500000000000000u64),
        json!(0),
        json!(""),
    ));

    // -- Null cursor values over a NULL sort group: the null-safe resume
    //    predicate (prefix `IS NULL`, asc boundary FALSE — nothing sorts
    //    after NULLS LAST; desc boundary IS NOT NULL — every non-null row
    //    sorts after NULLS FIRST). The engine comparators already place nulls
    //    this way, so the server's typed bind was the only side rejecting its
    //    own minted cursors past a NULL group ------------------------------
    rows.push(pag_row(
        "cursor-null-boundary-desc",
        pag_opt(FieldType::Number),
        &[
            ("todo", json!(1)),
            ("todo", json!(2)),
            ("todo", json!(null)),
        ],
        "todo",
        PagOrder::Desc,
        1,
        Some(cursor_at(&[json!(null)], true)),
    ));
    rows.push(pag_row(
        "cursor-null-boundary-asc",
        pag_opt(FieldType::Number),
        &[
            ("todo", json!(1)),
            ("todo", json!(2)),
            ("todo", json!(null)),
        ],
        "todo",
        PagOrder::Asc,
        1,
        Some(cursor_at(&[json!(null)], false)),
    ));
    rows.push(PaginateSpecRow {
        id: "multi-column-cursor-null-prefix",
        // v must be Optional: this row's doc set inserts an absent-v doc
        // (JSON null normalizes to a missing key on insert), which the
        // engines reject for a required field.
        fields: pag_fields(&[
            ("s", FieldType::String),
            ("v", pag_opt(FieldType::Number)),
            ("w", FieldType::String),
        ]),
        index_name: "by_s_v_w",
        index_fields: vec!["s", "v", "w"],
        docs: vec![
            pag_doc(&[("s", json!("todo")), ("w", json!("c"))]),
            pag_doc(&[("s", json!("todo")), ("v", json!(1)), ("w", json!("b"))]),
            pag_doc(&[("s", json!("todo")), ("v", json!(1)), ("w", json!("a"))]),
        ],
        eq: vec![json!("todo")],
        order: PagOrder::Desc,
        num_items: 2,
        cursor: Some(vec![
            json!(null),
            json!("a"),
            json!(CREATED_AT_PAST_END),
            json!(ID_MAX),
        ]),
    });

    // The drift net: every wire Order spelling and every keyset value kind
    // must appear in a row; a missing representative fails generation. A new
    // wire-visible paginate rule belongs here as a new representative row.
    let mut seen_orders: Vec<&str> = Vec::new();
    let mut seen_kinds: Vec<&str> = Vec::new();
    for r in &rows {
        seen_orders.push(r.order.wire());
        let ty = r.fields.get("v").unwrap_or(&FieldType::String);
        match ty {
            FieldType::Number => seen_kinds.push("number"),
            FieldType::Int64 => seen_kinds.push("int64"),
            FieldType::Boolean => seen_kinds.push("boolean"),
            FieldType::Optional { .. } => seen_kinds.push("optional"),
            FieldType::String => seen_kinds.push("string"),
            t => {
                let _ = t;
                seen_kinds.push("other");
            }
        }
    }
    for guard in [PagOrder::Asc, PagOrder::Desc] {
        assert!(
            seen_orders.contains(&guard.wire()),
            "no fixture row exercises order '{}'",
            guard.wire()
        );
    }
    for kind in ["number", "int64", "boolean", "optional"] {
        assert!(
            seen_kinds.contains(&kind),
            "no fixture row exercises value kind '{kind}'"
        );
    }

    rows
}

// ---------------------------------------------------------------------------
// The reference walk
// ---------------------------------------------------------------------------

/// A doc's value for one keyset column: index fields read off the doc with an
/// absent optional key normalized to JSON null, the created_at tiebreaker the
/// doc's seed ordinal, the id tiebreaker the engine-neutral representative.
fn pag_col_value(
    doc: &serde_json::Map<String, Value>,
    col: Col,
    unbound: &[&str],
    ordinal: usize,
) -> Value {
    match col {
        Col::Index(i) => doc.get(unbound[i]).cloned().unwrap_or(Value::Null),
        Col::CreatedAt => json!(ordinal as i64),
        Col::Id => json!(REF_DOC_ID),
    }
}

/// The shared column comparator, mirroring the engines'
/// `compareIndexValues` over the fixture's value shapes: null sorts Greater
/// than any value (the direction-aware consumers negate / test against the
/// direction), decimal strings on an int64 column parse to i128, booleans
/// order false < true, equal kinds compare natively. Total: a value-shape mix
/// the fixture never constructs compares by serialization.
fn pag_compare(a: &Value, b: &Value, int64: bool) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let (a_null, b_null) = (a.is_null(), b.is_null());
    if a_null || b_null {
        return match (a_null, b_null) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            _ => Ordering::Equal,
        };
    }
    if a == b {
        return Ordering::Equal;
    }
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            let (x, y) = (
                x.as_f64().unwrap_or(f64::NAN),
                y.as_f64().unwrap_or(f64::NAN),
            );
            x.partial_cmp(&y).unwrap_or(Ordering::Equal)
        }
        (Value::String(x), Value::String(y)) => {
            if int64 {
                match (x.parse::<i128>(), y.parse::<i128>()) {
                    (Ok(x), Ok(y)) => x.cmp(&y),
                    _ => x.cmp(y),
                }
            } else {
                x.cmp(y)
            }
        }
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        _ => {
            let (sx, sy) = (
                serde_json::to_string(a).unwrap_or_default(),
                serde_json::to_string(b).unwrap_or_default(),
            );
            sx.cmp(&sy)
        }
    }
}

/// Whether one unbound column's declared type projects through the int64
/// decimal-string convention (same shape as `query::is_int64_index_field`).
fn pag_col_int64(row: &PaginateSpecRow, field: &str) -> bool {
    fn deep(ty: &FieldType) -> bool {
        match ty {
            FieldType::Int64 => true,
            FieldType::Optional { inner } => deep(inner),
            _ => false,
        }
    }
    row.fields.get(field).is_some_and(deep)
}

/// The server-side reference walk, mirroring the wire-visible Postgres
/// semantics of the btree paginate terminal: the eq prefix filter, the
/// unbound-index + created_at + id sort with Postgres null placement (asc
/// NULLS LAST, desc NULLS FIRST — the comparator keeps null Greater and the
/// direction consumer flips it), the OR-of-AND resume predicate over decoded
/// cursor values, the LIMIT n+1 fetch with truncate-before-mint, and the next
/// cursor's keyset = the last page row's unbound-index values. The
/// tiebreakers are engine-neutral: a doc's created_at ordinal is its seed
/// position, its id the `REF_DOC_ID` representative, and every fixture
/// cursor tail sits at an engine-neutral extreme.
fn reference_paginate(row: &PaginateSpecRow) -> Result<Value, &'static str> {
    let asc = row.order.is_asc();
    let eq_len = row.eq.len();
    let unbound: Vec<&str> = row.index_fields[eq_len.min(row.index_fields.len())..].to_vec();
    let cols = unbound.len() + 2;

    // The typed cursor binds (server `SortCol::cursor_bind` + `eq_bind_for`):
    // the arity must match the keyset, created_at must be a JSON number, id a
    // JSON string, and an int64 index column a decimal STRING (a JSON number
    // rejects as bad_request). Null index cursor values ARE pinned: a null
    // carries its NULL sort group (prefix `IS NULL`, asc boundary FALSE, desc
    // boundary IS NOT NULL — mirroring the engines' null-safe comparator).
    // Only the tiebreaker kind checks stay kind-strict on nulls.
    let mut cursor: Option<Vec<Value>> = None;
    if let Some(raw) = &row.cursor {
        if raw.len() != cols {
            return Err("BAD_REQUEST");
        }
        for (i, v) in raw.iter().enumerate() {
            if let Col::Index(j) = cursor_col(i, unbound.len())
                && pag_col_int64(row, unbound[j])
                && v.is_number()
            {
                return Err("BAD_REQUEST");
            }
        }
        if !raw[unbound.len()].is_number() {
            return Err("BAD_REQUEST");
        }
        if !raw[unbound.len() + 1].is_string() {
            return Err("BAD_REQUEST");
        }
        cursor = Some(raw.clone());
    }

    // Seed-order ordinals stand in for the engine-minted stamps: doc i gets
    // ordinal i, strictly increasing, and every fixture cursor tail compares
    // identically against every engine's real ~1.7e12 stamps.
    let mut ordered: Vec<(usize, &serde_json::Map<String, Value>)> =
        row.docs.iter().enumerate().collect();
    let col_int64s: Vec<bool> = unbound.iter().map(|f| pag_col_int64(row, f)).collect();
    ordered.sort_by(|a, b| {
        for (i, _) in unbound.iter().enumerate() {
            let c = pag_compare(
                &pag_col_value(a.1, Col::Index(i), &unbound, a.0),
                &pag_col_value(b.1, Col::Index(i), &unbound, b.0),
                col_int64s[i],
            );
            if c != std::cmp::Ordering::Equal {
                return if asc { c } else { c.reverse() };
            }
        }
        // The created_at tiebreaker: seed order asc, reverse-insertion desc
        // (ids never participate — ordinals are distinct).
        if asc { a.0.cmp(&b.0) } else { b.0.cmp(&a.0) }
    });

    // The OR-of-AND resume predicate: strictly after the cursor in the sort
    // direction — some column strictly ahead with every earlier column equal.
    let after: Vec<(usize, &serde_json::Map<String, Value>)> = match &cursor {
        None => ordered,
        Some(cvals) => ordered
            .into_iter()
            .filter(|(ordinal, doc)| {
                for (i, cur_v) in cvals.iter().enumerate() {
                    let col = cursor_col(i, unbound.len());
                    let row_v = pag_col_value(doc, col, &unbound, *ordinal);
                    let int64 = matches!(col, Col::Index(j) if col_int64s[j]);
                    let c = pag_compare(&row_v, cur_v, int64);
                    let ahead = if asc {
                        c == std::cmp::Ordering::Greater
                    } else {
                        c == std::cmp::Ordering::Less
                    };
                    if ahead {
                        return true;
                    }
                    if c != std::cmp::Ordering::Equal {
                        return false;
                    }
                }
                false
            })
            .collect(),
    };

    // LIMIT n+1 with truncate-before-mint: an empty page never mints a cursor.
    let num = row.num_items as usize;
    let has_next = after.len() > num;
    let mut expect = serde_json::Map::new();
    expect.insert(
        "docs".to_string(),
        Value::Array(
            after
                .iter()
                .take(num)
                .map(|(_, d)| Value::Object((*d).clone()))
                .collect(),
        ),
    );
    if has_next
        && let Some((last_ordinal, last_doc)) = num.checked_sub(1).and_then(|idx| after.get(idx))
    {
        let prefix: Vec<Value> = unbound
            .iter()
            .enumerate()
            .map(|(i, _)| pag_col_value(last_doc, Col::Index(i), &unbound, *last_ordinal))
            .collect();
        expect.insert("nextCursorPrefix".to_string(), Value::Array(prefix));
    }
    Ok(Value::Object(expect))
}

/// The column position for one cursor slot: the first `unbound` slots are
/// index columns, the last two the tiebreakers.
fn cursor_col(slot: usize, unbound_len: usize) -> Col {
    if slot < unbound_len {
        Col::Index(slot)
    } else if slot == unbound_len {
        Col::CreatedAt
    } else {
        Col::Id
    }
}

// ---------------------------------------------------------------------------
// Fixture assembly
// ---------------------------------------------------------------------------

/// One fixture row per representative: the wire schema fragment (`fields` +
/// one btree `index`), the seed `docs`, the eq prefix, the wire `order` /
/// `paginate` spec (cursor values RAW), and the expected page envelope.
fn paginate_spec_fixture_rows() -> Result<Vec<Value>, serde_json::Error> {
    let mut rows = Vec::new();
    for row in paginate_spec_rows() {
        let index = json!({
            "name": row.index_name,
            "fields": row.index_fields,
        });
        let mut paginate = serde_json::Map::new();
        paginate.insert("numItems".to_string(), json!(row.num_items));
        if let Some(cursor) = &row.cursor {
            paginate.insert("cursor".to_string(), json!(cursor));
        }
        let expect = match reference_paginate(&row) {
            Ok(value) => value,
            Err(code) => json!({ "error": { "code": code } }),
        };
        rows.push(json!({
            "id": row.id,
            "fields": serde_json::to_value(&row.fields)?,
            "index": index,
            "docs": row.docs,
            "eq": row.eq,
            "order": row.order.wire(),
            "paginate": Value::Object(paginate),
            "expect": expect,
        }));
    }
    Ok(rows)
}

/// Render the committed fixture's exact bytes (pretty JSON + trailing
/// newline).
pub fn render_pagination_spec() -> Result<String, serde_json::Error> {
    let doc = json!({
        "$comment": "SPEC-TABLE CORPUS (ENH-052 family 4) — GENERATED, do not edit. \
                     The btree keyset-pagination decision table, computed from the \
                     server's reference walk of the SQL that \
                     query/terminals.rs::compile_paginate_terminal emits (see \
                     server/src/spec_corpus_pagination.rs) by \
                     'make gen-spec-pagination'. The wire-visible Postgres rules \
                     pinned per row: the sort spans the unbound index columns after \
                     the eq prefix then created_at/id, asc NULLS LAST / desc NULLS \
                     FIRST, the OR-of-AND resume predicate (a prefix-equal cursor \
                     re-serves its boundary row via the tiebreaker tail; a \
                     past-end tail skips the whole boundary group), ties within an \
                     equal prefix resolve by insertion order (created_at desc in \
                     desc order), int64 columns sort numerically through decimal \
                     strings past 2^53, booleans order false < true, LIMIT n+1 \
                     truncates before minting (numItems 0 pages empty with NO next \
                     cursor), and cursors reject on wrong arity or wrong value \
                     kind (created_at must be a number, id a string, an int64 \
                     column a decimal string) with BAD_REQUEST. Cursor values are \
                     carried RAW and each runner encodes them through its own \
                     codec; tiebreaker extremes (created_at 0 / 4e12, id '' / \
                     'zzzz') keep every expected page engine-neutral. Each runner \
                     pushes the row's schema fragment, inserts the docs, and runs \
                     the paginate query through its own engine, asserting the page \
                     envelope where nextCursorPrefix, when present, is the \
                     unbound-index prefix of the decoded minted cursor \
                     (`expect.error` rows assert the failure with its code). \
                     Cursors carrying a JSON-null INDEX value are pinned: the \
                     null-safe resume predicate binds nothing and emits prefix \
                     `IS NULL`, asc boundary FALSE (nothing sorts after \
                     NULLS LAST), desc boundary IS NOT NULL (every non-null \
                     row sorts after NULLS FIRST), mirroring the engines' \
                     null-safe comparator; the created_at/id tiebreaker kind \
                     checks stay strict on nulls. Deliberately excluded: \
                     cursors over the ranked terminals (search/vectorSearch rank keys are \
                     engine-minted stand-ins, covered by the semantics corpus), \
                     the numItems > 4096 clamp (a fixture would need 4097 docs), \
                     and malformed-base64 cursors (semantics corpus \
                     `error-paginate-malformed-cursor`). A semantics change \
                     regenerates this file in the same commit — see \
                     wire-corpus/README.md 'Spec-table corpus'.",
        "rows": paginate_spec_fixture_rows()?,
    });
    let mut out = serde_json::to_string_pretty(&doc)?;
    out.push('\n');
    Ok(out)
}
