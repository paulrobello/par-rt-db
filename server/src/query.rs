use std::collections::BTreeMap;

use sqlx::PgPool;

use crate::db::validate_db_name;
use crate::ddl::{pg_col, pg_schema, pg_search_col, pg_table, pg_vector_col};
use crate::error::RtDbError;
use crate::pagination::{decode_cursor, encode_cursor};
use crate::schema::{FieldType, IndexDef, SchemaDef, TableDef};
use crate::txn::{EqBind, eq_bind_for, eq_binds};

/// Hard cap on rows returned by a single query, whether via an explicit
/// `take` or a `take`-less collect.
const MAX_TAKE: u32 = 4096;

/// Hard cap on `vectorSearch` `limit`.
const VECTOR_SEARCH_MAX_LIMIT: u32 = 256;

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Order {
    Asc,
    Desc,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Query {
    pub table: String,
    #[serde(default)]
    pub get: Option<String>, // point read by id; excludes all below
    #[serde(default)]
    pub index: Option<String>,
    #[serde(default)]
    pub eq: Vec<serde_json::Value>, // prefix binds on index fields
    #[serde(default)]
    pub gt: Option<serde_json::Value>, // exclusive lower bound on the index field after the eq prefix
    #[serde(default)]
    pub gte: Option<serde_json::Value>, // inclusive lower bound; mutually exclusive with gt
    #[serde(default)]
    pub lt: Option<serde_json::Value>, // exclusive upper bound on the index field after the eq prefix
    #[serde(default)]
    pub lte: Option<serde_json::Value>, // inclusive upper bound; mutually exclusive with lt
    #[serde(default)]
    pub order: Option<Order>, // default Asc
    #[serde(default)]
    pub take: Option<u32>, // cap 4096; absent => collect (cap 4096)
    #[serde(default)]
    pub unique: bool, // with unique, take/order must be absent
    #[serde(default)]
    pub first: bool, // sugar over take(1); returns Doc(Some) or Doc(None); mutually exclusive with take/unique
    #[serde(default)]
    pub count: bool, // terminal: SELECT COUNT(*) over the same eq/range WHERE; mutually exclusive with get/take/unique/first/order
    #[serde(default)]
    pub paginate: Option<Paginate>,
    #[serde(default)]
    pub filter: Option<FilterExpr>, // additional WHERE predicate over doc fields; composes with index/order/take/cursor
    #[serde(default)]
    pub search: Option<SearchQuery>, // full-text search terminal: ranks by ts_rank over a search index's tsvector; composes with take
    #[serde(default, rename = "vectorSearch")]
    pub vector_search: Option<VectorSearchQuery>, // vector-similarity terminal: ranks by cosine distance over a vector index; carries its own limit
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Paginate {
    pub cursor: Option<String>,
    pub num_items: u32,
}

/// A full-text search terminal over a declared search index. `index` names a
/// search index on the query's table; `query` is free-form user text matched
/// via `plainto_tsquery` so it can't inject tsquery syntax.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SearchQuery {
    pub index: String,
    pub query: String,
}

/// A vector-similarity terminal over a declared vector index. `vector` is the
/// caller-supplied query embedding (length must equal the index dimensions);
/// ranked by cosine distance (`<=>`) ascending. `filter` is an optional eq-map
/// over the index's declared `filterFields`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct VectorSearchQuery {
    pub index: String,
    pub vector: Vec<f32>,
    pub limit: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub filter: BTreeMap<String, serde_json::Value>,
}

/// A db-side predicate appended to a query's WHERE clause. Leaves compare one
/// declared field to a value (`in` to a non-empty list); `and`/`or` nest
/// arbitrarily. Compilation: an *indexed* field compares against its typed
/// column (value typed via the field's declared `FieldType`, exactly like `eq`);
/// any other *declared* field uses jsonb extraction (`doc->>'field'`, cast for
/// non-text value kinds). Field names are schema-validated identifiers, so they
/// are safe to emit inside a quoted column name or a jsonb string literal.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "op", rename_all = "lowercase", deny_unknown_fields)]
pub enum FilterExpr {
    Eq {
        field: String,
        value: serde_json::Value,
    },
    Neq {
        field: String,
        value: serde_json::Value,
    },
    Gt {
        field: String,
        value: serde_json::Value,
    },
    Gte {
        field: String,
        value: serde_json::Value,
    },
    Lt {
        field: String,
        value: serde_json::Value,
    },
    Lte {
        field: String,
        value: serde_json::Value,
    },
    In {
        field: String,
        values: Vec<serde_json::Value>,
    },
    And {
        exprs: Vec<FilterExpr>,
    },
    Or {
        exprs: Vec<FilterExpr>,
    },
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(untagged)]
pub enum QueryResult {
    Doc(Option<serde_json::Value>), // get / unique: doc or null
    Docs(Vec<serde_json::Value>),   // take / collect
    Count(i64),                     // count: total matching rows, uncapped by MAX_TAKE
    Paginated(PaginatedResult),     // paginate: page of docs + optional next cursor
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PaginatedResult {
    pub docs: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

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

/// A `Query` field that can participate in a combination-rejection rule. Each
/// variant wraps the "is this field set?" predicate for one field, so peer lists
/// read as data instead of being inlined as boolean chains.
#[derive(Copy, Clone)]
enum Peer {
    Get,
    Index,
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
    Order,
    Take,
    Unique,
    First,
    Count,
    Paginate,
    Filter,
    Search,
    VectorSearch,
}

impl Peer {
    fn is_set(self, q: &Query) -> bool {
        match self {
            Self::Get => q.get.is_some(),
            Self::Index => q.index.is_some(),
            Self::Eq => !q.eq.is_empty(),
            Self::Gt => q.gt.is_some(),
            Self::Gte => q.gte.is_some(),
            Self::Lt => q.lt.is_some(),
            Self::Lte => q.lte.is_some(),
            Self::Order => q.order.is_some(),
            Self::Take => q.take.is_some(),
            Self::Unique => q.unique,
            Self::First => q.first,
            Self::Count => q.count,
            Self::Paginate => q.paginate.is_some(),
            Self::Filter => q.filter.is_some(),
            Self::Search => q.search.is_some(),
            Self::VectorSearch => q.vector_search.is_some(),
        }
    }
}

/// A peer that conflicts with a terminal, plus the message to emit when it's
/// set. Per-peer entries let a terminal emit different messages for different
/// peers (matching the pre-refactor cascade's per-peer messages for
/// `first`/`count`/`paginate`).
struct Incompatible {
    peer: Peer,
    message: &'static str,
}

/// Aggregate-message helper: if ANY of `peers` is set on `q`, return
/// `BadRequest(message)`. Used by terminals whose pre-refactor check emitted a
/// single message regardless of which peer was set (`get`, `unique`,
/// `vector_search`, `search`).
fn reject_if_any_set(q: &Query, peers: &[Peer], message: &str) -> Result<(), RtDbError> {
    if peers.iter().any(|p| p.is_set(q)) {
        return Err(RtDbError::bad_request(message));
    }
    Ok(())
}

/// Per-peer helper: for each entry in `entries` (in declaration order), if its
/// peer is set on `q`, return `BadRequest(entry.message)`. Used by terminals
/// whose pre-refactor check emitted peer-specific messages (`first`, `count`,
/// `paginate`). Declaration order preserves the original cascade's
/// first-match-wins ordering, so the same combination produces the same message.
fn reject_per_peer_set(q: &Query, entries: &[Incompatible]) -> Result<(), RtDbError> {
    for entry in entries {
        if entry.peer.is_set(q) {
            return Err(RtDbError::bad_request(entry.message));
        }
    }
    Ok(())
}

// Per-terminal incompatible-peer tables. Order within each table mirrors the
// pre-refactor cascade's check order.

const GET_PEERS: &[Peer] = &[
    Peer::Index,
    Peer::Eq,
    Peer::Gt,
    Peer::Gte,
    Peer::Lt,
    Peer::Lte,
    Peer::Order,
    Peer::Take,
    Peer::Unique,
    Peer::First,
    Peer::Count,
    Peer::Paginate,
    Peer::Filter,
    Peer::Search,
    Peer::VectorSearch,
];
const GET_MESSAGE: &str = "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, paginate, filter, search, or vector search";

const UNIQUE_PEERS: &[Peer] = &[Peer::Take, Peer::Order];
const UNIQUE_MESSAGE: &str = "unique cannot be combined with take or order";

const FIRST_INCOMPATIBLES: &[Incompatible] = &[
    Incompatible {
        peer: Peer::Unique,
        message: "first cannot be combined with unique",
    },
    Incompatible {
        peer: Peer::Take,
        message: "first cannot be combined with take",
    },
];

const COUNT_INCOMPATIBLES: &[Incompatible] = &[
    Incompatible {
        peer: Peer::Unique,
        message: "count cannot be combined with unique",
    },
    Incompatible {
        peer: Peer::Take,
        message: "count cannot be combined with take",
    },
    Incompatible {
        peer: Peer::First,
        message: "count cannot be combined with first",
    },
    Incompatible {
        peer: Peer::Order,
        message: "count cannot be combined with order",
    },
];

const PAGINATE_INCOMPATIBLES: &[Incompatible] = &[
    Incompatible {
        peer: Peer::Get,
        message: "paginate cannot be combined with get",
    },
    Incompatible {
        peer: Peer::Count,
        message: "paginate cannot be combined with count",
    },
    Incompatible {
        peer: Peer::Unique,
        message: "paginate cannot be combined with unique",
    },
    Incompatible {
        peer: Peer::First,
        message: "paginate cannot be combined with first",
    },
    Incompatible {
        peer: Peer::Take,
        message: "paginate cannot be combined with take",
    },
];

const VECTOR_SEARCH_PEERS: &[Peer] = &[
    Peer::Index,
    Peer::Eq,
    Peer::Gt,
    Peer::Gte,
    Peer::Lt,
    Peer::Lte,
    Peer::Order,
    Peer::Unique,
    Peer::First,
    Peer::Count,
    Peer::Paginate,
    Peer::Filter,
    Peer::Search,
    Peer::Take,
];
const VECTOR_SEARCH_MESSAGE: &str = "vectorSearch cannot be combined with any other terminal";

const SEARCH_PEERS: &[Peer] = &[
    Peer::Index,
    Peer::Eq,
    Peer::Gt,
    Peer::Gte,
    Peer::Lt,
    Peer::Lte,
    Peer::Order,
    Peer::Unique,
    Peer::First,
    Peer::Count,
    Peer::Paginate,
    Peer::Filter,
    Peer::VectorSearch,
];
const SEARCH_MESSAGE: &str = "search cannot be combined with index, eq, range bounds, order, unique, first, count, paginate, filter, or vector search";

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
/// Unknown table -> NotFound; unknown index / eq too long / get+query mix / unique+take /
/// first+take / first+unique / count+take / count+unique / count+first / count+order -> BadRequest.
/// `take: 0` is valid and returns an empty `Docs([])`, not an error.
/// `unique` without an `index` scans the whole table (LIMIT 2) and applies the same 0/1/>1 rule.
pub async fn execute_query(
    pool: &PgPool,
    db: &str,
    schema: &SchemaDef,
    q: &Query,
    owner: Option<&str>,
) -> Result<QueryResult, RtDbError> {
    validate_db_name(db)?;
    let table_def = schema.table(&q.table)?;
    let owner_field = table_def.owner_field.as_deref();

    if let Some(id) = &q.get {
        reject_if_any_set(q, GET_PEERS, GET_MESSAGE)?;
        return point_read(pool, db, &q.table, id, owner_field, owner).await;
    }

    if q.unique {
        reject_if_any_set(q, UNIQUE_PEERS, UNIQUE_MESSAGE)?;
    }

    if q.first {
        reject_per_peer_set(q, FIRST_INCOMPATIBLES)?;
    }

    if q.count {
        reject_per_peer_set(q, COUNT_INCOMPATIBLES)?;
    }

    if q.paginate.is_some() {
        reject_per_peer_set(q, PAGINATE_INCOMPATIBLES)?;
    }

    if q.gt.is_some() && q.gte.is_some() {
        return Err(RtDbError::bad_request("gt and gte cannot both be set"));
    }
    if q.lt.is_some() && q.lte.is_some() {
        return Err(RtDbError::bad_request("lt and lte cannot both be set"));
    }

    if let Some(take) = q.take
        && take > MAX_TAKE
    {
        return Err(RtDbError::bad_request(format!(
            "take exceeds maximum of {MAX_TAKE}"
        )));
    }

    // Vector-similarity terminal. Incompatible with every other terminal; it
    // carries its own `limit` and does not compose with `take` (or anything
    // else). Resolution and bind construction live in `execute_vector_search`.
    if let Some(vs) = &q.vector_search {
        reject_if_any_set(q, VECTOR_SEARCH_PEERS, VECTOR_SEARCH_MESSAGE)?;
        return execute_vector_search(pool, db, table_def, &q.table, vs, owner_field, owner).await;
    }

    // Full-text search terminal. It ranks over a search index's tsvector and is
    // incompatible with every index-based terminal; `take` (already capped) is
    // the only field it composes with.
    if let Some(search) = &q.search {
        reject_if_any_set(q, SEARCH_PEERS, SEARCH_MESSAGE)?;
        return execute_search(
            pool,
            db,
            table_def,
            &q.table,
            search,
            q.take,
            owner_field,
            owner,
        )
        .await;
    }

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
    // caller is an authenticated user and the table declares an `ownerField`,
    // `owner_filter` wraps the client filter with a server-side equality
    // predicate so the user sees only their own rows; bypass callers (`None`)
    // and tables without an `ownerField` get the original filter back unchanged.
    let effective_filter = owner_filter(q.filter.as_ref(), owner_field, owner);
    let filter_binds: Vec<EqBind> = match &effective_filter {
        Some(filter) => {
            let (fragment, binds) =
                compile_filter(filter, table_def, eq_len + range_binds.len() + 1)?;
            where_conditions.push(fragment);
            binds
        }
        None => Vec::new(),
    };
    let limit_placeholder = eq_len + range_binds.len() + filter_binds.len() + 1;

    if q.count {
        let pg_schema_name = pg_schema(db);
        let table_ident = pg_table(&q.table);
        let mut sql = format!("SELECT COUNT(*) FROM \"{pg_schema_name}\".\"{table_ident}\"");
        if !where_conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_conditions.join(" AND "));
        }
        let mut query = sqlx::query_scalar::<_, i64>(&sql);
        for bind in binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
            };
        }
        for bind in range_binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
            };
        }
        for bind in &filter_binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
            };
        }
        let count = query.fetch_one(pool).await?;
        return Ok(QueryResult::Count(count));
    }

    let mut sort_cols: Vec<String> = match index_def {
        Some(idx) => idx.fields[eq_len..]
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
        let table_ident = pg_table(&q.table);
        let mut sql = format!(
            "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\""
        );
        if !where_conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_conditions.join(" AND "));
        }
        sql.push_str(" ORDER BY ");
        sql.push_str(&order_by);
        sql.push_str(&format!(" LIMIT ${limit_placeholder}"));

        let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
        for bind in binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
            };
        }
        for bind in range_binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
            };
        }
        for bind in &filter_binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
            };
        }
        for bind in cursor_binds {
            query = match bind {
                EqBind::Text(v) => query.bind(v),
                EqBind::Num(v) => query.bind(v),
                EqBind::Bool(v) => query.bind(v),
            };
        }
        // Fetch one extra row so a next page can be detected without a second
        // round-trip; the extra is discarded after the has-next check.
        query = query.bind(i64::from(num_items) + 1);
        let mut rows = query.fetch_all(pool).await?;

        let has_next = rows.len() > num_items as usize;
        if has_next {
            rows.pop();
        }

        // The next cursor is built from the last row of the page (after the
        // extra is discarded); absent when the page is empty or last.
        let next_cursor =
            if has_next && let Some((last_id, last_doc, last_created_at, _)) = rows.last() {
                let mut cursor_values: Vec<serde_json::Value> = Vec::new();
                if let Some(idx) = index_def {
                    for fname in &idx.fields[eq_len..] {
                        let val = last_doc.get(fname).ok_or_else(|| {
                            RtDbError::internal(format!(
                                "stored doc is missing indexed field '{fname}'"
                            ))
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
        return Ok(QueryResult::Paginated(PaginatedResult {
            docs,
            next_cursor,
        }));
    }

    let limit: u32 = if q.unique {
        2
    } else if q.first {
        1
    } else {
        q.take.unwrap_or(MAX_TAKE)
    };

    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(&q.table);
    let mut sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\""
    );
    if !where_conditions.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(&where_conditions.join(" AND "));
    }
    sql.push_str(" ORDER BY ");
    sql.push_str(&order_by);
    sql.push_str(&format!(" LIMIT ${limit_placeholder}"));

    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
    for bind in binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
        };
    }
    for bind in range_binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
        };
    }
    for bind in &filter_binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
        };
    }
    query = query.bind(i64::from(limit));
    let mut rows = query.fetch_all(pool).await?;

    if q.unique {
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

    if q.first {
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

/// Pushes `bind` onto `binds` and returns its 1-based SQL placeholder (`$N`),
/// where `N = start_pos + binds.len()` evaluated BEFORE the push. Every
/// placeholder emission in `compile_filter`/`compile_filter_node` routes through
/// here so the offset arithmetic has one source of truth instead of being
/// inlined (with the "compute pos, then push" ordering) across each leaf.
fn push_filter_bind(start_pos: usize, binds: &mut Vec<EqBind>, bind: EqBind) -> String {
    let placeholder = format!("${}", start_pos + binds.len());
    binds.push(bind);
    placeholder
}

/// Compiles a `filter` into a fully-parenthesized SQL predicate plus its typed
/// binds, with `$n` placeholders numbered from 1-based `start_pos`. Every leaf
/// emits at least one bind, so the fragment is never empty.
fn compile_filter(
    filter: &FilterExpr,
    table: &TableDef,
    start_pos: usize,
) -> Result<(String, Vec<EqBind>), RtDbError> {
    let mut binds: Vec<EqBind> = Vec::new();
    let sql = compile_filter_node(filter, table, start_pos, &mut binds)?;
    Ok((sql, binds))
}

fn compile_filter_node(
    node: &FilterExpr,
    table: &TableDef,
    start_pos: usize,
    binds: &mut Vec<EqBind>,
) -> Result<String, RtDbError> {
    match node {
        FilterExpr::And { exprs } | FilterExpr::Or { exprs } => {
            if exprs.is_empty() {
                return Err(RtDbError::bad_request(format!(
                    "{} filter requires at least one expr",
                    if matches!(node, FilterExpr::And { .. }) {
                        "and"
                    } else {
                        "or"
                    }
                )));
            }
            let joiner = if matches!(node, FilterExpr::And { .. }) {
                " AND "
            } else {
                " OR "
            };
            let mut parts: Vec<String> = Vec::with_capacity(exprs.len());
            for expr in exprs {
                parts.push(compile_filter_node(expr, table, start_pos, binds)?);
            }
            Ok(format!("({})", parts.join(joiner)))
        }
        FilterExpr::Eq { field, value } => {
            compile_comparison(field, "=", value, table, start_pos, binds)
        }
        FilterExpr::Neq { field, value } => {
            compile_comparison(field, "<>", value, table, start_pos, binds)
        }
        FilterExpr::Gt { field, value } => {
            compile_comparison(field, ">", value, table, start_pos, binds)
        }
        FilterExpr::Gte { field, value } => {
            compile_comparison(field, ">=", value, table, start_pos, binds)
        }
        FilterExpr::Lt { field, value } => {
            compile_comparison(field, "<", value, table, start_pos, binds)
        }
        FilterExpr::Lte { field, value } => {
            compile_comparison(field, "<=", value, table, start_pos, binds)
        }
        FilterExpr::In { field, values } => {
            if values.is_empty() {
                return Err(RtDbError::bad_request(
                    "in filter requires at least one value",
                ));
            }
            let (lhs, first_bind) = field_lhs_and_bind(field, &values[0], table)?;
            let mut placeholders: Vec<String> =
                vec![push_filter_bind(start_pos, binds, first_bind)];
            for value in &values[1..] {
                let (this_lhs, bind) = field_lhs_and_bind(field, value, table)?;
                if this_lhs != lhs {
                    return Err(RtDbError::bad_request(
                        "in filter values must all be the same type",
                    ));
                }
                placeholders.push(push_filter_bind(start_pos, binds, bind));
            }
            Ok(format!("{lhs} IN ({})", placeholders.join(", ")))
        }
    }
}

/// Compiles a binary comparison leaf into `lhs OP $pos` and pushes one typed bind.
fn compile_comparison(
    field: &str,
    op: &str,
    value: &serde_json::Value,
    table: &TableDef,
    start_pos: usize,
    binds: &mut Vec<EqBind>,
) -> Result<String, RtDbError> {
    let (lhs, bind) = field_lhs_and_bind(field, value, table)?;
    let placeholder = push_filter_bind(start_pos, binds, bind);
    Ok(format!("{lhs} {op} {placeholder}"))
}

/// Resolves a filter field to its SQL left-hand side and types the comparison
/// value into a bind. Indexed fields compare against their typed column (value
/// typed via the field's declared `FieldType`, reusing the `eq` conversion);
/// other declared fields fall back to jsonb extraction with a value-kind cast.
fn field_lhs_and_bind(
    field: &str,
    value: &serde_json::Value,
    table: &TableDef,
) -> Result<(String, EqBind), RtDbError> {
    let field_type = table.fields.get(field).ok_or_else(|| {
        RtDbError::bad_request(format!("filter references unknown field '{field}'"))
    })?;
    let is_indexed = table
        .indexes
        .iter()
        .any(|idx| idx.fields.iter().any(|f| f == field));
    if is_indexed {
        Ok((
            format!("\"{}\"", pg_col(field)),
            eq_bind_for(field_type, value)?,
        ))
    } else {
        jsonb_lhs_and_bind(field, value)
    }
}

/// jsonb-extraction path for a declared-but-not-indexed field: compare
/// `doc->>'field'` directly for text, or cast to `float8`/`boolean` when the
/// value is a number/boolean. The field name is a schema-validated identifier,
/// so it is safe inside the jsonb string literal.
fn jsonb_lhs_and_bind(
    field: &str,
    value: &serde_json::Value,
) -> Result<(String, EqBind), RtDbError> {
    match value {
        serde_json::Value::String(s) => Ok((format!("(doc->>'{field}')"), EqBind::Text(s.clone()))),
        serde_json::Value::Number(n) => {
            let f = n.as_f64().ok_or_else(|| {
                RtDbError::bad_request("filter number value is out of representable range")
            })?;
            Ok((format!("(doc->>'{field}')::float8"), EqBind::Num(f)))
        }
        serde_json::Value::Bool(b) => Ok((format!("(doc->>'{field}')::boolean"), EqBind::Bool(*b))),
        _ => Err(RtDbError::bad_request(
            "filter value must be a string, number, or boolean",
        )),
    }
}

/// Full-text search terminal: matches a search index's generated tsvector
/// against `plainto_tsquery(<query text>)` and ranks by `ts_rank` descending,
/// with `(created_at, id)` tie-breakers. Composes with `take` (defaulting to
/// `MAX_TAKE`); the caller has already rejected every other terminal. The query
/// text is bound once via `$1` and reused in the `ORDER BY ts_rank`, so user
/// text can never inject tsquery syntax. Unknown index / empty query →
/// `BadRequest`, never a 500.
#[allow(clippy::too_many_arguments)]
async fn execute_search(
    pool: &PgPool,
    db: &str,
    table_def: &TableDef,
    table_name: &str,
    search: &SearchQuery,
    take: Option<u32>,
    owner_field: Option<&str>,
    owner: Option<&str>,
) -> Result<QueryResult, RtDbError> {
    if search.query.trim().is_empty() {
        return Err(RtDbError::bad_request(
            "search query text must not be empty",
        ));
    }
    let index_def = table_def
        .indexes
        .iter()
        .find(|index| index.name == search.index && index.search)
        .ok_or_else(|| {
            RtDbError::bad_request(format!("search index '{}' not found", search.index))
        })?;
    let sv_col = pg_search_col(&index_def.name);
    let limit = take.unwrap_or(MAX_TAKE);
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table_name);

    // Owner predicate: when the table declares an `ownerField` and the caller is
    // a user, restrict to rows whose `doc->>'<ownerField>'` equals the user id.
    // `owner_field` is `is_valid_identifier`-validated at schema push, so it is
    // safe to interpolate into the jsonb string-literal position; the user id is
    // bound via `$n`. The owner bind occupies `$2`, pushing `LIMIT` to `$3`.
    let owner_enforce: Option<(&str, &str)> = match (owner_field, owner) {
        (Some(f), Some(u)) => Some((f, u)),
        _ => None,
    };
    let (limit_ph, owner_clause) = match &owner_enforce {
        Some((field, _)) => (3, format!(" AND (doc->>'{field}') = $2")),
        None => (2, String::new()),
    };
    let sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" \
         WHERE \"{sv_col}\" @@ plainto_tsquery($1){owner_clause} \
         ORDER BY ts_rank(\"{sv_col}\", plainto_tsquery($1)) DESC, \"created_at\" DESC, \"id\" DESC \
         LIMIT ${limit_ph}"
    );
    let mut query =
        sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql).bind(&search.query);
    if let Some((_, uid)) = owner_enforce {
        query = query.bind(uid);
    }
    let rows = query.bind(i64::from(limit)).fetch_all(pool).await?;
    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Docs(docs))
}

/// Vector-similarity terminal: ranks rows by cosine distance (`<=>`) between
/// the index's `v_<index>` column and the query vector, ascending, limited to
/// `limit`. Optional `filter` eq-binds over the index's declared `filterFields`.
/// Unknown index / length mismatch / unknown filter key / out-of-range limit
/// → `BadRequest`. Bind order: filter eq-binds occupy `$1..$k`, then (when the
/// table is owner-gated and the caller is a user) the owner id occupies
/// `$(k+1)`, then the query vector (`$n::vector`), then `limit`.
async fn execute_vector_search(
    pool: &PgPool,
    db: &str,
    table_def: &TableDef,
    table_name: &str,
    vs: &VectorSearchQuery,
    owner_field: Option<&str>,
    owner: Option<&str>,
) -> Result<QueryResult, RtDbError> {
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

    // Build eq-binds for any filter entries; each key must be a declared
    // filterField of this index. The field's declared `FieldType` types the
    // value the same way `eq` prefixes are typed in `txn::eq_bind_for`.
    let mut filter_binds: Vec<EqBind> = Vec::new();
    let mut filter_cols: Vec<String> = Vec::new();
    for (key, value) in &vs.filter {
        if !vec_spec.filter_fields.iter().any(|f| f == key) {
            return Err(RtDbError::bad_request(format!(
                "vectorSearch filter key '{key}' is not a declared filterField of index '{}'",
                vs.index
            )));
        }
        let field_type = table_def.fields.get(key).ok_or_else(|| {
            RtDbError::internal(format!("filterField '{key}' missing from table fields"))
        })?;
        filter_binds.push(eq_bind_for(field_type, value)?);
        filter_cols.push(pg_col(key));
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

    // Bind numbering: filter eq-binds first ($1..$k), then the owner id bind
    // ($k+1) when owner-enforced, then the query vector (cast to `vector`),
    // then `limit`. The WHERE clause always excludes rows whose vector column
    // is NULL, so undimensioned rows never surface as bogus nearest-neighbors.
    // The owner predicate (`doc->>'<ownerField>'` = $n) mirrors the main query
    // path: `owner_field` is `is_valid_identifier`-validated at schema push, so
    // it is safe to interpolate into the jsonb string-literal position; the user
    // id is `$n`-bound, never interpolated.
    let owner_enforce: Option<(&str, &str)> = match (owner_field, owner) {
        (Some(f), Some(u)) => Some((f, u)),
        _ => None,
    };
    let mut bind_idx = 1usize;
    let mut filter_placeholders: Vec<String> = Vec::with_capacity(filter_cols.len());
    for _ in &filter_cols {
        filter_placeholders.push(format!("${bind_idx}"));
        bind_idx += 1;
    }
    let owner_ph: Option<usize> = if owner_enforce.is_some() {
        let ph = bind_idx;
        bind_idx += 1;
        Some(ph)
    } else {
        None
    };
    let qvec_ph = bind_idx;
    bind_idx += 1;
    let limit_ph = bind_idx;

    let mut where_clause = format!("\"{v_col}\" IS NOT NULL");
    if !filter_cols.is_empty() {
        let conds: Vec<String> = filter_cols
            .iter()
            .zip(filter_placeholders.iter())
            .map(|(col, ph)| format!("\"{col}\" = {ph}"))
            .collect();
        where_clause = format!("{where_clause} AND {}", conds.join(" AND "));
    }
    if let (Some(ph), Some((field, _))) = (owner_ph, owner_enforce) {
        where_clause.push_str(&format!(" AND (doc->>'{field}') = ${ph}"));
    }

    let sql = format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" \
         WHERE {where_clause} \
         ORDER BY \"{v_col}\" <=> ${qvec_ph}::vector \
         LIMIT ${limit_ph}"
    );

    let mut query = sqlx::query_as::<_, (String, serde_json::Value, i64, i64)>(&sql);
    for bind in filter_binds {
        query = match bind {
            EqBind::Text(v) => query.bind(v),
            EqBind::Num(v) => query.bind(v),
            EqBind::Bool(v) => query.bind(v),
        };
    }
    if let Some((_, uid)) = owner_enforce {
        query = query.bind(uid);
    }
    let rows = query
        .bind(qvec_text)
        .bind(i64::from(vs.limit))
        .fetch_all(pool)
        .await?;
    let docs = rows
        .into_iter()
        .map(|(id, doc, created_at, version)| merge_doc(id, doc, created_at, version))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(QueryResult::Docs(docs))
}

async fn point_read(
    pool: &PgPool,
    db: &str,
    table_name: &str,
    id: &str,
    owner_field: Option<&str>,
    owner: Option<&str>,
) -> Result<QueryResult, RtDbError> {
    let pg_schema_name = pg_schema(db);
    let table_ident = pg_table(table_name);
    let row: Option<(String, serde_json::Value, i64, i64)> = sqlx::query_as(&format!(
        "SELECT \"id\", \"doc\", \"created_at\", \"version\" FROM \"{pg_schema_name}\".\"{table_ident}\" WHERE \"id\" = $1"
    ))
    .bind(id)
    .fetch_optional(pool)
    .await?;

    match row {
        Some((id, doc, created_at, version)) => {
            // Per-row: a user may only point-read a doc they own. Silent
            // filter (Convex-like) — unowned docs read as absent.
            if let (Some(field), Some(uid)) = (owner_field, owner)
                && doc.get(field).and_then(|v| v.as_str()) != Some(uid)
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

/// Wraps the client-supplied `filter` with the owner equality predicate when
/// the table declares an `ownerField` and the caller is a user (`owner`).
/// Bypass callers (`None`) and tables without an `ownerField` get the original
/// filter back unchanged — no enforcement. The owner value is `$n`-bound by
/// `compile_filter`, never interpolated into SQL.
fn owner_filter(
    client_filter: Option<&FilterExpr>,
    owner_field: Option<&str>,
    owner: Option<&str>,
) -> Option<FilterExpr> {
    match (client_filter, owner_field, owner) {
        (Some(f), Some(field), Some(uid)) => Some(FilterExpr::And {
            exprs: vec![
                f.clone(),
                FilterExpr::Eq {
                    field: field.to_string(),
                    value: serde_json::Value::String(uid.to_string()),
                },
            ],
        }),
        (None, Some(field), Some(uid)) => Some(FilterExpr::Eq {
            field: field.to_string(),
            value: serde_json::Value::String(uid.to_string()),
        }),
        (Some(f), _, _) => Some(f.clone()),
        (None, _, _) => None,
    }
}

/// Merges a stored doc with its system fields. Result docs never collide on
/// these keys: `validate_doc` rejects any `"_"`-prefixed field at write time.
fn merge_doc(
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

/// Stable string form for change detection (jsonb key order is canonical in Postgres).
pub fn canonical(result: &QueryResult) -> String {
    serde_json::to_string(result).unwrap_or_else(|err| {
        tracing::error!(error = %err, "failed to serialize query result");
        String::new()
    })
}
