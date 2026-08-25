//! QA-001 / QA-002 cross-client combination-matrix safety net (rust-client mirror).
//!
//! Mirrors `ts-client/tests/query_combinations.test.ts` and
//! `server/tests/query_combinations.rs` case-for-case. All three run the SAME
//! matrix against their respective query implementations and must agree on every
//! accept/reject. Adding a new terminal? Add cases here AND in both mirrors —
//! the matrix exists so the next terminal addition fails the gate on whichever
//! side forgets (this is exactly the drift class that produced QA-001: the TS
//! `get` guard omitted `filter`/`search`/`vectorSearch` and silently returned
//! the wrong result).
//!
//! Each case mutates a base `{table:"items"}` query JSON by setting
//! terminal/peer fields, deserializes it into a [`Query`], and runs it on the
//! in-memory client. Accept = no error; Reject = `BAD_REQUEST`. Any other error
//! panics (a cascade case must surface as BAD_REQUEST, not an internal fault).
//! A case that should Reject but Accepts (or vice versa) is a REAL cascade gap
//! in the rust in-memory engine — it is fixed in `src/in_memory.rs`, not weakened here.

use par_rt_db_client::error::{ErrorCode, RtDbError};
use par_rt_db_client::in_memory::{InMemoryRtDbClient, InMemoryRtDbClientOptions};
use par_rt_db_client::query::Query;
use par_rt_db_client::schema::{DistanceMetric, FieldType, Schema, SchemaBuilderExt, Table};
use serde_json::{Map, Value, json};

const ID: &str = "0123456789abcdef0123456789abcdef";

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum Outcome {
    Accept,
    Reject,
}

struct Case {
    name: &'static str,
    build: fn(&mut Map<String, Value>),
    expected: Outcome,
}

// ---- schema + client (mirror of ts newClient + schema, lines 22-39) ----------

fn matrix_schema() -> Schema {
    Schema::builder()
        .table(
            "items",
            Table::new()
                .field("title", FieldType::String)
                .field("body", FieldType::String)
                .field("count", FieldType::Number)
                .field("embedding", FieldType::vector(3))
                .index("by_title", &["title"])
                .index("by_title_count", &["title", "count"])
                .search_index("search_body", &["title", "body"], None)
                .vector_index("by_embedding", "embedding", 3, &[], DistanceMetric::Cosine),
        )
        .build()
}

fn new_client() -> InMemoryRtDbClient {
    let mut c = InMemoryRtDbClient::new(
        InMemoryRtDbClientOptions::default()
            .now(|| 1_700_000_000_000)
            .random(|| 0.0),
    );
    c.push_schema(&matrix_schema()).expect("push_schema");
    c
}

// ---- base query + reusable peer value constructors (mirror ts lines 43-61) ---

fn base_query() -> Map<String, Value> {
    let mut q = Map::new();
    q.insert("table".into(), json!("items"));
    q
}

fn filter_eq_title_x() -> Value {
    json!({"op":"eq","field":"title","value":"x"})
}
fn search_body_x() -> Value {
    json!({"index":"search_body","query":"x"})
}
fn vector_embedding_limit_1() -> Value {
    json!({"index":"by_embedding","vector":[0,0,0],"limit":1})
}
fn hybrid_query_database_x() -> Value {
    json!({"query":"x","vector":[0,0,0],"limit":1})
}
fn paginate_num_1() -> Value {
    json!({"numItems":1})
}

// ---- the matrix (case-for-case port of the ts CASES array) ------------------

fn cases() -> Vec<Case> {
    vec![
        // ============ Solo accepts (each terminal alone is valid baseline) ============
        Case {
            name: "solo: get",
            build: |q| {
                q.insert("get".into(), json!(ID));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: collect",
            build: |_q| {},
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: index",
            build: |q| {
                q.insert("index".into(), json!("by_title"));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: eq",
            build: |q| {
                q.insert("index".into(), json!("by_title"));
                q.insert("eq".into(), json!(["x"]));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: gt",
            build: |q| {
                q.insert("index".into(), json!("by_title"));
                q.insert("gt".into(), json!("x"));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: gte",
            build: |q| {
                q.insert("index".into(), json!("by_title"));
                q.insert("gte".into(), json!("x"));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: lt",
            build: |q| {
                q.insert("index".into(), json!("by_title"));
                q.insert("lt".into(), json!("x"));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: lte",
            build: |q| {
                q.insert("index".into(), json!("by_title"));
                q.insert("lte".into(), json!("x"));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: order",
            build: |q| {
                q.insert("order".into(), json!("asc"));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: take",
            build: |q| {
                q.insert("take".into(), json!(1));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: unique",
            build: |q| {
                q.insert("unique".into(), json!(true));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: first",
            build: |q| {
                q.insert("first".into(), json!(true));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: count",
            build: |q| {
                q.insert("count".into(), json!(true));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: distinct",
            build: |q| {
                q.insert("distinct".into(), json!(true));
                q.insert("index".into(), json!("by_title"));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: paginate",
            build: |q| {
                q.insert("paginate".into(), paginate_num_1());
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: filter",
            build: |q| {
                q.insert("filter".into(), filter_eq_title_x());
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: search",
            build: |q| {
                q.insert("search".into(), search_body_x());
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: vectorSearch",
            build: |q| {
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "solo: hybridSearch",
            build: |q| {
                q.insert("hybridSearch".into(), hybrid_query_database_x());
            },
            expected: Outcome::Accept,
        },
        // ============ get rejects every peer (QA-001: last 3 are the drift) ============
        Case {
            name: "get+index",
            build: |q| {
                q.insert("get".into(), json!(ID));
                q.insert("index".into(), json!("by_title"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "get+eq",
            build: |q| {
                q.insert("get".into(), json!(ID));
                q.insert("eq".into(), json!(["x"]));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "get+gt",
            build: |q| {
                q.insert("get".into(), json!(ID));
                q.insert("gt".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "get+gte",
            build: |q| {
                q.insert("get".into(), json!(ID));
                q.insert("gte".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "get+lt",
            build: |q| {
                q.insert("get".into(), json!(ID));
                q.insert("lt".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "get+lte",
            build: |q| {
                q.insert("get".into(), json!(ID));
                q.insert("lte".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "get+order",
            build: |q| {
                q.insert("get".into(), json!(ID));
                q.insert("order".into(), json!("asc"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "get+take",
            build: |q| {
                q.insert("get".into(), json!(ID));
                q.insert("take".into(), json!(1));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "get+unique",
            build: |q| {
                q.insert("get".into(), json!(ID));
                q.insert("unique".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "get+first",
            build: |q| {
                q.insert("get".into(), json!(ID));
                q.insert("first".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "get+count",
            build: |q| {
                q.insert("get".into(), json!(ID));
                q.insert("count".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "get+paginate",
            build: |q| {
                q.insert("get".into(), json!(ID));
                q.insert("paginate".into(), paginate_num_1());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "get+filter",
            build: |q| {
                q.insert("get".into(), json!(ID));
                q.insert("filter".into(), filter_eq_title_x());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "get+search",
            build: |q| {
                q.insert("get".into(), json!(ID));
                q.insert("search".into(), search_body_x());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "get+vectorSearch",
            build: |q| {
                q.insert("get".into(), json!(ID));
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "get+hybridSearch",
            build: |q| {
                q.insert("get".into(), json!(ID));
                q.insert("hybridSearch".into(), hybrid_query_database_x());
            },
            expected: Outcome::Reject,
        },
        // ============ unique rejects take, order ============
        Case {
            name: "unique+take",
            build: |q| {
                q.insert("unique".into(), json!(true));
                q.insert("take".into(), json!(1));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "unique+order",
            build: |q| {
                q.insert("unique".into(), json!(true));
                q.insert("order".into(), json!("asc"));
            },
            expected: Outcome::Reject,
        },
        // ============ first rejects unique, take ============
        Case {
            name: "first+unique",
            build: |q| {
                q.insert("first".into(), json!(true));
                q.insert("unique".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "first+take",
            build: |q| {
                q.insert("first".into(), json!(true));
                q.insert("take".into(), json!(1));
            },
            expected: Outcome::Reject,
        },
        // ============ count rejects unique, take, first, order, distinct ============
        Case {
            name: "count+unique",
            build: |q| {
                q.insert("count".into(), json!(true));
                q.insert("unique".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "count+take",
            build: |q| {
                q.insert("count".into(), json!(true));
                q.insert("take".into(), json!(1));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "count+first",
            build: |q| {
                q.insert("count".into(), json!(true));
                q.insert("first".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "count+order",
            build: |q| {
                q.insert("count".into(), json!(true));
                q.insert("order".into(), json!("asc"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "count+distinct",
            build: |q| {
                q.insert("count".into(), json!(true));
                q.insert("distinct".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        // ============ distinct rejects get, take, unique, first, count, order,
        //              paginate, search, vectorSearch (standalone terminal like count) ============
        Case {
            name: "distinct+get",
            build: |q| {
                q.insert("distinct".into(), json!(true));
                q.insert("index".into(), json!("by_title"));
                q.insert("get".into(), json!(ID));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "distinct+take",
            build: |q| {
                q.insert("distinct".into(), json!(true));
                q.insert("index".into(), json!("by_title"));
                q.insert("take".into(), json!(1));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "distinct+unique",
            build: |q| {
                q.insert("distinct".into(), json!(true));
                q.insert("index".into(), json!("by_title"));
                q.insert("unique".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "distinct+first",
            build: |q| {
                q.insert("distinct".into(), json!(true));
                q.insert("index".into(), json!("by_title"));
                q.insert("first".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "distinct+count",
            build: |q| {
                q.insert("distinct".into(), json!(true));
                q.insert("index".into(), json!("by_title"));
                q.insert("count".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "distinct+order",
            build: |q| {
                q.insert("distinct".into(), json!(true));
                q.insert("index".into(), json!("by_title"));
                q.insert("order".into(), json!("asc"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "distinct+paginate",
            build: |q| {
                q.insert("distinct".into(), json!(true));
                q.insert("index".into(), json!("by_title"));
                q.insert("paginate".into(), paginate_num_1());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "distinct+search",
            build: |q| {
                q.insert("distinct".into(), json!(true));
                q.insert("index".into(), json!("by_title"));
                q.insert("search".into(), search_body_x());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "distinct+vectorSearch",
            build: |q| {
                q.insert("distinct".into(), json!(true));
                q.insert("index".into(), json!("by_title"));
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "distinct+hybridSearch",
            build: |q| {
                q.insert("distinct".into(), json!(true));
                q.insert("index".into(), json!("by_title"));
                q.insert("hybridSearch".into(), hybrid_query_database_x());
            },
            expected: Outcome::Reject,
        },
        // ============ aggregate rejects get, take, unique, first, count, distinct,
        //              order, paginate, search, vectorSearch (standalone terminal
        //              like count/distinct); composes with index/eq/range/filter ============
        Case {
            name: "solo: aggregate",
            build: |q| {
                q.insert("aggregate".into(), json!({"op":"min"}));
                q.insert("index".into(), json!("by_title"));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "aggregate+get",
            build: |q| {
                q.insert("aggregate".into(), json!({"op":"min"}));
                q.insert("index".into(), json!("by_title"));
                q.insert("get".into(), json!(ID));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "aggregate+take",
            build: |q| {
                q.insert("aggregate".into(), json!({"op":"min"}));
                q.insert("index".into(), json!("by_title"));
                q.insert("take".into(), json!(1));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "aggregate+unique",
            build: |q| {
                q.insert("aggregate".into(), json!({"op":"min"}));
                q.insert("index".into(), json!("by_title"));
                q.insert("unique".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "aggregate+first",
            build: |q| {
                q.insert("aggregate".into(), json!({"op":"min"}));
                q.insert("index".into(), json!("by_title"));
                q.insert("first".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "aggregate+count",
            build: |q| {
                q.insert("aggregate".into(), json!({"op":"min"}));
                q.insert("index".into(), json!("by_title"));
                q.insert("count".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "aggregate+distinct",
            build: |q| {
                q.insert("aggregate".into(), json!({"op":"min"}));
                q.insert("index".into(), json!("by_title"));
                q.insert("distinct".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "aggregate+order",
            build: |q| {
                q.insert("aggregate".into(), json!({"op":"min"}));
                q.insert("index".into(), json!("by_title"));
                q.insert("order".into(), json!("asc"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "aggregate+paginate",
            build: |q| {
                q.insert("aggregate".into(), json!({"op":"min"}));
                q.insert("index".into(), json!("by_title"));
                q.insert("paginate".into(), paginate_num_1());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "aggregate+search",
            build: |q| {
                q.insert("aggregate".into(), json!({"op":"min"}));
                q.insert("index".into(), json!("by_title"));
                q.insert("search".into(), search_body_x());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "aggregate+vectorSearch",
            build: |q| {
                q.insert("aggregate".into(), json!({"op":"min"}));
                q.insert("index".into(), json!("by_title"));
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "aggregate+hybridSearch",
            build: |q| {
                q.insert("aggregate".into(), json!({"op":"min"}));
                q.insert("index".into(), json!("by_title"));
                q.insert("hybridSearch".into(), hybrid_query_database_x());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "compose: aggregate+eq",
            build: |q| {
                q.insert("aggregate".into(), json!({"op":"sum"}));
                q.insert("index".into(), json!("by_title_count"));
                q.insert("eq".into(), json!(["x"]));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "compose: aggregate+filter",
            build: |q| {
                q.insert("aggregate".into(), json!({"op":"min"}));
                q.insert("index".into(), json!("by_title"));
                q.insert("filter".into(), filter_eq_title_x());
            },
            expected: Outcome::Accept,
        },
        // ============ paginate rejects count, unique, first, take (get covered above) ============
        Case {
            name: "paginate+count",
            build: |q| {
                q.insert("paginate".into(), paginate_num_1());
                q.insert("count".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "paginate+unique",
            build: |q| {
                q.insert("paginate".into(), paginate_num_1());
                q.insert("unique".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "paginate+first",
            build: |q| {
                q.insert("paginate".into(), paginate_num_1());
                q.insert("first".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "paginate+take",
            build: |q| {
                q.insert("paginate".into(), paginate_num_1());
                q.insert("take".into(), json!(1));
            },
            expected: Outcome::Reject,
        },
        // ============ range-bound incompatibilities ============
        Case {
            name: "gt+gte",
            build: |q| {
                q.insert("index".into(), json!("by_title"));
                q.insert("gt".into(), json!("x"));
                q.insert("gte".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "lt+lte",
            build: |q| {
                q.insert("index".into(), json!("by_title"));
                q.insert("lt".into(), json!("x"));
                q.insert("lte".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        // ============ vectorSearch rejects every peer (take included) ============
        Case {
            name: "vectorSearch+index",
            build: |q| {
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
                q.insert("index".into(), json!("by_title"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "vectorSearch+eq",
            build: |q| {
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
                q.insert("eq".into(), json!(["x"]));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "vectorSearch+gt",
            build: |q| {
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
                q.insert("gt".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "vectorSearch+gte",
            build: |q| {
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
                q.insert("gte".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "vectorSearch+lt",
            build: |q| {
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
                q.insert("lt".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "vectorSearch+lte",
            build: |q| {
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
                q.insert("lte".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "vectorSearch+order",
            build: |q| {
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
                q.insert("order".into(), json!("asc"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "vectorSearch+unique",
            build: |q| {
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
                q.insert("unique".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "vectorSearch+first",
            build: |q| {
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
                q.insert("first".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "vectorSearch+count",
            build: |q| {
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
                q.insert("count".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "vectorSearch+paginate",
            build: |q| {
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
                q.insert("paginate".into(), paginate_num_1());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "vectorSearch+filter",
            build: |q| {
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
                q.insert("filter".into(), filter_eq_title_x());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "vectorSearch+search",
            build: |q| {
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
                q.insert("search".into(), search_body_x());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "vectorSearch+take",
            build: |q| {
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
                q.insert("take".into(), json!(1));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "vectorSearch+hybridSearch",
            build: |q| {
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
                q.insert("hybridSearch".into(), hybrid_query_database_x());
            },
            expected: Outcome::Reject,
        },
        // ============ search rejects every peer except take ============
        Case {
            name: "search+index",
            build: |q| {
                q.insert("search".into(), search_body_x());
                q.insert("index".into(), json!("by_title"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "search+eq",
            build: |q| {
                q.insert("search".into(), search_body_x());
                q.insert("eq".into(), json!(["x"]));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "search+gt",
            build: |q| {
                q.insert("search".into(), search_body_x());
                q.insert("gt".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "search+gte",
            build: |q| {
                q.insert("search".into(), search_body_x());
                q.insert("gte".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "search+lt",
            build: |q| {
                q.insert("search".into(), search_body_x());
                q.insert("lt".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "search+lte",
            build: |q| {
                q.insert("search".into(), search_body_x());
                q.insert("lte".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "search+order",
            build: |q| {
                q.insert("search".into(), search_body_x());
                q.insert("order".into(), json!("asc"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "search+unique",
            build: |q| {
                q.insert("search".into(), search_body_x());
                q.insert("unique".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "search+first",
            build: |q| {
                q.insert("search".into(), search_body_x());
                q.insert("first".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "search+count",
            build: |q| {
                q.insert("search".into(), search_body_x());
                q.insert("count".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "search+paginate",
            build: |q| {
                q.insert("search".into(), search_body_x());
                q.insert("paginate".into(), paginate_num_1());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "search+filter",
            build: |q| {
                q.insert("search".into(), search_body_x());
                q.insert("filter".into(), filter_eq_title_x());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "search+vectorSearch",
            build: |q| {
                q.insert("search".into(), search_body_x());
                q.insert("vectorSearch".into(), vector_embedding_limit_1());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "search+hybridSearch",
            build: |q| {
                q.insert("search".into(), search_body_x());
                q.insert("hybridSearch".into(), hybrid_query_database_x());
            },
            expected: Outcome::Reject,
        },
        // ============ hybridSearch rejects every peer (standalone, like vectorSearch) ============
        Case {
            name: "hybridSearch+index",
            build: |q| {
                q.insert("hybridSearch".into(), hybrid_query_database_x());
                q.insert("index".into(), json!("by_title"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "hybridSearch+eq",
            build: |q| {
                q.insert("hybridSearch".into(), hybrid_query_database_x());
                q.insert("eq".into(), json!(["x"]));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "hybridSearch+gt",
            build: |q| {
                q.insert("hybridSearch".into(), hybrid_query_database_x());
                q.insert("gt".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "hybridSearch+gte",
            build: |q| {
                q.insert("hybridSearch".into(), hybrid_query_database_x());
                q.insert("gte".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "hybridSearch+lt",
            build: |q| {
                q.insert("hybridSearch".into(), hybrid_query_database_x());
                q.insert("lt".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "hybridSearch+lte",
            build: |q| {
                q.insert("hybridSearch".into(), hybrid_query_database_x());
                q.insert("lte".into(), json!("x"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "hybridSearch+order",
            build: |q| {
                q.insert("hybridSearch".into(), hybrid_query_database_x());
                q.insert("order".into(), json!("asc"));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "hybridSearch+unique",
            build: |q| {
                q.insert("hybridSearch".into(), hybrid_query_database_x());
                q.insert("unique".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "hybridSearch+first",
            build: |q| {
                q.insert("hybridSearch".into(), hybrid_query_database_x());
                q.insert("first".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "hybridSearch+count",
            build: |q| {
                q.insert("hybridSearch".into(), hybrid_query_database_x());
                q.insert("count".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "hybridSearch+distinct",
            build: |q| {
                q.insert("hybridSearch".into(), hybrid_query_database_x());
                q.insert("distinct".into(), json!(true));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "hybridSearch+aggregate",
            build: |q| {
                q.insert("hybridSearch".into(), hybrid_query_database_x());
                q.insert("aggregate".into(), json!({"op":"min"}));
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "hybridSearch+paginate",
            build: |q| {
                q.insert("hybridSearch".into(), hybrid_query_database_x());
                q.insert("paginate".into(), paginate_num_1());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "hybridSearch+filter",
            build: |q| {
                q.insert("hybridSearch".into(), hybrid_query_database_x());
                q.insert("filter".into(), filter_eq_title_x());
            },
            expected: Outcome::Reject,
        },
        Case {
            name: "hybridSearch+take",
            build: |q| {
                q.insert("hybridSearch".into(), hybrid_query_database_x());
                q.insert("take".into(), json!(1));
            },
            expected: Outcome::Reject,
        },
        // ============ composition accepts (smoke that valid combos don't false-reject) ============
        Case {
            name: "compose: search+take",
            build: |q| {
                q.insert("search".into(), search_body_x());
                q.insert("take".into(), json!(1));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "compose: index+take",
            build: |q| {
                q.insert("index".into(), json!("by_title"));
                q.insert("take".into(), json!(1));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "compose: index+eq+take",
            build: |q| {
                q.insert("index".into(), json!("by_title"));
                q.insert("eq".into(), json!(["x"]));
                q.insert("take".into(), json!(1));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "compose: index+order",
            build: |q| {
                q.insert("index".into(), json!("by_title"));
                q.insert("order".into(), json!("asc"));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "compose: index+gt+lt",
            build: |q| {
                q.insert("index".into(), json!("by_title"));
                q.insert("gt".into(), json!("a"));
                q.insert("lt".into(), json!("z"));
            },
            expected: Outcome::Accept,
        },
        Case {
            name: "compose: take+filter",
            build: |q| {
                q.insert("take".into(), json!(1));
                q.insert("filter".into(), filter_eq_title_x());
            },
            expected: Outcome::Accept,
        },
    ]
}

/// Resolve a case's outcome on the in-memory client. Panics on any non-BAD_REQUEST
/// error (a cascade case must surface as BAD_REQUEST, never an internal fault).
fn run_case(c: &InMemoryRtDbClient, case: &Case) -> Outcome {
    let mut q = base_query();
    (case.build)(&mut q);
    let query: Query = serde_json::from_value(Value::Object(q))
        .unwrap_or_else(|e| panic!("{}: failed to parse built query JSON: {e}", case.name));
    match c.run_query(&query) {
        Ok(_) => Outcome::Accept,
        Err(RtDbError {
            code: ErrorCode::BadRequest,
            ..
        }) => Outcome::Reject,
        Err(e) => panic!(
            "{}: expected {:?} but got non-cascade error {:?}: {}",
            case.name, case.expected, e.code, e.message
        ),
    }
}

#[test]
fn combination_matrix_cascade_outcomes_match_expectations() {
    let c = new_client();
    let cases = cases();
    assert_eq!(cases.len(), 124, "matrix case count drift (expected 124)");

    let mut failures: Vec<&str> = Vec::new();
    for case in &cases {
        let actual = run_case(&c, case);
        if actual != case.expected {
            failures.push(case.name);
            eprintln!(
                "  MISMATCH {:<28} expected {:?}, got {:?}",
                case.name, case.expected, actual
            );
        }
    }
    assert!(
        failures.is_empty(),
        "combination matrix drift — these cases diverged from expectations: {failures:?}"
    );
}

#[test]
fn matrix_covers_documented_qa001_drift_cases() {
    // These three cases are the QA-001 drift surface — the TS `get` guard used
    // to omit them and silently accept `get+filter`/`get+search`/`get+vectorSearch`.
    // If any are removed or reclassified, fail loudly: they are the load-bearing
    // regression cases for the QA-001 fix.
    let cases = cases();
    let names: Vec<&str> = cases.iter().map(|c| c.name).collect();
    for required in ["get+filter", "get+search", "get+vectorSearch"] {
        assert!(
            names.contains(&required),
            "matrix is missing load-bearing QA-001 case: {required}"
        );
        let expected = cases
            .iter()
            .find(|c| c.name == required)
            .map(|c| c.expected)
            .expect("required case present");
        assert_eq!(
            expected,
            Outcome::Reject,
            "QA-001 case {required} must be Reject"
        );
    }
}
