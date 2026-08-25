//! Feature-area split of the in-memory engine's test suite (QA-205; was one
//! 6,088-line `tests.rs`). Each submodule mirrors an engine surface: `schema`
//! and `migrate` the schema-push/migrate pipeline, `validate` the pure
//! validation helpers, `writes` insert/upsert/txn, `query`/`aggregate`/
//! `paginate`/`search` the read terminals, `filter` the predicate DSL,
//! `subscribe` live queries, `scheduler` the clock, `storage` blobs,
//! `unique` unique indexes, `presence` presence rooms, `cascade` foreign-key
//! cascade / soft delete (FM-33). Cross-cluster fixtures live here.
use super::*;
use crate::mutation::Mutation;
use crate::query::{Paginate, Paginated, SearchOpts, TableQuery, VectorSearchOpts};
use crate::schema::{Schema, SchemaBuilderExt, Table};
use crate::wire::{AggregateOp, AggregateSpec, FilterExpr, SearchMode};
use serde_json::json;
use std::sync::{Arc, Mutex};

mod aggregate;
mod cascade;
mod computed;
mod filter;
// Every migrate test (and its fixture) is `#[cfg(feature = "admin")]` — the
// migrate surface only exists on admin-enabled builds, so the module is too.
#[cfg(feature = "admin")]
mod migrate;
mod paginate;
mod presence;
mod query;
mod relative_filter;
mod scheduler;
mod schema;
mod search;
mod storage;
mod subscribe;
mod unique;
mod validate;
mod writes;

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
                .index("by_status_and_order", &["status", "order"])
                .search_index("by_content", &["name"], None),
        )
        .build()
}

fn items_table(schema: &SchemaDef) -> &TableDef {
    schema.tables.get("items").expect("items table present")
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
    client.push_schema(&test_schema()).unwrap();
    client
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
