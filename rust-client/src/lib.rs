//! Rust client for [par-rt-db](https://github.com/paulrobello/par-rt-db) — a port of the
//! TypeScript SDK for server-side apps. Speaks the server's declarative query/transaction
//! DSL over one-shot HTTP and reactive WebSocket, with an admin control-plane client. No
//! codegen: you build a `Schema` that serializes to the exact server `SchemaDef`, and
//! query/mutate results deserialize generically into your own serde structs.
//!
//! Crate name: `par-rt-db-client` → in Rust, `use par_rt_db_client::...`.
//!
//! # Features
//!
//! | Feature | Default | Surface |
//! | --- | --- | --- |
//! | `http` | yes | `RtDbHttpClient` — typed query / mutate / `auth_me` |
//! | `ws` | no | `RtDbClient` (`src/ws.rs`) — reactive WebSocket client (live query subscriptions + mutate) |
//! | `admin` | no | `/admin/*` control-plane client — push-schema, create-db, mint-token, revoke-token, allowlist, export, import |
//! | `in_memory` | no | `InMemoryRtDbClient` (`src/in_memory.rs`) — in-memory harness for unit tests (no network, no Postgres) |
//!
//! `core` (wire types, schema/query/mutation builders, error model) compiles with no
//! features. `[lints.rust] warnings = "deny"` — same zero-warning posture as the server.
//!
//! The `http` surface also carries `.filter()` / `.search()` / `.vector_search()` query
//! builders, the `mutate_with_retry` precondition-conflict helper, `upsert_by_index` /
//! `find_one_by_index` shortcuts, scheduled/cron transactions, and file storage
//! (`upload` / `delete_file` / `get_file_metadata` / `get_url`).
//!
//! # Wire contract
//!
//! `src/wire.rs` is the **third** implementation of par-rt-db's protocol contract
//! (alongside `server/src/protocol.rs` and `ts-client/src/protocol.ts`; the Python
//! client's `wire.py` is the fourth). They must stay byte-identical — same serde tags
//! and field names. Changing the wire format on any side is a breaking change unless
//! mirrored across all clients.
//!
//! See the crate [`README`](https://github.com/paulrobello/par-rt-db/blob/main/rust-client/README.md)
//! for install snippets, quick-start examples (HTTP query/mutate, scheduling, file
//! storage), and the design spec
//! (`docs/superpowers/specs/2026-07-22-rust-client-design.md`).

pub mod cursor;
pub mod error;
pub mod mutation;
pub mod optimistic;
pub mod query;
pub mod schema;
pub mod wire;

#[cfg(feature = "http")]
pub mod http;

#[cfg(feature = "in_memory")]
pub mod in_memory;

#[cfg(feature = "ws")]
pub mod ws;

pub use error::{ErrorCode, ErrorEnvelope, RtDbError, retry_on_precondition};
pub use mutation::{Mutation, StepResult, Transaction};
pub use query::{Order, Paginate, Paginated, Query, TableQuery};
pub use schema::{FieldType, IndexDef, SchemaDef, TableDef, VectorIndexSpec};
pub use wire::{
    AggregateGroup, AggregateOp, AggregateSpec, AuthedUser, ClientMessage, FilterExpr,
    ScheduleInfo, ScheduleKind, ScheduleStatus, ScheduleWhen, SearchQuery, ServerMessage, UserKind,
    VectorSearchQuery,
};

#[cfg(feature = "http")]
pub use http::RtDbHttpClient;

#[cfg(feature = "in_memory")]
pub use in_memory::InMemoryRtDbClient;

#[cfg(feature = "ws")]
pub use ws::{ClientStatus, Config, ConnectionState, RtDbClient, Snapshot, Subscription};

#[cfg(feature = "admin")]
pub use wire::admin::{
    AdminMember, ConfigResponse, DbStats, HotConfig, HotConfigPatch, LatencyStats, MetricsSnapshot,
    MintedToken, OpEvent, TableStat, TokenInfo,
};
