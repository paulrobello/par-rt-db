//! Rust client for par-rt-db. See `docs/superpowers/specs/2026-07-22-rust-client-design.md`.

pub mod cursor;
pub mod error;
pub mod mutation;
pub mod query;
pub mod schema;
pub mod wire;

#[cfg(feature = "http")]
pub mod http;

#[cfg(feature = "ws")]
pub mod ws;

pub use error::{ErrorCode, ErrorEnvelope, RtDbError, retry_on_precondition};
pub use mutation::{Mutation, StepResult, Transaction};
pub use query::{Order, Paginate, Paginated, Query, TableQuery};
pub use schema::{FieldType, IndexDef, SchemaDef, TableDef, VectorIndexSpec};
pub use wire::{
    AuthedUser, ClientMessage, FilterExpr, ScheduleInfo, ScheduleWhen, SearchQuery, ServerMessage,
    VectorSearchQuery,
};

#[cfg(feature = "http")]
pub use http::RtDbHttpClient;

#[cfg(feature = "ws")]
pub use ws::{ClientStatus, Config, ConnectionState, RtDbClient, Snapshot, Subscription};

#[cfg(feature = "admin")]
pub use wire::admin::MintedToken;
