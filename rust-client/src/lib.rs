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
//! | `http` | yes | [`RtDbHttpClient`] — typed query / mutate / `auth_me` |
//! | `ws` | no | [`RtDbClient`] (`src/ws.rs`) — reactive WebSocket client (live query subscriptions, presence, optimistic updates, mutate) |
//! | `admin` | no | [`RtDbAdminClient`] — `/admin/*` control-plane client: db create/list/push-schema, `explain_query`, webhooks, audit log, sessions, `merge_users`, backups, `clone_db`, and more |
//! | `in_memory` | no | [`InMemoryRtDbClient`] (`src/in_memory/mod.rs`) — in-memory harness for unit tests (no network, no Postgres) |
//!
//! `core` (wire types, schema/query/mutation builders, error model) compiles with no
//! features. `[lints.rust] warnings = "deny"` — same zero-warning posture as the server.
//!
//! The `http` surface also carries `.filter()` / `.search()` / `.vector_search()` /
//! `.hybrid_search()` / `.fields()` / `.distinct()` / `.aggregate()` query builders,
//! `batch_query` for fanning out many queries in one round trip, the
//! `mutate_with_retry` precondition-conflict helper, `upsert_by_index` /
//! `find_one_by_index` shortcuts, scheduled/cron transactions, durable workflows, and
//! file storage (`upload` / `upload_stream` / `delete_file` / `get_file_metadata` /
//! `get_url` / `get_signed_url` / `transform_url`). The `ws` surface additionally
//! exposes presence and optimistic updates on live query [`Subscription`]s,
//! configured via [`Config`] and reporting [`ConnectionState`].
//!
//! # Wire contract
//!
//! `src/wire.rs` is one of four implementations of par-rt-db's protocol contract
//! (alongside `server/src/protocol.rs`, `ts-client/src/protocol.ts`, and the Python
//! client's `wire.py`). They must stay byte-identical — same serde tags and field
//! names. Changing the wire format on any side is a breaking change unless mirrored
//! across all clients.
//!
//! See the crate [`README`](https://github.com/paulrobello/par-rt-db/blob/main/rust-client/README.md)
//! for install snippets, quick-start examples (HTTP query/mutate, scheduling, file
//! storage), and the design spec
//! (`docs/superpowers/specs/2026-07-22-rust-client-design.md`).

#![warn(missing_docs)]

pub mod cursor;
pub mod error;
pub mod mutation;
pub mod optimistic;
pub mod query;
pub mod schema;
pub mod value_expr;
pub mod wire;

#[cfg(feature = "admin")]
pub mod admin;

#[cfg(feature = "admin")]
pub mod migration;

#[cfg(feature = "http")]
pub mod http;

#[cfg(feature = "http")]
mod http_common;

#[cfg(feature = "in_memory")]
pub mod in_memory;

#[cfg(feature = "ws")]
pub mod ws;

pub use error::{ErrorCode, ErrorEnvelope, RtDbError, retry_on_precondition};
pub use mutation::{Mutation, StepResult, Transaction};
pub use query::{
    HybridSearchOpts, Order, Paginate, Paginated, Query, SearchOpts, TableQuery, VectorSearchOpts,
};
pub use schema::{
    DistanceMetric, FieldType, IndexDef, OnDeleteAction, SchemaBuilderExt, SchemaDef, TableDef,
    VectorIndexSpec,
};
pub use value_expr::{CaseWhen, Cast, ValueExpr};
pub use wire::{
    AggregateGroup, AggregateOp, AggregateSpec, AuthedUser, AwaitSignalSpec, ClientMessage,
    FilterExpr, OutcomeStatus, PresenceMember, ScheduleInfo, ScheduleKind, ScheduleStatus,
    ScheduleWhen, SearchMode, SearchQuery, ServerMessage, StepOutcome, StepRetry, UserKind,
    VectorSearchQuery, WorkflowInfo, WorkflowInfoFull, WorkflowSpec, WorkflowStatus,
    WorkflowStepSpec,
};

#[cfg(feature = "http")]
pub use http::{
    FileMetadata, Fit, OutFormat, RtDbHttpClient, SignedUrl, TransformOpts, UploadResult,
};

#[cfg(feature = "admin")]
pub use admin::RtDbAdminClient;

#[cfg(feature = "in_memory")]
pub use in_memory::{InMemoryRtDbClient, PresenceHandle, PresenceRooms};

#[cfg(feature = "ws")]
pub use ws::{
    ClientStatus, Config, ConnectionState, Presence, PresenceSnapshot, RtDbClient, Snapshot,
    Subscription,
};

#[cfg(feature = "admin")]
pub use wire::admin::{
    AdminMember, AuditEntry, AuditQuery, BackupsListResponse, CastFailure, ConfigResponse,
    CreateWebhookOptions, DbStats, Directive, DirectiveReport, HotConfig, HotConfigPatch,
    LatencyStats, ListDeliveriesOptions, MergeConflict, MergeDbResult, MergeReport,
    MetricsSnapshot, MigrateRequest, MigrateRequestOwned, MigrateResult, MintTokenOptions,
    MintedToken, OpEvent, SampleChange, SchemaPreviewColumnAdd, SchemaPreviewDiff,
    SchemaPreviewIndexAdd, SchemaPreviewRejection, SchemaPreviewTableAdd, SessionInfo,
    SessionListOptions, TableStat, TokenInfo, Webhook, WebhookDelivery, WebhookEditOptions,
    WorkflowListOptions,
};
// `Cast` (like `ValueExpr`/`CaseWhen`) is re-exported unconditionally from
// `value_expr` above — the same type `wire::admin` re-exports — so the root
// name exists in every feature combination, not only under `admin`.

#[cfg(feature = "admin")]
pub use migration::Migration;
