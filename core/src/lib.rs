//! `par-rt-db-core` — the types par-rt-db's server and Rust client both speak.
//!
//! This crate holds no I/O, no runtime, and no database access: it depends on
//! `serde` and `serde_json` and nothing else, so both a tokio/sqlx server and a
//! `no-default-features` client can take it without inheriting a stack.

#![deny(missing_docs)]
#![deny(warnings)]

pub mod engine;
pub mod fields;
pub mod mutation;
pub mod schema;
pub mod wire;
