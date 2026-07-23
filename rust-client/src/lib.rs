//! Rust client for par-rt-db. See `docs/superpowers/specs/2026-07-22-rust-client-design.md`.

pub mod cursor;
pub mod error;
pub mod mutation;
pub mod query;
pub mod schema;
pub mod wire;

#[cfg(feature = "http")]
pub mod http;
