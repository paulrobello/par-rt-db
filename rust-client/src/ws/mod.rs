//! Reactive WebSocket client for par-rt-db (`ws` feature).
//!
//! One long-lived connection to `/sync` runs in a background driver task. Callers
//! get an ergonomic, cloneable handle ([`RtDbClient`]) that exposes synchronous
//! [`subscribe`](RtDbClient::subscribe) (returning a [`Subscription`] backed by a
//! `tokio::sync::watch` channel) and async [`mutate`](RtDbClient::mutate) with
//! idempotency. The driver owns the socket; on disconnect it reconnects with
//! jittered exponential backoff, re-authenticates, and re-establishes every
//! active subscription.
//!
//! Wire vocabulary is shared with the server and the TS client via
//! [`crate::wire`]: tags and casing are load-bearing and are not redefined here.
//!
//! Design: `docs/superpowers/specs/2026-07-22-rust-client-design.md` (Reactive WS
//! client). The lifecycle mirrors `ts-client/src/client.ts` exactly.

#![cfg(feature = "ws")]

mod client;
mod driver;
mod server_msg;
mod state;
#[cfg(test)]
mod tests;

pub use client::{Presence, RtDbClient, Subscription};
pub use state::{ClientStatus, Config, ConnectionState, PresenceSnapshot, Snapshot};

// Re-export only for `crate::ws::sync_url` consumers (admin's `/admin/stream`);
// unused when that feature is off.
#[allow(unused_imports)]
pub(crate) use driver::sync_url;
