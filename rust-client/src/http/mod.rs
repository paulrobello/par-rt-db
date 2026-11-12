//! One-shot HTTP client for par-rt-db. `Authorization: Bearer <token>` and the
//! ARC-013 `X-Rtdb-Protocol` version header on every call, both attached by the
//! crate-internal `AuthedRequest::authed` seam in `http_common`.

#[cfg(feature = "admin")]
mod admin;
mod bulk;
mod changefeed;
mod client;
mod scheduling;
mod storage;
#[cfg(test)]
mod tests;
mod url;

pub use changefeed::{ChangeFeedStream, ChangeFeedStreamOptions};
pub use storage::{FileMetadata, Fit, OutFormat, SignedUrl, TransformOpts, UploadResult};

/// Maximum total attempts for [`RtDbHttpClient::mutate_with_retry`] when a
/// caller wants the SDK's default bound. Matches `ts-client`'s
/// `retryOnPrecondition` default of 4 retries (5 total attempts).
pub const DEFAULT_MUTATE_MAX_ATTEMPTS: u32 = 5;

/// One-shot HTTP client for one par-rt-db database: typed queries, atomic
/// transactions, scheduling/cron, durable workflows, and file storage, all
/// bearer-token authorized. For live query subscriptions use the reactive
/// `RtDbClient` (`ws` feature); for `/admin/*` work obtain an admin client via
/// `admin_client()` (the `admin` feature).
#[derive(Clone)]
pub struct RtDbHttpClient {
    url: String,
    db: String,
    token: String,
    client: reqwest::Client,
}
