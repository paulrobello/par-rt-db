//! One module per committer request arm. Every arm that commits a
//! document transaction calls `super::taps::publish_taps`; that is the
//! op-feed/audit/webhook completeness contract, and keeping the arms here
//! is what makes the set of call sites enumerable.

pub(in crate::committer) mod merge;
pub(in crate::committer) mod migrate;
pub(in crate::committer) mod mutate;
pub(in crate::committer) mod reaper;
pub(in crate::committer) mod scheduled;
pub(in crate::committer) mod schema;
pub(in crate::committer) mod subscribe;
pub(in crate::committer) mod workflow;
