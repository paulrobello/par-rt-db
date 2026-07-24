//! Build script: bakes build-time identity into the binary so `/healthz` can
//! report which commit/build is live without SSH access to the host.
//!
//! Emits two `cargo:rustc-env` values consumed via `env!` in `src/health.rs`:
//!
//! - `BUILD_GIT_COMMIT`: short sha. Resolution order is
//!   `RTDB_BUILD_COMMIT` env (e.g. a Docker build-arg) → `git rev-parse
//!   --short HEAD` → `unknown`. The host deploy dir has no `.git` (rsync
//!   excludes it), so prod builds set `RTDB_BUILD_COMMIT` via the compose
//!   build arg; local/CI builds resolve it from `git` directly.
//! - `BUILD_TIMESTAMP_SECS`: unix seconds the binary was built.
//!   `SOURCE_DATE_EPOCH` (reproducible builds) wins, else the build's wall
//!   clock. Emitted as raw seconds; formatted to RFC3339 at runtime by
//!   `health.rs` so this script needs no time-formatting dependency.
//!
//! Every step is fallible and non-fatal: a missing `git`, an unparseable
//! `SOURCE_DATE_EPOCH`, or an unset override degrades to a safe default
//! rather than failing the build.
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

fn main() {
    let commit = std::env::var("RTDB_BUILD_COMMIT")
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .or_else(git_short_sha)
        .unwrap_or_else(|| "unknown".to_owned());
    println!("cargo:rustc-env=BUILD_GIT_COMMIT={commit}");
    println!("cargo:rerun-if-env-changed=RTDB_BUILD_COMMIT");

    let secs = match std::env::var("SOURCE_DATE_EPOCH") {
        Ok(raw) => raw
            .trim()
            .parse::<u64>()
            .unwrap_or_else(|_| build_now_secs()),
        Err(_) => build_now_secs(),
    };
    println!("cargo:rustc-env=BUILD_TIMESTAMP_SECS={secs}");
    println!("cargo:rerun-if-env-changed=SOURCE_DATE_EPOCH");
}

fn git_short_sha() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if sha.is_empty() { None } else { Some(sha) }
}

fn build_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}
