//! OPENAPI drift gate: every route axum registers must appear in the document
//! served at `GET /openapi.json`, and every documented path must be a
//! registered route. Axum has no public route introspection, so the registered
//! side is recovered by scanning `server/src/**/*.rs` for `.route("…")` path
//! literals (all call sites use string literals; a dynamically-built path
//! would evade this gate, and none exists). Both directions are asserted, plus
//! scanner sanity guards, so an empty scan or an empty document can never go
//! green.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn collect_rs_files(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => panic!("cannot read {}: {e}", dir.display()),
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            collect_rs_files(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// Extract every `.route("…"…)` path literal, tolerating the multi-line
/// `.route(\n "path",` form used where the method chain wraps.
fn scan_route_paths(source: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = source;
    while let Some(idx) = rest.find(".route(") {
        let after = &rest[idx + ".route(".len()..];
        let trimmed = after.trim_start_matches([' ', '\t', '\r', '\n']);
        if let Some(stripped) = trimmed.strip_prefix('"')
            && let Some(end) = stripped.find('"')
        {
            out.push(stripped[..end].to_string());
        }
        rest = after;
    }
    out
}

#[test]
fn openapi_document_covers_every_registered_route() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut files = Vec::new();
    collect_rs_files(&src, &mut files);
    assert!(
        files.len() > 50,
        "source scan found {} .rs files — the scan is broken",
        files.len()
    );

    let mut routes = BTreeSet::new();
    for file in &files {
        let source = std::fs::read_to_string(file)
            .unwrap_or_else(|e| panic!("cannot read {}: {e}", file.display()));
        routes.extend(scan_route_paths(&source));
    }
    // Scanner sanity: the scan must find the real router, not nothing.
    assert!(
        routes.len() > 100,
        "scanned only {} route literals — the scan is broken",
        routes.len()
    );
    assert!(
        routes.contains("/api/query"),
        "scanner sanity failed: /api/query not found in the source scan"
    );

    let doc = rtdb_server::openapi::document();
    let doc_paths: BTreeSet<String> = doc["paths"]
        .as_object()
        .unwrap_or_else(|| panic!("openapi document has no paths object"))
        .keys()
        .cloned()
        .collect();
    assert!(
        doc_paths.contains("/api/query"),
        "openapi document sanity failed: /api/query missing"
    );

    let missing: Vec<&String> = routes.difference(&doc_paths).collect();
    assert!(
        missing.is_empty(),
        "routes registered in axum but missing from GET /openapi.json \
         (add them to server/src/openapi.rs): {missing:?}"
    );

    let extra: Vec<&String> = doc_paths.difference(&routes).collect();
    assert!(
        extra.is_empty(),
        "paths documented in server/src/openapi.rs but not registered in any \
         router (stale doc, likely a typo): {extra:?}"
    );
}
