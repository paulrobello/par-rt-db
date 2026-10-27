//! Hand-maintained OpenAPI 3.1 description of the HTTP surface, served at
//! `GET /openapi.json`. Deliberately NOT generated from handler annotations
//! (utoipa et al.): one place to maintain, zero per-handler churn. Fidelity is
//! "good enough to drive a client generator or an agent": full field/required
//! detail on the core data-plane request/response shapes, tagged enums as
//! `oneOf` variants, and summary-level operations on the wide admin surface.
//! The drift test (`server/tests/openapi_drift.rs`) fails the build when a
//! registered route is missing from the document, or a documented path is not
//! registered. Unauthenticated by design — the document carries shapes, not
//! secrets; every real route's auth requirements are unchanged.

use axum::Json;
use serde_json::{Map, Value, json};

/// `GET /openapi.json` — the machine-readable API description.
pub async fn handler() -> Json<Value> {
    Json(document())
}

/// Build the full document. Public so the drift test compares it against the
/// registered routes without going through HTTP.
pub fn document() -> Value {
    let mut paths = Map::new();

    // --- Top-level surface (lib.rs) --------------------------------------
    add(
        &mut paths,
        "/healthz",
        &[("get", ok_json(op("Liveness probe", "Health"), "Healthz"))],
    );
    add(
        &mut paths,
        "/privacy",
        &[("get", op("Privacy statement page", "Health"))],
    );
    add(
        &mut paths,
        "/metrics",
        &[(
            "get",
            ok_raw(
                op("Prometheus scrape (content-negotiated)", "Health"),
                "text/plain",
            ),
        )],
    );
    add(
        &mut paths,
        "/openapi.json",
        &[(
            "get",
            ok_json(op("This document", "Health"), "OpenApiDocument"),
        )],
    );

    // --- WebSocket --------------------------------------------------------
    add(
        &mut paths,
        "/sync",
        &[(
            "get",
            op(
                "WebSocket upgrade — the realtime transport (auth/subscribe/mutate frames)",
                "Realtime",
            ),
        )],
    );

    // --- Data plane (http_api) --------------------------------------------
    add(
        &mut paths,
        "/api/query",
        &[(
            "post",
            ok_json(
                body(op("Run one query", "Query"), "QueryRequest"),
                "QueryResponse",
            ),
        )],
    );
    add(
        &mut paths,
        "/api/query-batch",
        &[(
            "post",
            ok_json(
                body(
                    op(
                        "Run N queries in one round trip (per-query error isolation)",
                        "Query",
                    ),
                    "BatchQueryRequest",
                ),
                "BatchQueryOutcomeList",
            ),
        )],
    );
    add(
        &mut paths,
        "/api/mutate",
        &[(
            "post",
            ok_json(
                body(op("Run one transaction", "Mutate"), "MutateRequest"),
                "MutateResponse",
            ),
        )],
    );
    add(
        &mut paths,
        "/api/mutate-batch",
        &[(
            "post",
            ok_json(
                body(
                    op(
                        "Run N independent transactions (deliberately not atomic; per-entry idempotencyKey dedup)",
                        "Mutate",
                    ),
                    "BatchMutateRequest",
                ),
                "BatchMutateOutcomeList",
            ),
        )],
    );

    // Schedules (data plane)
    add(
        &mut paths,
        "/api/schedule",
        &[(
            "post",
            ok_json(
                body(op("Schedule a transaction", "Schedules"), "ScheduleRequest"),
                "CreatedId",
            ),
        )],
    );
    add(
        &mut paths,
        "/api/schedules",
        &[(
            "post",
            ok_json(
                body(op("List scheduled jobs", "Schedules"), "DbRequest"),
                "ScheduleList",
            ),
        )],
    );
    add(
        &mut paths,
        "/api/schedule/claim",
        &[(
            "post",
            ok_json(
                body(
                    op(
                        "Atomically claim due external jobs (app-worker mode)",
                        "Schedules",
                    ),
                    "ClaimSchedulesRequest",
                ),
                "ClaimedScheduleList",
            ),
        )],
    );
    for (suffix, summary) in [
        (
            "complete",
            "Mark an externally-executed job complete (fencing token required)",
        ),
        ("retry", "Re-arm an external job (fencing token required)"),
        (
            "fail",
            "Mark an external job failed (fencing token required)",
        ),
    ] {
        add(
            &mut paths,
            &format!("/api/schedule/{{id}}/{suffix}"),
            &[(
                "post",
                ok_json(
                    body(op(summary, "Schedules"), "FinalizeScheduleRequest"),
                    "OkResponse",
                ),
            )],
        );
    }
    for (suffix, summary) in [
        ("cancel", "Cancel a scheduled job (creator only)"),
        ("pause", "Pause a scheduled job (creator only)"),
        ("resume", "Resume a paused scheduled job (creator only)"),
    ] {
        add(
            &mut paths,
            &format!("/api/schedule/{{id}}/{suffix}"),
            &[(
                "post",
                ok_json(body(op(summary, "Schedules"), "DbRequest"), "OkResponse"),
            )],
        );
    }

    // Workflows (data plane)
    add(
        &mut paths,
        "/api/workflows",
        &[(
            "post",
            ok_json(
                body(
                    op("Start a workflow run", "Workflows"),
                    "StartWorkflowRequest",
                ),
                "CreatedId",
            ),
        )],
    );
    add(
        &mut paths,
        "/api/workflows/list",
        &[(
            "post",
            ok_json(
                body(
                    op("List workflow runs", "Workflows"),
                    "ListWorkflowsRequest",
                ),
                "WorkflowInfoList",
            ),
        )],
    );
    add(
        &mut paths,
        "/api/workflows/{id}/cancel",
        &[(
            "post",
            ok_json(
                body(op("Cancel a workflow run", "Workflows"), "DbRequest"),
                "CancelledResponse",
            ),
        )],
    );
    add(
        &mut paths,
        "/api/workflows/{id}/signal",
        &[(
            "post",
            ok_json(
                body(
                    op(
                        "Deliver a signal to a waiting run (latest-wins payload)",
                        "Workflows",
                    ),
                    "SignalWorkflowRequest",
                ),
                "DeliveredResponse",
            ),
        )],
    );

    // Schema + changes (data plane)
    add(
        &mut paths,
        "/api/db/{db}/schema",
        &[(
            "get",
            ok_json(
                op("The pushed SchemaDef for a database", "Schema"),
                "SchemaDef",
            ),
        )],
    );
    add(
        &mut paths,
        "/api/db/{db}/changes",
        &[(
            "get",
            ok_json(
                op(
                    "Resumable per-db change feed (machine tokens; ?since=&table=&limit=)",
                    "Changes",
                ),
                "ChangeFeedResponse",
            ),
        )],
    );

    // Storage (data plane)
    add(
        &mut paths,
        "/api/storage/{db}",
        &[(
            "post",
            op(
                "Upload a blob (body size capped by RTDB_MAX_FILE_SIZE)",
                "Storage",
            ),
        )],
    );
    add(
        &mut paths,
        "/api/storage/{db}/{id}",
        &[
            (
                "get",
                ok_raw(
                    op("Serve a blob (Range requests honored)", "Storage"),
                    "application/octet-stream",
                ),
            ),
            (
                "delete",
                ok_json(op("Delete a blob", "Storage"), "OkResponse"),
            ),
        ],
    );
    add(
        &mut paths,
        "/api/storage/{db}/{id}/metadata",
        &[(
            "get",
            ok_json(op("Blob metadata", "Storage"), "StorageMetadata"),
        )],
    );
    add(
        &mut paths,
        "/api/storage/{db}/{id}/signed-url",
        &[(
            "get",
            ok_json(
                op("Mint a signed, time-limited serve URL", "Storage"),
                "SignedUrl",
            ),
        )],
    );
    add(
        &mut paths,
        "/storage/{id}",
        &[(
            "get",
            ok_raw(
                op(
                    "Public, unauthenticated blob serve (Range requests honored)",
                    "Storage",
                ),
                "application/octet-stream",
            ),
        )],
    );

    // Share grants
    add(
        &mut paths,
        "/api/db/{db}/share-grants",
        &[(
            "post",
            ok_json(
                op("Mint a per-row public share grant", "Share"),
                "ShareGrant",
            ),
        )],
    );
    add(
        &mut paths,
        "/api/db/{db}/share-grants/{token}",
        &[(
            "delete",
            ok_json(op("Revoke a share grant", "Share"), "OkResponse"),
        )],
    );
    add(
        &mut paths,
        "/share/{token}",
        &[(
            "get",
            ok_json(
                op("Read exactly the granted row/blob (no auth)", "Share"),
                "SharedDoc",
            ),
        )],
    );
    add(
        &mut paths,
        "/share/{token}/signed-url",
        &[(
            "get",
            ok_json(
                op("Mint a signed serve URL for a granted blob", "Share"),
                "SignedUrl",
            ),
        )],
    );

    // --- Auth (auth/provider.rs) ------------------------------------------
    for provider in ["github", "google", "gitlab", "oidc", "microsoft", "apple"] {
        add(
            &mut paths,
            &format!("/auth/{provider}/begin"),
            &[(
                "get",
                op(
                    format!("Begin OAuth sign-in ({provider})",).as_str(),
                    "Auth",
                ),
            )],
        );
    }
    add(
        &mut paths,
        "/auth/callback",
        &[("get", op("GitHub OAuth callback", "Auth"))],
    );
    for provider in ["google", "gitlab", "oidc", "microsoft"] {
        add(
            &mut paths,
            &format!("/auth/{provider}/callback"),
            &[(
                "get",
                op(format!("OAuth callback ({provider})").as_str(), "Auth"),
            )],
        );
    }
    add(
        &mut paths,
        "/auth/apple/callback",
        &[("post", op("Apple OAuth callback (form_post)", "Auth"))],
    );
    add(
        &mut paths,
        "/auth/anonymous",
        &[(
            "post",
            op("Mint an ephemeral anonymous user + session", "Auth"),
        )],
    );
    add(
        &mut paths,
        "/auth/enroll",
        &[(
            "post",
            op(
                "Redeem a host machine-authorization enrollment grant",
                "Auth",
            ),
        )],
    );
    add(
        &mut paths,
        "/auth/state",
        &[("get", op("Poll popup sign-in state (?state=…)", "Auth"))],
    );
    add(
        &mut paths,
        "/auth/logout",
        &[("post", op("End the session", "Auth"))],
    );
    add(
        &mut paths,
        "/auth/me",
        &[("get", op("Current principal", "Auth"))],
    );
    add(
        &mut paths,
        "/auth/validate",
        &[("get", op("Validate a session token", "Auth"))],
    );

    // --- Admin control plane (admin/mod.rs) --------------------------------
    let admin_ops: &[(&str, &str, &str, &str)] = &[
        // (method, path, summary, tag)
        (
            "post",
            "/admin/login",
            "Admin-key sign-in (sets the session cookie)",
            "Admin",
        ),
        ("post", "/admin/logout", "End the admin session", "Admin"),
        ("post", "/admin/create-db", "Create a database", "Admin"),
        (
            "post",
            "/admin/delete-db",
            "Delete a database (typed confirm)",
            "Admin",
        ),
        (
            "post",
            "/admin/merge-users",
            "Merge an anonymous user into a real one (typed confirm)",
            "Admin",
        ),
        (
            "post",
            "/admin/push-schema",
            "Push a SchemaDef (additive DDL)",
            "Admin",
        ),
        ("get", "/admin/dbs", "List databases", "Admin"),
        (
            "post",
            "/admin/mint-token",
            "Mint a machine token (optional scoping/expiry)",
            "Admin",
        ),
        (
            "post",
            "/admin/revoke-token",
            "Revoke a machine token",
            "Admin",
        ),
        ("get", "/admin/tokens", "List machine tokens", "Admin"),
        (
            "post",
            "/admin/enrollment-grants",
            "Create a host machine-authorization grant",
            "Admin",
        ),
        (
            "post",
            "/admin/revoke-enrollment-grant",
            "Revoke a host enrollment grant",
            "Admin",
        ),
        (
            "get",
            "/admin/allowlist",
            "List the host machine allowlist",
            "Admin",
        ),
        (
            "post",
            "/admin/allowlist",
            "Write the host machine allowlist",
            "Admin",
        ),
        ("get", "/admin/admins", "List admin users", "Admin"),
        ("post", "/admin/admins", "Add an admin user", "Admin"),
        ("delete", "/admin/admins", "Remove an admin user", "Admin"),
        ("get", "/admin/metrics", "Metrics snapshot as JSON", "Admin"),
        (
            "get",
            "/admin/presence",
            "Presence room inspector (per replica)",
            "Admin",
        ),
        (
            "get",
            "/admin/slow-queries",
            "Bounded slow-query ring",
            "Admin",
        ),
        ("get", "/admin/config", "Hot config read-back", "Admin"),
        (
            "patch",
            "/admin/config",
            "Patch hot config (live, no restart)",
            "Admin",
        ),
        (
            "post",
            "/admin/backup",
            "Start a backup (202; 409 if one is in progress)",
            "Admin Backups",
        ),
        ("get", "/admin/backups", "List backups", "Admin Backups"),
        (
            "get",
            "/admin/backups/{name}",
            "Download a backup archive",
            "Admin Backups",
        ),
        (
            "delete",
            "/admin/backups/{name}",
            "Delete a backup",
            "Admin Backups",
        ),
        (
            "post",
            "/admin/restore",
            "Restore a backup into a fresh rtdb_restored_<stamp> db",
            "Admin Backups",
        ),
        (
            "get",
            "/admin/export-db",
            "Stream a database as NDJSON",
            "Admin",
        ),
        (
            "post",
            "/admin/import-db",
            "Import a database export",
            "Admin",
        ),
        (
            "post",
            "/admin/clone-db",
            "Copy a database (schema + documents)",
            "Admin",
        ),
        ("get", "/admin/ops/recent", "Recent op-feed events", "Admin"),
        ("get", "/admin/audit", "Recent audit entries", "Admin"),
        (
            "get",
            "/admin/subscriptions",
            "Live subscription inspector",
            "Admin",
        ),
        ("get", "/admin/stream", "Op-feed WebSocket (admin)", "Admin"),
        (
            "get",
            "/admin/sessions",
            "List interactive sessions (?user=&limit=)",
            "Admin",
        ),
        (
            "delete",
            "/admin/sessions",
            "Revoke interactive sessions (one, all, or expired sweep)",
            "Admin",
        ),
        (
            "delete",
            "/admin/sessions/{token_hash}",
            "Revoke one interactive session",
            "Admin",
        ),
        (
            "post",
            "/admin/db/{db}/query",
            "Owner-bypass query",
            "Admin",
        ),
        (
            "post",
            "/admin/db/{db}/mutate",
            "Owner-bypass mutate",
            "Admin",
        ),
        (
            "get",
            "/admin/db/{db}/changes",
            "Change feed under the admin session",
            "Admin",
        ),
        (
            "post",
            "/admin/db/{db}/migrate",
            "Run data migrations",
            "Admin",
        ),
        (
            "get",
            "/admin/dbs/{db}/schema",
            "The db's SchemaDef",
            "Admin Schema",
        ),
        (
            "get",
            "/admin/db/{db}/schema/json-schema",
            "Pushed schema as JSON Schema (draft 2020-12)",
            "Admin Schema",
        ),
        (
            "post",
            "/admin/db/{db}/schema/preview",
            "Advisory schema diff (applies nothing)",
            "Admin Schema",
        ),
        (
            "get",
            "/admin/db/{db}/schema/history",
            "Schema change history",
            "Admin Schema",
        ),
        (
            "get",
            "/admin/db/{db}/schema/history/{version}",
            "One schema history version",
            "Admin Schema",
        ),
        (
            "post",
            "/admin/db/{db}/schema/restore",
            "Restore a schema version",
            "Admin Schema",
        ),
        ("get", "/admin/dbs/{db}/stats", "Per-db stats", "Admin"),
        (
            "post",
            "/admin/db/{db}/explain",
            "Compile a query without executing (SQL + warnings)",
            "Admin",
        ),
        (
            "get",
            "/admin/db/{db}/anonymous-access",
            "Per-db anonymous-access toggle",
            "Admin",
        ),
        (
            "patch",
            "/admin/db/{db}/anonymous-access",
            "Set the per-db anonymous-access toggle",
            "Admin",
        ),
        (
            "get",
            "/admin/db/{db}/readonly",
            "Per-db read-only freeze state",
            "Admin",
        ),
        (
            "patch",
            "/admin/db/{db}/readonly",
            "Set the per-db read-only freeze",
            "Admin",
        ),
        (
            "get",
            "/admin/db/{db}/storage",
            "List db blobs",
            "Admin Storage",
        ),
        (
            "post",
            "/admin/db/{db}/storage",
            "Upload a blob into a db",
            "Admin Storage",
        ),
        (
            "delete",
            "/admin/db/{db}/storage/{id}",
            "Delete a db blob",
            "Admin Storage",
        ),
        (
            "get",
            "/admin/db/{db}/webhooks",
            "List webhooks",
            "Admin Webhooks",
        ),
        (
            "post",
            "/admin/db/{db}/webhooks",
            "Create a webhook",
            "Admin Webhooks",
        ),
        (
            "put",
            "/admin/db/{db}/webhooks/{id}",
            "Edit a webhook",
            "Admin Webhooks",
        ),
        (
            "delete",
            "/admin/db/{db}/webhooks/{id}",
            "Delete a webhook",
            "Admin Webhooks",
        ),
        (
            "get",
            "/admin/db/{db}/webhooks/{id}/deliveries",
            "List webhook deliveries",
            "Admin Webhooks",
        ),
        (
            "post",
            "/admin/db/{db}/webhooks/{id}/deliveries/{delivery_id}/redeliver",
            "Redeliver one delivery",
            "Admin Webhooks",
        ),
        (
            "post",
            "/admin/db/{db}/webhooks/{id}/deliveries/redeliver-failed",
            "Redeliver all failed deliveries",
            "Admin Webhooks",
        ),
        (
            "get",
            "/admin/db/{db}/schedules",
            "List scheduled jobs",
            "Admin Schedules",
        ),
        (
            "post",
            "/admin/db/{db}/schedules",
            "Create a scheduled job",
            "Admin Schedules",
        ),
        (
            "post",
            "/admin/db/{db}/schedules/{id}/cancel",
            "Cancel a scheduled job",
            "Admin Schedules",
        ),
        (
            "post",
            "/admin/db/{db}/schedules/{id}/pause",
            "Pause a scheduled job",
            "Admin Schedules",
        ),
        (
            "post",
            "/admin/db/{db}/schedules/{id}/resume",
            "Resume a scheduled job",
            "Admin Schedules",
        ),
        (
            "get",
            "/admin/db/{db}/workflows",
            "List workflow runs",
            "Admin Workflows",
        ),
        (
            "post",
            "/admin/db/{db}/workflows",
            "Start a workflow run",
            "Admin Workflows",
        ),
        (
            "get",
            "/admin/db/{db}/workflows/{id}",
            "One workflow run (with outcome trail)",
            "Admin Workflows",
        ),
        (
            "delete",
            "/admin/db/{db}/workflows/{id}",
            "Delete a workflow run",
            "Admin Workflows",
        ),
        (
            "post",
            "/admin/db/{db}/workflows/{id}/cancel",
            "Cancel a workflow run",
            "Admin Workflows",
        ),
        (
            "post",
            "/admin/db/{db}/workflows/{id}/signal",
            "Signal a waiting run",
            "Admin Workflows",
        ),
    ];
    for (method, path, summary, tag) in admin_ops {
        add(&mut paths, path, &[(method, op(summary, tag))]);
    }

    json!({
        "openapi": "3.1.0",
        "info": {
            "title": "par-rt-db",
            "version": "0.1.0",
            "description": "Self-hosted realtime document database. The full HTTP surface is documented here; \
                            the WebSocket transport at /sync shares the same vocabulary (see README.md). \
                            Every error response uses the RtDbError envelope {code, message}."
        },
        "tags": [
            {"name": "Health"}, {"name": "Realtime"}, {"name": "Query"}, {"name": "Mutate"},
            {"name": "Schedules"}, {"name": "Workflows"}, {"name": "Schema"}, {"name": "Changes"},
            {"name": "Storage"}, {"name": "Share"}, {"name": "Auth"}, {"name": "Admin"},
            {"name": "Admin Schema"}, {"name": "Admin Storage"}, {"name": "Admin Webhooks"},
            {"name": "Admin Schedules"}, {"name": "Admin Workflows"}, {"name": "Admin Backups"}
        ],
        "paths": Value::Object(paths),
        "components": components(),
    })
}

/// One operation with the standard error responses.
fn op(summary: &str, tag: &str) -> Value {
    json!({
        "summary": summary,
        "tags": [tag],
        "responses": {
            "200": {"description": "Success"},
            "4XX": {"$ref": "#/components/responses/Error"},
            "default": {"$ref": "#/components/responses/Error"}
        }
    })
}

/// Add a JSON request body to an operation.
fn body(mut o: Value, schema: &str) -> Value {
    o["requestBody"] = json!({
        "required": true,
        "content": {"application/json": {"schema": {"$ref": schema_ref(schema)}}}
    });
    o
}

/// Point an operation's 200 at a JSON schema.
fn ok_json(mut o: Value, schema: &str) -> Value {
    o["responses"]["200"] = json!({
        "description": "Success",
        "content": {"application/json": {"schema": {"$ref": schema_ref(schema)}}}
    });
    o
}

/// Point an operation's 200 at a non-JSON content type.
fn ok_raw(mut o: Value, content_type: &str) -> Value {
    o["responses"]["200"] = json!({
        "description": "Success",
        "content": {content_type: {}}
    });
    o
}

fn schema_ref(name: &str) -> String {
    format!("#/components/schemas/{name}")
}

/// Insert methods into a path item, auto-generating the required path
/// parameters for every `{name}` template segment.
fn add(paths: &mut Map<String, Value>, path: &str, methods: &[(&str, Value)]) {
    let entry = paths.entry(path.to_string()).or_insert_with(|| {
        let mut item = Map::new();
        let names: Vec<&str> = path
            .split(['{', '}'])
            .skip(1)
            .step_by(2)
            .filter(|s| !s.is_empty())
            .collect();
        if !names.is_empty() {
            item.insert(
                "parameters".into(),
                json!(
                    names
                        .iter()
                        .map(|n| json!({
                            "name": n,
                            "in": "path",
                            "required": true,
                            "schema": {"type": "string"}
                        }))
                        .collect::<Vec<_>>()
                ),
            );
        }
        Value::Object(item)
    });
    for (method, o) in methods {
        entry[method.to_owned()] = o.to_owned();
    }
}

/// Reusable schemas. Core data-plane shapes carry full field/required detail
/// (mirroring the wire types); the open-ended DSL objects (Query, Step,
/// SchemaDef, …) are described rather than enumerated — README.md is the
/// authoritative shape reference for those. Split into grouped helpers because
/// a single `json!` literal this deep overflows the macro's recursion limit.
fn components() -> Value {
    let mut schemas = Map::new();
    for group in [
        error_and_health_schemas(),
        query_schemas(),
        mutate_schemas(),
        schedule_schemas(),
        workflow_schemas(),
        surface_schemas(),
    ] {
        let Value::Object(obj) = group else {
            unreachable!("schema group helpers return objects");
        };
        schemas.extend(obj);
    }
    json!({
        "schemas": Value::Object(schemas),
        "responses": {
            "Error": {
                "description": "The RtDbError envelope",
                "content": {"application/json": {"schema": {"$ref": "#/components/schemas/Error"}}}
            }
        }
    })
}

fn string_schema() -> Value {
    json!({"type": "string"})
}

fn int_schema() -> Value {
    json!({"type": "integer"})
}

fn error_and_health_schemas() -> Value {
    json!({
        "Error": {
            "type": "object",
            "description": "The RtDbError wire envelope; every failure — HTTP and WS alike — uses it. `code` is an ErrorCode value (see README's Error envelope section).",
            "required": ["code", "message"],
            "properties": {"code": string_schema(), "message": string_schema()}
        },
        "Healthz": {
            "type": "object",
            "required": ["status"],
            "properties": {
                "status": {"enum": ["ok", "degraded"]},
                "started_at": int_schema(), "uptime_seconds": int_schema(),
                "postgres": {"type": "boolean"},
                "version": string_schema(), "git_commit": string_schema(), "build_timestamp": string_schema()
            }
        },
        "OpenApiDocument": {
            "type": "object",
            "description": "This OpenAPI 3.1 document."
        }
    })
}

fn query_schemas() -> Value {
    json!({
        "Query": {"type": "object", "description": "The declarative query DSL — see README's Query shape section."},
        "QueryResult": {"type": "object", "description": "Terminal result (rows / count / aggregate / pagination cursor) — see README's Query shape section."},
        "QueryRequest": {
            "type": "object", "required": ["db", "query"],
            "properties": {"db": string_schema(), "query": {"$ref": "#/components/schemas/Query"}}
        },
        "QueryResponse": {
            "type": "object", "required": ["result"],
            "properties": {"result": {"$ref": "#/components/schemas/QueryResult"}}
        },
        "BatchQueryRequest": {
            "type": "object", "required": ["db", "queries"],
            "properties": {"db": string_schema(), "queries": {"type": "array", "items": {"$ref": "#/components/schemas/Query"}}}
        },
        "BatchQueryOutcome": {
            "type": "object", "required": ["ok"],
            "description": "Positionally aligned slot; exactly one of result/error accompanies ok.",
            "properties": {
                "ok": {"type": "boolean"},
                "result": {"$ref": "#/components/schemas/QueryResult"},
                "error": {"$ref": "#/components/schemas/Error"}
            }
        },
        "BatchQueryOutcomeList": {"type": "array", "items": {"$ref": "#/components/schemas/BatchQueryOutcome"}}
    })
}

fn mutate_schemas() -> Value {
    json!({
        "Transaction": {
            "type": "object", "required": ["steps"],
            "properties": {"steps": {"type": "array", "items": {"$ref": "#/components/schemas/Step"}}}
        },
        "Step": {"type": "object", "description": "One declarative step (insert/patch/delete/… ) — see README's Transaction shape section."},
        "MutateRequest": {
            "type": "object", "required": ["db", "txn"],
            "properties": {
                "db": string_schema(),
                "txn": {"$ref": "#/components/schemas/Transaction"},
                "idempotencyKey": string_schema()
            }
        },
        "MutateResponse": {
            "type": "object", "required": ["results"],
            "properties": {"results": {"type": "array", "items": {"type": "object"}}}
        },
        "BatchMutateEntry": {
            "type": "object", "required": ["txn"],
            "properties": {"txn": {"$ref": "#/components/schemas/Transaction"}, "idempotencyKey": string_schema()}
        },
        "BatchMutateRequest": {
            "type": "object", "required": ["db", "txns"],
            "properties": {"db": string_schema(), "txns": {"type": "array", "items": {"$ref": "#/components/schemas/BatchMutateEntry"}}}
        },
        "BatchMutateOutcome": {
            "type": "object", "required": ["ok"],
            "description": "Positionally aligned slot; exactly one of results/error accompanies ok.",
            "properties": {
                "ok": {"type": "boolean"},
                "results": {"type": "array", "items": {"type": "object"}},
                "error": {"$ref": "#/components/schemas/Error"}
            }
        },
        "BatchMutateOutcomeList": {"type": "array", "items": {"$ref": "#/components/schemas/BatchMutateOutcome"}}
    })
}

fn schedule_schemas() -> Value {
    json!({
        "ScheduleWhen": {
            "description": "Tagged union (`type`): one-shot delay, absolute instant, 5-field cron (optional tz), or fixed interval.",
            "oneOf": [
                {"type": "object", "required": ["type", "ms"], "properties": {"type": {"enum": ["afterMs"]}, "ms": int_schema()}},
                {"type": "object", "required": ["type", "ms"], "properties": {"type": {"enum": ["runAt"]}, "ms": int_schema()}},
                {"type": "object", "required": ["type", "expr"], "properties": {"type": {"enum": ["cron"]}, "expr": string_schema(), "tz": string_schema()}},
                {"type": "object", "required": ["type", "everyMs"], "properties": {"type": {"enum": ["interval"]}, "everyMs": int_schema()}}
            ]
        },
        "ScheduleRequest": {
            "type": "object", "required": ["db", "when", "txn"],
            "properties": {
                "db": string_schema(),
                "when": {"$ref": "#/components/schemas/ScheduleWhen"},
                "txn": {"$ref": "#/components/schemas/Transaction"},
                "external": {"type": "boolean", "description": "External-claim job mode: never internally executed; claim via /api/schedule/claim."}
            }
        },
        "CreatedId": {"type": "object", "required": ["id"], "properties": {"id": string_schema()}},
        "DbRequest": {"type": "object", "required": ["db"], "properties": {"db": string_schema()}},
        "OkResponse": {"type": "object", "required": ["ok"], "properties": {"ok": {"type": "boolean"}}},
        "ClaimSchedulesRequest": {
            "type": "object", "required": ["db"],
            "properties": {"db": string_schema(), "limit": int_schema(), "leaseMs": int_schema()}
        },
        "ClaimedSchedule": {
            "type": "object",
            "required": ["id", "kind", "dueAt", "txn", "leaseGeneration", "leaseDeadlineMs"],
            "properties": {
                "id": string_schema(),
                "kind": {"enum": ["oneshot", "cron", "interval"]},
                "dueAt": int_schema(),
                "txn": {"$ref": "#/components/schemas/Transaction"},
                "cron": string_schema(), "everyMs": int_schema(),
                "leaseGeneration": int_schema(), "leaseDeadlineMs": int_schema()
            }
        },
        "ClaimedScheduleList": {"type": "array", "items": {"$ref": "#/components/schemas/ClaimedSchedule"}},
        "FinalizeScheduleRequest": {
            "type": "object", "required": ["db", "lease"],
            "properties": {"db": string_schema(), "lease": int_schema(), "delayMs": int_schema(), "error": string_schema()}
        },
        "ScheduleInfo": {"type": "object", "description": "One scheduled job's state row."},
        "ScheduleList": {"type": "object", "required": ["schedules"], "properties": {"schedules": {"type": "array", "items": {"$ref": "#/components/schemas/ScheduleInfo"}}}}
    })
}

fn workflow_schemas() -> Value {
    json!({
        "WorkflowSpec": {"type": "object", "description": "Workflow spec: ordered steps (txn or awaitSignal), retry + sleep policy — see README's Workflows section."},
        "StartWorkflowRequest": {
            "type": "object", "required": ["db", "spec"],
            "properties": {"db": string_schema(), "spec": {"$ref": "#/components/schemas/WorkflowSpec"}}
        },
        "WorkflowInfo": {"type": "object", "description": "Run projection: id, name, status, currentStep/stepCount, attempts, waitingFor/waitedSince, created/updated/started/finishedAt."},
        "WorkflowInfoList": {"type": "object", "required": ["workflows"], "properties": {"workflows": {"type": "array", "items": {"$ref": "#/components/schemas/WorkflowInfo"}}}},
        "CancelledResponse": {"type": "object", "required": ["cancelled"], "properties": {"cancelled": {"type": "boolean"}}},
        "SignalWorkflowRequest": {
            "type": "object", "required": ["db", "name"],
            "properties": {"db": string_schema(), "name": string_schema(), "payload": {"type": "object"}}
        },
        "DeliveredResponse": {"type": "object", "required": ["delivered"], "properties": {"delivered": {"type": "boolean"}}}
    })
}

fn surface_schemas() -> Value {
    json!({
        "SchemaDef": {"type": "object", "description": "The pushed schema: tables → fields (typed: string/number/boolean/null/id/literal/optional/union/array/object), indexes, softDelete, ownerField/collaboratorsField/authorize — see README's Schema section."},
        "ChangeFeedResponse": {
            "type": "object",
            "required": ["ops", "nextSeq", "head", "logId"],
            "properties": {
                "ops": {"type": "array", "items": {"$ref": "#/components/schemas/ChangeRow"}},
                "nextSeq": int_schema(), "head": int_schema(), "logId": string_schema()
            }
        },
        "ChangeRow": {"type": "object", "description": "One durable op-feed row (seq, table, docId, op, doc snapshot)."},
        "StorageMetadata": {"type": "object", "description": "Blob metadata: id, db, filename, content type, size, timestamps."},
        "SignedUrl": {"type": "object", "required": ["url"], "properties": {"url": string_schema(), "expiresAt": int_schema()}},
        "ShareGrant": {"type": "object", "required": ["token"], "properties": {"token": string_schema(), "url": string_schema()}},
        "SharedDoc": {"type": "object", "description": "Exactly the granted row or blob, nothing else."}
    })
}
