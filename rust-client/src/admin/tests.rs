use super::RtDbAdminClient;
use crate::error::ErrorCode;
use crate::mutation::Mutation;
use crate::query::TableQuery;
use crate::schema::{FieldType, SchemaBuilderExt, SchemaDef, Table};
use serde_json::{Value, json};
use wiremock::matchers::{
    body_bytes, body_partial_json, body_string_contains, header, method, path, query_param,
};
use wiremock::{Mock, MockServer, ResponseTemplate};

// Admin methods authorize via the bearer, so the token passed to the client
// is the instance admin key here — same field as every other call.
const BEARER: &str = "Bearer admin-key";

async fn setup() -> (MockServer, RtDbAdminClient) {
    let server = MockServer::start().await;
    let client = RtDbAdminClient::new(server.uri().as_str(), "admin-key");
    (server, client)
}

#[tokio::test]
async fn create_db_posts_name() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/create-db"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({"name": "kanban"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client.create_db("kanban").await.unwrap();
}

#[tokio::test]
async fn delete_db_posts_name_and_confirm() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/delete-db"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(
            json!({"name": "kanban", "confirm": "kanban"}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client.delete_db("kanban", "kanban").await.unwrap();
}

#[tokio::test]
async fn delete_db_surfaces_confirmation_mismatch_envelope() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/delete-db"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "code": "BAD_REQUEST",
            "message": "confirmation does not match database name"
        })))
        .mount(&server)
        .await;
    let err = client.delete_db("kanban", "wrong").await.unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert_eq!(err.message, "confirmation does not match database name");
}

#[tokio::test]
async fn delete_db_surfaces_unknown_database_envelope() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/delete-db"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "code": "NOT_FOUND",
            "message": "database not found"
        })))
        .mount(&server)
        .await;
    let err = client.delete_db("missing", "missing").await.unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
    assert_eq!(err.message, "database not found");
}

#[tokio::test]
async fn push_schema_serializes_schema_json() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/push-schema"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({
            "db": "kanban",
            "schema": {"tables": {"notes": {"fields": {"body": {"type": "string"}}}}}
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    let schema = SchemaDef::builder()
        .table("notes", Table::new().field("body", FieldType::String))
        .build();
    client.push_schema("kanban", &schema).await.unwrap();
}

#[tokio::test]
async fn list_dbs_returns_databases() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/dbs"))
        .and(header("authorization", BEARER))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"databases": ["kanban", "demo"]})),
        )
        .mount(&server)
        .await;
    let dbs = client.list_dbs().await.unwrap();
    assert_eq!(dbs, vec!["kanban".to_string(), "demo".to_string()]);
}

#[tokio::test]
async fn mint_token_returns_token_id_and_token() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/mint-token"))
        .and(body_partial_json(json!({"db": "kanban", "name": "cli"})))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"tokenId": "id1", "token": "secret"})),
        )
        .mount(&server)
        .await;
    let minted = client.mint_token("kanban", "cli").await.unwrap();
    assert_eq!(minted.token_id, "id1");
    assert_eq!(minted.token, "secret");
}

#[tokio::test]
async fn revoke_token_posts_token_id() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/revoke-token"))
        .and(body_partial_json(json!({"tokenId": "tid"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client.revoke_token("tid").await.unwrap();
}

#[tokio::test]
async fn allowlist_add_posts_action() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/allowlist"))
        .and(body_partial_json(
            json!({"db": "kanban", "action": "add", "email": "a@b.com"}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client.allowlist_add("kanban", "a@b.com").await.unwrap();
}

#[tokio::test]
async fn allowlist_remove_posts_action() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/allowlist"))
        .and(body_partial_json(
            json!({"db": "kanban", "action": "remove", "email": "a@b.com"}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client.allowlist_remove("kanban", "a@b.com").await.unwrap();
}

#[tokio::test]
async fn allowlist_list_uses_query_string() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/allowlist"))
        .and(query_param("db", "kanban"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"emails": ["a@b.com"]})))
        .mount(&server)
        .await;
    let emails = client.allowlist_list("kanban").await.unwrap();
    assert_eq!(emails, vec!["a@b.com".to_string()]);
}

#[tokio::test]
async fn admins_list_unwraps_admins_array() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/admins"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"admins":[{"email":"a@x.com","githubId":1},{"email":"b@x.com"}]}),
        ))
        .mount(&server)
        .await;
    let rows = client.admins_list().await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].email, "a@x.com");
    assert_eq!(rows[0].github_id, Some(1));
    assert_eq!(rows[1].github_id, None);
}

#[tokio::test]
async fn admins_add_posts_email_and_optional_github_id() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/admins"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({"email":"a@x.com","githubId":7})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client.admins_add("a@x.com", Some(7)).await.unwrap();
}

#[tokio::test]
async fn admins_remove_uses_delete_with_body() {
    let (server, client) = setup().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/admins"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({"email":"a@x.com"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client.admins_remove("a@x.com").await.unwrap();
}

#[tokio::test]
async fn export_db_returns_jsonl_text() {
    let (server, client) = setup().await;
    let jsonl = "{\"kind\":\"schema\",\"schema\":{\"tables\":{}}}\n";
    Mock::given(method("GET"))
        .and(path("/admin/export-db"))
        .and(query_param("db", "kanban"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_string(jsonl))
        .mount(&server)
        .await;
    let got = client.export_db("kanban").await.unwrap();
    assert_eq!(got, jsonl);
}

#[tokio::test]
async fn export_db_surfaces_error_envelope() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/export-db"))
        .respond_with(
            ResponseTemplate::new(404)
                .set_body_json(json!({"code": "NOT_FOUND", "message": "unknown database"})),
        )
        .mount(&server)
        .await;
    let err = client.export_db("missing").await.unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
    assert_eq!(err.message, "unknown database");
}

#[tokio::test]
async fn import_db_posts_ndjson_body() {
    let (server, client) = setup().await;
    let jsonl = "{\"kind\":\"schema\",\"schema\":{\"tables\":{}}}\n";
    Mock::given(method("POST"))
        .and(path("/admin/import-db"))
        .and(query_param("db", "kanban"))
        .and(header("content-type", "application/x-ndjson"))
        .and(body_string_contains(jsonl))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client.import_db("kanban", jsonl).await.unwrap();
}

#[tokio::test]
async fn get_schema_returns_schema_def() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/dbs/kanban/schema"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tables": {"notes": {"fields": {"body": {"type": "string"}}}}
        })))
        .mount(&server)
        .await;
    let schema = client.get_schema("kanban").await.unwrap();
    assert_eq!(schema.tables.len(), 1);
    assert!(schema.tables.contains_key("notes"));
}

#[tokio::test]
async fn schema_history_lists_summaries() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/db/kanban/schema/history"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "entries": [
                {"version": 3, "capturedAt": 30, "source": "migrate", "principal": "u@x"},
                {"version": 2, "capturedAt": 20, "source": "push", "principal": null}
            ]
        })))
        .mount(&server)
        .await;
    let entries = client
        .schema_history("kanban", Some(5), None)
        .await
        .unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].version, 3);
    assert_eq!(entries[0].source, "migrate");
    assert_eq!(entries[0].principal.as_deref(), Some("u@x"));
    assert!(entries[1].principal.is_none());
}

#[tokio::test]
async fn schema_history_get_returns_entry_with_schema_blob() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/db/kanban/schema/history/3"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "version": 3,
            "capturedAt": 30,
            "source": "restore",
            "principal": null,
            "schema": {"tables": {"notes": {"fields": {"body": {"type": "string"}}}}}
        })))
        .mount(&server)
        .await;
    let entry = client.schema_history_get("kanban", 3).await.unwrap();
    assert_eq!(entry.version, 3);
    assert_eq!(entry.source, "restore");
    assert_eq!(
        entry.schema["tables"]["notes"]["fields"]["body"]["type"],
        "string"
    );
}

#[tokio::test]
async fn restore_schema_posts_version_and_confirm() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/schema/restore"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(
            json!({"version": 2, "confirm": "kanban"}),
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"ok": true, "restoredTo": 2})),
        )
        .mount(&server)
        .await;
    let restored_to = client.restore_schema("kanban", 2, "kanban").await.unwrap();
    assert_eq!(restored_to, 2);
}

#[tokio::test]
async fn db_stats_returns_table_stats() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/dbs/kanban/stats"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tables": [{"name":"notes","rowCount":5,"sizeBytes":100}],
            "totalSizeBytes": 100,
            "tablesQuota": 10,
            "tablesUsed": 2,
            "storageQuotaBytes": 1048576,
            "storageUsedBytes": 100,
            "subsQuota": 50,
            "subsUsed": 3
        })))
        .mount(&server)
        .await;
    let stats = client.db_stats("kanban").await.unwrap();
    assert_eq!(stats.total_size_bytes, 100);
    assert_eq!(stats.tables.len(), 1);
    assert_eq!(stats.tables[0].name, "notes");
    assert_eq!(stats.tables[0].row_count, 5);
    assert_eq!(stats.tables[0].size_bytes, 100);
    assert_eq!(stats.tables_quota, 10);
    assert_eq!(stats.tables_used, 2);
    assert_eq!(stats.storage_quota_bytes, 1048576);
    assert_eq!(stats.storage_used_bytes, 100);
    assert_eq!(stats.subs_quota, 50);
    assert_eq!(stats.subs_used, 3);
}

#[tokio::test]
async fn list_tokens_returns_token_info() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/tokens"))
        .and(query_param("db", "kanban"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tokens": [
                {"id":"id1","name":"cli","createdAt":123,"revoked":false},
                {"id":"id2","name":"ci","createdAt":999,"revoked":true}
            ]
        })))
        .mount(&server)
        .await;
    let tokens = client.list_tokens("kanban").await.unwrap();
    assert_eq!(tokens.len(), 2);
    assert_eq!(tokens[0].id, "id1");
    assert_eq!(tokens[0].name, "cli");
    assert_eq!(tokens[0].created_at, 123);
    assert!(!tokens[0].revoked);
    assert!(tokens[1].revoked);
}

#[tokio::test]
async fn mint_token_with_options_posts_capabilities() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/mint-token"))
        .and(body_partial_json(json!({
            "db": "kanban",
            "name": "scraper",
            "expiresAt": 1700000000000_i64,
            "readOnly": true,
            "tables": ["users"],
        })))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"tokenId": "id2", "token": "s2"})),
        )
        .mount(&server)
        .await;
    let minted = client
        .mint_token_with_options(
            "kanban",
            "scraper",
            &crate::wire::admin::MintTokenOptions {
                expires_at: Some(1700000000000),
                read_only: Some(true),
                tables: Some(vec!["users".to_string()]),
            },
        )
        .await
        .unwrap();
    assert_eq!(minted.token_id, "id2");
    assert_eq!(minted.token, "s2");
}

#[tokio::test]
async fn mint_token_omits_unset_capabilities() {
    // Backward compat: an unset MintTokenOptions (Default) must serialize
    // to exactly {db, name} — no capability keys leak, so server defaults
    // (full access) apply. Verifies `skip_serializing_if = "Option::is_none"`.
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/mint-token"))
        .and(body_partial_json(json!({"db": "kanban", "name": "cli"})))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"tokenId": "id1", "token": "secret"})),
        )
        .mount(&server)
        .await;
    let minted = client
        .mint_token_with_options(
            "kanban",
            "cli",
            &crate::wire::admin::MintTokenOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(minted.token_id, "id1");
    assert_eq!(minted.token, "secret");
}

#[tokio::test]
async fn list_tokens_returns_capability_fields() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/tokens"))
        .and(query_param("db", "kanban"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tokens": [
                {
                    "id": "t1",
                    "name": "ci",
                    "createdAt": 1,
                    "revoked": false,
                    "expiresAt": null,
                    "readOnly": false,
                    "tables": null
                },
                {
                    "id": "t2",
                    "name": "scraper",
                    "createdAt": 2,
                    "revoked": false,
                    "expiresAt": 1700000000000_i64,
                    "readOnly": true,
                    "tables": ["users"]
                }
            ]
        })))
        .mount(&server)
        .await;
    let tokens = client.list_tokens("kanban").await.unwrap();
    assert_eq!(tokens.len(), 2);
    // Full-access token: null/None/false means "no limit".
    assert_eq!(tokens[0].expires_at, None);
    assert!(!tokens[0].read_only);
    assert_eq!(tokens[0].tables, None);
    // Restricted token: every capability field populated.
    assert_eq!(tokens[1].expires_at, Some(1700000000000));
    assert!(tokens[1].read_only);
    assert_eq!(tokens[1].tables, Some(vec!["users".to_string()]));
}

#[tokio::test]
async fn list_tokens_deserializes_legacy_fixture() {
    // Older servers that don't send the capability fields must still
    // deserialize (defaults: expires_at=None, read_only=false, tables=None).
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/tokens"))
        .and(query_param("db", "kanban"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tokens": [
                {"id":"old","name":"legacy","createdAt":42,"revoked":false}
            ]
        })))
        .mount(&server)
        .await;
    let tokens = client.list_tokens("kanban").await.unwrap();
    assert_eq!(tokens.len(), 1);
    assert_eq!(tokens[0].id, "old");
    assert_eq!(tokens[0].expires_at, None);
    assert!(!tokens[0].read_only);
    assert_eq!(tokens[0].tables, None);
}

/// Wire-parity for `MintTokenRequest`: camelCase keys, and `None`
/// capability fields are omitted entirely (not `null`) so server defaults
/// apply. Mirrors the ts-client `mintToken` body-shape assertion.
#[test]
fn mint_token_request_serializes_wire_shape() {
    use crate::wire::admin::MintTokenRequest;

    // Full-access: every capability None → body is exactly {db, name}.
    let full = MintTokenRequest {
        db: "kanban",
        name: "cli",
        expires_at: None,
        read_only: None,
        tables: None,
    };
    assert_eq!(
        serde_json::to_value(&full).unwrap(),
        json!({"db": "kanban", "name": "cli"}),
    );

    // Restricted: every capability set → camelCase keys present.
    let tables = vec!["users".to_string(), "notes".to_string()];
    let scoped = MintTokenRequest {
        db: "dbx",
        name: "scraper",
        expires_at: Some(1700000000000),
        read_only: Some(true),
        tables: Some(&tables),
    };
    assert_eq!(
        serde_json::to_value(&scoped).unwrap(),
        json!({
            "db": "dbx",
            "name": "scraper",
            "expiresAt": 1700000000000_i64,
            "readOnly": true,
            "tables": ["users", "notes"],
        }),
    );

    // Partial: only one capability set → only that key appears.
    let partial = MintTokenRequest {
        db: "dbx",
        name: "reader",
        expires_at: None,
        read_only: Some(true),
        tables: None,
    };
    assert_eq!(
        serde_json::to_value(&partial).unwrap(),
        json!({"db": "dbx", "name": "reader", "readOnly": true}),
    );
}

/// Wire-parity for `TokenInfo`: deserializes both the restricted fixture
/// (`expiresAt:number, readOnly:true, tables:[...]`) and the full-access
/// fixture (`expiresAt:null, readOnly:false, tables:null`), plus the
/// legacy fixture (capability fields absent → defaults).
#[test]
fn token_info_deserializes_wire_shapes() {
    use crate::wire::admin::TokenInfo;

    let restricted: TokenInfo = serde_json::from_value(json!({
        "id": "t2",
        "name": "scraper",
        "createdAt": 2,
        "revoked": false,
        "expiresAt": 1700000000000_i64,
        "readOnly": true,
        "tables": ["users"],
    }))
    .unwrap();
    assert_eq!(restricted.expires_at, Some(1700000000000));
    assert!(restricted.read_only);
    assert_eq!(restricted.tables, Some(vec!["users".to_string()]));

    let full: TokenInfo = serde_json::from_value(json!({
        "id": "t1",
        "name": "ci",
        "createdAt": 1,
        "revoked": false,
        "expiresAt": null,
        "readOnly": false,
        "tables": null,
    }))
    .unwrap();
    assert_eq!(full.expires_at, None);
    assert!(!full.read_only);
    assert_eq!(full.tables, None);

    let legacy: TokenInfo = serde_json::from_value(json!({
        "id": "old",
        "name": "legacy",
        "createdAt": 42,
        "revoked": false,
    }))
    .unwrap();
    assert_eq!(legacy.expires_at, None);
    assert!(!legacy.read_only);
    assert_eq!(legacy.tables, None);
}

#[tokio::test]
async fn metrics_returns_snapshot() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/metrics"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "queriesTotal": 10,
            "mutationsTotal": 20,
            "uploadsTotal": 30,
            "wsConnections": 40,
            "activeSubscriptions": 50,
            "poolSize": 60,
            "poolIdle": 70,
            "uptimeSeconds": 80,
            "queryLatency": {"p50": 110, "p95": 220, "p99": 330},
            "mutateLatency": {"p50": 440, "p95": 550, "p99": 660},
            "subscribeLatency": {"p50": 770, "p95": 880, "p99": 990}
        })))
        .mount(&server)
        .await;
    let snap = client.metrics().await.unwrap();
    assert_eq!(snap.queries_total, 10);
    assert_eq!(snap.mutations_total, 20);
    assert_eq!(snap.uploads_total, 30);
    assert_eq!(snap.ws_connections, 40);
    assert_eq!(snap.active_subscriptions, 50);
    assert_eq!(snap.pool_size, 60);
    assert_eq!(snap.pool_idle, 70);
    assert_eq!(snap.uptime_seconds, 80);
    assert_eq!(snap.query_latency.p50, 110);
    assert_eq!(snap.query_latency.p95, 220);
    assert_eq!(snap.query_latency.p99, 330);
    assert_eq!(snap.mutate_latency.p99, 660);
    assert_eq!(snap.subscribe_latency.p50, 770);
    // This mock body omits the invalidation counters entirely — i.e. it is
    // an OLDER server's response. `#[serde(default)]` on that field group
    // must keep the whole snapshot parseable rather than failing the call.
    assert_eq!(snap.subs_reruns_total, 0);
    assert_eq!(snap.subs_skips_ordered_total, 0);
    assert_eq!(snap.subs_missed_pushes_total, 0);
}

#[tokio::test]
async fn get_metrics_parses_invalidation_counters() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/metrics"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "queriesTotal": 0,
            "mutationsTotal": 0,
            "uploadsTotal": 0,
            "wsConnections": 0,
            "activeSubscriptions": 0,
            "poolSize": 0,
            "poolIdle": 0,
            "uptimeSeconds": 0,
            "queryLatency": {"p50": 0, "p95": 0, "p99": 0},
            "mutateLatency": {"p50": 0, "p95": 0, "p99": 0},
            "subscribeLatency": {"p50": 0, "p95": 0, "p99": 0},
            "subsRerunsTotal": 4,
            "subsSkipsPointTotal": 1,
            "subsSkipsIndexedTotal": 2,
            "subsSkipsOrderedTotal": 3,
            "subsSkipVerificationsTotal": 6,
            "subsMissedPushesTotal": 0
        })))
        .mount(&server)
        .await;
    let snap = client.metrics().await.unwrap();
    assert_eq!(snap.subs_reruns_total, 4);
    assert_eq!(snap.subs_skips_point_total, 1);
    assert_eq!(snap.subs_skips_indexed_total, 2);
    assert_eq!(snap.subs_skips_ordered_total, 3);
    assert_eq!(snap.subs_skip_verifications_total, 6);
    assert_eq!(snap.subs_missed_pushes_total, 0);
}

#[tokio::test]
async fn get_config_returns_response() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/config"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "port": 8080,
            "publicUrl": "https://rtdb.example",
            "githubBaseUrl": "https://github.com",
            "githubApiUrl": "https://api.github.com",
            "databaseUrlConfigured": true,
            "adminKeyConfigured": true,
            "githubConfigured": false,
            "googleConfigured": false,
            "gitlabConfigured": false,
            "oidcConfigured": false,
            "hot": {
                "allowedOrigins": ["https://app.example"],
                "sessionTtlDays": 30,
                "maxFileSize": 5242880,
                "idempotencyTtlMs": 300000,
                "maxTablesPerDb": 0,
                "maxStorageBytesPerDb": 0,
                "maxSubsPerDb": 0
            },
            "version": "0.1.0",
            "gitCommit": "abc1234",
            "admins": [{"email":"a@x.com","githubId":1}]
        })))
        .mount(&server)
        .await;
    let cfg = client.get_config().await.unwrap();
    assert_eq!(cfg.port, 8080);
    assert_eq!(cfg.public_url, "https://rtdb.example");
    assert!(cfg.database_url_configured);
    assert!(cfg.admin_key_configured);
    assert!(!cfg.github_configured);
    assert_eq!(
        cfg.hot.allowed_origins,
        vec!["https://app.example".to_string()]
    );
    assert_eq!(cfg.hot.session_ttl_days, 30);
    assert_eq!(cfg.hot.max_file_size, 5242880);
    assert_eq!(cfg.hot.idempotency_ttl_ms, 300000);
    assert_eq!(cfg.version, "0.1.0");
    assert_eq!(cfg.git_commit, "abc1234");
    assert_eq!(cfg.admins.len(), 1);
    assert_eq!(cfg.admins[0].email, "a@x.com");
    assert_eq!(cfg.admins[0].github_id, Some(1));
}

#[tokio::test]
async fn patch_config_patches_and_returns_config() {
    let (server, client) = setup().await;
    Mock::given(method("PATCH"))
        .and(path("/admin/config"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({
            "allowedOrigins": ["https://x.example"],
            "sessionTtlDays": 60
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "port": 8080,
            "publicUrl": "",
            "githubBaseUrl": "",
            "githubApiUrl": "",
            "databaseUrlConfigured": false,
            "adminKeyConfigured": false,
            "githubConfigured": false,
            "googleConfigured": false,
            "gitlabConfigured": false,
            "oidcConfigured": false,
            "hot": {
                "allowedOrigins": ["https://x.example"],
                "sessionTtlDays": 60,
                "maxFileSize": 5242880,
                "idempotencyTtlMs": 300000,
                "maxTablesPerDb": 0,
                "maxStorageBytesPerDb": 0,
                "maxSubsPerDb": 0
            },
            "version": "",
            "gitCommit": "",
            "admins": []
        })))
        .mount(&server)
        .await;
    let patch = crate::wire::admin::HotConfigPatch {
        allowed_origins: Some(vec!["https://x.example".to_string()]),
        session_ttl_days: Some(60),
        max_file_size: None,
        idempotency_ttl_ms: None,
        max_tables_per_db: None,
        max_storage_bytes_per_db: None,
        max_subs_per_db: None,
    };
    let cfg = client.patch_config(&patch).await.unwrap();
    assert_eq!(
        cfg.hot.allowed_origins,
        vec!["https://x.example".to_string()]
    );
    assert_eq!(cfg.hot.session_ttl_days, 60);
}

#[tokio::test]
async fn patch_config_surfaces_400_envelope() {
    let (server, client) = setup().await;
    Mock::given(method("PATCH"))
        .and(path("/admin/config"))
        .respond_with(ResponseTemplate::new(400).set_body_json(
            json!({"code": "BAD_REQUEST", "message": "sessionTtlDays must be >= 1"}),
        ))
        .mount(&server)
        .await;
    let err = client
        .patch_config(&crate::wire::admin::HotConfigPatch::default())
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert_eq!(err.message, "sessionTtlDays must be >= 1");
}

#[tokio::test]
async fn ops_recent_returns_events_and_query_params() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/ops/recent"))
        .and(query_param("db", "kanban"))
        .and(query_param("n", "50"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "ops": [
                {"db":"kanban","table":"notes","docId":"n1","kind":"insert","ts":1000,"owner":null},
                {"db":"kanban","table":"notes","docId":"n2","kind":"patch","ts":2000,"owner":"u1"}
            ]
        })))
        .mount(&server)
        .await;
    let ops = client.ops_recent("kanban", None, Some(50)).await.unwrap();
    assert_eq!(ops.len(), 2);
    assert_eq!(ops[0].db, "kanban");
    assert_eq!(ops[0].table, "notes");
    assert_eq!(ops[0].doc_id, "n1");
    assert_eq!(ops[0].kind, "insert");
    assert_eq!(ops[0].ts, 1000);
    assert_eq!(ops[0].owner, None);
    assert_eq!(ops[1].owner, Some("u1".to_string()));
}

// Owner-bypass document endpoints (`POST /admin/db/{db}/query|mutate`).
// Unlike the non-admin `run`/`mutate`, `db` rides in the path (singular
// `db`, not the plural `dbs` used by `get_schema`/`db_stats`), so the body
// omits it. `idempotencyKey` is omitted when `None`.

#[tokio::test]
async fn admin_query_posts_to_admin_db_singular_path_and_unwraps_result() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/query"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{"_id": "a"}, {"_id": "b"}]
        })))
        .mount(&server)
        .await;
    let q = TableQuery::new("items").take(2);
    let got: Vec<Value> = client.admin_query("kanban", &q, None).await.unwrap();
    assert_eq!(got.len(), 2);
    // `db` rides in the path, not the body
    let body: Value =
        serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
    assert!(
        body.get("db").is_none(),
        "admin_query body must not carry db: {body}"
    );
    assert!(
        body.get("query").is_some(),
        "admin_query body must carry query: {body}"
    );
    // includeDeleted omitted when None (the default) — never `null`.
    assert!(
        body.get("includeDeleted").is_none(),
        "admin_query must omit includeDeleted when None: {body}"
    );
}

#[tokio::test]
async fn admin_query_includes_include_deleted_when_some() {
    // Mirrors the ts-client pair test: `includeDeleted: true` rides the
    // body when `Some(true)`, and the key stays absent when `None`.
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/query"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({"includeDeleted": true})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{"_id": "a", "deleted_at": 5}]
        })))
        .mount(&server)
        .await;
    let q = TableQuery::new("items").take(1);
    let got: Vec<Value> = client.admin_query("kanban", &q, Some(true)).await.unwrap();
    assert_eq!(got.len(), 1);
    let body: Value =
        serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
    assert_eq!(body.get("includeDeleted"), Some(&json!(true)));
    assert!(
        body.get("query").is_some(),
        "admin_query body must carry query: {body}"
    );
}

#[tokio::test]
async fn admin_mutate_posts_to_admin_db_singular_path_and_unwraps_results() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/mutate"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [{"id": "new1"}, null]
        })))
        .mount(&server)
        .await;
    let txn = Mutation::new()
        .insert("items", json!({"name": "x"}))
        .patch("items", "i1", json!({"y": 1}))
        .build();
    let res = client.admin_mutate("kanban", &txn, None).await.unwrap();
    assert_eq!(res.len(), 2);
    assert!(matches!(
        res[0],
        crate::mutation::StepResult::Insert { ref id } if id == "new1"
    ));
    // omit-when-no-key: `idempotencyKey` is absent and `db` rides in the path
    let body: Value =
        serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
    assert!(
        body.get("db").is_none(),
        "admin_mutate body must not carry db: {body}"
    );
    assert!(
        body.get("idempotencyKey").is_none(),
        "admin_mutate must omit idempotencyKey when None: {body}"
    );
    assert!(
        body.get("txn").is_some(),
        "admin_mutate body must carry txn: {body}"
    );
}

#[tokio::test]
async fn admin_mutate_includes_idempotency_key_when_some() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/mutate"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({"idempotencyKey": "k1"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"results": []})))
        .mount(&server)
        .await;
    let txn = Mutation::new().delete("items", "i1").build();
    client
        .admin_mutate("kanban", &txn, Some("k1"))
        .await
        .unwrap();
}

#[tokio::test]
async fn migrate_schema_posts_directives_and_dry_run() {
    // Mirrors `push_schema_serializes_schema_json`: the body carries the
    // `directives` array (tagged by `op`, camelCase) plus `dryRun`, and the
    // parsed `MigrateResult` preserves `applied` / derived `schema` /
    // per-directive `affectedRows`.
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/migrate"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({
            "directives": [
                {"op": "renameField", "table": "items", "from": "name", "to": "title"},
                {"op": "dropIndex", "table": "items", "name": "by_name"}
            ],
            "dryRun": true
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "applied": false,
            "schema": {"tables": {"items": {"fields": {"title": {"type": "string"}}}}},
            "directives": [
                {"op": "renameField", "affectedRows": 2},
                {"op": "dropIndex", "affectedRows": 0}
            ]
        })))
        .mount(&server)
        .await;
    let directives = crate::migration::Migration::new()
        .rename_field("items", "name", "title")
        .drop_index("items", "by_name")
        .build();
    let result = client
        .migrate_schema("kanban", &directives, true)
        .await
        .unwrap();
    assert!(!result.applied);
    assert_eq!(result.directives.len(), 2);
    assert_eq!(result.directives[0].op, "renameField");
    assert_eq!(result.directives[0].affected_rows, 2);
    assert_eq!(result.directives[1].op, "dropIndex");
    assert_eq!(result.directives[1].affected_rows, 0);
    assert!(result.schema.tables.contains_key("items"));
    assert!(result.schema.tables["items"].fields.contains_key("title"));
}

// ── Admin backup methods (trigger / list / download / delete / restore) ──

#[tokio::test]
async fn backup_now_posts_empty_body_to_admin_backup() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/backup"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(202).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client.backup_now().await.unwrap();
}

#[tokio::test]
async fn list_backups_parses_running_and_backup_entries() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/backups"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "running": true,
            "backups": [
                {"name": "rtdb-20260728T143045Z.dump", "sizeBytes": 12345, "createdMs": 1753713045000_i64},
                {"name": "rtdb-20260727T010000Z.dump", "sizeBytes": 999,   "createdMs": 1753574400000_i64}
            ]
        })))
        .mount(&server)
        .await;
    let res = client.list_backups().await.unwrap();
    assert!(res.running);
    assert_eq!(res.backups.len(), 2);
    assert_eq!(res.backups[0].name, "rtdb-20260728T143045Z.dump");
    assert_eq!(res.backups[0].size_bytes, 12345);
    assert_eq!(res.backups[0].created_ms, 1753713045000_i64);
    assert_eq!(res.backups[1].size_bytes, 999);
}

#[tokio::test]
async fn download_backup_returns_raw_bytes_without_json_decoding() {
    let (server, client) = setup().await;
    let payload = b"PG_DUMP binary payload \x00\x01\x02 here";
    Mock::given(method("GET"))
        .and(path("/admin/backups/rtdb-20260728T143045Z.dump"))
        .and(header("authorization", BEARER))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/octet-stream")
                .set_body_bytes(payload.to_vec()),
        )
        .mount(&server)
        .await;
    let bytes = client
        .download_backup("rtdb-20260728T143045Z.dump")
        .await
        .unwrap();
    assert_eq!(bytes, payload);
}

#[tokio::test]
async fn download_backup_surfaces_not_found_envelope() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/backups/missing.dump"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "code": "NOT_FOUND",
            "message": "backup file not found"
        })))
        .mount(&server)
        .await;
    let err = client.download_backup("missing.dump").await.unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
    assert_eq!(err.message, "backup file not found");
}

#[tokio::test]
async fn delete_backup_returns_no_content_on_success() {
    let (server, client) = setup().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/backups/rtdb-20260728T143045Z.dump"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(204))
        .mount(&server)
        .await;
    client
        .delete_backup("rtdb-20260728T143045Z.dump")
        .await
        .unwrap();
}

#[tokio::test]
async fn restore_backup_sends_confirm_equal_to_name_and_parses_target() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/restore"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({
            "name": "rtdb-20260728T143045Z.dump",
            "confirm": "rtdb-20260728T143045Z.dump"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "target": "rtdb_restored_20260728T143045Z",
            "instructions": "Restore complete into database 'rtdb_restored_20260728T143045Z'."
        })))
        .mount(&server)
        .await;
    let r = client
        .restore_backup("rtdb-20260728T143045Z.dump")
        .await
        .unwrap();
    assert_eq!(r.target, "rtdb_restored_20260728T143045Z");
    assert!(r.instructions.starts_with("Restore complete"));
}

// ── Webhook management (mirror ts-client admin.test.ts webhook suite) ─────

#[tokio::test]
async fn list_webhooks_returns_rows() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/db/kanban/webhooks"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "webhooks": [
                {"id":1,"db":"kanban","table":null,"url":"https://a.example/hook","events":["*"],"createdAt":1000,"enabled":true},
                {"id":2,"db":"kanban","table":"notes","url":"https://b.example/hook","events":["insert","patch"],"createdAt":2000,"enabled":false}
            ]
        })))
        .mount(&server)
        .await;
    let rows = client.list_webhooks("kanban").await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].db, "kanban");
    assert_eq!(rows[0].table, None);
    assert_eq!(rows[0].url, "https://a.example/hook");
    assert_eq!(rows[0].events, vec!["*".to_string()]);
    assert_eq!(rows[0].created_at, 1000);
    assert!(rows[0].enabled);
    assert_eq!(rows[1].table.as_deref(), Some("notes"));
    assert_eq!(
        rows[1].events,
        vec!["insert".to_string(), "patch".to_string()]
    );
    assert!(!rows[1].enabled);
}

#[tokio::test]
async fn list_webhooks_deserializes_legacy_fixture_omitting_enabled() {
    // Older server (pre-ENH-003 `enabled` flag) must still parse via
    // `#[serde(default)]` on Webhook.enabled — defaulting to false.
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/db/kanban/webhooks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "webhooks": [
                {"id":1,"db":"kanban","table":null,"url":"https://a.example/hook","events":["*"],"createdAt":1000}
            ]
        })))
        .mount(&server)
        .await;
    let rows = client.list_webhooks("kanban").await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].table, None);
    assert!(!rows[0].enabled);
}

#[tokio::test]
async fn create_webhook_posts_options_and_returns_id() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/webhooks"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({
            "url": "https://hook.example/cb",
            "table": "notes",
            "events": ["insert", "patch"],
            "enabled": false
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": 7})))
        .mount(&server)
        .await;
    let id = client
        .create_webhook(
            "kanban",
            &crate::wire::admin::CreateWebhookOptions {
                url: "https://hook.example/cb".to_string(),
                table: Some("notes".to_string()),
                events: Some(vec!["insert".to_string(), "patch".to_string()]),
                enabled: Some(false),
            },
        )
        .await
        .unwrap();
    assert_eq!(id, 7);
}

#[tokio::test]
async fn create_webhook_omits_unset_options() {
    // Backward compat: only `url` is required — an unset `table`/`events`/
    // `enabled` must each stay off the wire (skip_serializing_if), so the
    // server applies its defaults (all-tables, ["*"], enabled=true).
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/webhooks"))
        .and(body_partial_json(json!({"url": "https://hook.example/cb"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": 1})))
        .mount(&server)
        .await;
    let id = client
        .create_webhook(
            "kanban",
            &crate::wire::admin::CreateWebhookOptions {
                url: "https://hook.example/cb".to_string(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(id, 1);
    // Assert nothing else leaked onto the wire.
    let body: Value =
        serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
    assert!(body.get("table").is_none(), "table leaked: {body}");
    assert!(body.get("events").is_none(), "events leaked: {body}");
    assert!(body.get("enabled").is_none(), "enabled leaked: {body}");
}

#[tokio::test]
async fn edit_webhook_puts_options_and_returns_webhook() {
    let (server, client) = setup().await;
    Mock::given(method("PUT"))
        .and(path("/admin/db/kanban/webhooks/3"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({
            "url": "https://new.example/cb",
            "enabled": false
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id":3,"db":"kanban","table":null,"url":"https://new.example/cb","events":["*"],"createdAt":1000,"enabled":false
        })))
        .mount(&server)
        .await;
    let updated = client
        .edit_webhook(
            "kanban",
            3,
            &crate::wire::admin::WebhookEditOptions {
                url: Some("https://new.example/cb".to_string()),
                enabled: Some(false),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.id, 3);
    assert_eq!(updated.url, "https://new.example/cb");
    assert!(!updated.enabled);
    // `opts.table = None` → field must be ABSENT from the body (leave alone).
    let body: Value =
        serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
    assert!(
        body.get("table").is_none(),
        "edit_webhook must omit table when None: {body}"
    );
}

#[tokio::test]
async fn edit_webhook_clears_table_with_some_none() {
    // `opts.table = Some(None)` → serialized as JSON `null` → server clears
    // to all-tables. The body must contain `"table": null` (NOT omit it).
    let (server, client) = setup().await;
    Mock::given(method("PUT"))
        .and(path("/admin/db/kanban/webhooks/3"))
        .and(body_partial_json(json!({"table": null})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id":3,"db":"kanban","table":null,"url":"https://x/cb","events":["*"],"createdAt":1,"enabled":true
        })))
        .mount(&server)
        .await;
    client
        .edit_webhook(
            "kanban",
            3,
            &crate::wire::admin::WebhookEditOptions {
                table: Some(None),
                ..Default::default()
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn edit_webhook_sets_table_with_some_some() {
    // `opts.table = Some(Some("notes"))` → serialized as `"notes"` → server
    // sets the filter.
    let (server, client) = setup().await;
    Mock::given(method("PUT"))
        .and(path("/admin/db/kanban/webhooks/3"))
        .and(body_partial_json(json!({"table": "notes"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id":3,"db":"kanban","table":"notes","url":"https://x/cb","events":["*"],"createdAt":1,"enabled":true
        })))
        .mount(&server)
        .await;
    client
        .edit_webhook(
            "kanban",
            3,
            &crate::wire::admin::WebhookEditOptions {
                table: Some(Some("notes".to_string())),
                ..Default::default()
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn delete_webhook_deletes_and_returns_ok() {
    let (server, client) = setup().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/db/kanban/webhooks/4"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client.delete_webhook("kanban", 4).await.unwrap();
}

#[tokio::test]
async fn delete_webhook_surfaces_not_found_envelope() {
    let (server, client) = setup().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/db/kanban/webhooks/99"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "code": "NOT_FOUND",
            "message": "webhook not found for this database"
        })))
        .mount(&server)
        .await;
    let err = client.delete_webhook("kanban", 99).await.unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
}

#[tokio::test]
async fn list_deliveries_returns_rows_with_query_params() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/db/kanban/webhooks/3/deliveries"))
        .and(query_param("status", "retrying"))
        .and(query_param("limit", "10"))
        .and(query_param("offset", "20"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "deliveries": [
                {"id":1,"attempts":2,"status":"retrying","nextAttempt":5000,"lastError":"boom","payload":{"db":"kanban","table":"notes","docId":"n1","kind":"insert","ts":1000,"owner":null,"source":"mutate"}},
                {"id":2,"attempts":0,"status":"retrying","nextAttempt":6000,"lastError":null,"payload":{"db":"kanban","table":"notes","docId":"n2","kind":"patch","ts":2000,"owner":"u1","source":"scheduled"}}
            ]
        })))
        .mount(&server)
        .await;
    let rows = client
        .list_deliveries(
            "kanban",
            3,
            Some(&crate::wire::admin::ListDeliveriesOptions {
                status: Some("retrying".to_string()),
                limit: Some(10),
                offset: Some(20),
            }),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].attempts, 2);
    assert_eq!(rows[0].status, "retrying");
    assert_eq!(rows[0].next_attempt, 5000);
    assert_eq!(rows[0].last_error.as_deref(), Some("boom"));
    assert_eq!(
        rows[0].payload.get("docId").and_then(Value::as_str),
        Some("n1")
    );
    assert_eq!(rows[1].last_error, None);
    assert_eq!(
        rows[1].payload.get("owner").and_then(Value::as_str),
        Some("u1")
    );
}

#[tokio::test]
async fn list_deliveries_none_opts_sends_no_query_params() {
    // `opts = None` ⇒ first page, no filter — no `status`/`limit`/`offset`
    // query params may appear on the wire.
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/db/kanban/webhooks/3/deliveries"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"deliveries": []})))
        .mount(&server)
        .await;
    let rows = client.list_deliveries("kanban", 3, None).await.unwrap();
    assert!(rows.is_empty());
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    assert!(
        reqs[0].url.query().is_none(),
        "query leaked: {}",
        reqs[0].url
    );
}

/// Wire-shape parity for `Webhook`: every field camelCase on the wire,
/// `null` table deserializes to `None`, and a missing `enabled`/`table`
/// (legacy fixture) still parses via `#[serde(default)]`.
#[test]
fn webhook_deserializes_wire_shapes() {
    use crate::wire::admin::Webhook;

    let full: Webhook = serde_json::from_value(json!({
        "id": 1,
        "db": "kanban",
        "table": null,
        "url": "https://a.example/hook",
        "events": ["*"],
        "createdAt": 1000,
        "enabled": true
    }))
    .unwrap();
    assert_eq!(full.id, 1);
    assert_eq!(full.table, None);
    assert!(full.enabled);

    let scoped: Webhook = serde_json::from_value(json!({
        "id": 2,
        "db": "kanban",
        "table": "notes",
        "url": "https://b.example/hook",
        "events": ["insert", "patch"],
        "createdAt": 2000,
        "enabled": false
    }))
    .unwrap();
    assert_eq!(scoped.table.as_deref(), Some("notes"));
    assert_eq!(
        scoped.events,
        vec!["insert".to_string(), "patch".to_string()]
    );
    assert!(!scoped.enabled);

    // Legacy server omitting `enabled` (and, defensively, `table`) must
    // still deserialize. `enabled` defaults to false; `table` to None.
    let legacy: Webhook = serde_json::from_value(json!({
        "id": 3,
        "db": "kanban",
        "url": "https://c.example/hook",
        "events": ["*"],
        "createdAt": 3000
    }))
    .unwrap();
    assert_eq!(legacy.table, None);
    assert!(!legacy.enabled);
}

/// Wire-shape parity for `WebhookDelivery`: camelCase keys, optional
/// `lastError` (`null` and absent both deserialize to `None`), and `payload`
/// is the verbatim JSON body the worker will/did POST.
#[test]
fn webhook_delivery_deserializes_wire_shapes() {
    use crate::wire::admin::WebhookDelivery;

    let with_err: WebhookDelivery = serde_json::from_value(json!({
        "id": 1,
        "attempts": 2,
        "status": "retrying",
        "nextAttempt": 5000,
        "lastError": "connection refused",
        "payload": {"db":"kanban","table":"notes","docId":"n1","kind":"insert","ts":1000,"owner":null,"source":"mutate"}
    }))
    .unwrap();
    assert_eq!(with_err.attempts, 2);
    assert_eq!(with_err.next_attempt, 5000);
    assert_eq!(with_err.last_error.as_deref(), Some("connection refused"));
    assert_eq!(
        with_err.payload.get("kind").and_then(Value::as_str),
        Some("insert")
    );

    // No error yet (queued, not yet attempted): `null` lastError.
    let no_err: WebhookDelivery = serde_json::from_value(json!({
        "id": 2,
        "attempts": 0,
        "status": "pending",
        "nextAttempt": 1000,
        "lastError": null,
        "payload": {"db":"kanban","table":"notes","docId":"n2","kind":"patch","ts":2000,"owner":"u1","source":"scheduled"}
    }))
    .unwrap();
    assert_eq!(no_err.last_error, None);
    assert_eq!(
        no_err.payload.get("source").and_then(Value::as_str),
        Some("scheduled")
    );

    // Defensive legacy: a server omitting `lastError` entirely must still
    // parse (defaults to None via Option's implicit default).
    let missing_err: WebhookDelivery = serde_json::from_value(json!({
        "id": 3,
        "attempts": 0,
        "status": "pending",
        "nextAttempt": 1000,
        "payload": {}
    }))
    .unwrap();
    assert_eq!(missing_err.last_error, None);
}

/// The load-bearing tri-state on `WebhookEditOptions.table`. Outer `None`
/// omits the field entirely (leave alone); `Some(None)` emits JSON `null`
/// (clear to all-tables); `Some(Some("x"))` emits `"x"` (set). The other
/// fields follow the standard `Option::is_none` skip pattern. Mirrors
/// `mint_token_request_serializes_wire_shape`'s body-shape assertion.
#[test]
fn webhook_edit_options_table_tri_state() {
    use crate::wire::admin::WebhookEditOptions;

    // 1) Every field None → empty body (nothing on the wire → server
    //    changes nothing).
    let all_none = WebhookEditOptions::default();
    assert_eq!(serde_json::to_value(&all_none).unwrap(), json!({}),);

    // 2) `table = None` (outer) → field ABSENT from body, even though the
    //    inner is also None. This is the "leave the filter alone" path.
    let table_omitted = WebhookEditOptions {
        enabled: Some(false),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(&table_omitted).unwrap(),
        json!({"enabled": false}),
    );

    // 3) `table = Some(None)` → serialized as JSON `null` → server clears
    //    to all-tables. This is the case that distinguishes the double
    //    Option from a flat Option.
    let table_cleared = WebhookEditOptions {
        table: Some(None),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(&table_cleared).unwrap(),
        json!({"table": null}),
    );

    // 4) `table = Some(Some("notes"))` → serialized as the string → server
    //    sets the filter.
    let table_set = WebhookEditOptions {
        table: Some(Some("notes".to_string())),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(&table_set).unwrap(),
        json!({"table": "notes"}),
    );

    // 5) Full body: every field set exercises the camelCase wire names.
    let events = vec!["insert".to_string(), "patch".to_string()];
    let full = WebhookEditOptions {
        url: Some("https://new.example/cb".to_string()),
        table: Some(Some("notes".to_string())),
        events: Some(events.clone()),
        enabled: Some(false),
        rotate_secret: Some(true),
    };
    assert_eq!(
        serde_json::to_value(&full).unwrap(),
        json!({
            "url": "https://new.example/cb",
            "table": "notes",
            "events": ["insert", "patch"],
            "enabled": false,
            "rotateSecret": true
        }),
    );
}

/// Wire-shape parity for `CreateWebhookOptions`: camelCase keys, and `None`
/// fields are omitted entirely so server defaults apply (the same contract
/// as `MintTokenOptions`).
#[test]
fn create_webhook_options_serializes_wire_shape() {
    use crate::wire::admin::CreateWebhookOptions;

    // Bare minimum: only `url` is required → body is exactly `{url}`.
    let bare = CreateWebhookOptions {
        url: "https://hook.example/cb".to_string(),
        ..Default::default()
    };
    assert_eq!(
        serde_json::to_value(&bare).unwrap(),
        json!({"url": "https://hook.example/cb"}),
    );

    // Fully-specified → camelCase keys for every option.
    let events = vec!["insert".to_string(), "patch".to_string()];
    let full = CreateWebhookOptions {
        url: "https://hook.example/cb".to_string(),
        table: Some("notes".to_string()),
        events: Some(events.clone()),
        enabled: Some(false),
    };
    assert_eq!(
        serde_json::to_value(&full).unwrap(),
        json!({
            "url": "https://hook.example/cb",
            "table": "notes",
            "events": ["insert", "patch"],
            "enabled": false
        }),
    );
}

// ── Audit log (GET /admin/audit) ─────────────────────────────────────────
//
// Mirror `list_deliveries`-style assertions: provided opts build the right
// `?db=&table=&op=&principal=&source=&limit=&offset=` query, `opts = None`
// sends only `db`, and the parsed `AuditEntry` rows match the wire shape
// (camelCase, `null` op/principal for system-initiated rows).

#[tokio::test]
async fn get_audit_builds_query_params_from_opts() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/audit"))
        .and(query_param("db", "kanban"))
        .and(query_param("table", "notes"))
        .and(query_param("op", "insert"))
        .and(query_param("principal", "u1"))
        .and(query_param("source", "mutate"))
        .and(query_param("limit", "50"))
        .and(query_param("offset", "100"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "entries": [
                {"id":1,"tsMs":1000,"db":"kanban","table":"notes","op":"insert","docId":"n1","principal":"u1","source":"mutate"}
            ]
        })))
        .mount(&server)
        .await;
    let rows = client
        .get_audit(
            "kanban",
            Some(&crate::wire::admin::AuditQuery {
                table: Some("notes".to_string()),
                op: Some("insert".to_string()),
                principal: Some("u1".to_string()),
                source: Some("mutate".to_string()),
                limit: Some(50),
                offset: Some(100),
            }),
        )
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, 1);
    assert_eq!(rows[0].ts_ms, 1000);
    assert_eq!(rows[0].db, "kanban");
    assert_eq!(rows[0].table, "notes");
    assert_eq!(rows[0].op.as_deref(), Some("insert"));
    assert_eq!(rows[0].doc_id, "n1");
    assert_eq!(rows[0].principal.as_deref(), Some("u1"));
    assert_eq!(rows[0].source, "mutate");
}

#[tokio::test]
async fn get_audit_none_opts_sends_only_db() {
    // `opts = None` ⇒ only `db` rides on the query string; no
    // `table`/`op`/`principal`/`source`/`limit`/`offset` may appear.
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/audit"))
        .and(query_param("db", "kanban"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"entries": []})))
        .mount(&server)
        .await;
    let rows = client.get_audit("kanban", None).await.unwrap();
    assert!(rows.is_empty());
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    let query = reqs[0].url.query().unwrap_or("");
    for key in ["table", "op", "principal", "source", "limit", "offset"] {
        assert!(
            !query.contains(&format!("{key}=")),
            "unexpected {key} in query: {query}"
        );
    }
}

/// Wire-shape parity for `AuditEntry`: camelCase keys, `null` op/principal
/// deserializes to `None`, and a legacy server omitting the optional fields
/// still parses via `#[serde(default)]`.
#[test]
fn audit_entry_deserializes_wire_shapes() {
    use crate::wire::admin::AuditEntry;

    // Fully-specified interactive row: every field present, op/principal
    // carrying real values.
    let interactive: AuditEntry = serde_json::from_value(json!({
        "id": 1,
        "tsMs": 1700000000000_i64,
        "db": "kanban",
        "table": "notes",
        "op": "insert",
        "docId": "n1",
        "principal": "u1",
        "source": "mutate"
    }))
    .unwrap();
    assert_eq!(interactive.id, 1);
    assert_eq!(interactive.ts_ms, 1700000000000_i64);
    assert_eq!(interactive.db, "kanban");
    assert_eq!(interactive.table, "notes");
    assert_eq!(interactive.op.as_deref(), Some("insert"));
    assert_eq!(interactive.doc_id, "n1");
    assert_eq!(interactive.principal.as_deref(), Some("u1"));
    assert_eq!(interactive.source, "mutate");

    // System-initiated row (TTL reap / scheduled job): op and principal
    // are JSON `null` on the wire.
    let system: AuditEntry = serde_json::from_value(json!({
        "id": 2,
        "tsMs": 1700000000001_i64,
        "db": "kanban",
        "table": "notes",
        "op": null,
        "docId": "n2",
        "principal": null,
        "source": "ttl"
    }))
    .unwrap();
    assert_eq!(system.op, None);
    assert_eq!(system.principal, None);
    assert_eq!(system.source, "ttl");

    // Legacy fixture: an older server that omits `op`/`principal` entirely
    // must still deserialize via `#[serde(default)]` (both default to None).
    let legacy: AuditEntry = serde_json::from_value(json!({
        "id": 3,
        "tsMs": 42,
        "db": "legacy",
        "table": "things",
        "docId": "t3",
        "source": "mutate"
    }))
    .unwrap();
    assert_eq!(legacy.id, 3);
    assert_eq!(legacy.op, None);
    assert_eq!(legacy.principal, None);
}

// ── Interactive-session management (mirror ts-client admin.test.ts) ─────

#[tokio::test]
async fn list_sessions_builds_query_from_opts() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/sessions"))
        .and(query_param("user", "u1"))
        .and(query_param("limit", "50"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "sessions": [
                {"tokenHash":"abc","userId":"u1","email":"a@x.example","login":null,
                 "anonymous":false,"createdAt":1000,"expiresAt":2000},
                {"tokenHash":"def","userId":"u1","email":null,"login":null,
                 "anonymous":true,"createdAt":1100,"expiresAt":2100}
            ]
        })))
        .mount(&server)
        .await;
    let rows = client
        .list_sessions(Some(&crate::wire::admin::SessionListOptions {
            user: Some("u1".into()),
            limit: Some(50),
        }))
        .await
        .unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].token_hash, "abc");
    assert_eq!(rows[0].user_id, "u1");
    assert_eq!(rows[0].email.as_deref(), Some("a@x.example"));
    assert_eq!(rows[0].login, None);
    assert!(!rows[0].anonymous);
    assert_eq!(rows[1].email, None);
    assert!(rows[1].anonymous);
}

#[tokio::test]
async fn list_sessions_none_opts_sends_no_query_params() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/sessions"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"sessions": []})))
        .mount(&server)
        .await;
    let rows = client.list_sessions(None).await.unwrap();
    assert!(rows.is_empty());
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    let query = reqs[0].url.query().unwrap_or("");
    for key in ["user", "limit"] {
        assert!(
            !query.contains(&format!("{key}=")),
            "unexpected {key} in query: {query}"
        );
    }
}

#[tokio::test]
async fn revoke_session_deletes_by_token_hash() {
    let (server, client) = setup().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/sessions/abc123"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client.revoke_session("abc123").await.unwrap();
}

#[tokio::test]
async fn revoke_user_sessions_deletes_with_user_query_and_parses_count() {
    let (server, client) = setup().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/sessions"))
        .and(query_param("user", "u1"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true, "revoked": 3})))
        .mount(&server)
        .await;
    let r = client.revoke_user_sessions("u1").await.unwrap();
    assert!(r.ok);
    assert_eq!(r.revoked, 3);
}

#[tokio::test]
async fn revoke_expired_sessions_deletes_with_expired_query_and_parses_count() {
    let (server, client) = setup().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/sessions"))
        .and(query_param("expired", "true"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true, "revoked": 5})))
        .mount(&server)
        .await;
    let r = client.revoke_expired_sessions().await.unwrap();
    assert!(r.ok);
    assert_eq!(r.revoked, 5);
}

#[test]
fn session_info_deserializes_wire_shapes() {
    use crate::wire::admin::SessionInfo;
    // Interactive session: email + login present.
    let interactive: SessionInfo = serde_json::from_value(json!({
        "tokenHash": "abc",
        "userId": "u1",
        "email": "a@x.example",
        "login": "alice",
        "anonymous": false,
        "createdAt": 1000_i64,
        "expiresAt": 2000_i64
    }))
    .unwrap();
    assert_eq!(interactive.token_hash, "abc");
    assert_eq!(interactive.user_id, "u1");
    assert_eq!(interactive.email.as_deref(), Some("a@x.example"));
    assert_eq!(interactive.login.as_deref(), Some("alice"));
    assert!(!interactive.anonymous);

    // Anonymous session: email/login are JSON `null` on the wire.
    let anon: SessionInfo = serde_json::from_value(json!({
        "tokenHash": "def",
        "userId": "u2",
        "email": null,
        "login": null,
        "anonymous": true,
        "createdAt": 1100_i64,
        "expiresAt": 2100_i64
    }))
    .unwrap();
    assert_eq!(anon.email, None);
    assert_eq!(anon.login, None);
    assert!(anon.anonymous);
}

// ── Anon→real account merge (mirror ts-client admin.test.ts) ─────────────

#[tokio::test]
async fn merge_users_posts_ids_and_confirm_equals_real() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/merge-users"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(
            json!({"anonUserId": "u-anon", "realUserId": "u-real", "confirm": "u-real"}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "dbs": {
                "kanban": {
                    "tables": {"notes": 2, "cursors": 1},
                    "conflicts": [{"table": "notes", "id": "n7"}]
                },
                "empty": {"tables": {}, "conflicts": []}
            },
            "storageRepointed": 4,
            "sessionsRepointed": 1,
            "anonDeleted": true
        })))
        .mount(&server)
        .await;
    let report = client.merge_users("u-anon", "u-real").await.unwrap();
    assert_eq!(report.dbs.len(), 2);
    let kanban = report.dbs.get("kanban").unwrap();
    assert_eq!(kanban.tables.get("notes"), Some(&2));
    assert_eq!(kanban.tables.get("cursors"), Some(&1));
    assert_eq!(kanban.conflicts.len(), 1);
    assert_eq!(kanban.conflicts[0].table, "notes");
    assert_eq!(kanban.conflicts[0].id, "n7");
    let empty = report.dbs.get("empty").unwrap();
    assert!(empty.tables.is_empty());
    assert!(empty.conflicts.is_empty());
    assert_eq!(report.storage_repointed, 4);
    assert_eq!(report.sessions_repointed, 1);
    assert!(report.anon_deleted);
}

#[tokio::test]
async fn merge_users_surfaces_missing_anon_envelope() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/merge-users"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "code": "NOT_FOUND",
            "message": "anonymous user not found; nothing to merge"
        })))
        .mount(&server)
        .await;
    let err = client.merge_users("missing", "u-real").await.unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
    assert_eq!(err.message, "anonymous user not found; nothing to merge");
}

// Explain + slow-query log (ENH-019). `explain_query` posts the Query DSL
// to `/admin/db/{db}/explain` (singular `db`, same as `admin_query`) and
// deserializes the `{sql, params, terminal, warnings}` shape. The slow-
// query log is a GET with optional `db`/`limit` query params.

#[tokio::test]
async fn explain_query_posts_to_admin_db_singular_path_and_deserializes() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/explain"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({"query": {"table": "items"}})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "sql": "SELECT doc FROM kanban.items LIMIT 10",
            "params": ["active", "50"],
            "terminal": "collect",
            "warnings": []
        })))
        .mount(&server)
        .await;
    let q = TableQuery::new("items").take(10);
    let got = client.explain_query("kanban", &q).await.unwrap();
    assert_eq!(got.sql, "SELECT doc FROM kanban.items LIMIT 10");
    assert_eq!(got.params, vec!["active".to_string(), "50".to_string()]);
    assert_eq!(got.terminal, "collect");
    assert!(got.warnings.is_empty());
    // `query` rides in the body, `db` rides in the path
    let body: Value =
        serde_json::from_slice(&server.received_requests().await.unwrap()[0].body).unwrap();
    assert!(
        body.get("query").is_some(),
        "explain body must carry query: {body}"
    );
}

#[tokio::test]
async fn get_slow_queries_passes_db_and_limit_query_params() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/slow-queries"))
        .and(query_param("db", "kanban"))
        .and(query_param("limit", "5"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "queries": [
                {
                    "startedAtMs": 1700000000000_i64,
                    "durationMs": 42,
                    "db": "kanban",
                    "table": "projects",
                    "terminal": "collect",
                    "sql": "SELECT doc FROM kanban.projects",
                    "params": ["active"]
                }
            ],
            "thresholdMs": 25,
            "capacity": 200
        })))
        .mount(&server)
        .await;
    let resp = client
        .get_slow_queries(Some("kanban"), Some(5))
        .await
        .unwrap();
    assert_eq!(resp.threshold_ms, 25);
    assert_eq!(resp.capacity, 200);
    assert_eq!(resp.queries.len(), 1);
    let row = &resp.queries[0];
    assert_eq!(row.started_at_ms, 1700000000000_i64);
    assert_eq!(row.duration_ms, 42);
    assert_eq!(row.db, "kanban");
    assert_eq!(row.table, "projects");
    assert_eq!(row.terminal, "collect");
    assert_eq!(row.params.as_deref(), Some(&["active".to_string()][..]));
}

#[tokio::test]
async fn get_slow_queries_omits_params_when_redacted() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/slow-queries"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "queries": [
                {
                    "startedAtMs": 1_i64,
                    "durationMs": 99,
                    "db": "x",
                    "table": "t",
                    "terminal": "get",
                    "sql": "SELECT 1"
                }
            ],
            "thresholdMs": 0,
            "capacity": 100
        })))
        .mount(&server)
        .await;
    // No db/limit params → neither query param should appear.
    let resp = client.get_slow_queries(None, None).await.unwrap();
    assert_eq!(resp.queries.len(), 1);
    assert_eq!(resp.queries[0].params, None);
}

// ── FM-29 workflow runs ─────────────────────────────────────────────────

#[tokio::test]
async fn list_workflows_builds_status_and_limit_query() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/db/kanban/workflows"))
        .and(header("authorization", BEARER))
        .and(query_param("status", "failed"))
        .and(query_param("limit", "5"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "workflows": [{
                "id": "wf1", "name": "drip", "status": "failed",
                "currentStep": 1, "stepCount": 3, "attempts": 3,
                "lastError": "version mismatch", "createdAt": 1, "updatedAt": 9
            }]
        })))
        .mount(&server)
        .await;
    let list = client
        .list_workflows(
            "kanban",
            Some(&crate::wire::admin::WorkflowListOptions {
                status: Some(crate::wire::WorkflowStatus::Failed),
                limit: Some(5),
            }),
        )
        .await
        .unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].id, "wf1");
    assert_eq!(list[0].status, crate::wire::WorkflowStatus::Failed);
    assert_eq!(list[0].last_error.as_deref(), Some("version mismatch"));
}

#[tokio::test]
async fn list_workflows_omits_unset_options() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/db/kanban/workflows"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"workflows": []})))
        .mount(&server)
        .await;
    let list = client.list_workflows("kanban", None).await.unwrap();
    assert!(list.is_empty());
    // `opts = None` → no status/limit query params on the wire.
    let req = &server.received_requests().await.unwrap()[0];
    let url = req.url.as_str();
    assert!(!url.contains("status"), "status leaked: {url}");
    assert!(!url.contains("limit"), "limit leaked: {url}");
}

#[tokio::test]
async fn get_workflow_returns_info_full_with_outcomes() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/db/kanban/workflows/wf1"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "id": "wf1", "name": "drip", "status": "success",
            "currentStep": 2, "stepCount": 2, "attempts": 1,
            "createdAt": 1, "updatedAt": 9, "startedAt": 2, "finishedAt": 9,
            "stepOutcomes": [
                { "stepIndex": 0, "status": "success", "attempts": 1, "at": 5 },
                { "stepIndex": 1, "status": "failed", "attempts": 3, "at": 8,
                  "error": "version mismatch" }
            ]
        })))
        .mount(&server)
        .await;
    let full = client.get_workflow("kanban", "wf1").await.unwrap();
    assert_eq!(full.info.id, "wf1");
    assert_eq!(full.info.status, crate::wire::WorkflowStatus::Success);
    assert_eq!(full.step_outcomes.len(), 2);
    assert_eq!(full.step_outcomes[1].attempts, 3);
}

#[tokio::test]
async fn admin_start_workflow_posts_spec_and_returns_id() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/workflows"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({
            "name": "drip",
            "steps": [ { "txn": { "steps": [] } } ]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "wf-7"})))
        .mount(&server)
        .await;
    let spec = crate::wire::WorkflowSpec {
        name: "drip".into(),
        steps: vec![crate::wire::WorkflowStepSpec {
            txn: Some(Mutation::new().build()),
            await_signal: None,
            retry: None,
            sleep_before_ms: None,
        }],
    };
    let id = client.start_workflow("kanban", &spec).await.unwrap();
    assert_eq!(id, "wf-7");
}

#[tokio::test]
async fn admin_cancel_and_delete_workflow_hit_their_paths() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/workflows/wf-1/cancel"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    Mock::given(method("DELETE"))
        .and(path("/admin/db/kanban/workflows/wf-1"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client.cancel_workflow("kanban", "wf-1").await.unwrap();
    client.delete_workflow("kanban", "wf-1").await.unwrap();
}

#[tokio::test]
async fn admin_signal_workflow_posts_name_and_optional_payload() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/workflows/wf-1/signal"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(
            json!({"name": "approve", "payload": {"ok": true}}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    // `payload: None` omits the key — exact raw body, not a partial match.
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/workflows/wf-1/signal"))
        .and(body_bytes(
            serde_json::to_string(&json!({"name": "approve"}))
                .unwrap()
                .into_bytes(),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    assert!(
        client
            .signal_workflow("kanban", "wf-1", "approve", Some(&json!({"ok": true})))
            .await
            .unwrap()
    );
    assert!(
        client
            .signal_workflow("kanban", "wf-1", "approve", None)
            .await
            .unwrap()
    );
}

// ── Schema preview (mirror ts-client admin.test.ts previewSchema) ────────

#[tokio::test]
async fn preview_schema_posts_schema_and_parses_diff() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/schema/preview"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({
            "schema": {"tables": {"notes": {"fields": {"body": {"type": "string"}}}}}
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "added": [
                {
                    "table": "notes",
                    "columns": [{"name": "body", "fieldType": "string"}],
                    "indexes": [{"name": "by_body", "fields": ["body"]}]
                }
            ],
            "rejected": [
                {"table": "old", "item": "gone", "reason": "drop not allowed"}
            ]
        })))
        .mount(&server)
        .await;
    let schema = SchemaDef::builder()
        .table("notes", Table::new().field("body", FieldType::String))
        .build();
    let diff = client.preview_schema("kanban", &schema).await.unwrap();
    assert_eq!(diff.added.len(), 1);
    assert_eq!(diff.added[0].table, "notes");
    assert_eq!(diff.added[0].columns.len(), 1);
    assert_eq!(diff.added[0].columns[0].name, "body");
    assert_eq!(diff.added[0].columns[0].field_type, "string");
    assert_eq!(diff.added[0].indexes.len(), 1);
    assert_eq!(diff.added[0].indexes[0].name, "by_body");
    assert_eq!(diff.added[0].indexes[0].fields, vec!["body".to_string()]);
    assert_eq!(diff.rejected.len(), 1);
    assert_eq!(diff.rejected[0].table, "old");
    assert_eq!(diff.rejected[0].item, "gone");
    assert_eq!(diff.rejected[0].reason, "drop not allowed");
}

#[tokio::test]
async fn preview_schema_surfaces_invalid_schema_envelope() {
    // Pure/advisory does not mean unvalidated: a malformed schema is a 400
    // BEFORE any diff is computed, and the envelope surfaces as RtDbError.
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/schema/preview"))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "code": "BAD_REQUEST",
            "message": "invalid schema: table name empty"
        })))
        .mount(&server)
        .await;
    let err = client
        .preview_schema("kanban", &SchemaDef::builder().build())
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::BadRequest);
    assert_eq!(err.message, "invalid schema: table name empty");
}

// ── Admin schedules (mirror ts-client admin.test.ts schedule suite) ─────

#[tokio::test]
async fn list_schedules_returns_rows() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/db/kanban/schedules"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "schedules": [
                {"id":"s1","kind":"oneshot","dueAt":100,"status":"pending",
                 "createdAt":10,"firedCount":0},
                {"id":"s2","kind":"cron","dueAt":200,"cron":"*/5 * * * *",
                 "status":"paused","lastError":"boom","createdAt":20,"firedCount":3}
            ]
        })))
        .mount(&server)
        .await;
    let rows = client.list_schedules("kanban").await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].id, "s1");
    assert_eq!(rows[0].kind, crate::wire::ScheduleKind::Oneshot);
    assert_eq!(rows[0].status, crate::wire::ScheduleStatus::Pending);
    assert_eq!(rows[0].cron, None);
    assert_eq!(rows[0].last_error, None);
    assert_eq!(rows[1].id, "s2");
    assert_eq!(rows[1].kind, crate::wire::ScheduleKind::Cron);
    assert_eq!(rows[1].cron.as_deref(), Some("*/5 * * * *"));
    assert_eq!(rows[1].status, crate::wire::ScheduleStatus::Paused);
    assert_eq!(rows[1].last_error.as_deref(), Some("boom"));
    assert_eq!(rows[1].fired_count, 3);
}

#[tokio::test]
async fn create_schedule_posts_when_and_txn_returns_id() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/schedules"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({
            "when": {"type": "afterMs", "ms": 5000},
            "txn": {"steps": [{"op": "insert", "table": "notes", "doc": {"body": "hi"}}]}
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "sch-9"})))
        .mount(&server)
        .await;
    let txn = Mutation::new()
        .insert("notes", json!({"body": "hi"}))
        .build();
    let id = client
        .create_schedule(
            "kanban",
            crate::wire::ScheduleWhen::AfterMs { ms: 5000 },
            &txn,
        )
        .await
        .unwrap();
    assert_eq!(id, "sch-9");
}

#[tokio::test]
async fn manage_schedules_hit_their_paths() {
    // Mirrors `admin_cancel_and_delete_workflow_hit_their_paths`: the three
    // manage ops POST their path segments with no body and return the ack.
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/schedules/sch-1/cancel"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/schedules/sch-1/pause"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/schedules/sch-1/resume"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    assert!(client.cancel_schedule("kanban", "sch-1").await.unwrap());
    assert!(client.pause_schedule("kanban", "sch-1").await.unwrap());
    assert!(client.resume_schedule("kanban", "sch-1").await.unwrap());
}

#[tokio::test]
async fn cancel_schedule_unknown_id_is_ok_false_not_error() {
    // Server acks {ok:false} for an unknown/already-terminal id — a no-op,
    // surfaced as Ok(false) rather than Err.
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/schedules/missing/cancel"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": false})))
        .mount(&server)
        .await;
    assert!(!client.cancel_schedule("kanban", "missing").await.unwrap());
}

// ── Admin storage (mirror ts-client admin.test.ts storage suite) ────────

#[tokio::test]
async fn list_files_returns_file_metadata() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/db/kanban/storage"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "files": [
                {"id":"f1","sha256":"aa","size":10,"contentType":"image/png",
                 "creationTime":100},
                {"id":"f2","sha256":"bb","size":20,"contentType":null,
                 "creationTime":200}
            ]
        })))
        .mount(&server)
        .await;
    let files = client.list_files("kanban").await.unwrap();
    assert_eq!(files.len(), 2);
    assert_eq!(files[0].id, "f1");
    assert_eq!(files[0].sha256, "aa");
    assert_eq!(files[0].size, 10);
    assert_eq!(files[0].content_type.as_deref(), Some("image/png"));
    assert_eq!(files[0].creation_time, 100);
    assert_eq!(files[1].content_type, None);
}

#[tokio::test]
async fn upload_file_posts_raw_bytes_with_content_type() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/storage"))
        .and(header("authorization", BEARER))
        .and(header("content-type", "image/png"))
        .and(body_string_contains("PNGDATA"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "f9"})))
        .mount(&server)
        .await;
    let id = client
        .upload_file("kanban", b"PNGDATA".as_slice(), Some("image/png"))
        .await
        .unwrap();
    assert_eq!(id, "f9");
}

#[tokio::test]
async fn upload_file_omits_content_type_header_when_none() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/db/kanban/storage"))
        .and(header("authorization", BEARER))
        .and(body_string_contains("RAW"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"id": "f10"})))
        .mount(&server)
        .await;
    let id = client
        .upload_file("kanban", b"RAW".as_slice(), None)
        .await
        .unwrap();
    assert_eq!(id, "f10");
    // No content-type header may be sent when None (wiremock only matches
    // the request above, so reaching here means the bare POST matched).
    let reqs = server.received_requests().await.unwrap();
    assert_eq!(reqs.len(), 1);
    assert!(
        reqs[0].headers.get("content-type").is_none(),
        "content-type leaked: {:?}",
        reqs[0].headers.get("content-type")
    );
}

#[tokio::test]
async fn delete_file_deletes_and_returns_ok() {
    let (server, client) = setup().await;
    Mock::given(method("DELETE"))
        .and(path("/admin/db/kanban/storage/f9"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client.delete_file("kanban", "f9").await.unwrap();
}

// ── Anonymous-access toggle (mirror ts-client admin.test.ts) ────────────

#[tokio::test]
async fn get_anonymous_access_returns_enabled() {
    let (server, client) = setup().await;
    Mock::given(method("GET"))
        .and(path("/admin/db/kanban/anonymous-access"))
        .and(header("authorization", BEARER))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"enabled": true})))
        .mount(&server)
        .await;
    assert!(client.get_anonymous_access("kanban").await.unwrap());
}

#[tokio::test]
async fn set_anonymous_access_patches_enabled() {
    let (server, client) = setup().await;
    Mock::given(method("PATCH"))
        .and(path("/admin/db/kanban/anonymous-access"))
        .and(header("authorization", BEARER))
        .and(body_partial_json(json!({"enabled": false})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    client.set_anonymous_access("kanban", false).await.unwrap();
}

#[tokio::test]
async fn set_anonymous_access_surfaces_unknown_db_envelope() {
    let (server, client) = setup().await;
    Mock::given(method("PATCH"))
        .and(path("/admin/db/missing/anonymous-access"))
        .respond_with(ResponseTemplate::new(404).set_body_json(json!({
            "code": "NOT_FOUND",
            "message": "database not registered"
        })))
        .mount(&server)
        .await;
    let err = client
        .set_anonymous_access("missing", true)
        .await
        .unwrap_err();
    assert_eq!(err.code, ErrorCode::NotFound);
    assert_eq!(err.message, "database not registered");
}

// ARC-013 follow-up: admin calls go through the same `AuthedRequest::authed`
// seam as the data plane, so every `/admin/*` request carries the client's
// protocol version — previously only query/mutate/batch did. Covers a POST
// (JSON body), a GET (query string), and a PATCH.
#[tokio::test]
async fn every_admin_request_carries_the_protocol_header() {
    let (server, client) = setup().await;
    Mock::given(method("POST"))
        .and(path("/admin/create-db"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/admin/export-db"))
        .respond_with(ResponseTemplate::new(200).set_body_string("{}\n"))
        .mount(&server)
        .await;
    Mock::given(method("PATCH"))
        .and(path("/admin/db/kanban/anonymous-access"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"ok": true})))
        .mount(&server)
        .await;

    client.create_db("kanban").await.unwrap();
    client.export_db("kanban").await.unwrap();
    client.set_anonymous_access("kanban", true).await.unwrap();

    let expected = crate::wire::PROTOCOL_VERSION.to_string();
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 3);
    for req in requests {
        let sent = req
            .headers
            .get("x-rtdb-protocol")
            .and_then(|v| v.to_str().ok());
        assert_eq!(
            sent,
            Some(expected.as_str()),
            "{} {} sent no protocol header",
            req.method,
            req.url.path()
        );
    }
}
