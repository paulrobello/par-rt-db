import Foundation

// MARK: - RtDbAdminClient

/// Admin control-plane client for par-rt-db (`/admin/*`) — the Swift mirror
/// of rust-client/src/admin/mod.rs `RtDbAdminClient` (itself the mirror of
/// ts-client's). The bearer token must be the instance admin key for these
/// to authorize. Every method's route, payload, and response shape ports
/// one-to-one from the rust client; only the method names are camelCased.
///
/// An actor like `RtDbHttpClient`, but keying the whole admin plane: every
/// call sends `Authorization: Bearer <admin key>`. Construct directly with
/// `init(url:adminKey:)`; for the data plane (queries, mutations,
/// subscriptions on one database) use `RtDbHttpClient`/`RtDbClient`.
public actor RtDbAdminClient {
    /// Base URL with any trailing `/` trimmed (rust `trim_end_matches('/')`).
    private let baseUrl: String
    private let adminKey: String
    private let session: URLSession

    /// Create a standalone admin client. `adminKey` is the instance admin
    /// key (the same value `RtDbHttpClient` would carry as its token for an
    /// admin caller).
    public init(url: String, adminKey: String, session: URLSession = .shared) {
        var base = url
        while base.hasSuffix("/") {
            base.removeLast()
        }
        baseUrl = base
        self.adminKey = adminKey
        self.session = session
    }

    // MARK: Database lifecycle

    /// `POST /admin/create-db` `{name}` → `{ok:true}`.
    public func createDb(_ name: String) async throws {
        try await postForOk("create db", "/admin/create-db", CreateDbRequest(name: name))
    }

    /// `POST /admin/delete-db` `{name, confirm}` → `{ok:true}`. The server
    /// rejects with BAD_REQUEST unless `confirm == name` exactly — the typed
    /// confirmation guard against accidental deletion. Drops the db's
    /// Postgres schema (CASCADE) and every per-db row (registry, tokens,
    /// allowlist, storage index).
    public func deleteDb(_ name: String, confirm: String) async throws {
        try await postForOk(
            "delete db", "/admin/delete-db", DeleteDbRequest(name: name, confirm: confirm)
        )
    }

    /// `GET /admin/dbs` → `{databases:[...]}`.
    public func listDbs() async throws -> [String] {
        let response: DatabasesResponse = try await getJson("list dbs", "/admin/dbs")
        return response.databases
    }

    /// `GET /admin/export-db?db=<db>` → the database's schema + every
    /// document as JSONL text (the body is text, not JSON-decoded).
    public func exportDb(_ db: String) async throws -> String {
        let (status, data) = try await execute(
            "export db", method: "GET", path: "/admin/export-db",
            query: [URLQueryItem(name: "db", value: db)]
        )
        guard (200 ..< 300).contains(status) else {
            throw RtDbError.decodeEnvelope(from: data)
                ?? RtDbError(code: .badRequest, message: "HTTP \(status)")
        }
        guard let text = String(data: data, encoding: .utf8) else {
            throw RtDbError(code: .internal, message: "invalid export body: not UTF-8")
        }
        return text
    }

    /// `POST /admin/import-db?db=<db>` with an `application/x-ndjson` body of
    /// a snapshot produced by `exportDb(_:)`.
    public func importDb(_ db: String, jsonl: String) async throws {
        try await expectOk(
            "import db", method: "POST", path: "/admin/import-db",
            body: Data(jsonl.utf8), contentType: "application/x-ndjson",
            query: [URLQueryItem(name: "db", value: db)]
        )
    }

    /// `POST /admin/clone-db?from=<from>&to=<to>` → `{ok:true}`. Clones
    /// `from` (schema + documents) into a freshly created `to` in one
    /// server-side step (ENH-009). `to` must not already exist; scope matches
    /// export/import — storage blobs and scheduled transactions are not
    /// copied.
    public func cloneDb(from: String, to: String) async throws {
        try await expectOk(
            "clone db", method: "POST", path: "/admin/clone-db",
            query: [
                URLQueryItem(name: "from", value: from),
                URLQueryItem(name: "to", value: to)
            ]
        )
    }

    // MARK: Schema plane

    /// `POST /admin/push-schema` `{db, schema}` → `{ok:true}`.
    public func pushSchema(db: String, schema: SchemaDef) async throws {
        try await postForOk(
            "push schema", "/admin/push-schema", PushSchemaRequest(db: db, schema: schema)
        )
    }

    /// `POST /admin/db/{db}/schema/preview` `{schema}` → `SchemaPreviewDiff`.
    /// Pure/advisory — validates the pending schema and diffs it against the
    /// currently-applied one WITHOUT applying anything: `added` lists every
    /// new table/column/index an additive-only push would create, `rejected`
    /// lists every drop or type change the DDL layer would refuse.
    public func previewSchema(db: String, schema: SchemaDef) async throws -> SchemaPreviewDiff {
        try await postJson(
            "preview schema", "/admin/db/\(encodePath(db))/schema/preview",
            PreviewSchemaRequest(schema: schema)
        )
    }

    /// `POST /admin/db/{db}/migrate` `{directives, dryRun}` →
    /// `MigrateResult`. Apply (when `dryRun` is false) or preview (when true)
    /// a declarative schema migration. The server validates and folds the
    /// directives transactionally; on a dry run nothing is committed and the
    /// returned `schema` is the derived preview.
    public func migrateSchema(
        db: String, directives: [Directive], dryRun: Bool
    ) async throws -> MigrateResult {
        try await postJson(
            "migrate schema", "/admin/db/\(encodePath(db))/migrate",
            MigrateRequest(directives: directives, dryRun: dryRun)
        )
    }

    /// `GET /admin/dbs/{db}/schema` → the database's pushed `SchemaDef`.
    public func getSchema(_ db: String) async throws -> SchemaDef {
        try await getJson("get schema", "/admin/dbs/\(encodePath(db))/schema")
    }

    /// `GET /admin/db/{db}/schema/history?limit=&offset=` → newest-first list
    /// of captured schema snapshots (summaries, no `schema` blob). `limit` /
    /// `offset` are optional paging params (server defaults: limit 100
    /// clamped to 1000, offset 0).
    public func schemaHistory(
        _ db: String, limit: Int? = nil, offset: Int? = nil
    ) async throws -> [SchemaHistorySummary] {
        var query: [URLQueryItem] = []
        if let limit {
            query.append(URLQueryItem(name: "limit", value: String(limit)))
        }
        if let offset {
            query.append(URLQueryItem(name: "offset", value: String(offset)))
        }
        let response: SchemaHistoryListResponse = try await getJson(
            "schema history", "/admin/db/\(encodePath(db))/schema/history", query: query
        )
        return response.entries
    }

    /// `GET /admin/db/{db}/schema/history/{version}` → one full snapshot,
    /// including the `schema` blob. Not-found if the database or version does
    /// not exist.
    public func schemaHistoryGet(
        _ db: String, version: Int64
    ) async throws -> SchemaHistoryEntry {
        try await getJson(
            "schema history entry",
            "/admin/db/\(encodePath(db))/schema/history/\(version)"
        )
    }

    /// `POST /admin/db/{db}/schema/restore` `{version, confirm}` → restore
    /// the live schema shape to a prior captured snapshot; returns the
    /// restored version. `confirm` must equal the db name (typed guard,
    /// mirrors delete-db). The redundant `ok` flag collapses into the throw —
    /// errors surface as `RtDbError`.
    public func restoreSchema(
        _ db: String, version: Int64, confirm: String
    ) async throws -> Int64 {
        let response: RestoreSchemaResponse = try await postJson(
            "restore schema", "/admin/db/\(encodePath(db))/schema/restore",
            RestoreSchemaRequest(version: version, confirm: confirm)
        )
        return response.restoredTo
    }

    /// `GET /admin/dbs/{db}/stats` → per-table row counts, sizes, and the
    /// ENH-011 quota/usage fields.
    public func dbStats(_ db: String) async throws -> DbStats {
        try await getJson("db stats", "/admin/dbs/\(encodePath(db))/stats")
    }

    // MARK: Tokens + allowlist + admins

    /// `POST /admin/mint-token` `{db, name}` → `{tokenId, token}` — a
    /// full-access mint (no expiry, read-write, all tables). For scoped
    /// capabilities pass `options`.
    public func mintToken(
        _ db: String, name: String, options: MintTokenOptions = MintTokenOptions()
    ) async throws -> MintedToken {
        try await postJson(
            "mint token", "/admin/mint-token",
            MintTokenRequest(
                db: db, name: name, expiresAt: options.expiresAt,
                readOnly: options.readOnly, tables: options.tables
            )
        )
    }

    /// `POST /admin/revoke-token` `{tokenId}` → `{ok:true}`.
    public func revokeToken(_ tokenId: String) async throws {
        try await postForOk(
            "revoke token", "/admin/revoke-token", RevokeTokenRequest(tokenId: tokenId)
        )
    }

    /// `POST /admin/allowlist` `{db, action:"add", email}` → `{ok:true}`.
    public func allowlistAdd(db: String, email: String) async throws {
        try await postForOk(
            "allowlist add", "/admin/allowlist",
            AllowlistWriteRequest(db: db, action: "add", email: email)
        )
    }

    /// `POST /admin/allowlist` `{db, action:"remove", email}` → `{ok:true}`.
    public func allowlistRemove(db: String, email: String) async throws {
        try await postForOk(
            "allowlist remove", "/admin/allowlist",
            AllowlistWriteRequest(db: db, action: "remove", email: email)
        )
    }

    /// `GET /admin/allowlist?db=<db>` → `{emails:[...]}`.
    public func allowlistList(db: String) async throws -> [String] {
        let response: AllowlistListResponse = try await getJson(
            "allowlist list", "/admin/allowlist",
            query: [URLQueryItem(name: "db", value: db)]
        )
        return response.emails
    }

    /// `GET /admin/admins` → `{admins:[{email, githubId?}]}`.
    public func adminsList() async throws -> [AdminMember] {
        let response: AdminsListResponse = try await getJson("admins list", "/admin/admins")
        return response.admins
    }

    /// `POST /admin/admins` `{email, githubId?}` → `{ok:true}`.
    public func adminsAdd(email: String, githubId: Int64? = nil) async throws {
        try await postForOk(
            "admins add", "/admin/admins", AdminsAddRequest(email: email, githubId: githubId)
        )
    }

    /// `DELETE /admin/admins` `{email}` → `{ok:true}`.
    public func adminsRemove(email: String) async throws {
        let response: OkResponse = try await deleteJson(
            "admins remove", "/admin/admins", AdminsRemoveRequest(email: email)
        )
        try requireOk(response)
    }

    /// `GET /admin/tokens?db=<db>` → machine tokens minted for this database.
    public func listTokens(_ db: String) async throws -> [TokenInfo] {
        let response: TokensResponse = try await getJson(
            "list tokens", "/admin/tokens", query: [URLQueryItem(name: "db", value: db)]
        )
        return response.tokens
    }

    // MARK: Metrics / config / op feed

    /// `GET /admin/metrics` → server-wide counters and gauges.
    public func metrics() async throws -> MetricsSnapshot {
        try await getJson("metrics", "/admin/metrics")
    }

    /// `GET /admin/subscriptions?db=<optional>` → live subscription
    /// inspector (ENH-010): every active subscription's db/table/terminal/
    /// read-set class/principal, plus invalidation-effectiveness counters
    /// both server-wide and per-db. Pass a db to scope to one database; nil
    /// for every database on the instance.
    public func listSubscriptions(db: String? = nil) async throws -> SubscriptionsResponse {
        var query: [URLQueryItem] = []
        if let db {
            query.append(URLQueryItem(name: "db", value: db))
        }
        return try await getJson("list subscriptions", "/admin/subscriptions", query: query)
    }

    /// `GET /admin/config` → redacted running config + build identity +
    /// admins.
    public func getConfig() async throws -> ConfigResponse {
        try await getJson("get config", "/admin/config")
    }

    /// `PATCH /admin/config` with a partial hot-config body → the updated
    /// config.
    public func patchConfig(_ patch: HotConfigPatch) async throws -> ConfigResponse {
        try await patchJson("patch config", "/admin/config", patch)
    }

    /// `GET /admin/ops/recent?db=<db>&table=<t>&n=<n>` → recent document-op
    /// events from the in-memory ring, newest-first. `table` and `n` are
    /// optional.
    public func opsRecent(
        _ db: String, table: String? = nil, count: Int? = nil
    ) async throws -> [OpEvent] {
        var query = [URLQueryItem(name: "db", value: db)]
        if let table {
            query.append(URLQueryItem(name: "table", value: table))
        }
        if let count {
            query.append(URLQueryItem(name: "n", value: String(count)))
        }
        let response: OpsRecentResponse = try await getJson(
            "ops recent", "/admin/ops/recent", query: query
        )
        return response.ops
    }

    // MARK: Admin query / mutate / explain / slow queries

    /// `POST /admin/db/{db}/query` `{query, includeDeleted?}` → `{result}`.
    /// Owner-bypass: an admin reads documents across every database
    /// regardless of `ownerField`. Mirrors `RtDbHttpClient.run` but routes
    /// through the admin path with `db` in the URL (singular `db`, not the
    /// plural `dbs` of `getSchema`), so the body omits `db`. The result is
    /// decoded into `T` the same way `run(_:as:)` does — use the type that
    /// matches the query's terminal.
    ///
    /// `includeDeleted` is an internal admin-route parameter, NOT a wire
    /// `Query` field: `true` surfaces soft-deleted (FM-33 `deleted_at`) rows
    /// so an operator can see them; nil (the default) omits the key entirely
    /// so the server's live-rows-only default applies.
    public func adminQuery<T: Codable & Sendable>(
        _ db: String, _ query: Query, includeDeleted: Bool? = nil, as _: T.Type
    ) async throws -> T {
        let response: ResultResponse = try await postJson(
            "admin query", "/admin/db/\(encodePath(db))/query",
            AdminQueryRequest(query: query, includeDeleted: includeDeleted)
        )
        return try parseResult(response.result, terminal: query.readTerminal)
    }

    /// `POST /admin/db/{db}/explain` `{query}` → `{sql, params, terminal,
    /// warnings}` (ENH-019). Compiles a Query DSL body for inspection without
    /// executing it; the returned `sql` is byte-identical to what the read
    /// path would run.
    public func explainQuery(
        _ db: String, _ query: Query
    ) async throws -> ExplainResult {
        try await postJson(
            "explain query", "/admin/db/\(encodePath(db))/explain",
            ExplainQueryRequest(query: query)
        )
    }

    /// `GET /admin/slow-queries?db=<optional>&limit=<n>` → the slow-query
    /// log (ENH-019): the bounded in-memory ring newest-first, optionally
    /// filtered by database. Pass nil for both args for the unfiltered
    /// instance-wide ring.
    public func getSlowQueries(
        db: String? = nil, limit: Int? = nil
    ) async throws -> SlowQueriesResponse {
        var query: [URLQueryItem] = []
        if let db {
            query.append(URLQueryItem(name: "db", value: db))
        }
        if let limit {
            query.append(URLQueryItem(name: "limit", value: String(limit)))
        }
        return try await getJson("slow queries", "/admin/slow-queries", query: query)
    }

    /// `POST /admin/db/{db}/mutate` `{txn, idempotencyKey?}` → `{results}`.
    /// Owner-bypass: an admin writes documents across every database
    /// regardless of `ownerField`. Mirrors `RtDbHttpClient.mutate` but routes
    /// through the admin path with `db` in the URL, so the body omits `db`.
    /// Returns one `StepResult` per step.
    public func adminMutate(
        _ db: String, _ txn: Transaction, idempotencyKey: String? = nil
    ) async throws -> [StepResult] {
        let response: AdminMutateResponse = try await postJson(
            "admin mutate", "/admin/db/\(encodePath(db))/mutate",
            AdminMutateRequest(txn: txn, idempotencyKey: idempotencyKey)
        )
        return response.results
    }

    // MARK: Backups

    /// `POST /admin/backup` (empty body) → 202 `{ok:true}`. Triggers one
    /// `pg_dump` immediately; the dump runs detached and the in-progress flag
    /// is observable via `listBackups()`. A second call while one is running
    /// → 409 CONFLICT. Runs outside the committer.
    public func backupNow() async throws {
        try await postForOk("backup now", "/admin/backup", EmptyRequest())
    }

    /// `GET /admin/backups` → `{running, backups:[{name, sizeBytes,
    /// createdMs}]}`. A missing backup dir returns an empty list.
    public func listBackups() async throws -> BackupsListResponse {
        try await getJson("list backups", "/admin/backups")
    }

    /// `GET /admin/backups/{name}` → the raw dump bytes
    /// (`application/octet-stream`) — NOT JSON-decoded.
    public func downloadBackup(_ name: String) async throws -> Data {
        let (status, data) = try await execute(
            "download backup", method: "GET", path: "/admin/backups/\(encodePath(name))"
        )
        guard (200 ..< 300).contains(status) else {
            throw RtDbError.decodeEnvelope(from: data)
                ?? RtDbError(code: .badRequest, message: "HTTP \(status)")
        }
        return data
    }

    /// `DELETE /admin/backups/{name}` → 204. Returns 404 if the file is
    /// already gone. The same `validate_dump_name` short-circuit as download
    /// runs server-side first.
    public func deleteBackup(_ name: String) async throws {
        let (status, data) = try await execute(
            "delete backup", method: "DELETE", path: "/admin/backups/\(encodePath(name))"
        )
        guard (200 ..< 300).contains(status) else {
            throw RtDbError.decodeEnvelope(from: data)
                ?? RtDbError(code: .badRequest, message: "HTTP \(status)")
        }
    }

    /// `POST /admin/restore` `{name, confirm}` → `{target, instructions}`.
    /// The client sends `confirm == name` (the typed confirmation guard
    /// mirrors `deleteDb`). Restores into a fresh `rtdb_restored_<stamp>` DB;
    /// the live DB is never touched.
    public func restoreBackup(_ name: String) async throws -> RestoreResult {
        try await postJson(
            "restore backup", "/admin/restore",
            RestoreBackupRequest(name: name, confirm: name)
        )
    }

    // MARK: Webhooks

    /// `GET /admin/db/{db}/webhooks` → `{webhooks:[...]}`. Returns an empty
    /// list when webhooks are disabled at boot.
    public func listWebhooks(_ db: String) async throws -> [Webhook] {
        let response: WebhooksResponse = try await getJson(
            "list webhooks", "/admin/db/\(encodePath(db))/webhooks"
        )
        return response.webhooks
    }

    /// `POST /admin/db/{db}/webhooks` `{url, table?, events?, enabled?}` →
    /// `{id}`. Only the provided option keys are sent; the server defaults
    /// `table` to all-tables, `events` to `["*"]`, and `enabled` to true when
    /// their keys are absent. Returns the new webhook's server-assigned id.
    public func createWebhook(
        _ db: String, options: CreateWebhookOptions
    ) async throws -> Int64 {
        let response: CreateWebhookResponse = try await postJson(
            "create webhook", "/admin/db/\(encodePath(db))/webhooks", options
        )
        return response.id
    }

    /// `PUT /admin/db/{db}/webhooks/{id}` `{url?, table?, events?,
    /// enabled?, rotateSecret?}` → the updated `Webhook`. Each present field
    /// overwrites the stored value; nil fields are unchanged. `table` is a
    /// tri-state on the wire: omitted leaves the filter alone, JSON null
    /// (`WebhookEditOptions.table = .some(nil)`) clears it to all-tables,
    /// and a string sets it.
    public func editWebhook(
        _ db: String, id: Int64, options: WebhookEditOptions
    ) async throws -> Webhook {
        try await putJson(
            "edit webhook", "/admin/db/\(encodePath(db))/webhooks/\(id)", options
        )
    }

    /// `DELETE /admin/db/{db}/webhooks/{id}` → `{ok:true}`. Cascades the
    /// webhook's pending deliveries via the foreign key.
    public func deleteWebhook(_ db: String, id: Int64) async throws {
        let response: OkResponse = try await request(
            "delete webhook", method: "DELETE",
            path: "/admin/db/\(encodePath(db))/webhooks/\(id)"
        )
        try requireOk(response)
    }

    /// `GET /admin/db/{db}/webhooks/{id}/deliveries?status=&limit=&offset=`
    /// → `{deliveries:[...]}`, newest `nextAttempt` first. `options` nil for
    /// the server-default first page (limit 50, no status filter, offset 0).
    public func listDeliveries(
        _ db: String, id: Int64, options: ListDeliveriesOptions? = nil
    ) async throws -> [WebhookDelivery] {
        var query: [URLQueryItem] = []
        if let status = options?.status {
            query.append(URLQueryItem(name: "status", value: status))
        }
        if let limit = options?.limit {
            query.append(URLQueryItem(name: "limit", value: String(limit)))
        }
        if let offset = options?.offset {
            query.append(URLQueryItem(name: "offset", value: String(offset)))
        }
        let response: DeliveriesResponse = try await getJson(
            "list deliveries", "/admin/db/\(encodePath(db))/webhooks/\(id)/deliveries",
            query: query
        )
        return response.deliveries
    }

    // MARK: Audit

    /// `GET /admin/audit?db=&table=&op=&principal=&source=&limit=&offset=` →
    /// `{entries:[...]}`, newest `tsMs` first. `db` is always sent; every
    /// other filter is omitted from the query when nil (matches all rows).
    /// `options` nil sends just `db` (server defaults: limit 100, offset 0).
    /// When audit is disabled at boot the server short-circuits to an empty
    /// list.
    public func getAudit(
        _ db: String, options: AuditQuery? = nil
    ) async throws -> [AuditEntry] {
        var query = [URLQueryItem(name: "db", value: db)]
        if let table = options?.table {
            query.append(URLQueryItem(name: "table", value: table))
        }
        if let op = options?.op {
            query.append(URLQueryItem(name: "op", value: op))
        }
        if let principal = options?.principal {
            query.append(URLQueryItem(name: "principal", value: principal))
        }
        if let source = options?.source {
            query.append(URLQueryItem(name: "source", value: source))
        }
        if let limit = options?.limit {
            query.append(URLQueryItem(name: "limit", value: String(limit)))
        }
        if let offset = options?.offset {
            query.append(URLQueryItem(name: "offset", value: String(offset)))
        }
        let response: AuditResponse = try await getJson("get audit", "/admin/audit", query: query)
        return response.entries
    }

    // MARK: Interactive sessions

    /// `GET /admin/sessions?user=&limit=` → `{sessions:[...]}`, newest-first.
    /// `options` nil lists every session server-wide (server default limit
    /// 200, clamped to 1...1000). `options.user` filters by user id or email.
    public func listSessions(
        options: SessionListOptions? = nil
    ) async throws -> [SessionInfo] {
        var query: [URLQueryItem] = []
        if let user = options?.user {
            query.append(URLQueryItem(name: "user", value: user))
        }
        if let limit = options?.limit {
            query.append(URLQueryItem(name: "limit", value: String(limit)))
        }
        let response: SessionsResponse = try await getJson(
            "list sessions", "/admin/sessions", query: query
        )
        return response.sessions
    }

    /// `DELETE /admin/sessions/{tokenHash}` → `{ok:true}`. Revokes a single
    /// session by its non-reversible sha256 digest.
    public func revokeSession(_ tokenHash: String) async throws {
        try await expectOk(
            "revoke session", method: "DELETE",
            path: "/admin/sessions/\(encodePath(tokenHash))"
        )
    }

    /// `DELETE /admin/sessions?user={userId}` → `{ok, revoked}`. Revokes
    /// every session for a user; `revoked` is the count of sessions dropped.
    public func revokeUserSessions(
        userId: String
    ) async throws -> RevokeUserSessionsResponse {
        try await request(
            "revoke user sessions", method: "DELETE", path: "/admin/sessions",
            query: [URLQueryItem(name: "user", value: userId)]
        )
    }

    /// `DELETE /admin/sessions?expired=true` → `{ok, revoked}`. Revokes every
    /// EXPIRED session instance-wide (OAuth/anonymous and admin-key login rows
    /// alike); `revoked` is the count of sessions dropped.
    public func revokeExpiredSessions() async throws -> RevokeUserSessionsResponse {
        try await request(
            "revoke expired sessions", method: "DELETE", path: "/admin/sessions",
            query: [URLQueryItem(name: "expired", value: "true")]
        )
    }

    // MARK: Anon→real account merge

    /// `POST /admin/merge-users` `{anonUserId, realUserId, confirm}` →
    /// `MergeReport`. Runs the anon→real account merge synchronously (FM-27's
    /// admin escape hatch). The server's typed guard is applied for you:
    /// `confirm` is sent as `realUserId` (same pattern as `deleteDb`). A 404
    /// means the anon user row does not exist (nothing to merge).
    public func mergeUsers(
        anonUserId: String, realUserId: String
    ) async throws -> MergeReport {
        try await postJson(
            "merge users", "/admin/merge-users",
            MergeUsersRequest(
                anonUserId: anonUserId, realUserId: realUserId, confirm: realUserId
            )
        )
    }

    // MARK: Workflow runs (admin view)

    /// `GET /admin/db/{db}/workflows?status=&limit=` → `{workflows:[...]}`,
    /// newest first. `options` nil for the server-default first page (limit
    /// default 100, capped at 500, no status filter).
    public func listWorkflows(
        _ db: String, options: WorkflowListOptions? = nil
    ) async throws -> [WorkflowInfo] {
        var query: [URLQueryItem] = []
        if let status = options?.status {
            query.append(URLQueryItem(name: "status", value: status.rawValue))
        }
        if let limit = options?.limit {
            query.append(URLQueryItem(name: "limit", value: String(limit)))
        }
        let response: WorkflowsListResponse = try await getJson(
            "list workflows", "/admin/db/\(encodePath(db))/workflows", query: query
        )
        return response.workflows
    }

    /// `GET /admin/db/{db}/workflows/{id}` → one full run row: the info
    /// projection plus the per-step outcome trail.
    public func getWorkflow(_ db: String, id: String) async throws -> WorkflowInfoFull {
        try await getJson(
            "get workflow", "/admin/db/\(encodePath(db))/workflows/\(encodePath(id))"
        )
    }

    /// `POST /admin/db/{db}/workflows` with the bare `WorkflowSpec` body (no
    /// wrapper) → `{id}`. Returns the new run's id.
    public func startWorkflow(_ db: String, spec: WorkflowSpec) async throws -> String {
        let response: IdResponse = try await postJson(
            "start workflow", "/admin/db/\(encodePath(db))/workflows", spec
        )
        return response.id
    }

    /// `POST /admin/db/{db}/workflows/{id}/cancel` → `{ok}`. False = an
    /// unknown or already-terminal run (a no-op, not an error).
    public func cancelWorkflow(_ db: String, id: String) async throws -> Bool {
        let response: OkResponse = try await request(
            "cancel workflow", method: "POST",
            path: "/admin/db/\(encodePath(db))/workflows/\(encodePath(id))/cancel"
        )
        return response.ok
    }

    /// `POST /admin/db/{db}/workflows/{id}/signal` `{name, payload?}` →
    /// `{ok}`. Ok only on delivery — the typed failures (unknown id, not
    /// waiting, name mismatch) surface as NOT_FOUND/CONFLICT errors.
    public func signalWorkflow(
        _ db: String, id: String, name: String, payload: JSONValue? = nil
    ) async throws -> Bool {
        let response: OkResponse = try await postJson(
            "signal workflow",
            "/admin/db/\(encodePath(db))/workflows/\(encodePath(id))/signal",
            SignalWorkflowParams(name: name, payload: payload)
        )
        return response.ok
    }

    /// `DELETE /admin/db/{db}/workflows/{id}` → `{ok}`. Hard-deletes the run
    /// row — unlike cancel, the outcome trail does not survive. False when
    /// already gone.
    public func deleteWorkflow(_ db: String, id: String) async throws -> Bool {
        let response: OkResponse = try await request(
            "delete workflow", method: "DELETE",
            path: "/admin/db/\(encodePath(db))/workflows/\(encodePath(id))"
        )
        return response.ok
    }

    // MARK: Schedules (admin view)

    /// `GET /admin/db/{db}/schedules` → `{schedules:[...]}`. Lists every
    /// pending and in-flight scheduled job for the database (the admin view
    /// spans all principals).
    public func listSchedules(_ db: String) async throws -> [ScheduleInfo] {
        let response: SchedulesListResponse = try await getJson(
            "list schedules", "/admin/db/\(encodePath(db))/schedules"
        )
        return response.schedules
    }

    /// `POST /admin/db/{db}/schedules` `{when, txn}` → `{id}`. Registers a
    /// scheduled job through the admin surface (the same enqueue the
    /// `schedule` mutation step and the WS `schedule` frame use). Returns the
    /// new job's server-assigned id.
    public func createSchedule(
        _ db: String, when: ScheduleWhen, txn: Transaction
    ) async throws -> String {
        let response: IdResponse = try await postJson(
            "create schedule", "/admin/db/\(encodePath(db))/schedules",
            CreateScheduleRequest(when: when, txn: txn)
        )
        return response.id
    }

    /// `POST /admin/db/{db}/schedules/{id}/cancel` → `{ok}`. False = an
    /// unknown or already-fired id (a no-op, not an error).
    public func cancelSchedule(_ db: String, id: String) async throws -> Bool {
        try await manageSchedule(db, id: id, op: "cancel")
    }

    /// `POST /admin/db/{db}/schedules/{id}/pause` → `{ok}`. False = an
    /// unknown or non-pausable id.
    public func pauseSchedule(_ db: String, id: String) async throws -> Bool {
        try await manageSchedule(db, id: id, op: "pause")
    }

    /// `POST /admin/db/{db}/schedules/{id}/resume` → `{ok}`. False = an
    /// unknown or non-paused id.
    public func resumeSchedule(_ db: String, id: String) async throws -> Bool {
        try await manageSchedule(db, id: id, op: "resume")
    }

    /// Shared bodyless-POST helper for the three manage ops — the server
    /// takes the id + op from the path and no body, and acks `{ok}` where
    /// false means "unknown or terminal id" (a no-op).
    private func manageSchedule(
        _ db: String, id: String, op: String
    ) async throws -> Bool {
        let response: OkResponse = try await request(
            "schedule \(op)", method: "POST",
            path: "/admin/db/\(encodePath(db))/schedules/\(encodePath(id))/\(op)"
        )
        return response.ok
    }

    // MARK: File storage (admin view)

    /// `GET /admin/db/{db}/storage` → `{files:[...]}`. Lists every blob the
    /// database owns (the admin view spans all principals).
    public func listFiles(_ db: String) async throws -> [FileMetadata] {
        let response: FilesListResponse = try await getJson(
            "list files", "/admin/db/\(encodePath(db))/storage"
        )
        return response.files
    }

    /// `POST /admin/db/{db}/storage` with the RAW bytes as the body (not
    /// JSON) → `{id}`. `contentType` sets the `Content-Type` header; when
    /// nil no header is sent and the server stores the blob untyped.
    /// Returns the new blob's server-assigned id.
    public func uploadFile(
        _ db: String, bytes: Data, contentType: String? = nil
    ) async throws -> String {
        let response: IdResponse = try await request(
            "upload file", method: "POST", path: "/admin/db/\(encodePath(db))/storage",
            body: bytes, contentType: contentType
        )
        return response.id
    }

    /// `DELETE /admin/db/{db}/storage/{id}` → `{ok:true}`. Idempotent — the
    /// server acks ok even when the blob is already gone.
    public func deleteFile(_ db: String, id: String) async throws {
        try await expectOk(
            "delete file", method: "DELETE",
            path: "/admin/db/\(encodePath(db))/storage/\(encodePath(id))"
        )
    }

    // MARK: Anonymous-access toggle (SEC-103)

    /// `GET /admin/db/{db}/anonymous-access` → `{enabled}` — the per-database
    /// flag only. The instance-wide `RTDB_AUTH_ANONYMOUS_ENABLED` boot gate
    /// is separate and always applies on top (both must allow for an
    /// anonymous sign-in to succeed).
    public func getAnonymousAccess(_ db: String) async throws -> Bool {
        let response: AnonymousAccessResponse = try await getJson(
            "get anonymous access", "/admin/db/\(encodePath(db))/anonymous-access"
        )
        return response.enabled
    }

    /// `PATCH /admin/db/{db}/anonymous-access` `{enabled}` → `{ok:true}`.
    /// Flips the per-database anonymous-access flag; a not-found error means
    /// the database is not registered.
    public func setAnonymousAccess(_ db: String, enabled: Bool) async throws {
        try await expectOk(
            "set anonymous access", method: "PATCH",
            path: "/admin/db/\(encodePath(db))/anonymous-access",
            body: jsonBody("set anonymous access", SetAnonymousAccessRequest(enabled: enabled)),
            contentType: "application/json"
        )
    }
}

// MARK: - Transport

extension RtDbAdminClient {
    /// encodeURIComponent parity (rust http.rs `encode_uri_component`) for
    /// caller-supplied path segments — the same allowed set as
    /// `RtDbHttpClient`.
    private static let pathSegmentAllowed = CharacterSet(
        charactersIn: "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.!~*'()"
    )

    private func encodePath(_ segment: String) -> String {
        segment.addingPercentEncoding(withAllowedCharacters: Self.pathSegmentAllowed) ?? segment
    }

    /// POST a JSON-encoded body, decode the JSON response as `Res`.
    private func postJson<Res: Decodable>(
        _ what: String, _ path: String, _ body: some Encodable
    ) async throws -> Res {
        try await request(
            what, method: "POST", path: path, body: jsonBody(what, body),
            contentType: "application/json"
        )
    }

    /// PUT a JSON-encoded body, decode the JSON response as `Res` (the one
    /// admin method that PUTs — `editWebhook`).
    private func putJson<Res: Decodable>(
        _ what: String, _ path: String, _ body: some Encodable
    ) async throws -> Res {
        try await request(
            what, method: "PUT", path: path, body: jsonBody(what, body),
            contentType: "application/json"
        )
    }

    /// DELETE with a JSON-encoded body (the admin-plane remove calls that
    /// carry their target in the body rather than the path).
    private func deleteJson<Res: Decodable>(
        _ what: String, _ path: String, _ body: some Encodable
    ) async throws -> Res {
        try await request(
            what, method: "DELETE", path: path, body: jsonBody(what, body),
            contentType: "application/json"
        )
    }

    /// PATCH a JSON-encoded body, decode the JSON response as `Res`.
    private func patchJson<Res: Decodable>(
        _ what: String, _ path: String, _ body: some Encodable
    ) async throws -> Res {
        try await request(
            what, method: "PATCH", path: path, body: jsonBody(what, body),
            contentType: "application/json"
        )
    }

    /// GET and decode a JSON response.
    private func getJson<Res: Decodable>(
        _ what: String, _ path: String, query: [URLQueryItem] = []
    ) async throws -> Res {
        try await request(what, method: "GET", path: path, query: query)
    }

    /// JSON-encode a body; encoding failures wrap as `RtDbError(internal)`
    /// like every other client path.
    private func jsonBody(_ what: String, _ body: some Encodable) throws -> Data {
        do {
            return try JSONEncoder().encode(body)
        } catch {
            throw RtDbError(code: .internal, message: "invalid \(what) request body: \(error)")
        }
    }

    /// POST/PATCH/DELETE a JSON body and require `{ok:true}`.
    private func postForOk(
        _ what: String, _ path: String, _ body: some Encodable
    ) async throws {
        let response: OkResponse = try await postJson(what, path, body)
        try requireOk(response)
    }

    /// Issue a request with a JSON body and require `{ok:true}` — the raw
    /// (non-JSON-encodable) body variants route here too.
    private func expectOk(
        _ what: String,
        method: String,
        path: String,
        body: Data? = nil,
        contentType: String? = nil,
        query: [URLQueryItem] = []
    ) async throws {
        let response: OkResponse = try await request(
            what, method: method, path: path, body: body,
            contentType: contentType, query: query
        )
        try requireOk(response)
    }

    private func requireOk(_ response: OkResponse) throws {
        guard response.ok else {
            throw RtDbError(code: .internal, message: "admin request returned ok=false")
        }
    }

    /// Issue a request and decode the response envelope as `Res`.
    private func request<Res: Decodable>(
        _ what: String,
        method: String,
        path: String,
        body: Data? = nil,
        contentType: String? = nil,
        query: [URLQueryItem] = []
    ) async throws -> Res {
        let (status, data) = try await execute(
            what, method: method, path: path, body: body,
            contentType: contentType, query: query
        )
        guard (200 ..< 300).contains(status) else {
            throw RtDbError.decodeEnvelope(from: data)
                ?? RtDbError(code: .badRequest, message: "HTTP \(status)")
        }
        do {
            return try JSONDecoder().decode(Res.self, from: data)
        } catch {
            throw RtDbError(code: .internal, message: "invalid response body: \(error)")
        }
    }

    /// Issue a request; returns `(status, body)`. Transport failures wrap as
    /// `RtDbError(internal)` — raw URL errors never escape to callers.
    private func execute(
        _ what: String,
        method: String,
        path: String,
        body: Data? = nil,
        contentType: String? = nil,
        query: [URLQueryItem] = []
    ) async throws -> (status: Int, data: Data) {
        var components = URLComponents(string: baseUrl + path)
        if !query.isEmpty {
            components?.queryItems = query
        }
        guard let url = components?.url else {
            throw RtDbError(code: .internal, message: "invalid request URL for \(what)")
        }
        var request = URLRequest(url: url)
        request.httpMethod = method
        request.setValue("Bearer \(adminKey)", forHTTPHeaderField: "Authorization")
        if let contentType {
            request.setValue(contentType, forHTTPHeaderField: "Content-Type")
        }
        request.httpBody = body
        do {
            let (data, response) = try await session.data(for: request)
            guard let http = response as? HTTPURLResponse else {
                throw RtDbError(code: .internal, message: "\(what) returned a non-HTTP response")
            }
            return (http.statusCode, data)
        } catch let error as RtDbError {
            throw error
        } catch {
            throw RtDbError(
                code: .internal, message: "\(what) request failed: \(String(describing: error))"
            )
        }
    }
}

// MARK: - Request bodies (private; nil optionals are omitted when encoded)

private struct CreateDbRequest: Encodable {
    let name: String
}

private struct DeleteDbRequest: Encodable {
    let name: String
    let confirm: String
}

private struct PushSchemaRequest: Encodable {
    let db: String
    let schema: SchemaDef
}

private struct PreviewSchemaRequest: Encodable {
    let schema: SchemaDef
}

private struct MintTokenRequest: Encodable {
    let db: String
    let name: String
    let expiresAt: Int64?
    let readOnly: Bool?
    let tables: [String]?

    enum CodingKeys: String, CodingKey {
        case db, name, expiresAt, readOnly, tables
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(db, forKey: .db)
        try container.encode(name, forKey: .name)
        try container.encodeIfPresent(expiresAt, forKey: .expiresAt)
        try container.encodeIfPresent(readOnly, forKey: .readOnly)
        try container.encodeIfPresent(tables, forKey: .tables)
    }
}

private struct RevokeTokenRequest: Encodable {
    let tokenId: String

    enum CodingKeys: String, CodingKey {
        case tokenId
    }
}

private struct AllowlistWriteRequest: Encodable {
    let db: String
    let action: String
    let email: String
}

private struct AdminsAddRequest: Encodable {
    let email: String
    let githubId: Int64?

    enum CodingKeys: String, CodingKey {
        case email, githubId
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(email, forKey: .email)
        try container.encodeIfPresent(githubId, forKey: .githubId)
    }
}

private struct AdminsRemoveRequest: Encodable {
    let email: String
}

private struct RestoreSchemaRequest: Encodable {
    let version: Int64
    let confirm: String
}

private struct AdminQueryRequest: Encodable {
    let query: Query
    let includeDeleted: Bool?

    enum CodingKeys: String, CodingKey {
        case query, includeDeleted
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(query, forKey: .query)
        try container.encodeIfPresent(includeDeleted, forKey: .includeDeleted)
    }
}

private struct ExplainQueryRequest: Encodable {
    let query: Query
}

private struct AdminMutateRequest: Encodable {
    let txn: Transaction
    let idempotencyKey: String?

    enum CodingKeys: String, CodingKey {
        case txn, idempotencyKey
    }

    func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(txn, forKey: .txn)
        try container.encodeIfPresent(idempotencyKey, forKey: .idempotencyKey)
    }
}

private struct RestoreBackupRequest: Encodable {
    let name: String
    let confirm: String
}

private struct MergeUsersRequest: Encodable {
    let anonUserId: String
    let realUserId: String
    let confirm: String

    enum CodingKeys: String, CodingKey {
        case anonUserId, realUserId, confirm
    }
}

private struct CreateScheduleRequest: Encodable {
    let when: ScheduleWhen
    let txn: Transaction

    enum CodingKeys: String, CodingKey {
        case when, txn
    }
}

private struct SetAnonymousAccessRequest: Encodable {
    let enabled: Bool
}

/// Encodes as `{}` — `backupNow`'s empty JSON body.
private struct EmptyRequest: Encodable {}

// MARK: - Response envelopes (private)

private struct OkResponse: Decodable {
    let ok: Bool
}

private struct SignalWorkflowParams: Encodable {
    let name: String
    let payload: JSONValue?
}

private struct DatabasesResponse: Decodable {
    let databases: [String]
}

private struct AllowlistListResponse: Decodable {
    let emails: [String]
}

private struct AdminsListResponse: Decodable {
    let admins: [AdminMember]
}

private struct TokensResponse: Decodable {
    let tokens: [TokenInfo]
}

private struct SchemaHistoryListResponse: Decodable {
    let entries: [SchemaHistorySummary]
}

private struct RestoreSchemaResponse: Decodable {
    let restoredTo: Int64

    enum CodingKeys: String, CodingKey {
        case restoredTo
    }
}

private struct OpsRecentResponse: Decodable {
    let ops: [OpEvent]
}

private struct ResultResponse: Decodable {
    let result: JSONValue
}

private struct AdminMutateResponse: Decodable {
    let results: [StepResult]
}

private struct CreateWebhookResponse: Decodable {
    let id: Int64
}

private struct WebhooksResponse: Decodable {
    let webhooks: [Webhook]
}

private struct DeliveriesResponse: Decodable {
    let deliveries: [WebhookDelivery]
}

private struct AuditResponse: Decodable {
    let entries: [AuditEntry]
}

private struct SessionsResponse: Decodable {
    let sessions: [SessionInfo]
}

private struct WorkflowsListResponse: Decodable {
    let workflows: [WorkflowInfo]
}

private struct SchedulesListResponse: Decodable {
    let schedules: [ScheduleInfo]
}

private struct FilesListResponse: Decodable {
    let files: [FileMetadata]
}

private struct IdResponse: Decodable {
    let id: String
}

private struct AnonymousAccessResponse: Decodable {
    let enabled: Bool
}
