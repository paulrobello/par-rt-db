import Foundation

// HTTP request/response bodies for `/admin/*` — the Swift mirror of
// rust-client/src/wire/admin.rs (itself the mirror of the server's admin
// handler structs, not the WS protocol.rs), field-for-field; the casing is
// load-bearing (`tokenId` is camelCase on the wire, `github_id` rides as
// `githubId`). Response types tolerate unknown fields (rust carries no
// `deny_unknown_fields` on them) and default the fields rust marks
// `#[serde(default)]` so an older server's payload still parses.

// MARK: - Members, stats, tokens

/// One row of the admin allowlist returned by `GET /admin/admins`.
public struct AdminMember: Equatable, Codable, Sendable {
    /// The admin's email (the allowlist key).
    public var email: String
    /// GitHub numeric id when linked; nil when not.
    public var githubId: Int64?

    public init(email: String, githubId: Int64? = nil) {
        self.email = email
        self.githubId = githubId
    }

    enum CodingKeys: String, CodingKey {
        case email, githubId
    }
}

/// One row of `DbStats.tables` (`GET /admin/dbs/{db}/stats`).
public struct TableStat: Equatable, Codable, Sendable {
    /// Table name.
    public var name: String
    /// Live row count.
    public var rowCount: Int64
    /// On-disk size in bytes.
    public var sizeBytes: Int64

    public init(name: String, rowCount: Int64, sizeBytes: Int64) {
        self.name = name
        self.rowCount = rowCount
        self.sizeBytes = sizeBytes
    }

    enum CodingKeys: String, CodingKey {
        case name, rowCount, sizeBytes
    }
}

/// `GET /admin/dbs/{db}/stats` response — per-table row counts, sizes, and
/// the six ENH-011 quota/usage fields (0 = unlimited).
public struct DbStats: Equatable, Codable, Sendable {
    /// Per-table stats.
    public var tables: [TableStat]
    /// Whole-db size in bytes.
    public var totalSizeBytes: Int64
    /// Per-db table quota; 0 = unlimited.
    public var tablesQuota: Int64
    /// Tables currently pushed.
    public var tablesUsed: Int64
    /// Storage cap in bytes; 0 = unlimited.
    public var storageQuotaBytes: Int64
    /// Storage currently used.
    public var storageUsedBytes: Int64
    /// Subscription cap; 0 = unlimited.
    public var subsQuota: Int64
    /// Live subscriptions.
    public var subsUsed: Int64

    public init(
        tables: [TableStat],
        totalSizeBytes: Int64,
        tablesQuota: Int64,
        tablesUsed: Int64,
        storageQuotaBytes: Int64,
        storageUsedBytes: Int64,
        subsQuota: Int64,
        subsUsed: Int64
    ) {
        self.tables = tables
        self.totalSizeBytes = totalSizeBytes
        self.tablesQuota = tablesQuota
        self.tablesUsed = tablesUsed
        self.storageQuotaBytes = storageQuotaBytes
        self.storageUsedBytes = storageUsedBytes
        self.subsQuota = subsQuota
        self.subsUsed = subsUsed
    }

    enum CodingKeys: String, CodingKey {
        case tables, totalSizeBytes, tablesQuota, tablesUsed
        case storageQuotaBytes, storageUsedBytes, subsQuota, subsUsed
    }
}

/// Returned by `mintToken`: the server's `{tokenId, token}` shape, with the
/// plaintext bearer shown once and never stored server-side.
public struct MintedToken: Equatable, Codable, Sendable {
    /// The minted token's stable id (for revoke/list).
    public var tokenId: String
    /// The plaintext bearer token.
    public var token: String

    public init(tokenId: String, token: String) {
        self.tokenId = tokenId
        self.token = token
    }

    enum CodingKeys: String, CodingKey {
        case tokenId, token
    }
}

/// Optional capabilities for
/// `RtDbAdminClient.mintToken(_:name:options:)`. Every field is optional;
/// the default is a full-access mint (no expiry, read-write, all tables) —
/// the server applies those defaults to any field left nil.
public struct MintTokenOptions: Equatable, Sendable {
    /// Unix-millis expiry (`expiresAt` on the wire). nil = no expiry.
    public var expiresAt: Int64?
    /// `readOnly` on the wire. nil = read-write (server default).
    public var readOnly: Bool?
    /// `tables` allowlist on the wire. nil = all tables (server default).
    public var tables: [String]?

    public init(expiresAt: Int64? = nil, readOnly: Bool? = nil, tables: [String]? = nil) {
        self.expiresAt = expiresAt
        self.readOnly = readOnly
        self.tables = tables
    }
}

/// One row of the token list returned by `GET /admin/tokens?db=…`. The
/// capability fields (`expiresAt`/`readOnly`/`tables`) default so an older
/// server that omits them still parses.
public struct TokenInfo: Equatable, Codable, Sendable {
    /// Stable token id (for revoke).
    public var id: String
    /// Operator-assigned label.
    public var name: String
    /// Mint time, epoch ms.
    public var createdAt: Int64
    /// Whether the token is revoked.
    public var revoked: Bool
    /// nil = no expiry (defaults nil for older servers).
    public var expiresAt: Int64?
    /// Server always emits `readOnly`; defaults false for older servers.
    public var readOnly: Bool
    /// nil = all tables (defaults nil for older servers).
    public var tables: [String]?

    public init(
        id: String,
        name: String,
        createdAt: Int64,
        revoked: Bool,
        expiresAt: Int64? = nil,
        readOnly: Bool = false,
        tables: [String]? = nil
    ) {
        self.id = id
        self.name = name
        self.createdAt = createdAt
        self.revoked = revoked
        self.expiresAt = expiresAt
        self.readOnly = readOnly
        self.tables = tables
    }

    enum CodingKeys: String, CodingKey {
        case id, name, createdAt, revoked, expiresAt, readOnly, tables
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(String.self, forKey: .id)
        name = try container.decode(String.self, forKey: .name)
        createdAt = try container.decode(Int64.self, forKey: .createdAt)
        revoked = try container.decode(Bool.self, forKey: .revoked)
        expiresAt = try container.decodeIfPresent(Int64.self, forKey: .expiresAt)
        readOnly = try container.decodeIfPresent(Bool.self, forKey: .readOnly) ?? false
        tables = try container.decodeIfPresent([String].self, forKey: .tables)
    }
}

// MARK: - Interactive sessions

/// One active interactive session as returned by `GET /admin/sessions`.
/// `tokenHash` is a non-reversible sha256 digest (the plaintext token is
/// never stored), safe to surface to an admin and used to target a row for
/// revoke. `email`/`login` are nil when the user has none (e.g. an
/// anonymous session).
public struct SessionInfo: Equatable, Codable, Sendable {
    /// Non-reversible sha256 of the session token (revoke target).
    public var tokenHash: String
    /// The authed user's id.
    public var userId: String
    /// nil when the user has no email (e.g. an anonymous session).
    public var email: String?
    /// nil when the user has no login handle.
    public var login: String?
    /// Whether this is an anonymous session.
    public var anonymous: Bool
    /// Login time, epoch ms.
    public var createdAt: Int64
    /// Expiry time, epoch ms.
    public var expiresAt: Int64

    public init(
        tokenHash: String,
        userId: String,
        email: String? = nil,
        login: String? = nil,
        anonymous: Bool,
        createdAt: Int64,
        expiresAt: Int64
    ) {
        self.tokenHash = tokenHash
        self.userId = userId
        self.email = email
        self.login = login
        self.anonymous = anonymous
        self.createdAt = createdAt
        self.expiresAt = expiresAt
    }

    enum CodingKeys: String, CodingKey {
        case tokenHash, userId, email, login, anonymous, createdAt, expiresAt
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        tokenHash = try container.decode(String.self, forKey: .tokenHash)
        userId = try container.decode(String.self, forKey: .userId)
        email = try container.decodeIfPresent(String.self, forKey: .email)
        login = try container.decodeIfPresent(String.self, forKey: .login)
        anonymous = try container.decode(Bool.self, forKey: .anonymous)
        createdAt = try container.decode(Int64.self, forKey: .createdAt)
        expiresAt = try container.decode(Int64.self, forKey: .expiresAt)
    }
}

/// Optional filter for `RtDbAdminClient.listSessions(options:)`
/// (`GET /admin/sessions?user=&limit=`): `user` filters by user id or email;
/// `limit` pages the result (server default 200, clamped to 1...1000).
public struct SessionListOptions: Equatable, Sendable {
    /// Filter by user id or email.
    public var user: String?
    /// Page size (server default 200, clamped to 1...1000).
    public var limit: Int64?

    public init(user: String? = nil, limit: Int64? = nil) {
        self.user = user
        self.limit = limit
    }
}

/// `DELETE /admin/sessions?user={userId}` → `{ok, revoked}` where `revoked`
/// is the count of sessions dropped.
public struct RevokeUserSessionsResponse: Equatable, Codable, Sendable {
    /// Always true on success.
    public var ok: Bool
    /// How many sessions were dropped.
    public var revoked: Int64

    public init(ok: Bool, revoked: Int64) {
        self.ok = ok
        self.revoked = revoked
    }

    enum CodingKeys: String, CodingKey {
        case ok, revoked
    }
}

// MARK: - Anon→real account merge

/// A row skipped by the anon→real merge because the re-stamp would collide
/// with an existing doc under a unique index.
public struct MergeConflict: Equatable, Codable, Sendable {
    /// The conflicting row's table.
    public var table: String
    /// The conflicting row's id.
    public var id: String

    public init(table: String, id: String) {
        self.table = table
        self.id = id
    }

    enum CodingKeys: String, CodingKey {
        case table, id
    }
}

/// Per-database outcome of an anon→real merge: re-stamped-doc counts per
/// table plus the rows skipped over unique-index conflicts.
public struct MergeDbResult: Equatable, Codable, Sendable {
    /// Re-stamped-doc counts per table.
    public var tables: [String: Int]
    /// Rows skipped over unique-index collisions.
    public var conflicts: [MergeConflict]

    public init(tables: [String: Int] = [:], conflicts: [MergeConflict] = []) {
        self.tables = tables
        self.conflicts = conflicts
    }

    enum CodingKeys: String, CodingKey {
        case tables, conflicts
    }
}

/// Full-instance anon→real merge outcome from `POST /admin/merge-users`:
/// per-db doc re-stamps, storage blobs repointed, sessions repointed (an
/// open WS or stored SDK token promotes to the real principal on its next
/// op), and whether the anon user row was deleted.
public struct MergeReport: Equatable, Codable, Sendable {
    /// Per-db doc re-stamp outcomes.
    public var dbs: [String: MergeDbResult]
    /// Storage blobs moved to the real user.
    public var storageRepointed: UInt64
    /// Sessions re-pointed to the real user.
    public var sessionsRepointed: UInt64
    /// Whether the anon user row was removed.
    public var anonDeleted: Bool

    public init(
        dbs: [String: MergeDbResult] = [:],
        storageRepointed: UInt64 = 0,
        sessionsRepointed: UInt64 = 0,
        anonDeleted: Bool = false
    ) {
        self.dbs = dbs
        self.storageRepointed = storageRepointed
        self.sessionsRepointed = sessionsRepointed
        self.anonDeleted = anonDeleted
    }

    enum CodingKeys: String, CodingKey {
        case dbs, storageRepointed, sessionsRepointed, anonDeleted
    }
}

// MARK: - Metrics + subscription inspector

/// p50/p95/p99 latency percentile triple (microseconds).
public struct LatencyStats: Equatable, Codable, Sendable {
    /// Median, microseconds.
    public var p50: Int64
    /// 95th percentile, microseconds.
    public var p95: Int64
    /// 99th percentile, microseconds.
    public var p99: Int64

    public init(p50: Int64, p95: Int64, p99: Int64) {
        self.p50 = p50
        self.p95 = p95
        self.p99 = p99
    }

    enum CodingKeys: String, CodingKey {
        case p50, p95, p99
    }
}

/// `GET /admin/metrics` snapshot — server counters and gauges. The
/// subscription-invalidation counters and `perDbSubs` default (0 / empty) so
/// a client built against a newer server still deserializes an older
/// server's response; 0 is the correct "not reported" value for a monotonic
/// counter.
public struct MetricsSnapshot: Equatable, Codable, Sendable {
    /// Queries served since boot.
    public var queriesTotal: Int64
    /// Transactions applied since boot.
    public var mutationsTotal: Int64
    /// File uploads since boot.
    public var uploadsTotal: Int64
    /// Open `/sync` sockets.
    public var wsConnections: Int64
    /// Live query subscriptions.
    public var activeSubscriptions: Int64
    /// Postgres pool connections.
    public var poolSize: Int64
    /// Idle pool connections.
    public var poolIdle: Int64
    /// Process uptime.
    public var uptimeSeconds: Int64
    /// Query latency percentiles.
    public var queryLatency: LatencyStats
    /// Mutation latency percentiles.
    public var mutateLatency: LatencyStats
    /// Subscribe latency percentiles.
    public var subscribeLatency: LatencyStats
    /// Read-set decisions that ended in a re-run.
    public var subsRerunsTotal: Int64
    /// Skips proven by a `get(id)` point read.
    public var subsSkipsPointTotal: Int64
    /// Skips proven by an eq-prefix window.
    public var subsSkipsIndexedTotal: Int64
    /// Skips proven by a top-N sort boundary.
    public var subsSkipsOrderedTotal: Int64
    /// Sampled shadow verifications of skips.
    public var subsSkipVerificationsTotal: Int64
    /// Verifications that found a skip WRONG — alert on any increase
    /// (invalidation under-approximated: a dropped realtime update).
    public var subsMissedPushesTotal: Int64
    /// ENH-010 per-db breakdown of the subscription counters above.
    public var perDbSubs: [DbSubCounters]

    public init(
        queriesTotal: Int64,
        mutationsTotal: Int64,
        uploadsTotal: Int64,
        wsConnections: Int64,
        activeSubscriptions: Int64,
        poolSize: Int64,
        poolIdle: Int64,
        uptimeSeconds: Int64,
        queryLatency: LatencyStats,
        mutateLatency: LatencyStats,
        subscribeLatency: LatencyStats,
        subsRerunsTotal: Int64 = 0,
        subsSkipsPointTotal: Int64 = 0,
        subsSkipsIndexedTotal: Int64 = 0,
        subsSkipsOrderedTotal: Int64 = 0,
        subsSkipVerificationsTotal: Int64 = 0,
        subsMissedPushesTotal: Int64 = 0,
        perDbSubs: [DbSubCounters] = []
    ) {
        self.queriesTotal = queriesTotal
        self.mutationsTotal = mutationsTotal
        self.uploadsTotal = uploadsTotal
        self.wsConnections = wsConnections
        self.activeSubscriptions = activeSubscriptions
        self.poolSize = poolSize
        self.poolIdle = poolIdle
        self.uptimeSeconds = uptimeSeconds
        self.queryLatency = queryLatency
        self.mutateLatency = mutateLatency
        self.subscribeLatency = subscribeLatency
        self.subsRerunsTotal = subsRerunsTotal
        self.subsSkipsPointTotal = subsSkipsPointTotal
        self.subsSkipsIndexedTotal = subsSkipsIndexedTotal
        self.subsSkipsOrderedTotal = subsSkipsOrderedTotal
        self.subsSkipVerificationsTotal = subsSkipVerificationsTotal
        self.subsMissedPushesTotal = subsMissedPushesTotal
        self.perDbSubs = perDbSubs
    }

    enum CodingKeys: String, CodingKey {
        case queriesTotal, mutationsTotal, uploadsTotal, wsConnections
        case activeSubscriptions, poolSize, poolIdle, uptimeSeconds
        case queryLatency, mutateLatency, subscribeLatency
        case subsRerunsTotal, subsSkipsPointTotal, subsSkipsIndexedTotal
        case subsSkipsOrderedTotal, subsSkipVerificationsTotal, subsMissedPushesTotal
        case perDbSubs
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        queriesTotal = try container.decode(Int64.self, forKey: .queriesTotal)
        mutationsTotal = try container.decode(Int64.self, forKey: .mutationsTotal)
        uploadsTotal = try container.decode(Int64.self, forKey: .uploadsTotal)
        wsConnections = try container.decode(Int64.self, forKey: .wsConnections)
        activeSubscriptions = try container.decode(Int64.self, forKey: .activeSubscriptions)
        poolSize = try container.decode(Int64.self, forKey: .poolSize)
        poolIdle = try container.decode(Int64.self, forKey: .poolIdle)
        uptimeSeconds = try container.decode(Int64.self, forKey: .uptimeSeconds)
        queryLatency = try container.decode(LatencyStats.self, forKey: .queryLatency)
        mutateLatency = try container.decode(LatencyStats.self, forKey: .mutateLatency)
        subscribeLatency = try container.decode(LatencyStats.self, forKey: .subscribeLatency)
        subsRerunsTotal = try container.decodeIfPresent(Int64.self, forKey: .subsRerunsTotal) ?? 0
        subsSkipsPointTotal =
            try container.decodeIfPresent(Int64.self, forKey: .subsSkipsPointTotal) ?? 0
        subsSkipsIndexedTotal =
            try container.decodeIfPresent(Int64.self, forKey: .subsSkipsIndexedTotal) ?? 0
        subsSkipsOrderedTotal =
            try container.decodeIfPresent(Int64.self, forKey: .subsSkipsOrderedTotal) ?? 0
        subsSkipVerificationsTotal =
            try container.decodeIfPresent(Int64.self, forKey: .subsSkipVerificationsTotal) ?? 0
        subsMissedPushesTotal =
            try container.decodeIfPresent(Int64.self, forKey: .subsMissedPushesTotal) ?? 0
        perDbSubs = try container.decodeIfPresent([DbSubCounters].self, forKey: .perDbSubs) ?? []
    }
}

/// Subscriber identity for `SubscriptionInfo`. The server emits null for
/// `userId`/`email` when the subscriber has no interactive identity — a
/// machine token, a scheduled job, or admin bypass.
public struct SubscriptionsPrincipal: Equatable, Codable, Sendable {
    /// User id when interactive.
    public var userId: String?
    /// Email when known.
    public var email: String?

    public init(userId: String? = nil, email: String? = nil) {
        self.userId = userId
        self.email = email
    }

    enum CodingKeys: String, CodingKey {
        case userId, email
    }
}

/// One live subscription and the read-set class that governs its skip/re-run
/// invalidation — one row of `SubscriptionsResponse.subscriptions`.
public struct SubscriptionInfo: Equatable, Codable, Sendable {
    /// Which database the subscription reads.
    public var db: String
    /// The queried table.
    public var table: String
    /// The query terminal.
    public var terminal: String
    /// `point` / `indexed` / `ordered` / `table`.
    public var readSetClass: String
    /// Subscriber identity (nil for machine/bypass).
    public var principal: SubscriptionsPrincipal?

    public init(
        db: String,
        table: String,
        terminal: String,
        readSetClass: String,
        principal: SubscriptionsPrincipal? = nil
    ) {
        self.db = db
        self.table = table
        self.terminal = terminal
        self.readSetClass = readSetClass
        self.principal = principal
    }

    enum CodingKeys: String, CodingKey {
        case db, table, terminal, readSetClass, principal
    }
}

/// Per-db subscription-invalidation counters — one row of
/// `SubscriptionsResponse.perDb` and `MetricsSnapshot.perDbSubs`.
public struct DbSubCounters: Equatable, Codable, Sendable {
    /// Which database.
    public var db: String
    /// Fan-out decisions that re-ran.
    public var reruns: UInt64
    /// Skips proven by a point read.
    public var skipsPoint: UInt64
    /// Skips proven by an eq-prefix window.
    public var skipsIndexed: UInt64
    /// Skips proven by a top-N boundary.
    public var skipsOrdered: UInt64
    /// Verifications that found a skip wrong.
    public var missed: UInt64
    /// Total skips across the three classes (ENH-024; defaults 0 for an
    /// older server).
    public var skips: UInt64
    /// `reruns / max(1, reruns + skips)` — in 0...1; sustained above 0.5
    /// means re-runs dominate this db's fan-out (ENH-024; same default).
    public var rerunRatio: Double

    public init(
        db: String,
        reruns: UInt64,
        skipsPoint: UInt64,
        skipsIndexed: UInt64,
        skipsOrdered: UInt64,
        missed: UInt64,
        skips: UInt64 = 0,
        rerunRatio: Double = 0
    ) {
        self.db = db
        self.reruns = reruns
        self.skipsPoint = skipsPoint
        self.skipsIndexed = skipsIndexed
        self.skipsOrdered = skipsOrdered
        self.missed = missed
        self.skips = skips
        self.rerunRatio = rerunRatio
    }

    enum CodingKeys: String, CodingKey {
        case db, reruns, skipsPoint, skipsIndexed, skipsOrdered, missed, skips, rerunRatio
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        db = try container.decode(String.self, forKey: .db)
        reruns = try container.decode(UInt64.self, forKey: .reruns)
        skipsPoint = try container.decode(UInt64.self, forKey: .skipsPoint)
        skipsIndexed = try container.decode(UInt64.self, forKey: .skipsIndexed)
        skipsOrdered = try container.decode(UInt64.self, forKey: .skipsOrdered)
        missed = try container.decode(UInt64.self, forKey: .missed)
        skips = try container.decodeIfPresent(UInt64.self, forKey: .skips) ?? 0
        rerunRatio = try container.decodeIfPresent(Double.self, forKey: .rerunRatio) ?? 0
    }
}

/// `GET /admin/subscriptions?db=<optional>` response (ENH-010): the live
/// subscription inspector — every active subscription plus the
/// invalidation-effectiveness counters server-wide and per-db.
public struct SubscriptionsResponse: Equatable, Codable, Sendable {
    /// Every live subscription.
    public var subscriptions: [SubscriptionInfo]
    /// Server-wide re-runs.
    public var subsRerunsTotal: UInt64
    /// Server-wide point skips.
    public var subsSkipsPointTotal: UInt64
    /// Server-wide indexed skips.
    public var subsSkipsIndexedTotal: UInt64
    /// Server-wide ordered skips.
    public var subsSkipsOrderedTotal: UInt64
    /// Server-wide missed pushes.
    public var subsMissedPushesTotal: UInt64
    /// The same counters per database.
    public var perDb: [DbSubCounters]

    public init(
        subscriptions: [SubscriptionInfo],
        subsRerunsTotal: UInt64,
        subsSkipsPointTotal: UInt64,
        subsSkipsIndexedTotal: UInt64,
        subsSkipsOrderedTotal: UInt64,
        subsMissedPushesTotal: UInt64,
        perDb: [DbSubCounters]
    ) {
        self.subscriptions = subscriptions
        self.subsRerunsTotal = subsRerunsTotal
        self.subsSkipsPointTotal = subsSkipsPointTotal
        self.subsSkipsIndexedTotal = subsSkipsIndexedTotal
        self.subsSkipsOrderedTotal = subsSkipsOrderedTotal
        self.subsMissedPushesTotal = subsMissedPushesTotal
        self.perDb = perDb
    }

    enum CodingKeys: String, CodingKey {
        case subscriptions, subsRerunsTotal, subsSkipsPointTotal
        case subsSkipsIndexedTotal, subsSkipsOrderedTotal, subsMissedPushesTotal, perDb
    }
}

// MARK: - Hot config

/// Runtime-mutable hot-config subset of `ConfigResponse`.
public struct HotConfig: Equatable, Codable, Sendable {
    /// CORS allowlist (hot-reloaded per request).
    public var allowedOrigins: [String]
    /// Session cookie lifetime in days.
    public var sessionTtlDays: Int64
    /// Upload size cap in bytes.
    public var maxFileSize: Int64
    /// Idempotency-key retention window.
    public var idempotencyTtlMs: Int64
    /// Per-db table quota; 0 = unlimited.
    public var maxTablesPerDb: Int64
    /// Storage cap per db in bytes; 0 = unlimited.
    public var maxStorageBytesPerDb: Int64
    /// Subscription cap per db; 0 = unlimited.
    public var maxSubsPerDb: Int64

    public init(
        allowedOrigins: [String],
        sessionTtlDays: Int64,
        maxFileSize: Int64,
        idempotencyTtlMs: Int64,
        maxTablesPerDb: Int64,
        maxStorageBytesPerDb: Int64,
        maxSubsPerDb: Int64
    ) {
        self.allowedOrigins = allowedOrigins
        self.sessionTtlDays = sessionTtlDays
        self.maxFileSize = maxFileSize
        self.idempotencyTtlMs = idempotencyTtlMs
        self.maxTablesPerDb = maxTablesPerDb
        self.maxStorageBytesPerDb = maxStorageBytesPerDb
        self.maxSubsPerDb = maxSubsPerDb
    }

    enum CodingKeys: String, CodingKey {
        case allowedOrigins, sessionTtlDays, maxFileSize, idempotencyTtlMs
        case maxTablesPerDb, maxStorageBytesPerDb, maxSubsPerDb
    }
}

/// `GET /admin/config` response — redacted boot config + hot config + build
/// identity + admin allowlist.
public struct ConfigResponse: Equatable, Codable, Sendable {
    /// HTTP listen port.
    public var port: Int64
    /// Configured public origin.
    public var publicUrl: String
    /// GitHub OAuth base (overrideable for GitHub Enterprise).
    public var githubBaseUrl: String
    /// GitHub API base.
    public var githubApiUrl: String
    /// Boot redaction: whether the DB URL is set.
    public var databaseUrlConfigured: Bool
    /// Boot redaction: whether the admin key is set.
    public var adminKeyConfigured: Bool
    /// Whether GitHub OAuth is configured.
    public var githubConfigured: Bool
    /// Whether Google OAuth is configured.
    public var googleConfigured: Bool
    /// Whether GitLab OAuth is configured.
    public var gitlabConfigured: Bool
    /// Whether generic OIDC is configured.
    public var oidcConfigured: Bool
    /// The runtime-mutable subset.
    public var hot: HotConfig
    /// Crate version.
    public var version: String
    /// Build commit label.
    public var gitCommit: String
    /// The server-wide admin allowlist.
    public var admins: [AdminMember]

    public init(
        port: Int64,
        publicUrl: String,
        githubBaseUrl: String,
        githubApiUrl: String,
        databaseUrlConfigured: Bool,
        adminKeyConfigured: Bool,
        githubConfigured: Bool,
        googleConfigured: Bool,
        gitlabConfigured: Bool,
        oidcConfigured: Bool,
        hot: HotConfig,
        version: String,
        gitCommit: String,
        admins: [AdminMember]
    ) {
        self.port = port
        self.publicUrl = publicUrl
        self.githubBaseUrl = githubBaseUrl
        self.githubApiUrl = githubApiUrl
        self.databaseUrlConfigured = databaseUrlConfigured
        self.adminKeyConfigured = adminKeyConfigured
        self.githubConfigured = githubConfigured
        self.googleConfigured = googleConfigured
        self.gitlabConfigured = gitlabConfigured
        self.oidcConfigured = oidcConfigured
        self.hot = hot
        self.version = version
        self.gitCommit = gitCommit
        self.admins = admins
    }

    enum CodingKeys: String, CodingKey {
        case port, publicUrl, githubBaseUrl, githubApiUrl, databaseUrlConfigured
        case adminKeyConfigured, githubConfigured, googleConfigured, gitlabConfigured
        case oidcConfigured, hot, version, gitCommit, admins
    }
}

/// `PATCH /admin/config` body — every field optional; nil leaves that
/// setting unchanged, and only the set keys are sent. The server rejects
/// unknown fields.
public struct HotConfigPatch: Equatable, Encodable, Sendable {
    /// New value; nil leaves it unchanged.
    public var allowedOrigins: [String]?
    /// New value; nil leaves it unchanged.
    public var sessionTtlDays: Int64?
    /// New value; nil leaves it unchanged.
    public var maxFileSize: Int64?
    /// New value; nil leaves it unchanged.
    public var idempotencyTtlMs: Int64?
    /// New value; nil leaves it unchanged.
    public var maxTablesPerDb: Int64?
    /// New value; nil leaves it unchanged.
    public var maxStorageBytesPerDb: Int64?
    /// New value; nil leaves it unchanged.
    public var maxSubsPerDb: Int64?

    public init(
        allowedOrigins: [String]? = nil,
        sessionTtlDays: Int64? = nil,
        maxFileSize: Int64? = nil,
        idempotencyTtlMs: Int64? = nil,
        maxTablesPerDb: Int64? = nil,
        maxStorageBytesPerDb: Int64? = nil,
        maxSubsPerDb: Int64? = nil
    ) {
        self.allowedOrigins = allowedOrigins
        self.sessionTtlDays = sessionTtlDays
        self.maxFileSize = maxFileSize
        self.idempotencyTtlMs = idempotencyTtlMs
        self.maxTablesPerDb = maxTablesPerDb
        self.maxStorageBytesPerDb = maxStorageBytesPerDb
        self.maxSubsPerDb = maxSubsPerDb
    }

    enum CodingKeys: String, CodingKey {
        case allowedOrigins, sessionTtlDays, maxFileSize, idempotencyTtlMs
        case maxTablesPerDb, maxStorageBytesPerDb, maxSubsPerDb
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encodeIfPresent(allowedOrigins, forKey: .allowedOrigins)
        try container.encodeIfPresent(sessionTtlDays, forKey: .sessionTtlDays)
        try container.encodeIfPresent(maxFileSize, forKey: .maxFileSize)
        try container.encodeIfPresent(idempotencyTtlMs, forKey: .idempotencyTtlMs)
        try container.encodeIfPresent(maxTablesPerDb, forKey: .maxTablesPerDb)
        try container.encodeIfPresent(maxStorageBytesPerDb, forKey: .maxStorageBytesPerDb)
        try container.encodeIfPresent(maxSubsPerDb, forKey: .maxSubsPerDb)
    }
}

// MARK: - Op feed

/// One row of the op feed returned by `GET /admin/ops/recent`. `kind` is a
/// pass-through string (`insert`/`patch`/…); consumers match on it.
public struct OpEvent: Equatable, Codable, Sendable {
    /// Which database.
    public var db: String
    /// Which table.
    public var table: String
    /// The document's id.
    public var docId: String
    /// The op kind (`insert`/`patch`/…).
    public var kind: String
    /// Commit time, epoch ms.
    public var ts: Int64
    /// Per-row owner principal, when one applies (nil for `string | null`).
    public var owner: String?

    public init(
        db: String, table: String, docId: String, kind: String, ts: Int64, owner: String? = nil
    ) {
        self.db = db
        self.table = table
        self.docId = docId
        self.kind = kind
        self.ts = ts
        self.owner = owner
    }

    enum CodingKeys: String, CodingKey {
        case db, table, docId, kind, ts, owner
    }
}

// MARK: - Schema history / preview

/// One row of `GET /admin/db/{db}/schema/history` (newest-first).
/// `source` is the event that captured the snapshot:
/// `"push"` | `"migrate"` | `"restore"`.
public struct SchemaHistorySummary: Equatable, Codable, Sendable {
    /// Snapshot version (monotonic).
    public var version: Int64
    /// Capture time, epoch ms.
    public var capturedAt: Int64
    /// `"push"` / `"migrate"` / `"restore"`.
    public var source: String
    /// Who captured it, when known.
    public var principal: String?

    public init(version: Int64, capturedAt: Int64, source: String, principal: String? = nil) {
        self.version = version
        self.capturedAt = capturedAt
        self.source = source
        self.principal = principal
    }

    enum CodingKeys: String, CodingKey {
        case version, capturedAt, source, principal
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        version = try container.decode(Int64.self, forKey: .version)
        capturedAt = try container.decode(Int64.self, forKey: .capturedAt)
        source = try container.decode(String.self, forKey: .source)
        principal = try container.decodeIfPresent(String.self, forKey: .principal)
    }
}

/// One full snapshot from `GET /admin/db/{db}/schema/history/{version}`,
/// adding the `schema` blob. The schema is the raw captured JSON (a
/// serialized `SchemaDef`), kept verbatim so an older snapshot never fails
/// to deserialize.
public struct SchemaHistoryEntry: Equatable, Codable, Sendable {
    /// Snapshot version.
    public var version: Int64
    /// Capture time, epoch ms.
    public var capturedAt: Int64
    /// `"push"` / `"migrate"` / `"restore"`.
    public var source: String
    /// Who captured it, when known.
    public var principal: String?
    /// The captured schema JSON, verbatim.
    public var schema: JSONValue

    public init(
        version: Int64,
        capturedAt: Int64,
        source: String,
        principal: String? = nil,
        schema: JSONValue
    ) {
        self.version = version
        self.capturedAt = capturedAt
        self.source = source
        self.principal = principal
        self.schema = schema
    }

    enum CodingKeys: String, CodingKey {
        case version, capturedAt, source, principal, schema
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        version = try container.decode(Int64.self, forKey: .version)
        capturedAt = try container.decode(Int64.self, forKey: .capturedAt)
        source = try container.decode(String.self, forKey: .source)
        principal = try container.decodeIfPresent(String.self, forKey: .principal)
        schema = try container.decode(JSONValue.self, forKey: .schema)
    }
}

/// One new column reported by `previewSchema`. `fieldType` is the
/// human-readable field type (e.g. `string`, `id<projects>`, `string?`).
public struct SchemaPreviewColumnAdd: Equatable, Codable, Sendable {
    /// Column name.
    public var name: String
    /// Human-readable type (`string`, `id<projects>`, …).
    public var fieldType: String

    public init(name: String, fieldType: String) {
        self.name = name
        self.fieldType = fieldType
    }

    enum CodingKeys: String, CodingKey {
        case name, fieldType
    }
}

/// One new index reported by `previewSchema`.
public struct SchemaPreviewIndexAdd: Equatable, Codable, Sendable {
    /// Index name.
    public var name: String
    /// The indexed fields.
    public var fields: [String]

    public init(name: String, fields: [String]) {
        self.name = name
        self.fields = fields
    }

    enum CodingKeys: String, CodingKey {
        case name, fields
    }
}

/// One new table reported by `previewSchema`: its name plus the columns and
/// indexes the additive-only push would add.
public struct SchemaPreviewTableAdd: Equatable, Codable, Sendable {
    /// New table name.
    public var table: String
    /// Columns the push would add.
    public var columns: [SchemaPreviewColumnAdd]
    /// Indexes the push would add.
    public var indexes: [SchemaPreviewIndexAdd]

    public init(table: String, columns: [SchemaPreviewColumnAdd], indexes: [SchemaPreviewIndexAdd]) {
        self.table = table
        self.columns = columns
        self.indexes = indexes
    }

    enum CodingKeys: String, CodingKey {
        case table, columns, indexes
    }
}

/// One rejection reported by `previewSchema`: a drop or type change the DDL
/// layer will refuse. `item` is the bare column/index name.
public struct SchemaPreviewRejection: Equatable, Codable, Sendable {
    /// Table holding the rejected item.
    public var table: String
    /// The column/index name.
    public var item: String
    /// Why the push would refuse it.
    public var reason: String

    public init(table: String, item: String, reason: String) {
        self.table = table
        self.item = item
        self.reason = reason
    }

    enum CodingKeys: String, CodingKey {
        case table, item, reason
    }
}

/// Result of `previewSchema` (`POST /admin/db/{db}/schema/preview`): what an
/// additive-only push would ADD and what it would REJECT (drops, type
/// changes). Pure/advisory — the preview applies nothing; `pushSchema`
/// remains the authoritative gate.
public struct SchemaPreviewDiff: Equatable, Codable, Sendable {
    /// Additive changes a push would make.
    public var added: [SchemaPreviewTableAdd]
    /// Drops/type changes a push would refuse.
    public var rejected: [SchemaPreviewRejection]

    public init(added: [SchemaPreviewTableAdd], rejected: [SchemaPreviewRejection]) {
        self.added = added
        self.rejected = rejected
    }

    enum CodingKeys: String, CodingKey {
        case added, rejected
    }
}

// MARK: - Migrate results

/// `POST /admin/db/{db}/migrate` response. `schema` is the post-migration
/// derived schema — returned even on a dry run (with `applied: false`), so a
/// caller can preview the resulting shape.
public struct MigrateResult: Equatable, Codable, Sendable {
    /// Whether the directives committed (false on a dry run).
    public var applied: Bool
    /// The post-migration derived schema.
    public var schema: SchemaDef
    /// Per-directive outcome reports.
    public var directives: [DirectiveReport]

    public init(applied: Bool, schema: SchemaDef, directives: [DirectiveReport]) {
        self.applied = applied
        self.schema = schema
        self.directives = directives
    }

    enum CodingKeys: String, CodingKey {
        case applied, schema, directives
    }
}

/// Per-directive outcome. `castFailures`/`sampleChanges` are omitted from
/// the wire when empty (the server's `skip_serializing_if`), so they surface
/// as empty arrays here when absent.
public struct DirectiveReport: Equatable, Codable, Sendable {
    /// Which directive ran.
    public var op: String
    /// Rows touched.
    public var affectedRows: Int64
    /// Rows that failed coercion (with their values).
    public var castFailures: [CastFailure]
    /// Before/after samples.
    public var sampleChanges: [SampleChange]

    public init(
        op: String,
        affectedRows: Int64,
        castFailures: [CastFailure] = [],
        sampleChanges: [SampleChange] = []
    ) {
        self.op = op
        self.affectedRows = affectedRows
        self.castFailures = castFailures
        self.sampleChanges = sampleChanges
    }

    enum CodingKeys: String, CodingKey {
        case op, affectedRows, castFailures, sampleChanges
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        op = try container.decode(String.self, forKey: .op)
        affectedRows = try container.decode(Int64.self, forKey: .affectedRows)
        castFailures = try container.decodeIfPresent([CastFailure].self, forKey: .castFailures) ?? []
        sampleChanges =
            try container.decodeIfPresent([SampleChange].self, forKey: .sampleChanges) ?? []
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(op, forKey: .op)
        try container.encode(affectedRows, forKey: .affectedRows)
        if !castFailures.isEmpty {
            try container.encode(castFailures, forKey: .castFailures)
        }
        if !sampleChanges.isEmpty {
            try container.encode(sampleChanges, forKey: .sampleChanges)
        }
    }
}

/// One row of `DirectiveReport.castFailures` — a row whose value failed to
/// coerce.
public struct CastFailure: Equatable, Codable, Sendable {
    /// The row's id.
    public var id: String
    /// The value that failed to coerce.
    public var value: JSONValue

    public init(id: String, value: JSONValue) {
        self.id = id
        self.value = value
    }

    enum CodingKeys: String, CodingKey {
        case id, value
    }
}

/// One row of `DirectiveReport.sampleChanges` — a row's before/after.
public struct SampleChange: Equatable, Codable, Sendable {
    /// The row's id.
    public var id: String
    /// The row before the directive.
    public var before: JSONValue
    /// The row after.
    public var after: JSONValue

    public init(id: String, before: JSONValue, after: JSONValue) {
        self.id = id
        self.before = before
        self.after = after
    }

    enum CodingKeys: String, CodingKey {
        case id, before, after
    }
}

// MARK: - Backups

/// One managed-backup file as returned by `GET /admin/backups`.
public struct BackupFile: Equatable, Codable, Sendable {
    /// Dump file name.
    public var name: String
    /// Dump size in bytes.
    public var sizeBytes: UInt64
    /// Dump time, epoch ms.
    public var createdMs: Int64

    public init(name: String, sizeBytes: UInt64, createdMs: Int64) {
        self.name = name
        self.sizeBytes = sizeBytes
        self.createdMs = createdMs
    }

    enum CodingKeys: String, CodingKey {
        case name, sizeBytes, createdMs
    }
}

/// `GET /admin/backups` response: the in-progress flag plus the on-disk
/// dump list, newest-first.
public struct BackupsListResponse: Equatable, Codable, Sendable {
    /// Whether a dump is in progress.
    public var running: Bool
    /// On-disk dumps, newest-first.
    public var backups: [BackupFile]

    public init(running: Bool, backups: [BackupFile]) {
        self.running = running
        self.backups = backups
    }

    enum CodingKeys: String, CodingKey {
        case running, backups
    }
}

/// `POST /admin/restore` response: the freshly-created target DB name and
/// cutover instructions.
public struct RestoreResult: Equatable, Codable, Sendable {
    /// The freshly-created restore DB name.
    public var target: String
    /// Operator cutover instructions.
    public var instructions: String

    public init(target: String, instructions: String) {
        self.target = target
        self.instructions = instructions
    }

    enum CodingKeys: String, CodingKey {
        case target, instructions
    }
}

// MARK: - Webhooks

/// One registered webhook returned by `GET /admin/db/{db}/webhooks`.
/// `table` nil means "all tables"; `events` carries op names
/// (`insert`/`patch`/`replace`/`delete`/`upsert`) or the single-element
/// `["*"]` to match every event. `enabled` and `secret` default so an older
/// server's response still parses.
public struct Webhook: Equatable, Codable, Sendable {
    /// Webhook id.
    public var id: Int64
    /// Owning database.
    public var db: String
    /// Scoped table, or nil for all tables.
    public var table: String?
    /// Delivery target.
    public var url: String
    /// Op names or `["*"]`.
    public var events: [String]
    /// Registration time, epoch ms.
    public var createdAt: Int64
    /// Added in ENH-003; defaults false for an older server.
    public var enabled: Bool
    /// Per-webhook HMAC signing key (SEC-115) — the receiver uses it to
    /// verify each delivery's `X-Rtdb-Signature` header. Server-generated;
    /// nil when an older server omits it.
    public var secret: String?

    public init(
        id: Int64,
        db: String,
        table: String? = nil,
        url: String,
        events: [String],
        createdAt: Int64,
        enabled: Bool = false,
        secret: String? = nil
    ) {
        self.id = id
        self.db = db
        self.table = table
        self.url = url
        self.events = events
        self.createdAt = createdAt
        self.enabled = enabled
        self.secret = secret
    }

    enum CodingKeys: String, CodingKey {
        case id, db, table, url, events, createdAt, enabled, secret
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(Int64.self, forKey: .id)
        db = try container.decode(String.self, forKey: .db)
        table = try container.decodeIfPresent(String.self, forKey: .table)
        url = try container.decode(String.self, forKey: .url)
        events = try container.decode([String].self, forKey: .events)
        createdAt = try container.decode(Int64.self, forKey: .createdAt)
        enabled = try container.decodeIfPresent(Bool.self, forKey: .enabled) ?? false
        secret = try container.decodeIfPresent(String.self, forKey: .secret)
    }
}

/// One delivery row from a webhook's outbox
/// (`GET /admin/db/{db}/webhooks/{id}/deliveries`). `payload` is the raw
/// JSON body queued at enqueue time, passed through verbatim so an operator
/// can inspect the exact event the worker will/did POST.
public struct WebhookDelivery: Equatable, Codable, Sendable {
    /// Delivery row id.
    public var id: Int64
    /// Delivery attempts so far.
    public var attempts: Int64
    /// `pending` / `retrying` / `delivered` / `failed`.
    public var status: String
    /// Scheduled retry time, epoch ms.
    public var nextAttempt: Int64
    /// The last failure, if any (plain null on the wire when none).
    public var lastError: String?
    /// The exact JSON body queued for POST.
    public var payload: JSONValue

    public init(
        id: Int64,
        attempts: Int64,
        status: String,
        nextAttempt: Int64,
        lastError: String? = nil,
        payload: JSONValue
    ) {
        self.id = id
        self.attempts = attempts
        self.status = status
        self.nextAttempt = nextAttempt
        self.lastError = lastError
        self.payload = payload
    }

    enum CodingKeys: String, CodingKey {
        case id, attempts, status, nextAttempt, lastError, payload
    }
}

/// Options for `RtDbAdminClient.createWebhook(_:options:)`. `url` is
/// required; the rest fall back to server defaults when nil (all-tables,
/// `["*"]` events, enabled). Only the set keys are sent.
public struct CreateWebhookOptions: Equatable, Encodable, Sendable {
    /// Delivery target (required).
    public var url: String
    /// Scope to one table (all tables when nil).
    public var table: String?
    /// Op names to match (`["*"]` when nil).
    public var events: [String]?
    /// Start enabled/disabled (enabled when nil).
    public var enabled: Bool?

    public init(url: String, table: String? = nil, events: [String]? = nil, enabled: Bool? = nil) {
        self.url = url
        self.table = table
        self.events = events
        self.enabled = enabled
    }

    enum CodingKeys: String, CodingKey {
        case url, table, events, enabled
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(url, forKey: .url)
        try container.encodeIfPresent(table, forKey: .table)
        try container.encodeIfPresent(events, forKey: .events)
        try container.encodeIfPresent(enabled, forKey: .enabled)
    }
}

/// Options for `RtDbAdminClient.editWebhook(_:id:options:)`. Every field is
/// optional — nil means "leave unchanged". `table` is a tri-state: nil
/// leaves the filter alone, `.some(nil)` (serialized as JSON `null`) clears
/// it to all-tables, and `.some(t)` sets it. `rotateSecret = true`
/// generates a fresh server-side signing secret (SEC-115); the secret value
/// itself is never accepted from the client.
public struct WebhookEditOptions: Equatable, Encodable, Sendable {
    /// New target URL; nil leaves it unchanged.
    public var url: String?
    /// Tri-state: skip / clear to all-tables / set.
    public var table: String??
    /// New event set; nil leaves it unchanged.
    public var events: [String]?
    /// Enable/disable; nil leaves it unchanged.
    public var enabled: Bool?
    /// True generates a fresh signing secret.
    public var rotateSecret: Bool?

    public init(
        url: String? = nil,
        table: String?? = nil,
        events: [String]? = nil,
        enabled: Bool? = nil,
        rotateSecret: Bool? = nil
    ) {
        self.url = url
        self.table = table
        self.events = events
        self.enabled = enabled
        self.rotateSecret = rotateSecret
    }

    enum CodingKeys: String, CodingKey {
        case url, table, events, enabled, rotateSecret
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encodeIfPresent(url, forKey: .url)
        // The tri-state: outer nil omits the key entirely; an inner nil
        // serializes as JSON null (the server's deserialize_some clear).
        if let table {
            try container.encode(table, forKey: .table)
        }
        try container.encodeIfPresent(events, forKey: .events)
        try container.encodeIfPresent(enabled, forKey: .enabled)
        try container.encodeIfPresent(rotateSecret, forKey: .rotateSecret)
    }
}

/// Optional filters for `RtDbAdminClient.listDeliveries(_:id:options:)`.
/// `status` filters by `pending|retrying|delivered|failed`; `limit`/`offset`
/// page (server defaults: limit 50 clamped to 1...1000, offset 0).
public struct ListDeliveriesOptions: Equatable, Sendable {
    /// Filter by delivery status.
    public var status: String?
    /// Page size (default 50, clamped to 1...1000).
    public var limit: Int64?
    /// Page offset.
    public var offset: Int64?

    public init(status: String? = nil, limit: Int64? = nil, offset: Int64? = nil) {
        self.status = status
        self.limit = limit
        self.offset = offset
    }
}

// MARK: - Audit

/// One durable-audit row as returned by `GET /admin/audit`. `op`/`principal`
/// are nil for system-initiated writes (TTL reaps, scheduled jobs) — the
/// server emits JSON null for those rows.
public struct AuditEntry: Equatable, Codable, Sendable {
    /// Audit row id.
    public var id: Int64
    /// Write time, epoch ms.
    public var tsMs: Int64
    /// Which database.
    public var db: String
    /// Which table.
    public var table: String
    /// The op; nil for system-initiated rows.
    public var op: String?
    /// Which document.
    public var docId: String
    /// The per-row owner when an interactive user wrote the doc; nil for
    /// machine tokens / system sources.
    public var principal: String?
    /// Tap arm (`mutate`/`ttl`/`merge`/…).
    public var source: String

    public init(
        id: Int64,
        tsMs: Int64,
        db: String,
        table: String,
        op: String? = nil,
        docId: String,
        principal: String? = nil,
        source: String
    ) {
        self.id = id
        self.tsMs = tsMs
        self.db = db
        self.table = table
        self.op = op
        self.docId = docId
        self.principal = principal
        self.source = source
    }

    enum CodingKeys: String, CodingKey {
        case id, tsMs, db, table, op, docId, principal, source
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(Int64.self, forKey: .id)
        tsMs = try container.decode(Int64.self, forKey: .tsMs)
        db = try container.decode(String.self, forKey: .db)
        table = try container.decode(String.self, forKey: .table)
        op = try container.decodeIfPresent(String.self, forKey: .op)
        docId = try container.decode(String.self, forKey: .docId)
        principal = try container.decodeIfPresent(String.self, forKey: .principal)
        source = try container.decode(String.self, forKey: .source)
    }
}

/// Optional filters for `RtDbAdminClient.getAudit(_:options:)`:
/// `table`/`op`/`principal`/`source` are equality filters combined with AND
/// (an absent field matches all rows); `limit`/`offset` page (server
/// defaults: limit 100 clamped to 1...1000, offset 0).
public struct AuditQuery: Equatable, Sendable {
    /// Equality filter on table.
    public var table: String?
    /// Equality filter on op.
    public var op: String?
    /// Equality filter on principal.
    public var principal: String?
    /// Equality filter on source.
    public var source: String?
    /// Page size (default 100, clamped to 1...1000).
    public var limit: Int64?
    /// Page offset.
    public var offset: Int64?

    public init(
        table: String? = nil,
        op: String? = nil,
        principal: String? = nil,
        source: String? = nil,
        limit: Int64? = nil,
        offset: Int64? = nil
    ) {
        self.table = table
        self.op = op
        self.principal = principal
        self.source = source
        self.limit = limit
        self.offset = offset
    }
}

// MARK: - Observability

/// `POST /admin/db/{db}/explain` → the compiled SQL + ordered bind params
/// for a Query DSL body (ENH-019). `sql` is byte-identical to what the read
/// path executes; `params` carries the same `$1..$n` binds formatted as
/// strings; `warnings` surfaces compile-time concerns (e.g. a filter on a
/// declared-but-unindexed field).
public struct ExplainResult: Equatable, Codable, Sendable {
    /// The compiled SQL (byte-identical to execution).
    public var sql: String
    /// The `$1..$n` binds, formatted as strings.
    public var params: [String]
    /// Which terminal compiled.
    public var terminal: String
    /// Compile-time concerns (e.g. unindexed filter field).
    public var warnings: [String]

    public init(sql: String, params: [String], terminal: String, warnings: [String]) {
        self.sql = sql
        self.params = params
        self.terminal = terminal
        self.warnings = warnings
    }

    enum CodingKeys: String, CodingKey {
        case sql, params, terminal, warnings
    }
}

/// One recorded slow-query event (ENH-019). `params` is included only when
/// the server has `RTDB_SLOW_QUERY_LOG_PARAMS=true` — otherwise it is
/// omitted on the wire and decodes to nil, keeping document content out of
/// the log by default.
public struct SlowQueryEntry: Equatable, Codable, Sendable {
    /// When the query started, as epoch milliseconds.
    public var startedAtMs: Int64
    /// Wall-clock duration in milliseconds.
    public var durationMs: UInt64
    /// Which database.
    public var db: String
    /// Which table.
    public var table: String
    /// Which terminal.
    public var terminal: String
    /// The executed SQL.
    public var sql: String
    /// Bound parameters; nil when the server redacts them (the default).
    public var params: [String]?

    public init(
        startedAtMs: Int64,
        durationMs: UInt64,
        db: String,
        table: String,
        terminal: String,
        sql: String,
        params: [String]? = nil
    ) {
        self.startedAtMs = startedAtMs
        self.durationMs = durationMs
        self.db = db
        self.table = table
        self.terminal = terminal
        self.sql = sql
        self.params = params
    }

    enum CodingKeys: String, CodingKey {
        case startedAtMs, durationMs, db, table, terminal, sql, params
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        startedAtMs = try container.decode(Int64.self, forKey: .startedAtMs)
        durationMs = try container.decode(UInt64.self, forKey: .durationMs)
        db = try container.decode(String.self, forKey: .db)
        table = try container.decode(String.self, forKey: .table)
        terminal = try container.decode(String.self, forKey: .terminal)
        sql = try container.decode(String.self, forKey: .sql)
        params = try container.decodeIfPresent([String].self, forKey: .params)
    }
}

/// `GET /admin/slow-queries?db=<optional>&limit=<n>` response (ENH-019):
/// the bounded in-memory ring newest-first. `thresholdMs` is the configured
/// `RTDB_SLOW_QUERY_MS` (0 = logging disabled → `queries` is empty);
/// `capacity` is the configured ring-buffer cap.
public struct SlowQueriesResponse: Equatable, Codable, Sendable {
    /// Recorded events, newest-first.
    public var queries: [SlowQueryEntry]
    /// Configured `RTDB_SLOW_QUERY_MS` (0 = disabled).
    public var thresholdMs: UInt64
    /// Ring-buffer cap.
    public var capacity: Int

    public init(queries: [SlowQueryEntry], thresholdMs: UInt64, capacity: Int) {
        self.queries = queries
        self.thresholdMs = thresholdMs
        self.capacity = capacity
    }

    enum CodingKeys: String, CodingKey {
        case queries, thresholdMs, capacity
    }
}

// MARK: - Workflows / files (admin views)

/// Optional filter for `RtDbAdminClient.listWorkflows(_:options:)`
/// (`GET /admin/db/{db}/workflows?status=&limit=`): `status` filters by run
/// state; `limit` pages the result (server default 100, capped at 500).
public struct WorkflowListOptions: Equatable, Sendable {
    /// Filter by run lifecycle state.
    public var status: WorkflowStatus?
    /// Page size (server default 100, capped 500).
    public var limit: Int?

    public init(status: WorkflowStatus? = nil, limit: Int? = nil) {
        self.status = status
        self.limit = limit
    }
}

/// One full workflow-run row from `GET /admin/db/{db}/workflows/{id}` — the
/// `WorkflowInfo` projection flattened alongside the per-step outcome trail
/// (rust `WorkflowInfoFull`, serde flatten + unknown fields rejected).
public struct WorkflowInfoFull: Equatable, Codable, Sendable {
    /// The run's info projection (flattened onto the top level on the wire).
    public var info: WorkflowInfo
    /// Terminal record per completed step.
    public var stepOutcomes: [StepOutcome]

    public init(info: WorkflowInfo, stepOutcomes: [StepOutcome]) {
        self.info = info
        self.stepOutcomes = stepOutcomes
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case id, name, status, currentStep, stepCount, attempts
        case sleepUntil, lastError, createdAt, updatedAt, startedAt, finishedAt
        case stepOutcomes
    }

    public init(from decoder: Decoder) throws {
        try decoder.rejectUnknownKeys("WorkflowInfoFull", as: CodingKeys.self)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        info = try WorkflowInfo(
            id: container.decode(String.self, forKey: .id),
            name: container.decode(String.self, forKey: .name),
            status: container.decode(WorkflowStatus.self, forKey: .status),
            currentStep: container.decode(UInt32.self, forKey: .currentStep),
            stepCount: container.decode(UInt32.self, forKey: .stepCount),
            attempts: container.decode(UInt32.self, forKey: .attempts),
            sleepUntil: container.decodeIfPresent(Int64.self, forKey: .sleepUntil),
            lastError: container.decodeIfPresent(String.self, forKey: .lastError),
            createdAt: container.decode(Int64.self, forKey: .createdAt),
            updatedAt: container.decode(Int64.self, forKey: .updatedAt),
            startedAt: container.decodeIfPresent(Int64.self, forKey: .startedAt),
            finishedAt: container.decodeIfPresent(Int64.self, forKey: .finishedAt)
        )
        stepOutcomes = try container.decode([StepOutcome].self, forKey: .stepOutcomes)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(info.id, forKey: .id)
        try container.encode(info.name, forKey: .name)
        try container.encode(info.status, forKey: .status)
        try container.encode(info.currentStep, forKey: .currentStep)
        try container.encode(info.stepCount, forKey: .stepCount)
        try container.encode(info.attempts, forKey: .attempts)
        try container.encodeIfPresent(info.sleepUntil, forKey: .sleepUntil)
        try container.encodeIfPresent(info.lastError, forKey: .lastError)
        try container.encode(info.createdAt, forKey: .createdAt)
        try container.encode(info.updatedAt, forKey: .updatedAt)
        try container.encodeIfPresent(info.startedAt, forKey: .startedAt)
        try container.encodeIfPresent(info.finishedAt, forKey: .finishedAt)
        try container.encode(stepOutcomes, forKey: .stepOutcomes)
    }
}

/// Stored metadata for one storage blob (the admin list-files row and the
/// `/metadata` body). `contentType` is nil when the server stored the blob
/// untyped.
public struct FileMetadata: Equatable, Codable, Sendable {
    /// Server-assigned opaque file id.
    public var id: String
    /// SHA-256 hex digest of the stored bytes.
    public var sha256: String
    /// Size in bytes.
    public var size: Int64
    /// The stored `Content-Type`, when the server recorded one.
    public var contentType: String?
    /// Upload timestamp, epoch milliseconds.
    public var creationTime: Int64

    public init(id: String, sha256: String, size: Int64, contentType: String? = nil, creationTime: Int64) {
        self.id = id
        self.sha256 = sha256
        self.size = size
        self.contentType = contentType
        self.creationTime = creationTime
    }

    enum CodingKeys: String, CodingKey {
        case id, sha256, size, contentType, creationTime
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(String.self, forKey: .id)
        sha256 = try container.decode(String.self, forKey: .sha256)
        size = try container.decode(Int64.self, forKey: .size)
        contentType = try container.decodeIfPresent(String.self, forKey: .contentType)
        creationTime = try container.decode(Int64.self, forKey: .creationTime)
    }
}
