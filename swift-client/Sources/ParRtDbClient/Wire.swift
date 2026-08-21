import Foundation

// MARK: - Internally tagged enum decode helpers (serde parity)

/// serde's internally-tagged-enum prelude: reads the discriminant and the full
/// raw payload key set in one permissive pass. Strictly-keyed containers cannot
/// see unknown keys, so this raw pass is what powers per-variant
/// `deny_unknown_fields` (see `rejectUnknownVariantFields`).
func taggedEnumPayload(
    _ typeName: String, tagKey: String, from decoder: Decoder
) throws -> (tag: String, keys: Set<String>) {
    let raw = try decoder.container(keyedBy: AnyStringCodingKey.self)
    let key = AnyStringCodingKey(stringValue: tagKey)
    do {
        let tag = try raw.decode(String.self, forKey: key)
        return (tag, Set(raw.allKeys.map(\.stringValue)))
    } catch {
        throw DecodingError.keyNotFound(
            key,
            DecodingError.Context(
                codingPath: decoder.codingPath,
                debugDescription: "\(typeName): missing field '\(tagKey)'"
            )
        )
    }
}

/// Per-variant `deny_unknown_fields`: throws when `keys` carries a field the
/// MATCHED variant does not declare. serde (and the pydantic clients'
/// `extra='forbid'`) reject per variant, not per union — a key declared on a
/// different variant (`queryId` on an auth frame) is still unknown. Pass the
/// tag key inside `allowed`.
func rejectUnknownVariantFields(
    _ typeName: String, variant: String, keys: Set<String>, allowed: Set<String>
) throws {
    if let first = keys.subtracting(allowed).sorted().first {
        throw DecodingError.dataCorrupted(
            DecodingError.Context(
                codingPath: [],
                debugDescription: "\(typeName).\(variant): unknown field '\(first)'"
            )
        )
    }
}

// MARK: - AuthedUser

/// Mirrors server/src/protocol.rs::UserKind — `"user"` | `"machine"` on the
/// wire (snake_case). Closed domain: an unknown string is a decode error.
public enum UserKind: String, Codable, Sendable {
    case user
    case machine
}

/// Mirrors server/src/protocol.rs::AuthedUser — camelCase keys. `email`/`name`
/// are plain Options (JSON `null` on the wire when absent, never omitted);
/// `githubLogin`/`githubId` are omitted entirely when absent. No unknown-field
/// rejection: the server and rust-client AuthedUser carry no
/// `deny_unknown_fields`.
public struct AuthedUser: Equatable, Codable, Sendable {
    public var kind: UserKind
    public var email: String?
    public var name: String?
    public var githubLogin: String?
    public var githubId: Int64?

    public init(
        kind: UserKind, email: String? = nil, name: String? = nil,
        githubLogin: String? = nil, githubId: Int64? = nil
    ) {
        self.kind = kind
        self.email = email
        self.name = name
        self.githubLogin = githubLogin
        self.githubId = githubId
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case kind, email, name, githubLogin, githubId
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        kind = try container.decode(UserKind.self, forKey: .kind)
        email = try container.decodeIfPresent(String.self, forKey: .email)
        name = try container.decodeIfPresent(String.self, forKey: .name)
        githubLogin = try container.decodeIfPresent(String.self, forKey: .githubLogin)
        githubId = try container.decodeIfPresent(Int64.self, forKey: .githubId)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(kind, forKey: .kind)
        try container.encode(email, forKey: .email) // plain Option: nil -> null
        try container.encode(name, forKey: .name) // plain Option: nil -> null
        try container.encodeIfPresent(githubLogin, forKey: .githubLogin)
        try container.encodeIfPresent(githubId, forKey: .githubId)
    }
}

// MARK: - Presence

/// Mirrors server/src/protocol.rs::PresenceMember — camelCase; `connectionId`
/// is the opaque per-session key, `state` an opaque client-supplied blob.
public struct PresenceMember: Equatable, Codable, Sendable {
    public var connectionId: String
    public var user: AuthedUser
    public var state: JSONValue

    public init(connectionId: String, user: AuthedUser, state: JSONValue) {
        self.connectionId = connectionId
        self.user = user
        self.state = state
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case connectionId, user, state
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        connectionId = try container.decode(String.self, forKey: .connectionId)
        user = try container.decode(AuthedUser.self, forKey: .user)
        state = try container.decode(JSONValue.self, forKey: .state)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(connectionId, forKey: .connectionId)
        try container.encode(user, forKey: .user)
        try container.encode(state, forKey: .state)
    }
}

// MARK: - ClientMessage

/// Mirrors server/src/protocol.rs::ClientMessage — internally tagged on
/// `"type"`, camelCase tags and fields, unknown fields rejected per variant
/// (the server closes the WS on them).
public enum ClientMessage: Equatable, Codable, Sendable {
    /// Authenticate the socket (first frame). `token` is optional — a browser
    /// dashboard authenticates from the HttpOnly session cookie, sending only
    /// `db` (SEC-001 phase 2).
    case auth(token: String?, db: String)
    /// Start a live query subscription.
    case subscribe(queryId: String, query: Query)
    /// Stop a subscription.
    case unsubscribe(queryId: String)
    /// Run a transaction; `idempotencyKey` replays a cached result when set.
    case mutate(mutId: String, idempotencyKey: String?, txn: Transaction)
    /// Schedule a transaction for later.
    case schedule(scheduleId: String, when: ScheduleWhen, txn: Transaction)
    /// Cancel a scheduled job.
    case cancelSchedule(scheduleId: String, id: String)
    /// Pause a cron job.
    case pauseSchedule(scheduleId: String, id: String)
    /// Resume a paused cron job.
    case resumeSchedule(scheduleId: String, id: String)
    /// List scheduled jobs.
    case listSchedules(scheduleId: String)
    /// Start a durable workflow run.
    case startWorkflow(workflowId: String, spec: WorkflowSpec)
    /// Cancel a workflow run.
    case cancelWorkflow(workflowId: String, id: String)
    /// List workflow runs; `status` filters by lifecycle.
    case listWorkflows(workflowId: String, status: WorkflowStatus?)
    /// Join a presence room; `state` is the opaque presence blob.
    case presence(room: String, state: JSONValue?)
    /// Update presence state without (re)joining; `ttlMs` per-state expiry.
    case presenceState(room: String, state: JSONValue, ttlMs: UInt64?)
    /// Leave a presence room.
    case leavePresence(room: String)
    /// Keepalive; the server replies `pong`.
    case ping

    enum CodingKeys: String, CodingKey, CaseIterable {
        case type, token, db, queryId, query, mutId, idempotencyKey, txn
        case scheduleId, when, id, workflowId, spec, status
        case room, state, ttlMs
    }

    // swiftlint:disable:next cyclomatic_complexity function_body_length
    public init(from decoder: Decoder) throws {
        let payload = try taggedEnumPayload("ClientMessage", tagKey: "type", from: decoder)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        switch payload.tag {
        case "auth":
            try rejectUnknownVariantFields(
                "ClientMessage", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "token", "db"]
            )
            self = try .auth(
                token: container.decodeIfPresent(String.self, forKey: .token),
                db: container.decode(String.self, forKey: .db)
            )
        case "subscribe":
            try rejectUnknownVariantFields(
                "ClientMessage", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "queryId", "query"]
            )
            self = try .subscribe(
                queryId: container.decode(String.self, forKey: .queryId),
                query: container.decode(Query.self, forKey: .query)
            )
        case "unsubscribe":
            try rejectUnknownVariantFields(
                "ClientMessage", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "queryId"]
            )
            self = try .unsubscribe(queryId: container.decode(String.self, forKey: .queryId))
        case "mutate":
            try rejectUnknownVariantFields(
                "ClientMessage", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "mutId", "idempotencyKey", "txn"]
            )
            self = try .mutate(
                mutId: container.decode(String.self, forKey: .mutId),
                idempotencyKey: container.decodeIfPresent(String.self, forKey: .idempotencyKey),
                txn: container.decode(Transaction.self, forKey: .txn)
            )
        case "schedule":
            try rejectUnknownVariantFields(
                "ClientMessage", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "scheduleId", "when", "txn"]
            )
            self = try .schedule(
                scheduleId: container.decode(String.self, forKey: .scheduleId),
                when: container.decode(ScheduleWhen.self, forKey: .when),
                txn: container.decode(Transaction.self, forKey: .txn)
            )
        case "cancelSchedule":
            try rejectUnknownVariantFields(
                "ClientMessage", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "scheduleId", "id"]
            )
            self = try .cancelSchedule(
                scheduleId: container.decode(String.self, forKey: .scheduleId),
                id: container.decode(String.self, forKey: .id)
            )
        case "pauseSchedule":
            try rejectUnknownVariantFields(
                "ClientMessage", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "scheduleId", "id"]
            )
            self = try .pauseSchedule(
                scheduleId: container.decode(String.self, forKey: .scheduleId),
                id: container.decode(String.self, forKey: .id)
            )
        case "resumeSchedule":
            try rejectUnknownVariantFields(
                "ClientMessage", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "scheduleId", "id"]
            )
            self = try .resumeSchedule(
                scheduleId: container.decode(String.self, forKey: .scheduleId),
                id: container.decode(String.self, forKey: .id)
            )
        case "listSchedules":
            try rejectUnknownVariantFields(
                "ClientMessage", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "scheduleId"]
            )
            self = try .listSchedules(scheduleId: container.decode(String.self, forKey: .scheduleId))
        case "startWorkflow":
            try rejectUnknownVariantFields(
                "ClientMessage", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "workflowId", "spec"]
            )
            self = try .startWorkflow(
                workflowId: container.decode(String.self, forKey: .workflowId),
                spec: container.decode(WorkflowSpec.self, forKey: .spec)
            )
        case "cancelWorkflow":
            try rejectUnknownVariantFields(
                "ClientMessage", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "workflowId", "id"]
            )
            self = try .cancelWorkflow(
                workflowId: container.decode(String.self, forKey: .workflowId),
                id: container.decode(String.self, forKey: .id)
            )
        case "listWorkflows":
            try rejectUnknownVariantFields(
                "ClientMessage", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "workflowId", "status"]
            )
            self = try .listWorkflows(
                workflowId: container.decode(String.self, forKey: .workflowId),
                status: container.decodeIfPresent(WorkflowStatus.self, forKey: .status)
            )
        case "presence":
            try rejectUnknownVariantFields(
                "ClientMessage", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "room", "state"]
            )
            self = try .presence(
                room: container.decode(String.self, forKey: .room),
                state: container.decodeIfPresent(JSONValue.self, forKey: .state)
            )
        case "presenceState":
            try rejectUnknownVariantFields(
                "ClientMessage", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "room", "state", "ttlMs"]
            )
            self = try .presenceState(
                room: container.decode(String.self, forKey: .room),
                state: container.decode(JSONValue.self, forKey: .state),
                ttlMs: container.decodeIfPresent(UInt64.self, forKey: .ttlMs)
            )
        case "leavePresence":
            try rejectUnknownVariantFields(
                "ClientMessage", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "room"]
            )
            self = try .leavePresence(room: container.decode(String.self, forKey: .room))
        case "ping":
            try rejectUnknownVariantFields(
                "ClientMessage", variant: payload.tag, keys: payload.keys,
                allowed: ["type"]
            )
            self = .ping
        case let unknown:
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: decoder.codingPath,
                    debugDescription: "ClientMessage: unknown type '\(unknown)'"
                )
            )
        }
    }

    // swiftlint:disable:next cyclomatic_complexity function_body_length
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case let .auth(token, db):
            try container.encode("auth", forKey: .type)
            try container.encodeIfPresent(token, forKey: .token)
            try container.encode(db, forKey: .db)
        case let .subscribe(queryId, query):
            try container.encode("subscribe", forKey: .type)
            try container.encode(queryId, forKey: .queryId)
            try container.encode(query, forKey: .query)
        case let .unsubscribe(queryId):
            try container.encode("unsubscribe", forKey: .type)
            try container.encode(queryId, forKey: .queryId)
        case let .mutate(mutId, idempotencyKey, txn):
            try container.encode("mutate", forKey: .type)
            try container.encode(mutId, forKey: .mutId)
            try container.encodeIfPresent(idempotencyKey, forKey: .idempotencyKey)
            try container.encode(txn, forKey: .txn)
        case let .schedule(scheduleId, when, txn):
            try container.encode("schedule", forKey: .type)
            try container.encode(scheduleId, forKey: .scheduleId)
            try container.encode(when, forKey: .when)
            try container.encode(txn, forKey: .txn)
        case let .cancelSchedule(scheduleId, id):
            try container.encode("cancelSchedule", forKey: .type)
            try container.encode(scheduleId, forKey: .scheduleId)
            try container.encode(id, forKey: .id)
        case let .pauseSchedule(scheduleId, id):
            try container.encode("pauseSchedule", forKey: .type)
            try container.encode(scheduleId, forKey: .scheduleId)
            try container.encode(id, forKey: .id)
        case let .resumeSchedule(scheduleId, id):
            try container.encode("resumeSchedule", forKey: .type)
            try container.encode(scheduleId, forKey: .scheduleId)
            try container.encode(id, forKey: .id)
        case let .listSchedules(scheduleId):
            try container.encode("listSchedules", forKey: .type)
            try container.encode(scheduleId, forKey: .scheduleId)
        case let .startWorkflow(workflowId, spec):
            try container.encode("startWorkflow", forKey: .type)
            try container.encode(workflowId, forKey: .workflowId)
            try container.encode(spec, forKey: .spec)
        case let .cancelWorkflow(workflowId, id):
            try container.encode("cancelWorkflow", forKey: .type)
            try container.encode(workflowId, forKey: .workflowId)
            try container.encode(id, forKey: .id)
        case let .listWorkflows(workflowId, status):
            try container.encode("listWorkflows", forKey: .type)
            try container.encode(workflowId, forKey: .workflowId)
            try container.encodeIfPresent(status, forKey: .status)
        case let .presence(room, state):
            try container.encode("presence", forKey: .type)
            try container.encode(room, forKey: .room)
            try container.encodeIfPresent(state, forKey: .state)
        case let .presenceState(room, state, ttlMs):
            try container.encode("presenceState", forKey: .type)
            try container.encode(room, forKey: .room)
            try container.encode(state, forKey: .state)
            try container.encodeIfPresent(ttlMs, forKey: .ttlMs)
        case let .leavePresence(room):
            try container.encode("leavePresence", forKey: .type)
            try container.encode(room, forKey: .room)
        case .ping:
            try container.encode("ping", forKey: .type)
        }
    }
}

// MARK: - ServerMessage

/// Mirrors server/src/protocol.rs::ServerMessage — internally tagged on
/// `"type"`, camelCase tags and fields. Unlike `ClientMessage`, unknown fields
/// are TOLERATED: the rust-client's ServerMessage deliberately omits
/// `deny_unknown_fields` so a newer server can add fields without breaking
/// older clients (client-side forward compatibility).
public enum ServerMessage: Equatable, Codable, Sendable {
    /// Authentication succeeded.
    case authOk(user: AuthedUser)
    /// Authentication failed; the socket closes.
    case authErr(error: RtDbError)
    /// A live query's new full result (sent only on change).
    case queryUpdate(queryId: String, result: JSONValue)
    /// Transaction applied; one entry per step.
    case mutateOk(mutId: String, results: [JSONValue])
    /// Transaction failed and rolled back.
    case mutateErr(mutId: String, error: RtDbError)
    /// Subscription rejected (bad query, authz).
    case subscribeErr(queryId: String, error: RtDbError)
    /// Job scheduled.
    case scheduleOk(scheduleId: String, id: String)
    /// Scheduling failed.
    case scheduleErr(scheduleId: String, error: RtDbError)
    /// Reply to cancel/pause/resume; `error` omitted when `ok`.
    case scheduleAck(scheduleId: String, ok: Bool, error: RtDbError?)
    /// Reply to `listSchedules`.
    case listSchedulesOk(scheduleId: String, schedules: [ScheduleInfo])
    /// Run started.
    case startWorkflowOk(workflowId: String, info: WorkflowInfo)
    /// Run rejected (spec validation, authz).
    case startWorkflowErr(workflowId: String, error: RtDbError)
    /// Reply to cancelWorkflow; `error` omitted when `ok`.
    case workflowAck(workflowId: String, ok: Bool, error: RtDbError?)
    /// Reply to `listWorkflows`.
    case listWorkflowsOk(workflowId: String, workflows: [WorkflowInfo])
    /// Full room membership (on join and on every change).
    case presenceSnapshot(room: String, members: [PresenceMember])
    /// Presence operation failed.
    case presenceErr(room: String, error: RtDbError)
    /// Reply to `ping`.
    case pong

    enum CodingKeys: String, CodingKey, CaseIterable {
        case type, user, error, queryId, result, mutId, results, schedules
        case scheduleId, id, ok, workflowId, info, workflows, room, members
    }

    // swiftlint:disable:next cyclomatic_complexity function_body_length
    public init(from decoder: Decoder) throws {
        let payload = try taggedEnumPayload("ServerMessage", tagKey: "type", from: decoder)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        switch payload.tag {
        case "authOk":
            self = try .authOk(user: container.decode(AuthedUser.self, forKey: .user))
        case "authErr":
            self = try .authErr(error: container.decode(RtDbError.self, forKey: .error))
        case "queryUpdate":
            self = try .queryUpdate(
                queryId: container.decode(String.self, forKey: .queryId),
                result: container.decode(JSONValue.self, forKey: .result)
            )
        case "mutateOk":
            self = try .mutateOk(
                mutId: container.decode(String.self, forKey: .mutId),
                results: container.decode([JSONValue].self, forKey: .results)
            )
        case "mutateErr":
            self = try .mutateErr(
                mutId: container.decode(String.self, forKey: .mutId),
                error: container.decode(RtDbError.self, forKey: .error)
            )
        case "subscribeErr":
            self = try .subscribeErr(
                queryId: container.decode(String.self, forKey: .queryId),
                error: container.decode(RtDbError.self, forKey: .error)
            )
        case "scheduleOk":
            self = try .scheduleOk(
                scheduleId: container.decode(String.self, forKey: .scheduleId),
                id: container.decode(String.self, forKey: .id)
            )
        case "scheduleErr":
            self = try .scheduleErr(
                scheduleId: container.decode(String.self, forKey: .scheduleId),
                error: container.decode(RtDbError.self, forKey: .error)
            )
        case "scheduleAck":
            self = try .scheduleAck(
                scheduleId: container.decode(String.self, forKey: .scheduleId),
                ok: container.decode(Bool.self, forKey: .ok),
                error: container.decodeIfPresent(RtDbError.self, forKey: .error)
            )
        case "listSchedulesOk":
            self = try .listSchedulesOk(
                scheduleId: container.decode(String.self, forKey: .scheduleId),
                schedules: container.decode([ScheduleInfo].self, forKey: .schedules)
            )
        case "startWorkflowOk":
            self = try .startWorkflowOk(
                workflowId: container.decode(String.self, forKey: .workflowId),
                info: container.decode(WorkflowInfo.self, forKey: .info)
            )
        case "startWorkflowErr":
            self = try .startWorkflowErr(
                workflowId: container.decode(String.self, forKey: .workflowId),
                error: container.decode(RtDbError.self, forKey: .error)
            )
        case "workflowAck":
            self = try .workflowAck(
                workflowId: container.decode(String.self, forKey: .workflowId),
                ok: container.decode(Bool.self, forKey: .ok),
                error: container.decodeIfPresent(RtDbError.self, forKey: .error)
            )
        case "listWorkflowsOk":
            self = try .listWorkflowsOk(
                workflowId: container.decode(String.self, forKey: .workflowId),
                workflows: container.decode([WorkflowInfo].self, forKey: .workflows)
            )
        case "presenceSnapshot":
            self = try .presenceSnapshot(
                room: container.decode(String.self, forKey: .room),
                members: container.decode([PresenceMember].self, forKey: .members)
            )
        case "presenceErr":
            self = try .presenceErr(
                room: container.decode(String.self, forKey: .room),
                error: container.decode(RtDbError.self, forKey: .error)
            )
        case "pong":
            self = .pong
        case let unknown:
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: decoder.codingPath,
                    debugDescription: "ServerMessage: unknown type '\(unknown)'"
                )
            )
        }
    }

    // swiftlint:disable:next cyclomatic_complexity function_body_length
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case let .authOk(user):
            try container.encode("authOk", forKey: .type)
            try container.encode(user, forKey: .user)
        case let .authErr(error):
            try container.encode("authErr", forKey: .type)
            try container.encode(error, forKey: .error)
        case let .queryUpdate(queryId, result):
            try container.encode("queryUpdate", forKey: .type)
            try container.encode(queryId, forKey: .queryId)
            try container.encode(result, forKey: .result)
        case let .mutateOk(mutId, results):
            try container.encode("mutateOk", forKey: .type)
            try container.encode(mutId, forKey: .mutId)
            try container.encode(results, forKey: .results)
        case let .mutateErr(mutId, error):
            try container.encode("mutateErr", forKey: .type)
            try container.encode(mutId, forKey: .mutId)
            try container.encode(error, forKey: .error)
        case let .subscribeErr(queryId, error):
            try container.encode("subscribeErr", forKey: .type)
            try container.encode(queryId, forKey: .queryId)
            try container.encode(error, forKey: .error)
        case let .scheduleOk(scheduleId, id):
            try container.encode("scheduleOk", forKey: .type)
            try container.encode(scheduleId, forKey: .scheduleId)
            try container.encode(id, forKey: .id)
        case let .scheduleErr(scheduleId, error):
            try container.encode("scheduleErr", forKey: .type)
            try container.encode(scheduleId, forKey: .scheduleId)
            try container.encode(error, forKey: .error)
        case let .scheduleAck(scheduleId, ok, error):
            try container.encode("scheduleAck", forKey: .type)
            try container.encode(scheduleId, forKey: .scheduleId)
            try container.encode(ok, forKey: .ok)
            try container.encodeIfPresent(error, forKey: .error)
        case let .listSchedulesOk(scheduleId, schedules):
            try container.encode("listSchedulesOk", forKey: .type)
            try container.encode(scheduleId, forKey: .scheduleId)
            try container.encode(schedules, forKey: .schedules)
        case let .startWorkflowOk(workflowId, info):
            try container.encode("startWorkflowOk", forKey: .type)
            try container.encode(workflowId, forKey: .workflowId)
            try container.encode(info, forKey: .info)
        case let .startWorkflowErr(workflowId, error):
            try container.encode("startWorkflowErr", forKey: .type)
            try container.encode(workflowId, forKey: .workflowId)
            try container.encode(error, forKey: .error)
        case let .workflowAck(workflowId, ok, error):
            try container.encode("workflowAck", forKey: .type)
            try container.encode(workflowId, forKey: .workflowId)
            try container.encode(ok, forKey: .ok)
            try container.encodeIfPresent(error, forKey: .error)
        case let .listWorkflowsOk(workflowId, workflows):
            try container.encode("listWorkflowsOk", forKey: .type)
            try container.encode(workflowId, forKey: .workflowId)
            try container.encode(workflows, forKey: .workflows)
        case let .presenceSnapshot(room, members):
            try container.encode("presenceSnapshot", forKey: .type)
            try container.encode(room, forKey: .room)
            try container.encode(members, forKey: .members)
        case let .presenceErr(room, error):
            try container.encode("presenceErr", forKey: .type)
            try container.encode(room, forKey: .room)
            try container.encode(error, forKey: .error)
        case .pong:
            try container.encode("pong", forKey: .type)
        }
    }
}

// MARK: - Scheduling

/// Mirrors server/src/protocol.rs::ScheduleWhen — internally tagged `"type"`,
/// camelCase tags, unknown fields rejected.
public enum ScheduleWhen: Equatable, Codable, Sendable {
    /// Fire `ms` milliseconds from now.
    case afterMs(ms: Int64)
    /// Fire at this UTC epoch-ms instant (in the past = fire immediately).
    case runAt(ms: Int64)
    /// Fire on this 5-field cron schedule (UTC, min-first).
    case cron(expr: String)
    /// Fire every `everyMs` milliseconds, starting one interval from now.
    /// Missed windows (downtime, pause) are skipped, never backfilled —
    /// each fire re-arms from its actual fire time, like cron recompute.
    case interval(everyMs: Int64)

    enum CodingKeys: String, CodingKey, CaseIterable {
        case type, ms, expr, everyMs
    }

    public init(from decoder: Decoder) throws {
        let payload = try taggedEnumPayload("ScheduleWhen", tagKey: "type", from: decoder)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        switch payload.tag {
        case "afterMs":
            try rejectUnknownVariantFields(
                "ScheduleWhen", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "ms"]
            )
            self = try .afterMs(ms: container.decode(Int64.self, forKey: .ms))
        case "runAt":
            try rejectUnknownVariantFields(
                "ScheduleWhen", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "ms"]
            )
            self = try .runAt(ms: container.decode(Int64.self, forKey: .ms))
        case "cron":
            try rejectUnknownVariantFields(
                "ScheduleWhen", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "expr"]
            )
            self = try .cron(expr: container.decode(String.self, forKey: .expr))
        case "interval":
            try rejectUnknownVariantFields(
                "ScheduleWhen", variant: payload.tag, keys: payload.keys,
                allowed: ["type", "everyMs"]
            )
            self = try .interval(everyMs: container.decode(Int64.self, forKey: .everyMs))
        case let unknown:
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: decoder.codingPath,
                    debugDescription: "ScheduleWhen: unknown type '\(unknown)'"
                )
            )
        }
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case let .afterMs(ms):
            try container.encode("afterMs", forKey: .type)
            try container.encode(ms, forKey: .ms)
        case let .runAt(ms):
            try container.encode("runAt", forKey: .type)
            try container.encode(ms, forKey: .ms)
        case let .cron(expr):
            try container.encode("cron", forKey: .type)
            try container.encode(expr, forKey: .expr)
        case let .interval(everyMs):
            try container.encode("interval", forKey: .type)
            try container.encode(everyMs, forKey: .everyMs)
        }
    }
}

/// Mirrors server/src/protocol.rs::ScheduleKind — `"oneshot"` | `"cron"`
/// | `"interval"`.
public enum ScheduleKind: String, Codable, Sendable {
    case oneshot
    case cron
    case interval
}

/// Mirrors server/src/protocol.rs::ScheduleStatus — `"pending"` | `"running"`
/// | `"paused"` | `"error"`.
public enum ScheduleStatus: String, Codable, Sendable {
    case pending
    case running
    case paused
    case error
}

/// Mirrors server/src/protocol.rs::ScheduleInfo — camelCase; `cron`,
/// `everyMs`, and `lastError` are omitted on the wire when absent. No
/// unknown-field rejection (the server type carries no
/// `deny_unknown_fields`).
public struct ScheduleInfo: Equatable, Codable, Sendable {
    public var id: String
    public var kind: ScheduleKind
    public var dueAt: Int64
    public var cron: String?
    /// Interval jobs only: the fixed recurrence in ms (`kind: "interval"`).
    public var everyMs: Int64?
    public var status: ScheduleStatus
    public var lastError: String?
    public var createdAt: Int64
    public var firedCount: Int64

    public init(
        id: String, kind: ScheduleKind, dueAt: Int64, cron: String? = nil, everyMs: Int64? = nil,
        status: ScheduleStatus, lastError: String? = nil, createdAt: Int64, firedCount: Int64
    ) {
        self.id = id
        self.kind = kind
        self.dueAt = dueAt
        self.cron = cron
        self.everyMs = everyMs
        self.status = status
        self.lastError = lastError
        self.createdAt = createdAt
        self.firedCount = firedCount
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case id, kind, dueAt, cron, everyMs, status, lastError, createdAt, firedCount
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(String.self, forKey: .id)
        kind = try container.decode(ScheduleKind.self, forKey: .kind)
        dueAt = try container.decode(Int64.self, forKey: .dueAt)
        cron = try container.decodeIfPresent(String.self, forKey: .cron)
        everyMs = try container.decodeIfPresent(Int64.self, forKey: .everyMs)
        status = try container.decode(ScheduleStatus.self, forKey: .status)
        lastError = try container.decodeIfPresent(String.self, forKey: .lastError)
        createdAt = try container.decode(Int64.self, forKey: .createdAt)
        firedCount = try container.decode(Int64.self, forKey: .firedCount)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(id, forKey: .id)
        try container.encode(kind, forKey: .kind)
        try container.encode(dueAt, forKey: .dueAt)
        try container.encodeIfPresent(cron, forKey: .cron)
        try container.encodeIfPresent(everyMs, forKey: .everyMs)
        try container.encode(status, forKey: .status)
        try container.encodeIfPresent(lastError, forKey: .lastError)
        try container.encode(createdAt, forKey: .createdAt)
        try container.encode(firedCount, forKey: .firedCount)
    }
}

// MARK: - Workflows

/// Mirrors server/src/protocol.rs::StepRetry — camelCase, unknown fields
/// rejected. `maxAttempts` counts TOTAL attempts; the other fields default
/// (1s initial backoff doubling to a 60s cap) and are ALWAYS serialized.
public struct StepRetry: Equatable, Codable, Sendable {
    public var maxAttempts: UInt32
    public var initialRetryMs: UInt64
    public var maxRetryMs: UInt64

    public init(maxAttempts: UInt32, initialRetryMs: UInt64 = 1000, maxRetryMs: UInt64 = 60000) {
        self.maxAttempts = maxAttempts
        self.initialRetryMs = initialRetryMs
        self.maxRetryMs = maxRetryMs
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case maxAttempts, initialRetryMs, maxRetryMs
    }

    public init(from decoder: Decoder) throws {
        try decoder.rejectUnknownKeys("StepRetry", as: CodingKeys.self)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        maxAttempts = try container.decode(UInt32.self, forKey: .maxAttempts)
        initialRetryMs = try container.decodeIfPresent(UInt64.self, forKey: .initialRetryMs) ?? 1000
        maxRetryMs = try container.decodeIfPresent(UInt64.self, forKey: .maxRetryMs) ?? 60000
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(maxAttempts, forKey: .maxAttempts)
        try container.encode(initialRetryMs, forKey: .initialRetryMs)
        try container.encode(maxRetryMs, forKey: .maxRetryMs)
    }
}

/// Mirrors server/src/protocol.rs::WorkflowStepSpec — camelCase, unknown
/// fields rejected; `retry`/`sleepBeforeMs` omitted when nil.
public struct WorkflowStepSpec: Equatable, Codable, Sendable {
    public var txn: Transaction
    public var retry: StepRetry?
    public var sleepBeforeMs: UInt64?

    public init(txn: Transaction, retry: StepRetry? = nil, sleepBeforeMs: UInt64? = nil) {
        self.txn = txn
        self.retry = retry
        self.sleepBeforeMs = sleepBeforeMs
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case txn, retry, sleepBeforeMs
    }

    public init(from decoder: Decoder) throws {
        try decoder.rejectUnknownKeys("WorkflowStepSpec", as: CodingKeys.self)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        txn = try container.decode(Transaction.self, forKey: .txn)
        retry = try container.decodeIfPresent(StepRetry.self, forKey: .retry)
        sleepBeforeMs = try container.decodeIfPresent(UInt64.self, forKey: .sleepBeforeMs)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(txn, forKey: .txn)
        try container.encodeIfPresent(retry, forKey: .retry)
        try container.encodeIfPresent(sleepBeforeMs, forKey: .sleepBeforeMs)
    }
}

/// Mirrors server/src/protocol.rs::WorkflowSpec — camelCase, unknown fields
/// rejected. Stored verbatim per run; a run snapshots its spec.
public struct WorkflowSpec: Equatable, Codable, Sendable {
    public var name: String
    public var steps: [WorkflowStepSpec]

    public init(name: String, steps: [WorkflowStepSpec]) {
        self.name = name
        self.steps = steps
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case name, steps
    }

    public init(from decoder: Decoder) throws {
        try decoder.rejectUnknownKeys("WorkflowSpec", as: CodingKeys.self)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        name = try container.decode(String.self, forKey: .name)
        steps = try container.decode([WorkflowStepSpec].self, forKey: .steps)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(name, forKey: .name)
        try container.encode(steps, forKey: .steps)
    }
}

/// Mirrors server/src/protocol.rs::WorkflowStatus — snake_case wire strings.
public enum WorkflowStatus: String, Codable, Sendable {
    case pending
    case running
    case success
    case failed
    case cancelled
}

/// Mirrors server/src/protocol.rs::OutcomeStatus — lowercase wire strings.
public enum OutcomeStatus: String, Codable, Sendable {
    case success
    case failed
}

/// Mirrors server/src/protocol.rs::StepOutcome — camelCase, unknown fields
/// rejected; `error` omitted when nil.
public struct StepOutcome: Equatable, Codable, Sendable {
    public var stepIndex: UInt32
    public var status: OutcomeStatus
    public var attempts: UInt32
    public var at: Int64
    public var error: String?

    public init(
        stepIndex: UInt32, status: OutcomeStatus, attempts: UInt32, at: Int64, error: String? = nil
    ) {
        self.stepIndex = stepIndex
        self.status = status
        self.attempts = attempts
        self.at = at
        self.error = error
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case stepIndex, status, attempts, at, error
    }

    public init(from decoder: Decoder) throws {
        try decoder.rejectUnknownKeys("StepOutcome", as: CodingKeys.self)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        stepIndex = try container.decode(UInt32.self, forKey: .stepIndex)
        status = try container.decode(OutcomeStatus.self, forKey: .status)
        attempts = try container.decode(UInt32.self, forKey: .attempts)
        at = try container.decode(Int64.self, forKey: .at)
        error = try container.decodeIfPresent(String.self, forKey: .error)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(stepIndex, forKey: .stepIndex)
        try container.encode(status, forKey: .status)
        try container.encode(attempts, forKey: .attempts)
        try container.encode(at, forKey: .at)
        try container.encodeIfPresent(error, forKey: .error)
    }
}

/// Mirrors server/src/protocol.rs::WorkflowInfo — camelCase, unknown fields
/// rejected; optional fields omitted when nil.
public struct WorkflowInfo: Equatable, Codable, Sendable {
    public var id: String
    public var name: String
    public var status: WorkflowStatus
    public var currentStep: UInt32
    public var stepCount: UInt32
    public var attempts: UInt32
    public var sleepUntil: Int64?
    public var lastError: String?
    public var createdAt: Int64
    public var updatedAt: Int64
    public var startedAt: Int64?
    public var finishedAt: Int64?

    public init(
        id: String, name: String, status: WorkflowStatus, currentStep: UInt32,
        stepCount: UInt32, attempts: UInt32, sleepUntil: Int64? = nil,
        lastError: String? = nil, createdAt: Int64, updatedAt: Int64,
        startedAt: Int64? = nil, finishedAt: Int64? = nil
    ) {
        self.id = id
        self.name = name
        self.status = status
        self.currentStep = currentStep
        self.stepCount = stepCount
        self.attempts = attempts
        self.sleepUntil = sleepUntil
        self.lastError = lastError
        self.createdAt = createdAt
        self.updatedAt = updatedAt
        self.startedAt = startedAt
        self.finishedAt = finishedAt
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case id, name, status, currentStep, stepCount, attempts
        case sleepUntil, lastError, createdAt, updatedAt, startedAt, finishedAt
    }

    public init(from decoder: Decoder) throws {
        try decoder.rejectUnknownKeys("WorkflowInfo", as: CodingKeys.self)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        id = try container.decode(String.self, forKey: .id)
        name = try container.decode(String.self, forKey: .name)
        status = try container.decode(WorkflowStatus.self, forKey: .status)
        currentStep = try container.decode(UInt32.self, forKey: .currentStep)
        stepCount = try container.decode(UInt32.self, forKey: .stepCount)
        attempts = try container.decode(UInt32.self, forKey: .attempts)
        sleepUntil = try container.decodeIfPresent(Int64.self, forKey: .sleepUntil)
        lastError = try container.decodeIfPresent(String.self, forKey: .lastError)
        createdAt = try container.decode(Int64.self, forKey: .createdAt)
        updatedAt = try container.decode(Int64.self, forKey: .updatedAt)
        startedAt = try container.decodeIfPresent(Int64.self, forKey: .startedAt)
        finishedAt = try container.decodeIfPresent(Int64.self, forKey: .finishedAt)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(id, forKey: .id)
        try container.encode(name, forKey: .name)
        try container.encode(status, forKey: .status)
        try container.encode(currentStep, forKey: .currentStep)
        try container.encode(stepCount, forKey: .stepCount)
        try container.encode(attempts, forKey: .attempts)
        try container.encodeIfPresent(sleepUntil, forKey: .sleepUntil)
        try container.encodeIfPresent(lastError, forKey: .lastError)
        try container.encode(createdAt, forKey: .createdAt)
        try container.encode(updatedAt, forKey: .updatedAt)
        try container.encodeIfPresent(startedAt, forKey: .startedAt)
        try container.encodeIfPresent(finishedAt, forKey: .finishedAt)
    }
}

// MARK: - FilterExpr

/// Mirrors server/src/dsl.rs::FilterExpr — internally tagged `"op"`, lowercase
/// tags, unknown fields rejected per variant. Leaves compare one declared
/// field to a value (`in` to a non-empty list); `and`/`or` nest arbitrarily;
/// `not` wraps a nested expr; `contains` tests membership of `value` in
/// `doc.field[]`; `exists` tests the field is present and non-null.
public indirect enum FilterExpr: Equatable, Codable, Sendable {
    /// `field == value`.
    case eq(field: String, value: JSONValue)
    /// `field != value`.
    case neq(field: String, value: JSONValue)
    /// `field > value`.
    case gt(field: String, value: JSONValue)
    /// `field >= value`.
    case gte(field: String, value: JSONValue)
    /// `field < value`.
    case lt(field: String, value: JSONValue)
    /// `field <= value`.
    case lte(field: String, value: JSONValue)
    /// `field` equals any of `values` (non-empty).
    case inValues(field: String, values: [JSONValue])
    /// Every sub-expression matches.
    case and(exprs: [FilterExpr])
    /// Any sub-expression matches.
    case or(exprs: [FilterExpr])
    /// Negation.
    case not(expr: FilterExpr)
    /// `value` is a member of `doc.field[]`.
    case contains(field: String, value: JSONValue)
    /// The field is present and non-null.
    case exists(field: String)

    enum CodingKeys: String, CodingKey, CaseIterable {
        case op, field, value, values, exprs, expr
    }

    // swiftlint:disable:next cyclomatic_complexity function_body_length
    public init(from decoder: Decoder) throws {
        let payload = try taggedEnumPayload("FilterExpr", tagKey: "op", from: decoder)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        func leaf(_ field: CodingKeys, value: CodingKeys) throws -> (String, JSONValue) {
            try (container.decode(String.self, forKey: field),
                 container.decode(JSONValue.self, forKey: value))
        }
        switch payload.tag {
        case "eq":
            try rejectUnknownVariantFields(
                "FilterExpr", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "field", "value"]
            )
            let (field, value) = try leaf(.field, value: .value)
            self = .eq(field: field, value: value)
        case "neq":
            try rejectUnknownVariantFields(
                "FilterExpr", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "field", "value"]
            )
            let (field, value) = try leaf(.field, value: .value)
            self = .neq(field: field, value: value)
        case "gt":
            try rejectUnknownVariantFields(
                "FilterExpr", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "field", "value"]
            )
            let (field, value) = try leaf(.field, value: .value)
            self = .gt(field: field, value: value)
        case "gte":
            try rejectUnknownVariantFields(
                "FilterExpr", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "field", "value"]
            )
            let (field, value) = try leaf(.field, value: .value)
            self = .gte(field: field, value: value)
        case "lt":
            try rejectUnknownVariantFields(
                "FilterExpr", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "field", "value"]
            )
            let (field, value) = try leaf(.field, value: .value)
            self = .lt(field: field, value: value)
        case "lte":
            try rejectUnknownVariantFields(
                "FilterExpr", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "field", "value"]
            )
            let (field, value) = try leaf(.field, value: .value)
            self = .lte(field: field, value: value)
        case "in":
            try rejectUnknownVariantFields(
                "FilterExpr", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "field", "values"]
            )
            self = try .inValues(
                field: container.decode(String.self, forKey: .field),
                values: container.decode([JSONValue].self, forKey: .values)
            )
        case "and":
            try rejectUnknownVariantFields(
                "FilterExpr", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "exprs"]
            )
            self = try .and(exprs: container.decode([FilterExpr].self, forKey: .exprs))
        case "or":
            try rejectUnknownVariantFields(
                "FilterExpr", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "exprs"]
            )
            self = try .or(exprs: container.decode([FilterExpr].self, forKey: .exprs))
        case "not":
            try rejectUnknownVariantFields(
                "FilterExpr", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "expr"]
            )
            self = try .not(expr: container.decode(FilterExpr.self, forKey: .expr))
        case "contains":
            try rejectUnknownVariantFields(
                "FilterExpr", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "field", "value"]
            )
            let (field, value) = try leaf(.field, value: .value)
            self = .contains(field: field, value: value)
        case "exists":
            try rejectUnknownVariantFields(
                "FilterExpr", variant: payload.tag, keys: payload.keys,
                allowed: ["op", "field"]
            )
            self = try .exists(field: container.decode(String.self, forKey: .field))
        case let unknown:
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: decoder.codingPath,
                    debugDescription: "FilterExpr: unknown op '\(unknown)'"
                )
            )
        }
    }

    // swiftlint:disable:next cyclomatic_complexity
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case let .eq(field, value):
            try container.encode("eq", forKey: .op)
            try container.encode(field, forKey: .field)
            try container.encode(value, forKey: .value)
        case let .neq(field, value):
            try container.encode("neq", forKey: .op)
            try container.encode(field, forKey: .field)
            try container.encode(value, forKey: .value)
        case let .gt(field, value):
            try container.encode("gt", forKey: .op)
            try container.encode(field, forKey: .field)
            try container.encode(value, forKey: .value)
        case let .gte(field, value):
            try container.encode("gte", forKey: .op)
            try container.encode(field, forKey: .field)
            try container.encode(value, forKey: .value)
        case let .lt(field, value):
            try container.encode("lt", forKey: .op)
            try container.encode(field, forKey: .field)
            try container.encode(value, forKey: .value)
        case let .lte(field, value):
            try container.encode("lte", forKey: .op)
            try container.encode(field, forKey: .field)
            try container.encode(value, forKey: .value)
        case let .inValues(field, values):
            try container.encode("in", forKey: .op)
            try container.encode(field, forKey: .field)
            try container.encode(values, forKey: .values)
        case let .and(exprs):
            try container.encode("and", forKey: .op)
            try container.encode(exprs, forKey: .exprs)
        case let .or(exprs):
            try container.encode("or", forKey: .op)
            try container.encode(exprs, forKey: .exprs)
        case let .not(expr):
            try container.encode("not", forKey: .op)
            try container.encode(expr, forKey: .expr)
        case let .contains(field, value):
            try container.encode("contains", forKey: .op)
            try container.encode(field, forKey: .field)
            try container.encode(value, forKey: .value)
        case let .exists(field):
            try container.encode("exists", forKey: .op)
            try container.encode(field, forKey: .field)
        }
    }
}

// MARK: - Search terminals

/// Mirrors server/src/dsl.rs::SearchMode — lowercase: `"tsquery"` (default
/// full-text) | `"trgm"` (substring/autocomplete).
public enum SearchMode: String, Codable, Sendable {
    case tsquery
    case trgm
}

/// Mirrors server/src/dsl.rs::SearchQuery — camelCase, unknown fields
/// rejected; `filter`/`mode`/`snippet` omitted when nil.
public struct SearchQuery: Equatable, Codable, Sendable {
    public var index: String
    public var query: String
    public var filter: FilterExpr?
    public var mode: SearchMode?
    public var snippet: Bool?

    public init(
        index: String, query: String, filter: FilterExpr? = nil,
        mode: SearchMode? = nil, snippet: Bool? = nil
    ) {
        self.index = index
        self.query = query
        self.filter = filter
        self.mode = mode
        self.snippet = snippet
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case index, query, filter, mode, snippet
    }

    public init(from decoder: Decoder) throws {
        try decoder.rejectUnknownKeys("SearchQuery", as: CodingKeys.self)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        index = try container.decode(String.self, forKey: .index)
        query = try container.decode(String.self, forKey: .query)
        filter = try container.decodeIfPresent(FilterExpr.self, forKey: .filter)
        mode = try container.decodeIfPresent(SearchMode.self, forKey: .mode)
        snippet = try container.decodeIfPresent(Bool.self, forKey: .snippet)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(index, forKey: .index)
        try container.encode(query, forKey: .query)
        try container.encodeIfPresent(filter, forKey: .filter)
        try container.encodeIfPresent(mode, forKey: .mode)
        try container.encodeIfPresent(snippet, forKey: .snippet)
    }
}

/// Mirrors server/src/dsl.rs::VectorSearchQuery — camelCase, unknown fields
/// rejected. `vector` is `[Double]` (f64) for wire-precision parity with the
/// other clients (ARC-008(a)); `filter` omitted when nil.
public struct VectorSearchQuery: Equatable, Codable, Sendable {
    public var index: String
    public var vector: [Double]
    public var limit: UInt32
    public var filter: FilterExpr?

    public init(index: String, vector: [Double], limit: UInt32, filter: FilterExpr? = nil) {
        self.index = index
        self.vector = vector
        self.limit = limit
        self.filter = filter
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case index, vector, limit, filter
    }

    public init(from decoder: Decoder) throws {
        try decoder.rejectUnknownKeys("VectorSearchQuery", as: CodingKeys.self)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        index = try container.decode(String.self, forKey: .index)
        vector = try container.decode([Double].self, forKey: .vector)
        limit = try container.decode(UInt32.self, forKey: .limit)
        filter = try container.decodeIfPresent(FilterExpr.self, forKey: .filter)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(index, forKey: .index)
        try container.encode(vector, forKey: .vector)
        try container.encode(limit, forKey: .limit)
        try container.encodeIfPresent(filter, forKey: .filter)
    }
}

/// Mirrors server/src/dsl.rs::HybridSearchQuery — camelCase, unknown fields
/// rejected; `searchIndex`/`vectorIndex`/`k` omitted when nil.
public struct HybridSearchQuery: Equatable, Codable, Sendable {
    public var query: String
    public var vector: [Double]
    public var limit: UInt32
    public var searchIndex: String?
    public var vectorIndex: String?
    public var k: UInt32?

    public init(
        query: String, vector: [Double], limit: UInt32, searchIndex: String? = nil,
        vectorIndex: String? = nil, k: UInt32? = nil
    ) {
        self.query = query
        self.vector = vector
        self.limit = limit
        self.searchIndex = searchIndex
        self.vectorIndex = vectorIndex
        self.k = k
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case query, vector, limit, searchIndex, vectorIndex, k
    }

    public init(from decoder: Decoder) throws {
        try decoder.rejectUnknownKeys("HybridSearchQuery", as: CodingKeys.self)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        query = try container.decode(String.self, forKey: .query)
        vector = try container.decode([Double].self, forKey: .vector)
        limit = try container.decode(UInt32.self, forKey: .limit)
        searchIndex = try container.decodeIfPresent(String.self, forKey: .searchIndex)
        vectorIndex = try container.decodeIfPresent(String.self, forKey: .vectorIndex)
        k = try container.decodeIfPresent(UInt32.self, forKey: .k)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(query, forKey: .query)
        try container.encode(vector, forKey: .vector)
        try container.encode(limit, forKey: .limit)
        try container.encodeIfPresent(searchIndex, forKey: .searchIndex)
        try container.encodeIfPresent(vectorIndex, forKey: .vectorIndex)
        try container.encodeIfPresent(k, forKey: .k)
    }
}

/// Mirrors server/src/dsl.rs::AggregateOp — lowercase: sum/avg/min/max/count.
public enum AggregateOp: String, Codable, Sendable {
    case sum
    case avg
    case min
    case max
    case count
}

/// Mirrors server/src/dsl.rs::AggregateSpec — camelCase, unknown fields
/// rejected; `groupBy` omitted when false (client convention; the server's
/// `#[serde(default)]` accepts both forms).
public struct AggregateSpec: Equatable, Codable, Sendable {
    public var op: AggregateOp
    public var groupBy: Bool

    public init(op: AggregateOp, groupBy: Bool = false) {
        self.op = op
        self.groupBy = groupBy
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case op, groupBy
    }

    public init(from decoder: Decoder) throws {
        try decoder.rejectUnknownKeys("AggregateSpec", as: CodingKeys.self)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        op = try container.decode(AggregateOp.self, forKey: .op)
        groupBy = try container.decodeIfPresent(Bool.self, forKey: .groupBy) ?? false
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(op, forKey: .op)
        if groupBy {
            try container.encode(groupBy, forKey: .groupBy)
        }
    }
}
