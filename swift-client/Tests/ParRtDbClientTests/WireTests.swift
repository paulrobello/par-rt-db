import Foundation
@testable import ParRtDbClient
import Testing

// Wire-contract tests (Task 4) — the fifth implementation of the protocol.
// Fixtures are copied verbatim from server/src/protocol.rs inline tests,
// rust-client/src/{wire,mutation,query}.rs inline tests, and the
// wire-corpus/wire-corpus.json sections Task 7 round-trips.

// MARK: - Helpers

private func roundTrip<T: Codable & Equatable>(_ value: T) throws -> T {
    try JSONDecoder().decode(T.self, from: JSONEncoder().encode(value))
}

private func decode<T: Decodable>(_: T.Type, _ json: String) throws -> T {
    try JSONDecoder().decode(T.self, from: Data(json.utf8))
}

/// Encode-then-compare against a JSON literal as PARSED objects. Key order is
/// not contract — JSONEncoder emits CodingKeys order while the corpus compares
/// parsed values, so byte-order assertions are wrong by construction here.
/// Throws on mismatch (no #expect inside: the macro's autoclosure thunk inside
/// a generic function trips a Swift 6.3.3 compiler crash).
private func expectEncodes(_ value: some Codable, as json: String) throws {
    let data = try JSONEncoder().encode(value)
    let dumped = try JSONSerialization.jsonObject(with: data) as AnyObject
    let expected = try JSONSerialization.jsonObject(with: Data(json.utf8)) as AnyObject
    guard dumped.isEqual(expected) else {
        throw WireMismatch(
            "encoded \(String(data: data, encoding: .utf8) ?? "<unencodable>") but expected \(json)"
        )
    }
}

private struct WireMismatch: Error, CustomStringConvertible {
    let message: String
    init(_ message: String) {
        self.message = message
    }

    var description: String {
        message
    }
}

private func encodedText(_ value: some Codable) throws -> String {
    try String(data: JSONEncoder().encode(value), encoding: .utf8) ?? ""
}

private func expectDecodingThrows<T: Decodable>(_: T.Type, _ json: String) {
    // Issue.record instead of #expect — see expectEncodes for the reason.
    do {
        _ = try JSONDecoder().decode(T.self, from: Data(json.utf8))
        Issue.record("\(json): expected a DecodingError rejection")
    } catch let error as DecodingError {
        // The expected rejection.
    } catch {
        Issue.record("\(json): expected DecodingError, got \(error)")
    }
}

// MARK: - ClientMessage

struct WireTests {
    @Test func authMessageRoundTrips() throws {
        // protocol.rs client_message_wire_tags_and_fields: token form and the
        // SEC-001 phase-2 tokenless (cookie-mode) form.
        let withToken = ClientMessage.auth(token: "t", db: "d")
        try expectEncodes(withToken, as: #"{"type":"auth","token":"t","db":"d"}"#)
        #expect(try roundTrip(withToken) == withToken)

        let tokenless = ClientMessage.auth(token: nil, db: "d")
        try expectEncodes(tokenless, as: #"{"type":"auth","db":"d"}"#)
        let parsed = try decode(ClientMessage.self, #"{"type":"auth","db":"d"}"#)
        #expect(parsed == tokenless)
    }

    @Test func mutateOmitsIdempotencyKeyWhenNil() throws {
        let txn = Transaction(steps: [])
        let without = ClientMessage.mutate(mutId: "m1", idempotencyKey: nil, txn: txn)
        let with = ClientMessage.mutate(mutId: "m1", idempotencyKey: "key1", txn: txn)
        try expectEncodes(without, as: #"{"type":"mutate","mutId":"m1","txn":{"steps":[]}}"#)
        try expectEncodes(
            with, as: #"{"type":"mutate","mutId":"m1","idempotencyKey":"key1","txn":{"steps":[]}}"#
        )
        #expect(try !encodedText(without).contains("idempotencyKey"))
        #expect(try encodedText(with).contains(#""idempotencyKey":"key1""#))
    }

    @Test func pingEncodesExactly() throws {
        // Single-key frame — exact bytes are safe here (one key, no order).
        let ping = ClientMessage.ping
        #expect(try encodedText(ping) == #"{"type":"ping"}"#)
        #expect(try roundTrip(ping) == ping)
    }

    @Test func subscribeRoundTrips() throws {
        // protocol.rs client_message_round_trips_through_json.
        let msg = ClientMessage.subscribe(queryId: "q1", query: Query(table: "workItems"))
        try expectEncodes(
            msg, as: #"{"type":"subscribe","queryId":"q1","query":{"table":"workItems"}}"#
        )
        #expect(try roundTrip(msg) == msg)
    }

    @Test func clientMessageRejectsUnknownField() {
        // Brief fixture, protocol.rs client_message_rejects_unknown_fields, and
        // the corpus rejects_client_message_unknown_field entries.
        expectDecodingThrows(ClientMessage.self, #"{"type":"ping","zzz":1}"#)
        expectDecodingThrows(ClientMessage.self, #"{"type":"auth","token":"t","db":"d","bogus":true}"#)
        expectDecodingThrows(ClientMessage.self, #"{"type":"unsubscribe","queryId":"q1","extra":1}"#)
        // serde's deny_unknown_fields is PER VARIANT: queryId is a declared key
        // on another variant, but unknown on auth.
        expectDecodingThrows(ClientMessage.self, #"{"type":"auth","token":"t","db":"d","queryId":"q"}"#)
    }

    @Test func clientMessageRejectsUnknownTypeAndMissingType() {
        expectDecodingThrows(ClientMessage.self, #"{"type":"bogus"}"#)
        expectDecodingThrows(ClientMessage.self, #"{"token":"t","db":"d"}"#)
    }

    @Test func scheduleClientMessageWireTags() throws {
        // protocol.rs schedule_message_wire_tags.
        let msg = ClientMessage.schedule(
            scheduleId: "s1",
            when: .afterMs(ms: 100),
            txn: Transaction(steps: [])
        )
        try expectEncodes(
            msg,
            as: #"{"type":"schedule","scheduleId":"s1","when":{"type":"afterMs","ms":100},"txn":{"steps":[]}}"#
        )
        #expect(try roundTrip(msg) == msg)
    }

    @Test func scheduleControlFrames() throws {
        try expectEncodes(
            ClientMessage.cancelSchedule(scheduleId: "s1", id: "job-9"),
            as: #"{"type":"cancelSchedule","scheduleId":"s1","id":"job-9"}"#
        )
        try expectEncodes(
            ClientMessage.pauseSchedule(scheduleId: "s1", id: "job-9"),
            as: #"{"type":"pauseSchedule","scheduleId":"s1","id":"job-9"}"#
        )
        try expectEncodes(
            ClientMessage.resumeSchedule(scheduleId: "s1", id: "job-9"),
            as: #"{"type":"resumeSchedule","scheduleId":"s1","id":"job-9"}"#
        )
        try expectEncodes(
            ClientMessage.listSchedules(scheduleId: "s1"),
            as: #"{"type":"listSchedules","scheduleId":"s1"}"#
        )
    }

    @Test func workflowClientFrames() throws {
        // protocol.rs workflow_frame_wire_shapes.
        let spec = WorkflowSpec(name: "drip", steps: [WorkflowStepSpec(txn: Transaction(steps: []))])
        try expectEncodes(
            ClientMessage.startWorkflow(workflowId: "c1", spec: spec),
            as: #"{"type":"startWorkflow","workflowId":"c1","spec":{"name":"drip","steps":[{"txn":{"steps":[]}}]}}"#
        )
        try expectEncodes(
            ClientMessage.cancelWorkflow(workflowId: "c2", id: "wf9"),
            as: #"{"type":"cancelWorkflow","workflowId":"c2","id":"wf9"}"#
        )
        // status omitted when nil, snake_case string when set, parses back.
        try expectEncodes(
            ClientMessage.listWorkflows(workflowId: "c3", status: nil),
            as: #"{"type":"listWorkflows","workflowId":"c3"}"#
        )
        let filtered = try decode(
            ClientMessage.self, #"{"type":"listWorkflows","workflowId":"c3","status":"failed"}"#
        )
        #expect(filtered == ClientMessage.listWorkflows(workflowId: "c3", status: .failed))
    }

    @Test func presenceClientFrames() throws {
        // protocol.rs presence_client_message_wire_tags / presence_state_ttl_ms_wire_tag.
        try expectEncodes(
            ClientMessage.presence(room: "doc:1", state: nil),
            as: #"{"type":"presence","room":"doc:1"}"#
        )
        try expectEncodes(
            ClientMessage.presence(room: "doc:1", state: .object(["x": .int(3), "y": .int(4)])),
            as: #"{"type":"presence","room":"doc:1","state":{"x":3,"y":4}}"#
        )
        try expectEncodes(
            ClientMessage.presenceState(room: "doc:1", state: .object(["typing": .bool(true)]), ttlMs: nil),
            as: #"{"type":"presenceState","room":"doc:1","state":{"typing":true}}"#
        )
        // ttlMs present when Some — including Some(0): skip checks is_none, not falsiness.
        try expectEncodes(
            ClientMessage.presenceState(room: "doc:1", state: .object(["typing": .bool(true)]), ttlMs: 0),
            as: #"{"type":"presenceState","room":"doc:1","state":{"typing":true},"ttlMs":0}"#
        )
        let back = try decode(
            ClientMessage.self, #"{"type":"presenceState","room":"doc:1","state":{},"ttlMs":500}"#
        )
        #expect(
            back == ClientMessage.presenceState(room: "doc:1", state: .object([:]), ttlMs: 500)
        )
        try expectEncodes(
            ClientMessage.leavePresence(room: "doc:1"),
            as: #"{"type":"leavePresence","room":"doc:1"}"#
        )
    }
}

/// Server-message frame fixtures — the server -> client half of the WS
/// vocabulary (its own suite only for the 400-line type-body lint cap).
struct WireServerMessageTests {
    // MARK: - ServerMessage

    @Test func serverMessageCoreFixtures() throws {
        // protocol.rs server_message_wire_tags_and_fields.
        try expectEncodes(
            ServerMessage.queryUpdate(queryId: "q1", result: .array([])),
            as: #"{"type":"queryUpdate","queryId":"q1","result":[]}"#
        )
        try expectEncodes(
            ServerMessage.mutateOk(mutId: "m1", results: []),
            as: #"{"type":"mutateOk","mutId":"m1","results":[]}"#
        )
        try expectEncodes(
            ServerMessage.subscribeErr(
                queryId: "q1",
                error: RtDbError(code: .badRequest, message: "bad index")
            ),
            as: #"{"type":"subscribeErr","queryId":"q1","error":{"code":"BAD_REQUEST","message":"bad index"}}"#
        )
        #expect(try encodedText(ServerMessage.pong) == #"{"type":"pong"}"#)
    }

    @Test func authOkCarriesExplicitNullEmailAndName() throws {
        // AuthedUser.email/name are plain Options — null on the wire when
        // absent (never omitted); githubLogin/githubId are omitted (below).
        let msg = ServerMessage.authOk(
            user: AuthedUser(kind: .user, email: "a@b.com", name: nil)
        )
        try expectEncodes(
            msg,
            as: #"{"type":"authOk","user":{"kind":"user","email":"a@b.com","name":null}}"#
        )
        #expect(try roundTrip(msg) == msg)
    }

    @Test func queryUpdateResultValueVariants() throws {
        // Corpus server_messages queryUpdate fixtures: the untagged result is
        // any JSON value — null, object, array, number, paginated shape.
        let fixtures = [
            #"{"type":"queryUpdate","queryId":"q1","result":null}"#,
            #"{"type":"queryUpdate","queryId":"q1","result":{"_id":"abc","title":"first"}}"#,
            #"{"type":"queryUpdate","queryId":"q1","result":[]}"#,
            #"{"type":"queryUpdate","queryId":"q1","result":[{"_id":"a"},{"_id":"b"}]}"#,
            #"{"type":"queryUpdate","queryId":"q1","result":42}"#,
            #"{"type":"queryUpdate","queryId":"q1","result":{"docs":[{"_id":"a"}],"nextCursor":"cur1"}}"#
        ]
        for json in fixtures {
            let msg = try decode(ServerMessage.self, json)
            try expectEncodes(msg, as: json)
        }
    }

    @Test func scheduleServerFrames() throws {
        try expectEncodes(
            ServerMessage.scheduleOk(scheduleId: "s1", id: "job-9"),
            as: #"{"type":"scheduleOk","scheduleId":"s1","id":"job-9"}"#
        )
        // Ack error omitted when nil, present when failed (corpus fixtures).
        try expectEncodes(
            ServerMessage.scheduleAck(scheduleId: "s1", ok: true, error: nil),
            as: #"{"type":"scheduleAck","scheduleId":"s1","ok":true}"#
        )
        try expectEncodes(
            ServerMessage.scheduleAck(
                scheduleId: "s1", ok: false,
                error: RtDbError(code: .notFound, message: "missing job")
            ),
            as: """
            {"type":"scheduleAck","scheduleId":"s1","ok":false,
            "error":{"code":"NOT_FOUND","message":"missing job"}}
            """
        )
    }

    @Test func workflowServerFrames() throws {
        // protocol.rs workflow_frame_wire_shapes + workflow_info_wire_shape:
        // optional info fields are omitted when nil (lastError/finishedAt).
        let info = WorkflowInfo(
            id: "wf1", name: "drip", status: .pending, currentStep: 0, stepCount: 2,
            attempts: 0, sleepUntil: 123, lastError: nil, createdAt: 1, updatedAt: 2,
            startedAt: nil, finishedAt: nil
        )
        let expectedInfo = #""id":"wf1","name":"drip","status":"pending","currentStep":0"# +
            #","stepCount":2,"attempts":0,"sleepUntil":123,"createdAt":1,"updatedAt":2"#
        try expectEncodes(
            ServerMessage.startWorkflowOk(workflowId: "c1", info: info),
            as: #"{"type":"startWorkflowOk","workflowId":"c1","info":{\#(expectedInfo)}}"#
        )
        try expectEncodes(
            ServerMessage.startWorkflowErr(
                workflowId: "c1", error: RtDbError(code: .badRequest, message: "bad spec")
            ),
            as: #"{"type":"startWorkflowErr","workflowId":"c1","error":{"code":"BAD_REQUEST","message":"bad spec"}}"#
        )
        try expectEncodes(
            ServerMessage.workflowAck(workflowId: "c1", ok: true, error: nil),
            as: #"{"type":"workflowAck","workflowId":"c1","ok":true}"#
        )
        try expectEncodes(
            ServerMessage.listWorkflowsOk(workflowId: "c4", workflows: [info]),
            as: #"{"type":"listWorkflowsOk","workflowId":"c4","workflows":[{\#(expectedInfo)}]}"#
        )
    }

    @Test func presenceServerFrames() throws {
        // protocol.rs presence_server_message_wire_tags — the member's user
        // carries name: null (plain Option) and omits github* (skip rule).
        let member = PresenceMember(
            connectionId: "42",
            user: AuthedUser(kind: .user, email: "a@b.com", name: nil),
            state: .object(["x": .int(1)])
        )
        try expectEncodes(
            ServerMessage.presenceSnapshot(room: "doc:1", members: [member]),
            as: """
            {"type":"presenceSnapshot","room":"doc:1","members":[{"connectionId":"42",
            "user":{"kind":"user","email":"a@b.com","name":null},"state":{"x":1}}]}
            """
        )
        try expectEncodes(
            ServerMessage.presenceErr(
                room: "doc:1", error: RtDbError(code: .forbidden, message: "presence not enabled")
            ),
            as: #"{"type":"presenceErr","room":"doc:1","error":{"code":"FORBIDDEN","message":"presence not enabled"}}"#
        )
    }

    @Test func serverMessageToleratesUnknownFields() throws {
        // rust-client parity: ServerMessage deliberately does NOT carry
        // deny_unknown_fields (client-side forward compatibility — a newer
        // server may add fields without breaking older clients).
        let msg = try decode(ServerMessage.self, #"{"type":"pong","futureField":1}"#)
        #expect(msg == ServerMessage.pong)
    }

    @Test func serverMessageRejectsUnknownType() {
        expectDecodingThrows(ServerMessage.self, #"{"type":"bogus"}"#)
    }
} // message-frame fixtures end; DSL fixtures below

/// Wire DSL fixtures — AuthedUser/schedule/workflow scalars, FilterExpr, the
/// search terminals, Query, Step/StepResult/Transaction (same Task 4 contract,
/// split only to keep both suites under the 400-line type-body lint cap).
struct WireDslTests {
    // MARK: - AuthedUser / UserKind

    @Test func userKindWireStrings() throws {
        #expect(UserKind.user.rawValue == "user")
        #expect(UserKind.machine.rawValue == "machine")
        #expect(try roundTrip(UserKind.user) == .user)
        expectDecodingThrows(UserKind.self, #""robot""#)
    }

    @Test func authedUserCorpusFixtures() throws {
        // Corpus authed_users — all four round-trip byte-parity.
        let fixtures = [
            #"{"kind":"user","email":"a@b.com","name":null,"githubLogin":"alice","githubId":12345}"#,
            #"{"kind":"user","email":"a@b.com","name":"Alice"}"#,
            #"{"kind":"machine","email":null,"name":null}"#,
            #"{"kind":"machine","email":null,"name":null,"githubLogin":"ci-bot","githubId":999}"#
        ]
        for json in fixtures {
            let user = try decode(AuthedUser.self, json)
            try expectEncodes(user, as: json)
        }
    }

    @Test func authedUserEmailIsNullNeverOmittedGithubOmittedWhenNil() throws {
        let user = try decode(AuthedUser.self, #"{"kind":"user","email":null,"name":null}"#)
        let out = try encodedText(user)
        #expect(out.contains(#""email":null"#))
        #expect(out.contains(#""name":null"#))
        #expect(!out.contains("githubLogin"))
        #expect(!out.contains("githubId"))
    }

    @Test func authedUserRejectsUnknownKind() {
        expectDecodingThrows(AuthedUser.self, #"{"kind":"robot","email":null,"name":null}"#)
    }

    // MARK: - ScheduleWhen / ScheduleInfo

    @Test func scheduleWhenTags() throws {
        // protocol.rs schedule_when_wire_tags — camelCase tags on "type".
        try expectEncodes(ScheduleWhen.afterMs(ms: 5), as: #"{"type":"afterMs","ms":5}"#)
        try expectEncodes(ScheduleWhen.runAt(ms: 9), as: #"{"type":"runAt","ms":9}"#)
        try expectEncodes(
            ScheduleWhen.cron(expr: "*/5 * * * *"), as: #"{"type":"cron","expr":"*/5 * * * *"}"#
        )
        let whens = [ScheduleWhen.afterMs(ms: 100), .runAt(ms: 1_700_000_000_000),
                     .cron(expr: "0 * * * *")]
        for when in whens {
            #expect(try roundTrip(when) == when)
        }
    }

    @Test func scheduleWhenRejectsUnknownFieldAndTag() {
        // Corpus rejects_schedule_when_unknown_field + unknown tag.
        expectDecodingThrows(ScheduleWhen.self, #"{"type":"afterMs","ms":1,"x":9}"#)
        expectDecodingThrows(ScheduleWhen.self, #"{"type":"yearly","ms":1}"#)
    }

    @Test func scheduleInfoCorpusFixtures() throws {
        // Corpus schedule_infos — cron/lastError omitted when nil.
        let fixtures = [
            #"{"id":"j1","kind":"oneshot","dueAt":1000,"status":"pending","createdAt":500,"firedCount":0}"#,
            """
            {"id":"j4","kind":"oneshot","dueAt":1300,"status":"error",
            "lastError":"boom","createdAt":500,"firedCount":0}
            """,
            """
            {"id":"j5","kind":"cron","dueAt":2000,"cron":"*/5 * * * *",
            "status":"pending","createdAt":500,"firedCount":0}
            """,
            """
            {"id":"j8","kind":"cron","dueAt":2300,"cron":"0 * * * *",
            "status":"error","lastError":"kaboom","createdAt":500,"firedCount":7}
            """
        ]
        for json in fixtures {
            let info = try decode(ScheduleInfo.self, json)
            try expectEncodes(info, as: json)
        }
    }

    @Test func scheduleInfoRejectsUnknownKindAndStatus() {
        let rejectFixtures = [
            #"{"id":"j1","kind":"interval","dueAt":1000,"status":"pending","createdAt":500,"firedCount":0}"#,
            #"{"id":"j1","kind":"oneshot","dueAt":1000,"status":"queued","createdAt":500,"firedCount":0}"#
        ]
        for json in rejectFixtures {
            expectDecodingThrows(ScheduleInfo.self, json)
        }
    }

    // MARK: - Workflow status / retry / spec

    @Test func workflowStatusWireIsSnakeCase() throws {
        let all: [(WorkflowStatus, String)] = [
            (.pending, "pending"), (.running, "running"), (.success, "success"),
            (.failed, "failed"), (.cancelled, "cancelled")
        ]
        for (variant, wire) in all {
            #expect(variant.rawValue == wire)
            #expect(try roundTrip(variant) == variant)
        }
        expectDecodingThrows(WorkflowStatus.self, #""bogus""#)
    }

    @Test func stepRetryRequiresMaxAttemptsAndDefaultsTheRest() throws {
        // protocol.rs step_retry_requires_max_attempts.
        expectDecodingThrows(StepRetry.self, #"{"initialRetryMs":100,"maxRetryMs":200}"#)
        let retry = try decode(StepRetry.self, #"{"maxAttempts":4}"#)
        #expect(retry == StepRetry(maxAttempts: 4))
        #expect(retry.initialRetryMs == 1000)
        #expect(retry.maxRetryMs == 60000)
        expectDecodingThrows(StepRetry.self, #"{"maxAttempts":1,"bogus":true}"#)
    }

    @Test func workflowSpecRoundTrips() throws {
        // protocol.rs workflow_spec_wire_shape — optionals skipped on serialize.
        let spec = try decode(
            WorkflowSpec.self,
            """
            {"name":"drip","steps":[
            {"txn":{"steps":[{"op":"insert","table":"t","doc":{}}]}},
            {"txn":{"steps":[]},"retry":{"maxAttempts":5,"initialRetryMs":500,"maxRetryMs":2000},
            "sleepBeforeMs":86400000}
            ]}
            """
        )
        #expect(spec.steps.count == 2)
        #expect(spec.steps[1].sleepBeforeMs == 86_400_000)
        #expect(spec.steps[1].retry == StepRetry(maxAttempts: 5, initialRetryMs: 500, maxRetryMs: 2000))
        #expect(spec.steps[0].retry == nil)
        #expect(try roundTrip(spec) == spec)
    }
} // schedule/workflow fixtures end; DSL fixtures below

/// Filter/search/query/mutation DSL fixtures (fourth suite — same 400-line
/// lint cap).
struct WireFilterQueryTests {
    // MARK: - FilterExpr

    @Test func filterExprLeafShapes() throws {
        try expectEncodes(
            FilterExpr.eq(field: "status", value: .string("done")),
            as: #"{"op":"eq","field":"status","value":"done"}"#
        )
        try expectEncodes(
            FilterExpr.neq(field: "status", value: .string("todo")),
            as: #"{"op":"neq","field":"status","value":"todo"}"#
        )
        try expectEncodes(
            FilterExpr.gt(field: "createdAt", value: .int(1_780_000_000_000)),
            as: #"{"op":"gt","field":"createdAt","value":1780000000000}"#
        )
        try expectEncodes(
            FilterExpr.gte(field: "createdAt", value: .int(1_780_000_000_000)),
            as: #"{"op":"gte","field":"createdAt","value":1780000000000}"#
        )
        try expectEncodes(
            FilterExpr.lt(field: "order", value: .int(5)),
            as: #"{"op":"lt","field":"order","value":5}"#
        )
        try expectEncodes(
            FilterExpr.lte(field: "order", value: .int(5)),
            as: #"{"op":"lte","field":"order","value":5}"#
        )
        try expectEncodes(
            FilterExpr.inValues(field: "status", values: [.string("a"), .string("b")]),
            as: #"{"op":"in","field":"status","values":["a","b"]}"#
        )
        try expectEncodes(
            FilterExpr.contains(field: "tags", value: .string("red")),
            as: #"{"op":"contains","field":"tags","value":"red"}"#
        )
        try expectEncodes(
            FilterExpr.exists(field: "email"),
            as: #"{"op":"exists","field":"email"}"#
        )
    }

    @Test func filterExprNests() throws {
        // Corpus queries q-filter fixture + rust wire.rs nesting shapes.
        let or = FilterExpr.or(exprs: [
            .inValues(field: "status", values: [.string("a"), .string("b")]),
            .lte(field: "order", value: .int(5))
        ])
        try expectEncodes(
            or,
            as: """
            {"op":"or","exprs":[{"op":"in","field":"status","values":["a","b"]},
            {"op":"lte","field":"order","value":5}]}
            """
        )
        let and = FilterExpr.and(exprs: [
            .eq(field: "channel", value: .string("#general")),
            .gt(field: "createdAt", value: .int(1_780_000_000_000))
        ])
        // ##"..."## delimiters: the fixture contains "#general, which would
        // close a single-# raw string.
        try expectEncodes(
            and,
            as: """
            {"op":"and","exprs":[{"op":"eq","field":"channel","value":"#general"},
            {"op":"gt","field":"createdAt","value":1780000000000}]}
            """
        )
        let not = FilterExpr.not(expr: .exists(field: "email"))
        try expectEncodes(not, as: #"{"op":"not","expr":{"op":"exists","field":"email"}}"#)
        #expect(try roundTrip(and) == and)
        #expect(try roundTrip(not) == not)
    }

    @Test func filterExprRejectsUnknownFieldAndOp() {
        expectDecodingThrows(FilterExpr.self, #"{"op":"eq","field":"f","value":1,"zzz":2}"#)
        expectDecodingThrows(FilterExpr.self, #"{"op":"between","field":"f","value":1}"#)
    }

    // MARK: - Search / vector / hybrid / aggregate

    @Test func searchQueryShapes() throws {
        // rust wire.rs search_query_* fixtures + corpus queries search entries.
        try expectEncodes(
            SearchQuery(index: "search_body", query: "hello world"),
            as: #"{"index":"search_body","query":"hello world"}"#
        )
        try expectEncodes(
            SearchQuery(index: "search_body", query: "conv", mode: .trgm),
            as: #"{"index":"search_body","query":"conv","mode":"trgm"}"#
        )
        try expectEncodes(
            SearchQuery(
                index: "search_body", query: "conv",
                filter: .eq(field: "status", value: .string("open")), mode: .tsquery
            ),
            as: """
            {"index":"search_body","query":"conv",
            "filter":{"op":"eq","field":"status","value":"open"},"mode":"tsquery"}
            """
        )
        try expectEncodes(
            SearchQuery(index: "search_body", query: "hello world", snippet: true),
            as: #"{"index":"search_body","query":"hello world","snippet":true}"#
        )
        // Unknown mode strings are rejected.
        expectDecodingThrows(SearchQuery.self, #"{"index":"i","query":"q","mode":"bogus"}"#)
        expectDecodingThrows(SearchQuery.self, #"{"index":"i","query":"q","zzz":1}"#)
    }

    @Test func vectorSearchShapes() throws {
        // Corpus queries vectorSearch fixtures — vector is f64 on the wire.
        try expectEncodes(
            VectorSearchQuery(index: "by_embedding", vector: [1.0, 0.5, -0.5], limit: 5),
            as: #"{"index":"by_embedding","vector":[1.0,0.5,-0.5],"limit":5}"#
        )
        try expectEncodes(
            VectorSearchQuery(
                index: "by_embedding", vector: [1.0], limit: 3,
                filter: .eq(field: "userId", value: .string("u1"))
            ),
            as: #"{"index":"by_embedding","vector":[1.0],"limit":3,"filter":{"op":"eq","field":"userId","value":"u1"}}"#
        )
        expectDecodingThrows(
            VectorSearchQuery.self, #"{"index":"i","vector":[1.0],"limit":5,"bogus":true}"#
        )
    }

    @Test func hybridSearchShapes() throws {
        // rust wire.rs hybrid_search_query_wire_shape.
        try expectEncodes(
            HybridSearchQuery(query: "hello world", vector: [1.0, 0.0, 0.0], limit: 5),
            as: #"{"query":"hello world","vector":[1.0,0.0,0.0],"limit":5}"#
        )
        try expectEncodes(
            HybridSearchQuery(
                query: "x", vector: [1.0], limit: 1,
                searchIndex: "search_body", vectorIndex: "by_embedding", k: 42
            ),
            as: """
            {"query":"x","vector":[1.0],"limit":1,
            "searchIndex":"search_body","vectorIndex":"by_embedding","k":42}
            """
        )
        expectDecodingThrows(HybridSearchQuery.self, #"{"query":"x","vector":[1.0],"limit":1,"bogus":true}"#)
    }

    @Test func aggregateSpecOmitsFalseGroupBy() throws {
        // rust wire.rs AggregateSpec: groupBy omitted when false (client
        // convention; the server's #[serde(default)] accepts both forms).
        try expectEncodes(AggregateSpec(op: .sum), as: #"{"op":"sum"}"#)
        try expectEncodes(AggregateSpec(op: .avg, groupBy: true), as: #"{"op":"avg","groupBy":true}"#)
        expectDecodingThrows(AggregateSpec.self, #"{"op":"sum","zzz":1}"#)
        expectDecodingThrows(AggregateSpec.self, #"{"op":"median"}"#)
    }

    // MARK: - Query

    @Test func queryBareTableAndPointGet() throws {
        // rust query.rs bare_table_query / point_get.
        try expectEncodes(Query(table: "items"), as: #"{"table":"items"}"#)
        try expectEncodes(
            Query(table: "items", get: "abc"), as: #"{"table":"items","get":"abc"}"#
        )
    }

    @Test func queryIndexEqTerminals() throws {
        // rust query.rs index_eq_unique + corpus distinct fixture. Optional
        // fields and false bool terminals are omitted; empty eq is omitted.
        try expectEncodes(
            Query(table: "items", index: "by_project", eq: [.string("p1")], unique: true),
            as: #"{"table":"items","index":"by_project","eq":["p1"],"unique":true}"#
        )
        try expectEncodes(
            Query(
                table: "workItems", index: "by_project_and_status", eq: [.string("p1")],
                distinct: true
            ),
            as: #"{"table":"workItems","index":"by_project_and_status","eq":["p1"],"distinct":true}"#
        )
        let decoded = try decode(Query.self, #"{"table":"items","index":"by_project"}"#)
        #expect(decoded.eq.isEmpty)
        try expectEncodes(decoded, as: #"{"table":"items","index":"by_project"}"#)
    }

    @Test func queryRangeOrderTake() throws {
        let query = Query(
            table: "items", index: "by_order", gt: .int(5), order: .desc, take: 10
        )
        try expectEncodes(
            query, as: #"{"table":"items","index":"by_order","gt":5,"order":"desc","take":10}"#
        )
        #expect(try roundTrip(query) == query)
    }

    @Test func queryPaginateFixtures() throws {
        // Corpus queries paginate entries.
        try expectEncodes(
            Query(table: "workItems", paginate: Paginate(numItems: 10)),
            as: #"{"table":"workItems","paginate":{"numItems":10}}"#
        )
        try expectEncodes(
            Query(table: "workItems", paginate: Paginate(cursor: "abc", numItems: 10)),
            as: #"{"table":"workItems","paginate":{"cursor":"abc","numItems":10}}"#
        )
    }

    @Test func querySearchTerminalsRoundTrip() throws {
        // Corpus queries search/vectorSearch entries — camelCase wire keys.
        let vectorWithFilter = """
        {"table":"embeds","vectorSearch":{"index":"by_embedding","vector":[1.0],"limit":3,
        "filter":{"op":"eq","field":"userId","value":"u1"}}}
        """
        let fixtures = [
            #"{"table":"notes","search":{"index":"search_body","query":"hello world"}}"#,
            #"{"table":"notes","search":{"index":"search_body","query":"conv","mode":"trgm"}}"#,
            #"{"table":"embeds","vectorSearch":{"index":"by_embedding","vector":[1.0,0.5,-0.5],"limit":5}}"#,
            vectorWithFilter
        ]
        for json in fixtures {
            let query = try decode(Query.self, json)
            try expectEncodes(query, as: json)
            let text = try encodedText(query)
            if json.contains("vectorSearch") {
                #expect(text.contains(#""vectorSearch":"#))
                #expect(!text.contains("vector_search"))
            }
        }
    }

    @Test func queryRejectsUnknownField() {
        expectDecodingThrows(Query.self, #"{"table":"t","zzz":1}"#)
        expectDecodingThrows(Query.self, #"{"table":"t","take":5,"bogus":true}"#)
    }

    // MARK: - Step / Transaction

    @Test func stepOpsSerializeExactShapes() throws {
        // rust mutation.rs builder_serializes_all_step_kinds — 7 step kinds.
        let txn = Transaction(steps: [
            .insert(table: "items", doc: ["projectId": .string("p1"), "title": .string("a")]),
            .patch(table: "items", id: "i1", fields: ["title": .string("b")]),
            .replace(table: "items", id: "i4", doc: ["projectId": .string("p1"), "title": .string("c")]),
            .delete(table: "items", id: "i2"),
            .expectVersion(table: "items", id: "i3", version: 7),
            .expectAbsent(
                table: "items", index: "by_project_and_title",
                eq: [.string("p1"), .string("dup")]
            ),
            .upsert(
                table: "items", index: "by_project", eq: [.string("p1")],
                insert: ["projectId": .string("p1")], patch: ["title": .string("u")]
            )
        ])
        try expectEncodes(
            txn,
            as: """
            {"steps":[
            {"op":"insert","table":"items","doc":{"projectId":"p1","title":"a"}},
            {"op":"patch","table":"items","id":"i1","fields":{"title":"b"}},
            {"op":"replace","table":"items","id":"i4","doc":{"projectId":"p1","title":"c"}},
            {"op":"delete","table":"items","id":"i2"},
            {"op":"expectVersion","table":"items","id":"i3","version":7},
            {"op":"expectAbsent","table":"items","index":"by_project_and_title","eq":["p1","dup"]},
            {"op":"upsert","table":"items","index":"by_project","eq":["p1"],
            "insert":{"projectId":"p1"},"patch":{"title":"u"}}
            ]}
            """
        )
        #expect(try roundTrip(txn) == txn)
    }

    @Test func patchByQueryOmitsLimitWhenNil() throws {
        // rust mutation.rs patch_by_query_serializes / delete_by_query_with_limit.
        let patchTxn = Transaction(steps: [
            .patchByQuery(
                table: "items", filter: .eq(field: "status", value: .string("backlog")),
                patch: ["status": .string("done")], limit: nil
            )
        ])
        try expectEncodes(
            patchTxn,
            as: """
            {"steps":[{"op":"patchByQuery","table":"items",
            "filter":{"op":"eq","field":"status","value":"backlog"},"patch":{"status":"done"}}]}
            """
        )
        let deleteTxn = Transaction(steps: [
            .deleteByQuery(
                table: "items", filter: .eq(field: "status", value: .string("archived")), limit: 50
            )
        ])
        try expectEncodes(
            deleteTxn,
            as: """
            {"steps":[{"op":"deleteByQuery","table":"items",
            "filter":{"op":"eq","field":"status","value":"archived"},"limit":50}]}
            """
        )
    }

    @Test func scheduleStepsSerialize() throws {
        // rust mutation.rs schedule_and_cancel_schedule_serialize +
        // start_and_cancel_workflow_serialize + undelete fixtures.
        let scheduleTxn = Transaction(steps: [
            .schedule(
                when: .afterMs(ms: 60000),
                txn: Transaction(steps: [
                    .insert(table: "workItems", doc: ["title": .string("later")])
                ])
            ),
            .cancelSchedule(id: "j1")
        ])
        try expectEncodes(
            scheduleTxn,
            as: """
            {"steps":[
            {"op":"schedule","when":{"type":"afterMs","ms":60000},
            "txn":{"steps":[{"op":"insert","table":"workItems","doc":{"title":"later"}}]}},
            {"op":"cancelSchedule","id":"j1"}
            ]}
            """
        )
    }

    @Test func workflowAndUndeleteStepsSerialize() throws {
        // rust mutation.rs start_and_cancel_workflow_serialize + undelete.
        let workflowTxn = Transaction(steps: [
            .startWorkflow(
                spec: WorkflowSpec(
                    name: "drip",
                    steps: [
                        WorkflowStepSpec(
                            txn: Transaction(steps: [
                                .insert(table: "workItems", doc: ["title": .string("first")])
                            ])
                        ),
                        WorkflowStepSpec(
                            txn: Transaction(steps: []),
                            retry: StepRetry(maxAttempts: 5, initialRetryMs: 500, maxRetryMs: 2000),
                            sleepBeforeMs: 86_400_000
                        )
                    ]
                )
            ),
            .cancelWorkflow(id: "wf1")
        ])
        try expectEncodes(
            workflowTxn,
            as: """
            {"steps":[
            {"op":"startWorkflow","spec":{"name":"drip","steps":[
            {"txn":{"steps":[{"op":"insert","table":"workItems","doc":{"title":"first"}}]}},
            {"txn":{"steps":[]},"retry":{"maxAttempts":5,"initialRetryMs":500,"maxRetryMs":2000},
            "sleepBeforeMs":86400000}
            ]}},
            {"op":"cancelWorkflow","id":"wf1"}
            ]}
            """
        )

        let undeleteTxn = Transaction(steps: [.undelete(table: "projects", id: "p1")])
        try expectEncodes(
            undeleteTxn, as: #"{"steps":[{"op":"undelete","table":"projects","id":"p1"}]}"#
        )
    }

    @Test func stepRejectsUnknownFieldAndOp() {
        expectDecodingThrows(
            Step.self, #"{"op":"insert","table":"t","doc":{},"zzz":1}"#
        )
        expectDecodingThrows(Step.self, #"{"op":"merge","table":"t"}"#)
    }

    @Test func transactionRequiresSteps() {
        expectDecodingThrows(Transaction.self, #"{}"#)
        // client_messages mutate m4/m5/m6/m7/m8 corpus fixtures all decode.
        let nestedScheduleMutate = """
        {"type":"mutate","mutId":"m4","txn":{"steps":[{"op":"schedule",
        "when":{"type":"afterMs","ms":60000},
        "txn":{"steps":[{"op":"insert","table":"workItems","doc":{"title":"x"}}]}}]}}
        """
        let corpusMutations = [
            nestedScheduleMutate,
            #"{"type":"mutate","mutId":"m5","txn":{"steps":[{"op":"cancelSchedule","id":"0199ab_cd"}]}}"#,
            #"{"type":"mutate","mutId":"m7","txn":{"steps":[{"op":"cancelWorkflow","id":"wf-9"}]}}"#,
            #"{"type":"mutate","mutId":"m8","txn":{"steps":[{"op":"undelete","table":"tasks","id":"t-1"}]}}"#
        ]
        var decodedCount = 0
        for json in corpusMutations {
            if let msg = try? decode(ClientMessage.self, json), case .mutate = msg {
                decodedCount += 1
            }
        }
        #expect(decodedCount == corpusMutations.count)
    }

    // MARK: - StepResult

    @Test func stepResultDecodesInVariantOrder() throws {
        // rust mutation.rs: Upsert precedes Insert so {"id","inserted"} is not
        // greedily captured by Insert; {"id"} alone falls through to Insert.
        let upsert = try decode(StepResult.self, #"{"id":"x","inserted":true}"#)
        #expect(upsert == .upsert(id: "x", inserted: true))
        let patched = try decode(StepResult.self, #"{"id":"x","inserted":false}"#)
        #expect(patched == .upsert(id: "x", inserted: false))
        let plain = try decode(StepResult.self, #"{"id":"x"}"#)
        #expect(plain == .insert(id: "x"))
        // Extra keys are ignored by a matching variant (untagged serde parity).
        let extra = try decode(StepResult.self, #"{"id":"x","zzz":1}"#)
        #expect(extra == .insert(id: "x"))
    }

    @Test func stepResultDecodesAllShapes() throws {
        #expect(
            try decode(StepResult.self, #"{"patched":3,"truncated":false}"#)
                == .patchByQuery(patched: 3, truncated: false)
        )
        #expect(
            try decode(StepResult.self, #"{"deleted":1000,"truncated":true}"#)
                == .deleteByQuery(deleted: 1000, truncated: true)
        )
        #expect(
            try decode(StepResult.self, #"{"scheduleId":"s1"}"#) == .schedule(scheduleId: "s1")
        )
        #expect(
            try decode(StepResult.self, #"{"cancelled":true}"#) == .cancelled(cancelled: true)
        )
        #expect(try decode(StepResult.self, #"{"cancelled":false}"#) == .cancelled(cancelled: false))
        #expect(
            try decode(StepResult.self, #"{"workflowId":"wf9"}"#) == .workflowId(workflowId: "wf9")
        )
        #expect(try decode(StepResult.self, #"null"#) == StepResult.null)
    }

    @Test func stepResultRejectsPartialShapes() throws {
        // A variant fails when its required fields are missing/wrong-typed,
        // matching serde's untagged fall-through; nothing matches → error.
        expectDecodingThrows(StepResult.self, #"{"patched":3}"#)
        expectDecodingThrows(StepResult.self, #"{"id":5}"#)
        expectDecodingThrows(StepResult.self, #""hello""#)
        expectDecodingThrows(StepResult.self, #"5"#)
        // A wrong-typed field on an EARLIER variant falls through to a later
        // one that ignores the extra key (serde untagged parity).
        let fallen = try decode(StepResult.self, #"{"id":"x","inserted":"yes"}"#)
        #expect(fallen == .insert(id: "x"))
    }

    @Test func stepResultEncodes() throws {
        try expectEncodes(
            StepResult.upsert(id: "x", inserted: true), as: #"{"id":"x","inserted":true}"#
        )
        try expectEncodes(StepResult.insert(id: "x"), as: #"{"id":"x"}"#)
        // A bare null fragment is not an object — compare bytes directly.
        #expect(try encodedText(StepResult.null) == "null")
    }
}

/// Task 5 verification suite — the decode-side round-trips and scalar-enum
/// casing loops the Task 4 type drop left thin: every FilterExpr variant
/// decoded from pinned wire bytes (the leaf/nesting tests above pin encode
/// only), the schedule scalar domains' full case sets, and the
/// AggregateOp/SearchMode/OutcomeStatus wire strings.
struct WireEnumRoundTripTests {
    // MARK: - FilterExpr (decode side, every variant)

    @Test func filterExprEveryVariantRoundTrips() throws {
        // Payload shapes from rust-client/src/wire.rs::FilterExpr (and the
        // shipped Swift mirror): leaves carry {field, value}; `in` carries
        // {field, values}; and/or carry {exprs}; not carries {expr};
        // contains carries {field, value}; exists carries {field}.
        let cases: [(String, FilterExpr)] = [
            (#"{"op":"eq","field":"status","value":"done"}"#,
             .eq(field: "status", value: .string("done"))),
            (#"{"op":"neq","field":"status","value":"todo"}"#,
             .neq(field: "status", value: .string("todo"))),
            (#"{"op":"gt","field":"createdAt","value":1780000000000}"#,
             .gt(field: "createdAt", value: .int(1_780_000_000_000))),
            (#"{"op":"gte","field":"createdAt","value":1780000000000}"#,
             .gte(field: "createdAt", value: .int(1_780_000_000_000))),
            (#"{"op":"lt","field":"order","value":5}"#,
             .lt(field: "order", value: .int(5))),
            (#"{"op":"lte","field":"order","value":5}"#,
             .lte(field: "order", value: .int(5))),
            (#"{"op":"in","field":"status","values":["a","b"]}"#,
             .inValues(field: "status", values: [.string("a"), .string("b")])),
            (#"""
             {"op":"and","exprs":[{"op":"eq","field":"channel","value":"#general"},
             {"op":"gt","field":"createdAt","value":1780000000000}]}
             """#,
             .and(exprs: [
                 .eq(field: "channel", value: .string("#general")),
                 .gt(field: "createdAt", value: .int(1_780_000_000_000))
             ])),
            (#"""
             {"op":"or","exprs":[{"op":"in","field":"status","values":["a","b"]},
             {"op":"lte","field":"order","value":5}]}
             """#,
             .or(exprs: [
                 .inValues(field: "status", values: [.string("a"), .string("b")]),
                 .lte(field: "order", value: .int(5))
             ])),
            (#"{"op":"not","expr":{"op":"exists","field":"email"}}"#,
             .not(expr: .exists(field: "email"))),
            (#"{"op":"contains","field":"tags","value":"red"}"#,
             .contains(field: "tags", value: .string("red"))),
            (#"{"op":"exists","field":"email"}"#,
             .exists(field: "email"))
        ]
        for (json, expected) in cases {
            let decoded = try decode(FilterExpr.self, json)
            #expect(decoded == expected)
            try expectEncodes(decoded, as: json)
            #expect(try roundTrip(expected) == expected)
        }
    }

    // MARK: - Schedule scalars

    @Test func scheduleKindAndStatusWireStrings() throws {
        // Full case sets — the corpus fixtures exercise only oneshot/cron by
        // way of kinds and pending/error statuses; running/paused are wire
        // strings too (server protocol.rs ScheduleStatus).
        let kinds: [(ScheduleKind, String)] = [(.oneshot, "oneshot"), (.cron, "cron")]
        let statuses: [(ScheduleStatus, String)] = [
            (.pending, "pending"), (.running, "running"),
            (.paused, "paused"), (.error, "error")
        ]
        for (variant, wire) in kinds {
            #expect(variant.rawValue == wire)
            #expect(try roundTrip(variant) == variant)
        }
        for (variant, wire) in statuses {
            #expect(variant.rawValue == wire)
            #expect(try roundTrip(variant) == variant)
        }
        // Closed domains: the reject fixtures from scheduleInfoRejectsUnknown*
        // as bare strings.
        expectDecodingThrows(ScheduleKind.self, #""interval""#)
        expectDecodingThrows(ScheduleStatus.self, #""queued""#)
    }

    // MARK: - Query-terminal scalars

    @Test func aggregateSearchOutcomeEnumWireStrings() throws {
        // rawValue IS the wire string for every case (Task 3 casing-loop
        // pattern). AggregateOp min/max/count and OutcomeStatus had no
        // coverage at all before this suite.
        let aggregateOps: [(AggregateOp, String)] = [
            (.sum, "sum"), (.avg, "avg"), (.min, "min"), (.max, "max"), (.count, "count")
        ]
        let searchModes: [(SearchMode, String)] = [(.tsquery, "tsquery"), (.trgm, "trgm")]
        let outcomeStatuses: [(OutcomeStatus, String)] = [(.success, "success"), (.failed, "failed")]
        for (variant, wire) in aggregateOps {
            #expect(variant.rawValue == wire)
            #expect(try roundTrip(variant) == variant)
        }
        for (variant, wire) in searchModes {
            #expect(variant.rawValue == wire)
            #expect(try roundTrip(variant) == variant)
        }
        for (variant, wire) in outcomeStatuses {
            #expect(variant.rawValue == wire)
            #expect(try roundTrip(variant) == variant)
        }
        expectDecodingThrows(AggregateOp.self, #""median""#)
        expectDecodingThrows(SearchMode.self, #""bogus""#)
        // "cancelled" is a valid WorkflowStatus wire string but not an
        // OutcomeStatus — the closed domains must not bleed into each other.
        expectDecodingThrows(OutcomeStatus.self, #""cancelled""#)
    }

    @Test func stepOutcomeRoundTripsAndOmitsErrorWhenNil() throws {
        // StepOutcome is OutcomeStatus's wire carrier: camelCase, unknown
        // fields rejected, `error` omitted when nil (protocol.rs shape).
        let done = StepOutcome(stepIndex: 0, status: .success, attempts: 1, at: 123)
        try expectEncodes(done, as: #"{"stepIndex":0,"status":"success","attempts":1,"at":123}"#)
        #expect(try roundTrip(done) == done)

        let failed = StepOutcome(stepIndex: 1, status: .failed, attempts: 3, at: 456, error: "boom")
        try expectEncodes(
            failed, as: #"{"stepIndex":1,"status":"failed","attempts":3,"at":456,"error":"boom"}"#
        )
        #expect(try roundTrip(failed) == failed)

        expectDecodingThrows(StepOutcome.self, #"{"stepIndex":0,"status":"bogus","attempts":1,"at":1}"#)
        expectDecodingThrows(StepOutcome.self, #"{"stepIndex":0,"status":"success","attempts":1,"zzz":1}"#)
    }
}
