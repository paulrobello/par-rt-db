import Foundation
@testable import ParRtDbClient
import Testing

// Task 6 — MutationLimits contract constant + Step/StepResult wire smoke.
// MutationLimits.maxSteps mirrors server/src/txn.rs MAX_STEPS (1024) and
// rust-client/src/in_memory/mod.rs MAX_STEPS (1024); the wire-corpus
// protocol_constants.max_steps assertion (Task 7) pins this number.
//
// Task 9 — MutationBuilderTests: the fluent MutationBuilder. Every wire-shape
// assertion is whole-object against the rust-client builder fixtures
// (rust-client/src/mutation.rs tests), so a stray key fails the test.

// MARK: - Helpers

private func roundTrip<T: Codable & Equatable>(_ value: T) throws -> T {
    try JSONDecoder().decode(T.self, from: JSONEncoder().encode(value))
}

// MARK: - MutationLimits

struct MutationTests {
    @Test func maxStepsMatchesRustAndServer() {
        #expect(MutationLimits.maxSteps == 1024)
    }

    @Test func stepOpTagsAreCamelCase() throws {
        // Exact payload from the shipped Step type: `filter` is a required
        // FilterExpr (the brief's `filter: nil` sketch predates the shipped
        // shape), `limit` is optional.
        let step = Step.patchByQuery(
            table: "t",
            filter: .eq(field: "status", value: .string("open")),
            patch: ["done": .bool(true)],
            limit: 5
        )
        let text = try String(data: JSONEncoder().encode(step), encoding: .utf8) ?? ""
        #expect(text.contains(#""op":"patchByQuery""#))
        #expect(try roundTrip(step) == step)
    }

    @Test func stepResultUntaggedOrder() throws {
        // `{"id":"x"}` alone decodes as .insert — upsert requires `inserted`.
        let insert = try JSONDecoder().decode(
            StepResult.self, from: Data(#"{"id":"x"}"#.utf8)
        )
        #expect(insert == .insert(id: "x"))

        // `{"id","inserted"}` decodes as .upsert even though `.insert` would
        // also match on `id` alone (extra keys ignored, serde untagged parity)
        // — the upsert-before-insert declaration order is load-bearing.
        let upsert = try JSONDecoder().decode(
            StepResult.self, from: Data(#"{"id":"x","inserted":true}"#.utf8)
        )
        #expect(upsert == .upsert(id: "x", inserted: true))

        // `null` (patch/delete/expect*/undelete results) round-trips.
        #expect(try roundTrip(StepResult.null) == .null)
    }
}

// MARK: - MutationBuilder

/// Task 9 — the fluent `MutationBuilder`. Wire shapes mirror the rust-client
/// builder fixtures one-to-one; whole-object equality so a stray key fails.
struct MutationBuilderTests {
    @Test func threeStepChainBuildsExactShape() throws {
        // The brief's chained fixture, with the shipped wire key: patch carries
        // `fields` (rust: {"op":"patch",...,"fields":{...}}), not `doc`.
        let txn = try MutationBuilder()
            .insert("users", ["email": .string("a@b.c")])
            .patch("counters", "c1", ["n": .int(1)])
            .delete("sessions", "s9")
            .build()
        #expect(txn.steps.count == 3)
        #expect(try txn.wireObject() == ["steps": .array([
            .object([
                "op": .string("insert"),
                "table": .string("users"),
                "doc": .object(["email": .string("a@b.c")])
            ]),
            .object([
                "op": .string("patch"),
                "table": .string("counters"),
                "id": .string("c1"),
                "fields": .object(["n": .int(1)])
            ]),
            .object([
                "op": .string("delete"),
                "table": .string("sessions"),
                "id": .string("s9")
            ])
        ])])
    }

    // swiftlint:disable:next function_body_length
    @Test func builderSerializesAllStepKinds() throws {
        // Mirrors rust `builder_serializes_all_step_kinds` fixture 1:1.
        let txn = try MutationBuilder()
            .insert("items", ["projectId": .string("p1"), "title": .string("a")])
            .patch("items", "i1", ["title": .string("b")])
            .replace("items", "i4", ["projectId": .string("p1"), "title": .string("c")])
            .delete("items", "i2")
            .expectVersion("items", "i3", 7)
            .expectAbsent("items", "by_project_and_title", [.string("p1"), .string("dup")])
            .upsert(
                "items", index: "by_project", eq: [.string("p1")],
                insert: ["projectId": .string("p1")], patch: ["title": .string("u")]
            )
            .build()
        #expect(try txn.wireObject() == ["steps": .array([
            .object([
                "op": .string("insert"),
                "table": .string("items"),
                "doc": .object(["projectId": .string("p1"), "title": .string("a")])
            ]),
            .object([
                "op": .string("patch"),
                "table": .string("items"),
                "id": .string("i1"),
                "fields": .object(["title": .string("b")])
            ]),
            .object([
                "op": .string("replace"),
                "table": .string("items"),
                "id": .string("i4"),
                "doc": .object(["projectId": .string("p1"), "title": .string("c")])
            ]),
            .object([
                "op": .string("delete"),
                "table": .string("items"),
                "id": .string("i2")
            ]),
            .object([
                "op": .string("expectVersion"),
                "table": .string("items"),
                "id": .string("i3"),
                "version": .int(7)
            ]),
            .object([
                "op": .string("expectAbsent"),
                "table": .string("items"),
                "index": .string("by_project_and_title"),
                "eq": .array([.string("p1"), .string("dup")])
            ]),
            .object([
                "op": .string("upsert"),
                "table": .string("items"),
                "index": .string("by_project"),
                "eq": .array([.string("p1")]),
                "insert": .object(["projectId": .string("p1")]),
                "patch": .object(["title": .string("u")])
            ])
        ])])
    }

    @Test func patchByQueryOmitsLimitWhenNil() throws {
        // Mirrors rust `patch_by_query_serializes`: `limit` omitted when None.
        let txn = try MutationBuilder()
            .patchByQuery(
                "items",
                filter: .eq(field: "status", value: .string("backlog")),
                patch: ["status": .string("done")]
            )
            .build()
        #expect(try txn.wireObject() == ["steps": .array([
            .object([
                "op": .string("patchByQuery"),
                "table": .string("items"),
                "filter": .object([
                    "op": .string("eq"),
                    "field": .string("status"),
                    "value": .string("backlog")
                ]),
                "patch": .object(["status": .string("done")])
            ])
        ])])
    }

    @Test func deleteByQuerySerializesLimit() throws {
        // Mirrors rust `delete_by_query_serializes_with_limit`.
        let txn = try MutationBuilder()
            .deleteByQuery(
                "items", filter: .eq(field: "status", value: .string("archived")), limit: 50
            )
            .build()
        #expect(try txn.wireObject() == ["steps": .array([
            .object([
                "op": .string("deleteByQuery"),
                "table": .string("items"),
                "filter": .object([
                    "op": .string("eq"),
                    "field": .string("status"),
                    "value": .string("archived")
                ]),
                "limit": .int(50)
            ])
        ])])
    }

    @Test func scheduleAndCancelScheduleSerialize() throws {
        // Mirrors rust `schedule_and_cancel_schedule_serialize`; the nested txn
        // is itself built with the builder.
        let nested = try MutationBuilder()
            .insert("workItems", ["title": .string("later")])
            .build()
        let txn = try MutationBuilder()
            .schedule(.afterMs(ms: 60000), nested)
            .cancelSchedule("j1")
            .build()
        #expect(try txn.wireObject() == ["steps": .array([
            .object([
                "op": .string("schedule"),
                "when": .object(["type": .string("afterMs"), "ms": .int(60000)]),
                "txn": .object(["steps": .array([
                    .object([
                        "op": .string("insert"),
                        "table": .string("workItems"),
                        "doc": .object(["title": .string("later")])
                    ])
                ])])
            ]),
            .object([
                "op": .string("cancelSchedule"),
                "id": .string("j1")
            ])
        ])])
    }

    @Test func startAndCancelWorkflowSerialize() throws {
        // Mirrors rust `start_and_cancel_workflow_serialize`.
        let spec = try WorkflowSpec(name: "drip", steps: [
            WorkflowStepSpec(
                txn: MutationBuilder()
                    .insert("workItems", ["title": .string("first")])
                    .build()
            ),
            WorkflowStepSpec(
                txn: Transaction(steps: []),
                retry: StepRetry(maxAttempts: 5, initialRetryMs: 500, maxRetryMs: 2000),
                sleepBeforeMs: 86_400_000
            )
        ])
        let txn = try MutationBuilder()
            .startWorkflow(spec)
            .cancelWorkflow("wf1")
            .build()
        #expect(try txn.wireObject() == ["steps": .array([
            .object([
                "op": .string("startWorkflow"),
                "spec": .object([
                    "name": .string("drip"),
                    "steps": .array([
                        .object(["txn": .object(["steps": .array([
                            .object([
                                "op": .string("insert"),
                                "table": .string("workItems"),
                                "doc": .object(["title": .string("first")])
                            ])
                        ])])]),
                        .object([
                            "txn": .object(["steps": .array([])]),
                            "retry": .object([
                                "maxAttempts": .int(5),
                                "initialRetryMs": .int(500),
                                "maxRetryMs": .int(2000)
                            ]),
                            "sleepBeforeMs": .int(86_400_000)
                        ])
                    ])
                ])
            ]),
            .object([
                "op": .string("cancelWorkflow"),
                "id": .string("wf1")
            ])
        ])])
    }

    @Test func undeleteSerializesAndRoundTrips() throws {
        // Mirrors rust `undelete_serializes_and_round_trips` — the same wire
        // shape as delete.
        let txn = try MutationBuilder().undelete("projects", "p1").build()
        #expect(try txn.wireObject() == ["steps": .array([
            .object([
                "op": .string("undelete"),
                "table": .string("projects"),
                "id": .string("p1")
            ])
        ])])
        #expect(try roundTrip(txn) == txn)
    }

    // MARK: - maxSteps cap (server count_steps port)

    @Test func buildAcceptsExactlyMaxSteps() throws {
        var builder = MutationBuilder()
        for _ in 0 ..< MutationLimits.maxSteps {
            builder = builder.insert("t", [:])
        }
        let txn = try builder.build()
        #expect(txn.steps.count == MutationLimits.maxSteps)
    }

    @Test func buildRejectsOverMaxSteps() {
        // Value-semantics chaining: each insert returns a NEW builder, so the
        // loop must reassign (the brief's discarded-result loop predates the
        // value-semantics requirement).
        var builder = MutationBuilder()
        for _ in 0 ... MutationLimits.maxSteps { // maxSteps + 1 steps
            builder = builder.insert("t", [:])
        }
        let error = buildError(builder)
        #expect(error?.code == .badRequest)
        #expect(
            error?.message
                == "transaction exceeds maximum of \(MutationLimits.maxSteps) steps "
                + "(counted recursively, including scheduled txns)"
        )
    }

    @Test func buildCountsScheduledAndWorkflowStepsRecursively() throws {
        // Server count_steps: schedule = 1 + nested txn; startWorkflow = 1 +
        // every spec step's txn. The builder's cap uses the same recursive
        // total, not the flat top-level count.
        let nested = try MutationBuilder()
            .insert("t", [:])
            .delete("t", "x")
            .build() // 2 steps
        var base = MutationBuilder()
        for _ in 0 ..< MutationLimits.maxSteps - 3 {
            base = base.insert("t", [:])
        }
        // (maxSteps - 3) flat + 1 schedule + 2 nested = maxSteps — legal.
        let legal = try base.schedule(.afterMs(ms: 1), nested).build()
        #expect(legal.steps.count == MutationLimits.maxSteps - 2)
        // One more flat step tips the recursive total to maxSteps + 1.
        let over = base.schedule(.afterMs(ms: 1), nested).insert("t", [:])
        #expect(buildError(over)?.code == .badRequest)
    }

    @Test func countStepsMirrorsServerRecursion() {
        let spec = WorkflowSpec(name: "w", steps: [
            WorkflowStepSpec(txn: Transaction(steps: [
                .delete(table: "t", id: "a"), .delete(table: "t", id: "b")
            ])),
            WorkflowStepSpec(txn: Transaction(steps: []))
        ])
        let txn = Transaction(steps: [.startWorkflow(spec: spec), .cancelSchedule(id: "j")])
        // cancelSchedule (1) + startWorkflow (1 + 2 nested + 0 nested) = 4.
        #expect(countSteps(txn) == 4)
    }

    // MARK: - Value semantics + numeric conversion

    @Test func chainingHasValueSemantics() throws {
        let base = MutationBuilder().insert("t", ["k": .int(1)])
        let left = try base.delete("t", "a").build()
        let right = try base.undelete("t", "a").build()
        #expect(left.steps.count == 2)
        #expect(right.steps.count == 2)
        #expect(left.steps[1] == .delete(table: "t", id: "a"))
        #expect(right.steps[1] == .undelete(table: "t", id: "a"))
        // The original builder is unchanged — no shared mutable state.
        #expect(try base.build().steps.count == 1)
    }

    @Test func buildRejectsOutOfRangeByQueryLimits() {
        let negative = MutationBuilder()
            .patchByQuery("t", filter: .exists(field: "x"), patch: [:], limit: -1)
        let error = buildError(negative)
        #expect(error?.code == .badRequest)
        #expect(
            error?.message
                == "patchByQuery limit must be a non-negative 32-bit integer, got -1"
        )
        let overflow = MutationBuilder()
            .deleteByQuery("t", filter: .exists(field: "x"), limit: Int(UInt32.max) + 1)
        #expect(
            buildError(overflow)?.message
                == "deleteByQuery limit must be a non-negative 32-bit integer, got 4294967296"
        )
    }

    @Test func byQueryLimitAcceptsUInt32Max() throws {
        let txn = try MutationBuilder()
            .deleteByQuery("t", filter: .exists(field: "x"), limit: Int(UInt32.max))
            .build()
        #expect(
            txn.steps[0]
                == .deleteByQuery(table: "t", filter: .exists(field: "x"), limit: UInt32.max)
        )
    }
}

private func buildError(_ builder: MutationBuilder) -> RtDbError? {
    do {
        _ = try builder.build()
        return nil
    } catch let error as RtDbError {
        return error
    } catch {
        return nil
    }
}
