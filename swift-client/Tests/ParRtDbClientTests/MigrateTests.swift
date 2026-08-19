import Foundation
@testable import ParRtDbClient
import Testing

// Builder + wire tests for the Migration DSL — the Swift mirror of
// rust-client/src/migration.rs's test module, strengthened to full exact-JSON
// comparisons: every chain below encodes through `MigrateRequest` and must
// equal the expected wire object value-for-value (op tags, camelCase keys,
// the `where` alias, and the plain-Option `default` included).

/// Encode a Codable and re-decode as JSONValue for value-based comparison.
private func wire(_ value: some Codable) throws -> JSONValue {
    try JSONDecoder().decode(JSONValue.self, from: JSONEncoder().encode(value))
}

struct MigrateTests {
    @Test func builderEmitsAllDirectiveKinds() throws {
        let request = Migration()
            .renameField("users", from: "name", to: "fullName")
            .renameTable(from: "old", to: "new")
            .changeType("users", field: "age", to: .string, cast: .toString, default: .string("0"))
            .dropField("users", "legacy")
            .dropTable("gone")
            .dropIndex("users", "by_email")
            .setDefault("users", field: "role", value: .string("member"))
            .evalExpr("users", set: "upper", expr: "upper(doc->>'fullName')", where: "doc ? 'fullName'")
            .buildRequest()
        let expected: JSONValue = .object([
            "directives": .array([
                .object([
                    "op": .string("renameField"), "table": .string("users"),
                    "from": .string("name"), "to": .string("fullName")
                ]),
                .object([
                    "op": .string("renameTable"), "from": .string("old"), "to": .string("new")
                ]),
                .object([
                    "op": .string("changeType"), "table": .string("users"),
                    "field": .string("age"), "to": .object(["type": .string("string")]),
                    "cast": .string("toString"), "default": .string("0")
                ]),
                .object([
                    "op": .string("dropField"), "table": .string("users"), "field": .string("legacy")
                ]),
                .object(["op": .string("dropTable"), "name": .string("gone")]),
                .object([
                    "op": .string("dropIndex"), "table": .string("users"), "name": .string("by_email")
                ]),
                .object([
                    "op": .string("setDefault"), "table": .string("users"),
                    "field": .string("role"), "value": .string("member")
                ]),
                .object([
                    "op": .string("evalExpr"), "table": .string("users"), "set": .string("upper"),
                    "expr": .string("upper(doc->>'fullName')"), "where": .string("doc ? 'fullName'")
                ])
            ]),
            "dryRun": .bool(false)
        ])
        #expect(try wire(request) == expected)
    }

    @Test func dryRunFlagSurfacesOnBuildRequest() throws {
        let request = Migration()
            .dryRun(true)
            .renameField("users", from: "name", to: "fullName")
            .buildRequest()
        #expect(try wire(request).objectValue?["dryRun"] == .bool(true))
    }

    @Test func buildReturnsDirectivesOnly() {
        let directives = Migration().dryRun(true).dropTable("gone").build()
        #expect(directives == [.dropTable(name: "gone")])
    }

    /// rust's plain `Option<serde_json::Value>` on `changeType.default`
    /// serializes None as JSON null (only `evalExpr.where` is omitted when
    /// absent) — pin that parity.
    @Test func changeTypeSerializesNullDefaultWhenOmitted() throws {
        let request = Migration()
            .changeType("users", field: "age", to: .number, cast: .toNumber)
            .buildRequest()
        let directive = try wire(request).objectValue?["directives"]
        if case let .array(items)? = directive {
            #expect(items.count == 1)
            #expect(items[0].objectValue?["default"] == .null)
        } else {
            Issue.record("directives should be an array, got \(String(describing: directive))")
        }
    }

    @Test func evalExprTypedBuildsTypedSources() throws {
        let request = Migration()
            .evalExprTyped(
                "users",
                set: "band",
                expr: .caseExpr(
                    whens: [
                        CaseWhen(
                            when: .lt(field: "score", value: .int(10)),
                            then: .literal(value: .string("low"))
                        )
                    ],
                    otherwise: .coalesce(parts: [
                        .field(field: "score"), .literal(value: .int(0))
                    ])
                ),
                where: .exists(field: "score")
            )
            .buildRequest()
        let directive = try wire(request).objectValue?["directives"]
        guard case let .array(items)? = directive, items.count == 1 else {
            Issue.record("expected one directive, got \(String(describing: directive))")
            return
        }
        let expected: JSONValue = .object([
            "op": .string("evalExpr"), "table": .string("users"), "set": .string("band"),
            "expr": .object([
                "op": .string("case"),
                "whens": .array([
                    .object([
                        "when": .object([
                            "op": .string("lt"), "field": .string("score"), "value": .int(10)
                        ]),
                        "then": .object([
                            "op": .string("literal"), "value": .string("low")
                        ])
                    ])
                ]),
                "otherwise": .object([
                    "op": .string("coalesce"),
                    "parts": .array([
                        .object(["op": .string("field"), "field": .string("score")]),
                        .object(["op": .string("literal"), "value": .int(0)])
                    ])
                ])
            ]),
            "where": .object(["op": .string("exists"), "field": .string("score")])
        ])
        #expect(items[0] == expected)
    }

    /// The untagged dual-accept sources: a wire string is legacy, an object is
    /// its typed arm, and a hostile object that is neither fails BOTH arms
    /// (it must not silently become legacy).
    @Test func exprSourceDecodesLegacyAndTypedArms() throws {
        let legacy = try JSONDecoder().decode(
            ExprSource.self, from: Data(#""upper(doc->>'x')""#.utf8)
        )
        #expect(legacy == .legacy("upper(doc->>'x')"))

        let typed = try JSONDecoder().decode(
            ExprSource.self,
            from: Data(#"{"op":"upper","value":{"op":"field","field":"x"}}"#.utf8)
        )
        #expect(
            typed == .typed(.upper(value: .field(field: "x")))
        )

        #expect(throws: DecodingError.self) {
            _ = try JSONDecoder().decode(
                ExprSource.self, from: Data(#"{"op":"subquery","sql":"1=1"}"#.utf8)
            )
        }
        #expect(throws: DecodingError.self) {
            _ = try JSONDecoder().decode(
                CondSource.self, from: Data(#"{"op":"bogus"}"#.utf8)
            )
        }
    }

    /// Round-trip through the typed arm re-encodes as the object form, and
    /// the legacy arm as the bare string — serde untagged parity.
    @Test func untaggedSourcesRoundTrip() throws {
        #expect(
            try wire(ExprSource.typed(.now))
                == .object(["op": .string("now")])
        )
        #expect(try wire(CondSource.legacy("doc ? 'x'")) == .string("doc ? 'x'"))
    }

    /// deny_unknown_fields parity: an unknown key on a matched variant is a
    /// decode error (the same shape contract as the mutation `Step`).
    @Test func directiveRejectsUnknownVariantField() throws {
        #expect(throws: DecodingError.self) {
            _ = try JSONDecoder().decode(
                Directive.self,
                from: Data(#"{"op":"dropTable","name":"gone","cascade":true}"#.utf8)
            )
        }
        #expect(throws: DecodingError.self) {
            _ = try JSONDecoder().decode(
                ValueExpr.self,
                from: Data(#"{"op":"now","tz":"utc"}"#.utf8)
            )
        }
    }
}
