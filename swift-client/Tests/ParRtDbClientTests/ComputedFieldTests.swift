import Foundation
@testable import ParRtDbClient
import Testing

// ENH-028 computed fields — the swift-client mirror of Task 1/3's server
// tests: interpreter semantics-table edges (null propagation, trim-spaces-only,
// cast error paths, the ToBoolean word set), write-path stamping (client
// values dropped before validation, null removes the key, eval errors fail
// BAD_REQUEST naming the field), the `.computed` DSL + wire shape, push
// validation's six rules, and the migrate interplay (rename rewrite, dropField
// rejection, changeType re-validation).

/// Encode a Codable and re-decode as JSONValue for value-based comparison
/// (MigrateTests' `wire` helper).
private func wire(_ value: some Codable) throws -> JSONValue {
    try JSONDecoder().decode(JSONValue.self, from: JSONEncoder().encode(value))
}

/// Pinned clock (fixed now) + pinned RNG.
private func fixedClient() -> InMemoryRtDbClient {
    InMemoryRtDbClient(options: InMemoryRtDbClientOptions(now: { 1_700_000_000_000 }, random: { 0 }))
}

/// Monotonically increasing clock so later stamps are strictly greater.
private func monotonicClient() -> InMemoryRtDbClient {
    let clock = MonotonicMs(1_700_000_000_000)
    return InMemoryRtDbClient(options: InMemoryRtDbClientOptions(
        now: { clock.next() },
        random: { 0 }
    ))
}

private func insert(
    _ client: InMemoryRtDbClient, table: String, doc: [String: JSONValue]
) throws -> String {
    let results = try client.mutate(Transaction(steps: [.insert(table: table, doc: doc)]))
    guard case let .insert(id) = results[0] else {
        throw RtDbError(code: .internal, message: "expected insert result")
    }
    return id
}

private func get(_ client: InMemoryRtDbClient, table: String, _ id: String) throws -> JSONValue {
    try client.query(Query(table: table, get: id))
}

private func expectPushError(_ schema: SchemaDef, _ fragment: String) throws {
    do {
        try fixedClient().pushSchema(schema)
        Issue.record("expected pushSchema failure containing '\(fragment)'")
    } catch let error as RtDbError {
        #expect(error.code == .badRequest, "computed push errors are BAD_REQUEST (got \(error.code))")
        #expect(error.message.contains(fragment))
    }
}

private func expectMutateError(
    _ client: InMemoryRtDbClient, _ txn: Transaction, _ fragment: String
) throws {
    do {
        _ = try client.mutate(txn)
        Issue.record("expected mutate failure containing '\(fragment)'")
    } catch let error as RtDbError {
        #expect(error.code == .badRequest, "computed eval errors are BAD_REQUEST (got \(error.code))")
        #expect(error.message.contains(fragment))
    }
}

private func fullNameSchema() throws -> SchemaDef {
    try SchemaBuilder()
        .table("users") {
            $0.field("first", .string)
                .field("last", .string)
                .field("fullName", .string)
                .index("by_fullName", on: ["fullName"])
                .computed(
                    "fullName",
                    .concat(parts: [
                        .field(field: "first"),
                        .literal(value: .string(" ")),
                        .field(field: "last")
                    ])
                )
        }
        .build()
}

struct ComputedFieldTests {
    // MARK: - Interpreter (semantics table)

    private let fields: FieldMap = [:]

    private func eval(
        _ ve: ValueExpr, _ doc: [String: JSONValue], now: Int64 = 0
    ) throws -> JSONValue {
        try evalValueExpr(ve, doc, now, fields)
    }

    private func evalError(
        _ ve: ValueExpr, _ doc: [String: JSONValue]
    ) throws -> RtDbError {
        do {
            _ = try evalValueExpr(ve, doc, 0, fields)
            throw RtDbError(code: .internal, message: "expected eval failure")
        } catch let error as RtDbError {
            return error
        }
    }

    @Test func fieldReadsAreTextAndAbsentIsNull() throws {
        let doc: [String: JSONValue] = [
            "s": .string("x"),
            "n": .int(42),
            "f": .double(42.5),
            "b": .bool(true),
            "o": .object(["a": .int(1)]),
            "nil": .null
        ]
        #expect(try eval(.field(field: "s"), doc) == .string("x"))
        #expect(try eval(.field(field: "n"), doc) == .string("42"))
        #expect(try eval(.field(field: "f"), doc) == .string("42.5"))
        #expect(try eval(.field(field: "b"), doc) == .string("true"))
        // Objects use compact JSON text (the pinned convention).
        #expect(try eval(.field(field: "o"), doc) == .string(#"{"a":1}"#))
        #expect(try eval(.field(field: "nil"), doc) == .null)
        #expect(try eval(.field(field: "missing"), doc) == .null)
    }

    @Test func literalPassesThrough() throws {
        for value: JSONValue in [
            .string("s"), .int(42), .double(42.5), .bool(true),
            .object(["a": .array([.int(1), .int(2)])]), .null
        ] {
            #expect(try eval(.literal(value: value), [:]) == value)
        }
    }

    @Test func concatSkipsNullsAndCastsNumbersToText() throws {
        let doc: [String: JSONValue] = ["first": .string("Ada"), "n": .int(42)]
        let expr = ValueExpr.concat(parts: [
            .field(field: "first"),
            .field(field: "missing"),
            .field(field: "n")
        ])
        #expect(try eval(expr, doc) == .string("Ada42"))
    }

    @Test func concatAllNullPartsIsEmptyString() throws {
        let expr = ValueExpr.concat(parts: [.field(field: "missing"), .literal(value: .null)])
        #expect(try eval(expr, [:]) == .string(""))
    }

    @Test func addCoercesStringFieldsToNumeric() throws {
        let doc: [String: JSONValue] = ["a": .string("42"), "b": .string("1")]
        #expect(try eval(.add(left: .field(field: "a"), right: .field(field: "b")), doc) == .int(43))
    }

    @Test func arithmeticPropagatesNullOverOperandsAndZeroDivisor() throws {
        let missing = ValueExpr.field(field: "missing")
        let one = ValueExpr.literal(value: .int(1))
        #expect(try eval(.add(left: missing, right: one), [:]) == .null)
        #expect(try eval(.sub(left: one, right: missing), [:]) == .null)
        #expect(try eval(.mul(left: missing, right: one), [:]) == .null)
        #expect(try eval(.div(left: one, right: missing), [:]) == .null)
        // Null precedes the zero check: null / 0 is null, not an error.
        #expect(
            try eval(.div(left: missing, right: .literal(value: .int(0))), [:]) == .null
        )
    }

    @Test func divisionByZeroErrors() throws {
        let one = ValueExpr.literal(value: .int(1))
        #expect(
            try evalError(.div(left: one, right: .literal(value: .int(0))), [:]).message
                == "division by zero"
        )
        // -0.0 is IEEE-equal to 0 — the same divisor error.
        #expect(
            try evalError(.div(left: one, right: .literal(value: .double(-0.0))), [:]).message
                == "division by zero"
        )
    }

    @Test func nonFiniteResultErrors() throws {
        let expr = ValueExpr.div(
            left: .literal(value: .double(1e308)),
            right: .literal(value: .double(1e-10))
        )
        #expect(try evalError(expr, [:]).message == "numeric result is not finite")
    }

    @Test func divisionHappyPathIsNumeric() throws {
        let expr = ValueExpr.div(
            left: .literal(value: .int(1)),
            right: .literal(value: .int(4))
        )
        #expect(try eval(expr, [:]) == .double(0.25))
    }

    @Test func coalesceReturnsFirstNonNullElseNull() throws {
        let firstMissing = ValueExpr.coalesce(parts: [
            .field(field: "missing"), .literal(value: .int(7))
        ])
        #expect(try eval(firstMissing, [:]) == .int(7))
        let allMissing = ValueExpr.coalesce(parts: [.field(field: "a"), .field(field: "b")])
        #expect(try eval(allMissing, [:]) == .null)
    }

    @Test func lowerUpperTrimTrimSpacesOnly() throws {
        let doc: [String: JSONValue] = [
            "mixed": .string("MiXeD"),
            "padded": .string("  x  "),
            "tabbed": .string("  \t x  ")
        ]
        #expect(try eval(.lower(value: .field(field: "mixed")), doc) == .string("mixed"))
        #expect(try eval(.upper(value: .field(field: "mixed")), doc) == .string("MIXED"))
        #expect(try eval(.trim(value: .field(field: "padded")), doc) == .string("x"))
        // Spaces only — the leading tab survives btrim's default.
        #expect(try eval(.trim(value: .field(field: "tabbed")), doc) == .string("\t x"))
        #expect(try eval(.lower(value: .field(field: "missing")), doc) == .null)
    }

    @Test func castToStringUsesTextExtraction() throws {
        let doc: [String: JSONValue] = [
            "n": .int(42),
            "o": .object(["a": .int(1)])
        ]
        #expect(try eval(.cast(value: .field(field: "n"), to: .toString), doc) == .string("42"))
        #expect(
            try eval(.cast(value: .field(field: "o"), to: .toString), doc) == .string(#"{"a":1}"#)
        )
        #expect(try eval(.cast(value: .field(field: "missing"), to: .toString), doc) == .null)
    }

    @Test func castToNumberParsesTrimmedStrings() throws {
        let doc: [String: JSONValue] = [
            "s": .string("  3.5 "),
            "bad": .string("abc")
        ]
        #expect(try eval(.cast(value: .field(field: "s"), to: .toNumber), doc) == .double(3.5))
        #expect(
            try evalError(.cast(value: .field(field: "bad"), to: .toNumber), doc).message
                .contains("cannot cast")
        )
        // A bool literal hits the type-error arm (a bool FIELD reaches the cast
        // as its text form "true", which fails the string parse instead).
        #expect(
            try evalError(.cast(value: .literal(value: .bool(true)), to: .toNumber), doc).message
                == "cannot cast to number"
        )
        #expect(try eval(.cast(value: .field(field: "missing"), to: .toNumber), doc) == .null)
    }

    @Test func castToInt64RejectsFloatPayloads() throws {
        let doc: [String: JSONValue] = [
            "i": .int(42),
            "float": .double(3.5),
            "wholeFloat": .double(3.0),
            "s": .string("  7 "),
            "bad": .string("8x")
        ]
        #expect(try eval(.cast(value: .field(field: "i"), to: .toInt64), doc) == .int(42))
        #expect(try eval(.cast(value: .field(field: "s"), to: .toInt64), doc) == .int(7))
        // A float payload is not integral EVEN when mathematically whole — a
        // float LITERAL hits the numeric arm directly. (A whole-float FIELD
        // reaches the cast as its text form "3" — the JS number-string
        // convention — and parses, mirroring the ts engine.)
        #expect(try evalError(.cast(value: .field(field: "float"), to: .toInt64), doc).code == .badRequest)
        #expect(
            try evalError(.cast(value: .literal(value: .double(3.0)), to: .toInt64), doc).code
                == .badRequest
        )
        #expect(try evalError(.cast(value: .field(field: "bad"), to: .toInt64), doc).code == .badRequest)
        #expect(
            try evalError(.cast(value: .literal(value: .bool(true)), to: .toInt64), doc).message
                == "cannot cast to int64"
        )
        #expect(try eval(.cast(value: .field(field: "missing"), to: .toInt64), doc) == .null)
    }

    @Test func castToBooleanAcceptsPostgresLiteralSet() throws {
        let doc: [String: JSONValue] = ["b": .bool(true), "two": .int(2)]
        #expect(try eval(.cast(value: .field(field: "b"), to: .toBoolean), doc) == .bool(true))
        #expect(try eval(.cast(value: .literal(value: .int(1)), to: .toBoolean), doc) == .bool(true))
        #expect(try eval(.cast(value: .literal(value: .int(0)), to: .toBoolean), doc) == .bool(false))
        for (word, want) in [
            ("TRUE", true), ("t", true), ("Yes", true), ("on", true), ("1", true),
            ("False", false), ("f", false), ("No", false), ("OFF", false), ("0", false)
        ] {
            #expect(
                try eval(.cast(value: .literal(value: .string(word)), to: .toBoolean), doc)
                    == .bool(want),
                "word \(word)"
            )
        }
        #expect(
            try evalError(.cast(value: .literal(value: .string("maybe")), to: .toBoolean), doc).code
                == .badRequest
        )
        // A number field reaches the cast as its TEXT form ("2" parses), but a
        // number LITERAL hits the numeric 1/0-only arm.
        #expect(
            try evalError(.cast(value: .literal(value: .int(2)), to: .toBoolean), doc).code
                == .badRequest
        )
        #expect(try eval(.cast(value: .field(field: "missing"), to: .toBoolean), doc) == .null)
    }

    @Test func nowYieldsEpochMsAsNumber() throws {
        #expect(try eval(.now, [:], now: 1_234_567_890) == .int(1_234_567_890))
    }

    @Test func caseTakesFirstMatchThenOtherwise() throws {
        let doc: [String: JSONValue] = ["status": .string("admin"), "n": .int(5)]
        let matched = ValueExpr.caseExpr(
            whens: [
                CaseWhen(
                    when: .eq(field: "status", value: .string("user")),
                    then: .literal(value: .int(1))
                ),
                CaseWhen(
                    when: .eq(field: "status", value: .string("admin")),
                    then: .literal(value: .int(2))
                )
            ],
            otherwise: .literal(value: .int(4))
        )
        #expect(try eval(matched, doc) == .int(2))

        let otherwise = ValueExpr.caseExpr(
            whens: [
                CaseWhen(
                    when: .gt(field: "n", value: .int(10)),
                    then: .literal(value: .int(3))
                )
            ],
            otherwise: .field(field: "status")
        )
        #expect(try eval(otherwise, doc) == .string("admin"))
    }

    @Test func walkVisitsFieldsAndCaseWhenFields() {
        let expr = ValueExpr.concat(parts: [
            .field(field: "a"),
            .caseExpr(
                whens: [
                    CaseWhen(
                        when: .and(exprs: [
                            .eq(field: "b", value: .int(1)),
                            .not(expr: .contains(field: "c", value: .string("x")))
                        ]),
                        then: .field(field: "d")
                    ),
                    CaseWhen(when: .exists(field: "e"), then: .literal(value: .int(1)))
                ],
                otherwise: .field(field: "f")
            ),
            .add(
                left: .field(field: "g"),
                right: .div(
                    left: .field(field: "h"),
                    right: .coalesce(parts: [.field(field: "i")])
                )
            )
        ])
        var seen: Set<String> = []
        walkValueExprFields(expr) { seen.insert($0) }
        #expect(seen == ["a", "b", "c", "d", "e", "f", "g", "h", "i"])
    }

    // MARK: - Write-path stamping

    @Test func insertOverwritesClientSuppliedComputedValue() throws {
        let client = fixedClient()
        try client.pushSchema(fullNameSchema())
        let id = try insert(
            client, table: "users",
            doc: ["first": .string("Ada"), "last": .string("Lovelace"), "fullName": .string("WRONG")]
        )
        #expect(try get(client, table: "users", id).objectValue?["fullName"] == .string("Ada Lovelace"))
    }

    @Test func patchRecomputesFromMergedDoc() throws {
        let client = fixedClient()
        try client.pushSchema(fullNameSchema())
        let id = try insert(client, table: "users", doc: ["first": .string("Gracie"), "last": .string("Hopper")])
        try client.mutate(Transaction(steps: [
            .patch(table: "users", id: id, fields: ["first": .string("Grace")])
        ]))
        #expect(try get(client, table: "users", id).objectValue?["fullName"] == .string("Grace Hopper"))
    }

    @Test func wrongTypedClientComputedPatchValueIsDroppedNotValidated() throws {
        let client = fixedClient()
        try client.pushSchema(fullNameSchema())
        let id = try insert(client, table: "users", doc: ["first": .string("Ada"), "last": .string("Lovelace")])
        // A wrong-TYPED client-supplied computed value must not fail
        // validation — it is dropped before the merge and the stamp re-derives.
        try client.mutate(Transaction(steps: [
            .patch(table: "users", id: id, fields: ["fullName": .int(123)])
        ]))
        #expect(try get(client, table: "users", id).objectValue?["fullName"] == .string("Ada Lovelace"))
    }

    @Test func nullResultRemovesOptionalComputedKey() throws {
        let client = fixedClient()
        let schema = try SchemaBuilder()
            .table("users") {
                $0.field("name", .string)
                    .field("nickname", .optional(.string))
                    .field("nick", .optional(.string))
                    .index("by_name", on: ["name"])
                    .computed("nick", .coalesce(parts: [.field(field: "nickname")]))
            }
            .build()
        try client.pushSchema(schema)
        let id = try insert(
            client, table: "users", doc: ["name": .string("Ada"), "nickname": .string("Ace")]
        )
        #expect(try get(client, table: "users", id).objectValue?["nick"] == .string("Ace"))
        // Nulling the input removes BOTH the optional input key and the
        // computed key — an absent key, never a stored null.
        try client.mutate(Transaction(steps: [
            .patch(table: "users", id: id, fields: ["nickname": .null])
        ]))
        let doc = try get(client, table: "users", id)
        #expect(doc.objectValue?["nick"] == nil)
        #expect(doc.objectValue?["nickname"] == nil)
    }

    @Test func evalErrorFailsWriteBadRequestNamingField() throws {
        let client = fixedClient()
        let schema = try SchemaBuilder()
            .table("metrics") {
                $0.field("num", .number)
                    .field("denom", .number)
                    .field("ratio", .number)
                    .index("by_num", on: ["num"])
                    .computed(
                        "ratio",
                        .div(left: .field(field: "num"), right: .field(field: "denom"))
                    )
            }
            .build()
        try client.pushSchema(schema)
        try expectMutateError(client, Transaction(steps: [
            .insert(table: "metrics", doc: ["num": .int(1), "denom": .int(0)])
        ]), "computed field 'ratio': division by zero")
        let id = try insert(client, table: "metrics", doc: ["num": .int(1), "denom": .int(2)])
        try expectMutateError(client, Transaction(steps: [
            .patch(table: "metrics", id: id, fields: ["denom": .int(0)])
        ]), "computed field 'ratio': division by zero")
        // The failed write left the stored doc untouched.
        #expect(try get(client, table: "metrics", id).objectValue?["denom"] == .int(2))
    }

    @Test func upsertReplaceAndPatchByQueryAllRestamp() throws {
        let client = monotonicClient()
        try client.pushSchema(fullNameSchema())
        // Upsert insert branch: client value overwritten.
        var results = try client.mutate(Transaction(steps: [
            .upsert(
                table: "users", index: "by_fullName",
                eq: [.string("WRONG")],
                insert: ["first": .string("Ada"), "last": .string("Lovelace"), "fullName": .string("WRONG")],
                patch: [:]
            )
        ]))
        guard case let .upsert(firstId, inserted) = results[0] else {
            Issue.record("expected upsert result")
            return
        }
        #expect(inserted)
        #expect(
            try get(client, table: "users", firstId).objectValue?["fullName"]
                == .string("Ada Lovelace")
        )
        // Upsert update branch: recomputed over the merged doc.
        results = try client.mutate(Transaction(steps: [
            .upsert(
                table: "users", index: "by_fullName",
                eq: [.string("Ada Lovelace")],
                insert: ["first": .string("X"), "last": .string("Y")],
                patch: ["first": .string("Adeline")]
            )
        ]))
        #expect(try get(client, table: "users", firstId).objectValue?["fullName"] == .string("Adeline Lovelace"))
        // Replace restamps over the full doc.
        try client.mutate(Transaction(steps: [
            .replace(
                table: "users", id: firstId,
                doc: ["first": .string("Grace"), "last": .string("Hopper"), "fullName": .string("WRONG")]
            )
        ]))
        #expect(try get(client, table: "users", firstId).objectValue?["fullName"] == .string("Grace Hopper"))
        // patchByQuery restamps per row.
        _ = try client.mutate(Transaction(steps: [
            .patchByQuery(
                table: "users",
                filter: .eq(field: "last", value: .string("Hopper")),
                patch: ["first": .string("Mary")],
                limit: nil
            )
        ]))
        #expect(try get(client, table: "users", firstId).objectValue?["fullName"] == .string("Mary Hopper"))
    }

    @Test func int64ComputedFieldStoresDecimalStringViaCastToString() throws {
        let client = fixedClient()
        let schema = try SchemaBuilder()
            .table("events") {
                $0.field("count", .int64)
                    .field("total", .int64)
                    .index("by_count", on: ["count"])
                    .computed(
                        "total",
                        .cast(
                            value: .add(left: .field(field: "count"), right: .literal(value: .int(1))),
                            to: .toString
                        )
                    )
            }
            .build()
        try client.pushSchema(schema)
        let id = try insert(client, table: "events", doc: ["count": .string("41")])
        // Arithmetic produces a JSON number; Cast(toString) lands the int64
        // decimal-string wire form so validateDoc passes.
        #expect(try get(client, table: "events", id).objectValue?["total"] == .string("42"))
    }

    // MARK: - DSL + wire

    @Test func builderEmitsComputedMapAndOmitsWhenAbsent() throws {
        let schema = try fullNameSchema()
        let encoded = try wire(schema.tables["users"] ?? TableDef(fields: [:]))
        #expect(
            encoded.objectValue?["computed"]
                == .object([
                    "fullName": .object([
                        "op": .string("concat"),
                        "parts": .array([
                            .object(["op": .string("field"), "field": .string("first")]),
                            .object(["op": .string("literal"), "value": .string(" ")]),
                            .object(["op": .string("field"), "field": .string("last")])
                        ])
                    ])
                ])
        )
        // A table with no computed entries serializes identically to before —
        // the key is absent, not an empty object.
        let plain = try SchemaBuilder()
            .table("plain") { $0.field("a", .string) }
            .build()
        #expect(try wire(#require(plain.tables["plain"])).objectValue?["computed"] == nil)
    }

    @Test func tableDefDecodesServerWireComputedMap() throws {
        // Byte-for-byte the corpus schema shape (server serde output).
        let json = Data(#"""
        {"fields":{"first":{"type":"string"},"last":{"type":"string"},"fullName":{"type":"string"}},
         "indexes":[{"name":"by_fullName","fields":["fullName"]}],
         "computed":{"fullName":{"op":"concat","parts":[
            {"op":"field","field":"first"},
            {"op":"literal","value":" "},
            {"op":"field","field":"last"}]}}}
        """#.utf8)
        let table = try JSONDecoder().decode(TableDef.self, from: json)
        #expect(
            table.computed["fullName"]
                == .concat(parts: [
                    .field(field: "first"),
                    .literal(value: .string(" ")),
                    .field(field: "last")
                ])
        )
        // Round-trip re-encodes the same wire shape.
        let roundTrip = try JSONDecoder().decode(
            JSONValue.self, from: JSONEncoder().encode(table)
        )
        #expect(roundTrip.objectValue?["computed"] != nil)
    }

    // MARK: - Push validation

    @Test func pushAcceptsCanonicalComputedSchemas() throws {
        // concat on string, lower-trim on optional string, arithmetic on
        // number, caseExpr on a union, now on number, cast(toString) on int64.
        let schema = try SchemaBuilder()
            .table("users") {
                $0.field("first", .string)
                    .field("last", .string)
                    .field("fullName", .string)
                    .field("alias", .optional(.string))
                    .field("slug", .optional(.string))
                    .field("nick", .optional(.string))
                    .field("score", .number)
                    .field("band", .union([.literal(.string("low")), .literal(.string("high"))]))
                    .field("seenAt", .number)
                    .field("loginCount", .number)
                    .field("count", .int64)
                    .field("countText", .int64)
                    .index("by_fullName", on: ["fullName"])
                    .index("by_score", on: ["score"])
                    .computed(
                        "fullName",
                        .concat(parts: [.field(field: "first"), .field(field: "last")])
                    )
                    .computed(
                        "slug",
                        .lower(value: .trim(value: .field(field: "last")))
                    )
                    .computed("nick", .coalesce(parts: [.field(field: "alias")]))
                    .computed(
                        "score",
                        .add(left: .literal(value: .int(1)), right: .literal(value: .int(2)))
                    )
                    .computed(
                        "band",
                        .caseExpr(
                            whens: [
                                CaseWhen(
                                    when: .gt(field: "loginCount", value: .int(0)),
                                    then: .literal(value: .string("high"))
                                )
                            ],
                            otherwise: .literal(value: .string("low"))
                        )
                    )
                    .computed("seenAt", .now)
                    .computed(
                        "countText",
                        .cast(value: .field(field: "count"), to: .toString)
                    )
            }
            .build()
        try fixedClient().pushSchema(schema)
    }

    @Test func pushRejectsComputedKeyNotDeclared() throws {
        let schema = try SchemaBuilder()
            .table("users") {
                $0.field("name", .string)
                    .computed("slug", .lower(value: .field(field: "name")))
            }
            .build()
        try expectPushError(schema, "computed field 'users.slug' is not a declared field")
    }

    @Test func pushRejectsComputedOnStampedDeclarationFields() throws {
        let owner = try SchemaBuilder()
            .table("rows") {
                $0.field("owner", .string)
                    .ownerField("owner")
                    .computed("owner", .literal(value: .string("x")))
            }
            .build()
        try expectPushError(owner, "must not be the table's ownerField")
        let collaborators = try SchemaBuilder()
            .table("rows") {
                $0.field("title", .string)
                    .field("mates", .array(.string))
                    .collaboratorsField("mates")
                    .computed("mates", .literal(value: .array([])))
            }
            .build()
        try expectPushError(collaborators, "must not be the table's collaboratorsField")
        let autoIncrement = try SchemaBuilder()
            .table("rows") {
                $0.field("title", .string)
                    .field("num", .int64)
                    .autoIncrementField("num")
                    .computed("num", .cast(value: .literal(value: .int(1)), to: .toString))
            }
            .build()
        try expectPushError(autoIncrement, "must not be the table's autoIncrementField")
    }

    @Test func pushRejectsUndeclaredAndComputedReferences() throws {
        let undeclared = try SchemaBuilder()
            .table("users") {
                $0.field("first", .string)
                    .field("fullName", .string)
                    .computed(
                        "fullName",
                        .concat(parts: [.field(field: "first"), .field(field: "middle")])
                    )
            }
            .build()
        try expectPushError(undeclared, "references undeclared field 'middle'")
        let chained = try SchemaBuilder()
            .table("users") {
                $0.field("first", .string)
                    .field("fullName", .string)
                    .field("shout", .string)
                    .computed(
                        "fullName",
                        .concat(parts: [.field(field: "first")])
                    )
                    .computed("shout", .upper(value: .field(field: "fullName")))
            }
            .build()
        try expectPushError(chained, "references computed field 'fullName'")
    }

    @Test func pushRejectsCaseWhenReferencesIncludingFilters() throws {
        // A Field ref inside a Case.when FILTER is covered by the walk.
        let filterRef = try SchemaBuilder()
            .table("users") {
                $0.field("status", .string)
                    .field("band", .string)
                    .computed(
                        "band",
                        .caseExpr(
                            whens: [
                                CaseWhen(
                                    when: .eq(field: "level", value: .int(1)),
                                    then: .literal(value: .string("a"))
                                )
                            ],
                            otherwise: .literal(value: .string("b"))
                        )
                    )
            }
            .build()
        try expectPushError(filterRef, "references undeclared field 'level'")
        // Principal markers inside a Case.when are rejected.
        let marker = try SchemaBuilder()
            .table("users") {
                $0.field("uid", .string)
                    .field("band", .string)
                    .computed(
                        "band",
                        .caseExpr(
                            whens: [
                                CaseWhen(
                                    when: .eq(field: "uid", value: .object(["$user": .bool(true)])),
                                    then: .literal(value: .string("mine"))
                                )
                            ],
                            otherwise: .literal(value: .string("other"))
                        )
                    )
            }
            .build()
        try expectPushError(marker, "principal markers")
    }

    @Test func pushRejectsStaticKindMismatches() throws {
        let concatIntoNumber = try SchemaBuilder()
            .table("metrics") {
                $0.field("denom", .optional(.number))
                    .field("ratio", .optional(.number))
                    .index("by_denom", on: ["denom"])
                    .computed(
                        "ratio",
                        .concat(parts: [.field(field: "denom"), .literal(value: .string("x"))])
                    )
            }
            .build()
        try expectPushError(concatIntoNumber, "produces a string, which the field type does not accept")
        let arithmeticIntoInt64 = try SchemaBuilder()
            .table("metrics") {
                $0.field("a", .int64)
                    .field("b", .int64)
                    .computed("b", .add(left: .field(field: "a"), right: .literal(value: .int(1))))
            }
            .build()
        try expectPushError(arithmeticIntoInt64, "produces a number, which the field type does not accept")
        let lowerIntoBoolean = try SchemaBuilder()
            .table("users") {
                $0.field("name", .string)
                    .field("flag", .boolean)
                    .computed("flag", .lower(value: .field(field: "name")))
            }
            .build()
        try expectPushError(lowerIntoBoolean, "produces a string, which the field type does not accept")
    }

    @Test func pushRejectsAuthorizePredicateOverComputedField() throws {
        let schema = try SchemaBuilder()
            .table("rows") {
                $0.field("title", .string)
                    .field("ownerUid", .string)
                    .field("computedOwner", .string)
                    .computed("computedOwner", .lower(value: .field(field: "ownerUid")))
                    .authorize(.eq(field: "computedOwner", value: .object(["$user": .bool(true)])))
            }
            .build()
        try expectPushError(
            schema,
            "must not be referenced by the table's authorize predicate"
        )
    }

    // MARK: - Migrate interplay

    @Test func renameFieldRewritesComputedReferencesAndKeyedEntry() throws {
        let client = fixedClient()
        try client.pushSchema(fullNameSchema())
        let id = try insert(client, table: "users", doc: ["first": .string("Ada"), "last": .string("Lovelace")])
        let result = try client.migrate(MigrateRequest(directives: [
            .renameField(table: "users", from: "first", to: "givenName")
        ], dryRun: false))
        // The derived schema carries the rewritten expression and the moved key.
        let table = result.schema.tables["users"]
        #expect(
            table?.computed["fullName"]
                == .concat(parts: [
                    .field(field: "givenName"),
                    .literal(value: .string(" ")),
                    .field(field: "last")
                ])
        )
        // Input values are unchanged, so the stored computed value stays
        // correct — and the next write re-stamps from the renamed key.
        #expect(try get(client, table: "users", id).objectValue?["fullName"] == .string("Ada Lovelace"))
        try client.mutate(Transaction(steps: [
            .patch(table: "users", id: id, fields: ["givenName": .string("Adeline")])
        ]))
        #expect(try get(client, table: "users", id).objectValue?["fullName"] == .string("Adeline Lovelace"))
    }

    @Test func renameFieldRewritesCaseWhenFilterFields() throws {
        let client = fixedClient()
        let schema = try SchemaBuilder()
            .table("users") {
                $0.field("status", .string)
                    .field("band", .string)
                    .computed(
                        "band",
                        .caseExpr(
                            whens: [
                                CaseWhen(
                                    when: .eq(field: "status", value: .string("admin")),
                                    then: .literal(value: .string("hi"))
                                )
                            ],
                            otherwise: .literal(value: .string("lo"))
                        )
                    )
            }
            .build()
        try client.pushSchema(schema)
        let result = try client.migrate(MigrateRequest(directives: [
            .renameField(table: "users", from: "status", to: "role")
        ], dryRun: false))
        let whens: [CaseWhen]
        if case let .caseExpr(whenList, _) = result.schema.tables["users"]?.computed["band"] {
            whens = whenList
        } else {
            Issue.record("expected caseExpr after rename")
            return
        }
        #expect(whens[0].when == .eq(field: "role", value: .string("admin")))
    }

    @Test func dropFieldOnReferencedFieldIsRejectedNamingComputedField() throws {
        let client = fixedClient()
        try client.pushSchema(fullNameSchema())
        do {
            _ = try client.migrate(MigrateRequest(directives: [
                .dropField(table: "users", field: "first")
            ], dryRun: false))
            Issue.record("expected dropField rejection")
        } catch let error as RtDbError {
            #expect(error.code == .badRequest)
            #expect(
                error.message.contains(
                    "referenced by computed field 'users.fullName'; drop the computed field first"
                )
            )
        }
    }

    @Test func droppingTheComputedFieldRemovesItsEntry() throws {
        let client = fixedClient()
        // No index on the computed field here — dropField empties an index's
        // fields list, and re-pushing an index with no fields is its own
        // schema-violation (this test pins the computed entry, not that rule).
        let schema = try SchemaBuilder()
            .table("users") {
                $0.field("first", .string)
                    .field("last", .string)
                    .field("fullName", .string)
                    .index("by_first", on: ["first"])
                    .computed(
                        "fullName",
                        .concat(parts: [.field(field: "first"), .field(field: "last")])
                    )
            }
            .build()
        try client.pushSchema(schema)
        let result = try client.migrate(MigrateRequest(directives: [
            .dropField(table: "users", field: "fullName")
        ], dryRun: false))
        #expect(result.schema.tables["users"]?.computed.isEmpty == true)
        // The derived schema pushes cleanly (no dangling entry).
        try client.pushSchema(result.schema)
    }

    @Test func changeTypeRevalidatesDerivedComputedMap() throws {
        let client = fixedClient()
        // `total` = cast(add(count,1), toString) on an int64 field — legal.
        let schema = try SchemaBuilder()
            .table("events") {
                $0.field("count", .int64)
                    .field("total", .int64)
                    .index("by_count", on: ["count"])
                    .computed(
                        "total",
                        .cast(
                            value: .add(left: .field(field: "count"), right: .literal(value: .int(1))),
                            to: .toString
                        )
                    )
            }
            .build()
        try client.pushSchema(schema)
        // changeType to `number`: the derived map now produces a string for a
        // number field — the plan-time re-validation rejects it.
        do {
            _ = try client.migrate(MigrateRequest(directives: [
                .changeType(table: "events", field: "total", to: .number, cast: .toNumber, default: nil)
            ], dryRun: false))
            Issue.record("expected changeType re-validation failure")
        } catch let error as RtDbError {
            #expect(error.code == .badRequest)
            #expect(error.message.contains("produces a string, which the field type does not accept"))
        }
    }

    @Test func migrateDryRunReportsDerivedSchemaWithoutPersisting() throws {
        let client = fixedClient()
        try client.pushSchema(fullNameSchema())
        _ = try insert(client, table: "users", doc: ["first": .string("Ada"), "last": .string("Lovelace")])
        let result = try client.migrate(MigrateRequest(directives: [
            .renameField(table: "users", from: "first", to: "givenName")
        ], dryRun: true))
        #expect(result.applied == false)
        #expect(result.schema.tables["users"]?.fields["givenName"] != nil)
        // dryRun committed nothing: the live schema still carries `first`.
        let rows = try client.query(Query(table: "users"))
        guard case let .array(docs) = rows, let doc = docs.first else {
            Issue.record("expected one row")
            return
        }
        #expect(doc.objectValue?["first"] == .string("Ada"))
    }
}
