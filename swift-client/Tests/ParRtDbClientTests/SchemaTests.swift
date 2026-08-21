import Foundation
@testable import ParRtDbClient
import Testing

// Task 10 — the schema DSL: `FieldType` (15 variants), `IndexDef` /
// `VectorIndexSpec` / `TtlDef` / `TableDef` / `SchemaDef`, and the
// `TableBuilder`/`SchemaBuilder` fluent builders. Every wire-shape assertion
// is whole-object against the rust-client builder fixtures
// (rust-client/src/schema.rs tests), so a stray key fails the test.

// MARK: - Helpers

/// Generic helpers THROW (never `#expect` inside a generic function — it
/// crashes the Swift 6.3.3 frontend); assertions stay in the test methods.
private func roundTrip<T: Codable & Equatable>(_ value: T) throws -> T {
    try JSONDecoder().decode(T.self, from: JSONEncoder().encode(value))
}

private func wireValue(_ value: some Codable) throws -> JSONValue {
    try JSONDecoder().decode(JSONValue.self, from: JSONEncoder().encode(value))
}

/// Decode a `FieldType` from raw JSON — nil on any decode failure (unknown
/// tag, unknown variant field, missing payload key).
private func decodeField(_ json: String) -> FieldType? {
    try? JSONDecoder().decode(FieldType.self, from: Data(json.utf8))
}

private func decodeStruct<T: Decodable>(_ type: T.Type, _ json: String) throws -> T {
    try JSONDecoder().decode(type, from: Data(json.utf8))
}

private func obj(_ pairs: [String: JSONValue]) -> JSONValue {
    .object(pairs)
}

private func arrayValue(_ value: JSONValue) -> [JSONValue]? {
    if case let .array(array) = value {
        return array
    }
    return nil
}

// MARK: - FieldType wire tags

struct SchemaFieldTypeTests {
    @Test func scalarVariantsCarryOnlyTheTag() throws {
        // The 7 scalar variants (rust `field_type_wire_tags`, extended to the
        // full scalar set): {"type": "<camelCase tag>"} and nothing else.
        #expect(try wireValue(FieldType.string) == obj(["type": .string("string")]))
        #expect(try wireValue(FieldType.number) == obj(["type": .string("number")]))
        #expect(try wireValue(FieldType.boolean) == obj(["type": .string("boolean")]))
        #expect(try wireValue(FieldType.null) == obj(["type": .string("null")]))
        #expect(try wireValue(FieldType.int64) == obj(["type": .string("int64")]))
        #expect(try wireValue(FieldType.bytes) == obj(["type": .string("bytes")]))
        #expect(try wireValue(FieldType.any) == obj(["type": .string("any")]))
    }

    @Test func compoundVariantsCarryExactlyOnePayloadField() throws {
        // The 8 compound variants (id also carries the additive `onDelete`).
        #expect(
            try wireValue(FieldType.id(table: "projects", onDelete: nil))
                == obj(["type": .string("id"), "table": .string("projects")])
        )
        #expect(
            try wireValue(FieldType.literal(value: .string("active")))
                == obj(["type": .string("literal"), "value": .string("active")])
        )
        #expect(
            try wireValue(FieldType.optional(inner: .boolean))
                == obj(["type": .string("optional"), "inner": obj(["type": .string("boolean")])])
        )
        #expect(
            try wireValue(FieldType.union(variants: [
                .literal(value: .string("backlog")), .literal(value: .string("done"))
            ])) == obj([
                "type": .string("union"),
                "variants": .array([
                    obj(["type": .string("literal"), "value": .string("backlog")]),
                    obj(["type": .string("literal"), "value": .string("done")])
                ])
            ])
        )
        #expect(
            try wireValue(FieldType.array(element: .id("users")))
                == obj([
                    "type": .string("array"),
                    "element": obj(["type": .string("id"), "table": .string("users")])
                ])
        )
        #expect(
            try wireValue(FieldType.object(fields: ["name": .string, "count": .number]))
                == obj([
                    "type": .string("object"),
                    "fields": obj([
                        "name": obj(["type": .string("string")]),
                        "count": obj(["type": .string("number")])
                    ])
                ])
        )
        #expect(
            try wireValue(FieldType.record(value: .int64))
                == obj(["type": .string("record"), "value": obj(["type": .string("int64")])])
        )
        #expect(
            try wireValue(FieldType.vector(dimensions: 4))
                == obj(["type": .string("vector"), "dimensions": .int(4)])
        )
    }

    @Test func everyVariantRoundTrips() throws {
        let all: [FieldType] = [
            .string, .number, .boolean, .null, .int64, .bytes, .any,
            .id(table: "projects", onDelete: nil),
            .literal(value: .bool(true)),
            .optional(inner: .id(table: "projects", onDelete: .setNull)),
            .union(variants: [.literal(value: .int(1)), .string]),
            .array(element: .array(element: .bytes)),
            .object(fields: ["nested": .record(value: .any)]),
            .record(value: .optional(inner: .number)),
            .vector(dimensions: 1536)
        ]
        for field in all {
            #expect(try roundTrip(field) == field)
        }
    }

    @Test func fieldTypeRejectsUnknownVariantFields() {
        // serde `deny_unknown_fields` parity — rejected per MATCHED variant.
        #expect(decodeField(#"{"type":"string","bogus":1}"#) == nil)
        #expect(decodeField(#"{"type":"id","table":"t","extra":1}"#) == nil)
        #expect(decodeField(#"{"type":"vector","dimensions":4,"filterFields":[]}"#) == nil)
        // A key legal on a DIFFERENT variant is still unknown here.
        #expect(decodeField(#"{"type":"string","table":"t"}"#) == nil)
    }

    @Test func fieldTypeRejectsUnknownOrMissingTypeTag() {
        #expect(decodeField(#"{"type":"wat"}"#) == nil)
        #expect(decodeField(#"{"table":"t"}"#) == nil)
    }

    @Test func onDeleteSerializesAndRoundTrips() throws {
        // FM-33: camelCase action tags on the id field, present only when set.
        let actions: [(OnDeleteAction, String)] = [
            (.cascade, "cascade"), (.restrict, "restrict"), (.setNull, "setNull")
        ]
        for (action, wire) in actions {
            let field = FieldType.id("projects").onDelete(action)
            #expect(try wireValue(field) == obj([
                "type": .string("id"), "table": .string("projects"), "onDelete": .string(wire)
            ]))
            #expect(try roundTrip(field) == field)
        }
        // No action omits the key entirely (not serialized as null).
        #expect(
            try wireValue(FieldType.id("projects"))
                == obj(["type": .string("id"), "table": .string("projects")])
        )
        // setNull composes with the optional wrapper — the legal nullable shape.
        let optional = FieldType.optional(FieldType.id("projects").onDelete(.setNull))
        #expect(try wireValue(optional) == obj([
            "type": .string("optional"),
            "inner": obj([
                "type": .string("id"), "table": .string("projects"), "onDelete": .string("setNull")
            ])
        ]))
        #expect(try roundTrip(optional) == optional)
    }

    @Test func onDeleteBuilderIsAdditiveAndLastWins() {
        // Chains after `.id(...)`; calling it twice overwrites (last wins).
        let swapped = FieldType.id("projects").onDelete(.cascade).onDelete(.restrict)
        #expect(swapped == .id(table: "projects", onDelete: .restrict))
        // A non-id variant passes through unchanged.
        let passthrough = FieldType.optional(.string).onDelete(.cascade)
        #expect(passthrough == .optional(.string))
    }
}

// MARK: - Indexes

struct SchemaIndexTests {
    @Test func btreeIndexSerializesNameAndFieldsOnly() throws {
        // A plain btree index is exactly {"name","fields"} — search/vector/
        // unique/where/language all omitted.
        let table = TableBuilder()
            .field("title", .string)
            .index("by_title", on: ["title"])
            .finish()
        #expect(try wireValue(table).objectValue?["indexes"] == .array([
            obj(["name": .string("by_title"), "fields": .array([.string("title")])])
        ]))
    }

    @Test func searchIndexSerializesAndRoundTrips() throws {
        // Mirrors rust `search_index_serializes_and_round_trips`: a search
        // index carries `search: true`; a btree index omits the flag and
        // decodes back to `search: false`.
        let schema = SchemaBuilder()
            .table("notes") {
                $0.field("title", .string)
                    .field("body", .string)
                    .index("by_title", on: ["title"])
                    .searchIndex("search_content", on: ["title", "body"])
            }
            .build()
        let notes = try schema.wireObject()["tables"]?.objectValue?["notes"]?.objectValue
        #expect(notes?["indexes"] == .array([
            obj(["name": .string("by_title"), "fields": .array([.string("title")])]),
            obj([
                "name": .string("search_content"),
                "fields": .array([.string("title"), .string("body")]),
                "search": .bool(true)
            ])
        ]))
        let back = try roundTrip(schema)
        let indexes = back.tables["notes"]?.indexes ?? []
        #expect(indexes.first { $0.name == "search_content" }?.search == true)
        #expect(indexes.first { $0.name == "by_title" }?.search == false)
    }

    @Test func searchIndexLanguageOmittedWhenNilPresentWhenSet() throws {
        // Mirrors rust `search_index_language_serializes_and_round_trips`.
        let schema = SchemaBuilder()
            .table("notes") {
                $0.field("title", .string)
                    .field("body", .string)
                    .searchIndex("search_default", on: ["title", "body"])
                    .searchIndex("search_spanish", on: ["title", "body"], language: "spanish")
            }
            .build()
        let indexes = try arrayValue(
            schema.wireObject()["tables"]?.objectValue?["notes"]?.objectValue?["indexes"]
                ?? .null
        ) ?? []
        #expect(indexes[0].objectValue?["name"] == .string("search_default"))
        #expect(indexes[0].objectValue?["search"] == .bool(true))
        #expect(indexes[0].objectValue?["language"] == nil)
        #expect(indexes[1].objectValue?["name"] == .string("search_spanish"))
        #expect(indexes[1].objectValue?["language"] == .string("spanish"))
        // Round-trips: nil stays nil, "spanish" preserved.
        let back = try roundTrip(schema).tables["notes"]?.indexes ?? []
        #expect(back.first { $0.name == "search_default" }?.language == nil)
        #expect(back.first { $0.name == "search_spanish" }?.language == "spanish")
        // A legacy search index without a `language` key decodes to nil.
        let legacy = try decodeStruct(
            IndexDef.self, #"{"name":"s","fields":["body"],"search":true}"#
        )
        #expect(legacy.language == nil)
    }

    @Test func vectorIndexSerializesAndRoundTrips() throws {
        // Mirrors rust `vector_index_serializes_and_round_trips`: the spec is
        // camelCase `{"dimensions", "filterFields"}`; a btree index in the same
        // schema omits `vector`.
        let schema = SchemaBuilder()
            .table("notes") {
                $0.field("title", .string)
                    .field("embedding", .vector(4))
                    .index("by_title", on: ["title"])
                    .vectorIndex("by_embedding", on: "embedding", dimensions: 4,
                                 filterFields: ["userId"])
            }
            .build()
        let notes = try schema.wireObject()["tables"]?.objectValue?["notes"]?.objectValue
        #expect(notes?["indexes"] == .array([
            obj(["name": .string("by_title"), "fields": .array([.string("title")])]),
            obj([
                "name": .string("by_embedding"),
                "fields": .array([.string("embedding")]),
                "vector": obj(["dimensions": .int(4), "filterFields": .array([.string("userId")])])
            ])
        ]))
        let back = try roundTrip(schema)
        let indexes = back.tables["notes"]?.indexes ?? []
        #expect(indexes.first { $0.name == "by_embedding" }?.vector?.dimensions == 4)
        #expect(indexes.first { $0.name == "by_embedding" }?.vector?.filterFields == ["userId"])
        #expect(indexes.first { $0.name == "by_title" }?.vector == nil)
    }

    @Test func vectorIndexEmptyFilterFieldsOmitsKey() throws {
        // Mirrors rust `vector_index_with_empty_filter_fields_omits_key`.
        let table = TableBuilder()
            .field("embedding", .vector(8))
            .vectorIndex("by_embedding", on: "embedding", dimensions: 8)
            .finish()
        let vector = try arrayValue(wireValue(table).objectValue?["indexes"] ?? .null)?[0]
            .objectValue?["vector"]?.objectValue
        #expect(vector?["dimensions"] == .int(8))
        #expect(vector?["filterFields"] == nil)
    }

    @Test func vectorMetricOmittedForCosinePresentOtherwise() throws {
        // Mirrors rust `vector_index_metric_serializes_and_round_trips`.
        let l2 = VectorIndexSpec(dimensions: 4, filterFields: [], metric: .l2)
        #expect(try wireValue(l2).objectValue?["metric"] == .string("l2"))
        let cosine = VectorIndexSpec(dimensions: 4)
        #expect(try wireValue(cosine).objectValue?["metric"] == nil)
        #expect(try wireValue(cosine).objectValue?["filterFields"] == nil)
        #expect(try roundTrip(l2) == l2)
        // A legacy spec without `metric` deserializes to the default `.cosine`.
        let legacy = try decodeStruct(
            VectorIndexSpec.self, #"{"dimensions": 4, "filterFields": []}"#
        )
        #expect(legacy.metric == .cosine)
        #expect(legacy == VectorIndexSpec(dimensions: 4, filterFields: []))
    }

    @Test func uniqueIndexBuilderAndWireShape() throws {
        // Mirrors rust `unique_index_builder_and_wire_shape`: `.unique()` marks
        // the most recently declared index; a plain index omits the flag.
        let table = TableBuilder()
            .field("email", .string)
            .field("org", .string)
            .index("by_email", on: ["email"])
            .unique()
            .index("by_org", on: ["org"])
            .finish()
        let indexes = try arrayValue(wireValue(table).objectValue?["indexes"] ?? .null) ?? []
        #expect(indexes[0].objectValue?["name"] == .string("by_email"))
        #expect(indexes[0].objectValue?["unique"] == .bool(true))
        #expect(indexes[0].objectValue?["where"] == nil)
        #expect(indexes[1].objectValue?["name"] == .string("by_org"))
        #expect(indexes[1].objectValue?["unique"] == nil)
        #expect(indexes[1].objectValue?["where"] == nil)
        let back = try roundTrip(table)
        #expect(back.indexes?[0].unique == true)
        #expect(back.indexes?[1].unique == false)
    }

    @Test func partialUniqueIndexBuilderAndWireShape() throws {
        // Mirrors rust `partial_unique_index_builder_and_wire_shape`:
        // `.whereClause(...)` attaches a partial-index predicate (wire key
        // `where`) to the most recent index; composes with `.unique()`.
        let table = TableBuilder()
            .field("email", .string)
            .field("archived", .optional(.boolean))
            .index("by_email", on: ["email"])
            .unique()
            .whereClause(.neq(field: "archived", value: .bool(true)))
            .finish()
        let index = try arrayValue(wireValue(table).objectValue?["indexes"] ?? .null)?[0]
            .objectValue
        #expect(index?["unique"] == .bool(true))
        #expect(index?["where"] == obj([
            "op": .string("neq"), "field": .string("archived"), "value": .bool(true)
        ]))
        let back = try roundTrip(table)
        #expect(back.indexes?[0].unique == true)
        #expect(back.indexes?[0].whereClause == .neq(field: "archived", value: .bool(true)))
    }

    @Test func uniqueAndWhereSettersNoopBeforeAnyIndex() {
        // Mirrors rust `unique_and_where_setters_noop_before_any_index`.
        let table = TableBuilder()
            .field("email", .string)
            .unique()
            .whereClause(.eq(field: "email", value: .string("x")))
            .finish()
        #expect(table.indexes == nil)
    }
}

// MARK: - Builders + SchemaDef

struct SchemaBuilderTests {
    // swiftlint:disable:next function_body_length
    @Test func builderSerializesFullSchema() throws {
        // Mirrors rust `builder_serializes_full_schema` 1:1 (two tables, btree
        // indexes, optional/union/literal types).
        let schema = SchemaDef.builder()
            .table("projects") {
                $0.field("name", .string)
                    .field("archived", .optional(.boolean))
                    .index("by_name", on: ["name"])
            }
            .table("items") {
                $0.field("projectId", .id("projects"))
                    .field("title", .string)
                    .field("status", .union([
                        .literal(.string("backlog")), .literal(.string("done"))
                    ]))
                    .field("order", .number)
                    .index("by_project", on: ["projectId"])
                    .index("by_project_and_title", on: ["projectId", "title"])
            }
            .build()
        #expect(try schema.wireObject() == ["tables": .object([
            "projects": .object([
                "fields": .object([
                    "name": obj(["type": .string("string")]),
                    "archived": obj([
                        "type": .string("optional"),
                        "inner": obj(["type": .string("boolean")])
                    ])
                ]),
                "indexes": .array([
                    obj(["name": .string("by_name"), "fields": .array([.string("name")])])
                ])
            ]),
            "items": .object([
                "fields": .object([
                    "projectId": obj(["type": .string("id"), "table": .string("projects")]),
                    "title": obj(["type": .string("string")]),
                    "status": obj([
                        "type": .string("union"),
                        "variants": .array([
                            obj(["type": .string("literal"), "value": .string("backlog")]),
                            obj(["type": .string("literal"), "value": .string("done")])
                        ])
                    ]),
                    "order": obj(["type": .string("number")])
                ]),
                "indexes": .array([
                    obj(["name": .string("by_project"), "fields": .array([.string("projectId")])]),
                    obj([
                        "name": .string("by_project_and_title"),
                        "fields": .array([.string("projectId"), .string("title")])
                    ])
                ])
            ])
        ])])
    }

    @Test func tableWithNoIndexesOmitsKey() throws {
        // Mirrors rust `table_with_no_indexes_omits_key`.
        let schema = SchemaBuilder()
            .table("solo") { $0.field("x", .number) }
            .build()
        let solo = try schema.wireObject()["tables"]?.objectValue?["solo"]?.objectValue
        #expect(solo?["fields"] == .object(["x": obj(["type": .string("number")])]))
        #expect(solo?["indexes"] == nil)
    }

    // swiftlint:disable:next function_body_length
    @Test func schemaBuildsExactWireShape() throws {
        // The Task 10 composite: every table feature in one whole-object
        // assertion — string field + unique/where partial index, id field with
        // onDelete, vector field + vector index, search index, ownerField,
        // collaboratorsField, ttl, authorize (with the `$user` marker),
        // defaults, softDelete.
        let schema = SchemaBuilder()
            .table("users") {
                $0.field("email", .string)
                    .field("org", .id("orgs").onDelete(.cascade))
                    .field("embedding", .vector(1536))
                    .field("bio", .string)
                    .field("archived", .optional(.boolean))
                    .field("collaborators", .array(.string))
                    .field("role", .union([.literal(.string("member")), .literal(.string("admin"))]))
                    .field("expiresAt", .number)
                    .field("owner", .string)
                    .index("by_email", on: ["email"])
                    .unique()
                    .whereClause(.neq(field: "archived", value: .bool(true)))
                    .searchIndex("search_bio", on: ["bio"])
                    .vectorIndex("by_embedding", on: "embedding", dimensions: 1536)
                    .ownerField("owner")
                    .collaboratorsField("collaborators")
                    .ttl("expiresAt", defaultDurationMs: 86_400_000)
                    .authorize(.eq(field: "owner", value: .object(["$user": .bool(true)])))
                    .defaults(["role": .string("member")])
                    .softDelete()
            }
            .build()
        #expect(try schema.wireObject() == ["tables": .object([
            "users": .object([
                "fields": .object([
                    "email": obj(["type": .string("string")]),
                    "org": obj([
                        "type": .string("id"), "table": .string("orgs"),
                        "onDelete": .string("cascade")
                    ]),
                    "embedding": obj(["type": .string("vector"), "dimensions": .int(1536)]),
                    "bio": obj(["type": .string("string")]),
                    "archived": obj([
                        "type": .string("optional"), "inner": obj(["type": .string("boolean")])
                    ]),
                    "collaborators": obj([
                        "type": .string("array"), "element": obj(["type": .string("string")])
                    ]),
                    "role": obj([
                        "type": .string("union"),
                        "variants": .array([
                            obj(["type": .string("literal"), "value": .string("member")]),
                            obj(["type": .string("literal"), "value": .string("admin")])
                        ])
                    ]),
                    "expiresAt": obj(["type": .string("number")]),
                    "owner": obj(["type": .string("string")])
                ]),
                "indexes": .array([
                    obj([
                        "name": .string("by_email"), "fields": .array([.string("email")]),
                        "unique": .bool(true),
                        "where": obj([
                            "op": .string("neq"), "field": .string("archived"),
                            "value": .bool(true)
                        ])
                    ]),
                    obj([
                        "name": .string("search_bio"), "fields": .array([.string("bio")]),
                        "search": .bool(true)
                    ]),
                    obj([
                        "name": .string("by_embedding"),
                        "fields": .array([.string("embedding")]),
                        "vector": obj(["dimensions": .int(1536)])
                    ])
                ]),
                "ownerField": .string("owner"),
                "collaboratorsField": .string("collaborators"),
                "ttl": obj([
                    "field": .string("expiresAt"), "defaultDurationMs": .int(86_400_000)
                ]),
                "authorize": obj([
                    "op": .string("eq"), "field": .string("owner"),
                    "value": .object(["$user": .bool(true)])
                ]),
                "defaults": .object(["role": .string("member")]),
                "softDelete": .bool(true)
            ])
        ])])
    }

    @Test func schemaDefRoundTripsThroughCodable() throws {
        let schema = SchemaBuilder()
            .table("users") {
                $0.field("org", .id("orgs").onDelete(.cascade))
                    .field("embedding", .vector(8))
                    .field("archived", .optional(.boolean))
                    .index("by_org", on: ["org"])
                    .unique()
                    .searchIndex("search_all", on: ["archived"])
                    .vectorIndex("by_embedding", on: "embedding", dimensions: 8,
                                 filterFields: ["org"], metric: .ip)
                    .ownerField("org")
                    .ttl("archived")
                    .authorize(.eq(field: "org", value: .object(["$user": .bool(true)])))
                    .defaults(["archived": .bool(false)])
                    .softDelete()
            }
            .build()
        #expect(try roundTrip(schema) == schema)
        #expect(try wireValue(schema) == .object(schema.wireObject()))
    }

    @Test func ownerAndCollaboratorsFieldsSerializeAndRoundTrip() throws {
        // Mirrors rust `owner_field_...` + `collaborators_field_...`.
        let table = TableBuilder()
            .field("userId", .string)
            .field("collaborators", .array(.string))
            .field("title", .string)
            .index("by_user", on: ["userId"])
            .ownerField("userId")
            .collaboratorsField("collaborators")
            .finish()
        let object = try wireValue(table).objectValue ?? [:]
        #expect(object["ownerField"] == .string("userId"))
        #expect(object["collaboratorsField"] == .string("collaborators"))
        #expect(try roundTrip(table) == table)
        // Absent -> omitted entirely (not serialized as null).
        let none = TableBuilder().field("title", .string).finish()
        let noneObject = try wireValue(none).objectValue ?? [:]
        #expect(noneObject["ownerField"] == nil)
        #expect(noneObject["collaboratorsField"] == nil)
    }

    @Test func authorizeSerializesAndRoundTrips() throws {
        // Mirrors rust `authorize_serializes_and_round_trips`: the Model C
        // predicate (with principal markers) survives a round trip unchanged.
        let predicate = FilterExpr.or(exprs: [
            .eq(field: "owner", value: .object(["$user": .bool(true)])),
            .eq(field: "visibility", value: .string("public"))
        ])
        let table = TableBuilder()
            .field("owner", .string)
            .field("visibility", .string)
            .authorize(predicate)
            .finish()
        #expect(try wireValue(table).objectValue?["authorize"] == obj([
            "op": .string("or"),
            "exprs": .array([
                obj(["op": .string("eq"), "field": .string("owner"),
                     "value": .object(["$user": .bool(true)])]),
                obj(["op": .string("eq"), "field": .string("visibility"),
                     "value": .string("public")])
            ])
        ]))
        #expect(try roundTrip(table) == table)
        let none = TableBuilder().field("title", .string).finish()
        #expect(try wireValue(none).objectValue?["authorize"] == nil)
    }

    @Test func defaultsSerializeAndRoundTrip() throws {
        // Mirrors rust `defaults_serializes_and_round_trips`.
        let table = TableBuilder()
            .field("status", .union([.literal(.string("backlog")), .literal(.string("done"))]))
            .field("priority", .number)
            .defaults(["status": .string("backlog"), "priority": .int(0)])
            .finish()
        #expect(try wireValue(table).objectValue?["defaults"] == .object([
            "status": .string("backlog"), "priority": .int(0)
        ]))
        #expect(try roundTrip(table) == table)
        // Empty -> omitted entirely (not `{}` or null); legacy decodes empty.
        let none = TableBuilder().field("title", .string).finish()
        #expect(try wireValue(none).objectValue?["defaults"] == nil)
        let legacy = try decodeStruct(TableDef.self, #"{"fields":{"title":{"type":"string"}}}"#)
        #expect(legacy.defaults.isEmpty)
    }

    @Test func ttlSerializesAndRoundTrips() throws {
        let bare = TableBuilder()
            .field("expiresAt", .number)
            .ttl("expiresAt")
            .finish()
        #expect(try wireValue(bare).objectValue?["ttl"] == obj(["field": .string("expiresAt")]))
        let withDefault = TableBuilder()
            .field("expiresAt", .number)
            .ttl("expiresAt", defaultDurationMs: 86_400_000)
            .finish()
        #expect(try wireValue(withDefault).objectValue?["ttl"] == obj([
            "field": .string("expiresAt"), "defaultDurationMs": .int(86_400_000)
        ]))
        #expect(try roundTrip(withDefault) == withDefault)
        let none = TableBuilder().field("x", .string).finish()
        #expect(try wireValue(none).objectValue?["ttl"] == nil)
    }

    @Test func updatedAtFieldSerializesAndRoundTrips() throws {
        // Mirrors the server's `push_accepts_and_round_trips_updated_at_field`
        // (FM-36): camelCase wire key when set, omitted when unset.
        let table = TableBuilder()
            .field("title", .string)
            .field("updatedAt", .number)
            .index("by_title", on: ["title"])
            .updatedAtField("updatedAt")
            .finish()
        #expect(try wireValue(table).objectValue?["updatedAtField"] == .string("updatedAt"))
        #expect(try roundTrip(table) == table)
        // Omitted when unset — an ordinary table serializes without the key.
        let none = TableBuilder().field("title", .string).finish()
        #expect(try wireValue(none).objectValue?["updatedAtField"] == nil)
    }

    @Test func autoIncrementFieldSerializesAndRoundTrips() throws {
        // Mirrors the server's autoIncrementField push round-trip (FM-37):
        // camelCase wire key when set, omitted when unset.
        let table = TableBuilder()
            .field("title", .string)
            .field("num", .int64)
            .index("by_title", on: ["title"])
            .autoIncrementField("num")
            .finish()
        #expect(try wireValue(table).objectValue?["autoIncrementField"] == .string("num"))
        #expect(try roundTrip(table) == table)
        // Omitted when unset — an ordinary table serializes without the key.
        let none = TableBuilder().field("title", .string).finish()
        #expect(try wireValue(none).objectValue?["autoIncrementField"] == nil)
    }

    @Test func softDeleteSerializesAndRoundTrips() throws {
        // Mirrors rust `soft_delete_serializes_and_round_trips`.
        let table = TableBuilder()
            .field("title", .string)
            .softDelete()
            .finish()
        #expect(try wireValue(table).objectValue?["softDelete"] == .bool(true))
        #expect(try roundTrip(table) == table)
        let none = TableBuilder().field("title", .string).finish()
        #expect(try wireValue(none).objectValue?["softDelete"] == nil)
        let legacy = try decodeStruct(TableDef.self, #"{"fields":{"title":{"type":"string"}}}"#)
        #expect(!legacy.softDelete)
    }

    @Test func legacyTableDefDecodesWithDefaults() throws {
        // A payload carrying only `fields` decodes with every optional key at
        // its default (serde `#[serde(default)]` parity).
        let table = try decodeStruct(TableDef.self, #"{"fields":{"title":{"type":"string"}}}"#)
        #expect(table.indexes == nil)
        #expect(table.ownerField == nil)
        #expect(table.collaboratorsField == nil)
        #expect(table.ttl == nil)
        #expect(table.updatedAtField == nil)
        #expect(table.autoIncrementField == nil)
        #expect(table.authorize == nil)
        #expect(table.defaults.isEmpty)
        #expect(!table.softDelete)
        let index = try decodeStruct(IndexDef.self, #"{"name":"by_title","fields":["title"]}"#)
        #expect(!index.search)
        #expect(index.vector == nil)
        #expect(!index.unique)
        #expect(index.whereClause == nil)
        #expect(index.language == nil)
    }

    @Test func chainingHasValueSemantics() {
        // Every method returns a NEW builder — branching a chain never shares
        // state (the Task 9 house pattern).
        let base = TableBuilder().field("email", .string).index("by_email", on: ["email"])
        let unique = base.unique()
        let partial = base.whereClause(.eq(field: "email", value: .string("x")))
        #expect(base.finish().indexes?[0].unique == false)
        #expect(base.finish().indexes?[0].whereClause == nil)
        #expect(unique.finish().indexes?[0].unique == true)
        #expect(unique.finish().indexes?[0].whereClause == nil)
        #expect(partial.finish().indexes?[0].unique == false)
        #expect(
            partial.finish().indexes?[0].whereClause
                == .eq(field: "email", value: .string("x"))
        )

        let builder = SchemaBuilder().table("t") { $0.field("x", .number) }
        let twoTables = builder.table("u") { $0.field("y", .string) }.build()
        let oneTable = builder.build()
        #expect(twoTables.tables.count == 2)
        #expect(oneTable.tables.count == 1)
    }
}
