import Foundation

// MARK: - OnDeleteAction

/// Referential action applied to child rows when the referenced parent row is
/// hard-deleted (FM-33). Carried on the CHILD table's `id` field as an additive
/// `onDelete` wire key (`cascade` | `restrict` | `setNull`); the cascade
/// executes app-level inside the server's txn executor (not a SQL FK) so every
/// cascaded row is a first-class op. Mirrors rust-client/src/schema.rs
/// `OnDeleteAction` byte-for-byte (camelCase wire tags).
public enum OnDeleteAction: String, Equatable, Codable, Sendable {
    /// Delete the child rows too.
    case cascade
    /// Block the parent delete while live children reference it (Conflict).
    case restrict
    /// Clear the child's referencing field (the key is removed).
    case setNull
}

// MARK: - FieldType

/// A field's declared type — the wire shape shared with the server (internally
/// tagged `"type"`, camelCase, unknown fields rejected per variant). Mirrors
/// rust-client/src/schema.rs `FieldType` exactly — 15 variants: scalars
/// (`string`/`number`/`boolean`/`null`/`int64`/`bytes`/`any` carry only the
/// tag) and compound (`id{table,onDelete?}`, `literal{value}`,
/// `optional{inner}`, `union{variants}`, `array{element}`, `object{fields}`,
/// `record{value}`, `vector{dimensions}`).
public indirect enum FieldType: Equatable, Codable, Sendable {
    /// JSON string.
    case string
    /// JSON number (Double).
    case number
    /// JSON boolean.
    case boolean
    /// JSON null.
    case null
    /// Reference to a document in `table` (an id string on the wire). `onDelete`
    /// is legal only on a TOP-LEVEL field of the table (an `id` directly, or one
    /// `optional` wrapping an `id`; server push validation enforces this).
    case id(table: String, onDelete: OnDeleteAction?)
    /// Exactly one accepted literal value (enum-like).
    case literal(value: JSONValue)
    /// `T | null`.
    case optional(inner: FieldType)
    /// Any one of the variants.
    case union(variants: [FieldType])
    /// Array of `element`.
    case array(element: FieldType)
    /// Fixed-shape nested object.
    case object(fields: [String: FieldType])
    /// 64-bit integer (wire-encoded as a string to keep JSON precision).
    case int64
    /// Binary payload (base64 on the wire).
    case bytes
    /// Any JSON value.
    case any
    /// Dynamic-key map with a uniform value type.
    case record(value: FieldType)
    /// Embedding vector of fixed `dimensions` (pgvector).
    case vector(dimensions: UInt32)

    enum CodingKeys: String, CodingKey {
        case type, table, onDelete, value, inner, variants, element, fields, dimensions
    }

    // swiftlint:disable:next cyclomatic_complexity function_body_length
    public init(from decoder: Decoder) throws {
        let payload = try taggedEnumPayload("FieldType", tagKey: "type", from: decoder)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        func reject(_ allowed: Set<String>) throws {
            try rejectUnknownVariantFields(
                "FieldType", variant: payload.tag, keys: payload.keys, allowed: allowed
            )
        }
        switch payload.tag {
        case "string":
            try reject(["type"])
            self = .string
        case "number":
            try reject(["type"])
            self = .number
        case "boolean":
            try reject(["type"])
            self = .boolean
        case "null":
            try reject(["type"])
            self = .null
        case "id":
            try reject(["type", "table", "onDelete"])
            self = try .id(
                table: container.decode(String.self, forKey: .table),
                onDelete: container.decodeIfPresent(OnDeleteAction.self, forKey: .onDelete)
            )
        case "literal":
            try reject(["type", "value"])
            self = try .literal(value: container.decode(JSONValue.self, forKey: .value))
        case "optional":
            try reject(["type", "inner"])
            self = try .optional(inner: container.decode(FieldType.self, forKey: .inner))
        case "union":
            try reject(["type", "variants"])
            self = try .union(variants: container.decode([FieldType].self, forKey: .variants))
        case "array":
            try reject(["type", "element"])
            self = try .array(element: container.decode(FieldType.self, forKey: .element))
        case "object":
            try reject(["type", "fields"])
            self = try .object(fields: container.decode([String: FieldType].self, forKey: .fields))
        case "int64":
            try reject(["type"])
            self = .int64
        case "bytes":
            try reject(["type"])
            self = .bytes
        case "any":
            try reject(["type"])
            self = .any
        case "record":
            try reject(["type", "value"])
            self = try .record(value: container.decode(FieldType.self, forKey: .value))
        case "vector":
            try reject(["type", "dimensions"])
            self = try .vector(
                dimensions: container.decode(UInt32.self, forKey: .dimensions)
            )
        case let unknown:
            throw DecodingError.dataCorrupted(
                DecodingError.Context(
                    codingPath: decoder.codingPath,
                    debugDescription: "FieldType: unknown type '\(unknown)'"
                )
            )
        }
    }

    // swiftlint:disable:next cyclomatic_complexity
    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        switch self {
        case .string:
            try container.encode("string", forKey: .type)
        case .number:
            try container.encode("number", forKey: .type)
        case .boolean:
            try container.encode("boolean", forKey: .type)
        case .null:
            try container.encode("null", forKey: .type)
        case let .id(table, onDelete):
            try container.encode("id", forKey: .type)
            try container.encode(table, forKey: .table)
            try container.encodeIfPresent(onDelete, forKey: .onDelete)
        case let .literal(value):
            try container.encode("literal", forKey: .type)
            try container.encode(value, forKey: .value)
        case let .optional(inner):
            try container.encode("optional", forKey: .type)
            try container.encode(inner, forKey: .inner)
        case let .union(variants):
            try container.encode("union", forKey: .type)
            try container.encode(variants, forKey: .variants)
        case let .array(element):
            try container.encode("array", forKey: .type)
            try container.encode(element, forKey: .element)
        case let .object(fields):
            try container.encode("object", forKey: .type)
            try container.encode(fields, forKey: .fields)
        case .int64:
            try container.encode("int64", forKey: .type)
        case .bytes:
            try container.encode("bytes", forKey: .type)
        case .any:
            try container.encode("any", forKey: .type)
        case let .record(value):
            try container.encode("record", forKey: .type)
            try container.encode(value, forKey: .value)
        case let .vector(dimensions):
            try container.encode("vector", forKey: .type)
            try container.encode(dimensions, forKey: .dimensions)
        }
    }
}

public extension FieldType {
    /// Shorthand for an id reference without an `onDelete` action.
    static func id(_ table: String) -> FieldType {
        .id(table: table, onDelete: nil)
    }

    /// Declare the `onDelete` referential action on an id field (FM-33):
    /// `.id("projects").onDelete(.cascade)`. Mirrors the rust/TS chainable.
    /// Only the `id` variant carries the action — on any other variant this is
    /// a no-op (server push validation rejects a misplaced `onDelete` anyway,
    /// and only a top-level id or `optional(id)` is legal). Last call wins.
    func onDelete(_ action: OnDeleteAction) -> FieldType {
        if case let .id(table, _) = self {
            return .id(table: table, onDelete: action)
        }
        return self
    }

    /// Wrap `inner` as `optional`.
    static func optional(_ inner: FieldType) -> FieldType {
        .optional(inner: inner)
    }

    /// A lone accepted literal value.
    static func literal(_ value: JSONValue) -> FieldType {
        .literal(value: value)
    }

    /// A union over the given variants.
    static func union(_ variants: [FieldType]) -> FieldType {
        .union(variants: variants)
    }

    /// An array of `element`.
    static func array(_ element: FieldType) -> FieldType {
        .array(element: element)
    }

    /// An embedding vector type of `dimensions`.
    static func vector(_ dimensions: UInt32) -> FieldType {
        .vector(dimensions: dimensions)
    }
}

// MARK: - DistanceMetric

/// Distance metric for a vector index. Mirrors rust-client/src/schema.rs
/// `DistanceMetric`: lowercase `"cosine" | "l2" | "ip"`; the default (`.cosine`)
/// is omitted on the wire by `VectorIndexSpec`, so existing schemas serialize
/// identically.
public enum DistanceMetric: String, Equatable, Codable, Sendable {
    /// Cosine distance (default).
    case cosine
    /// Euclidean L2 distance.
    case l2
    /// Inner product.
    case ip
}

// MARK: - VectorIndexSpec

/// Declaration of a vector (approximate nearest-neighbor) index — camelCase
/// wire (`filterFields`, `metric`). Mirrors rust-client `VectorIndexSpec`:
/// `filterFields` omitted when empty and `metric` omitted when `.cosine`.
public struct VectorIndexSpec: Equatable, Codable, Sendable {
    /// Vector dimension count.
    public var dimensions: UInt32
    /// Scalar fields usable as eq-filters in `vectorSearch`.
    public var filterFields: [String]
    /// Distance metric used by this index (default `.cosine`, wire-omitted).
    public var metric: DistanceMetric

    public init(
        dimensions: UInt32, filterFields: [String] = [], metric: DistanceMetric = .cosine
    ) {
        self.dimensions = dimensions
        self.filterFields = filterFields
        self.metric = metric
    }

    enum CodingKeys: String, CodingKey {
        case dimensions, filterFields, metric
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        dimensions = try container.decode(UInt32.self, forKey: .dimensions)
        filterFields = try container.decodeIfPresent([String].self, forKey: .filterFields) ?? []
        metric = try container.decodeIfPresent(DistanceMetric.self, forKey: .metric) ?? .cosine
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(dimensions, forKey: .dimensions)
        if !filterFields.isEmpty {
            try container.encode(filterFields, forKey: .filterFields)
        }
        if metric != .cosine {
            try container.encode(metric, forKey: .metric)
        }
    }
}

// MARK: - IndexDef

/// One declared index on a table (btree, search, or vector). Mirrors
/// rust-client `IndexDef`: `search`/`unique` omitted when false, `vector`/
/// `where`/`language` omitted when nil, so a plain btree index serializes as
/// `{"name","fields"}` only. `whereClause` serializes as the wire key `where`
/// (byte-identical to the server and the other three clients).
public struct IndexDef: Equatable, Codable, Sendable {
    /// Index name (used in queries' `withIndex`).
    public var name: String
    /// The indexed field names, in key order.
    public var fields: [String]
    /// `true` marks a full-text search index.
    public var search: Bool
    /// When present, marks this as a vector index: `fields[0]` must name a
    /// `vector` field whose `dimensions` match.
    public var vector: VectorIndexSpec?
    /// `true` marks a unique btree index (`CREATE UNIQUE INDEX`). May not
    /// combine with `search` or `vector`.
    public var unique: Bool
    /// Optional partial-index predicate: the index constrains only rows
    /// matching this filter (wire key `where`).
    public var whereClause: FilterExpr?
    /// Full-text search language — a Postgres `regconfig` name (e.g. `"english"`,
    /// `"simple"`, `"spanish"`). Only meaningful when `search` is true; absent
    /// behaves as `english` server-side.
    public var language: String?

    public init(
        name: String,
        fields: [String],
        search: Bool = false,
        vector: VectorIndexSpec? = nil,
        unique: Bool = false,
        whereClause: FilterExpr? = nil,
        language: String? = nil
    ) {
        self.name = name
        self.fields = fields
        self.search = search
        self.vector = vector
        self.unique = unique
        self.whereClause = whereClause
        self.language = language
    }

    enum CodingKeys: String, CodingKey {
        case name, fields, search, vector, unique
        case whereClause = "where"
        case language
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        name = try container.decode(String.self, forKey: .name)
        fields = try container.decode([String].self, forKey: .fields)
        search = try container.decodeIfPresent(Bool.self, forKey: .search) ?? false
        vector = try container.decodeIfPresent(VectorIndexSpec.self, forKey: .vector)
        unique = try container.decodeIfPresent(Bool.self, forKey: .unique) ?? false
        whereClause = try container.decodeIfPresent(FilterExpr.self, forKey: .whereClause)
        language = try container.decodeIfPresent(String.self, forKey: .language)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(name, forKey: .name)
        try container.encode(fields, forKey: .fields)
        if search {
            try container.encode(search, forKey: .search)
        }
        try container.encodeIfPresent(vector, forKey: .vector)
        if unique {
            try container.encode(unique, forKey: .unique)
        }
        try container.encodeIfPresent(whereClause, forKey: .whereClause)
        try container.encodeIfPresent(language, forKey: .language)
    }
}

// MARK: - TtlDef

/// Declarative document TTL (auto-expiry): a declared numeric `field` whose
/// value is each document's absolute epoch-ms expiry, with an optional
/// `defaultDurationMs` stamped at insert time when the document omits the
/// field. Mirrors rust-client `TtlDef` (camelCase wire; `defaultDurationMs`
/// omitted when nil).
public struct TtlDef: Equatable, Codable, Sendable {
    /// The declared numeric field holding each doc's epoch-ms expiry.
    public var field: String
    /// Stamped at insert time when the document omits `field`.
    public var defaultDurationMs: Int64?

    public init(field: String, defaultDurationMs: Int64? = nil) {
        self.field = field
        self.defaultDurationMs = defaultDurationMs
    }

    enum CodingKeys: String, CodingKey {
        case field, defaultDurationMs
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        field = try container.decode(String.self, forKey: .field)
        defaultDurationMs = try container.decodeIfPresent(Int64.self, forKey: .defaultDurationMs)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(field, forKey: .field)
        try container.encodeIfPresent(defaultDurationMs, forKey: .defaultDurationMs)
    }
}

// MARK: - TableDef

/// One table: fields, indexes, and opt-in per-row rules / TTL / defaults.
/// Mirrors rust-client `TableDef` — `indexes` omitted when nil, `ownerField` /
/// `collaboratorsField` / `ttl` / `updatedAtField` / `autoIncrementField` /
/// `authorize` omitted when nil, `defaults` and `computed` omitted when empty,
/// `softDelete` omitted when false.
public struct TableDef: Equatable, Codable, Sendable {
    /// Field name to declared type.
    public var fields: [String: FieldType]
    /// Declared indexes, if any.
    public var indexes: [IndexDef]?
    /// Opt-in per-row authorization: names a declared, string-compatible field
    /// whose value is the owning user's id. Server-enforced; clients only
    /// declare it.
    public var ownerField: String?
    /// Opt-in extension of `ownerField`: names a declared array-of-strings (or
    /// array-of-id) field whose values are additional user ids that may
    /// read/mutate the row (owner OR collaborator). May be declared alone.
    public var collaboratorsField: String?
    /// Declarative document TTL.
    public var ttl: TtlDef?
    /// Server-stamped last-write time (FM-36): names a declared `number` or
    /// `int64` field the server stamps with the current epoch-ms on every
    /// version-bumping write (insert, patch, replace, upsert — both branches,
    /// patchByQuery, cascade setNull), overwriting any client-supplied value.
    /// A `number` field takes a JSON number, an `int64` field a decimal string
    /// (the int64 wire convention). Must differ from `ttl.field`; no index is
    /// required on the field. Server-enforced; the client only declares it.
    public var updatedAtField: String?
    /// Server-assigned per-table monotonic counter (FM-37): names a declared
    /// `int64` field stamped with the next counter value on insert (and
    /// upsert's insert branch), overwriting any client-supplied value.
    /// Immutable after insert — a patch or replace that changes the stored
    /// value is rejected. Legal in a unique index (the ticket-number
    /// guarantee). Server-enforced; the client only declares it.
    public var autoIncrementField: String?
    /// Opt-in per-row authorization predicate: a `FilterExpr` over this table's
    /// declared doc fields and the principal's markers (`{"$user":true}` /
    /// `{"$email":true}`). Marker values are valid only here — client
    /// `.filter()` queries reject them. Server-enforced.
    public var authorize: FilterExpr?
    /// Field-level default values: applied to a NEW document
    /// (insert/replace/upsert-insert) when it omits the key; `patch` never
    /// re-applies. Literals the server validates at push time.
    public var defaults: [String: JSONValue]
    /// Computed fields (ENH-028): field name -> expression. The server (and
    /// the engine's write path) re-evaluates every entry over the final doc on
    /// every write and stores the result — a null result removes the key, and
    /// client-supplied values never survive (dropped before validation). The
    /// expression grammar is `Migrate.swift`'s `ValueExpr`, shared with the
    /// migrate `evalExpr` directive: one grammar type, two executions.
    /// Omitted from the wire when empty (the server's `BTreeMap::is_empty`).
    public var computed: [String: ValueExpr]
    /// Soft delete: `delete`/`deleteByQuery` rows on this table are stamped
    /// (`deleted_at`) instead of removed — invisible to every read and write
    /// lookup, restorable via the `undelete` mutation step.
    public var softDelete: Bool

    public init(
        fields: [String: FieldType],
        indexes: [IndexDef]? = nil,
        ownerField: String? = nil,
        collaboratorsField: String? = nil,
        ttl: TtlDef? = nil,
        updatedAtField: String? = nil,
        autoIncrementField: String? = nil,
        authorize: FilterExpr? = nil,
        defaults: [String: JSONValue] = [:],
        computed: [String: ValueExpr] = [:],
        softDelete: Bool = false
    ) {
        self.fields = fields
        self.indexes = indexes
        self.ownerField = ownerField
        self.collaboratorsField = collaboratorsField
        self.ttl = ttl
        self.updatedAtField = updatedAtField
        self.autoIncrementField = autoIncrementField
        self.authorize = authorize
        self.defaults = defaults
        self.computed = computed
        self.softDelete = softDelete
    }

    enum CodingKeys: String, CodingKey {
        case fields, indexes, ttl, updatedAtField, autoIncrementField, authorize, defaults
        case computed, ownerField, collaboratorsField, softDelete
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        fields = try container.decode([String: FieldType].self, forKey: .fields)
        indexes = try container.decodeIfPresent([IndexDef].self, forKey: .indexes)
        ownerField = try container.decodeIfPresent(String.self, forKey: .ownerField)
        collaboratorsField = try container.decodeIfPresent(
            String.self, forKey: .collaboratorsField
        )
        ttl = try container.decodeIfPresent(TtlDef.self, forKey: .ttl)
        updatedAtField = try container.decodeIfPresent(String.self, forKey: .updatedAtField)
        autoIncrementField = try container.decodeIfPresent(
            String.self, forKey: .autoIncrementField
        )
        authorize = try container.decodeIfPresent(FilterExpr.self, forKey: .authorize)
        defaults = try container.decodeIfPresent(
            [String: JSONValue].self, forKey: .defaults
        ) ?? [:]
        computed = try container.decodeIfPresent(
            [String: ValueExpr].self, forKey: .computed
        ) ?? [:]
        softDelete = try container.decodeIfPresent(Bool.self, forKey: .softDelete) ?? false
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(fields, forKey: .fields)
        try container.encodeIfPresent(indexes, forKey: .indexes)
        try container.encodeIfPresent(ownerField, forKey: .ownerField)
        try container.encodeIfPresent(collaboratorsField, forKey: .collaboratorsField)
        try container.encodeIfPresent(ttl, forKey: .ttl)
        try container.encodeIfPresent(updatedAtField, forKey: .updatedAtField)
        try container.encodeIfPresent(autoIncrementField, forKey: .autoIncrementField)
        try container.encodeIfPresent(authorize, forKey: .authorize)
        if !defaults.isEmpty {
            try container.encode(defaults, forKey: .defaults)
        }
        if !computed.isEmpty {
            try container.encode(computed, forKey: .computed)
        }
        if softDelete {
            try container.encode(softDelete, forKey: .softDelete)
        }
    }
}

// MARK: - SchemaDef

/// A whole schema: named tables. Pushed via `POST /admin/push-schema`.
public struct SchemaDef: Equatable, Codable, Sendable {
    /// Table name to definition.
    public var tables: [String: TableDef]

    public init(tables: [String: TableDef] = [:]) {
        self.tables = tables
    }

    enum CodingKeys: String, CodingKey {
        case tables
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        tables = try container.decode([String: TableDef].self, forKey: .tables)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(tables, forKey: .tables)
    }

    /// Start a `SchemaBuilder` for this definition type.
    public static func builder() -> SchemaBuilder {
        SchemaBuilder()
    }
}

/// Finished schema (alias for the wire type) — mirrors rust `pub type Schema`.
public typealias Schema = SchemaDef

// MARK: - SchemaDef: WireEncodable

/// `wireObject()` comes from `WireEncodable`'s Codable default implementation
/// (JSONValue.swift), like `Query` and `Transaction`.
extension SchemaDef: WireEncodable {}

// MARK: - TableBuilder

/// Fluent builder producing a `TableDef` — the Swift mirror of rust-client's
/// `TableBuilder`. A struct with value-semantics chaining (the Task 9 house
/// pattern): every method returns a NEW builder, so branching a chain never
/// shares state. `unique()` and `whereClause(_:)` configure the most recently
/// declared index (no-op before any index — mirroring rust).
public struct TableBuilder: Sendable {
    /// The accumulator, held as one field so its property names cannot collide
    /// with the like-named builder methods (the `TableQuery.Acc` pattern).
    private var acc: Acc

    /// Start an empty table.
    public init() {
        acc = Acc()
    }

    private func with(_ mutate: (inout Acc) -> Void) -> TableBuilder {
        var copy = self
        mutate(&copy.acc)
        return copy
    }

    /// Declare `name` with type `type`.
    public func field(_ name: String, _ type: FieldType) -> TableBuilder {
        with { $0.fields[name] = type }
    }

    /// Declare a btree index over `fields`. Chain `.unique()` / `.whereClause(_)`
    /// to refine it.
    public func index(_ name: String, on fields: [String]) -> TableBuilder {
        with {
            $0.indexes.append(IndexDef(name: name, fields: fields))
            $0.lastIndex = $0.indexes.count - 1
        }
    }

    /// Declare a full-text search index. The server tsvectorizes the (text)
    /// `fields` and ranks matches via the `search` query terminal. Pass
    /// `language` to override the server's default (`english`) Postgres
    /// `regconfig`; nil omits the field on the wire.
    public func searchIndex(
        _ name: String, on fields: [String], language: String? = nil
    ) -> TableBuilder {
        with {
            $0.indexes.append(IndexDef(name: name, fields: fields, search: true, language: language))
            $0.lastIndex = $0.indexes.count - 1
        }
    }

    /// Declare a vector index over a `vector`-typed `field`. `filterFields`
    /// names scalar fields usable as eq-filters in `vectorSearch`; `metric`
    /// selects the distance metric (`.cosine` default, wire-omitted).
    public func vectorIndex(
        _ name: String,
        on field: String,
        dimensions: UInt32,
        filterFields: [String] = [],
        metric: DistanceMetric = .cosine
    ) -> TableBuilder {
        with {
            $0.indexes.append(IndexDef(
                name: name,
                fields: [field],
                vector: VectorIndexSpec(
                    dimensions: dimensions, filterFields: filterFields, metric: metric
                )
            ))
            $0.lastIndex = $0.indexes.count - 1
        }
    }

    /// Mark the most recently declared index as unique (`.index(...).unique()`):
    /// the server emits `CREATE UNIQUE INDEX` over the index's `fields`. May
    /// not combine with search or vector indexes (the server rejects that at
    /// push time). No-ops when no index has been declared yet.
    public func unique() -> TableBuilder {
        with {
            if let last = $0.lastIndex {
                $0.indexes[last].unique = true
            }
        }
    }

    /// Attach a partial-index predicate to the most recently declared index
    /// (`.index(...).whereClause(...)`); serialized as the wire key `where`.
    /// No-ops when no index has been declared yet.
    public func whereClause(_ predicate: FilterExpr) -> TableBuilder {
        with {
            if let last = $0.lastIndex {
                $0.indexes[last].whereClause = predicate
            }
        }
    }

    /// Declare the per-row owner field for authorization (wire `ownerField`):
    /// names a declared, string-compatible field whose value is the owning
    /// user's id. Server-enforced; the client only declares it.
    public func ownerField(_ field: String) -> TableBuilder {
        with { $0.ownerField = field }
    }

    /// Declare the per-row collaborators field (wire `collaboratorsField`):
    /// names a declared array-of-strings (or array-of-id) field whose values
    /// are additional user ids that may read/mutate the row (owner OR
    /// collaborator). May be declared alone.
    public func collaboratorsField(_ field: String) -> TableBuilder {
        with { $0.collaboratorsField = field }
    }

    /// Declare document TTL (auto-expiry): `field` names a declared numeric
    /// field whose value is each document's absolute epoch-ms expiry;
    /// `defaultDurationMs` stamps the field at insert time when the caller
    /// omits it. Mirrors the rust/TS chainable `.ttl(field, defaultDurationMs?)`.
    public func ttl(_ field: String, defaultDurationMs: Int64? = nil) -> TableBuilder {
        with { $0.ttl = TtlDef(field: field, defaultDurationMs: defaultDurationMs) }
    }

    /// Declare the server-stamped last-write field (wire `updatedAtField`):
    /// `field` names a declared `number` or `int64` field the server stamps
    /// with the current epoch-ms on every version-bumping write (insert,
    /// patch, replace, upsert — both branches, patchByQuery, cascade
    /// setNull), overwriting any client-supplied value. Must differ from
    /// `ttl.field`. Server-enforced; the client only declares it.
    public func updatedAtField(_ field: String) -> TableBuilder {
        with { $0.updatedAtField = field }
    }

    /// Declare the server-assigned per-table counter field (wire
    /// `autoIncrementField`): `field` names a declared `int64` field the
    /// server stamps with the next counter value on insert (and upsert's
    /// insert branch), overwriting any client-supplied value. Immutable
    /// after insert — a patch or replace that changes the stored value is
    /// rejected. Must differ from `ttl.field` and `updatedAtField`.
    /// Server-enforced; the client only declares it.
    public func autoIncrementField(_ field: String) -> TableBuilder {
        with { $0.autoIncrementField = field }
    }

    /// Declare the per-row authorization predicate: a `FilterExpr` over this
    /// table's declared doc fields and the principal's markers
    /// (`{"$user":true}` / `{"$email":true}`). Marker values are valid only
    /// here — client `.filter()` queries reject them. Server-enforced.
    public func authorize(_ predicate: FilterExpr) -> TableBuilder {
        with { $0.authorize = predicate }
    }

    /// Declare a computed field (ENH-028, wire `computed`): `name` names a
    /// declared field the server re-derives from `expr` over the final doc on
    /// every write — a null result removes the key, and client-supplied values
    /// never survive. The expression may reference only declared non-computed
    /// fields (push validation rejects anything else). Re-declaring the same
    /// name overwrites the expression (map-insert semantics).
    public func computed(_ name: String, _ expr: ValueExpr) -> TableBuilder {
        with { $0.computed[name] = expr }
    }

    /// Declare field-level default values. Each key must name a declared field
    /// and its value a non-null literal satisfying that field's type (the
    /// server validates at push time). Server-stamped values (ttl default,
    /// updatedAtField, ownerField) win over a default on the same field.
    public func defaults(_ values: [String: JSONValue]) -> TableBuilder {
        with { $0.defaults.merge(values) { _, new in new } }
    }

    /// Declare soft delete: rows on this table are stamped (`deleted_at`)
    /// instead of removed on delete — invisible to every read and write
    /// lookup, restorable via the `undelete` mutation step. Wire-omitted when
    /// disabled.
    public func softDelete(_ enabled: Bool = true) -> TableBuilder {
        with { $0.softDelete = enabled }
    }

    /// Consume into the finished `TableDef` (internal: reached through
    /// `SchemaBuilder.table`; `@testable` tests mirror rust's direct
    /// `.finish()` fixtures).
    func finish() -> TableDef {
        TableDef(
            fields: acc.fields,
            indexes: acc.indexes.isEmpty ? nil : acc.indexes,
            ownerField: acc.ownerField,
            collaboratorsField: acc.collaboratorsField,
            ttl: acc.ttl,
            updatedAtField: acc.updatedAtField,
            autoIncrementField: acc.autoIncrementField,
            authorize: acc.authorize,
            defaults: acc.defaults,
            computed: acc.computed,
            softDelete: acc.softDelete
        )
    }
}

/// The builder's field accumulator (see `TableBuilder.acc`).
private struct Acc: Sendable {
    var fields: [String: FieldType] = [:]
    var indexes: [IndexDef] = []
    var ownerField: String?
    var collaboratorsField: String?
    var ttl: TtlDef?
    var updatedAtField: String?
    var autoIncrementField: String?
    var authorize: FilterExpr?
    var defaults: [String: JSONValue] = [:]
    var computed: [String: ValueExpr] = [:]
    var softDelete = false
    /// Position of the most recently declared index, for the chainable
    /// `unique()` / `whereClause(_:)` setters. nil until the first index.
    var lastIndex: Int?
}

/// Convenience alias used in builder closures for readability — mirrors rust
/// `pub type Table = TableBuilder`.
public typealias Table = TableBuilder

// MARK: - SchemaBuilder

/// Fluent builder producing a `SchemaDef` — value-semantics chaining like
/// `TableBuilder`. `table` receives a fresh `TableBuilder` and returns the
/// configured one (`(TableBuilder) -> TableBuilder`, the value-semantics
/// closure shape; the brief's `(TableBuilder) -> Void` sketch predates the
/// house pattern and would silently discard every chained call).
public struct SchemaBuilder: Sendable {
    private let tables: [String: TableDef]

    /// Start an empty schema.
    public init() {
        tables = [:]
    }

    private init(tables: [String: TableDef]) {
        self.tables = tables
    }

    /// Add a table under `name`, configured by the `build` closure. Re-declaring
    /// a name overwrites (map-insert semantics, mirroring rust's BTreeMap).
    public func table(_ name: String, _ build: (TableBuilder) -> TableBuilder) -> SchemaBuilder {
        var next = tables
        next[name] = build(TableBuilder()).finish()
        return SchemaBuilder(tables: next)
    }

    /// Finish to the wire `SchemaDef`.
    public func build() -> SchemaDef {
        SchemaDef(tables: tables)
    }
}
