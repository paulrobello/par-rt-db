import Foundation

// MARK: - QueryTerminal

/// The terminal a query ran with — the discriminator for the server's untagged
/// `QueryResult` payload (server/src/dsl.rs). Rust's `parse_result` leans on
/// serde's type-directed discrimination; Swift Codable has none, so `parseResult`
/// takes the terminal and uses it to gate the payload's shape before decoding.
/// Mirrors `Query::terminal_name`'s set (plus the aggregate scalar/grouped split,
/// which are two `QueryResult` variants of one wire terminal).
public enum QueryTerminal: Equatable, Sendable {
    /// Point read — object or null.
    case get
    /// Collect-all (the default when no terminal is set) — array.
    case collect
    /// `take(N)` — array.
    case take
    /// The one matching row or null — object or null.
    case unique
    /// First matching row or null — object or null.
    case first
    /// Number of matching rows — number.
    case count
    /// Unique values of the index field after the eq prefix — array of scalars.
    case distinct
    /// Aggregate scalar — bare number/string/null (null when no rows match).
    case aggregate
    /// Grouped aggregate (`aggregate(groupBy: true)`) — `[{key, value}]` rows.
    case aggregateGroups
    /// Cursor pagination — `{docs, nextCursor?}`.
    case paginate
    /// Full-text search — array.
    case search
    /// Vector-similarity search — array.
    case vectorSearch
    /// Hybrid (RRF) search — array.
    case hybridSearch
}

// MARK: - Paginated<T>

/// One page of cursor-paginated results. Wire keys are `docs`/`nextCursor`
/// (server/src/dsl.rs::PaginatedResult; the ts/rust/python clients decode the
/// same), so the CodingKeys map is deliberately `docs` while the property is
/// `items` — the name this SDK's builder-facing surface uses. `docs` is
/// required on the wire; `nextCursor` is omitted when nil (last page).
public struct Paginated<T: Codable & Sendable>: Codable, Sendable {
    /// This page's rows (wire key `docs`).
    public var items: [T]
    /// Cursor for the next page; nil when exhausted.
    public var nextCursor: String?

    public init(items: [T] = [], nextCursor: String? = nil) {
        self.items = items
        self.nextCursor = nextCursor
    }

    private enum CodingKeys: String, CodingKey {
        case docs
        case nextCursor
    }

    public init(from decoder: Decoder) throws {
        let container = try decoder.container(keyedBy: CodingKeys.self)
        items = try container.decode([T].self, forKey: .docs)
        nextCursor = try container.decodeIfPresent(String.self, forKey: .nextCursor)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(items, forKey: .docs)
        try container.encodeIfPresent(nextCursor, forKey: .nextCursor)
    }
}

extension Paginated: Equatable where T: Equatable {}

// MARK: - AggregateGroup

/// One `{key, value}` row from a grouped `aggregate` terminal. Mirrors
/// server/src/dsl.rs::AggregateGroup (camelCase; single-word keys). No
/// unknown-key rejection: the server type carries no `deny_unknown_fields`
/// and the rust mirror is `#[non_exhaustive]`.
public struct AggregateGroup: Codable, Equatable, Sendable {
    /// The group's key value (the index field after the eq prefix).
    public var key: JSONValue
    /// The group's aggregate (the field after that).
    public var value: JSONValue

    public init(key: JSONValue, value: JSONValue) {
        self.key = key
        self.value = value
    }
}

// MARK: - TableQuery

/// Fluent builder over the wire `Query`. Every method returns a copy with one
/// more clause set; `build()` validates terminal combinations (ported from the
/// server's cascade so a combination the server rejects fails client-side with
/// the same BadRequest message) and constructs the wire struct.
///
/// Deviations from the task brief, forced by the shipped wire structs:
/// `aggregate(groupBy:)` is a `Bool` (the wire `AggregateSpec.groupBy` is a
/// bool; a `[String]?` cannot be represented), and `vectorSearch`/`hybridSearch`
/// take a required `limit: Int` (the wire structs require `limit`; there is no
/// server default to omit it to).
public struct TableQuery: Sendable {
    /// The accumulator, held as one field so its property names (`count`,
    /// `first`, …) cannot collide with the like-named builder methods.
    /// `build()` is the only constructor of the wire Query, so numeric
    /// terminals stay Int here and are converted (exactly, or thrown) at build
    /// time rather than trapped at method time.
    private var acc: Acc

    public init(_ table: String) {
        acc = Acc(table: table)
    }

    private func with(_ mutate: (inout Acc) -> Void) -> TableQuery {
        var copy = self
        mutate(&copy.acc)
        return copy
    }

    /// Point-read terminal: the document id. Excludes every other clause.
    public func get(_ id: String) -> TableQuery {
        with { $0.get = id }
    }

    /// Use `index` for eq/range access.
    public func withIndex(_ name: String) -> TableQuery {
        with { $0.index = name }
    }

    /// Eq-prefix values bound to the index's leading fields (one per field).
    public func eq(_ values: JSONValue...) -> TableQuery {
        with { $0.eq = values }
    }

    /// Exclusive lower bound on the index field after the eq prefix.
    public func gt(_ value: JSONValue) -> TableQuery {
        with { $0.gt = value }
    }

    /// Inclusive lower bound on the index field after the eq prefix.
    public func gte(_ value: JSONValue) -> TableQuery {
        with { $0.gte = value }
    }

    /// Exclusive upper bound on the index field after the eq prefix.
    public func lt(_ value: JSONValue) -> TableQuery {
        with { $0.lt = value }
    }

    /// Inclusive upper bound on the index field after the eq prefix.
    public func lte(_ value: JSONValue) -> TableQuery {
        with { $0.lte = value }
    }

    /// Sort direction over the index.
    public func order(_ direction: Order) -> TableQuery {
        with { $0.order = direction }
    }

    /// `take(N)` terminal: first N rows. Composes with `search`; conflicts with
    /// every other terminal.
    public func take(_ limit: Int) -> TableQuery {
        with { $0.take = limit }
    }

    /// Additional db-side WHERE predicate over doc fields. Composes with
    /// index/order/take; conflicts with the ranked-search terminals (which take
    /// their own nested `filter` argument instead).
    public func filter(_ expr: FilterExpr) -> TableQuery {
        with { $0.filter = expr }
    }

    /// Collect-all terminal — the default when no terminal is set, so this is
    /// an explicit no-op on the wire (the Query struct has no `collect` field).
    public func collect() -> TableQuery {
        with { _ in }
    }

    /// `unique` terminal: the one matching row or null (error on >1).
    public func unique() -> TableQuery {
        with { $0.unique = true }
    }

    /// `first` terminal: the first matching row or null.
    public func first() -> TableQuery {
        with { $0.first = true }
    }

    /// `count` terminal: number of matching rows.
    public func count() -> TableQuery {
        with { $0.count = true }
    }

    /// Distinct-values terminal: unique values of the index field after the eq
    /// prefix. Server rejects when no index is set or the eq prefix consumes
    /// every index field.
    public func distinct() -> TableQuery {
        with { $0.distinct = true }
    }

    /// Aggregate terminal: `op` over the index field after the eq prefix.
    /// `groupBy: true` shifts to a grouped aggregate returning `[{key, value}]`.
    public func aggregate(_ op: AggregateOp, groupBy: Bool = false) -> TableQuery {
        with { $0.aggregate = AggregateSpec(op: op, groupBy: groupBy) }
    }

    /// Cursor-pagination terminal. Pass the previous page's `nextCursor` (nil
    /// starts at the first page) and the page size.
    public func paginate(cursor: String? = nil, numItems: Int) -> TableQuery {
        with {
            $0.paginateCursor = cursor
            $0.paginateNumItems = numItems
        }
    }

    /// Full-text `search` terminal over a declared search index. Composes only
    /// with `take`. `filter` here is NESTED on the terminal — distinct from the
    /// top-level `.filter()` builder, which the server rejects alongside
    /// `search`. `mode: .trgm` opts into substring matching; `snippet: true`
    /// attaches a `_searchSnippet` highlight per hit.
    public func search(
        _ index: String,
        _ query: String,
        filter: FilterExpr? = nil,
        mode: SearchMode? = nil,
        snippet: Bool? = nil
    ) -> TableQuery {
        with {
            $0.search = SearchQuery(
                index: index, query: query, filter: filter, mode: mode, snippet: snippet
            )
        }
    }

    /// Vector-similarity `vectorSearch` terminal. Standalone: carries its own
    /// `limit` and conflicts with every other terminal. `filter` is nested on
    /// the terminal (the top-level `.filter()` builder conflicts with it).
    public func vectorSearch(
        _ index: String,
        _ vector: [Double],
        limit: Int,
        filter: FilterExpr? = nil
    ) -> TableQuery {
        with {
            $0.vectorSearch = VectorSearchArgs(
                index: index, vector: vector, limit: limit, filter: filter
            )
        }
    }

    /// Hybrid `hybridSearch` terminal: RRF fusion of full-text and vector
    /// ranking over the same table. Standalone, like `vectorSearch`.
    /// `searchIndex`/`vectorIndex` auto-select server-side when nil; `k` is the
    /// RRF constant (server default 60) when nil.
    public func hybridSearch(
        _ query: String,
        _ vector: [Double],
        limit: Int,
        searchIndex: String? = nil,
        vectorIndex: String? = nil,
        k: Int? = nil
    ) -> TableQuery {
        with {
            $0.hybridSearch = HybridSearchArgs(
                query: query, vector: vector, limit: limit,
                searchIndex: searchIndex, vectorIndex: vectorIndex, k: k
            )
        }
    }

    /// Field projection: keep only these user fields per result doc.
    /// `_`-prefixed keys — the system fields (`_id`/`_creationTime`/
    /// `_version`) plus synthetic result fields like `_searchSnippet` — are
    /// always kept. An empty list is meaningful (the system-fields-only view);
    /// not calling this is full docs. Composes with every terminal (doc-less
    /// terminals — count/distinct/aggregate — ignore it). Each name must be a
    /// table field or one of the three system fields; anything else is
    /// BAD_REQUEST at execution.
    public func fields(_ names: String...) -> TableQuery {
        with { $0.fields = names }
    }

    /// Validate terminal combinations (the server cascade's rules and messages,
    /// ported verbatim) and construct the wire `Query`.
    public func build() throws -> Query {
        try validateTerminalCombination()
        return try Query(
            table: acc.table,
            get: acc.get,
            index: acc.index,
            eq: acc.eq,
            gt: acc.gt,
            gte: acc.gte,
            lt: acc.lt,
            lte: acc.lte,
            order: acc.order,
            take: acc.take.map { try uint32($0, "take") },
            unique: acc.unique,
            first: acc.first,
            count: acc.count,
            distinct: acc.distinct,
            aggregate: acc.aggregate,
            paginate: wirePaginate(),
            filter: acc.filter,
            search: acc.search,
            vectorSearch: wireVectorSearch(),
            hybridSearch: wireHybridSearch(),
            fields: acc.fields
        )
    }

    private func wirePaginate() throws -> Paginate? {
        try acc.paginateNumItems.map { numItems in
            try Paginate(cursor: acc.paginateCursor, numItems: uint32(numItems, "paginate numItems"))
        }
    }

    private func wireVectorSearch() throws -> VectorSearchQuery? {
        try acc.vectorSearch.map { args in
            try VectorSearchQuery(
                index: args.index, vector: args.vector,
                limit: uint32(args.limit, "vectorSearch limit"), filter: args.filter
            )
        }
    }

    private func wireHybridSearch() throws -> HybridSearchQuery? {
        try acc.hybridSearch.map { args in
            try HybridSearchQuery(
                query: args.query, vector: args.vector,
                limit: uint32(args.limit, "hybridSearch limit"),
                searchIndex: args.searchIndex, vectorIndex: args.vectorIndex,
                k: args.k.map { try uint32($0, "hybridSearch k") }
            )
        }
    }
}

/// The builder's field accumulator (see `TableQuery.acc`).
private struct Acc: Sendable {
    var table: String
    var get: String?
    var index: String?
    var eq: [JSONValue] = []
    var gt: JSONValue?
    var gte: JSONValue?
    var lt: JSONValue?
    var lte: JSONValue?
    var order: Order?
    var take: Int?
    var unique = false
    var first = false
    var count = false
    var distinct = false
    var aggregate: AggregateSpec?
    var paginateCursor: String?
    var paginateNumItems: Int?
    var filter: FilterExpr?
    var search: SearchQuery?
    var vectorSearch: VectorSearchArgs?
    var hybridSearch: HybridSearchArgs?
    var fields: [String]?
}

/// Accumulator-only spec structs: keep the numeric fields as Int so `build()`
/// can reject out-of-UInt32-range values with a badRequest instead of trapping.
private struct VectorSearchArgs: Sendable {
    var index: String
    var vector: [Double]
    var limit: Int
    var filter: FilterExpr?
}

private struct HybridSearchArgs: Sendable {
    var query: String
    var vector: [Double]
    var limit: Int
    var searchIndex: String?
    var vectorIndex: String?
    var k: Int?
}

// MARK: - Terminal-combination validation (server cascade port)

/// A `Query` field that can participate in a combination-rejection rule —
/// port of server/src/query/mod.rs::Peer.
private enum Peer {
    case get
    case index
    case eq
    case gt
    case gte
    case lt
    case lte
    case order
    case take
    case unique
    case first
    case count
    case distinct
    case aggregate
    case paginate
    case filter
    case search
    case vectorSearch
    case hybridSearch
}

/// Verbatim server messages (server/src/query/mod.rs) so a combination the
/// server rejects produces the identical BadRequest message client-side.
private enum ConflictMessage {
    static let get = "get cannot be combined with index, eq, range bounds, order, take, "
        + "unique, first, count, distinct, aggregate, paginate, filter, search, or vector search"
    static let unique = "unique cannot be combined with take, order, distinct, or aggregate"
    static let vectorSearch = "vectorSearch cannot be combined with any other terminal"
    static let hybridSearch = "hybridSearch cannot be combined with any other terminal"
    static let search = "search cannot be combined with index, eq, range bounds, order, "
        + "unique, first, count, distinct, aggregate, paginate, filter, or vector search"
}

extension TableQuery {
    /// Per-peer (peer, message) tables in the server cascade's declaration
    /// order — first match wins, so the same combination yields the same
    /// message as the server.
    private static let firstIncompatibles: [(Peer, String)] = [
        (.unique, "first cannot be combined with unique"),
        (.take, "first cannot be combined with take"),
        (.distinct, "first cannot be combined with distinct"),
        (.aggregate, "first cannot be combined with aggregate")
    ]

    private static let countIncompatibles: [(Peer, String)] = [
        (.unique, "count cannot be combined with unique"),
        (.take, "count cannot be combined with take"),
        (.first, "count cannot be combined with first"),
        (.order, "count cannot be combined with order"),
        (.distinct, "count cannot be combined with distinct"),
        (.aggregate, "count cannot be combined with aggregate")
    ]

    private static let distinctIncompatibles: [(Peer, String)] = [
        (.get, "distinct cannot be combined with get"),
        (.take, "distinct cannot be combined with take"),
        (.unique, "distinct cannot be combined with unique"),
        (.first, "distinct cannot be combined with first"),
        (.count, "distinct cannot be combined with count"),
        (.aggregate, "distinct cannot be combined with aggregate"),
        (.order, "distinct cannot be combined with order"),
        (.paginate, "distinct cannot be combined with paginate"),
        (.search, "distinct cannot be combined with search"),
        (.vectorSearch, "distinct cannot be combined with vector search"),
        (.hybridSearch, "distinct cannot be combined with hybrid search")
    ]

    private static let aggregateIncompatibles: [(Peer, String)] = [
        (.get, "aggregate cannot be combined with get"),
        (.take, "aggregate cannot be combined with take"),
        (.unique, "aggregate cannot be combined with unique"),
        (.first, "aggregate cannot be combined with first"),
        (.count, "aggregate cannot be combined with count"),
        (.distinct, "aggregate cannot be combined with distinct"),
        (.order, "aggregate cannot be combined with order"),
        (.paginate, "aggregate cannot be combined with paginate"),
        (.search, "aggregate cannot be combined with search"),
        (.vectorSearch, "aggregate cannot be combined with vector search"),
        (.hybridSearch, "aggregate cannot be combined with hybrid search")
    ]

    private static let paginateIncompatibles: [(Peer, String)] = [
        (.get, "paginate cannot be combined with get"),
        (.count, "paginate cannot be combined with count"),
        (.distinct, "paginate cannot be combined with distinct"),
        (.aggregate, "paginate cannot be combined with aggregate"),
        (.unique, "paginate cannot be combined with unique"),
        (.first, "paginate cannot be combined with first"),
        (.take, "paginate cannot be combined with take")
    ]

    // The server's check sequence (server/src/query/terminals.rs
    // `compile_query`): get → unique → first → count → distinct → aggregate →
    // paginate → range-bound pairs → vectorSearch → hybridSearch → search.
    // MAX_TAKE is deliberately NOT enforced here — it is a server limit, not a
    // terminal-combination rule, and a client-side copy would drift.
    private func validateTerminalCombination() throws {
        if acc.get != nil {
            try rejectIfAnySet(
                [
                    .index, .eq, .gt, .gte, .lt, .lte, .order, .take, .unique, .first, .count,
                    .distinct, .aggregate, .paginate, .filter, .search, .vectorSearch, .hybridSearch
                ],
                message: ConflictMessage.get
            )
        }
        if acc.unique {
            try rejectIfAnySet(
                [.take, .order, .distinct, .aggregate], message: ConflictMessage.unique
            )
        }
        if acc.first {
            try rejectPerPeerSet(Self.firstIncompatibles)
        }
        if acc.count {
            try rejectPerPeerSet(Self.countIncompatibles)
        }
        if acc.distinct {
            try rejectPerPeerSet(Self.distinctIncompatibles)
        }
        if acc.aggregate != nil {
            try rejectPerPeerSet(Self.aggregateIncompatibles)
        }
        if acc.paginateNumItems != nil {
            try rejectPerPeerSet(Self.paginateIncompatibles)
        }
        try validateRangeBounds()
        try validateRankedTerminals()
    }

    /// The ranked-search terminals run their checks after the btree terminals
    /// (server cascade order): vectorSearch → hybridSearch → search.
    private func validateRankedTerminals() throws {
        if acc.vectorSearch != nil {
            try rejectIfAnySet(
                [
                    .index, .eq, .gt, .gte, .lt, .lte, .order, .unique, .first, .count, .distinct,
                    .aggregate, .paginate, .filter, .search, .take, .hybridSearch
                ],
                message: ConflictMessage.vectorSearch
            )
        }
        if acc.hybridSearch != nil {
            try rejectIfAnySet(
                [
                    .index, .eq, .gt, .gte, .lt, .lte, .order, .take, .unique, .first, .count,
                    .distinct, .aggregate, .paginate, .filter, .search, .vectorSearch
                ],
                message: ConflictMessage.hybridSearch
            )
        }
        if acc.search != nil {
            // take is deliberately absent — search composes with take.
            try rejectIfAnySet(
                [
                    .index, .eq, .gt, .gte, .lt, .lte, .order, .unique, .first, .count, .distinct,
                    .aggregate, .paginate, .filter, .vectorSearch, .hybridSearch
                ],
                message: ConflictMessage.search
            )
        }
    }

    /// The range-bound pairs (dsl.rs: `gt`/`gte` and `lt`/`lte` are each
    /// mutually exclusive within their side).
    private func validateRangeBounds() throws {
        if acc.gt != nil, acc.gte != nil {
            throw RtDbError(code: .badRequest, message: "gt and gte cannot both be set")
        }
        if acc.lt != nil, acc.lte != nil {
            throw RtDbError(code: .badRequest, message: "lt and lte cannot both be set")
        }
    }

    private func rejectIfAnySet(_ peers: [Peer], message: String) throws {
        if peers.contains(where: { isSet($0) }) {
            throw RtDbError(code: .badRequest, message: message)
        }
    }

    private func rejectPerPeerSet(_ entries: [(Peer, String)]) throws {
        for (peer, message) in entries where isSet(peer) {
            throw RtDbError(code: .badRequest, message: message)
        }
    }

    // swiftlint:disable:next cyclomatic_complexity
    private func isSet(_ peer: Peer) -> Bool {
        switch peer {
        case .get: acc.get != nil
        case .index: acc.index != nil
        case .eq: !acc.eq.isEmpty
        case .gt: acc.gt != nil
        case .gte: acc.gte != nil
        case .lt: acc.lt != nil
        case .lte: acc.lte != nil
        case .order: acc.order != nil
        case .take: acc.take != nil
        case .unique: acc.unique
        case .first: acc.first
        case .count: acc.count
        case .distinct: acc.distinct
        case .aggregate: acc.aggregate != nil
        case .paginate: acc.paginateNumItems != nil
        case .filter: acc.filter != nil
        case .search: acc.search != nil
        case .vectorSearch: acc.vectorSearch != nil
        case .hybridSearch: acc.hybridSearch != nil
        }
    }
}

// MARK: - parseResult

/// Decode the server's untagged `QueryResult` payload into the caller's type.
/// Ports rust-client `parse_result` (which leans on serde's type-directed
/// discrimination): the terminal both gates the payload's shape (array for
/// collect/take/distinct/ranked-search, object-or-null for get/unique/first,
/// number for count, bare scalar for aggregate, `{docs, nextCursor}` for
/// paginate) and documents what the caller should pass for `T` — `[Doc]` for
/// array terminals, `Doc?` for object-or-null, `Int` for count, `Double?` or
/// `JSONValue?` for an aggregate scalar, `[AggregateGroup]` for
/// `.aggregateGroups`, `Paginated<Doc>` for `.paginate`. Decode failures map to
/// `RtDbError` (internal, "invalid query result: …"), matching rust.
public func parseResult<T: Codable & Sendable>(
    _ value: JSONValue,
    terminal: QueryTerminal
) throws -> T {
    if let shapeError = resultShapeError(value, terminal: terminal) {
        throw shapeError
    }
    let data: Data
    do {
        data = try JSONEncoder().encode(value)
    } catch {
        throw RtDbError(code: .internal, message: "invalid query result: \(error)")
    }
    do {
        return try JSONDecoder().decode(T.self, from: data)
    } catch {
        throw RtDbError(code: .internal, message: "invalid query result: \(error)")
    }
}

/// Shape gate for `parseResult` — nil when `value` matches the terminal's
/// `QueryResult` variant, an `RtDbError` describing the mismatch otherwise.
private func resultShapeError(_ value: JSONValue, terminal: QueryTerminal) -> RtDbError? {
    func invalid(_ expected: String) -> RtDbError {
        RtDbError(
            code: .internal,
            message: "invalid query result: \(terminal) expects \(expected), got \(describe(value))"
        )
    }
    switch terminal {
    case .get, .unique, .first:
        return (value == .null || value.objectValue != nil) ? nil : invalid("an object or null")
    case .collect, .take, .search, .vectorSearch, .hybridSearch, .distinct, .aggregateGroups:
        if case .array = value {
            return nil
        }
        return invalid("an array")
    case .count:
        return value.doubleValue != nil ? nil : invalid("a number")
    case .aggregate:
        switch value {
        case .int, .double, .string, .null: return nil
        default: return invalid("a scalar (number, string, or null)")
        }
    case .paginate:
        if case let .object(object) = value, case .array? = object["docs"] {
            return nil
        }
        return invalid("an object with a docs array")
    }
}

private func describe(_ value: JSONValue) -> String {
    switch value {
    case .null: "null"
    case .bool: "a boolean"
    case .int, .double: "a number"
    case .string: "a string"
    case .array: "an array"
    case .object: "an object"
    }
}

// MARK: - Query: WireEncodable

/// `wireObject()` comes from `WireEncodable`'s Codable default implementation
/// (JSONValue.swift) — the Task 8 helper generalized in Task 9. The encoding
/// and its error message are unchanged.
extension Query: WireEncodable {}
