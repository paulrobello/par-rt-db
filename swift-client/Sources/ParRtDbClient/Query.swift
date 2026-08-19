import Foundation

/// Mirrors server/src/dsl.rs::Order (rust-client/src/query.rs::Order) —
/// lowercase `"asc"` | `"desc"`.
public enum Order: String, Codable, Sendable {
    case asc
    case desc
}

/// Mirrors server/src/dsl.rs::Paginate (rust-client/src/query.rs::Paginate) —
/// camelCase, unknown fields rejected; `cursor` omitted when nil.
public struct Paginate: Equatable, Codable, Sendable {
    /// Opaque cursor from a previous page; nil starts at the first page.
    public var cursor: String?
    /// Page size.
    public var numItems: UInt32

    public init(cursor: String? = nil, numItems: UInt32) {
        self.cursor = cursor
        self.numItems = numItems
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case cursor, numItems
    }

    public init(from decoder: Decoder) throws {
        try decoder.rejectUnknownKeys("Paginate", as: CodingKeys.self)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        cursor = try container.decodeIfPresent(String.self, forKey: .cursor)
        numItems = try container.decode(UInt32.self, forKey: .numItems)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encodeIfPresent(cursor, forKey: .cursor)
        try container.encode(numItems, forKey: .numItems)
    }
}

/// Mirrors server/src/dsl.rs::Query (rust-client/src/query.rs::Query) — the
/// wire `Query`: one table plus at most one read terminal. Single-word keys
/// carry no case convention; `vectorSearch`/`hybridSearch` are explicitly
/// camelCase. Unknown fields rejected. Omit rules: nil Optionals, false bool
/// terminals, and an empty `eq` are omitted from the wire form.
public struct Query: Equatable, Codable, Sendable {
    /// Table name.
    public var table: String
    /// Point-read terminal: the document id.
    public var get: String?
    /// Index name for eq/range access.
    public var index: String?
    /// Eq-prefix values bound to the index's leading fields (omitted when empty).
    public var eq: [JSONValue]
    /// Exclusive lower bound on the index field after the eq prefix.
    public var gt: JSONValue?
    /// Inclusive lower bound on the index field after the eq prefix.
    public var gte: JSONValue?
    /// Exclusive upper bound on the index field after the eq prefix.
    public var lt: JSONValue?
    /// Inclusive upper bound on the index field after the eq prefix.
    public var lte: JSONValue?
    /// Sort direction over the index.
    public var order: Order?
    /// `take(N)` terminal: first N rows.
    public var take: UInt32?
    /// `unique` terminal: the one matching row or null (error on >1).
    public var unique: Bool
    /// `first` terminal: the first matching row or null.
    public var first: Bool
    /// `count` terminal: number of matching rows.
    public var count: Bool
    /// `distinct` terminal: unique values of the index field after the eq prefix.
    public var distinct: Bool
    /// Aggregate terminal (SUM/AVG/MIN/MAX/COUNT; `groupBy` shifts to grouped).
    public var aggregate: AggregateSpec?
    /// Cursor-pagination terminal.
    public var paginate: Paginate?
    /// Additional db-side WHERE predicate; composes with index/order/take.
    public var filter: FilterExpr?
    /// Full-text search terminal (ranks by ts_rank over a search index).
    public var search: SearchQuery?
    /// Vector-similarity terminal (camelCase wire key; carries its own limit).
    public var vectorSearch: VectorSearchQuery?
    /// Hybrid terminal: RRF fusion of full-text and vector ranking.
    public var hybridSearch: HybridSearchQuery?

    public init(
        table: String, get: String? = nil, index: String? = nil, eq: [JSONValue] = [],
        gt: JSONValue? = nil, gte: JSONValue? = nil, lt: JSONValue? = nil,
        lte: JSONValue? = nil, order: Order? = nil, take: UInt32? = nil,
        unique: Bool = false, first: Bool = false, count: Bool = false,
        distinct: Bool = false, aggregate: AggregateSpec? = nil,
        paginate: Paginate? = nil, filter: FilterExpr? = nil,
        search: SearchQuery? = nil, vectorSearch: VectorSearchQuery? = nil,
        hybridSearch: HybridSearchQuery? = nil
    ) {
        self.table = table
        self.get = get
        self.index = index
        self.eq = eq
        self.gt = gt
        self.gte = gte
        self.lt = lt
        self.lte = lte
        self.order = order
        self.take = take
        self.unique = unique
        self.first = first
        self.count = count
        self.distinct = distinct
        self.aggregate = aggregate
        self.paginate = paginate
        self.filter = filter
        self.search = search
        self.vectorSearch = vectorSearch
        self.hybridSearch = hybridSearch
    }

    enum CodingKeys: String, CodingKey, CaseIterable {
        case table, get, index, eq, gt, gte, lt, lte, order, take
        case unique, first, count, distinct, aggregate, paginate, filter
        case search, vectorSearch, hybridSearch
    }

    public init(from decoder: Decoder) throws {
        try decoder.rejectUnknownKeys("Query", as: CodingKeys.self)
        let container = try decoder.container(keyedBy: CodingKeys.self)
        table = try container.decode(String.self, forKey: .table)
        get = try container.decodeIfPresent(String.self, forKey: .get)
        index = try container.decodeIfPresent(String.self, forKey: .index)
        eq = try container.decodeIfPresent([JSONValue].self, forKey: .eq) ?? []
        gt = try container.decodeIfPresent(JSONValue.self, forKey: .gt)
        gte = try container.decodeIfPresent(JSONValue.self, forKey: .gte)
        lt = try container.decodeIfPresent(JSONValue.self, forKey: .lt)
        lte = try container.decodeIfPresent(JSONValue.self, forKey: .lte)
        order = try container.decodeIfPresent(Order.self, forKey: .order)
        take = try container.decodeIfPresent(UInt32.self, forKey: .take)
        unique = try container.decodeIfPresent(Bool.self, forKey: .unique) ?? false
        first = try container.decodeIfPresent(Bool.self, forKey: .first) ?? false
        count = try container.decodeIfPresent(Bool.self, forKey: .count) ?? false
        distinct = try container.decodeIfPresent(Bool.self, forKey: .distinct) ?? false
        aggregate = try container.decodeIfPresent(AggregateSpec.self, forKey: .aggregate)
        paginate = try container.decodeIfPresent(Paginate.self, forKey: .paginate)
        filter = try container.decodeIfPresent(FilterExpr.self, forKey: .filter)
        search = try container.decodeIfPresent(SearchQuery.self, forKey: .search)
        vectorSearch = try container.decodeIfPresent(VectorSearchQuery.self, forKey: .vectorSearch)
        hybridSearch = try container.decodeIfPresent(HybridSearchQuery.self, forKey: .hybridSearch)
    }

    public func encode(to encoder: Encoder) throws {
        var container = encoder.container(keyedBy: CodingKeys.self)
        try container.encode(table, forKey: .table)
        try container.encodeIfPresent(get, forKey: .get)
        try container.encodeIfPresent(index, forKey: .index)
        if !eq.isEmpty {
            try container.encode(eq, forKey: .eq)
        }
        try container.encodeIfPresent(gt, forKey: .gt)
        try container.encodeIfPresent(gte, forKey: .gte)
        try container.encodeIfPresent(lt, forKey: .lt)
        try container.encodeIfPresent(lte, forKey: .lte)
        try container.encodeIfPresent(order, forKey: .order)
        try container.encodeIfPresent(take, forKey: .take)
        if unique {
            try container.encode(unique, forKey: .unique)
        }
        if first {
            try container.encode(first, forKey: .first)
        }
        if count {
            try container.encode(count, forKey: .count)
        }
        if distinct {
            try container.encode(distinct, forKey: .distinct)
        }
        try container.encodeIfPresent(aggregate, forKey: .aggregate)
        try container.encodeIfPresent(paginate, forKey: .paginate)
        try container.encodeIfPresent(filter, forKey: .filter)
        try container.encodeIfPresent(search, forKey: .search)
        try container.encodeIfPresent(vectorSearch, forKey: .vectorSearch)
        try container.encodeIfPresent(hybridSearch, forKey: .hybridSearch)
    }
}
