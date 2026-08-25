// GENERATED transcription of wire-corpus/query-combinations.json — kept byte-parity
// with the JSON source via QueryCombinationsParityTests. Do not hand-edit without
// re-running the transcription; a mismatch fails that test loudly.

/// One rule from `wire-corpus/query-combinations.json`: either `forbid` (all listed
/// clauses present is an error) or `atMostOne` (more than one listed clause present is
/// an error) — never both.
struct QueryCombinationRule: Equatable, Codable, Sendable {
    let id: String
    let forbid: [String]?
    let atMostOne: [String]?
    let code: String
    let message: String
}

/// The table-driven replacement for the hand-written combination ladder (ENH-028
/// phase 2) — the swift mirror of `wire-corpus/query-combinations.json`, which every
/// runner (server + all four client in-memory engines) now shares as the single source
/// of truth for which read-`Query` clauses may not be set together.
enum QueryCombinationRules {
    /// Every clause name a rule can reference (wire camelCase).
    static let clauses: [String] = [
        "get", "index", "eq", "gt", "gte", "lt", "lte", "order", "take", "unique", "first",
        "count", "distinct", "aggregate", "paginate", "filter", "search", "vectorSearch",
        "hybridSearch"
    ]

    /// Rules in table order — order has no effect on which `code` a query fails with
    /// (every rule here is `BAD_REQUEST`), but is preserved for parity with the JSON.
    static let rules: [QueryCombinationRule] = [
        QueryCombinationRule(
            id: "terminal-exclusive",
            forbid: nil,
            atMostOne: ["aggregate", "count", "distinct", "first", "get", "paginate", "take", "unique"],
            code: "BAD_REQUEST",
            message: "only one terminal may be set"
        ),
        QueryCombinationRule(
            id: "search-mode-exclusive",
            forbid: nil,
            atMostOne: ["hybridSearch", "search", "vectorSearch"],
            code: "BAD_REQUEST",
            message: "only one search mode terminal may be set"
        ),
        QueryCombinationRule(
            id: "get-excludes-index",
            forbid: ["get", "index"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, "
                + "distinct, aggregate, paginate, filter, search, or vector search"
        ),
        QueryCombinationRule(
            id: "eq-excludes-get",
            forbid: ["eq", "get"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, "
                + "distinct, aggregate, paginate, filter, search, or vector search"
        ),
        QueryCombinationRule(
            id: "get-excludes-gt",
            forbid: ["get", "gt"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, "
                + "distinct, aggregate, paginate, filter, search, or vector search"
        ),
        QueryCombinationRule(
            id: "get-excludes-gte",
            forbid: ["get", "gte"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, "
                + "distinct, aggregate, paginate, filter, search, or vector search"
        ),
        QueryCombinationRule(
            id: "get-excludes-lt",
            forbid: ["get", "lt"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, "
                + "distinct, aggregate, paginate, filter, search, or vector search"
        ),
        QueryCombinationRule(
            id: "get-excludes-lte",
            forbid: ["get", "lte"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, "
                + "distinct, aggregate, paginate, filter, search, or vector search"
        ),
        QueryCombinationRule(
            id: "get-excludes-order",
            forbid: ["get", "order"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, "
                + "distinct, aggregate, paginate, filter, search, or vector search"
        ),
        QueryCombinationRule(
            id: "filter-excludes-get",
            forbid: ["filter", "get"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, "
                + "distinct, aggregate, paginate, filter, search, or vector search"
        ),
        QueryCombinationRule(
            id: "get-excludes-search",
            forbid: ["get", "search"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, "
                + "distinct, aggregate, paginate, filter, search, or vector search"
        ),
        QueryCombinationRule(
            id: "get-excludes-vectorSearch",
            forbid: ["get", "vectorSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, "
                + "distinct, aggregate, paginate, filter, search, or vector search"
        ),
        QueryCombinationRule(
            id: "get-excludes-hybridSearch",
            forbid: ["get", "hybridSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "get cannot be combined with index, eq, range bounds, order, take, unique, first, count, "
                + "distinct, aggregate, paginate, filter, search, or vector search"
        ),
        QueryCombinationRule(
            id: "order-excludes-unique",
            forbid: ["order", "unique"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "unique cannot be combined with take, order, distinct, or aggregate"
        ),
        QueryCombinationRule(
            id: "count-excludes-order",
            forbid: ["count", "order"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "count cannot be combined with order"
        ),
        QueryCombinationRule(
            id: "distinct-excludes-order",
            forbid: ["distinct", "order"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "distinct cannot be combined with order"
        ),
        QueryCombinationRule(
            id: "distinct-excludes-search",
            forbid: ["distinct", "search"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "distinct cannot be combined with search"
        ),
        QueryCombinationRule(
            id: "distinct-excludes-vectorSearch",
            forbid: ["distinct", "vectorSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "distinct cannot be combined with vector search"
        ),
        QueryCombinationRule(
            id: "distinct-excludes-hybridSearch",
            forbid: ["distinct", "hybridSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "distinct cannot be combined with hybrid search"
        ),
        QueryCombinationRule(
            id: "aggregate-excludes-order",
            forbid: ["aggregate", "order"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "aggregate cannot be combined with order"
        ),
        QueryCombinationRule(
            id: "aggregate-excludes-search",
            forbid: ["aggregate", "search"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "aggregate cannot be combined with search"
        ),
        QueryCombinationRule(
            id: "aggregate-excludes-vectorSearch",
            forbid: ["aggregate", "vectorSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "aggregate cannot be combined with vector search"
        ),
        QueryCombinationRule(
            id: "aggregate-excludes-hybridSearch",
            forbid: ["aggregate", "hybridSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "aggregate cannot be combined with hybrid search"
        ),
        QueryCombinationRule(
            id: "gt-excludes-gte",
            forbid: ["gt", "gte"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "gt and gte cannot both be set"
        ),
        QueryCombinationRule(
            id: "lt-excludes-lte",
            forbid: ["lt", "lte"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "lt and lte cannot both be set"
        ),
        QueryCombinationRule(
            id: "index-excludes-vectorSearch",
            forbid: ["index", "vectorSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "vectorSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "eq-excludes-vectorSearch",
            forbid: ["eq", "vectorSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "vectorSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "gt-excludes-vectorSearch",
            forbid: ["gt", "vectorSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "vectorSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "gte-excludes-vectorSearch",
            forbid: ["gte", "vectorSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "vectorSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "lt-excludes-vectorSearch",
            forbid: ["lt", "vectorSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "vectorSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "lte-excludes-vectorSearch",
            forbid: ["lte", "vectorSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "vectorSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "order-excludes-vectorSearch",
            forbid: ["order", "vectorSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "vectorSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "unique-excludes-vectorSearch",
            forbid: ["unique", "vectorSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "vectorSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "first-excludes-vectorSearch",
            forbid: ["first", "vectorSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "vectorSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "count-excludes-vectorSearch",
            forbid: ["count", "vectorSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "vectorSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "paginate-excludes-vectorSearch",
            forbid: ["paginate", "vectorSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "vectorSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "filter-excludes-vectorSearch",
            forbid: ["filter", "vectorSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "vectorSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "take-excludes-vectorSearch",
            forbid: ["take", "vectorSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "vectorSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "hybridSearch-excludes-index",
            forbid: ["hybridSearch", "index"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "hybridSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "eq-excludes-hybridSearch",
            forbid: ["eq", "hybridSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "hybridSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "gt-excludes-hybridSearch",
            forbid: ["gt", "hybridSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "hybridSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "gte-excludes-hybridSearch",
            forbid: ["gte", "hybridSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "hybridSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "hybridSearch-excludes-lt",
            forbid: ["hybridSearch", "lt"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "hybridSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "hybridSearch-excludes-lte",
            forbid: ["hybridSearch", "lte"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "hybridSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "hybridSearch-excludes-order",
            forbid: ["hybridSearch", "order"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "hybridSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "hybridSearch-excludes-take",
            forbid: ["hybridSearch", "take"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "hybridSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "hybridSearch-excludes-unique",
            forbid: ["hybridSearch", "unique"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "hybridSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "first-excludes-hybridSearch",
            forbid: ["first", "hybridSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "hybridSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "count-excludes-hybridSearch",
            forbid: ["count", "hybridSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "hybridSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "hybridSearch-excludes-paginate",
            forbid: ["hybridSearch", "paginate"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "hybridSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "filter-excludes-hybridSearch",
            forbid: ["filter", "hybridSearch"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "hybridSearch cannot be combined with any other terminal"
        ),
        QueryCombinationRule(
            id: "index-excludes-search",
            forbid: ["index", "search"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, "
                + "aggregate, paginate, filter, or vector search"
        ),
        QueryCombinationRule(
            id: "eq-excludes-search",
            forbid: ["eq", "search"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, "
                + "aggregate, paginate, filter, or vector search"
        ),
        QueryCombinationRule(
            id: "gt-excludes-search",
            forbid: ["gt", "search"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, "
                + "aggregate, paginate, filter, or vector search"
        ),
        QueryCombinationRule(
            id: "gte-excludes-search",
            forbid: ["gte", "search"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, "
                + "aggregate, paginate, filter, or vector search"
        ),
        QueryCombinationRule(
            id: "lt-excludes-search",
            forbid: ["lt", "search"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, "
                + "aggregate, paginate, filter, or vector search"
        ),
        QueryCombinationRule(
            id: "lte-excludes-search",
            forbid: ["lte", "search"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, "
                + "aggregate, paginate, filter, or vector search"
        ),
        QueryCombinationRule(
            id: "order-excludes-search",
            forbid: ["order", "search"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, "
                + "aggregate, paginate, filter, or vector search"
        ),
        QueryCombinationRule(
            id: "search-excludes-unique",
            forbid: ["search", "unique"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, "
                + "aggregate, paginate, filter, or vector search"
        ),
        QueryCombinationRule(
            id: "first-excludes-search",
            forbid: ["first", "search"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, "
                + "aggregate, paginate, filter, or vector search"
        ),
        QueryCombinationRule(
            id: "count-excludes-search",
            forbid: ["count", "search"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, "
                + "aggregate, paginate, filter, or vector search"
        ),
        QueryCombinationRule(
            id: "paginate-excludes-search",
            forbid: ["paginate", "search"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, "
                + "aggregate, paginate, filter, or vector search"
        ),
        QueryCombinationRule(
            id: "filter-excludes-search",
            forbid: ["filter", "search"],
            atMostOne: nil,
            code: "BAD_REQUEST",
            message: "search cannot be combined with index, eq, range bounds, order, unique, first, count, distinct, "
                + "aggregate, paginate, filter, or vector search"
        )
    ]
}

/// Returns the first rule (in table order) violated by the given set of present clause
/// names, or `nil` if the combination is legal.
func firstViolatedQueryCombinationRule(_ present: Set<String>) -> QueryCombinationRule? {
    for rule in QueryCombinationRules.rules {
        if let forbid = rule.forbid {
            if forbid.allSatisfy({ present.contains($0) }) {
                return rule
            }
        } else if let atMostOne = rule.atMostOne {
            let presentCount = atMostOne.count { present.contains($0) }
            if presentCount > 1 {
                return rule
            }
        }
    }
    return nil
}
