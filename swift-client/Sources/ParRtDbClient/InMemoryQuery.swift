import Foundation

// MARK: - Limits

// Mirrors ts-client/src/in_memory/query.ts — the query engine for the
// in-memory engine: the `executeQuery` dispatcher, the per-terminal
// executors, and the index/cursor/aggregate/search helpers they share
// (mirrors rust-client/src/in_memory/query.rs).

/// Hard cap on rows one `take`/collect/search returns (server `MAX_TAKE`).
let maxQueryTake = 4096

// MARK: - Full-text helpers

/// Lowercase a value to FTS-indexable text (query.ts `ftsStringify`).
func ftsStringify(_ value: JSONValue) -> String {
    switch value {
    case .null: return ""
    case let .string(string): return string
    case let .int(int): return String(int)
    case let .double(double): return jsNumberString(double)
    case let .bool(bool): return bool ? "true" : "false"
    case .array, .object:
        let data = (try? JSONEncoder().encode(value)) ?? Data()
        return String(data: data, encoding: .utf8) ?? ""
    }
}

/// Split text into lowercase word tokens — an approximation of the lexemes
/// `websearch_to_tsquery` produces (query.ts `ftsTokens`). Deliberately no
/// stemming/stopwords; exact `ts_rank` ordering is out of scope.
func ftsTokens(_ text: String) -> [String] {
    text.lowercased().split { !$0.isASCII || !isASCIIAlphaNumeric($0) }.map(String.init)
}

/// One `or`-separated alternative of a websearch-syntax query (query.ts
/// `WebsearchAlt`): positive terms/phrases that must ALL be present, plus the
/// terms/phrases that must be absent.
struct WebsearchAlt {
    var terms: [String] = []
    var phrases: [[String]] = []
    var excludedTerms: [String] = []
    var excludedPhrases: [[String]] = []
}

// swiftlint:disable cyclomatic_complexity function_body_length
/// Parse `websearch_to_tsquery` syntax (FM-31; query.ts
/// `parseWebsearchQuery`): quoted phrases require adjacency, a bare
/// case-insensitive `or` splits alternatives, `-term`/`-"phrase"` negates, and
/// remaining plain terms stay AND. Constructs Postgres expresses exactly
/// (stemming, stopword dropping, tsquery precedence) over-approximate.
func parseWebsearchQuery(_ query: String) -> [WebsearchAlt] {
    var alts = [WebsearchAlt()]
    let chars = Array(query)
    var index = 0
    while index < chars.count {
        let character = chars[index]
        if character == " " || character == "\t" || character == "\n" {
            index += 1
            continue
        }
        // Optional '-' prefix, then either a double-quoted phrase (when a
        // closing quote exists) or a bare whitespace-free token.
        let negated = character == "-"
        var tokenStart = index
        if negated {
            tokenStart += 1
        }
        guard tokenStart < chars.count else {
            index += 1 // a lone trailing '-': the regex finds no match here
            continue
        }
        var phraseClose: Int?
        if chars[tokenStart] == "\"" {
            phraseClose = chars[(tokenStart + 1)...].firstIndex(of: "\"")
        }
        if let close = phraseClose {
            let phrase = String(chars[(tokenStart + 1) ..< close])
            index = close + 1
            let words = ftsTokens(phrase)
            if !words.isEmpty {
                if negated {
                    alts[alts.count - 1].excludedPhrases.append(words)
                } else {
                    alts[alts.count - 1].phrases.append(words)
                }
            }
            continue
        }
        if chars[tokenStart] == " " || chars[tokenStart] == "\t" || chars[tokenStart] == "\n" {
            index += 1 // '-' followed by whitespace: no match at this position
            continue
        }
        var end = tokenStart
        while end < chars.count, chars[end] != " ", chars[end] != "\t", chars[end] != "\n" {
            end += 1
        }
        let token = String(chars[tokenStart ..< end])
        index = end
        if !negated, token.lowercased() == "or" {
            alts.append(WebsearchAlt())
            continue
        }
        let words = ftsTokens(token)
        if words.isEmpty {
            continue
        }
        if negated {
            alts[alts.count - 1].excludedTerms.append(contentsOf: words)
        } else {
            alts[alts.count - 1].terms.append(contentsOf: words)
        }
    }
    return alts
}

// swiftlint:enable cyclomatic_complexity function_body_length

/// True when `phrase` appears in `tokens` as a consecutive run (query.ts
/// `tokensContainRun`).
func tokensContainRun(_ tokens: [String], _ phrase: [String]) -> Bool {
    guard !phrase.isEmpty, tokens.count >= phrase.count else { return false }
    if phrase.isEmpty {
        return false
    }
    let last = tokens.count - phrase.count
    for start in 0 ... last {
        let window = tokens[start ..< (start + phrase.count)]
        if window == phrase[...] {
            return true
        }
    }
    return false
}

func altMatches(_ alt: WebsearchAlt, _ docTokens: [String]) -> Bool {
    for term in alt.excludedTerms where docTokens.contains(term) {
        return false
    }
    for phrase in alt.excludedPhrases where tokensContainRun(docTokens, phrase) {
        return false
    }
    for term in alt.terms where !docTokens.contains(term) {
        return false
    }
    for phrase in alt.phrases where !tokensContainRun(docTokens, phrase) {
        return false
    }
    if alt.terms.isEmpty, alt.phrases.isEmpty {
        // A pure-negation alternative mirrors `!term`: matches every doc its
        // exclusions don't rule out. A fully empty alternative (stray `or`)
        // matches nothing.
        return !alt.excludedTerms.isEmpty || !alt.excludedPhrases.isEmpty
    }
    return true
}

/// Server-fixed word bound for harness snippets — the ts_headline
/// `MaxWords=35` option the server pins for `snippet: true` (FM-31).
let snippetMaxWords = 35

/// Snippet stand-in for the server's `ts_headline` (query.ts
/// `buildSearchSnippet`): a window of <=35 original-case words around the
/// first matched term, each matched term wrapped in `<mark>`. Shape parity
/// only — never byte-compared to Postgres.
func buildSearchSnippet(_ source: String, _ matchTerms: Set<String>) -> String {
    let words = source.split { !$0.isASCII || !isASCIIAlphaNumeric($0) }.map(String.init)
    let first = words.firstIndex { matchTerms.contains($0.lowercased()) } ?? 0
    let start = max(0, first - 5)
    let window = words[start ..< min(start + snippetMaxWords, words.count)]
    return window.map { matchTerms.contains($0.lowercased()) ? "<mark>\($0)</mark>" : $0 }
        .joined(separator: " ")
}

// MARK: - Comparators

// swiftlint:disable cyclomatic_complexity function_body_length
/// `null`-sorts-last comparison for one sort key (query.ts
/// `compareIndexValues`). Numbers compare as doubles (JS has one number
/// type), strings by code-unit order, booleans false < true. When `pg` is
/// `int64`, decimal-string operands parse as Int64 so they sort numerically.
func compareIndexValues(_ left: JSONValue, _ right: JSONValue, _ pg: PgType?) -> Int {
    let leftNull = left == .null
    let rightNull = right == .null
    if leftNull, rightNull {
        return 0
    }
    if leftNull {
        return 1
    }
    if rightNull {
        return -1
    }
    if pg == .int64 {
        guard case let .string(leftString) = left, case let .string(rightString) = right,
              let leftInt = Int64(leftString), let rightInt = Int64(rightString) else { return 0 }
        if leftInt < rightInt {
            return -1
        }
        if leftInt > rightInt {
            return 1
        }
        return 0
    }
    switch (left, right) {
    case let (.int(first), .int(second)):
        if first < second {
            return -1
        }
        if first > second {
            return 1
        }
        return 0
    case let (.int(first), .double(second)), let (.double(second), .int(first)):
        let firstDouble = Double(first)
        if firstDouble < second {
            return -1
        }
        if firstDouble > second {
            return 1
        }
        return 0
    case let (.double(first), .double(second)):
        if first < second {
            return -1
        }
        if first > second {
            return 1
        }
        return 0
    case let (.string(first), .string(second)):
        if first < second {
            return -1
        }
        if first > second {
            return 1
        }
        return 0
    case let (.bool(first), .bool(second)):
        if first == second {
            return 0
        }
        return first ? -1 : 1 // false sorts before true
    default:
        // Mixed kinds (never produced by a typed scan): JS relational ops on
        // mixed types coerce to NaN and compare false both ways.
        return 0
    }
}

// swiftlint:enable cyclomatic_complexity function_body_length

/// A JSON number whose integral values collapse to `.int` — the exact form
/// both JS (`JSON.stringify`) and the Swift decoder (Int64-first) produce.
func jsonNumber(_ double: Double) -> JSONValue {
    if double.isFinite, double == double.rounded(), let int = Int64(exactly: double) {
        return .int(int)
    }
    return .double(double)
}

/// Applies one aggregate op over a non-empty value array (query.ts
/// `applyAggregate`). SUM/AVG require numeric entries; MIN/MAX order per
/// `compareIndexValues`. Over an int64 field, SUM/MIN/MAX project the exact
/// decimal string — the int64 wire convention the server casts to
/// (`OP("col")::text`), exact past 2^53 where a JSON number is an IEEE-754
/// double. AVG stays a number (a fractional mean has no int64 form —
/// documented f64).
func applyAggregate(_ op: AggregateOp, _ values: [JSONValue], _ pg: PgType?) -> JSONValue {
    switch op {
    case .count:
        return .int(Int64(values.count))
    case .sum:
        if pg == .int64 {
            // Exact decimal-string sum. Decimal is exact to 38 significant
            // digits — the same practical boundary as the other engines'
            // i128/BigInt/int accumulators (the server's numeric is
            // unbounded).
            let total = values.reduce(Decimal(0)) { acc, value in
                acc + (value.stringValue.flatMap { Decimal(string: $0) } ?? Decimal(0))
            }
            return .string(total.description)
        }
        let total = values.reduce(0.0) { $0 + ($1.doubleValue ?? 0) }
        return jsonNumber(total)
    case .avg:
        // An int64 field's values are decimal strings — `doubleValue` is nil
        // for those, so parse them explicitly instead of reducing to 0.
        let total = values.reduce(0.0) { acc, value in
            acc + (pg == .int64
                ? (value.stringValue.flatMap { Double($0) } ?? 0)
                : (value.doubleValue ?? 0))
        }
        return jsonNumber(total / Double(values.count))
    case .min:
        return values.dropFirst().reduce(values[0]) { best, next in
            compareIndexValues(best, next, pg) <= 0 ? best : next
        }
    case .max:
        return values.dropFirst().reduce(values[0]) { best, next in
            compareIndexValues(best, next, pg) >= 0 ? best : next
        }
    }
}

/// Resolves an index definition from a table, throwing the server-shaped
/// BAD_REQUEST when the name is unknown (query.ts `requireIndex`).
func requireIndex(_ tableDef: TableDef, _ name: String) throws -> IndexDef {
    guard let index = (tableDef.indexes ?? []).first(where: { $0.name == name }) else {
        throw RtDbError(code: .badRequest, message: "index '\(name)' not found")
    }
    return index
}

/// Merges a stored row with its system fields — a port of server `merge_doc`
/// (query.ts `mergeDoc`).
func mergeDoc(_ row: StoredRow) -> JSONValue {
    var merged = row.doc
    merged["_id"] = .string(row.id)
    merged["_creationTime"] = .int(row.createdAt)
    merged["_version"] = .int(row.version)
    return .object(merged)
}

// MARK: - Projection (Query.fields)

/// Validate a `Query.fields` projection against the table — a port of server
/// `validate_projection`: every name must be a declared field or one of the
/// system fields (`_id`/`_creationTime`/`_version` — always included, so
/// listing them is an allowed no-op). Anything else — including typo'd system
/// names and other `_`-prefixed names — is BAD_REQUEST. `[]` (system fields
/// only) validates trivially.
func validateProjection(_ tableDef: TableDef, _ fields: [String]) throws {
    let systemFields: Set = ["_id", "_creationTime", "_version"]
    for name in fields {
        if systemFields.contains(name) || tableDef.fields[name] != nil {
            continue
        }
        throw RtDbError(code: .badRequest, message: "unknown projection field '\(name)'")
    }
}

/// Apply `transform` to every result doc of a doc-bearing terminal — the
/// shared walker for the projection (server `project_result`) and the
/// subscription diff's `_version` strip (server `diff_canonical`). The
/// server discriminates by `QueryResult` variant; this engine's untagged
/// `JSONValue` cannot (an aggregate-group row is as much an object as a doc),
/// so the query's terminal drives the walk instead: doc-less terminals
/// (count/distinct/aggregate) return the result untouched; `paginate`
/// transforms `docs`; get/unique/first transform the object-or-null doc;
/// everything else (collect/take/search/vectorSearch/hybridSearch) is an
/// array of docs.
func mapResultDocs(
    _ query: Query, _ result: JSONValue, _ transform: (JSONValue) -> JSONValue
) -> JSONValue {
    if query.count || query.distinct || query.aggregate != nil {
        return result
    }
    if query.paginate != nil {
        guard case var .object(page) = result, case let .array(docs) = page["docs"] else {
            return result
        }
        page["docs"] = .array(docs.map(transform))
        return .object(page)
    }
    if query.get != nil || query.unique || query.first {
        return transform(result) // null passes through the transform's object guard
    }
    guard case let .array(docs) = result else {
        return result
    }
    return .array(docs.map(transform))
}

/// Apply a `Query.fields` projection to an executed result — a port of server
/// `project_result`: each result doc keeps its `_`-prefixed keys and the
/// listed user fields; every other user field is dropped. `_`-prefixed keys
/// are exactly the system fields plus synthetic result fields
/// (`_searchSnippet`) — user fields can never be `_`-prefixed (write
/// validation rejects them) — so this rule IS "system fields are always
/// kept". Doc-less terminals are unaffected by construction.
func projectedResult(_ query: Query, _ result: JSONValue, _ fields: [String]) -> JSONValue {
    mapResultDocs(query, result) { doc in
        guard case var .object(object) = doc else { return doc }
        for key in Array(object.keys) where !key.hasPrefix("_") && !fields.contains(key) {
            object.removeValue(forKey: key)
        }
        return .object(object)
    }
}

// MARK: - Scan plan

/// Everything the row scan needs besides the query itself (query.ts
/// `ScanPlan`): the resolved index, the type-checked eq prefix, and the
/// coerced range bounds. Produced once by `prepareScan`.
struct ScanPlan {
    var indexDef: IndexDef?
    var typedEq: [JSONValue] = []
    var rangeField: String?
    var rangeFieldPg: PgType?
    var gt: JSONValue?
    var gte: JSONValue?
    var lt: JSONValue?
    var lte: JSONValue?
}

// MARK: - Dispatcher

/// One-shot query execution — same shape as the HTTP client's `query`
/// (query.ts `executeQuery`). Projection seam (server `execute_query`):
/// validation runs before every early return so all terminals — including
/// `get` — reject unknown field names up front, and the projection is applied
/// at this one exit so one-shot queries, the initial subscribe push, and
/// every subscription re-run all see the projected shape.
func executeQuery(
    _ query: Query,
    _ tableDef: TableDef,
    _ rowsFor: (String) -> [String: StoredRow]
) throws -> JSONValue {
    if let fields = query.fields {
        try validateProjection(tableDef, fields)
    }
    let result = try executeQueryUnprojected(query, tableDef, rowsFor)
    if let fields = query.fields {
        return projectedResult(query, result, fields)
    }
    return result
}

/// The dispatcher under the projection seam: guards, standalone terminals,
/// then the shared scan -> per-terminal executors. Table access goes through
/// the lazy `rowsFor` accessor the engine core passes in. FM-33: stamped
/// (soft-deleted) rows are invisible to every terminal.
private func executeQueryUnprojected(
    _ query: Query,
    _ tableDef: TableDef,
    _ rowsFor: (String) -> [String: StoredRow]
) throws -> JSONValue {
    let eq = query.eq
    let hasRange = query.gt != nil || query.gte != nil || query.lt != nil || query.lte != nil

    if let id = query.get {
        return try executeGetTerminal(query, id: id, eq: eq, hasRange: hasRange, rowsFor: rowsFor)
    }

    try checkQueryCombinations(query)

    if let vectorSearch = query.vectorSearch {
        return try executeVectorSearchTerminal(
            query, vectorSearch: vectorSearch, tableDef: tableDef, eq: eq, hasRange: hasRange,
            rowsFor: rowsFor
        )
    }

    if query.hybridSearch != nil {
        return try executeHybridSearchTerminal(query, eq: eq, hasRange: hasRange)
    }

    if let search = query.search {
        return try executeSearchTerminal(
            query, search: search, tableDef: tableDef, eq: eq, hasRange: hasRange, rowsFor: rowsFor
        )
    }

    let plan = try prepareScan(query, tableDef, eq, hasRange)
    let filtered = fetchFilteredRows(query, plan, rowsFor, tableDef.fields)

    if query.count {
        return .int(Int64(filtered.count))
    }

    if query.distinct {
        return try executeDistinctTerminal(tableDef, plan.indexDef, plan.typedEq.count, filtered)
    }

    if let aggregate = query.aggregate {
        return try executeAggregateTerminal(
            aggregate, tableDef, plan.indexDef, plan.typedEq.count, filtered
        )
    }

    let dir = query.order ?? .asc
    let sorted = try sortFilteredRows(filtered, tableDef, plan, dir)

    if let paginate = query.paginate {
        return try executePaginateTerminal(paginate, tableDef, sorted, plan, dir)
    }

    return try executeCollectTerminal(query, sorted)
}

/// Conflicting-clause guards, table-driven from `wire-corpus/query-combinations.json`
/// (mirrored as `QueryCombinationRules` — ENH-028 phase 2). Runs before the
/// `vectorSearch`/`hybridSearch`/`search`/scan dispatch; `get` is handled earlier by
/// `executeGetTerminal` and is never present when this runs, so it never fires the
/// `get`-side rules here (they exist in the table for the other four runners' sake).
private func checkQueryCombinations(_ query: Query) throws {
    let flags: [(String, Bool)] = [
        ("get", query.get != nil),
        ("index", query.index != nil),
        ("eq", !query.eq.isEmpty),
        ("gt", query.gt != nil),
        ("gte", query.gte != nil),
        ("lt", query.lt != nil),
        ("lte", query.lte != nil),
        ("order", query.order != nil),
        ("take", query.take != nil),
        ("unique", query.unique),
        ("first", query.first),
        ("count", query.count),
        ("distinct", query.distinct),
        ("aggregate", query.aggregate != nil),
        ("paginate", query.paginate != nil),
        ("filter", query.filter != nil),
        ("search", query.search != nil),
        ("vectorSearch", query.vectorSearch != nil),
        ("hybridSearch", query.hybridSearch != nil)
    ]
    let present = Set(flags.filter(\.1).map(\.0))

    if let rule = firstViolatedQueryCombinationRule(present) {
        throw RtDbError(code: ErrorCode(rawValue: rule.code) ?? .badRequest, message: rule.message)
    }
    // Not a combination rule (no counterpart clause) — stays a standalone bound check.
    if let take = query.take, take > maxQueryTake {
        throw RtDbError(code: .badRequest, message: "take exceeds maximum of \(maxQueryTake)")
    }
}

// swiftlint:disable cyclomatic_complexity
/// Index resolution, eq-prefix binding, range-bound coercion, and one-time
/// filter validation (query.ts `prepareScan`).
private func prepareScan(
    _ query: Query, _ tableDef: TableDef, _ eq: [JSONValue], _ hasRange: Bool
) throws -> ScanPlan {
    var plan = ScanPlan()
    if let indexName = query.index {
        plan.indexDef = try requireIndex(tableDef, indexName)
    } else if !eq.isEmpty {
        throw RtDbError(code: .badRequest, message: "eq requires an index")
    }

    let eqLen = eq.count
    if let indexDef = plan.indexDef, eqLen > indexDef.fields.count {
        throw RtDbError(
            code: .badRequest,
            message: "index '\(indexDef.name)' expects at most \(indexDef.fields.count) eq "
                + "value(s), got \(eqLen)"
        )
    }
    // Type-check each eq prefix bind (server `eq_binds`).
    if let indexDef = plan.indexDef {
        plan.typedEq = try eq.enumerated().map { index, value in
            try coerceIndexValue(tableDef, indexDef.fields[index], value)
        }
    }

    if hasRange {
        guard let indexDef = plan.indexDef else {
            throw RtDbError(code: .badRequest, message: "range bound requires an index")
        }
        guard eqLen < indexDef.fields.count else {
            throw RtDbError(
                code: .badRequest,
                message: "range bound requires a remaining index field after eq"
            )
        }
        plan.rangeField = indexDef.fields[eqLen]
        plan.rangeFieldPg = try indexColumnType(requireFieldType(tableDef, indexDef.fields[eqLen])).pg
    }

    if let rangeField = plan.rangeField {
        if let gt = query.gt {
            plan.gt = try coerceIndexValue(tableDef, rangeField, gt)
        }
        if let gte = query.gte {
            plan.gte = try coerceIndexValue(tableDef, rangeField, gte)
        }
        if let lt = query.lt {
            plan.lt = try coerceIndexValue(tableDef, rangeField, lt)
        }
        if let lte = query.lte {
            plan.lte = try coerceIndexValue(tableDef, rangeField, lte)
        }
    }

    // Validate the filter against the table def once (mirrors server
    // compile_filter).
    if let filter = query.filter {
        try validateFilter(filter, tableDef)
    }
    return plan
}

// swiftlint:enable cyclomatic_complexity

// swiftlint:disable cyclomatic_complexity
/// Row fetch + filter (eq prefix -> range -> filter hook) — query.ts
/// `fetchFilteredRows`.
private func fetchFilteredRows(
    _ query: Query,
    _ plan: ScanPlan,
    _ rowsFor: (String) -> [String: StoredRow],
    _ fields: FieldMap
) -> [StoredRow] {
    var filtered: [StoredRow] = []
    for row in rowsFor(query.table).values {
        if row.deletedAt != nil {
            continue
        } // FM-33: stamped rows are invisible
        if let indexDef = plan.indexDef {
            var ok = true
            for (index, typed) in plan.typedEq.enumerated() {
                guard let value = row.doc[indexDef.fields[index]], value != .null,
                      jsonEq(value, typed)
                else {
                    ok = false
                    break
                }
            }
            if !ok {
                continue
            }
        }
        if let rangeField = plan.rangeField {
            guard let value = row.doc[rangeField], value != .null else { continue }
            let pg = plan.rangeFieldPg
            if let gt = plan.gt, compareIndexValues(value, gt, pg) <= 0 {
                continue
            }
            if let gte = plan.gte, compareIndexValues(value, gte, pg) < 0 {
                continue
            }
            if let lt = plan.lt, compareIndexValues(value, lt, pg) >= 0 {
                continue
            }
            if let lte = plan.lte, compareIndexValues(value, lte, pg) > 0 {
                continue
            }
        }
        if let filter = query.filter, !evalFilterExpr(filter, row.doc, fields) {
            continue
        }
        filtered.append(row)
    }
    return filtered
}

// swiftlint:enable cyclomatic_complexity

/// The sort column list every ordered terminal shares (query.ts
/// `sortKeysFor`): unbound index fields (after the eq prefix), then
/// `__createdAt`, then `__id`. `sortPgs[i]` is the storage type of
/// `sortKeys[i]` so the comparator can pick the int64 numeric path.
private func sortKeysFor(
    _ tableDef: TableDef, _ indexDef: IndexDef?, _ eqLen: Int
) throws -> (sortKeys: [String], sortPgs: [PgType]) {
    var sortKeys: [String] = []
    var sortPgs: [PgType] = []
    if let indexDef {
        for field in indexDef.fields.dropFirst(eqLen) {
            sortKeys.append(field)
            try sortPgs.append(indexColumnType(requireFieldType(tableDef, field)).pg)
        }
    }
    sortKeys.append("__createdAt")
    sortPgs.append(.number)
    sortKeys.append("__id")
    sortPgs.append(.text)
    return (sortKeys, sortPgs)
}

/// The declared field type, or INTERNAL — index fields are schema-validated
/// at push time, so a miss is an engine wiring bug.
private func requireFieldType(_ tableDef: TableDef, _ field: String) throws -> FieldType {
    guard let fieldType = tableDef.fields[field] else {
        throw RtDbError(code: .internal, message: "index references unknown field '\(field)'")
    }
    return fieldType
}

/// Sorts the filtered set by the shared sort columns in direction `dir`
/// (query.ts `sortFilteredRows`). The unique `__id` tiebreaker makes the
/// order total. Returns the sorted array (Swift arrays are values; the TS
/// sorts in place).
private func sortFilteredRows(
    _ filtered: [StoredRow], _ tableDef: TableDef, _ plan: ScanPlan, _ dir: Order
) throws -> [StoredRow] {
    let (sortKeys, sortPgs) = try sortKeysFor(tableDef, plan.indexDef, plan.typedEq.count)
    var rows = filtered
    rows.sort { left, right in
        for (index, field) in sortKeys.enumerated() {
            let cmp = compareIndexValues(
                sortValue(left, field), sortValue(right, field), sortPgs[index]
            )
            if cmp != 0 {
                return (dir == .desc ? -cmp : cmp) < 0
            }
        }
        return false
    }
    return rows
}

/// Sort value for a synthetic sort key, normalizing an absent optional index
/// field to `null` (query.ts `sortValue`).
private func sortValue(_ row: StoredRow, _ key: String) -> JSONValue {
    if key == "__createdAt" {
        return .int(row.createdAt)
    }
    if key == "__id" {
        return .string(row.id)
    }
    return row.doc[key] ?? .null
}

// MARK: - Terminals

/// `get` terminal: point read by id (query.ts `executeGetTerminal`).
private func executeGetTerminal(
    _ query: Query,
    id: String,
    eq: [JSONValue],
    hasRange: Bool,
    rowsFor: (String) -> [String: StoredRow]
) throws -> JSONValue {
    let combined = query.index != nil || !eq.isEmpty || hasRange || query.order != nil
        || query.take != nil || query.unique || query.first || query.count || query.distinct
        || query.aggregate != nil || query.paginate != nil || query.filter != nil
        || query.search != nil || query.vectorSearch != nil || query.hybridSearch != nil
    if combined {
        throw RtDbError(
            code: .badRequest,
            message: "get cannot be combined with index, eq, range bounds, order, take, unique, "
                + "first, count, distinct, aggregate, paginate, filter, search, or vector search"
        )
    }
    // FM-33: a soft-deleted row is absent to the get terminal.
    guard let row = rowsFor(query.table)[id], row.deletedAt == nil else {
        return .null
    }
    return mergeDoc(row)
}

// swiftlint:disable function_parameter_count
/// `vectorSearch` terminal (query.ts `executeVectorSearchTerminal`):
/// filter-narrowed candidates — the in-memory engine does not rank by vector
/// distance; rows return in insertion order as a deterministic stand-in.
private func executeVectorSearchTerminal(
    _ query: Query,
    vectorSearch: VectorSearchQuery,
    tableDef: TableDef,
    eq: [JSONValue],
    hasRange: Bool,
    rowsFor: (String) -> [String: StoredRow]
) throws -> JSONValue {
    let combined = query.index != nil || !eq.isEmpty || hasRange || query.order != nil
        || query.unique || query.first || query.count || query.distinct
        || query.aggregate != nil || query.filter != nil
        || query.search != nil || query.take != nil || query.hybridSearch != nil
    if combined {
        throw RtDbError(
            code: .badRequest,
            message: "vectorSearch cannot be combined with any other terminal"
        )
    }
    guard (tableDef.indexes ?? []).contains(where: { $0.name == vectorSearch.index && $0.vector != nil })
    else {
        throw RtDbError(
            code: .badRequest, message: "vector index '\(vectorSearch.index)' not found"
        )
    }
    // Validate the vector-search-level filter once (server compile_filter).
    if let filter = vectorSearch.filter {
        try validateFilter(filter, tableDef)
    }
    var candidates: [StoredRow] = []
    for row in rowsFor(query.table).values {
        if row.deletedAt != nil {
            continue
        } // FM-33: stamped rows are invisible
        if let filter = vectorSearch.filter, !evalFilterExpr(filter, row.doc, tableDef.fields) {
            continue
        }
        candidates.append(row)
    }

    if let paginate = query.paginate {
        // ENH-030: `limit` keeps its meaning — the size of the ranked candidate
        // POOL — and `numItems` slices that pool into pages, so paging delivers
        // the same candidates the unpaginated call returns, in instalments.
        //
        // Distance ordering is NOT modelled here, so the paged order is the
        // deterministic tie-breaker order (createdAt DESC, id DESC) rather than
        // a distance ranking. The pool is taken AFTER that sort, not before:
        // `rowsFor` hands back a Dictionary whose iteration order varies with
        // the per-process hash seed, so cutting the pool first would make the
        // page CONTENTS differ run to run whenever candidates exceed `limit`.
        candidates.sort { left, right in
            left.createdAt != right.createdAt
                ? left.createdAt > right.createdAt
                : left.id > right.id
        }
        let pool = candidates.prefix(Int(vectorSearch.limit))
        return try paginateRanked(
            paginate,
            pool.map {
                RankedRow(
                    sortValues: [.int($0.createdAt), .string($0.id)], doc: .object($0.doc)
                )
            },
            [.createdAt, .id]
        )
    }
    return .array(candidates.prefix(Int(vectorSearch.limit)).map { .object($0.doc) })
}

// swiftlint:enable function_parameter_count

/// `hybridSearch` terminal (query.ts `executeHybridSearchTerminal`): the
/// in-memory engine returns an empty result (no ts_rank + vector distance
/// fusion) rather than silently misranking.
private func executeHybridSearchTerminal(
    _ query: Query, eq: [JSONValue], hasRange: Bool
) throws -> JSONValue {
    let combined = query.index != nil || !eq.isEmpty || hasRange || query.order != nil
        || query.unique || query.first || query.count || query.distinct
        || query.aggregate != nil || query.filter != nil
        || query.search != nil || query.vectorSearch != nil || query.take != nil
    if combined {
        throw RtDbError(
            code: .badRequest,
            message: "hybridSearch cannot be combined with any other terminal"
        )
    }
    // ENH-030: `paginate` composes, but there is nothing to page — this engine
    // fuses no ranking, so every page of an empty result is an empty page, and
    // an empty page is by definition the last one (no `nextCursor`).
    if query.paginate != nil {
        return .object(["docs": .array([])])
    }
    return .array([])
}

// swiftlint:disable cyclomatic_complexity function_body_length function_parameter_count
/// `search` terminal (query.ts `executeSearchTerminal`): full-text matching
/// under websearch syntax (`tsquery` mode) or case-insensitive substring
/// matching (`trgm` mode), each with a deterministic relevance stand-in.
private func executeSearchTerminal(
    _ query: Query,
    search: SearchQuery,
    tableDef: TableDef,
    eq: [JSONValue],
    hasRange: Bool,
    rowsFor: (String) -> [String: StoredRow]
) throws -> JSONValue {
    let combined = query.index != nil || !eq.isEmpty || hasRange || query.order != nil
        || query.unique || query.first || query.count || query.distinct
        || query.aggregate != nil || query.filter != nil
        || query.vectorSearch != nil || query.hybridSearch != nil
    if combined {
        throw RtDbError(
            code: .badRequest,
            message: "search cannot be combined with index, eq, range bounds, order, unique, "
                + "first, count, distinct, aggregate, filter, or vector search"
        )
    }
    if search.query.trimmingCharacters(in: .whitespaces).isEmpty {
        throw RtDbError(code: .badRequest, message: "search query text must not be empty")
    }
    // SEC-007: bound the text handed to the search engine before any
    // compilation work happens. Mirrors server `MAX_SEARCH_QUERY_BYTES`,
    // capping the UTF-8 byte length (not the character count).
    if search.query.utf8.count > InMemoryLimits.maxSearchQueryBytes {
        throw RtDbError(
            code: .badRequest,
            message: "search query text must be at most \(InMemoryLimits.maxSearchQueryBytes) bytes"
        )
    }
    guard let searchDef = (tableDef.indexes ?? []).first(where: {
        $0.name == search.index && $0.search
    }) else {
        throw RtDbError(code: .badRequest, message: "search index '\(search.index)' not found")
    }
    // Validate the search-level filter once (server compile_filter).
    if let filter = search.filter {
        try validateFilter(filter, tableDef)
    }
    // `snippet` needs a tsquery tree to highlight; trgm matches raw
    // substrings, so the combination is rejected (server compile_search).
    let snippet = search.snippet == true
    if snippet, search.mode == .trgm {
        throw RtDbError(
            code: .badRequest, message: "snippet is only supported in tsquery mode"
        )
    }
    let limit = query.take.map(Int.init) ?? maxQueryTake

    struct Scored {
        var row: StoredRow
        var score: Double
        var snippet: String?
    }
    var scored: [Scored] = []
    if search.mode == .trgm {
        // A doc matches when ANY indexed field's lowercased text contains the
        // lowercased query as a substring. Similarity stand-in, pinned for
        // cross-client parity: query.length / field.length, max across fields.
        let needle = search.query.lowercased()
        for row in rowsFor(query.table).values {
            if row.deletedAt != nil {
                continue
            }
            if let filter = search.filter, !evalFilterExpr(filter, row.doc, tableDef.fields) {
                continue
            }
            var best = 0.0
            for field in searchDef.fields {
                let text = ftsStringify(row.doc[field] ?? .null).lowercased()
                if text.contains(needle) {
                    let similarity = Double(needle.count) / Double(max(1, text.count))
                    if similarity > best {
                        best = similarity
                    }
                }
            }
            if best > 0 {
                scored.append(Scored(row: row, score: best, snippet: nil))
            }
        }
    } else {
        let alts = parseWebsearchQuery(search.query)
        // Every positive lexeme across the alternatives — reused for scoring
        // and snippet highlights.
        var positives: Set<String> = []
        for alt in alts {
            positives.formUnion(alt.terms)
            positives.formUnion(alt.phrases.flatMap(\.self))
        }
        for row in rowsFor(query.table).values {
            if row.deletedAt != nil {
                continue
            }
            if let filter = search.filter, !evalFilterExpr(filter, row.doc, tableDef.fields) {
                continue
            }
            let source = searchDef.fields
                .map { ftsStringify(row.doc[$0] ?? .null) }
                .joined(separator: " ")
            let docTokens = ftsTokens(source)
            if !alts.contains(where: { altMatches($0, docTokens) }) {
                continue
            }
            var score = 0.0
            for token in docTokens where positives.contains(token) {
                score += 1
            }
            let built = snippet ? buildSearchSnippet(source, positives) : nil
            scored.append(Scored(row: row, score: score, snippet: built))
        }
    }
    scored.sort { first, second in
        if first.score != second.score {
            return first.score > second.score
        }
        if first.row.createdAt != second.row.createdAt {
            return first.row.createdAt > second.row.createdAt
        }
        return first.row.id > second.row.id
    }
    func resultDoc(_ scored: Scored) -> JSONValue {
        guard let snippetText = scored.snippet else { return mergeDoc(scored.row) }
        guard case var .object(doc) = mergeDoc(scored.row) else { return mergeDoc(scored.row) }
        doc["_searchSnippet"] = .string(snippetText)
        return .object(doc)
    }
    if let paginate = query.paginate {
        // ENH-030: page the ranked order the sort above just produced — score
        // DESC, createdAt DESC, id DESC — keyset-resuming over exactly those
        // three columns. `take` cannot be set alongside `paginate`, so `limit`
        // is the untouched MAX_TAKE default and the page size governs instead.
        return try paginateRanked(
            paginate,
            scored.map {
                RankedRow(
                    sortValues: [
                        jsonNumber($0.score), .int($0.row.createdAt), .string($0.row.id)
                    ],
                    doc: resultDoc($0)
                )
            },
            [.rank, .createdAt, .id]
        )
    }
    return .array(scored.prefix(limit).map(resultDoc))
}

// swiftlint:enable cyclomatic_complexity function_body_length function_parameter_count

/// `distinct` terminal (query.ts `executeDistinctTerminal`): unique values of
/// the index field after the eq prefix over the matching set. An absent
/// optional field is SQL NULL — one null entry, sorted after every value
/// (Postgres NULLS LAST).
private func executeDistinctTerminal(
    _ tableDef: TableDef, _ indexDef: IndexDef?, _ eqLen: Int, _ filtered: [StoredRow]
) throws -> JSONValue {
    guard let indexDef, eqLen < indexDef.fields.count else {
        throw RtDbError(
            code: .badRequest,
            message: "distinct requires an index field beyond the eq prefix"
        )
    }
    let field = indexDef.fields[eqLen]
    let fieldPg = try indexColumnType(requireFieldType(tableDef, field)).pg
    var seen = Set<JSONValue>()
    var values: [JSONValue] = []
    for row in filtered {
        let value = row.doc[field] ?? .null
        if !seen.contains(value) {
            seen.insert(value)
            values.append(value)
        }
    }
    values.sort { compareIndexValues($0, $1, fieldPg) < 0 }
    return .array(Array(values.prefix(maxQueryTake)))
}

/// One (alias, op) pair to evaluate. A single-`op` spec aliases its one
/// entry by the lowercase op name (matches the server/ts `aliased_ops`
/// convention; only load-bearing for the wire-v2 result shapes).
private struct AggOpAlias {
    let alias: String
    let op: AggregateOp
}

private let aggAliasCharset = CharacterSet(charactersIn: "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
    "abcdefghijklmnopqrstuvwxyz0123456789_")

// swiftlint:disable cyclomatic_complexity function_body_length
/// `aggregate` terminal (ts-client `in_memory/query.ts` `executeAggregateTerminal`):
/// one code path handles the legacy single-`op` scalar/grouped shapes and the
/// wire-v2 `aggregates` map / composite `groupBy` shapes, dispatching only at
/// the final result-shape choice.
private func executeAggregateTerminal(
    _ aggregate: AggregateSpec,
    _ tableDef: TableDef,
    _ indexDef: IndexDef?,
    _ eqLen: Int,
    _ filtered: [StoredRow]
) throws -> JSONValue {
    /// `number` and `int64` are the numeric indexable types (server
    /// `is_numeric_index_field`); an optional wrapper unwraps.
    func isNumeric(_ field: String) -> Bool {
        guard let fieldType = tableDef.fields[field] else { return false }
        var ty = fieldType
        if case let .optional(inner) = ty {
            ty = inner
        }
        if case .number = ty {
            return true
        }
        if case .int64 = ty {
            return true
        }
        return false
    }

    if aggregate.op != nil, aggregate.aggregates != nil {
        throw RtDbError(
            code: .badRequest, message: "aggregate op and aggregates are mutually exclusive"
        )
    }
    let ops: [AggOpAlias] = if let aggregates = aggregate.aggregates {
        aggregates.sorted { $0.key < $1.key }.map { AggOpAlias(alias: $0.key, op: $0.value) }
    } else if let op = aggregate.op {
        [AggOpAlias(alias: op.rawValue, op: op)]
    } else {
        []
    }
    if ops.isEmpty {
        throw RtDbError(code: .badRequest, message: "aggregate requires op or aggregates")
    }
    for entry in ops {
        let invalidCharset = entry.alias.rangeOfCharacter(from: aggAliasCharset.inverted) != nil
        if entry.alias.isEmpty || entry.alias.count > 64 || invalidCharset {
            throw RtDbError(
                code: .badRequest, message: "aggregates alias may contain only [A-Za-z0-9_]"
            )
        }
    }
    let needsField = ops.contains { $0.op != .count }
    let groupByIsFields: Bool = {
        if case .fields = aggregate.groupBy {
            return true
        }
        return false
    }()
    let legacy = aggregate.op != nil && aggregate.aggregates == nil && !groupByIsFields

    var groupFields: [String] = []
    switch aggregate.groupBy {
    case let .bool(flag):
        if flag {
            guard let indexDef, eqLen < indexDef.fields.count else {
                throw RtDbError(
                    code: .badRequest,
                    message: "aggregate groupBy requires an index field beyond the eq prefix"
                )
            }
            groupFields = [indexDef.fields[eqLen]]
        }
    case let .fields(fields):
        groupFields = fields
        if groupFields.isEmpty {
            throw RtDbError(
                code: .badRequest, message: "aggregate groupBy list must not be empty"
            )
        }
    }
    if !groupFields.isEmpty {
        guard let indexDef, groupFields.allSatisfy({ indexDef.fields.contains($0) }) else {
            throw RtDbError(
                code: .badRequest, message: "aggregate groupBy field is not a declared index field"
            )
        }
    }

    var aggField: String?
    if needsField {
        guard let indexDef else {
            throw RtDbError(
                code: .badRequest, message: "aggregate requires an index field beyond the eq prefix"
            )
        }
        aggField = indexDef.fields[eqLen...].first { !groupFields.contains($0) }
        guard let field = aggField else {
            throw RtDbError(
                code: .badRequest, message: "aggregate requires an index field beyond the eq prefix"
            )
        }
        for entry in ops where entry.op == .sum || entry.op == .avg {
            if !isNumeric(field) {
                throw RtDbError(
                    code: .badRequest,
                    message: "aggregate op \(entry.op.rawValue) requires a numeric index field"
                )
            }
        }
        if case .bool(true) = aggregate.groupBy, eqLen + 1 >= indexDef.fields.count {
            throw RtDbError(
                code: .badRequest,
                message: "aggregate groupBy requires two index fields beyond the eq prefix"
            )
        }
    }
    let aggFieldPg: PgType? = try aggField.map { try indexColumnType(requireFieldType(tableDef, $0)).pg }

    func evaluate(_ rows: [StoredRow], _ op: AggregateOp) -> JSONValue {
        if op == .count {
            return .int(Int64(rows.count))
        }
        let values = rows.compactMap { row -> JSONValue? in
            guard let field = aggField, let value = row.doc[field], value != .null else { return nil }
            return value
        }
        return values.isEmpty ? .null : applyAggregate(op, values, aggFieldPg)
    }

    if !groupFields.isEmpty {
        let groupPgs = try groupFields.map { try indexColumnType(requireFieldType(tableDef, $0)).pg }
        struct Group {
            var keys: [JSONValue]
            var rows: [StoredRow] = []
        }
        var groups: [Group] = []
        var groupIndex: [String: Int] = [:]
        for row in filtered {
            let keys = groupFields.map { row.doc[$0] ?? .null }
            let key = keys.map(String.init(describing:)).joined(separator: "\u{1f}")
            if let index = groupIndex[key] {
                groups[index].rows.append(row)
            } else {
                groupIndex[key] = groups.count
                groups.append(Group(keys: keys, rows: [row]))
            }
        }
        groups.sort { lhs, rhs in
            for (index, pg) in groupPgs.enumerated() {
                let cmp = compareIndexValues(lhs.keys[index], rhs.keys[index], pg)
                if cmp != 0 {
                    return cmp < 0
                }
            }
            return false
        }
        let sorted = groups.prefix(maxQueryTake)
        if legacy, case .bool(true) = aggregate.groupBy {
            return .array(sorted.map { group in
                .object(["key": group.keys[0], "value": evaluate(group.rows, ops[0].op)])
            })
        }
        return .array(sorted.map { group in
            var values: [String: JSONValue] = [:]
            for entry in ops {
                values[entry.alias] = evaluate(group.rows, entry.op)
            }
            return .object(["keys": .array(group.keys), "values": .object(values)])
        })
    }
    if legacy {
        return evaluate(filtered, ops[0].op)
    }
    var out: [String: JSONValue] = [:]
    for entry in ops {
        out[entry.alias] = evaluate(filtered, entry.op)
    }
    return .object(out)
}

// swiftlint:enable cyclomatic_complexity function_body_length

/// `paginate` terminal (query.ts `executePaginateTerminal`): keyset-cursor
/// paging over the already-filtered, already-sorted set.
private func executePaginateTerminal(
    _ paginate: Paginate,
    _ tableDef: TableDef,
    _ filtered: [StoredRow],
    _ plan: ScanPlan,
    _ dir: Order
) throws -> JSONValue {
    let (sortKeys, sortPgs) = try sortKeysFor(tableDef, plan.indexDef, plan.typedEq.count)
    return try paginateResult(paginate, tableDef, filtered, sortKeys, sortPgs, dir)
}

/// Collect terminal (query.ts `executeCollectTerminal`): the post-sort tail
/// covering `unique` (at-most-one match), `first`, and the default
/// `take`-limited collect.
private func executeCollectTerminal(_ query: Query, _ filtered: [StoredRow]) throws -> JSONValue {
    if query.unique {
        if filtered.count > 1 {
            throw RtDbError(
                code: .preconditionFailed, message: "unique query matched multiple documents"
            )
        }
        return filtered.first.map(mergeDoc) ?? .null
    }
    if query.first {
        return filtered.first.map(mergeDoc) ?? .null
    }
    let limit = query.take.map(Int.init) ?? maxQueryTake
    return .array(filtered.prefix(limit).map(mergeDoc))
}

// MARK: - Cursor pagination

// swiftlint:disable function_parameter_count
/// Cursor keyset pagination — a port of server `query.rs`'s paginate branch
/// (query.ts `paginateResult`). `sorted` is already filtered and sorted over
/// `sortKeys` (unbound index fields, then `__createdAt`, then `__id`) in
/// direction `dir`. The cursor stores one value per sort column; the resume
/// predicate is the standard OR-of-AND row-value comparison, so paging is
/// stable across pages.
private func paginateResult(
    _ paginate: Paginate,
    _ tableDef: TableDef,
    _ sorted: [StoredRow],
    _ sortKeys: [String],
    _ sortPgs: [PgType],
    _ dir: Order
) throws -> JSONValue {
    let numItems = min(Int(paginate.numItems), maxQueryTake)

    var rows = sorted
    if let cursor = paginate.cursor {
        let cursorValues = try decodePaginateCursor(cursor)
        guard cursorValues.count == sortKeys.count else {
            throw RtDbError(
                code: .badRequest,
                message: "cursor has \(cursorValues.count) value(s) but this query sorts over "
                    + "\(sortKeys.count) column(s)"
            )
        }
        try validateCursorValues(cursorValues, sortKeys, tableDef)
        rows = sorted.filter { isAfterCursor($0, cursorValues, sortKeys, sortPgs, dir) }
    }

    // Fetch one past the page size so a next page is detectable (server
    // LIMIT n+1); the extra is discarded after the has-next check.
    var fetched = Array(rows.prefix(numItems + 1))
    let hasNext = fetched.count > numItems
    if hasNext {
        fetched.removeLast()
    }
    var result: [String: JSONValue] = ["docs": .array(fetched.map(mergeDoc))]
    if hasNext, let last = fetched.last {
        result["nextCursor"] = .string(encodeCursor(sortKeys.map { sortValue(last, $0) }))
    }
    return .object(result)
}

// swiftlint:enable function_parameter_count

/// Decodes a paginate cursor, rethrowing a malformed one as the server-shaped
/// BAD_REQUEST (query.ts `decodePaginateCursor`).
private func decodePaginateCursor(_ cursor: String) throws -> [JSONValue] {
    guard let values = decodeCursor(cursor) else {
        throw RtDbError(code: .badRequest, message: "invalid cursor")
    }
    return values
}

/// Type-checks decoded cursor values positionally against the sort columns —
/// a port of server `SortCol::cursor_bind` (query.ts `validateCursorValues`):
/// index fields via the eq-bind conversion, `created_at` as number, `id` as
/// string.
private func validateCursorValues(
    _ cursorValues: [JSONValue], _ sortKeys: [String], _ tableDef: TableDef
) throws {
    for index in 0 ..< max(0, sortKeys.count - 2) {
        let value = cursorValues[index]
        // Null sorts (nulls-last) and is legitimate for an optional index
        // field; only type-check present values.
        if value != .null {
            _ = try coerceIndexValue(tableDef, sortKeys[index], value)
        }
    }
    guard isJSONNumber(cursorValues[cursorValues.count - 2]) else {
        throw RtDbError(code: .badRequest, message: "cursor value for created_at must be a number")
    }
    guard case .string = cursorValues[cursorValues.count - 1] else {
        throw RtDbError(code: .badRequest, message: "cursor value for id must be a string")
    }
}

/// The keyset resume predicate: true when `row` sorts strictly after the
/// cursor row — `(c0 OP v0) OR (c0 = v0 AND c1 OP v1) OR ...` with OP `>`
/// (asc) / `<` (desc) — evaluated with the same null-sorts-last comparator
/// as the producing sort (query.ts `isAfterCursor`).
private func isAfterCursor(
    _ row: StoredRow,
    _ cursorValues: [JSONValue],
    _ sortKeys: [String],
    _ sortPgs: [PgType],
    _ dir: Order
) -> Bool {
    isAfterCursorValues(
        sortKeys.map { sortValue(row, $0) }, cursorValues, sortPgs, dir
    )
}

/// `isAfterCursor` over sort-key VALUES rather than a `StoredRow` — the one
/// implementation of the predicate. The btree path resolves a row's values
/// through `sortValue`; a ranked terminal's leading sort column is a computed
/// score that no stored field holds, so it passes its values in directly
/// (ENH-030). Same comparator, same OR-of-AND shape, same direction handling.
private func isAfterCursorValues(
    _ rowValues: [JSONValue],
    _ cursorValues: [JSONValue],
    _ sortPgs: [PgType],
    _ dir: Order
) -> Bool {
    for index in rowValues.indices {
        var prefixEqual = true
        for prior in 0 ..< index {
            let priorCmp = compareIndexValues(
                rowValues[prior], cursorValues[prior], sortPgs[prior]
            )
            if priorCmp != 0 {
                prefixEqual = false
                break
            }
        }
        if !prefixEqual {
            continue
        }
        let cmp = compareIndexValues(rowValues[index], cursorValues[index], sortPgs[index])
        if dir == .desc ? cmp < 0 : cmp > 0 {
            return true
        }
    }
    return false
}

// MARK: - Ranked-terminal pagination (ENH-030)

// `paginate` composes with `search`/`vectorSearch`/`hybridSearch` as a peer
// clause exactly the way `take` already composes with `search`: it does not
// change WHICH terminal runs, only the row cap, the keyset resume predicate,
// and the result envelope (`{docs, nextCursor?}` instead of a doc array).
//
// The btree path's `paginateResult` keys off `StoredRow` columns by name; a
// ranked terminal sorts by a computed key first, so these twins take each
// row's sort-key values precomputed. Everything else — the cursor codec, the
// resume predicate, the `numItems + 1` probe — is shared.

/// One sort column of a ranked terminal's cursor. The ranking key is the
/// server's `SortCol::Rank`; `createdAt`/`id` are the tie-breakers that make
/// the order total, which is what guarantees no row is skipped or duplicated
/// across pages.
private enum RankedSortCol {
    case rank
    case createdAt
    case id

    /// Storage type handed to `compareIndexValues` for this column.
    var pg: PgType {
        switch self {
        case .rank, .createdAt: .number
        case .id: .text
        }
    }

    /// The server's `SortCol::cursor_bind` message for a wrongly typed value.
    var cursorTypeMessage: String {
        switch self {
        case .rank: "cursor value for the ranking key must be a number"
        case .createdAt: "cursor value for created_at must be a number"
        case .id: "cursor value for id must be a string"
        }
    }
}

/// One already-ranked row prepared for `paginateRanked`: its sort-key values
/// (in `cols` order) and the result doc to emit, snippet and all.
private struct RankedRow {
    var sortValues: [JSONValue]
    var doc: JSONValue
}

/// Keyset pagination over a ranked terminal's result. `ranked` must already be
/// sorted in that terminal's own order, which is DESC on every column
/// (the server's ranked `ORDER BY <rank> DESC, created_at DESC, id DESC`).
private func paginateRanked(
    _ paginate: Paginate, _ ranked: [RankedRow], _ cols: [RankedSortCol]
) throws -> JSONValue {
    let numItems = min(Int(paginate.numItems), maxQueryTake)
    let sortPgs = cols.map(\.pg)

    var rows = ranked
    if let cursor = paginate.cursor {
        let cursorValues = try decodePaginateCursor(cursor)
        guard cursorValues.count == cols.count else {
            throw RtDbError(
                code: .badRequest,
                message: "cursor has \(cursorValues.count) value(s) but this query sorts over "
                    + "\(cols.count) column(s)"
            )
        }
        try validateRankedCursorValues(cursorValues, cols)
        rows = ranked.filter { isAfterCursorValues($0.sortValues, cursorValues, sortPgs, .desc) }
    }

    // One past the page size so a next page is detectable (server LIMIT n+1);
    // the probe row is discarded after the has-next check.
    var fetched = Array(rows.prefix(numItems + 1))
    let hasNext = fetched.count > numItems
    if hasNext {
        fetched.removeLast()
    }
    var result: [String: JSONValue] = ["docs": .array(fetched.map(\.doc))]
    if hasNext, let last = fetched.last {
        result["nextCursor"] = .string(encodeCursor(last.sortValues))
    }
    return .object(result)
}

/// Type-checks a ranked terminal's decoded cursor positionally against its
/// sort columns — the ranked twin of `validateCursorValues` (server
/// `SortCol::cursor_bind`), whose index-field arm has no counterpart here.
private func validateRankedCursorValues(
    _ cursorValues: [JSONValue], _ cols: [RankedSortCol]
) throws {
    for (index, col) in cols.enumerated() {
        let ok = switch col.pg {
        case .text: cursorValues[index].stringValue != nil
        default: isJSONNumber(cursorValues[index])
        }
        if !ok {
            throw RtDbError(code: .badRequest, message: col.cursorTypeMessage)
        }
    }
}
