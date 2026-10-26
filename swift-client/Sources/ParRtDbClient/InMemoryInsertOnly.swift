import Foundation

// MARK: - insertOnly gate + search ranking options

// In-memory mirrors of the server's `enforce_insert_only` (txn/mod.rs) and the
// `rankMode` / `minSimilarity` gates of `compile_search` (query/search.rs).

private extension Step {
    /// The table of a step that modifies or removes an existing row, nil for
    /// `insert`/`insertMany` and every non-mutating or control-flow step.
    var existingRowMutationTable: String? {
        switch self {
        case let .patch(table, _, _), let .replace(table, _, _), let .delete(table, _),
             let .undelete(table, _), let .upsertMany(table, _, _), let .upsert(table, _, _, _, _),
             let .patchByQuery(table, _, _, _), let .deleteByQuery(table, _, _):
            table
        case let .adjustCounter(table, _, _, _, _, _, _):
            table
        default:
            nil
        }
    }
}

extension InMemoryRtDbClient {
    /// Runs first for every step: `insertOnly` tables reject all but insert
    /// and insertMany with `.schemaViolation`. Cascades and TTL reaping never
    /// pass through `executeStep`, so they stay exempt.
    func enforceInsertOnly(_ step: Step) throws {
        guard let table = step.existingRowMutationTable,
              schema?.tables[table]?.insertOnly == true
        else { return }
        throw RtDbError(
            code: .schemaViolation,
            message: "table '\(table)' is insertOnly: only insert and insertMany are allowed"
        )
    }
}

/// BAD_REQUEST gates for the search ranking options.
func validateSearchRanking(_ search: SearchQuery) throws {
    if search.rankMode != nil, search.mode == .trgm {
        throw RtDbError(code: .badRequest, message: "rankMode is only supported in tsquery mode")
    }
    if let floor = search.minSimilarity {
        if search.mode != .trgm {
            throw RtDbError(
                code: .badRequest, message: "minSimilarity is only supported in trgm mode"
            )
        }
        if !(0 ... 1).contains(floor) {
            throw RtDbError(
                code: .badRequest, message: "minSimilarity must be between 0 and 1"
            )
        }
    }
}
