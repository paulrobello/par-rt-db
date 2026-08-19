import Foundation

/// Outcome of projecting a transaction onto a subscription's last result —
/// the port of rust-client/src/optimistic.rs::OptimisticProjection.
public enum OptimisticProjection: Equatable, Sendable {
    /// Do not overlay (no-op or ambiguous effect).
    case skip
    /// Overlay this value immediately.
    case overlaid(JSONValue)
}

/// Clearly-branded temporary id for an optimistically-inserted doc (replaced
/// on reconcile with the server-assigned id). Process-wide counter, mirroring
/// rust's `static COUNTER: AtomicU64`.
/// @unchecked Sendable: `value` is only ever touched while holding `lock`.
private final class SyntheticIdCounter: @unchecked Sendable {
    static let shared = SyntheticIdCounter()
    private let lock = NSLock()
    private var value: UInt64 = 0

    func next() -> String {
        let id = lock.withLock {
            value += 1
            return value
        }
        return "__optimistic__\(id)"
    }
}

/// Client-side optimistic-update projection — the port of rust-client's
/// `project_optimistic_update` (itself a port of ts-client's optimistic.ts).
/// Pure: given a query, its last authoritative result, and a transaction,
/// produce the projected result to overlay immediately (before the server
/// round-trip), or `.skip` when the effect is ambiguous. Conservative: only
/// unambiguous cases overlay — a wrong overlay is worse than a brief wait
/// for the authoritative `queryUpdate`.
///
/// The reactive `RtDbClient` caches each subscription's last result and holds
/// neither a schema nor a table store, so an overlay can only be computed
/// from the documents already in that cached result. This function mirrors
/// the server/in-memory DSL semantics for the cases where the effect on the
/// result set is unambiguous from those documents alone, and declines to
/// guess everywhere else.
///
/// `now` (epoch-millis) is a parameter so this function is pure and
/// clock-free. Canonical no-op detection needs no key-sorting here (unlike
/// the ts-client): `JSONValue.object` is a `Dictionary`, so `==` is already
/// key-order-independent.
public func projectOptimisticUpdate(
    query: Query,
    last: JSONValue,
    txn: Transaction,
    now: Int64
) -> OptimisticProjection {
    if query.get != nil {
        return projectGet(query, last, txn)
    }
    guard isArrayQuery(query) else {
        return .skip
    }
    if hasFilter(query) {
        return projectFilteredArray(query, last, txn)
    }
    return projectUnfilteredArray(query, last, txn, now)
}

/// `get` point-read, `unique`/`first`/`count`/`distinct`, `paginate`, and the
/// `search`/`vectorSearch` terminals are non-array shapes (or rank-based)
/// whose result cannot be projected from cached documents alone. A `filter`
/// predicate is NOT excluded here: a filtered collect is still an array
/// read, just one whose membership is handled by `hasFilter`.
private func isArrayQuery(_ query: Query) -> Bool {
    query.get == nil
        && !query.unique
        && !query.first
        && !query.count
        && !query.distinct
        && query.paginate == nil
        && query.search == nil
        && query.vectorSearch == nil
}

/// A query whose result membership depends on a predicate that cannot be
/// evaluated without the schema (index/eq/range or a db-side `filter`). Only
/// deletes of already-cached docs are unambiguous under such a filter.
private func hasFilter(_ query: Query) -> Bool {
    query.index != nil
        || !query.eq.isEmpty
        || query.gt != nil
        || query.gte != nil
        || query.lt != nil
        || query.lte != nil
        || query.filter != nil
}

// swiftlint:disable cyclomatic_complexity
/// Unfiltered full-table read (`collect`/`take` with no index/eq/range/
/// filter): every doc is present, so insert/patch/replace/delete on a known
/// id are all unambiguous.
private func projectUnfilteredArray(
    _ query: Query,
    _ last: JSONValue,
    _ txn: Transaction,
    _ now: Int64
) -> OptimisticProjection {
    guard case var .array(working) = last else {
        return .skip
    }
    for step in txn.steps {
        guard step.optimisticTable == query.table else {
            continue
        }
        switch step {
        case let .insert(_, doc):
            // A full-table window already at its `take` limit would evict an
            // unknown doc — the right window can't be picked, so decline.
            if let take = query.take, UInt32(working.count) >= take {
                return .skip
            }
            var draft = doc
            draft["_id"] = .string(SyntheticIdCounter.shared.next())
            draft["_creationTime"] = .int(now)
            draft["_version"] = .int(1)
            working.append(.object(draft))
        case let .patch(_, id, fields):
            mergeById(&working, id: id, fields: fields)
        case let .replace(_, id, doc):
            replaceById(&working, id: id, doc: doc)
        case let .delete(_, id):
            removeById(&working, id: id)
        case .undelete:
            // The restored doc's body is not in the cached result
            // (soft-deleted rows are invisible to reads), so there is
            // nothing unambiguous to overlay — the authoritative update
            // delivers the restored row (ts-client fallthrough).
            continue
        case .upsert:
            return .skip
        case .expectVersion, .expectAbsent:
            continue
        case .patchByQuery, .deleteByQuery:
            // By-query steps match an unbounded set of rows by a filter this
            // projection can't evaluate (no table store, no schema) — the
            // effect on the cached result is membership-ambiguous, so decline.
            return .skip
        case .schedule, .cancelSchedule, .startWorkflow, .cancelWorkflow:
            // Act on future execution, not this result — nothing to project.
            continue
        }
    }
    return finalize(.array(working), last)
}

// swiftlint:enable cyclomatic_complexity

/// Filtered read (index/eq/range or `filter` predicate): only a delete of a
/// doc already known to be in the result is unambiguous — adding or changing
/// a doc may move it in or out of the filter.
private func projectFilteredArray(
    _ query: Query,
    _ last: JSONValue,
    _ txn: Transaction
) -> OptimisticProjection {
    guard case var .array(working) = last else {
        return .skip
    }
    for step in txn.steps {
        guard step.optimisticTable == query.table else {
            continue
        }
        switch step {
        case let .delete(_, id):
            removeById(&working, id: id)
        case .undelete:
            // Restores a doc whose body is not in this cached result —
            // nothing unambiguous to overlay (ts-client fallthrough).
            continue
        case .insert, .patch, .replace, .upsert:
            // Membership-ambiguous under a filter.
            return .skip
        case .expectVersion, .expectAbsent:
            continue
        case .patchByQuery, .deleteByQuery:
            return .skip
        case .schedule, .cancelSchedule, .startWorkflow, .cancelWorkflow:
            continue
        }
    }
    return finalize(.array(working), last)
}

// swiftlint:disable cyclomatic_complexity
/// Point read by id: the result is exactly that id's doc (or null), so
/// patch/replace/delete of the same id are unambiguous; a freshly inserted id
/// can never match a pre-existing `get(target)`.
private func projectGet(
    _ query: Query,
    _ last: JSONValue,
    _ txn: Transaction
) -> OptimisticProjection {
    let target = query.get ?? ""
    var working = last
    for step in txn.steps {
        guard step.optimisticTable == query.table else {
            continue
        }
        switch step {
        case let .delete(_, id):
            if id == target {
                working = .null
            }
        case let .patch(_, id, fields):
            if id == target, case var .object(patched) = working {
                for (key, value) in fields {
                    patched[key] = value
                }
                working = .object(patched)
            }
        case let .replace(_, id, doc):
            if id == target, case let .object(old) = last {
                var replacement = doc
                replacement["_id"] = old["_id"]
                replacement["_creationTime"] = old["_creationTime"]
                replacement.removeValue(forKey: "_version")
                working = .object(replacement)
            }
        case .upsert:
            return .skip
        case .patchByQuery, .deleteByQuery:
            // A by-query step may patch/delete the target row, but the filter
            // is unevaluable here — decline rather than guess.
            return .skip
        case .insert, .expectVersion, .expectAbsent, .undelete,
             .schedule, .cancelSchedule, .startWorkflow, .cancelWorkflow:
            // Insert (fresh id never matches a pre-existing get target),
            // Expect*/preconditions (no data effect), undelete (the restored
            // body is not the cached value, and restoring a live target is a
            // no-op), schedule/workflow steps (future execution), and
            // non-target patch/replace/delete: nothing to do here.
            continue
        }
    }
    return finalize(working, last)
}

// swiftlint:enable cyclomatic_complexity

private extension Step {
    /// The table this step targets (rust's `Step::table`). Every variant
    /// except `expectAbsent` and the schedule/workflow steps carries one;
    /// `expectAbsent` is a precondition with no data effect, so its table is
    /// masked here (the per-step table guard then skips it — harmless, since
    /// the variant is a no-op in every projection). Schedule/workflow steps
    /// act on future execution, not the queried table.
    var optimisticTable: String? {
        switch self {
        case let .insert(table, _),
             let .patch(table, _, _),
             let .replace(table, _, _),
             let .delete(table, _),
             let .undelete(table, _),
             let .expectVersion(table, _, _),
             let .upsert(table, _, _, _, _),
             let .patchByQuery(table, _, _, _),
             let .deleteByQuery(table, _, _):
            table
        case .expectAbsent, .schedule, .cancelSchedule, .startWorkflow, .cancelWorkflow:
            nil
        }
    }
}

private func finalize(_ next: JSONValue, _ last: JSONValue) -> OptimisticProjection {
    next == last ? .skip : .overlaid(next)
}

private func mergeById(_ working: inout [JSONValue], id: String, fields: [String: JSONValue]) {
    for (index, value) in working.enumerated() {
        guard case var .object(existing) = value, existing["_id"]?.stringValue == id else {
            continue
        }
        for (key, fieldValue) in fields {
            existing[key] = fieldValue
        }
        working[index] = .object(existing)
    }
}

private func replaceById(_ working: inout [JSONValue], id: String, doc: [String: JSONValue]) {
    for (index, value) in working.enumerated() {
        guard case let .object(existing) = value, existing["_id"]?.stringValue == id else {
            continue
        }
        var replacement = doc
        replacement["_id"] = existing["_id"]
        replacement["_creationTime"] = existing["_creationTime"]
        replacement.removeValue(forKey: "_version")
        working[index] = .object(replacement)
    }
}

private func removeById(_ working: inout [JSONValue], id: String) {
    working.removeAll { value in
        guard case let .object(candidate) = value else { return false }
        return candidate["_id"]?.stringValue == id
    }
}
