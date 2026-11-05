import Foundation

// The stored scheduled-job row (store.ts `ScheduledJob`), extracted from
// ``InMemoryEngine`` (QA-003b) and moved verbatim into a sibling file. The
// client's schedule methods stay on the engine class in ``InMemoryEngine``.

// MARK: - Schedules & workflows

/// A stored scheduled job (store.ts `ScheduledJob`): `tick` fires due
/// non-paused jobs by applying `txn` through the same atomic path as
/// `mutate`. A class so tick/pause/resume stamp in place.
final class ScheduledJob {
    let id: String
    let kind: ScheduleKind
    let txn: Transaction
    var dueAt: Int64
    var cron: String?
    var tz: String?
    var everyMs: Int64?
    var status: ScheduleStatus
    let createdAt: Int64
    var firedCount: Int64
    var lastError: String?
    /// External-claim mode: `tick` skips it entirely — an application worker
    /// claims the job over HTTP and finalizes it with the fencing token.
    let external: Bool
    /// Cumulative count of recurring-job windows that elapsed before a fire.
    /// Computed exactly for interval jobs; always 0 for cron jobs, since this
    /// harness re-arms cron on a fixed `cronStepMs` approximation rather than
    /// real cron occurrence math and cannot count missed cron windows
    /// accurately.
    var missedCount: Int64
    var lastMissedAt: Int64?

    init(
        id: String, kind: ScheduleKind, txn: Transaction, dueAt: Int64, cron: String?,
        tz: String? = nil, everyMs: Int64?, status: ScheduleStatus, createdAt: Int64,
        firedCount: Int64, lastError: String? = nil, external: Bool = false,
        missedCount: Int64 = 0, lastMissedAt: Int64? = nil
    ) {
        self.id = id
        self.kind = kind
        self.txn = txn
        self.dueAt = dueAt
        self.cron = cron
        self.tz = tz
        self.everyMs = everyMs
        self.status = status
        self.createdAt = createdAt
        self.firedCount = firedCount
        self.lastError = lastError
        self.external = external
        self.missedCount = missedCount
        self.lastMissedAt = lastMissedAt
    }
}
