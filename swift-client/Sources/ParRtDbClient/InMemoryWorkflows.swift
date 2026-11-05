import Foundation

// The stored workflow-run row (store.ts `WorkflowRun`) and the FM-29 retry
// helpers, extracted from ``InMemoryEngine`` (QA-003b) and moved verbatim
// into a sibling file. Spec validation and the client's workflow methods
// stay on the engine class in ``InMemoryEngine``; the advance loop lives in
// `InMemoryWorkflowEngine.swift` (QA-007).

/// FM-29 retry policy applied when a step spec omits `retry` (server
/// `protocol::StepRetry::default` — the Swift wire type already carries the
/// same defaults).
let defaultStepRetry = StepRetry(maxAttempts: 3)

/// FM-29: exponential backoff after the `attempts`-th failure of a step —
/// `initialRetryMs * 2^(attempts-1)` (shift capped at 32), clamped to
/// `maxRetryMs` (store.ts `backoffMs`).
func backoffMs(_ retry: StepRetry, _ attempts: Int) -> Int64 {
    let shift = UInt64(min(attempts - 1, 32))
    let doubled = retry.initialRetryMs << shift
    let capped = min(doubled, retry.maxRetryMs)
    return Int64(clamping: capped)
}

/// FM-29: a stored workflow run (store.ts `WorkflowRun`); field semantics
/// mirror the server's `workflows` table. `sleepUntil` is always set. A class
/// so `advanceWorkflow` can identity-check the live entry between steps.
/// The `awaitSignal` side-table columns (`waitName`/`waitedSince`/
/// `signalPayload`) are nil on runs never parked; set on a claim they tell
/// the advancer why the run woke (payload = delivered, else `waitedSince`
/// set = the timeout gate expired).
final class WorkflowRun {
    let id: String
    let spec: WorkflowSpec
    var status: WorkflowStatus
    var currentStep: Int
    var attempts: Int
    var sleepUntil: Int64
    var lastError: String?
    var waitName: String?
    var waitedSince: Int64?
    var signalPayload: JSONValue?
    let createdAt: Int64
    var updatedAt: Int64
    var startedAt: Int64?
    var finishedAt: Int64?
    var stepOutcomes: [StepOutcome]
    /// Insertion sequence — `listWorkflows` ties break on it (the TS sort is
    /// stable over insertion order; Swift's is not).
    let seq: Int

    init(
        id: String, spec: WorkflowSpec, status: WorkflowStatus, currentStep: Int, attempts: Int,
        sleepUntil: Int64, createdAt: Int64, updatedAt: Int64, stepOutcomes: [StepOutcome], seq: Int
    ) {
        self.id = id
        self.spec = spec
        self.status = status
        self.currentStep = currentStep
        self.attempts = attempts
        self.sleepUntil = sleepUntil
        self.createdAt = createdAt
        self.updatedAt = updatedAt
        self.stepOutcomes = stepOutcomes
        self.seq = seq
    }
}
