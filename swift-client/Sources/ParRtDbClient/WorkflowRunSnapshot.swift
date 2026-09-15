struct WorkflowRunSnapshot {
    private let status: WorkflowStatus
    private let currentStep: Int
    private let attempts: Int
    private let sleepUntil: Int64
    private let lastError: String?
    private let waitName: String?
    private let waitedSince: Int64?
    private let signalPayload: JSONValue?
    private let updatedAt: Int64
    private let startedAt: Int64?
    private let finishedAt: Int64?
    private let stepOutcomes: [StepOutcome]

    init(_ run: WorkflowRun) {
        status = run.status
        currentStep = run.currentStep
        attempts = run.attempts
        sleepUntil = run.sleepUntil
        lastError = run.lastError
        waitName = run.waitName
        waitedSince = run.waitedSince
        signalPayload = run.signalPayload
        updatedAt = run.updatedAt
        startedAt = run.startedAt
        finishedAt = run.finishedAt
        stepOutcomes = run.stepOutcomes
    }

    func restore(into run: WorkflowRun) {
        run.status = status
        run.currentStep = currentStep
        run.attempts = attempts
        run.sleepUntil = sleepUntil
        run.lastError = lastError
        run.waitName = waitName
        run.waitedSince = waitedSince
        run.signalPayload = signalPayload
        run.updatedAt = updatedAt
        run.startedAt = startedAt
        run.finishedAt = finishedAt
        run.stepOutcomes = stepOutcomes
    }
}

struct WorkflowTransactionSnapshot {
    private let runs: [String: WorkflowRun]
    private let states: [String: WorkflowRunSnapshot]
    private let order: [String]

    init(_ runs: [String: WorkflowRun], order: [String]) {
        self.runs = runs
        states = runs.mapValues(WorkflowRunSnapshot.init)
        self.order = order
    }

    func restore(into currentRuns: inout [String: WorkflowRun], order currentOrder: inout [String]) {
        currentRuns = runs
        for (id, state) in states {
            if let run = currentRuns[id] {
                state.restore(into: run)
            }
        }
        currentOrder = order
    }
}
