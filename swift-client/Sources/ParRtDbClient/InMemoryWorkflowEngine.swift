// FM-29 workflow advance engine for the in-memory harness — the
// `advanceWorkflow` loop and its `awaitSignal` half, extracted from
// ``InMemoryEngine`` (QA-007). Moved verbatim from the engine class body as
// an extension in a sibling file so the class file stays under the
// project's `file_length` cap.

import Foundation

extension InMemoryRtDbClient {
    /// FM-29: drives one claimed run across step boundaries (store.ts
    /// `advanceWorkflow`). Success on the last step finalizes; success earlier
    /// moves to the next step and applies its `sleepBeforeMs` gate (a future
    /// gate re-pends the run; a `now` gate continues in the same tick);
    /// failure re-pends with exponential backoff or, once attempts are
    /// exhausted, marks the run failed with a terminal outcome. An
    /// `awaitSignal` step takes the server committer's three-way branch
    /// instead: a delivered payload consumes the wait as a success outcome
    /// carrying the payload; a first arrival parks the run (`waiting`, fresh
    /// `waitedSince`, gate `now + timeoutMs` or forever); an expired gate is a
    /// timeout attempt — re-parked with the FULL timeout again, terminal fail
    /// `awaitSignal '<name>' timed out` at exhaustion.
    func advanceWorkflow(_ run: WorkflowRun, now: Int64) {
        while true {
            // Per-boundary liveness check: a cancel (or terminal transition)
            // between steps ends the pass — the server re-checks the row each
            // boundary.
            guard workflows[run.id] === run, run.status == .running else { return }
            guard run.currentStep < run.spec.steps.count else { return }
            let step = run.spec.steps[run.currentStep]
            if let signal = step.awaitSignal {
                if advanceAwaitSignal(run, signal, step, now: now) {
                    return
                }
                continue
            }
            // Every step carries exactly one of txn/awaitSignal (submit-time
            // `validateWorkflowSpec`); the branch above handled the latter. A
            // txn-less step here is unreachable — stop the pass like the
            // out-of-range guard above rather than trap.
            guard let txn = step.txn else { return }
            var execError: String?
            do {
                _ = try executeTransaction(txn)
            } catch {
                execError = errorMessage(error)
            }
            if let error = execError {
                advanceWorkflowFailed(run, step, error, now: now)
                return
            }
            if advanceWorkflowStepped(run, now: now) {
                return
            }
        }
    }

    /// Records the success outcome for the current step and advances: the
    /// last step finalizes the run; otherwise the next step's
    /// `sleepBeforeMs` gate applies (a future gate re-pends the run and
    /// returns true to end the pass; a now gate keeps looping).
    private func advanceWorkflowStepped(_ run: WorkflowRun, now: Int64) -> Bool {
        let outcome = StepOutcome(
            stepIndex: UInt32(run.currentStep),
            status: .success,
            attempts: UInt32(run.attempts + 1),
            at: now
        )
        let isLast = run.currentStep + 1 >= run.spec.steps.count
        run.stepOutcomes.append(outcome)
        run.updatedAt = now
        if isLast {
            run.status = .success
            run.attempts = 0
            run.lastError = nil
            run.finishedAt = now
            return true
        }
        run.currentStep += 1
        run.attempts = 0
        let next = run.spec.steps[run.currentStep]
        let gate = now + Int64(next.sleepBeforeMs ?? 0)
        if gate > now {
            run.status = .pending
            run.sleepUntil = gate
            run.updatedAt = now
            return true
        }
        return false
    }

    /// The failure half: re-pend with exponential backoff while attempts
    /// remain, otherwise mark the run failed with a terminal outcome.
    private func advanceWorkflowFailed(
        _ run: WorkflowRun, _ step: WorkflowStepSpec, _ error: String, now: Int64
    ) {
        let retry = step.retry ?? defaultStepRetry
        run.attempts += 1
        if run.attempts < Int(retry.maxAttempts) {
            run.status = .pending
            run.sleepUntil = now + backoffMs(retry, run.attempts)
            run.updatedAt = now
            return
        }
        run.stepOutcomes.append(
            StepOutcome(
                stepIndex: UInt32(run.currentStep),
                status: .failed,
                attempts: UInt32(run.attempts),
                at: now,
                error: error
            )
        )
        run.status = .failed
        run.lastError = error
        run.finishedAt = now
        run.updatedAt = now
    }

    /// The awaitSignal half of `advanceWorkflow` — the server committer's
    /// three-way branch. Side-store only (no document writes); the wake
    /// discriminator is the claimed run itself: `signalPayload` set =
    /// delivered, else `waitedSince` nil = first arrival, set = the timeout
    /// gate expired. Returns true when the pass ends (boundary written and
    /// re-pended, parked, or terminal); false when the consumed signal's next
    /// step gate is due now and the caller should keep looping.
    func advanceAwaitSignal(
        _ run: WorkflowRun, _ signal: AwaitSignalSpec, _ step: WorkflowStepSpec, now: Int64
    ) -> Bool {
        if let payload = run.signalPayload {
            return awaitSignalDelivered(run, payload, now: now)
        }
        // Timeout gate — Int64.max when the step omits `timeoutMs` (never
        // due; only a delivery or cancel wakes the run). The u64→Int64 clamp
        // and saturating add mirror the server's wrap-hazard guards.
        let timeoutGate: Int64
        if let timeoutMs = signal.timeoutMs {
            let clamped = Int64(min(timeoutMs, UInt64(Int64.max)))
            let (added, overflow) = now.addingReportingOverflow(clamped)
            timeoutGate = overflow ? Int64.max : added
        } else {
            timeoutGate = Int64.max
        }
        if run.waitedSince == nil {
            // First arrival: park. `attempts` rides the run so a timeout
            // retry that re-parks keeps its count.
            run.status = .waiting
            run.waitName = signal.name
            run.waitedSince = now
            run.sleepUntil = timeoutGate
            run.updatedAt = now
            return true
        }
        return awaitSignalTimedOut(run, signal, step, timeoutGate: timeoutGate, now: now)
    }

    /// The delivered half: consume the payload as a success outcome carrying
    /// it, then advance — the last step finalizes, otherwise the next step's
    /// `sleepBeforeMs` gate applies (future gate re-pends and ends the pass;
    /// a now gate keeps looping). Returns true when the pass ends.
    private func awaitSignalDelivered(
        _ run: WorkflowRun, _ payload: JSONValue, now: Int64
    ) -> Bool {
        run.signalPayload = nil
        let outcome = StepOutcome(
            stepIndex: UInt32(run.currentStep),
            status: .success,
            attempts: UInt32(run.attempts + 1),
            at: now,
            signal: payload
        )
        run.stepOutcomes.append(outcome)
        run.updatedAt = now
        let isLast = run.currentStep + 1 >= run.spec.steps.count
        if isLast {
            run.status = .success
            run.attempts = 0
            run.lastError = nil
            run.waitName = nil
            run.waitedSince = nil
            run.finishedAt = now
            return true
        }
        run.currentStep += 1
        run.attempts = 0
        run.waitName = nil
        run.waitedSince = nil
        let next = run.spec.steps[run.currentStep]
        let gate = now + Int64(next.sleepBeforeMs ?? 0)
        if gate > now {
            run.status = .pending
            run.sleepUntil = gate
            run.updatedAt = now
            return true
        }
        return false
    }

    /// The timed-out half: a retry re-parks waiting the FULL timeoutMs again
    /// (never backoff); exhaustion terminal-fails with
    /// `awaitSignal '<name>' timed out`. Always ends the pass.
    private func awaitSignalTimedOut(
        _ run: WorkflowRun, _ signal: AwaitSignalSpec, _ step: WorkflowStepSpec,
        timeoutGate: Int64, now: Int64
    ) -> Bool {
        run.attempts += 1
        let retry = step.retry ?? defaultStepRetry
        if run.attempts < Int(retry.maxAttempts) {
            run.status = .waiting
            run.waitName = signal.name
            run.waitedSince = now
            run.sleepUntil = timeoutGate
            run.updatedAt = now
            return true
        }
        let error = "awaitSignal '\(signal.name)' timed out"
        run.stepOutcomes.append(
            StepOutcome(
                stepIndex: UInt32(run.currentStep),
                status: .failed,
                attempts: UInt32(run.attempts),
                at: now,
                error: error
            )
        )
        run.status = .failed
        run.lastError = error
        run.waitName = nil
        run.waitedSince = nil
        run.signalPayload = nil
        run.finishedAt = now
        run.updatedAt = now
        return true
    }
}
