/**
 * Schedule/workflow machinery for the in-memory par-rt-db harness (split out
 * of `./store.ts`, QA-003b): the stored-job/run shapes, the shared
 * step-count/step-cap gates, spec validation, retry math, and the tick-time
 * engine (`claimDueWorkflows`/`advanceWorkflow` as module functions over the
 * caller's run map and transaction executor). `InMemoryRtDbClient` keeps its
 * public schedule/workflow surface and delegates here.
 */

import { RtDbError } from "../errors.js";
import type {
  ScheduleInfo,
  ScheduleWhen,
  StepOutcome,
  StepRetry,
  TransactionJson,
  WorkflowInfo,
  WorkflowSpec,
  WorkflowStatus,
} from "../protocol.js";

/** Hard cap on steps per transaction — and, counted recursively through
 * nested schedule/startWorkflow subtrees, per workflow spec. Mirrors server
 * `txn::MAX_STEPS`; the txn executor and `validateWorkflowSpec` share it. */
export const MAX_STEPS = 1024;

/** FM-28/FM-29: recursive step count — a `schedule` step counts as itself plus
 * every step in its nested txn; a `startWorkflow` step counts as itself plus
 * every step of every txn in its spec. Mirrors the server's recursive gate
 * against `MAX_STEPS` (a nested tree can't smuggle past the flat cap). */
export function countSteps(txn: TransactionJson): number {
  let total = txn.steps.length;
  for (const step of txn.steps) {
    if (step.op === "schedule") total += countSteps(step.txn);
    if (step.op === "startWorkflow") {
      for (const s of step.spec.steps) total += s.txn ? countSteps(s.txn) : 0;
    }
  }
  return total;
}

/** Mirrors server schedule status values. */
type ScheduleStatus = "pending" | "running" | "paused" | "error";

/** A stored scheduled job in the in-memory harness. `tick` fires due non-paused
 * jobs by applying `txn` through the same atomic path as `mutate`; `external`
 * jobs are never fired by `tick` (the server's internal scheduler likewise
 * excludes them — application workers claim them through the external claim
 * surface instead). */
export interface ScheduledJob {
  id: string;
  kind: "oneshot" | "cron" | "interval";
  txn: TransactionJson;
  dueAt: number;
  cron?: string;
  tz?: string;
  everyMs?: number;
  status: ScheduleStatus;
  createdAt: number;
  firedCount: number;
  /** Never executed by the harness's `tick` — served to application workers
   * via the external claim surface (server `scheduled_txns.external`). */
  external: boolean;
  lastError?: string;
  /** Cumulative count of recurring-job windows that elapsed before a fire.
   * Computed exactly for interval jobs; always 0 for cron jobs, since this
   * harness re-arms cron on a fixed `CRON_STEP_MS` approximation rather than
   * real cron occurrence math and cannot count missed cron windows accurately. */
  missedCount: number;
  lastMissedAt?: number;
}

/** Approximate cron re-fire interval for the in-memory stub. Real 5-field cron
 * parsing is deferred to the server; the harness only needs crons to re-arm. */
export const CRON_STEP_MS = 60_000;

/** Upper bound on an interval job's `everyMs`: one year in ms. Mirrors server
 * `scheduler::MAX_EVERY_MS`; bounds `now + everyMs` and the horizon a recurring
 * job can occupy a row for. */
export const MAX_EVERY_MS = 365 * 24 * 60 * 60 * 1000;

/** FM-29: hard cap on steps per workflow spec. Mirrors server
 * `workflows::MAX_WORKFLOW_STEPS`. */
const MAX_WORKFLOW_STEPS = 64;

/** FM-29: retry policy applied when a step spec omits `retry`. Mirrors server
 * `protocol::StepRetry::default`. */
const DEFAULT_STEP_RETRY: Required<StepRetry> = {
  maxAttempts: 3,
  initialRetryMs: 1_000,
  maxRetryMs: 60_000,
};

/** FM-29: exponential backoff after the `attempts`-th failure of a step —
 * `initialRetryMs * 2^(attempts-1)` (shift capped at 32), clamped to
 * `maxRetryMs`. Mirrors server `workflows::backoff_ms`. */
function backoffMs(retry: Required<StepRetry>, attempts: number): number {
  const shift = Math.min(attempts - 1, 32);
  return Math.min(retry.initialRetryMs * 2 ** shift, retry.maxRetryMs);
}

/** FM-29: the effective retry policy for a step — the spec's partial policy
 * with server `StepRetry::default` filling the gaps. */
function resolveStepRetry(retry?: StepRetry): Required<StepRetry> {
  return retry
    ? {
        maxAttempts: retry.maxAttempts,
        initialRetryMs: retry.initialRetryMs ?? DEFAULT_STEP_RETRY.initialRetryMs,
        maxRetryMs: retry.maxRetryMs ?? DEFAULT_STEP_RETRY.maxRetryMs,
      }
    : DEFAULT_STEP_RETRY;
}

/** FM-29: submit-time spec validation. Mirrors server `workflows::validate_spec`
 * (same checks, same BAD_REQUEST messages, including the recursive
 * `MAX_STEPS` gate over the spec's step txns). */
export function validateWorkflowSpec(spec: WorkflowSpec): void {
  if (spec.steps.length === 0) {
    throw new RtDbError("BAD_REQUEST", "workflow must have at least one step");
  }
  if (spec.steps.length > MAX_WORKFLOW_STEPS) {
    throw new RtDbError("BAD_REQUEST", `workflow exceeds ${MAX_WORKFLOW_STEPS} steps`);
  }
  for (const [i, step] of spec.steps.entries()) {
    // awaitSignal parity: exactly one of txn/awaitSignal per step, and the
    // wait declaration is bounded (name 1..=256 chars, timeoutMs > 0).
    const hasTxn = step.txn !== undefined;
    const hasSig = step.awaitSignal !== undefined;
    if (hasTxn === hasSig) {
      throw new RtDbError(
        "BAD_REQUEST",
        `steps[${i}] must carry exactly one of txn or awaitSignal`,
      );
    }
    if (step.awaitSignal !== undefined) {
      const sig = step.awaitSignal;
      if (sig.name.length === 0 || sig.name.length > 256) {
        throw new RtDbError("BAD_REQUEST", `steps[${i}].awaitSignal.name must be 1..=256 chars`);
      }
      if (sig.timeoutMs === 0) {
        throw new RtDbError("BAD_REQUEST", `steps[${i}].awaitSignal.timeoutMs must be > 0`);
      }
    }
    if (step.retry) {
      if (step.retry.maxAttempts === 0) {
        throw new RtDbError("BAD_REQUEST", `steps[${i}].retry.maxAttempts must be >= 1`);
      }
      const initial = step.retry.initialRetryMs ?? DEFAULT_STEP_RETRY.initialRetryMs;
      const max = step.retry.maxRetryMs ?? DEFAULT_STEP_RETRY.maxRetryMs;
      if (initial === 0 || max < initial) {
        throw new RtDbError(
          "BAD_REQUEST",
          `steps[${i}].retry requires initialRetryMs > 0 and maxRetryMs >= initialRetryMs`,
        );
      }
    }
  }
  const total = spec.steps.reduce((sum, s) => sum + (s.txn ? countSteps(s.txn) : 0), 0);
  if (total > MAX_STEPS) {
    throw new RtDbError(
      "BAD_REQUEST",
      `workflow recursive step count ${total} exceeds MAX_STEPS ${MAX_STEPS}`,
    );
  }
}

/** awaitSignal: gate stored on a run parked with no `timeoutMs` — never due,
 * so only a delivery or a cancel can wake the run (the server's `i64::MAX`).
 * Below any real clock reading `sleepUntil` can be compared against. */
const NEVER_DUE = Number.MAX_SAFE_INTEGER;

/** awaitSignal: serialized-size cap on one signal payload — mirrors server
 * `workflows::MAX_SIGNAL_PAYLOAD_BYTES` (64 KiB). */
export const MAX_SIGNAL_PAYLOAD_BYTES = 64 * 1024;

/** FM-29: a stored workflow run in the in-memory harness. Field names mirror
 * the server's `workflows` table columns; `sleepUntil` is always set (the
 * column is NOT NULL server-side — insert computes the initial gate, later
 * transitions overwrite it, terminal states leave it). The `wait*`/
 * `signalPayload` fields mirror the server's awaitSignal side-table columns
 * (`wait_name`/`waited_since`/`signal_payload`): present only from park until
 * the wait resolves — every leave-`waiting` transition clears them. */
export interface WorkflowRun {
  id: string;
  spec: WorkflowSpec;
  status: WorkflowStatus;
  currentStep: number;
  attempts: number;
  sleepUntil: number;
  lastError?: string;
  waitName?: string;
  waitedSince?: number;
  signalPayload?: unknown;
  createdAt: number;
  updatedAt: number;
  startedAt?: number;
  finishedAt?: number;
  stepOutcomes: StepOutcome[];
}

/** A new job's first due time — mirrors server `scheduler::resolve_when`
 * (cron deferred to the fixed `CRON_STEP_MS` approximation). */
export function dueAtFor(when: ScheduleWhen, now: number): number {
  switch (when.type) {
    case "afterMs":
      return now + when.ms;
    case "runAt":
      return when.ms;
    case "cron":
      return now + CRON_STEP_MS;
    case "interval":
      return now + when.everyMs;
  }
}

/** Maps a stored `ScheduledJob` to its `ScheduleInfo` row (wire shape). */
export function toScheduleInfo(job: ScheduledJob): ScheduleInfo {
  const info: ScheduleInfo = {
    id: job.id,
    kind: job.kind,
    dueAt: job.dueAt,
    status: job.status,
    createdAt: job.createdAt,
    firedCount: job.firedCount,
  };
  if (job.external) {
    info.external = job.external;
  }
  if (job.cron !== undefined) {
    info.cron = job.cron;
  }
  if (job.tz !== undefined) {
    info.tz = job.tz;
  }
  if (job.everyMs !== undefined) {
    info.everyMs = job.everyMs;
  }
  if (job.lastError !== undefined) {
    info.lastError = job.lastError;
  }
  if (job.missedCount > 0) {
    info.missedCount = job.missedCount;
  }
  if (job.lastMissedAt !== undefined) {
    info.lastMissedAt = job.lastMissedAt;
  }
  return info;
}

/** Maps a stored `WorkflowRun` to its `WorkflowInfo` row (wire shape). */
export function toWorkflowInfo(run: WorkflowRun): WorkflowInfo {
  return {
    id: run.id,
    name: run.spec.name,
    status: run.status,
    currentStep: run.currentStep,
    stepCount: run.spec.steps.length,
    attempts: run.attempts,
    sleepUntil: run.sleepUntil,
    createdAt: run.createdAt,
    updatedAt: run.updatedAt,
    ...(run.lastError === undefined ? {} : { lastError: run.lastError }),
    // Projected only while `waiting` (server `info_from_row`): a delivered
    // signal flips the run to `pending` with the slot still set during the
    // latest-wins window — the info row must not leak the stale wait.
    ...(run.status !== "waiting" || run.waitName === undefined ? {} : { waitingFor: run.waitName }),
    ...(run.status !== "waiting" || run.waitedSince === undefined
      ? {}
      : { waitedSince: run.waitedSince }),
    ...(run.startedAt === undefined ? {} : { startedAt: run.startedAt }),
    ...(run.finishedAt === undefined ? {} : { finishedAt: run.finishedAt }),
  };
}

/** Claim and advance every due workflow run (the second `tick` phase):
 * pending runs and waiting runs whose timeout gate expired (server
 * `claim_due` covers both) flip to running — `startedAt` stamped on the
 * first claim only — then advance through `advanceWorkflow`. The caller
 * supplies the run map and the atomic transaction executor (the client's
 * `executeTransaction`). */
export function claimDueWorkflows(
  workflows: Map<string, WorkflowRun>,
  now: number,
  executeTxn: (txn: TransactionJson) => unknown[],
): void {
  const due = [...workflows.values()].filter(
    (run) => (run.status === "pending" || run.status === "waiting") && run.sleepUntil <= now,
  );
  for (const run of due) {
    run.status = "running";
    if (run.startedAt === undefined) {
      run.startedAt = now;
    }
    run.updatedAt = now;
    advanceWorkflow(workflows, run, now, executeTxn);
  }
}

/** FM-29: drives one claimed run across step boundaries, mirroring the
 * server committer's advance loop. Success on the last step finalizes;
 * success earlier moves to the next step and applies its `sleepBeforeMs`
 * gate (a future gate re-pends the run and ends this pass — a `now` gate
 * continues in the same tick); failure re-pends with exponential backoff
 * (no outcome recorded for retried attempts) or, once attempts are
 * exhausted, marks the run failed with a terminal outcome. */
function advanceWorkflow(
  workflows: Map<string, WorkflowRun>,
  run: WorkflowRun,
  now: number,
  executeTxn: (txn: TransactionJson) => unknown[],
): void {
  for (;;) {
    // Per-boundary liveness check: a cancel (or terminal transition) between
    // steps ends the pass — the server re-checks the row each boundary.
    const live = workflows.get(run.id);
    if (live !== run || run.status !== "running") {
      return;
    }
    const step = run.spec.steps[run.currentStep];
    if (!step) {
      return;
    }
    // awaitSignal: side-state only — no document writes. The server's
    // three-way wake discriminator, read off the claimed run: a present
    // payload = delivered, else unset waitedSince = first arrival, else
    // the timeout gate expired.
    if (step.awaitSignal !== undefined) {
      const sig = step.awaitSignal;
      if (run.signalPayload !== undefined) {
        const outcome: StepOutcome = {
          stepIndex: run.currentStep,
          status: "success",
          attempts: run.attempts + 1,
          at: now,
          signal: run.signalPayload,
        };
        run.stepOutcomes.push(outcome);
        run.updatedAt = now;
        delete run.signalPayload;
        delete run.waitName;
        delete run.waitedSince;
        const isLast = run.currentStep + 1 >= run.spec.steps.length;
        if (isLast) {
          run.status = "success";
          run.attempts = 0;
          delete run.lastError;
          run.finishedAt = now;
          return;
        }
        run.currentStep += 1;
        run.attempts = 0;
        // Same next-gate logic as the txn path:
        const gate = now + (run.spec.steps[run.currentStep].sleepBeforeMs ?? 0);
        if (gate > now) {
          run.status = "pending";
          run.sleepUntil = gate;
          run.updatedAt = now;
          return;
        }
        continue;
      }
      // An omitted timeoutMs is never due — cancel or a delivery is the
      // only escape (the server's i64::MAX gate).
      const timeoutGate = sig.timeoutMs !== undefined ? now + sig.timeoutMs : NEVER_DUE;
      if (run.waitedSince === undefined) {
        // First arrival: park.
        run.status = "waiting";
        run.waitName = sig.name;
        run.waitedSince = now;
        run.sleepUntil = timeoutGate;
        delete run.signalPayload;
        run.updatedAt = now;
        return;
      }
      // The row parked and its gate expired: a timed-out attempt. A retry
      // waits the FULL timeoutMs again — never backoff.
      const retry = resolveStepRetry(step.retry);
      run.attempts += 1;
      if (run.attempts < retry.maxAttempts) {
        run.status = "waiting";
        run.waitedSince = now;
        run.sleepUntil = timeoutGate;
        run.updatedAt = now;
        return;
      }
      const error = `awaitSignal '${sig.name}' timed out`;
      run.stepOutcomes.push({
        stepIndex: run.currentStep,
        status: "failed",
        attempts: run.attempts,
        at: now,
        error,
      });
      run.status = "failed";
      run.lastError = error;
      run.finishedAt = now;
      run.updatedAt = now;
      delete run.signalPayload;
      delete run.waitName;
      delete run.waitedSince;
      return;
    }
    if (step.txn === undefined) {
      // Exactly-one-of is submit-validated; a step with neither is a
      // corrupt snapshot — stop the pass rather than throw the tick.
      return;
    }
    let execError: string | null = null;
    try {
      executeTxn(step.txn);
    } catch (e) {
      execError = e instanceof Error ? e.message : String(e);
    }
    if (execError === null) {
      const outcome: StepOutcome = {
        stepIndex: run.currentStep,
        status: "success",
        attempts: run.attempts + 1,
        at: now,
      };
      const isLast = run.currentStep + 1 >= run.spec.steps.length;
      run.stepOutcomes.push(outcome);
      run.updatedAt = now;
      if (isLast) {
        run.status = "success";
        run.attempts = 0;
        delete run.lastError;
        run.finishedAt = now;
        return;
      }
      run.currentStep += 1;
      run.attempts = 0;
      const next = run.spec.steps[run.currentStep];
      const gate = now + (next.sleepBeforeMs ?? 0);
      if (gate > now) {
        run.status = "pending";
        run.sleepUntil = gate;
        run.updatedAt = now;
        return;
      }
      continue;
    }
    const retry = resolveStepRetry(step.retry);
    run.attempts += 1;
    if (run.attempts < retry.maxAttempts) {
      run.status = "pending";
      run.sleepUntil = now + backoffMs(retry, run.attempts);
      run.updatedAt = now;
      return;
    }
    run.stepOutcomes.push({
      stepIndex: run.currentStep,
      status: "failed",
      attempts: run.attempts,
      at: now,
      error: execError,
    });
    run.status = "failed";
    run.lastError = execError;
    run.finishedAt = now;
    run.updatedAt = now;
    return;
  }
}
