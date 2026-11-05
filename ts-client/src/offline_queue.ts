import { RtDbError } from "./errors.js";
import type { StepResult } from "./mutation.js";
import type { ScheduleWhen, TransactionJson, WorkflowSpec, WorkflowStatus } from "./protocol.js";

/** Snapshot of the opt-in offline mutation queue, from
 * {@link RtDbClient.getOfflineQueueStatus}. */
export interface OfflineQueueStatus {
  /** Number of mutations currently queued for replay. */
  depth: number;
  /** Age in ms of the oldest queued entry (from the client's injectable
   * clock), or `null` when the queue is empty. */
  oldestAgeMs: number | null;
}

export interface PendingMutate {
  resolve: (results: StepResult[]) => void;
  reject: (error: RtDbError) => void;
}

export interface QueuedMutate extends PendingMutate {
  mutId: string;
  idempotencyKey?: string;
  txn: TransactionJson;
  /** Client-clock timestamp of the mutation call. The offline queue's TTL and
   * age observability are measured from this, across reconnects. */
  queuedAt: number;
}

/** The ClientMessage a schedule call will send once authenticated, carried while
 * queued so `flushOnAuth` can dispatch it verbatim. */
export type ScheduleMsg =
  | { kind: "schedule"; when: ScheduleWhen; txn: TransactionJson; external?: true }
  | { kind: "cancel"; id: string }
  | { kind: "pause"; id: string }
  | { kind: "resume"; id: string }
  | { kind: "list" };

/** A schedule call awaiting its server reply (resolve/reject plus the message
 * to send). Queued while unauthenticated; once dispatched, only the handlers
 * remain tracked in `pendingSchedules` keyed by `scheduleId`. */
export interface QueuedSchedule {
  scheduleId: string;
  msg: ScheduleMsg;
  resolve: (value: unknown) => void;
  reject: (error: RtDbError) => void;
}

/** The ClientMessage a workflow call will send once authenticated (FM-29). */
export type WorkflowMsg =
  | { kind: "start"; spec: WorkflowSpec }
  | { kind: "cancel"; id: string }
  | { kind: "signal"; id: string; name: string; payload?: unknown }
  | { kind: "list"; status?: WorkflowStatus };

/** A workflow call awaiting its server reply — the FM-29 analogue of
 * {@link QueuedSchedule}, correlated by a `wf-${n}` id. */
export interface QueuedWorkflow {
  workflowId: string;
  msg: WorkflowMsg;
  resolve: (value: unknown) => void;
  reject: (error: RtDbError) => void;
}

/**
 * Default TTL for the opt-in offline mutation queue — deliberately the server's
 * default idempotency-key dedup TTL (`mutation_log::DEFAULT_DEDUP_TTL_MS`): a
 * replay older than the dedup table's memory could double-apply.
 */
export const DEFAULT_OFFLINE_QUEUE_TTL_MS = 300_000;

export function pruneOfflineQueue(
  offlineQueue: QueuedMutate[],
  now: () => number,
  offlineTtlMs: number,
  revertOptimistic: (mutId: string) => void,
): void {
  const cutoff = now() - offlineTtlMs;
  for (let i = offlineQueue.length - 1; i >= 0; i--) {
    const entry = offlineQueue[i];
    if (entry && entry.queuedAt <= cutoff) {
      offlineQueue.splice(i, 1);
      revertOptimistic(entry.mutId);
      entry.reject(new RtDbError("INTERNAL", "offline mutation queue TTL expired"));
    }
  }
}

export function mintOfflineKey(mutId: string, random: () => number): string {
  return `off-${mutId}-${random().toString(36).slice(2, 10)}`;
}

export function offlineQueueStatus(
  offlineQueue: QueuedMutate[],
  now: () => number,
): OfflineQueueStatus {
  if (offlineQueue.length === 0) {
    return { depth: 0, oldestAgeMs: null };
  }
  let oldest = Number.POSITIVE_INFINITY;
  for (const entry of offlineQueue) {
    if (entry.queuedAt < oldest) {
      oldest = entry.queuedAt;
    }
  }
  return { depth: offlineQueue.length, oldestAgeMs: now() - oldest };
}

export function flushOfflineQueue(
  offlineQueue: QueuedMutate[],
  canDispatch: () => boolean,
  dispatch: (entry: QueuedMutate) => void,
): void {
  // Leave the tail queued if a send synchronously closes the socket.
  while (canDispatch() && offlineQueue.length > 0) {
    const entry = offlineQueue.shift();
    if (entry) {
      dispatch(entry);
    }
  }
}
