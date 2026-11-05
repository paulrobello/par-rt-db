import { RtDbError } from "./errors.js";
import type { ChangeOp, ClientMessage, ServerMessage } from "./protocol.js";

/** Options for {@link RtDbClient.subscribeChanges}. */
export interface SubscribeChangesOptions {
  /** Restrict the feed to these tables (omitted/empty = all tables). */
  tables?: string[];
  /** Resume cursor: deliver ops with `seq > since`. Omitted = start at the current head. */
  since?: number;
  /** Epoch (`logId`) the `since` cursor was taken from; a mismatch yields `CURSOR_EXPIRED`. */
  feedEpoch?: string;
}

/** Per-batch metadata passed to a {@link RtDbClient.subscribeChanges} callback. */
export interface ChangeBatchMeta {
  /** Epoch of the log the ops belong to (same value as the HTTP feed's `logId`). */
  feedEpoch: string;
  /** The log head at send time. */
  head: number;
}

export interface ChangeFeedSub {
  feedId: string;
  tables: string[];
  since: number | undefined;
  feedEpoch: string | undefined;
  onBatch: (ops: ChangeOp[], meta: ChangeBatchMeta) => void;
  onError: ((err: RtDbError) => void) | undefined;
}

export function iterateChanges(
  subscribe: (
    opts: SubscribeChangesOptions,
    onBatch: (ops: ChangeOp[], meta: ChangeBatchMeta) => void,
    onError: (err: RtDbError) => void,
  ) => () => void,
  opts: SubscribeChangesOptions,
): AsyncIterableIterator<{ ops: ChangeOp[]; meta: ChangeBatchMeta }> {
  type Item = { ops: ChangeOp[]; meta: ChangeBatchMeta };
  const queue: Item[] = [];
  let failure: RtDbError | null = null;
  let done = false;
  let wake: (() => void) | null = null;
  const notify = () => {
    const w = wake;
    wake = null;
    w?.();
  };
  const unsubscribe = subscribe(
    opts,
    (ops, meta) => {
      queue.push({ ops, meta });
      notify();
    },
    (err) => {
      failure = err;
      notify();
    },
  );
  const it: AsyncIterableIterator<Item> = {
    [Symbol.asyncIterator]: () => it,
    next: async () => {
      for (;;) {
        const item = queue.shift();
        if (item) {
          return { value: item, done: false };
        }
        if (failure) {
          const err = failure;
          failure = null;
          done = true;
          throw err;
        }
        if (done) {
          return { value: undefined, done: true };
        }
        await new Promise<void>((resolve) => {
          wake = resolve;
        });
      }
    },
    return: async () => {
      done = true;
      unsubscribe();
      notify();
      return { value: undefined, done: true };
    },
  };
  return it;
}

export function sendFeedSubscribe(sub: ChangeFeedSub, send: (frame: ClientMessage) => void): void {
  const frame: ClientMessage = { type: "feedSubscribe", feedId: sub.feedId };
  if (sub.since !== undefined) {
    frame.since = sub.since;
  }
  if (sub.feedEpoch !== undefined) {
    frame.feedEpoch = sub.feedEpoch;
  }
  if (sub.tables.length > 0) {
    frame.tables = sub.tables;
  }
  send(frame);
}

export function onFeedFrame(
  msg: Extract<ServerMessage, { type: "feedEvents" | "feedErr" }>,
  changeFeeds: Map<string, ChangeFeedSub>,
  send: (frame: ClientMessage) => void,
): void {
  const sub = changeFeeds.get(msg.feedId);
  if (!sub) {
    return;
  }
  if (msg.type === "feedErr") {
    changeFeeds.delete(msg.feedId);
    sub.onError?.(RtDbError.fromEnvelope(msg.error));
    return;
  }
  // An epoch change means the log was recreated: seqs restart, so the old
  // cursor no longer applies to this stream.
  const sameEpoch = sub.feedEpoch === undefined || sub.feedEpoch === msg.feedEpoch;
  const lastSeq = sameEpoch ? (sub.since ?? Number.NEGATIVE_INFINITY) : Number.NEGATIVE_INFINITY;
  const fresh = msg.ops.filter((op) => op.seq > lastSeq);
  sub.feedEpoch = msg.feedEpoch;
  if (sub.since === undefined && msg.ops.length === 0) {
    // Started at the head: pin the cursor so a reconnect resumes from here
    // instead of skipping whatever committed while disconnected.
    sub.since = msg.head;
  }
  if (fresh.length > 0) {
    sub.onBatch(fresh, { feedEpoch: msg.feedEpoch, head: msg.head });
    sub.since = Math.max(...fresh.map((o) => o.seq));
  }
  if (msg.ops.length > 0 && changeFeeds.get(msg.feedId) === sub) {
    // Ack even a fully-deduped batch so the server's flow-control window advances.
    const ackSeq = Math.max(...msg.ops.map((o) => o.seq));
    send({ type: "feedAck", feedId: msg.feedId, seq: ackSeq });
  }
}
