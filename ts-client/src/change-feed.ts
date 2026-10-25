import { RtDbError } from "./errors.js";
import type { ChangeFeedResponse, ChangeOp } from "./protocol.js";

/**
 * Options for {@link RtDbHttpClient.streamChanges}.
 */
export interface StreamChangesOptions {
  /**
   * Cursor to resume from — every op with `seq > since` is delivered.
   * Default 0 (the whole retained log).
   */
  since?: number;
  /**
   * Restrict the stream to one table. `head` stays global, so the feed
   * stays live for tables with sparse traffic.
   */
  table?: string;
  /**
   * Page size per poll (server default 500, clamped to 1000).
   */
  limit?: number;
  /**
   * How long to wait before re-polling after a page comes back empty
   * (the stream is caught up). Default 1000ms.
   */
  pollIntervalMs?: number;
  /**
   * Base for the jittered exponential retry backoff after a failed poll.
   * Default 500ms.
   */
  backoffBaseMs?: number;
  /**
   * Cap for the retry backoff. Default 15000ms.
   */
  backoffMaxMs?: number;
  /**
   * Abort signal — aborting ends the iteration, like breaking out of the
   * `for await`.
   */
  signal?: AbortSignal;
}

/**
 * Auto-resuming iterator over the durable per-db change feed
 * (`GET /api/db/{db}/changes`, machine tokens only). Created via
 * {@link RtDbHttpClient.streamChanges}; iterate with `for await`, stop by
 * breaking out, aborting `signal`, or calling `.stop()`.
 *
 * Each poll fetches one page with `since = cursor`, yields its ops one by
 * one, and advances `cursor` to the last yielded op's `seq`. A failed poll
 * retries from the same cursor on a jittered exponential backoff — the feed
 * never skips or replays an op across an error. When a page comes back
 * empty the stream is caught up and waits `pollIntervalMs` before polling
 * again.
 *
 * Two failures are terminal rather than retried, mirroring `rtdb changes
 * --follow`: `CURSOR_EXPIRED` (the cursor is older than retention or ahead
 * of the log — the caller must resync from `since: 0` deliberately) and a
 * `logId` change mid-stream (the db was dropped and recreated under the
 * same name — seqs restarted, cursors are meaningless). Both surface as a
 * thrown {@link RtDbError} and end the iteration.
 *
 * The iterator never persists the cursor: read `.cursor` (typically on
 * every yielded op) and store it wherever restart-resume durability lives.
 */
export class ChangeFeedStream implements AsyncIterable<ChangeOp> {
  private readonly fetchPage: (since: number) => Promise<ChangeFeedResponse>;
  private readonly pollIntervalMs: number;
  private readonly backoffBaseMs: number;
  private readonly backoffMaxMs: number;
  private readonly signal: AbortSignal | undefined;
  private readonly waiters: Array<() => void> = [];
  private stopped = false;
  private cursorValue: number;
  private logId: string | null = null;

  /** @internal — construct via {@link RtDbHttpClient.streamChanges}. */
  constructor(
    fetchPage: (since: number) => Promise<ChangeFeedResponse>,
    opts: StreamChangesOptions = {},
  ) {
    this.fetchPage = fetchPage;
    this.cursorValue = opts.since ?? 0;
    this.pollIntervalMs = opts.pollIntervalMs ?? 1000;
    this.backoffBaseMs = opts.backoffBaseMs ?? 500;
    this.backoffMaxMs = opts.backoffMaxMs ?? 15000;
    this.signal = opts.signal;
  }

  /**
   * The resume cursor: the last op's `seq` handed to the consumer. Persist
   * this value (per yielded op or on shutdown) and pass it back as `since`
   * after a restart.
   */
  get cursor(): number {
    return this.cursorValue;
  }

  /** Ends the iteration — a pending poll wait wakes and the generator returns. */
  stop(): void {
    this.stopped = true;
    for (const wake of this.waiters.splice(0)) wake();
  }

  /**
   * Iterates change ops from the resume cursor onward, polling for new pages
   * as they land. On a failed poll it retries from the last cursor with
   * jittered backoff; a terminal error (`CURSOR_EXPIRED`, or the feed's
   * `logId` changing mid-stream) is thrown once and ends iteration.
   */
  async *[Symbol.asyncIterator](): AsyncGenerator<ChangeOp, void, void> {
    const onAbort = () => this.stop();
    this.signal?.addEventListener("abort", onAbort);
    let attempt = 0;
    try {
      while (!this.stopped) {
        let page: ChangeFeedResponse;
        try {
          page = await this.fetchPage(this.cursorValue);
        } catch (error) {
          if (error instanceof RtDbError && error.code === "CURSOR_EXPIRED") {
            throw error;
          }
          if (this.stopped) return;
          attempt += 1;
          await this.sleep(this.backoffDelay(attempt));
          continue;
        }
        if (this.logId === null) {
          this.logId = page.logId;
        } else if (page.logId !== this.logId) {
          throw new RtDbError(
            "INTERNAL",
            `change feed logId changed (${this.logId} -> ${page.logId}): the database was dropped and recreated; resync from since: 0`,
          );
        }
        attempt = 0;
        if (page.ops.length === 0) {
          await this.sleep(this.pollIntervalMs);
          continue;
        }
        for (const op of page.ops) {
          this.cursorValue = op.seq;
          yield op;
          if (this.stopped) return;
        }
      }
    } finally {
      this.signal?.removeEventListener("abort", onAbort);
      this.stopped = true;
    }
  }

  /** `min(max, base * 2^attempt) * (0.5 + rand * 0.5)` — the reactive client's reconnect schedule. */
  private backoffDelay(attempt: number): number {
    const raw = Math.min(this.backoffMaxMs, this.backoffBaseMs * 2 ** Math.min(attempt, 30));
    return raw * (0.5 + Math.random() * 0.5);
  }

  private sleep(ms: number): Promise<void> {
    return new Promise<void>((resolve) => {
      let done = false;
      const finish = () => {
        if (done) return;
        done = true;
        clearTimeout(timer);
        resolve();
      };
      const timer = setTimeout(finish, ms);
      this.waiters.push(finish);
    });
  }
}
