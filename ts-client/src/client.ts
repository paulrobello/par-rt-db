import { RtDbError } from "./errors.js";
import { RtDbHttpClient } from "./http.js";
import { projectOptimisticUpdate } from "./optimistic.js";
import type {
  AuthedUser,
  ClientMessage,
  QueryJson,
  ScheduleInfo,
  ScheduleWhen,
  ServerMessage,
  TransactionJson,
} from "./protocol.js";
import type { RtQuery } from "./query.js";

/** Minimal surface of a WebSocket the client depends on (browser/Node/bun compatible). */
export interface WebSocketLike {
  send(data: string): void;
  close(code?: number, reason?: string): void;
  onopen: (() => void) | null;
  onmessage: ((ev: { data: unknown }) => void) | null;
  onclose: ((ev: { code: number; reason: string }) => void) | null;
  onerror: (() => void) | null;
}

export type ConnectionState = "idle" | "connecting" | "connected" | "reconnecting" | "closed";
export type AuthState = "unauthenticated" | "authenticating" | "authenticated";

export interface RtDbClientOptions {
  url: string;
  db: string;
  getToken?: () => string | null | Promise<string | null>;
  webSocketFactory?: (url: string) => WebSocketLike;
  backoff?: { baseMs: number; maxMs: number };
  heartbeatMs?: number;
  now?: () => number;
  random?: () => number;
  setTimeoutImpl?: (fn: () => void, ms: number) => ReturnType<typeof setTimeout>;
  clearTimeoutImpl?: (handle: ReturnType<typeof setTimeout>) => void;
  /**
   * When true, a locally-submitted mutation's projected effect is overlaid on
   * each active subscription's last result (via `onUpdate`) before the
   * authoritative server update arrives, then replaced by the server value on
   * the next `queryUpdate`. Off by default; the subscribe/mutate contract is
   * unchanged when disabled.
   */
  optimisticUpdates?: boolean;
}

interface Subscription {
  queryId: string;
  query: QueryJson;
  key: string;
  listeners: Set<(value: unknown) => void>;
  last?: unknown;
  hasValue: boolean;
  /** The last authoritative server value — the base an optimistic overlay reverts to. */
  serverLast?: unknown;
  /** True while `last` includes an unconfirmed optimistic overlay. */
  optimistic: boolean;
}

interface PendingMutate {
  resolve: (results: unknown[]) => void;
  reject: (error: RtDbError) => void;
}

interface QueuedMutate extends PendingMutate {
  mutId: string;
  idempotencyKey?: string;
  txn: TransactionJson;
}

/** The ClientMessage a schedule call will send once authenticated, carried while
 * queued so `flushOnAuth` can dispatch it verbatim. */
type ScheduleMsg =
  | { kind: "schedule"; when: ScheduleWhen; txn: TransactionJson }
  | { kind: "cancel"; id: string }
  | { kind: "pause"; id: string }
  | { kind: "resume"; id: string }
  | { kind: "list" };

/** A schedule call awaiting its server reply (resolve/reject plus the message
 * to send). Queued while unauthenticated; once dispatched, only the handlers
 * remain tracked in `pendingSchedules` keyed by `scheduleId`. */
interface QueuedSchedule {
  scheduleId: string;
  msg: ScheduleMsg;
  resolve: (value: unknown) => void;
  reject: (error: RtDbError) => void;
}

const DEFAULT_BACKOFF = { baseMs: 500, maxMs: 15_000 };
const DEFAULT_HEARTBEAT_MS = 20_000;
/**
 * App-range close code we use for the drops WE initiate (heartbeat timeout,
 * socket error). The WebSocket spec forbids passing the reserved 1006 to
 * `close()` (it throws `InvalidAccessError`), so we must use 1000 or a
 * 4000–4999 code. `handleClose` treats any non-4401 code as a reconnectable drop.
 */
const CLOSE_APP_DROP = 4000;

function isThenable(value: unknown): value is Promise<unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { then?: unknown }).then === "function"
  );
}

function httpToWs(url: string): string {
  return url.replace(/^http/, "ws").replace(/\/+$/, "");
}

/** Reactive WebSocket client: one connection, serialized subscriptions, correlated mutations. */
export class RtDbClient {
  private readonly options: RtDbClientOptions;
  private readonly factory: (url: string) => WebSocketLike;
  private readonly now: () => number;
  private readonly random: () => number;
  private readonly setTimeoutImpl: (fn: () => void, ms: number) => ReturnType<typeof setTimeout>;
  private readonly clearTimeoutImpl: (handle: ReturnType<typeof setTimeout>) => void;
  private readonly backoff: { baseMs: number; maxMs: number };
  private readonly heartbeatMs: number;

  private socket: WebSocketLike | null = null;
  private connState: ConnectionState = "idle";
  private authState: AuthState = "unauthenticated";
  private user: AuthedUser | null = null;
  private token: string | null = null;
  private hasToken = false;

  private readonly subsByKey = new Map<string, Subscription>();
  private readonly subsById = new Map<string, Subscription>();
  private readonly pendingMutates = new Map<string, PendingMutate>();
  private readonly unsentMutates: QueuedMutate[] = [];
  private readonly pendingSchedules = new Map<string, QueuedSchedule>();
  private readonly unsentSchedules: QueuedSchedule[] = [];
  private readonly authListeners = new Set<(state: AuthState, user: AuthedUser | null) => void>();
  private readonly connListeners = new Set<(state: ConnectionState) => void>();
  /** mutId → subscriptions whose last result this mutation optimistically overlaid. */
  private readonly optimisticOverlays = new Map<string, Set<Subscription>>();
  private readonly optimistic: boolean;

  private counter = 0;
  /**
   * Bumps on every socket (re)open and every teardown. Async token resolutions
   * and reconnect-timer callbacks capture the generation they were scheduled in
   * and abort if it has advanced — this is what stops a stale reconnect or a
   * late `getToken` promise from opening a duplicate socket.
   */
  private generation = 0;
  private reconnectAttempt = 0;
  private stopped = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private heartbeatTimer: ReturnType<typeof setTimeout> | null = null;
  private lastPongAt = 0;
  /**
   * SEC-001 phase 2: true when no `getToken` was supplied — the browser's
   * HttpOnly session cookie authenticates the WS upgrade, so the client dials
   * with a tokenless `Auth` instead of landing idle.
   */
  private readonly cookieMode: boolean;

  constructor(options: RtDbClientOptions) {
    this.options = options;
    this.cookieMode = options.getToken === undefined;
    this.factory =
      options.webSocketFactory ?? ((url) => new WebSocket(url) as unknown as WebSocketLike);
    this.now = options.now ?? (() => Date.now());
    this.random = options.random ?? Math.random;
    this.setTimeoutImpl = options.setTimeoutImpl ?? ((fn, ms) => setTimeout(fn, ms));
    this.clearTimeoutImpl = options.clearTimeoutImpl ?? ((h) => clearTimeout(h));
    this.backoff = options.backoff ?? DEFAULT_BACKOFF;
    this.heartbeatMs = options.heartbeatMs ?? DEFAULT_HEARTBEAT_MS;
    this.optimistic = options.optimisticUpdates ?? false;
  }

  connect(): void {
    this.stopped = false;
    if (this.connState === "connecting" || this.connState === "connected") {
      return;
    }
    this.openSocket();
  }

  close(): void {
    this.stopped = true;
    this.generation++;
    this.clearTimers();
    this.detachSocket(1000, "client closed");
    this.setConnState("closed");
    this.setAuthState("unauthenticated");
    this.rejectAllMutates("client is closed");
    this.rejectAllSchedules("client is closed");
    // Drop any overlay left by mutations that already resolved but whose
    // reconciling queryUpdate will now never arrive (no notify — the client is
    // closing). The state is reset to the last authoritative value.
    this.optimisticOverlays.clear();
    for (const sub of this.subsByKey.values()) {
      if (sub.optimistic && sub.serverLast !== undefined) {
        sub.optimistic = false;
        sub.last = sub.serverLast;
      }
    }
  }

  setToken(token: string | null): void {
    this.token = token;
    this.hasToken = true;
    if (this.stopped) {
      return;
    }
    // A fresh credential invalidates every in-flight mutation and any pending
    // reconnect; `openSocket` tears down the old socket without a reconnect.
    this.rejectAllMutates("connection reset for re-authentication");
    this.rejectAllSchedules("connection reset for re-authentication");
    this.reconnectAttempt = 0;
    this.setAuthState("authenticating");
    this.openSocket();
  }

  getAuthState(): AuthState {
    return this.authState;
  }

  getUser(): AuthedUser | null {
    return this.user;
  }

  onAuthChange(cb: (state: AuthState, user: AuthedUser | null) => void): () => void {
    this.authListeners.add(cb);
    return () => this.authListeners.delete(cb);
  }

  getConnectionState(): ConnectionState {
    return this.connState;
  }

  onConnectionChange(cb: (state: ConnectionState) => void): () => void {
    this.connListeners.add(cb);
    return () => this.connListeners.delete(cb);
  }

  subscribe<R>(query: RtQuery<R>, onUpdate: (value: R) => void): () => void {
    const key = JSON.stringify(query.json);
    let sub = this.subsByKey.get(key);
    if (!sub) {
      sub = {
        queryId: `sub-${++this.counter}`,
        query: query.json,
        key,
        listeners: new Set(),
        hasValue: false,
        optimistic: false,
      };
      this.subsByKey.set(key, sub);
      this.subsById.set(sub.queryId, sub);
      if (this.authState === "authenticated") {
        this.send({ type: "subscribe", queryId: sub.queryId, query: sub.query });
      }
    }
    const listener = onUpdate as (value: unknown) => void;
    sub.listeners.add(listener);
    if (sub.hasValue) {
      listener(sub.last);
    }

    return () => {
      const current = this.subsByKey.get(key);
      if (!current) {
        return;
      }
      current.listeners.delete(listener);
      if (current.listeners.size === 0) {
        this.subsByKey.delete(key);
        this.subsById.delete(current.queryId);
        if (this.authState === "authenticated") {
          this.send({ type: "unsubscribe", queryId: current.queryId });
        }
      }
    };
  }

  /**
   * `opts.mutId` is an idempotency key, not a display/tracking id: supply the
   * *same* value again to safely retry a mutation whose result you never
   * received (e.g. after a dropped connection) instead of double-applying
   * it. The server does not fingerprint the transaction body, so reusing a
   * key for a different mutation replays the first one's cached result.
   * Omit it for ordinary at-most-once calls (today's default behavior).
   */
  mutate(txn: TransactionJson, opts?: { mutId?: string }): Promise<unknown[]> {
    const mutId = `mut-${++this.counter}`;
    return new Promise<unknown[]>((resolve, reject) => {
      if (this.stopped) {
        reject(new RtDbError("INTERNAL", "client is closed"));
        return;
      }
      // Overlay the projected effect on every active subscription before the
      // round-trip. Computed synchronously so `onUpdate` fires before this
      // promise even resolves; reconciled (server wins) on the next queryUpdate.
      if (this.optimistic) {
        this.applyOptimistic(mutId, txn);
      }
      const entry: QueuedMutate = { mutId, idempotencyKey: opts?.mutId, txn, resolve, reject };
      if (this.authState === "authenticated" && this.socket) {
        this.dispatchMutate(entry);
      } else {
        // Never sent yet: flush once on the next authOk. Not a retry.
        this.unsentMutates.push(entry);
      }
    });
  }

  /** Schedules `txn` for `when`. Resolves with the new schedule `{id}` on
   * `scheduleOk`; rejects with `RtDbError` on `scheduleErr` (e.g. a bad cron
   * expression — the server validates cron). While unauthenticated, the request
   * queues and fires on the next `authOk`, mirroring `mutate`. */
  schedule(txn: TransactionJson, when: ScheduleWhen): Promise<{ id: string }> {
    return this.queueSchedule<{ id: string }>({ kind: "schedule", when, txn });
  }

  /** Cancels a scheduled job. Resolves on `scheduleAck.ok:true`; rejects with
   * `RtDbError` when the server returns `ok:false` (e.g. unknown id). */
  cancelSchedule(id: string): Promise<void> {
    return this.queueSchedule<void>({ kind: "cancel", id });
  }

  /** Pauses a scheduled job until `resumeSchedule`. Same ack contract as
   * `cancelSchedule`. */
  pauseSchedule(id: string): Promise<void> {
    return this.queueSchedule<void>({ kind: "pause", id });
  }

  /** Resumes a paused scheduled job. Same ack contract as `cancelSchedule`. */
  resumeSchedule(id: string): Promise<void> {
    return this.queueSchedule<void>({ kind: "resume", id });
  }

  /** Lists scheduled jobs. Resolves with the `schedules` array on
   * `listSchedulesOk`. */
  listSchedules(): Promise<ScheduleInfo[]> {
    return this.queueSchedule<ScheduleInfo[]>({ kind: "list" });
  }

  // ---- file storage ----------------------------------------------------------
  //
  // Storage is HTTP-only on the live server; the reactive WS client delegates
  // each call to a transient RtDbHttpClient built from its current connection
  // params. The http client is rebuilt per call rather than cached so a rotated
  // token (setToken / re-auth) is always reflected.

  upload(bytes: Uint8Array, contentType?: string) {
    return this.httpForStorage().upload(bytes, contentType);
  }

  deleteFile(id: string) {
    return this.httpForStorage().deleteFile(id);
  }

  getFileMetadata(id: string) {
    return this.httpForStorage().getFileMetadata(id);
  }

  getUrl(id: string) {
    return this.httpForStorage().getUrl(id);
  }

  private httpForStorage(): RtDbHttpClient {
    return new RtDbHttpClient({
      url: this.options.url,
      db: this.options.db,
      token: this.token ?? "",
    });
  }

  /** Mints a `sch-${n}` correlation id and either dispatches (when authenticated)
   * or queues the request for the next `authOk`, exactly like `mutate`. */
  private queueSchedule<T>(msg: ScheduleMsg): Promise<T> {
    const scheduleId = `sch-${++this.counter}`;
    return new Promise<T>((resolve, reject) => {
      if (this.stopped) {
        reject(new RtDbError("INTERNAL", "client is closed"));
        return;
      }
      const entry: QueuedSchedule = {
        scheduleId,
        msg,
        resolve: resolve as (value: unknown) => void,
        reject,
      };
      if (this.authState === "authenticated" && this.socket) {
        this.dispatchSchedule(entry);
      } else {
        // Never sent yet: flush once on the next authOk. Not a retry.
        this.unsentSchedules.push(entry);
      }
    });
  }

  private dispatchSchedule(entry: QueuedSchedule): void {
    this.pendingSchedules.set(entry.scheduleId, entry);
    switch (entry.msg.kind) {
      case "schedule":
        this.send({
          type: "schedule",
          scheduleId: entry.scheduleId,
          when: entry.msg.when,
          txn: entry.msg.txn,
        });
        break;
      case "cancel":
        this.send({ type: "cancelSchedule", scheduleId: entry.scheduleId, id: entry.msg.id });
        break;
      case "pause":
        this.send({ type: "pauseSchedule", scheduleId: entry.scheduleId, id: entry.msg.id });
        break;
      case "resume":
        this.send({ type: "resumeSchedule", scheduleId: entry.scheduleId, id: entry.msg.id });
        break;
      case "list":
        this.send({ type: "listSchedules", scheduleId: entry.scheduleId });
        break;
    }
  }

  /** Projects `txn` onto each subscription's last result and notifies listeners
   * of the overlaid value. Records which subscriptions changed so a rejected
   * mutation can roll them back to the authoritative `serverLast`. */
  private applyOptimistic(mutId: string, txn: TransactionJson): void {
    const overlaid = new Set<Subscription>();
    for (const sub of this.subsByKey.values()) {
      if (!sub.hasValue) {
        continue;
      }
      const projection = projectOptimisticUpdate(sub.query, sub.last, txn, this.now);
      if (projection.overlaid) {
        sub.optimistic = true;
        this.pushValue(sub, projection.value);
        overlaid.add(sub);
      }
    }
    if (overlaid.size > 0) {
      this.optimisticOverlays.set(mutId, overlaid);
    }
  }

  /** Restores every subscription overlaid by `mutId` to its authoritative server
   * value, if it has not already been reconciled by a queryUpdate. Called on every
   * rejection path (server error, dropped connection, teardown) so an optimistic
   * overlay can never outlive the mutation that produced it. */
  private revertOptimistic(mutId: string): void {
    const subs = this.optimisticOverlays.get(mutId);
    if (!subs) {
      return;
    }
    this.optimisticOverlays.delete(mutId);
    for (const sub of subs) {
      if (sub.optimistic && sub.serverLast !== undefined) {
        sub.optimistic = false;
        this.pushValue(sub, sub.serverLast);
      }
    }
  }

  /** Sets a subscription's last value and notifies its listeners. */
  private pushValue(sub: Subscription, value: unknown): void {
    sub.last = value;
    sub.hasValue = true;
    for (const listener of sub.listeners) {
      listener(value);
    }
  }

  private dispatchMutate(entry: QueuedMutate): void {
    this.pendingMutates.set(entry.mutId, { resolve: entry.resolve, reject: entry.reject });
    this.send({
      type: "mutate",
      mutId: entry.mutId,
      idempotencyKey: entry.idempotencyKey,
      txn: entry.txn,
    });
  }

  private openSocket(): void {
    // Single entry point for opening a connection: cancel any timers, tear down
    // any existing socket (no reconnect), and advance the generation so stale
    // async/timer callbacks abort.
    this.clearTimers();
    this.detachSocket(1000, "reopen");
    const gen = ++this.generation;
    this.setConnState(this.reconnectAttempt === 0 ? "connecting" : "reconnecting");
    // SEC-001 phase 2: cookie mode (no `getToken`) — dial immediately and send a
    // tokenless Auth; the browser's HttpOnly `rtdb_session` cookie authenticates
    // the WS upgrade.
    if (this.cookieMode) {
      this.openWithToken(null);
      return;
    }
    const provided = this.hasToken ? this.token : this.options.getToken!();
    if (isThenable(provided)) {
      // A rejected getToken() is treated as "no credential" rather than left
      // to hang in "connecting" (and to avoid an unhandled promise rejection).
      void provided.then(
        (tok) => {
          if (gen === this.generation && !this.stopped) {
            this.openWithToken(tok as string | null);
          }
        },
        () => {
          if (gen === this.generation && !this.stopped) {
            this.openWithToken(null);
          }
        },
      );
    } else {
      this.openWithToken(provided as string | null);
    }
  }

  private openWithToken(token: string | null): void {
    this.token = token;
    this.hasToken = true;
    if (this.stopped) {
      return;
    }
    if (token == null && !this.cookieMode) {
      // No credential (initial connect without a token, or sign-out): do not
      // dial a socket — that would spin the reconnect loop forever. Land
      // unauthenticated/idle so an explicit connect() can revive later.
      // Cookie mode is exempt: it dials with a tokenless Auth.
      this.setAuthState("unauthenticated");
      this.setConnState("idle");
      return;
    }
    const socket = this.factory(`${httpToWs(this.options.url)}/sync`);
    this.socket = socket;
    this.setAuthState("authenticating");

    socket.onopen = () => {
      if (this.token == null && !this.cookieMode) {
        this.setAuthState("unauthenticated");
        this.setConnState("idle");
        this.detachSocket(1000, "no token");
        return;
      }
      // SEC-001 phase 2: cookie mode sends a tokenless Auth; the browser cookie
      // authenticates the upgrade.
      this.send(
        this.token == null
          ? { type: "auth", db: this.options.db }
          : { type: "auth", token: this.token, db: this.options.db },
      );
    };
    socket.onmessage = (ev) => this.handleMessage(ev.data);
    socket.onclose = (ev) => this.handleClose(ev.code);
    socket.onerror = () => {
      this.socket?.close(CLOSE_APP_DROP, "error");
    };
  }

  private handleMessage(data: unknown): void {
    let msg: ServerMessage;
    try {
      msg = JSON.parse(String(data)) as ServerMessage;
    } catch {
      return;
    }
    switch (msg.type) {
      case "authOk":
        this.setConnState("connected");
        this.reconnectAttempt = 0;
        this.user = msg.user;
        // Resubscribe + flush unsent BEFORE notifying auth listeners, so a
        // listener that subscribes synchronously does not get a duplicate frame.
        this.flushOnAuth();
        this.setAuthState("authenticated");
        this.startHeartbeat();
        break;
      case "authErr":
        this.setAuthState("unauthenticated");
        this.socket?.close(4401, "authErr");
        break;
      case "queryUpdate": {
        const sub = this.subsById.get(msg.queryId);
        if (sub) {
          // Authoritative value: it wins over any optimistic overlay.
          sub.serverLast = msg.result;
          sub.optimistic = false;
          this.pushValue(sub, msg.result);
        }
        break;
      }
      case "subscribeErr": {
        // A malformed query (e.g. unknown index) can never succeed. Remove it
        // from BOTH maps so it is neither resent on reconnect nor left routing
        // updates; listeners stay `undefined`.
        const sub = this.subsById.get(msg.queryId);
        if (sub) {
          this.subsById.delete(sub.queryId);
          this.subsByKey.delete(sub.key);
        }
        break;
      }
      case "mutateOk": {
        const pending = this.pendingMutates.get(msg.mutId);
        this.pendingMutates.delete(msg.mutId);
        // Succeeded: a queryUpdate will reconcile the overlay, so just drop the
        // tracking — do not revert.
        this.optimisticOverlays.delete(msg.mutId);
        pending?.resolve(msg.results);
        break;
      }
      case "mutateErr": {
        const pending = this.pendingMutates.get(msg.mutId);
        this.pendingMutates.delete(msg.mutId);
        // Failed: the server never applied it, so roll the overlay back.
        this.revertOptimistic(msg.mutId);
        pending?.reject(RtDbError.fromEnvelope(msg.error));
        break;
      }
      case "scheduleOk": {
        const pending = this.pendingSchedules.get(msg.scheduleId);
        this.pendingSchedules.delete(msg.scheduleId);
        pending?.resolve({ id: msg.id });
        break;
      }
      case "scheduleErr": {
        const pending = this.pendingSchedules.get(msg.scheduleId);
        this.pendingSchedules.delete(msg.scheduleId);
        pending?.reject(RtDbError.fromEnvelope(msg.error));
        break;
      }
      case "scheduleAck": {
        const pending = this.pendingSchedules.get(msg.scheduleId);
        this.pendingSchedules.delete(msg.scheduleId);
        if (msg.ok) {
          pending?.resolve(undefined);
        } else if (msg.error) {
          pending?.reject(RtDbError.fromEnvelope(msg.error));
        } else {
          pending?.reject(new RtDbError("INTERNAL", "schedule operation failed"));
        }
        break;
      }
      case "listSchedulesOk": {
        const pending = this.pendingSchedules.get(msg.scheduleId);
        this.pendingSchedules.delete(msg.scheduleId);
        pending?.resolve(msg.schedules);
        break;
      }
      case "pong":
        this.lastPongAt = this.now();
        break;
    }
  }

  private flushOnAuth(): void {
    for (const sub of this.subsByKey.values()) {
      this.send({ type: "subscribe", queryId: sub.queryId, query: sub.query });
    }
    const queued = this.unsentMutates.splice(0);
    for (const entry of queued) {
      this.dispatchMutate(entry);
    }
    const queuedSchedules = this.unsentSchedules.splice(0);
    for (const entry of queuedSchedules) {
      this.dispatchSchedule(entry);
    }
  }

  private handleClose(code: number): void {
    this.socket = null;
    this.clearHeartbeat();
    // In-flight (already-sent) mutations are never auto-resent — reject them.
    this.rejectPendingMutates("connection closed before the mutation was acknowledged");
    // Same for in-flight schedule requests: they are never auto-resent.
    this.rejectPendingSchedules("connection closed before the schedule was acknowledged");
    if (code === 4401) {
      this.setAuthState("unauthenticated");
      this.setConnState("idle"); // an explicit connect() (e.g. after re-login) may revive
      return;
    }
    if (this.stopped) {
      this.setConnState("closed");
      return;
    }
    this.setAuthState("authenticating");
    this.scheduleReconnect();
  }

  /** Rejects only in-flight (sent, unacked) mutations; never-sent ones stay queued. */
  private rejectPendingMutates(reason: string): void {
    if (this.pendingMutates.size === 0) {
      return;
    }
    const error = new RtDbError("INTERNAL", reason);
    for (const [mutId, pending] of this.pendingMutates) {
      this.revertOptimistic(mutId);
      pending.reject(error);
    }
    this.pendingMutates.clear();
  }

  /** Rejects every mutation — in-flight and never-sent. Used on terminal teardown. */
  private rejectAllMutates(reason: string): void {
    this.rejectPendingMutates(reason);
    if (this.unsentMutates.length === 0) {
      return;
    }
    const error = new RtDbError("INTERNAL", reason);
    for (const entry of this.unsentMutates.splice(0)) {
      this.revertOptimistic(entry.mutId);
      entry.reject(error);
    }
  }

  /** Rejects only in-flight (sent, unacked) schedule requests; never-sent ones stay queued. */
  private rejectPendingSchedules(reason: string): void {
    if (this.pendingSchedules.size === 0) {
      return;
    }
    const error = new RtDbError("INTERNAL", reason);
    for (const entry of this.pendingSchedules.values()) {
      entry.reject(error);
    }
    this.pendingSchedules.clear();
  }

  /** Rejects every schedule request — in-flight and never-sent. Used on terminal teardown. */
  private rejectAllSchedules(reason: string): void {
    this.rejectPendingSchedules(reason);
    if (this.unsentSchedules.length === 0) {
      return;
    }
    const error = new RtDbError("INTERNAL", reason);
    for (const entry of this.unsentSchedules.splice(0)) {
      entry.reject(error);
    }
  }

  /** Closes the current socket after detaching its handlers, so its `onclose` cannot reconnect. */
  private detachSocket(code: number, reason: string): void {
    const socket = this.socket;
    this.socket = null;
    if (!socket) {
      return;
    }
    socket.onclose = null;
    socket.onmessage = null;
    socket.onopen = null;
    socket.onerror = null;
    try {
      socket.close(code, reason);
    } catch {
      // Ignore invalid-state close (socket already closing/closed).
    }
  }

  private scheduleReconnect(): void {
    const attempt = this.reconnectAttempt++;
    const raw = Math.min(this.backoff.maxMs, this.backoff.baseMs * 2 ** attempt);
    const delay = raw * (0.5 + this.random() * 0.5);
    const gen = this.generation;
    this.reconnectTimer = this.setTimeoutImpl(() => {
      if (gen === this.generation && !this.stopped) {
        this.openSocket();
      }
    }, delay);
  }

  private startHeartbeat(): void {
    this.clearHeartbeat();
    if (this.heartbeatMs <= 0) {
      return;
    }
    this.lastPongAt = this.now();
    this.heartbeatTimer = this.setTimeoutImpl(() => this.beat(), this.heartbeatMs);
  }

  private beat(): void {
    if (this.now() - this.lastPongAt >= this.heartbeatMs * 2) {
      this.socket?.close(CLOSE_APP_DROP, "heartbeat timeout");
      return;
    }
    this.send({ type: "ping" });
    this.heartbeatTimer = this.setTimeoutImpl(() => this.beat(), this.heartbeatMs);
  }

  private clearHeartbeat(): void {
    if (this.heartbeatTimer) {
      this.clearTimeoutImpl(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      this.clearTimeoutImpl(this.reconnectTimer);
      this.reconnectTimer = null;
    }
  }

  private clearTimers(): void {
    this.clearHeartbeat();
    this.clearReconnectTimer();
  }

  private setAuthState(state: AuthState): void {
    if (state === this.authState) {
      return;
    }
    this.authState = state;
    if (state === "unauthenticated") {
      this.user = null;
    }
    for (const cb of this.authListeners) {
      cb(this.authState, this.user);
    }
  }

  private setConnState(state: ConnectionState): void {
    if (state === this.connState) {
      return;
    }
    this.connState = state;
    for (const cb of this.connListeners) {
      cb(this.connState);
    }
  }

  private send(message: ClientMessage): void {
    this.socket?.send(JSON.stringify(message));
  }
}
