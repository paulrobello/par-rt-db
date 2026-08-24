import { RtDbError } from "./errors.js";
import type { TransformOpts, UploadInput } from "./http.js";
import { RtDbHttpClient } from "./http.js";
import { parseStepResults, type StepResult } from "./mutation.js";
import { projectOptimisticUpdate } from "./optimistic.js";
import type {
  AuthedUser,
  ClientMessage,
  PresenceMember,
  QueryJson,
  ScheduleInfo,
  ScheduleWhen,
  ServerMessage,
  TransactionJson,
  WorkflowInfo,
  WorkflowSpec,
  WorkflowStatus,
} from "./protocol.js";
import { PROTOCOL_VERSION } from "./protocol.js";
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
/**
 * `"unreachable"`: the auth endpoint could not be reached — the socket kept
 * closing before the auth handshake completed (`authOk`/`authErr` never
 * arrived) for `authUnreachableAfterAttempts` consecutive attempts. The
 * client keeps retrying in the background (the outage or the origin
 * misconfiguration may heal), but apps can now render "sign-in unavailable"
 * instead of an eternal "authenticating" spinner. Any completed handshake,
 * `4401`, or a fresh `connect()`/`setToken()` clears it.
 */
export type AuthState = "unauthenticated" | "authenticating" | "authenticated" | "unreachable";

export interface RtDbClientOptions {
  url: string;
  db: string;
  getToken?: () => string | null | Promise<string | null>;
  webSocketFactory?: (url: string) => WebSocketLike;
  backoff?: { baseMs: number; maxMs: number };
  heartbeatMs?: number;
  /**
   * After this many consecutive socket closes during the auth handshake
   * (never reached `authOk`/`authErr`), `authState` becomes `"unreachable"`
   * (the client keeps reconnecting — the state is a signal, not a stop).
   * Bounds the blind spot of a cookie-mode app served from a non-allowlisted
   * origin: the WS upgrade is 403'd, which the browser surfaces only as close
   * 1006, indistinguishable from an outage without this counter. Default 5;
   * 0 disables the signal (pre-fix behavior).
   */
  authUnreachableAfterAttempts?: number;
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
  resolve: (results: StepResult[]) => void;
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

/** The ClientMessage a workflow call will send once authenticated (FM-29). */
type WorkflowMsg =
  | { kind: "start"; spec: WorkflowSpec }
  | { kind: "cancel"; id: string }
  | { kind: "signal"; id: string; name: string; payload?: unknown }
  | { kind: "list"; status?: WorkflowStatus };

/** A workflow call awaiting its server reply — the FM-29 analogue of
 * {@link QueuedSchedule}, correlated by a `wf-${n}` id. */
interface QueuedWorkflow {
  workflowId: string;
  msg: WorkflowMsg;
  resolve: (value: unknown) => void;
  reject: (error: RtDbError) => void;
}

const DEFAULT_BACKOFF = { baseMs: 500, maxMs: 15_000 };
const DEFAULT_HEARTBEAT_MS = 20_000;
const DEFAULT_AUTH_UNREACHABLE_AFTER = 5;
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
  private readonly authUnreachableAfter: number;
  /** Consecutive socket closes before the auth handshake ever completed. */
  private preAuthFailures = 0;

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
  private readonly pendingWorkflows = new Map<string, QueuedWorkflow>();
  private readonly unsentWorkflows: QueuedWorkflow[] = [];
  private readonly authListeners = new Set<(state: AuthState, user: AuthedUser | null) => void>();
  private readonly connListeners = new Set<(state: ConnectionState) => void>();
  /** Per-room presence callbacks. Inbound `presenceSnapshot` fans out to the
   * registered set for `msg.room`, mirroring how `subsById` routes `queryUpdate`
   * to per-`queryId` handlers. `leavePresence` drops the whole set (the server
   * stops sending snapshots for that room once the connection has left). */
  private readonly presenceListeners = new Map<string, Set<(members: PresenceMember[]) => void>>();
  /** Joined rooms + their last state, so `flushOnAuth` can re-send every join
   * after a reconnect/authOk — mirroring how `subsByKey` re-sends `subscribe`
   * frames. A `leavePresence` removes the entry; an `updatePresence` updates
   * the cached state so a reconnect re-joins with the latest, not the original. */
  private readonly joinedRooms = new Map<string, unknown>();
  /** Per-room count of outstanding `presence()` join interests. `leavePresence`
   *  only tears the room down (wire frame + `joinedRooms`/listener clear) when
   *  the LAST listener detaches, so two `usePresence(room)` hooks sharing a room
   *  don't kill it for each other on the first unmount. The wire JOIN itself is
   *  already idempotent server-side (one membership per conn+room). */
  private readonly presenceRefcounts = new Map<string, number>();
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
   * with a tokenless `Auth` instead of landing idle. Exposed (readonly) so the
   * React provider can branch: in cookie mode it never touches `localStorage`
   * and re-dials via `setToken(null)` after sign-in/sign-out (SEC-002).
   */
  readonly cookieMode: boolean;

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
    this.authUnreachableAfter =
      options.authUnreachableAfterAttempts ?? DEFAULT_AUTH_UNREACHABLE_AFTER;
    this.optimistic = options.optimisticUpdates ?? false;
  }

  /**
   * Opens the WebSocket connection and begins the auth handshake. Safe to
   * call when already connecting or connected — it is a no-op in those
   * states. Clears any prior `"unreachable"` auth signal so the next
   * `authUnreachableAfterAttempts` closes are needed to re-signal it.
   */
  connect(): void {
    this.stopped = false;
    if (this.connState === "connecting" || this.connState === "connected") {
      return;
    }
    // An explicit connect() is a fresh attempt (e.g. the user hit retry after
    // "unreachable") — clear the pre-auth failure count and the surfaced
    // signal so the next N closes are needed to re-signal.
    this.preAuthFailures = 0;
    if (this.authState === "unreachable") {
      this.setAuthState("authenticating");
    }
    this.openSocket();
  }

  /**
   * Closes the connection and stops reconnecting. Rejects every in-flight
   * mutation, scheduled-txn call, and workflow call, and reverts any
   * unresolved optimistic subscription overlay back to its last
   * server-confirmed value. Call `connect()` to reopen.
   */
  close(): void {
    this.stopped = true;
    this.generation++;
    this.clearTimers();
    this.detachSocket(1000, "client closed");
    this.setConnState("closed");
    this.setAuthState("unauthenticated");
    this.rejectAllMutates("client is closed");
    this.rejectAllSchedules("client is closed");
    this.rejectAllWorkflows("client is closed");
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

  /**
   * Sets (or clears, with `null`) the bearer token used for the WS
   * handshake and re-dials the connection under the new credential.
   * Rejects any in-flight mutation, scheduled-txn call, or workflow call —
   * they were authorized under the old credential and must be retried.
   * Cookie-mode apps call this with `null` after sign-in/sign-out to
   * trigger a re-dial without touching `localStorage`.
   */
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
    this.rejectAllWorkflows("connection reset for re-authentication");
    this.reconnectAttempt = 0;
    this.preAuthFailures = 0;
    this.setAuthState("authenticating");
    this.openSocket();
  }

  /** Returns the current auth state (`"unauthenticated"` |
   * `"authenticating"` | `"authenticated"` | `"unreachable"`). */
  getAuthState(): AuthState {
    return this.authState;
  }

  /** Returns the authenticated principal, or `null` when not authenticated. */
  getUser(): AuthedUser | null {
    return this.user;
  }

  /**
   * Registers a callback fired on every auth state or user change. Returns
   * an unsubscribe function.
   */
  onAuthChange(cb: (state: AuthState, user: AuthedUser | null) => void): () => void {
    this.authListeners.add(cb);
    return () => this.authListeners.delete(cb);
  }

  /** Returns the current WebSocket connection state. */
  getConnectionState(): ConnectionState {
    return this.connState;
  }

  /**
   * Registers a callback fired on every connection state change. Returns
   * an unsubscribe function.
   */
  onConnectionChange(cb: (state: ConnectionState) => void): () => void {
    this.connListeners.add(cb);
    return () => this.connListeners.delete(cb);
  }

  /**
   * Subscribes to a live query: `onUpdate` fires with the current result
   * immediately (once known) and again on every server-pushed change.
   * Deduplicates identical queries under one server subscription. Returns
   * an unsubscribe function; the server subscription is torn down once its
   * last listener unsubscribes.
   */
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
   * `opts.idempotencyKey` is an idempotency key, not a display/tracking id:
   * supply the *same* value again to safely retry a mutation whose result you
   * never received (e.g. after a dropped connection) instead of double-applying
   * it. The server does not fingerprint the transaction body, so reusing a key
   * for a different mutation replays the first one's cached result. Omit it for
   * ordinary at-most-once calls (today's default behavior).
   *
   * `opts.mutId` is a deprecated alias for `opts.idempotencyKey` and remains
   * accepted for backwards compatibility; it is unrelated to the wire-only
   * `mutId` reply-correlation field.
   */
  mutate(
    txn: TransactionJson,
    opts?: {
      idempotencyKey?: string;
      /** @deprecated use `idempotencyKey`. Unrelated to the wire reply-correlation field. */
      mutId?: string;
    },
  ): Promise<StepResult[]> {
    const mutId = `mut-${++this.counter}`;
    return new Promise<StepResult[]>((resolve, reject) => {
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
      const entry: QueuedMutate = {
        mutId,
        // ARC-133: only set `idempotencyKey` when it has a value. With
        // exactOptionalPropertyTypes the `?:` field cannot accept a literal
        // `undefined`, so the ?? chain must resolve to either a real key or
        // omission (absence = server generates one), never an explicit undefined.
        ...(opts?.idempotencyKey || opts?.mutId
          ? { idempotencyKey: (opts?.idempotencyKey ?? opts?.mutId) as string }
          : {}),
        txn,
        resolve,
        reject,
      };
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

  /** Cancels a scheduled job. Resolves `true` on `scheduleAck.ok:true`; a bare
   * `ok:false` (unknown/already-terminal job) resolves `false` — not an error;
   * `ok:false` carrying an `error` rejects. */
  cancelSchedule(id: string): Promise<boolean> {
    return this.queueSchedule<boolean>({ kind: "cancel", id });
  }

  /** Pauses a scheduled job until `resumeSchedule`. Same ack contract as
   * `cancelSchedule`. */
  pauseSchedule(id: string): Promise<boolean> {
    return this.queueSchedule<boolean>({ kind: "pause", id });
  }

  /** Resumes a paused scheduled job. Same ack contract as `cancelSchedule`. */
  resumeSchedule(id: string): Promise<boolean> {
    return this.queueSchedule<boolean>({ kind: "resume", id });
  }

  /** Lists scheduled jobs. Resolves with the `schedules` array on
   * `listSchedulesOk`. */
  listSchedules(): Promise<ScheduleInfo[]> {
    return this.queueSchedule<ScheduleInfo[]>({ kind: "list" });
  }

  /** Starts a durable workflow run from `spec` (FM-29). Resolves with the new
   * run's `WorkflowInfo` on `startWorkflowOk`; rejects with `RtDbError` on
   * `startWorkflowErr` (e.g. a spec validation failure). Queues while
   * unauthenticated exactly like `mutate`. */
  startWorkflow(spec: WorkflowSpec): Promise<WorkflowInfo> {
    return this.queueWorkflow<WorkflowInfo>({ kind: "start", spec });
  }

  /** Cancels a pending/running workflow by id (FM-29). Resolves `true` on
   * `workflowAck.ok:true`; a bare `ok:false` (unknown/terminal run) resolves
   * `false` — not an error; `ok:false` carrying an `error` rejects. */
  cancelWorkflow(id: string): Promise<boolean> {
    return this.queueWorkflow<boolean>({ kind: "cancel", id });
  }

  /** Delivers a named signal to a waiting run's `awaitSignal` step. Resolves
   * `true` on `workflowAck.ok:true` (latest-wins: the payload overwrites any
   * earlier unconsumed delivery); typed failures reject via the ack's `error`
   * envelope — unknown id (`NOT_FOUND`), not waiting / name mismatch
   * (`CONFLICT`). */
  signalWorkflow(id: string, name: string, payload?: unknown): Promise<boolean> {
    return this.queueWorkflow<boolean>({
      kind: "signal",
      id,
      name,
      ...(payload === undefined ? {} : { payload }),
    });
  }

  /** Lists workflow runs, newest first, optionally filtered by `status`
   * (FM-29). Resolves with the `workflows` array on `listWorkflowsOk`. A list
   * failure arrives typed `startWorkflowErr` (the frame vocabulary has no
   * `listWorkflowsErr` — the `listSchedules` precedent). */
  listWorkflows(status?: WorkflowStatus): Promise<WorkflowInfo[]> {
    return this.queueWorkflow<WorkflowInfo[]>({
      kind: "list",
      ...(status === undefined ? {} : { status }),
    });
  }

  // ---- file storage ----------------------------------------------------------
  //
  // Storage is HTTP-only on the live server; the reactive WS client delegates
  // each call to a transient RtDbHttpClient built from its current connection
  // params. The http client is rebuilt per call rather than cached so a rotated
  // token (setToken / re-auth) is always reflected.

  /** Upload raw bytes (`Uint8Array`/`ArrayBuffer`/`Blob`/`ReadableStream`) to
   * `POST /api/storage/{db}`; resolves with `{ id, sha256, size, contentType }`. */
  upload(body: UploadInput, contentType?: string) {
    return this.httpForStorage().upload(body, contentType);
  }

  /** Delete a stored file — also revokes its public serve URL (idempotent). */
  deleteFile(id: string) {
    return this.httpForStorage().deleteFile(id);
  }

  /** Fetch stored metadata: `{ id, sha256, size, contentType?, creationTime }`. */
  getFileMetadata(id: string) {
    return this.httpForStorage().getFileMetadata(id);
  }

  /** Mint an HMAC-signed, time-limited public URL for one blob (default 1h,
   * max 7d). Unlike `getUrl` this makes a network request. */
  getSignedUrl(id: string, ttlSeconds?: number) {
    return this.httpForStorage().getSignedUrl(id, ttlSeconds);
  }

  /** The public serve URL for `id` — no request is made. */
  getUrl(id: string) {
    return this.httpForStorage().getUrl(id);
  }

  /** The public serve URL with image-transform params appended (`w`/`h`/`fit`/
   * `q`/`format`) — no request is made. */
  transformUrl(id: string, opts: TransformOpts) {
    return this.httpForStorage().transformUrl(id, opts);
  }

  private httpForStorage(): RtDbHttpClient {
    return new RtDbHttpClient({
      url: this.options.url,
      db: this.options.db,
      token: this.token ?? "",
    });
  }

  // ---- presence -------------------------------------------------------------

  /**
   * Joins presence room `room`, optionally with initial `state`. When `onUpdate`
   * is supplied, it fires on every inbound `presenceSnapshot` for this room
   * (including the first one the server sends on join, which lists the current
   * members). Returns an unsubscribe that stops listening but does NOT leave
   * the room — call `leavePresence(room)` for that. Mirrors `subscribe`'s
   * send + register pattern, keyed by `room` instead of `queryId`.
   *
   * The join is recorded in `joinedRooms` and the wire frame is sent ONLY when
   * authenticated — same gate as `subscribe`. A pre-auth call buffers the join,
   * and `flushOnAuth` replays it on authOk (exactly how `subsByKey` replays
   * `subscribe` frames), so a direct caller doing `connect(); presence(...)`
   * sends exactly one join, not two.
   */
  presence(
    room: string,
    state?: unknown,
    onUpdate?: (members: PresenceMember[]) => void,
  ): () => void {
    this.joinedRooms.set(room, state);
    this.presenceRefcounts.set(room, (this.presenceRefcounts.get(room) ?? 0) + 1);
    if (onUpdate) {
      let set = this.presenceListeners.get(room);
      if (!set) {
        set = new Set();
        this.presenceListeners.set(room, set);
      }
      set.add(onUpdate);
    }
    if (this.authState === "authenticated") {
      this.send({ type: "presence", room, state });
    }
    return () => {
      const set = this.presenceListeners.get(room);
      if (!set) {
        return;
      }
      set.delete(onUpdate as (members: PresenceMember[]) => void);
      if (set.size === 0) {
        this.presenceListeners.delete(room);
      }
    };
  }

  /** Broadcasts updated `state` for the current connection in `room`. The
   * server fans out a fresh `presenceSnapshot` to every member of the room.
   * Also updates the cached join state so a reconnect re-joins with the latest.
   * No-op on the wire when not authenticated — without a join the server has
   * no member to update — but still updates `joinedRooms` so the buffered join
   * (if any) replays with the latest state on authOk. */
  updatePresence(room: string, state: unknown, ttlMs?: number): void {
    if (this.joinedRooms.has(room)) {
      this.joinedRooms.set(room, state);
    }
    if (this.authState === "authenticated") {
      const frame: ClientMessage =
        ttlMs == null
          ? { type: "presenceState", room, state }
          : { type: "presenceState", room, state, ttlMs };
      this.send(frame);
    }
  }

  /** Leaves presence room `room`: drops one outstanding join interest (from a
   *  matching `presence()` call) and, when the LAST listener detaches, clears
   *  local state (`joinedRooms`, listeners) and sends the wire `leavePresence`
   *  frame (only when authenticated). This refcounting keeps a shared room alive
   *  until every `usePresence(room)` / `presence(room)` consumer has detached —
   *  the server models one membership per conn+room, so the wire leave must fire
   *  once, not once per listener. Local state is cleared regardless of auth
   *  state on the final detach so a buffered pre-auth join does not replay after
   *  the caller has left. */
  leavePresence(room: string): void {
    const count = this.presenceRefcounts.get(room) ?? 0;
    if (count > 1) {
      // Other listeners still hold this room — drop one join interest only.
      this.presenceRefcounts.set(room, count - 1);
      return;
    }
    if (count <= 0) {
      // Not joined (or already left) — no-op, never a duplicate wire leave.
      return;
    }
    // count === 1: last detach — tear down fully.
    this.presenceRefcounts.delete(room);
    this.joinedRooms.delete(room);
    this.presenceListeners.delete(room);
    if (this.authState === "authenticated") {
      this.send({ type: "leavePresence", room });
    }
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

  /** Mints a `wf-${n}` correlation id and either dispatches (when authenticated)
   * or queues the request for the next `authOk`, exactly like `mutate`. */
  private queueWorkflow<T>(msg: WorkflowMsg): Promise<T> {
    const workflowId = `wf-${++this.counter}`;
    return new Promise<T>((resolve, reject) => {
      if (this.stopped) {
        reject(new RtDbError("INTERNAL", "client is closed"));
        return;
      }
      const entry: QueuedWorkflow = {
        workflowId,
        msg,
        resolve: resolve as (value: unknown) => void,
        reject,
      };
      if (this.authState === "authenticated" && this.socket) {
        this.dispatchWorkflow(entry);
      } else {
        this.unsentWorkflows.push(entry);
      }
    });
  }

  private dispatchWorkflow(entry: QueuedWorkflow): void {
    this.pendingWorkflows.set(entry.workflowId, entry);
    switch (entry.msg.kind) {
      case "start":
        this.send({ type: "startWorkflow", workflowId: entry.workflowId, spec: entry.msg.spec });
        break;
      case "cancel":
        this.send({ type: "cancelWorkflow", workflowId: entry.workflowId, id: entry.msg.id });
        break;
      case "signal":
        this.send({
          type: "signalWorkflow",
          workflowId: entry.workflowId,
          id: entry.msg.id,
          name: entry.msg.name,
          ...(entry.msg.payload === undefined ? {} : { payload: entry.msg.payload }),
        });
        break;
      case "list":
        this.send({
          type: "listWorkflows",
          workflowId: entry.workflowId,
          ...(entry.msg.status === undefined ? {} : { status: entry.msg.status }),
        });
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
      ...(entry.idempotencyKey === undefined ? {} : { idempotencyKey: entry.idempotencyKey }),
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
    // the WS upgrade. Testing `getToken` here rather than the `cookieMode` field
    // (which the constructor derives from this same immutable option) is what
    // narrows it to defined below.
    const getToken = this.options.getToken;
    if (getToken === undefined) {
      this.openWithToken(null);
      return;
    }
    const provided = this.hasToken ? this.token : getToken();
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
    // Keep a surfaced "unreachable" across retry dials — flipping back to
    // "authenticating" one backoff after the signal would erase it. Only a
    // completed handshake, a 4401, or a fresh connect()/setToken() clears it.
    if (this.authState !== "unreachable") {
      this.setAuthState("authenticating");
    }

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
          ? { type: "auth", db: this.options.db, protocolVersion: PROTOCOL_VERSION }
          : {
              type: "auth",
              token: this.token,
              db: this.options.db,
              protocolVersion: PROTOCOL_VERSION,
            },
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
        this.preAuthFailures = 0;
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
      case "queryUpdate":
      case "subscribeErr":
        this.onQueryUpdate(msg);
        break;
      case "mutateOk":
      case "mutateErr":
        this.onMutateReply(msg);
        break;
      case "scheduleOk":
      case "scheduleErr":
      case "scheduleAck":
      case "listSchedulesOk":
        this.onScheduleReply(msg);
        break;
      case "startWorkflowOk":
      case "startWorkflowErr":
      case "workflowAck":
      case "listWorkflowsOk":
        this.onWorkflowReply(msg);
        break;
      case "presenceSnapshot":
      case "presenceErr":
        this.onPresence(msg);
        break;
      case "pong":
        this.lastPongAt = this.now();
        break;
    }
  }

  /** Route a `queryUpdate`/`subscribeErr` frame to its subscription's listeners. */
  private onQueryUpdate(
    msg: Extract<ServerMessage, { type: "queryUpdate" | "subscribeErr" }>,
  ): void {
    if (msg.type === "queryUpdate") {
      const sub = this.subsById.get(msg.queryId);
      if (sub) {
        // Authoritative value: it wins over any optimistic overlay.
        sub.serverLast = msg.result;
        sub.optimistic = false;
        this.pushValue(sub, msg.result);
      }
      return;
    }
    // A malformed query (e.g. unknown index) can never succeed. Remove it
    // from BOTH maps so it is neither resent on reconnect nor left routing
    // updates; listeners stay `undefined`.
    const sub = this.subsById.get(msg.queryId);
    if (sub) {
      this.subsById.delete(sub.queryId);
      this.subsByKey.delete(sub.key);
    }
  }

  /** Route a `mutateOk`/`mutateErr` frame to its pending caller, syncing the
   * optimistic-overlay tracking for both outcomes. */
  private onMutateReply(msg: Extract<ServerMessage, { type: "mutateOk" | "mutateErr" }>): void {
    const pending = this.pendingMutates.get(msg.mutId);
    this.pendingMutates.delete(msg.mutId);
    if (msg.type === "mutateOk") {
      // Succeeded: a queryUpdate will reconcile the overlay, so just drop the
      // tracking — do not revert.
      this.optimisticOverlays.delete(msg.mutId);
      pending?.resolve(parseStepResults(msg.results));
      return;
    }
    // Failed: the server never applied it, so roll the overlay back.
    this.revertOptimistic(msg.mutId);
    pending?.reject(RtDbError.fromEnvelope(msg.error));
  }

  /** Route a `scheduleOk`/`scheduleErr`/`scheduleAck`/`listSchedulesOk` frame
   * to its pending caller. */
  private onScheduleReply(
    msg: Extract<
      ServerMessage,
      { type: "scheduleOk" | "scheduleErr" | "scheduleAck" | "listSchedulesOk" }
    >,
  ): void {
    const pending = this.pendingSchedules.get(msg.scheduleId);
    this.pendingSchedules.delete(msg.scheduleId);
    switch (msg.type) {
      case "scheduleOk":
        pending?.resolve({ id: msg.id });
        break;
      case "scheduleErr":
        pending?.reject(RtDbError.fromEnvelope(msg.error));
        break;
      case "scheduleAck":
        if (msg.ok) {
          pending?.resolve(true);
        } else if (msg.error) {
          pending?.reject(RtDbError.fromEnvelope(msg.error));
        } else {
          // Bare ok:false = unknown/terminal job: a no-op, not a failure.
          pending?.resolve(false);
        }
        break;
      case "listSchedulesOk":
        pending?.resolve(msg.schedules);
        break;
    }
  }

  /** Route a `startWorkflowOk`/`startWorkflowErr`/`workflowAck`/
   * `listWorkflowsOk` frame to its pending caller. */
  private onWorkflowReply(
    msg: Extract<
      ServerMessage,
      { type: "startWorkflowOk" | "startWorkflowErr" | "workflowAck" | "listWorkflowsOk" }
    >,
  ): void {
    const pending = this.pendingWorkflows.get(msg.workflowId);
    this.pendingWorkflows.delete(msg.workflowId);
    switch (msg.type) {
      case "startWorkflowOk":
        pending?.resolve(msg.info);
        break;
      case "startWorkflowErr":
        // Also carries ListWorkflows failures: the server types both replies
        // with this frame (no listWorkflowsErr exists), so a pending list
        // entry under the same correlation id rejects here too.
        pending?.reject(RtDbError.fromEnvelope(msg.error));
        break;
      case "workflowAck":
        if (msg.ok) {
          pending?.resolve(true);
        } else if (msg.error) {
          pending?.reject(RtDbError.fromEnvelope(msg.error));
        } else {
          // Bare ok:false = unknown/terminal run: a no-op, not a failure.
          pending?.resolve(false);
        }
        break;
      case "listWorkflowsOk":
        pending?.resolve(msg.workflows);
        break;
    }
  }

  /** Route a `presenceSnapshot`/`presenceErr` frame to its room's listeners. */
  private onPresence(
    msg: Extract<ServerMessage, { type: "presenceSnapshot" | "presenceErr" }>,
  ): void {
    if (msg.type === "presenceSnapshot") {
      // Per-room fan-out, mirroring how `queryUpdate` routes to per-`queryId`
      // handlers via `subsById`.
      const set = this.presenceListeners.get(msg.room);
      if (set) {
        for (const fn of set) {
          fn(msg.members);
        }
      }
      return;
    }
    // The server rejected the join (e.g. presence not enabled). Drop local
    // listeners so a stale room doesn't keep accumulating snapshots the
    // caller can no longer act on.
    this.presenceListeners.delete(msg.room);
  }

  private flushOnAuth(): void {
    for (const sub of this.subsByKey.values()) {
      this.send({ type: "subscribe", queryId: sub.queryId, query: sub.query });
    }
    // Replay buffered presence joins, mirroring subscription replay above.
    for (const [room, state] of this.joinedRooms) {
      this.send({ type: "presence", room, state });
    }
    const queued = this.unsentMutates.splice(0);
    for (const entry of queued) {
      this.dispatchMutate(entry);
    }
    const queuedSchedules = this.unsentSchedules.splice(0);
    for (const entry of queuedSchedules) {
      this.dispatchSchedule(entry);
    }
    const queuedWorkflows = this.unsentWorkflows.splice(0);
    for (const entry of queuedWorkflows) {
      this.dispatchWorkflow(entry);
    }
  }

  private handleClose(code: number): void {
    this.socket = null;
    this.clearHeartbeat();
    // In-flight (already-sent) mutations are never auto-resent — reject them.
    this.rejectPendingMutates("connection closed before the mutation was acknowledged");
    // Same for in-flight schedule and workflow requests: they are never auto-resent.
    this.rejectPendingSchedules("connection closed before the schedule was acknowledged");
    this.rejectPendingWorkflows("connection closed before the workflow call was acknowledged");
    if (code === 4401) {
      this.preAuthFailures = 0;
      this.setAuthState("unauthenticated");
      this.setConnState("idle"); // an explicit connect() (e.g. after re-login) may revive
      return;
    }
    if (this.stopped) {
      this.setConnState("closed");
      return;
    }
    if (this.authState === "authenticated") {
      // A drop AFTER a completed handshake is a connection blip — reconnect
      // from a clean counter. Only closes that never reached authOk/authErr
      // count toward "unreachable".
      this.preAuthFailures = 0;
    } else {
      this.preAuthFailures += 1;
    }
    this.setAuthState(
      this.authUnreachableAfter > 0 && this.preAuthFailures >= this.authUnreachableAfter
        ? "unreachable"
        : "authenticating",
    );
    // Even in "unreachable" the retry continues — the state is a surfaced
    // signal for the app to render, not a stop.
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

  /** Rejects only in-flight (sent, unacked) workflow requests; never-sent ones stay queued. */
  private rejectPendingWorkflows(reason: string): void {
    if (this.pendingWorkflows.size === 0) {
      return;
    }
    const error = new RtDbError("INTERNAL", reason);
    for (const entry of this.pendingWorkflows.values()) {
      entry.reject(error);
    }
    this.pendingWorkflows.clear();
  }

  /** Rejects every workflow request — in-flight and never-sent. Used on terminal teardown. */
  private rejectAllWorkflows(reason: string): void {
    this.rejectPendingWorkflows(reason);
    if (this.unsentWorkflows.length === 0) {
      return;
    }
    const error = new RtDbError("INTERNAL", reason);
    for (const entry of this.unsentWorkflows.splice(0)) {
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
