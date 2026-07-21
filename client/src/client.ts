import { RtDbError } from "./errors.js";
import type {
  AuthedUser,
  ClientMessage,
  QueryJson,
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
  getToken: () => string | null | Promise<string | null>;
  webSocketFactory?: (url: string) => WebSocketLike;
  backoff?: { baseMs: number; maxMs: number };
  heartbeatMs?: number;
  now?: () => number;
  random?: () => number;
  setTimeoutImpl?: (fn: () => void, ms: number) => ReturnType<typeof setTimeout>;
  clearTimeoutImpl?: (handle: ReturnType<typeof setTimeout>) => void;
}

interface Subscription {
  queryId: string;
  query: QueryJson;
  key: string;
  listeners: Set<(value: unknown) => void>;
  last?: unknown;
  hasValue: boolean;
}

interface PendingMutate {
  resolve: (results: unknown[]) => void;
  reject: (error: RtDbError) => void;
}

interface QueuedMutate extends PendingMutate {
  mutId: string;
  txn: TransactionJson;
}

const DEFAULT_BACKOFF = { baseMs: 500, maxMs: 15_000 };
const DEFAULT_HEARTBEAT_MS = 20_000;

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

  private readonly subsByKey = new Map<string, Subscription>();
  private readonly subsById = new Map<string, Subscription>();
  private readonly pendingMutates = new Map<string, PendingMutate>();
  private readonly unsentMutates: QueuedMutate[] = [];
  private readonly authListeners = new Set<(state: AuthState, user: AuthedUser | null) => void>();

  private counter = 0;
  private reconnectAttempt = 0;
  private stopped = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private heartbeatTimer: ReturnType<typeof setTimeout> | null = null;
  private lastPongAt = 0;

  constructor(options: RtDbClientOptions) {
    this.options = options;
    this.factory =
      options.webSocketFactory ?? ((url) => new WebSocket(url) as unknown as WebSocketLike);
    this.now = options.now ?? (() => Date.now());
    this.random = options.random ?? Math.random;
    this.setTimeoutImpl = options.setTimeoutImpl ?? ((fn, ms) => setTimeout(fn, ms));
    this.clearTimeoutImpl = options.clearTimeoutImpl ?? ((h) => clearTimeout(h));
    this.backoff = options.backoff ?? DEFAULT_BACKOFF;
    this.heartbeatMs = options.heartbeatMs ?? DEFAULT_HEARTBEAT_MS;
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
    this.clearTimers();
    this.connState = "closed";
    this.detachAndClose(1000, "client closed");
    this.rejectPendingMutates("client closed");
  }

  setToken(token: string | null): void {
    this.token = token;
    if (this.stopped) {
      return;
    }
    // Tear the current connection down without triggering a reconnect, then re-auth.
    this.detachAndClose(1000, "reauth");
    this.rejectPendingMutates("connection reset for re-authentication");
    this.reconnectAttempt = 0;
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

  mutate(txn: TransactionJson): Promise<unknown[]> {
    const mutId = `mut-${++this.counter}`;
    return new Promise<unknown[]>((resolve, reject) => {
      const entry: QueuedMutate = { mutId, txn, resolve, reject };
      if (this.authState === "authenticated") {
        this.dispatchMutate(entry);
      } else {
        // Never sent yet: flush once on the next authOk. Not a retry.
        this.unsentMutates.push(entry);
      }
    });
  }

  private dispatchMutate(entry: QueuedMutate): void {
    this.pendingMutates.set(entry.mutId, { resolve: entry.resolve, reject: entry.reject });
    this.send({ type: "mutate", mutId: entry.mutId, txn: entry.txn });
  }

  private openSocket(): void {
    this.connState = this.reconnectAttempt === 0 ? "connecting" : "reconnecting";
    const provided = this.token ?? this.options.getToken();
    if (isThenable(provided)) {
      void provided.then((tok) => this.openWithToken(tok as string | null));
    } else {
      this.openWithToken(provided);
    }
  }

  private openWithToken(token: string | null): void {
    this.token = token;
    if (this.stopped) {
      return;
    }
    const socket = this.factory(`${httpToWs(this.options.url)}/sync`);
    this.socket = socket;
    this.setAuthState("authenticating");

    socket.onopen = () => {
      if (this.token == null) {
        this.setAuthState("unauthenticated");
        this.detachAndClose(1000, "no token");
        return;
      }
      this.send({ type: "auth", token: this.token, db: this.options.db });
    };
    socket.onmessage = (ev) => this.handleMessage(ev.data);
    socket.onclose = (ev) => this.handleClose(ev.code);
    socket.onerror = () => socket.close(1006, "error");
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
        this.connState = "connected";
        this.reconnectAttempt = 0;
        this.user = msg.user;
        this.setAuthState("authenticated");
        this.flushOnAuth();
        this.startHeartbeat();
        break;
      case "authErr":
        this.setAuthState("unauthenticated");
        this.socket?.close(4401, "authErr");
        break;
      case "queryUpdate": {
        const sub = this.subsById.get(msg.queryId);
        if (sub) {
          sub.last = msg.result;
          sub.hasValue = true;
          for (const listener of sub.listeners) {
            listener(msg.result);
          }
        }
        break;
      }
      case "subscribeErr":
        // A malformed query (e.g. unknown index) can never succeed; drop the
        // subscription so it is not resent on reconnect. Listeners stay `undefined`.
        this.subsById.delete(msg.queryId);
        break;
      case "mutateOk": {
        const pending = this.pendingMutates.get(msg.mutId);
        this.pendingMutates.delete(msg.mutId);
        pending?.resolve(msg.results);
        break;
      }
      case "mutateErr": {
        const pending = this.pendingMutates.get(msg.mutId);
        this.pendingMutates.delete(msg.mutId);
        pending?.reject(RtDbError.fromEnvelope(msg.error));
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
  }

  private handleClose(code: number): void {
    this.socket = null;
    this.clearHeartbeat();
    // In-flight (already-sent) mutations are never auto-resent — reject them.
    this.rejectPendingMutates("connection closed before the mutation was acknowledged");
    if (code === 4401) {
      this.setAuthState("unauthenticated");
      return; // do not reconnect until setToken is called
    }
    if (this.stopped) {
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
    for (const pending of this.pendingMutates.values()) {
      pending.reject(error);
    }
    this.pendingMutates.clear();
  }

  /** Closes the current socket without letting its `onclose` schedule a reconnect. */
  private detachAndClose(code: number, reason: string): void {
    const socket = this.socket;
    this.socket = null;
    if (socket) {
      socket.onclose = null;
      socket.close(code, reason);
    }
  }

  private scheduleReconnect(): void {
    const attempt = this.reconnectAttempt++;
    const raw = Math.min(this.backoff.maxMs, this.backoff.baseMs * 2 ** attempt);
    const delay = raw * (0.5 + this.random() * 0.5);
    this.reconnectTimer = this.setTimeoutImpl(() => this.openSocket(), delay);
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
    if (this.now() - this.lastPongAt > this.heartbeatMs * 2) {
      this.socket?.close(1006, "heartbeat timeout");
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

  private clearTimers(): void {
    this.clearHeartbeat();
    if (this.reconnectTimer) {
      this.clearTimeoutImpl(this.reconnectTimer);
      this.reconnectTimer = null;
    }
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

  private send(message: ClientMessage): void {
    this.socket?.send(JSON.stringify(message));
  }
}
