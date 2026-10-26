package dev.parrtdb.ws

import dev.parrtdb.errors.ErrorCode
import dev.parrtdb.errors.RtDbException
import dev.parrtdb.wire.ClientMessage
import dev.parrtdb.wire.ErrorEnvelope
import dev.parrtdb.wire.PROTOCOL_VERSION
import dev.parrtdb.wire.PresenceMember
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.ScheduleInfo
import dev.parrtdb.wire.ScheduleWhen
import dev.parrtdb.wire.ServerMessage
import dev.parrtdb.wire.StepResult
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.WireJson
import dev.parrtdb.wire.WorkflowInfo
import dev.parrtdb.wire.WorkflowSpec
import dev.parrtdb.wire.WorkflowStatus
import java.net.URI
import java.net.http.HttpClient
import java.util.UUID
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.BufferOverflow
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonElement

/** Tunables for [RtDbWsClient]. */
data class WsOptions(
    val backoffBaseMs: Long = 500,
    val backoffMaxMs: Long = 15_000,
    val heartbeatMs: Long = 20_000,
    val authTimeoutMs: Long = 10_000,
    /** Observes connection-loss and reconnect-failure reasons. */
    val onError: ((Throwable) -> Unit)? = null,
)

internal fun newCorrelationId(): String = UUID.randomUUID().toString().replace("-", "")

internal fun ErrorEnvelope.toException(): RtDbException = RtDbException(ErrorCode.fromWire(code) ?: ErrorCode.INTERNAL, message, retryAfter)

/** Maps `http(s)://host[:port]` to `ws(s)://host[:port]/sync`. */
fun syncUrl(baseUrl: String): String {
    val trimmed = baseUrl.trimEnd('/')
    val ws = if (trimmed.startsWith("https://")) "wss://" + trimmed.removePrefix("https://") else if (trimmed.startsWith("http://")) "ws://" + trimmed.removePrefix("http://") else trimmed
    return "$ws/sync"
}

/**
 * The `/sync` WebSocket client. [connect] dials and authenticates; afterwards a manager coroutine
 * keeps the connection alive (ping heartbeat, exponential-backoff reconnect) and replays every live
 * subscription and presence room on each new connection. [tokens] is called on every (re)connect.
 *
 * Request methods throw [RtDbException] for server errors and [WsClosedException] when the
 * connection is down. They do not time out by themselves: wrap them in `withTimeout` when needed.
 */
class RtDbWsClient(
    private val wsUrl: String,
    private val db: String,
    private val tokens: suspend () -> String,
    private val options: WsOptions = WsOptions(),
    private val http: HttpClient = HttpClient.newHttpClient(),
) {
    private class Sub(val id: String, val query: Query, val updates: Channel<JsonElement>)

    private class Room(val name: String, var state: JsonElement?) {
        val fold = PresenceFold()
        val collectors = LinkedHashSet<Channel<List<PresenceMember>>>()
        var last: List<PresenceMember>? = null
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val lock = Any()
    private var conn: WsConnection? = null
    private var closed = false
    private var manager: Job? = null
    private val subs = HashMap<String, Sub>()
    private val rooms = HashMap<String, Room>()
    private val pending = HashMap<String, CompletableDeferred<ServerMessage>>()

    /** Dials, authenticates and starts the manager. Throws [RtDbException] on authErr. */
    suspend fun connect() {
        synchronized(lock) { check(!closed) { "client is closed" }; check(manager == null) { "already connected" } }
        val c = establish()
        synchronized(lock) { conn = c }
        manager = scope.launch { run(c) }
    }

    /** Closes the connection, ends every subscription flow normally and cancels the manager. */
    suspend fun close() {
        val (c, subList, roomList, waiting) =
            synchronized(lock) {
                if (closed) return
                closed = true
                val c = conn
                conn = null
                Quad(c, subs.values.toList(), rooms.values.toList(), pending.values.toList())
            }
        waiting.forEach { it.completeExceptionally(WsClosedException(1000, "client closed")) }
        subList.forEach { it.updates.close() }
        roomList.forEach { r -> r.collectors.toList().forEach { it.close() } }
        scope.cancel()
        c?.close()
    }

    // ---- connection lifecycle ------------------------------------------------------------

    private data class Quad<A, B, C, D>(val a: A, val b: B, val c: C, val d: D)

    private suspend fun establish(): WsConnection {
        val token = tokens()
        val c = WsConnection.dial(http, URI.create(wsUrl))
        try {
            c.send(ClientMessage.Auth(token, db, PROTOCOL_VERSION).toJson().let { WireJson.encode(it) })
            val first = withTimeout(options.authTimeoutMs) { c.incoming.receive() }
            when (val m = ServerMessage.fromJson(WireJson.parse(first))) {
                is ServerMessage.AuthOk -> return c
                is ServerMessage.AuthErr -> throw m.error.toException()
                else -> throw WsClosedException(1002, "unexpected frame before authOk")
            }
        } catch (e: Throwable) {
            withContext(NonCancellable) { c.close() }
            throw e
        }
    }

    private suspend fun run(first: WsConnection) {
        var c: WsConnection = first
        val base = options.backoffBaseMs
        while (scope.isActive) {
            val hb = scope.launch { heartbeat() }
            val reason = pump(c)
            hb.cancel()
            val waiting =
                synchronized(lock) {
                    if (closed) return
                    conn = null
                    pending.values.toList().also { pending.clear() }
                }
            waiting.forEach { it.completeExceptionally(WsClosedException(1006, "connection lost")) }
            options.onError?.invoke(reason)
            c.close()
            var wait = base
            while (true) {
                delay(wait)
                if (synchronized(lock) { closed }) return
                try {
                    c = establish()
                    synchronized(lock) { if (closed) { return } else conn = c }
                    replay(c)
                    break
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Exception) {
                    options.onError?.invoke(e)
                    wait = (wait * 2).coerceAtMost(options.backoffMaxMs)
                }
            }
        }
    }

    /** Reads frames until the socket ends; returns the loss reason. */
    private suspend fun pump(c: WsConnection): Throwable {
        try {
            for (text in c.incoming) {
                val msg = try { ServerMessage.fromJson(WireJson.parse(text)) } catch (e: Exception) { continue }
                dispatch(msg)
            }
            return WsClosedException(1006, "connection closed")
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            return e
        }
    }

    private suspend fun heartbeat() {
        while (true) {
            delay(options.heartbeatMs)
            val c = synchronized(lock) { conn } ?: continue
            c.checkAlive()
            runCatching { send(ClientMessage.Ping) }
        }
    }

    private suspend fun replay(c: WsConnection) {
        val (subList, roomList) = synchronized(lock) { subs.values.toList() to rooms.values.toList() }
        for (s in subList) runCatching { c.send(WireJson.encode(ClientMessage.Subscribe(s.id, s.query).toJson())) }
        for (r in roomList) runCatching { c.send(WireJson.encode(ClientMessage.Presence(r.name, r.state).toJson())) }
    }

    // ---- frame dispatch ------------------------------------------------------------------

    private suspend fun dispatch(msg: ServerMessage) {
        when (msg) {
            is ServerMessage.QueryUpdate -> synchronized(lock) { subs[msg.queryId] }?.updates?.trySend(msg.result)
            is ServerMessage.SubscribeErr -> synchronized(lock) { subs[msg.queryId] }?.updates?.close(msg.error.toException())
            is ServerMessage.MutateOk -> reply(msg.mutId, msg)
            is ServerMessage.MutateErr -> reply(msg.mutId, msg)
            is ServerMessage.ScheduleOk -> reply(msg.scheduleId, msg)
            is ServerMessage.ScheduleErr -> reply(msg.scheduleId, msg)
            is ServerMessage.ScheduleAck -> reply(msg.scheduleId, msg)
            is ServerMessage.ListSchedulesOk -> reply(msg.scheduleId, msg)
            is ServerMessage.StartWorkflowOk -> reply(msg.workflowId, msg)
            is ServerMessage.StartWorkflowErr -> reply(msg.workflowId, msg)
            is ServerMessage.WorkflowAck -> reply(msg.workflowId, msg)
            is ServerMessage.ListWorkflowsOk -> reply(msg.workflowId, msg)
            is ServerMessage.PresenceSnapshot -> presenceFrame(msg.room) { it.fold.snapshot(msg) }
            is ServerMessage.PresenceDelta -> presenceFrame(msg.room) { it.fold.delta(msg) }
            is ServerMessage.PresenceErr ->
                synchronized(lock) { rooms[msg.room]?.collectors?.toList() }?.forEach { it.close(msg.error.toException()) }
            is ServerMessage.AuthOk, is ServerMessage.AuthErr, ServerMessage.Pong -> Unit
        }
    }

    private fun reply(corr: String, msg: ServerMessage) {
        synchronized(lock) { pending.remove(corr) }?.complete(msg)
    }

    private suspend fun presenceFrame(name: String, fold: (Room) -> PresenceFold.Outcome) {
        var resync: JsonElement? = null
        var targets: List<Channel<List<PresenceMember>>> = emptyList()
        var members: List<PresenceMember>? = null
        synchronized(lock) {
            val room = rooms[name] ?: return
            when (val o = fold(room)) {
                is PresenceFold.Outcome.Emit -> { room.last = o.members; members = o.members; targets = room.collectors.toList() }
                PresenceFold.Outcome.Drop -> Unit
                PresenceFold.Outcome.Resync -> resync = room.state ?: kotlinx.serialization.json.JsonNull
            }
        }
        members?.let { m -> targets.forEach { it.trySend(m) } }
        if (resync != null) {
            // Re-joining is idempotent server-side and makes the next broadcast a full snapshot.
            val state = if (resync is kotlinx.serialization.json.JsonNull) null else resync
            runCatching { send(ClientMessage.Presence(name, state)) }
        }
    }

    // ---- sending -------------------------------------------------------------------------

    private suspend fun send(msg: ClientMessage) {
        val c = synchronized(lock) { conn } ?: throw WsClosedException(1006, "not connected")
        c.send(WireJson.encode(msg.toJson()))
    }

    private suspend fun request(corr: String, frame: ClientMessage): ServerMessage {
        val d = CompletableDeferred<ServerMessage>()
        synchronized(lock) { pending[corr] = d }
        try {
            send(frame)
            return d.await()
        } finally {
            synchronized(lock) { pending.remove(corr) }
        }
    }

    // ---- subscriptions -------------------------------------------------------------------

    /**
     * Subscribes on collect and emits each `queryUpdate` result; the subscription is unsubscribed when
     * collection is cancelled. A `subscribeErr` ends the flow with an [RtDbException]. Updates are
     * buffered (64, oldest dropped) so a slow collector never blocks the read loop. Live
     * subscriptions are replayed after a reconnect.
     */
    fun subscribe(query: Query): Flow<JsonElement> =
        flow {
            val id = newCorrelationId()
            val sub = Sub(id, query, Channel(64, BufferOverflow.DROP_OLDEST))
            synchronized(lock) {
                check(!closed) { "client is closed" }
                subs[id] = sub
            }
            try {
                // A failed send is fine while reconnecting: the replay re-sends the subscribe frame.
                try { send(ClientMessage.Subscribe(id, query)) } catch (e: WsClosedException) { if (synchronized(lock) { closed }) throw e }
                for (v in sub.updates) emit(v)
            } finally {
                synchronized(lock) { subs.remove(id) }
                withContext(NonCancellable) { runCatching { send(ClientMessage.Unsubscribe(id)) } }
            }
        }

    // ---- mutations, schedules, workflows ------------------------------------------------

    /** Runs a transaction over the socket; correlates `mutateOk` / `mutateErr` by mutId. */
    suspend fun mutate(txn: Transaction, idempotencyKey: String? = null): List<StepResult> {
        val id = newCorrelationId()
        return when (val m = request(id, ClientMessage.Mutate(id, txn, idempotencyKey?.takeIf { it.isNotEmpty() }))) {
            is ServerMessage.MutateOk -> m.results.map { StepResult.fromJson(it) }
            is ServerMessage.MutateErr -> throw m.error.toException()
            else -> throw unexpected("mutate")
        }
    }

    /** Creates a scheduled job and returns its id. */
    suspend fun schedule(whenSpec: ScheduleWhen, txn: Transaction, external: Boolean = false): String {
        val id = newCorrelationId()
        return when (val m = request(id, ClientMessage.Schedule(id, whenSpec, txn, if (external) true else null))) {
            is ServerMessage.ScheduleOk -> m.id
            is ServerMessage.ScheduleErr -> throw m.error.toException()
            else -> throw unexpected("schedule")
        }
    }

    suspend fun listSchedules(): List<ScheduleInfo> {
        val id = newCorrelationId()
        return when (val m = request(id, ClientMessage.ListSchedules(id))) {
            is ServerMessage.ListSchedulesOk -> m.schedules
            is ServerMessage.ScheduleErr -> throw m.error.toException()
            else -> throw unexpected("listSchedules")
        }
    }

    /** Cancels a job; false = unknown or already terminal. */
    suspend fun cancelSchedule(scheduleId: String): Boolean = scheduleAck { ClientMessage.CancelSchedule(it, scheduleId) }

    suspend fun pauseSchedule(scheduleId: String): Boolean = scheduleAck { ClientMessage.PauseSchedule(it, scheduleId) }

    suspend fun resumeSchedule(scheduleId: String): Boolean = scheduleAck { ClientMessage.ResumeSchedule(it, scheduleId) }

    private suspend fun scheduleAck(build: (String) -> ClientMessage): Boolean {
        val corr = newCorrelationId()
        return when (val m = request(corr, build(corr))) {
            is ServerMessage.ScheduleAck -> m.error?.let { throw it.toException() } ?: m.ok
            is ServerMessage.ScheduleErr -> throw m.error.toException()
            else -> throw unexpected("schedule ack")
        }
    }

    /** Starts a workflow run and returns its id. */
    suspend fun startWorkflow(spec: WorkflowSpec): String {
        val corr = newCorrelationId()
        return when (val m = request(corr, ClientMessage.StartWorkflow(corr, spec))) {
            is ServerMessage.StartWorkflowOk -> m.info.id
            is ServerMessage.StartWorkflowErr -> throw m.error.toException()
            else -> throw unexpected("startWorkflow")
        }
    }

    suspend fun cancelWorkflow(id: String): Boolean = workflowAck { ClientMessage.CancelWorkflow(it, id) }

    /** Delivers a named signal to a waiting run; false = not delivered. */
    suspend fun signalWorkflow(id: String, name: String, payload: JsonElement? = null): Boolean =
        workflowAck { ClientMessage.SignalWorkflow(it, id, name, payload) }

    private suspend fun workflowAck(build: (String) -> ClientMessage): Boolean {
        val corr = newCorrelationId()
        return when (val m = request(corr, build(corr))) {
            is ServerMessage.WorkflowAck -> m.error?.let { throw it.toException() } ?: m.ok
            is ServerMessage.StartWorkflowErr -> throw m.error.toException()
            else -> throw unexpected("workflow ack")
        }
    }

    suspend fun listWorkflows(status: WorkflowStatus? = null): List<WorkflowInfo> {
        val corr = newCorrelationId()
        return when (val m = request(corr, ClientMessage.ListWorkflows(corr, status))) {
            is ServerMessage.ListWorkflowsOk -> m.workflows
            else -> throw unexpected("listWorkflows")
        }
    }

    private fun unexpected(what: String) = RtDbException(ErrorCode.INTERNAL, "unexpected reply to $what")

    // ---- presence ------------------------------------------------------------------------

    /**
     * Joins [room] with [state] on collect and emits the room's full member list on every change
     * (a snapshot replaces it, a delta is folded in). Leaves the room when the last collector of that
     * room is cancelled. A `presenceErr` ends the flow with an [RtDbException].
     */
    fun presence(room: String, state: JsonElement? = null): Flow<List<PresenceMember>> =
        flow {
            val ch = Channel<List<PresenceMember>>(64, BufferOverflow.DROP_OLDEST)
            var first = false
            var replay: List<PresenceMember>? = null
            synchronized(lock) {
                check(!closed) { "client is closed" }
                val r = rooms.getOrPut(room) { first = true; Room(room, state) }
                if (first) r.state = state else replay = r.last
                r.collectors += ch
            }
            replay?.let { ch.trySend(it) }
            try {
                if (first) send(ClientMessage.Presence(room, state))
                for (v in ch) emit(v)
            } finally {
                val lastOut = synchronized(lock) {
                    val r = rooms[room]
                    r?.collectors?.remove(ch)
                    if (r != null && r.collectors.isEmpty()) { rooms.remove(room); true } else false
                }
                if (lastOut) withContext(NonCancellable) { runCatching { send(ClientMessage.LeavePresence(room)) } }
            }
        }

    /** Updates this connection's state in a joined room; [ttlMs] makes it expire server-side. */
    suspend fun setPresenceState(room: String, state: JsonElement, ttlMs: Long? = null) {
        synchronized(lock) { rooms[room]?.state = state }
        send(ClientMessage.PresenceState(room, state, ttlMs))
    }
}
