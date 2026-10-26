package dev.parrtdb.inmemory

import dev.parrtdb.admin.Directive
import dev.parrtdb.admin.MigrateResult
import dev.parrtdb.wire.AuthedUser
import dev.parrtdb.wire.PresenceMember
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.StepResult
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.UserKind
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.serialization.json.JsonElement
import java.util.concurrent.atomic.AtomicBoolean

/**
 * In-memory engine configuration. [now] and [random] inject the epoch-millis clock and the
 * [0,1) RNG for deterministic ids and timestamps; [presenceRooms] shares a presence backing
 * between clients (null gets a private instance).
 */
public data class InMemoryOptions(
    val now: (() -> Long)? = null,
    val random: (() -> Double)? = null,
    val connectionId: String = "c1",
    val presenceUser: AuthedUser = AuthedUser(UserKind.User),
    val presenceRooms: PresenceRooms? = null,
)

/** Handle for a live query subscription. */
public class Subscription internal constructor(
    private val sub: StoreSubscription,
    private val store: Store,
    private val closing: AtomicBoolean,
) {
    /** Detaches the listener; no further callbacks are delivered. */
    public fun unsubscribe() {
        closing.set(true)
        sub.alive = false
        store.locked { store.subscribers.removeIf { !it.alive } }
    }
}

/**
 * A pure in-memory par-rt-db engine with the same usage surface as the HTTP and WebSocket
 * clients: push a schema, query, mutate, subscribe, presence, and a deterministic [tick].
 */
public class InMemoryClient(opts: InMemoryOptions = InMemoryOptions()) {
    /** The underlying engine state (test and inspection seam). */
    public val store: Store = Store()
    private val presence: PresenceRooms = opts.presenceRooms ?: PresenceRooms()
    private val presenceUser: AuthedUser = opts.presenceUser
    private val connId: String = opts.connectionId
    private val presenceUnsubs = HashMap<String, () -> Unit>()

    init {
        opts.now?.let { store.withClock(it) }
        opts.random?.let { store.withRandom(it) }
    }

    /** Installs [schema] (additive merges; destructive changes are rejected). */
    public fun pushSchema(schema: JsonElement) {
        store.pushSchema(schema)
    }

    /** Runs a one-shot query. */
    public fun query(q: Query): JsonElement = evalQuery(store, q)

    /** Executes [txn] atomically; a repeated [idempotencyKey] replays the first results. */
    public fun mutate(txn: Transaction, idempotencyKey: String? = null): List<StepResult> = applyTxn(store, txn, idempotencyKey)

    /** Plans and applies a declarative migration; with [dryRun] nothing commits. */
    public fun applyMigration(directives: List<Directive>, dryRun: Boolean): MigrateResult = applyMigration(store, directives, dryRun)

    /** Advances the scheduler and TTL reaper to [now]; returns the number of reaped docs. */
    public fun tick(now: Long): Int = store.tick(now)

    /** Toggles the per-database read-only freeze; while set, [mutate] fails with READ_ONLY. */
    public fun setReadOnly(readOnly: Boolean) {
        store.setReadOnly(readOnly)
    }

    /**
     * Subscribes to [q]: [onUpdate] receives the initial snapshot synchronously, then one call
     * per real change to the result. A failing query never fires. Callbacks run after the
     * store lock is released.
     */
    public fun subscribe(q: Query, onUpdate: (JsonElement) -> Unit): Subscription {
        val closing = AtomicBoolean(false)
        val sub =
            StoreSubscription(query = q, table = q.table, callback = { v -> if (!closing.get()) onUpdate(v) })
        val initial =
            store.locked {
                val snapshot = runCatching { evalQueryLocked(store, q) }.getOrNull()
                if (snapshot != null) {
                    sub.last = diffCanonical(snapshot, q)
                    sub.hasLast = true
                }
                store.subscribers.add(sub)
                snapshot
            }
        if (initial != null) sub.callback(initial)
        return Subscription(sub, store, closing)
    }

    /** [subscribe] as a cold [Flow]; collection subscribes, cancellation unsubscribes. */
    public fun subscribeFlow(q: Query): Flow<JsonElement> =
        callbackFlow {
            val sub = subscribe(q) { trySend(it) }
            awaitClose { sub.unsubscribe() }
        }.buffer(Channel.UNLIMITED)

    /**
     * Joins [room] with [state]; [onUpdate] fires with the member list on join and on every
     * change (own update, or a peer's on a shared backing).
     */
    public fun joinPresence(room: String, state: JsonElement, onUpdate: ((List<PresenceMember>) -> Unit)? = null) {
        presence.join(room, PresenceMember(connId, presenceUser, state))
        if (onUpdate != null) {
            presenceUnsubs[room] = presence.subscribe(room, onUpdate)
            onUpdate(presence.snapshot(room))
        }
    }

    /** Broadcasts updated state for this connection in [room]; a positive [ttlMs] schedules a state-nulling expiry. */
    public fun updatePresence(room: String, state: JsonElement, ttlMs: Long? = null) {
        presence.update(room, connId, state, ttlMs, store.nowMs())
    }

    /** Leaves [room] and drops this client's listener. */
    public fun leavePresence(room: String) {
        presenceUnsubs.remove(room)?.invoke()
        presence.leave(room, connId)
    }

    /** The room's members in join order. */
    public fun presenceSnapshot(room: String): List<PresenceMember> = presence.snapshot(room)
}
