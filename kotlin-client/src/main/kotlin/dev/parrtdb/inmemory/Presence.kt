package dev.parrtdb.inmemory

import dev.parrtdb.wire.PresenceMember
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Shared presence backing: room to connection to member, per-room listeners, and per-member
 * state expiry. Two clients sharing one instance see each other's joins, updates and leaves.
 * Listener callbacks run after the internal lock is released.
 */
public class PresenceRooms {
    private class Entry(val connId: String, var member: PresenceMember)

    private class Listener(val alive: AtomicBoolean, val callback: (List<PresenceMember>) -> Unit)

    private val lock = ReentrantLock()
    private val members = HashMap<String, MutableList<Entry>>()
    private val subs = HashMap<String, MutableList<Listener>>()
    private val expiry = HashMap<String, MutableMap<String, Long>>()

    /** Members of [room] in join order. */
    public fun snapshot(room: String): List<PresenceMember> = lock.run { snapshotLocked(room) }

    private fun snapshotLocked(room: String): List<PresenceMember> = members[room].orEmpty().map { it.member }

    /** Adds or replaces [member] in [room] and fans out. */
    public fun join(room: String, member: PresenceMember) {
        val fan =
            lock.run {
                val entries = members.getOrPut(room) { ArrayList() }
                val existing = entries.firstOrNull { it.connId == member.connectionId }
                if (existing != null) existing.member = member else entries += Entry(member.connectionId, member)
                prepareFanOutLocked(room)
            }
        fan()
    }

    /**
     * Sets [connectionId]'s state in [room] (no-op for a non-member). A [ttlMs] above zero
     * schedules a state-nulling expiry at `now + ttlMs`; otherwise any pending expiry is cleared.
     */
    public fun update(room: String, connectionId: String, state: JsonElement, ttlMs: Long?, now: Long) {
        val fan =
            lock.run {
                val entry = members[room]?.firstOrNull { it.connId == connectionId } ?: return
                entry.member = entry.member.copy(state = state)
                val exp = expiry.getOrPut(room) { HashMap() }
                if (ttlMs != null && ttlMs > 0) exp[connectionId] = now + ttlMs else exp.remove(connectionId)
                prepareFanOutLocked(room)
            }
        fan()
    }

    /** Removes [connectionId] from [room] and fans out (no-op for a non-member). */
    public fun leave(room: String, connectionId: String) {
        val fan =
            lock.run {
                val entries = members[room] ?: return
                val before = entries.size
                entries.removeAll { it.connId == connectionId }
                expiry[room]?.remove(connectionId)
                if (entries.size != before) prepareFanOutLocked(room) else return
            }
        fan()
    }

    /** Nulls the state of every member whose expiry has passed (the member stays listed); true if any expired. */
    public fun expire(now: Long): Boolean =
        lock.run {
            var any = false
            for ((room, exp) in expiry) {
                val it = exp.entries.iterator()
                while (it.hasNext()) {
                    val (connId, at) = it.next()
                    if (at > now) continue
                    members[room]?.filter { e -> e.connId == connId }?.forEach { e ->
                        e.member = e.member.copy(state = JsonNull)
                        any = true
                    }
                    it.remove()
                }
            }
            any
        }

    /** Registers a room listener; the returned function detaches it. */
    public fun subscribe(room: String, callback: (List<PresenceMember>) -> Unit): () -> Unit {
        val alive = AtomicBoolean(true)
        lock.run { subs.getOrPut(room) { ArrayList() } += Listener(alive, callback) }
        return { alive.set(false) }
    }

    /** Captures a fresh snapshot and the live listeners under the lock; the returned action delivers it. */
    private fun prepareFanOutLocked(room: String): () -> Unit {
        val snapshot = snapshotLocked(room)
        val listeners = subs[room]
        val live = listeners.orEmpty().filter { it.alive.get() }
        if (listeners != null) {
            listeners.clear()
            listeners.addAll(live)
        }
        return { live.forEach { if (it.alive.get()) it.callback(snapshot) } }
    }
}
