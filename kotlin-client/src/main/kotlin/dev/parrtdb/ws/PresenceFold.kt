package dev.parrtdb.ws

import dev.parrtdb.wire.PresenceMember
import dev.parrtdb.wire.ServerMessage

/**
 * Folds one room's `presenceSnapshot` / `presenceDelta` frames into the full member list.
 * A snapshot replaces the list; a delta applies incrementally and is dropped when stale. A delta
 * whose seq skips ahead means a missed frame, which requires a resync (re-send the join).
 */
internal class PresenceFold {
    private var members: List<PresenceMember> = emptyList()
    private var lastSeq: Long = 0
    private var hasSeq: Boolean = false

    sealed interface Outcome {
        data class Emit(val members: List<PresenceMember>) : Outcome

        data object Drop : Outcome

        data object Resync : Outcome
    }

    fun snapshot(m: ServerMessage.PresenceSnapshot): Outcome {
        members = m.members.toList()
        lastSeq = 0
        hasSeq = false
        return Outcome.Emit(members)
    }

    fun delta(m: ServerMessage.PresenceDelta): Outcome {
        if (hasSeq) {
            if (m.seq <= lastSeq) return Outcome.Drop
            if (m.seq > lastSeq + 1) return Outcome.Resync
        }
        val next = members.toMutableList()
        for (j in m.joined) {
            val i = next.indexOfFirst { it.connectionId == j.connectionId }
            if (i >= 0) next[i] = j else next += j
        }
        if (m.left.isNotEmpty()) next.removeAll { it.connectionId in m.left }
        for (s in m.stateChanged) {
            val i = next.indexOfFirst { it.connectionId == s.connectionId }
            if (i >= 0) next[i] = s
        }
        members = next
        lastSeq = m.seq
        hasSeq = true
        return Outcome.Emit(next.toList())
    }
}
