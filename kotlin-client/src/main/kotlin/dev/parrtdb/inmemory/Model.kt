package dev.parrtdb.inmemory

import dev.parrtdb.wire.FilterExpr
import dev.parrtdb.wire.Query
import dev.parrtdb.wire.StepResult
import dev.parrtdb.wire.Transaction
import dev.parrtdb.wire.ValueExpr
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import java.util.concurrent.locks.ReentrantLock

enum class OnDeleteAction(val wire: String) {
    Cascade("cascade"),
    Restrict("restrict"),
    SetNull("setNull"),
}

/**
 * Parsed field type. [kind] is the wire `type` tag: string number boolean null id literal
 * optional union array object int64 bytes any record vector.
 * Use [equalTo] (not data-class `==`) for schema-diff equality: literal values compare by
 * numeric value, not spelling.
 */
data class FieldType(
    val kind: String,
    val table: String = "", // id
    val onDelete: OnDeleteAction? = null, // id
    val value: JsonElement? = null, // literal
    val inner: FieldType? = null, // optional, array element, record value
    val variants: List<FieldType> = emptyList(), // union
    val fields: Map<String, FieldType> = emptyMap(), // object
    val dimensions: Int = 0, // vector
)

data class VectorIndexSpec(val dimensions: Int, val filterFields: List<String> = emptyList(), val metric: String = "cosine")

data class IndexDef(
    val name: String,
    val fields: List<String>,
    val search: Boolean = false,
    val trgm: Boolean = false,
    val vector: VectorIndexSpec? = null,
    val unique: Boolean = false,
    val whereClause: FilterExpr? = null,
    val language: String? = null,
)

data class TtlDef(val field: String, val defaultDurationMs: Long? = null)

/** Bounds are kept as JSON number literals (min/max) so int64 values past 2^53 stay exact. */
data class FieldConstraints(
    val min: JsonElement? = null,
    val max: JsonElement? = null,
    val minLength: Long? = null,
    val maxLength: Long? = null,
    val pattern: String? = null,
)

data class TableDef(
    val fields: Map<String, FieldType>,
    val indexes: List<IndexDef> = emptyList(),
    val ownerField: String? = null,
    val collaboratorsField: String? = null,
    val ttl: TtlDef? = null,
    val updatedAtField: String? = null,
    val autoIncrementField: String? = null,
    val authorize: FilterExpr? = null,
    val defaults: Map<String, JsonElement> = emptyMap(),
    val constraints: Map<String, FieldConstraints> = emptyMap(),
    val computed: Map<String, ValueExpr> = emptyMap(),
    val softDelete: Boolean = false,
    val description: String? = null,
    val fieldDescriptions: Map<String, String>? = null,
)

data class SchemaDef(val tables: Map<String, TableDef>)

data class RowKey(val table: String, val id: String)

/** [doc] never contains the system fields; [mergeDoc] adds `_id`/`_creationTime`/`_version`. */
class StoredRow(
    val id: String,
    var doc: JsonObject,
    var version: Long,
    val createdAt: Long,
    var deletedAt: Long? = null,
)

enum class ScheduleKindMem { OneShot, Cron, Interval }

enum class ScheduleStatusMem { Pending, Paused, Running, Done, Cancelled, Error }

class ScheduledJob(
    val id: String,
    val kind: ScheduleKindMem,
    val txn: Transaction,
    var dueAt: Long,
    val cron: String? = null,
    val tz: String? = null,
    val everyMs: Long? = null,
    var status: ScheduleStatusMem = ScheduleStatusMem.Pending,
    val createdAt: Long,
    var firedCount: Long = 0,
    var lastError: String? = null,
    val external: Boolean = false,
    var missedCount: Long = 0,
    var lastMissedAt: Long? = null,
)

class StoredBlob(val bytes: ByteArray, val contentType: String?, val createdAt: Long, val sha256: String)

class StoreSubscription(
    val query: Query,
    val table: String,
    @Volatile var alive: Boolean = true,
    val callback: (JsonElement) -> Unit,
    var last: String = "",
    var hasLast: Boolean = false,
)

class NotifyFire(val callback: (JsonElement) -> Unit, val value: JsonElement)

/**
 * The in-memory engine state. All access happens under [lock] (re-entrant); functions named
 * `...Locked` or taking a bare [Store] assume the caller already holds it. Subscriber callbacks
 * are queued in [pendingFires] while the lock is held and fired by the lock-releasing entry
 * point (`applyTxn`, `tick`, `applyMigration`) after unlocking.
 */
class Store {
    val lock = ReentrantLock()
    internal var now: () -> Long = { System.currentTimeMillis() }
    internal var random: () -> Double = { java.util.concurrent.ThreadLocalRandom.current().nextDouble() }
    internal var schema: SchemaDef? = null
    internal val tables = LinkedHashMap<String, TableDef>()
    internal var docs = LinkedHashMap<RowKey, StoredRow>()
    internal val autoIncrementCounters = HashMap<String, Long>()
    internal var idCounter = 0L
    internal val idempotency = HashMap<String, List<StepResult>>()
    internal var readOnly = false
    internal var scheduledJobs = ArrayList<ScheduledJob>()
    internal val subscribers = ArrayList<StoreSubscription>()
    internal val storage = HashMap<String, StoredBlob>()
    internal var pendingFires = ArrayList<NotifyFire>()

    fun withClock(f: () -> Long): Store = also { lock.lock(); try { now = f } finally { lock.unlock() } }

    fun withRandom(f: () -> Double): Store = also { lock.lock(); try { random = f } finally { lock.unlock() } }

    fun nowMs(): Long = locked { now() }

    fun setReadOnly(value: Boolean) {
        lock.lock()
        try { readOnly = value } finally { lock.unlock() }
    }

    internal fun takePendingFires(): List<NotifyFire> {
        val fires = pendingFires
        pendingFires = ArrayList()
        return fires
    }

    /**
     * 12 hex timestamp digits, version nibble `7`, 13 random hex digits and a 6-digit counter
     * (the ts-client shape). The counter keeps ids unique when the clock is pinned or two ids
     * are minted in the same millisecond, including under a constant `random`.
     */
    internal fun newIdLocked(): String {
        val ts = now() and 0xFFFF_FFFF_FFFFL
        val counter = (idCounter++ % 0x1000000L)
        return String.format("%012x7%s%06x", ts, randomHexLocked(13), counter)
    }

    internal fun randomHexLocked(count: Int): String {
        val sb = StringBuilder(count)
        repeat(count) {
            val digit = (random() * 16.0).toInt() and 0xF
            sb.append("0123456789abcdef"[digit])
        }
        return sb.toString()
    }
}

/** Runs [block] holding the store lock. */
internal inline fun <T> Store.locked(block: () -> T): T {
    lock.lock()
    try {
        return block()
    } finally {
        lock.unlock()
    }
}
