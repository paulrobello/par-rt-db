package dev.parrtdb.inmemory

import dev.parrtdb.errors.RtDbException

/**
 * Fires due non-paused jobs (external jobs are never executed internally) through the same
 * atomic path as [applyTxn], then reaps expired TTL docs. Returns the number of docs reaped.
 * Subscriber callbacks run after the lock is released.
 */
fun Store.tick(now: Long): Int {
    val reaped: Int
    val fires: List<NotifyFire>
    lock.lock()
    try {
        var i = 0
        while (i < scheduledJobs.size) {
            val job = scheduledJobs[i]
            if (job.external || job.status == ScheduleStatusMem.Paused || job.dueAt > now) {
                i++
                continue
            }
            val jobId = job.id
            val kind = job.kind
            val everyMs = job.everyMs
            try {
                executeTransaction(this, job.txn)
            } catch (e: RtDbException) {
                findJob(jobId)?.let { j ->
                    j.status = ScheduleStatusMem.Error
                    j.lastError = e.message
                    if (kind == ScheduleKindMem.Cron) j.dueAt = now + CRON_STEP_MS
                    if (kind == ScheduleKindMem.Interval && everyMs != null) j.dueAt = now + everyMs
                }
                i++
                continue
            }
            val j = findJob(jobId)
            if (j != null) {
                val prevDueAt = j.dueAt
                j.firedCount++
                when (kind) {
                    ScheduleKindMem.OneShot -> {
                        scheduledJobs.removeIf { it.id == jobId }
                        continue
                    }
                    ScheduleKindMem.Cron -> {
                        j.dueAt = now + CRON_STEP_MS
                        j.status = ScheduleStatusMem.Pending
                    }
                    ScheduleKindMem.Interval ->
                        if (everyMs != null) {
                            // Windows missed during the fire's latency are skipped, not backfilled.
                            val delta = now - prevDueAt
                            if (delta > 0) {
                                val missed = (delta - 1) / everyMs
                                if (missed > 0) {
                                    j.missedCount += missed
                                    j.lastMissedAt = now
                                }
                            }
                            j.dueAt = now + everyMs
                            j.status = ScheduleStatusMem.Pending
                        } else {
                            j.status = ScheduleStatusMem.Error
                            j.lastError = "interval job missing everyMs"
                        }
                }
            }
            i++
        }
        reaped = reapTtl(this, now)
        fires = takePendingFires()
    } finally {
        lock.unlock()
    }
    for (f in fires) f.callback(f.value)
    return reaped
}

private fun Store.findJob(id: String): ScheduledJob? = scheduledJobs.firstOrNull { it.id == id }

/**
 * Removes docs whose TTL field is a number strictly less than [now], for every table declaring
 * a ttl. A table with onDelete children reaps through the cascade with forceHard; a per-row
 * failure skips that row (at-least-once). The caller holds the lock.
 */
fun reapTtl(s: Store, now: Long): Int {
    val schema = s.schema ?: return 0
    val toRemove = ArrayList<RowKey>()
    val cascadeTables = HashSet<String>()
    for ((tableName, tableDef) in s.tables) {
        val ttl = tableDef.ttl ?: continue
        if (hasOnDeleteChildren(schema, tableName)) cascadeTables += tableName
        for ((key, row) in s.docs) {
            if (key.table != tableName) continue
            val v = row.doc[ttl.field]
            if (v != null && isJsonNumber(v) && jsonNumberF64(v) < now.toDouble()) toRemove += key
        }
    }
    val byTable = LinkedHashMap<String, MutableList<String>>()
    for (key in toRemove) byTable.getOrPut(key.table) { ArrayList() } += key.id
    var removed = 0
    val touched = LinkedHashSet<String>()
    for ((tableName, ids) in byTable) {
        if (tableName in cascadeTables) {
            val visited = HashSet<RowKey>()
            for (id in ids) {
                val stepTouched = ArrayList<String>()
                try {
                    deleteRowCascade(s, tableName, id, visited, CascadeBudget(), true, stepTouched)
                    removed++
                    touched.addAll(stepTouched)
                } catch (e: RtDbException) {
                    // per-row failure skips the row
                }
            }
            continue
        }
        for (id in ids) {
            if (s.docs.remove(RowKey(tableName, id)) != null) {
                removed++
                touched += tableName
            }
        }
    }
    if (touched.isNotEmpty()) notifySubs(s, touched)
    return removed
}
