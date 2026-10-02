"""Schedule-engine mixin for the in-memory harness: the scheduled-job
store (create/cancel/pause/resume/list), the ``tick`` driver, and the
TTL reaper. Extracted from ``store.py`` (QA-007); methods move
verbatim onto ``_InMemoryStoreCore`` via mixin assembly in
``__init__.py``."""

from __future__ import annotations

from typing import TYPE_CHECKING

from ..errors import ErrorCode, RtDbError
from ..mutation import (
    Transaction,
)
from ..wire import (
    AfterMs,
    Cron,
    Interval,
    RunAt,
    ScheduleInfo,
    ScheduleWhen,
)
from .migrate import (
    _on_delete_ref as _on_delete_ref,
)
from .store import (
    CRON_STEP_MS as CRON_STEP_MS,
)
from .store import (
    MAX_EVERY_MS as MAX_EVERY_MS,
)
from .store import (
    _schedule_info as _schedule_info,
)
from .store import (
    _ScheduledJob as _ScheduledJob,
)

if TYPE_CHECKING:
    from .store import _InMemoryStoreCore as _Core
else:
    _Core = object


class _ScheduleEngine(_Core):
    """_ScheduleEngine: methods extracted verbatim from ``_InMemoryStoreCore``."""

    if TYPE_CHECKING:
        # Provided by _WritesEngine at assembly (writes.py).
        def _delete_row_cascade(
            self,
            table_name: str,
            sid: str,
            visited: set[tuple[str, str]],
            cascade_rows: list[int],
            force_hard: bool,
            touched: set[str],
        ) -> None: ...

    def _prepare_job(self, when: ScheduleWhen) -> tuple[str, int, str | None, int | None]:
        """(kind, due_at, cron, every_ms) for a ``ScheduleWhen`` — shared by the
        standalone ``schedule`` op and the ``schedule`` txn step. The clock
        comes from the injectable ``now``, never ``time.time()`` directly.
        ``everyMs`` is validated here (positive, at most :data:`MAX_EVERY_MS`)
        before any row is created, mirroring the server's ``resolve_when``."""
        now = self._now()
        match when:
            case Interval(every_ms=every_ms):
                if every_ms <= 0:
                    raise RtDbError(ErrorCode.BAD_REQUEST, "everyMs must be positive")
                if every_ms > MAX_EVERY_MS:
                    raise RtDbError(
                        ErrorCode.BAD_REQUEST, f"everyMs must be at most {MAX_EVERY_MS}"
                    )
                return "interval", self._due_at_for(when, now), None, every_ms
            case Cron(expr=expr_str):
                return "cron", self._due_at_for(when, now), expr_str, None
            case _:
                return "oneshot", self._due_at_for(when, now), None, None

    def schedule(self, txn: Transaction, when: ScheduleWhen, *, external: bool = False) -> str:
        """Store ``txn`` scheduled for ``when`` and return its id. Cron
        validation is deferred to the live server; the harness accepts any
        expression. ``everyMs`` (interval) IS validated — positive and at most
        :data:`MAX_EVERY_MS` — mirroring the server's ``resolve_when``. An
        external job is never fired by :meth:`tick` — it sits ``pending`` until
        an application worker claims it (mirrors the server's external-claim
        surface)."""
        new_id = self._new_id()
        kind, due_at, cron, every_ms = self._prepare_job(when)
        self._schedules.append(
            _ScheduledJob(
                id=new_id,
                kind=kind,
                txn=txn,
                due_at=due_at,
                cron=cron,
                every_ms=every_ms,
                status="pending",
                created_at=self._now(),
                fired_count=0,
                last_error=None,
                external=external,
            )
        )
        return new_id

    def cancel_schedule(self, id: str) -> bool:
        """Remove the scheduled job. ``False`` when no such id exists (a no-op,
        not an error) — the server's ``scheduler::cancel`` contract."""
        before = len(self._schedules)
        self._schedules = [j for j in self._schedules if j.id != id]
        return len(self._schedules) != before

    def pause_schedule(self, id: str) -> bool:
        """Flip a pending job to ``paused``. ``False`` when the job is missing
        or not pending (a no-op, not an error)."""
        job = self._find_job(id)
        if job is None or job.status != "pending":
            return False
        job.status = "paused"
        return True

    def resume_schedule(self, id: str) -> bool:
        """Flip a paused job back to ``pending``. ``False`` when the job is
        missing or not paused (a no-op, not an error). An interval job's
        ``due_at`` shifts to ``now + everyMs`` (windows elapsed while paused
        are skipped, never backfilled — mirrors the server's ``set_paused``
        resume arm); one-shots and crons keep their ``due_at`` (the harness
        cannot recompute a cron's next fire)."""
        job = self._find_job(id)
        if job is None or job.status != "paused":
            return False
        job.status = "pending"
        if job.kind == "interval" and job.every_ms is not None:
            job.due_at = self._now() + job.every_ms
        return True

    def list_schedules(self) -> list[ScheduleInfo]:
        """Snapshot of every scheduled job's public view."""
        return [_schedule_info(job) for job in self._schedules]

    def _reap_ttl(self, now: int) -> int:
        """Remove docs whose declared TTL ``field`` (a number) is ``< now`` — the
        in-memory mirror of the server's per-tick TTL reaper. Fires only on
        tables that declare ``ttl``; non-numeric or absent values are left alone.
        Notifies subscribers on each touched table so reactive subscriptions see
        the expiry as a delete. Returns the count of removed docs.

        FM-33: the reaper ALWAYS hard-deletes (``force_hard`` — even on a
        ``softDelete`` table; the reaper is the purge mechanism), and when some
        table declares an ``onDelete`` ref targeting the reaped table the expiry
        runs through :meth:`_delete_row_cascade` so children follow their
        declared action. Mirror of server ``handle_reaper``'s bulk-vs-cascade
        branch: ``visited`` is shared across the sweep (a row cascaded by an
        earlier expiry is skipped) while the budget is fresh per initiating
        row; a failing row is skipped and retried on the next sweep, not fatal."""
        touched: set[str] = set()
        removed = 0
        # Shared across the whole sweep: a row already hard-deleted (or
        # stamped) by an earlier expiry's cascade is skipped, not an error.
        # Locally scoped so a failed row retries on the NEXT sweep.
        sweep_visited: set[tuple[str, str]] = set()
        # Snapshot the items — popping mid-iteration would skip rows.
        for (table, doc_id), row in list(self._docs.items()):
            tdef = self._tables.get(table)
            if tdef is None or tdef.ttl is None:
                continue
            value = row.doc.get(tdef.ttl.field)
            if isinstance(value, (int, float)) and value < now:
                if any(
                    _on_delete_ref(ft, table) is not None
                    for other in self._tables.values()
                    for ft in other.fields.values()
                ):
                    try:
                        self._delete_row_cascade(table, doc_id, sweep_visited, [0], True, touched)
                    except RtDbError:
                        # Per-row failures are skipped and retried next sweep
                        # (at-least-once, like the server's warn-and-continue);
                        # cascade work before the failure stays, as server-side.
                        continue
                else:
                    self._docs.pop((table, doc_id), None)
                    touched.add(table)
                removed += 1
        if touched:
            self._notify_subs(touched)
        return removed

    def tick(self, now_ms: int | None = None) -> None:
        """Advance the harness clock to ``now_ms`` (or the client clock when
        omitted), then (1) reap docs whose TTL field is in the past and (2) fire
        every due non-paused scheduled job by applying its txn through the same
        atomic path as :meth:`mutate` (so reactive subscriptions see the write).
        One-shots are removed after a successful fire; crons re-arm by
        :data:`CRON_STEP_MS` and interval jobs by their ``everyMs`` (missed
        windows are skipped, never backfilled). A job whose txn fails is marked
        ``error`` but left in place (recurring kinds re-arm), so a subsequent
        ``tick`` retries it. External jobs are NEVER fired — the worker that
        claims them over HTTP owns their execution.

        Workflows (FM-29): after schedules, one claim pass advances every due
        pending run (see :meth:`_advance_workflows`)."""
        now = now_ms if now_ms is not None else self._now()
        self._reap_ttl(now)
        self._advance_workflows(now)
        i = 0
        while i < len(self._schedules):
            job = self._schedules[i]
            # External jobs are never internally executed (server claim_due/
            # next_due exclude external rows) — tick skips them so the job
            # stays pending for its claiming worker.
            if job.status == "paused" or job.external or job.due_at > now:
                i += 1
                continue
            txn = job.txn
            job_id = job.id
            kind = job.kind
            try:
                self._execute_transaction(txn)
            except RtDbError as err:
                j = self._find_job(job_id)
                if j is not None:
                    j.status = "error"
                    j.last_error = err.message
                    if kind == "cron":
                        j.due_at = now + CRON_STEP_MS
                    elif kind == "interval" and j.every_ms is not None:
                        # Error path re-arms too (server reschedule_recurring_error).
                        j.due_at = now + j.every_ms
            else:
                j = self._find_job(job_id)
                if j is not None:
                    prev_due_at = j.due_at
                    j.fired_count += 1
                    if kind == "oneshot":
                        # Remove after a successful fire; don't bump i (the next
                        # job shifts into this index).
                        self._schedules = [s for s in self._schedules if s.id != job_id]
                        continue
                    if kind == "interval" and j.every_ms is not None:
                        # The elapsed-window count is exact for interval jobs
                        # (unlike cron, which this harness approximates on a
                        # fixed CRON_STEP_MS rather than real cron math).
                        delta = now - prev_due_at
                        if delta > 0:
                            missed = (delta - 1) // j.every_ms
                            if missed > 0:
                                j.missed_count += missed
                                j.last_missed_at = now
                        j.due_at = now + j.every_ms
                    else:
                        j.due_at = now + CRON_STEP_MS
                    j.status = "pending"
            i += 1

    def _find_job(self, job_id: str) -> _ScheduledJob | None:
        for j in self._schedules:
            if j.id == job_id:
                return j
        return None

    def _due_at_for(self, when: ScheduleWhen, now: int) -> int:
        match when:
            case AfterMs(ms=ms):
                return now + ms
            case RunAt(ms=ms):
                return ms
            case Interval(every_ms=every_ms):
                return now + every_ms
            case _:
                return now + CRON_STEP_MS  # cron
