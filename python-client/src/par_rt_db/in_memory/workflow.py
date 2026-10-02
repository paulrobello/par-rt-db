"""Workflow-engine mixin for the in-memory harness (FM-29): run
create/cancel/signal/list and the tick-time advance loop
(``_advance_run`` / the ``awaitSignal`` side-table path). Extracted
from ``store.py`` (QA-007); methods move verbatim onto
``_InMemoryStoreCore`` via mixin assembly in ``__init__.py``."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from ..errors import ErrorCode, RtDbError
from ..mutation import (
    Transaction,
)
from ..wire import (
    AwaitSignalSpec,
    StepOutcome,
    StepRetry,
    WorkflowInfo,
    WorkflowSpec,
    WorkflowStatus,
)
from .store import (
    _DEFAULT_STEP_RETRY as _DEFAULT_STEP_RETRY,
)
from .store import (
    _NEVER_DUE as _NEVER_DUE,
)
from .store import (
    _NO_SIGNAL as _NO_SIGNAL,
)
from .store import (
    MAX_SIGNAL_PAYLOAD_BYTES as MAX_SIGNAL_PAYLOAD_BYTES,
)
from .store import (
    _validate_workflow_spec as _validate_workflow_spec,
)
from .store import (
    _workflow_info as _workflow_info,
)
from .store import (
    _WorkflowRun as _WorkflowRun,
)

if TYPE_CHECKING:
    from .store import _InMemoryStoreCore as _Core
else:
    _Core = object


class _WorkflowEngine(_Core):
    """_WorkflowEngine: methods extracted verbatim from ``_InMemoryStoreCore``."""

    def start_workflow(self, spec: WorkflowSpec) -> str:
        """Insert a run from ``spec`` and return its id. The run starts
        ``pending`` at step 0; the first step's ``sleepBeforeMs`` gates its
        initial claim (``tick()`` advances it afterwards)."""
        return self._insert_workflow(spec)

    def cancel_workflow(self, id: str) -> bool:
        """Flip a pending/running/waiting run to ``cancelled``. ``False`` when
        the run is missing or already terminal (a no-op, not an error). The
        wait columns drop with the flip (leave-``waiting`` rule) — a cancelled
        run never matches a later signal delivery."""
        run = self._find_workflow(id)
        if run is None or run.status not in ("pending", "running", "waiting"):
            return False
        now = self._now()
        run.status = "cancelled"
        run.wait_name = None
        run.waited_since = None
        run.signal_payload = _NO_SIGNAL
        run.updated_at = now
        run.finished_at = now
        return True

    def signal_workflow(self, id: str, name: str, payload: Any | None = None) -> bool:
        """Deliver a named signal to a run parked at an ``awaitSignal`` step —
        the in-memory mirror of server ``workflows::deliver_signal``. The slot
        is latest-wins (every delivery while the wait is unconsumed overwrites
        the payload) and the wake lands on the NEXT tick: the run flips to
        ``pending`` due immediately, and the claim pass advances it. An
        omitted payload is delivered as JSON null — the slot is always set on
        a delivery, so the wait is consumed either way.

        Raises the server's typed errors: ``NOT_FOUND`` for an unknown run,
        ``CONFLICT`` when the run is not waiting for a signal, and ``CONFLICT``
        naming both names when it waits on a different one. A payload over
        64 KiB serialized rejects ``BAD_REQUEST``."""
        if payload is not None:
            size = len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
            if size > MAX_SIGNAL_PAYLOAD_BYTES:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"signal payload exceeds {MAX_SIGNAL_PAYLOAD_BYTES} bytes",
                )
        run = self._find_workflow(id)
        if run is None:
            raise RtDbError(ErrorCode.NOT_FOUND, "unknown workflow")
        if run.status in ("waiting", "pending") and run.wait_name == name:
            now = self._now()
            run.status = "pending"
            run.sleep_until = now
            run.signal_payload = payload
            run.updated_at = now
            return True
        if run.status == "waiting":
            raise RtDbError(
                ErrorCode.CONFLICT,
                f"workflow waiting on '{run.wait_name}', got '{name}'",
            )
        raise RtDbError(ErrorCode.CONFLICT, "workflow is not waiting for a signal")

    def list_workflows(self, status: WorkflowStatus | None = None) -> list[WorkflowInfo]:
        """Every run's info projection, newest first; ``status`` filters to a
        lifecycle state."""
        runs = [r for r in self._workflows if status is None or r.status == status]
        runs.sort(key=lambda r: r.created_at, reverse=True)
        return [_workflow_info(r) for r in runs]

    def _insert_workflow(self, spec: WorkflowSpec) -> str:
        _validate_workflow_spec(spec)
        now = self._now()
        # The server column is NOT NULL — the insert gate is always
        # ``now + unwrap_or(0)``: sleepBeforeMs absent/0 means due immediately
        # (gate == the insert instant), not "no gate".
        gate = now + (spec.steps[0].sleep_before_ms or 0)
        run = _WorkflowRun(
            id=self._new_id(),
            spec=spec,
            status="pending",
            current_step=0,
            attempts=0,
            sleep_until=gate,
            step_outcomes=[],
            last_error=None,
            created_at=now,
            updated_at=now,
            started_at=None,
            finished_at=None,
        )
        self._workflows.append(run)
        return run.id

    def _find_workflow(self, run_id: str) -> _WorkflowRun | None:
        for r in self._workflows:
            if r.id == run_id:
                return r
        return None

    def _advance_workflows(self, now: int) -> None:
        """One claim pass per tick (mirroring the server's scheduler poll
        cadence): every due pending OR waiting run flips to running
        (``startedAt`` stamped on the first claim only), then each advances
        through :meth:`_advance_run`. A claimed ``waiting`` row means the wait
        timed out — or was woken by a delivery, which flips the row to
        ``pending`` first (``signal_workflow``). A run that a sibling's step
        cancelled mid-pass is skipped by the in-loop status re-check."""
        due = [
            r
            for r in self._workflows
            if r.status in ("pending", "waiting")
            and (r.sleep_until is None or r.sleep_until <= now)
        ]
        for run in due:
            # Re-resolve from the live store: a sibling run's step txn may have
            # cancelled (or rolled back a cancel of) this one, and a failed
            # sibling txn replaces self._workflows with its snapshot — the
            # ``due`` reference would then read stale state.
            live = self._find_workflow(run.id)
            if live is None or live.status not in ("pending", "waiting"):
                continue
            live.status = "running"
            if live.started_at is None:
                live.started_at = now
            live.updated_at = now
            self._advance_run(live, now)

    def _advance_run(self, run: _WorkflowRun, now: int) -> None:
        """Advance one claimed run — the port of the committer's
        ``handle_workflow_advance`` loop. Re-checks the status at every loop
        boundary (only a running run continues — a cancel between steps stops
        advancement), and branches per step kind: an ``awaitSignal`` step takes
        the side-table path (consume a delivered signal / park / time out —
        no document writes), anything else executes the step's txn atomically.
        On txn success the run either moves to the next step (gating on its
        ``sleepBeforeMs``; a future gate releases to pending, an immediate one
        keeps looping in this same turn) or finalizes. On failure, retries with
        exponential backoff until ``maxAttempts`` is exhausted, then marks the
        run failed with the last error and a terminal failed outcome."""
        run_id = run.id
        while True:
            # Re-resolve from the live store at every boundary — the server
            # re-reads the row's status each loop iteration
            # (``workflows::status_of``), and here a failed step txn restores
            # ``self._workflows`` from its deepcopy snapshot, detaching any
            # prior reference. Mutations must land on the live object.
            live = self._find_workflow(run_id)
            if live is None or live.status != "running":
                return
            run = live
            step = run.spec.steps[run.current_step]
            retry = step.retry or _DEFAULT_STEP_RETRY
            if step.await_signal is not None:
                if self._advance_await_signal(run, step.await_signal, retry, now):
                    continue
                return
            try:
                txn = Transaction.model_validate(step.txn)
                self._execute_transaction(txn)
            except RtDbError as err:
                # The failed txn's rollback replaced the store with its
                # snapshot — re-resolve so attempts/backoff hit the live row.
                restored = self._find_workflow(run_id)
                if restored is None:
                    return
                run = restored
                run.attempts += 1
                run.updated_at = now
                if run.attempts < retry.max_attempts:
                    backoff = min(
                        retry.initial_retry_ms * (2 ** min(run.attempts - 1, 32)),
                        retry.max_retry_ms,
                    )
                    run.sleep_until = now + backoff
                    run.status = "pending"
                    return
                run.status = "failed"
                run.last_error = err.message
                run.finished_at = now
                run.step_outcomes.append(
                    StepOutcome(
                        step_index=run.current_step,
                        status="failed",
                        attempts=run.attempts,
                        at=now,
                        error=err.message,
                    )
                )
                return
            run.step_outcomes.append(
                StepOutcome(
                    step_index=run.current_step,
                    status="success",
                    attempts=run.attempts + 1,
                    at=now,
                )
            )
            run.attempts = 0
            run.last_error = None
            run.updated_at = now
            if run.current_step == run.step_count - 1:
                run.status = "success"
                run.finished_at = now
                return
            run.current_step += 1
            gate = now + (run.spec.steps[run.current_step].sleep_before_ms or 0)
            if gate > now:
                run.sleep_until = gate
                run.status = "pending"
                return

    def _advance_await_signal(
        self, run: _WorkflowRun, sig: AwaitSignalSpec, retry: StepRetry, now: int
    ) -> bool:
        """One boundary of an ``awaitSignal`` step — the port of the committer's
        three-way branch (side-table only: no document writes, no subscriber
        notifications). The claimed row itself is the wake discriminator:
        ``signal_payload`` set = a delivery to consume; else ``waited_since``
        unset = first arrival (park); set = the gate expired (a timed-out
        attempt). A timed-out retry waits the FULL ``timeoutMs`` again — never
        backoff — and exhaustion terminal-fails with
        ``awaitSignal '<name>' timed out``. Returns ``True`` when the advance
        loop should continue in this same turn (a consumed signal whose next
        step's gate is already due), ``False`` when the run parked, timed out
        into a retry, or finalized."""
        if run.signal_payload is not _NO_SIGNAL:
            payload = run.signal_payload
            run.signal_payload = _NO_SIGNAL
            run.step_outcomes.append(
                StepOutcome(
                    step_index=run.current_step,
                    status="success",
                    attempts=run.attempts + 1,
                    at=now,
                    signal=payload,
                )
            )
            run.attempts = 0
            run.last_error = None
            run.wait_name = None
            run.waited_since = None
            run.updated_at = now
            if run.current_step == run.step_count - 1:
                run.status = "success"
                run.finished_at = now
                return False
            run.current_step += 1
            gate = now + (run.spec.steps[run.current_step].sleep_before_ms or 0)
            if gate > now:
                run.sleep_until = gate
                run.status = "pending"
                return False
            return True
        # An omitted timeoutMs is never due — only a delivery or a cancel
        # wakes the run.
        timeout_gate = now + sig.timeout_ms if sig.timeout_ms is not None else _NEVER_DUE
        if run.waited_since is None:
            # First arrival: park (attempts persist, so a timeout retry that
            # re-parks keeps its count).
            run.status = "waiting"
            run.wait_name = sig.name
            run.waited_since = now
            run.sleep_until = timeout_gate
            run.signal_payload = _NO_SIGNAL
            run.updated_at = now
            return False
        # The run parked and its gate expired: a timed-out attempt. A retry
        # waits the FULL timeoutMs again — never backoff.
        run.attempts += 1
        run.updated_at = now
        if run.attempts < retry.max_attempts:
            run.status = "waiting"
            run.wait_name = sig.name
            run.waited_since = now
            run.sleep_until = timeout_gate
            run.signal_payload = _NO_SIGNAL
            return False
        error = f"awaitSignal '{sig.name}' timed out"
        run.status = "failed"
        run.last_error = error
        run.finished_at = now
        run.wait_name = None
        run.waited_since = None
        run.signal_payload = _NO_SIGNAL
        run.step_outcomes.append(
            StepOutcome(
                step_index=run.current_step,
                status="failed",
                attempts=run.attempts,
                at=now,
                error=error,
            )
        )
        return False
