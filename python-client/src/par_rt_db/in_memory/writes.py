"""Write-path engine mixin for the in-memory harness: the txn step
executor and the per-step row operations (insert/patch/replace/
delete + cascade, adjust-counter, upsert lookups, unique-index
checks). Extracted from ``store.py`` (QA-007); methods move
verbatim onto ``_InMemoryStoreCore`` via mixin assembly in
``__init__.py``."""

from __future__ import annotations

import math
from copy import replace
from typing import TYPE_CHECKING, Any

from ..errors import ErrorCode, RtDbError
from ..mutation import (
    Step,
    StepResult,
    _AdjustCounter,
    _CancelSchedule,
    _CancelWorkflow,
    _Delete,
    _DeleteByQuery,
    _ExpectAbsent,
    _ExpectVersion,
    _Insert,
    _is_safe_counter_integer,
    _Patch,
    _PatchByQuery,
    _Replace,
    _Schedule,
    _StartWorkflow,
    _Undelete,
    _Upsert,
)
from ..schema import TableDef, _FNumber, _FOptional
from . import store as _store
from .migrate import (
    _on_delete_ref as _on_delete_ref,
)
from .store import (
    MAX_BY_QUERY_ROWS as MAX_BY_QUERY_ROWS,
)
from .store import (
    StoredRow as StoredRow,
)
from .store import (
    _apply_defaults as _apply_defaults,
)
from .store import (
    _cancel_schedule_result as _cancel_schedule_result,
)
from .store import (
    _cancel_workflow_result as _cancel_workflow_result,
)
from .store import (
    _coerce_index_value as _coerce_index_value,
)
from .store import (
    _collect_index_key as _collect_index_key,
)
from .store import (
    _delete_by_query_result as _delete_by_query_result,
)
from .store import (
    _insert_result as _insert_result,
)
from .store import (
    _is_live as _is_live,
)
from .store import (
    _json_structurally_equal as _json_structurally_equal,
)
from .store import (
    _patch_by_query_result as _patch_by_query_result,
)
from .store import (
    _require_index as _require_index,
)
from .store import (
    _schedule_result as _schedule_result,
)
from .store import (
    _stamp_updated_at as _stamp_updated_at,
)
from .store import (
    _start_workflow_result as _start_workflow_result,
)
from .store import (
    _strip_unset_optionals as _strip_unset_optionals,
)
from .store import (
    _upsert_result as _upsert_result,
)
from .store import (
    apply_patch as apply_patch,
)
from .store import (
    validate_doc as validate_doc,
)
from .validate import (
    _eval_filter_expr as _eval_filter_expr,
)
from .validate import (
    _validate_filter as _validate_filter,
)
from .value_expr import (
    _stamp_computed as _stamp_computed,
)

if TYPE_CHECKING:
    from .store import _InMemoryStoreCore as _Core
else:
    _Core = object


class _WritesEngine(_Core):
    """_WritesEngine: methods extracted verbatim from ``_InMemoryStoreCore``."""

    def _stamp_auto_increment(
        self, table_name: str, table_def: TableDef, doc: dict[str, Any]
    ) -> dict[str, Any]:
        """FM-37: stamp the table's ``autoIncrementField`` with the next value
        of its per-table counter, overwriting any client-supplied value — the
        same authority model as ``_stamp_updated_at``. Runs on the two insert
        paths only (insert / upsert's insert branch, both via ``_do_insert``);
        after insert the field is immutable (``apply_patch`` / ``_do_replace``
        reject changes). The value is a decimal string, matching the int64 wire
        convention. Returns a NEW dict; the counter bump is not rolled back
        (gaps on rollback, like the server's sequence)."""
        field = table_def.auto_increment_field
        if field is None:
            return doc
        next_value = self._auto_counters.get(table_name, 0) + 1
        self._auto_counters[table_name] = next_value
        return {**doc, field: str(next_value)}

    def _execute_step(self, step: Step) -> tuple[StepResult, set[str]]:
        """Run one step, returning its result and the set of tables it wrote
        (empty for read-only/control-flow steps). FM-33: a delete step's set
        includes every child table its ``onDelete`` cascade touched, so
        subscribers on those tables re-run."""
        match step:
            case _Insert(table=table, doc=doc):
                table_def = self._require_table(table)
                new_id = self._do_insert(table, table_def, doc)
                return _insert_result(new_id), {table}
            case _Patch(table=table, id=sid, fields=fields):
                table_def = self._require_table(table)
                self._do_patch(table_def, table, sid, fields)
                return None, {table}
            case _AdjustCounter(
                table=table,
                id=sid,
                field=field,
                delta=delta,
                min=minimum,
                max=maximum,
                expected=expected,
            ):
                table_def = self._require_table(table)
                self._do_adjust_counter(
                    table_def, table, sid, field, delta, minimum, maximum, expected
                )
                return None, {table}
            case _Replace(table=table, id=sid, doc=doc):
                table_def = self._require_table(table)
                self._do_replace(table_def, table, sid, doc)
                return None, {table}
            case _Delete(table=table, id=sid):
                table_def = self._require_table(table)
                touched: set[str] = set()
                self._do_delete(table_def, table, sid, touched)
                return None, touched
            case _Undelete(table=table, id=sid):
                # FM-33: restore a soft-deleted row. BAD_REQUEST on a table
                # without `softDelete`; NOT_FOUND when absent; idempotent None
                # result when already live.
                table_def = self._require_table(table)
                if not table_def.soft_delete:
                    raise RtDbError(
                        ErrorCode.BAD_REQUEST,
                        f"table '{table}' does not declare softDelete",
                    )
                row = self._docs.get((table, sid))
                if row is None:
                    raise RtDbError(ErrorCode.NOT_FOUND, f"document '{sid}' not found")
                if _is_live(row):
                    # Idempotent: restoring a live row changes nothing.
                    return None, set()
                # Restoring re-enters the live-row unique predicate — the
                # server's physical partial unique index makes the UPDATE
                # collide; the harness enforces the same CONFLICT up front.
                self._check_unique_indexes(table_def, table, row.doc, sid)
                self._docs[(table, sid)] = replace(row, deleted_at=None, version=row.version + 1)
                return None, {table}
            case _ExpectVersion(table=table, id=sid, version=version):
                table_def = self._require_table(table)
                self._do_expect_version(table_def, table, sid, version)
                return None, set()
            case _ExpectAbsent(table=table, index=index, eq=eq_vals):
                table_def = self._require_table(table)
                rows = self._eq_lookup(table_def, table, index, eq_vals)
                if rows:
                    raise RtDbError(
                        ErrorCode.PRECONDITION_FAILED,
                        f"index '{index}' already has a matching document",
                    )
                return None, set()
            case _Upsert(
                table=table,
                index=index,
                eq=eq_vals,
                insert=insert_doc,
                patch=patch_fields,
            ):
                table_def = self._require_table(table)
                rows = self._eq_lookup(table_def, table, index, eq_vals)
                if len(rows) > 1:
                    raise RtDbError(
                        ErrorCode.PRECONDITION_FAILED, "upsert matched multiple documents"
                    )
                if rows:
                    row = rows[0]
                    # FM-36: the update branch stamps the patch fields before
                    # the merge (server `step_upsert`), so an upsert that never
                    # mentions the field still restamps, and a client-supplied
                    # value is overwritten.
                    merged = apply_patch(
                        table_def,
                        row.doc,
                        _stamp_updated_at(table_def, patch_fields, self._now()),
                        now=self._now(),
                    )
                    self._do_update(table_def, table, row.id, merged)
                    return _upsert_result(row.id, False), {table}
                new_id = self._do_insert(table, table_def, insert_doc)
                return _upsert_result(new_id, True), {table}
            case _PatchByQuery(table=table, filter=flt, patch=patch_fields, limit=limit_opt):
                table_def = self._require_table(table)
                # `allow_relative_time=True` makes the by-query scan the one
                # surface that accepts `olderThan` (server compile_scan_where).
                _validate_filter(flt, table_def, allow_relative_time=True)
                # The cutoff clock is read once per step execution (compile ==
                # execution inside the committer turn on the server).
                step_now = self._now()
                # FM-33: soft-deleted rows are absent to the scan (the server
                # selects through `compile_scan_where`'s `deleted_at IS NULL`).
                matched = [
                    row
                    for (t, _id), row in self._docs.items()
                    if t == table
                    and _is_live(row)
                    and _eval_filter_expr(flt, row.doc, table_def.fields, step_now)
                ]
                matched.sort(key=lambda r: (r.created_at, r.id))
                limit = (
                    MAX_BY_QUERY_ROWS if limit_opt is None else min(limit_opt, MAX_BY_QUERY_ROWS)
                )
                truncated = len(matched) > limit
                take = matched[:limit]
                for row in take:
                    # FM-36: stamp per row with a fresh `now` (server
                    # `step_patch_by_query`), exactly like a per-id patch.
                    merged = apply_patch(
                        table_def,
                        row.doc,
                        _stamp_updated_at(table_def, patch_fields, self._now()),
                        now=self._now(),
                    )
                    self._do_update(table_def, table, row.id, merged)
                return _patch_by_query_result(len(take), truncated), {table}
            case _DeleteByQuery(table=table, filter=flt, limit=limit_opt):
                table_def = self._require_table(table)
                _validate_filter(flt, table_def, allow_relative_time=True)
                step_now = self._now()
                matched = [
                    row
                    for (t, _id), row in self._docs.items()
                    if t == table
                    and _is_live(row)
                    and _eval_filter_expr(flt, row.doc, table_def.fields, step_now)
                ]
                matched.sort(key=lambda r: (r.created_at, r.id))
                limit = (
                    MAX_BY_QUERY_ROWS if limit_opt is None else min(limit_opt, MAX_BY_QUERY_ROWS)
                )
                truncated = len(matched) > limit
                take = matched[:limit]
                # FM-33: every selected row deletes through the same
                # onDelete-aware path as a per-id delete (stamp on a
                # softDelete table, else cascade). `visited` and the row
                # budget are shared across the whole step: a row already
                # handled by an earlier row's cascade is skipped, and one
                # budget bounds every cascade the step starts.
                visited: set[tuple[str, str]] = set()
                cascade_rows = [0]
                touched = {table}
                for row in take:
                    self._delete_row_cascade(table, row.id, visited, cascade_rows, False, touched)
                return _delete_by_query_result(len(take), truncated), touched
            case _Schedule(when=when, txn=nested_txn, external=step_external):
                # FM-28: enqueue, don't execute — tick() fires the nested txn
                # later through _execute_transaction (which re-validates it).
                # Routes through schedule() so the when is validated (everyMs
                # bounds) identically on the step and standalone paths. An
                # external step enqueues an external job — tick() skips it the
                # same way the server's claim_due/next_due exclude external rows.
                return (
                    _schedule_result(self.schedule(nested_txn, when, external=bool(step_external))),
                    set(),
                )
            case _CancelSchedule(id=job_id):
                # Unlike the standalone cancel op (NOT_FOUND on a miss), the
                # step reports {"cancelled": bool} — a miss is not an error.
                before = len(self._schedules)
                self._schedules = [j for j in self._schedules if j.id != job_id]
                return _cancel_schedule_result(len(self._schedules) < before), set()
            case _StartWorkflow(spec=wf_spec):
                # FM-29: insert the run on the open txn — the rollback snapshot
                # above restores it if a later step fails, so a rolled-back txn
                # leaves no orphan run.
                return _start_workflow_result(self._insert_workflow(wf_spec)), set()
            case _CancelWorkflow(id=wf_id):
                # Same shape as cancelSchedule: {"cancelled": bool}, a miss or
                # terminal run is a no-op False, not an error.
                return _cancel_workflow_result(self.cancel_workflow(wf_id)), set()
            case _:
                raise RtDbError(ErrorCode.INTERNAL, "unknown step op")

    def _do_insert(self, table_name: str, table_def: TableDef, doc: dict[str, Any]) -> str:
        # TTL default: stamp the declared field at insert only when the caller
        # omitted it and a default duration is declared (mirrors server
        # `committer::execute_txn`). After insert the field is ordinary —
        # patch/replace/delete treat it like any other field. Runs before
        # validation so a required TTL field is populated.
        ttl = table_def.ttl
        if ttl is not None and ttl.default_duration_ms is not None and ttl.field not in doc:
            doc[ttl.field] = self._now() + ttl.default_duration_ms
        # FM-36: the updatedAt stamp sits between the ttl default and the
        # FM-32 defaults (server `step_insert` order): it overwrites any
        # client-supplied value, a `defaults` entry on the same field loses
        # (the key is already present when defaults run), and it runs before
        # validation so a required updatedAt field is populated.
        doc = _stamp_updated_at(table_def, doc, self._now())
        # FM-32: after the ttl stamp (a ttl default on the same field wins),
        # before validation — so a default can populate a required field.
        _apply_defaults(table_def, doc)
        # FM-37: the auto-increment stamp runs LAST among the insert stamps
        # (server `step_insert` order) — after defaults, so a `defaults` entry
        # on the same field loses — and before validation, so a required
        # counter field is always populated.
        doc = self._stamp_auto_increment(table_name, table_def, doc)
        # ENH-028: computed stamping is the LAST insert stamp — after the ttl /
        # updatedAt / defaults / autoIncrement chain, so expressions see final
        # inputs — and before validation, so a client-supplied computed value
        # never reaches validation (server `do_insert` order).
        doc = _stamp_computed(table_def, doc, self._now())
        validate_doc(table_def, doc)
        stored = _strip_unset_optionals(table_def, doc)
        self._check_unique_indexes(table_def, table_name, stored, None)
        new_id = self._new_id()
        self._docs[(table_name, new_id)] = StoredRow(
            id=new_id, doc=stored, version=1, created_at=self._now()
        )
        return new_id

    def _do_patch(
        self,
        table_def: TableDef,
        table_name: str,
        sid: str,
        fields: dict[str, Any],
    ) -> None:
        key = (table_name, sid)
        row = self._docs.get(key)
        # FM-33: a soft-deleted row is absent to every write lookup.
        if row is None or not _is_live(row):
            raise RtDbError(ErrorCode.NOT_FOUND, f"document '{sid}' not found")
        # FM-36: stamp the patch fields before the merge (server `step_patch`)
        # — a patch that never mentions the field still restamps, and a
        # client-supplied value is overwritten. Before `apply_patch`'s whole-
        # doc validation, so a legacy doc missing the field re-populates.
        merged = apply_patch(
            table_def,
            row.doc,
            _stamp_updated_at(table_def, fields, self._now()),
            now=self._now(),
        )
        self._do_update(table_def, table_name, sid, merged)

    def _do_adjust_counter(
        self,
        table_def: TableDef,
        table_name: str,
        sid: str,
        field: str,
        delta: Any,
        minimum: Any,
        maximum: Any,
        expected: dict[str, Any] | None,
    ) -> None:
        def safe_number_integer(value: Any) -> bool:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                return False
            if isinstance(value, float):
                return math.isfinite(value) and value.is_integer() and abs(value) <= 2**53 - 1
            return abs(value) <= 2**53 - 1

        if (
            not _is_safe_counter_integer(delta)
            or (minimum is not None and not _is_safe_counter_integer(minimum))
            or (maximum is not None and not _is_safe_counter_integer(maximum))
            or (minimum is not None and maximum is not None and minimum > maximum)
        ):
            raise RtDbError(
                ErrorCode.BAD_REQUEST, "adjustCounter numeric arguments must be safe integers"
            )
        field_type = table_def.fields.get(field)
        if field_type is None:
            raise RtDbError(ErrorCode.SCHEMA_VIOLATION, f"unknown field '{field}'")
        if field in table_def.computed or field in (
            table_def.auto_increment_field,
            table_def.updated_at_field,
        ):
            raise RtDbError(ErrorCode.BAD_REQUEST, f"field '{field}' is server-managed")
        if not (
            isinstance(field_type, _FNumber)
            or (isinstance(field_type, _FOptional) and isinstance(field_type.inner, _FNumber))
        ):
            raise RtDbError(
                ErrorCode.SCHEMA_VIOLATION, f"field '{field}' must be number or optional(number)"
            )
        if expected is not None:
            unknown = set(expected) - set(table_def.fields)
            if unknown:
                raise RtDbError(
                    ErrorCode.SCHEMA_VIOLATION, f"unknown expected field '{sorted(unknown)[0]}'"
                )
        key = (table_name, sid)
        row = self._docs.get(key)
        if row is None or not _is_live(row):
            raise RtDbError(ErrorCode.NOT_FOUND, f"document '{sid}' not found")
        if expected is not None and any(
            name not in row.doc or not _json_structurally_equal(row.doc[name], value)
            for name, value in expected.items()
        ):
            raise RtDbError(ErrorCode.PRECONDITION_FAILED, "expected field values do not match")
        current = row.doc.get(field)
        if not safe_number_integer(current):
            raise RtDbError(ErrorCode.BAD_REQUEST, f"field '{field}' must contain a safe integer")
        result = current + delta
        if not safe_number_integer(result):
            raise RtDbError(ErrorCode.BAD_REQUEST, "adjustCounter result must be a safe integer")
        if (minimum is not None and result < minimum) or (maximum is not None and result > maximum):
            raise RtDbError(
                ErrorCode.PRECONDITION_FAILED, "adjustCounter result is outside the allowed bounds"
            )
        self._do_patch(table_def, table_name, sid, {field: result})

    def _do_replace(
        self,
        table_def: TableDef,
        table_name: str,
        sid: str,
        doc: dict[str, Any],
    ) -> None:
        key = (table_name, sid)
        row = self._docs.get(key)
        # FM-33: a soft-deleted row is absent to every write lookup.
        if row is None or not _is_live(row):
            raise RtDbError(ErrorCode.NOT_FOUND, f"document '{sid}' not found")
        # Replace writes a whole NEW document, so defaults apply (FM-32) —
        # unlike patch, clearing a field then replacing re-stamps it. The
        # FM-36 stamp runs after defaults (server `step_replace` order), so a
        # `defaults` entry on the stamped field still loses.
        _apply_defaults(table_def, doc)
        doc = _stamp_updated_at(table_def, doc, self._now())
        # FM-37: a replace validates as a complete document, so the stamped
        # counter must be present — an omitted field is filled from the stored
        # row (preserved, never re-assigned), and a supplied value must equal
        # the stored one (round-trip replace works, changing the counter does
        # not). A stored doc that PREDATES the declaration has no value to
        # preserve, so a replace may set one — first-set, like an insert
        # (mirrors server `do_replace`; runs after the stamps so a `defaults`
        # entry on the field is rejected rather than silently winning).
        auto = table_def.auto_increment_field
        if auto is not None and row.doc.get(auto) is not None:
            supplied = doc.get(auto)
            if supplied is None:
                doc = {**doc, auto: row.doc[auto]}
            elif supplied != row.doc[auto]:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST, f"autoIncrementField '{auto}' cannot be changed"
                )
        # ENH-028: client-supplied computed values are dropped before
        # validation — the stamp re-derives them from the final doc, and a
        # wrong-typed client value must not fail validate_doc first (server
        # `do_replace` order).
        for name in table_def.computed:
            doc.pop(name, None)
        doc = _stamp_computed(table_def, dict(doc), self._now())
        validate_doc(table_def, doc)
        stored = _strip_unset_optionals(table_def, doc)
        self._check_unique_indexes(table_def, table_name, stored, sid)
        row.doc = stored
        row.version += 1

    def _do_delete(
        self,
        table_def: TableDef,
        table_name: str,
        sid: str,
        touched: set[str],
    ) -> None:
        """Delete ``(table_name, sid)`` the FM-33 way: a ``softDelete`` table
        stamps a ``deleted_at`` tombstone (live-row-guarded — an already-stamped
        or absent row is ``NOT_FOUND``, and a soft delete never triggers a
        cascade); anything else hard-deletes through
        :meth:`_delete_row_cascade`, expanding the schema's ``onDelete`` rules."""
        if table_def.soft_delete:
            row = self._docs.get((table_name, sid))
            if row is None or not _is_live(row):
                raise RtDbError(ErrorCode.NOT_FOUND, f"document '{sid}' not found")
            self._docs[(table_name, sid)] = replace(
                row, deleted_at=self._now(), version=row.version + 1
            )
            touched.add(table_name)
            return
        visited: set[tuple[str, str]] = set()
        cascade_rows = [0]
        self._delete_row_cascade(table_name, sid, visited, cascade_rows, False, touched)

    def _delete_row_cascade(
        self,
        table_name: str,
        sid: str,
        visited: set[tuple[str, str]],
        cascade_rows: list[int],
        force_hard: bool,
        touched: set[str],
    ) -> None:
        """Delete row ``sid`` of ``table_name`` expanding the app-level
        ``onDelete`` rules (FM-33) — the port of server
        ``txn.rs::delete_row_cascade``. Not a SQL FK: the graph is declared in
        the pushed schema and walked here, children first (recursively — a
        child's own delete re-enters this walk), parent last.

        * ``softDelete`` table (unless ``force_hard``): the row is STAMPED, not
          removed, and the recursion stops — nothing past a stamped row is
          touched, and a soft delete is never itself a cascade trigger.
        * ``restrict``: the first live child (a ``LIMIT 1`` probe server-side)
          aborts with ``CONFLICT`` naming ``child_table.field`` and the child.
        * ``cascade``: recurse per live child; a ``softDelete`` child table
          gets its stamp (its own delete semantics apply to every delete that
          reaches it).
        * ``setNull``: per live child, patch ``{field: None}`` — which REMOVES
          the key (``apply_patch``'s unset semantics) — bumping ``version``.
        * ``visited`` guards cycles (self- and mutual-reference) and lets a
          ``deleteByQuery`` step skip rows an earlier row's cascade removed.
          ``cascade_rows`` (one shared cell) is the
          :data:`MAX_CASCADE_ROWS` budget; over-budget aborts with ``CONFLICT``.
        * ``force_hard`` (the TTL reaper) physically removes rows even on
          ``softDelete`` tables and propagates through the recursion.
        * ``touched`` accumulates every table written, for subscriber fan-out
          (the server's ``WriteSet.tables``).
        """
        table_def = self._require_table(table_name)
        if (table_name, sid) in visited:
            return
        visited.add((table_name, sid))
        if cascade_rows[0] >= _store.MAX_CASCADE_ROWS:
            raise RtDbError(
                ErrorCode.CONFLICT,
                f"onDelete cascade exceeds the limit of {_store.MAX_CASCADE_ROWS} rows",
            )
        cascade_rows[0] += 1

        if table_def.soft_delete and not force_hard:
            row = self._docs.get((table_name, sid))
            if row is None or not _is_live(row):
                raise RtDbError(ErrorCode.NOT_FOUND, f"document '{sid}' not found")
            self._docs[(table_name, sid)] = replace(
                row, deleted_at=self._now(), version=row.version + 1
            )
            touched.add(table_name)
            return

        # Children first: every schema table field declaring an onDelete action
        # referencing this table (the server's deterministic BTreeMap order is
        # not correctness-relevant; insertion order here).
        for child_table_name, child_table_def in self._tables.items():
            for field_name, field_type in child_table_def.fields.items():
                action = _on_delete_ref(field_type, table_name)
                if action is None:
                    continue
                if action == "restrict":
                    hits = self._visible_child_ids(
                        child_table_def, child_table_name, field_name, sid, limit_one=True
                    )
                    if hits:
                        raise RtDbError(
                            ErrorCode.CONFLICT,
                            f"cannot delete '{table_name}': "
                            f"'{child_table_name}.{field_name}' is referenced "
                            f"by document '{hits[0]}'",
                        )
                elif action == "cascade":
                    for child_id in self._visible_child_ids(
                        child_table_def, child_table_name, field_name, sid, limit_one=False
                    ):
                        self._delete_row_cascade(
                            child_table_name, child_id, visited, cascade_rows, force_hard, touched
                        )
                else:  # setNull
                    for child_id in self._visible_child_ids(
                        child_table_def, child_table_name, field_name, sid, limit_one=False
                    ):
                        if cascade_rows[0] >= _store.MAX_CASCADE_ROWS:
                            raise RtDbError(
                                ErrorCode.CONFLICT,
                                f"onDelete cascade exceeds the limit of "
                                f"{_store.MAX_CASCADE_ROWS} rows",
                            )
                        cascade_rows[0] += 1
                        # `{field: None}` on the optional id REMOVES the key
                        # (apply_patch's unset semantics) and bumps version.
                        # Written as a fresh row (not _do_patch/_do_update, which
                        # mutate in place) so a later cascade failure rolls the
                        # null back with the txn snapshot — every cascade write
                        # is snapshot-rollback-safe. FM-36: the CHILD table's
                        # updatedAtField joins the null patch (server
                        # `delete_row_cascade`) — setNull is a version-bumping
                        # write, so the child restamps.
                        child_row = self._docs[(child_table_name, child_id)]
                        merged = apply_patch(
                            child_table_def,
                            child_row.doc,
                            _stamp_updated_at(child_table_def, {field_name: None}, self._now()),
                            now=self._now(),
                        )
                        self._docs[(child_table_name, child_id)] = replace(
                            child_row, doc=merged, version=child_row.version + 1
                        )
                        touched.add(child_table_name)

        # Parent last. A soft-deleted row only reaches here under force_hard —
        # the stamp branch above returns first — so this is a physical remove.
        if self._docs.pop((table_name, sid), None) is None:
            raise RtDbError(ErrorCode.NOT_FOUND, f"document '{sid}' not found")
        touched.add(table_name)

    def _visible_child_ids(
        self,
        child_table_def: TableDef,
        child_table_name: str,
        field_name: str,
        parent_id: str,
        *,
        limit_one: bool,
    ) -> list[str]:
        """Ids of live rows in ``child_table_name`` whose ``field_name``
        references ``parent_id`` (the port of server ``visible_child_ids``).
        Soft-deleted children are invisible to every ``onDelete`` action."""
        out: list[str] = []
        for (t, row_id), row in self._docs.items():
            if t != child_table_name:
                continue
            if not _is_live(row):
                continue
            if row.doc.get(field_name) == parent_id:
                out.append(row_id)
                if limit_one:
                    break
        return out

    def _do_expect_version(
        self,
        table_def: TableDef,
        table_name: str,
        sid: str,
        expected: int,
    ) -> None:
        row = self._docs.get((table_name, sid))
        # FM-33: a soft-deleted row is absent — same NOT_FOUND as a miss.
        if row is None or not _is_live(row):
            raise RtDbError(ErrorCode.NOT_FOUND, f"document '{sid}' not found")
        if row.version != expected:
            raise RtDbError(
                ErrorCode.PRECONDITION_FAILED,
                f"version mismatch: expected {expected}, actual {row.version}",
            )

    def _do_update(
        self,
        table_def: TableDef,
        table_name: str,
        sid: str,
        merged: dict[str, Any],
    ) -> None:
        row = self._docs.get((table_name, sid))
        if row is not None:
            self._check_unique_indexes(table_def, table_name, merged, sid)
            row.doc = merged
            row.version += 1

    def _check_unique_indexes(
        self,
        table_def: TableDef,
        table_name: str,
        candidate_doc: dict[str, Any],
        exclude_id: str | None,
    ) -> None:
        """Enforce ``unique`` indexes on a candidate write (mirrors server
        ``CREATE UNIQUE INDEX`` and the TS/Rust ``checkUniqueIndexes``): for each
        unique index on ``table_name``, no OTHER row (excluding ``exclude_id``
        when given) that satisfies the index's ``where`` predicate may share the
        candidate's key values on the index's declared ``fields``. NULL/absent
        key fields disable the constraint for that row (Postgres ``UNIQUE`` treats
        NULLs as distinct). Raises ``CONFLICT`` on collision;
        :meth:`_execute_transaction` then rolls back the whole txn via the same
        snapshot/restore path as the ``PRECONDITION_FAILED`` checks. Uniqueness is
        on ``fields`` only — never ``id`` or ``created_at`` (a trailing
        tiebreaker column would defeat uniqueness, as it does on the server)."""
        for index in table_def.indexes:
            if not index.unique:
                continue
            pred = index.where
            # A partial unique index constrains only rows matching its predicate.
            if pred is not None and not _eval_filter_expr(pred, candidate_doc, table_def.fields):
                continue
            # Build the collision key from declared `fields` only. NULL/absent key
            # fields disable the constraint for this row (Postgres UNIQUE treats
            # NULLs as distinct) — skip the index for this candidate.
            candidate_key = _collect_index_key(index.fields, candidate_doc)
            if candidate_key is None:
                continue
            for (t, _row_id), row in self._docs.items():
                if t != table_name:
                    continue
                # FM-33: soft-deleted rows are outside the unique predicate
                # (the server widens it with `deleted_at IS NULL`).
                if not _is_live(row):
                    continue
                if exclude_id is not None and row.id == exclude_id:
                    continue
                if pred is not None and not _eval_filter_expr(pred, row.doc, table_def.fields):
                    continue
                row_key = _collect_index_key(index.fields, row.doc)
                if row_key is None:
                    continue
                if row_key == candidate_key:
                    raise RtDbError(
                        ErrorCode.CONFLICT,
                        f"unique index '{index.name}' violated",
                    )

    def _eq_lookup(
        self,
        table_def: TableDef,
        table_name: str,
        index_name: str,
        eq: list[Any],
    ) -> list[StoredRow]:
        index = _require_index(table_def, index_name)
        if len(eq) != len(index.fields):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"index '{index_name}' expects {len(index.fields)} eq value(s), got {len(eq)}",
            )
        typed = [
            _coerce_index_value(table_def, fld, value)
            for fld, value in zip(index.fields, eq, strict=True)
        ]
        matches: list[StoredRow] = []
        for (t, _id), row in self._docs.items():
            if t != table_name:
                continue
            # FM-33: soft-deleted rows are absent to ExpectAbsent and Upsert
            # (upserting a soft-deleted key inserts a fresh row).
            if not _is_live(row):
                continue
            if all(
                (rv := row.doc.get(fld)) is not None and rv == tv
                for fld, tv in zip(index.fields, typed, strict=True)
            ):
                matches.append(row)
        return matches
