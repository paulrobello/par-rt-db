"""Client-side optimistic-update projection.

Pure: given a query's wire dict, its last authoritative result, and a
transaction's step list, produce the value to overlay immediately (before the
server round-trip), or decline when the effect is ambiguous.

Ports ``rust-client/src/optimistic.rs`` (cross-checked against
``ts-client/src/optimistic.ts``). Conservative — only unambiguous cases overlay;
a wrong overlay is worse than a brief wait for the authoritative ``queryUpdate``.

The reactive ``RtDbClient`` caches each subscription's last result and holds
neither a schema nor a table store, so an overlay can only be computed from the
documents already in that cached result. This module mirrors the server/in-memory
DSL semantics for the cases where the effect on the result set is unambiguous
from those documents alone, and declines to guess everywhere else.

Overlay scope (mirrors the rust decline rules):

* unfiltered ``collect`` / ``take`` (no index/eq/range/filter): insert / patch /
  replace / delete on a known id all overlay.
* filtered ``collect`` / ``take`` (index/eq/range or a ``filter`` predicate):
  only a delete of an already-cached doc overlays — adding or changing a doc may
  move it in or out of the filter, so its membership is ambiguous.
* ``get(id)`` point read: patch / replace / delete of that id overlay; a freshly
  inserted id can never match a pre-existing ``get(target)``.

Declines (return ``(None, False)``): ``upsert`` (anywhere), and any
rank/membership-dependent terminal — ``first`` / ``unique`` / ``count`` /
``distinct`` / ``aggregate`` / ``paginate`` / ``search`` / ``vectorSearch`` /
``hybridSearch`` — whose result cannot be derived from cached documents alone.

``now_ms`` (epoch-millis) is taken as a parameter so this module is pure and
clock-free; the caller (the WS wiring) supplies ``int(time.time() * 1000)`` and
tests pass a fixed value.

Canonical no-op detection relies on Python dict/list structural equality being
order-independent for dict keys (mirrors rust's ``BTreeMap``-backed
``serde_json::Value::eq`` and TS's explicit ``canonical()`` key-sort) — so a
patch that sets a field to its existing value is detected as a no-op and skipped.
"""

from __future__ import annotations

import itertools
from typing import Any

__all__ = ["project"]


_SYNTHETIC_COUNTER = itertools.count(1)


def _synthetic_id() -> str:
    """A clearly-branded temporary id for an optimistically-inserted doc.

    Replaced on reconcile with the server-assigned id. Module-global counter
    (single-threaded async client); mirrors rust's ``AtomicU64``."""
    return f"__optimistic__{next(_SYNTHETIC_COUNTER)}"


def project(
    query_dict: dict[str, Any],
    last_value: Any,
    txn_steps: list[dict[str, Any]],
    now_ms: int,
) -> tuple[Any, bool]:
    """Project ``txn_steps`` onto ``last_value`` (the cached result for ``query_dict``).

    Returns ``(overlaid_value, True)`` to overlay, or ``(None, False)`` to decline
    (no-op or ambiguous effect — the caller does not distinguish the two; both
    mean "do not overlay"). Routes to one of three shapes — unfiltered array,
    filtered array (delete-only), or ``get(id)`` point-read — or declines when the
    query shape or step kind makes the effect ambiguous.

    Pure: no I/O, no clock, no mutation of ``last_value`` or ``txn_steps``.
    """
    if query_dict.get("get") is not None:
        return _project_get(query_dict, last_value, txn_steps)
    if not _is_array_query(query_dict):
        return (None, False)
    if _has_filter(query_dict):
        return _project_filtered_array(query_dict, last_value, txn_steps)
    return _project_unfiltered_array(query_dict, last_value, txn_steps, now_ms)


def _is_array_query(q: dict[str, Any]) -> bool:
    """``get``/``unique``/``first``/``count``/``distinct``/``paginate``/``search``/
    ``vectorSearch`` terminals are non-array (or rank-based) shapes whose result
    cannot be projected from cached documents alone. A ``filter`` predicate is NOT
    excluded here: a filtered collect is still an array read, just one whose
    membership is handled by ``_has_filter`` (delete-only)."""
    return (
        q.get("get") is None
        and not q.get("unique")
        and not q.get("first")
        and not q.get("count")
        and not q.get("distinct")
        and q.get("paginate") is None
        and q.get("search") is None
        and q.get("vectorSearch") is None
    )


def _has_filter(q: dict[str, Any]) -> bool:
    """A query whose result membership depends on a predicate we cannot evaluate
    without the schema (index/eq/range or a db-side ``filter``). Only deletes of
    already-cached docs are unambiguous under such a filter."""
    return (
        q.get("index") is not None
        or bool(q.get("eq"))
        or q.get("gt") is not None
        or q.get("gte") is not None
        or q.get("lt") is not None
        or q.get("lte") is not None
        or q.get("filter") is not None
    )


def _step_table(step: dict[str, Any]) -> str | None:
    """The table this step targets. Every op except ``expectAbsent`` carries one;
    ``expectAbsent`` is a precondition with no data effect, so its table is masked
    here (returning None makes the per-step table guard skip it, which is harmless
    since the variant is a no-op in every projection)."""
    if step.get("op") == "expectAbsent":
        return None
    table = step.get("table")
    return table if isinstance(table, str) else None


def _project_unfiltered_array(
    query_dict: dict[str, Any],
    last_value: Any,
    txn_steps: list[dict[str, Any]],
    now_ms: int,
) -> tuple[Any, bool]:
    """Unfiltered full-table read (``collect``/``take`` with no index/eq/range/
    filter): every doc is present, so insert/patch/replace/delete on a known id
    are all unambiguous."""
    if not isinstance(last_value, list):
        return (None, False)
    qtable = query_dict.get("table")
    take = query_dict.get("take")
    working: list[Any] = [dict(d) if isinstance(d, dict) else d for d in last_value]
    for step in txn_steps:
        if _step_table(step) != qtable:
            continue
        op = step.get("op")
        if op == "insert":
            # A full-table window already at its take limit would evict an unknown
            # doc — we can't pick the right window, so decline.
            if take is not None and len(working) >= take:
                return (None, False)
            new_doc = dict(step.get("doc") or {})
            new_doc["_id"] = _synthetic_id()
            new_doc["_creationTime"] = now_ms
            new_doc["_version"] = 1
            working.append(new_doc)
        elif op == "patch":
            _merge_by_id(working, step.get("id"), step.get("fields") or {})
        elif op == "replace":
            _replace_by_id(working, step.get("id"), step.get("doc") or {})
        elif op == "delete":
            _remove_by_id(working, step.get("id"))
        elif op == "upsert":
            return (None, False)
        # expectVersion / expectAbsent: preconditions, no data effect.
    return _finalize(working, last_value)


def _project_filtered_array(
    query_dict: dict[str, Any],
    last_value: Any,
    txn_steps: list[dict[str, Any]],
) -> tuple[Any, bool]:
    """Filtered read (index/eq/range or ``filter`` predicate): only a delete of a
    doc already known to be in the result is unambiguous — adding or changing a
    doc may move it in or out of the filter."""
    if not isinstance(last_value, list):
        return (None, False)
    qtable = query_dict.get("table")
    working: list[Any] = [dict(d) if isinstance(d, dict) else d for d in last_value]
    for step in txn_steps:
        if _step_table(step) != qtable:
            continue
        op = step.get("op")
        if op == "delete":
            _remove_by_id(working, step.get("id"))
        elif op in ("insert", "patch", "replace", "upsert"):
            # insert/patch/replace/upsert are membership-ambiguous under a filter.
            return (None, False)
        # expectVersion / expectAbsent: no data effect.
    return _finalize(working, last_value)


def _project_get(
    query_dict: dict[str, Any],
    last_value: Any,
    txn_steps: list[dict[str, Any]],
) -> tuple[Any, bool]:
    """Point read by id: the result is exactly that id's doc (or null), so
    patch/replace/delete of the same id are unambiguous; a freshly inserted id can
    never match a pre-existing ``get(target)``."""
    target = query_dict.get("get")
    if target is None:
        return (None, False)
    if last_value is not None and not isinstance(last_value, dict):
        return (None, False)
    qtable = query_dict.get("table")
    working: dict[str, Any] | None = None if last_value is None else dict(last_value)
    for step in txn_steps:
        if _step_table(step) != qtable:
            continue
        op = step.get("op")
        sid = step.get("id")
        if op == "delete":
            if sid == target:
                working = None
        elif op == "patch":
            if sid == target and working is not None:
                working.update(step.get("fields") or {})
        elif op == "replace":
            if sid == target and working is not None:
                new_doc = dict(step.get("doc") or {})
                old_id = working.get("_id")
                old_ct = working.get("_creationTime")
                if old_id is not None:
                    new_doc["_id"] = old_id
                if old_ct is not None:
                    new_doc["_creationTime"] = old_ct
                new_doc.pop("_version", None)
                working = new_doc
        elif op == "upsert":
            return (None, False)
        # insert (fresh id never matches), expectVersion / expectAbsent: no-op.
    return _finalize(working, last_value)


def _finalize(next_value: Any, last_value: Any) -> tuple[Any, bool]:
    """Overlay only when the projection actually changed the value. Dict/list
    structural equality is order-independent for dict keys, so a no-op patch
    (setting a field to its existing value) is detected and declined."""
    if next_value == last_value:
        return (None, False)
    return (next_value, True)


def _merge_by_id(working: list[Any], id_: Any, fields: dict[str, Any]) -> None:
    if id_ is None:
        return
    for i, v in enumerate(working):
        if isinstance(v, dict) and v.get("_id") == id_:
            working[i].update(fields)


def _replace_by_id(working: list[Any], id_: Any, doc: dict[str, Any]) -> None:
    if id_ is None:
        return
    for i, v in enumerate(working):
        if isinstance(v, dict) and v.get("_id") == id_:
            new_doc = dict(doc)
            old_id = v.get("_id")
            old_ct = v.get("_creationTime")
            if old_id is not None:
                new_doc["_id"] = old_id
            if old_ct is not None:
                new_doc["_creationTime"] = old_ct
            new_doc.pop("_version", None)
            working[i] = new_doc


def _remove_by_id(working: list[Any], id_: Any) -> None:
    if id_ is None:
        return
    working[:] = [v for v in working if not (isinstance(v, dict) and v.get("_id") == id_)]
