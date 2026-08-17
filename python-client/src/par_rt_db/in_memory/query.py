"""Query engine for the in-memory harness (mirrors
``rust-client/src/in_memory/query.rs``): the ``run_query`` dispatcher, the
per-terminal executors, and the search/cursor/aggregate helpers they share.
``run_query`` is a thin dispatcher — ``_check_query_combinations``,
``_prepare_scan``, ``_fetch_filtered_rows``, ``_sort_filtered_rows``, then
one executor per terminal."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from functools import cmp_to_key
from typing import TYPE_CHECKING, Any

from ..cursor import decode_cursor, encode_cursor
from ..errors import ErrorCode, RtDbError
from ..query import Query
from ..schema import (
    IndexDef,
    TableDef,
)
from ..wire import (
    AggregateOp,
)
from .store import (
    _INT64,
    _NUMBER,
    _TEXT,
    MAX_TAKE,
    StoredRow,
    _coerce_index_value,
    _compare_index_values,
    _index_column_type,
    _is_live,
    _merge_doc,
    _parse_i64,
    _pg_for_field,
    _PgType,
    _require_index,
    _to_float,
)
from .validate import _eval_filter_expr, _validate_filter

if TYPE_CHECKING:
    from .store import _InMemoryStoreCore as _Core
else:
    _Core = object


def _check_query_combinations(q: Query) -> None:
    """Conflicting-terminal guards, in the server's validation order: each
    terminal rejects the peers it cannot compose with, then the range-bound and
    take-cap checks apply to every remaining shape."""
    unique = bool(q.unique)
    first = bool(q.first)
    count = bool(q.count)

    # Conflicting-terminal guards.
    if unique and (
        q.take is not None or q.order is not None or q.distinct or q.aggregate is not None
    ):
        raise RtDbError(
            ErrorCode.BAD_REQUEST,
            "unique cannot be combined with take, order, distinct, or aggregate",
        )
    if first and unique:
        raise RtDbError(ErrorCode.BAD_REQUEST, "first cannot be combined with unique")
    if first and q.take is not None:
        raise RtDbError(ErrorCode.BAD_REQUEST, "first cannot be combined with take")
    if first and q.distinct:
        raise RtDbError(ErrorCode.BAD_REQUEST, "first cannot be combined with distinct")
    if first and q.aggregate is not None:
        raise RtDbError(ErrorCode.BAD_REQUEST, "first cannot be combined with aggregate")
    if count and unique:
        raise RtDbError(ErrorCode.BAD_REQUEST, "count cannot be combined with unique")
    if count and q.take is not None:
        raise RtDbError(ErrorCode.BAD_REQUEST, "count cannot be combined with take")
    if count and first:
        raise RtDbError(ErrorCode.BAD_REQUEST, "count cannot be combined with first")
    if count and q.order is not None:
        raise RtDbError(ErrorCode.BAD_REQUEST, "count cannot be combined with order")
    if count and q.distinct:
        raise RtDbError(ErrorCode.BAD_REQUEST, "count cannot be combined with distinct")
    if count and q.aggregate is not None:
        raise RtDbError(ErrorCode.BAD_REQUEST, "count cannot be combined with aggregate")
    if q.paginate is not None:
        if count:
            raise RtDbError(ErrorCode.BAD_REQUEST, "paginate cannot be combined with count")
        if unique:
            raise RtDbError(ErrorCode.BAD_REQUEST, "paginate cannot be combined with unique")
        if first:
            raise RtDbError(ErrorCode.BAD_REQUEST, "paginate cannot be combined with first")
        if q.take is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "paginate cannot be combined with take")
        if q.distinct:
            raise RtDbError(ErrorCode.BAD_REQUEST, "paginate cannot be combined with distinct")
        if q.aggregate is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "paginate cannot be combined with aggregate")
    if q.gt is not None and q.gte is not None:
        raise RtDbError(ErrorCode.BAD_REQUEST, "gt and gte cannot both be set")
    if q.lt is not None and q.lte is not None:
        raise RtDbError(ErrorCode.BAD_REQUEST, "lt and lte cannot both be set")
    if q.take is not None and q.take > MAX_TAKE:
        raise RtDbError(ErrorCode.BAD_REQUEST, f"take exceeds maximum of {MAX_TAKE}")

    # `distinct`/`aggregate` are standalone terminals (like `count`): they
    # compose only with index/eq/range/filter. `get`/`unique`/`first`/`count`
    # rejected their own combinations above (validated first, matching the
    # server's check order), so these blocks reject the remaining peers each
    # terminal owns — mirroring the server's DISTINCT/AGGREGATE_INCOMPATIBLES.
    if q.distinct:
        if q.take is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "distinct cannot be combined with take")
        if q.order is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "distinct cannot be combined with order")
        if q.aggregate is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "distinct cannot be combined with aggregate")
        if q.paginate is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "distinct cannot be combined with paginate")
        if q.search is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "distinct cannot be combined with search")
        if q.vector_search is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "distinct cannot be combined with vector search")
        if q.hybrid_search is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "distinct cannot be combined with hybrid search")
    if q.aggregate is not None:
        if q.take is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "aggregate cannot be combined with take")
        if q.order is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "aggregate cannot be combined with order")
        if q.paginate is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "aggregate cannot be combined with paginate")
        if q.search is not None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "aggregate cannot be combined with search")
        if q.vector_search is not None:
            raise RtDbError(
                ErrorCode.BAD_REQUEST, "aggregate cannot be combined with vector search"
            )
        if q.hybrid_search is not None:
            raise RtDbError(
                ErrorCode.BAD_REQUEST, "aggregate cannot be combined with hybrid search"
            )


@dataclass
class _ScanPlan:
    """Everything the row scan needs besides the query itself: the resolved
    index, the type-checked eq prefix, and the coerced range bounds. Produced
    once by ``_prepare_scan``, consumed by ``_fetch_filtered_rows`` /
    ``_sort_filtered_rows`` / ``_execute_paginate_terminal``."""

    index_def: IndexDef | None
    typed_eq: list[Any]
    range_field: str | None
    range_pg: _PgType
    gt: Any
    gte: Any
    lt: Any
    lte: Any


def _prepare_scan(q: Query, table_def: TableDef, eq: list[Any], has_range: bool) -> _ScanPlan:
    """Index resolution, eq-prefix binding, range-bound coercion, and one-time
    filter validation — everything the row scan needs before touching a row."""
    # Resolve index — required for `eq` and for any range bound.
    index_def: IndexDef | None = None
    if q.index is not None:
        index_def = _require_index(table_def, q.index)
    elif eq:
        raise RtDbError(ErrorCode.BAD_REQUEST, "eq requires an index")

    # eq-arity check.
    if index_def is not None and len(eq) > len(index_def.fields):
        raise RtDbError(
            ErrorCode.BAD_REQUEST,
            f"index '{index_def.name}' expects at most {len(index_def.fields)} "
            f"eq value(s), got {len(eq)}",
        )

    # Type-check each eq prefix bind positionally.
    typed_eq: list[Any] = []
    if index_def is not None:
        for i, value in enumerate(eq):
            typed_eq.append(_coerce_index_value(table_def, index_def.fields[i], value))

    # Range bounds apply to the next index field after the eq prefix.
    range_field: str | None = None
    if has_range:
        if index_def is None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "range bound requires an index")
        if len(eq) >= len(index_def.fields):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                "range bound requires a remaining index field after eq",
            )
        range_field = index_def.fields[len(eq)]
    range_pg = _TEXT
    if range_field is not None:
        range_field_ty = table_def.fields.get(range_field)
        if range_field_ty is not None:
            try:
                range_pg = _index_column_type(range_field_ty).pg
            except RtDbError:
                range_pg = _TEXT
    gt = (
        _coerce_index_value(table_def, range_field, q.gt)
        if q.gt is not None and range_field
        else None
    )
    gte = (
        _coerce_index_value(table_def, range_field, q.gte)
        if q.gte is not None and range_field
        else None
    )
    lt = (
        _coerce_index_value(table_def, range_field, q.lt)
        if q.lt is not None and range_field
        else None
    )
    lte = (
        _coerce_index_value(table_def, range_field, q.lte)
        if q.lte is not None and range_field
        else None
    )

    # Compile the filter against the table's declared fields once up front.
    if q.filter is not None:
        _validate_filter(q.filter, table_def)
    return _ScanPlan(
        index_def=index_def,
        typed_eq=typed_eq,
        range_field=range_field,
        range_pg=range_pg,
        gt=gt,
        gte=gte,
        lt=lt,
        lte=lte,
    )


def _sort_filtered_rows(
    filtered: list[StoredRow],
    table_def: TableDef,
    index_def: IndexDef | None,
    typed_eq: list[Any],
    direction: str,
) -> None:
    """Sorts the filtered set in place by the shared sort columns (unbound
    index fields after the eq prefix, then ``_creationTime``, then ``_id``) in
    direction ``direction``. The unique id tiebreaker makes the order total."""
    unbound_fields: list[str] = index_def.fields[len(typed_eq) :] if index_def is not None else []
    sort_field_pgs: list[_PgType] = [_pg_for_field(table_def, f) for f in unbound_fields]

    def cmp(a: StoredRow, b: StoredRow) -> int:
        for i, fld in enumerate(unbound_fields):
            av = a.doc.get(fld)
            bv = b.doc.get(fld)
            c = _compare_index_values(av, bv, sort_field_pgs[i])
            if c != 0:
                return _dir_order(c, direction)
        c = (a.created_at > b.created_at) - (a.created_at < b.created_at)
        if c != 0:
            return _dir_order(c, direction)
        return _dir_order((a.id > b.id) - (a.id < b.id), direction)

    filtered.sort(key=cmp_to_key(cmp))


def _is_number(value: Any) -> bool:
    """``True`` iff ``value`` is a JSON number (booleans excluded)."""
    return isinstance(value, float | int) and not isinstance(value, bool)


def _apply_aggregate(op: str, values: list[Any], pg: _PgType) -> Any:
    """Apply one aggregate op over a non-empty list of non-null values, mirroring
    the server's SQL semantics and the TS/Rust harnesses. SUM/AVG reduce
    numerically (int64 values are decimal strings -> parsed); MIN/MAX pick the
    smallest/largest per :func:`_compare_index_values`, so a string field's
    extremes match Postgres lexicographic ordering. Only called on non-empty
    input — the caller maps an empty set to ``None``."""
    if op in (AggregateOp.SUM, AggregateOp.AVG):
        nums = [_to_numeric(v, pg) for v in values]
        total = sum(nums)
        return total / len(values) if op == AggregateOp.AVG else total
    want_min = op == AggregateOp.MIN
    best = values[0]
    for v in values[1:]:
        c = _compare_index_values(v, best, pg)
        if c < 0 if want_min else c > 0:
            best = v
    return best


def _to_numeric(v: Any, pg: _PgType) -> float | int:
    """Reduce one index value to a number for SUM/AVG. ``_INT64`` values are
    decimal strings on the wire -> parsed to int; ``_NUMBER`` values are floats."""
    if pg == _INT64:
        return _parse_i64(v)
    return _to_float(v)


def _dedupe_key(v: Any) -> str:
    """Canonical JSON key so equal scalars (and equal compound values) share a
    key. Distinct/group keys are always index fields (scalars), so this reduces
    to the scalar's string form in practice."""
    return json.dumps(v, sort_keys=True, separators=(",", ":"))


def _dir_order(o: int, direction: str) -> int:
    return o if direction == "asc" else -o


def _paginate_result(
    paginate: Any,
    table_def: TableDef,
    sorted_rows: list[StoredRow],
    sort_cols: list[tuple[str, str | None]],
    col_types: list[_PgType],
    direction: str,
) -> dict[str, Any]:
    num_items = min(int(paginate.num_items), MAX_TAKE)
    cursor_values: list[Any] | None = None
    if paginate.cursor is not None:
        try:
            decoded = decode_cursor(paginate.cursor)
        except ValueError as err:
            raise RtDbError(ErrorCode.BAD_REQUEST, f"invalid cursor: {err}") from err
        if len(decoded) != len(sort_cols):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"cursor has {len(decoded)} value(s) but this query sorts over "
                f"{len(sort_cols)} column(s)",
            )
        _validate_cursor_values(decoded, sort_cols, table_def)
        cursor_values = decoded

    if cursor_values is not None:
        rows = [
            row
            for row in sorted_rows
            if _is_after_cursor(row, cursor_values, sort_cols, col_types, direction)
        ]
    else:
        rows = sorted_rows

    has_next = len(rows) > num_items
    page = rows[:num_items]
    docs = [_merge_doc(row) for row in page]

    out: dict[str, Any] = {"docs": docs}
    if has_next and page:
        last = page[-1]
        keyset = [_sort_value(last, col) for col in sort_cols]
        out["nextCursor"] = encode_cursor(keyset)
    return out


def _validate_cursor_values(
    cursor_values: list[Any],
    sort_cols: list[tuple[str, str | None]],
    table_def: TableDef,
) -> None:
    for (kind, fld), value in zip(sort_cols, cursor_values, strict=True):
        if kind == "index":
            if fld is not None and value is not None:
                _coerce_index_value(table_def, fld, value)
        elif kind == "createdAt" and not _is_number(value):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                "cursor value for created_at must be a number",
            )
        elif kind == "id" and not isinstance(value, str):
            raise RtDbError(ErrorCode.BAD_REQUEST, "cursor value for id must be a string")


def _is_after_cursor(
    row: StoredRow,
    cursor_values: list[Any],
    sort_cols: list[tuple[str, str | None]],
    col_types: list[_PgType],
    direction: str,
) -> bool:
    for i in range(len(sort_cols)):
        prefix_equal = True
        for j in range(i):
            rv = _sort_value(row, sort_cols[j])
            if _compare_index_values(rv, cursor_values[j], col_types[j]) != 0:
                prefix_equal = False
                break
        if not prefix_equal:
            continue
        rv = _sort_value(row, sort_cols[i])
        c = _compare_index_values(rv, cursor_values[i], col_types[i])
        ahead = c > 0 if direction == "asc" else c < 0
        if ahead:
            return True
    return False


def _sort_value(row: StoredRow, col: tuple[str, str | None]) -> Any:
    kind, fld = col
    if kind == "createdAt":
        return row.created_at
    if kind == "id":
        return row.id
    return row.doc.get(fld) if fld is not None else None


#: Word cap on the harness ``_searchSnippet`` excerpt — the server's
#: ``ts_headline`` ``MaxWords=35`` bound (``SNIPPET_HEADLINE_OPTS`` in
#: ``server/src/query.rs``). ``MinWords=15`` has no harness analogue: the
#: excerpt simply runs to the cap (or the end of the text).
_WEBSEARCH_SNIPPET_MAX_WORDS = 35


@dataclass
class _Websearch:
    """Parsed websearch query — the output of :func:`_parse_websearch`.

    ``segments`` are AND-groups joined by OR (websearch precedence: ``&``
    binds tighter than ``|`` — ``a b or c`` is ``(a & b) | c``); each unit is
    a word tuple — one word for a bare term, the phrase words for a quoted
    span (matched as a contiguous run). ``excluded`` units (from ``-term``)
    filter globally: a doc containing one never matches.
    """

    segments: list[list[tuple[str, ...]]]
    excluded: list[tuple[str, ...]]


def _websearch_tokens(text: str) -> list[tuple[bool, str]]:
    """Split websearch text into ``(is_phrase, raw)`` tokens: quoted spans and
    whitespace-delimited bare words. An unterminated quote takes the rest of
    the text as one phrase (Postgres behavior)."""
    tokens: list[tuple[bool, str]] = []
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if ch.isspace():
            i += 1
        elif ch == '"':
            end = text.find('"', i + 1)
            if end == -1:
                tokens.append((True, text[i + 1 :]))
                break
            tokens.append((True, text[i + 1 : end]))
            i = end + 1
        else:
            j = i
            while j < n and not text[j].isspace() and text[j] != '"':
                j += 1
            tokens.append((False, text[i:j]))
            i = j
    return tokens


def _parse_websearch(text: str) -> _Websearch:
    """Approximate Postgres ``websearch_to_tsquery`` (FM-31).

    Builds the same expression shape PG builds — OR-of-AND-segments over
    positive units — where a unit is a bare word or a quoted phrase (matched
    as a contiguous word run, case-insensitive). ``or`` (outside quotes,
    case-insensitive) starts a new AND-segment; a doubled or leading ``or``
    is a literal word (a PG quirk kept for parity: ``a or or b`` is
    ``a | (or & b)``). ``-word`` — and ``-"a phrase"`` — excludes. Deliberate
    simplifications vs. the live server: stemming is not modeled (exact word
    equality — ``"running"`` misses a doc saying ``runs``), punctuation inside
    tokens is kept verbatim, and an exclusion always filters the whole
    result even where PG would scope it into one ``or`` branch (``x or -y``).
    """
    tokens = _websearch_tokens(text)
    segments: list[list[tuple[str, ...]]] = [[]]
    excluded: list[tuple[str, ...]] = []
    i = 0
    while i < len(tokens):
        is_phrase, raw = tokens[i]
        i += 1
        if is_phrase:
            unit = tuple(w.lower() for w in raw.split())
            if unit:
                segments[-1].append(unit)
            continue
        if raw.lower() == "or" and segments[-1]:
            segments.append([])
            continue
        if raw.startswith("-"):
            body = raw.lstrip("-")
            if body:
                excluded.append((body.lower(),))
                continue
            # A bare `-` negates the next token when it is a quoted phrase
            # (`-"a b"` -> exclude the adjacent run); otherwise it carries no
            # operand and is dropped.
            if i < len(tokens) and tokens[i][0]:
                phrase = tuple(w.lower() for w in tokens[i][1].split())
                i += 1
                if phrase:
                    excluded.append(phrase)
            continue
        segments[-1].append((raw.lower(),))
    return _Websearch(segments=[s for s in segments if s], excluded=excluded)


def _search_field_words(search_def: IndexDef, doc: dict[str, Any]) -> list[str]:
    """The search surface's text as a word list: the index's declared string
    fields joined in order — the same concatenation the server's generated
    tsvector column is built over (``coalesce(f, '') || ' ' || ...``), so
    phrases may span field boundaries here too. Non-string values contribute
    nothing (their tsvector text is empty)."""
    parts = [v for field in search_def.fields if isinstance(v := doc.get(field), str)]
    return " ".join(parts).split()


def _websearch_unit_in(unit: tuple[str, ...], low_words: list[str]) -> bool:
    """A unit matches when its words appear as a contiguous run in the
    lowercased word list — a phrase needs adjacency; a lone word is a 1-run
    (present anywhere)."""
    n = len(unit)
    if n == 0:
        return False
    return any(low_words[i : i + n] == list(unit) for i in range(len(low_words) - n + 1))


def _websearch_matches(parsed: _Websearch, words: list[str]) -> bool:
    """Evaluate a parsed websearch query against one doc's (original-case)
    word list: no excluded unit may match, and some AND-segment must match in
    full. No positive segments (a pure ``-term`` query, or bare punctuation)
    matches everything not excluded — PG's bare ``!term`` behavior."""
    low = [w.lower() for w in words]
    if any(_websearch_unit_in(unit, low) for unit in parsed.excluded):
        return False
    return (not parsed.segments) or any(
        all(_websearch_unit_in(unit, low) for unit in segment) for segment in parsed.segments
    )


def _websearch_snippet(parsed: _Websearch, words: list[str]) -> str:
    """The ``_searchSnippet`` stand-in: a ≤35-word excerpt (the server's
    ``ts_headline`` ``MaxWords`` bound) starting at the first matched word,
    with every positive-unit word wrapped in ``<mark>...</mark>`` — phrases
    render as adjacent per-word marks, like the server. Excluded words are
    not marked (the headline renders the positive tree only)."""
    mark = {w for segment in parsed.segments for unit in segment for w in unit}
    start = next((i for i, w in enumerate(words) if w.lower() in mark), 0)
    window = words[start : start + _WEBSEARCH_SNIPPET_MAX_WORDS]
    return " ".join(f"<mark>{w}</mark>" if w.lower() in mark else w for w in window)


class _QueryEngine(_Core):
    """The ``run_query`` dispatcher and one executor per query terminal."""

    def run_query(self, q: Query) -> Any:
        """Execute a one-shot query. Returns the terminal result:

        * ``get(id)`` / ``first`` → merged doc, or ``None`` when absent.
        * ``unique`` → merged doc, ``None`` when zero match, or
          :class:`RtDbError` ``PRECONDITION_FAILED`` when more than one matches.
        * ``count`` → ``int``.
        * ``take`` / ``collect`` → ``list`` of merged docs.
        * ``paginate`` → ``{"docs": [...], "nextCursor"?: str}``.
        * ``search`` → list of merged docs narrowed by the terminal's optional
          ``filter`` (ranking is not modeled — every table row is a candidate).
        * ``vectorSearch`` → list of merged docs narrowed by the terminal's
          optional ``filter`` (vector similarity is not modeled — every table
          row is a candidate; ``hybridSearch`` still returns an empty list).

        ``filter`` is structurally validated once up front, then evaluated per
        row. See the module docs for the unimplemented terminals.
        """
        table_def = self._require_table(q.table)
        eq = q.eq or []
        has_range = q.gt is not None or q.gte is not None or q.lt is not None or q.lte is not None

        if q.get is not None:
            return self._execute_get_terminal(q, eq, has_range)

        _check_query_combinations(q)

        if q.vector_search is not None:
            return self._execute_vector_search_terminal(q, table_def, eq, has_range)
        if q.search is not None:
            return self._execute_search_terminal(q, table_def, eq, has_range)
        if q.hybrid_search is not None:
            return self._execute_hybrid_search_terminal(q, eq, has_range)

        plan = _prepare_scan(q, table_def, eq, has_range)
        filtered = self._fetch_filtered_rows(q, plan, table_def.fields)

        if q.count:
            return self._execute_count_terminal(filtered)

        if q.distinct:
            return self._execute_distinct_terminal(
                table_def, plan.index_def, plan.typed_eq, filtered
            )

        if q.aggregate is not None:
            return self._execute_aggregate_terminal(
                q, table_def, plan.index_def, plan.typed_eq, filtered
            )

        direction = q.order or "asc"
        _sort_filtered_rows(filtered, table_def, plan.index_def, plan.typed_eq, direction)

        if q.paginate is not None:
            return self._execute_paginate_terminal(
                q.paginate, table_def, filtered, plan.index_def, plan.typed_eq, direction
            )

        return self._execute_collect_terminal(q, filtered)

    def _execute_get_terminal(self, q: Query, eq: list[Any], has_range: bool) -> Any:
        """``get(id)`` terminal: point read by id, exclusive of every other clause.

        Lift of the former inline ``if q.get is not None:`` arm of
        :meth:`run_query`; mirrors ``ts-client``'s ``executeGetTerminal``. The
        ``unique``/``first``/``count`` locals of ``run_query`` are read here
        straight off ``q`` (``bool | None`` — identical truthiness to the
        ``bool(q.*)`` precomputed locals).
        """
        if (
            q.index is not None
            or eq
            or has_range
            or q.order is not None
            or q.take is not None
            or q.unique
            or q.first
            or q.count
            or q.distinct
            or q.aggregate is not None
            or q.paginate is not None
            or q.filter is not None
            or q.search is not None
            or q.vector_search is not None
            or q.hybrid_search is not None
        ):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                "get cannot be combined with index, eq, range bounds, order, take, "
                "unique, first, count, distinct, aggregate, paginate, filter, search, "
                "vector search, or hybrid search",
            )
        assert q.get is not None  # caller dispatches only when get is set
        return self.get(q.table, q.get)

    def _execute_vector_search_terminal(
        self, q: Query, table_def: TableDef, eq: list[Any], has_range: bool
    ) -> list[dict[str, Any]]:
        """``vectorSearch`` terminal.

        Lift of the former inline ``if q.vector_search is not None:`` arm of
        :meth:`run_query`; mirrors ``ts-client``'s ``executeVectorSearchTerminal``.
        Vector similarity is not modeled in-memory, so every table row is a
        candidate (the sound over-approximation); a declared ``filter`` narrows
        the set via :func:`_eval_filter_expr`. The terminal's ``limit`` is not
        applied: without ranking there is no meaningful "top N".
        """
        assert q.vector_search is not None  # caller dispatches only when set
        if (
            q.index is not None
            or eq
            or has_range
            or q.order is not None
            or q.unique
            or q.first
            or q.count
            or q.filter is not None
            or q.search is not None
            or q.take is not None
            or q.paginate is not None
            or q.hybrid_search is not None
        ):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                "vectorSearch cannot be combined with any other terminal",
            )
        if q.vector_search.filter is not None:
            _validate_filter(q.vector_search.filter, table_def)
        vector_candidates: list[StoredRow] = [
            row for (t, _id), row in self._docs.items() if t == q.table and _is_live(row)
        ]
        if q.vector_search.filter is not None:
            vector_candidates = [
                row
                for row in vector_candidates
                if _eval_filter_expr(q.vector_search.filter, row.doc, table_def.fields)
            ]
        return [_merge_doc(row) for row in vector_candidates]

    def _execute_search_terminal(
        self, q: Query, table_def: TableDef, eq: list[Any], has_range: bool
    ) -> list[dict[str, Any]]:
        """``search`` terminal.

        Lift of the former inline ``if q.search is not None:`` arm of
        :meth:`run_query`; mirrors ``ts-client``'s ``executeSearchTerminal``.
        A declared ``filter`` narrows the candidate set via
        :func:`_eval_filter_expr`. ``tsquery`` mode (the default) approximates
        the server's ``websearch_to_tsquery`` match — quoted phrases require
        adjacency, ``or`` unions, ``-term`` excludes, other terms ANDed
        (:func:`_parse_websearch`; stemming and ``ts_rank`` are not modeled) —
        and routes to :meth:`_execute_tsquery_search`. ``trgm`` mode (FM-30)
        matches for real — substring containment is modeled — and routes to
        :meth:`_execute_trgm_search`.
        """
        assert q.search is not None  # caller dispatches only when set
        if (
            q.index is not None
            or eq
            or has_range
            or q.order is not None
            or q.unique
            or q.first
            or q.count
            or q.filter is not None
            or q.vector_search is not None
            or q.paginate is not None
            or q.hybrid_search is not None
        ):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                "search cannot be combined with index, eq, range bounds, order, "
                "unique, first, count, filter, vector search, paginate, or hybrid search",
            )
        # Shared validation prologue — mirrors the server's ``compile_search``
        # order and applies to BOTH modes: empty query text, then search-index
        # resolution (a btree name is not a search surface), then the
        # snippet+trgm rejection (a snippet needs a tsquery tree to highlight),
        # then filter shape.
        if not q.search.query.strip():
            raise RtDbError(ErrorCode.BAD_REQUEST, "search query text must not be empty")
        search_def = next(
            (i for i in table_def.indexes if i.name == q.search.index and i.search), None
        )
        if search_def is None:
            raise RtDbError(ErrorCode.BAD_REQUEST, f"search index '{q.search.index}' not found")
        if q.search.snippet is True and q.search.mode == "trgm":
            raise RtDbError(ErrorCode.BAD_REQUEST, "snippet is only supported in tsquery mode")
        if q.search.filter is not None:
            _validate_filter(q.search.filter, table_def)
        candidates: list[StoredRow] = [
            row for (t, _id), row in self._docs.items() if t == q.table and _is_live(row)
        ]
        if q.search.filter is not None:
            candidates = [
                row
                for row in candidates
                if _eval_filter_expr(q.search.filter, row.doc, table_def.fields)
            ]
        if q.search.mode == "trgm":
            return self._execute_trgm_search(q, search_def, candidates)
        return self._execute_tsquery_search(q, search_def, candidates)

    def _execute_tsquery_search(
        self, q: Query, search_def: IndexDef, candidates: list[StoredRow]
    ) -> list[dict[str, Any]]:
        """``search`` terminal, default ``tsquery`` mode (FM-31): websearch match.

        Approximates the server's ``tsvector @@ websearch_to_tsquery`` over the
        index's fields: exact case-insensitive word equality (stemming is not
        modeled — ``"running"`` does not match ``runs``), phrases as contiguous
        word runs, ``or`` as a union of AND-segments, ``-term`` as exclusion.
        Ranking (``ts_rank``) is not modeled, so hits return in insertion
        order and ``take`` is not applied (the matched set is a sound
        superset of any server top-N). ``snippet=True`` attaches a
        ``_searchSnippet`` to each hit — a ≤35-word excerpt with matched words
        wrapped in ``<mark>`` (the ``ts_headline`` stand-in; the server's
        MaxWords=35/MinWords=15 bounds are approximated by the word cap). The
        caller's shared prologue already rejected an empty query, resolved the
        search index, and rejected ``snippet`` + ``trgm``.
        """
        assert q.search is not None  # caller dispatches only in the tsquery arm
        parsed = _parse_websearch(q.search.query)
        snippet = q.search.snippet is True
        out: list[dict[str, Any]] = []
        for row in candidates:
            words = _search_field_words(search_def, row.doc)
            if not _websearch_matches(parsed, words):
                continue
            doc = _merge_doc(row)
            if snippet:
                doc["_searchSnippet"] = _websearch_snippet(parsed, words)
            out.append(doc)
        return out

    def _execute_trgm_search(
        self, q: Query, search_def: IndexDef, candidates: list[StoredRow]
    ) -> list[dict[str, Any]]:
        """``search`` terminal, ``mode="trgm"`` (FM-30): substring matching.

        Case-insensitive substring containment over the search index's declared
        fields — the stand-in for the server's ``ILIKE '%query%'``. Ranking is
        the deterministic similarity pinned across the client harnesses: a doc
        scores ``len(query) / len(field)`` on each field whose lowercased value
        contains the lowercased query (a shorter containing field is a closer
        match) and ranks by its best field, mirroring the server's
        ``GREATEST(similarity(...))`` shape. Ties break ``created_at`` desc
        then ``id`` desc. ``take`` (capped to ``MAX_TAKE``) limits the result.
        Non-string field values never contain the query. The caller's shared
        prologue already rejected an empty query and resolved the search
        index (``search_def``).
        """
        assert q.search is not None  # caller dispatches only in the trgm arm
        needle = q.search.query.lower()
        scored: list[tuple[float, StoredRow]] = []
        for row in candidates:
            best: float | None = None
            for field in search_def.fields:
                value = row.doc.get(field)
                if not isinstance(value, str):
                    continue
                text = value.lower()
                if needle in text:
                    # An empty field cannot contain a non-empty query; keep the
                    # empty-field case to a finite 0.0 score (rust harness parity).
                    score = len(needle) / len(text) if text else 0.0
                    if best is None or score > best:
                        best = score
            if best is not None:
                scored.append((best, row))

        def cmp(a: tuple[float, StoredRow], b: tuple[float, StoredRow]) -> int:
            (sa, ra), (sb, rb) = a, b
            c = (sa < sb) - (sa > sb)
            if c != 0:
                return c
            c = (ra.created_at < rb.created_at) - (ra.created_at > rb.created_at)
            if c != 0:
                return c
            return (ra.id < rb.id) - (ra.id > rb.id)

        scored.sort(key=cmp_to_key(cmp))
        limit = q.take if q.take is not None else MAX_TAKE
        return [_merge_doc(row) for _score, row in scored[:limit]]

    def _execute_hybrid_search_terminal(
        self, q: Query, eq: list[Any], has_range: bool
    ) -> list[Any]:
        """``hybridSearch`` terminal.

        Lift of the former inline ``if q.hybrid_search is not None:`` arm of
        :meth:`run_query`; mirrors ``ts-client``'s ``executeHybridSearchTerminal``.
        Standalone like ``vectorSearch``: rejects every peer. RRF ranking is not
        modeled in-memory, so a valid (peer-free) hybridSearch returns an empty
        list (the sound stub — the combination guards the server enforces are
        still exercised).
        """
        assert q.hybrid_search is not None  # caller dispatches only when set
        if (
            q.index is not None
            or eq
            or has_range
            or q.order is not None
            or q.unique
            or q.first
            or q.count
            or q.distinct
            or q.aggregate is not None
            or q.paginate is not None
            or q.filter is not None
            or q.search is not None
            or q.vector_search is not None
            or q.take is not None
        ):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                "hybridSearch cannot be combined with any other terminal",
            )
        return []

    def _execute_count_terminal(self, filtered: list[StoredRow]) -> int:
        """``count`` terminal: number of matching rows.

        Lift of the former inline ``if count:`` arm of :meth:`run_query`; mirrors
        ``ts-client``'s ``executeCountTerminal``.
        """
        return len(filtered)

    def _execute_distinct_terminal(
        self,
        table_def: TableDef,
        index_def: IndexDef | None,
        typed_eq: list[Any],
        filtered: list[StoredRow],
    ) -> list[Any]:
        """``distinct`` terminal: unique values of the index field immediately
        after the eq prefix over the matching set, sorted ascending, capped by
        ``MAX_TAKE``. Absent field values appear as one ``None`` entry sorted
        last — the server's ``SELECT DISTINCT to_jsonb("<col>")`` keeps a
        single NULL row and ``ORDER BY v`` defaults to NULLS LAST.

        Lift of the former inline ``if q.distinct:`` arm of :meth:`run_query`;
        mirrors ``ts-client``'s ``executeDistinctTerminal``.
        """
        if index_def is None or len(typed_eq) >= len(index_def.fields):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                "distinct requires an index field beyond the eq prefix",
            )
        field = index_def.fields[len(typed_eq)]
        field_pg = _pg_for_field(table_def, field)
        seen: set[str] = set()
        distinct_values: list[Any] = []
        for row in filtered:
            v = row.doc.get(field)
            key = _dedupe_key(v)
            if key not in seen:
                seen.add(key)
                distinct_values.append(v)
        distinct_values.sort(key=cmp_to_key(lambda a, b: _compare_index_values(a, b, field_pg)))
        return distinct_values[:MAX_TAKE]

    def _execute_aggregate_terminal(
        self,
        q: Query,
        table_def: TableDef,
        index_def: IndexDef | None,
        typed_eq: list[Any],
        filtered: list[StoredRow],
    ) -> Any:
        """``aggregate`` terminal: ``<OP>`` over the index field after the eq
        prefix (``groupBy``: group by that field, aggregate the next).

        ``count`` aggregates rows, not a field — it consumes no aggregate index
        field (a scalar ``count`` needs no index at all; a grouped ``count``
        needs one index field beyond the eq prefix to group by). Null agg
        values are skipped for the field-bearing ops (SQL ``NULL`` semantics);
        an empty scalar set -> ``None`` (``count`` -> ``0``); groups are ordered
        by key asc and capped by ``MAX_TAKE``.

        Lift of the former inline ``if q.aggregate is not None:`` arm of
        :meth:`run_query`; mirrors ``ts-client``'s ``executeAggregateTerminal``.
        """
        agg = q.aggregate
        assert agg is not None  # caller dispatches only when set
        eq_len = len(typed_eq)
        # `count` aggregates rows and consumes no aggregate field.
        needs_field = agg.op != AggregateOp.COUNT

        # Resolve the group field: groupBy always needs one index field
        # beyond the eq prefix.
        if agg.group_by:
            if index_def is None or eq_len >= len(index_def.fields):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    "aggregate groupBy requires an index field beyond the eq prefix",
                )
            group_field = index_def.fields[eq_len]
        else:
            group_field = None

        # Resolve the aggregate field (count consumes none; the rest need
        # one beyond the eq prefix, or two when grouped).
        if needs_field:
            if index_def is None:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    "aggregate requires an index field beyond the eq prefix",
                )
            if agg.group_by:
                if eq_len + 1 >= len(index_def.fields):
                    raise RtDbError(
                        ErrorCode.BAD_REQUEST,
                        "aggregate groupBy requires two index fields beyond the eq prefix",
                    )
                agg_field = index_def.fields[eq_len + 1]
            else:
                if eq_len >= len(index_def.fields):
                    raise RtDbError(
                        ErrorCode.BAD_REQUEST,
                        "aggregate requires an index field beyond the eq prefix",
                    )
                agg_field = index_def.fields[eq_len]
            agg_pg = _pg_for_field(table_def, agg_field)
            if agg.op in (AggregateOp.SUM, AggregateOp.AVG) and agg_pg not in (_NUMBER, _INT64):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"aggregate op {agg.op} requires a numeric index field",
                )
        else:
            agg_field = None
            agg_pg = _TEXT  # unused for count

        if group_field is not None:
            group_pg = _pg_for_field(table_def, group_field)
            groups: list[tuple[Any, list[Any]]] = []
            group_index: dict[str, int] = {}
            for row in filtered:
                k = row.doc.get(group_field)
                if k is None:
                    continue
                key = _dedupe_key(k)
                i = group_index.get(key)
                if i is None:
                    i = len(groups)
                    group_index[key] = i
                    groups.append((k, []))
                if agg_field is not None:
                    av = row.doc.get(agg_field)
                    if av is not None:
                        groups[i][1].append(av)
                else:
                    # count: every row in the group counts (COUNT(*)).
                    groups[i][1].append(1)
            if agg.op == AggregateOp.COUNT:
                out: list[dict[str, Any]] = [{"key": k, "value": len(vs)} for k, vs in groups]
            else:
                out = [
                    {"key": k, "value": _apply_aggregate(agg.op, vs, agg_pg) if vs else None}
                    for k, vs in groups
                ]
            out.sort(
                key=cmp_to_key(lambda a, b: _compare_index_values(a["key"], b["key"], group_pg))
            )
            return out[:MAX_TAKE]
        # Scalar path: count returns the matching-row count (0 if none);
        # the field-bearing ops reduce their non-null agg values (None if empty).
        if agg.op == AggregateOp.COUNT:
            return len(filtered)
        assert agg_field is not None  # needs_field is True for every non-count op
        agg_values = [row.doc.get(agg_field) for row in filtered]
        agg_values = [v for v in agg_values if v is not None]
        if not agg_values:
            return None
        return _apply_aggregate(agg.op, agg_values, agg_pg)

    def _execute_collect_terminal(self, q: Query, filtered: list[StoredRow]) -> Any:
        """``unique`` / ``first`` / plain ``collect`` terminal over the sorted
        matching set.

        ``unique`` returns the single match or raises ``PRECONDITION_FAILED``
        when more than one matches (and ``None`` when none match). ``first``
        returns the head match or ``None``. The plain path returns the first
        ``take`` rows (default ``MAX_TAKE``).

        Lift of the former inline trailing tail of :meth:`run_query` (the
        ``if unique: … if first: … return [...]`` block); mirrors
        ``ts-client``'s ``executeCollectTerminal``.
        """
        if q.unique:
            if len(filtered) > 1:
                raise RtDbError(
                    ErrorCode.PRECONDITION_FAILED,
                    "unique query matched multiple documents",
                )
            return _merge_doc(filtered[0]) if filtered else None
        if q.first:
            return _merge_doc(filtered[0]) if filtered else None

        limit = q.take if q.take is not None else MAX_TAKE
        return [_merge_doc(row) for row in filtered[:limit]]

    def _execute_paginate_terminal(
        self,
        paginate: Any,
        table_def: TableDef,
        filtered: list[StoredRow],
        index_def: IndexDef | None,
        typed_eq: list[Any],
        direction: str,
    ) -> Any:
        """``paginate`` terminal: keyset-cursor paging over the sorted set.

        The sort columns mirror the producing sort (unbound index fields
        after the eq prefix, then ``createdAt``, then ``id``); the cursor
        encodes one value per column."""
        unbound_fields: list[str] = (
            index_def.fields[len(typed_eq) :] if index_def is not None else []
        )
        sort_cols: list[tuple[str, str | None]] = [
            *[("index", f) for f in unbound_fields],
            ("createdAt", None),
            ("id", None),
        ]
        col_types: list[_PgType] = [
            _pg_for_field(table_def, fld)
            if kind == "index" and fld is not None
            else (_NUMBER if kind == "createdAt" else _TEXT)
            for kind, fld in sort_cols
        ]
        return _paginate_result(paginate, table_def, filtered, sort_cols, col_types, direction)

    def _fetch_filtered_rows(
        self, q: Query, plan: _ScanPlan, fields: Mapping[str, Any]
    ) -> list[StoredRow]:
        """Row fetch + filter (eq prefix -> range -> filter hook) over
        ``self._docs``. FM-33: soft-deleted rows are absent to every read
        terminal (the server's ``compile_scan_where`` ``deleted_at IS NULL``
        literal). ``fields`` is the table's declared field map — the filter
        evaluator's typed-int64 arm keys off it (ENH-027)."""
        index_def = plan.index_def
        typed_eq = plan.typed_eq
        range_field = plan.range_field
        range_pg = plan.range_pg
        gt, gte, lt, lte = plan.gt, plan.gte, plan.lt, plan.lte
        # Row fetch + filter (eq prefix -> range -> filter hook). FM-33:
        # soft-deleted rows are absent to every read terminal (the server's
        # `compile_scan_where` `deleted_at IS NULL` literal).
        filtered: list[StoredRow] = []
        for (t, _id), row in self._docs.items():
            if t != q.table:
                continue
            if not _is_live(row):
                continue
            if index_def is not None:
                ok = True
                for i, tv in enumerate(typed_eq):
                    rv = row.doc.get(index_def.fields[i])
                    if rv is None or rv != tv:
                        ok = False
                        break
                if not ok:
                    continue
            if range_field is not None:
                v = row.doc.get(range_field)
                if v is None:
                    continue
                if gt is not None and _compare_index_values(v, gt, range_pg) <= 0:
                    continue
                if gte is not None and _compare_index_values(v, gte, range_pg) < 0:
                    continue
                if lt is not None and _compare_index_values(v, lt, range_pg) >= 0:
                    continue
                if lte is not None and _compare_index_values(v, lte, range_pg) > 0:
                    continue
            if q.filter is not None and not _eval_filter_expr(q.filter, row.doc, fields):
                continue
            filtered.append(row)
        return filtered
