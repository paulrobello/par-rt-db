"""In-memory par-rt-db client for unit tests. No network, no Postgres; mirrors
server DSL/step-result/system-field semantics. Ports ``rust-client/src/in_memory.rs``
(and through it ``ts-client/src/in_memory.ts``).

The server (``server/src/{txn,query,schema,protocol}.rs``) is the source of truth
for the declarative DSL, step-result shapes, system fields, and query semantics;
this module mirrors them so app code can exercise query/txn/schema behavior with
no network and no live Postgres. It exposes the same data surface as the live
clients — :meth:`push_schema`, :meth:`run_query` (one-shot), :meth:`mutate`
(transactions), :meth:`subscribe` (reactive ``queryUpdate``-style results), and
:meth:`tick` (advance scheduled jobs) — so a test can swap it in behind a shared
interface.

Parity is deliberately scoped to the documented core (schema push, insert /
patch / replace / delete / expectVersion / expectAbsent / upsert, point reads,
index eq + range queries with order/take/unique/first/count, ``distinct`` and
``aggregate`` (scalar + grouped), filter expressions, keyset-cursor pagination,
reactive subscriptions, and scheduled-job ``tick``). FM-33 semantics are
mirrored end-to-end: ``onDelete`` cascades (``cascade``/``restrict``/
``setNull``, recursive, cycle-guarded, row-budgeted), ``softDelete`` tables
(delete stamps a ``deleted_at`` tombstone that every read and write lookup
filters; undelete restores; unique indexes ignore soft-deleted rows; the TTL
reaper always hard-deletes), and push-time ``onDelete`` validation at the
server's own rule depth.
``vectorSearch``/``hybridSearch``/``search`` apply their optional ``filter``:
``vectorSearch`` treats every table row as a candidate (the sound
over-approximation — it does not rank by vector similarity), ``hybridSearch``
returns an empty list after the same combination guards the server enforces,
``search`` in ``trgm`` mode (FM-30) matches for real — case-insensitive
substring containment over the index's fields, ranked by the pinned
``len(query) / len(field)`` similarity — and ``search`` in the default
``tsquery`` mode (FM-31) approximates the server's
``websearch_to_tsquery`` match: quoted phrases require word adjacency
(case-insensitive), the bare word ``or`` unions alternatives, ``-term``
excludes, and other terms are ANDed (stemming and ``ts_rank`` are not
modeled — see :func:`_parse_websearch`). ``snippet=True`` (FM-31) attaches a
``_searchSnippet`` excerpt (≤35 words, matched terms wrapped in ``<mark>``)
to each hit, and is rejected with ``mode="trgm"``. Both modes share the
server's ``compile_search`` validation prologue: empty query text and
non-search index names are rejected.

Simplifications vs. the live server (be explicit when relying on these):

* Cron validation is deferred to the live server; the harness accepts any
  expression and re-arms crons by a fixed :data:`CRON_STEP_MS` interval (it does
  not parse 5-field cron). One-shots catch up if past due; crons skip missed
  windows (they do not backfill).
* Storage is an in-memory ``bytes`` map; :meth:`get_url` returns a synthetic
  ``memory://`` handle (there is no real byte stream to serve).
* Subscription callbacks fire inline on the writing thread; never recursively
  mutate the same client from inside a callback.
* Unsubscription is explicit (:meth:`SubscriptionHandle.unsubscribe`) or via a
  context manager (``with client.subscribe(...) as sub:``). The Rust RAII
  "dropping the handle unsubscribes" idiom is not relied on here — Python's GC
  is not deterministic, so prefer the explicit form.

ARC-201: this module is now the assembly point of the ``in_memory/``
package (mirroring the rust-client's layout). The former ``in_memory.py``
monolith is decomposed into ``store.py`` (client core), ``query.py`` (the
``run_query`` dispatcher + per-terminal executors), ``migrate.py``
(per-directive migration engine), and ``validate.py`` (filter
validation/evaluation). ``InMemoryRtDbClient`` is assembled here from the
engine mixins over the store core, and the former module surface is
re-exported unchanged.
"""

from __future__ import annotations

from .migrate import (
    _cast_valid_for as _cast_valid_for,
)
from .migrate import (
    _coerce_value as _coerce_value,
)
from .migrate import (
    _detect_destructive_changes as _detect_destructive_changes,
)
from .migrate import (
    _field_has_nested_on_delete as _field_has_nested_on_delete,
)
from .migrate import (
    _field_type_signature as _field_type_signature,
)
from .migrate import (
    _is_widening_of as _is_widening_of,
)
from .migrate import (
    _literal_set as _literal_set,
)
from .migrate import (
    _migrate_table_mut as _migrate_table_mut,
)
from .migrate import _MigrateEngine
from .migrate import (
    _on_delete_ref as _on_delete_ref,
)
from .migrate import (
    _strip_on_delete_keys as _strip_on_delete_keys,
)
from .migrate import (
    _validate_on_delete as _validate_on_delete,
)
from .migrate import (
    _vector_signature as _vector_signature,
)
from .migrate import (
    _where_signature as _where_signature,
)
from .query import (
    _WEBSEARCH_SNIPPET_MAX_WORDS as _WEBSEARCH_SNIPPET_MAX_WORDS,
)
from .query import (
    MAX_TAKE as MAX_TAKE,
)
from .query import (
    _apply_aggregate as _apply_aggregate,
)
from .query import (
    _dedupe_key as _dedupe_key,
)
from .query import (
    _dir_order as _dir_order,
)
from .query import (
    _is_after_cursor as _is_after_cursor,
)
from .query import (
    _is_number as _is_number,
)
from .query import (
    _paginate_result as _paginate_result,
)
from .query import (
    _parse_websearch as _parse_websearch,
)
from .query import _QueryEngine
from .query import (
    _search_field_words as _search_field_words,
)
from .query import (
    _sort_value as _sort_value,
)
from .query import (
    _to_numeric as _to_numeric,
)
from .query import (
    _validate_cursor_values as _validate_cursor_values,
)
from .query import (
    _Websearch as _Websearch,
)
from .query import (
    _websearch_matches as _websearch_matches,
)
from .query import (
    _websearch_snippet as _websearch_snippet,
)
from .query import (
    _websearch_tokens as _websearch_tokens,
)
from .query import (
    _websearch_unit_in as _websearch_unit_in,
)
from .store import (
    _BOOLEAN as _BOOLEAN,
)
from .store import (
    _DEFAULT_STEP_RETRY as _DEFAULT_STEP_RETRY,
)
from .store import (
    _EXPECTED_INDEX_VALUE as _EXPECTED_INDEX_VALUE,
)
from .store import (
    _INT64 as _INT64,
)
from .store import (
    _NUMBER as _NUMBER,
)
from .store import (
    _STEP_RESULT as _STEP_RESULT,
)
from .store import (
    _TEXT as _TEXT,
)
from .store import (
    CRON_STEP_MS as CRON_STEP_MS,
)
from .store import (
    MAX_AFFECTED_ROWS_PER_TXN as MAX_AFFECTED_ROWS_PER_TXN,
)
from .store import (
    MAX_BY_QUERY_ROWS as MAX_BY_QUERY_ROWS,
)
from .store import (
    MAX_BY_QUERY_STEPS_PER_TXN as MAX_BY_QUERY_STEPS_PER_TXN,
)
from .store import (
    MAX_CASCADE_ROWS as MAX_CASCADE_ROWS,
)
from .store import (
    MAX_STEPS as MAX_STEPS,
)
from .store import (
    MAX_WORKFLOW_STEPS as MAX_WORKFLOW_STEPS,
)
from .store import (
    FileMetadata as FileMetadata,
)
from .store import (
    InMemoryRtDbClientOptions as InMemoryRtDbClientOptions,
)
from .store import (
    PresenceRooms as PresenceRooms,
)
from .store import (
    PresenceTestHandle as PresenceTestHandle,
)
from .store import (
    StoredBlob as StoredBlob,
)
from .store import (
    StoredRow as StoredRow,
)
from .store import (
    SubscriptionHandle as SubscriptionHandle,
)
from .store import (
    UploadResult as UploadResult,
)
from .store import (
    _apply_defaults as _apply_defaults,
)
from .store import (
    _base36 as _base36,
)
from .store import (
    _cancel_schedule_result as _cancel_schedule_result,
)
from .store import (
    _cancel_workflow_result as _cancel_workflow_result,
)
from .store import (
    _canonical as _canonical,
)
from .store import (
    _coerce_index_value as _coerce_index_value,
)
from .store import (
    _collect_index_key as _collect_index_key,
)
from .store import (
    _compare_index_values as _compare_index_values,
)
from .store import (
    _count_steps as _count_steps,
)
from .store import (
    _delete_by_query_result as _delete_by_query_result,
)
from .store import (
    _index_column_type as _index_column_type,
)
from .store import (
    _IndexedType as _IndexedType,
)
from .store import _InMemoryStoreCore
from .store import (
    _insert_result as _insert_result,
)
from .store import (
    _is_live as _is_live,
)
from .store import (
    _merge_doc as _merge_doc,
)
from .store import (
    _optional_rejects_null as _optional_rejects_null,
)
from .store import (
    _parse_i64 as _parse_i64,
)
from .store import (
    _patch_by_query_result as _patch_by_query_result,
)
from .store import (
    _pg_for_field as _pg_for_field,
)
from .store import (
    _PgType as _PgType,
)
from .store import (
    _require_index as _require_index,
)
from .store import (
    _schedule_info as _schedule_info,
)
from .store import (
    _schedule_result as _schedule_result,
)
from .store import (
    _ScheduledJob as _ScheduledJob,
)
from .store import (
    _sha256_hex as _sha256_hex,
)
from .store import (
    _start_workflow_result as _start_workflow_result,
)
from .store import (
    _strip_unset_optionals as _strip_unset_optionals,
)
from .store import (
    _Subscription as _Subscription,
)
from .store import (
    _to_float as _to_float,
)
from .store import (
    _upsert_result as _upsert_result,
)
from .store import (
    _validate_workflow_spec as _validate_workflow_spec,
)
from .store import (
    _wall_now as _wall_now,
)
from .store import (
    _workflow_info as _workflow_info,
)
from .store import (
    _WorkflowRun as _WorkflowRun,
)
from .store import (
    apply_patch as apply_patch,
)
from .store import (
    is_base64_string as is_base64_string,
)
from .store import (
    is_hex_id as is_hex_id,
)
from .store import (
    is_int64_string as is_int64_string,
)
from .store import (
    validate_doc as validate_doc,
)
from .store import (
    validate_value as validate_value,
)
from .store import (
    worst_case_affected as worst_case_affected,
)
from .validate import (
    _check_leaf_value as _check_leaf_value,
)
from .validate import (
    _compare_leaf as _compare_leaf,
)
from .validate import (
    _compare_values as _compare_values,
)
from .validate import (
    _doc_to_number as _doc_to_number,
)
from .validate import (
    _doc_to_text as _doc_to_text,
)
from .validate import (
    _eval_filter_expr as _eval_filter_expr,
)
from .validate import (
    _in_value_kind as _in_value_kind,
)
from .validate import (
    _validate_filter as _validate_filter,
)


class InMemoryRtDbClient(_QueryEngine, _MigrateEngine, _InMemoryStoreCore):
    """In-memory par-rt-db client for unit tests.

    Construct with :class:`InMemoryRtDbClientOptions` (defaults: system clock,
    constant ``0.5`` RNG), then :meth:`push_schema` a schema and drive it with
    :meth:`run_query` / :meth:`mutate` / :meth:`subscribe` / :meth:`tick`."""

    pass


# Suppress an unused-import warning for `field` (re-exported for parity with the
# Rust harness's public surface; not used internally).
__all__ = [
    "CRON_STEP_MS",
    "FileMetadata",
    "InMemoryRtDbClient",
    "InMemoryRtDbClientOptions",
    "MAX_STEPS",
    "MAX_TAKE",
    "StoredBlob",
    "StoredRow",
    "SubscriptionHandle",
    "UploadResult",
    "apply_patch",
    "is_base64_string",
    "is_hex_id",
    "is_int64_string",
    "validate_doc",
    "validate_value",
]
