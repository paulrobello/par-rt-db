"""Schema-migration engine for the in-memory harness (mirrors
``rust-client/src/in_memory/migrate.rs``): destructive-change detection,
``onDelete`` push validation, and the migration-directive interpreter —
``_apply_migration_directive`` dispatching to one function per directive
kind."""

from __future__ import annotations

import math
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from pydantic import TypeAdapter

from ..errors import ErrorCode, RtDbError
from ..migration import (
    Cast,
    Directive,
    _ChangeType,
    _DropField,
    _DropIndex,
    _DropTable,
    _EvalExpr,
    _RenameField,
    _RenameTable,
    _SetDefault,
)
from ..schema import (
    SchemaDef,
    TableDef,
    _FArray,
    _FBoolean,
    _FId,
    _FInt64,
    _FLiteral,
    _FNumber,
    _FObject,
    _FOptional,
    _FRecord,
    _FString,
    _FUnion,
)
from ..value_expr import ValueExpr
from ..wire import FilterExpr
from .value_expr import _validate_computed, walk_value_expr_fields

if TYPE_CHECKING:
    from .store import _InMemoryStoreCore as _Core
else:
    _Core = object


def _migrate_table_mut(schema: SchemaDef, table: str) -> TableDef:
    """Return a mutable reference to ``table`` within ``schema``, or raise
    ``BAD_REQUEST`` if the table does not exist. Mirrors the server's
    ``migrate::table_mut``."""
    t = schema.tables.get(table)
    if t is None:
        raise RtDbError(ErrorCode.BAD_REQUEST, f"table '{table}' does not exist")
    return t


def _cast_valid_for(cast: Cast, old_ty: Any) -> bool:
    """Mirror of ``server::migrate::cast_valid_for`` — true if ``cast`` can
    coerce from the ``old_ty`` field type."""
    if cast == Cast.TO_STRING:
        return isinstance(old_ty, (_FString, _FNumber, _FBoolean, _FInt64))
    if cast == Cast.TO_NUMBER:
        return isinstance(old_ty, (_FString, _FBoolean, _FInt64))
    if cast == Cast.TO_INT64:
        return isinstance(old_ty, (_FString, _FNumber))
    if cast == Cast.TO_BOOLEAN:
        return isinstance(old_ty, (_FString, _FNumber))
    return False


def _coerce_value(cast: Cast, v: Any) -> Any:
    """Pure-Python coercion mirroring ``server::migrate::coerce_value`` (and
    ``rust-client::in_memory::coerce_value``). Returns the coerced value or
    ``None`` if the value cannot be coerced under this cast.

    ``ToInt64`` emits a decimal-string (int64 travels as a canonical decimal
    string on the wire — see ``schema::is_valid_int64`` and
    ``FEATURE_MATRIX.md`` #13); ``ToNumber`` emits a ``float``; the others
    produce the natural Python representation.
    """
    if cast == Cast.TO_STRING:
        if isinstance(v, str):
            return v
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (int, float)):
            return str(v)
        return None
    if cast == Cast.TO_NUMBER:
        if isinstance(v, bool):
            return 1.0 if v else 0.0
        if isinstance(v, (int, float)):
            return float(v)
        if isinstance(v, str):
            try:
                f = float(v)
            except ValueError:
                return None
            if not math.isfinite(f):
                return None
            return f
        return None
    if cast == Cast.TO_INT64:
        # ``bool`` is a subclass of ``int`` — check it first and reject (mirrors
        # Rust's ``Value::Bool`` falling through to ``_ => None``).
        if isinstance(v, bool):
            return None
        # i64 range: the server's ``i64::from_str`` / ``Number::as_i64`` reject
        # outside [-(2**63), 2**63), but Python ints are arbitrary-precision, so
        # bound explicitly to keep parity (else a huge int silently "coerces").
        i64_min, i64_max = -(2**63), 2**63 - 1
        if isinstance(v, int):
            if not i64_min <= v <= i64_max:
                return None
            return str(v)
        if isinstance(v, float):
            if not v.is_integer():
                return None
            iv = int(v)
            if not i64_min <= iv <= i64_max:
                return None
            return str(iv)
        if isinstance(v, str):
            try:
                iv = int(v)
            except ValueError:
                return None
            if not i64_min <= iv <= i64_max:
                return None
            return str(iv)
        return None
    if cast == Cast.TO_BOOLEAN:
        if isinstance(v, str):
            if v in ("true", "1"):
                return True
            if v in ("false", "0"):
                return False
            return None
        # ``bool`` is a subclass of ``int`` — check it first and reject
        # (``ToBoolean`` only accepts String and Number source types).
        if isinstance(v, bool):
            return None
        if isinstance(v, (int, float)):
            return v != 0.0
        return None
    return None


def _detect_destructive_changes(old: SchemaDef, new: SchemaDef) -> None:
    """Mirror of ``server/src/ddl.rs::detect_destructive_changes``: reject any
    removed table, removed/changed field, or removed/changed index with
    ``BAD_REQUEST``. Additive changes (new tables/fields/indexes) pass through."""
    for table_name, old_table in old.tables.items():
        new_table = new.tables.get(table_name)
        if new_table is None:
            raise RtDbError(ErrorCode.BAD_REQUEST, f"removed table '{table_name}'")
        for field_name, old_field_type in old_table.fields.items():
            new_field_type = new_table.fields.get(field_name)
            if new_field_type is None:
                raise RtDbError(ErrorCode.BAD_REQUEST, f"removed field '{table_name}.{field_name}'")
            if _field_type_signature(old_field_type) != _field_type_signature(
                new_field_type
            ) and not _is_widening_of(old_field_type, new_field_type):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"changed type of field '{table_name}.{field_name}'",
                )
        for old_index in old_table.indexes:
            new_index = next((i for i in new_table.indexes if i.name == old_index.name), None)
            if new_index is None:
                raise RtDbError(ErrorCode.BAD_REQUEST, f"removed index '{old_index.name}'")
            if new_index.fields != old_index.fields:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"changed fields of index '{old_index.name}'",
                )
            if bool(new_index.search) != bool(old_index.search):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"changed kind of index '{old_index.name}' (btree <-> search)",
                )
            if _vector_signature(new_index.vector) != _vector_signature(old_index.vector):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"changed vector spec of index '{old_index.name}'",
                )
            if bool(new_index.unique) != bool(old_index.unique):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"changed uniqueness of index '{old_index.name}'",
                )
            if _where_signature(new_index.where) != _where_signature(old_index.where):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"changed partial predicate of index '{old_index.name}'",
                )
            if (new_index.language or None) != (old_index.language or None):
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"changed language of search index '{old_index.name}'",
                )


def _field_type_signature(ty: Any) -> Any:
    """Structural signature of a ``FieldType`` for destructive-change detection
    (the live server compares the parsed type tree directly). FM-33: ``onDelete``
    is stripped first — the server's ``strip_on_delete`` — so adding or changing
    the action is additive, while the referenced ``table`` still participates
    (changing it remains a type change)."""
    return _strip_on_delete_keys(ty.model_dump(mode="json"))


def _strip_on_delete_keys(dumped: Any) -> Any:
    """Recursively drop every ``onDelete`` key from a dumped field-type tree —
    the wire-dump equivalent of server ``schema::strip_on_delete`` (the action
    is behavior, not storage shape). Both the alias (``onDelete``) and the
    field name (``on_delete``) are stripped: ``model_dump(mode="json")``
    without ``by_alias`` emits field names."""
    if isinstance(dumped, dict):
        return {
            k: _strip_on_delete_keys(v)
            for k, v in dumped.items()
            if k not in ("onDelete", "on_delete")
        }
    if isinstance(dumped, list):
        return [_strip_on_delete_keys(v) for v in dumped]
    return dumped


def _on_delete_ref(ty: Any, parent_table: str) -> str | None:
    """The ``onDelete`` action ``ty`` declares when it references
    ``parent_table``, or ``None`` when the type is not an id/optional-id
    pointing at it (or declares no action). Port of server
    ``txn::on_delete_ref``; push validation guarantees an onDelete-bearing id
    appears only at the top level or directly under one ``Optional``, so this
    two-shape walk is exhaustive."""
    match ty:
        case _FId(table=table, on_delete=action) if table == parent_table:
            return action
        case _FOptional(inner=inner):
            return _on_delete_ref(inner, parent_table)
        case _:
            return None


def _field_has_nested_on_delete(ty: Any) -> bool:
    """Whether any id variant reachable through the type's compositors carries
    an ``onDelete`` action — used to reject actions nested deeper than the two
    legal top-level shapes. Port of server ``field_has_nested_on_delete``."""
    match ty:
        case _FId(on_delete=action):
            return action is not None
        case _FOptional(inner=inner) | _FArray(element=inner) | _FRecord(value=inner):
            return _field_has_nested_on_delete(inner)
        case _FUnion(variants=variants):
            return any(_field_has_nested_on_delete(v) for v in variants)
        case _FObject(fields=fields):
            return any(_field_has_nested_on_delete(v) for v in fields.values())
        case _:
            return False


def _validate_on_delete(schema: SchemaDef) -> None:
    """FM-33 push-time ``onDelete`` validation — the port of server
    ``schema.rs::validate_on_delete`` plus the second referenced-table pass in
    ``SchemaDef::validate``, with the server's exact ``SCHEMA_VIOLATION``
    messages. An action is legal only on a TOP-LEVEL field, in one of two
    shapes: ``id{table, onDelete}`` or ``optional{id{table, onDelete}}``
    (nested deeper there is no well-defined "the ref field" to index or null).
    ``setNull`` additionally requires the ``Optional`` wrapper, the field needs
    a single-field, non-unique, non-partial btree index on it (the cascade
    lookup must be an index scan; a partial ``where`` could hide children), and
    the referenced table must be declared. Self-reference is legal."""
    for table_name, table_def in schema.tables.items():
        for field_name, field_type in table_def.fields.items():
            action: str | None
            is_optional: bool
            match field_type:
                case _FId(on_delete=a):
                    action, is_optional = a, False
                case _FOptional(inner=_FId(on_delete=a)):
                    action, is_optional = a, True
                case _:
                    if _field_has_nested_on_delete(field_type):
                        raise RtDbError(
                            ErrorCode.SCHEMA_VIOLATION,
                            f"field '{field_name}' on table '{table_name}': onDelete is legal "
                            "only on a top-level id or optional-id field",
                        )
                    continue
            if action is None:
                continue
            if action == "setNull" and not is_optional:
                raise RtDbError(
                    ErrorCode.SCHEMA_VIOLATION,
                    f"field '{field_name}' on table '{table_name}': onDelete 'setNull' "
                    "requires the id field to be optional",
                )
            has_ref_index = any(
                not idx.search
                and idx.vector is None
                and not idx.unique
                and idx.where is None
                and len(idx.fields) == 1
                and idx.fields[0] == field_name
                for idx in table_def.indexes
            )
            if not has_ref_index:
                raise RtDbError(
                    ErrorCode.SCHEMA_VIOLATION,
                    f"onDelete field '{field_name}' on table '{table_name}' requires a "
                    "single-field, non-unique, non-partial btree index on it",
                )
    # Second pass (needs whole-schema access): every top-level onDelete id
    # field must reference a table declared in this schema.
    for table_name, table_def in schema.tables.items():
        for field_name, field_type in table_def.fields.items():
            ref_table: str | None = None
            match field_type:
                case _FId(table=ref, on_delete=action) if action is not None:
                    ref_table = ref
                case _FOptional(inner=_FId(table=ref, on_delete=action)) if action is not None:
                    ref_table = ref
            if ref_table is not None and ref_table not in schema.tables:
                raise RtDbError(
                    ErrorCode.SCHEMA_VIOLATION,
                    f"onDelete field '{field_name}' on table '{table_name}' references "
                    f"unknown table '{ref_table}'",
                )


def _literal_set(ty: Any) -> list[Any] | None:
    """Finite literal set carried by a ``_FLiteral`` or a ``_FUnion`` of pure
    literals — a port of ``server/src/schema.rs::literal_set``. Returns ``None``
    for anything that is not a finite literal set (scalars, optionals, objects,
    arrays, mixed or empty unions)."""
    match ty:
        case _FLiteral(value=v):
            return [v]
        case _FUnion(variants=variants):
            if not variants:
                return None
            out: list[Any] = []
            for variant in variants:
                match variant:
                    case _FLiteral(value=v):
                        out.append(v)
                    case _:
                        return None
            return out
        case _:
            return None


def _is_widening_of(old: Any, new: Any) -> bool:
    """``True`` iff ``new`` carries a finite literal set that is a superset of
    ``old``'s — a port of ``server/src/schema.rs::is_widening_of``. Lets
    ``pushSchema`` accept additive widening of a literal-union field (e.g.
    ``{a,b}`` -> ``{a,b,c}``, or ``"a"`` -> ``{a,b}``) as a non-destructive
    change."""
    old_vals = _literal_set(old)
    new_vals = _literal_set(new)
    if old_vals is None or new_vals is None:
        return False
    return all(any(o == n for n in new_vals) for o in old_vals)


def _vector_signature(spec: Any) -> Any:
    if spec is None:
        return None
    return spec.model_dump(mode="json")


def _where_signature(pred: Any) -> Any:
    """Structural signature of an ``IndexDef.where`` predicate (a
    ``FilterExpr``) for destructive-change detection — the live server compares
    the parsed ``FilterExpr`` tree directly."""
    if pred is None:
        return None
    return pred.model_dump(mode="json")


#: Revalidation adapter for a rewritten ``ValueExpr`` (the rename rewrite works
#: on the dumped wire tree, then re-parses through the discriminated union).
_VE_ADAPTER = TypeAdapter(ValueExpr)


def _rewrite_ve_field(d: Any, from_: str, to: str) -> Any:
    """Rename ``from_`` → ``to`` in one dumped ``ValueExpr`` tree: every
    ``field`` node's ``field``, and every ``case.whens[].when`` filter's leaf
    ``field`` (filter value positions are never touched — they carry values,
    not field references). Port of server ``migrate.rs::rename_value_expr_fields``."""
    if not isinstance(d, dict):
        return d
    op = d.get("op")
    if op == "field":
        return {"op": "field", "field": to if d.get("field") == from_ else d.get("field")}
    if op in ("literal", "now"):
        return d
    if op in ("concat", "coalesce"):
        return {**d, "parts": [_rewrite_ve_field(p, from_, to) for p in d.get("parts", [])]}
    if op in ("add", "sub", "mul", "div"):
        return {
            **d,
            "left": _rewrite_ve_field(d.get("left"), from_, to),
            "right": _rewrite_ve_field(d.get("right"), from_, to),
        }
    if op in ("lower", "upper", "trim", "cast"):
        return {**d, "value": _rewrite_ve_field(d.get("value"), from_, to)}
    if op == "case":
        return {
            **d,
            "whens": [
                {
                    "when": _rewrite_filter_fields(cw.get("when"), from_, to),
                    "then": _rewrite_ve_field(cw.get("then"), from_, to),
                }
                for cw in d.get("whens", [])
            ],
            "otherwise": _rewrite_ve_field(d.get("otherwise"), from_, to),
        }
    return d


def _rewrite_filter_fields(d: Any, from_: str, to: str) -> Any:
    """The ``FilterExpr`` half of the rename rewrite: leaf ``field`` keys move,
    ``and``/``or``/``not`` recurse, value positions (``value``/``values``) are
    left untouched."""
    if not isinstance(d, dict):
        return d
    op = d.get("op")
    if op in ("and", "or"):
        return {**d, "exprs": [_rewrite_filter_fields(e, from_, to) for e in d.get("exprs", [])]}
    if op == "not":
        return {**d, "expr": _rewrite_filter_fields(d.get("expr"), from_, to)}
    if isinstance(d.get("field"), str):
        return {**d, "field": to if d["field"] == from_ else d["field"]}
    return d


def _rename_value_expr_fields(expr: ValueExpr, from_: str, to: str) -> ValueExpr:
    """One computed entry with its ``field`` references (including
    ``case.when`` filter fields) rewritten from ``from_`` to ``to``."""
    dumped = expr.model_dump(by_alias=True, mode="json")
    return _VE_ADAPTER.validate_python(_rewrite_ve_field(dumped, from_, to))


_FILTER_ADAPTER = TypeAdapter(FilterExpr)


def _rename_filter_fields(expr: FilterExpr, from_: str, to: str) -> FilterExpr:
    """The ``authorize`` predicate's ``field`` references rewritten from
    ``from_`` to ``to``. Mirrors server ``migrate::rename_filter_fields``;
    reuses the same dict-level ``_rewrite_filter_fields`` the computed
    ``case.when`` rewrite uses."""
    dumped = expr.model_dump(by_alias=True, mode="json")
    return _FILTER_ADAPTER.validate_python(_rewrite_filter_fields(dumped, from_, to))


class _MigrateEngine(_Core):
    """``migrate_schema`` and the per-directive appliers over ``self._docs``."""

    def migrate_schema(
        self,
        directives: list[Directive],
        *,
        dry_run: bool = False,
    ) -> Any:
        """Apply (or preview) a declarative schema migration in-memory.

        Ports ``rust-client::InMemoryRtDbClient::migrate_schema`` and through it
        ``server::migrate::plan_migration`` + ``apply_migration``. Each directive
        is validated against the working schema fold and applied to the doc
        store. On the first failure the doc store is restored (``self._schema``
        was never touched — the fold lives in a local ``planned`` copy) and the
        error surfaces. On ``dry_run`` the full plan is validated and
        ``affected_rows`` reported against the derived schema, but nothing is
        committed (``applied: False``).

        ``evalExpr`` has no in-memory SQL engine and raises
        :class:`RtDbError` ``BAD_REQUEST`` — same convention as the
        search/vector stubs. ENH-020's typed ``ValueExpr`` path is also
        unsupported in-memory (no SQL engine to compile it to); both arms
        (typed and legacy raw-SQL) raise identically. Affected-rows counts
        mirror the server:
        ``renameField`` / ``setDefault`` / ``changeType`` / ``dropField`` count
        the rows whose docs actually changed; ``dropTable`` counts every row
        (all deleted); ``renameTable`` / ``dropIndex`` report zero.

        Returns a :class:`par_rt_db.http_client.MigrateResult` (imported lazily
        to avoid a circular dependency at module load time).
        """
        from ..http_client import DirectiveReport, MigrateResult

        if self._schema is None:
            raise RtDbError(ErrorCode.BAD_REQUEST, "no schema pushed for migration")

        planned = deepcopy(self._schema)
        snapshot = deepcopy(self._docs)
        touched: set[str] = set()
        reports: list[DirectiveReport] = []

        for d in directives:
            try:
                report, table = self._apply_migration_directive(planned, d)
            except RtDbError:
                self._docs = snapshot
                raise
            reports.append(report)
            if table is not None:
                touched.add(table)

        # ENH-028: directive folding must not be able to invalidate a computed
        # entry — e.g. a `changeType` retyping a computed field so its
        # expression's static kind no longer fits. Re-validate the derived
        # computed maps (pure) before anything commits, mirroring server
        # `plan_migration`'s post-fold `validate_computed` call.
        try:
            _validate_computed(planned)
        except RtDbError:
            self._docs = snapshot
            raise

        if dry_run:
            self._docs = snapshot
            return MigrateResult(applied=False, schema=planned, directives=reports)

        self._schema = planned
        self._tables.clear()
        for name, def_ in planned.tables.items():
            self._tables[name] = def_
        self._notify_subs(touched)
        return MigrateResult(applied=True, schema=planned, directives=reports)

    def _apply_migration_directive(
        self,
        planned: SchemaDef,
        d: Directive,
    ) -> tuple[Any, str | None]:
        """Apply one directive to the working ``planned`` schema and ``self._docs``.

        Returns ``(DirectiveReport, Optional[table_name])`` where the table name
        marks the directive's touched table for subscription re-run. Mirrors
        ``rust-client::InMemoryRtDbClient::apply_migration_directive``.
        """

        if isinstance(d, _RenameField):
            return self._apply_rename_field_directive(planned, d), d.table
        if isinstance(d, _RenameTable):
            return self._apply_rename_table_directive(planned, d), d.to
        if isinstance(d, _ChangeType):
            return self._apply_change_type_directive(planned, d), d.table
        if isinstance(d, _DropField):
            return self._apply_drop_field_directive(planned, d), d.table
        if isinstance(d, _DropTable):
            return self._apply_drop_table_directive(planned, d), d.name
        if isinstance(d, _DropIndex):
            return self._apply_drop_index_directive(planned, d), d.table
        if isinstance(d, _SetDefault):
            return self._apply_set_default_directive(planned, d), d.table
        if isinstance(d, _EvalExpr):
            return self._apply_eval_expr_directive(planned, d), d.table
        # Exhaustive over the Directive union — if a new variant is added,
        # pyright flags this fallback as unreachable. Do not collapse.
        raise RtDbError(ErrorCode.INTERNAL, f"unknown migration directive: {d!r}")

    def _apply_eval_expr_directive(self, planned: SchemaDef, d: _EvalExpr) -> Any:
        """``evalExpr`` directive: no in-memory SQL engine exists, so both the
        ENH-020 typed ``ValueExpr`` path and the legacy raw-SQL path raise
        ``BAD_REQUEST`` — same convention as the search/vector stubs."""
        raise RtDbError(
            ErrorCode.BAD_REQUEST,
            f"evalExpr unsupported in-memory (table '{d.table}')",
        )

    def _apply_rename_field_directive(
        self,
        planned: SchemaDef,
        d: _RenameField,
    ) -> Any:
        from ..http_client import DirectiveReport

        t = _migrate_table_mut(planned, d.table)
        if d.to in t.fields:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"rename target '{d.table}.{d.to}' already exists",
            )
        ft = t.fields.pop(d.from_, None)
        if ft is None:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"renamed field '{d.table}.{d.from_}' does not exist",
            )
        t.fields[d.to] = ft
        for ix in t.indexes:
            ix.fields = [d.to if f == d.from_ else f for f in ix.fields]
        if t.owner_field == d.from_:
            t.owner_field = d.to
        if t.collaborators_field == d.from_:
            t.collaborators_field = d.to
        # QA-002: `auto_increment_field`, `updated_at_field`, and `ttl.field`
        # are name-bearing surfaces the same way `owner_field`/
        # `collaborators_field` are — missed here previously. Mirrors server
        # `migrate::rename_field_refs`.
        if t.auto_increment_field == d.from_:
            t.auto_increment_field = d.to
        if t.updated_at_field == d.from_:
            t.updated_at_field = d.to
        if t.ttl is not None and t.ttl.field == d.from_:
            t.ttl = t.ttl.model_copy(update={"field": d.to})
        # QA-002: the `authorize` predicate follows the rename the way
        # `computed` expressions do — missed here previously. Mirrors server
        # `migrate::rename_field_refs`.
        if t.authorize is not None:
            t.authorize = _rename_filter_fields(t.authorize, d.from_, d.to)
        # ENH-028: the computed map follows the rename the way the server's
        # does — an entry KEYED on the renamed field moves to the new name
        # (its declared field moved; leaving it keyed on `from_` would fail
        # `validate_computed`'s declared-field rule on the derived schema), and
        # every expression's `field` references (including `case.whens`
        # predicates) are rewritten to read the renamed doc key. Input values
        # are unchanged by the rename, so stored computed values stay correct;
        # the next write re-stamps.
        if d.from_ in t.computed:
            t.computed[d.to] = t.computed.pop(d.from_)
        for key in list(t.computed):
            t.computed[key] = _rename_value_expr_fields(t.computed[key], d.from_, d.to)
        # QA-002: `defaults` is keyed by field name the same way `computed`
        # is — the entry moves to the new key. Missed here previously.
        if d.from_ in t.defaults:
            t.defaults[d.to] = t.defaults.pop(d.from_)
        affected = 0
        for (tname, _), row in self._docs.items():
            if tname != d.table:
                continue
            if d.from_ in row.doc:
                row.doc[d.to] = row.doc.pop(d.from_)
                affected += 1
        return DirectiveReport(op="renameField", affected_rows=affected)

    def _apply_rename_table_directive(
        self,
        planned: SchemaDef,
        d: _RenameTable,
    ) -> Any:
        from ..http_client import DirectiveReport

        if d.to in planned.tables:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"rename target table '{d.to}' already exists",
            )
        def_ = planned.tables.pop(d.from_, None)
        if def_ is None:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"renamed table '{d.from_}' does not exist",
            )
        # Id references to `from_` in other tables follow the rename.
        for other in planned.tables.values():
            for ft in other.fields.values():
                if isinstance(ft, _FId) and ft.table == d.from_:
                    ft.table = d.to
        planned.tables[d.to] = def_
        # Re-key the live doc store: (from_, id) → (to, id).
        keys_to_move = [k for k in self._docs if k[0] == d.from_]
        for k in keys_to_move:
            row = self._docs.pop(k)
            self._docs[(d.to, k[1])] = row
        return DirectiveReport(op="renameTable", affected_rows=0)

    def _apply_change_type_directive(
        self,
        planned: SchemaDef,
        d: _ChangeType,
    ) -> Any:
        from ..http_client import DirectiveReport

        t = _migrate_table_mut(planned, d.table)
        old_ty = t.fields.get(d.field)
        if old_ty is None:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"changed field '{d.table}.{d.field}' does not exist",
            )
        if not _cast_valid_for(d.cast, old_ty):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"cast {d.cast.value} is not valid for {d.table}.{d.field}",
            )
        affected = 0
        for (tname, row_id), row in self._docs.items():
            if tname != d.table:
                continue
            val = row.doc.get(d.field)
            if val is None:
                continue
            affected += 1
            coerced = _coerce_value(d.cast, val)
            if coerced is not None:
                row.doc[d.field] = coerced
            elif d.default is not None:
                dv = _coerce_value(d.cast, d.default)
                row.doc[d.field] = dv if dv is not None else d.default
            else:
                raise RtDbError(
                    ErrorCode.BAD_REQUEST,
                    f"changeType cannot coerce value in {d.table}.{row_id} ({val}) "
                    "and no default given",
                )
        t.fields[d.field] = d.to
        return DirectiveReport(op="changeType", affected_rows=affected)

    def _apply_drop_field_directive(
        self,
        planned: SchemaDef,
        d: _DropField,
    ) -> Any:
        from ..http_client import DirectiveReport

        t = _migrate_table_mut(planned, d.table)
        if t.fields.pop(d.field, None) is None:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"dropped field '{d.table}.{d.field}' does not exist",
            )
        for ix in t.indexes:
            ix.fields = [f for f in ix.fields if f != d.field]
        if t.owner_field == d.field:
            t.owner_field = None
        if t.collaborators_field == d.field:
            t.collaborators_field = None
        # ENH-028: a computed expression reading the dropped field would
        # dangle — every future write fails its stamp. Reject, naming the
        # computed field, so the caller amends the computed map first (mirrors
        # server `migrate.rs`).
        computed_offender: str | None = None
        for computed_field, expr in t.computed.items():
            refs: list[str] = []
            walk_value_expr_fields(expr, refs.append)
            if d.field in refs:
                computed_offender = computed_field
                break
        if computed_offender is not None:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"cannot drop field '{d.table}.{d.field}': it is referenced by computed"
                f" field '{d.table}.{computed_offender}'; drop the computed field first",
            )
        # An entry KEYED on the dropped field goes with it: the applier below
        # removes the stored key from every doc, so leaving the entry would
        # fail `validate_computed`'s declared-field rule on the derived schema.
        t.computed.pop(d.field, None)
        affected = 0
        for (tname, _), row in self._docs.items():
            if tname != d.table:
                continue
            if d.field not in row.doc:
                continue
            row.doc.pop(d.field, None)
            affected += 1
        return DirectiveReport(op="dropField", affected_rows=affected)

    def _apply_drop_table_directive(
        self,
        planned: SchemaDef,
        d: _DropTable,
    ) -> Any:
        from ..http_client import DirectiveReport

        if planned.tables.pop(d.name, None) is None:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"dropped table '{d.name}' does not exist",
            )
        keys_to_remove = [k for k in self._docs if k[0] == d.name]
        for k in keys_to_remove:
            del self._docs[k]
        return DirectiveReport(op="dropTable", affected_rows=len(keys_to_remove))

    def _apply_drop_index_directive(
        self,
        planned: SchemaDef,
        d: _DropIndex,
    ) -> Any:
        from ..http_client import DirectiveReport

        t = _migrate_table_mut(planned, d.table)
        if not any(ix.name == d.name for ix in t.indexes):
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"dropped index '{d.table}.{d.name}' does not exist",
            )
        t.indexes = [ix for ix in t.indexes if ix.name != d.name]
        return DirectiveReport(op="dropIndex", affected_rows=0)

    def _apply_set_default_directive(
        self,
        planned: SchemaDef,
        d: _SetDefault,
    ) -> Any:
        from ..http_client import DirectiveReport

        t = _migrate_table_mut(planned, d.table)
        if d.field not in t.fields:
            raise RtDbError(
                ErrorCode.BAD_REQUEST,
                f"setDefault target '{d.table}.{d.field}' does not exist",
            )
        affected = 0
        for (tname, _), row in self._docs.items():
            if tname != d.table:
                continue
            if d.field not in row.doc:
                row.doc[d.field] = d.value
                affected += 1
        return DirectiveReport(op="setDefault", affected_rows=affected)
