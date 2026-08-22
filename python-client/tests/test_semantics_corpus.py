"""ENH-023: behavioral-semantics corpus runner (python-client view).

Enumerates every ``*.json`` case in ``wire-corpus/semantics/`` (repo root — the
single source of truth; one self-contained case per file carrying its own
schema, seed, operation, and expected result) and executes each against a fresh
in-memory engine instance, comparing normalized results. The same fixture files
are consumed by the server, ts-client, and rust-client runners; the server is
the source of truth for every expected value.

The runner implements ``wire-corpus/README.md``'s "How a runner executes a
case" algorithm, mirroring ``server/tests/semantics_corpus_test.rs`` (whose
comparison/substitution/enumeration logic is the reference semantics): runtime
directory enumeration (the directory IS the case count — no hardcoded
constant), per-case fresh instance, seed through the normal insert path with
``$id`` label capture, ``{"$idRef": ...}`` substitution throughout ``op`` /
``then.query``, the ``"$prev"`` paginate-cursor sentinel, error cases asserting
the ``ErrorCode`` wire name only, ``normalize`` projection applied recursively
to both trees, ``unordered`` multiset comparison via canonical-JSON sort,
numeric-tolerant equality, and structural ``expect_next_cursor`` presence. No
clock advance / TTL tick between seeding and the op — the corpus pins
synchronous semantics only.

Two additive case kinds (ENH-028): a ``pushError`` case asserts the schema
PUSH itself fails with the given code (push is the whole case — no seed, no
op), and an ``op.migrate`` case runs the engine's migrate machinery
(:meth:`InMemoryRtDbClient.migrate_schema`, apply-persisted before ``then``
reads, ``dryRun`` honored) with the ``MigrateResult`` compared like any op
result under the runner's existing normalize rules.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from par_rt_db import Mutation
from par_rt_db.errors import ErrorCode, RtDbError
from par_rt_db.in_memory import InMemoryRtDbClient, InMemoryRtDbClientOptions
from par_rt_db.migration import MigrateRequest
from par_rt_db.query import Query
from par_rt_db.schema import SchemaDef

_CORPUS_DIR = Path(__file__).resolve().parents[2] / "wire-corpus" / "semantics"

#: System fields minted at run time and projected out of both sides unless a
#: case's ``normalize`` list replaces the default (README "Semantics corpus
#: format"). A present list REPLACES; absent falls back (a ``then`` block
#: inherits the case-level list unless it gives its own).
_DEFAULT_NORMALIZE: list[str] = ["_id", "_creationTime", "_version"]


def _case_files() -> list[Path]:
    """Every corpus case file, sorted by filename — the directory IS the count."""
    files = sorted(_CORPUS_DIR.glob("*.json"))
    assert files, "wire-corpus/semantics contains no fixture files"
    return files


def _load_case(path: Path) -> dict[str, Any]:
    """Parse one case file and pin ``name`` == filename stem."""
    case = json.loads(path.read_text())
    assert isinstance(case, dict), f"{path.stem}: case must be a JSON object"
    assert case.get("name") == path.stem, f"{path.stem}: case `name` must equal the filename stem"
    return case


def _parse_seed_entry(
    entry: Any, single_table: str | None, case: str
) -> tuple[str, dict[str, Any], str | None]:
    """Resolve one ``seed`` entry into ``(table, doc, label)``.

    A wrapped entry is an object with a ``doc`` key whose value is an object
    (with optional ``table`` and ``$id`` siblings); any other object is a plain
    doc, legal only when the schema declares exactly one table — the
    disambiguation rule the corpus README states. ``$id`` lives as a sibling of
    ``doc``, so it never reaches the insert.
    """
    if not isinstance(entry, dict):
        raise AssertionError(f"{case}: seed entry must be a JSON object")
    doc = entry.get("doc")
    if isinstance(doc, dict):
        table = entry.get("table")
        if table is None:
            if single_table is None:
                raise AssertionError(
                    f"{case}: wrapped seed entry without `table` requires a single-table schema"
                )
            table = single_table
        label = entry.get("$id")
        if label is not None and not isinstance(label, str):
            raise AssertionError(f"{case}: seed $id label must be a string")
        return table, doc, label
    if single_table is None:
        raise AssertionError(f"{case}: plain-doc seed requires a single-table schema")
    return single_table, entry, None


def _substitute(node: Any, ids: dict[str, str], case: str) -> Any:
    """Replace every ``{"$idRef": "<label>"}`` object anywhere in the tree with
    the minted id recorded for that seed label (README "Substitution
    placeholders"). Returns a new tree; the fixture dict is never mutated."""

    def sub(n: Any) -> Any:
        if isinstance(n, dict):
            if set(n.keys()) == {"$idRef"}:
                label = n["$idRef"]
                if not isinstance(label, str):
                    raise AssertionError(f"{case}: $idRef label must be a string")
                if label not in ids:
                    raise AssertionError(f"{case}: $idRef references unknown seed label '{label}'")
                return ids[label]
            return {k: sub(v) for k, v in n.items()}
        if isinstance(n, list):
            return [sub(v) for v in n]
        return n

    return sub(node)


def _project_recursive(node: Any, keys: list[str]) -> None:
    """Remove every ``keys`` member from every object in the tree, recursively —
    the README's ``normalize`` projection applies to every object in both the
    actual and expected trees (docs inside ``paginate.docs``, step results,
    ...)."""
    if isinstance(node, dict):
        for k in keys:
            node.pop(k, None)
        for v in node.values():
            _project_recursive(v, keys)
    elif isinstance(node, list):
        for v in node:
            _project_recursive(v, keys)


def _canonical(v: Any) -> str:
    """Canonical JSON for the unordered multiset sort: compact serialization
    with object keys sorted recursively."""
    return json.dumps(v, sort_keys=True, separators=(",", ":"))


def _json_eq_numeric(a: Any, b: Any) -> bool:
    """Numeric-tolerant equality: two numbers are equal when their float forms
    match (so SQL-numeric ``6`` and client-float ``6.0`` agree — the same
    tolerance golden-vector applies). Booleans are not numbers (``True != 1``),
    mirroring the JSON value model."""
    a_num = isinstance(a, int | float) and not isinstance(a, bool)
    b_num = isinstance(b, int | float) and not isinstance(b, bool)
    if a_num and b_num:
        return float(a) == float(b) or abs(float(a) - float(b)) < 1e-9
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(_json_eq_numeric(x, y) for x, y in zip(a, b, strict=True))
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(_json_eq_numeric(a[k], b[k]) for k in a)
    return a == b


def _assert_expected(got: Any, want: Any, unordered: bool, msg: str) -> None:
    """Assert actual == expected under ``normalize`` projection already applied:
    equal-as-sequences is checked first (it also covers every unordered case);
    otherwise ``unordered`` compares the two arrays as multisets (each side
    sorted by canonical JSON, then element-wise numeric-tolerant)."""
    if _json_eq_numeric(got, want):
        return
    if not unordered:
        raise AssertionError(f"{msg}\n got {got!r}\nwant {want!r}")
    if not isinstance(got, list) or not isinstance(want, list):
        raise AssertionError(
            f"{msg}: unordered comparison requires arrays — got {got}, want {want}"
        )
    if len(got) != len(want):
        raise AssertionError(
            f"{msg}: row count mismatch (unordered) — got {len(got)}, want {len(want)}"
        )
    got_sorted = sorted(got, key=_canonical)
    want_sorted = sorted(want, key=_canonical)
    for i, (g, w) in enumerate(zip(got_sorted, want_sorted, strict=True)):
        if not _json_eq_numeric(g, w):
            raise AssertionError(
                f"{msg}: row {i} mismatch (unordered compare)\n got {got!r}\nwant {want!r}"
            )
    # Lengths equal and every sorted row matched: the multisets agree, so the
    # values differ only in order — exactly what `unordered` forgives.


def _normalize_keys(block: dict[str, Any], fallback: list[str], case: str) -> list[str]:
    """The effective ``normalize`` key list for an expect block: a present list
    REPLACES the default; absent falls back to ``fallback`` (the case-level
    list, itself defaulted — ``then`` inherits the case's list unless it
    overrides)."""
    raw = block.get("normalize")
    if raw is None:
        return list(fallback)
    if not isinstance(raw, list):
        raise AssertionError(f"{case}: normalize must be an array")
    for v in raw:
        if not isinstance(v, str):
            raise AssertionError(f"{case}: normalize entries must be strings")
    return list(raw)


def _assert_error_code(err: RtDbError, expect: dict[str, Any], case: str) -> None:
    """Error-case assertion: only the code is compared, never the message."""
    want_code = expect["error"]["code"]
    want = ErrorCode(want_code)  # raises loudly on an unknown fixture code
    assert err.code == want, (
        f"{case}: error code mismatch (got {err.code.value}, want {want.value}) "
        f"— engine message: {err.message}"
    )


def _step_result_json(result: Any) -> Any:
    """One per-step result in wire JSON shape (an untagged step result is a
    pydantic model or ``None``)."""
    if isinstance(result, BaseModel):
        return result.model_dump(by_alias=True, mode="json")
    return result


def _assert_result(
    case: str, actual: Any, block: dict[str, Any], keys: list[str], unordered: bool
) -> None:
    """Compare an op/then success result against its ``expect`` block: apply the
    ``normalize`` projection to both trees, structurally assert ``nextCursor``
    presence when the case pins it (paginate — the minted cursor value itself is
    projected out and never compared), then ordered/unordered compare."""
    got = copy.deepcopy(actual)
    want = copy.deepcopy(block["expect"])
    want_cursor = block.get("expect_next_cursor")
    projected = list(keys)
    if isinstance(want_cursor, bool):
        has = isinstance(got, dict) and got.get("nextCursor") is not None
        assert has == want_cursor, (
            f"{case}: nextCursor presence mismatch (got {has}, want {want_cursor})"
        )
        projected.append("nextCursor")
    _project_recursive(got, projected)
    _project_recursive(want, projected)
    _assert_expected(got, want, unordered, f"{case}: result mismatch")


def _run_query_json(client: InMemoryRtDbClient, q_json: dict[str, Any], case: str) -> Any:
    """Validate and execute one wire-shaped query dict."""
    try:
        q = Query.model_validate(q_json)
    except Exception as err:  # noqa: BLE001 — named loudly with the case
        raise AssertionError(f"{case}: query does not parse: {err}") from err
    return client.run_query(q)


def _run_case(case: dict[str, Any]) -> None:
    case = copy.deepcopy(case)
    name: str = case["name"]

    # Fresh instance per case with a deterministic clock (golden-vector's
    # convention) — no time advance / TTL tick between seeding and the op.
    counter = [1_700_000_000_000]

    def now() -> int:
        counter[0] += 1
        return counter[0]

    client = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=now, random=lambda: 0.0))
    schema_json = case["schema"]
    assert isinstance(schema_json, dict), f"{name}: schema must be an object"

    # A `pushError` case asserts the schema PUSH itself fails (README format:
    # the value carries the same `{code}` object `expect.error` does; only the
    # code is asserted, never the message). Push is the whole case — a stray
    # seed/op/then/expect is an authoring error.
    push_error = case.get("pushError")
    if push_error is not None:
        for stray in ("seed", "op", "then", "expect"):
            assert stray not in case, (
                f"{name}: a pushError case must not carry `{stray}` — push is the whole case"
            )
        assert isinstance(push_error, dict) and "code" in push_error, (
            f"{name}: pushError must carry a code object"
        )
        want = ErrorCode(push_error["code"])  # raises loudly on an unknown fixture code
        with pytest.raises(RtDbError) as ei:
            client.push_schema(SchemaDef.model_validate(schema_json))
        assert ei.value.code == want, (
            f"{name}: push error code mismatch (got {ei.value.code.value}, want"
            f" {want.value}) — engine message: {ei.value.message}"
        )
        return

    client.push_schema(SchemaDef.model_validate(schema_json))

    tables = list(schema_json["tables"])
    single_table = tables[0] if len(tables) == 1 else None

    # Seed in array order through the normal insert path, recording
    # `label -> minted id` for `$id`-labeled entries.
    seed = case["seed"]
    assert isinstance(seed, list), f"{name}: seed must be an array"
    ids: dict[str, str] = {}
    for i, entry in enumerate(seed):
        table, doc, label = _parse_seed_entry(entry, single_table, name)
        results = client.mutate(Mutation.builder().insert(table, doc).build())
        if label is not None:
            first = _step_result_json(results[0])
            assert isinstance(first, dict) and "id" in first, (
                f"{name}: seed #{i}: insert result missing id"
            )
            ids[label] = first["id"]

    # Key presence, not value truthiness: `get`-miss cases carry an explicit
    # JSON-null `expect` (the serialized miss result) — present-null is legal.
    assert "expect" in case, f"{name}: missing expect"
    expect = case["expect"]
    expects_error = (
        isinstance(expect, dict)
        and isinstance(expect.get("error"), dict)
        and "code" in expect["error"]
    )
    case_keys = _normalize_keys(case, _DEFAULT_NORMALIZE, name)

    op = case.get("op")
    assert isinstance(op, dict) and ("txn" in op or "query" in op or "migrate" in op), (
        f"{name}: op must carry `query`, `txn`, or `migrate`"
    )

    # Execute the op. A query op first resolves the "$prev" paginate-cursor
    # sentinel (README step 4): run the cursor-less query, take its nextCursor
    # (fail loudly if there is none), then run the real query with it —
    # `expect` describes the SECOND page.
    if "txn" in op:
        txn = Mutation.model_validate(_substitute(op["txn"], ids, name))
        try:
            results = client.mutate(txn)
        except RtDbError as err:
            if not expects_error:
                raise AssertionError(
                    f"{name}: unexpected txn error ({err.code.value}): {err.message}"
                ) from err
            _assert_error_code(err, expect, name)
            return  # a failed op has no `then` follow-up
        op_result: Any = [_step_result_json(r) for r in results]
    elif "migrate" in op:
        # An `op.migrate` case routes the admin MigrateRequest wire body
        # through the engine's migrate (apply-persisted unless `dryRun`, so a
        # follow-up `then` reads resolve fields through the derived schema —
        # the engine installs it on apply). The MigrateResult is compared like
        # any op result under the runner's normalize rules; migrate-domain
        # errors assert the `expect.error` envelope code.
        migrate_json = _substitute(op["migrate"], ids, name)
        assert isinstance(migrate_json, dict), f"{name}: op.migrate must be an object"
        try:
            req = MigrateRequest.model_validate(migrate_json)
        except Exception as err:  # noqa: BLE001 — named loudly with the case
            raise AssertionError(f"{name}: op.migrate does not parse: {err}") from err
        try:
            result = client.migrate_schema(list(req.directives), dry_run=req.dry_run)
        except RtDbError as err:
            if not expects_error:
                raise AssertionError(
                    f"{name}: unexpected migrate error ({err.code.value}): {err.message}"
                ) from err
            _assert_error_code(err, expect, name)
            return  # a failed op has no `then` follow-up
        op_result = result.model_dump(by_alias=True, mode="json")
    else:
        q_json = _substitute(op["query"], ids, name)
        assert isinstance(q_json, dict), f"{name}: op.query must be an object"
        paginate = q_json.get("paginate")
        if isinstance(paginate, dict) and paginate.get("cursor") == "$prev":
            first_json = copy.deepcopy(q_json)
            del first_json["paginate"]["cursor"]
            first_result = _run_query_json(client, first_json, f"{name} $prev first page")
            cursor = first_result.get("nextCursor") if isinstance(first_result, dict) else None
            assert isinstance(cursor, str) and cursor, (
                f"{name}: $prev: first page has no nextCursor"
            )
            q_json["paginate"]["cursor"] = cursor
        try:
            op_result = _run_query_json(client, q_json, name)
        except RtDbError as err:
            if not expects_error:
                raise AssertionError(
                    f"{name}: unexpected query error ({err.code.value}): {err.message}"
                ) from err
            _assert_error_code(err, expect, name)
            return  # a failed op has no `then` follow-up

    assert not expects_error, (
        f"{name}: expected error {expect['error']['code']}, got success {op_result!r}"
    )
    _assert_result(name, op_result, case, case_keys, bool(case.get("unordered", False)))

    # Follow-up read after a successful op (write-then-read visibility cases).
    then = case.get("then")
    if then is not None:
        assert isinstance(then, dict) and "query" in then, f"{name}: then requires query"
        then_result = _run_query_json(client, _substitute(then["query"], ids, name), f"{name} then")
        _assert_result(
            name,
            then_result,
            then,
            _normalize_keys(then, case_keys, name),
            bool(then.get("unordered", False)),
        )


def test_corpus_enumeration() -> None:
    """The directory IS the case count: every file parses, carries a ``name``
    equal to its filename stem, and is collected for execution (each executed
    or explicitly skipped by the per-case test below — nothing dropped)."""
    files = _case_files()
    loaded = [_load_case(p) for p in files]
    assert len(loaded) == len(files)
    assert len(loaded) > 0


@pytest.mark.parametrize("path", _case_files(), ids=lambda p: p.stem)
def test_semantics_corpus_case(path: Path) -> None:
    """Execute one corpus case against a fresh in-memory engine instance."""
    case = _load_case(path)
    skip = case.get("skip")
    if isinstance(skip, dict) and skip.get("python") is not None:
        pytest.skip(f"{case['name']}: {skip['python']}")
    _run_case(case)
