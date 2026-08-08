"""QA-001: Golden-vector parity test (python-client view).

Loads ``wire-corpus/golden-vector.json`` (repo root — the single source of
truth) and runs each query case through the python-client in-memory engine,
comparing canonicalized projected results. The same fixture is consumed by
the ts-client, rust-client, and server (against Postgres) tests; a divergence
in any one implementation surfaces there.

The fixture encodes the dataset, the per-case wire-shape ``Query``, and the
expected canonical result. System fields (``_id``, ``_creationTime``,
``_owner``, ``_updatedAt``) are projected out before comparison so the
client's id-minting order doesn't cause spurious divergence — the audit point
is to catch **sort-comparator / boundary / terminal-cascade** divergence, not
id-minting drift.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from par_rt_db import Mutation
from par_rt_db.in_memory import InMemoryRtDbClient, InMemoryRtDbClientOptions
from par_rt_db.query import Query
from par_rt_db.schema import Schema, t

_FIXTURE = Path(__file__).resolve().parents[2] / "wire-corpus" / "golden-vector.json"


def _load_fixture() -> dict[str, Any]:
    return json.loads(_FIXTURE.read_text())


def _build_schema(fx: dict[str, Any]) -> Any:
    """Translate the fixture's `schema_fields` / `schema_indexes` into a Schema."""
    table = fx["schema_table"]
    fields = fx["schema_fields"]
    indexes = fx["schema_indexes"]

    def field_type(spec: str) -> Any:
        if spec == "string":
            return t.string()
        if spec == "number":
            return t.number()
        if spec == "optional(string)":
            return t.optional(t.string())
        raise AssertionError(f"fixture field type not implemented: {spec}")

    def table_fn(tb: Any) -> Any:
        for name, ty in fields.items():
            tb = tb.field(name, field_type(ty))
        for ix in indexes:
            tb = tb.index(ix["name"], ix["fields"])
        return tb

    return Schema.builder().table(table, table_fn).build()


def _seed_client(fx: dict[str, Any]) -> InMemoryRtDbClient:
    """Build an in-memory client with the fixture's schema and seed docs."""
    counter = [1_700_000_000_000]

    def now() -> int:
        v = counter[0]
        counter[0] += 1
        return v

    c = InMemoryRtDbClient(InMemoryRtDbClientOptions(now=now, random=lambda: 0.0))
    c.push_schema(_build_schema(fx))
    table = fx["schema_table"]
    for doc in fx["seed"]:
        txn = Mutation.builder().insert(table, doc).build()
        c.mutate(txn)
    return c


def _project(doc: Any) -> dict[str, Any]:
    """Strip system fields, keep only the user-declared fields, in stable key order."""
    return {k: doc[k] for k in ("name", "status", "order") if k in doc}


def _project_list(docs: list[Any]) -> list[dict[str, Any]]:
    return [_project(d) for d in docs]


def _run_case(c: InMemoryRtDbClient, case: dict[str, Any]) -> Any:
    q = Query.model_validate(case["query"])
    return c.run_query(q)


@pytest.fixture(scope="module")
def seeded_client() -> InMemoryRtDbClient:
    return _seed_client(_load_fixture())


@pytest.mark.parametrize(
    "case_id",
    [c["id"] for c in _load_fixture()["cases"]],
)
def test_golden_vector_case(seeded_client: InMemoryRtDbClient, case_id: str) -> None:
    fx = _load_fixture()
    case = next(c for c in fx["cases"] if c["id"] == case_id)
    result = _run_case(seeded_client, case)

    if case.get("expected_scalar") is not None:
        # count terminal: scalar result
        assert result == case["expected_scalar"], (
            f"{case_id}: expected count {case['expected_scalar']}, got {result}"
        )
        return

    if case.get("expected_unordered"):
        # No sort key declared → protocol order is unspecified; compare as a set.
        got = sorted(_project_list(result), key=lambda d: d["name"])
        want = sorted(case["expected"], key=lambda d: d["name"])
        assert got == want, f"{case_id}: unordered mismatch\n got={got}\n want={want}"
        return

    if case.get("expected_has_next_cursor"):
        # paginate terminal: result is a PaginatedResult dict (docs + nextCursor).
        assert isinstance(result, dict), (
            f"{case_id}: expected PaginatedResult dict, got {type(result).__name__}"
        )
        got = _project_list(result["docs"])
        want = case["expected"]
        assert got == want, f"{case_id}: page mismatch\n got={got}\n want={want}"
        assert result.get("nextCursor") is not None, (
            f"{case_id}: expected nextCursor present (more pages remain)"
        )
        return

    if isinstance(case["expected"], list):
        got = _project_list(result)
        want = case["expected"]
        assert got == want, f"{case_id}: ordered mismatch\n got={got}\n want={want}"
        return

    # single-doc terminal (get / first / unique)
    got = _project(result)
    want = case["expected"]
    assert got == want, f"{case_id}: single-doc mismatch\n got={got}\n want={want}"
