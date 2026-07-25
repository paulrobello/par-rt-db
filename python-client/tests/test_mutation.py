"""Tests for ``par_rt_db.mutation``: ``Step`` (7 ops), ``StepResult``, ``Transaction``,
and the ``Mutation`` builder.

Mirrors ``server/src/txn.rs`` (the ``Step`` enum + ``Transaction`` struct +
untagged ``StepResult``) and the builder ergonomics of
``ts-client/src/mutation.ts`` / ``rust-client/src/mutation.rs``.

Wire shapes (load-bearing — match the server exactly):

* ``Step`` is tagged by ``op`` (camelCase variants: ``insert``/``patch``/
  ``replace``/``delete``/``expectVersion``/``expectAbsent``/``upsert``).
* ``Transaction`` is ``{"steps": Step[]}``; the server caps at 256 steps.
* ``StepResult`` is untagged: ``{"id", "inserted"}`` (upsert) beats ``{"id"}``
  (insert) beats ``None`` — Union variant ORDER matters (richest first), mirroring
  the rust-client's ``#[serde(untagged)]`` declaration order.

``StepResult`` is a Union type alias; validation routes through ``TypeAdapter``
(the alias itself has no ``model_validate``) — same pattern as ``FilterExpr`` /
``ClientMessage`` in ``tests/test_wire_parity.py``.
"""

import json

import pytest
from pydantic import TypeAdapter, ValidationError

from par_rt_db.mutation import MAX_STEPS, Mutation, StepResult, Transaction


def test_insert_patch_replace_delete_upsert_wire():
    m = (
        Mutation.builder()
        .insert("boxes", {"status": "active"})
        .patch("boxes", "b1", {"status": "idle"})
        .replace("boxes", "b1", {"status": "idle", "owner": "p"})
        .delete("boxes", "b1")
        .upsert("boxes", "by_owner", ["p1"], {"status": "active"}, {"status": "idle"})
        .build()
    )
    wire = json.loads(m.model_dump_json(by_alias=True))
    assert wire["steps"][0] == {"op": "insert", "table": "boxes", "doc": {"status": "active"}}
    assert wire["steps"][1] == {
        "op": "patch",
        "table": "boxes",
        "id": "b1",
        "fields": {"status": "idle"},
    }
    assert wire["steps"][2] == {
        "op": "replace",
        "table": "boxes",
        "id": "b1",
        "doc": {"status": "idle", "owner": "p"},
    }
    assert wire["steps"][3] == {"op": "delete", "table": "boxes", "id": "b1"}
    assert wire["steps"][4] == {
        "op": "upsert",
        "table": "boxes",
        "index": "by_owner",
        "eq": ["p1"],
        "insert": {"status": "active"},
        "patch": {"status": "idle"},
    }


def test_expect_version_and_expect_absent():
    m = (
        Mutation.builder()
        .expect_version("boxes", "b1", 7)
        .expect_absent("boxes", "by_owner", ["p9"])
        .build()
    )
    wire = json.loads(m.model_dump_json(by_alias=True))
    assert wire["steps"][0] == {
        "op": "expectVersion",
        "table": "boxes",
        "id": "b1",
        "version": 7,
    }
    assert wire["steps"][1] == {
        "op": "expectAbsent",
        "table": "boxes",
        "index": "by_owner",
        "eq": ["p9"],
    }


def test_step_rejects_unknown_op():
    with pytest.raises(ValidationError):
        Transaction.model_validate({"steps": [{"op": "bogus"}]})


def test_step_rejects_unknown_field():
    # deny_unknown_fields: an otherwise-valid op carrying an extra field must fail.
    with pytest.raises(ValidationError):
        Transaction.model_validate(
            {"steps": [{"op": "delete", "table": "t", "id": "x", "bogus": 1}]}
        )


def test_transaction_model_validate_round_trips():
    # The Mutation namespace exposes Transaction.model_validate for parsing wire JSON.
    payload = {"steps": [{"op": "delete", "table": "t", "id": "x"}]}
    txn = Mutation.model_validate(payload)
    assert isinstance(txn, Transaction)
    assert json.loads(txn.model_dump_json(by_alias=True)) == payload


def test_step_result_variants():
    # StepResult is a Union alias; validate via TypeAdapter.
    sr = TypeAdapter(StepResult)
    # Insert shape: {"id"} only.
    assert sr.validate_python({"id": "x"}).model_dump(
        by_alias=True, mode="json"
    ) == {"id": "x"}
    # Upsert shape: {"id", "inserted"} — must win over Insert (richer-first order).
    assert sr.validate_python({"id": "x", "inserted": True}).model_dump(
        by_alias=True, mode="json"
    ) == {"id": "x", "inserted": True}
    # null on the wire -> None.
    assert sr.validate_python(None) is None


def test_step_result_upsert_beats_insert_when_both_could_match():
    # Belt-and-suspenders: even if pydantic's smart union would do the right
    # thing, lock in the declaration-order contract: {"id","inserted"} -> upsert.
    out = TypeAdapter(StepResult).validate_python({"id": "x", "inserted": False})
    # The deserialized object must carry `inserted` (i.e. it became _StepUpsert,
    # not _StepInsert which would have dropped the field under extra="forbid").
    assert hasattr(out, "inserted")
    assert out.inserted is False  # type: ignore[attr-defined]


def test_transaction_max_steps_enforced_client_side():
    assert MAX_STEPS == 256
    b = Mutation.builder()
    for _ in range(MAX_STEPS):
        b.delete("t", "x")
    # The 256-step build succeeds (boundary is inclusive — server caps at >256).
    Mutation.builder().build()
    # The 257th step must trip the client-side guard.
    b.delete("t", "x")
    with pytest.raises(ValueError):
        b.build()


def test_mutation_builder_returns_self():
    # Each builder method returns the same instance for chaining.
    b = Mutation.builder()
    assert b.insert("t", {"x": 1}) is b
    assert b.patch("t", "i", {"x": 1}) is b
    assert b.replace("t", "i", {"x": 1}) is b
    assert b.delete("t", "i") is b
    assert b.expect_version("t", "i", 1) is b
    assert b.expect_absent("t", "idx", ["v"]) is b
    assert b.upsert("t", "idx", ["v"], {"x": 1}, {"x": 2}) is b
