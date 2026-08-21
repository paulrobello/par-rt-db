"""Tests for ``par_rt_db.mutation``: ``Step`` (14 ops), ``StepResult``, ``Transaction``,
and the ``Mutation`` builder.

Mirrors ``server/src/txn.rs`` (the ``Step`` enum + ``Transaction`` struct +
untagged ``StepResult``) and the builder ergonomics of
``ts-client/src/mutation.ts`` / ``rust-client/src/mutation.rs``.

Wire shapes (load-bearing — match the server exactly):

* ``Step`` is tagged by ``op`` (camelCase variants: ``insert``/``patch``/
  ``replace``/``delete``/``undelete``/``expectVersion``/``expectAbsent``/
  ``upsert``/``patchByQuery``/``deleteByQuery``/``schedule``/``cancelSchedule``/
  ``startWorkflow``/``cancelWorkflow``).
* ``Transaction`` is ``{"steps": Step[]}``; the server caps at 1024 steps.
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

from par_rt_db.errors import ErrorCode, RtDbError
from par_rt_db.mutation import MAX_STEPS, Mutation, StepResult, Transaction, await_signal
from par_rt_db.wire import AfterMs, FilterExpr, StepRetry, WorkflowSpec, WorkflowStepSpec


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
    assert sr.validate_python({"id": "x"}).model_dump(by_alias=True, mode="json") == {"id": "x"}
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
    assert out.inserted is False


def test_transaction_max_steps_enforced_client_side():
    assert MAX_STEPS == 1024
    b = Mutation.builder()
    for _ in range(MAX_STEPS):
        b.delete("t", "x")
    # The MAX_STEPS-step build succeeds (boundary is inclusive — server caps at >MAX_STEPS).
    b.build()
    # One more step trips the client-side guard, raised as RtDbError (BAD_REQUEST)
    # for taxonomy consistency with the rest of the client — not a bare ValueError.
    b.delete("t", "x")
    with pytest.raises(RtDbError) as exc_info:
        b.build()
    assert exc_info.value.code is ErrorCode.BAD_REQUEST


def test_transaction_accepts_more_than_256_steps():
    # ARC-104: the server raised MAX_STEPS 256 -> 1024, but the python client
    # still enforced the stale 256 cap in the production ``Mutation.build()``
    # path. A 300-step transaction is legal on the server and must build
    # client-side; only >1024 rejects.
    b = Mutation.builder()
    for _ in range(300):
        b.delete("t", "x")
    b.build()  # must not raise


def test_transaction_max_steps_matches_wire_corpus():
    # ARC-104: the wire-corpus ``protocol_constants.max_steps`` is the canonical
    # agreed value across the server and all four clients. A change to the
    # server constant without updating the corpus AND every client fails here.
    from pathlib import Path

    corpus = json.loads(
        (Path(__file__).resolve().parents[2] / "wire-corpus" / "wire-corpus.json").read_text()
    )
    assert corpus["protocol_constants"]["max_steps"] == MAX_STEPS


def test_mutation_builder_returns_self():
    # Each builder method returns the same instance for chaining.
    flt = _flt({"op": "eq", "field": "x", "value": 1})
    b = Mutation.builder()
    assert b.insert("t", {"x": 1}) is b
    assert b.patch("t", "i", {"x": 1}) is b
    assert b.replace("t", "i", {"x": 1}) is b
    assert b.delete("t", "i") is b
    assert b.expect_version("t", "i", 1) is b
    assert b.expect_absent("t", "idx", ["v"]) is b
    assert b.upsert("t", "idx", ["v"], {"x": 1}, {"x": 2}) is b
    assert b.patch_by_query("t", flt, {"x": 1}) is b
    assert b.delete_by_query("t", flt) is b
    assert b.schedule(AfterMs(ms=1000), Transaction(steps=[])) is b
    assert b.cancel_schedule("j1") is b


def _flt(expr: dict[str, object]) -> FilterExpr:
    return TypeAdapter(FilterExpr).validate_python(expr)


def test_patch_by_query_and_delete_by_query_wire_omit_limit():
    flt = _flt({"op": "eq", "field": "status", "value": "todo"})
    m = (
        Mutation.builder()
        .patch_by_query("items", flt, {"status": "done"})
        .delete_by_query("items", flt)
        .build()
    )
    wire = json.loads(m.model_dump_json(by_alias=True))
    assert wire["steps"][0] == {
        "op": "patchByQuery",
        "table": "items",
        "filter": {"op": "eq", "field": "status", "value": "todo"},
        "patch": {"status": "done"},
    }
    assert wire["steps"][1] == {
        "op": "deleteByQuery",
        "table": "items",
        "filter": {"op": "eq", "field": "status", "value": "todo"},
    }


def test_patch_by_query_and_delete_by_query_wire_emit_limit():
    flt = _flt({"op": "eq", "field": "status", "value": "todo"})
    m = (
        Mutation.builder()
        .patch_by_query("items", flt, {"status": "done"}, limit=50)
        .delete_by_query("items", flt, limit=10)
        .build()
    )
    wire = json.loads(m.model_dump_json(by_alias=True))
    assert wire["steps"][0]["limit"] == 50
    assert wire["steps"][1]["limit"] == 10


def test_patch_by_query_rejects_unknown_field():
    with pytest.raises(ValidationError):
        Transaction.model_validate(
            {
                "steps": [
                    {
                        "op": "patchByQuery",
                        "table": "t",
                        "filter": {"op": "eq", "field": "x", "value": 1},
                        "patch": {"x": 1},
                        "bogus": 7,
                    }
                ]
            }
        )


def test_delete_by_query_rejects_unknown_field():
    with pytest.raises(ValidationError):
        Transaction.model_validate(
            {
                "steps": [
                    {
                        "op": "deleteByQuery",
                        "table": "t",
                        "filter": {"op": "eq", "field": "x", "value": 1},
                        "limit": 5,
                        "bogus": 7,
                    }
                ]
            }
        )


def test_step_result_patch_by_query_and_delete_by_query():
    sr = TypeAdapter(StepResult)
    assert sr.validate_python({"patched": 3, "truncated": False}).model_dump(
        by_alias=True, mode="json"
    ) == {"patched": 3, "truncated": False}
    assert sr.validate_python({"deleted": 2, "truncated": True}).model_dump(
        by_alias=True, mode="json"
    ) == {"deleted": 2, "truncated": True}


def test_schedule_and_cancel_schedule_wire_shapes():
    # FM-28: byte-exact wire shapes for the schedule/cancelSchedule steps —
    # mirrors wire-corpus client_messages m4/m5.
    inner = Mutation.builder().insert("workItems", {"title": "later"}).build()
    m = Mutation.builder().schedule(AfterMs(ms=60_000), inner).cancel_schedule("0199ab_cd").build()
    wire = json.loads(m.model_dump_json(by_alias=True))
    assert wire["steps"][0] == {
        "op": "schedule",
        "when": {"type": "afterMs", "ms": 60000},
        "txn": json.loads(inner.model_dump_json(by_alias=True)),
    }
    assert wire["steps"][1] == {"op": "cancelSchedule", "id": "0199ab_cd"}
    # Round-trips through model_validate (corpus parity path).
    assert json.loads(Mutation.model_validate(wire).model_dump_json(by_alias=True)) == wire


def test_step_result_schedule_and_cancel_schedule():
    sr = TypeAdapter(StepResult)
    assert sr.validate_python({"scheduleId": "s1"}).model_dump(by_alias=True, mode="json") == {
        "scheduleId": "s1"
    }
    assert sr.validate_python({"cancelled": True}).model_dump(by_alias=True, mode="json") == {
        "cancelled": True
    }


# --- FM-29 workflow steps ---


def _wf_spec() -> WorkflowSpec:
    return WorkflowSpec(
        name="drip",
        steps=[
            WorkflowStepSpec(
                txn=Transaction(steps=[]).model_dump(by_alias=True),
                retry=StepRetry(max_attempts=5, initial_retry_ms=500, max_retry_ms=2000),
                sleep_before_ms=1000,
            )
        ],
    )


def test_start_and_cancel_workflow_wire_shapes():
    spec = _wf_spec()
    m = Mutation.builder().start_workflow(spec).cancel_workflow("wf9").build()
    wire = json.loads(m.model_dump_json(by_alias=True))
    assert wire["steps"][0] == {
        "op": "startWorkflow",
        "spec": json.loads(spec.model_dump_json(by_alias=True)),
    }
    assert wire["steps"][1] == {"op": "cancelWorkflow", "id": "wf9"}
    # Round-trips through model_validate (corpus parity path).
    assert json.loads(Mutation.model_validate(wire).model_dump_json(by_alias=True)) == wire


def test_workflow_builder_methods_return_self():
    b = Mutation.builder()
    assert b.start_workflow(_wf_spec()) is b
    assert b.cancel_workflow("wf1") is b


def test_await_signal_helper_builds_wait_declaration():
    # The spec-level counterpart of the step constructors: an awaitSignal step
    # serializes to exactly {"awaitSignal": {"name", "timeoutMs"?}}.
    step = WorkflowStepSpec(await_signal=await_signal("approve", timeout_ms=60_000))
    assert json.loads(step.model_dump_json(by_alias=True)) == {
        "awaitSignal": {"name": "approve", "timeoutMs": 60_000}
    }
    bare = WorkflowStepSpec(await_signal=await_signal("approve"))
    assert json.loads(bare.model_dump_json(by_alias=True)) == {"awaitSignal": {"name": "approve"}}


def test_step_result_start_and_cancel_workflow():
    sr = TypeAdapter(StepResult)
    assert sr.validate_python({"workflowId": "wf1"}).model_dump(by_alias=True, mode="json") == {
        "workflowId": "wf1"
    }
    # cancelWorkflow shares the {"cancelled": bool} shape with cancelSchedule.
    assert sr.validate_python({"cancelled": False}).model_dump(by_alias=True, mode="json") == {
        "cancelled": False
    }


# --- FM-33 undelete step ---


def test_undelete_wire_shape():
    m = Mutation.builder().undelete("notes", "n1").build()
    wire = json.loads(m.model_dump_json(by_alias=True))
    assert wire["steps"][0] == {"op": "undelete", "table": "notes", "id": "n1"}
    # Round-trips through model_validate (corpus parity path).
    assert json.loads(Mutation.model_validate(wire).model_dump_json(by_alias=True)) == wire


def test_undelete_builder_returns_self():
    b = Mutation.builder()
    assert b.undelete("notes", "n1") is b


def test_undelete_rejects_unknown_field():
    with pytest.raises(ValidationError):
        Transaction.model_validate(
            {"steps": [{"op": "undelete", "table": "t", "id": "x", "bogus": 1}]}
        )
