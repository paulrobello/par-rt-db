"""Round-trip parity: our serialized JSON must equal the server's wire bytes.

Fixtures are copied verbatim from ``server/src/protocol.rs`` tests (the
authoritative wire shapes). A failure here means a wire model drifted from the
contract.

``ClientMessage`` and ``ServerMessage`` are ``Annotated[Union, Field(discriminator=...)]``
aliases, so validation routes through ``TypeAdapter`` (the alias itself has no
``model_validate``) — same pattern as ``tests/test_wire.py``.

ARC-008: in addition to the per-package fixtures above, this test also runs the
shared canonical corpus at ``wire-corpus/wire-corpus.json`` (repo root) through
the python wire types. The server, rust-client, and ts-client each run the same
corpus; drift in any one package is caught there. Python's ``extra='forbid'``
on every wire model means every fixture is also a deny-unknown-fields test.
"""

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import TypeAdapter, ValidationError

from par_rt_db.wire import (
    AuthedUser,
    ClientMessage,
    ScheduleInfo,
    ScheduleWhen,
    ServerMessage,
    WorkflowSpec,
)

# (wire_json_string) — every entry must round-trip identically.
CLIENT_FIXTURES: list[str] = [
    '{"type": "auth", "token": "t", "db": "d"}',
    '{"type": "subscribe", "queryId": "q1", "query": {"table": "t"}}',
    '{"type": "unsubscribe", "queryId": "q1"}',
    '{"type": "mutate", "mutId": "m1", "txn": {"steps": []}}',
    '{"type": "mutate", "mutId": "m1", "idempotencyKey": "key1", "txn": {"steps": []}}',
    '{"type": "ping"}',
    (
        '{"type": "schedule", "scheduleId": "s1", '
        '"when": {"type": "afterMs", "ms": 100}, "txn": {"steps": []}}'
    ),
    '{"type": "cancelSchedule", "scheduleId": "s1", "id": "job-9"}',
    '{"type": "pauseSchedule", "scheduleId": "s1", "id": "job-9"}',
    '{"type": "resumeSchedule", "scheduleId": "s1", "id": "job-9"}',
    '{"type": "listSchedules", "scheduleId": "s1"}',
    # FM-29 workflow frames (mirrors server protocol.rs tests).
    (
        '{"type": "startWorkflow", "workflowId": "c1", '
        '"spec": {"name": "drip", "steps": [{"txn": {"steps": []}}]}}'
    ),
    '{"type": "cancelWorkflow", "workflowId": "c2", "id": "wf9"}',
    # awaitSignal delivery (mirrors server protocol.rs
    # signal_workflow_frame_wire_shape): payload rides the frame verbatim and
    # is omitted when absent.
    (
        '{"type": "signalWorkflow", "workflowId": "c4", "id": "wf1", '
        '"name": "approve", "payload": {"ok": true}}'
    ),
    '{"type": "signalWorkflow", "workflowId": "c5", "id": "wf1", "name": "approve"}',
    '{"type": "listWorkflows", "workflowId": "c3"}',
    '{"type": "listWorkflows", "workflowId": "c3", "status": "failed"}',
]

SERVER_FIXTURES: list[str] = [
    '{"type": "queryUpdate", "queryId": "q1", "result": []}',
    '{"type": "mutateOk", "mutId": "m1", "results": []}',
    (
        '{"type": "subscribeErr", "queryId": "q1", '
        '"error": {"code": "BAD_REQUEST", "message": "bad index"}}'
    ),
    '{"type": "pong"}',
    '{"type": "scheduleOk", "scheduleId": "s1", "id": "job-9"}',
    '{"type": "scheduleAck", "scheduleId": "s1", "ok": true}',
    (
        '{"type": "scheduleAck", "scheduleId": "s1", "ok": false, '
        '"error": {"code": "NOT_FOUND", "message": "x"}}'
    ),
    # FM-29 workflow frames (mirrors server protocol.rs tests). The
    # startWorkflowOk info carries the omitted-when-None optional fields'
    # absence; workflowAck mirrors scheduleAck's ok/error shape.
    (
        '{"type": "startWorkflowOk", "workflowId": "c1", "info": {"id": "wf1", "name": "drip", '
        '"status": "pending", "currentStep": 0, "stepCount": 2, "attempts": 0, '
        '"createdAt": 100, "updatedAt": 100}}'
    ),
    (
        '{"type": "startWorkflowErr", "workflowId": "c1", '
        '"error": {"code": "BAD_REQUEST", "message": "empty steps"}}'
    ),
    '{"type": "workflowAck", "workflowId": "c2", "ok": true}',
    (
        '{"type": "workflowAck", "workflowId": "c2", "ok": false, '
        '"error": {"code": "NOT_FOUND", "message": "no run"}}'
    ),
    (
        '{"type": "listWorkflowsOk", "workflowId": "c3", "workflows": [{"id": "wf1", '
        '"name": "drip", "status": "running", "currentStep": 1, "stepCount": 2, '
        '"attempts": 0, "sleepUntil": 9000, "createdAt": 100, "updatedAt": 150}]}'
    ),
]


_client_adapter = TypeAdapter(ClientMessage)
_server_adapter = TypeAdapter(ServerMessage)


@pytest.mark.parametrize("wire", CLIENT_FIXTURES)
def test_client_message_round_trip(wire: str) -> None:
    expected = json.loads(wire)
    msg = _client_adapter.validate_python(expected)
    dumped = json.loads(msg.model_dump_json(by_alias=True))
    assert dumped == expected, f"client wire drift: {dumped} != {expected}"


@pytest.mark.parametrize("wire", SERVER_FIXTURES)
def test_server_message_round_trip(wire: str) -> None:
    expected = json.loads(wire)
    msg = _server_adapter.validate_python(expected)
    dumped = json.loads(msg.model_dump_json(by_alias=True))
    assert dumped == expected, f"server wire drift: {dumped} != {expected}"


# --- ARC-008: canonical cross-client wire-parity corpus ---------------------
#
# Loads the shared ``wire-corpus/wire-corpus.json`` at the repo root. Every
# entry must round-trip byte-identically. Python's ``extra='forbid'`` on every
# wire model (inherited from ``_Camel``) means every section is also a
# deny-unknown-fields check; the ``rejects_*`` sections additionally assert
# ARC-004's narrowed ``Literal`` unions reject out-of-domain values.

_CORPUS_PATH = Path(__file__).resolve().parents[2] / "wire-corpus" / "wire-corpus.json"


def _load_corpus() -> dict[str, Any]:
    return json.loads(_CORPUS_PATH.read_text())


def _corpus_section(name: str) -> list[Any]:
    data = _load_corpus()
    section = data.get(name)
    if not isinstance(section, list):
        raise TypeError(f"corpus section '{name}' missing or not a list")
    return section


_CORPUS_CLIENT = pytest.mark.parametrize(
    "entry", _corpus_section("client_messages"), ids=lambda e: e.get("type", "?")
)
_CORPUS_SERVER = pytest.mark.parametrize(
    "entry", _corpus_section("server_messages"), ids=lambda e: e.get("type", "?")
)


@_CORPUS_CLIENT
def test_corpus_client_messages_round_trip(entry: dict[str, Any]) -> None:
    msg = _client_adapter.validate_python(entry)
    dumped = json.loads(msg.model_dump_json(by_alias=True))
    assert dumped == entry, f"client wire drift: {dumped} != {entry}"


@_CORPUS_SERVER
def test_corpus_server_messages_round_trip(entry: dict[str, Any]) -> None:
    msg = _server_adapter.validate_python(entry)
    dumped = json.loads(msg.model_dump_json(by_alias=True))
    assert dumped == entry, f"server wire drift: {dumped} != {entry}"


@pytest.mark.parametrize(
    "entry",
    _corpus_section("authed_users"),
    ids=lambda e: f"kind={e.get('kind')}",
)
def test_corpus_authed_users_round_trip(entry: dict[str, Any]) -> None:
    msg = AuthedUser.model_validate(entry)
    dumped = json.loads(msg.model_dump_json(by_alias=True))
    assert dumped == entry, f"AuthedUser wire drift: {dumped} != {entry}"


@pytest.mark.parametrize(
    "entry",
    _corpus_section("schedule_whens"),
    ids=lambda e: e.get("type", "?"),
)
def test_corpus_schedule_whens_round_trip(entry: dict[str, Any]) -> None:
    adapter = TypeAdapter(ScheduleWhen)
    msg = adapter.validate_python(entry)
    dumped = json.loads(msg.model_dump_json(by_alias=True))
    assert dumped == entry, f"ScheduleWhen wire drift: {dumped} != {entry}"


@pytest.mark.parametrize(
    "entry",
    _corpus_section("schedule_infos"),
    ids=lambda e: f"kind={e.get('kind')},status={e.get('status')}",
)
def test_corpus_schedule_infos_round_trip(entry: dict[str, Any]) -> None:
    msg = ScheduleInfo.model_validate(entry)
    dumped = json.loads(msg.model_dump_json(by_alias=True))
    assert dumped == entry, f"ScheduleInfo wire drift: {dumped} != {entry}"


# --- corpus: read queries (the `Query` DSL wire model) ------------------------
#
# The `queries` section pins the read-query DSL every client must serialize
# identically — including the search terminal's additive fields (`mode` FM-30,
# operator-syntax query text and `snippet` FM-31). The python `Query` model
# (par_rt_db.query) carries `extra='forbid'`, so this doubles as a
# deny-unknown-fields check.


@pytest.mark.parametrize(
    "entry",
    _corpus_section("queries"),
    ids=lambda e: ",".join(sorted(e.keys() - {"table"})) or "bare",
)
def test_corpus_queries_round_trip(entry: dict[str, Any]) -> None:
    from par_rt_db.query import Query

    msg = Query.model_validate(entry)
    dumped = msg.model_dump(by_alias=True, mode="json")
    assert dumped == entry, f"Query wire drift: {dumped} != {entry}"


# --- corpus: admin migrate wire (Directive list + MigrateResult) -------------
#
# The migrate types are part of the four-client wire contract (op tag, camelCase,
# ``where``/``from`` aliases, cast literals). These sections extend the shared
# corpus so the python client asserts byte-identical serialization alongside the
# server, rust-client, and ts-client.


@pytest.mark.parametrize(
    "entry",
    _corpus_section("migrate_requests"),
    ids=lambda e: f"dryRun={e.get('dryRun')},{len(e.get('directives', []))}ds",
)
def test_corpus_migrate_requests_round_trip(entry: dict[str, Any]) -> None:
    from par_rt_db.migration import MigrateRequest

    msg = MigrateRequest.model_validate(entry)
    dumped = msg.model_dump(by_alias=True, mode="json")
    assert dumped == entry, f"MigrateRequest wire drift: {dumped} != {entry}"


@pytest.mark.parametrize(
    "entry",
    _corpus_section("migrate_results"),
    ids=lambda e: f"applied={e.get('applied')},{len(e.get('directives', []))}ds",
)
def test_corpus_migrate_results_round_trip(entry: dict[str, Any]) -> None:
    from par_rt_db.http_client import MigrateResult

    msg = MigrateResult.model_validate(entry)
    dumped = json.loads(msg.model_dump_json(by_alias=True))
    assert dumped == entry, f"MigrateResult wire drift: {dumped} != {entry}"


@pytest.mark.parametrize(
    "entry",
    _corpus_section("rejects_client_message_unknown_field"),
    ids=lambda e: json.dumps(e, sort_keys=True),
)
def test_corpus_client_message_rejects_unknown_field(entry: dict[str, Any]) -> None:
    with pytest.raises(ValidationError):
        _client_adapter.validate_python(entry)


@pytest.mark.parametrize(
    "entry",
    _corpus_section("rejects_schedule_when_unknown_field"),
    ids=lambda e: json.dumps(e, sort_keys=True),
)
def test_corpus_schedule_when_rejects_unknown_field(entry: dict[str, Any]) -> None:
    adapter = TypeAdapter(ScheduleWhen)
    with pytest.raises(ValidationError):
        adapter.validate_python(entry)


@pytest.mark.parametrize(
    "entry",
    _corpus_section("rejects_workflow_spec_unknown_field"),
    ids=lambda e: json.dumps(e, sort_keys=True),
)
def test_corpus_workflow_spec_rejects_unknown_field(entry: dict[str, Any]) -> None:
    """A bare ``WorkflowSpec`` (the nested-shape pattern of the schedule-when
    reject section — ``ClientMessage`` types ``txn`` as ``dict`` and so cannot
    reject nested step-spec fields at the envelope layer): an unknown field
    inside ``awaitSignal`` must fail ``extra='forbid'``."""
    with pytest.raises(ValidationError):
        WorkflowSpec.model_validate(entry)


@pytest.mark.parametrize(
    "entry",
    _corpus_section("rejects_authed_user_unknown_kind"),
    ids=lambda e: f"kind={e.get('kind')}",
)
def test_corpus_authed_user_rejects_unknown_kind(entry: dict[str, Any]) -> None:
    """ARC-004/QA-008: ``AuthedUser.kind`` is now ``Literal["user","machine"]``;
    a value outside that set must be rejected at parse time."""
    with pytest.raises(ValidationError):
        AuthedUser.model_validate(entry)


@pytest.mark.parametrize(
    "entry",
    _corpus_section("rejects_schedule_info_unknown_kind"),
    ids=lambda e: f"kind={e.get('kind')}",
)
def test_corpus_schedule_info_rejects_unknown_kind(entry: dict[str, Any]) -> None:
    """ARC-004/QA-008: ``ScheduleInfo.kind`` is now ``Literal["oneshot","cron",
    "interval"]``."""
    with pytest.raises(ValidationError):
        ScheduleInfo.model_validate(entry)


@pytest.mark.parametrize(
    "entry",
    _corpus_section("rejects_schedule_info_unknown_status"),
    ids=lambda e: f"status={e.get('status')}",
)
def test_corpus_schedule_info_rejects_unknown_status(entry: dict[str, Any]) -> None:
    """ARC-004/QA-008: ``ScheduleInfo.status`` is now ``Literal["pending",
    "running","paused","error"]``."""
    with pytest.raises(ValidationError):
        ScheduleInfo.model_validate(entry)


# ---------------------------------------------------------------------------
# Migration wire parity (tag "op", camelCase — mirrors server::migrate)
# ---------------------------------------------------------------------------

# (wire_json_string) — every entry must round-trip identically through
# MigrateRequest. Fixtures mirror ``server/src/migrate.rs``'s ``directive_round_trip``
# test: tag ``op``, camelCase keys, ``where`` alias, ``cast`` camelCase literals.
MIGRATE_FIXTURES: list[str] = [
    # renameField with `from` alias.
    '{"directives": [{"op": "renameField", "table": "users", '
    '"from": "name", "to": "fullName"}], "dryRun": false}',
    # changeType with cast literal and explicit default.
    '{"directives": [{"op": "changeType", "table": "users", "field": "age", '
    '"to": {"type": "string"}, "cast": "toString", "default": "0"}], "dryRun": false}',
    # evalExpr with `where` alias.
    '{"directives": [{"op": "evalExpr", "table": "users", "set": "upper", '
    '"expr": "upper(doc->>\'fullName\')", "where": "doc ? \'fullName\'"}], "dryRun": true}',
    # dropTable / dropField / dropIndex / setDefault / renameTable.
    '{"directives": [{"op": "dropTable", "name": "gone"}], "dryRun": false}',
    '{"directives": [{"op": "dropField", "table": "users", "field": "legacy"}], "dryRun": false}',
    '{"directives": [{"op": "dropIndex", "table": "users", "name": "by_email"}], "dryRun": false}',
    '{"directives": [{"op": "setDefault", "table": "users", "field": "role", '
    '"value": "member"}], "dryRun": false}',
    '{"directives": [{"op": "renameTable", "from": "old", "to": "new"}], "dryRun": false}',
]


@pytest.mark.parametrize("fixture", MIGRATE_FIXTURES)
def test_migrate_request_round_trip(fixture: str) -> None:
    """Each migration fixture must round-trip through ``MigrateRequest``
    byte-identically (alias-preserving, discriminator-stable). A failure means
    a wire model drifted from the server contract."""
    from par_rt_db.migration import MigrateRequest

    original = json.loads(fixture)
    parsed = MigrateRequest.model_validate(original)
    dumped = parsed.model_dump(by_alias=True, mode="json")
    assert dumped == original


def test_migrate_request_parses_server_default_null() -> None:
    """The server serializes ``changeType.default`` as ``null`` (via
    ``#[serde(default)]`` with no ``skip_serializing_if``). Our model must accept
    that form (filling in ``None``), even though we omit it when serializing."""
    from par_rt_db.migration import MigrateRequest

    fixture = json.loads(
        '{"directives": [{"op": "changeType", "table": "users", "field": "age", '
        '"to": {"type": "string"}, "cast": "toString", "default": null}], "dryRun": false}'
    )
    parsed = MigrateRequest.model_validate(fixture)
    assert parsed.directives[0].default is None  # type: ignore[union-attr]


# --- ARC-017: error-code parity -----------------------------------------------
#
# `wire-corpus/error-codes.json` is the canonical `{code, httpStatus}` table
# generated from the server's `ErrorCode` enum
# (`server/src/error.rs::tests::error_codes_match_wire_corpus`). `ErrorCode` is
# a `StrEnum`, so `list(ErrorCode)` enumerates every known variant with no
# manually-kept list to fall out of sync; `RtDbError(...).status_code` exposes
# the private `_STATUS` mapping per code.

_ERROR_CODES_PATH = Path(__file__).resolve().parents[2] / "wire-corpus" / "error-codes.json"


def _load_error_codes_corpus() -> list[dict[str, Any]]:
    data = json.loads(_ERROR_CODES_PATH.read_text())
    codes = data.get("codes")
    if not isinstance(codes, list):
        raise TypeError("error-codes.json missing 'codes' array")
    return codes


def test_error_codes_known_to_python_client() -> None:
    """Every corpus code is a known ``ErrorCode`` and the reverse: no
    ``ErrorCode`` member is absent from the corpus."""
    from par_rt_db.errors import ErrorCode

    corpus_codes = sorted(entry["code"] for entry in _load_error_codes_corpus())
    known_codes = sorted(member.value for member in ErrorCode)
    assert corpus_codes == known_codes, "python ErrorCode drifted from wire-corpus/error-codes.json"


def test_error_codes_http_status_matches_corpus() -> None:
    """Each corpus ``httpStatus`` matches ``RtDbError(code, "x").status_code``."""
    from par_rt_db.errors import ErrorCode, RtDbError

    for entry in _load_error_codes_corpus():
        code = ErrorCode(entry["code"])
        err = RtDbError(code, "x")
        assert err.status_code == entry["httpStatus"], (
            f"{code}: python status_code {err.status_code} "
            f"!= corpus httpStatus {entry['httpStatus']}"
        )
