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
    """ARC-004/QA-008: ``ScheduleInfo.kind`` is now ``Literal["oneshot","cron"]``."""
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
