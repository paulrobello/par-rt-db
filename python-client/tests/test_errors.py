import pytest

from par_rt_db.errors import ErrorCode, RtDbError, retry_on_precondition


def test_envelope_round_trip():
    err = RtDbError.from_envelope({"code": "NOT_FOUND", "message": "no doc"})
    assert err.code is ErrorCode.NOT_FOUND
    assert err.message == "no doc"
    assert err.status_code == 404
    assert "NOT_FOUND" in str(err)


def test_envelope_unknown_code_falls_back_to_internal():
    err = RtDbError.from_envelope({"code": "WAT", "message": "x"})
    assert err.code is ErrorCode.INTERNAL
    assert err.status_code == 500


def test_conflict_code_is_string_CONFLICT_and_maps_to_409():
    """``CONFLICT`` is the wire string ``"CONFLICT"`` (byte-for-byte with the
    server/TS/Rust clients) and maps to HTTP 409 — same status as
    ``PRECONDITION_FAILED`` but a distinct code."""
    assert ErrorCode.CONFLICT.value == "CONFLICT"
    err = RtDbError.from_envelope({"code": "CONFLICT", "message": "unique index 'i' violated"})
    assert err.code is ErrorCode.CONFLICT
    assert err.status_code == 409


def test_from_http_parses_body():
    err = RtDbError.from_http(422, b'{"code":"SCHEMA_VIOLATION","message":"bad"}')
    assert err.code is ErrorCode.SCHEMA_VIOLATION
    assert err.status_code == 422


def test_from_http_envelope_code_wins_over_status():
    # The server sends a status consistent with the code; if they ever differ,
    # the body's {code,message} is authoritative.
    err = RtDbError.from_http(500, b'{"code":"SCHEMA_VIOLATION","message":"bad"}')
    assert err.code is ErrorCode.SCHEMA_VIOLATION
    assert err.status_code == 422


def test_from_http_non_json_body_is_internal():
    err = RtDbError.from_http(500, b"<html>boom</html>")
    assert err.code is ErrorCode.INTERNAL
    assert err.status_code == 500
    assert "500" in err.message


@pytest.mark.asyncio
async def test_retry_on_precondition_succeeds_after_precondition_failed():
    calls = {"n": 0}

    async def fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RtDbError(ErrorCode.PRECONDITION_FAILED, "version mismatch")
        return "ok"

    out = await retry_on_precondition(fn, max_attempts=5)
    assert out == "ok"
    assert calls["n"] == 3


@pytest.mark.asyncio
async def test_retry_on_precondition_does_not_retry_other_errors():
    async def fn():
        raise RtDbError(ErrorCode.NOT_FOUND, "missing")

    with pytest.raises(RtDbError) as ei:
        await retry_on_precondition(fn, max_attempts=5)
    assert ei.value.code is ErrorCode.NOT_FOUND


@pytest.mark.asyncio
async def test_retry_on_precondition_exhausts_attempts():
    async def fn():
        raise RtDbError(ErrorCode.PRECONDITION_FAILED, "nope")

    with pytest.raises(RtDbError) as ei:
        await retry_on_precondition(fn, max_attempts=3)
    assert ei.value.code is ErrorCode.PRECONDITION_FAILED


def test_rate_limited_envelope_carries_retry_after():
    err = RtDbError.from_envelope(
        {"code": "RATE_LIMITED", "message": "rate limit exceeded", "retryAfter": 42}
    )
    assert err.code is ErrorCode.RATE_LIMITED
    assert err.status_code == 429
    assert err.retry_after == 42


def test_envelope_without_retry_after_leaves_it_none():
    err = RtDbError.from_envelope({"code": "NOT_FOUND", "message": "x"})
    assert err.retry_after is None
