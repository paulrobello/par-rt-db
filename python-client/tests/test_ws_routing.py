"""No-socket unit tests for the reactive WS client (framing, backoff, dedup, routing)."""

from par_rt_db.ws_client import _backoff_delay, _canonical_key, _sync_url


def test_sync_url_flips_scheme_and_appends_sync():
    assert _sync_url("http://localhost:8300") == "ws://localhost:8300/sync"
    assert _sync_url("https://rtdb.pardev.net") == "wss://rtdb.pardev.net/sync"
    assert _sync_url("ws://localhost:8300/") == "ws://localhost:8300/sync"
    assert _sync_url("wss://rtdb.pardev.net///") == "wss://rtdb.pardev.net/sync"


def test_canonical_key_is_order_independent():
    a = {"table": "items", "index": "by_x", "eq": [1]}
    b = {"index": "by_x", "eq": [1], "table": "items"}
    assert _canonical_key(a) == _canonical_key(b)


def test_canonical_key_distinguishes_different_shapes():
    assert _canonical_key({"table": "a"}) != _canonical_key({"table": "b"})


def test_backoff_delay_is_bounded_and_jittered():
    base, top = 0.5, 15.0
    # rand = 0.5 -> multiplier (0.5 + 0.5*0.5) = 0.75 of the raw cap.
    assert _backoff_delay(0, base, top, 0.5) == 0.375
    # attempt grows exponentially until capped at `top`; jitter in [0.5, 1.0] of raw.
    raw = min(top, base * (2**5))
    lo, hi = raw * 0.5, raw * 1.0
    assert lo <= _backoff_delay(5, base, top, 0.0) <= hi
    # never exceeds `top`.
    assert _backoff_delay(50, base, top, 1.0) <= top
