import base64
import json

import pytest

from par_rt_db.cursor import decode_cursor, encode_cursor


def test_round_trip_mixed_types():
    values = ["a", 3, 1.5, None, True]
    cur = encode_cursor(values)
    assert isinstance(cur, str)
    assert decode_cursor(cur) == values


def test_empty():
    assert decode_cursor(encode_cursor([])) == []


def test_decode_rejects_garbage():
    with pytest.raises(ValueError):
        decode_cursor("not-valid-base64-or-json!!!")


def test_decode_rejects_non_array():
    blob = base64.b64encode(json.dumps({"not": "array"}).encode()).decode()
    with pytest.raises(ValueError):
        decode_cursor(blob)
