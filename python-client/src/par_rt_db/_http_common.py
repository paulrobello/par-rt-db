"""Shared helpers used by both :mod:`par_rt_db.http_client` and
:mod:`par_rt_db.aio_http_client`.

These two modules mirror each other one-to-one (sync vs. async), so any piece
of pure, non-request logic they need identically lives here once instead of
being copy-pasted across the pair.
"""

from __future__ import annotations

from typing import Literal


def build_transform_url(
    base: str,
    id: str,
    *,
    w: int | None = None,
    h: int | None = None,
    fit: Literal["cover", "contain", "scale-down"] | None = None,
    q: int | None = None,
    format: Literal["jpeg", "png", "auto"] | None = None,
) -> str:
    """The public serve URL for ``id`` under ``base`` with image-transform params (ENH-014).

    No request is made. Params appear in the deterministic order
    ``w, h, fit, q, format``; unset params (and ``format="auto"``, the server
    default) are omitted.
    """
    parts: list[str] = []
    if w is not None:
        parts.append(f"w={w}")
    if h is not None:
        parts.append(f"h={h}")
    if fit is not None:
        parts.append(f"fit={fit}")
    if q is not None:
        parts.append(f"q={q}")
    # "auto" is the server default — omit so the URL stays minimal (rust parity).
    if format is not None and format != "auto":
        parts.append(f"format={format}")
    url = f"{base}/storage/{id}"
    return f"{url}?{'&'.join(parts)}" if parts else url
