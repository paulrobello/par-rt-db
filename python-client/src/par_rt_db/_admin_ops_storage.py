"""Admin file-storage request builders (list / raw upload / delete / signed
URL), split from ``admin.py`` (QA-003b pure move)."""

from __future__ import annotations

from typing import Any

from ._admin_request import _AdminRequest, _parse_expect_ok, _parse_model, _parse_model_list
from .admin_models import FileMetadata, SignedUrl

# --- admin file storage ---
#
# GET|POST /admin/db/{db}/storage + DELETE .../{id}. The upload body is the
# file itself (raw bytes, not JSON); ``content_type`` sets the ``Content-Type``
# header and is left unset when ``None`` (the server then stores the blob
# untyped) — the same convention as the data-plane ``RtDbHttpClient.upload``.


def _op_admin_list_files(db: str) -> _AdminRequest:
    return _AdminRequest(
        "GET", f"/admin/db/{db}/storage", {}, _parse_model_list(FileMetadata, "files")
    )


def _op_admin_upload_file(
    db: str,
    data: bytes,
    *,
    content_type: str | None = None,
) -> _AdminRequest:
    headers = {"Content-Type": content_type} if content_type is not None else None
    return _AdminRequest(
        "POST",
        f"/admin/db/{db}/storage",
        {"content": data, "headers": headers},
        lambda resp: str(resp.json()["id"]),
    )


def _op_admin_delete_file(db: str, id: str) -> _AdminRequest:
    return _AdminRequest("DELETE", f"/admin/db/{db}/storage/{id}", {}, _parse_expect_ok)


def _op_get_signed_url(
    db: str,
    id: str,
    *,
    w: int | None = None,
    h: int | None = None,
    fit: str | None = None,
    q: int | None = None,
    format: str | None = None,
) -> _AdminRequest:
    """Mint a signed, time-limited URL for a storage blob. The transform
    params ride the mint so the server binds them into the signature
    (SEC-003); ``format="auto"`` is the server default and is omitted."""
    params: dict[str, Any] = {}
    if w is not None:
        params["w"] = w
    if h is not None:
        params["h"] = h
    if fit is not None:
        params["fit"] = fit
    if q is not None:
        params["q"] = q
    if format is not None and format != "auto":
        params["format"] = format
    return _AdminRequest(
        "GET",
        f"/api/storage/{db}/{id}/signed-url",
        {"params": params or None},
        _parse_model(SignedUrl),
    )
