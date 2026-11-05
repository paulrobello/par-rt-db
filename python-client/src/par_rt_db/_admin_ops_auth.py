"""Credential admin request builders — machine tokens (ENH-005), enrollment
grants (host-machine-auth), interactive sessions, and user merges. Split from
``admin.py`` (QA-003b pure move)."""

from __future__ import annotations

from typing import Any

from ._admin_request import (
    _AdminRequest,
    _parse_expect_ok,
    _parse_model,
    _parse_model_list,
    _parse_revoked_count,
)
from .admin_models import (
    EnrollmentGrant,
    HostCapability,
    MergeReport,
    MintedToken,
    SessionInfo,
    TokenInfo,
)


def _op_mint_token(
    db: str,
    name: str,
    *,
    expires_at: int | None = None,
    read_only: bool = False,
    tables: list[str] | None = None,
    host_capability: HostCapability | None = None,
) -> _AdminRequest:
    body: dict[str, Any] = {"db": db, "name": name, "readOnly": read_only}
    if expires_at is not None:
        body["expiresAt"] = expires_at
    if tables is not None:
        body["tables"] = list(tables)
    if host_capability is not None:
        body["hostCapability"] = host_capability.model_dump(by_alias=True, mode="json")
    return _AdminRequest("POST", "/admin/mint-token", {"json": body}, _parse_model(MintedToken))


def _op_revoke_token(token_id: str) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        "/admin/revoke-token",
        {"json": {"tokenId": token_id}},
        _parse_expect_ok,
    )


# --- enrollment grants (host-machine-auth) ----------------------------------
#
# POST /admin/enrollment-grants (create, admin bearer) +
# POST /admin/revoke-enrollment-grant (revoke, admin bearer). The public
# redemption route, POST /auth/enroll, is NOT an admin route — it lives on the
# data-plane HTTP clients next to /auth/me (see par_rt_db.http_client).


def _op_create_enrollment_grant(
    db: str,
    name: str,
    capability: HostCapability,
    expires_at: int,
    *,
    token_expires_at: int | None = None,
) -> _AdminRequest:
    body: dict[str, Any] = {
        "db": db,
        "name": name,
        "capability": capability.model_dump(by_alias=True, mode="json"),
        "expiresAt": expires_at,
    }
    if token_expires_at is not None:
        body["tokenExpiresAt"] = token_expires_at
    return _AdminRequest(
        "POST",
        "/admin/enrollment-grants",
        {"json": body},
        _parse_model(EnrollmentGrant),
    )


def _op_revoke_enrollment_grant(grant_id: str) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        "/admin/revoke-enrollment-grant",
        {"json": {"grantId": grant_id}},
        _parse_expect_ok,
    )


def _op_list_tokens(db: str) -> _AdminRequest:
    return _AdminRequest(
        "GET", "/admin/tokens", {"params": {"db": db}}, _parse_model_list(TokenInfo, "tokens")
    )


def _op_list_sessions(
    *,
    user: str | None = None,
    limit: int | None = None,
) -> _AdminRequest:
    params: dict[str, Any] = {}
    if user is not None:
        params["user"] = user
    if limit is not None:
        params["limit"] = limit
    return _AdminRequest(
        "GET", "/admin/sessions", {"params": params}, _parse_model_list(SessionInfo, "sessions")
    )


def _op_revoke_session(token_hash: str) -> _AdminRequest:
    return _AdminRequest("DELETE", f"/admin/sessions/{token_hash}", {}, _parse_expect_ok)


def _op_revoke_user_sessions(user_id: str) -> _AdminRequest:
    return _AdminRequest(
        "DELETE",
        "/admin/sessions",
        {"params": {"user": user_id}},
        _parse_revoked_count,
    )


def _op_revoke_expired_sessions() -> _AdminRequest:
    return _AdminRequest(
        "DELETE",
        "/admin/sessions",
        {"params": {"expired": "true"}},
        _parse_revoked_count,
    )


def _op_merge_users(anon_user_id: str, real_user_id: str) -> _AdminRequest:
    return _AdminRequest(
        "POST",
        "/admin/merge-users",
        {
            "json": {
                "anonUserId": anon_user_id,
                "realUserId": real_user_id,
                "confirm": real_user_id,
            }
        },
        _parse_model(MergeReport),
    )
