from __future__ import annotations

import base64
import hmac
from typing import Optional, Tuple

from flask import Response, current_app, request


def _parse_basic_auth(auth_header: str) -> Optional[Tuple[str, str]]:
    # Authorization: Basic base64(username:password)
    try:
        scheme, b64 = auth_header.split(" ", 1)
        if scheme.lower() != "basic":
            return None
        raw = base64.b64decode(b64.strip()).decode("utf-8")
        if ":" not in raw:
            return None
        username, password = raw.split(":", 1)
        return username, password
    except Exception:
        return None


def require_basic_auth() -> Optional[Response]:
    settings = current_app.config["AUDIT_SETTINGS"]

    auth_header = request.headers.get("Authorization", "")
    parsed = _parse_basic_auth(auth_header) if auth_header else None
    if not parsed:
        return Response(
            "Authentication required",
            401,
            {"WWW-Authenticate": 'Basic realm="Audit Portal"'},
        )

    username, password = parsed
    ok_user = hmac.compare_digest(username, settings.audit_username)
    ok_pass = hmac.compare_digest(password, settings.audit_password)
    if not (ok_user and ok_pass):
        return Response(
            "Invalid credentials",
            401,
            {"WWW-Authenticate": 'Basic realm="Audit Portal"'},
        )

    return None

