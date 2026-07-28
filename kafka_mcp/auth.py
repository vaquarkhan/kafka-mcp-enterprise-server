"""Bearer audience/issuer validation (HTTP / OAuth path)."""

from __future__ import annotations

from typing import Any, Dict, Optional

from .errors import UNAUTHORIZED, McpError


def validate_bearer(cfg: Any, claims: Optional[Dict[str, Any]]) -> bool:
    """Validate bearer claims against configured audience/issuer.

    Auth is off when neither audience nor issuer is configured.
    Raises UNAUTHORIZED (-32001) on failure.
    """
    expected_aud = getattr(cfg, "oauth_expected_audience", None)
    expected_iss = getattr(cfg, "oauth_expected_issuer", None)
    if not expected_aud and not expected_iss:
        return True
    if not claims:
        raise McpError(UNAUTHORIZED, "missing bearer claims")
    if expected_aud:
        aud = claims.get("aud")
        if isinstance(aud, list):
            ok = expected_aud in aud
        else:
            ok = aud == expected_aud
        if not ok:
            raise McpError(UNAUTHORIZED, "invalid audience")
    if expected_iss:
        if claims.get("iss") != expected_iss:
            raise McpError(UNAUTHORIZED, "invalid issuer")
    return True
