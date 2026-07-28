"""HMAC-signed, TTL-bounded approval tokens (stateless + optional nonce)."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import time
from typing import Any, Dict, Optional, Set


def mint(
    secret: Optional[bytes],
    tool: str,
    ttl: int = 300,
    *,
    resource: Optional[str] = None,
    principal: Optional[str] = None,
    nonce: Optional[str] = None,
) -> str:
    """Mint base64(payload).hmac bound to tool (+ optional resource/principal/nonce)."""
    if not secret:
        raise ValueError("approval_signing_secret is required to mint tokens")
    payload: Dict[str, Any] = {
        "tool": tool,
        "exp": int(time.time()) + int(ttl),
        "iat": int(time.time()),
        "nonce": nonce or secrets.token_hex(8),
    }
    if resource is not None:
        payload["resource"] = str(resource)
    if principal is not None:
        payload["principal"] = str(principal)
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    b64 = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    sig = hmac.new(secret, b64.encode("ascii"), hashlib.sha256).hexdigest()
    return f"{b64}.{sig}"


def verify(
    secret: Optional[bytes],
    token: str,
    tool: str,
    *,
    resource: Optional[str] = None,
    principal: Optional[str] = None,
    used_nonces: Optional[Set[str]] = None,
) -> bool:
    """Verify signature, tool, expiry, optional resource/principal, and single-use nonce."""
    if not secret or not token or "." not in token:
        return False
    try:
        b64, sig = token.rsplit(".", 1)
        expected = hmac.new(secret, b64.encode("ascii"), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, sig):
            return False
        pad = "=" * (-len(b64) % 4)
        raw = base64.urlsafe_b64decode(b64 + pad)
        payload = json.loads(raw.decode("utf-8"))
        if payload.get("tool") != tool:
            return False
        if int(payload.get("exp", 0)) < int(time.time()):
            return False
        if "resource" in payload:
            if resource is None or str(payload.get("resource")) != str(resource):
                return False
        if "principal" in payload:
            if principal is None or str(payload.get("principal")) != str(principal):
                return False
        nonce = payload.get("nonce")
        if nonce and used_nonces is not None:
            if nonce in used_nonces:
                return False
            used_nonces.add(str(nonce))
        return True
    except Exception:
        return False


def mint_expired(
    secret: Optional[bytes],
    tool: str,
    *,
    resource: Optional[str] = None,
    principal: Optional[str] = None,
) -> str:
    """Test helper: mint an already-expired token."""
    if not secret:
        raise ValueError("approval_signing_secret is required to mint tokens")
    payload: Dict[str, Any] = {
        "tool": tool,
        "exp": int(time.time()) - 10,
        "iat": int(time.time()) - 20,
        "nonce": secrets.token_hex(8),
    }
    if resource is not None:
        payload["resource"] = str(resource)
    if principal is not None:
        payload["principal"] = str(principal)
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    b64 = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    sig = hmac.new(secret, b64.encode("ascii"), hashlib.sha256).hexdigest()
    return f"{b64}.{sig}"
