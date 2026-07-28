"""HMAC-signed, TTL-bounded approval tokens (stateless)."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Optional


def mint(secret: bytes, tool: str, ttl: int = 300) -> str:
    """Mint base64(payload).hmac for the given tool."""
    payload = {
        "tool": tool,
        "exp": int(time.time()) + int(ttl),
        "iat": int(time.time()),
    }
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    b64 = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    sig = hmac.new(secret, b64.encode("ascii"), hashlib.sha256).hexdigest()
    return f"{b64}.{sig}"


def verify(secret: bytes, token: str, tool: str) -> bool:
    """Verify signature, tool name, and expiry. Returns False on any failure."""
    try:
        if not token or "." not in token:
            return False
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
        return True
    except Exception:
        return False


def mint_expired(secret: bytes, tool: str) -> str:
    """Test helper: mint an already-expired token."""
    payload = {
        "tool": tool,
        "exp": int(time.time()) - 10,
        "iat": int(time.time()) - 310,
    }
    raw = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")
    b64 = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
    sig = hmac.new(secret, b64.encode("ascii"), hashlib.sha256).hexdigest()
    return f"{b64}.{sig}"
