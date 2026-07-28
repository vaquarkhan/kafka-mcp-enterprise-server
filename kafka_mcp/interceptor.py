"""Legacy record redaction hook (kept for compatibility; DLP is primary)."""

from __future__ import annotations

import re
from typing import Any, Dict


_EMAIL = re.compile(r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+")
_SSN = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
_CARD = re.compile(r"\b(?:\d[ -]*?){13,19}\b")


def redact_record(rec: Dict[str, Any]) -> Dict[str, Any]:
    """Best-effort regex redaction of common PII in a record value."""
    out = dict(rec)
    val = out.get("value")
    if isinstance(val, str):
        val = _EMAIL.sub("[REDACTED_EMAIL]", val)
        val = _SSN.sub("[REDACTED_SSN]", val)
        val = _CARD.sub("[REDACTED_CARD]", val)
        out["value"] = val
    return out
