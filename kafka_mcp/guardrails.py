"""Input validation, egress scan, sensitive-topic gating, scrub, anomaly kill-switch."""

from __future__ import annotations

import json
import re
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, Optional, Set

from .dlp import Dlp
from .errors import APPROVAL_REQUIRED, McpError

SENSITIVE_DATA_BLOCKED = -32045
VALIDATION_FAILED = -32046
QUARANTINED = -32047

_IDENT = re.compile(r"^[A-Za-z0-9._-]+$")
_SENSITIVE_CONFIG_KEYS = {
    "sasl.jaas.config",
    "ssl.keystore.password",
    "ssl.key.password",
    "ssl.truststore.password",
    "password",
    "secret",
}


def validate_arguments(tool: str, params: Dict[str, Any], cfg: Any) -> None:
    """Validate identifiers and value size; raise VALIDATION_FAILED (-32046)."""
    for key in ("name", "topic", "groupId", "group_id"):
        if key in params and params[key] is not None:
            val = str(params[key])
            if not _IDENT.match(val):
                raise McpError(
                    VALIDATION_FAILED,
                    f"malformed identifier for {key}",
                    data={"field": key, "value": val},
                )
    if "value" in params and params["value"] is not None:
        raw = params["value"]
        size = len(raw.encode("utf-8")) if isinstance(raw, str) else len(str(raw))
        max_b = getattr(cfg, "max_value_bytes", 1_000_000)
        if size > max_b:
            raise McpError(
                VALIDATION_FAILED,
                "value exceeds max_value_bytes",
                data={"size": size, "max": max_b},
            )


def egress_scan(value: Any, dlp: Dlp) -> None:
    """Block produce of secrets / block-category hits -> SENSITIVE_DATA_BLOCKED."""
    text = value if isinstance(value, str) else (json.dumps(value) if value is not None else "")
    _, hits, blocked = dlp.process(text)
    # For egress we always treat block_categories as blocked regardless of mode
    if blocked or hits & set(dlp.block_categories):
        raise McpError(
            SENSITIVE_DATA_BLOCKED,
            "egress blocked: sensitive data in produce payload",
            data={"categories": sorted(hits)},
        )


def sensitive_topic_requires_approval(
    topic: str,
    patterns: List[str],
    params: Dict[str, Any],
    approval_verify_fn,
    secret: bytes,
    tool: str = "consume_messages",
) -> None:
    """If topic matches sensitive patterns, require valid approval token."""
    if not patterns:
        return
    for pat in patterns:
        if pat and (pat in topic or topic.startswith(pat.rstrip("*")) or _globish(pat, topic)):
            token = params.get("_approval_token")
            if not token or not approval_verify_fn(secret, token, tool):
                raise McpError(
                    APPROVAL_REQUIRED,
                    "sensitive topic requires approval",
                    data={"topic": topic},
                )
            return


def _globish(pattern: str, name: str) -> bool:
    if pattern.endswith("*"):
        return name.startswith(pattern[:-1])
    return pattern == name


def scrub_result(result: Any, dlp: Dlp, cfg: Any) -> Any:
    """Scrub/redact entire result tree; truncate under max_output_bytes."""
    if not getattr(cfg, "scrub_all_outputs", True) and not getattr(cfg, "redaction_enabled", True):
        return _truncate(result, getattr(cfg, "max_output_bytes", 262_144))

    def _walk(obj: Any) -> Any:
        if isinstance(obj, str):
            red, _, _ = dlp.process(obj)
            return red
        if isinstance(obj, list):
            return [_walk(x) for x in obj]
        if isinstance(obj, dict):
            out = {}
            for k, v in obj.items():
                if getattr(cfg, "redact_sensitive_configs", True) and (
                    str(k).lower() in _SENSITIVE_CONFIG_KEYS
                    or "password" in str(k).lower()
                    or "secret" in str(k).lower()
                ):
                    out[k] = "[REDACTED_CONFIG]"
                else:
                    out[k] = _walk(v)
            return out
        return obj

    scrubbed = _walk(result) if getattr(cfg, "redaction_enabled", True) else result
    return _truncate(scrubbed, getattr(cfg, "max_output_bytes", 262_144))


def _truncate(obj: Any, max_bytes: int) -> Any:
    raw = json.dumps(obj, default=str)
    if len(raw.encode("utf-8")) <= max_bytes:
        return obj
    # Prefer truncating records lists if present
    if isinstance(obj, dict) and "records" in obj and isinstance(obj["records"], list):
        records = list(obj["records"])
        while records and len(json.dumps({**obj, "records": records}, default=str).encode("utf-8")) > max_bytes:
            records.pop()
        out = dict(obj)
        out["records"] = records
        out["truncated"] = True
        out["_truncation"] = "max_output_bytes"
        return out
    # Generic string truncation tag
    enc = raw.encode("utf-8")[: max(0, max_bytes - 64)]
    try:
        text = enc.decode("utf-8", errors="ignore")
    except Exception:
        text = ""
    return {"truncated": True, "_truncation": "max_output_bytes", "preview": text}


class AnomalyTracker:
    """Rogue-agent kill-switch: per-identity destructive rate quarantine."""

    def __init__(self, max_per_minute: int = 10) -> None:
        self.max_per_minute = max_per_minute
        self._events: Dict[str, Deque[float]] = defaultdict(deque)
        self._quarantined: Set[str] = set()

    def record_destructive(self, identity: str) -> None:
        now = time.monotonic()
        q = self._events[identity]
        q.append(now)
        while q and now - q[0] > 60.0:
            q.popleft()
        if len(q) >= self.max_per_minute:
            self._quarantined.add(identity)

    def check(self, identity: str) -> None:
        if identity in self._quarantined:
            raise McpError(QUARANTINED, f"identity quarantined: {identity}")
