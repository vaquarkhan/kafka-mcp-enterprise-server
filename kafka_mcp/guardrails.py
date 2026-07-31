"""Input validation, egress scan, sensitive-topic gating, scrub, anomaly kill-switch."""

from __future__ import annotations

import fnmatch
import json
import re
import time
from collections import defaultdict, deque
from typing import Any, Deque, Dict, List, Optional, Set

from .dlp import Dlp
from .errors import (
    APPROVAL_REQUIRED,
    QUARANTINED,
    SENSITIVE_DATA_BLOCKED,
    VALIDATION_FAILED,
    McpError,
)

# Kafka topic / name identifiers (strict)
_TOPIC_IDENT = re.compile(r"^[A-Za-z0-9._-]+$")
# Consumer group IDs allow a wider set (Kafka permits printable UTF-8; keep a safe subset)
_GROUP_IDENT = re.compile(r"^[A-Za-z0-9._\-:/=+@]+$")
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
    for key in ("name", "topic"):
        if key in params and params[key] is not None:
            val = str(params[key])
            if not _TOPIC_IDENT.match(val):
                raise McpError(
                    VALIDATION_FAILED,
                    f"malformed identifier for {key}",
                    data={"field": key, "value": val},
                )
    for key in ("groupId", "group_id"):
        if key in params and params[key] is not None:
            val = str(params[key])
            if not _GROUP_IDENT.match(val):
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
    if blocked:
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
    secret: Optional[bytes],
    tool: str = "consume_messages",
    *,
    principal: Optional[str] = None,
    used_nonces: Optional[Set[str]] = None,
) -> None:
    """If topic matches sensitive patterns, require valid approval token."""
    if not patterns:
        return
    for pat in patterns:
        if not pat:
            continue
        matched = (
            fnmatch.fnmatch(topic, pat)
            or topic.startswith(pat.rstrip("*"))
            or pat.rstrip("*") == topic
        )
        if matched:
            token = params.get("_approval_token")
            if not token or not approval_verify_fn(
                secret,
                token,
                tool,
                resource=topic,
                principal=principal,
                used_nonces=used_nonces,
            ):
                raise McpError(
                    APPROVAL_REQUIRED,
                    "sensitive topic requires approval",
                    data={"topic": topic},
                )
            return


def scrub_result(result: Any, dlp: Dlp, cfg: Any) -> Any:
    """Scrub/redact result; default is payloads-only (message values), not full trees."""
    max_out = getattr(cfg, "max_output_bytes", 262_144)
    redaction_on = getattr(cfg, "redaction_enabled", True)
    scrub_all = getattr(cfg, "scrub_all_outputs", False)
    payloads_only = getattr(cfg, "scrub_payloads_only", True)

    if not redaction_on and not scrub_all:
        return _truncate(result, max_out)

    if scrub_all and redaction_on:
        scrubbed = _walk_all(result, dlp, cfg)
        return _truncate(scrubbed, max_out)

    if payloads_only and redaction_on and isinstance(result, dict):
        out = dict(result)
        if "records" in out and isinstance(out["records"], list):
            recs = []
            for rec in out["records"]:
                if isinstance(rec, dict) and "value" in rec and isinstance(rec["value"], str):
                    red, _, _ = dlp.process(rec["value"])
                    rec = dict(rec)
                    rec["value"] = red
                recs.append(rec)
            out["records"] = recs
        if getattr(cfg, "redact_sensitive_configs", True) and isinstance(out.get("config"), dict):
            cfg_out = {}
            for k, v in out["config"].items():
                if (
                    str(k).lower() in _SENSITIVE_CONFIG_KEYS
                    or "password" in str(k).lower()
                    or "secret" in str(k).lower()
                ):
                    cfg_out[k] = "[REDACTED_CONFIG]"
                else:
                    cfg_out[k] = v
            out["config"] = cfg_out
        return _truncate(out, max_out)

    return _truncate(result, max_out)


def _walk_all(obj: Any, dlp: Dlp, cfg: Any) -> Any:
    if isinstance(obj, str):
        red, _, _ = dlp.process(obj)
        return red
    if isinstance(obj, list):
        return [_walk_all(x, dlp, cfg) for x in obj]
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
                out[k] = _walk_all(v, dlp, cfg)
        return out
    return obj


def _truncate(obj: Any, max_bytes: int) -> Any:
    raw = json.dumps(obj, default=str)
    if len(raw.encode("utf-8")) <= max_bytes:
        return obj
    if isinstance(obj, dict) and "records" in obj and isinstance(obj["records"], list):
        records = list(obj["records"])
        while records and len(
            json.dumps({**obj, "records": records}, default=str).encode("utf-8")
        ) > max_bytes:
            records.pop()
        out = dict(obj)
        out["records"] = records
        out["truncated"] = True
        out["_truncation"] = "max_output_bytes"
        return out
    # C4: preserve top-level shape - attach preview sibling instead of replacing object
    if isinstance(obj, dict):
        enc = raw.encode("utf-8")[: max(0, max_bytes - 128)]
        text = enc.decode("utf-8", errors="ignore")
        out = dict(obj)
        out["truncated"] = True
        out["_truncation"] = "max_output_bytes"
        out["preview"] = text
        return out
    enc = raw.encode("utf-8")[: max(0, max_bytes - 64)]
    text = enc.decode("utf-8", errors="ignore")
    return {"truncated": True, "_truncation": "max_output_bytes", "preview": text}


class AnomalyTracker:
    """Rogue-agent kill-switch: per-identity destructive rate quarantine (in-process)."""

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
