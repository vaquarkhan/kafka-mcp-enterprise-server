"""Tamper-resistant (best-effort) in-memory audit sink."""

from __future__ import annotations

import hashlib
import json
import time
from collections import deque
from typing import Any, Deque, Dict, List, Optional


def _truncate_params(params: Any, limit: int = 64) -> Any:
    if isinstance(params, dict):
        out = {}
        for k, v in params.items():
            if isinstance(v, str) and len(v) > limit:
                out[k] = v[:limit] + "…[truncated]"
            elif isinstance(v, dict):
                out[k] = _truncate_params(v, limit)
            else:
                out[k] = v
        return out
    if isinstance(params, str) and len(params) > limit:
        return params[:limit] + "…[truncated]"
    return params


class AuditSink:
    """Ring-buffer audit trail with simple hash chaining."""

    def __init__(self, maxlen: int = 1000) -> None:
        self._buf: Deque[Dict[str, Any]] = deque(maxlen=maxlen)
        self._prev_hash = "0" * 16

    def record(
        self,
        identity: str,
        tool: str,
        params: Any,
        decision: str,
        result: Any = None,
        corr_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        entry = {
            "ts": time.time(),
            "identity": identity,
            "tool": tool,
            "params": _truncate_params(params),
            "decision": decision,
            "result_summary": _truncate_params(
                result if isinstance(result, (str, dict, list, int, float, bool, type(None))) else str(result),
                64,
            ),
            "corr_id": corr_id,
            "prev_hash": self._prev_hash,
        }
        blob = json.dumps(entry, sort_keys=True, default=str).encode("utf-8")
        entry_hash = hashlib.sha256(blob).hexdigest()[:16]
        entry["hash"] = entry_hash
        self._prev_hash = entry_hash
        self._buf.append(entry)
        return entry

    def recent(self, limit: int = 50) -> List[Dict[str, Any]]:
        items = list(self._buf)
        if limit <= 0:
            return []
        return items[-limit:]
