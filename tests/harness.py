"""Shared test harness for kafka_mcp conformance suite."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from kafka_mcp.approval import mint, mint_expired  # noqa: F401,E402
from kafka_mcp.config import Config  # noqa: E402
from kafka_mcp.server import KafkaMcpServer, unwrap_tool_result  # noqa: E402

TEST_APPROVAL_SECRET = b"test-approval-key"


class Checker:
    def __init__(self, category: str) -> None:
        self.category = category
        self.passed = 0
        self.failed = 0
        self.failures: List[str] = []

    def check(self, name: str, cond: bool, detail: str = "") -> None:
        if cond:
            self.passed += 1
            print(f"  PASS  {name}")
        else:
            self.failed += 1
            msg = f"  FAIL  {name}" + (f" - {detail}" if detail else "")
            print(msg)
            self.failures.append(msg)

    def summary(self) -> Tuple[int, int]:
        return self.passed, self.failed


def new_server(**kwargs: Any) -> KafkaMcpServer:
    if "approval_signing_secret" not in kwargs:
        kwargs["approval_signing_secret"] = TEST_APPROVAL_SECRET
    # Quiet insecure-default warnings in the harness unless testing them.
    logging.getLogger("kafka_mcp.server").setLevel(logging.ERROR)
    cfg = Config(**kwargs)
    return KafkaMcpServer(cfg)


def rpc(
    server: KafkaMcpServer,
    method: str,
    params: Optional[Dict] = None,
    session: Optional[Dict] = None,
) -> Dict:
    req = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params or {}}
    return server.handle(req, session if session is not None else {})


def call(
    server: KafkaMcpServer,
    tool: str,
    arguments: Optional[Dict] = None,
    session: Optional[Dict] = None,
) -> Dict:
    return rpc(
        server,
        "tools/call",
        {"name": tool, "arguments": arguments or {}},
        session,
    )


def err_code(resp: Dict) -> Optional[int]:
    err = resp.get("error")
    if not err:
        return None
    return err.get("code")


def result(resp: Dict) -> Any:
    """Return domain payload (unwraps MCP content wrapper when present)."""
    raw = resp.get("result")
    return unwrap_tool_result(raw)


def has_result(resp: Dict) -> bool:
    if "error" in resp:
        return False
    if "result" not in resp:
        return False
    r = resp["result"]
    if isinstance(r, dict) and r.get("isError") is True:
        return False
    return True


def mcp_content_ok(resp: Dict) -> bool:
    """True when tools/call result is MCP content-shaped and not isError."""
    r = resp.get("result")
    if not isinstance(r, dict):
        return False
    if r.get("isError"):
        return False
    content = r.get("content")
    return isinstance(content, list) and len(content) >= 1
