"""Shared test harness for kafka_mcp conformance suite."""

from __future__ import annotations

import json
import sys
import traceback
from typing import Any, Callable, Dict, List, Optional, Tuple

# Ensure project root is on path when running from tests/
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from kafka_mcp.approval import mint, mint_expired, verify  # noqa: E402
from kafka_mcp.config import Config  # noqa: E402
from kafka_mcp.errors import McpError  # noqa: E402
from kafka_mcp.server import KafkaMcpServer  # noqa: E402


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
            msg = f"  FAIL  {name}" + (f" — {detail}" if detail else "")
            print(msg)
            self.failures.append(msg)

    def summary(self) -> Tuple[int, int]:
        return self.passed, self.failed


def new_server(**kwargs: Any) -> KafkaMcpServer:
    cfg = Config(**kwargs) if kwargs else Config()
    return KafkaMcpServer(cfg)


def rpc(server: KafkaMcpServer, method: str, params: Optional[Dict] = None, session: Optional[Dict] = None) -> Dict:
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
    return resp.get("result")


def has_result(resp: Dict) -> bool:
    return "result" in resp and "error" not in resp
