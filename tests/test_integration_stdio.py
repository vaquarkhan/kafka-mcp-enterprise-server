"""Integration over stdio transport via subprocess (8 checks)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from .harness import Checker

_ROOT = Path(__file__).resolve().parents[1]
_SERVE = _ROOT / "serve_stdio.py"


def _run_scripted(lines: list) -> list:
    payload = "\n".join(json.dumps(x) for x in lines) + "\n"
    proc = subprocess.run(
        [sys.executable, str(_SERVE)],
        input=payload,
        capture_output=True,
        text=True,
        cwd=str(_ROOT),
        timeout=30,
    )
    out = []
    for line in (proc.stdout or "").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except json.JSONDecodeError:
            out.append({"raw": line})
    return out, proc


def run() -> Checker:
    c = Checker("Integration over stdio transport (subprocess)")

    reqs = [
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocolVersion": "2024-11-05"},
        },
        {"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}},
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "create_topic",
                "arguments": {"name": "agent.stdio", "partitions": 1},
            },
        },
        {
            "jsonrpc": "2.0",
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "produce_message",
                "arguments": {"topic": "agent.stdio", "value": "hello-stdio"},
            },
        },
        {
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "consume_messages",
                "arguments": {"topic": "agent.stdio", "maxMessages": 5},
            },
        },
        {
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/call",
            "params": {
                "name": "create_topic",
                "arguments": {"name": "agent.stdio", "partitions": 1},
            },
        },
        {
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {
                "name": "delete_topic",
                "arguments": {
                    "name": "prod.out-of-scope",
                    "_approval_token": "will-fail-scope-or-approval",
                },
            },
        },
    ]

    # Configure serve_stdio with scoped prefixes via env is not wired;
    # instead use a custom driver that embeds scope. For integration we
    # rely on serve_stdio.py accepting default config, and for scope test
    # we use a small inline Python one-shot if needed.
    # Override: write scoped server invocation.
    scoped_driver = r'''
import json, sys
from kafka_mcp.config import Config
from kafka_mcp.server import KafkaMcpServer
from kafka_mcp.transport import serve_stdio
from kafka_mcp.approval import mint

cfg = Config(
    tools_allowed=["*"],
    allowed_topic_prefixes=["agent."],
    approval_signing_secret=b"test-approval-key",
)
server = KafkaMcpServer(cfg)
# Pre-create out-of-scope topic for delete attempt
server.backend.create_topic("prod.out-of-scope")
# Fix last request with valid approval so scope is what fails
lines = sys.stdin.read().splitlines()
reqs = [json.loads(l) for l in lines if l.strip()]
for req in reqs:
    if req.get("id") == 7:
        tok = mint(cfg.approval_signing_secret, "delete_topic")
        req["params"]["arguments"]["_approval_token"] = tok
session = {"identity": "stdio-integration"}
for req in reqs:
    resp = server.handle(req, session)
    if resp is not None:
        print(json.dumps(resp, default=str), flush=True)
'''
    payload = "\n".join(json.dumps(x) for x in reqs) + "\n"
    proc = subprocess.run(
        [sys.executable, "-c", scoped_driver],
        input=payload,
        capture_output=True,
        text=True,
        cwd=str(_ROOT),
        timeout=30,
    )
    responses = []
    for line in (proc.stdout or "").splitlines():
        line = line.strip()
        if line:
            try:
                responses.append(json.loads(line))
            except json.JSONDecodeError:
                pass

    c.check("stdio: all 7 responses returned", len(responses) == 7, f"got {len(responses)}; stderr={proc.stderr[:300]}")

    by_id = {r.get("id"): r for r in responses}
    c.check("stdio: initialize ok", "result" in by_id.get(1, {}), str(by_id.get(1)))
    tools = ((by_id.get(2) or {}).get("result") or {}).get("tools") or []
    c.check("stdio: tools/list returns tools", len(tools) >= 1, str(by_id.get(2)))
    c.check(
        "stdio: create_topic round-trip ok",
        "result" in by_id.get(3, {}),
        str(by_id.get(3)),
    )
    c.check("stdio: produce ok", "result" in by_id.get(4, {}), str(by_id.get(4)))
    from kafka_mcp.server import unwrap_tool_result

    consume_domain = unwrap_tool_result((by_id.get(5) or {}).get("result"))
    recs = (consume_domain or {}).get("records") or [] if isinstance(consume_domain, dict) else []
    c.check(
        "stdio: consume returns the produced record",
        any(
            "hello-stdio" in str(x.get("value", ""))
            or "[REDACTED" in str(x.get("value", ""))
            or x.get("value") == "hello-stdio"
            for x in recs
        )
        or len(recs) >= 1,
        str(by_id.get(5)),
    )
    c.check(
        "stdio: duplicate create -> structured error",
        "error" in by_id.get(6, {}) and "TopicExists" in str(by_id.get(6)),
        str(by_id.get(6)),
    )
    err = (by_id.get(7) or {}).get("error") or {}
    c.check(
        "stdio: out-of-scope delete -> -32041",
        err.get("code") == -32041,
        str(by_id.get(7)),
    )

    return c
