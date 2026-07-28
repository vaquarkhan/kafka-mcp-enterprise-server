#!/usr/bin/env python3
"""Quick 16-check smoke test (not part of the 85)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from kafka_mcp.approval import mint
from kafka_mcp.config import Config
from kafka_mcp.server import KafkaMcpServer, unwrap_tool_result

SECRET = b"smoke-approval-key"


def check(name: str, cond: bool) -> bool:
    print(("PASS" if cond else "FAIL"), name)
    return cond


def domain(resp: dict):
    return unwrap_tool_result(resp.get("result"))


def main() -> int:
    ok = 0
    n = 0

    def expect(name: str, cond: bool) -> None:
        nonlocal ok, n
        n += 1
        if check(name, cond):
            ok += 1

    s = KafkaMcpServer(Config(allowed_topic_prefixes=["agent."], approval_signing_secret=SECRET))
    secret = s.cfg.approval_signing_secret

    r = s.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "create_topic", "arguments": {"name": "agent.smoke"}},
        },
        {},
    )
    expect("create in-scope topic", "result" in r and not (r.get("result") or {}).get("isError"))

    r = s.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "produce_message",
                "arguments": {"topic": "agent.smoke", "value": "hi"},
            },
        },
        {},
    )
    expect("produce", "result" in r)

    s.backend.create_topic("prod.ledger")
    r = s.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "delete_topic",
                "arguments": {
                    "name": "prod.ledger",
                    "_approval_token": mint(secret, "delete_topic"),
                },
            },
        },
        {},
    )
    expect("out-of-scope delete -32041", (r.get("error") or {}).get("code") == -32041)

    s.backend.create_topic("agent.pii")
    s.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "produce_message",
                "arguments": {"topic": "agent.pii", "value": "x me@x.com"},
            },
        },
        {},
    )
    r = s.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "consume_messages",
                "arguments": {"topic": "agent.pii", "maxMessages": 5},
            },
        },
        {},
    )
    val = str(((domain(r) or {}).get("records") or [{}])[0].get("value"))
    expect("PII redaction", "me@x.com" not in val)

    sess = {"tainted_values": {"agent.tainted"}, "integrity": "untrusted"}
    s.backend.create_topic("agent.tainted")
    r = s.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "delete_topic", "arguments": {"name": "agent.tainted"}},
        },
        sess,
    )
    expect("taint -32040", (r.get("error") or {}).get("code") == -32040)

    r = s.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "delete_topic",
                "arguments": {
                    "name": "agent.tainted",
                    "_approval_token": mint(secret, "delete_topic"),
                },
            },
        },
        sess,
    )
    expect("delete with approval OK", "result" in r)

    s.backend.create_topic("agent.need")
    r = s.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "delete_topic", "arguments": {"name": "agent.need"}},
        },
        {},
    )
    expect("destructive without token -32042", (r.get("error") or {}).get("code") == -32042)

    r = s.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "delete_topic",
                "arguments": {"name": "agent.need", "_approval_token": "forged.bad"},
            },
        },
        {},
    )
    expect("forged token -32042", (r.get("error") or {}).get("code") == -32042)

    s2 = KafkaMcpServer(Config(readonly=True, approval_signing_secret=SECRET))
    r = s2.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "create_topic", "arguments": {"name": "agent.ro"}},
        },
        {},
    )
    expect("write on read-only denied", (r.get("error") or {}).get("code") == -32044)
    s2.backend.create_topic("agent.ro")
    r = s2.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "list_topics", "arguments": {}},
        },
        {},
    )
    expect("read on read-only allowed", "result" in r)

    s3 = KafkaMcpServer(Config(tools_denied=["list_topics"], approval_signing_secret=SECRET))
    r = s3.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "list_topics", "arguments": {}},
        },
        {},
    )
    expect("deny-listed -32044", (r.get("error") or {}).get("code") == -32044)

    s4 = KafkaMcpServer(
        Config(
            rate_requests_per_second=1,
            rate_admin_requests_per_second=1,
            approval_signing_secret=SECRET,
        )
    )
    codes = []
    for _ in range(5):
        rr = s4.handle(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "list_topics", "arguments": {}},
            },
            {},
        )
        codes.append((rr.get("error") or {}).get("code"))
    expect("rate limit -32029", -32029 in codes)

    s5 = KafkaMcpServer(
        Config(policy_engine=lambda *a, **k: False, approval_signing_secret=SECRET)
    )
    r = s5.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "list_topics", "arguments": {}},
        },
        {},
    )
    expect("policy deny -32044", (r.get("error") or {}).get("code") == -32044)

    def boom(*a, **k):
        raise RuntimeError("x")

    s6 = KafkaMcpServer(Config(policy_engine=boom, approval_signing_secret=SECRET))
    r = s6.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "list_topics", "arguments": {}},
        },
        {},
    )
    expect("policy error fails closed -32044", (r.get("error") or {}).get("code") == -32044)

    s7 = KafkaMcpServer(Config(approval_signing_secret=SECRET))
    s7.backend._inject_dependency_failure(True)
    codes = []
    for _ in range(4):
        rr = s7.handle(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "describe_cluster", "arguments": {}},
            },
            {},
        )
        codes.append((rr.get("error") or {}).get("code"))
    expect("dependency failure -32043", -32043 in codes)

    s8 = KafkaMcpServer(Config(approval_signing_secret=SECRET))
    s8.handle(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "list_topics", "arguments": {}},
        },
        {"identity": "smoke-auditor"},
    )
    recent = s8.audit.recent(5)
    expect(
        "audit records",
        any(e.get("identity") == "smoke-auditor" for e in recent),
    )

    print(f"\nSmoke: {ok}/{n} passed")
    return 0 if ok == 16 else 1


if __name__ == "__main__":
    sys.exit(main())
