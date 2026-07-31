#!/usr/bin/env python3
"""Change-window approvals - delete deprecated topics only with signed tokens."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from kafka_mcp.approval import mint  # noqa: E402
from examples._common import (  # noqa: E402
    Config,
    KafkaMcpServer,
    banner,
    call,
    data_dir,
    expect_code,
    expect_ok,
    finish,
    load_json,
    load_jsonl,
    step,
    tool_result,
)


def main() -> None:
    ticket = load_json(data_dir(__file__) / "change_ticket.json")
    deprecated = load_jsonl(data_dir(__file__) / "deprecated_topics.jsonl")
    banner("03 · Change-window approvals")
    print(f"Change ticket: {ticket['changeTicket']} window={ticket['window']}")
    print(f"Approver: {ticket['approver']} · Agent: {ticket['agent']}")

    cfg = Config(
        approval_signing_secret=b"example-approval-key",
        allowed_topic_prefixes=["agent."],
        tools_allowed=["create_topic", "delete_topic", "list_topics", "describe_topic"],
        approval_required_tools=["delete_topic"],
    )
    server = KafkaMcpServer(cfg)
    secret = cfg.approval_signing_secret
    session = {"identity": ticket["agent"]}
    ok = True

    step(1, "Register deprecated topics from fixture inventory")
    for row in deprecated:
        server.backend.create_topic(row["name"], config={"owner": row.get("owner", "")})
    print(f"         registered {len(deprecated)} deprecated topics")

    target = ticket["targetTopic"]
    step(2, f"Delete {target} without token - APPROVAL_REQUIRED")
    ok &= expect_code(
        call(server, "delete_topic", {"name": target}, session),
        -32042,
        "delete blocked without approval",
    )

    step(3, "Forged token rejected")
    ok &= expect_code(
        call(
            server,
            "delete_topic",
            {"name": target, "_approval_token": "forged.notreal"},
            session,
        ),
        -32042,
        "forged token rejected",
    )

    step(4, "Approver mints TTL token for change window - delete succeeds")
    token = mint(secret, "delete_topic", ttl=int(ticket["approvalTtlSeconds"]))
    print(f"         minted token for {ticket['changeTicket']}: {token[:36]}…")
    ok &= expect_ok(
        call(server, "delete_topic", {"name": target, "_approval_token": token}, session),
        "approved delete ok",
    )

    step(5, "Confirm target removed; sibling deprecated topic remains")
    topics = (tool_result(call(server, "list_topics", {}, session)) or {}).get("topics") or []
    gone = target not in topics
    sibling = "agent.deprecated.cart.v0" in topics
    print(f"  {'PASS' if gone else 'FAIL'}  target removed")
    print(f"  {'PASS' if sibling else 'FAIL'}  sibling topic retained")
    ok &= gone and sibling

    finish(ok)


if __name__ == "__main__":
    main()
