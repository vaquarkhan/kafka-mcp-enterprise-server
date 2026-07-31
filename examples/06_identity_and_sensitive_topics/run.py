#!/usr/bin/env python3
"""Identity propagation + sensitive-topic gating with payroll fixtures."""

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
    seed_topic_from_jsonl,
    step,
)


def main() -> None:
    scenario = load_json(data_dir(__file__) / "scenario.json")
    banner("06 · Identity propagation + sensitive topics")
    print("Persona: reader principal + security officer approvals")
    print("Goal: ACL deny on create; approval for secret.* consume; PII scrub")

    cfg = Config(
        approval_signing_secret=b"example-approval-key",
        identity_propagation=True,
        allowed_topic_prefixes=["agent.", "secret."],
        sensitive_topic_patterns=scenario["sensitiveTopicPatterns"],
        tools_allowed=[
            "create_topic",
            "produce_message",
            "consume_messages",
            "list_topics",
        ],
        scrub_all_outputs=True,
        redaction_enabled=True,
    )
    server = KafkaMcpServer(cfg)
    for acl in scenario["readerAcls"]:
        server.backend.set_principal_acl(
            scenario["readerIdentity"], acl["operation"], acl["prefix"]
        )

    agent_n = seed_topic_from_jsonl(
        server, scenario["agentTopic"], data_dir(__file__) / "agent_events.jsonl"
    )
    pay_n = seed_topic_from_jsonl(
        server, scenario["sensitiveTopic"], data_dir(__file__) / "secret_payroll.jsonl"
    )
    print(f"Seeded agent_events={agent_n} payroll={pay_n}")

    reader = {"identity": scenario["readerIdentity"]}
    ok = True

    step(1, "Reader can consume agent.events (ACL allows READ)")
    ok &= expect_ok(
        call(
            server,
            "consume_messages",
            {"topic": scenario["agentTopic"], "maxMessages": 10},
            reader,
        ),
        "READ agent.* allowed",
    )

    step(2, "Reader cannot create topics (no CREATE ACL)")
    ok &= expect_code(
        call(server, "create_topic", {"name": "agent.hack"}, reader),
        -32044,
        "CREATE denied via identity propagation",
    )

    step(3, "Sensitive payroll consume without approval - blocked")
    ok &= expect_code(
        call(
            server,
            "consume_messages",
            {"topic": scenario["sensitiveTopic"], "maxMessages": 10},
            reader,
        ),
        -32042,
        "sensitive topic needs approval",
    )

    step(4, "Approved consume - SSN redacted in model-facing output")
    token = mint(cfg.approval_signing_secret, "consume_messages")
    resp = call(
        server,
        "consume_messages",
        {
            "topic": scenario["sensitiveTopic"],
            "maxMessages": 10,
            "_approval_token": token,
        },
        reader,
    )
    ok &= expect_ok(resp, "approved sensitive consume")
    from examples._common import tool_result

    blob = str(tool_result(resp) or {})
    leaked = "123-45-6789" in blob or "987-65-4321" in blob
    print(f"  {'PASS' if not leaked else 'FAIL'}  SSN redacted from payroll consume")
    ok &= not leaked

    finish(ok)


if __name__ == "__main__":
    main()
