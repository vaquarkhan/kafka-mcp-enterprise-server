#!/usr/bin/env python3
"""Support-copilot DLP - block secret egress; redact PII from case traffic."""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

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
    attempts = load_json(data_dir(__file__) / "egress_attempts.json")
    banner("04 · PII / secret egress guard")
    print("Persona: copilot:support")
    print("Goal: DLP on produce + scrub PII on consume using fixture case data")

    cfg = Config(
        approval_signing_secret=b"example-approval-key",
        allowed_topic_prefixes=["support."],
        tools_allowed=["create_topic", "produce_message", "consume_messages"],
        dlp_mode="redact",
        dlp_block_categories=["private_key", "aws_access_key", "jwt"],
        scrub_all_outputs=True,
        redaction_enabled=True,
    )
    server = KafkaMcpServer(cfg)
    session = {"identity": "copilot:support"}
    ok = True

    step(1, "Create topic and seed support cases from JSONL")
    call(server, "create_topic", {"name": "support.cases"}, session)
    n = seed_topic_from_jsonl(
        server, "support.cases", data_dir(__file__) / "support_cases.jsonl"
    )
    # seed_topic_from_jsonl creates if missing; topic already created - produce only happened
    print(f"         seeded {n} case records")

    step(2, "Produce allowed resolution event")
    ok &= expect_ok(
        call(
            server,
            "produce_message",
            {
                "topic": "support.cases",
                "value": json.dumps(attempts["allowedEvent"]),
            },
            session,
        ),
        "normal produce ok",
    )

    step(3, "Block secret egress attempts from fixture")
    for item in attempts["blockedSecrets"]:
        ok &= expect_code(
            call(
                server,
                "produce_message",
                {"topic": "support.cases", "value": item["value"]},
                session,
            ),
            -32045,
            f"blocked {item['label']}",
        )

    step(4, "Consume - email/phone from fixture must be redacted")
    resp = call(
        server,
        "consume_messages",
        {"topic": "support.cases", "maxMessages": 50},
        session,
    )
    ok &= expect_ok(resp, "consume ok")
    from examples._common import tool_result

    domain = tool_result(resp) or {}
    blob = str(domain)
    leaked = "jane.doe@example.com" in blob or "415-555-0199" in blob
    print(f"  {'PASS' if not leaked else 'FAIL'}  PII not present in consume result")
    ok &= not leaked
    for rec in domain.get("records") or []:
        val = str(rec.get("value") or "")
        if "REDACTED" in val:
            print(f"         redacted sample: {val[:120]}…")
            break

    finish(ok)


if __name__ == "__main__":
    main()
