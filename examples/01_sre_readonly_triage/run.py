#!/usr/bin/env python3
"""Night-shift SRE triage — read-only agent against real observability events."""

from __future__ import annotations

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
    here = Path(__file__).resolve().parent
    scenario = load_json(data_dir(__file__) / "scenario.json")
    banner("01 · SRE read-only triage")
    print(f"Persona: {scenario['persona']}\nMission: {scenario['mission']}")

    cfg = Config(
        approval_signing_secret=b"example-approval-key",
        readonly=True,
        tools_allowed=[
            "list_topics",
            "describe_topic",
            "describe_cluster",
            "consume_messages",
            "list_consumer_groups",
            "describe_consumer_group",
        ],
        allowed_topic_prefixes=scenario["allowed_topic_prefixes"],
        hard_max_records=50,
    )
    server = KafkaMcpServer(cfg)
    n = seed_topic_from_jsonl(
        server,
        "agent.observability.errors",
        data_dir(__file__) / "observability_errors.jsonl",
        partitions=2,
    )
    print(f"Seeded {n} observability events from data/observability_errors.jsonl")

    session = {"identity": scenario["persona"]}
    ok = True

    step(1, "List in-scope topics")
    ok &= expect_ok(call(server, "list_topics", {}, session), "list_topics allowed")

    step(2, "Describe the errors topic")
    ok &= expect_ok(
        call(server, "describe_topic", {"name": "agent.observability.errors"}, session),
        "describe_topic allowed",
    )

    step(3, "Peek recent errors (direct assignment — no consumer group)")
    resp = call(
        server,
        "consume_messages",
        {"topic": "agent.observability.errors", "maxMessages": 20},
        session,
    )
    ok &= expect_ok(resp, "consume_messages allowed")
    from examples._common import tool_result

    body = tool_result(resp) or {}
    recs = body.get("records") or []
    print(f"         assignment={body.get('assignment')} records={len(recs)}")
    if recs:
        print(f"         sample: {str(recs[0].get('value'))[:100]}…")

    step(4, "Attempt produce — denied (readonly)")
    ok &= expect_code(
        call(
            server,
            "produce_message",
            {"topic": "agent.observability.errors", "value": '{"level":"INFO","msg":"injected"}'},
            session,
        ),
        -32044,
        "produce blocked by readonly",
    )

    step(5, "Attempt create_topic — denied")
    ok &= expect_code(
        call(server, "create_topic", {"name": "agent.observability.temp"}, session),
        -32044,
        "create_topic blocked",
    )

    finish(ok)


if __name__ == "__main__":
    main()
