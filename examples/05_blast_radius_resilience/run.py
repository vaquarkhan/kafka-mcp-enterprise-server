#!/usr/bin/env python3
"""Blast-radius resilience - breakers + rogue quarantine with order fixtures."""

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
    expect_ok,
    finish,
    load_json,
    seed_topic_from_jsonl,
    step,
)


def main() -> None:
    scenario = load_json(data_dir(__file__) / "scenario.json")
    banner("05 · Blast-radius resilience")
    print("Persona: platform gateway serving many agents")
    print("Goal: isolate admin outages; contain rogue destructive bursts")

    cfg = Config(
        approval_signing_secret=b"example-approval-key",
        allowed_topic_prefixes=["agent."],
        max_destructive_per_minute=int(scenario["maxDestructivePerMinute"]),
        circuit_breaker_enabled=True,
    )
    server = KafkaMcpServer(cfg)
    secret = cfg.approval_signing_secret
    ok = True

    step(1, "Seed data-plane orders from fixture")
    n = seed_topic_from_jsonl(
        server, scenario["dataPlaneTopic"], data_dir(__file__) / "orders.jsonl", partitions=2
    )
    print(f"         seeded {n} orders on {scenario['dataPlaneTopic']}")

    step(2, "Inject control_plane dependency failure; trip breaker")
    server.backend._fail_module("control_plane", True)
    codes = []
    for _ in range(4):
        resp = call(server, "describe_cluster", {}, {"identity": "admin-ui"})
        codes.append((resp.get("error") or {}).get("code"))
    breaker_open = server.breakers["control_plane"].state == "open" or -32043 in codes
    print(f"  {'PASS' if breaker_open else 'FAIL'}  control_plane degraded (codes={codes})")
    ok &= breaker_open

    step(3, "Data plane still serves order traffic")
    ok &= expect_ok(
        call(
            server,
            "consume_messages",
            {"topic": scenario["dataPlaneTopic"], "maxMessages": 10},
            {"identity": "reader"},
        ),
        "consume still works",
    )
    data_ok = server.breakers["data_plane"].state == "closed"
    print(f"  {'PASS' if data_ok else 'FAIL'}  data_plane breaker remains closed")
    ok &= data_ok

    step(4, "Rogue agent bursts deletes - quarantined")
    server.backend._fail_module("control_plane", False)
    server.breakers["control_plane"].state = "closed"
    server.breakers["control_plane"].failures = 0
    for name in scenario["tempTopics"]:
        server.backend.create_topic(name)
    rogue = {"identity": scenario["rogueIdentity"]}
    q_codes = []
    for name in scenario["tempTopics"]:
        resp = call(
            server,
            "delete_topic",
            {"name": name, "_approval_token": mint(secret, "delete_topic")},
            rogue,
        )
        q_codes.append((resp.get("error") or {}).get("code"))
    quarantined = -32047 in q_codes
    print(f"  {'PASS' if quarantined else 'FAIL'}  rogue hit QUARANTINED (codes={q_codes})")
    ok &= quarantined

    step(5, "Innocent operator still deletes with approval")
    server.backend.create_topic("agent.other")
    ok &= expect_ok(
        call(
            server,
            "delete_topic",
            {
                "name": "agent.other",
                "_approval_token": mint(secret, "delete_topic"),
            },
            {"identity": scenario["innocentIdentity"]},
        ),
        "other identity not quarantined",
    )

    finish(ok)


if __name__ == "__main__":
    main()
