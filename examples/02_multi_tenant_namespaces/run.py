#!/usr/bin/env python3
"""Multi-tenant isolation - payments vs inventory namespaces with fixture data."""

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


def team_server(prefix: str) -> KafkaMcpServer:
    return KafkaMcpServer(
        Config(
        approval_signing_secret=b"example-approval-key",
            allowed_topic_prefixes=[prefix],
            tools_allowed=[
                "list_topics",
                "create_topic",
                "produce_message",
                "consume_messages",
                "describe_topic",
            ],
        )
    )


def main() -> None:
    scenario = load_json(data_dir(__file__) / "scenario.json")
    banner("02 · Multi-tenant namespace isolation")
    print("Persona: payments-agent vs inventory-agent")
    print("Goal: prefix isolation with production-shaped event fixtures")

    payments = team_server("team.payments.")
    inventory = team_server("team.inventory.")
    # Cross-scope bait topics
    payments.backend.create_topic("team.inventory.stock")
    inventory.backend.create_topic("team.payments.settlements")

    pay_n = seed_topic_from_jsonl(
        payments, "team.payments.settlements", data_dir(__file__) / "payments_settlements.jsonl"
    )
    inv_n = seed_topic_from_jsonl(
        inventory, "team.inventory.stock", data_dir(__file__) / "inventory_stock.jsonl"
    )
    print(f"Seeded payments={pay_n} inventory={inv_n} fixture records")

    tenants = {t["id"]: t for t in scenario["tenants"]}
    pay_sess = {"identity": tenants["payments"]["identity"]}
    inv_sess = {"identity": tenants["inventory"]["identity"]}
    ok = True

    step(1, "Payments consumes its own settlements")
    ok &= expect_ok(
        call(
            payments,
            "consume_messages",
            {"topic": "team.payments.settlements", "maxMessages": 10},
            pay_sess,
        ),
        "payments consume own topic",
    )

    step(2, "Payments tries inventory topic - SCOPE_VIOLATION")
    ok &= expect_code(
        call(
            payments,
            "consume_messages",
            {"topic": "team.inventory.stock", "maxMessages": 5},
            pay_sess,
        ),
        -32041,
        "payments blocked from inventory.*",
    )

    step(3, "Inventory consumes stock; blocked from payments produce")
    ok &= expect_ok(
        call(
            inventory,
            "consume_messages",
            {"topic": "team.inventory.stock", "maxMessages": 10},
            inv_sess,
        ),
        "inventory consume own topic",
    )
    ok &= expect_code(
        call(
            inventory,
            "produce_message",
            {"topic": "team.payments.settlements", "value": '{"evil":true}'},
            inv_sess,
        ),
        -32041,
        "inventory blocked from payments.*",
    )

    step(4, "Payments can still produce a new settlement event")
    ok &= expect_ok(
        call(
            payments,
            "produce_message",
            {
                "topic": "team.payments.settlements",
                "value": '{"eventType":"SettlementPosted","amount":1.00,"currency":"USD"}',
            },
            pay_sess,
        ),
        "payments produce own topic",
    )

    finish(ok)


if __name__ == "__main__":
    main()
