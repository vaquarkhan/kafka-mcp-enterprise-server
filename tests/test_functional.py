"""Functional / tool-level unit tests (12 checks)."""

from __future__ import annotations

from .harness import Checker, call, err_code, has_result, mint, new_server, result


def run() -> Checker:
    c = Checker("Functional / tool-level (Unit Tests)")
    s = new_server()
    secret = s.cfg.approval_signing_secret

    r = call(s, "create_topic", {"name": "agent.orders", "partitions": 3})
    c.check(
        "create_topic returns topic+partitions",
        has_result(r) and result(r).get("name") == "agent.orders" and result(r).get("partitions") == 3,
        str(r),
    )

    r2 = call(s, "create_topic", {"name": "agent.orders", "partitions": 3})
    data = (r2.get("error") or {}).get("data") or {}
    c.check(
        "duplicate create_topic -> structured error (TopicExists)",
        err_code(r2) is not None and ("TopicExists" in str(r2) or data.get("error") == "TopicExists"),
        str(r2),
    )

    r3 = call(s, "describe_topic", {"name": "agent.orders"})
    c.check(
        "describe_topic reports partition count",
        has_result(r3) and result(r3).get("partitions") == 3,
        str(r3),
    )

    r4 = call(s, "describe_topic", {"name": "no.such.topic"})
    c.check(
        "describe unknown topic -> structured error",
        err_code(r4) is not None and "UnknownTopic" in str(r4),
        str(r4),
    )

    r5 = call(
        s,
        "alter_topic_config",
        {"name": "agent.orders", "config": {"retention.ms": "60000"}},
    )
    c.check(
        "alter_topic_config applies config",
        has_result(r5) and result(r5).get("config", {}).get("retention.ms") == "60000",
        str(r5),
    )

    r6 = call(s, "produce_message", {"topic": "agent.orders", "value": "hello"})
    c.check(
        "produce_message returns offset/partition",
        has_result(r6) and "offset" in result(r6) and "partition" in result(r6),
        str(r6),
    )

    r7 = call(s, "produce_message", {"topic": "missing.topic", "value": "x"})
    c.check(
        "produce to non-existent topic -> structured error",
        err_code(r7) is not None and "UnknownTopic" in str(r7),
        str(r7),
    )

    for i in range(5):
        call(s, "produce_message", {"topic": "agent.orders", "value": f"m{i}"})
    r8 = call(s, "consume_messages", {"topic": "agent.orders", "maxMessages": 3})
    recs = (result(r8) or {}).get("records") or []
    c.check(
        "consume_messages bounded by maxMessages (<=3)",
        has_result(r8) and len(recs) <= 3,
        str(r8),
    )

    # establish a group with offsets
    call(
        s,
        "consume_messages",
        {"topic": "agent.orders", "maxMessages": 1, "groupId": "g1", "fromBeginning": True},
    )
    r9 = call(s, "describe_consumer_group", {"groupId": "g1"})
    c.check(
        "describe_consumer_group returns offsets",
        has_result(r9) and "offsets" in (result(r9) or {}),
        str(r9),
    )

    call(s, "produce_message", {"topic": "agent.orders", "value": "lag-me"})
    lag = s.backend.group_lag("g1", "agent.orders")
    c.check(
        "lag computation = endOffset - committed (>0 after new produce)",
        lag.get("lag", 0) > 0,
        str(lag),
    )

    token = mint(secret, "create_acls")
    r11 = call(
        s,
        "create_acls",
        {
            "bindings": [{"principal": "User:alice", "operation": "READ", "resource": "agent."}],
            "_approval_token": token,
        },
    )
    c.check(
        "create_acls creates binding (with approval)",
        has_result(r11) and result(r11).get("created", 0) >= 1,
        str(r11),
    )

    r12 = call(s, "describe_cluster", {})
    c.check(
        "describe_cluster returns clusterId + brokers",
        has_result(r12)
        and "clusterId" in (result(r12) or {})
        and "brokers" in (result(r12) or {}),
        str(r12),
    )

    return c
