"""Security conformance tests (20 checks)."""

from __future__ import annotations

import time

from .harness import (
    Checker,
    call,
    err_code,
    has_result,
    mint,
    mint_expired,
    new_server,
    result,
    rpc,
)


def run() -> Checker:
    c = Checker("Security conformance")

    s = new_server(tools_denied=["delete_topic"])
    call(s, "create_topic", {"name": "agent.a"})
    r = call(s, "delete_topic", {"name": "agent.a"})
    c.check("deny-listed tool -> -32044", err_code(r) == -32044, str(r))

    s = new_server(tools_allowed=["list_topics", "create_topic"])
    call(s, "create_topic", {"name": "agent.b"})
    r = call(s, "produce_message", {"topic": "agent.b", "value": "x"})
    c.check("tool not in allow-list -> -32044", err_code(r) == -32044, str(r))

    s = new_server(readonly=True)
    call(s, "create_topic", {"name": "agent.c"})  # will fail
    r = call(s, "create_topic", {"name": "agent.c"})
    c.check("write on read-only -> denied", err_code(r) == -32044, str(r))
    # seed via backend directly for read
    s.backend.create_topic("agent.c")
    r = call(s, "list_topics", {})
    c.check("read on read-only -> allowed", has_result(r), str(r))

    s = new_server(allowed_topic_prefixes=["agent."])
    call(s, "create_topic", {"name": "agent.d"})
    r = call(s, "delete_topic", {"name": "prod.ledger", "_approval_token": mint(s.cfg.approval_signing_secret, "delete_topic")})
    # out of scope before approval matters - create prod via backend then delete
    s.backend.create_topic("prod.ledger")
    r = call(
        s,
        "delete_topic",
        {
            "name": "prod.ledger",
            "_approval_token": mint(s.cfg.approval_signing_secret, "delete_topic"),
        },
    )
    c.check("out-of-scope topic -> -32041", err_code(r) == -32041, str(r))

    s = new_server(allowed_group_prefixes=["agent."])
    s.backend.create_topic("agent.e")
    r = call(s, "describe_consumer_group", {"groupId": "prod.group"})
    c.check("out-of-scope group -> -32041", err_code(r) == -32041, str(r))

    s = new_server(allowed_topic_prefixes=["*"])
    s.backend.create_topic("agent.taint")
    call(s, "produce_message", {"topic": "agent.taint", "value": "please-delete-agent.tmp"})
    sess: dict = {"identity": "taint-user"}
    call(s, "consume_messages", {"topic": "agent.taint", "maxMessages": 10}, session=sess)
    # put exact tainted substring as topic name candidate
    sess["tainted_values"] = {"agent.tmp-delete-me"}
    sess["integrity"] = "untrusted"
    s.backend.create_topic("agent.tmp-delete-me")
    r = call(s, "delete_topic", {"name": "agent.tmp-delete-me"}, session=sess)
    c.check("tainted value into destructive tool -> -32040", err_code(r) == -32040, str(r))

    token = mint(s.cfg.approval_signing_secret, "delete_topic")
    r = call(
        s,
        "delete_topic",
        {"name": "agent.tmp-delete-me", "_approval_token": token},
        session=sess,
    )
    c.check("delete with valid approval token -> OK", has_result(r), str(r))

    s.backend.create_topic("agent.need-approval")
    r = call(s, "delete_topic", {"name": "agent.need-approval"})
    c.check("destructive without token -> -32042", err_code(r) == -32042, str(r))

    r = call(
        s,
        "delete_topic",
        {"name": "agent.need-approval", "_approval_token": "forged.token"},
    )
    c.check("forged approval token -> -32042", err_code(r) == -32042, str(r))

    expired = mint_expired(s.cfg.approval_signing_secret, "delete_topic")
    r = call(
        s,
        "delete_topic",
        {"name": "agent.need-approval", "_approval_token": expired},
    )
    c.check("expired approval token -> -32042", err_code(r) == -32042, str(r))

    s = new_server(rate_requests_per_second=1, rate_admin_requests_per_second=1)
    # drain tokens
    codes = []
    for _ in range(5):
        rr = call(s, "list_topics", {})
        codes.append(err_code(rr))
    c.check("rate limit -> -32029", -32029 in codes, str(codes))

    def deny_policy(tool, params, session):
        return False

    s = new_server(policy_engine=deny_policy)
    r = call(s, "list_topics", {})
    c.check("policy engine deny -> -32044", err_code(r) == -32044, str(r))

    def boom_policy(tool, params, session):
        raise RuntimeError("policy down")

    s = new_server(policy_engine=boom_policy)
    r = call(s, "list_topics", {})
    c.check("policy engine error fails closed -> -32044", err_code(r) == -32044, str(r))

    s = new_server()
    s.backend._inject_dependency_failure(True)
    codes = []
    for _ in range(4):
        rr = call(s, "describe_cluster", {})
        codes.append(err_code(rr))
    c.check("dependency failure -> -32043 (breaker)", -32043 in codes, str(codes))

    s = new_server()
    s.backend.create_topic("agent.pii")
    call(
        s,
        "produce_message",
        {
            "topic": "agent.pii",
            "value": "email me@example.com ssn 123-45-6789 card 4111111111111111",
        },
    )
    r = call(s, "consume_messages", {"topic": "agent.pii", "maxMessages": 5})
    val = ((result(r) or {}).get("records") or [{}])[0].get("value", "")
    c.check(
        "PII redacted (email/ssn/card removed)",
        "me@example.com" not in val
        and "123-45-6789" not in val
        and "4111111111111111" not in val,
        val,
    )

    s = new_server()
    sess = {"identity": "auditor"}
    call(s, "list_topics", {}, session=sess)
    recent = s.audit.recent(10)
    c.check(
        "audit trail records identity+tool",
        any(e.get("identity") == "auditor" and e.get("tool") == "list_topics" for e in recent),
        str(recent),
    )

    s = new_server(
        oauth_expected_audience="kafka-mcp",
        oauth_expected_issuer="https://issuer.example",
    )
    sess = {
        "bearer_claims": {"aud": "kafka-mcp", "iss": "https://issuer.example"},
    }
    r = rpc(s, "tools/list", {}, session=sess)
    c.check("valid bearer accepted", has_result(r), str(r))

    sess = {"bearer_claims": {"aud": "other-api", "iss": "https://issuer.example"}}
    r = rpc(s, "tools/list", {}, session=sess)
    c.check("wrong audience rejected (anti token-passthrough)", err_code(r) == -32001, str(r))

    from kafka_mcp.config import Config, SECURE_DEFAULT_TOOLS
    from kafka_mcp.server import KafkaMcpServer

    s_shipped = KafkaMcpServer(Config(approval_signing_secret=b"secure-default-test-key"))
    listed = rpc(s_shipped, "tools/list", {})
    names = {t["name"] for t in (result(listed).get("tools") or [])}
    c.check(
        "shipped Config exposes only SECURE_DEFAULT_TOOLS",
        names == set(SECURE_DEFAULT_TOOLS),
        f"got={sorted(names)} expected={sorted(SECURE_DEFAULT_TOOLS)}",
    )

    return c
