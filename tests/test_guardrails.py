"""Data-protection guardrails tests (14 checks)."""

from __future__ import annotations

from kafka_mcp.dlp import Dlp, scan
from kafka_mcp.approval import mint

from .harness import Checker, call, err_code, has_result, new_server, result


def run() -> Checker:
    c = Checker("Data-protection guardrails")

    dlp = Dlp(mode="redact", block_categories=["private_key", "aws_access_key", "jwt"])
    text = (
        "-----BEGIN RSA PRIVATE KEY-----\n"
        "AKIAIOSFODNN7EXAMPLE\n"
        "eyJhbGciOiJIUzI1NiJ9.eyJzdWI6IjEyMyJ9.sig"
    )
    _, hits, blocked = dlp.process(text)
    c.check(
        "DLP detects private key + aws key + jwt (block categories)",
        blocked
        and "private_key" in hits
        and "aws_access_key" in hits
        and "jwt" in hits,
        str(hits),
    )

    c.check(
        "DLP credit-card Luhn: valid card detected",
        "credit_card" in scan("card 4111111111111111"),
        "",
    )
    c.check(
        "DLP credit-card Luhn: random 16 digits NOT flagged",
        "credit_card" not in scan("card 1234567890123456"),
        "",
    )

    s = new_server()
    s.backend.create_topic("agent.guard")
    call(
        s,
        "produce_message",
        {
            "topic": "agent.guard",
            "value": "mail a@b.co ip 10.0.0.1 phone 415-555-1212",
        },
    )
    r = call(s, "consume_messages", {"topic": "agent.guard", "maxMessages": 5})
    val = ((result(r) or {}).get("records") or [{}])[0].get("value", "")
    c.check(
        "consume redacts email+phone (ipv4 redaction is opt-in)",
        "a@b.co" not in val and "415-555-1212" not in val,
        val,
    )

    s = new_server()
    s.backend.create_topic("agent.cfg", config={"sasl.jaas.config": "secret-value"})
    r = call(s, "describe_topic", {"name": "agent.cfg"})
    cfg = (result(r) or {}).get("config") or {}
    c.check(
        "describe redacts sensitive config value",
        cfg.get("sasl.jaas.config") == "[REDACTED_CONFIG]"
        or "secret-value" not in str(cfg),
        str(cfg),
    )

    s = new_server()
    s.backend.create_topic("agent.egress")
    r = call(
        s,
        "produce_message",
        {"topic": "agent.egress", "value": "key=AKIAIOSFODNN7EXAMPLE"},
    )
    c.check("produce of AWS key blocked (egress) -> -32045", err_code(r) == -32045, str(r))

    r = call(s, "produce_message", {"topic": "agent.egress", "value": "normal payload"})
    c.check("produce of normal data allowed", has_result(r), str(r))

    s = new_server(sensitive_topic_patterns=["secret.*", "secret."])
    s.backend.create_topic("secret.payroll")
    call(s, "produce_message", {"topic": "secret.payroll", "value": "x"})
    r = call(s, "consume_messages", {"topic": "secret.payroll", "maxMessages": 1})
    c.check(
        "consume sensitive topic without approval -> -32042",
        err_code(r) == -32042,
        str(r),
    )
    token = mint(s.cfg.approval_signing_secret, "consume_messages")
    r = call(
        s,
        "consume_messages",
        {"topic": "secret.payroll", "maxMessages": 1, "_approval_token": token},
    )
    c.check("consume sensitive topic WITH approval -> OK", has_result(r), str(r))

    s = new_server()
    r = call(s, "create_topic", {"name": "bad topic; DROP"})
    c.check(
        "invalid topic name (spaces/injection) -> -32046",
        err_code(r) == -32046,
        str(r),
    )

    s = new_server(max_value_bytes=16)
    s.backend.create_topic("agent.big")
    r = call(s, "produce_message", {"topic": "agent.big", "value": "x" * 100})
    c.check("oversized value -> -32046", err_code(r) == -32046, str(r))

    s = new_server(max_output_bytes=200)
    s.backend.create_topic("agent.trunc")
    for i in range(20):
        call(s, "produce_message", {"topic": "agent.trunc", "value": f"record-{i}-" + ("y" * 40)})
    r = call(s, "consume_messages", {"topic": "agent.trunc", "maxMessages": 100})
    body = result(r) or {}
    c.check(
        "output is truncated under max_output_bytes",
        body.get("truncated") is True or body.get("_truncation") == "max_output_bytes",
        str(body)[:200],
    )

    s = new_server(max_destructive_per_minute=3)
    # create topics to delete
    for i in range(5):
        s.backend.create_topic(f"agent.q{i}")
    sess = {"identity": "rogue"}
    codes = []
    for i in range(5):
        tok = mint(s.cfg.approval_signing_secret, "delete_topic")
        rr = call(
            s,
            "delete_topic",
            {"name": f"agent.q{i}", "_approval_token": tok},
            session=sess,
        )
        codes.append(err_code(rr))
    c.check(
        "rogue identity quarantined after destructive burst -> -32047",
        -32047 in codes,
        str(codes),
    )

    other = {"identity": " innocent".strip()}
    s.backend.create_topic("agent.other")
    tok = mint(s.cfg.approval_signing_secret, "delete_topic")
    r = call(
        s,
        "delete_topic",
        {"name": "agent.other", "_approval_token": tok},
        session=other,
    )
    c.check("other identity not affected by quarantine", has_result(r), str(r))

    return c
