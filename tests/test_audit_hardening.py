"""Audit-hardening validation (protocol + security fixes A1-B8 / C)."""

from __future__ import annotations

from kafka_mcp.approval import mint, verify
from kafka_mcp.config import DEFAULT_PROTOCOL_VERSION
from kafka_mcp.server import unwrap_tool_result
from kafka_mcp.tools import TOOL_INPUT_SCHEMAS

from .harness import Checker, call, err_code, has_result, mcp_content_ok, new_server, result, rpc


def run() -> Checker:
    c = Checker("Audit hardening (protocol + security)")

    s = new_server()
    init = rpc(s, "initialize", {"protocolVersion": "99.0.0"})
    c.check(
        "A3: initialize returns fixed supported protocolVersion",
        (init.get("result") or {}).get("protocolVersion") == DEFAULT_PROTOCOL_VERSION,
        str(init),
    )

    listed = rpc(s, "tools/list")
    tools = (listed.get("result") or {}).get("tools") or []
    schemas_ok = True
    for t in tools:
        schema = t.get("inputSchema") or {}
        if t["name"] in TOOL_INPUT_SCHEMAS:
            expected = TOOL_INPUT_SCHEMAS[t["name"]]
            if schema.get("properties") != expected.get("properties"):
                # allow required/extra keys; require properties present when expected
                if "properties" in expected and "properties" not in schema:
                    schemas_ok = False
        if t["name"] == "consume_messages":
            props = schema.get("properties") or {}
            if "topic" not in props or "maxMessages" not in props:
                schemas_ok = False
    c.check("A2: tools/list publishes real inputSchema (13 tools)", len(TOOL_INPUT_SCHEMAS) == 13 and schemas_ok, str(tools[:2]))

    r = call(s, "list_topics", {})
    c.check("A1: tools/call result is MCP content-shaped", mcp_content_ok(r), str(r))
    domain = result(r)
    c.check(
        "A1: content unwraps to domain dict",
        isinstance(domain, dict) and "topics" in domain,
        str(domain),
    )

    # B1: no secret => cannot mint
    try:
        mint(None, "delete_topic")
        minted_none = True
    except ValueError:
        minted_none = False
    c.check("B1: mint without secret raises ValueError", minted_none is False)

    s2 = new_server()
    s2.backend.create_topic("agent.spoof")
    r = call(
        s2,
        "delete_topic",
        {
            "name": "agent.spoof",
            "_identity": "admin-spoof",
            "_approval_token": mint(s2.cfg.approval_signing_secret, "delete_topic"),
        },
        session={"identity": "real-user"},
    )
    # Should succeed as real-user with approval; identity spoof discarded
    c.check("B3: _identity in arguments ignored (call still uses session)", has_result(r), str(r))
    # Audit should show real-user
    entries = s2.audit.recent(5)
    last_allow = next((e for e in reversed(entries) if e.get("decision") == "ALLOW"), {})
    c.check(
        "B3: audit identity is session identity not spoofed arg",
        last_allow.get("identity") == "real-user",
        str(last_allow),
    )

    # B2: resource-bound token rejects wrong resource
    secret = s2.cfg.approval_signing_secret
    s2.backend.create_topic("agent.a")
    s2.backend.create_topic("agent.b")
    tok = mint(secret, "delete_topic", resource="agent.a", principal="binder")
    r = call(
        s2,
        "delete_topic",
        {"name": "agent.b", "_approval_token": tok},
        session={"identity": "binder"},
    )
    c.check("B2: approval token resource mismatch -> -32042", err_code(r) == -32042, str(r))

    tok2 = mint(secret, "delete_topic", resource="agent.a", principal="binder")
    r = call(
        s2,
        "delete_topic",
        {"name": "agent.a", "_approval_token": tok2},
        session={"identity": "binder"},
    )
    c.check("B2: matching resource+principal approval OK", has_result(r), str(r))

    # B7: create_acls out of scope
    s3 = new_server(allowed_topic_prefixes=["agent."])
    tok = mint(s3.cfg.approval_signing_secret, "create_acls")
    r = call(
        s3,
        "create_acls",
        {
            "bindings": [{"principal": "User:x", "operation": "READ", "resource": "prod.ledger"}],
            "_approval_token": tok,
        },
    )
    c.check("B7: create_acls out-of-scope binding -> -32041", err_code(r) == -32041, str(r))

    # B5: mutate blocked by taint when value reused
    s4 = new_server(taint_min_length=8)
    s4.backend.create_topic("agent.taintsrc")
    s4.backend.create_topic("agent.taintdst")
    poison = "poison-payload-xyz"
    call(s4, "produce_message", {"topic": "agent.taintsrc", "value": poison})
    sess = {"identity": "taint-mutate"}
    call(s4, "consume_messages", {"topic": "agent.taintsrc", "maxMessages": 1}, session=sess)
    r = call(
        s4,
        "produce_message",
        {"topic": "agent.taintdst", "value": poison},
        session=sess,
    )
    c.check("B5: tainted mutate produce -> -32040", err_code(r) == -32040, str(r))

    # B6: short taint values do not match everything
    sess2 = {"identity": "short", "tainted_values": {"ok"}, "integrity": "untrusted"}
    s4.backend.create_topic("agent.shortok")
    tok = mint(s4.cfg.approval_signing_secret, "delete_topic")
    # Without approval, short "ok" should not taint-match topic name under min_length=8
    r = call(s4, "delete_topic", {"name": "agent.shortok"}, session=dict(sess2))
    c.check(
        "B6: short tainted span does not self-DoS destructive",
        err_code(r) == -32042,  # approval required, not taint
        str(r),
    )

    # C5: UNAUTHORIZED from errors module
    from kafka_mcp.errors import UNAUTHORIZED
    from kafka_mcp import auth as auth_mod

    c.check("C5: auth uses errors.UNAUTHORIZED (-32001)", auth_mod.UNAUTHORIZED == UNAUTHORIZED == -32001)

    return c
