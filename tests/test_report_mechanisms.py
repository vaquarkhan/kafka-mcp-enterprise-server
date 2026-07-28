"""KIP-1318 report mechanisms tests (13 checks)."""

from __future__ import annotations

from .harness import Checker, call, err_code, has_result, mint, new_server, result


def run() -> Checker:
    c = Checker("KIP-1318 report mechanisms")

    # strict IFC
    s = new_server(ifc_strict=True)
    s.backend.create_topic("agent.data")
    call(s, "produce_message", {"topic": "agent.data", "value": "poison"})
    sess = {"identity": "ifc"}
    call(s, "consume_messages", {"topic": "agent.data", "maxMessages": 1}, session=sess)
    s.backend.create_topic("agent.victim")
    r = call(
        s,
        "delete_topic",
        {"name": "agent.victim"},
        session=sess,
    )
    # without approval, strict IFC should block
    c.check(
        "strict IFC blocks control-plane after untrusted read -> -32040",
        err_code(r) == -32040,
        str(r),
    )

    tok = mint(s.cfg.approval_signing_secret, "delete_topic")
    r = call(
        s,
        "delete_topic",
        {"name": "agent.victim", "_approval_token": tok},
        session=sess,
    )
    c.check(
        "approval (Trusted) elevates and allows the control-plane call",
        has_result(r),
        str(r),
    )

    # non-strict: unrelated delete with approval OK even if session tainted
    s = new_server(ifc_strict=False)
    s.backend.create_topic("agent.x")
    sess = {"identity": "nonstrict", "integrity": "untrusted", "tainted_values": {"poison-name"}}
    s.backend.create_topic("agent.unrelated")
    tok = mint(s.cfg.approval_signing_secret, "delete_topic")
    r = call(
        s,
        "delete_topic",
        {"name": "agent.unrelated", "_approval_token": tok},
        session=sess,
    )
    c.check(
        "non-strict allows unrelated delete (with approval, arg not tainted)",
        has_result(r),
        str(r),
    )

    # identity propagation ACLs
    s = new_server(identity_propagation=True)
    s.backend.set_principal_acl("reader", "READ", "agent.")
    s.backend.create_topic("agent.acl")
    call(s, "produce_message", {"topic": "agent.acl", "value": "v"})
    # produce goes through WRITE - reader may not have WRITE; produce as anonymous with enforcement
    # Actually identity_propagation checks on every tool. Default identity anonymous has no ACLs.
    # Seed produce via backend.
    r = call(
        s,
        "consume_messages",
        {"topic": "agent.acl", "maxMessages": 1},
        session={"identity": "reader"},
    )
    c.check(
        "reader principal CAN consume agent.* (ACL allows READ)",
        has_result(r),
        str(r),
    )
    r = call(
        s,
        "create_topic",
        {"name": "agent.new"},
        session={"identity": "reader"},
    )
    c.check(
        "reader principal CANNOT create topic (no CREATE ACL) -> -32044",
        err_code(r) == -32044,
        str(r),
    )

    s = new_server(hard_max_records=5)
    s.backend.create_topic("agent.clamp")
    for i in range(20):
        call(s, "produce_message", {"topic": "agent.clamp", "value": f"m{i}"})
    r = call(s, "consume_messages", {"topic": "agent.clamp", "maxMessages": 1000})
    recs = (result(r) or {}).get("records") or []
    c.check(
        "consume clamped to hard_max_records (<=5) despite maxMessages=1000",
        has_result(r) and len(recs) <= 5,
        str(len(recs)),
    )

    s = new_server()
    s.backend.create_topic("agent.direct")
    call(s, "produce_message", {"topic": "agent.direct", "value": "d"})
    before = s.backend.rebalances
    r = call(s, "consume_messages", {"topic": "agent.direct", "maxMessages": 5})
    body = result(r) or {}
    c.check(
        "ephemeral consume uses direct assignment",
        body.get("assignment") == "direct" and body.get("groupId") is None,
        str(body),
    )
    c.check(
        "ephemeral consume triggers NO rebalance",
        s.backend.rebalances == before,
        f"{before}->{s.backend.rebalances}",
    )
    c.check(
        "ephemeral consumer is NOT registered as a group",
        "groups" in s.backend.list_groups() and len(s.backend.groups) == 0,
        str(s.backend.list_groups()),
    )

    r = call(
        s,
        "consume_messages",
        {"topic": "agent.direct", "maxMessages": 1, "groupId": "g-direct"},
    )
    body = result(r) or {}
    c.check(
        "explicit groupId consume registers a group + 1 rebalance",
        body.get("assignment") == "group"
        and "g-direct" in s.backend.groups
        and s.backend.rebalances >= 1,
        f"{body} reb={s.backend.rebalances}",
    )

    # breaker isolation
    s = new_server()
    s.backend.create_topic("agent.iso")
    call(s, "produce_message", {"topic": "agent.iso", "value": "ok"})
    s.backend._fail_module("control_plane", True)
    codes = []
    for _ in range(4):
        rr = call(s, "describe_cluster", {})
        codes.append(err_code(rr))
    c.check(
        "control_plane breaker OPEN after its dependency fails",
        s.breakers["control_plane"].state == "open" or -32043 in codes,
        f"state={s.breakers['control_plane'].state} codes={codes}",
    )
    c.check(
        "data_plane breaker remains CLOSED (isolated)",
        s.breakers["data_plane"].state == "closed",
        s.breakers["data_plane"].state,
    )
    r = call(s, "consume_messages", {"topic": "agent.iso", "maxMessages": 1})
    c.check(
        "data_plane still serves while control_plane degraded",
        has_result(r),
        str(r),
    )

    return c
