"""KIP-1318 conformance matrix: every KIP requirement maps to a pass/fail (or explicit gap).

Sources: KIP-1318 Test Plan + Phase 1 tool/resource set + this reference's security control model.
Spec-only items (HTTP, Connect, EOS, live brokers) are asserted as *not implemented*.
"""

from __future__ import annotations

from kafka_mcp import errors
from kafka_mcp.config import SECURE_DEFAULT_TOOLS
from kafka_mcp.interceptor import redact_record
from kafka_mcp.tools import TOOL_INPUT_SCHEMAS, build_tools

from .harness import Checker, call, err_code, has_result, mint, new_server, result, rpc

# KIP Phase 1 (must be implemented + tested in this reference)
PHASE1_TOOLS = {
    "create_topic",
    "delete_topic",
    "produce_message",
    "consume_messages",
    "alter_consumer_group_offsets",
    "delete_consumer_group",
}
PHASE1_RESOURCES = {
    "kafka://topics",
    "kafka://groups",
    "kafka://groups/{id}/lag",
    "kafka://cluster",
}

# KIP tools that remain Java-track / not in this reference
SPEC_ONLY_TOOLS = {
    "create_partitions",
    "delete_records",
    "produce_batch",
    "produce_transactional",
    "remove_group_members",
    "delete_consumer_group_offsets",
    "delete_acls",
    "alter_broker_config",
    "elect_leaders",
    "alter_partition_reassignments",
    "alter_client_quotas",
    "abort_transaction",
    "fence_producers",
    "create_connector",
    "update_connector_config",
    "delete_connector",
    "restart_connector",
    "pause_connector",
    "resume_connector",
    "stop_connector",
    "restart_task",
}

ALL_ERROR_CODES = [
    errors.PARSE_ERROR,
    errors.INVALID_REQUEST,
    errors.METHOD_NOT_FOUND,
    errors.INVALID_PARAMS,
    errors.INTERNAL_ERROR,
    errors.UNAUTHORIZED,
    errors.RATE_LIMITED,
    errors.TAINT_VIOLATION,
    errors.SCOPE_VIOLATION,
    errors.APPROVAL_REQUIRED,
    errors.DEPENDENCY_UNAVAILABLE,
    errors.POLICY_DENIED,
    errors.SENSITIVE_DATA_BLOCKED,
    errors.VALIDATION_FAILED,
    errors.QUARANTINED,
]


def run() -> Checker:
    c = Checker("KIP-1318 conformance matrix")
    s = new_server()
    secret = s.cfg.approval_signing_secret

    # --- KIP Test Plan: TopicToolsTest ---
    r = call(s, "create_topic", {"name": "agent.kip", "partitions": 2})
    c.check(
        "KIP TopicTools: create_topic returns name+partitions",
        has_result(r)
        and result(r).get("name") == "agent.kip"
        and result(r).get("partitions") == 2,
        str(r),
    )
    r = call(s, "create_topic", {"name": "agent.kip"})
    c.check(
        "KIP TopicTools: duplicate create -> TopicExists structured error",
        err_code(r) == errors.INVALID_PARAMS and "TopicExists" in str(r),
        str(r),
    )

    # --- KIP Test Plan: MessageToolsTest ---
    r = call(s, "produce_message", {"topic": "agent.kip", "value": "hello-kip", "key": "k1"})
    meta = result(r) or {}
    c.check(
        "KIP MessageTools: produce returns topic/partition/offset",
        has_result(r)
        and meta.get("topic") == "agent.kip"
        and "partition" in meta
        and "offset" in meta,
        str(r),
    )
    r = call(s, "produce_message", {"topic": "agent.missing-topic", "value": "x"})
    c.check(
        "KIP MessageTools: produce unknown topic -> structured error",
        err_code(r) == errors.INVALID_PARAMS and "UnknownTopic" in str(r),
        str(r),
    )
    r = call(s, "consume_messages", {"topic": "agent.kip", "maxMessages": 5})
    recs = (result(r) or {}).get("records") or []
    c.check(
        "KIP MessageTools: consume retrieves produced message",
        has_result(r) and any("hello-kip" in str(x.get("value")) for x in recs),
        str(r),
    )
    # Spec-only: produce_transactional not registered
    listed = rpc(s, "tools/list", {})
    names = {t["name"] for t in (result(listed).get("tools") or [])}
    c.check(
        "KIP MessageTools: produce_transactional NOT in reference (gap)",
        "produce_transactional" not in names,
        str(sorted(names)),
    )

    # --- KIP Test Plan: AclToolsTest ---
    tok = mint(secret, "create_acls")
    r = call(
        s,
        "create_acls",
        {
            "bindings": [
                {
                    "resource": "agent.kip",
                    "resourceType": "TOPIC",
                    "operation": "READ",
                    "principal": "User:agent",
                }
            ],
            "_approval_token": tok,
        },
    )
    c.check(
        "KIP AclTools: create_acls records bindings",
        has_result(r) and int((result(r) or {}).get("created") or 0) >= 1,
        str(r),
    )

    # --- KIP Test Plan: GroupResourcesTest (lag) ---
    call(s, "consume_messages", {"topic": "agent.kip", "groupId": "agent.kip-g", "maxMessages": 1})
    call(s, "produce_message", {"topic": "agent.kip", "value": "more"})
    lag = s.backend.group_lag("agent.kip-g", "agent.kip")
    c.check(
        "KIP GroupResources: lag = endOffset - committed",
        int(lag.get("lag") or 0) > 0,
        str(lag),
    )
    r = rpc(s, "resources/read", {"uri": "kafka://groups/agent.kip-g/lag"})
    c.check(
        "KIP Phase1 resource kafka://groups/{id}/lag served",
        has_result(r) and (result(r) or {}).get("groupId") == "agent.kip-g",
        str(r),
    )

    # --- KIP Phase 1 tools present ---
    for tool in sorted(PHASE1_TOOLS):
        c.check(f"KIP Phase1 tool registered: {tool}", tool in names, str(sorted(names)))

    r = call(
        s,
        "alter_consumer_group_offsets",
        {
            "groupId": "agent.kip-g",
            "offsets": [{"topic": "agent.kip", "partition": 0, "offset": 0}],
        },
    )
    c.check(
        "KIP Phase1: alter_consumer_group_offsets works",
        has_result(r) and (result(r) or {}).get("groupId") == "agent.kip-g",
        str(r),
    )

    call(s, "create_topic", {"name": "agent.kip-del-g-seed"})
    call(
        s,
        "consume_messages",
        {"topic": "agent.kip", "groupId": "agent.kip-to-delete", "maxMessages": 1},
    )
    tok_g = mint(secret, "delete_consumer_group", resource="agent.kip-to-delete", principal="anonymous")
    # harness session identity defaults to empty -> "anonymous" in server
    r = call(
        s,
        "delete_consumer_group",
        {"groupId": "agent.kip-to-delete", "_approval_token": tok_g},
        session={"identity": "anonymous"},
    )
    c.check(
        "KIP Phase1: delete_consumer_group works with approval",
        has_result(r) and (result(r) or {}).get("deleted") == "agent.kip-to-delete",
        str(r),
    )
    r = call(s, "delete_consumer_group", {"groupId": "agent.kip-g"})
    c.check(
        "KIP Phase1: delete_consumer_group without token -> -32042",
        err_code(r) == errors.APPROVAL_REQUIRED,
        str(r),
    )

    # --- KIP Phase 1 resources ---
    catalog = rpc(s, "resources/list", {})
    uris = {x.get("uri") for x in ((result(catalog) or {}).get("resources") or [])}
    for uri in sorted(PHASE1_RESOURCES):
        # catalog uses template form for lag
        present = uri in uris or any(uri.split("{")[0] in (u or "") for u in uris)
        c.check(f"KIP Phase1 resource catalog: {uri}", present, str(sorted(uris)))

    for uri in ("kafka://topics", "kafka://groups", "kafka://cluster"):
        r = rpc(s, "resources/read", {"uri": uri})
        c.check(f"KIP Phase1 resources/read {uri}", has_result(r), str(r))

    r = rpc(s, "resources/read", {"uri": "kafka://topics/agent.kip/offsets"})
    c.check(
        "KIP resource kafka://topics/{name}/offsets",
        has_result(r) and (result(r) or {}).get("topic") == "agent.kip",
        str(r),
    )
    r = rpc(s, "resources/read", {"uri": "kafka://groups/agent.kip-g/offsets"})
    c.check(
        "KIP resource kafka://groups/{id}/offsets",
        has_result(r) and (result(r) or {}).get("groupId") == "agent.kip-g",
        str(r),
    )

    # --- KIP Integration Tests (stdio already covered elsewhere; in-process mirror) ---
    c.check(
        "KIP Integration: tools/list exposes schemas for all registered tools",
        all(n in TOOL_INPUT_SCHEMAS for n in names) and len(TOOL_INPUT_SCHEMAS) == len(build_tools(s.backend)),
        f"schemas={len(TOOL_INPUT_SCHEMAS)} tools={len(names)}",
    )

    # --- KIP security: allow/deny lists ---
    s_deny = new_server(tools_denied=["delete_topic"])
    call(s_deny, "create_topic", {"name": "agent.d1"})
    r = call(
        s_deny,
        "delete_topic",
        {"name": "agent.d1", "_approval_token": mint(s_deny.cfg.approval_signing_secret, "delete_topic")},
    )
    c.check("KIP tools.denied takes precedence -> -32044", err_code(r) == errors.POLICY_DENIED, str(r))

    # --- Secure-by-default (reference posture; KIP enterprise guidance) ---
    from kafka_mcp.config import Config
    from kafka_mcp.server import KafkaMcpServer

    shipped = KafkaMcpServer(Config(approval_signing_secret=b"kip-matrix-secret!!"))
    shipped_names = {
        t["name"]
        for t in (
            result(rpc(shipped, "tools/list", {})).get("tools") or []
        )
    }
    c.check(
        "KIP secure-by-default: shipped allow-list is read/consume only",
        shipped_names == set(SECURE_DEFAULT_TOOLS),
        str(sorted(shipped_names)),
    )
    for destructive in ("delete_topic", "create_acls", "delete_consumer_group"):
        c.check(
            f"KIP secure-by-default: {destructive} not in shipped list",
            destructive not in shipped_names,
        )

    # --- Rate limit -32029 (KIP) ---
    s_rate = new_server(rate_requests_per_second=1, rate_admin_requests_per_second=1)
    codes = []
    for _ in range(5):
        codes.append(err_code(call(s_rate, "list_topics", {})) or 0)
    c.check("KIP rate limit yields -32029", errors.RATE_LIMITED in codes, str(codes))

    # --- PII interceptor / DLP (KIP Data Governance) ---
    red = redact_record({"value": "mail me@x.com ssn 123-45-6789"})
    c.check(
        "KIP interceptor redacts email/ssn before agent sees payload",
        "[REDACTED_EMAIL]" in red["value"] and "[REDACTED_SSN]" in red["value"],
        str(red),
    )

    # --- Error code table: each security code observed at least once in this matrix or constants exist ---
    for code in ALL_ERROR_CODES:
        c.check(f"KIP error code constant defined: {code}", isinstance(code, int))

    # Fire remaining codes not already asserted above
    r = s.handle({"jsonrpc": "1.0", "id": 1, "method": "tools/list"}, {})
    c.check("error -32600 INVALID_REQUEST", err_code(r) == errors.INVALID_REQUEST, str(r))
    r = call(s, "nope_tool", {})
    c.check("error -32601 METHOD_NOT_FOUND", err_code(r) == errors.METHOD_NOT_FOUND, str(r))
    r = call(s, "create_topic", {"name": "bad name!!"})
    c.check("error -32046 VALIDATION_FAILED", err_code(r) == errors.VALIDATION_FAILED, str(r))

    s_scope = new_server(allowed_topic_prefixes=["agent."])
    s_scope.backend.create_topic("prod.x")
    r = call(
        s_scope,
        "delete_topic",
        {
            "name": "prod.x",
            "_approval_token": mint(s_scope.cfg.approval_signing_secret, "delete_topic"),
        },
    )
    c.check("error -32041 SCOPE_VIOLATION", err_code(r) == errors.SCOPE_VIOLATION, str(r))

    s_oauth = new_server(oauth_expected_audience="kafka-mcp")
    r = rpc(s_oauth, "tools/list", {}, session={"bearer_claims": {"aud": "wrong"}})
    c.check("error -32001 UNAUTHORIZED", err_code(r) == errors.UNAUTHORIZED, str(r))

    s_taint = new_server()
    sess = {"identity": "t", "tainted_values": {"evilpayload"}, "integrity": "untrusted"}
    r = call(s_taint, "produce_message", {"topic": "agent.kip", "value": "evilpayload"}, session=sess)
    c.check("error -32040 TAINT_VIOLATION", err_code(r) == errors.TAINT_VIOLATION, str(r))

    s_dep = new_server()
    s_dep.backend._inject_dependency_failure(True)
    r = call(s_dep, "list_topics", {})
    c.check("error -32043 DEPENDENCY_UNAVAILABLE", err_code(r) == errors.DEPENDENCY_UNAVAILABLE, str(r))

    s_pol = new_server(policy_engine=lambda *a, **k: False)
    r = call(s_pol, "list_topics", {})
    c.check("error -32044 POLICY_DENIED (policy engine)", err_code(r) == errors.POLICY_DENIED, str(r))

    s_eg = new_server()
    call(s_eg, "create_topic", {"name": "agent.eg"})
    r = call(s_eg, "produce_message", {"topic": "agent.eg", "value": "AKIAIOSFODNN7EXAMPLE"})
    c.check("error -32045 SENSITIVE_DATA_BLOCKED", err_code(r) == errors.SENSITIVE_DATA_BLOCKED, str(r))

    s_q = new_server(max_destructive_per_minute=1)
    call(s_q, "create_topic", {"name": "agent.q1"})
    call(s_q, "create_topic", {"name": "agent.q2"})
    tok_d = mint(s_q.cfg.approval_signing_secret, "delete_topic")
    call(s_q, "delete_topic", {"name": "agent.q1", "_approval_token": tok_d}, session={"identity": "rogue"})
    tok_d2 = mint(s_q.cfg.approval_signing_secret, "delete_topic")
    r = call(
        s_q,
        "delete_topic",
        {"name": "agent.q2", "_approval_token": tok_d2},
        session={"identity": "rogue"},
    )
    c.check(
        "error -32047 QUARANTINED",
        err_code(r) == errors.QUARANTINED or err_code(r) == errors.APPROVAL_REQUIRED,
        str(r),
    )
    # Ensure quarantine path: burst more
    if err_code(r) != errors.QUARANTINED:
        for i in range(3):
            s_q.backend.create_topic(f"agent.qx{i}")
            t = mint(s_q.cfg.approval_signing_secret, "delete_topic")
            rr = call(
                s_q,
                "delete_topic",
                {"name": f"agent.qx{i}", "_approval_token": t},
                session={"identity": "rogue"},
            )
            if err_code(rr) == errors.QUARANTINED:
                c.check("error -32047 QUARANTINED (retry)", True)
                break
        else:
            c.check("error -32047 QUARANTINED (retry)", False, "quarantine not hit")

    # --- Spec-only tools explicitly absent ---
    for tool in sorted(SPEC_ONLY_TOOLS):
        c.check(f"KIP spec-only NOT registered: {tool}", tool not in names)

    # --- Spec-only transports / backends ---
    c.check("KIP spec-only: transport is stdio (not HTTP server)", s.cfg.transport == "stdio")
    c.check(
        "KIP spec-only: backend is InMemoryKafka (not live broker)",
        s.backend.__class__.__name__ == "InMemoryKafka",
    )

    # Connect tools gap (KIP Test Plan ConnectToolsTest)
    c.check(
        "KIP ConnectTools: create_connector NOT in reference (gap)",
        "create_connector" not in names,
    )

    # Prompts deferred (KIP Rejected / FAQ)
    init = rpc(s, "initialize", {"protocolVersion": "2024-11-05"})
    caps = (result(init) or {}).get("capabilities") or {}
    c.check(
        "KIP Prompts not used in Phase 1 (no prompts capability required)",
        "prompts" not in caps,
        str(caps),
    )

    # Direct Partition Assignment (reference control; agent-friendly consume)
    before = s.backend.rebalances
    call(s, "consume_messages", {"topic": "agent.kip", "maxMessages": 1})
    c.check(
        "Direct Partition Assignment: no rebalance without groupId",
        s.backend.rebalances == before,
        f"before={before} after={s.backend.rebalances}",
    )

    # Edge branches for new Phase-1 APIs (coverage)
    try:
        s.backend.delete_group("no-such-group-xyz")
        c.check("delete_group unknown raises", False)
    except Exception:
        c.check("delete_group unknown raises", True)
    r = call(
        s,
        "alter_consumer_group_offsets",
        {"groupId": "agent.kip-empty-topic", "offsets": [{"topic": "", "partition": 0, "offset": 1}]},
    )
    c.check("alter offsets skips empty topic entries", has_result(r), str(r))
    try:
        s.backend.topic_offsets("missing-topic-xyz")
        c.check("topic_offsets unknown raises", False)
    except Exception:
        c.check("topic_offsets unknown raises", True)
    empty_off = s.backend.group_offsets("never-seen-group")
    c.check("group_offsets empty group", empty_off.get("state") == "Empty")
    # lag_all skips topics missing from cluster
    s.backend.groups["agent.orphan"] = {"offsets": {"gone.topic:0": 1}, "topics": {"gone.topic"}}
    orphan_lag = s.backend.group_lag_all("agent.orphan")
    c.check("group_lag_all skips missing topics", orphan_lag.get("lag") == 0, str(orphan_lag))
    try:
        resources_mod = __import__("kafka_mcp.resources", fromlist=["read_resource"])
        resources_mod.read_resource("kafka://topics/agent.kip/nope", s.backend)
        c.check("unknown topic sub-resource", False)
    except Exception:
        c.check("unknown topic sub-resource", True)
    try:
        resources_mod.read_resource("kafka://groups/agent.kip-g/nope", s.backend)
        c.check("unknown group sub-resource", False)
    except Exception:
        c.check("unknown group sub-resource", True)

    return c
