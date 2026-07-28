"""Resources (kafka:// URIs) tests (6 checks)."""

from __future__ import annotations

from .harness import Checker, call, err_code, has_result, new_server, result, rpc


def run() -> Checker:
    c = Checker("Resources (kafka:// URIs)")
    s = new_server()
    call(s, "create_topic", {"name": "agent.res"})

    r = rpc(s, "resources/list", {})
    resources = (result(r) or {}).get("resources") or []
    c.check(
        "resources/list returns catalog (>= 5 entries)",
        has_result(r) and len(resources) >= 5,
        str(resources),
    )

    r = rpc(s, "resources/read", {"uri": "kafka://topics"})
    c.check(
        "kafka://topics lists topics",
        has_result(r) and "agent.res" in ((result(r) or {}).get("topics") or []),
        str(r),
    )

    r = rpc(s, "resources/read", {"uri": "kafka://topics/agent.res"})
    c.check(
        "kafka://topics/{name} describes topic",
        has_result(r) and (result(r) or {}).get("name") == "agent.res",
        str(r),
    )

    r = rpc(s, "resources/read", {"uri": "kafka://cluster"})
    c.check(
        "kafka://cluster returns clusterId",
        has_result(r) and "clusterId" in (result(r) or {}),
        str(r),
    )

    call(s, "list_topics", {}, session={"identity": "res-user"})
    r = rpc(s, "resources/read", {"uri": "kafka://audit/recent"})
    entries = (result(r) or {}).get("entries") or []
    c.check(
        "kafka://audit/recent returns entries",
        has_result(r) and isinstance(entries, list) and len(entries) >= 1,
        str(r),
    )

    r = rpc(s, "resources/read", {"uri": "kafka://nope/unknown"})
    c.check("unknown resource uri -> error", err_code(r) is not None, str(r))

    return c
