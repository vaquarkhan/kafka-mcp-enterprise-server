"""SecurityPipeline: deny/allow/readonly, scope, policy, taint, approval."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set

from . import approval as approval_mod
from .errors import (
    APPROVAL_REQUIRED,
    POLICY_DENIED,
    SCOPE_VIOLATION,
    TAINT_VIOLATION,
    McpError,
)


def _normalize(val: Any) -> str:
    if val is None:
        return ""
    return str(val).strip().lower()


def _prefix_allowed(name: str, prefixes: List[str]) -> bool:
    if not prefixes or "*" in prefixes:
        return True
    for p in prefixes:
        if p == "*":
            return True
        if p.endswith("*"):
            if name.startswith(p[:-1]):
                return True
        elif name.startswith(p) or name == p:
            return True
    return False


def register_taint(session: Dict[str, Any], values: List[Any]) -> None:
    """Add normalized values to session tainted set; mark integrity untrusted."""
    tainted: Set[str] = session.setdefault("tainted_values", set())
    for v in values:
        n = _normalize(v)
        if n:
            tainted.add(n)
    session["integrity"] = "untrusted"


class SecurityPipeline:
    """Authorize tool calls (stages 2-7 of the documented 9-step order)."""

    def __init__(self, cfg: Any) -> None:
        self.cfg = cfg

    def _has_valid_approval(self, tool: str, params: Dict[str, Any]) -> bool:
        token = (params or {}).get("_approval_token")
        if not token:
            return False
        return approval_mod.verify(self.cfg.approval_signing_secret, token, tool)

    def authorize(
        self,
        tool: str,
        meta: Dict[str, Any],
        params: Dict[str, Any],
        session: Dict[str, Any],
    ) -> None:
        cfg = self.cfg
        kind = meta.get("kind", "read")

        # 2) deny-list
        denied = cfg.tools_denied or []
        if tool in denied or "*" in denied:
            raise McpError(POLICY_DENIED, f"tool denied: {tool}")

        # 3) allow-list + readonly
        allowed = cfg.tools_allowed or ["*"]
        if "*" not in allowed and tool not in allowed:
            raise McpError(POLICY_DENIED, f"tool not in allow-list: {tool}")
        if cfg.readonly and kind != "read":
            raise McpError(POLICY_DENIED, "read-only mode: writes disabled")

        # 4) resource scope (topic + group)
        topic_key = meta.get("topic_arg") or meta.get("resource_arg")
        if topic_key and topic_key in (params or {}):
            name = str(params[topic_key])
            if not _prefix_allowed(name, cfg.allowed_topic_prefixes):
                raise McpError(
                    SCOPE_VIOLATION,
                    f"topic out of scope: {name}",
                    data={"topic": name},
                )
        group_key = meta.get("group_arg")
        if group_key and group_key in (params or {}):
            gname = str(params[group_key])
            if not _prefix_allowed(gname, cfg.allowed_group_prefixes):
                raise McpError(
                    SCOPE_VIOLATION,
                    f"group out of scope: {gname}",
                    data={"group": gname},
                )

        # 5) policy engine (fail-closed)
        pe = cfg.policy_engine
        if pe is not None:
            try:
                ok = pe(tool, params, session)
            except Exception as e:
                raise McpError(
                    POLICY_DENIED,
                    f"policy engine error (fail-closed): {e}",
                ) from e
            if not ok:
                raise McpError(POLICY_DENIED, "policy engine denied")

        # 6) taint guard (destructive only; approval bypasses)
        if (
            cfg.taint_guard_enabled
            and kind == "destructive"
            and not self._has_valid_approval(tool, params)
        ):
            if cfg.ifc_strict and session.get("integrity") == "untrusted":
                raise McpError(
                    TAINT_VIOLATION,
                    "strict IFC: untrusted session cannot call destructive/control tools",
                )
            tainted: Set[str] = session.get("tainted_values") or set()
            if tainted:
                for v in (params or {}).values():
                    if isinstance(v, (dict, list)):
                        continue
                    nv = _normalize(v)
                    if not nv:
                        continue
                    for t in tainted:
                        if t and (t in nv or nv in t):
                            raise McpError(
                                TAINT_VIOLATION,
                                "tainted value used in destructive tool",
                                data={"value": str(v)},
                            )

        # 7) approval gate
        required = cfg.approval_required_tools or []
        if tool in required and not self._has_valid_approval(tool, params):
            # dry-run tools: still require token conceptually, but mark dryrun
            raise McpError(
                APPROVAL_REQUIRED,
                f"approval required for tool: {tool}",
                data={"tool": tool},
            )
