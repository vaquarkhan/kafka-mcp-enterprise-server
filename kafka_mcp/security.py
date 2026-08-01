"""SecurityPipeline: deny/allow/readonly, scope, policy, taint, approval."""

from __future__ import annotations

import json
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
        if p.endswith("*"):
            if name.startswith(p[:-1]):
                return True
        elif name.startswith(p) or name == p:
            return True
    return False


def register_taint(session: Dict[str, Any], values: List[Any], cfg: Any = None) -> None:
    """Add normalized values to session tainted set; mark integrity untrusted."""
    min_len = int(getattr(cfg, "taint_min_length", 8) if cfg is not None else 8)
    max_vals = int(getattr(cfg, "taint_max_values", 256) if cfg is not None else 256)
    tainted: Set[str] = session.setdefault("tainted_values", set())
    for v in values:
        n = _normalize(v)
        if n and len(n) >= min_len:
            tainted.add(n)
    # Cap set size (drop arbitrary extras by rebuilding truncated set)
    if len(tainted) > max_vals:
        session["tainted_values"] = set(list(tainted)[:max_vals])
    session["integrity"] = "untrusted"


def approval_resource_key(tool: str, meta: Dict[str, Any], params: Dict[str, Any]) -> Optional[str]:
    """Canonical resource binding for approval tokens."""
    if tool == "create_acls":
        bindings = params.get("bindings") or params.get("acls") or []
        names = []
        for b in bindings:
            if isinstance(b, dict):
                names.append(str(b.get("resource") or b.get("name") or b.get("topic") or ""))
        return json.dumps(sorted(names), separators=(",", ":"))
    key = meta.get("resource_arg") or meta.get("topic_arg") or meta.get("group_arg")
    if key and key in (params or {}):
        return str(params[key])
    return None


def _taint_matches(arg_norm: str, tainted_norm: str, min_len: int) -> bool:
    """Whole-field equality, or substring only when both sides meet min length."""
    if not arg_norm or not tainted_norm:
        return False
    if arg_norm == tainted_norm:
        return True
    if len(arg_norm) >= min_len and len(tainted_norm) >= min_len:
        return tainted_norm in arg_norm or arg_norm in tainted_norm
    return False


def _iter_scalar_params(params: Dict[str, Any]) -> List[Any]:
    out: List[Any] = []
    for k, v in (params or {}).items():
        if k.startswith("_"):
            continue
        if isinstance(v, (dict, list)):
            # Flatten one level for create_acls bindings resource names
            if isinstance(v, list):
                for item in v:
                    if isinstance(item, dict):
                        for kk in ("resource", "name", "topic", "value", "key"):
                            if kk in item:
                                out.append(item[kk])
                    elif not isinstance(item, (dict, list)):
                        out.append(item)
            continue
        out.append(v)
    return out


class SecurityPipeline:
    """Authorize tool calls (stages 2-7 of the documented 9-step order)."""

    def __init__(self, cfg: Any) -> None:
        self.cfg = cfg
        self.used_nonces: Set[str] = set()

    def _has_valid_approval(
        self,
        tool: str,
        meta: Dict[str, Any],
        params: Dict[str, Any],
        session: Dict[str, Any],
        *,
        consume_nonce: bool = True,
    ) -> bool:
        token = (params or {}).get("_approval_token")
        if not token:
            return False
        resource = approval_resource_key(tool, meta, params)
        principal = str(session.get("identity") or "anonymous")
        used = None
        if consume_nonce and getattr(self.cfg, "approval_single_use_nonce", True):
            used = self.used_nonces
        return approval_mod.verify(
            self.cfg.approval_signing_secret,
            token,
            tool,
            resource=resource,
            principal=principal,
            used_nonces=used,
        )

    def _check_create_acls_scope(self, params: Dict[str, Any]) -> None:
        bindings = params.get("bindings") or params.get("acls") or []
        prefixes = self.cfg.allowed_topic_prefixes or ["*"]
        group_prefixes = self.cfg.allowed_group_prefixes or ["*"]
        for b in bindings:
            if not isinstance(b, dict):
                continue
            rtype = str(b.get("resourceType") or b.get("resource_type") or "TOPIC").upper()
            name = str(b.get("resource") or b.get("name") or b.get("topic") or "")
            if not name or rtype == "CLUSTER":
                continue
            if "GROUP" in rtype:
                if not _prefix_allowed(name, group_prefixes):
                    raise McpError(
                        SCOPE_VIOLATION,
                        f"ACL binding group out of scope: {name}",
                        data={"resource": name},
                    )
            else:
                # TOPIC and unknown types: enforce topic prefixes
                if not _prefix_allowed(name, prefixes):
                    raise McpError(
                        SCOPE_VIOLATION,
                        f"ACL binding resource out of scope: {name}",
                        data={"resource": name},
                    )

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
        allowed = cfg.tools_allowed
        if "*" not in allowed and tool not in allowed:
            raise McpError(POLICY_DENIED, f"tool not in allow-list: {tool}")
        if cfg.readonly and kind != "read":
            raise McpError(POLICY_DENIED, "read-only mode: writes disabled")

        # 4) resource scope (topic + group + create_acls bindings)
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
        if tool == "create_acls":
            self._check_create_acls_scope(params or {})

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

        # 6) taint guard - mutate + destructive (approval bypasses)
        if (
            cfg.taint_guard_enabled
            and kind in ("destructive", "mutate")
            and not self._has_valid_approval(
                tool, meta, params, session, consume_nonce=False
            )
        ):
            if cfg.ifc_strict and session.get("integrity") == "untrusted":
                raise McpError(
                    TAINT_VIOLATION,
                    "strict IFC: untrusted session cannot call mutate/destructive tools",
                )
            tainted: Set[str] = session.get("tainted_values") or set()
            min_len = int(getattr(cfg, "taint_min_length", 8))
            if tainted:
                for v in _iter_scalar_params(params or {}):
                    nv = _normalize(v)
                    if not nv:
                        continue
                    for t in tainted:
                        if _taint_matches(nv, t, min_len):
                            raise McpError(
                                TAINT_VIOLATION,
                                "tainted value used in mutate/destructive tool",
                                data={"value": str(v)},
                            )

        # 7) approval gate
        required = cfg.approval_required_tools or []
        if tool in required and not self._has_valid_approval(tool, meta, params, session):
            raise McpError(
                APPROVAL_REQUIRED,
                f"approval required for tool: {tool}",
                data={"tool": tool},
            )
