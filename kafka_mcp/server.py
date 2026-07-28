"""KafkaMcpServer: JSON-RPC dispatch + full fail-closed control pipeline."""

from __future__ import annotations

import fnmatch
import json
import uuid
from typing import Any, Dict, List, Optional

from . import approval as approval_mod
from . import auth as auth_mod
from . import guardrails
from . import resources as resources_mod
from .audit import AuditSink
from .backend import InMemoryKafka
from .config import Config
from .dlp import Dlp
from .errors import (
    APPROVAL_REQUIRED,
    INTERNAL_ERROR,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    POLICY_DENIED,
    McpError,
)
from .resilience import CircuitBreaker, RateLimiter
from .security import SecurityPipeline, register_taint
from .tools import build_tools


class KafkaMcpServer:
    """Reference MCP server implementing KIP-1318 control order."""

    def __init__(self, cfg: Optional[Config] = None, backend: Optional[InMemoryKafka] = None) -> None:
        self.cfg = cfg or Config()
        self.backend = backend or InMemoryKafka()
        self.security = SecurityPipeline(self.cfg)
        self.dlp = Dlp(self.cfg.dlp_mode, self.cfg.dlp_block_categories)
        self.audit = AuditSink()
        self.anomaly = guardrails.AnomalyTracker(self.cfg.max_destructive_per_minute)
        self.rate = RateLimiter(
            self.cfg.rate_requests_per_second,
            self.cfg.rate_admin_requests_per_second,
        )
        self.tools = build_tools(self.backend)
        self.breakers = {
            "data_plane": CircuitBreaker("data_plane", threshold=3, reset_seconds=5.0),
            "control_plane": CircuitBreaker("control_plane", threshold=3, reset_seconds=5.0),
            "ecosystem": CircuitBreaker("ecosystem", threshold=3, reset_seconds=5.0),
        }
        if not self.cfg.circuit_breaker_enabled:
            # High threshold effectively disables opening in normal tests
            for b in self.breakers.values():
                b.threshold = 10**9

    def handle(self, req: Dict[str, Any], session: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        session = session if session is not None else {}
        if not isinstance(req, dict) or req.get("jsonrpc") != "2.0":
            return self._error_response(None, INVALID_REQUEST, "invalid request")
        req_id = req.get("id")
        method = req.get("method")
        params = req.get("params") or {}
        if not isinstance(params, dict):
            params = {}

        try:
            # Stage 1: auth (when configured)
            claims = session.get("bearer_claims") or params.pop("_bearer_claims", None)
            if claims is not None or self.cfg.oauth_expected_audience or self.cfg.oauth_expected_issuer:
                auth_mod.validate_bearer(self.cfg, claims)

            if method == "initialize":
                result = self._initialize(params)
            elif method == "tools/list":
                result = self._tools_list()
            elif method == "tools/call":
                result = self._call_tool(params, session)
            elif method == "resources/list":
                result = {"resources": list(resources_mod.RESOURCE_LIST)}
            elif method == "resources/read":
                result = self._resources_read(params, session)
            elif method == "notifications/initialized":
                return None  # type: ignore[return-value]
            else:
                raise McpError(METHOD_NOT_FOUND, f"unknown method: {method}")
            return {"jsonrpc": "2.0", "id": req_id, "result": result}
        except McpError as e:
            identity = session.get("identity", "anonymous")
            tool = (params or {}).get("name") if method == "tools/call" else method
            self.audit.record(
                identity=str(identity),
                tool=str(tool or method),
                params=params.get("arguments") if method == "tools/call" else params,
                decision="DENY",
                result={"code": e.code, "message": e.message},
                corr_id=session.get("corr_id"),
            )
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": e.to_dict(),
            }
        except Exception as e:
            return self._error_response(req_id, INTERNAL_ERROR, str(e))

    def _error_response(self, req_id: Any, code: int, message: str) -> Dict[str, Any]:
        return {"jsonrpc": "2.0", "id": req_id, "error": {"code": code, "message": message}}

    @staticmethod
    def _clamp_consume_bytes(result: Dict[str, Any], hard_max_bytes: int) -> Dict[str, Any]:
        """Trim consume records until serialized size <= hard_max_bytes."""
        if hard_max_bytes <= 0 or not isinstance(result, dict):
            return result
        records = list(result.get("records") or [])
        out = dict(result)
        while records:
            out["records"] = records
            size = len(json.dumps(out, default=str).encode("utf-8"))
            if size <= hard_max_bytes:
                break
            records.pop()
            out["truncated"] = True
            out["_truncation"] = "hard_max_bytes"
        out["records"] = records
        return out

    def _initialize(self, params: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "protocolVersion": params.get("protocolVersion") or "2024-11-05",
            "serverInfo": {"name": "kafka-mcp-reference", "version": "0.1.0"},
            "capabilities": {
                "tools": {},
                "resources": {},
            },
        }

    def _visible_tools(self) -> List[str]:
        names = []
        for name, (_h, meta) in self.tools.items():
            if name in (self.cfg.tools_denied or []):
                continue
            allowed = self.cfg.tools_allowed or ["*"]
            if "*" not in allowed and name not in allowed:
                continue
            if self.cfg.readonly and meta.get("kind") != "read":
                continue
            names.append(name)
        return sorted(names)

    def _tools_list(self) -> Dict[str, Any]:
        tools = []
        for name in self._visible_tools():
            _h, meta = self.tools[name]
            tools.append(
                {
                    "name": name,
                    "description": f"{meta.get('kind')} / {meta.get('module')} / {meta.get('operation')}",
                    "inputSchema": {"type": "object"},
                    "annotations": {
                        "kind": meta.get("kind"),
                        "module": meta.get("module"),
                        "operation": meta.get("operation"),
                    },
                }
            )
        return {"tools": tools}

    def _resources_read(self, params: Dict[str, Any], session: Dict[str, Any]) -> Any:
        uri = params.get("uri") or ""
        if uri == "kafka://health" or uri.rstrip("/") == "kafka://health":
            return {
                "status": "ok",
                "breakers": {k: v.state for k, v in self.breakers.items()},
                "clusterId": self.backend.cluster_id,
            }

        def _read() -> Any:
            self.backend.check_dependency("control_plane")
            return resources_mod.read_resource(
                uri,
                self.backend,
                audit_recent=self.audit.recent,
            )

        if self.cfg.circuit_breaker_enabled:
            return self.breakers["control_plane"].call(_read)
        return _read()

    def _call_tool(self, params: Dict[str, Any], session: Dict[str, Any]) -> Any:
        tool = params.get("name")
        arguments = dict(params.get("arguments") or {})
        if not tool or tool not in self.tools:
            raise McpError(METHOD_NOT_FOUND, f"unknown tool: {tool}")

        handler, meta = self.tools[tool]
        identity = str(session.get("identity") or arguments.pop("_identity", None) or "anonymous")
        session.setdefault("identity", identity)
        corr_id = session.get("corr_id") or str(uuid.uuid4())
        session["corr_id"] = corr_id

        # validate + anomaly (pre-authz)
        guardrails.validate_arguments(tool, arguments, self.cfg)
        self.anomaly.check(identity)

        # stages 2-7
        self.security.authorize(tool, meta, arguments, session)

        # identity propagation -> broker ACLs
        if self.cfg.identity_propagation:
            op = meta.get("operation") or "ALL"
            resource_key = meta.get("resource_arg") or meta.get("topic_arg") or meta.get("group_arg")
            resource = str(arguments.get(resource_key, "")) if resource_key else ""
            principal = identity
            if not self.backend.authorize(principal, op, resource or "*"):
                raise McpError(
                    POLICY_DENIED,
                    f"broker ACL denied for {principal} {op} {resource}",
                )

        # clamp consume + sensitive-topic gating
        if tool == "consume_messages":
            mm = int(arguments.get("maxMessages", arguments.get("max_messages", 10)))
            mm = min(mm, self.cfg.hard_max_records)
            arguments["maxMessages"] = mm

            topic = str(arguments.get("topic", ""))
            matched = False
            for pat in self.cfg.sensitive_topic_patterns or []:
                if pat and (
                    fnmatch.fnmatch(topic, pat)
                    or topic.startswith(pat.rstrip("*"))
                    or pat.rstrip("*") == topic
                ):
                    matched = True
                    break
            if matched:
                token = arguments.get("_approval_token")
                if not token or not approval_mod.verify(
                    self.cfg.approval_signing_secret, token, "consume_messages"
                ):
                    raise McpError(APPROVAL_REQUIRED, "sensitive topic requires approval")
        # egress scan for produce
        if tool == "produce_message":
            guardrails.egress_scan(arguments.get("value"), self.dlp)

        # record destructive attempt for kill-switch
        if meta.get("kind") == "destructive":
            self.anomaly.record_destructive(identity)

        # dry-run short-circuit
        if tool in (self.cfg.dryrun_tools or []):
            result = {"dryRun": True, "tool": tool, "arguments": arguments}
            self.audit.record(identity, tool, arguments, "ALLOW", result, corr_id)
            return result

        # stage 8: rate limit
        is_admin = meta.get("module") == "control_plane"
        self.rate.check(is_admin=is_admin)

        # stage 9: execute via circuit breaker
        module = meta.get("module") or "ecosystem"

        def guarded() -> Any:
            self.backend.check_dependency(module)
            return handler(arguments, session)

        if self.cfg.circuit_breaker_enabled:
            breaker = self.breakers.get(module) or self.breakers["ecosystem"]
            result = breaker.call(guarded)
        else:
            result = guarded()

        # post: consume redaction + taint registration
        if tool == "consume_messages" and isinstance(result, dict):
            records = result.get("records") or []
            scrubbed_records = []
            taint_vals: List[Any] = []
            for rec in records:
                val = rec.get("value")
                if isinstance(val, str) and self.cfg.redaction_enabled:
                    red, hits, blocked = self.dlp.process(val)
                    # block-mode on sensitive categories for consume responses
                    if blocked and self.cfg.dlp_mode == "block":
                        raise McpError(
                            guardrails.SENSITIVE_DATA_BLOCKED,
                            "consume blocked by DLP",
                            data={"categories": sorted(hits)},
                        )
                    rec = dict(rec)
                    rec["value"] = red
                    taint_vals.append(red)
                    taint_vals.append(val)
                else:
                    taint_vals.append(val)
                scrubbed_records.append(rec)
            result = dict(result)
            result["records"] = scrubbed_records
            result = self._clamp_consume_bytes(result, self.cfg.hard_max_bytes)
            # also taint topic name from consumed data context
            if arguments.get("topic"):
                taint_vals.append(arguments.get("topic"))
            for rec in result.get("records") or []:
                if rec.get("value"):
                    taint_vals.append(rec.get("value"))
            register_taint(session, [v for v in taint_vals if v is not None])

        # scrub whole result
        result = guardrails.scrub_result(result, self.dlp, self.cfg)

        self.audit.record(identity, tool, arguments, "ALLOW", result, corr_id)
        return result
