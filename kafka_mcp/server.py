"""KafkaMcpServer: JSON-RPC dispatch + full fail-closed control pipeline."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Dict, List, Optional

from . import approval as approval_mod
from . import auth as auth_mod
from . import guardrails
from .audit import AuditSink
from .backend import InMemoryKafka
from .config import DEFAULT_PROTOCOL_VERSION, SUPPORTED_PROTOCOL_VERSIONS, Config
from .dlp import Dlp
from .errors import (
    INTERNAL_ERROR,
    INVALID_REQUEST,
    METHOD_NOT_FOUND,
    POLICY_DENIED,
    SENSITIVE_DATA_BLOCKED,
    McpError,
)
from .resilience import CircuitBreaker, RateLimiter
from .security import SecurityPipeline, register_taint
from .tools import TOOL_INPUT_SCHEMAS, _mcp_annotations, build_tools
from . import resources as resources_mod

logger = logging.getLogger("kafka_mcp.server")


def wrap_tool_result(domain: Any, *, is_error: bool = False) -> Dict[str, Any]:
    """A1: MCP tools/call result shape - content blocks + isError."""
    if isinstance(domain, str):
        text = domain
    else:
        text = json.dumps(domain, default=str)
    return {
        "content": [{"type": "text", "text": text}],
        "isError": bool(is_error),
    }


def unwrap_tool_result(mcp_result: Any) -> Any:
    """Helper for tests/clients: parse domain object from MCP content wrapper."""
    if not isinstance(mcp_result, dict) or "content" not in mcp_result:
        return mcp_result
    blocks = mcp_result.get("content") or []
    if not blocks:
        return mcp_result
    text = blocks[0].get("text") if isinstance(blocks[0], dict) else None
    if text is None:
        return mcp_result
    try:
        return json.loads(text)
    except (TypeError, json.JSONDecodeError):
        return text


class KafkaMcpServer:
    """Reference MCP server implementing KIP-1318 control order."""

    def __init__(self, cfg: Optional[Config] = None, backend: Optional[InMemoryKafka] = None) -> None:
        self.cfg = cfg or Config()
        self.backend = backend or InMemoryKafka()
        self.security = SecurityPipeline(self.cfg)
        self.dlp = Dlp(
            self.cfg.dlp_mode,
            self.cfg.dlp_block_categories,
            redact_ipv4=self.cfg.dlp_redact_ipv4,
        )
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
            for b in self.breakers.values():
                b.threshold = 10**9
        self._warn_insecure_defaults()

    def _warn_insecure_defaults(self) -> None:
        if not self.cfg.approval_signing_secret:
            logger.warning(
                "approval_signing_secret is unset: approval-gated tools cannot be "
                "authorized until MCP_APPROVAL_SIGNING_SECRET / Config secret is set"
            )
        if not self.cfg.identity_propagation:
            logger.warning(
                "identity_propagation=False: broker ACL enforcement via identity is skipped "
                "(prefix scope + guardrails only)"
            )

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
        # A3: advertise a fixed supported version (do not mirror arbitrary client claims)
        client_v = params.get("protocolVersion")
        if client_v and client_v not in SUPPORTED_PROTOCOL_VERSIONS:
            logger.info(
                "client protocolVersion %s not in supported set %s; returning %s",
                client_v,
                sorted(SUPPORTED_PROTOCOL_VERSIONS),
                DEFAULT_PROTOCOL_VERSION,
            )
        return {
            "protocolVersion": DEFAULT_PROTOCOL_VERSION,
            "serverInfo": {"name": "kafka-mcp-reference", "version": "0.1.3"},
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
            allowed = self.cfg.tools_allowed
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
            kind = str(meta.get("kind") or "read")
            tools.append(
                {
                    "name": name,
                    "description": meta.get("description")
                    or f"{kind} / {meta.get('module')} / {meta.get('operation')}",
                    "inputSchema": TOOL_INPUT_SCHEMAS.get(name, {"type": "object"}),
                    "annotations": {
                        **_mcp_annotations(kind),
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
        # B3: never accept identity from tool arguments
        arguments.pop("_identity", None)
        identity = str(session.get("identity") or "anonymous")
        session["identity"] = identity
        corr_id = session.get("corr_id") or str(uuid.uuid4())
        session["corr_id"] = corr_id

        guardrails.validate_arguments(tool, arguments, self.cfg)
        self.anomaly.check(identity)

        self.security.authorize(tool, meta, arguments, session)

        if self.cfg.identity_propagation:
            op = meta.get("operation") or "ALL"
            resource_key = meta.get("resource_arg") or meta.get("topic_arg") or meta.get("group_arg")
            resource = str(arguments.get(resource_key, "")) if resource_key else ""
            if not self.backend.authorize(identity, op, resource or "*"):
                raise McpError(
                    POLICY_DENIED,
                    f"broker ACL denied for {identity} {op} {resource}",
                )

        if tool == "consume_messages":
            mm = int(arguments.get("maxMessages", arguments.get("max_messages", 10)))
            mm = min(mm, self.cfg.hard_max_records)
            arguments["maxMessages"] = mm
            topic = str(arguments.get("topic", ""))
            used = (
                self.security.used_nonces
                if getattr(self.cfg, "approval_single_use_nonce", True)
                else None
            )
            guardrails.sensitive_topic_requires_approval(
                topic,
                self.cfg.sensitive_topic_patterns or [],
                arguments,
                approval_mod.verify,
                self.cfg.approval_signing_secret,
                tool="consume_messages",
                principal=identity,
                used_nonces=used,
            )

        if tool == "produce_message":
            guardrails.egress_scan(arguments.get("value"), self.dlp)

        if meta.get("kind") == "destructive":
            self.anomaly.record_destructive(identity)

        if tool in (self.cfg.dryrun_tools or []):
            domain = {"dryRun": True, "tool": tool, "arguments": arguments}
            self.audit.record(identity, tool, arguments, "ALLOW", domain, corr_id)
            return wrap_tool_result(domain)

        is_admin = meta.get("module") == "control_plane"
        self.rate.check(is_admin=is_admin)

        module = meta.get("module") or "ecosystem"

        def guarded() -> Any:
            self.backend.check_dependency(module)
            return handler(arguments, session)

        try:
            if self.cfg.circuit_breaker_enabled:
                breaker = self.breakers.get(module) or self.breakers["ecosystem"]
                result = breaker.call(guarded)
            else:
                result = guarded()
        except McpError:
            raise
        except Exception as e:
            # A1: unexpected tool execution failure -> isError content (not only JSON-RPC)
            domain = {"error": str(e), "tool": tool}
            self.audit.record(identity, tool, arguments, "DENY", domain, corr_id)
            return wrap_tool_result(domain, is_error=True)

        if tool == "consume_messages" and isinstance(result, dict):
            records = result.get("records") or []
            scrubbed_records = []
            taint_vals: List[Any] = []
            for rec in records:
                val = rec.get("value")
                if isinstance(val, str) and self.cfg.redaction_enabled:
                    red, hits, blocked = self.dlp.process(val)
                    if blocked and self.cfg.dlp_mode == "block":
                        raise McpError(
                            SENSITIVE_DATA_BLOCKED,
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
            # Taint message payloads only (not the topic name arg - avoids false positives
            # on later mutate tools that reuse the same topic identifier).
            for rec in result.get("records") or []:
                if rec.get("value"):
                    taint_vals.append(rec.get("value"))
            register_taint(session, [v for v in taint_vals if v is not None], self.cfg)

        result = guardrails.scrub_result(result, self.dlp, self.cfg)

        self.audit.record(identity, tool, arguments, "ALLOW", result, corr_id)
        return wrap_tool_result(result)
