"""MCP / JSON-RPC error codes for the Kafka MCP reference server."""

from __future__ import annotations

from typing import Any, Dict, Optional


# Standard JSON-RPC 2.0
PARSE_ERROR = -32700
INVALID_REQUEST = -32600
METHOD_NOT_FOUND = -32601
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603

# Security / operational (defined here; auth/guardrails also contribute codes)
RATE_LIMITED = -32029
TAINT_VIOLATION = -32040
SCOPE_VIOLATION = -32041
APPROVAL_REQUIRED = -32042
DEPENDENCY_UNAVAILABLE = -32043
POLICY_DENIED = -32044


class McpError(Exception):
    """Structured MCP/JSON-RPC error."""

    def __init__(
        self,
        code: int,
        message: str,
        data: Optional[Any] = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data

    def to_dict(self) -> Dict[str, Any]:
        err: Dict[str, Any] = {"code": self.code, "message": self.message}
        if self.data is not None:
            err["data"] = self.data
        return err
