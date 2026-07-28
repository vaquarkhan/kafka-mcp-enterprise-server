"""Configuration dataclass for the Kafka MCP reference server (32 fields)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, List, Optional


@dataclass
class Config:
    """Authoritative config surface (~28 user-facing mcp.* properties + host keys)."""

    bootstrap_servers: str = "in-memory:9092"
    transport: str = "stdio"
    # Secure-by-default recommendation in KIP: prefer read/non-destructive allow-list.
    # Default "*" keeps the harness flexible; tests/demos set explicit allow-lists.
    tools_allowed: List[str] = field(default_factory=lambda: ["*"])
    tools_denied: List[str] = field(default_factory=list)
    readonly: bool = False
    allowed_topic_prefixes: List[str] = field(default_factory=lambda: ["*"])
    allowed_group_prefixes: List[str] = field(default_factory=lambda: ["*"])
    taint_guard_enabled: bool = True
    approval_required_tools: List[str] = field(
        default_factory=lambda: [
            "delete_topic",
            "delete_records",
            "create_acls",
            "delete_acls",
            "alter_partition_reassignments",
            "alter_broker_config",
        ]
    )
    dryrun_tools: List[str] = field(default_factory=list)
    audit_topic: str = "__mcp_audit"
    policy_engine: Optional[Callable[..., Any]] = None
    circuit_breaker_enabled: bool = True
    dependency_timeout_ms: int = 10000
    rate_requests_per_second: int = 50
    rate_admin_requests_per_second: int = 20
    oauth_expected_audience: Optional[str] = None
    oauth_expected_issuer: Optional[str] = None
    approval_signing_secret: bytes = b"reference-approval-key"
    redaction_enabled: bool = True
    dlp_mode: str = "redact"  # redact | block | off
    dlp_block_categories: List[str] = field(
        default_factory=lambda: ["private_key", "aws_access_key", "jwt"]
    )
    scrub_all_outputs: bool = True
    redact_sensitive_configs: bool = True
    sensitive_topic_patterns: List[str] = field(default_factory=list)
    max_value_bytes: int = 1_000_000
    max_output_bytes: int = 262_144
    max_destructive_per_minute: int = 10
    ifc_strict: bool = False
    hard_max_records: int = 100
    hard_max_bytes: int = 1_048_576
    identity_propagation: bool = False
