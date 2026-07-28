#!/usr/bin/env python3
"""Console entrypoints for the KIP-1318 Kafka MCP reference package."""

from __future__ import annotations

import logging
import os
import sys

from .config import Config
from .server import KafkaMcpServer
from .transport import serve_stdio

logger = logging.getLogger("kafka_mcp.cli")


def _cfg_from_env() -> Config:
    prefixes = os.environ.get("MCP_ALLOWED_TOPIC_PREFIXES", "*")
    cfg = Config(
        allowed_topic_prefixes=[p.strip() for p in prefixes.split(",") if p.strip()],
    )
    allowed = os.environ.get("MCP_TOOLS_ALLOWED")
    if allowed:
        cfg.tools_allowed = [t.strip() for t in allowed.split(",") if t.strip()]
    if os.environ.get("MCP_READONLY", "").lower() in ("1", "true", "yes"):
        cfg.readonly = True
    secret = os.environ.get("MCP_APPROVAL_SIGNING_SECRET")
    if secret:
        cfg.approval_signing_secret = secret.encode("utf-8")
    if os.environ.get("MCP_IDENTITY_PROPAGATION", "").lower() in ("1", "true", "yes"):
        cfg.identity_propagation = True
    if os.environ.get("MCP_SCRUB_ALL_OUTPUTS", "").lower() in ("1", "true", "yes"):
        cfg.scrub_all_outputs = True
    if os.environ.get("MCP_DLP_REDACT_IPV4", "").lower() in ("1", "true", "yes"):
        cfg.dlp_redact_ipv4 = True
    return cfg


def main_stdio() -> None:
    """Run newline-delimited JSON-RPC over stdio (MCP host integration)."""
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    cfg = _cfg_from_env()
    if not cfg.approval_signing_secret:
        logger.warning(
            "MCP_APPROVAL_SIGNING_SECRET unset — approval-gated tools will deny all tokens"
        )
    server = KafkaMcpServer(cfg)
    session = {"identity": os.environ.get("MCP_IDENTITY", "stdio-user")}
    serve_stdio(server, session)


if __name__ == "__main__":
    main_stdio()
