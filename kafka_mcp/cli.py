#!/usr/bin/env python3
"""Console entrypoints for the KIP-1318 Kafka MCP reference package."""

from __future__ import annotations

import os

from .config import Config
from .server import KafkaMcpServer
from .transport import serve_stdio


def main_stdio() -> None:
    """Run newline-delimited JSON-RPC over stdio (MCP host integration)."""
    prefixes = os.environ.get("MCP_ALLOWED_TOPIC_PREFIXES", "*")
    cfg = Config(
        allowed_topic_prefixes=[p.strip() for p in prefixes.split(",") if p.strip()],
    )
    # Optional hardening via env (production-shaped defaults when set)
    allowed = os.environ.get("MCP_TOOLS_ALLOWED")
    if allowed:
        cfg.tools_allowed = [t.strip() for t in allowed.split(",") if t.strip()]
    if os.environ.get("MCP_READONLY", "").lower() in ("1", "true", "yes"):
        cfg.readonly = True
    server = KafkaMcpServer(cfg)
    session = {"identity": os.environ.get("MCP_IDENTITY", "stdio-user")}
    serve_stdio(server, session)


if __name__ == "__main__":
    main_stdio()
