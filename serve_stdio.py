#!/usr/bin/env python3
"""stdio entry point - thin wrapper around kafka_mcp.cli."""

from kafka_mcp.cli import main_stdio

if __name__ == "__main__":
    main_stdio()
