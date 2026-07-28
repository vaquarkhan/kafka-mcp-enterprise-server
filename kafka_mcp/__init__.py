"""Kafka MCP Enterprise Server reference implementation (KIP-1318).

Teaching / conformance package aligned with KIP-1318 / KAFKA-20436.
Production recommendation in the KIP is Java.
"""

from .observability import get_meter, get_tracer, otel_available

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "get_tracer",
    "get_meter",
    "otel_available",
]
