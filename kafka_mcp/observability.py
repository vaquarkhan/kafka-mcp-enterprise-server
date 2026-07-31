"""Observability hooks (OpenTelemetry optional — not required).

The reference server stays **stdlib-only**. Audit + ``kafka://health`` cover
local demos. For production Java (KIP-1318 / KAFKA-20436), prefer first-class
OpenTelemetry in the official module.

Python users who want OTel can install the optional extra::

    pip install kafka-mcp-enterprise[otel]

Then pass a custom tracer/meter into your host, or wrap ``KafkaMcpServer.handle``.
This module never imports OpenTelemetry unless it is installed.
"""

from __future__ import annotations

from typing import Any, Optional


def get_tracer(name: str = "kafka_mcp") -> Any:
    """Return an OpenTelemetry tracer if the SDK is installed, else a no-op."""
    try:
        from opentelemetry import trace  # type: ignore

        return trace.get_tracer(name)
    except Exception:
        return _NoopTracer()


def get_meter(name: str = "kafka_mcp") -> Any:
    """Return an OpenTelemetry meter if the SDK is installed, else a no-op."""
    try:
        from opentelemetry import metrics  # type: ignore

        return metrics.get_meter(name)
    except Exception:
        return _NoopMeter()


class _NoopSpan:
    def __enter__(self) -> "_NoopSpan":
        return self

    def __exit__(self, *args: Any) -> None:
        return None

    def set_attribute(self, *args: Any, **kwargs: Any) -> None:
        return None

    def record_exception(self, *args: Any, **kwargs: Any) -> None:
        return None


class _NoopTracer:
    def start_as_current_span(self, _name: str, **_kwargs: Any) -> _NoopSpan:
        return _NoopSpan()


class _NoopCounter:
    def add(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class _NoopMeter:
    def create_counter(self, *_args: Any, **_kwargs: Any) -> _NoopCounter:
        return _NoopCounter()


def otel_available() -> bool:
    try:
        import opentelemetry  # noqa: F401

        return True
    except Exception:
        return False
