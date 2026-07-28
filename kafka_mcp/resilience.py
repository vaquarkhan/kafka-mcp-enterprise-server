"""Token bucket, rate limiter, and circuit breaker primitives.

C1 note: RateLimiter, CircuitBreaker, and AnomalyTracker are **per-process /
in-memory**. Limits do not sync across replicas. For multi-instance HTTP
deployments, treat them as per-instance or plug in a shared store (out of
scope for this stdio reference).
"""

from __future__ import annotations

import time
from typing import Callable, Optional, TypeVar

from .errors import DEPENDENCY_UNAVAILABLE, RATE_LIMITED, McpError

T = TypeVar("T")


class TokenBucket:
    """Simple token bucket with continuous refill."""

    def __init__(self, rate: float, capacity: Optional[float] = None) -> None:
        self.rate = float(rate) if rate > 0 else 0.001
        self.capacity = float(capacity if capacity is not None else max(rate, 1.0))
        self.tokens = self.capacity
        self.updated = time.monotonic()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.updated
        self.updated = now
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

    def allow(self, cost: float = 1.0) -> bool:
        self._refill()
        if self.tokens >= cost:
            self.tokens -= cost
            return True
        return False


class RateLimiter:
    """Dual-bucket rate limiter for general vs admin/control tools."""

    def __init__(self, general_rps: float, admin_rps: float) -> None:
        # Capacity slightly above rate so a short burst can be observed in tests.
        self.general = TokenBucket(general_rps, capacity=max(general_rps, 1.0))
        self.admin = TokenBucket(admin_rps, capacity=max(admin_rps, 1.0))

    def check(self, is_admin: bool = False) -> None:
        bucket = self.admin if is_admin else self.general
        if not bucket.allow(1.0):
            raise McpError(RATE_LIMITED, "rate limited")


class CircuitBreaker:
    """Closed -> Open -> Half-Open circuit breaker."""

    def __init__(
        self,
        name: str,
        threshold: int = 3,
        reset_seconds: float = 5.0,
    ) -> None:
        self.name = name
        self.threshold = threshold
        self.reset_seconds = reset_seconds
        self.failures = 0
        self.state = "closed"  # closed | open | half_open
        self.opened_at: Optional[float] = None

    def _maybe_half_open(self) -> None:
        if self.state == "open" and self.opened_at is not None:
            if time.monotonic() - self.opened_at >= self.reset_seconds:
                self.state = "half_open"

    def call(self, fn: Callable[[], T]) -> T:
        self._maybe_half_open()
        if self.state == "open":
            raise McpError(
                DEPENDENCY_UNAVAILABLE,
                f"circuit breaker open: {self.name}",
            )
        try:
            result = fn()
            self.failures = 0
            self.state = "closed"
            self.opened_at = None
            return result
        except McpError as e:
            if e.code == DEPENDENCY_UNAVAILABLE:
                self.failures += 1
                if self.failures >= self.threshold:
                    self.state = "open"
                    self.opened_at = time.monotonic()
            raise
        except Exception:
            self.failures += 1
            if self.failures >= self.threshold:
                self.state = "open"
                self.opened_at = time.monotonic()
            raise
