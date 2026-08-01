"""kafka:// resource handlers + RESOURCE_LIST catalog."""

from __future__ import annotations

from typing import Any, Callable, Dict, List, Optional
from urllib.parse import urlparse

from .backend import InMemoryKafka
from .errors import INVALID_PARAMS, METHOD_NOT_FOUND, McpError


RESOURCE_LIST: List[Dict[str, str]] = [
    {"uri": "kafka://topics", "name": "topics", "description": "List topics"},
    {"uri": "kafka://topics/{name}", "name": "topic", "description": "Describe a topic"},
    {
        "uri": "kafka://topics/{name}/offsets",
        "name": "topic-offsets",
        "description": "Earliest/latest offsets per partition",
    },
    {"uri": "kafka://cluster", "name": "cluster", "description": "Cluster metadata"},
    {"uri": "kafka://groups", "name": "groups", "description": "Consumer groups"},
    {
        "uri": "kafka://groups/{id}",
        "name": "group",
        "description": "Describe a consumer group",
    },
    {
        "uri": "kafka://groups/{id}/offsets",
        "name": "group-offsets",
        "description": "Committed offsets for a group",
    },
    {
        "uri": "kafka://groups/{id}/lag",
        "name": "group-lag",
        "description": "Per-topic/partition consumer lag (KIP Phase 1)",
    },
    {"uri": "kafka://audit/recent", "name": "audit", "description": "Recent audit entries"},
    {"uri": "kafka://health", "name": "health", "description": "Health / circuit status"},
]


def build_resource_handlers(
    backend: InMemoryKafka,
    audit_recent: Optional[Callable[[int], Any]] = None,
) -> Dict[str, Callable[[str], Any]]:
    """Map path prefixes to handlers. Full URI routing is in read_resource."""

    def _topics(_uri: str) -> Any:
        return backend.list_topics()

    def _cluster(_uri: str) -> Any:
        return backend.describe_cluster()

    def _groups(_uri: str) -> Any:
        return backend.list_groups()

    def _audit(_uri: str) -> Any:
        if audit_recent is None:
            return {"entries": []}
        return {"entries": audit_recent(50)}

    return {
        "topics": _topics,
        "cluster": _cluster,
        "groups": _groups,
        "audit": _audit,
    }


def read_resource(
    uri: str,
    backend: InMemoryKafka,
    audit_recent: Optional[Callable[[int], Any]] = None,
) -> Any:
    """Read a kafka:// resource URI."""
    if not uri or not uri.startswith("kafka://"):
        raise McpError(INVALID_PARAMS, f"invalid resource uri: {uri}")
    parsed = urlparse(uri)
    # urlparse("kafka://topics/foo") -> netloc=topics, path=/foo
    host = parsed.netloc or ""
    path = (parsed.path or "").lstrip("/")
    parts = [p for p in [host] + (path.split("/") if path else []) if p]

    if not parts:
        raise McpError(METHOD_NOT_FOUND, f"unknown resource uri: {uri}")

    head = parts[0]
    if head == "topics":
        if len(parts) == 1:
            return backend.list_topics()
        if len(parts) == 2:
            return backend.describe_topic(parts[1])
        if len(parts) == 3 and parts[2] == "offsets":
            return backend.topic_offsets(parts[1])
        raise McpError(METHOD_NOT_FOUND, f"unknown resource uri: {uri}")
    if head == "cluster":
        return backend.describe_cluster()
    if head == "groups":
        if len(parts) == 1:
            return backend.list_groups()
        if len(parts) == 2:
            return backend.describe_group(parts[1])
        if len(parts) == 3 and parts[2] == "offsets":
            return backend.group_offsets(parts[1])
        if len(parts) == 3 and parts[2] == "lag":
            return backend.group_lag_all(parts[1])
        raise McpError(METHOD_NOT_FOUND, f"unknown resource uri: {uri}")
    if head == "audit":
        if audit_recent is None:
            return {"entries": []}
        return {"entries": audit_recent(50)}
    if head == "health":
        # Caller may override; basic backend liveness
        return {"status": "ok", "clusterId": backend.cluster_id}

    raise McpError(METHOD_NOT_FOUND, f"unknown resource uri: {uri}")
