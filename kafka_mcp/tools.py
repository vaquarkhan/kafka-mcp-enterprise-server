"""Tool registry: 11 tools with kind/module/operation/resource_arg + JSON Schemas."""

from __future__ import annotations

from typing import Any, Callable, Dict, Tuple

from .backend import InMemoryKafka


Handler = Callable[..., Any]
Meta = Dict[str, Any]

# A2: real JSON Schemas for tools/list (LLM / host argument guidance)
TOOL_INPUT_SCHEMAS: Dict[str, Dict[str, Any]] = {
    "list_topics": {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    "describe_topic": {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "Topic name"},
        },
        "required": ["name"],
        "additionalProperties": False,
    },
    "describe_cluster": {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    "list_consumer_groups": {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    },
    "describe_consumer_group": {
        "type": "object",
        "properties": {
            "groupId": {"type": "string", "description": "Consumer group id"},
        },
        "required": ["groupId"],
        "additionalProperties": False,
    },
    "consume_messages": {
        "type": "object",
        "properties": {
            "topic": {"type": "string"},
            "maxMessages": {
                "type": "integer",
                "minimum": 1,
                "description": "Max records to return (clamped by hard_max_records)",
            },
            "fromBeginning": {"type": "boolean"},
            "groupId": {
                "type": "string",
                "description": "Optional; omit for Direct Partition Assignment",
            },
            "_approval_token": {
                "type": "string",
                "description": "Required when topic matches sensitive_topic_patterns",
            },
        },
        "required": ["topic"],
        "additionalProperties": False,
    },
    "create_topic": {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "partitions": {"type": "integer", "minimum": 1},
            "replicationFactor": {"type": "integer", "minimum": 1},
            "config": {"type": "object", "additionalProperties": {"type": "string"}},
        },
        "required": ["name"],
        "additionalProperties": False,
    },
    "alter_topic_config": {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "config": {"type": "object", "additionalProperties": {"type": "string"}},
        },
        "required": ["name", "config"],
        "additionalProperties": False,
    },
    "produce_message": {
        "type": "object",
        "properties": {
            "topic": {"type": "string"},
            "value": {"type": "string"},
            "key": {"type": "string"},
            "partition": {"type": "integer", "minimum": 0},
        },
        "required": ["topic"],
        "additionalProperties": False,
    },
    "delete_topic": {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "_approval_token": {"type": "string"},
        },
        "required": ["name"],
        "additionalProperties": False,
    },
    "create_acls": {
        "type": "object",
        "properties": {
            "bindings": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "resource": {"type": "string"},
                        "resourceType": {
                            "type": "string",
                            "enum": ["TOPIC", "GROUP", "CLUSTER"],
                        },
                        "operation": {"type": "string"},
                        "principal": {"type": "string"},
                    },
                },
            },
            "_approval_token": {"type": "string"},
        },
        "required": ["bindings"],
        "additionalProperties": False,
    },
}


def build_tools(backend: InMemoryKafka) -> Dict[str, Tuple[Handler, Meta]]:
    """Return tool_name -> (handler, meta)."""

    def list_topics(params: Dict[str, Any], session: Dict[str, Any]) -> Any:
        return backend.list_topics()

    def describe_topic(params: Dict[str, Any], session: Dict[str, Any]) -> Any:
        return backend.describe_topic(params["name"])

    def describe_cluster(params: Dict[str, Any], session: Dict[str, Any]) -> Any:
        return backend.describe_cluster()

    def list_consumer_groups(params: Dict[str, Any], session: Dict[str, Any]) -> Any:
        return backend.list_groups()

    def describe_consumer_group(params: Dict[str, Any], session: Dict[str, Any]) -> Any:
        gid = params.get("groupId") or params.get("group_id")
        return backend.describe_group(gid)

    def consume_messages(params: Dict[str, Any], session: Dict[str, Any]) -> Any:
        return backend.consume(
            topic=params["topic"],
            max_messages=int(params.get("maxMessages", params.get("max_messages", 10))),
            from_beginning=bool(params.get("fromBeginning", params.get("from_beginning", True))),
            group_id=params.get("groupId", params.get("group_id")),
        )

    def create_topic(params: Dict[str, Any], session: Dict[str, Any]) -> Any:
        return backend.create_topic(
            name=params["name"],
            partitions=int(params.get("partitions", 1)),
            replication_factor=int(
                params.get("replicationFactor", params.get("replication_factor", 1))
            ),
            config=params.get("config"),
        )

    def alter_topic_config(params: Dict[str, Any], session: Dict[str, Any]) -> Any:
        return backend.alter_topic_config(params["name"], params.get("config") or {})

    def produce_message(params: Dict[str, Any], session: Dict[str, Any]) -> Any:
        return backend.produce(
            topic=params["topic"],
            value=params.get("value", ""),
            key=params.get("key"),
            partition=int(params.get("partition", 0)),
        )

    def delete_topic(params: Dict[str, Any], session: Dict[str, Any]) -> Any:
        return backend.delete_topic(params["name"])

    def create_acls(params: Dict[str, Any], session: Dict[str, Any]) -> Any:
        bindings = params.get("bindings") or params.get("acls") or []
        return backend.create_acls(bindings)

    return {
        "list_topics": (
            list_topics,
            {"kind": "read", "module": "control_plane", "operation": "DESCRIBE"},
        ),
        "describe_topic": (
            describe_topic,
            {
                "kind": "read",
                "module": "control_plane",
                "operation": "DESCRIBE",
                "resource_arg": "name",
                "topic_arg": "name",
            },
        ),
        "describe_cluster": (
            describe_cluster,
            {"kind": "read", "module": "control_plane", "operation": "DESCRIBE"},
        ),
        "list_consumer_groups": (
            list_consumer_groups,
            {"kind": "read", "module": "control_plane", "operation": "DESCRIBE"},
        ),
        "describe_consumer_group": (
            describe_consumer_group,
            {
                "kind": "read",
                "module": "control_plane",
                "operation": "DESCRIBE",
                "group_arg": "groupId",
                "resource_arg": "groupId",
            },
        ),
        "consume_messages": (
            consume_messages,
            {
                "kind": "read",
                "module": "data_plane",
                "operation": "READ",
                "topic_arg": "topic",
                "resource_arg": "topic",
            },
        ),
        "create_topic": (
            create_topic,
            {
                "kind": "mutate",
                "module": "control_plane",
                "operation": "CREATE",
                "resource_arg": "name",
                "topic_arg": "name",
            },
        ),
        "alter_topic_config": (
            alter_topic_config,
            {
                "kind": "mutate",
                "module": "control_plane",
                "operation": "ALTER",
                "resource_arg": "name",
                "topic_arg": "name",
            },
        ),
        "produce_message": (
            produce_message,
            {
                "kind": "mutate",
                "module": "data_plane",
                "operation": "WRITE",
                "topic_arg": "topic",
                "resource_arg": "topic",
            },
        ),
        "delete_topic": (
            delete_topic,
            {
                "kind": "destructive",
                "module": "control_plane",
                "operation": "DELETE",
                "resource_arg": "name",
                "topic_arg": "name",
            },
        ),
        "create_acls": (
            create_acls,
            {
                "kind": "destructive",
                "module": "control_plane",
                "operation": "ALTER",
                # Scope checked via SecurityPipeline._check_create_acls_scope (bindings).
            },
        ),
    }
