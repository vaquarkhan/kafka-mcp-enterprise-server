"""Tool registry: classified tools with kind/module/operation/resource_arg + JSON Schemas."""

from __future__ import annotations

from typing import Any, Callable, Dict, Tuple

from .backend import InMemoryKafka


Handler = Callable[..., Any]
Meta = Dict[str, Any]

# Agent-facing descriptions (Glama / MCP hosts score these; keep purpose, usage, side effects).
TOOL_DESCRIPTIONS: Dict[str, str] = {
    "list_topics": (
        "List topic names known to the cluster. Read-only; no side effects. "
        "Use before describe_topic or produce/consume when you need the catalog. "
        "Prefer describe_topic for partitions/config of one topic."
    ),
    "describe_topic": (
        "Describe one topic by name: partitions, config, and metadata. Read-only. "
        "Requires `name`. Use list_topics first if the name is unknown. "
        "Does not create, alter, or delete the topic."
    ),
    "describe_cluster": (
        "Return cluster identity and broker/controller metadata. Read-only; no side effects. "
        "Use for health/context before mutating topics. Prefer list_topics for the topic catalog "
        "and kafka://health for circuit-breaker status."
    ),
    "list_consumer_groups": (
        "List consumer group ids. Read-only. Use describe_consumer_group for members, "
        "state, and offsets of one group. Does not join or leave groups."
    ),
    "describe_consumer_group": (
        "Describe one consumer group (members, state, offsets) by `groupId`. Read-only. "
        "Use list_consumer_groups to discover ids. Does not change group membership."
    ),
    "consume_messages": (
        "Read messages from a topic (bounded by maxMessages / hard_max_records). "
        "Omit groupId for Direct Partition Assignment (no consumer-group join/rebalance); "
        "set groupId only for classic group consume. Read path; may register values into "
        "session taint. Sensitive topics may require `_approval_token`. Prefer produce_message "
        "to write; prefer describe_topic for metadata only."
    ),
    "create_topic": (
        "Create a topic with optional partitions, replicationFactor, and config. Mutating. "
        "Fails if the topic already exists or name is out of allowed prefixes / policy. "
        "Use alter_topic_config to change config later; use delete_topic to remove."
    ),
    "alter_topic_config": (
        "Update dynamic config for an existing topic (`name` + `config` map). Mutating. "
        "Does not create or delete the topic. Use describe_topic to inspect current config; "
        "use create_topic only when the topic does not exist yet."
    ),
    "produce_message": (
        "Produce one message to a topic (`topic`, optional key/value/partition). Mutating write. "
        "Egress DLP may block secrets/PII in the value. Prefer consume_messages to read; "
        "does not create the topic if missing."
    ),
    "delete_topic": (
        "Permanently delete a topic and its data. Destructive; requires a valid HMAC "
        "`_approval_token` when approval is enabled. Irreversible for in-memory and broker "
        "data. Prefer alter_topic_config for non-destructive config changes."
    ),
    "create_acls": (
        "Create ACL bindings (resource, resourceType, operation, principal). Destructive "
        "authorization change; typically requires `_approval_token`. Binding resources must "
        "stay within topic/group scope prefixes. This reference does not list or delete ACLs; "
        "use broker-native admin for revoke/list outside this tool surface."
    ),
    "alter_consumer_group_offsets": (
        "Reset committed offsets for a consumer group (`groupId` + `offsets` list of "
        "{topic, partition, offset}). Mutating. KIP Phase 1 tool; group should be idle. "
        "Use describe_consumer_group / kafka://groups/{id}/lag to inspect before reset."
    ),
    "delete_consumer_group": (
        "Delete a consumer group and its committed offsets. Destructive; requires "
        "`_approval_token` when approval is enabled. Use list_consumer_groups to discover ids."
    ),
}

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
            "name": {
                "type": "string",
                "description": "Exact topic name to describe (must exist)",
            },
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
            "groupId": {
                "type": "string",
                "description": "Consumer group id to describe",
            },
        },
        "required": ["groupId"],
        "additionalProperties": False,
    },
    "consume_messages": {
        "type": "object",
        "properties": {
            "topic": {"type": "string", "description": "Topic to read from"},
            "maxMessages": {
                "type": "integer",
                "minimum": 1,
                "description": "Max records to return (clamped by hard_max_records)",
            },
            "fromBeginning": {
                "type": "boolean",
                "description": "If true, read from earliest available offset",
            },
            "groupId": {
                "type": "string",
                "description": "Optional; omit for Direct Partition Assignment (no rebalance)",
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
            "name": {"type": "string", "description": "New topic name"},
            "partitions": {
                "type": "integer",
                "minimum": 1,
                "description": "Partition count (default 1)",
            },
            "replicationFactor": {
                "type": "integer",
                "minimum": 1,
                "description": "Replication factor (default 1)",
            },
            "config": {
                "type": "object",
                "additionalProperties": {"type": "string"},
                "description": "Optional topic config key/value strings",
            },
        },
        "required": ["name"],
        "additionalProperties": False,
    },
    "alter_topic_config": {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "Existing topic name"},
            "config": {
                "type": "object",
                "additionalProperties": {"type": "string"},
                "description": "Config entries to set/overwrite",
            },
        },
        "required": ["name", "config"],
        "additionalProperties": False,
    },
    "produce_message": {
        "type": "object",
        "properties": {
            "topic": {"type": "string", "description": "Destination topic"},
            "value": {"type": "string", "description": "Message value (scanned by egress DLP)"},
            "key": {"type": "string", "description": "Optional message key"},
            "partition": {
                "type": "integer",
                "minimum": 0,
                "description": "Optional target partition",
            },
        },
        "required": ["topic"],
        "additionalProperties": False,
    },
    "delete_topic": {
        "type": "object",
        "properties": {
            "name": {"type": "string", "description": "Topic to delete permanently"},
            "_approval_token": {
                "type": "string",
                "description": "HMAC approval token when approval gate is enabled",
            },
        },
        "required": ["name"],
        "additionalProperties": False,
    },
    "create_acls": {
        "type": "object",
        "properties": {
            "bindings": {
                "type": "array",
                "description": "ACL bindings to create",
                "items": {
                    "type": "object",
                    "properties": {
                        "resource": {
                            "type": "string",
                            "description": "Resource name or prefix (scoped)",
                        },
                        "resourceType": {
                            "type": "string",
                            "enum": ["TOPIC", "GROUP", "CLUSTER"],
                        },
                        "operation": {
                            "type": "string",
                            "description": "Kafka ACL operation (e.g. READ, WRITE, ALTER)",
                        },
                        "principal": {
                            "type": "string",
                            "description": "Principal receiving the permission",
                        },
                    },
                },
            },
            "_approval_token": {
                "type": "string",
                "description": "HMAC approval token when approval gate is enabled",
            },
        },
        "required": ["bindings"],
        "additionalProperties": False,
    },
    "alter_consumer_group_offsets": {
        "type": "object",
        "properties": {
            "groupId": {"type": "string", "description": "Consumer group id"},
            "offsets": {
                "type": "array",
                "description": "Offsets to commit/reset",
                "items": {
                    "type": "object",
                    "properties": {
                        "topic": {"type": "string"},
                        "partition": {"type": "integer", "minimum": 0},
                        "offset": {"type": "integer", "minimum": 0},
                    },
                    "required": ["topic", "partition", "offset"],
                },
            },
        },
        "required": ["groupId", "offsets"],
        "additionalProperties": False,
    },
    "delete_consumer_group": {
        "type": "object",
        "properties": {
            "groupId": {"type": "string", "description": "Consumer group to delete"},
            "_approval_token": {
                "type": "string",
                "description": "HMAC approval token when approval gate is enabled",
            },
        },
        "required": ["groupId"],
        "additionalProperties": False,
    },
}


def _mcp_annotations(kind: str) -> Dict[str, Any]:
    """Standard MCP tool hints plus Kafka classification tags."""
    read_only = kind == "read"
    destructive = kind == "destructive"
    return {
        "readOnlyHint": read_only,
        "destructiveHint": destructive,
        "idempotentHint": read_only,
        "openWorldHint": False,
        "kind": kind,
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

    def alter_consumer_group_offsets(params: Dict[str, Any], session: Dict[str, Any]) -> Any:
        gid = params.get("groupId") or params.get("group_id")
        return backend.alter_group_offsets(str(gid), params.get("offsets") or [])

    def delete_consumer_group(params: Dict[str, Any], session: Dict[str, Any]) -> Any:
        gid = params.get("groupId") or params.get("group_id")
        return backend.delete_group(str(gid))

    def meta(
        name: str,
        *,
        kind: str,
        module: str,
        operation: str,
        **extra: Any,
    ) -> Meta:
        out: Meta = {
            "kind": kind,
            "module": module,
            "operation": operation,
            "description": TOOL_DESCRIPTIONS[name],
            **extra,
        }
        return out

    return {
        "list_topics": (
            list_topics,
            meta("list_topics", kind="read", module="control_plane", operation="DESCRIBE"),
        ),
        "describe_topic": (
            describe_topic,
            meta(
                "describe_topic",
                kind="read",
                module="control_plane",
                operation="DESCRIBE",
                resource_arg="name",
                topic_arg="name",
            ),
        ),
        "describe_cluster": (
            describe_cluster,
            meta("describe_cluster", kind="read", module="control_plane", operation="DESCRIBE"),
        ),
        "list_consumer_groups": (
            list_consumer_groups,
            meta(
                "list_consumer_groups",
                kind="read",
                module="control_plane",
                operation="DESCRIBE",
            ),
        ),
        "describe_consumer_group": (
            describe_consumer_group,
            meta(
                "describe_consumer_group",
                kind="read",
                module="control_plane",
                operation="DESCRIBE",
                group_arg="groupId",
                resource_arg="groupId",
            ),
        ),
        "consume_messages": (
            consume_messages,
            meta(
                "consume_messages",
                kind="read",
                module="data_plane",
                operation="READ",
                topic_arg="topic",
                resource_arg="topic",
            ),
        ),
        "create_topic": (
            create_topic,
            meta(
                "create_topic",
                kind="mutate",
                module="control_plane",
                operation="CREATE",
                resource_arg="name",
                topic_arg="name",
            ),
        ),
        "alter_topic_config": (
            alter_topic_config,
            meta(
                "alter_topic_config",
                kind="mutate",
                module="control_plane",
                operation="ALTER",
                resource_arg="name",
                topic_arg="name",
            ),
        ),
        "produce_message": (
            produce_message,
            meta(
                "produce_message",
                kind="mutate",
                module="data_plane",
                operation="WRITE",
                topic_arg="topic",
                resource_arg="topic",
            ),
        ),
        "delete_topic": (
            delete_topic,
            meta(
                "delete_topic",
                kind="destructive",
                module="control_plane",
                operation="DELETE",
                resource_arg="name",
                topic_arg="name",
            ),
        ),
        "create_acls": (
            create_acls,
            meta(
                "create_acls",
                kind="destructive",
                module="control_plane",
                operation="ALTER",
                # Scope checked via SecurityPipeline._check_create_acls_scope (bindings).
            ),
        ),
        "alter_consumer_group_offsets": (
            alter_consumer_group_offsets,
            meta(
                "alter_consumer_group_offsets",
                kind="mutate",
                module="control_plane",
                operation="ALTER",
                group_arg="groupId",
                resource_arg="groupId",
            ),
        ),
        "delete_consumer_group": (
            delete_consumer_group,
            meta(
                "delete_consumer_group",
                kind="destructive",
                module="control_plane",
                operation="DELETE",
                group_arg="groupId",
                resource_arg="groupId",
            ),
        ),
    }
