# Tools & resources

## Tools (13)

| Tool | kind | module | Kafka op | Resource arg |
|------|------|--------|----------|--------------|
| `list_topics` | read | control_plane | DESCRIBE | - |
| `describe_topic` | read | control_plane | DESCRIBE | `name` |
| `describe_cluster` | read | control_plane | DESCRIBE | - |
| `list_consumer_groups` | read | control_plane | DESCRIBE | - |
| `describe_consumer_group` | read | control_plane | DESCRIBE | `groupId` |
| `consume_messages` | read | data_plane | READ | `topic` |
| `create_topic` | mutate | control_plane | CREATE | `name` |
| `alter_topic_config` | mutate | control_plane | ALTER | `name` |
| `produce_message` | mutate | data_plane | WRITE | `topic` |
| `alter_consumer_group_offsets` | mutate | control_plane | ALTER | `groupId` |
| `delete_topic` | destructive | control_plane | DELETE | `name` |
| `delete_consumer_group` | destructive | control_plane | DELETE | `groupId` |
| `create_acls` | destructive | control_plane | ALTER | - |

Shipped default (`SECURE_DEFAULT_TOOLS`) exposes only the six **read** tools. Mutate/destructive tools require an explicit allow-list.

KIP tools **not** registered here (Connect, EOS, fencing, …) are intentional gaps - see [kip-alignment.md](kip-alignment.md). They belong in the **Java production** implementation, not this Python reference.

### Common arguments

- **Approval:** pass `_approval_token` (from `kafka_mcp.approval.mint(secret, tool_name)`). Required by default for `delete_topic`, `delete_consumer_group`, `create_acls`.
- **Consume:** `topic`, `maxMessages`, optional `groupId`, `fromBeginning`.
- **Produce:** `topic`, `value`, optional `key`, `partition`.
- **Alter offsets:** `groupId`, `offsets` = `[{topic, partition, offset}, ...]`.

## MCP methods

| Method | Result |
|--------|--------|
| `initialize` | protocolVersion, serverInfo, capabilities |
| `tools/list` | Visible tools (honors deny/allow/readonly) |
| `tools/call` | Full security pipeline + handler |
| `resources/list` | Catalog |
| `resources/read` | URI payload |

## Resources (`kafka://`)

| URI | Description |
|-----|-------------|
| `kafka://topics` | List topics |
| `kafka://topics/{name}` | Describe topic |
| `kafka://topics/{name}/offsets` | Earliest/latest offsets per partition |
| `kafka://cluster` | Cluster id + brokers |
| `kafka://groups` | Consumer groups |
| `kafka://groups/{id}` | Describe a consumer group |
| `kafka://groups/{id}/offsets` | Committed offsets |
| `kafka://groups/{id}/lag` | Per-topic/partition lag (KIP Phase 1) |
| `kafka://audit/recent` | Recent audit entries |
| `kafka://health` | Liveness + breaker states |
