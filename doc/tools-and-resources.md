# Tools & resources

## Tools (11)

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
| `delete_topic` | destructive | control_plane | DELETE | `name` |
| `create_acls` | destructive | control_plane | ALTER | - |

### Common arguments

- **Approval:** pass `_approval_token` (from `kafka_mcp.approval.mint(secret, tool_name)`).
- **Consume:** `topic`, `maxMessages`, optional `groupId`, `fromBeginning`.
- **Produce:** `topic`, `value`, optional `key`, `partition`.

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
| `kafka://cluster` | Cluster id + brokers |
| `kafka://groups` | Consumer groups |
| `kafka://audit/recent` | Recent audit entries |
| `kafka://health` | Liveness + breaker states |
