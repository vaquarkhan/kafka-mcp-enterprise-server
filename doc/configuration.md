# Configuration

`kafka_mcp.config.Config` - fields below. Pass into `KafkaMcpServer(Config(...))`.

| Field | Default | Purpose |
|-------|---------|---------|
| `bootstrap_servers` | `in-memory:9092` | Broker bootstrap (reference uses in-memory) |
| `transport` | `stdio` | Transport hint |
| `tools_allowed` | `SECURE_DEFAULT_TOOLS` (read/consume only) | Allow-list. Use `["*"]` only in harnesses; expand deliberately for mutate/destructive |
| `tools_denied` | `[]` | Deny-list |
| `readonly` | `False` | Block all non-read tools (incl. produce) |
| `allowed_topic_prefixes` | `["*"]` | Topic namespace scope |
| `allowed_group_prefixes` | `["*"]` | Group namespace scope |
| `taint_guard_enabled` | `True` | Context → **mutate + destructive** taint checks |
| `taint_min_length` | `8` | Ignore shorter tainted spans (reduces false positives) |
| `taint_max_values` | `256` | Cap size of session tainted set |
| `approval_required_tools` | `delete_topic`, `delete_consumer_group`, `create_acls` | Registered tools needing `_approval_token` |
| `approval_signing_secret` | `None` | **Required** to mint/verify tokens (no hardcoded default). Set via Config or `MCP_APPROVAL_SIGNING_SECRET` |
| `approval_single_use_nonce` | `True` | Reject replayed approval nonces (in-process) |
| `dryrun_tools` | `[]` | Tools that return dry-run result only |
| `audit_topic` | `__mcp_audit` | Audit topic name |
| `policy_engine` | `None` | Callable `(tool, params, session) -> bool` |
| `circuit_breaker_enabled` | `True` | Per-module breakers (**per-process**) |
| `dependency_timeout_ms` | `10000` | Dependency timeout hint |
| `rate_requests_per_second` | `50` | General RPS (**per-process**) |
| `rate_admin_requests_per_second` | `20` | Control-plane RPS (**per-process**) |
| `oauth_expected_audience` | `None` | Bearer `aud` (off if unset) |
| `oauth_expected_issuer` | `None` | Bearer `iss` (off if unset) |
| `redaction_enabled` | `True` | Enable redaction paths |
| `dlp_mode` | `redact` | `redact` / `block` / `off` |
| `dlp_block_categories` | private_key, aws_access_key, jwt | Always-block categories |
| `dlp_redact_ipv4` | `False` | Opt-in IPv4 redaction (off so metadata IPs survive) |
| `scrub_all_outputs` | `False` | Walk/scrub entire result trees (off by default) |
| `scrub_payloads_only` | `True` | Scrub message `records[].value` (+ sensitive configs) |
| `redact_sensitive_configs` | `True` | Mask password-like configs |
| `sensitive_topic_patterns` | `[]` | Consume requires approval |
| `max_value_bytes` | `1_000_000` | Produce value cap |
| `max_output_bytes` | `262_144` | Response truncation (preserves top-level shape) |
| `max_destructive_per_minute` | `10` | Rogue kill-switch threshold (**per-process**) |
| `ifc_strict` | `False` | Block mutate/destructive after any untrusted read |
| `hard_max_records` | `100` | Hard clamp on `maxMessages` |
| `hard_max_bytes` | `1_048_576` | Hard byte bound |
| `identity_propagation` | `False` | Check `backend` per-principal ACLs (warns when False) |

## Environment (CLI `kafka-mcp-enterprise`)

| Env | Effect |
|-----|--------|
| `MCP_APPROVAL_SIGNING_SECRET` | Sets `approval_signing_secret` |
| `MCP_IDENTITY` | Session identity (stdio transport; never from tool args) |
| `MCP_IDENTITY_PROPAGATION` | `1`/`true` enables broker ACL checks |
| `MCP_ALLOWED_TOPIC_PREFIXES` | Comma-separated prefixes |
| `MCP_TOOLS_ALLOWED` | Comma-separated allow-list |
| `MCP_READONLY` | `1`/`true` enables readonly |
| `MCP_SCRUB_ALL_OUTPUTS` | `1`/`true` enables full-tree scrub |
| `MCP_DLP_REDACT_IPV4` | `1`/`true` enables IPv4 redaction |

## Example: production-shaped defaults

```python
from kafka_mcp.config import Config

cfg = Config(
    approval_signing_secret=b"use-a-real-secret-from-your-vault",
    identity_propagation=True,
    tools_allowed=[
        "list_topics", "describe_topic", "describe_cluster",
        "list_consumer_groups", "describe_consumer_group", "consume_messages",
    ],
    allowed_topic_prefixes=["agent.", "observability."],
    allowed_group_prefixes=["agent."],
    rate_requests_per_second=20,
    rate_admin_requests_per_second=5,
    hard_max_records=50,
    scrub_payloads_only=True,
    scrub_all_outputs=False,
    dlp_mode="redact",
)
```

## MCP wire notes

- `tools/call` results are wrapped as `{"content":[{"type":"text","text":"<json>"}], "isError": false}`.
- `tools/list` publishes real JSON Schemas per tool (`TOOL_INPUT_SCHEMAS`).
- `initialize` always returns protocol version `2024-11-05` (supported set), not an arbitrary client echo.
