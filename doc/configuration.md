# Configuration

`kafka_mcp.config.Config` — 32 fields. Pass into `KafkaMcpServer(Config(...))`.

| Field | Default | Purpose |
|-------|---------|---------|
| `bootstrap_servers` | `in-memory:9092` | Broker bootstrap (reference uses in-memory) |
| `transport` | `stdio` | Transport hint |
| `tools_allowed` | `["*"]` | Allow-list (`*` = all for harness). **KIP secure-by-default:** set an explicit read/non-destructive list in prod |
| `tools_denied` | `[]` | Deny-list |
| `readonly` | `False` | Block all non-read tools (incl. produce) |
| `allowed_topic_prefixes` | `["*"]` | Topic namespace scope |
| `allowed_group_prefixes` | `["*"]` | Group namespace scope |
| `taint_guard_enabled` | `True` | Context→destructive taint checks |
| `approval_required_tools` | delete/ACL/alter list | Tools needing `_approval_token` |
| `dryrun_tools` | `[]` | Tools that return dry-run result only |
| `audit_topic` | `__mcp_audit` | Audit topic name |
| `policy_engine` | `None` | Callable `(tool, params, session) -> bool` |
| `circuit_breaker_enabled` | `True` | Per-module breakers |
| `dependency_timeout_ms` | `10000` | Dependency timeout hint |
| `rate_requests_per_second` | `50` | General RPS |
| `rate_admin_requests_per_second` | `20` | Control-plane RPS |
| `oauth_expected_audience` | `None` | Bearer `aud` (off if unset) |
| `oauth_expected_issuer` | `None` | Bearer `iss` (off if unset) |
| `approval_signing_secret` | `b"reference-approval-key"` | HMAC key for tokens |
| `redaction_enabled` | `True` | Enable redaction paths |
| `dlp_mode` | `redact` | `redact` / `block` / `off` |
| `dlp_block_categories` | private_key, aws_access_key, jwt | Always-block categories |
| `scrub_all_outputs` | `True` | Walk/scrub entire results |
| `redact_sensitive_configs` | `True` | Mask password-like configs |
| `sensitive_topic_patterns` | `[]` | Consume requires approval |
| `max_value_bytes` | `1_000_000` | Produce value cap |
| `max_output_bytes` | `262_144` | Response truncation |
| `max_destructive_per_minute` | `10` | Rogue kill-switch threshold |
| `ifc_strict` | `False` | Block destructive after any untrusted read |
| `hard_max_records` | `100` | Hard clamp on `maxMessages` |
| `hard_max_bytes` | `1_048_576` | Hard byte bound |
| `identity_propagation` | `False` | Check `backend` per-principal ACLs |

## Example: production-shaped defaults

```python
from kafka_mcp.config import Config

cfg = Config(
    tools_allowed=[
        "list_topics", "describe_topic", "describe_cluster",
        "list_consumer_groups", "describe_consumer_group", "consume_messages",
    ],
    allowed_topic_prefixes=["agent.", "observability."],
    allowed_group_prefixes=["agent."],
    readonly=False,
    rate_requests_per_second=20,
    rate_admin_requests_per_second=5,
    hard_max_records=50,
    scrub_all_outputs=True,
    dlp_mode="redact",
)
```
