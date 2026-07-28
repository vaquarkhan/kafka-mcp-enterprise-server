# Error codes

## Standard JSON-RPC

| Code | Constant | Meaning |
|------|----------|---------|
| -32700 | `PARSE_ERROR` | JSON parse error |
| -32600 | `INVALID_REQUEST` | Invalid request |
| -32601 | `METHOD_NOT_FOUND` | Unknown method/tool |
| -32602 | `INVALID_PARAMS` | Invalid params / structured Kafka errors |
| -32603 | `INTERNAL_ERROR` | Internal error |

## Security / operational

| Code | Constant | Where | Meaning |
|------|----------|-------|---------|
| -32001 | `UNAUTHORIZED` | `auth.py` | Bad/missing bearer |
| -32029 | `RATE_LIMITED` | `resilience.py` | Rate limited |
| -32040 | `TAINT_VIOLATION` | `security.py` | Tainted value into destructive tool |
| -32041 | `SCOPE_VIOLATION` | `security.py` | Topic/group out of scope |
| -32042 | `APPROVAL_REQUIRED` | `security.py` | Missing/invalid approval |
| -32043 | `DEPENDENCY_UNAVAILABLE` | `resilience.py` / backend | Breaker open / dependency down |
| -32044 | `POLICY_DENIED` | `security.py` | Deny/allow/readonly/policy/ACL prop |
| -32045 | `SENSITIVE_DATA_BLOCKED` | `guardrails.py` | Egress / DLP block |
| -32046 | `VALIDATION_FAILED` | `guardrails.py` | Bad identifier / oversized value |
| -32047 | `QUARANTINED` | `guardrails.py` | Rogue-agent kill-switch |

Responses look like:

```json
{"jsonrpc":"2.0","id":1,"error":{"code":-32041,"message":"topic out of scope: prod.ledger"}}
```
