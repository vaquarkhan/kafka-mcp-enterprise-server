# KIP-1318 alignment (reference vs KIP)

Official tracking:

- **KIP:** [KIP-1318](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1318%3A+Model+Context+Protocol+%28MCP%29+Server+for+Apache+Kafka)
- **Jira:** [KAFKA-20436](https://issues.apache.org/jira/browse/KAFKA-20436)

This matrix compares the **Python reference** in this repo to the KIP-1318 feature set. Internal drafts live under `internal/` (gitignored).

## Implemented and covered by tests

| KIP feature | Status | Evidence |
|-------------|--------|----------|
| JSON-RPC: initialize, tools/list, tools/call, resources/list, resources/read | ✅ | 72-check suite + stdio |
| 11 classified tools | ✅ | `tools.py` |
| 9-step fail-closed pipeline | ✅ | `server.py` + `security.py` |
| Topic/group prefix scope | ✅ | `-32041` tests |
| Deny/allow/readonly | ✅ | `-32044` tests |
| Policy engine fail-closed | ✅ | tests |
| Taint guard + `ifc_strict` | ✅ | best-effort; honesty in docs |
| Approval tokens (HMAC/TTL) | ✅ | `-32042` |
| Dry-run tools | ✅ | `dryrun_tools` short-circuit |
| Rate limits | ✅ | `-32029` |
| Per-module circuit breakers + `kafka://health` | ✅ | report-mechanisms tests |
| Direct Partition Assignment | ✅ | no group / no rebalance |
| DLP (10 detectors, Luhn) + egress | ✅ | guardrails tests |
| Sensitive-topic gating | ✅ | `-32042` on consume |
| Bounded output (`hard_max_records`, `max_output_bytes`, `hard_max_bytes`) | ✅ | clamp + scrub truncate |
| Rogue-agent quarantine | ✅ | `-32047` |
| Identity propagation (in-memory ACLs) | ✅ | report-mechanisms tests |
| Bearer audience/issuer | ✅ | `-32001` |
| Audit trail + `kafka://audit/recent` | ✅ | hash-chained buffer |
| Error code table (15) | ✅ | code + `doc/error-codes.md` |
| Honesty: ACLs load-bearing / taint best-effort | ✅ | README + security docs |

## Intentional reference gaps (documented)

| Item | Notes |
|------|-------|
| **HTTP transport** | Spec’d in KIP; reference ships **stdio** + HTTP *notes* only (no HTTP server). |
| **Secure-by-default `tools_allowed`** | KIP *recommends* read/non-destructive default. Config default remains `["*"]` for harness flexibility — **operators must tighten** (see `doc/configuration.md`). |
| **Real Kafka brokers** | `InMemoryKafka` only — teaching/conformance, not a client. |
| **`mcp.policy.engine.url`** | Callable hook in-process; no HTTP policy client. |
| **`audit_topic` publish** | Name configured; trail is in-memory (+ resource), not mirrored to a durable Kafka topic. |
| **`dependency_timeout_ms`** | Config field present; reference failures are injected synchronously (no wall-clock timeout loop). |
| **Extra approval tool names** | `delete_records`, `delete_acls`, … listed as approval-required names for forward-compat; not all are registered tools yet. |
| **Production language** | KIP: **Java**. This repo: Python teaching reference. |

## Examples coverage

| Example folder | KIP themes |
|----------------|------------|
| `01_sre_readonly_triage/` | readonly, allow-list, Direct Partition Assignment |
| `02_multi_tenant_namespaces/` | topic prefix scope |
| `03_change_window_approvals/` | approval gate, forged token |
| `04_pii_secret_egress_guard/` | egress `-32045`, PII redact |
| `05_blast_radius_resilience/` | breakers, quarantine |
| `06_identity_and_sensitive_topics/` | identity propagation ACLs, sensitive-topic approval |
