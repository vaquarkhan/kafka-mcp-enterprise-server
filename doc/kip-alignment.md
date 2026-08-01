# KIP-1318 alignment (reference vs KIP)

Official tracking:

- **KIP:** [KIP-1318](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1318%3A+Model+Context+Protocol+%28MCP%29+Server+for+Apache+Kafka)
- **Jira:** [KAFKA-20436](https://issues.apache.org/jira/browse/KAFKA-20436)
- **This repo:** [vaquarkhan/kafka-mcp-enterprise-server](https://github.com/vaquarkhan/kafka-mcp-enterprise-server) (renamed from `…-kip-1318`; same project)
- **PyPI:** [`kafka-mcp-enterprise`](https://pypi.org/project/kafka-mcp-enterprise/)

> **Scope banner:** This is a **stdio, in-memory conformance reference** for the KIP-1318 **security control model**. Real brokers, Streamable HTTP, OAuth 2.1, EOS/fencing, Connect tools, distributed rate/breaker stores, and durable audit topics are **specified in the KIP** (and/or Java production track) and are **not implemented here**.

A KIP may be aspirational. A reference may be a subset. Do **not** claim the reference implements a capability listed as a gap below.

## Why some KIP items stay as intentional gaps

This repo is a **Python stdlib conformance / teaching reference**, not the KIP's production server. The KIP itself names **Java** as the production target and wraps real `Admin` / `KafkaProducer` / `KafkaConsumer`.

| Gap class | Why it stays out of *this* repo |
|-----------|----------------------------------|
| Streamable HTTP + OAuth server | Needs a real HTTP stack; KIP production path is Java/Jetty. Reference proves the control model over **stdio**. |
| Live broker backend | Would pull in Kafka client deps and cluster ops; reference uses **InMemoryKafka** so CI stays zero-dep and deterministic. |
| EOS / fencing / Connect / remaining ~20 tools | Need real brokers, Connect REST, transactional producers - Java-track work under [KAFKA-20436](https://issues.apache.org/jira/browse/KAFKA-20436). |
| Distributed rate/breaker/audit topic | Multi-replica shared state; out of scope for a single-process stdio teaching server. |
| JMX / shadowJar / graceful Java shutdown | Language- and packaging-specific to the Kafka Gradle module. |

What we *do* keep current: Phase-1 tools/resources that fit in-memory, the full fail-closed security pipeline, and a test matrix that **asserts gaps are absent** (`tests/test_kip_conformance.py`) so messaging cannot claim them.

## Implemented vs Spec-only (summary)

| Area | Reference (this repo) | KIP / Java production track |
|------|------------------------|-----------------------------|
| Transport | **stdio only** | stdio + Streamable HTTP + OAuth 2.1 / PKCE |
| Backend | **InMemoryKafka** (teaching) | Real Admin / Producer / Consumer |
| Tools | **13** registered tools | ~30+ including Connect, EOS, group admin, etc. |
| Resources | **subset** incl. Phase-1 lag/offsets (`topics`, `groups`, `cluster`, `audit`, `health`) | ~20 `kafka://` URIs |
| Rate / quarantine | **per-process** counters | Optional distributed backend + broker quota ceiling |
| Audit | **in-memory** hash-chained buffer | Durable append-only audit topic |
| Secure-by-default | **read/consume allow-list** (`SECURE_DEFAULT_TOOLS`) | Same posture in KIP text |
| Language | **Python** stdlib reference | **Java** production module |
| Conformance | **302/302** automated checks | Test plan in KIP |

## Implemented and covered by tests

| KIP feature | Status | Evidence |
|-------------|--------|----------|
| JSON-RPC: initialize, tools/list, tools/call, resources/list, resources/read | ✅ | 302-check suite + stdio |
| 11→13 classified tools + real `inputSchema` | ✅ | `tools.py` (Phase-1 group tools included) |
| MCP `content` / `isError` tool results | ✅ | `server.py` |
| Fixed `protocolVersion` negotiation | ✅ | `2024-11-05` |
| 9-step fail-closed pipeline | ✅ | `server.py` + `security.py` |
| Topic/group prefix scope | ✅ | `-32041` |
| Deny/allow/readonly | ✅ | `-32044` |
| Secure-by-default allow-list | ✅ | `SECURE_DEFAULT_TOOLS` in `config.py` |
| Policy engine fail-closed | ✅ | in-process callable |
| Taint guard + `ifc_strict` | ✅ | best-effort; honesty in docs |
| Approval tokens (HMAC/TTL/nonce/resource bind) | ✅ | `-32042`; registered destructive tools |
| Dry-run tools | ✅ | `dryrun_tools` |
| Rate limits | ✅ | `-32029` (per-process) |
| Per-module circuit breakers + `kafka://health` | ✅ | |
| Direct Partition Assignment | ✅ | no group / no rebalance when `groupId` omitted |
| DLP (10 detectors, Luhn) + egress | ✅ | `-32045` |
| Sensitive-topic gating | ✅ | `-32042` on consume |
| Bounded output | ✅ | hard caps + scrub truncate |
| Rogue-agent quarantine | ✅ | `-32047` |
| Identity propagation (in-memory ACLs) | ✅ | simulation, not live broker ACLs |
| Bearer audience/issuer | ✅ | `-32001` |
| Audit trail + `kafka://audit/recent` | ✅ | in-memory hash chain |
| Phase-1 group tools + lag resource | ✅ | `alter_consumer_group_offsets`, `delete_consumer_group`, `kafka://groups/{id}/lag` |
| KIP Test Plan matrix | ✅ | `tests/test_kip_conformance.py` |
| Error code table (15) | ✅ | `doc/error-codes.md` |
| Honesty: ACLs load-bearing / taint best-effort | ✅ | README + security docs |

## Intentional reference gaps (documented)

| Item | Notes |
|------|-------|
| **HTTP / Streamable HTTP** | Spec'd in KIP; reference ships **stdio** only (HTTP notes in `transport.py`, no server). |
| **Real Kafka brokers** | `InMemoryKafka` only - not a client. |
| **Tool coverage beyond 13** | Missing KIP tools include `delete_records`, `delete_acls`, `produce_batch` / `produce_transactional`, Connect suite, fencing, etc. |
| **Phase-1 exact set** | ✅ Covered: create/delete/produce/consume + `alter_consumer_group_offsets` / `delete_consumer_group` + `kafka://groups/{id}/lag`. `create_acls` remains (KIP Phase 2) as an in-memory binding recorder. |
| **Resource coverage** | No offsets/log-dirs/quorum/features/transactions/streams/share/connectors resources. |
| **Shared rate/breaker/quarantine store** | Per-process only. |
| **`mcp.policy.engine.url`** | Callable hook only; no OPA/Cedar HTTP client. |
| **`audit_topic` publish** | Config name only; not mirrored to Kafka. |
| **`dependency_timeout_ms`** | Field present; failures injected synchronously (no wall-clock loop). |
| **EOS / fencing / read_committed** | Not demonstrated (no real broker/transactions). |
| **JMX metrics** | Java-target; Python has optional OTel hooks only. |
| **Signed tool manifests** | Not present. |
| **Production language** | KIP: **Java**. This repo: Python teaching / validation artifact. |

## Examples coverage

| Example folder | KIP themes |
|----------------|------------|
| `01_sre_readonly_triage/` | readonly, allow-list, Direct Partition Assignment |
| `02_multi_tenant_namespaces/` | topic prefix scope |
| `03_change_window_approvals/` | approval gate, forged token |
| `04_pii_secret_egress_guard/` | egress `-32045`, PII redact |
| `05_blast_radius_resilience/` | breakers, quarantine |
| `06_identity_and_sensitive_topics/` | identity propagation ACLs, sensitive-topic approval |

---

## Note: Python reference vs Java production code

This repository is a **Python reference implementation**. It exists to teach and test the KIP-1318 control model (fail-closed pipeline, scopes, approval, DLP, error codes) with **zero hard third-party dependencies**.

It is **not** the production language for KIP-1318. The KIP’s production target is **Java**, wrapping real `Admin` / `KafkaProducer` / `KafkaConsumer`, delivered under [KAFKA-20436](https://issues.apache.org/jira/browse/KAFKA-20436).

**Why gaps remain here:** Streamable HTTP + OAuth, live brokers, EOS/fencing, Connect, distributed rate/breaker stores, durable audit topics, JMX, and the remaining tools/resources need that Java module and a real cluster. They are **intentionally not built** in this Python reference so we do not pretend a teaching harness is the shipped Kafka server.

**Where they will be added:** in the **actual Java production code** for KIP-1318 / KAFKA-20436 - not by claiming them as done in this repo. This Python tree stays the conformance / teaching artifact until (or alongside) that work.
