# Kafka MCP Enterprise Server

[![PyPI](https://img.shields.io/pypi/v/kafka-mcp-enterprise-kip1318.svg)](https://pypi.org/project/kafka-mcp-enterprise-kip1318/)
[![Python](https://img.shields.io/pypi/pyversions/kafka-mcp-enterprise-kip1318.svg)](https://pypi.org/project/kafka-mcp-enterprise-kip1318/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

### PyPI

```bash
pip install kafka-mcp-enterprise-kip1318
echo {"jsonrpc":"2.0","id":1,"method":"tools/list"} | kafka-mcp-enterprise
```

| | |
|---|---|
| **Package** | [`kafka-mcp-enterprise-kip1318`](https://pypi.org/project/kafka-mcp-enterprise-kip1318/) |
| **CLI** | `kafka-mcp-enterprise` |
| **Import** | `import kafka_mcp` |
| **Optional** | `pip install kafka-mcp-enterprise-kip1318[otel]` |

### Agents & skills (Cursor, Kiro, ChatGPT, Gemini, Copilot, …)

| | |
|---|---|
| **AGENTS.md** | [`AGENTS.md`](AGENTS.md) — canonical instructions for every coding agent |
| **Skills** | [`.cursor/skills/`](.cursor/skills/) (Cursor) · [`skills/`](skills/) (portable) |
| **Guide** | [`doc/agents-and-skills.md`](doc/agents-and-skills.md) — how to load in each IDE |

---

**Reference implementation** of [**KIP-1318**](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1318%3A+Model+Context+Protocol+%28MCP%29+Server+for+Apache+Kafka): a first-party [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server for Apache Kafka—secure by design, fail-closed by default, and built for agent workloads that must not become a confused deputy on your cluster.

| | |
|---|---|
| **KIP** | [KIP-1318: MCP Server for Apache Kafka](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1318%3A+Model+Context+Protocol+%28MCP%29+Server+for+Apache+Kafka) |
| **Jira** | [KAFKA-20436](https://issues.apache.org/jira/browse/KAFKA-20436) — *Implement KIP-1318* |
| **Discuss** | [[DISCUSS] KIP-1318](https://www.mail-archive.com/dev@kafka.apache.org/msg155971.html) on `dev@kafka.apache.org` |
| **This repo** | Stdlib Python **reference / conformance** server (teaching, demos, security validation) |
| **KIP production target** | **Java** module (`tools/mcp-server`) wrapping native Kafka clients — see the KIP |

> **Scope clarity:** The Apache Kafka project tracks the official implementation under **KAFKA-20436**. This repository is an independent, zero-dependency **reference** that encodes the enterprise control plane, error model, and conformance tests so designs can be validated before or alongside the Java work. It is **not** a drop-in replacement for the forthcoming first-party Java MCP server.

---

## Why this exists

AI agents need governed Kafka access—not ad-hoc scripts, unbounded consumes, or shared “god” principals. KIP-1318 proposes a standalone MCP process (stdio / HTTP) that exposes tools and `kafka://` resources without changing the Kafka wire protocol. This reference implements the **security and operability spine** that enterprise reviewers expect:

- Fail-closed **9-step** control evaluation before any mutate/destructive call  
- **Direct Partition Assignment** for ephemeral consume (no accidental rebalance storms)  
- DLP / egress controls, approval gates, taint/IFC (best-effort), scopes, breakers, audit  

Broker **ACLs remain authoritative**. Guardrails here *complement* them; they never replace them.

---

## Features (production-minded)

| Area | What you get |
|------|----------------|
| **MCP surface** | JSON-RPC 2.0 — `initialize`, `tools/list`, `tools/call`, `resources/list`, `resources/read` |
| **Tools** | 11 classified tools (`read` / `mutate` / `destructive` × `data_plane` / `control_plane`) |
| **Resources** | `kafka://topics`, `kafka://cluster`, `kafka://groups`, `kafka://audit/recent`, `kafka://health` |
| **Security pipeline** | Auth → deny → allow/readonly → scope → policy → taint → approval → rate limit → circuit breaker |
| **Data protection** | DLP (10 detectors + Luhn), egress block, sensitive-topic gating, output scrubbing & hard caps |
| **Resilience** | Per-module circuit breakers, dual rate buckets, rogue-agent quarantine |
| **Identity** | Optional per-principal ACL propagation (in-memory backend for the reference) |
| **Audit** | Hash-chained recent trail + resource export |
| **Quality bar** | **72/72** conformance checks, stdio integration tests, 6 real-world examples, stdlib-only |

### Error model (agent-operable)

Standard JSON-RPC plus security codes `-32001`, `-32029`, `-32040`…`-32047` — see [doc/error-codes.md](doc/error-codes.md).

---

## Quick start

Requires **Python 3.8+**. Core has **no third-party packages**.

```bash
# From source
python run_tests.py
python demo_end_to_end.py
python examples/01_sre_readonly_triage/run.py
echo {"jsonrpc":"2.0","id":1,"method":"tools/list"} | python serve_stdio.py
```

See the **PyPI** section at the top for `pip install`, or [doc/publishing.md](doc/publishing.md) to publish a release.

---

## Documentation & examples

| Resource | Description |
|----------|-------------|
| **[doc/](doc/README.md)** | End-to-end guides: getting started, architecture, security, config, tools, errors, testing |
| **[doc/kip-alignment.md](doc/kip-alignment.md)** | Feature matrix vs KIP-1318 — what is implemented vs intentional reference gaps |
| **[doc/publishing.md](doc/publishing.md)** | PyPI package `kafka-mcp-enterprise-kip1318` |
| **[doc/observability.md](doc/observability.md)** | OpenTelemetry: optional, not required |
| **[doc/agents-and-skills.md](doc/agents-and-skills.md)** | AGENTS.md + skills for all IDEs |
| **[examples/](examples/README.md)** | Six folder-based scenarios (`run.py` + real-world `data/` fixtures) |

---

## Repository layout

```text
kafka_mcp/          # Reference server package (security pipeline + in-memory Kafka)
tests/              # 72-check conformance suite
doc/                # Public documentation
examples/           # Production-shaped scenarios
serve_stdio.py      # stdio entrypoint
run_tests.py        # Master test runner
demo_end_to_end.py  # 22-step control demo
```

---

## Engineering standards

This reference aims at **production-grade practice** even while staying a teaching implementation:

| Practice | How it shows up |
|----------|-----------------|
| **Fail-closed** | First denial wins; no execute-then-check paths |
| **Least privilege** | Prefix scopes, allow/deny lists, readonly, approval for destructive ops |
| **Defense in depth** | MCP controls + explicit honesty that **broker ACLs** are load-bearing |
| **Bounded blast radius** | Hard record/byte caps, rate limits, per-plane breakers, quarantine |
| **Observable denials** | Stable error codes, correlation IDs, audit ALLOW/DENY |
| **Testability** | Deterministic in-memory backend; security + integration coverage |
| **Zero dependency debt** | Python **stdlib only** — easy to audit and run in CI |
| **Clear product boundary** | Official Kafka delivery tracked on **KAFKA-20436** (Java) |

### Honest limitations (by design)

- **Taint / IFC is best-effort** — defeatable by data laundering; do not treat as complete mediation.  
- **In-memory Kafka** — validates control logic; not a broker client.  
- **stdio-first** — HTTP is specified in the KIP; this reference documents notes, full HTTP is a Java/production concern.  
- **Secure-by-default in ops** — tighten `tools_allowed` to read/non-destructive in real deployments (see configuration guide).

---

## Related links

- **KIP-1318 (wiki):** https://cwiki.apache.org/confluence/display/KAFKA/KIP-1318%3A+Model+Context+Protocol+%28MCP%29+Server+for+Apache+Kafka  
- **Jira:** https://issues.apache.org/jira/browse/KAFKA-20436  
- **MCP specification:** https://modelcontextprotocol.io/  

---

## License & affiliation

Apache Kafka, KIP-1318, and KAFKA-20436 are trademarks / projects of the Apache Software Foundation. This repository is a **community reference** aligned with that proposal; it is not the official ASF deliverable unless and until merged under the Kafka project.
