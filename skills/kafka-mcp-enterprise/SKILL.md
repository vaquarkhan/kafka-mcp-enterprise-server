---
name: kafka-mcp-enterprise
description: >-
  Guides work on the kafka-mcp-enterprise-kip1318 reference server (KIP-1318 /
  KAFKA-20436): run tests, stdio MCP, security pipeline, examples, and PyPI
  packaging. Use when the user mentions Kafka MCP, KIP-1318, kafka_mcp,
  tools/call, DLP, approval tokens, or this repository.
---

# Kafka MCP Enterprise — agent skill

## Quick facts

- Package: `kafka-mcp-enterprise-kip1318` · CLI: `kafka-mcp-enterprise` · import: `kafka_mcp`
- Official KIP/Jira: KIP-1318 / KAFKA-20436 (Java is production target)
- This repo: Python stdlib reference + 72 conformance tests

## Always do

1. Read `AGENTS.md` at repo root.
2. Prefer existing modules over new dependencies.
3. After code changes run: `python run_tests.py` (expect `72/72`).
4. Preserve honesty: taint best-effort; broker ACLs authoritative.

## Common workflows

### Validate

```bash
python run_tests.py
python demo_end_to_end.py
python examples/01_sre_readonly_triage/run.py
```

### Call a tool in-process

```python
from kafka_mcp.config import Config
from kafka_mcp.server import KafkaMcpServer

s = KafkaMcpServer(Config(allowed_topic_prefixes=["agent."]))
print(s.handle({
  "jsonrpc":"2.0","id":1,"method":"tools/call",
  "params":{"name":"create_topic","arguments":{"name":"agent.demo"}}
}, {"identity":"agent"}))
```

### Add an example

Create `examples/0N_slug/{README.md,run.py,data/}` and seed via `examples._common.seed_topic_from_jsonl`.

### Security change checklist

- Update evaluation order docs if stages change
- Add/adjust tests under `tests/test_security.py` or `test_guardrails.py`
- Document new codes in `doc/error-codes.md`
