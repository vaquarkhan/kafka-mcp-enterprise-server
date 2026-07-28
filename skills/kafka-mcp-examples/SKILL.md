---
name: kafka-mcp-examples
description: >-
  Creates or fixes production-grade examples under examples/0N_*/ with README,
  run.py, and real-world data/ fixtures for the Kafka MCP KIP-1318 reference.
  Use when adding demos, scenarios, or sample JSONL data.
---

# Kafka MCP examples skill

## Layout (required)

```text
examples/0N_short_slug/
  README.md      # persona, goal, data dictionary
  run.py         # executable; exit 0/1
  data/          # *.jsonl / *.json fixtures
```

## Rules

- Use `examples._common` helpers (`seed_topic_from_jsonl`, `expect_ok`, `expect_code`).
- Seed from files — do not hardcode large payloads in `run.py`.
- Print PASS/FAIL; `finish(ok)` for exit codes.
- Update `examples/README.md` catalog.
- Run the new example before finishing.
