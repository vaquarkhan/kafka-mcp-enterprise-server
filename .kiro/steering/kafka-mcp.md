---
inclusion: always
---

# Kiro steering — Kafka MCP Enterprise (KIP-1318)

Always follow the repository root **`AGENTS.md`**.

- This is a **reference** implementation of KIP-1318 (Jira KAFKA-20436), not the official Java MCP server.
- Install: `pip install kafka-mcp-enterprise` · CLI: `kafka-mcp-enterprise`
- Validate with `python run_tests.py` (must remain 85/85).
- Use portable skills under `skills/*/SKILL.md` for workflows (enterprise, security-review, examples).
- Do not add hard dependencies to `kafka_mcp/`; do not commit `internal/`.
