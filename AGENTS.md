# AGENTS.md — Kafka MCP Enterprise (KIP-1318 reference)

Instructions for **any** coding agent (Cursor, Kiro, GitHub Copilot, ChatGPT/Codex, Google Gemini/Antigravity, Claude Code, Windsurf, etc.).

## What this repo is

- **Python stdlib-only reference** for [KIP-1318](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1318%3A+Model+Context+Protocol+%28MCP%29+Server+for+Apache+Kafka) / [KAFKA-20436](https://issues.apache.org/jira/browse/KAFKA-20436).
- PyPI: `kafka-mcp-enterprise-kip1318` · CLI: `kafka-mcp-enterprise` · import: `kafka_mcp`.
- **Not** the official Apache Kafka Java MCP server. Production language in the KIP is **Java**.
- In-memory Kafka backend for conformance — not a live broker client.

## Non-negotiables

1. **Fail-closed** 9-step order: auth → deny → allow/readonly → scope → policy → taint → approval → rate limit → circuit breaker execute.
2. **Broker ACLs are load-bearing**; MCP guardrails complement them. Do not claim MCP-only authz.
3. **Taint/IFC is best-effort** — never market as complete prompt-injection prevention.
4. Keep **zero hard third-party deps** for core (`kafka_mcp/`). OTel is optional extra only.
5. Do not commit `internal/` or `CURSOR-BUILD-INSTRUCTIONS.md` (gitignored).
6. Destructive tools must stay behind allow-list + approval in production guidance.
7. After behavior changes: `python run_tests.py` must stay **85/85**.

## Layout

```text
kafka_mcp/     # server package
tests/         # 72 conformance checks
examples/0N_*/ # one folder per scenario (run.py + data/)
doc/           # public docs
AGENTS.md      # this file
.cursor/skills/# Cursor skills
skills/        # portable skills (any IDE / ChatGPT / Kiro)
```

## Commands agents should run

```bash
python run_tests.py
python demo_end_to_end.py
python examples/01_sre_readonly_triage/run.py
echo {"jsonrpc":"2.0","id":1,"method":"tools/list"} | python serve_stdio.py
# or: kip install / kafka-mcp-enterprise
```

## How to help users

| User ask | Do this |
|----------|---------|
| Run / install | Point to README PyPI block + `doc/getting-started.md` |
| Security model | `doc/security-controls.md` + honesty rules above |
| Config knobs | `doc/configuration.md` (32 fields) |
| Errors | `doc/error-codes.md` |
| KIP gaps | `doc/kip-alignment.md` |
| Add example | New `examples/0N_name/{README.md,run.py,data/}` using `examples/_common.py` |
| Change pipeline | Update `server.py` / `security.py` / `guardrails.py` + matching tests |
| Publish | `doc/publishing.md` — package name `kafka-mcp-enterprise-kip1318` |

## Coding standards

- Match existing style; stdlib only in `kafka_mcp/`.
- Prefer small, focused diffs; do not rewrite docs wholesale unless asked.
- New security codes must appear in code **and** `doc/error-codes.md`.
- Examples must exit non-zero on failed expectations.
- Never invent Apache ASF “official” status for this Python package.

## Skills

Use project skills when relevant:

- Cursor: `.cursor/skills/*/SKILL.md`
- Portable (ChatGPT, Kiro, Gemini, etc.): `skills/*/SKILL.md`
- Index: `doc/agents-and-skills.md`
