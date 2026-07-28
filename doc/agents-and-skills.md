# Agents & skills (all IDEs)

Portable instructions so Cursor, Kiro, GitHub Copilot, ChatGPT/Codex, Google Gemini, Claude Code, and similar tools behave consistently in this repo.

## Primary file (all tools)

| File | Purpose |
|------|---------|
| [`AGENTS.md`](../AGENTS.md) | Canonical agent instructions — load this first |

Most modern agent hosts auto-discover `AGENTS.md` at the repo root. If not, paste or `@`-mention it.

## Skills

| Skill | Cursor path | Portable path |
|-------|-------------|-----------------|
| General Kafka MCP work | `.cursor/skills/kafka-mcp-enterprise/` | `skills/kafka-mcp-enterprise/` |
| Security review | `.cursor/skills/kafka-mcp-security-review/` | `skills/kafka-mcp-security-review/` |
| Examples authoring | `.cursor/skills/kafka-mcp-examples/` | `skills/kafka-mcp-examples/` |

Each skill is a folder with `SKILL.md` (YAML frontmatter + instructions).

## Per-product setup

### Cursor
- Open the repo; project skills under `.cursor/skills/` are available to Agent.
- `AGENTS.md` is picked up as project guidance.
- Invoke skills by name when relevant (e.g. “use kafka-mcp-security-review”).

### Kiro
- Steering file: `.kiro/steering/kafka-mcp.md` (always include).
- Also follow `AGENTS.md` and `skills/*/SKILL.md`.

### GitHub Copilot (VS Code / IDE)
- Uses `.github/copilot-instructions.md` (points at `AGENTS.md`).
- Optionally attach `skills/.../SKILL.md` in chat context.

### ChatGPT / Codex
- Upload or open the repo; instruct: “Follow AGENTS.md”.
- Attach `skills/kafka-mcp-enterprise/SKILL.md` for task-specific runs.
- Custom GPT: paste `AGENTS.md` into Instructions; add skill files as Knowledge.

### Google Gemini / Antigravity / Android Studio agents
- Point the agent at `AGENTS.md` as project rules.
- Attach portable `skills/` files when scaffolding examples or security reviews.

### Claude Code / Windsurf / other
- Prefer `AGENTS.md` as the project rule file.
- Import portable skills from `skills/` the same way you import other markdown playbooks.

## Suggested prompts (any IDE)

```text
Follow AGENTS.md. Run the conformance suite and fix any failures.
```

```text
Use the kafka-mcp-security-review skill on my latest changes.
```

```text
Use kafka-mcp-examples to add a new example folder with real JSONL data for …
```

```text
Explain how this maps to KIP-1318 / KAFKA-20436 without claiming ASF official status.
```
