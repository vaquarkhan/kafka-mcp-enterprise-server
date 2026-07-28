---
name: kafka-mcp-security-review
description: >-
  Reviews Kafka MCP changes against the KIP-1318 fail-closed pipeline, DLP,
  approval, taint honesty, scopes, breakers, and secure-by-default tool
  exposure. Use when reviewing PRs, security questions, or hardening this repo.
---

# Kafka MCP security review skill

## Review order (must match runtime)

1. Auth (bearer aud/iss) → `-32001`
2. Deny-list → `-32044`
3. Allow-list / readonly → `-32044`
4. Topic/group scope → `-32041`
5. Policy engine (fail-closed) → `-32044`
6. Taint / IFC (destructive; approval bypasses) → `-32040`
7. Approval → `-32042`
8. Rate limit → `-32029`
9. Execute via module circuit breaker → `-32043`

Also: validation `-32046`, egress `-32045`, quarantine `-32047`.

## Fail the review if

- Execute happens before policy checks
- Docs claim taint fully stops prompt injection
- Destructive tools enabled by default without calling out operator allow-list
- New third-party deps added to core without explicit user request
- Tests drop below 85/85 or omit new control paths

## Pass criteria

- Guardrails framed as complement to broker ACLs
- Audit ALLOW/DENY retained
- Examples/docs updated when behavior changes
