# Security controls

## Fail-closed evaluation order

Every `tools/call` runs this order; **first denial wins**:

1. **Auth** — bearer audience/issuer when configured (`-32001`)
2. **Deny-list** — (`-32044`)
3. **Allow-list / readonly** — writes blocked in readonly (`-32044`)
4. **Topic / group scope** — prefix allow-lists (`-32041`)
5. **Policy engine** — fail-closed on deny or exception (`-32044`)
6. **Taint / IFC** — destructive tools; approval bypasses (`-32040`)
7. **Approval** — signed token for destructive / sensitive ops (`-32042`)
8. **Rate limit** — general vs admin buckets (`-32029`)
9. **Execute** — via per-module circuit breaker (`-32043`)

Additional guards around execute:

- Input validation (`-32046`)
- Rogue-agent quarantine (`-32047`)
- Egress DLP on produce (`-32045`)
- Sensitive-topic gating on consume
- Post-execute DLP scrub + audit ALLOW/DENY

## Secure-by-default guidance

In production, set `tools_allowed` to **read / non-destructive** tools only. Enable `delete_topic`, `create_acls`, etc. explicitly and keep them on `approval_required_tools`.

## Honesty (important)

| Control | Reality |
|---------|---------|
| Taint / IFC | **Best-effort** — can be defeated by data laundering |
| Broker ACLs | **Load-bearing** — least privilege at the cluster |
| MCP guardrails | **Complement** broker authz; they do not replace it |
| Audit hash chain | Tamper-**resistant** in-memory; export for true retention |

## DLP

Ten detectors (email, SSN, Luhn credit card, phone, IPv4, AWS key, private key, JWT, IBAN, secret assignment). Modes: `redact` | `block` | `off`. Default block categories: `private_key`, `aws_access_key`, `jwt`.
