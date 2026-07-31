# 06 - Identity propagation + sensitive topics

**Persona:** Least-privilege reader + security officer approvals  
**Goal:** Propagated ACLs deny CREATE; sensitive payroll consume requires approval; DLP still redacts SSN on read.

## Run

```bash
python examples/06_identity_and_sensitive_topics/run.py
```

## Real-world data

| File | Description |
|------|-------------|
| `data/agent_events.jsonl` | Product analytics events (safe namespace) |
| `data/secret_payroll.jsonl` | Payroll rows with salary + SSN (sensitive) |
| `data/scenario.json` | Reader ACLs + sensitive patterns |
