# 05 — Blast-radius resilience

**Persona:** Shared MCP gateway under partial outage + compromised agent  
**Goal:** Control-plane breaker isolates admin failures; data plane keeps serving; rogue deletes quarantined.

## Run

```bash
python examples/05_blast_radius_resilience/run.py
```

## Real-world data

| File | Description |
|------|-------------|
| `data/orders.jsonl` | Order events on the data-plane topic |
| `data/scenario.json` | Identities, quarantine threshold, temp topic names |
