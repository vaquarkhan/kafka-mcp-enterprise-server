# 01 — SRE read-only triage

**Persona:** Night-shift SRE agent  
**Goal:** Inspect production observability topics; never mutate Kafka.

## Run

```bash
python examples/01_sre_readonly_triage/run.py
```

## Real-world data

| File | Description |
|------|-------------|
| `data/observability_errors.jsonl` | Prod-shaped ERROR/WARN events (checkout, payments, inventory) |
| `data/scenario.json` | Persona + policy snapshot |

## Controls demonstrated

- `readonly=True` + read-only allow-list  
- Topic prefix scope (`agent.observability.`)  
- Direct Partition Assignment on consume (no consumer group)
