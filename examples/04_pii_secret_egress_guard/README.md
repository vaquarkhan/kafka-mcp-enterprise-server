# 04 - PII / secret egress guard

**Persona:** Customer-support copilot  
**Goal:** Allow normal case events; block secret egress; redact PII on consume.

## Run

```bash
python examples/04_pii_secret_egress_guard/run.py
```

## Real-world data

| File | Description |
|------|-------------|
| `data/support_cases.jsonl` | Support case stream including a PII-bearing note |
| `data/egress_attempts.json` | Allowed event + secret paste attempts (AWS key, JWT) |
