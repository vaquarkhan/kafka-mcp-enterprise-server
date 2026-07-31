# 03 - Change-window approvals

**Persona:** Platform engineer agent + human change approver  
**Goal:** Destructive delete only with a signed approval tied to a change ticket.

## Run

```bash
python examples/03_change_window_approvals/run.py
```

## Real-world data

| File | Description |
|------|-------------|
| `data/deprecated_topics.jsonl` | Topics scheduled for retirement |
| `data/change_ticket.json` | CHG ticket metadata (window, approver, TTL) |
