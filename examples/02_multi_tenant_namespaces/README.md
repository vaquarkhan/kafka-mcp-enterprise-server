# 02 — Multi-tenant namespace isolation

**Persona:** Payments agent vs inventory agent on a shared MCP gateway  
**Goal:** Each team may only touch its topic prefix (`-32041` on cross-tenant access).

## Run

```bash
python examples/02_multi_tenant_namespaces/run.py
```

## Real-world data

| File | Description |
|------|-------------|
| `data/payments_settlements.jsonl` | Settlement / chargeback events |
| `data/inventory_stock.jsonl` | Stock adjustments & reservation failures |
| `data/scenario.json` | Tenant prefixes + identities |
