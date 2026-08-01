# Examples

Production-grade scenarios for the Kafka MCP **reference** server ([KIP-1318](https://cwiki.apache.org/confluence/display/KAFKA/KIP-1318%3A+Model+Context+Protocol+%28MCP%29+Server+for+Apache+Kafka) / [KAFKA-20436](https://issues.apache.org/jira/browse/KAFKA-20436)).

Each example is a **self-contained folder**: `README.md`, `run.py`, and `data/` with realistic fixtures (JSON / JSONL).

## Run (from repository root)

```bash
python examples/01_sre_readonly_triage/run.py
python examples/02_multi_tenant_namespaces/run.py
python examples/03_change_window_approvals/run.py
python examples/04_pii_secret_egress_guard/run.py
python examples/05_blast_radius_resilience/run.py
python examples/06_identity_and_sensitive_topics/run.py
```

## Catalog

| Folder | Story | Fixture highlights |
|--------|-------|--------------------|
| [`01_sre_readonly_triage/`](01_sre_readonly_triage/) | Night-shift SRE: inspect only | Prod ERROR/WARN observability events |
| [`02_multi_tenant_namespaces/`](02_multi_tenant_namespaces/) | Payments vs inventory isolation | Settlements + stock adjustments |
| [`03_change_window_approvals/`](03_change_window_approvals/) | Approved destructive delete | Change ticket + deprecated topic inventory |
| [`04_pii_secret_egress_guard/`](04_pii_secret_egress_guard/) | Support copilot DLP | Case notes with PII; secret paste attempts |
| [`05_blast_radius_resilience/`](05_blast_radius_resilience/) | Breakers + rogue quarantine | Order stream under admin outage |
| [`06_identity_and_sensitive_topics/`](06_identity_and_sensitive_topics/) | ACL propagation + sensitive consume | Analytics events + payroll (SSN) |

## Layout convention

```text
examples/
  _common.py                 # shared helpers (seed JSONL, expect_ok/code)
  0N_name/
    README.md                # persona, controls, data dictionary
    run.py                   # executable scenario
    data/                    # real-world fixtures
      *.jsonl / *.json
```

Each `run.py` exits **0** on success and **non-zero** if an expected control did not fire.

---

## Note: Python reference vs Java production code

These examples exercise the **Python reference** server (in-memory Kafka). They do not demonstrate live brokers, Streamable HTTP, or EOS. Those belong in the **Java production** implementation under KAFKA-20436 - see [`doc/kip-alignment.md`](../doc/kip-alignment.md).
